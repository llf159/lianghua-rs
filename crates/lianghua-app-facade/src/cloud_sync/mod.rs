mod bundles;
mod database;
mod dav;
mod strategies;
mod watchlists;
pub use strategies::transfer as transfer_strategies;

pub use watchlists::{
    TransferMode as WatchlistTransferMode, TransferResult as WatchlistTransferResult,
    transfer as transfer_watchlists,
};

use chrono::Utc;
use flate2::{Compression, read::GzDecoder, write::GzEncoder};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::{Read, Write},
    path::{Component, Path, PathBuf},
    sync::RwLock,
};

// 普通 IPC 任务持共享锁；普通同步在快照和应用下载时独占，明确恢复在保留本地内容至替换完成期间独占。
pub static SOURCE_ACCESS: RwLock<()> = RwLock::new(());

#[derive(Clone, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WebDavConfig {
    pub endpoint: String,
    pub folder: String,
    pub username: String,
    #[serde(default)]
    pub password: String,
    #[serde(default)]
    pub has_password: bool,
}

impl WebDavConfig {
    fn root_url(&self) -> Result<reqwest::Url, String> {
        let mut url = reqwest::Url::parse(self.endpoint.trim()).map_err(|_| "WebDAV 地址无效")?;
        if url.scheme() != "https"
            && !(url.scheme() == "http"
                && matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "[::1]")))
        {
            return Err("WebDAV 必须使用 HTTPS；仅本机测试允许 HTTP".into());
        }
        if !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err("地址中不能包含账号、密码、查询参数或片段".into());
        }
        let folder = self.folder.trim();
        if folder.is_empty() || folder.contains(['/', '\\']) || matches!(folder, "." | "..") {
            return Err("请填写一个子文件夹名称，不能直接使用网盘根目录".into());
        }
        url.path_segments_mut()
            .map_err(|_| "地址不支持目录")?
            .pop_if_empty()
            .push(folder)
            .push("");
        if self.username.trim().is_empty() || self.password.is_empty() {
            return Err("请填写账号和第三方应用密码".into());
        }
        Ok(url)
    }
}

pub fn get_config(app_data: &Path) -> Result<WebDavConfig, String> {
    let path = app_data.join("cloud-webdav.json");
    if !path.exists() {
        return Ok(WebDavConfig {
            endpoint: "https://dav.jianguoyun.com/dav/".into(),
            folder: "lianghua-rs".into(),
            ..Default::default()
        });
    }
    read_json(&path)
}

pub fn save_config(app_data: &Path, mut config: WebDavConfig) -> Result<(), String> {
    let old = get_config(app_data)?;
    if config.password.is_empty()
        && config.endpoint.trim() == old.endpoint
        && config.username.trim() == old.username
    {
        config.password = old.password;
    }
    config.endpoint = config.endpoint.trim().into();
    config.folder = config.folder.trim().into();
    config.username = config.username.trim().into();
    config.root_url()?;
    config.has_password = !config.password.is_empty();
    atomic_write(
        &app_data.join("cloud-webdav.json"),
        &serde_json::to_vec(&config).map_err(|e| e.to_string())?,
    )
}

pub fn test_connection(app_data: &Path) -> Result<(), String> {
    let mut dav = dav::Dav::new(&get_config(app_data)?)?;
    dav.acquire()?;
    dav.release()
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
struct Lease {
    owner: String,
    expires: i64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CloudVersion {
    pub id: String,
    pub hash: String,
    pub parents: Vec<String>,
    pub created_at: String,
    pub size: u64,
    pub compressed_size: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
struct Manifest {
    format: u32,
    entries: BTreeMap<String, Vec<CloudVersion>>,
    deleted: BTreeSet<String>,
    garbage: BTreeSet<String>,
    lease: Option<Lease>,
    #[serde(default)]
    packs: BTreeMap<String, String>,
}
impl Default for Manifest {
    fn default() -> Self {
        Self {
            format: 1,
            entries: BTreeMap::new(),
            deleted: BTreeSet::new(),
            garbage: BTreeSet::new(),
            lease: None,
            packs: BTreeMap::new(),
        }
    }
}
impl Manifest {
    fn validate(&self) -> Result<(), String> {
        if !matches!(self.format, 1 | 2 | 3) {
            return Err("云端同步格式不兼容，请更新应用".into());
        }
        if (self.format == 1 && !self.packs.is_empty())
            || self.packs.iter().any(|(hash, pack)| {
                !valid_hash(hash) || !valid_hash(pack) || self.packs.contains_key(pack)
            })
        {
            return Err("云端小文件包索引无效".into());
        }
        for (path, versions) in &self.entries {
            validate_path(path)?;
            for version in versions {
                for hash in [&version.hash, &version.id]
                    .into_iter()
                    .chain(version.parents.iter())
                {
                    if !valid_hash(hash) {
                        return Err("云端版本标识无效".into());
                    }
                }
            }
        }
        if self
            .garbage
            .iter()
            .chain(self.deleted.iter())
            .any(|h| !valid_hash(h))
        {
            return Err("云端目录标识无效".into());
        }
        Ok(())
    }
    fn hashes(&self) -> BTreeSet<String> {
        self.entries
            .values()
            .flatten()
            .filter(|v| !self.deleted.contains(&v.id))
            .map(|v| v.hash.clone())
            .chain(self.packs.values().cloned())
            .collect()
    }
    fn heads(&self, path: &str) -> Vec<CloudVersion> {
        let Some(versions) = self.entries.get(path) else {
            return vec![];
        };
        let parents: BTreeSet<_> = versions.iter().flat_map(|v| &v.parents).collect();
        versions
            .iter()
            .filter(|v| !parents.contains(&v.id) && !self.deleted.contains(&v.id))
            .cloned()
            .collect()
    }
    fn add(
        &mut self,
        path: &str,
        hash: &str,
        parents: Vec<String>,
        size: u64,
        compressed_size: u64,
    ) -> CloudVersion {
        let mut parents = parents;
        parents.sort();
        parents.dedup();
        let id = digest(format!("{path}\n{hash}\n{}", parents.join("\n")).as_bytes());
        let versions = self.entries.entry(path.into()).or_default();
        if let Some(version) = versions.iter().find(|v| v.id == id) {
            return version.clone();
        }
        let version = CloudVersion {
            id,
            hash: hash.into(),
            parents,
            created_at: Utc::now().to_rfc3339(),
            size,
            compressed_size,
        };
        versions.push(version.clone());
        version
    }
    fn prune(&mut self) {
        for path in [
            "ranking/scoring_result.zip",
            "similarity/strategy_similarity.zip",
        ] {
            let heads = self.heads(path);
            if let Some(versions) = self.entries.get_mut(path) {
                versions.retain(|version| {
                    if heads.iter().any(|head| head.id == version.id) {
                        true
                    } else {
                        self.garbage.insert(version.hash.clone());
                        false
                    }
                });
            }
        }
        for (kind, count) in [("ranking/", 5), ("similarity/", 1)] {
            let mut keys: Vec<_> = self
                .entries
                .keys()
                .filter(|p| p.starts_with(kind) && !database::is_snapshot(p))
                .cloned()
                .collect();
            keys.sort_by(|a, b| b.cmp(a));
            for key in keys.into_iter().skip(count) {
                if let Some(versions) = self.entries.remove(&key) {
                    self.garbage.extend(versions.into_iter().map(|v| v.hash));
                }
            }
        }
    }
}

#[derive(Clone, Deserialize, Serialize)]
struct Base {
    id: String,
    hash: String,
}
#[derive(Default, Deserialize, Serialize)]
struct SyncState {
    base: BTreeMap<String, Base>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CloudItem {
    pub path: String,
    pub version: CloudVersion,
    pub is_head: bool,
    pub conflict: bool,
    pub is_local_base: bool,
}
#[derive(Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncResult {
    pub uploaded: usize,
    pub downloaded: usize,
    pub skipped: usize,
    pub conflicts: Vec<String>,
    pub transferred_bytes: u64,
    pub uploaded_bytes: u64,
    pub downloaded_bytes: u64,
    pub settings: BTreeMap<String, String>,
    pub warnings: Vec<String>,
}

fn replacement_path(path: &str) -> Option<&'static str> {
    bundles::for_path(path).or_else(|| {
        if database::is_snapshot(path) || database::date_from_path(path).is_err() {
            return None;
        }
        Some(if path.starts_with("ranking/") {
            "ranking/scoring_result.zip"
        } else {
            "similarity/strategy_similarity.zip"
        })
    })
}

pub fn list_cloud(app_data: &Path, source: &Path) -> Result<Vec<CloudItem>, String> {
    let config = get_config(app_data)?;
    let state_path = local_root(app_data, source, &config)?.join("state.json");
    let state: SyncState = if state_path.exists() {
        read_json(&state_path)?
    } else {
        SyncState::default()
    };
    let mut dav = dav::Dav::new(&config)?;
    dav.read_manifest()?;
    let mut items = Vec::new();
    for (path, versions) in &dav.manifest.entries {
        let superseded =
            replacement_path(path).is_some_and(|group| !dav.manifest.heads(group).is_empty());
        let heads = if superseded {
            Vec::new()
        } else {
            dav.manifest.heads(path)
        };
        for version in versions
            .iter()
            .filter(|v| !dav.manifest.deleted.contains(&v.id))
        {
            items.push(CloudItem {
                path: path.clone(),
                version: version.clone(),
                is_head: heads.iter().any(|v| v.id == version.id),
                conflict: heads.len() > 1,
                is_local_base: state
                    .base
                    .get(path)
                    .is_some_and(|base| base.id == version.id),
            });
        }
    }
    items.sort_by(|a, b| {
        a.path
            .cmp(&b.path)
            .then(b.version.created_at.cmp(&a.version.created_at))
    });
    Ok(items)
}

fn local_snapshots(
    app_data: &Path,
    source: &Path,
    settings: &BTreeMap<String, String>,
) -> Result<BTreeMap<String, Vec<u8>>, String> {
    let _guard = SOURCE_ACCESS
        .try_write()
        .map_err(|_| "下载、计算或数据操作正在进行，请完成后再同步")?;
    let mut snapshots = bundles::snapshots(&local_files(app_data, source, settings)?)?;
    snapshots.extend(database::export(source)?);
    Ok(snapshots)
}

fn local_files(
    app_data: &Path,
    source: &Path,
    settings: &BTreeMap<String, String>,
) -> Result<BTreeMap<String, Vec<u8>>, String> {
    let mut snapshots = BTreeMap::new();
    for name in [
        "score_rule.toml",
        "chip_change_rule.toml",
        "ind.toml",
        "chart_indicators.toml",
    ] {
        let path = source.join(name);
        if path.exists() {
            snapshots.insert(
                format!("source/{name}"),
                fs::read(path).map_err(|e| e.to_string())?,
            );
        }
    }
    for name in [
        "strategy_backups",
        "chip_change_rule_backups",
        "strategy_snapshots",
        "cloud_sync_backups",
    ] {
        let root = source.join(name);
        if !root.exists() {
            continue;
        }
        let mut dirs = vec![root];
        while let Some(dir) = dirs.pop() {
            for entry in fs::read_dir(dir).map_err(|e| e.to_string())? {
                let entry = entry.map_err(|e| e.to_string())?;
                let kind = entry.file_type().map_err(|e| e.to_string())?;
                if kind.is_dir() {
                    dirs.push(entry.path());
                } else if kind.is_file()
                    && matches!(
                        entry.path().extension().and_then(|e| e.to_str()),
                        Some("toml" | "json")
                    )
                    && !entry.file_name().to_string_lossy().starts_with('.')
                {
                    let key = format!(
                        "source/{}",
                        entry
                            .path()
                            .strip_prefix(source)
                            .map_err(|e| e.to_string())?
                            .to_string_lossy()
                            .replace('\\', "/")
                    );
                    validate_path(&key)?;
                    snapshots.insert(key, fs::read(entry.path()).map_err(|e| e.to_string())?);
                }
            }
        }
    }
    let watch = app_data.join("watch_observe.json");
    if watch.exists() {
        snapshots.insert(
            "app/watch_observe.json".into(),
            fs::read(watch).map_err(|e| e.to_string())?,
        );
    }
    for (key, value) in settings {
        if allowed_setting(&key) {
            snapshots.insert(format!("settings/{key}"), value.as_bytes().to_vec());
        }
    }
    Ok(snapshots)
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UploadSizeItem {
    pub path: String,
    pub compressed_bytes: u64,
}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UploadEstimate {
    pub initial_upload_bytes: u64,
    pub unique_objects: usize,
    pub upload_objects: usize,
    pub skipped_duplicates: usize,
    pub configuration_bytes: u64,
    pub ranking_daily_bytes: u64,
    pub similarity_daily_bytes: u64,
    pub monthly_22_days_bytes: u64,
    pub monthly_31_days_bytes: u64,
    pub missing_similarity: bool,
    pub items: Vec<UploadSizeItem>,
}

// 只在本机使用与实际同步相同的压缩和查重流程；不消耗网盘流量。
pub fn estimate_upload(
    app_data: &Path,
    source: &Path,
    settings: &BTreeMap<String, String>,
) -> Result<UploadEstimate, String> {
    let mut snapshots = local_snapshots(app_data, source, settings)?;
    snapshots.retain(|path, _| SyncScope::Management.includes(path));
    let mut hashes = BTreeSet::new();
    let mut initial_upload_bytes = 0;
    let mut configuration_bytes = 0;
    let mut ranking = Vec::new();
    let mut similarity = Vec::new();
    let mut items = Vec::new();
    for (path, bytes) in &snapshots {
        let hash = digest(bytes);
        let size = encode(bytes)?.len() as u64;
        if path.starts_with("ranking/") {
            ranking.push(size);
        } else if path.starts_with("similarity/") {
            similarity.push(size);
        }
        if hashes.insert(hash.clone()) {
            initial_upload_bytes += size;
            if !path.starts_with("ranking/") && !path.starts_with("similarity/") {
                configuration_bytes += size;
            }
        }
        items.push(UploadSizeItem {
            path: path.clone(),
            compressed_bytes: size,
        });
    }
    let upload_objects = hashes.len();
    let ranking_daily_bytes = ranking.iter().sum::<u64>() / ranking.len().max(1) as u64;
    let similarity_daily_bytes = similarity.iter().sum::<u64>() / similarity.len().max(1) as u64;
    Ok(UploadEstimate {
        initial_upload_bytes,
        unique_objects: hashes.len(),
        upload_objects,
        skipped_duplicates: snapshots.len() - hashes.len(),
        configuration_bytes,
        ranking_daily_bytes,
        similarity_daily_bytes,
        monthly_22_days_bytes: configuration_bytes
            + (ranking_daily_bytes + similarity_daily_bytes) * 22,
        monthly_31_days_bytes: configuration_bytes
            + (ranking.iter().max().unwrap_or(&0) + similarity.iter().max().unwrap_or(&0)) * 31,
        missing_similarity: similarity.is_empty(),
        items,
    })
}

#[derive(Clone, Copy, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum SyncScope {
    #[default]
    All,
    Management,
    Watchlists,
}

impl SyncScope {
    fn includes(self, path: &str) -> bool {
        let watch = path == "bundles/watchlists.zip"
            || bundles::for_path(path) == Some("bundles/watchlists.zip");
        match self {
            Self::All => true,
            Self::Management => !watch,
            Self::Watchlists => watch,
        }
    }
}

pub fn sync(
    app_data: &Path,
    source: &Path,
    settings: BTreeMap<String, String>,
    download_only: bool,
    progress: impl Fn(&str),
) -> Result<SyncResult, String> {
    sync_scoped(
        app_data,
        source,
        settings,
        download_only,
        SyncScope::All,
        progress,
    )
}

pub fn sync_scoped(
    app_data: &Path,
    source: &Path,
    settings: BTreeMap<String, String>,
    download_only: bool,
    scope: SyncScope,
    progress: impl Fn(&str),
) -> Result<SyncResult, String> {
    let config = get_config(app_data)?;
    let local = local_root(app_data, source, &config)?;
    let state_path = local.join("state.json");
    let mut state: SyncState = if state_path.exists() {
        read_json(&state_path)?
    } else {
        SyncState::default()
    };
    progress(if matches!(scope, SyncScope::Watchlists) {
        "正在读取本地自选"
    } else {
        "正在读取本地策略、配置和按日榜单"
    });
    let mut snapshots = if matches!(scope, SyncScope::Watchlists) {
        let _guard = SOURCE_ACCESS
            .try_write()
            .map_err(|_| "数据操作正在进行，请完成后再同步")?;
        let files = watchlists::local_files(app_data, source, &settings)?;
        bundles::snapshots(&files)?
    } else {
        local_snapshots(app_data, source, &settings)?
    };
    snapshots.retain(|path, _| scope.includes(path));
    progress(if download_only {
        "正在读取云端目录"
    } else {
        "正在连接云端并取得同步锁"
    });
    let mut dav = dav::Dav::new(&config)?;
    if download_only {
        dav.read_manifest()?;
    } else {
        dav.acquire()?;
    }
    let original_manifest = dav.manifest.clone();
    let mut result = SyncResult::default();
    let mut retained_days = BTreeSet::new();
    for (kind, count) in [("ranking/", 5), ("similarity/", 1)] {
        let keys: BTreeSet<_> = snapshots
            .keys()
            .chain(dav.manifest.entries.keys())
            .filter(|p| p.starts_with(kind))
            .cloned()
            .collect();
        retained_days.extend(keys.into_iter().rev().take(count));
    }
    for (path, bytes) in snapshots.iter().filter(|_| !download_only) {
        progress(&format!("核对并上传：{path}"));
        if (path.starts_with("ranking/") || path.starts_with("similarity/"))
            && !database::is_snapshot(path)
            && !retained_days.contains(path)
        {
            result.skipped += 1;
            continue;
        }
        let hash = digest(bytes);
        let heads = dav.manifest.heads(path);
        if let Some(version) = heads.iter().find(|v| v.hash == hash) {
            state.base.insert(
                path.clone(),
                Base {
                    id: version.id.clone(),
                    hash,
                },
            );
            result.skipped += 1;
            continue;
        }
        if state.base.get(path).is_some_and(|base| base.hash == hash) {
            // 未改动的本地副本不能把已删除或已更新的云端版本重新上传。
            result.skipped += 1;
            continue;
        }
        let parents = state
            .base
            .get(path)
            .filter(|base| {
                dav.manifest
                    .entries
                    .get(path)
                    .is_some_and(|versions| versions.iter().any(|v| v.id == base.id))
            })
            .map(|b| vec![b.id.clone()])
            .unwrap_or_default();
        let compressed = encode(bytes)?;
        if !dav.manifest.hashes().contains(&hash) && !dav.manifest.packs.contains_key(&hash) {
            dav.upload(&hash, &compressed)?;
        }
        atomic_write(
            &local.join("objects").join(format!("{hash}.gz")),
            &compressed,
        )?;
        if path.starts_with("bundles/") || database::is_snapshot(path) {
            dav.manifest.format = 3;
        }
        let version = dav.manifest.add(
            path,
            &hash,
            parents,
            bytes.len() as u64,
            compressed.len() as u64,
        );
        state.base.insert(
            path.clone(),
            Base {
                id: version.id,
                hash,
            },
        );
        result.uploaded += 1;
    }
    if !download_only && !matches!(scope, SyncScope::Watchlists) {
        dav.manifest.prune();
    }
    // 先提交云端，再应用本地。中断后基线可以重建，已上传版本仍可恢复。
    if dav.manifest != original_manifest {
        progress("正在提交云端目录");
        dav.persist()?;
    }
    for path in dav
        .manifest
        .entries
        .keys()
        .filter(|path| scope.includes(path))
        .cloned()
        .collect::<Vec<_>>()
    {
        if replacement_path(&path).is_some_and(|group| !dav.manifest.heads(group).is_empty()) {
            continue;
        }
        let heads = dav.manifest.heads(&path);
        if heads.len() > 1 {
            result.conflicts.push(path);
            continue;
        }
        let Some(version) = heads.first() else {
            continue;
        };
        if snapshots
            .get(&path)
            .is_some_and(|bytes| digest(bytes) == version.hash)
        {
            continue;
        }
        progress(&format!("下载并校验：{path}"));
        let bytes = dav.download(&version.hash, &local.join("objects"))?;
        let _guard = SOURCE_ACCESS
            .try_write()
            .map_err(|_| "数据操作正在进行，请完成后再次同步以接收下载")?;
        // 网络请求期间本地可能被编辑；数据库快照和配置分组重新导出核对。
        let current = current_bytes(app_data, source, &path, &settings)?;
        if !path.starts_with("settings/")
            && current.as_ref().map(|v| digest(v)) != snapshots.get(&path).map(|v| digest(v))
        {
            result.conflicts.push(path.clone());
            result
                .warnings
                .push(format!("同步期间本地已变化，保留当前内容：{path}"));
            continue;
        }
        apply(app_data, source, &path, &bytes, &mut result)?;
        // 界面设置由前端实际落盘后，在下一轮相同内容校验时确认基线。
        if !path.starts_with("settings/")
            && !matches!(
                path.as_str(),
                "bundles/browser_settings.zip" | "bundles/watchlists.zip"
            )
        {
            state.base.insert(
                path,
                Base {
                    id: version.id.clone(),
                    hash: version.hash.clone(),
                },
            );
        }
        result.downloaded += 1;
    }
    atomic_write(
        &state_path,
        &serde_json::to_vec(&state).map_err(|e| e.to_string())?,
    )?;
    if !download_only {
        if !matches!(scope, SyncScope::Watchlists) {
            progress("正在清理过期榜单");
            dav.garbage_collect()?;
            if !dav.manifest.garbage.is_empty() {
                result
                    .warnings
                    .push("部分过期榜单清理失败，下次同步会重试".into());
            }
        }
        dav.release()?;
    }
    result.uploaded_bytes = dav.uploaded_bytes;
    result.downloaded_bytes = dav.downloaded_bytes;
    result.transferred_bytes = dav.uploaded_bytes + dav.downloaded_bytes;
    Ok(result)
}

pub fn restore(
    app_data: &Path,
    source: &Path,
    path: &str,
    id: &str,
    settings: BTreeMap<String, String>,
    progress: impl Fn(&str),
) -> Result<SyncResult, String> {
    validate_path(path)?;
    let config = get_config(app_data)?;
    let local = local_root(app_data, source, &config)?;
    let mut dav = dav::Dav::new(&config)?;
    dav.acquire()?;
    let version = dav
        .manifest
        .entries
        .get(path)
        .and_then(|v| {
            v.iter()
                .find(|v| v.id == id && !dav.manifest.deleted.contains(id))
        })
        .cloned()
        .ok_or("云端版本不存在，请刷新列表")?;
    progress("正在下载选定版本并校验");
    let bytes = dav.download(&version.hash, &local.join("objects"))?;
    let mut result = SyncResult::default();
    let _guard = SOURCE_ACCESS
        .try_write()
        .map_err(|_| "下载、计算或数据操作正在进行，请完成后再恢复")?;
    // 尚未同步的本地改动也先保留到云端，恢复动作才可以合并所有冲突分支。
    let current = if let Some(key) = path.strip_prefix("settings/") {
        settings.get(key).map(|v| v.as_bytes().to_vec())
    } else {
        current_bytes(app_data, source, path, &settings)?
    };
    if let Some(current) = current {
        let hash = digest(&current);
        if hash != version.hash && !dav.manifest.heads(path).iter().any(|v| v.hash == hash) {
            let compressed = encode(&current)?;
            if !dav.manifest.hashes().contains(&hash) && !dav.manifest.packs.contains_key(&hash) {
                dav.upload(&hash, &compressed)?;
            }
            dav.manifest.add(
                path,
                &hash,
                vec![],
                current.len() as u64,
                compressed.len() as u64,
            );
        }
    }
    let parents = dav.manifest.heads(path).into_iter().map(|v| v.id).collect();
    let resolved = dav.manifest.add(
        path,
        &version.hash,
        parents,
        version.size,
        version.compressed_size,
    );
    dav.persist()?;
    apply(app_data, source, path, &bytes, &mut result)?;
    let state_path = local.join("state.json");
    let mut state: SyncState = if state_path.exists() {
        read_json(&state_path)?
    } else {
        SyncState::default()
    };
    if !path.starts_with("settings/")
        && !matches!(
            path,
            "bundles/browser_settings.zip" | "bundles/watchlists.zip"
        )
    {
        state.base.insert(
            path.into(),
            Base {
                id: resolved.id,
                hash: resolved.hash,
            },
        );
    }
    atomic_write(
        &state_path,
        &serde_json::to_vec(&state).map_err(|e| e.to_string())?,
    )?;
    result.downloaded = 1;
    dav.release()?;
    result.uploaded_bytes = dav.uploaded_bytes;
    result.downloaded_bytes = dav.downloaded_bytes;
    result.transferred_bytes = dav.uploaded_bytes + dav.downloaded_bytes;
    Ok(result)
}

pub fn delete_cloud(app_data: &Path, path: &str, id: &str) -> Result<(), String> {
    validate_path(path)?;
    let mut dav = dav::Dav::new(&get_config(app_data)?)?;
    dav.acquire()?;
    let version = dav
        .manifest
        .entries
        .get(path)
        .and_then(|v| v.iter().find(|v| v.id == id))
        .cloned()
        .ok_or("云端版本不存在")?;
    dav.manifest.deleted.insert(id.into());
    dav.manifest.garbage.insert(version.hash);
    dav.persist()?;
    dav.garbage_collect()?;
    dav.release()
}

fn current_bytes(
    app_data: &Path,
    source: &Path,
    path: &str,
    settings: &BTreeMap<String, String>,
) -> Result<Option<Vec<u8>>, String> {
    if path.starts_with("bundles/") {
        return bundles::snapshots(&local_files(app_data, source, settings)?)
            .map(|mut groups| groups.remove(path));
    }
    if path.starts_with("settings/") {
        return Ok(None);
    }
    if path.starts_with("ranking/") || path.starts_with("similarity/") {
        return database::export_day(source, path);
    }
    let target = target_path(app_data, source, path)?;
    if target.exists() {
        fs::read(target).map(Some).map_err(|e| e.to_string())
    } else {
        Ok(None)
    }
}

fn apply(
    app_data: &Path,
    source: &Path,
    path: &str,
    bytes: &[u8],
    result: &mut SyncResult,
) -> Result<(), String> {
    validate_path(path)?;
    if path.starts_with("bundles/") {
        let files = bundles::unpack(path, bytes)?;
        // 先完整验证，再应用成员。策略文件替换前沿用逐文件备份。
        for (member, bytes) in files {
            apply(app_data, source, &member, &bytes, result)?;
        }
        return Ok(());
    }
    if let Some(key) = path.strip_prefix("settings/") {
        result.settings.insert(
            key.into(),
            String::from_utf8(bytes.to_vec()).map_err(|_| "云端设置不是有效文本")?,
        );
        return Ok(());
    }
    if path.starts_with("ranking/") || path.starts_with("similarity/") {
        return database::import(source, path, bytes);
    }
    if path.ends_with(".toml") {
        toml::from_str::<toml::Value>(
            std::str::from_utf8(bytes).map_err(|_| "云端配置不是 UTF-8")?,
        )
        .map_err(|e| format!("云端 TOML 无效，未替换：{e}"))?;
    }
    if path.ends_with(".json") {
        serde_json::from_slice::<serde_json::Value>(bytes)
            .map_err(|e| format!("云端 JSON 无效，未替换：{e}"))?;
    }
    let target = target_path(app_data, source, path)?;
    if target.exists() {
        let old = fs::read(&target).map_err(|e| e.to_string())?;
        if old == bytes {
            return Ok(());
        }
        let stamp = format!(
            "cloud-{}-{}",
            Utc::now().format("%Y%m%d%H%M%S%f"),
            &digest(&old)[..12]
        );
        match path {
            "source/score_rule.toml" => {
                let backup = source.join("strategy_backups").join(&stamp);
                atomic_write(&backup.join("score_rule.toml"), &old)?;
                atomic_write(&backup.join("meta.json"), &serde_json::to_vec(&serde_json::json!({"version":1,"createdAt":Utc::now().to_rfc3339(),"sourceKind":"cloud_sync","sourceFileName":"score_rule.toml","description":"云端同步或恢复前自动备份"})).map_err(|e| e.to_string())?)?;
            }
            "source/chip_change_rule.toml" => {
                super::cyq_chen::backup_cyq_chen_strategy_file_with_meta(
                    &source.to_string_lossy(),
                    "cloud_sync",
                    "云端同步或恢复前自动备份",
                )?;
            }
            _ => {
                atomic_write(
                    &source.join("cloud_sync_backups").join(&stamp).join(path),
                    &old,
                )?;
            }
        }
    }
    atomic_write(&target, bytes)
}

fn target_path(app_data: &Path, source: &Path, path: &str) -> Result<PathBuf, String> {
    validate_path(path)?;
    let (root, target) = if let Some(relative) = path.strip_prefix("source/") {
        (source, source.join(relative))
    } else {
        (app_data, app_data.join("watch_observe.json"))
    };
    // 只检查本地同步根目录及其内部；上级目录可能是 Android 的系统路径别名。
    let mut current = target.as_path();
    loop {
        match fs::symlink_metadata(current) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(format!(
                    "本地同步目标包含符号链接：{}；已停止恢复",
                    current.display()
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(format!(
                    "检查本地同步路径失败 {}：{error}",
                    current.display()
                ));
            }
        }
        if current == root {
            break;
        }
        current = current.parent().ok_or("本地同步目标不在同步根目录内")?;
    }
    Ok(target)
}

fn local_root(app_data: &Path, source: &Path, config: &WebDavConfig) -> Result<PathBuf, String> {
    let key = digest(
        format!(
            "{}\n{}\n{}",
            config.root_url()?,
            config.username,
            source.display()
        )
        .as_bytes(),
    );
    let path = app_data.join("cloud-sync").join(key);
    fs::create_dir_all(&path).map_err(|e| e.to_string())?;
    Ok(path)
}

pub fn setting_keys() -> &'static [&'static str] {
    &[
        "lh_concept_exclusions",
        "lh_exclude_st_board",
        "lh_default_board_filter_v1",
        "lh_realtime_quote_provider_v1",
        "lh_backtest_highlight_settings_v1",
        "lh_scene_layer_backtest_common_params",
        "lh_expression_stock_pick_templates_v1",
        "expression-stock-pick-filter-state-v2",
        "concept-stock-pick-filter-state-v3",
        "lh_market_analysis_params_v1",
        "lh_cyq_chen_test_draft_v1",
        "lh_strategy_paper_validation_templates_v1",
        "lh_intraday_monitor_realtime_templates_v1",
        "lh_intraday_monitor_watchlist_v1",
        "lh_detail_cyq_model_v1",
        "lh_details_nav_long_press_interval_seconds_v1",
        "lh_details_similarity_nav_mode_v1",
        "am_refresh_seconds",
        "am_volume_body_min",
        "am_volume_body_absolute",
        "am_volume_above_average",
        "am_volume_rank_max",
        "am_volume_ratio_notifications",
        "am_speed_period",
        "am_speed_threshold",
        "am_speed_min_ticks",
        "am_volume_ratio_threshold",
        "am_rank_highlight_threshold",
        "am_scene_stage_threshold",
        "am_top_limit",
        "am_board_filter",
        "am_other_sort_expression",
        "am_other_sort_direction",
        "am_compact_mode",
        "am_side_list_visibility",
        "am_template_display_mode",
        "lh_chart_main_width_ratio_v1",
        "lh_chart_indicator_width_ratio_v1",
        "lh_chart_main_height_mode_v1",
        "lh_chart_main_percent_pixels_v1",
        "lh_chart_main_percent_min_height_v1",
        "lh_chart_main_percent_max_height_v1",
        "lh_chart_default_visible_bars_v1",
    ]
}
pub fn allowed_setting(key: &str) -> bool {
    setting_keys().contains(&key)
}

fn validate_path(path: &str) -> Result<(), String> {
    if path.contains('\\')
        || Path::new(path)
            .components()
            .any(|c| !matches!(c, Component::Normal(_)))
    {
        return Err("云端路径无效".into());
    }
    let valid = if path.starts_with("bundles/") {
        matches!(
            path,
            "bundles/strategies.zip"
                | "bundles/watchlists.zip"
                | "bundles/indicator_config.zip"
                | "bundles/browser_settings.zip"
        )
    } else if let Some(key) = path.strip_prefix("settings/") {
        allowed_setting(key) || key == "lh_intraday_monitor_watchlist_enabled_v1"
    } else if let Some(relative) = path.strip_prefix("source/") {
        matches!(
            relative,
            "score_rule.toml" | "chip_change_rule.toml" | "ind.toml" | "chart_indicators.toml"
        ) || [
            "strategy_backups/",
            "chip_change_rule_backups/",
            "strategy_snapshots/",
            "cloud_sync_backups/",
        ]
        .iter()
        .any(|p| relative.starts_with(p))
            && matches!(
                Path::new(relative).extension().and_then(|v| v.to_str()),
                Some("toml" | "json")
            )
    } else if path.starts_with("ranking/") || path.starts_with("similarity/") {
        database::is_snapshot(path) || database::date_from_path(path).is_ok()
    } else {
        path == "app/watch_observe.json"
    };
    if valid {
        Ok(())
    } else {
        Err(format!("不允许同步此路径：{path}"))
    }
}
fn valid_hash(hash: &str) -> bool {
    hash.len() == 64
        && hash
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}
fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T, String> {
    serde_json::from_slice(&fs::read(path).map_err(|e| e.to_string())?)
        .map_err(|e| format!("读取 {} 失败：{e}", path.display()))
}
fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), String> {
    let parent = path.parent().ok_or("文件没有父目录")?;
    fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    let mut temp = tempfile::NamedTempFile::new_in(parent).map_err(|e| e.to_string())?;
    temp.write_all(bytes)
        .and_then(|_| temp.as_file().sync_all())
        .map_err(|e| e.to_string())?;
    temp.persist(path).map_err(|e| e.to_string())?;
    Ok(())
}
fn encode(bytes: &[u8]) -> Result<Vec<u8>, String> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::new(6));
    encoder.write_all(bytes).map_err(|e| e.to_string())?;
    encoder.finish().map_err(|e| e.to_string())
}
fn decode(bytes: &[u8], limit: u64) -> Result<Vec<u8>, String> {
    let mut raw = Vec::new();
    GzDecoder::new(bytes)
        .take(limit + 1)
        .read_to_end(&mut raw)
        .map_err(|e| format!("压缩文件无效：{e}"))?;
    if raw.len() as u64 > limit {
        return Err("同步文件超过安全大小限制".into());
    }
    Ok(raw)
}

#[cfg(test)]
mod tests;
