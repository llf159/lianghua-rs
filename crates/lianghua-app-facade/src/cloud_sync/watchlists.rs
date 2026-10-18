use super::{
    Base, SOURCE_ACCESS, SyncResult, SyncState, atomic_write, bundles, dav, digest, encode,
    get_config, local_root, read_json,
};
use lianghua_app_market::watch_observe::WatchObserveStoredRow;
use lianghua_app_shared::normalize_ts_code;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
};

#[derive(Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TransferMode {
    Upload,
    Download,
    Merge,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TransferResult {
    #[serde(flatten)]
    pub result: SyncResult,
    pub last_modified: Option<String>,
    pub watch_rows: Option<Vec<WatchObserveStoredRow>>,
}

pub(super) fn local_files(
    app_data: &Path,
    source: &Path,
    settings: &BTreeMap<String, String>,
) -> Result<BTreeMap<String, Vec<u8>>, String> {
    let mut files = BTreeMap::new();
    let target = super::target_path(app_data, source, "app/watch_observe.json")?;
    if target.exists() {
        files.insert(
            "app/watch_observe.json".into(),
            fs::read(target).map_err(|e| e.to_string())?,
        );
    }
    for key in ["lh_intraday_monitor_watchlist_v1", "am_board_filter"] {
        if let Some(value) = settings.get(key) {
            files.insert(format!("settings/{key}"), value.as_bytes().to_vec());
        }
    }
    Ok(files)
}

// 后面的来源优先，空字段沿用前面的信息；名单取并集，不推断没有墓碑的删除。
fn merge_files(sources: &[BTreeMap<String, Vec<u8>>]) -> Result<BTreeMap<String, Vec<u8>>, String> {
    let mut rows = BTreeMap::<String, WatchObserveStoredRow>::new();
    let mut codes = BTreeSet::new();
    let mut board_filter = "全部".as_bytes().to_vec();
    for files in sources {
        if let Some(bytes) = files.get("app/watch_observe.json") {
            let incoming: Vec<WatchObserveStoredRow> =
                serde_json::from_slice(bytes).map_err(|e| format!("自选数据格式无效：{e}"))?;
            for mut row in incoming {
                row.ts_code = normalize_ts_code(&row.ts_code).ok_or("自选股票代码无效")?;
                if let Some(old) = rows.get(&row.ts_code) {
                    if row.name.trim().is_empty() {
                        row.name = old.name.clone();
                    }
                    if row.watch_date.trim().is_empty() {
                        row.watch_date = old.watch_date.clone();
                    }
                    if row.tag.trim().is_empty() {
                        row.tag = old.tag.clone();
                    }
                    if row.concept.trim().is_empty() {
                        row.concept = old.concept.clone();
                    }
                    if row
                        .marked_date
                        .as_ref()
                        .is_none_or(|value| value.trim().is_empty())
                    {
                        row.marked_date = old.marked_date.clone();
                    }
                }
                rows.insert(row.ts_code.clone(), row);
            }
        }
        if let Some(bytes) = files.get("settings/lh_intraday_monitor_watchlist_v1") {
            let incoming: Vec<String> =
                serde_json::from_slice(bytes).map_err(|e| format!("监控自选格式无效：{e}"))?;
            for code in incoming {
                codes.insert(normalize_ts_code(&code).ok_or("监控自选股票代码无效")?);
            }
        }
        if let Some(bytes) = files.get("settings/am_board_filter") {
            let value = std::str::from_utf8(bytes).map_err(|_| "监控板块范围格式无效")?;
            if !matches!(
                value,
                "全部" | "主板" | "科创板" | "创业板" | "北交所" | "ST" | "自选"
            ) {
                return Err("监控板块范围格式无效".into());
            }
            board_filter = bytes.clone();
        } else if let Some(bytes) = files.get("settings/lh_intraday_monitor_watchlist_enabled_v1") {
            if bytes != b"0" && bytes != b"1" {
                return Err("监控自选开关格式无效".into());
            }
            board_filter = if bytes == b"1" { "自选" } else { "全部" }
                .as_bytes()
                .to_vec();
        }
    }
    Ok(BTreeMap::from([
        (
            "app/watch_observe.json".into(),
            serde_json::to_vec(&rows.into_values().collect::<Vec<_>>())
                .map_err(|e| e.to_string())?,
        ),
        (
            "settings/lh_intraday_monitor_watchlist_v1".into(),
            serde_json::to_vec(&codes).map_err(|e| e.to_string())?,
        ),
        ("settings/am_board_filter".into(), board_filter),
    ]))
}

pub fn transfer(
    app_data: &Path,
    source: &Path,
    settings: BTreeMap<String, String>,
    watch_rows: Option<Vec<WatchObserveStoredRow>>,
    mode: TransferMode,
    progress: impl Fn(&str),
) -> Result<TransferResult, String> {
    transfer_bundle(
        app_data, source, settings, watch_rows, mode, false, progress,
    )
}

pub(super) fn transfer_bundle(
    app_data: &Path,
    source: &Path,
    settings: BTreeMap<String, String>,
    watch_rows: Option<Vec<WatchObserveStoredRow>>,
    mode: TransferMode,
    strategies: bool,
    progress: impl Fn(&str),
) -> Result<TransferResult, String> {
    let bundle = if strategies {
        "bundles/strategies.zip"
    } else {
        "bundles/watchlists.zip"
    };
    let label = if strategies { "策略资产" } else { "自选" };
    // 明确替换或合并沿用手动恢复的门禁，避免网络等待期间编辑被覆盖。
    let _guard = SOURCE_ACCESS
        .try_write()
        .map_err(|_| "数据操作正在进行，请完成后再同步")?;
    let config = get_config(app_data)?;
    let local = local_root(app_data, source, &config)?;
    let state_path = local.join("state.json");
    let mut state: SyncState = if state_path.exists() {
        read_json(&state_path)?
    } else {
        SyncState::default()
    };
    let mut local_files = if strategies {
        let mut files = super::local_files(app_data, source, &BTreeMap::new())?;
        files.retain(|path, _| bundles::for_path(path) == Some(bundle));
        files
    } else {
        local_files(app_data, source, &settings)?
    };
    if let Some(rows) = watch_rows {
        local_files.insert(
            "app/watch_observe.json".into(),
            serde_json::to_vec(&rows).map_err(|e| e.to_string())?,
        );
    }
    let local_backup = if mode != TransferMode::Upload {
        bundles::snapshots(&local_files)?.remove(bundle)
    } else {
        None
    };
    let mut dav = dav::Dav::new(&config)?;
    progress(&format!("正在读取云端{label}…"));
    if mode == TransferMode::Download {
        dav.read_manifest()?;
    } else {
        dav.acquire()?;
    }
    let bundle_heads = dav.manifest.heads(bundle);
    let mut versions = Vec::new();
    if bundle_heads.is_empty() {
        let legacy_paths = if strategies {
            dav.manifest
                .entries
                .keys()
                .filter(|path| bundles::for_path(path) == Some(bundle))
                .cloned()
                .collect::<Vec<_>>()
        } else {
            [
                "app/watch_observe.json",
                "settings/lh_intraday_monitor_watchlist_v1",
                "settings/am_board_filter",
                "settings/lh_intraday_monitor_watchlist_enabled_v1",
            ]
            .into_iter()
            .map(String::from)
            .collect()
        };
        for path in legacy_paths {
            let heads = dav.manifest.heads(&path);
            if mode == TransferMode::Download && heads.len() > 1 {
                return Err(format!("云端{label}存在多个版本，请选择合并"));
            }
            versions.extend(heads.into_iter().map(|version| (path.clone(), version)));
        }
    } else {
        if mode == TransferMode::Download && bundle_heads.len() > 1 {
            return Err(format!("云端{label}存在多个版本，请选择合并"));
        }
        versions.extend(
            bundle_heads
                .iter()
                .cloned()
                .map(|version| (bundle.to_string(), version)),
        );
    }
    versions.sort_by(|a, b| {
        a.1.created_at
            .cmp(&b.1.created_at)
            .then(a.1.id.cmp(&b.1.id))
    });
    let mut last_modified = versions
        .last()
        .map(|(_, version)| version.created_at.clone());
    let mut sources = Vec::new();
    let mut downloaded_bundle = None;
    if mode != TransferMode::Upload {
        if mode == TransferMode::Download && versions.is_empty() {
            return Err(format!("云端暂无{label}数据"));
        }
        for (path, version) in &versions {
            progress(&format!("正在下载云端{label}…"));
            let bytes = dav.download(&version.hash, &local.join("objects"))?;
            sources.push(if path == bundle {
                let files = bundles::unpack(path, &bytes)?;
                if mode == TransferMode::Download {
                    downloaded_bundle = Some(bytes);
                }
                files
            } else {
                BTreeMap::from([(path.to_string(), bytes)])
            });
        }
    }
    if mode != TransferMode::Download {
        sources.push(local_files);
    }
    let files = if strategies {
        super::strategies::merge_files(&sources)?
    } else {
        merge_files(&sources)?
    };
    let bytes = if let Some(bytes) = downloaded_bundle {
        bytes
    } else {
        bundles::snapshots(&files)?
            .remove(bundle)
            .ok_or_else(|| format!("本地暂无{label}数据"))?
    };
    if strategies {
        bundles::unpack(bundle, &bytes)?;
    }
    let hash = digest(&bytes);
    let mut result = SyncResult::default();
    if mode != TransferMode::Download {
        progress(&format!("正在上传{label}…"));
        let version = if bundle_heads.len() == 1 && bundle_heads[0].hash == hash {
            result.skipped = 1;
            bundle_heads[0].clone()
        } else {
            let compressed = encode(&bytes)?;
            if !dav.manifest.hashes().contains(&hash) && !dav.manifest.packs.contains_key(&hash) {
                dav.upload(&hash, &compressed)?;
            }
            atomic_write(
                &local.join("objects").join(format!("{hash}.gz")),
                &compressed,
            )?;
            dav.manifest.format = 3;
            let version = dav.manifest.add(
                bundle,
                &hash,
                bundle_heads.into_iter().map(|v| v.id).collect(),
                bytes.len() as u64,
                compressed.len() as u64,
            );
            dav.persist()?;
            result.uploaded = 1;
            version
        };
        last_modified = Some(version.created_at.clone());
        state.base.insert(
            bundle.into(),
            Base {
                id: version.id,
                hash,
            },
        );
    } else if let Some(version) = bundle_heads.first() {
        state.base.insert(
            bundle.into(),
            Base {
                id: version.id.clone(),
                hash: version.hash.clone(),
            },
        );
    }
    if mode != TransferMode::Upload {
        // 替换前保存完整本地包；自选使用页面传入的实际条目。
        if let Some(backup) = local_backup {
            atomic_write(
                &source
                    .join("cloud_sync_backups")
                    .join(format!(
                        "{}-{}",
                        if strategies {
                            "strategies"
                        } else {
                            "watchlists"
                        },
                        chrono::Utc::now().format("%Y%m%d%H%M%S%f")
                    ))
                    .join(if strategies {
                        "strategies.zip"
                    } else {
                        "watchlists.zip"
                    }),
                &backup,
            )?;
        }
        progress(&format!("正在更新本地{label}…"));
        super::apply(app_data, source, bundle, &bytes, &mut result)?;
        result.downloaded = 1;
    }
    atomic_write(
        &state_path,
        &serde_json::to_vec(&state).map_err(|e| e.to_string())?,
    )?;
    if mode != TransferMode::Download {
        dav.release()?;
    }
    result.uploaded_bytes = dav.uploaded_bytes;
    result.downloaded_bytes = dav.downloaded_bytes;
    result.transferred_bytes = result.uploaded_bytes + result.downloaded_bytes;
    let watch_rows = if !strategies && mode != TransferMode::Upload {
        Some(serde_json::from_slice(&files["app/watch_observe.json"]).map_err(|e| e.to_string())?)
    } else {
        None
    };
    Ok(TransferResult {
        result,
        last_modified,
        watch_rows,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn union_keeps_both_lists_and_local_metadata() {
        let cloud = BTreeMap::from([
            ("app/watch_observe.json".into(), br#"[{"tsCode":"000001.SZ","name":"cloud","watchDate":"2026-10-13","tag":"cloud-tag","concept":"bank"},{"tsCode":"600000.SH","name":"remote-only","tag":"","concept":""}]"#.to_vec()),
            ("settings/lh_intraday_monitor_watchlist_v1".into(), br#"["000001.SZ","600000.SH"]"#.to_vec()),
            ("settings/am_board_filter".into(), "自选".as_bytes().to_vec()),
        ]);
        let local = BTreeMap::from([
            ("app/watch_observe.json".into(), br#"[{"tsCode":"000001.sz","name":"local","tag":"local-tag","concept":""},{"tsCode":"300001.SZ","name":"local-only","tag":"","concept":""}]"#.to_vec()),
            ("settings/lh_intraday_monitor_watchlist_v1".into(), br#"["000001.sz","300001.SZ"]"#.to_vec()),
            ("settings/am_board_filter".into(), "主板".as_bytes().to_vec()),
        ]);
        let merged = merge_files(&[cloud, local]).unwrap();
        let rows: Vec<WatchObserveStoredRow> =
            serde_json::from_slice(&merged["app/watch_observe.json"]).unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].name, "local");
        assert_eq!(rows[0].tag, "local-tag");
        assert_eq!(rows[0].watch_date, "2026-10-13");
        assert_eq!(rows[0].concept, "bank");
        assert_eq!(
            merged["settings/lh_intraday_monitor_watchlist_v1"],
            br#"["000001.SZ","300001.SZ","600000.SH"]"#
        );
        assert_eq!(merged["settings/am_board_filter"], "主板".as_bytes());
        assert!(
            merge_files(&[BTreeMap::from([(
                "app/watch_observe.json".into(),
                br#"{"symbols":[]}"#.to_vec()
            )])])
            .is_err()
        );
    }
}
