use super::*;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PreviewFile {
    path: String,
    hash: String,
    text: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PreviewSide {
    id: String,
    created_at: Option<String>,
    files: Vec<PreviewFile>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CloudPreview {
    path: String,
    local_hash: Option<String>,
    heads: Vec<String>,
    base_available: bool,
    sides: Vec<PreviewSide>,
}

// 预览只读云端；基线只能使用已校验的本机缓存，缺失时不猜测修改来源。
pub fn preview(
    app_data: &Path,
    source: &Path,
    path: &str,
    version_id: Option<&str>,
    settings: &BTreeMap<String, String>,
) -> Result<CloudPreview, String> {
    validate_path(path)?;
    if path.starts_with("ranking/") || path.starts_with("similarity/") {
        return Err("数据库快照不提供文本对比".into());
    }
    let config = get_config(app_data)?;
    let local = local_root(app_data, source, &config)?;
    let state_path = local.join("state.json");
    let state: SyncState = if state_path.exists() {
        read_json(&state_path)?
    } else {
        SyncState::default()
    };
    let bytes = {
        let _guard = SOURCE_ACCESS
            .try_write()
            .map_err(|_| "数据操作正在进行，请稍后查看差异")?;
        if let Some(key) = path.strip_prefix("settings/") {
            settings.get(key).map(|value| value.as_bytes().to_vec())
        } else {
            current_bytes(app_data, source, path, settings)?
        }
    };
    let local_hash = bytes.as_ref().map(|bytes| digest(bytes));
    let mut payloads = vec![("local".to_string(), None, bytes)];
    let mut base_available = false;
    if let Some(base) = state.base.get(path) {
        if let Ok(compressed) = fs::read(local.join("objects").join(format!("{}.gz", base.hash))) {
            if let Ok(raw) = decode(&compressed, 512 * 1024 * 1024) {
                if digest(&raw) == base.hash {
                    base_available = true;
                    payloads.push(("base".to_string(), None, Some(raw)));
                }
            }
        }
    }
    let mut dav = dav::Dav::new(&config)?;
    dav.read_manifest()?;
    let versions = dav.manifest.heads(path);
    let heads = versions.iter().map(|v| v.id.clone()).collect();
    let mut selected = versions;
    if let Some(id) = version_id {
        if !selected.iter().any(|v| v.id == id) {
            let version = dav
                .manifest
                .entries
                .get(path)
                .and_then(|versions| {
                    versions
                        .iter()
                        .find(|v| v.id == id && !dav.manifest.deleted.contains(id))
                })
                .cloned()
                .ok_or("所选版本已变化，请刷新差异")?;
            selected.push(version);
        }
    }
    for version in selected {
        let bytes = dav.download(&version.hash, &local.join("objects"))?;
        payloads.push((version.id, Some(version.created_at), Some(bytes)));
    }
    let mut sides = Vec::new();
    for (id, created_at, bytes) in payloads {
        let files = match bytes {
            Some(bytes) if path.starts_with("bundles/") => {
                bundles::read(path, &bytes, id != "local")?
            }
            Some(bytes) => BTreeMap::from([(path.to_string(), bytes)]),
            None => BTreeMap::new(),
        };
        let mut preview_files = Vec::new();
        for (path, bytes) in files {
            preview_files.push(PreviewFile {
                path,
                hash: digest(&bytes),
                text: String::from_utf8(bytes).map_err(|_| "对比文件不是有效 UTF-8 文本")?,
            });
        }
        sides.push(PreviewSide {
            id,
            created_at,
            files: preview_files,
        });
    }
    Ok(CloudPreview {
        path: path.into(),
        local_hash,
        heads,
        base_available,
        sides,
    })
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StrategyResolution {
    pub local_hash: Option<String>,
    pub heads: Vec<String>,
    // 仅为两份正在使用的策略选择完整文件来源；历史资产自动保留所有版本。
    pub choices: BTreeMap<String, String>,
}

pub fn resolve_strategies(
    app_data: &Path,
    source: &Path,
    resolution: StrategyResolution,
    progress: impl Fn(&str),
) -> Result<SyncResult, String> {
    let _guard = SOURCE_ACCESS
        .try_write()
        .map_err(|_| "数据操作正在进行，请完成后再处理策略冲突")?;
    let config = get_config(app_data)?;
    let local = local_root(app_data, source, &config)?;
    let bundle = "bundles/strategies.zip";
    let current = current_bytes(app_data, source, bundle, &BTreeMap::new())?;
    if current.as_ref().map(|bytes| digest(bytes)) != resolution.local_hash {
        return Err("本地策略在预览后已变化，请刷新差异后重新选择".into());
    }
    let mut dav = dav::Dav::new(&config)?;
    dav.acquire()?;
    let mut heads = dav.manifest.heads(bundle);
    if heads.iter().map(|v| v.id.clone()).collect::<BTreeSet<_>>()
        != resolution.heads.iter().cloned().collect::<BTreeSet<_>>()
    {
        return Err("云端策略在预览后已变化，请刷新差异后重新选择".into());
    }
    if heads.is_empty() {
        return Err("云端暂无策略包，请先上传；旧版单文件数据请先合并为策略包".into());
    }
    heads.sort_by(|a, b| a.created_at.cmp(&b.created_at).then(a.id.cmp(&b.id)));
    let mut sources = Vec::new();
    let mut by_id = BTreeMap::new();
    for version in &heads {
        progress("正在校验并保留云端策略分支");
        let bytes = dav.download(&version.hash, &local.join("objects"))?;
        let files = bundles::unpack(bundle, &bytes)?;
        by_id.insert(version.id.clone(), files.clone());
        sources.push(files);
    }
    let local_files = current
        .as_ref()
        .map(|bytes| bundles::read(bundle, bytes, false))
        .transpose()?
        .unwrap_or_default();
    by_id.insert("local".to_string(), local_files.clone());
    sources.push(local_files);
    let mut chosen = BTreeMap::new();
    for path in ["source/score_rule.toml", "source/chip_change_rule.toml"] {
        if !sources.iter().any(|files| files.contains_key(path)) {
            continue;
        }
        let origin = resolution
            .choices
            .get(path)
            .ok_or_else(|| format!("请明确选择当前策略的来源：{path}"))?;
        let bytes = by_id
            .get(origin)
            .and_then(|files| files.get(path))
            .ok_or("所选来源缺少策略文件，请刷新差异")?;
        chosen.insert(path.to_string(), bytes.clone());
    }
    if resolution.choices.keys().any(|path| {
        !matches!(
            path.as_str(),
            "source/score_rule.toml" | "source/chip_change_rule.toml"
        )
    }) {
        return Err("只能选择打分和筹码当前策略的来源".into());
    }
    let merged = strategies::merge_files(&sources)?;
    let files = strategies::merge_files(&[merged, chosen])?;
    let bytes = bundles::snapshots(&files)?
        .remove(bundle)
        .ok_or("没有可合并的策略")?;
    bundles::unpack(bundle, &bytes)?;
    let hash = digest(&bytes);
    let compressed = encode(&bytes)?;
    if !dav.manifest.hashes().contains(&hash) && !dav.manifest.packs.contains_key(&hash) {
        dav.upload(&hash, &compressed)?;
    }
    atomic_write(
        &local.join("objects").join(format!("{hash}.gz")),
        &compressed,
    )?;
    let version = dav.manifest.add(
        bundle,
        &hash,
        heads.into_iter().map(|v| v.id).collect(),
        bytes.len() as u64,
        compressed.len() as u64,
    );
    dav.manifest.format = 3;
    // 先提交保留所有分支的完整结果，再更新本地；失败后仍可从云端恢复。
    dav.persist()?;
    let mut result = SyncResult::default();
    let state_path = local.join("state.json");
    let mut state: SyncState = if state_path.exists() {
        read_json(&state_path)?
    } else {
        SyncState::default()
    };
    state.receiving.insert(bundle.into());
    atomic_write(
        &state_path,
        &serde_json::to_vec(&state).map_err(|e| e.to_string())?,
    )?;
    apply(app_data, source, bundle, &bytes, &mut result)?;
    record_applied(
        &mut state,
        app_data,
        source,
        bundle,
        &version,
        &BTreeMap::new(),
        &BTreeMap::new(),
    )?;
    state.receiving.remove(bundle);
    atomic_write(
        &state_path,
        &serde_json::to_vec(&state).map_err(|e| e.to_string())?,
    )?;
    dav.release()?;
    result.uploaded = 1;
    result.downloaded = 1;
    result.uploaded_bytes = dav.uploaded_bytes;
    result.downloaded_bytes = dav.downloaded_bytes;
    result.transferred_bytes = result.uploaded_bytes + result.downloaded_bytes;
    Ok(result)
}
