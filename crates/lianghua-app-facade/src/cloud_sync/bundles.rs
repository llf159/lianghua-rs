use super::{allowed_setting, validate_path};
use std::{
    collections::BTreeMap,
    io::{Cursor, Read, Write},
};
use zip::{ZipArchive, ZipWriter, write::SimpleFileOptions};

pub(super) fn for_path(path: &str) -> Option<&'static str> {
    if path == "app/watch_observe.json"
        || matches!(
            path.strip_prefix("settings/"),
            Some(
                "lh_intraday_monitor_watchlist_v1"
                    | "lh_intraday_monitor_watchlist_enabled_v1"
                    | "am_board_filter"
            )
        )
    {
        Some("bundles/watchlists.zip")
    } else if let Some(key) = path.strip_prefix("settings/") {
        allowed_setting(key).then_some("bundles/browser_settings.zip")
    } else if path.starts_with("source/") {
        if path.ends_with("/ind.toml") || path.ends_with("/chart_indicators.toml") {
            Some("bundles/indicator_config.zip")
        } else {
            Some("bundles/strategies.zip")
        }
    } else {
        None
    }
}

pub(super) fn snapshots(
    files: &BTreeMap<String, Vec<u8>>,
) -> Result<BTreeMap<String, Vec<u8>>, String> {
    let mut groups = BTreeMap::<&str, Vec<(&String, &Vec<u8>)>>::new();
    for (path, bytes) in files {
        let group = for_path(path).ok_or_else(|| format!("不允许打包此路径：{path}"))?;
        groups.entry(group).or_default().push((path, bytes));
    }
    let mut snapshots = BTreeMap::new();
    for (group, members) in groups {
        let mut archive = ZipWriter::new(Cursor::new(Vec::new()));
        for (path, bytes) in members {
            archive
                .start_file(
                    path,
                    SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored),
                )
                .map_err(|e| e.to_string())?;
            archive.write_all(bytes).map_err(|e| e.to_string())?;
        }
        let raw = archive.finish().map_err(|e| e.to_string())?.into_inner();
        if raw.len() > 512 * 1024 * 1024 {
            return Err("配置包超过同步大小限制".into());
        }
        snapshots.insert(group.into(), raw);
    }
    Ok(snapshots)
}

pub(super) fn unpack(group: &str, bytes: &[u8]) -> Result<BTreeMap<String, Vec<u8>>, String> {
    read(group, bytes, true)
}

pub(super) fn read(
    group: &str,
    bytes: &[u8],
    validate_content: bool,
) -> Result<BTreeMap<String, Vec<u8>>, String> {
    let mut archive = ZipArchive::new(Cursor::new(bytes)).map_err(|e| e.to_string())?;
    let mut files = BTreeMap::new();
    let mut total = 0u64;
    for index in 0..archive.len() {
        let entry = archive.by_index(index).map_err(|e| e.to_string())?;
        let path = entry.name().map_err(|e| e.to_string())?.into_owned();
        validate_path(&path)?;
        if for_path(&path) != Some(group) || files.contains_key(&path) {
            return Err("配置包内容分组错误或存在重复路径".into());
        }
        total = total.checked_add(entry.size()).ok_or("配置包大小无效")?;
        if total > 512 * 1024 * 1024 {
            return Err("配置包展开超过大小限制".into());
        }
        let mut raw = Vec::new();
        entry
            .take(512 * 1024 * 1024 + 1)
            .read_to_end(&mut raw)
            .map_err(|e| e.to_string())?;
        if raw.len() as u64 > 512 * 1024 * 1024 {
            return Err("配置包成员超过大小限制".into());
        }
        // 历史策略保留原文，即使旧内容已无法解析；只有会直接启用的配置需要语法验证。
        if validate_content
            && matches!(
                path.as_str(),
                "source/score_rule.toml"
                    | "source/chip_change_rule.toml"
                    | "source/ind.toml"
                    | "source/chart_indicators.toml"
            )
        {
            toml::from_str::<toml::Value>(
                std::str::from_utf8(&raw).map_err(|_| "配置包 TOML 不是有效文本")?,
            )
            .map_err(|e| format!("配置包 TOML 无效：{e}"))?;
        } else if validate_content && path.ends_with(".json") {
            serde_json::from_slice::<serde_json::Value>(&raw)
                .map_err(|e| format!("配置包 JSON 无效：{e}"))?;
        } else {
            std::str::from_utf8(&raw).map_err(|_| "配置包习惯设置不是有效文本")?;
        }
        files.insert(path, raw);
    }
    if files.is_empty() {
        return Err("配置包为空，未替换本地数据".into());
    }
    Ok(files)
}
