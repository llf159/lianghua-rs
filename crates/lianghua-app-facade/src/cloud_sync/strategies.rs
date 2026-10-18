use super::{WatchlistTransferMode, WatchlistTransferResult, digest};
use std::{collections::BTreeMap, path::Path};

pub fn transfer(
    app_data: &Path,
    source: &Path,
    mode: WatchlistTransferMode,
    progress: impl Fn(&str),
) -> Result<WatchlistTransferResult, String> {
    super::watchlists::transfer_bundle(
        app_data,
        source,
        BTreeMap::new(),
        None,
        mode,
        true,
        progress,
    )
}

// 来源按云端时间排序，本地最后；策略作为完整资产合并，不拼接规则。
pub(super) fn merge_files(
    sources: &[BTreeMap<String, Vec<u8>>],
) -> Result<BTreeMap<String, Vec<u8>>, String> {
    let mut merged = BTreeMap::<String, Vec<u8>>::new();
    for files in sources {
        let mut groups = BTreeMap::<String, BTreeMap<String, Vec<u8>>>::new();
        for (path, bytes) in files {
            if matches!(
                path.as_str(),
                "source/score_rule.toml" | "source/chip_change_rule.toml"
            ) {
                continue;
            }
            let parts = path.split('/').collect::<Vec<_>>();
            let key = if parts.len() >= 4 {
                parts[..parts.len() - 1].join("/")
            } else if parts.get(1) == Some(&"chip_change_rule_backups") && parts.len() == 3 {
                format!(
                    "source/chip_change_rule_backups/{}",
                    parts[2]
                        .split_once(".toml")
                        .map_or(parts[2], |(stem, _)| stem)
                )
            } else {
                path.rsplit_once('.')
                    .map_or(path.clone(), |(stem, _)| stem.to_string())
            };
            groups
                .entry(key)
                .or_default()
                .insert(path.clone(), bytes.clone());
        }
        for (group, incoming) in groups {
            if incoming
                .iter()
                .any(|(path, bytes)| merged.get(path).is_some_and(|old| old != bytes))
            {
                // 同名备份连同元数据一起另存，避免一端覆盖另一端的描述或规则。
                let old = merged
                    .iter()
                    .filter(|(path, _)| {
                        *path == &group
                            || path.starts_with(&format!("{group}/"))
                            || incoming
                                .keys()
                                .any(|member| member.starts_with(&format!("{group}.")))
                                && path.starts_with(&format!("{group}."))
                    })
                    .map(|(path, bytes)| (path.clone(), bytes.clone()))
                    .collect::<BTreeMap<_, _>>();
                let hash = digest(&serde_json::to_vec(&old).map_err(|e| e.to_string())?);
                let parent = group
                    .rsplit_once('/')
                    .map_or("source", |(parent, _)| parent);
                for (path, bytes) in old {
                    merged.remove(&path);
                    merged.insert(
                        format!("{parent}/cloud-{hash}{}", &path[group.len()..]),
                        bytes,
                    );
                }
            }
            merged.extend(incoming);
        }
        for (active, root, file_name) in [
            (
                "source/score_rule.toml",
                "source/strategy_backups",
                "score_rule.toml",
            ),
            (
                "source/chip_change_rule.toml",
                "source/chip_change_rule_backups",
                "chip_change_rule.toml",
            ),
        ] {
            let Some(incoming) = files.get(active) else {
                continue;
            };
            if let Some(old) = merged.get(active).filter(|old| *old != incoming).cloned() {
                let hash = digest(&old);
                let (backup, meta) = if file_name == "score_rule.toml" {
                    (
                        format!("{root}/cloud-{hash}/{file_name}"),
                        format!("{root}/cloud-{hash}/meta.json"),
                    )
                } else {
                    (
                        format!("{root}/cloud-{hash}.toml"),
                        format!("{root}/cloud-{hash}.toml.meta.json"),
                    )
                };
                merged.entry(backup).or_insert(old);
                if let std::collections::btree_map::Entry::Vacant(entry) = merged.entry(meta) {
                    entry.insert(
                        serde_json::to_vec(&serde_json::json!({
                            "version": 1, "createdAt": chrono::Utc::now().to_rfc3339(),
                            "sourceKind": "cloud_sync", "sourceFileName": file_name,
                            "description": "云端合并保留的当前策略"
                        }))
                        .map_err(|e| e.to_string())?,
                    );
                }
            }
            merged.insert(active.into(), incoming.clone());
        }
    }
    Ok(merged)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_preserves_active_and_colliding_assets_and_is_stable() {
        let cloud = BTreeMap::from([
            ("source/score_rule.toml".into(), b"rules = [1]".to_vec()),
            (
                "source/chip_change_rule.toml".into(),
                b"rules = [2]".to_vec(),
            ),
            (
                "source/strategy_backups/shared/score_rule.toml".into(),
                b"rules = [3]".to_vec(),
            ),
            (
                "source/strategy_backups/shared/meta.json".into(),
                br#"{"description":"cloud"}"#.to_vec(),
            ),
            (
                "source/chip_change_rule_backups/shared.toml".into(),
                b"rules = [4]".to_vec(),
            ),
            (
                "source/chip_change_rule_backups/shared.toml.meta.json".into(),
                br#"{"description":"cloud"}"#.to_vec(),
            ),
            (
                "source/strategy_snapshots/rank_compute/day/score_rule.toml".into(),
                b"rules = [9]".to_vec(),
            ),
            (
                "source/cloud_sync_backups/old.toml".into(),
                b"rules = [9]".to_vec(),
            ),
        ]);
        let local = BTreeMap::from([
            ("source/score_rule.toml".into(), b"rules = [5]".to_vec()),
            (
                "source/chip_change_rule.toml".into(),
                b"rules = [6]".to_vec(),
            ),
            (
                "source/strategy_backups/shared/score_rule.toml".into(),
                b"rules = [7]".to_vec(),
            ),
            (
                "source/strategy_backups/shared/meta.json".into(),
                br#"{"description":"local"}"#.to_vec(),
            ),
            (
                "source/chip_change_rule_backups/shared.toml".into(),
                b"rules = [8]".to_vec(),
            ),
            (
                "source/chip_change_rule_backups/shared.toml.meta.json".into(),
                br#"{"description":"local"}"#.to_vec(),
            ),
            (
                "source/strategy_snapshots/rank_compute/day/score_rule.toml".into(),
                b"rules = [10]".to_vec(),
            ),
            (
                "source/cloud_sync_backups/old.toml".into(),
                b"rules = [10]".to_vec(),
            ),
        ]);
        let merged = merge_files(&[cloud.clone(), local.clone()]).unwrap();
        assert_eq!(
            merged["source/score_rule.toml"],
            local["source/score_rule.toml"]
        );
        assert_eq!(
            merged["source/chip_change_rule.toml"],
            local["source/chip_change_rule.toml"]
        );
        for bytes in cloud.values().chain(local.values()) {
            assert!(merged.values().any(|value| value == bytes));
        }
        assert_eq!(merge_files(&[cloud, merged.clone()]).unwrap(), merged);
        let bundle = super::super::bundles::snapshots(&merged).unwrap();
        assert_eq!(
            super::super::bundles::unpack(
                "bundles/strategies.zip",
                &bundle["bundles/strategies.zip"]
            )
            .unwrap(),
            merged
        );
    }
}
