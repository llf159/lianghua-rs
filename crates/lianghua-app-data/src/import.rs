use std::{
    fs,
    path::{Component, Path, PathBuf},
};

pub fn validate_target_relative_path(path: &str) -> Result<(), String> {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        return Err("empty target path".into());
    }

    let normalized = trimmed.replace('\\', "/");
    let target_path = Path::new(&normalized);
    if target_path.is_absolute() {
        return Err("target path must be relative".into());
    }

    for component in target_path.components() {
        match component {
            Component::Normal(_) | Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err("target path contains invalid segments".into());
            }
        }
    }

    Ok(())
}

pub fn resolve_source_root(app_data_root: &Path, source_dir: &str) -> Result<PathBuf, String> {
    validate_target_relative_path(source_dir)?;
    let normalized_source_dir = source_dir.trim().replace('\\', "/");
    Ok(app_data_root.join(normalized_source_dir))
}

pub fn managed_source_file_name(file_id: &str) -> Option<&'static str> {
    match file_id {
        "source-db" => Some("stock_data.db"),
        "dragon-tiger-db" => Some("dragon_tiger.db"),
        "stock-list" => Some("stock_list.csv"),
        "trade-calendar" => Some("trade_calendar.csv"),
        "result-db" => Some("scoring_result.db"),
        "concept-performance-db" => Some("concept_performance.db"),
        "cyq-db" => Some("cyq.db"),
        "cyq-chen-db" => Some("cyq_chen.db"),
        "chip-change-rule" => Some("chip_change_rule.toml"),
        "score-rule" => Some("score_rule.toml"),
        "indicator-config" => Some("ind.toml"),
        "chart-indicator-config" => Some("chart_indicators.toml"),
        "ths-concepts" => Some("stock_concepts.csv"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{is_managed_source_temporary_path, managed_source_file_name};
    use std::path::Path;

    #[test]
    fn export_skips_temporary_artifacts_but_keeps_database_wal_and_backups() {
        for name in [
            ".cyq_chen.db.rebuild-123.tmp",
            ".cyq_chen.db.rebuild-123.tmp.wal",
            "cyq_chen.db.tmp/duckdb_temp_storage-0.tmp",
            ".score_rule.toml.123.tmp",
        ] {
            assert!(is_managed_source_temporary_path(Path::new(name)), "{name}");
        }
        for name in [
            "cyq_chen.db",
            "cyq_chen.db.wal",
            "chip_change_rule_backups/strategy.toml",
            "strategy_snapshots/rank_compute/meta.json",
        ] {
            assert!(!is_managed_source_temporary_path(Path::new(name)), "{name}");
        }
    }

    #[test]
    fn resolves_all_specialized_managed_assets() {
        assert_eq!(
            managed_source_file_name("dragon-tiger-db"),
            Some("dragon_tiger.db")
        );
        assert_eq!(
            managed_source_file_name("chip-change-rule"),
            Some("chip_change_rule.toml")
        );
    }
}

pub fn resolve_managed_source_file_path(
    app_data_root: &Path,
    source_dir: &str,
    file_id: &str,
) -> Result<(String, PathBuf), String> {
    let source_root = resolve_source_root(app_data_root, source_dir)?;
    let normalized_file_id = file_id.trim();
    let file_name = managed_source_file_name(normalized_file_id)
        .ok_or_else(|| format!("未知文件项: {normalized_file_id}"))?;
    let target_relative_path = if source_dir.trim().is_empty() {
        file_name.to_string()
    } else {
        format!("{}/{}", source_dir.trim().replace('\\', "/"), file_name)
    };
    Ok((target_relative_path, source_root.join(file_name)))
}

/// Temporary files and spill directories are not portable source assets.
/// Keep ordinary database WAL files: they can contain committed data.
pub fn is_managed_source_temporary_path(path: &Path) -> bool {
    path.components().any(|component| {
        let Component::Normal(name) = component else {
            return false;
        };
        let name = name.to_string_lossy();
        name.ends_with(".tmp") || name.ends_with(".tmp.wal") || name == ".cyq_chen.rebuild.lock"
    })
}

pub fn copy_directory_recursive(source: &Path, target: &Path) -> Result<u64, String> {
    fs::create_dir_all(target).map_err(|error| error.to_string())?;
    let mut file_count = 0u64;

    for entry in fs::read_dir(source).map_err(|error| error.to_string())? {
        let entry = entry.map_err(|error| error.to_string())?;
        let entry_path = entry.path();
        if is_managed_source_temporary_path(Path::new(&entry.file_name())) {
            continue;
        }
        let entry_type = entry.file_type().map_err(|error| error.to_string())?;
        let target_path = target.join(entry.file_name());

        if entry_type.is_dir() {
            file_count += copy_directory_recursive(&entry_path, &target_path)?;
            continue;
        }

        if entry_type.is_file() {
            if let Some(parent) = target_path.parent() {
                fs::create_dir_all(parent).map_err(|error| error.to_string())?;
            }
            fs::copy(&entry_path, &target_path).map_err(|error| error.to_string())?;
            file_count += 1;
        }
    }

    Ok(file_count)
}
