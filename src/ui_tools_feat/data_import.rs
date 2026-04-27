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
        "stock-list" => Some("stock_list.csv"),
        "trade-calendar" => Some("trade_calendar.csv"),
        "result-db" => Some("scoring_result.db"),
        "concept-performance-db" => Some("concept_performance.db"),
        "cyq-db" => Some("cyq.db"),
        "score-rule" => Some("score_rule.toml"),
        "indicator-config" => Some("ind.toml"),
        "chart-indicator-config" => Some("chart_indicators.toml"),
        "ths-concepts" => Some("stock_concepts.csv"),
        _ => None,
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

pub fn copy_directory_recursive(source: &Path, target: &Path) -> Result<u64, String> {
    fs::create_dir_all(target).map_err(|error| error.to_string())?;
    let mut file_count = 0u64;

    for entry in fs::read_dir(source).map_err(|error| error.to_string())? {
        let entry = entry.map_err(|error| error.to_string())?;
        let entry_path = entry.path();
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
