use std::path::{Path, PathBuf};

use lianghua_app_data::import::{copy_directory_recursive, is_managed_source_temporary_path};
use serde::{Deserialize, Serialize};
use tauri::Manager;
use tauri_plugin_fs::FsExt;

#[derive(Serialize, Deserialize)]
struct WarehouseConfig {
    root: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WarehouseMoveResult {
    source_root: String,
    target_root: String,
    file_count: u64,
    total_bytes: u64,
}

fn warehouse_config_path(app: &tauri::AppHandle) -> Result<PathBuf, String> {
    app.path()
        .resolve("warehouse.json", tauri::path::BaseDirectory::AppData)
        .map_err(|error| error.to_string())
}

fn normalize_root(root: PathBuf) -> PathBuf {
    std::fs::canonicalize(&root).unwrap_or(root)
}

fn configured_root(app: &tauri::AppHandle) -> Option<PathBuf> {
    if let Ok(raw) = std::env::var("LIANGHUA_WAREHOUSE_ROOT") {
        let trimmed = raw.trim();
        if !trimmed.is_empty() {
            return Some(PathBuf::from(trimmed));
        }
    }

    let raw = std::fs::read_to_string(warehouse_config_path(app).ok()?).ok()?;
    let config: WarehouseConfig = serde_json::from_str(&raw).ok()?;
    let trimmed = config.root.trim();
    if trimmed.is_empty() {
        return None;
    }

    Some(PathBuf::from(trimmed))
}

pub fn warehouse_root(app: &tauri::AppHandle) -> Result<PathBuf, String> {
    if let Some(root) = configured_root(app) {
        return Ok(normalize_root(root));
    }

    app.path()
        .resolve("", tauri::path::BaseDirectory::AppData)
        .map_err(|error| error.to_string())
}

pub fn allow_warehouse_root(app: &tauri::AppHandle) -> Result<(), String> {
    let root = warehouse_root(app)?;
    app.fs_scope()
        .allow_directory(root, true)
        .map_err(|error| error.to_string())
}

fn directory_usage(root: &Path) -> Result<(u64, u64), String> {
    let mut file_count = 0u64;
    let mut total_bytes = 0u64;

    for entry in std::fs::read_dir(root).map_err(|error| error.to_string())? {
        let entry = entry.map_err(|error| error.to_string())?;
        if is_managed_source_temporary_path(Path::new(&entry.file_name())) {
            continue;
        }
        let entry_type = entry.file_type().map_err(|error| error.to_string())?;
        if entry_type.is_dir() {
            let (child_file_count, child_total_bytes) = directory_usage(&entry.path())?;
            file_count += child_file_count;
            total_bytes += child_total_bytes;
            continue;
        }
        if entry_type.is_file() {
            file_count += 1;
            total_bytes += entry.metadata().map_err(|error| error.to_string())?.len();
        }
    }

    Ok((file_count, total_bytes))
}

fn move_source_directory(
    source_root: &Path,
    target_root: &Path,
) -> Result<WarehouseMoveResult, String> {
    if !source_root.is_dir() {
        return Err(format!("当前数据仓库目录不存在: {}", source_root.display()));
    }

    let target_source_root = target_root.join("source");
    if target_source_root == source_root {
        return Err("目标目录与当前数据仓库目录相同".into());
    }
    if target_source_root
        .read_dir()
        .map(|mut entries| entries.next().is_some())
        .unwrap_or(false)
    {
        return Err(format!(
            "目标目录下已有 source 数据，请换一个目录或先手动处理: {}",
            target_source_root.display()
        ));
    }

    let (_, source_bytes) = directory_usage(source_root)?;
    let file_count = copy_directory_recursive(source_root, &target_source_root)?;
    let (target_file_count, target_bytes) = directory_usage(&target_source_root)?;
    if target_file_count != file_count || target_bytes != source_bytes {
        return Err(format!(
            "复制校验失败：源 {file_count} 个文件 / {source_bytes} 字节，目标 {target_file_count} 个文件 / {target_bytes} 字节；源目录未改动，目标目录可能残留未完成副本 {}",
            target_source_root.display()
        ));
    }

    Ok(WarehouseMoveResult {
        source_root: source_root.display().to_string(),
        target_root: target_source_root.display().to_string(),
        file_count,
        total_bytes: target_bytes,
    })
}

#[tauri::command]
pub fn get_warehouse_root(app: tauri::AppHandle) -> Result<String, String> {
    Ok(warehouse_root(&app)?.display().to_string())
}

#[tauri::command]
pub fn set_warehouse_root(app: tauri::AppHandle, path: String) -> Result<String, String> {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        return Err("empty warehouse root".into());
    }

    let root = PathBuf::from(trimmed);
    if !root.is_dir() {
        return Err(format!("数据仓库目录不存在: {}", root.display()));
    }

    let root = normalize_root(root);
    let payload = serde_json::to_string_pretty(&WarehouseConfig {
        root: root.display().to_string(),
    })
    .map_err(|error| error.to_string())?;
    std::fs::write(warehouse_config_path(&app)?, payload).map_err(|error| error.to_string())?;
    allow_warehouse_root(&app)?;

    Ok(root.display().to_string())
}

#[tauri::command]
pub async fn move_warehouse_source(
    app: tauri::AppHandle,
    target_root: String,
) -> Result<WarehouseMoveResult, String> {
    tauri::async_runtime::spawn_blocking(move || {
        let trimmed = target_root.trim();
        if trimmed.is_empty() {
            return Err("empty warehouse root".into());
        }

        let target_root = PathBuf::from(trimmed);
        if !target_root.is_dir() {
            return Err(format!("数据仓库目录不存在: {}", target_root.display()));
        }
        let target_root = normalize_root(target_root);

        let source_root = warehouse_root(&app)?.join("source");
        move_source_directory(&source_root, &target_root)
    })
    .await
    .map_err(|error| error.to_string())?
}

#[cfg(test)]
mod tests {
    use super::move_source_directory;

    #[test]
    fn move_source_directory_copies_files_and_rejects_existing_target() {
        let base = std::env::temp_dir().join(format!(
            "lianghua-warehouse-move-{}-{:?}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|value| value.as_nanos())
        ));
        let source_root = base.join("old").join("source");
        let target_root = base.join("new");
        std::fs::create_dir_all(source_root.join("nested")).unwrap();
        std::fs::create_dir_all(&target_root).unwrap();
        std::fs::write(source_root.join("a.db"), b"aaa").unwrap();
        std::fs::write(source_root.join("nested").join("b.toml"), b"bb").unwrap();
        std::fs::write(source_root.join("c.tmp"), b"tmp").unwrap();

        let result = move_source_directory(&source_root, &target_root).unwrap();
        assert_eq!(result.file_count, 2);
        assert!(target_root
            .join("source")
            .join("nested")
            .join("b.toml")
            .is_file());
        assert!(!target_root.join("source").join("c.tmp").exists());

        assert!(move_source_directory(&source_root, &target_root).is_err());
        assert!(move_source_directory(&source_root, &base.join("old")).is_err());

        std::fs::remove_dir_all(&base).unwrap();
    }
}
