use std::path::{Path, PathBuf};

use lianghua_app_data::import::{copy_directory_recursive, is_managed_source_temporary_path};
use serde::{Deserialize, Serialize};
use tauri::Manager;
use tauri_plugin_fs::FsExt;

#[cfg(target_os = "android")]
use jni::{
    objects::{GlobalRef, JObject, JString, JValue},
    JNIEnv, JavaVM,
};

#[cfg(target_os = "android")]
static ANDROID_ACTIVITY: std::sync::OnceLock<std::sync::Mutex<Option<(JavaVM, GlobalRef)>>> =
    std::sync::OnceLock::new();

#[cfg(target_os = "android")]
pub fn set_android_activity(env: &mut JNIEnv, activity: &JObject) -> Result<(), String> {
    let vm = env.get_java_vm().map_err(|error| error.to_string())?;
    let activity = env
        .new_global_ref(activity)
        .map_err(|error| error.to_string())?;
    let mut slot = ANDROID_ACTIVITY
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .map_err(|error| error.to_string())?;
    *slot = Some((vm, activity));
    Ok(())
}

#[cfg(target_os = "android")]
fn android_warehouse_method(method: &str, input: Option<&str>) -> Result<Option<String>, String> {
    let slot = ANDROID_ACTIVITY
        .get()
        .ok_or_else(|| "Android 界面尚未初始化".to_string())?
        .lock()
        .map_err(|error| error.to_string())?;
    let (vm, activity) = slot
        .as_ref()
        .ok_or_else(|| "Android 界面尚未初始化".to_string())?;
    let mut env = vm
        .attach_current_thread()
        .map_err(|error| error.to_string())?;
    let activity = activity.as_obj();
    let result = if let Some(input) = input {
        let argument = env.new_string(input).map_err(|error| error.to_string())?;
        env.call_method(
            activity,
            method,
            "(Ljava/lang/String;)Ljava/lang/String;",
            &[JValue::Object(&argument)],
        )
    } else if method == "hasWarehouseStorageAccess" {
        return env
            .call_method(activity, method, "()Z", &[])
            .and_then(|value| value.z())
            .map(|allowed| Some(allowed.to_string()))
            .map_err(|error| error.to_string());
    } else if method == "pollWarehouseDirectoryPicker" {
        env.call_method(activity, method, "()Ljava/lang/String;", &[])
    } else {
        return env
            .call_method(activity, method, "()V", &[])
            .map(|_| None)
            .map_err(|error| error.to_string());
    }
    .map_err(|error| error.to_string())?
    .l()
    .map_err(|error| error.to_string())?;
    if result.is_null() {
        return Ok(None);
    }
    let result = JString::from(result);
    env.get_string(&result)
        .map(|value| Some(value.into()))
        .map_err(|error| error.to_string())
}

#[cfg(target_os = "android")]
fn android_storage_allowed() -> Result<bool, String> {
    Ok(android_warehouse_method("hasWarehouseStorageAccess", None)?.as_deref() == Some("true"))
}

#[cfg(target_os = "android")]
#[tauri::command]
pub fn request_android_warehouse_access() -> Result<bool, String> {
    if android_storage_allowed()? {
        return Ok(true);
    }
    android_warehouse_method("requestWarehouseStorageAccess", None)?;
    Ok(false)
}

#[cfg(target_os = "android")]
pub fn resolve_android_warehouse_directory(uri: String) -> Result<String, String> {
    if !android_storage_allowed()? {
        return Err("请先在系统设置中授予文件读写权限，然后返回应用重试".into());
    }
    android_warehouse_method("resolveWarehouseDirectory", Some(&uri))?
        .ok_or_else(|| "请选择内部共享存储中的子目录；该目录无法作为数据库路径".into())
}

#[cfg(target_os = "android")]
#[tauri::command]
pub async fn pick_android_warehouse_directory() -> Result<Option<String>, String> {
    android_warehouse_method("startWarehouseDirectoryPicker", None)?;
    loop {
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        if let Some(uri) = android_warehouse_method("pollWarehouseDirectoryPicker", None)? {
            if uri.is_empty() {
                return Ok(None);
            }
            return resolve_android_warehouse_directory(uri).map(Some);
        }
    }
}

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
    if target_root.starts_with(source_root) {
        return Err("目标目录不能位于当前 source 数据目录内部".into());
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
    #[cfg(target_os = "android")]
    {
        if !android_storage_allowed()? {
            return Err("请先在系统设置中授予文件读写权限，然后返回应用重试".into());
        }
        std::fs::read_dir(&root)
            .map_err(|error| format!("目标目录不可读 {}: {error}", root.display()))?;
        let probe = root.join(format!(".lianghua-write-check-{}", std::process::id()));
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&probe)
            .map_err(|error| format!("目标目录不可写 {}: {error}", root.display()))?;
        std::fs::remove_file(&probe)
            .map_err(|error| format!("清理目录读写检查文件失败 {}: {error}", probe.display()))?;
    }
    app.fs_scope()
        .allow_directory(&root, true)
        .map_err(|error| error.to_string())?;
    let payload = serde_json::to_string_pretty(&WarehouseConfig {
        root: root.display().to_string(),
    })
    .map_err(|error| error.to_string())?;
    std::fs::write(warehouse_config_path(&app)?, payload).map_err(|error| error.to_string())?;

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
