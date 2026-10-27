use lianghua_app_facade::cloud_sync::{self, CloudItem, SyncResult, WebDavConfig};
use std::{collections::BTreeMap, sync::Mutex};
use tauri::{Emitter, Manager};

// 防止本机两个窗口同时同步同一个基线文件。
static CLOUD_TASK: Mutex<()> = Mutex::new(());

#[tauri::command]
pub fn get_cloud_sync_config(app: tauri::AppHandle) -> Result<WebDavConfig, String> {
    let root = app.path().app_data_dir().map_err(|e| e.to_string())?;
    let mut config = cloud_sync::get_config(&root)?;
    config.has_password = !config.password.is_empty();
    config.password.clear();
    Ok(config)
}

#[tauri::command]
pub async fn save_cloud_sync_config(
    app: tauri::AppHandle,
    config: WebDavConfig,
) -> Result<(), String> {
    tauri::async_runtime::spawn_blocking(move || {
        let _guard = CLOUD_TASK
            .try_lock()
            .map_err(|_| "云端操作正在进行，请完成后再修改连接")?;
        cloud_sync::save_config(
            &app.path().app_data_dir().map_err(|e| e.to_string())?,
            config,
        )
    })
    .await
    .map_err(|e| e.to_string())?
}

#[tauri::command]
pub async fn test_cloud_sync_connection(app: tauri::AppHandle) -> Result<(), String> {
    tauri::async_runtime::spawn_blocking(move || {
        let _guard = CLOUD_TASK.try_lock().map_err(|_| "云端操作正在进行")?;
        cloud_sync::test_connection(&app.path().app_data_dir().map_err(|e| e.to_string())?)
    })
    .await
    .map_err(|e| e.to_string())?
}

#[tauri::command]
pub async fn run_cloud_sync(
    app: tauri::AppHandle,
    settings: BTreeMap<String, String>,
    download_only: Option<bool>,
    scope: Option<cloud_sync::SyncScope>,
) -> Result<SyncResult, String> {
    tauri::async_runtime::spawn_blocking(move || {
        let _guard = CLOUD_TASK.try_lock().map_err(|_| "云端操作正在进行")?;
        let source = crate::warehouse::warehouse_root(&app)?.join("source");
        cloud_sync::sync_scoped(
            &app.path().app_data_dir().map_err(|e| e.to_string())?,
            &source,
            settings,
            download_only.unwrap_or(false),
            scope.unwrap_or(cloud_sync::SyncScope::Management),
            |message| {
                let _ = app.emit("cloud-sync-progress", message);
            },
        )
    })
    .await
    .map_err(|e| e.to_string())?
}

#[tauri::command]
pub async fn list_cloud_sync_data(app: tauri::AppHandle) -> Result<Vec<CloudItem>, String> {
    tauri::async_runtime::spawn_blocking(move || {
        let _guard = CLOUD_TASK.try_lock().map_err(|_| "云端操作正在进行")?;
        let source = crate::warehouse::warehouse_root(&app)?.join("source");
        cloud_sync::list_cloud(
            &app.path().app_data_dir().map_err(|e| e.to_string())?,
            &source,
        )
    })
    .await
    .map_err(|e| e.to_string())?
}

#[tauri::command]
pub async fn restore_cloud_sync_version(
    app: tauri::AppHandle,
    path: String,
    version_id: String,
    settings: BTreeMap<String, String>,
) -> Result<SyncResult, String> {
    tauri::async_runtime::spawn_blocking(move || {
        let _guard = CLOUD_TASK.try_lock().map_err(|_| "云端操作正在进行")?;
        let source = crate::warehouse::warehouse_root(&app)?.join("source");
        cloud_sync::restore(
            &app.path().app_data_dir().map_err(|e| e.to_string())?,
            &source,
            &path,
            &version_id,
            settings,
            |message| {
                let _ = app.emit("cloud-sync-progress", message);
            },
        )
    })
    .await
    .map_err(|e| e.to_string())?
}

#[tauri::command]
pub async fn delete_cloud_sync_version(
    app: tauri::AppHandle,
    path: String,
    version_id: String,
) -> Result<(), String> {
    tauri::async_runtime::spawn_blocking(move || {
        let _guard = CLOUD_TASK.try_lock().map_err(|_| "云端操作正在进行")?;
        cloud_sync::delete_cloud(
            &app.path().app_data_dir().map_err(|e| e.to_string())?,
            &path,
            &version_id,
        )
    })
    .await
    .map_err(|e| e.to_string())?
}

#[tauri::command]
pub fn get_cloud_sync_setting_keys() -> Vec<String> {
    cloud_sync::setting_keys()
        .iter()
        .map(|key| (*key).into())
        .collect()
}

#[tauri::command]
pub async fn estimate_cloud_sync_upload(
    app: tauri::AppHandle,
    settings: BTreeMap<String, String>,
) -> Result<cloud_sync::UploadEstimate, String> {
    tauri::async_runtime::spawn_blocking(move || {
        let _guard = CLOUD_TASK.try_lock().map_err(|_| "云端操作正在进行")?;
        let source = crate::warehouse::warehouse_root(&app)?.join("source");
        cloud_sync::estimate_upload(
            &app.path().app_data_dir().map_err(|e| e.to_string())?,
            &source,
            &settings,
        )
    })
    .await
    .map_err(|e| e.to_string())?
}

#[tauri::command]
pub async fn transfer_watchlist_cloud(
    app: tauri::AppHandle,
    settings: BTreeMap<String, String>,
    watch_rows: Vec<lianghua_app_market::watch_observe::WatchObserveStoredRow>,
    mode: cloud_sync::WatchlistTransferMode,
) -> Result<cloud_sync::WatchlistTransferResult, String> {
    tauri::async_runtime::spawn_blocking(move || {
        let _guard = CLOUD_TASK.try_lock().map_err(|_| "云端操作正在进行")?;
        let source = crate::warehouse::warehouse_root(&app)?.join("source");
        cloud_sync::transfer_watchlists(
            &app.path().app_data_dir().map_err(|e| e.to_string())?,
            &source,
            settings,
            Some(watch_rows),
            mode,
            |message| {
                let _ = app.emit("cloud-sync-progress", message);
            },
        )
    })
    .await
    .map_err(|e| e.to_string())?
}

#[tauri::command]
pub async fn transfer_strategy_cloud(
    app: tauri::AppHandle,
    mode: cloud_sync::WatchlistTransferMode,
) -> Result<cloud_sync::WatchlistTransferResult, String> {
    tauri::async_runtime::spawn_blocking(move || {
        let _guard = CLOUD_TASK.try_lock().map_err(|_| "云端操作正在进行")?;
        let source = crate::warehouse::warehouse_root(&app)?.join("source");
        cloud_sync::transfer_strategies(
            &app.path().app_data_dir().map_err(|e| e.to_string())?,
            &source,
            mode,
            |message| {
                let _ = app.emit("cloud-sync-progress", message);
            },
        )
    })
    .await
    .map_err(|e| e.to_string())?
}

#[tauri::command]
pub async fn preview_cloud_sync_data(
    app: tauri::AppHandle,
    path: String,
    version_id: Option<String>,
    settings: BTreeMap<String, String>,
) -> Result<cloud_sync::CloudPreview, String> {
    tauri::async_runtime::spawn_blocking(move || {
        let _guard = CLOUD_TASK.try_lock().map_err(|_| "云端操作正在进行")?;
        let source = crate::warehouse::warehouse_root(&app)?.join("source");
        cloud_sync::preview(
            &app.path().app_data_dir().map_err(|e| e.to_string())?,
            &source,
            &path,
            version_id.as_deref(),
            &settings,
        )
    })
    .await
    .map_err(|e| e.to_string())?
}

#[tauri::command]
pub async fn resolve_cloud_strategy_conflicts(
    app: tauri::AppHandle,
    resolution: cloud_sync::StrategyResolution,
) -> Result<SyncResult, String> {
    tauri::async_runtime::spawn_blocking(move || {
        let _guard = CLOUD_TASK.try_lock().map_err(|_| "云端操作正在进行")?;
        let source = crate::warehouse::warehouse_root(&app)?.join("source");
        cloud_sync::resolve_strategies(
            &app.path().app_data_dir().map_err(|e| e.to_string())?,
            &source,
            resolution,
            |message| {
                let _ = app.emit("cloud-sync-progress", message);
            },
        )
    })
    .await
    .map_err(|e| e.to_string())?
}
