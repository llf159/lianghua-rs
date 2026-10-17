use lianghua_app_strategy::tracking::{self, TrackingPage};

#[tauri::command]
pub(crate) fn get_strategy_tracking_page(source_path: String) -> Result<TrackingPage, String> {
    let _guard = lianghua_app_facade::cloud_sync::SOURCE_ACCESS
        .read()
        .unwrap_or_else(|e| e.into_inner());
    tracking::get_strategy_tracking_page(&source_path)
}

#[tauri::command]
pub(crate) fn save_strategy_track(
    source_path: String,
    draft: serde_json::Value,
) -> Result<TrackingPage, String> {
    let _guard = lianghua_app_facade::cloud_sync::SOURCE_ACCESS
        .read()
        .unwrap_or_else(|e| e.into_inner());
    tracking::save_strategy_track(
        &source_path,
        serde_json::from_value(draft).map_err(|e| e.to_string())?,
    )
}

#[tauri::command]
pub(crate) fn remove_strategy_track(
    source_path: String,
    id: String,
) -> Result<TrackingPage, String> {
    let _guard = lianghua_app_facade::cloud_sync::SOURCE_ACCESS
        .read()
        .unwrap_or_else(|e| e.into_inner());
    tracking::remove_strategy_track(&source_path, &id)
}

#[tauri::command]
pub(crate) async fn run_strategy_tracking(
    source_path: String,
    reference: Option<String>,
) -> Result<TrackingPage, String> {
    tauri::async_runtime::spawn_blocking(move || {
        let _guard = lianghua_app_facade::cloud_sync::SOURCE_ACCESS
            .read()
            .unwrap_or_else(|e| e.into_inner());
        tracking::run_strategy_tracking(&source_path, reference.as_deref())
    })
    .await
    .map_err(|e| e.to_string())?
}
