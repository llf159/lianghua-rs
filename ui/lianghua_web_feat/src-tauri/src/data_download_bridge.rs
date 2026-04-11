use std::time::Instant;

use lianghua_rs::{
    download::runner::DownloadProgress as CoreDownloadProgress,
    ui_tools_feat::data_download::{
        ConceptPerformanceRepairRunInput as CoreConceptPerformanceRepairRunInput,
        DataDownloadRunInput as CoreDataDownloadRunInput, DataDownloadRunResult,
        DataDownloadStatus, IndicatorManageDraft as CoreIndicatorManageDraft,
        IndicatorManagePageData, MissingStockRepairRunInput as CoreMissingStockRepairRunInput,
        ThsConceptDownloadRunInput as CoreThsConceptDownloadRunInput,
        get_data_download_status as core_get_data_download_status,
        get_indicator_manage_page as core_get_indicator_manage_page,
        prepare_concept_performance_repair_run as core_prepare_concept_performance_repair_run,
        prepare_data_download_run as core_prepare_data_download_run,
        prepare_missing_stock_repair_run as core_prepare_missing_stock_repair_run,
        prepare_ths_concept_download_run as core_prepare_ths_concept_download_run,
        run_prepared_concept_performance_repair as core_run_prepared_concept_performance_repair,
        run_prepared_data_download as core_run_prepared_data_download,
        run_prepared_missing_stock_repair as core_run_prepared_missing_stock_repair,
        run_prepared_ths_concept_download as core_run_prepared_ths_concept_download,
        save_indicator_manage_page as core_save_indicator_manage_page,
    },
};
use serde::{Deserialize, Serialize};
use tauri::Emitter;

const DATA_DOWNLOAD_EVENT: &str = "data-download-status";

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DataDownloadRequest {
    download_id: String,
    source_path: String,
    token: String,
    start_date: String,
    end_date: String,
    threads: usize,
    retry_times: usize,
    limit_calls_per_min: usize,
    include_turnover: bool,
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MissingStockRepairRequest {
    download_id: String,
    source_path: String,
    token: String,
    threads: usize,
    retry_times: usize,
    limit_calls_per_min: usize,
    include_turnover: bool,
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ThsConceptDownloadRequest {
    download_id: String,
    source_path: String,
    retry_enabled: bool,
    retry_times: usize,
    retry_interval_secs: u64,
    concurrent_enabled: bool,
    worker_threads: usize,
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConceptPerformanceRepairRequest {
    download_id: String,
    source_path: String,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct DataDownloadEventPayload {
    download_id: String,
    phase: String,
    action: String,
    action_label: String,
    elapsed_ms: u64,
    finished: u64,
    total: u64,
    current_label: Option<String>,
    message: String,
}

fn emit_data_download_event(app: &tauri::AppHandle, payload: DataDownloadEventPayload) {
    if let Err(emit_error) = app.emit(DATA_DOWNLOAD_EVENT, payload) {
        log::warn!("failed to emit data download event: {}", emit_error);
    }
}

fn emit_core_download_progress(
    app: &tauri::AppHandle,
    download_id: &str,
    action: &str,
    action_label: &str,
    elapsed_ms: u64,
    progress: CoreDownloadProgress,
) {
    emit_data_download_event(
        app,
        DataDownloadEventPayload {
            download_id: download_id.to_string(),
            phase: progress.phase,
            action: action.to_string(),
            action_label: action_label.to_string(),
            elapsed_ms,
            finished: progress.finished as u64,
            total: progress.total as u64,
            current_label: progress.current_label,
            message: progress.message,
        },
    );
}

#[tauri::command]
pub fn get_data_download_status(source_path: String) -> Result<DataDownloadStatus, String> {
    core_get_data_download_status(&source_path)
}

#[tauri::command]
pub fn get_indicator_manage_page(source_path: String) -> Result<IndicatorManagePageData, String> {
    core_get_indicator_manage_page(&source_path)
}

#[tauri::command]
pub fn save_indicator_manage_page(
    source_path: String,
    items: Vec<CoreIndicatorManageDraft>,
) -> Result<IndicatorManagePageData, String> {
    core_save_indicator_manage_page(&source_path, items)
}

#[tauri::command]
pub async fn run_data_download(
    app: tauri::AppHandle,
    request: DataDownloadRequest,
) -> Result<DataDownloadRunResult, String> {
    let download_id = request.download_id.trim().to_string();
    if download_id.is_empty() {
        return Err("download_id 不能为空".to_string());
    }

    let prepared = core_prepare_data_download_run(CoreDataDownloadRunInput {
        source_path: request.source_path,
        token: request.token,
        start_date: request.start_date,
        end_date: request.end_date,
        threads: request.threads,
        retry_times: request.retry_times,
        limit_calls_per_min: request.limit_calls_per_min,
        include_turnover: request.include_turnover,
    })?;
    let action = prepared.action.clone();
    let action_label = prepared.action_label.clone();
    emit_data_download_event(
        &app,
        DataDownloadEventPayload {
            download_id: download_id.clone(),
            phase: "started".to_string(),
            action: action.clone(),
            action_label: action_label.clone(),
            elapsed_ms: 0,
            finished: 0,
            total: 0,
            current_label: None,
            message: format!("{action_label} 已启动，正在准备执行下载。"),
        },
    );

    tauri::async_runtime::spawn_blocking(move || {
        let started_at = Instant::now();
        let result = (|| -> Result<DataDownloadRunResult, String> {
            let progress_app = app.clone();
            let progress_download_id = download_id.clone();
            let progress_action = action.clone();
            let progress_action_label = action_label.clone();
            let progress_started_at = started_at;
            let progress_cb = move |progress: CoreDownloadProgress| {
                emit_core_download_progress(
                    &progress_app,
                    progress_download_id.as_str(),
                    progress_action.as_str(),
                    progress_action_label.as_str(),
                    progress_started_at.elapsed().as_millis() as u64,
                    progress,
                );
            };

            let mut run_result = core_run_prepared_data_download(&prepared, Some(&progress_cb))?;
            run_result.elapsed_ms = started_at.elapsed().as_millis() as u64;
            Ok(run_result)
        })();

        match &result {
            Ok(run_result) => emit_data_download_event(
                &app,
                DataDownloadEventPayload {
                    download_id: download_id.clone(),
                    phase: "completed".to_string(),
                    action: action.clone(),
                    action_label: action_label.clone(),
                    elapsed_ms: run_result.elapsed_ms,
                    finished: run_result.summary.success_count + run_result.summary.failed_count,
                    total: run_result.summary.success_count + run_result.summary.failed_count,
                    current_label: None,
                    message: format!(
                        "{} 已完成，成功 {} 只，失败 {} 只。",
                        action_label,
                        run_result.summary.success_count,
                        run_result.summary.failed_count
                    ),
                },
            ),
            Err(error) => emit_data_download_event(
                &app,
                DataDownloadEventPayload {
                    download_id: download_id.clone(),
                    phase: "failed".to_string(),
                    action: action.clone(),
                    action_label: action_label.clone(),
                    elapsed_ms: started_at.elapsed().as_millis() as u64,
                    finished: 0,
                    total: 0,
                    current_label: None,
                    message: format!("{} 失败: {}", action_label, error),
                },
            ),
        }

        result
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
pub async fn run_missing_stock_repair(
    app: tauri::AppHandle,
    request: MissingStockRepairRequest,
) -> Result<DataDownloadRunResult, String> {
    let download_id = request.download_id.trim().to_string();
    if download_id.is_empty() {
        return Err("download_id 不能为空".to_string());
    }

    let prepared = core_prepare_missing_stock_repair_run(CoreMissingStockRepairRunInput {
        source_path: request.source_path,
        token: request.token,
        threads: request.threads,
        retry_times: request.retry_times,
        limit_calls_per_min: request.limit_calls_per_min,
        include_turnover: request.include_turnover,
    })?;
    let action = prepared.action.clone();
    let action_label = prepared.action_label.clone();
    emit_data_download_event(
        &app,
        DataDownloadEventPayload {
            download_id: download_id.clone(),
            phase: "started".to_string(),
            action: action.clone(),
            action_label: action_label.clone(),
            elapsed_ms: 0,
            finished: 0,
            total: 0,
            current_label: None,
            message: format!("{action_label} 已启动，正在准备缺失股票补全。"),
        },
    );

    tauri::async_runtime::spawn_blocking(move || {
        let started_at = Instant::now();
        let result = (|| -> Result<DataDownloadRunResult, String> {
            let progress_app = app.clone();
            let progress_download_id = download_id.clone();
            let progress_action = action.clone();
            let progress_action_label = action_label.clone();
            let progress_started_at = started_at;
            let progress_cb = move |progress: CoreDownloadProgress| {
                emit_core_download_progress(
                    &progress_app,
                    progress_download_id.as_str(),
                    progress_action.as_str(),
                    progress_action_label.as_str(),
                    progress_started_at.elapsed().as_millis() as u64,
                    progress,
                );
            };

            let mut run_result =
                core_run_prepared_missing_stock_repair(&prepared, Some(&progress_cb))?;
            run_result.elapsed_ms = started_at.elapsed().as_millis() as u64;
            Ok(run_result)
        })();

        match &result {
            Ok(run_result) => emit_data_download_event(
                &app,
                DataDownloadEventPayload {
                    download_id: download_id.clone(),
                    phase: "completed".to_string(),
                    action: action.clone(),
                    action_label: action_label.clone(),
                    elapsed_ms: run_result.elapsed_ms,
                    finished: run_result.summary.success_count + run_result.summary.failed_count,
                    total: run_result.summary.success_count + run_result.summary.failed_count,
                    current_label: None,
                    message: format!(
                        "{} 已完成，成功 {} 只，失败 {} 只。",
                        action_label,
                        run_result.summary.success_count,
                        run_result.summary.failed_count
                    ),
                },
            ),
            Err(error) => emit_data_download_event(
                &app,
                DataDownloadEventPayload {
                    download_id: download_id.clone(),
                    phase: "failed".to_string(),
                    action: action.clone(),
                    action_label: action_label.clone(),
                    elapsed_ms: started_at.elapsed().as_millis() as u64,
                    finished: 0,
                    total: 0,
                    current_label: None,
                    message: format!("{} 失败: {}", action_label, error),
                },
            ),
        }

        result
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
pub async fn run_ths_concept_download(
    app: tauri::AppHandle,
    request: ThsConceptDownloadRequest,
) -> Result<DataDownloadRunResult, String> {
    let download_id = request.download_id.trim().to_string();
    if download_id.is_empty() {
        return Err("download_id 不能为空".to_string());
    }

    let prepared = core_prepare_ths_concept_download_run(CoreThsConceptDownloadRunInput {
        source_path: request.source_path,
        retry_enabled: request.retry_enabled,
        retry_times: request.retry_times,
        retry_interval_secs: request.retry_interval_secs,
        concurrent_enabled: request.concurrent_enabled,
        worker_threads: request.worker_threads,
    })?;
    let action = prepared.action.clone();
    let action_label = prepared.action_label.clone();
    emit_data_download_event(
        &app,
        DataDownloadEventPayload {
            download_id: download_id.clone(),
            phase: "started".to_string(),
            action: action.clone(),
            action_label: action_label.clone(),
            elapsed_ms: 0,
            finished: 0,
            total: 0,
            current_label: None,
            message: format!("{action_label} 已启动，正在准备概念数据下载。"),
        },
    );

    tauri::async_runtime::spawn_blocking(move || {
        let started_at = Instant::now();
        let result = (|| -> Result<DataDownloadRunResult, String> {
            let progress_app = app.clone();
            let progress_download_id = download_id.clone();
            let progress_action = action.clone();
            let progress_action_label = action_label.clone();
            let progress_started_at = started_at;
            let progress_cb = move |progress: CoreDownloadProgress| {
                emit_core_download_progress(
                    &progress_app,
                    progress_download_id.as_str(),
                    progress_action.as_str(),
                    progress_action_label.as_str(),
                    progress_started_at.elapsed().as_millis() as u64,
                    progress,
                );
            };

            let mut run_result =
                core_run_prepared_ths_concept_download(&prepared, Some(&progress_cb))?;
            run_result.elapsed_ms = started_at.elapsed().as_millis() as u64;
            Ok(run_result)
        })();

        match &result {
            Ok(run_result) => emit_data_download_event(
                &app,
                DataDownloadEventPayload {
                    download_id: download_id.clone(),
                    phase: "completed".to_string(),
                    action: action.clone(),
                    action_label: action_label.clone(),
                    elapsed_ms: run_result.elapsed_ms,
                    finished: run_result.summary.success_count + run_result.summary.failed_count,
                    total: run_result.summary.success_count + run_result.summary.failed_count,
                    current_label: None,
                    message: format!(
                        "{} 已完成，成功 {} 只，失败 {} 只。",
                        action_label,
                        run_result.summary.success_count,
                        run_result.summary.failed_count
                    ),
                },
            ),
            Err(error) => emit_data_download_event(
                &app,
                DataDownloadEventPayload {
                    download_id: download_id.clone(),
                    phase: "failed".to_string(),
                    action: action.clone(),
                    action_label: action_label.clone(),
                    elapsed_ms: started_at.elapsed().as_millis() as u64,
                    finished: 0,
                    total: 0,
                    current_label: None,
                    message: format!("{} 失败: {}", action_label, error),
                },
            ),
        }

        result
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
pub async fn run_concept_performance_repair(
    app: tauri::AppHandle,
    request: ConceptPerformanceRepairRequest,
) -> Result<DataDownloadRunResult, String> {
    let download_id = request.download_id.trim().to_string();
    if download_id.is_empty() {
        return Err("download_id 不能为空".to_string());
    }

    let prepared =
        core_prepare_concept_performance_repair_run(CoreConceptPerformanceRepairRunInput {
            source_path: request.source_path,
        })?;
    let action = prepared.action.clone();
    let action_label = prepared.action_label.clone();
    emit_data_download_event(
        &app,
        DataDownloadEventPayload {
            download_id: download_id.clone(),
            phase: "started".to_string(),
            action: action.clone(),
            action_label: action_label.clone(),
            elapsed_ms: 0,
            finished: 0,
            total: 0,
            current_label: None,
            message: format!("{action_label} 已启动，正在准备补全概念表现库。"),
        },
    );

    tauri::async_runtime::spawn_blocking(move || {
        let started_at = Instant::now();
        let result = (|| -> Result<DataDownloadRunResult, String> {
            let progress_app = app.clone();
            let progress_download_id = download_id.clone();
            let progress_action = action.clone();
            let progress_action_label = action_label.clone();
            let progress_started_at = started_at;
            let progress_cb = move |progress: CoreDownloadProgress| {
                emit_core_download_progress(
                    &progress_app,
                    progress_download_id.as_str(),
                    progress_action.as_str(),
                    progress_action_label.as_str(),
                    progress_started_at.elapsed().as_millis() as u64,
                    progress,
                );
            };

            let mut run_result =
                core_run_prepared_concept_performance_repair(&prepared, Some(&progress_cb))?;
            run_result.elapsed_ms = started_at.elapsed().as_millis() as u64;
            Ok(run_result)
        })();

        match &result {
            Ok(run_result) => emit_data_download_event(
                &app,
                DataDownloadEventPayload {
                    download_id: download_id.clone(),
                    phase: "completed".to_string(),
                    action: action.clone(),
                    action_label: action_label.clone(),
                    elapsed_ms: run_result.elapsed_ms,
                    finished: run_result.summary.success_count + run_result.summary.failed_count,
                    total: run_result.summary.success_count + run_result.summary.failed_count,
                    current_label: None,
                    message: format!(
                        "{} 已完成，写入 {} 行。",
                        action_label, run_result.summary.saved_rows
                    ),
                },
            ),
            Err(error) => emit_data_download_event(
                &app,
                DataDownloadEventPayload {
                    download_id: download_id.clone(),
                    phase: "failed".to_string(),
                    action: action.clone(),
                    action_label: action_label.clone(),
                    elapsed_ms: started_at.elapsed().as_millis() as u64,
                    finished: 0,
                    total: 0,
                    current_label: None,
                    message: format!("{} 失败: {}", action_label, error),
                },
            ),
        }

        result
    })
    .await
    .map_err(|error| error.to_string())?
}
