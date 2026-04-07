use duckdb::Connection;
use lianghua_rs::{
    crawler::SinaQuote,
    data::{
        load_stock_list, load_trade_date_list, result_db_path, score_rule_path, source_db_path,
        stock_list_path, ths_concepts_path, trade_calendar_path,
    },
    download::runner::DownloadProgress as CoreDownloadProgress,
    scoring::{TieBreakWay, build_rank_tiebreak, runner::scoring_all_to_db},
    ui_tools::{
        board_analysis::{
            BoardAnalysisGroupDetail, BoardAnalysisPageData,
            get_board_analysis_group_detail as core_get_board_analysis_group_detail,
            get_board_analysis_page as core_get_board_analysis_page,
        },
        data_download::{
            DataDownloadRunInput as CoreDataDownloadRunInput, DataDownloadRunResult,
            DataDownloadStatus, IndicatorManageDraft as CoreIndicatorManageDraft,
            IndicatorManagePageData, MissingStockRepairRunInput as CoreMissingStockRepairRunInput,
            ThsConceptDownloadRunInput as CoreThsConceptDownloadRunInput,
            get_data_download_status as core_get_data_download_status,
            get_indicator_manage_page as core_get_indicator_manage_page,
            prepare_data_download_run as core_prepare_data_download_run,
            prepare_missing_stock_repair_run as core_prepare_missing_stock_repair_run,
            prepare_ths_concept_download_run as core_prepare_ths_concept_download_run,
            run_prepared_data_download as core_run_prepared_data_download,
            run_prepared_missing_stock_repair as core_run_prepared_missing_stock_repair,
            run_prepared_ths_concept_download as core_run_prepared_ths_concept_download,
            save_indicator_manage_page as core_save_indicator_manage_page,
        },
        details::{
            StockDetailPageData, StockDetailRealtimeData,
            build_stock_detail_realtime_from_quote_map,
            get_stock_detail_page as core_get_stock_detail_page,
        },
        market_monitor::{MarketMonitorPageData, build_market_monitor_page_from_rows},
        market_simulation::{
            MarketSimulationPageData, MarketSimulationRealtimeRefreshData,
            MarketSimulationRealtimeScenarioInput, MarketSimulationScenarioInput,
            build_market_simulation_page_from_rows,
            refresh_market_simulation_realtime as core_refresh_market_simulation_realtime,
        },
        overview::{
            OverviewPageData, OverviewRow, get_rank_overview as core_get_rank_overview,
            get_rank_overview_page as core_get_rank_overview_page,
            get_rank_trade_date_options as core_get_rank_trade_date_options,
        },
        realtime::{RealtimeFetchMeta, fetch_realtime_quote_map},
        return_backtest::{
            ReturnBacktestPageData, ReturnBacktestStrengthOverviewData,
            get_return_backtest_page as core_get_return_backtest_page,
            get_return_backtest_strength_overview as core_get_return_backtest_strength_overview,
        },
        statistics::{
            StrategyStatisticsDetailData, StrategyStatisticsPageData, TriggeredStockRow,
            get_strategy_statistics_detail as core_get_strategy_statistics_detail,
            get_strategy_statistics_page as core_get_strategy_statistics_page,
            get_strategy_triggered_stocks as core_get_strategy_triggered_stocks,
        },
        stock_pick::{
            AdvancedStockPickResultData, StockPickOptionsData, StockPickResultData,
            get_stock_pick_options as core_get_stock_pick_options,
            run_advanced_stock_pick as core_run_advanced_stock_pick,
            run_concept_stock_pick as core_run_concept_stock_pick,
            run_expression_stock_pick as core_run_expression_stock_pick,
        },
        strategy_manage::{
            StrategyManagePageData, StrategyManageRuleDraft,
            add_strategy_manage_rule as core_add_strategy_manage_rule,
            check_strategy_manage_rule_draft as core_check_strategy_manage_rule_draft,
            create_strategy_manage_rule as core_create_strategy_manage_rule,
            get_strategy_manage_page as core_get_strategy_manage_page,
            remove_strategy_manage_rules as core_remove_strategy_manage_rules,
            update_strategy_manage_rule as core_update_strategy_manage_rule,
        },
        strategy_performance::{
            StrategyPerformanceHorizonViewData, StrategyPerformancePageData,
            StrategyPerformanceRuleDetail, StrategyPerformanceValidationPageData,
            StrategyPerformanceValidationDraft,
            get_strategy_performance_horizon_view as core_get_strategy_performance_horizon_view,
            get_latest_strategy_pick_cache as core_get_latest_strategy_pick_cache,
            get_or_build_strategy_pick_cache as core_get_or_build_strategy_pick_cache,
            save_manual_strategy_pick_cache as core_save_manual_strategy_pick_cache,
            get_strategy_pick_cache as core_get_strategy_pick_cache,
            get_strategy_performance_page as core_get_strategy_performance_page,
            StrategyPerformancePickCachePayload,
            get_strategy_performance_rule_detail as core_get_strategy_performance_rule_detail,
            get_strategy_performance_validation_page as core_get_strategy_performance_validation_page,
        },
        watch_observe::{
            WatchObserveRow as CoreWatchObserveRow, WatchObserveSnapshotData,
            WatchObserveStoredRow, build_watch_observe_snapshot_data, hydrate_watch_observe_rows,
            normalize_trade_date, normalize_ts_code,
        },
    },
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::io::{Read, Seek, Write};
use std::path::{Component, Path};
use std::str::FromStr;
use std::time::Instant;
use tauri::Emitter;
use tauri::Manager;
use tauri_plugin_fs::FilePath;
use tauri_plugin_fs::FsExt;
use zip::{CompressionMethod, ZipWriter, write::FileOptions};

#[cfg(target_os = "android")]
use jni::{JNIEnv, objects::JObject, sys::jboolean};

#[cfg(target_os = "android")]
use lianghua_rs::ui_tools::realtime::fetch_realtime_quote_map_async;

const MANAGED_SOURCE_IMPORT_EVENT: &str = "managed-source-import";
const DATA_DOWNLOAD_EVENT: &str = "data-download-status";
const WATCH_OBSERVE_STORAGE_FILE: &str = "watch_observe.json";
const IMPORT_BUFFER_SIZE: usize = 1024 * 1024;
const IMPORT_PROGRESS_STEP_BYTES: u64 = 32 * 1024 * 1024;

#[cfg(target_os = "android")]
#[allow(non_snake_case)]
#[no_mangle]
pub extern "system" fn Java_com_lmingyuanl_lianghua_MainActivity_initRustlsPlatformVerifier(
    mut env: JNIEnv,
    activity: JObject,
) -> jboolean {
    match rustls_platform_verifier::android::init_hosted(&mut env, activity) {
        Ok(()) => 1,
        Err(error) => {
            eprintln!("初始化 rustls-platform-verifier 失败: {error}");
            0
        }
    }
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct ManagedSourceImportEventPayload {
    import_id: String,
    target_relative_path: String,
    phase: String,
    bytes_copied: u64,
    total_bytes: Option<u64>,
    error: Option<String>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WatchObserveUpsertPayload {
    ts_code: String,
    name: Option<String>,
    added_date: Option<String>,
    tag: Option<String>,
    concept: Option<String>,
    trade_date: Option<String>,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct RankComputeDbRange {
    file_name: String,
    table_name: String,
    exists: bool,
    min_trade_date: Option<String>,
    max_trade_date: Option<String>,
    distinct_trade_dates: u64,
    row_count: u64,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct RankComputeResultContinuity {
    checked: bool,
    is_continuous: bool,
    range_start: Option<String>,
    range_end: Option<String>,
    expected_trade_dates: u64,
    actual_trade_dates: u64,
    missing_trade_dates_count: u64,
    missing_trade_dates_sample: Vec<String>,
    unexpected_trade_dates_count: u64,
    unexpected_trade_dates_sample: Vec<String>,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct RankComputeStatus {
    source_path: String,
    source_db: RankComputeDbRange,
    result_db: RankComputeDbRange,
    result_db_continuity: RankComputeResultContinuity,
    suggested_start_date: Option<String>,
    suggested_end_date: Option<String>,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct RankComputeRunResult {
    action: String,
    start_date: Option<String>,
    end_date: Option<String>,
    elapsed_ms: u64,
    status: RankComputeStatus,
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DataDownloadRequest {
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
struct MissingStockRepairRequest {
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
struct ThsConceptDownloadRequest {
    download_id: String,
    source_path: String,
    retry_enabled: bool,
    retry_times: usize,
    retry_interval_secs: u64,
    concurrent_enabled: bool,
    worker_threads: usize,
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

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct ManagedSourceExportResult {
    source_path: String,
    exported_path: String,
    file_count: u64,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct ManagedSourceFileExportResult {
    file_id: String,
    file_name: String,
    source_path: String,
    exported_path: String,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct ManagedSourceDbPreviewRow {
    ts_code: String,
    trade_date: String,
    adj_type: String,
    open: Option<f64>,
    high: Option<f64>,
    low: Option<f64>,
    close: Option<f64>,
    pre_close: Option<f64>,
    pct_chg: Option<f64>,
    vol: Option<f64>,
    amount: Option<f64>,
    tor: Option<f64>,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct ManagedSourceDbPreviewResult {
    source_path: String,
    db_path: String,
    row_count: u64,
    matched_rows: u64,
    min_trade_date: Option<String>,
    max_trade_date: Option<String>,
    rows: Vec<ManagedSourceDbPreviewRow>,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct ManagedSourceDatasetPreviewResult {
    source_path: String,
    target_path: String,
    dataset_id: String,
    dataset_label: String,
    row_count: u64,
    matched_rows: u64,
    columns: Vec<String>,
    rows: Vec<Vec<String>>,
}

#[derive(Clone, Serialize)]
struct StockLookupRow {
    ts_code: String,
    name: String,
    cnspell: Option<String>,
}

fn emit_import_event(
    app: &tauri::AppHandle,
    import_id: &Option<String>,
    target_relative_path: &str,
    phase: &str,
    bytes_copied: u64,
    total_bytes: Option<u64>,
    error: Option<String>,
) {
    let Some(import_id) = import_id.as_ref() else {
        return;
    };

    let payload = ManagedSourceImportEventPayload {
        import_id: import_id.clone(),
        target_relative_path: target_relative_path.to_string(),
        phase: phase.to_string(),
        bytes_copied,
        total_bytes,
        error,
    };

    if let Err(emit_error) = app.emit(MANAGED_SOURCE_IMPORT_EVENT, payload) {
        log::warn!("failed to emit import event: {}", emit_error);
    }
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

fn resolve_watch_observe_storage_path(
    app: &tauri::AppHandle,
) -> Result<std::path::PathBuf, String> {
    app.path()
        .resolve(
            WATCH_OBSERVE_STORAGE_FILE,
            tauri::path::BaseDirectory::AppData,
        )
        .map_err(|error| error.to_string())
}

async fn fetch_realtime_quote_map_platform(
    ts_codes: Vec<String>,
) -> Result<(HashMap<String, SinaQuote>, RealtimeFetchMeta), String> {
    #[cfg(target_os = "android")]
    {
        fetch_realtime_quote_map_async(&ts_codes).await
    }

    #[cfg(not(target_os = "android"))]
    {
        tauri::async_runtime::spawn_blocking(move || fetch_realtime_quote_map(&ts_codes))
            .await
            .map_err(|error| error.to_string())?
    }
}

async fn load_stock_detail_realtime_data(
    source_path: String,
    ts_code: String,
    chart_window_days: Option<u32>,
) -> Result<StockDetailRealtimeData, String> {
    let normalized_ts_code = ts_code.trim().to_ascii_uppercase();
    let quote_codes = vec![normalized_ts_code.clone()];
    let (quote_map, fetch_meta) = fetch_realtime_quote_map_platform(quote_codes).await?;

    tauri::async_runtime::spawn_blocking(move || {
        build_stock_detail_realtime_from_quote_map(
            source_path,
            normalized_ts_code,
            chart_window_days,
            quote_map,
            fetch_meta,
        )
    })
    .await
    .map_err(|error| error.to_string())?
}

async fn load_market_monitor_page_data(
    source_path: String,
    reference_trade_date: Option<String>,
    top_limit: Option<u32>,
) -> Result<MarketMonitorPageData, String> {
    let overview_rows = tauri::async_runtime::spawn_blocking({
        let source_path = source_path.clone();
        let reference_trade_date = reference_trade_date.clone();
        move || {
            core_get_rank_overview(
                source_path,
                reference_trade_date,
                Some(top_limit.unwrap_or(20).max(1)),
                None,
                None,
                None,
            )
        }
    })
    .await
    .map_err(|error| error.to_string())??;

    let ts_codes: Vec<String> = overview_rows
        .iter()
        .map(|row| row.ts_code.clone())
        .collect();
    let (quote_map, fetch_meta) = fetch_realtime_quote_map_platform(ts_codes).await?;
    build_market_monitor_page_from_rows(&source_path, overview_rows, quote_map, fetch_meta)
}

async fn load_market_simulation_page_data(
    source_path: String,
    reference_trade_date: Option<String>,
    top_limit: Option<u32>,
    scenarios: Vec<MarketSimulationScenarioInput>,
    sort_mode: Option<String>,
    strong_score_floor: Option<f64>,
    fetch_realtime: Option<bool>,
) -> Result<MarketSimulationPageData, String> {
    let overview_rows = tauri::async_runtime::spawn_blocking({
        let source_path = source_path.clone();
        let reference_trade_date = reference_trade_date.clone();
        move || {
            core_get_rank_overview(
                source_path,
                reference_trade_date,
                Some(top_limit.unwrap_or(50).max(1)),
                None,
                None,
                None,
            )
        }
    })
    .await
    .map_err(|error| error.to_string())??;

    let (quote_map, fetch_meta) = if fetch_realtime.unwrap_or(false) {
        let ts_codes: Vec<String> = overview_rows.iter().map(|row| row.ts_code.clone()).collect();
        fetch_realtime_quote_map_platform(ts_codes).await?
    } else {
        (
            std::collections::HashMap::new(),
            RealtimeFetchMeta {
                requested_count: 0,
                effective_count: 0,
                fetched_count: 0,
                truncated: false,
                refreshed_at: None,
                quote_trade_date: None,
                quote_time: None,
            },
        )
    };

    tauri::async_runtime::spawn_blocking(move || {
        build_market_simulation_page_from_rows(
            &source_path,
            overview_rows,
            quote_map,
            fetch_meta,
            scenarios,
            sort_mode,
            strong_score_floor,
        )
    })
    .await
    .map_err(|error| error.to_string())?
}

async fn load_market_simulation_realtime_refresh_data(
    source_path: String,
    scenarios: Vec<MarketSimulationRealtimeScenarioInput>,
) -> Result<MarketSimulationRealtimeRefreshData, String> {
    tauri::async_runtime::spawn_blocking(move || {
        core_refresh_market_simulation_realtime(source_path, scenarios)
    })
    .await
    .map_err(|error| error.to_string())?
}

async fn load_watch_observe_realtime_snapshot(
    source_path: Option<String>,
    stored_rows: Vec<WatchObserveStoredRow>,
    reference_trade_date: Option<String>,
) -> Result<WatchObserveSnapshotData, String> {
    let ts_codes: Vec<String> = stored_rows.iter().map(|row| row.ts_code.clone()).collect();
    let (quote_map, fetch_meta) = fetch_realtime_quote_map_platform(ts_codes).await?;

    tauri::async_runtime::spawn_blocking(move || {
        build_watch_observe_snapshot_data(
            source_path.as_deref(),
            &stored_rows,
            reference_trade_date,
            quote_map,
            fetch_meta,
        )
    })
    .await
    .map_err(|error| error.to_string())?
}

fn read_watch_observe_storage(
    app: &tauri::AppHandle,
) -> Result<Vec<WatchObserveStoredRow>, String> {
    let path = resolve_watch_observe_storage_path(app)?;
    if !path.exists() {
        return Ok(Vec::new());
    }

    let raw = fs::read_to_string(&path).map_err(|error| error.to_string())?;
    if raw.trim().is_empty() {
        return Ok(Vec::new());
    }

    serde_json::from_str(&raw).map_err(|error| format!("解析自选观察存储失败: {error}"))
}

fn write_watch_observe_storage(
    app: &tauri::AppHandle,
    rows: &[WatchObserveStoredRow],
) -> Result<(), String> {
    let path = resolve_watch_observe_storage_path(app)?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }

    let payload = serde_json::to_string_pretty(rows)
        .map_err(|error| format!("序列化自选观察存储失败: {error}"))?;
    fs::write(path, payload).map_err(|error| error.to_string())
}

fn merge_stored_watch_observe_row(
    incoming: WatchObserveStoredRow,
    existing: Option<&WatchObserveStoredRow>,
) -> WatchObserveStoredRow {
    let existing = existing.cloned().unwrap_or(WatchObserveStoredRow {
        ts_code: incoming.ts_code.clone(),
        name: String::new(),
        added_date: String::new(),
        tag: String::new(),
        concept: String::new(),
        trade_date: None,
    });

    WatchObserveStoredRow {
        ts_code: incoming.ts_code,
        name: if incoming.name.trim().is_empty() {
            existing.name
        } else {
            incoming.name
        },
        added_date: if incoming.added_date.trim().is_empty() {
            existing.added_date
        } else {
            incoming.added_date
        },
        tag: if incoming.tag.trim().is_empty() {
            existing.tag
        } else {
            incoming.tag
        },
        concept: if incoming.concept.trim().is_empty() {
            existing.concept
        } else {
            incoming.concept
        },
        trade_date: incoming.trade_date.or(existing.trade_date),
    }
}

fn normalize_watch_observe_upsert_payload(
    row: WatchObserveUpsertPayload,
) -> Result<WatchObserveStoredRow, String> {
    let ts_code = normalize_ts_code(&row.ts_code).ok_or_else(|| "自选代码无效".to_string())?;
    let added_date = row
        .added_date
        .as_deref()
        .and_then(normalize_trade_date)
        .unwrap_or_default();
    let trade_date = row.trade_date.as_deref().and_then(normalize_trade_date);

    Ok(WatchObserveStoredRow {
        ts_code,
        name: row.name.unwrap_or_default().trim().to_string(),
        added_date,
        tag: row.tag.unwrap_or_default().trim().to_string(),
        concept: row.concept.unwrap_or_default().trim().to_string(),
        trade_date,
    })
}

fn normalize_watch_observe_rows_payload(
    rows: Vec<WatchObserveUpsertPayload>,
) -> Result<Vec<WatchObserveStoredRow>, String> {
    rows.into_iter()
        .map(normalize_watch_observe_upsert_payload)
        .collect()
}

fn normalize_rank_compute_date(raw: &str, field_name: &str) -> Result<String, String> {
    normalize_trade_date(raw)
        .ok_or_else(|| format!("{field_name} 格式无效，应为 YYYYMMDD 或 YYYY-MM-DD"))
}

fn query_trade_date_range(
    db_path: &Path,
    file_name: &str,
    table_name: &str,
) -> Result<RankComputeDbRange, String> {
    if !db_path.exists() {
        return Ok(RankComputeDbRange {
            file_name: file_name.to_string(),
            table_name: table_name.to_string(),
            exists: false,
            min_trade_date: None,
            max_trade_date: None,
            distinct_trade_dates: 0,
            row_count: 0,
        });
    }

    let db_path_str = db_path
        .to_str()
        .ok_or_else(|| format!("{file_name} 路径不是有效 UTF-8"))?;
    let conn = Connection::open(db_path_str).map_err(|e| format!("打开 {file_name} 失败: {e}"))?;
    let table_exists = conn
        .query_row(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
            [table_name],
            |row| row.get::<_, i64>(0),
        )
        .map_err(|e| format!("检查 {file_name} 表结构失败: {e}"))?;
    if table_exists <= 0 {
        return Ok(RankComputeDbRange {
            file_name: file_name.to_string(),
            table_name: table_name.to_string(),
            exists: true,
            min_trade_date: None,
            max_trade_date: None,
            distinct_trade_dates: 0,
            row_count: 0,
        });
    }

    let sql = format!(
        "SELECT MIN(trade_date), MAX(trade_date), COUNT(DISTINCT trade_date), COUNT(*) FROM {table_name}"
    );
    let mut stmt = conn
        .prepare(&sql)
        .map_err(|e| format!("查询 {file_name} 日期范围失败: {e}"))?;
    let mut rows = stmt
        .query([])
        .map_err(|e| format!("读取 {file_name} 日期范围失败: {e}"))?;

    if let Some(row) = rows
        .next()
        .map_err(|e| format!("读取 {file_name} 日期范围行失败: {e}"))?
    {
        let min_trade_date: Option<String> = row
            .get(0)
            .map_err(|e| format!("读取 {file_name} 最小日期失败: {e}"))?;
        let max_trade_date: Option<String> = row
            .get(1)
            .map_err(|e| format!("读取 {file_name} 最大日期失败: {e}"))?;
        let distinct_trade_dates_i64: i64 = row
            .get(2)
            .map_err(|e| format!("读取 {file_name} 交易日数量失败: {e}"))?;
        let row_count_i64: i64 = row
            .get(3)
            .map_err(|e| format!("读取 {file_name} 行数失败: {e}"))?;
        return Ok(RankComputeDbRange {
            file_name: file_name.to_string(),
            table_name: table_name.to_string(),
            exists: true,
            min_trade_date,
            max_trade_date,
            distinct_trade_dates: distinct_trade_dates_i64.max(0) as u64,
            row_count: row_count_i64.max(0) as u64,
        });
    }

    Ok(RankComputeDbRange {
        file_name: file_name.to_string(),
        table_name: table_name.to_string(),
        exists: true,
        min_trade_date: None,
        max_trade_date: None,
        distinct_trade_dates: 0,
        row_count: 0,
    })
}

fn query_next_trade_date_after(
    db_path: &Path,
    after_trade_date: &str,
) -> Result<Option<String>, String> {
    if !db_path.exists() {
        return Ok(None);
    }

    let db_path_str = db_path
        .to_str()
        .ok_or_else(|| "原始库路径不是有效 UTF-8".to_string())?;
    let conn =
        Connection::open(db_path_str).map_err(|e| format!("打开 stock_data.db 失败: {e}"))?;
    let table_exists = conn
        .query_row(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'stock_data'",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map_err(|e| format!("检查 stock_data.db 表结构失败: {e}"))?;
    if table_exists <= 0 {
        return Ok(None);
    }

    conn.query_row(
        "SELECT MIN(trade_date) FROM stock_data WHERE trade_date > ?",
        [after_trade_date],
        |row| row.get::<_, Option<String>>(0),
    )
    .map_err(|e| format!("查询下一个交易日失败: {e}"))
}

fn query_distinct_trade_dates(
    db_path: &Path,
    file_name: &str,
    table_name: &str,
) -> Result<Vec<String>, String> {
    if !db_path.exists() {
        return Ok(Vec::new());
    }

    let db_path_str = db_path
        .to_str()
        .ok_or_else(|| format!("{file_name} 路径不是有效 UTF-8"))?;
    let conn = Connection::open(db_path_str).map_err(|e| format!("打开 {file_name} 失败: {e}"))?;
    let table_exists = conn
        .query_row(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
            [table_name],
            |row| row.get::<_, i64>(0),
        )
        .map_err(|e| format!("检查 {file_name} 表结构失败: {e}"))?;
    if table_exists <= 0 {
        return Ok(Vec::new());
    }

    let sql = format!(
        "SELECT DISTINCT trade_date FROM {table_name} WHERE trade_date IS NOT NULL ORDER BY trade_date"
    );
    let mut stmt = conn
        .prepare(&sql)
        .map_err(|e| format!("准备 {file_name} 交易日列表失败: {e}"))?;
    let mut rows = stmt
        .query([])
        .map_err(|e| format!("读取 {file_name} 交易日列表失败: {e}"))?;
    let mut trade_dates = Vec::new();

    while let Some(row) = rows
        .next()
        .map_err(|e| format!("遍历 {file_name} 交易日列表失败: {e}"))?
    {
        let trade_date: String = row
            .get(0)
            .map_err(|e| format!("读取 {file_name} 交易日失败: {e}"))?;
        trade_dates.push(trade_date);
    }

    Ok(trade_dates)
}

fn sample_trade_dates(values: &[String], limit: usize) -> Vec<String> {
    values.iter().take(limit).cloned().collect()
}

fn check_result_db_continuity(
    source_path: &str,
    result_db_range: &RankComputeDbRange,
) -> Result<RankComputeResultContinuity, String> {
    if !result_db_range.exists {
        return Ok(RankComputeResultContinuity {
            checked: false,
            is_continuous: false,
            range_start: None,
            range_end: None,
            expected_trade_dates: 0,
            actual_trade_dates: 0,
            missing_trade_dates_count: 0,
            missing_trade_dates_sample: Vec::new(),
            unexpected_trade_dates_count: 0,
            unexpected_trade_dates_sample: Vec::new(),
        });
    }

    let Some(range_start) = result_db_range.min_trade_date.clone() else {
        return Ok(RankComputeResultContinuity {
            checked: false,
            is_continuous: false,
            range_start: None,
            range_end: None,
            expected_trade_dates: 0,
            actual_trade_dates: 0,
            missing_trade_dates_count: 0,
            missing_trade_dates_sample: Vec::new(),
            unexpected_trade_dates_count: 0,
            unexpected_trade_dates_sample: Vec::new(),
        });
    };
    let Some(range_end) = result_db_range.max_trade_date.clone() else {
        return Ok(RankComputeResultContinuity {
            checked: false,
            is_continuous: false,
            range_start: None,
            range_end: None,
            expected_trade_dates: 0,
            actual_trade_dates: 0,
            missing_trade_dates_count: 0,
            missing_trade_dates_sample: Vec::new(),
            unexpected_trade_dates_count: 0,
            unexpected_trade_dates_sample: Vec::new(),
        });
    };

    let trade_calendar = load_trade_date_list(source_path)?;
    let expected_dates: Vec<String> = trade_calendar
        .into_iter()
        .filter(|trade_date| trade_date >= &range_start && trade_date <= &range_end)
        .collect();
    let expected_set: std::collections::HashSet<&str> =
        expected_dates.iter().map(String::as_str).collect();

    let result_db = result_db_path(source_path);
    let actual_dates =
        query_distinct_trade_dates(&result_db, "scoring_result.db", "score_summary")?;
    let actual_set: std::collections::HashSet<&str> =
        actual_dates.iter().map(String::as_str).collect();

    let missing_dates: Vec<String> = expected_dates
        .iter()
        .filter(|trade_date| !actual_set.contains(trade_date.as_str()))
        .cloned()
        .collect();
    let unexpected_dates: Vec<String> = actual_dates
        .iter()
        .filter(|trade_date| !expected_set.contains(trade_date.as_str()))
        .cloned()
        .collect();

    Ok(RankComputeResultContinuity {
        checked: true,
        is_continuous: missing_dates.is_empty() && unexpected_dates.is_empty(),
        range_start: Some(range_start),
        range_end: Some(range_end),
        expected_trade_dates: expected_dates.len() as u64,
        actual_trade_dates: actual_dates.len() as u64,
        missing_trade_dates_count: missing_dates.len() as u64,
        missing_trade_dates_sample: sample_trade_dates(&missing_dates, 8),
        unexpected_trade_dates_count: unexpected_dates.len() as u64,
        unexpected_trade_dates_sample: sample_trade_dates(&unexpected_dates, 8),
    })
}

fn get_rank_compute_status_inner(source_path: &str) -> Result<RankComputeStatus, String> {
    let source_db = source_db_path(source_path);
    let result_db = result_db_path(source_path);
    let source_db_range = query_trade_date_range(&source_db, "stock_data.db", "stock_data")?;
    let result_db_range = query_trade_date_range(&result_db, "scoring_result.db", "score_summary")?;
    let result_db_continuity = check_result_db_continuity(source_path, &result_db_range)?;

    let suggested_end_date = source_db_range.max_trade_date.clone();
    let suggested_start_date = match (
        source_db_range.min_trade_date.as_deref(),
        source_db_range.max_trade_date.as_deref(),
        result_db_range.max_trade_date.as_deref(),
    ) {
        (_, None, _) => None,
        (Some(source_min), Some(_), None) => Some(source_min.to_string()),
        (_, Some(source_max), Some(result_max)) if result_max < source_max => {
            query_next_trade_date_after(&source_db, result_max)?
                .or_else(|| Some(source_max.to_string()))
        }
        (_, Some(source_max), _) => Some(source_max.to_string()),
    };

    Ok(RankComputeStatus {
        source_path: source_path.trim().to_string(),
        source_db: source_db_range,
        result_db: result_db_range,
        result_db_continuity,
        suggested_start_date,
        suggested_end_date,
    })
}

#[tauri::command]
fn get_ranking_compute_status(source_path: String) -> Result<RankComputeStatus, String> {
    let trimmed = source_path.trim();
    if trimmed.is_empty() {
        return Err("数据目录为空，请先到数据管理页确认当前目录".to_string());
    }
    get_rank_compute_status_inner(trimmed)
}

#[tauri::command]
fn get_data_download_status(source_path: String) -> Result<DataDownloadStatus, String> {
    core_get_data_download_status(&source_path)
}

#[tauri::command]
fn get_indicator_manage_page(source_path: String) -> Result<IndicatorManagePageData, String> {
    core_get_indicator_manage_page(&source_path)
}

#[tauri::command]
fn save_indicator_manage_page(
    source_path: String,
    items: Vec<CoreIndicatorManageDraft>,
) -> Result<IndicatorManagePageData, String> {
    core_save_indicator_manage_page(&source_path, items)
}

#[tauri::command]
async fn run_data_download(
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
async fn run_missing_stock_repair(
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
async fn run_ths_concept_download(
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
async fn run_ranking_score_calculation(
    source_path: String,
    start_date: String,
    end_date: String,
) -> Result<RankComputeRunResult, String> {
    let source_path = source_path.trim().to_string();
    if source_path.is_empty() {
        return Err("数据目录为空，请先到数据管理页确认当前目录".to_string());
    }

    let start_date = normalize_rank_compute_date(&start_date, "开始日期")?;
    let end_date = normalize_rank_compute_date(&end_date, "结束日期")?;
    if start_date > end_date {
        return Err("开始日期不能晚于结束日期".to_string());
    }

    tauri::async_runtime::spawn_blocking(move || {
        let started_at = Instant::now();
        scoring_all_to_db(&source_path, "qfq", &start_date, &end_date)?;
        let status = get_rank_compute_status_inner(&source_path)?;
        Ok(RankComputeRunResult {
            action: "score".to_string(),
            start_date: Some(start_date),
            end_date: Some(end_date),
            elapsed_ms: started_at.elapsed().as_millis() as u64,
            status,
        })
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
async fn run_ranking_tiebreak_fill(source_path: String) -> Result<RankComputeRunResult, String> {
    let source_path = source_path.trim().to_string();
    if source_path.is_empty() {
        return Err("数据目录为空，请先到数据管理页确认当前目录".to_string());
    }

    tauri::async_runtime::spawn_blocking(move || {
        let started_at = Instant::now();
        let result_db = result_db_path(&source_path);
        let source_db = source_db_path(&source_path);
        let result_db_str = result_db
            .to_str()
            .ok_or_else(|| "结果库路径不是有效 UTF-8".to_string())?;
        let source_db_str = source_db
            .to_str()
            .ok_or_else(|| "原始库路径不是有效 UTF-8".to_string())?;

        build_rank_tiebreak(result_db_str, source_db_str, "qfq", TieBreakWay::KdjJ)?;

        let status = get_rank_compute_status_inner(&source_path)?;
        Ok(RankComputeRunResult {
            action: "tiebreak".to_string(),
            start_date: None,
            end_date: None,
            elapsed_ms: started_at.elapsed().as_millis() as u64,
            status,
        })
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
fn get_rank_overview(
    source_path: String,
    trade_date: Option<String>,
    limit: Option<u32>,
    board: Option<String>,
    total_mv_min: Option<f64>,
    total_mv_max: Option<f64>,
) -> Result<Vec<OverviewRow>, String> {
    core_get_rank_overview(
        source_path,
        trade_date,
        limit,
        board,
        total_mv_min,
        total_mv_max,
    )
}

#[tauri::command]
fn get_rank_trade_date_options(source_path: String) -> Result<Vec<String>, String> {
    core_get_rank_trade_date_options(source_path)
}

#[tauri::command]
fn list_stock_lookup_rows(source_path: String) -> Result<Vec<StockLookupRow>, String> {
    let trimmed = source_path.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }

    let rows = load_stock_list(trimmed)?;
    let mut out = Vec::with_capacity(rows.len());

    for cols in rows {
        let Some(ts_code) = cols.first() else {
            continue;
        };
        let Some(name) = cols.get(2) else {
            continue;
        };
        let cnspell = cols
            .get(13)
            .map(|value| value.trim())
            .filter(|value| !value.is_empty());

        let ts_code = ts_code.trim();
        let name = name.trim();
        if ts_code.is_empty() || name.is_empty() {
            continue;
        }

        out.push(StockLookupRow {
            ts_code: ts_code.to_string(),
            name: name.to_string(),
            cnspell: cnspell.map(|value| value.to_string()),
        });
    }

    Ok(out)
}

#[tauri::command]
fn get_rank_overview_page(
    source_path: String,
    rank_date: Option<String>,
    ref_date: Option<String>,
    limit: Option<u32>,
    board: Option<String>,
    total_mv_min: Option<f64>,
    total_mv_max: Option<f64>,
) -> Result<OverviewPageData, String> {
    core_get_rank_overview_page(
        source_path,
        rank_date,
        ref_date,
        limit,
        board,
        total_mv_min,
        total_mv_max,
    )
}

#[tauri::command]
fn get_stock_detail_page(
    source_path: String,
    trade_date: Option<String>,
    ts_code: String,
    chart_window_days: Option<u32>,
    prev_rank_days: Option<u32>,
) -> Result<StockDetailPageData, String> {
    core_get_stock_detail_page(
        source_path,
        trade_date,
        ts_code,
        chart_window_days,
        prev_rank_days,
    )
}

#[tauri::command]
async fn get_stock_detail_realtime(
    source_path: String,
    ts_code: String,
    chart_window_days: Option<u32>,
) -> Result<StockDetailRealtimeData, String> {
    load_stock_detail_realtime_data(source_path, ts_code, chart_window_days).await
}

#[tauri::command]
async fn get_market_monitor_page(
    source_path: String,
    reference_trade_date: Option<String>,
    top_limit: Option<u32>,
) -> Result<MarketMonitorPageData, String> {
    load_market_monitor_page_data(source_path, reference_trade_date, top_limit).await
}

#[tauri::command]
async fn get_market_simulation_page(
    source_path: String,
    reference_trade_date: Option<String>,
    top_limit: Option<u32>,
    scenarios: Vec<MarketSimulationScenarioInput>,
    sort_mode: Option<String>,
    strong_score_floor: Option<f64>,
    fetch_realtime: Option<bool>,
) -> Result<MarketSimulationPageData, String> {
    load_market_simulation_page_data(
        source_path,
        reference_trade_date,
        top_limit,
        scenarios,
        sort_mode,
        strong_score_floor,
        fetch_realtime,
    )
    .await
}

#[tauri::command]
async fn refresh_market_simulation_realtime(
    source_path: String,
    scenarios: Vec<MarketSimulationRealtimeScenarioInput>,
) -> Result<MarketSimulationRealtimeRefreshData, String> {
    load_market_simulation_realtime_refresh_data(source_path, scenarios).await
}

#[tauri::command]
async fn get_strategy_statistics_page(
    source_path: String,
    strategy_name: Option<String>,
    analysis_trade_date: Option<String>,
) -> Result<StrategyStatisticsPageData, String> {
    tauri::async_runtime::spawn_blocking(move || {
        core_get_strategy_statistics_page(source_path, strategy_name, analysis_trade_date)
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
async fn get_strategy_statistics_detail(
    source_path: String,
    strategy_name: String,
    analysis_trade_date: Option<String>,
) -> Result<StrategyStatisticsDetailData, String> {
    tauri::async_runtime::spawn_blocking(move || {
        core_get_strategy_statistics_detail(source_path, strategy_name, analysis_trade_date)
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
async fn get_strategy_triggered_stocks(
    source_path: String,
    strategy_name: String,
    analysis_trade_date: String,
) -> Result<Vec<TriggeredStockRow>, String> {
    tauri::async_runtime::spawn_blocking(move || {
        core_get_strategy_triggered_stocks(source_path, strategy_name, analysis_trade_date)
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
async fn get_strategy_performance_page(
    source_path: String,
    selected_horizon: Option<u32>,
    strong_quantile: Option<f64>,
    advantage_rule_mode: Option<String>,
    manual_rule_names: Option<Vec<String>>,
    auto_min_samples_2: Option<u32>,
    auto_min_samples_3: Option<u32>,
    auto_min_samples_5: Option<u32>,
    auto_min_samples_10: Option<u32>,
    require_win_rate_above_market: Option<bool>,
    min_pass_horizons: Option<u32>,
    min_adv_hits: Option<u32>,
    top_limit: Option<u32>,
    noisy_companion_rule_names: Option<Vec<String>>,
    selected_rule_name: Option<String>,
) -> Result<StrategyPerformancePageData, String> {
    tauri::async_runtime::spawn_blocking(move || {
        core_get_strategy_performance_page(
            source_path,
            selected_horizon,
            strong_quantile,
            advantage_rule_mode,
            manual_rule_names,
            auto_min_samples_2,
            auto_min_samples_3,
            auto_min_samples_5,
            auto_min_samples_10,
            require_win_rate_above_market,
            min_pass_horizons,
            min_adv_hits,
            top_limit,
            noisy_companion_rule_names,
            selected_rule_name,
        )
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
async fn get_strategy_pick_cache(
    source_path: String,
    selected_horizon: Option<u32>,
    strong_quantile: Option<f64>,
    advantage_rule_mode: Option<String>,
    manual_rule_names: Option<Vec<String>>,
    auto_min_samples_2: Option<u32>,
    auto_min_samples_3: Option<u32>,
    auto_min_samples_5: Option<u32>,
    auto_min_samples_10: Option<u32>,
    require_win_rate_above_market: Option<bool>,
    min_pass_horizons: Option<u32>,
    min_adv_hits: Option<u32>,
) -> Result<StrategyPerformancePickCachePayload, String> {
    tauri::async_runtime::spawn_blocking(move || {
        core_get_strategy_pick_cache(
            source_path,
            selected_horizon,
            strong_quantile,
            advantage_rule_mode,
            manual_rule_names,
            auto_min_samples_2,
            auto_min_samples_3,
            auto_min_samples_5,
            auto_min_samples_10,
            require_win_rate_above_market,
            min_pass_horizons,
            min_adv_hits,
        )
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
async fn get_or_build_strategy_pick_cache(
    source_path: String,
    selected_horizon: Option<u32>,
    strong_quantile: Option<f64>,
    advantage_rule_mode: Option<String>,
    manual_rule_names: Option<Vec<String>>,
    auto_min_samples_2: Option<u32>,
    auto_min_samples_3: Option<u32>,
    auto_min_samples_5: Option<u32>,
    auto_min_samples_10: Option<u32>,
    require_win_rate_above_market: Option<bool>,
    min_pass_horizons: Option<u32>,
    min_adv_hits: Option<u32>,
) -> Result<StrategyPerformancePickCachePayload, String> {
    tauri::async_runtime::spawn_blocking(move || {
        core_get_or_build_strategy_pick_cache(
            source_path,
            selected_horizon,
            strong_quantile,
            advantage_rule_mode,
            manual_rule_names,
            auto_min_samples_2,
            auto_min_samples_3,
            auto_min_samples_5,
            auto_min_samples_10,
            require_win_rate_above_market,
            min_pass_horizons,
            min_adv_hits,
        )
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
async fn save_manual_strategy_pick_cache(
    source_path: String,
    selected_horizon: Option<u32>,
    strong_quantile: Option<f64>,
    manual_rule_names: Vec<String>,
    auto_min_samples_2: Option<u32>,
    auto_min_samples_3: Option<u32>,
    auto_min_samples_5: Option<u32>,
    auto_min_samples_10: Option<u32>,
    require_win_rate_above_market: Option<bool>,
    min_pass_horizons: Option<u32>,
    min_adv_hits: Option<u32>,
) -> Result<StrategyPerformancePickCachePayload, String> {
    tauri::async_runtime::spawn_blocking(move || {
        core_save_manual_strategy_pick_cache(
            source_path,
            selected_horizon,
            strong_quantile,
            manual_rule_names,
            auto_min_samples_2,
            auto_min_samples_3,
            auto_min_samples_5,
            auto_min_samples_10,
            require_win_rate_above_market,
            min_pass_horizons,
            min_adv_hits,
        )
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
async fn get_latest_strategy_pick_cache(
    source_path: String,
) -> Result<StrategyPerformancePickCachePayload, String> {
    tauri::async_runtime::spawn_blocking(move || core_get_latest_strategy_pick_cache(source_path))
        .await
        .map_err(|error| error.to_string())?
}

#[tauri::command]
async fn get_strategy_performance_horizon_view(
    source_path: String,
    selected_horizon: Option<u32>,
    strong_quantile: Option<f64>,
    resolved_advantage_rule_names: Vec<String>,
    auto_min_samples_2: Option<u32>,
    auto_min_samples_3: Option<u32>,
    auto_min_samples_5: Option<u32>,
    auto_min_samples_10: Option<u32>,
    require_win_rate_above_market: Option<bool>,
    min_pass_horizons: Option<u32>,
    min_adv_hits: Option<u32>,
    noisy_companion_rule_names: Option<Vec<String>>,
) -> Result<StrategyPerformanceHorizonViewData, String> {
    tauri::async_runtime::spawn_blocking(move || {
        core_get_strategy_performance_horizon_view(
            source_path,
            selected_horizon,
            strong_quantile,
            resolved_advantage_rule_names,
            auto_min_samples_2,
            auto_min_samples_3,
            auto_min_samples_5,
            auto_min_samples_10,
            require_win_rate_above_market,
            min_pass_horizons,
            min_adv_hits,
            noisy_companion_rule_names,
        )
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
async fn get_strategy_performance_rule_detail(
    source_path: String,
    selected_horizon: Option<u32>,
    strong_quantile: Option<f64>,
    selected_rule_name: String,
) -> Result<Option<StrategyPerformanceRuleDetail>, String> {
    tauri::async_runtime::spawn_blocking(move || {
        core_get_strategy_performance_rule_detail(
            source_path,
            selected_horizon,
            strong_quantile,
            selected_rule_name,
        )
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
async fn get_strategy_performance_validation_page(
    source_path: String,
    selected_horizon: Option<u32>,
    strong_quantile: Option<f64>,
    draft: StrategyPerformanceValidationDraft,
) -> Result<StrategyPerformanceValidationPageData, String> {
    tauri::async_runtime::spawn_blocking(move || {
        core_get_strategy_performance_validation_page(
            source_path,
            selected_horizon,
            strong_quantile,
            draft,
        )
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
async fn get_return_backtest_page(
    source_path: String,
    rank_date: Option<String>,
    ref_date: Option<String>,
    top_limit: Option<u32>,
    board: Option<String>,
) -> Result<ReturnBacktestPageData, String> {
    tauri::async_runtime::spawn_blocking(move || {
        core_get_return_backtest_page(source_path, rank_date, ref_date, top_limit, board)
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
async fn get_return_backtest_strength_overview(
    source_path: String,
    holding_days: Option<u32>,
    top_limit: Option<u32>,
    board: Option<String>,
) -> Result<ReturnBacktestStrengthOverviewData, String> {
    tauri::async_runtime::spawn_blocking(move || {
        core_get_return_backtest_strength_overview(source_path, holding_days, top_limit, board)
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
async fn get_board_analysis_page(
    source_path: String,
    ref_date: Option<String>,
    weighting_range_start: Option<u32>,
    weighting_range_end: Option<u32>,
    backtest_period_days: Option<u32>,
) -> Result<BoardAnalysisPageData, String> {
    tauri::async_runtime::spawn_blocking(move || {
        core_get_board_analysis_page(
            source_path,
            ref_date,
            weighting_range_start,
            weighting_range_end,
            backtest_period_days,
        )
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
async fn get_board_analysis_group_detail(
    source_path: String,
    ref_date: Option<String>,
    weighting_range_start: Option<u32>,
    weighting_range_end: Option<u32>,
    backtest_period_days: Option<u32>,
    group_kind: String,
    metric_kind: String,
    group_name: String,
) -> Result<BoardAnalysisGroupDetail, String> {
    tauri::async_runtime::spawn_blocking(move || {
        core_get_board_analysis_group_detail(
            source_path,
            ref_date,
            weighting_range_start,
            weighting_range_end,
            backtest_period_days,
            group_kind,
            metric_kind,
            group_name,
        )
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
fn get_stock_pick_options(source_path: String) -> Result<StockPickOptionsData, String> {
    core_get_stock_pick_options(&source_path)
}

#[tauri::command]
async fn run_expression_stock_pick(
    source_path: String,
    board: Option<String>,
    reference_trade_date: Option<String>,
    lookback_periods: Option<usize>,
    scope_way: String,
    expression: String,
    consec_threshold: Option<usize>,
) -> Result<StockPickResultData, String> {
    let source_path = source_path.trim().to_string();
    if source_path.is_empty() {
        return Err("数据目录为空，请先到数据管理页确认当前目录".to_string());
    }

    tauri::async_runtime::spawn_blocking(move || {
        core_run_expression_stock_pick(
            &source_path,
            board,
            reference_trade_date,
            lookback_periods,
            scope_way,
            expression,
            consec_threshold,
        )
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
fn run_concept_stock_pick(
    source_path: String,
    board: Option<String>,
    trade_date: Option<String>,
    include_concepts: Vec<String>,
    exclude_concepts: Vec<String>,
    match_mode: String,
) -> Result<StockPickResultData, String> {
    core_run_concept_stock_pick(
        &source_path,
        board,
        trade_date,
        include_concepts,
        exclude_concepts,
        match_mode,
    )
}

#[tauri::command]
async fn run_advanced_stock_pick(
    source_path: String,
    trade_date: Option<String>,
    board: Option<String>,
    area: Option<String>,
    industry: Option<String>,
    include_concepts: Vec<String>,
    exclude_concepts: Vec<String>,
    concept_match_mode: Option<String>,
    method_key: Option<String>,
    selected_horizon: Option<u32>,
    strong_quantile: Option<f64>,
    advantage_rule_mode: Option<String>,
    manual_rule_names: Option<Vec<String>>,
    auto_min_samples_2: Option<u32>,
    auto_min_samples_3: Option<u32>,
    auto_min_samples_5: Option<u32>,
    auto_min_samples_10: Option<u32>,
    require_win_rate_above_market: Option<bool>,
    min_pass_horizons: Option<u32>,
    min_adv_hits: Option<u32>,
    top_limit: Option<u32>,
    mixed_sort_keys: Option<Vec<String>>,
    rank_max: Option<u32>,
    total_score_min: Option<f64>,
    total_score_max: Option<f64>,
    total_mv_min: Option<f64>,
    total_mv_max: Option<f64>,
    circ_mv_min: Option<f64>,
    circ_mv_max: Option<f64>,
) -> Result<AdvancedStockPickResultData, String> {
    let source_path = source_path.trim().to_string();
    if source_path.is_empty() {
        return Err("数据目录为空，请先到数据管理页确认当前目录".to_string());
    }

    tauri::async_runtime::spawn_blocking(move || {
        core_run_advanced_stock_pick(
            &source_path,
            trade_date,
            board,
            area,
            industry,
            include_concepts,
            exclude_concepts,
            concept_match_mode,
            method_key,
            selected_horizon,
            strong_quantile,
            advantage_rule_mode,
            manual_rule_names,
            auto_min_samples_2,
            auto_min_samples_3,
            auto_min_samples_5,
            auto_min_samples_10,
            require_win_rate_above_market,
            min_pass_horizons,
            min_adv_hits,
            top_limit,
            mixed_sort_keys,
            rank_max,
            total_score_min,
            total_score_max,
            total_mv_min,
            total_mv_max,
            circ_mv_min,
            circ_mv_max,
        )
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
fn get_strategy_manage_page(source_path: String) -> Result<StrategyManagePageData, String> {
    core_get_strategy_manage_page(&source_path)
}

#[tauri::command]
fn add_strategy_manage_rule(source_path: String) -> Result<StrategyManagePageData, String> {
    core_add_strategy_manage_rule(&source_path)
}

#[tauri::command]
fn check_strategy_manage_rule_draft(
    source_path: String,
    original_name: Option<String>,
    draft: StrategyManageRuleDraft,
) -> Result<String, String> {
    core_check_strategy_manage_rule_draft(&source_path, original_name.as_deref(), draft)
}

#[tauri::command]
fn create_strategy_manage_rule(
    source_path: String,
    draft: StrategyManageRuleDraft,
) -> Result<StrategyManagePageData, String> {
    core_create_strategy_manage_rule(&source_path, draft)
}

#[tauri::command]
fn remove_strategy_manage_rules(
    source_path: String,
    names: Vec<String>,
) -> Result<StrategyManagePageData, String> {
    core_remove_strategy_manage_rules(&source_path, &names)
}

#[tauri::command]
fn update_strategy_manage_rule(
    source_path: String,
    original_name: String,
    draft: StrategyManageRuleDraft,
) -> Result<StrategyManagePageData, String> {
    core_update_strategy_manage_rule(&source_path, &original_name, draft)
}

#[tauri::command]
fn export_strategy_rule_file(
    source_path: String,
    destination_dir: String,
) -> Result<String, String> {
    let destination_dir = destination_dir.trim();
    if destination_dir.is_empty() {
        return Err("empty export destination".into());
    }

    let rule_path = score_rule_path(&source_path);
    if !rule_path.exists() {
        return Err(format!("策略规则文件不存在: {}", rule_path.display()));
    }

    let destination_root = Path::new(destination_dir);
    fs::create_dir_all(destination_root).map_err(|error| error.to_string())?;
    let export_path = destination_root.join("score_rule.toml");
    fs::copy(&rule_path, &export_path).map_err(|error| error.to_string())?;
    Ok(export_path.display().to_string())
}

#[tauri::command]
fn allow_import_path(
    app: tauri::AppHandle,
    path: String,
    directory: bool,
    recursive: bool,
) -> Result<(), String> {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        return Err("empty import path".into());
    }

    let scope = app.fs_scope();
    if directory {
        scope
            .allow_directory(trimmed, recursive)
            .map_err(|error| error.to_string())?;
    } else {
        scope
            .allow_file(trimmed)
            .map_err(|error| error.to_string())?;
    }

    Ok(())
}

fn validate_target_relative_path(path: &str) -> Result<(), String> {
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
            Component::Normal(_) => {}
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err("target path contains invalid segments".into());
            }
        }
    }

    Ok(())
}

fn normalize_preview_trade_date(value: Option<String>) -> Result<Option<String>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    if trimmed.len() != 8 || !trimmed.chars().all(|ch| ch.is_ascii_digit()) {
        return Err("交易日格式无效，应为 YYYYMMDD".to_string());
    }
    Ok(Some(trimmed.to_string()))
}

fn normalize_preview_ts_code(value: Option<String>) -> Result<Option<String>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    if !trimmed
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '.' || ch == '_' || ch == '-')
    {
        return Err("股票代码格式无效".to_string());
    }
    Ok(Some(trimmed.to_ascii_uppercase()))
}

fn quote_sql_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn load_relation_columns(conn: &Connection, relation_sql: &str) -> Result<Vec<String>, String> {
    let describe_sql = format!("DESCRIBE SELECT * FROM {relation_sql}");
    let mut stmt = conn
        .prepare(&describe_sql)
        .map_err(|error| format!("读取数据集列结构失败: {error}"))?;
    let mut rows = stmt
        .query([])
        .map_err(|error| format!("查询数据集列结构失败: {error}"))?;
    let mut columns = Vec::with_capacity(32);

    while let Some(row) = rows
        .next()
        .map_err(|error| format!("读取数据集列失败: {error}"))?
    {
        let column_name: String = row
            .get(0)
            .map_err(|error| format!("读取数据集列名失败: {error}"))?;
        columns.push(column_name);
    }

    Ok(columns)
}

fn build_csv_relation_sql(path: &Path) -> Result<String, String> {
    let path_str = path
        .to_str()
        .ok_or_else(|| "CSV 路径不是有效 UTF-8".to_string())?;
    Ok(format!(
        "read_csv_auto({}, header = true, all_varchar = true)",
        quote_sql_string(path_str)
    ))
}

fn preview_managed_source_dataset_inner(
    app: tauri::AppHandle,
    source_dir: String,
    dataset_id: String,
    trade_date: Option<String>,
    ts_code: Option<String>,
    limit: usize,
) -> Result<ManagedSourceDatasetPreviewResult, String> {
    validate_target_relative_path(&source_dir)?;

    let normalized_source_dir = source_dir.trim().replace('\\', "/");
    let source_path = app
        .path()
        .resolve(&normalized_source_dir, tauri::path::BaseDirectory::AppData)
        .map_err(|error| error.to_string())?;
    let source_path_str = source_path
        .to_str()
        .ok_or_else(|| "当前应用数据路径不是有效 UTF-8".to_string())?;

    let normalized_trade_date = normalize_preview_trade_date(trade_date)?;
    let normalized_ts_code = normalize_preview_ts_code(ts_code)?;
    let normalized_dataset_id = dataset_id.trim();
    if normalized_dataset_id.is_empty() {
        return Err("dataset_id 不能为空".to_string());
    }

    let mut filter_trade_column = None;
    let mut filter_ts_code_column = None;
    let dataset_label;
    let target_path;
    let relation_sql;
    let all_columns;
    let selected_columns;
    let order_by_sql;

    match normalized_dataset_id {
        "stock-data-base" | "stock-data-indicators" => {
            let db_path = source_db_path(source_path_str);
            if !db_path.exists() {
                return Err(format!("原始行情库不存在: {}", db_path.display()));
            }
            let db_path_str = db_path
                .to_str()
                .ok_or_else(|| "stock_data.db 路径不是有效 UTF-8".to_string())?;
            let conn = Connection::open(db_path_str)
                .map_err(|error| format!("打开 stock_data.db 失败: {error}"))?;
            let relation = quote_ident("stock_data");
            let columns = load_relation_columns(&conn, &relation)?;
            let base_columns = [
                "ts_code",
                "trade_date",
                "adj_type",
                "open",
                "high",
                "low",
                "close",
                "pre_close",
                "change",
                "pct_chg",
                "vol",
                "amount",
                "tor",
            ];
            let mut selected = base_columns
                .iter()
                .filter(|column| columns.iter().any(|item| item.eq_ignore_ascii_case(column)))
                .map(|column| (*column).to_string())
                .collect::<Vec<_>>();
            for column in &columns {
                if base_columns
                    .iter()
                    .any(|item| item.eq_ignore_ascii_case(column))
                {
                    continue;
                }
                selected.push(column.clone());
            }
            filter_trade_column = Some("trade_date");
            filter_ts_code_column = Some("ts_code");
            dataset_label = "原始行情库";
            target_path = db_path.display().to_string();
            relation_sql = relation;
            all_columns = columns;
            selected_columns = selected;
            order_by_sql = "trade_date DESC, ts_code ASC";
            drop(conn);
        }
        "score-summary" => {
            let db_path = result_db_path(source_path_str);
            if !db_path.exists() {
                return Err(format!("结果库不存在: {}", db_path.display()));
            }
            let db_path_str = db_path
                .to_str()
                .ok_or_else(|| "scoring_result.db 路径不是有效 UTF-8".to_string())?;
            let conn = Connection::open(db_path_str)
                .map_err(|error| format!("打开 scoring_result.db 失败: {error}"))?;
            let relation = quote_ident("score_summary");
            let columns = load_relation_columns(&conn, &relation)?;
            filter_trade_column = Some("trade_date");
            filter_ts_code_column = Some("ts_code");
            dataset_label = "结果库 score_summary";
            target_path = db_path.display().to_string();
            relation_sql = relation;
            selected_columns = columns.clone();
            all_columns = columns;
            order_by_sql = "trade_date DESC, ts_code ASC";
            drop(conn);
        }
        "score-details" => {
            let db_path = result_db_path(source_path_str);
            if !db_path.exists() {
                return Err(format!("结果库不存在: {}", db_path.display()));
            }
            let db_path_str = db_path
                .to_str()
                .ok_or_else(|| "scoring_result.db 路径不是有效 UTF-8".to_string())?;
            let conn = Connection::open(db_path_str)
                .map_err(|error| format!("打开 scoring_result.db 失败: {error}"))?;
            let relation = quote_ident("score_details");
            let columns = load_relation_columns(&conn, &relation)?;
            filter_trade_column = Some("trade_date");
            filter_ts_code_column = Some("ts_code");
            dataset_label = "结果库 score_details";
            target_path = db_path.display().to_string();
            relation_sql = relation;
            selected_columns = columns.clone();
            all_columns = columns;
            order_by_sql = "trade_date DESC, ts_code ASC, rule_name ASC";
            drop(conn);
        }
        "stock-list-csv" => {
            let csv_path = stock_list_path(source_path_str);
            if !csv_path.exists() {
                return Err(format!("stock_list.csv 不存在: {}", csv_path.display()));
            }
            let conn = Connection::open_in_memory()
                .map_err(|error| format!("打开内存查询连接失败: {error}"))?;
            let relation = build_csv_relation_sql(&csv_path)?;
            let columns = load_relation_columns(&conn, &relation)?;
            filter_trade_column = Some("trade_date");
            filter_ts_code_column = Some("ts_code");
            dataset_label = "股票列表 CSV";
            target_path = csv_path.display().to_string();
            relation_sql = relation;
            selected_columns = columns.clone();
            all_columns = columns;
            order_by_sql = "trade_date DESC, ts_code ASC";
            drop(conn);
        }
        "trade-calendar-csv" => {
            let csv_path = trade_calendar_path(source_path_str);
            if !csv_path.exists() {
                return Err(format!("trade_calendar.csv 不存在: {}", csv_path.display()));
            }
            let conn = Connection::open_in_memory()
                .map_err(|error| format!("打开内存查询连接失败: {error}"))?;
            let relation = build_csv_relation_sql(&csv_path)?;
            let columns = load_relation_columns(&conn, &relation)?;
            filter_trade_column = Some("cal_date");
            dataset_label = "交易日历 CSV";
            target_path = csv_path.display().to_string();
            relation_sql = relation;
            selected_columns = columns.clone();
            all_columns = columns;
            order_by_sql = "cal_date DESC";
            drop(conn);
        }
        "stock-concepts-csv" => {
            let csv_path = ths_concepts_path(source_path_str);
            if !csv_path.exists() {
                return Err(format!("stock_concepts.csv 不存在: {}", csv_path.display()));
            }
            let conn = Connection::open_in_memory()
                .map_err(|error| format!("打开内存查询连接失败: {error}"))?;
            let relation = build_csv_relation_sql(&csv_path)?;
            let columns = load_relation_columns(&conn, &relation)?;
            filter_ts_code_column = Some("ts_code");
            dataset_label = "同花顺概念 CSV";
            target_path = csv_path.display().to_string();
            relation_sql = relation;
            selected_columns = columns.clone();
            all_columns = columns;
            order_by_sql = "ts_code ASC";
            drop(conn);
        }
        _ => {
            return Err(format!("不支持的数据集: {normalized_dataset_id}"));
        }
    }

    if selected_columns.is_empty() {
        return Err(format!("数据集 {normalized_dataset_id} 没有可展示列"));
    }

    let uses_memory_conn = normalized_dataset_id.ends_with("-csv");
    let conn = if uses_memory_conn {
        Connection::open_in_memory().map_err(|error| format!("打开内存查询连接失败: {error}"))?
    } else if normalized_dataset_id.starts_with("stock-data-") {
        let db_path = source_db_path(source_path_str);
        let db_path_str = db_path
            .to_str()
            .ok_or_else(|| "stock_data.db 路径不是有效 UTF-8".to_string())?;
        Connection::open(db_path_str)
            .map_err(|error| format!("打开 stock_data.db 失败: {error}"))?
    } else {
        let db_path = result_db_path(source_path_str);
        let db_path_str = db_path
            .to_str()
            .ok_or_else(|| "scoring_result.db 路径不是有效 UTF-8".to_string())?;
        Connection::open(db_path_str)
            .map_err(|error| format!("打开 scoring_result.db 失败: {error}"))?
    };

    let mut where_clauses = Vec::with_capacity(2);
    if let (Some(column), Some(value)) = (filter_trade_column, normalized_trade_date.as_deref()) {
        if all_columns
            .iter()
            .any(|item| item.eq_ignore_ascii_case(column))
        {
            where_clauses.push(format!(
                "{} = {}",
                quote_ident(column),
                quote_sql_string(value)
            ));
        }
    }
    if let (Some(column), Some(value)) = (filter_ts_code_column, normalized_ts_code.as_deref()) {
        if all_columns
            .iter()
            .any(|item| item.eq_ignore_ascii_case(column))
        {
            where_clauses.push(format!(
                "{} = {}",
                quote_ident(column),
                quote_sql_string(value)
            ));
        }
    }
    let where_sql = if where_clauses.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", where_clauses.join(" AND "))
    };

    let count_sql = format!("SELECT COUNT(*) FROM {relation_sql}");
    let row_count_i64 = conn
        .query_row(&count_sql, [], |row| row.get::<_, i64>(0))
        .map_err(|error| format!("读取数据集总行数失败: {error}"))?;

    let matched_sql = format!("SELECT COUNT(*) FROM {relation_sql}{where_sql}");
    let matched_rows_i64 = conn
        .query_row(&matched_sql, [], |row| row.get::<_, i64>(0))
        .map_err(|error| format!("读取数据集筛选行数失败: {error}"))?;

    let select_sql = selected_columns
        .iter()
        .map(|column| {
            format!(
                "COALESCE(CAST({} AS VARCHAR), '') AS {}",
                quote_ident(column),
                quote_ident(column)
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let preview_sql = format!(
        "SELECT {select_sql} FROM {relation_sql}{where_sql} ORDER BY {order_by_sql} LIMIT {limit}"
    );
    let mut stmt = conn
        .prepare(&preview_sql)
        .map_err(|error| format!("准备数据集预览查询失败: {error}"))?;
    let mut rows = stmt
        .query([])
        .map_err(|error| format!("执行数据集预览查询失败: {error}"))?;
    let mut preview_rows = Vec::with_capacity(limit);

    while let Some(row) = rows
        .next()
        .map_err(|error| format!("读取数据集预览行失败: {error}"))?
    {
        let mut values = Vec::with_capacity(selected_columns.len());
        for index in 0..selected_columns.len() {
            let value: Option<String> = row
                .get(index)
                .map_err(|error| format!("读取预览字段失败: {error}"))?;
            values.push(value.unwrap_or_default());
        }
        preview_rows.push(values);
    }

    Ok(ManagedSourceDatasetPreviewResult {
        source_path: source_path.display().to_string(),
        target_path,
        dataset_id: normalized_dataset_id.to_string(),
        dataset_label: dataset_label.to_string(),
        row_count: row_count_i64.max(0) as u64,
        matched_rows: matched_rows_i64.max(0) as u64,
        columns: selected_columns,
        rows: preview_rows,
    })
}

fn copy_directory_recursive(source: &Path, target: &Path) -> Result<u64, String> {
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

fn append_directory_to_zip<W: Write + Seek>(
    zip_writer: &mut ZipWriter<W>,
    source_root: &Path,
    current_dir: &Path,
    archive_root: &str,
) -> Result<u64, String> {
    let file_options = FileOptions::default().compression_method(CompressionMethod::Deflated);
    let mut file_count = 0u64;

    for entry in fs::read_dir(current_dir).map_err(|error| error.to_string())? {
        let entry = entry.map_err(|error| error.to_string())?;
        let entry_path = entry.path();
        let entry_type = entry.file_type().map_err(|error| error.to_string())?;
        let relative_path = entry_path
            .strip_prefix(source_root)
            .map_err(|error| error.to_string())?;
        let archive_path = Path::new(archive_root).join(relative_path);
        let mut archive_name = archive_path.to_string_lossy().replace('\\', "/");

        if entry_type.is_dir() {
            if !archive_name.ends_with('/') {
                archive_name.push('/');
            }
            zip_writer
                .add_directory(archive_name, file_options)
                .map_err(|error| error.to_string())?;
            file_count +=
                append_directory_to_zip(zip_writer, source_root, &entry_path, archive_root)?;
            continue;
        }

        if entry_type.is_file() {
            zip_writer
                .start_file(archive_name, file_options)
                .map_err(|error| error.to_string())?;
            let mut source_file = fs::File::open(&entry_path).map_err(|error| error.to_string())?;
            std::io::copy(&mut source_file, zip_writer).map_err(|error| error.to_string())?;
            file_count += 1;
        }
    }

    Ok(file_count)
}

fn preview_managed_source_stock_data_inner(
    app: tauri::AppHandle,
    source_dir: String,
    trade_date: Option<String>,
    ts_code: Option<String>,
    limit: usize,
) -> Result<ManagedSourceDbPreviewResult, String> {
    validate_target_relative_path(&source_dir)?;

    let normalized_source_dir = source_dir.trim().replace('\\', "/");
    let source_path = app
        .path()
        .resolve(&normalized_source_dir, tauri::path::BaseDirectory::AppData)
        .map_err(|error| error.to_string())?;
    let source_path_str = source_path
        .to_str()
        .ok_or_else(|| "当前应用数据路径不是有效 UTF-8".to_string())?;
    let db_path = source_db_path(source_path_str);
    if !db_path.exists() {
        return Err(format!("原始行情库不存在: {}", db_path.display()));
    }

    let db_path_str = db_path
        .to_str()
        .ok_or_else(|| "stock_data.db 路径不是有效 UTF-8".to_string())?;
    let conn = Connection::open(db_path_str)
        .map_err(|error| format!("打开 stock_data.db 失败: {error}"))?;
    let table_exists = conn
        .query_row(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'stock_data'",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map_err(|error| format!("检查 stock_data 表结构失败: {error}"))?;
    if table_exists <= 0 {
        return Err("stock_data 表不存在".to_string());
    }

    let normalized_trade_date = normalize_preview_trade_date(trade_date)?;
    let normalized_ts_code = normalize_preview_ts_code(ts_code)?;
    let mut where_clauses = Vec::with_capacity(2);
    if let Some(value) = normalized_trade_date.as_deref() {
        where_clauses.push(format!("trade_date = {}", quote_sql_string(value)));
    }
    if let Some(value) = normalized_ts_code.as_deref() {
        where_clauses.push(format!("ts_code = {}", quote_sql_string(value)));
    }
    let where_sql = if where_clauses.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", where_clauses.join(" AND "))
    };

    let row_count_i64 = conn
        .query_row("SELECT COUNT(*) FROM stock_data", [], |row| {
            row.get::<_, i64>(0)
        })
        .map_err(|error| format!("读取 stock_data 总行数失败: {error}"))?;

    let summary_sql =
        format!("SELECT COUNT(*), MIN(trade_date), MAX(trade_date) FROM stock_data{where_sql}");
    let (matched_rows_i64, min_trade_date, max_trade_date) = conn
        .query_row(&summary_sql, [], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<String>>(2)?,
            ))
        })
        .map_err(|error| format!("读取预览范围失败: {error}"))?;

    let preview_sql = format!(
        "SELECT \
            ts_code, trade_date, adj_type, \
            CAST(open AS DOUBLE), CAST(high AS DOUBLE), CAST(low AS DOUBLE), \
            CAST(close AS DOUBLE), CAST(pre_close AS DOUBLE), CAST(pct_chg AS DOUBLE), \
            CAST(vol AS DOUBLE), CAST(amount AS DOUBLE), CAST(tor AS DOUBLE) \
        FROM stock_data{where_sql} \
        ORDER BY trade_date DESC, ts_code ASC \
        LIMIT {limit}"
    );
    let mut stmt = conn
        .prepare(&preview_sql)
        .map_err(|error| format!("准备预览查询失败: {error}"))?;
    let mut rows = stmt
        .query([])
        .map_err(|error| format!("执行预览查询失败: {error}"))?;
    let mut preview_rows = Vec::with_capacity(limit);

    while let Some(row) = rows
        .next()
        .map_err(|error| format!("读取预览行失败: {error}"))?
    {
        preview_rows.push(ManagedSourceDbPreviewRow {
            ts_code: row
                .get(0)
                .map_err(|error| format!("读取 ts_code 失败: {error}"))?,
            trade_date: row
                .get(1)
                .map_err(|error| format!("读取 trade_date 失败: {error}"))?,
            adj_type: row
                .get(2)
                .map_err(|error| format!("读取 adj_type 失败: {error}"))?,
            open: row
                .get(3)
                .map_err(|error| format!("读取 open 失败: {error}"))?,
            high: row
                .get(4)
                .map_err(|error| format!("读取 high 失败: {error}"))?,
            low: row
                .get(5)
                .map_err(|error| format!("读取 low 失败: {error}"))?,
            close: row
                .get(6)
                .map_err(|error| format!("读取 close 失败: {error}"))?,
            pre_close: row
                .get(7)
                .map_err(|error| format!("读取 pre_close 失败: {error}"))?,
            pct_chg: row
                .get(8)
                .map_err(|error| format!("读取 pct_chg 失败: {error}"))?,
            vol: row
                .get(9)
                .map_err(|error| format!("读取 vol 失败: {error}"))?,
            amount: row
                .get(10)
                .map_err(|error| format!("读取 amount 失败: {error}"))?,
            tor: row
                .get(11)
                .map_err(|error| format!("读取 tor 失败: {error}"))?,
        });
    }

    Ok(ManagedSourceDbPreviewResult {
        source_path: source_path.display().to_string(),
        db_path: db_path.display().to_string(),
        row_count: row_count_i64.max(0) as u64,
        matched_rows: matched_rows_i64.max(0) as u64,
        min_trade_date,
        max_trade_date,
        rows: preview_rows,
    })
}

fn copy_import_file_to_appdata_inner(
    app: tauri::AppHandle,
    source_path: String,
    target_relative_path: String,
    import_id: Option<String>,
) -> Result<(), String> {
    let source_path = source_path.trim();
    if source_path.is_empty() {
        return Err("empty import source path".into());
    }
    validate_target_relative_path(&target_relative_path)?;

    let target_path = app
        .path()
        .resolve(&target_relative_path, tauri::path::BaseDirectory::AppData)
        .map_err(|error| error.to_string())?;

    if let Some(parent) = target_path.parent() {
        std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }

    let mut open_options = tauri_plugin_fs::OpenOptions::new();
    open_options.read(true);
    let source_file_path = FilePath::from_str(source_path).map_err(|error| error.to_string())?;
    let total_bytes = source_file_path
        .clone()
        .into_path()
        .ok()
        .and_then(|path| std::fs::metadata(path).ok())
        .map(|metadata| metadata.len());
    let mut source = app
        .fs()
        .open(source_file_path, open_options)
        .map_err(|error| error.to_string())?;
    let mut target = std::fs::File::create(&target_path).map_err(|error| error.to_string())?;
    let mut buffer = vec![0u8; IMPORT_BUFFER_SIZE];
    let mut bytes_copied = 0u64;
    let mut next_progress_threshold = IMPORT_PROGRESS_STEP_BYTES;

    emit_import_event(
        &app,
        &import_id,
        &target_relative_path,
        "started",
        0,
        total_bytes,
        None,
    );

    loop {
        let read_bytes = source
            .read(&mut buffer)
            .map_err(|error| error.to_string())?;
        if read_bytes == 0 {
            break;
        }

        target
            .write_all(&buffer[..read_bytes])
            .map_err(|error| error.to_string())?;
        bytes_copied += read_bytes as u64;

        if bytes_copied >= next_progress_threshold {
            emit_import_event(
                &app,
                &import_id,
                &target_relative_path,
                "progress",
                bytes_copied,
                total_bytes,
                None,
            );
            next_progress_threshold = bytes_copied.saturating_add(IMPORT_PROGRESS_STEP_BYTES);
        }
    }

    target.flush().map_err(|error| error.to_string())?;
    target.sync_all().map_err(|error| error.to_string())?;

    emit_import_event(
        &app,
        &import_id,
        &target_relative_path,
        "completed",
        bytes_copied,
        total_bytes,
        None,
    );

    Ok(())
}

#[tauri::command]
async fn copy_import_file_to_appdata(
    app: tauri::AppHandle,
    source_path: String,
    target_relative_path: String,
    import_id: Option<String>,
) -> Result<(), String> {
    log::info!("starting import copy to {}", target_relative_path);
    let target_for_log = target_relative_path.clone();
    let import_id_for_error = import_id.clone();
    let app_for_copy = app.clone();
    let result = tauri::async_runtime::spawn_blocking(move || {
        copy_import_file_to_appdata_inner(
            app_for_copy,
            source_path,
            target_relative_path,
            import_id,
        )
    })
    .await
    .map_err(|error| error.to_string())?;

    if let Err(error) = &result {
        log::error!("import copy failed for {}: {}", target_for_log, error);
        emit_import_event(
            &app,
            &import_id_for_error,
            &target_for_log,
            "failed",
            0,
            None,
            Some(error.clone()),
        );
    } else {
        log::info!("finished import copy to {}", target_for_log);
    }

    result
}

#[tauri::command]
async fn preview_managed_source_stock_data(
    app: tauri::AppHandle,
    source_dir: String,
    trade_date: Option<String>,
    ts_code: Option<String>,
    limit: Option<usize>,
) -> Result<ManagedSourceDbPreviewResult, String> {
    let limit = limit.unwrap_or(100).clamp(20, 500);
    tauri::async_runtime::spawn_blocking(move || {
        preview_managed_source_stock_data_inner(app, source_dir, trade_date, ts_code, limit)
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
async fn preview_managed_source_dataset(
    app: tauri::AppHandle,
    source_dir: String,
    dataset_id: String,
    trade_date: Option<String>,
    ts_code: Option<String>,
    limit: Option<usize>,
) -> Result<ManagedSourceDatasetPreviewResult, String> {
    let limit = limit.unwrap_or(100).clamp(20, 500);
    tauri::async_runtime::spawn_blocking(move || {
        preview_managed_source_dataset_inner(
            app, source_dir, dataset_id, trade_date, ts_code, limit,
        )
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
fn export_managed_source_directory(
    app: tauri::AppHandle,
    source_dir: String,
    destination_dir: String,
) -> Result<ManagedSourceExportResult, String> {
    validate_target_relative_path(&source_dir)?;

    let normalized_source_dir = source_dir.trim().replace('\\', "/");
    let destination_dir = destination_dir.trim();
    if destination_dir.is_empty() {
        return Err("empty export destination".into());
    }

    let source_path = app
        .path()
        .resolve(&normalized_source_dir, tauri::path::BaseDirectory::AppData)
        .map_err(|error| error.to_string())?;

    if !source_path.exists() {
        return Err(format!("当前应用数据目录不存在: {}", source_path.display()));
    }

    if !source_path.is_dir() {
        return Err(format!(
            "当前应用数据目录不是文件夹: {}",
            source_path.display()
        ));
    }

    let export_root = normalized_source_dir
        .split('/')
        .filter(|segment| !segment.trim().is_empty())
        .fold(
            std::path::PathBuf::from(destination_dir),
            |current, segment| current.join(segment),
        );

    if export_root == source_path || export_root.starts_with(&source_path) {
        return Err("导出目录不能选在当前应用数据目录内部".into());
    }

    let file_count = copy_directory_recursive(&source_path, &export_root)?;

    Ok(ManagedSourceExportResult {
        source_path: source_path.display().to_string(),
        exported_path: export_root.display().to_string(),
        file_count,
    })
}

#[tauri::command]
fn export_managed_source_directory_mobile(
    app: tauri::AppHandle,
    source_dir: String,
    destination_file: String,
) -> Result<ManagedSourceExportResult, String> {
    validate_target_relative_path(&source_dir)?;

    let normalized_source_dir = source_dir.trim().replace('\\', "/");
    let destination_file = destination_file.trim();
    if destination_file.is_empty() {
        return Err("empty export destination file".into());
    }

    let source_path = app
        .path()
        .resolve(&normalized_source_dir, tauri::path::BaseDirectory::AppData)
        .map_err(|error| error.to_string())?;

    if !source_path.exists() {
        return Err(format!("当前应用数据目录不存在: {}", source_path.display()));
    }

    if !source_path.is_dir() {
        return Err(format!(
            "当前应用数据目录不是文件夹: {}",
            source_path.display()
        ));
    }

    let mut open_options = tauri_plugin_fs::OpenOptions::new();
    open_options.write(true).truncate(true).create(true);
    let destination_file =
        FilePath::from_str(destination_file).map_err(|error| error.to_string())?;
    let destination_label = destination_file.to_string();
    let target_file = app
        .fs()
        .open(destination_file, open_options)
        .map_err(|error| error.to_string())?;
    let mut zip_writer = ZipWriter::new(target_file);
    let archive_root = normalized_source_dir.trim_matches('/');
    let file_count = append_directory_to_zip(
        &mut zip_writer,
        &source_path,
        &source_path,
        if archive_root.is_empty() {
            "source"
        } else {
            archive_root
        },
    )?;
    zip_writer.finish().map_err(|error| error.to_string())?;

    Ok(ManagedSourceExportResult {
        source_path: source_path.display().to_string(),
        exported_path: destination_label,
        file_count,
    })
}

fn managed_source_file_name(file_id: &str) -> Option<&'static str> {
    match file_id {
        "source-db" => Some("stock_data.db"),
        "app-config" => Some("config.toml"),
        "stock-list" => Some("stock_list.csv"),
        "trade-calendar" => Some("trade_calendar.csv"),
        "result-db" => Some("scoring_result.db"),
        "score-rule" => Some("score_rule.toml"),
        "indicator-config" => Some("ind.toml"),
        "ths-concepts" => Some("stock_concepts.csv"),
        _ => None,
    }
}

fn export_managed_source_file_inner(
    app: tauri::AppHandle,
    source_dir: String,
    file_id: String,
    destination_file: String,
) -> Result<ManagedSourceFileExportResult, String> {
    validate_target_relative_path(&source_dir)?;

    let normalized_source_dir = source_dir.trim().replace('\\', "/");
    let normalized_file_id = file_id.trim();
    let destination_file = destination_file.trim();
    if destination_file.is_empty() {
        return Err("empty export destination file".into());
    }

    let file_name = managed_source_file_name(normalized_file_id)
        .ok_or_else(|| format!("未知文件项: {normalized_file_id}"))?;
    let target_relative_path = if normalized_source_dir.is_empty() {
        file_name.to_string()
    } else {
        format!("{normalized_source_dir}/{file_name}")
    };

    let source_path = app
        .path()
        .resolve(&target_relative_path, tauri::path::BaseDirectory::AppData)
        .map_err(|error| error.to_string())?;

    if !source_path.exists() {
        return Err(format!(
            "当前应用数据目录缺少文件: {}",
            source_path.display()
        ));
    }

    if !source_path.is_file() {
        return Err(format!(
            "当前应用数据目录目标不是文件: {}",
            source_path.display()
        ));
    }

    let mut source = std::fs::File::open(&source_path).map_err(|error| error.to_string())?;
    let mut open_options = tauri_plugin_fs::OpenOptions::new();
    open_options.write(true).truncate(true).create(true);
    let destination_file =
        FilePath::from_str(destination_file).map_err(|error| error.to_string())?;
    let destination_label = destination_file.to_string();
    let mut target = app
        .fs()
        .open(destination_file, open_options)
        .map_err(|error| error.to_string())?;

    let mut buffer = vec![0u8; IMPORT_BUFFER_SIZE];
    loop {
        let read_bytes = source
            .read(&mut buffer)
            .map_err(|error| error.to_string())?;
        if read_bytes == 0 {
            break;
        }
        target
            .write_all(&buffer[..read_bytes])
            .map_err(|error| error.to_string())?;
    }

    target.flush().map_err(|error| error.to_string())?;

    Ok(ManagedSourceFileExportResult {
        file_id: normalized_file_id.to_string(),
        file_name: file_name.to_string(),
        source_path: source_path.display().to_string(),
        exported_path: destination_label,
    })
}

#[tauri::command]
async fn export_managed_source_file(
    app: tauri::AppHandle,
    source_dir: String,
    file_id: String,
    destination_file: String,
) -> Result<ManagedSourceFileExportResult, String> {
    tauri::async_runtime::spawn_blocking(move || {
        export_managed_source_file_inner(app, source_dir, file_id, destination_file)
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
fn list_watch_observe_rows(
    app: tauri::AppHandle,
    source_path: Option<String>,
    reference_trade_date: Option<String>,
    rows: Option<Vec<WatchObserveUpsertPayload>>,
) -> Result<Vec<CoreWatchObserveRow>, String> {
    let stored_rows = match rows {
        Some(rows) => normalize_watch_observe_rows_payload(rows)?,
        None => read_watch_observe_storage(&app)?,
    };
    hydrate_watch_observe_rows(source_path.as_deref(), &stored_rows, reference_trade_date)
}

#[tauri::command]
async fn refresh_watch_observe_rows(
    app: tauri::AppHandle,
    source_path: Option<String>,
    reference_trade_date: Option<String>,
    rows: Option<Vec<WatchObserveUpsertPayload>>,
) -> Result<WatchObserveSnapshotData, String> {
    let stored_rows = match rows {
        Some(rows) => normalize_watch_observe_rows_payload(rows)?,
        None => read_watch_observe_storage(&app)?,
    };
    load_watch_observe_realtime_snapshot(source_path, stored_rows, reference_trade_date).await
}

#[tauri::command]
fn upsert_watch_observe_row(
    app: tauri::AppHandle,
    source_path: Option<String>,
    row: WatchObserveUpsertPayload,
) -> Result<Vec<CoreWatchObserveRow>, String> {
    let incoming = normalize_watch_observe_upsert_payload(row)?;
    let mut rows = read_watch_observe_storage(&app)?;
    if let Some(existing_index) = rows
        .iter()
        .position(|item| item.ts_code == incoming.ts_code)
    {
        let existing = rows.get(existing_index).cloned();
        let merged = merge_stored_watch_observe_row(incoming, existing.as_ref());
        rows[existing_index] = merged;
    } else {
        rows.insert(0, incoming);
    }

    write_watch_observe_storage(&app, &rows)?;
    hydrate_watch_observe_rows(source_path.as_deref(), &rows, None)
}

#[tauri::command]
fn merge_watch_observe_rows(
    app: tauri::AppHandle,
    source_path: Option<String>,
    rows: Vec<WatchObserveUpsertPayload>,
) -> Result<Vec<CoreWatchObserveRow>, String> {
    let mut saved_rows = read_watch_observe_storage(&app)?;

    for incoming in rows {
        let normalized = normalize_watch_observe_upsert_payload(incoming)?;
        if let Some(existing_index) = saved_rows
            .iter()
            .position(|item| item.ts_code == normalized.ts_code)
        {
            let existing = saved_rows.get(existing_index).cloned();
            saved_rows[existing_index] =
                merge_stored_watch_observe_row(normalized, existing.as_ref());
        } else {
            saved_rows.push(normalized);
        }
    }

    write_watch_observe_storage(&app, &saved_rows)?;
    hydrate_watch_observe_rows(source_path.as_deref(), &saved_rows, None)
}

#[tauri::command]
fn update_watch_observe_tag(
    app: tauri::AppHandle,
    source_path: Option<String>,
    ts_code: String,
    tag: String,
) -> Result<Vec<CoreWatchObserveRow>, String> {
    let normalized_ts_code =
        normalize_ts_code(&ts_code).ok_or_else(|| "自选代码无效".to_string())?;
    let mut rows = read_watch_observe_storage(&app)?;
    let Some(existing_row) = rows
        .iter_mut()
        .find(|item| item.ts_code == normalized_ts_code)
    else {
        return Err(format!("未找到自选记录: {normalized_ts_code}"));
    };

    existing_row.tag = tag.trim().to_string();
    write_watch_observe_storage(&app, &rows)?;
    hydrate_watch_observe_rows(source_path.as_deref(), &rows, None)
}

#[tauri::command]
fn remove_watch_observe_rows(
    app: tauri::AppHandle,
    source_path: Option<String>,
    ts_codes: Vec<String>,
) -> Result<Vec<CoreWatchObserveRow>, String> {
    let normalized_codes: Vec<String> = ts_codes
        .into_iter()
        .filter_map(|value| normalize_ts_code(&value))
        .collect();
    let mut rows = read_watch_observe_storage(&app)?;
    rows.retain(|item| {
        !normalized_codes
            .iter()
            .any(|ts_code| ts_code == &item.ts_code)
    });
    write_watch_observe_storage(&app, &rows)?;
    hydrate_watch_observe_rows(source_path.as_deref(), &rows, None)
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let builder = tauri::Builder::default().setup(|app| {
        if cfg!(debug_assertions) {
            app.handle().plugin(
                tauri_plugin_log::Builder::default()
                    .level(log::LevelFilter::Info)
                    .build(),
            )?;
        }
        Ok(())
    });

    let builder = builder
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_fs::init());

    builder
        .invoke_handler(tauri::generate_handler![
            allow_import_path,
            copy_import_file_to_appdata,
            preview_managed_source_stock_data,
            preview_managed_source_dataset,
            export_managed_source_directory,
            export_managed_source_directory_mobile,
            export_managed_source_file,
            get_data_download_status,
            get_indicator_manage_page,
            get_ranking_compute_status,
            get_rank_overview,
            get_rank_trade_date_options,
            list_stock_lookup_rows,
            get_rank_overview_page,
            get_stock_detail_page,
            get_stock_detail_realtime,
            get_market_monitor_page,
            get_market_simulation_page,
            refresh_market_simulation_realtime,
            get_strategy_statistics_page,
            get_strategy_statistics_detail,
            get_strategy_triggered_stocks,
            get_strategy_performance_page,
            get_strategy_pick_cache,
            get_or_build_strategy_pick_cache,
            save_manual_strategy_pick_cache,
            get_latest_strategy_pick_cache,
            get_strategy_performance_horizon_view,
            get_strategy_performance_rule_detail,
            get_strategy_performance_validation_page,
            get_return_backtest_page,
            get_return_backtest_strength_overview,
            get_board_analysis_page,
            get_board_analysis_group_detail,
            get_stock_pick_options,
            run_expression_stock_pick,
            run_concept_stock_pick,
            run_advanced_stock_pick,
            get_strategy_manage_page,
            add_strategy_manage_rule,
            check_strategy_manage_rule_draft,
            create_strategy_manage_rule,
            remove_strategy_manage_rules,
            update_strategy_manage_rule,
            export_strategy_rule_file,
            run_data_download,
            run_missing_stock_repair,
            run_ths_concept_download,
            run_ranking_score_calculation,
            save_indicator_manage_page,
            run_ranking_tiebreak_fill,
            list_watch_observe_rows,
            refresh_watch_observe_rows,
            upsert_watch_observe_row,
            merge_watch_observe_rows,
            update_watch_observe_tag,
            remove_watch_observe_rows
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
