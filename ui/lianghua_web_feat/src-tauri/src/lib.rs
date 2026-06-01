mod data_download_bridge;
mod managed_source_bridge;

use std::{
    fs,
    io::{Read, Write},
    path::{Path, PathBuf},
    str::FromStr,
    time::Instant,
};

fn decode_percent_encoded_path(raw: &str) -> String {
    if !raw.contains('%') {
        return raw.to_string();
    }

    let input = raw.as_bytes();
    let mut bytes = Vec::with_capacity(input.len());
    let mut i = 0;
    while i < input.len() {
        if input[i] == b'%' && i + 2 < input.len() {
            let hi = hex_val(input[i + 1]);
            let lo = hex_val(input[i + 2]);
            if let (Some(hi), Some(lo)) = (hi, lo) {
                bytes.push(hi << 4 | lo);
                i += 3;
                continue;
            }
        }
        bytes.push(input[i]);
        i += 1;
    }

    String::from_utf8(bytes).unwrap_or_else(|_| raw.to_string())
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'A'..=b'F' => Some(b - b'A' + 10),
        b'a'..=b'f' => Some(b - b'a' + 10),
        _ => None,
    }
}

use lianghua_rs::ui_tools_feat::{
    chart_indicator_settings::{
        get_chart_indicator_settings as core_get_chart_indicator_settings,
        reset_chart_indicator_settings as core_reset_chart_indicator_settings,
        save_chart_indicator_settings as core_save_chart_indicator_settings,
        validate_chart_indicator_settings as core_validate_chart_indicator_settings,
        ChartIndicatorSettingsPayload, ChartIndicatorValidationResult,
    },
    concept_stock_pick::{
        run_concept_stock_pick as core_run_concept_stock_pick,
        StockPickResultData as ConceptStockPickResultData,
    },
    cyq_chen::{
        activate_cyq_chen_strategy_backup as core_activate_cyq_chen_strategy_backup,
        backup_cyq_chen_strategy_file as core_backup_cyq_chen_strategy_file,
        check_cyq_chen_strategy_file_draft as core_check_cyq_chen_strategy_file_draft,
        delete_cyq_chen_strategy_backup as core_delete_cyq_chen_strategy_backup,
        get_cyq_chen_strategy_backup_diff as core_get_cyq_chen_strategy_backup_diff,
        get_cyq_chen_strategy_page as core_get_cyq_chen_strategy_page,
        import_cyq_chen_strategy_backup as core_import_cyq_chen_strategy_backup,
        run_cyq_chen_single_stock_test as core_run_cyq_chen_single_stock_test,
        save_cyq_chen_strategy_file as core_save_cyq_chen_strategy_file, CyqChenSingleStockData,
        CyqChenSingleStockRequest, CyqChenStrategyBackupDiff, CyqChenStrategyFileDraft,
        CyqChenStrategyFileExportResult, CyqChenStrategyPageData,
    },
    data_viewer::{list_stock_lookup_rows as core_list_stock_lookup_rows, StockLookupRow},
    details::{
        get_stock_detail_cyq as core_get_stock_detail_cyq,
        get_stock_detail_page as core_get_stock_detail_page,
        get_stock_detail_realtime as core_get_stock_detail_realtime,
        get_stock_detail_strategy_snapshot as core_get_stock_detail_strategy_snapshot,
        StockDetailCyqData, StockDetailPageData, StockDetailRealtimeData,
        StockDetailStrategySnapshotData,
    },
    expression_stock_pick::{
        run_expression_stock_pick as core_run_expression_stock_pick,
        StockPickResultData as ExpressionStockPickResultData,
    },
    intraday_monitor::{
        get_intraday_monitor_page as core_get_intraday_monitor_page,
        refresh_intraday_monitor_realtime as core_refresh_intraday_monitor_realtime,
        refresh_intraday_monitor_template_tags as core_refresh_intraday_monitor_template_tags,
        validate_intraday_monitor_template_expression as core_validate_intraday_monitor_template_expression,
        IntradayMonitorPageData, IntradayMonitorRankModeConfig, IntradayMonitorRow,
        IntradayMonitorTemplate, IntradayMonitorTemplateValidationData,
    },
    overview::{
        get_scene_rank_overview_page as core_get_scene_rank_overview_page,
        get_scene_rank_trade_date_options as core_get_scene_rank_trade_date_options,
        SceneOverviewPageData,
    },
    overview_classic::{
        get_rank_overview as core_get_rank_overview,
        get_rank_overview_page as core_get_rank_overview_page,
        get_rank_trade_date_options as core_get_rank_trade_date_options, OverviewPageData,
        OverviewRow,
    },
    ranking_compute::{
        get_ranking_compute_status as core_get_ranking_compute_status,
        preview_ranking_score_calculation_warnings as core_preview_ranking_score_calculation_warnings,
        run_concept_performance_compute as core_run_concept_performance_compute,
        run_cyq_chen_compute_with_range_and_progress as core_run_cyq_chen_compute,
        run_cyq_compute_with_range_and_progress as core_run_cyq_compute,
        run_ranking_score_calculation as core_run_ranking_score_calculation,
        run_ranking_tiebreak_fill as core_run_ranking_tiebreak_fill,
        ConceptPerformanceComputeResult, CyqChenComputeResult, CyqComputeResult,
        RankComputeRunResult, RankComputeStatus,
    },
    statistics::{
        get_market_analysis as core_get_market_analysis,
        get_market_contribution as core_get_market_contribution,
        get_rule_layer_backtest_defaults as core_get_rule_layer_backtest_defaults,
        get_scene_layer_backtest_defaults as core_get_scene_layer_backtest_defaults,
        get_scene_statistics_page as core_get_scene_statistics_page,
        get_strategy_statistics_detail as core_get_strategy_statistics_detail,
        get_strategy_statistics_page as core_get_strategy_statistics_page,
        get_strategy_triggered_stocks as core_get_strategy_triggered_stocks,
        run_rank_layer_backtest as core_run_rank_layer_backtest,
        run_rule_expression_validation as core_run_rule_expression_validation,
        run_rule_layer_backtest as core_run_rule_layer_backtest,
        run_scene_layer_backtest as core_run_scene_layer_backtest,
        run_transient_rank_layer_backtest as core_run_transient_rank_layer_backtest,
        run_transient_rule_layer_backtest as core_run_transient_rule_layer_backtest,
        run_transient_scene_layer_backtest as core_run_transient_scene_layer_backtest,
        MarketAnalysisData, MarketContributionData, RankLayerBacktestData,
        RuleExpressionValidationData, RuleExpressionValidationManualStrategy,
        RuleLayerBacktestData, RuleLayerBacktestDefaultsData, RuleValidationUnknownConfig,
        SceneLayerBacktestData, SceneLayerBacktestDefaultsData, SceneStatisticsPageData,
        StrategyStatisticsDetailData, StrategyStatisticsPageData, TriggeredStockRow,
    },
    stock_pick::{get_stock_pick_options as core_get_stock_pick_options, StockPickOptionsData},
    stock_similarity::{
        get_stock_similarity_page as core_get_stock_similarity_page, StockSimilarityPageData,
    },
    strategy_manage::{
        check_strategy_manage_rule_draft as core_check_strategy_manage_rule_draft,
        check_strategy_manage_scene_draft as core_check_strategy_manage_scene_draft,
        create_strategy_manage_rule as core_create_strategy_manage_rule,
        create_strategy_manage_scene as core_create_strategy_manage_scene,
        get_strategy_manage_page as core_get_strategy_manage_page,
        remove_strategy_manage_rules as core_remove_strategy_manage_rules,
        remove_strategy_manage_scene as core_remove_strategy_manage_scene,
        save_strategy_manage_refactor_file as core_save_strategy_manage_refactor_file,
        update_strategy_manage_rule as core_update_strategy_manage_rule,
        update_strategy_manage_scene as core_update_strategy_manage_scene, StrategyManagePageData,
        StrategyManageRefactorDraft, StrategyManageRuleDraft, StrategyManageSceneDraft,
    },
    strategy_paper_validation::{
        get_strategy_paper_validation_defaults as core_get_strategy_paper_validation_defaults,
        run_strategy_paper_validation as core_run_strategy_paper_validation,
        validate_strategy_paper_validation_template_expressions as core_validate_strategy_paper_validation_template_expressions,
        StrategyPaperValidationData, StrategyPaperValidationDefaultsData,
        StrategyPaperValidationTemplateValidationData,
    },
    watch_observe::{
        hydrate_watch_observe_rows as core_hydrate_watch_observe_rows,
        normalize_trade_date as core_normalize_watch_observe_trade_date,
        normalize_ts_code as core_normalize_watch_observe_ts_code,
        refresh_watch_observe_rows as core_refresh_watch_observe_rows,
        WatchObserveRow as CoreWatchObserveRow, WatchObserveSnapshotData, WatchObserveStoredRow,
    },
};
use serde::{Deserialize, Serialize};
use tauri::Emitter;
use tauri_plugin_fs::{FilePath, FsExt};

use data_download_bridge::{
    get_data_download_status, get_indicator_manage_page, run_concept_most_related_repair,
    run_concept_performance_repair, run_data_download, run_missing_stock_repair,
    run_stock_data_indicator_columns_delete, run_stock_data_indicator_columns_rebuild,
    run_ths_concept_download, save_indicator_manage_page,
};
use managed_source_bridge::{
    activate_managed_strategy_backup, allow_import_path,
    auto_backup_managed_active_strategy_on_entry, backup_managed_active_strategy,
    copy_import_file_to_appdata, create_managed_empty_strategy_backup,
    delete_managed_strategy_backup, export_managed_source_directory,
    export_managed_source_directory_mobile, export_managed_source_file,
    export_managed_strategy_backup_file, export_managed_strategy_bundle,
    get_managed_strategy_assets_status, get_managed_strategy_backup_diff,
    import_managed_source_zip, import_managed_strategy_backup, preview_managed_source_dataset,
    preview_managed_source_stock_data, snapshot_rank_compute_strategy,
    update_managed_strategy_backup_description,
};

#[cfg(target_os = "android")]
use jni::{objects::JObject, sys::jboolean, JNIEnv};

#[cfg(target_os = "android")]
use rustls_platform_verifier;
use tauri::Manager;

const WATCH_OBSERVE_STORAGE_FILE: &str = "watch_observe.json";
const DEFAULT_MANAGED_SOURCE_DIR: &str = "source";
const RANKING_COMPUTE_PROGRESS_EVENT: &str = "data-download-status";
const CHIP_CHANGE_BACKUP_DIR_NAME: &str = "chip_change_rule_backups";
const CHIP_CHANGE_RULE_FILE_NAME: &str = "chip_change_rule.toml";

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct RankingComputeProgressEventPayload {
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

fn emit_ranking_compute_progress_event(
    app: &tauri::AppHandle,
    payload: RankingComputeProgressEventPayload,
) {
    if let Err(emit_error) = app.emit(RANKING_COMPUTE_PROGRESS_EVENT, payload) {
        log::warn!(
            "failed to emit ranking compute progress event: {}",
            emit_error
        );
    }
}

#[cfg(target_os = "linux")]
fn trim_process_heap() {
    // SAFETY: `malloc_trim(0)` is a libc allocator maintenance call. It does
    // not take ownership of any Rust pointer and does not dereference caller
    // memory; it only asks glibc to release free heap pages back to the OS.
    unsafe {
        libc::malloc_trim(0);
    }
}

#[cfg(target_os = "android")]
fn trim_process_heap() {
    use libc::{c_int, c_void};

    const M_DECAY_TIME: c_int = -100;
    const M_PURGE: c_int = -101;
    type MalloptFn = unsafe extern "C" fn(c_int, c_int) -> c_int;

    // SAFETY: Android's bionic does not expose `malloc_trim`. We resolve
    // `mallopt` dynamically so old devices without the symbol simply skip the
    // purge. The function pointer comes from the already-loaded libc image and
    // is used immediately; no Rust pointer is passed to C and no pointer from C
    // is retained, so this does not create a UAF path.
    unsafe {
        let symbol = libc::dlsym(libc::RTLD_DEFAULT, b"mallopt\0".as_ptr().cast());
        if symbol.is_null() {
            return;
        }

        let mallopt: MalloptFn =
            std::mem::transmute::<*mut c_void, MalloptFn>(symbol.cast::<c_void>());
        mallopt(M_DECAY_TIME, 0);
        mallopt(M_PURGE, 0);
    }
}

#[cfg(not(any(target_os = "linux", target_os = "android")))]
fn trim_process_heap() {}

fn run_with_heap_trim<T>(f: impl FnOnce() -> T) -> T {
    let result = f();
    trim_process_heap();
    result
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

#[cfg(target_os = "android")]
fn init_rustls_platform_verifier_impl(mut env: JNIEnv, activity: JObject) -> jboolean {
    match rustls_platform_verifier::android::init_hosted(&mut env, activity) {
        Ok(()) => 1,
        Err(error) => {
            eprintln!("初始化 rustls-platform-verifier 失败: {error}");
            0
        }
    }
}

#[cfg(target_os = "android")]
#[allow(non_snake_case)]
#[no_mangle]
pub extern "system" fn Java_com_lmingyuanl_lianghua_MainActivity_initRustlsPlatformVerifier(
    env: JNIEnv,
    activity: JObject,
) -> jboolean {
    init_rustls_platform_verifier_impl(env, activity)
}

#[cfg(target_os = "android")]
#[allow(non_snake_case)]
#[no_mangle]
pub extern "system" fn Java_com_mingyuan_lianghua_MainActivity_initRustlsPlatformVerifier(
    env: JNIEnv,
    activity: JObject,
) -> jboolean {
    init_rustls_platform_verifier_impl(env, activity)
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
    let ts_code = core_normalize_watch_observe_ts_code(&row.ts_code)
        .ok_or_else(|| "自选代码无效".to_string())?;
    let added_date = row
        .added_date
        .as_deref()
        .and_then(core_normalize_watch_observe_trade_date)
        .unwrap_or_default();
    let trade_date = row
        .trade_date
        .as_deref()
        .and_then(core_normalize_watch_observe_trade_date);

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

#[tauri::command]
fn list_stock_lookup_rows(source_path: String) -> Result<Vec<StockLookupRow>, String> {
    core_list_stock_lookup_rows(&source_path)
}

#[tauri::command]
fn get_stock_pick_options(source_path: String) -> Result<StockPickOptionsData, String> {
    core_get_stock_pick_options(&source_path)
}

#[tauri::command]
fn get_chart_indicator_settings(
    source_path: String,
) -> Result<ChartIndicatorSettingsPayload, String> {
    core_get_chart_indicator_settings(&source_path)
}

#[tauri::command]
fn validate_chart_indicator_settings(
    source_path: String,
    text: String,
) -> Result<ChartIndicatorValidationResult, String> {
    core_validate_chart_indicator_settings(&source_path, &text)
}

#[tauri::command]
fn save_chart_indicator_settings(
    source_path: String,
    text: String,
) -> Result<ChartIndicatorSettingsPayload, String> {
    core_save_chart_indicator_settings(&source_path, &text)
}

#[tauri::command]
fn reset_chart_indicator_settings(
    source_path: String,
) -> Result<ChartIndicatorSettingsPayload, String> {
    core_reset_chart_indicator_settings(&source_path)
}

#[tauri::command]
fn get_ranking_compute_status(
    source_path: String,
    strategy_path: Option<String>,
) -> Result<RankComputeStatus, String> {
    core_get_ranking_compute_status(&source_path, strategy_path.as_deref())
}

#[tauri::command]
fn preview_ranking_score_calculation_warnings(
    source_path: String,
    strategy_path: Option<String>,
    start_date: String,
    end_date: String,
) -> Result<Vec<String>, String> {
    core_preview_ranking_score_calculation_warnings(
        &source_path,
        strategy_path.as_deref(),
        &start_date,
        &end_date,
    )
}

#[tauri::command]
fn get_rank_overview(
    source_path: String,
    trade_date: Option<String>,
    limit: Option<u32>,
    board: Option<String>,
    exclude_st_board: Option<bool>,
    total_mv_min: Option<f64>,
    total_mv_max: Option<f64>,
) -> Result<Vec<OverviewRow>, String> {
    core_get_rank_overview(
        source_path,
        trade_date,
        limit,
        board,
        exclude_st_board,
        total_mv_min,
        total_mv_max,
    )
}

#[tauri::command]
fn get_rank_trade_date_options(source_path: String) -> Result<Vec<String>, String> {
    core_get_rank_trade_date_options(source_path)
}

#[tauri::command]
fn get_rank_overview_page(
    source_path: String,
    rank_date: Option<String>,
    ref_date: Option<String>,
    limit: Option<u32>,
    board: Option<String>,
    exclude_st_board: Option<bool>,
    total_mv_min: Option<f64>,
    total_mv_max: Option<f64>,
) -> Result<OverviewPageData, String> {
    core_get_rank_overview_page(
        source_path,
        rank_date,
        ref_date,
        limit,
        board,
        exclude_st_board,
        total_mv_min,
        total_mv_max,
    )
}

#[tauri::command]
fn get_scene_rank_trade_date_options(source_path: String) -> Result<Vec<String>, String> {
    core_get_scene_rank_trade_date_options(&source_path)
}

#[tauri::command]
fn get_scene_rank_overview_page(
    source_path: String,
    rank_date: Option<String>,
    limit: Option<u32>,
    board: Option<String>,
    exclude_st_board: Option<bool>,
    total_mv_min: Option<f64>,
    total_mv_max: Option<f64>,
) -> Result<SceneOverviewPageData, String> {
    core_get_scene_rank_overview_page(
        &source_path,
        rank_date,
        limit,
        board,
        exclude_st_board,
        total_mv_min,
        total_mv_max,
    )
}

#[tauri::command]
fn get_intraday_monitor_page(
    source_path: String,
    rank_mode: Option<String>,
    rank_date: Option<String>,
    scene_name: Option<String>,
    limit: Option<u32>,
    board: Option<String>,
    exclude_st_board: Option<bool>,
    total_mv_min: Option<f64>,
    total_mv_max: Option<f64>,
) -> Result<IntradayMonitorPageData, String> {
    core_get_intraday_monitor_page(
        &source_path,
        rank_mode,
        rank_date,
        scene_name,
        limit,
        board,
        exclude_st_board,
        total_mv_min,
        total_mv_max,
    )
}

#[tauri::command]
async fn refresh_intraday_monitor_realtime(
    source_path: String,
    rows: Vec<IntradayMonitorRow>,
    templates: Vec<IntradayMonitorTemplate>,
    rank_mode_configs: Vec<IntradayMonitorRankModeConfig>,
) -> Result<IntradayMonitorPageData, String> {
    tauri::async_runtime::spawn_blocking(move || {
        core_refresh_intraday_monitor_realtime(&source_path, rows, templates, rank_mode_configs)
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
async fn refresh_intraday_monitor_template_tags(
    source_path: String,
    rows: Vec<IntradayMonitorRow>,
    templates: Vec<IntradayMonitorTemplate>,
    rank_mode_configs: Vec<IntradayMonitorRankModeConfig>,
) -> Result<IntradayMonitorPageData, String> {
    tauri::async_runtime::spawn_blocking(move || {
        core_refresh_intraday_monitor_template_tags(
            &source_path,
            rows,
            templates,
            rank_mode_configs,
        )
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
fn validate_intraday_monitor_template_expression(
    source_path: Option<String>,
    expression: String,
) -> Result<IntradayMonitorTemplateValidationData, String> {
    core_validate_intraday_monitor_template_expression(source_path.as_deref(), expression)
}

#[tauri::command]
fn get_strategy_manage_page(source_path: String) -> Result<StrategyManagePageData, String> {
    core_get_strategy_manage_page(&source_path)
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
fn get_stock_detail_strategy_snapshot(
    source_path: String,
    trade_date: Option<String>,
    ts_code: String,
) -> Result<StockDetailStrategySnapshotData, String> {
    core_get_stock_detail_strategy_snapshot(source_path, trade_date, ts_code)
}

#[tauri::command]
fn get_stock_detail_cyq(
    source_path: String,
    ts_code: String,
    chip_model: Option<String>,
) -> Result<StockDetailCyqData, String> {
    core_get_stock_detail_cyq(source_path, ts_code, chip_model)
}

#[tauri::command]
fn get_stock_detail_realtime(
    source_path: String,
    ts_code: String,
    chart_window_days: Option<u32>,
) -> Result<StockDetailRealtimeData, String> {
    core_get_stock_detail_realtime(source_path, ts_code, chart_window_days)
}

#[tauri::command]
async fn get_stock_similarity_page(
    source_path: String,
    trade_date: Option<String>,
    ts_code: String,
    limit: Option<u32>,
) -> Result<StockSimilarityPageData, String> {
    tauri::async_runtime::spawn_blocking(move || {
        core_get_stock_similarity_page(source_path, trade_date, ts_code, limit)
    })
    .await
    .map_err(|error| error.to_string())?
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
fn get_strategy_paper_validation_defaults(
    source_path: String,
) -> Result<StrategyPaperValidationDefaultsData, String> {
    core_get_strategy_paper_validation_defaults(&source_path)
}

#[tauri::command]
fn validate_strategy_paper_validation_template_expressions(
    buy_expression: String,
    sell_expression: String,
) -> Result<StrategyPaperValidationTemplateValidationData, String> {
    core_validate_strategy_paper_validation_template_expressions(buy_expression, sell_expression)
}

#[tauri::command]
async fn run_strategy_paper_validation(
    source_path: String,
    start_date: Option<String>,
    end_date: Option<String>,
    min_listed_trade_days: Option<usize>,
    index_ts_code: Option<String>,
    test_ts_code: Option<String>,
    board: Option<String>,
    buy_price_basis: String,
    slippage_pct: Option<f64>,
    max_position_count: Option<usize>,
    buy_selection_mode: Option<String>,
    buy_expression: String,
    sell_expression: String,
) -> Result<StrategyPaperValidationData, String> {
    tauri::async_runtime::spawn_blocking(move || {
        run_with_heap_trim(|| {
            core_run_strategy_paper_validation(
                &source_path,
                start_date,
                end_date,
                min_listed_trade_days,
                index_ts_code,
                test_ts_code,
                board,
                buy_price_basis,
                slippage_pct,
                max_position_count,
                buy_selection_mode,
                buy_expression,
                sell_expression,
            )
        })
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
async fn get_scene_statistics_page(
    source_path: String,
    scene_name: Option<String>,
    analysis_trade_date: Option<String>,
) -> Result<SceneStatisticsPageData, String> {
    tauri::async_runtime::spawn_blocking(move || {
        core_get_scene_statistics_page(source_path, scene_name, analysis_trade_date)
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
fn get_scene_layer_backtest_defaults(
    source_path: String,
) -> Result<SceneLayerBacktestDefaultsData, String> {
    core_get_scene_layer_backtest_defaults(source_path)
}

#[tauri::command]
fn get_rule_layer_backtest_defaults(
    source_path: String,
) -> Result<RuleLayerBacktestDefaultsData, String> {
    core_get_rule_layer_backtest_defaults(source_path)
}

#[tauri::command]
async fn get_market_analysis(
    source_path: String,
    lookback_period: Option<usize>,
    reference_trade_date: Option<String>,
    board: Option<String>,
    exclude_st_board: Option<bool>,
    min_listed_trade_days: Option<usize>,
    stock_rank_limit: Option<usize>,
    sub_interval_period: Option<usize>,
    min_board_stock_count: Option<usize>,
) -> Result<MarketAnalysisData, String> {
    tauri::async_runtime::spawn_blocking(move || {
        core_get_market_analysis(
            source_path,
            lookback_period,
            reference_trade_date,
            board,
            exclude_st_board,
            min_listed_trade_days,
            stock_rank_limit,
            sub_interval_period,
            min_board_stock_count,
        )
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
async fn get_market_contribution(
    source_path: String,
    scope: String,
    kind: String,
    name: String,
    lookback_period: Option<usize>,
    reference_trade_date: Option<String>,
) -> Result<MarketContributionData, String> {
    tauri::async_runtime::spawn_blocking(move || {
        core_get_market_contribution(
            source_path,
            scope,
            kind,
            name,
            lookback_period,
            reference_trade_date,
        )
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
async fn run_scene_layer_backtest(
    source_path: String,
    stock_adj_type: Option<String>,
    index_ts_code: String,
    index_beta: Option<f64>,
    concept_beta: Option<f64>,
    industry_beta: Option<f64>,
    start_date: String,
    end_date: String,
    min_samples_per_scene_day: Option<usize>,
    min_listed_trade_days: Option<usize>,
    backtest_period: Option<usize>,
) -> Result<SceneLayerBacktestData, String> {
    tauri::async_runtime::spawn_blocking(move || {
        run_with_heap_trim(|| {
            core_run_scene_layer_backtest(
                source_path,
                stock_adj_type,
                index_ts_code,
                index_beta,
                concept_beta,
                industry_beta,
                start_date,
                end_date,
                min_samples_per_scene_day,
                min_listed_trade_days,
                backtest_period,
            )
        })
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
async fn run_rule_layer_backtest(
    source_path: String,
    stock_adj_type: Option<String>,
    index_ts_code: String,
    index_beta: Option<f64>,
    concept_beta: Option<f64>,
    industry_beta: Option<f64>,
    start_date: String,
    end_date: String,
    min_samples_per_rule_day: Option<usize>,
    min_listed_trade_days: Option<usize>,
    backtest_period: Option<usize>,
) -> Result<RuleLayerBacktestData, String> {
    tauri::async_runtime::spawn_blocking(move || {
        run_with_heap_trim(|| {
            core_run_rule_layer_backtest(
                source_path,
                stock_adj_type,
                index_ts_code,
                index_beta,
                concept_beta,
                industry_beta,
                start_date,
                end_date,
                min_samples_per_rule_day,
                min_listed_trade_days,
                backtest_period,
            )
        })
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
async fn run_rank_layer_backtest(
    source_path: String,
    stock_adj_type: Option<String>,
    index_ts_code: String,
    index_beta: Option<f64>,
    concept_beta: Option<f64>,
    industry_beta: Option<f64>,
    start_date: String,
    end_date: String,
    min_samples_per_rank_day: Option<usize>,
    min_listed_trade_days: Option<usize>,
    backtest_period: Option<usize>,
    layer_count: Option<usize>,
    layer_method: Option<String>,
) -> Result<RankLayerBacktestData, String> {
    tauri::async_runtime::spawn_blocking(move || {
        run_with_heap_trim(|| {
            core_run_rank_layer_backtest(
                source_path,
                stock_adj_type,
                index_ts_code,
                index_beta,
                concept_beta,
                industry_beta,
                start_date,
                end_date,
                min_samples_per_rank_day,
                min_listed_trade_days,
                backtest_period,
                layer_count,
                layer_method,
            )
        })
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
async fn run_transient_scene_layer_backtest(
    source_path: String,
    stock_adj_type: Option<String>,
    index_ts_code: String,
    index_beta: Option<f64>,
    concept_beta: Option<f64>,
    industry_beta: Option<f64>,
    start_date: String,
    end_date: String,
    min_samples_per_scene_day: Option<usize>,
    min_listed_trade_days: Option<usize>,
    backtest_period: Option<usize>,
) -> Result<SceneLayerBacktestData, String> {
    tauri::async_runtime::spawn_blocking(move || {
        run_with_heap_trim(|| {
            core_run_transient_scene_layer_backtest(
                source_path,
                stock_adj_type,
                index_ts_code,
                index_beta,
                concept_beta,
                industry_beta,
                start_date,
                end_date,
                min_samples_per_scene_day,
                min_listed_trade_days,
                backtest_period,
            )
        })
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
async fn run_transient_rule_layer_backtest(
    source_path: String,
    stock_adj_type: Option<String>,
    index_ts_code: String,
    index_beta: Option<f64>,
    concept_beta: Option<f64>,
    industry_beta: Option<f64>,
    start_date: String,
    end_date: String,
    min_samples_per_rule_day: Option<usize>,
    min_listed_trade_days: Option<usize>,
    backtest_period: Option<usize>,
) -> Result<RuleLayerBacktestData, String> {
    tauri::async_runtime::spawn_blocking(move || {
        run_with_heap_trim(|| {
            core_run_transient_rule_layer_backtest(
                source_path,
                stock_adj_type,
                index_ts_code,
                index_beta,
                concept_beta,
                industry_beta,
                start_date,
                end_date,
                min_samples_per_rule_day,
                min_listed_trade_days,
                backtest_period,
            )
        })
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
async fn run_transient_rank_layer_backtest(
    source_path: String,
    stock_adj_type: Option<String>,
    index_ts_code: String,
    index_beta: Option<f64>,
    concept_beta: Option<f64>,
    industry_beta: Option<f64>,
    start_date: String,
    end_date: String,
    min_samples_per_rank_day: Option<usize>,
    min_listed_trade_days: Option<usize>,
    backtest_period: Option<usize>,
    layer_count: Option<usize>,
    layer_method: Option<String>,
) -> Result<RankLayerBacktestData, String> {
    tauri::async_runtime::spawn_blocking(move || {
        run_with_heap_trim(|| {
            core_run_transient_rank_layer_backtest(
                source_path,
                stock_adj_type,
                index_ts_code,
                index_beta,
                concept_beta,
                industry_beta,
                start_date,
                end_date,
                min_samples_per_rank_day,
                min_listed_trade_days,
                backtest_period,
                layer_count,
                layer_method,
            )
        })
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
async fn run_rule_expression_validation(
    source_path: String,
    import_rule_name: String,
    when: Option<String>,
    scope_way: Option<String>,
    scope_windows: Option<usize>,
    stock_adj_type: Option<String>,
    index_ts_code: String,
    index_beta: Option<f64>,
    concept_beta: Option<f64>,
    industry_beta: Option<f64>,
    start_date: String,
    end_date: String,
    min_samples_per_rule_day: Option<usize>,
    min_listed_trade_days: Option<usize>,
    backtest_period: Option<usize>,
    manual_strategy: Option<RuleExpressionValidationManualStrategy>,
    unknown_configs: Option<Vec<RuleValidationUnknownConfig>>,
    sample_limit_per_group: Option<usize>,
) -> Result<RuleExpressionValidationData, String> {
    tauri::async_runtime::spawn_blocking(move || {
        run_with_heap_trim(|| {
            core_run_rule_expression_validation(
                source_path,
                import_rule_name,
                when,
                scope_way,
                scope_windows,
                stock_adj_type,
                index_ts_code,
                index_beta,
                concept_beta,
                industry_beta,
                start_date,
                end_date,
                min_samples_per_rule_day,
                min_listed_trade_days,
                backtest_period,
                manual_strategy,
                unknown_configs,
                sample_limit_per_group,
            )
        })
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
fn check_strategy_manage_scene_draft(
    source_path: String,
    original_name: Option<String>,
    draft: StrategyManageSceneDraft,
) -> Result<String, String> {
    core_check_strategy_manage_scene_draft(&source_path, original_name.as_deref(), draft)
}

#[tauri::command]
fn create_strategy_manage_scene(
    source_path: String,
    draft: StrategyManageSceneDraft,
) -> Result<StrategyManagePageData, String> {
    core_create_strategy_manage_scene(&source_path, draft)
}

#[tauri::command]
fn update_strategy_manage_scene(
    source_path: String,
    original_name: String,
    draft: StrategyManageSceneDraft,
) -> Result<StrategyManagePageData, String> {
    core_update_strategy_manage_scene(&source_path, &original_name, draft)
}

#[tauri::command]
fn remove_strategy_manage_scene(
    source_path: String,
    name: String,
) -> Result<StrategyManagePageData, String> {
    core_remove_strategy_manage_scene(&source_path, &name)
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
fn save_strategy_manage_refactor_file(
    source_path: String,
    file_name: String,
    draft: StrategyManageRefactorDraft,
) -> Result<String, String> {
    core_save_strategy_manage_refactor_file(&source_path, &file_name, draft)
}

#[tauri::command]
async fn run_ranking_score_calculation(
    app: tauri::AppHandle,
    source_path: String,
    strategy_path: Option<String>,
    start_date: String,
    end_date: String,
) -> Result<RankComputeRunResult, String> {
    tauri::async_runtime::spawn_blocking(move || {
        let strategy_file_path =
            lianghua_rs::data::resolve_strategy_path(&source_path, strategy_path.as_deref());
        let app_data_root = app
            .path()
            .resolve("", tauri::path::BaseDirectory::AppData)
            .map_err(|error| error.to_string())?;
        let snapshot_strategy_path = snapshot_rank_compute_strategy(
            &app_data_root,
            DEFAULT_MANAGED_SOURCE_DIR,
            &strategy_file_path,
            Some(&start_date),
            Some(&end_date),
        )?;
        let snapshot_strategy_path = snapshot_strategy_path.display().to_string();
        core_run_ranking_score_calculation(
            &source_path,
            Some(snapshot_strategy_path.as_str()),
            &start_date,
            &end_date,
        )
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
async fn run_concept_performance_compute(
    source_path: String,
) -> Result<ConceptPerformanceComputeResult, String> {
    tauri::async_runtime::spawn_blocking(move || core_run_concept_performance_compute(&source_path))
        .await
        .map_err(|error| error.to_string())?
}

#[tauri::command]
async fn run_cyq_compute(
    app: tauri::AppHandle,
    source_path: String,
    factor: usize,
    start_date: Option<String>,
    end_date: Option<String>,
    download_id: Option<String>,
) -> Result<CyqComputeResult, String> {
    let download_id = download_id
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    tauri::async_runtime::spawn_blocking(move || {
        let started_at = Instant::now();
        let action = "cyq".to_string();
        let action_label = "筹码计算".to_string();
        if let Some(download_id) = download_id.as_deref() {
            emit_ranking_compute_progress_event(
                &app,
                RankingComputeProgressEventPayload {
                    download_id: download_id.to_string(),
                    phase: "started".to_string(),
                    action: action.clone(),
                    action_label: action_label.clone(),
                    elapsed_ms: 0,
                    finished: 0,
                    total: 0,
                    current_label: None,
                    message: "筹码计算已启动，正在准备数据。".to_string(),
                },
            );
        }

        let progress_app = app.clone();
        let progress_download_id = download_id.clone();
        let progress_action = action.clone();
        let progress_action_label = action_label.clone();
        let progress_cb = move |progress: lianghua_rs::download::runner::DownloadProgress| {
            if let Some(download_id) = progress_download_id.as_deref() {
                emit_ranking_compute_progress_event(
                    &progress_app,
                    RankingComputeProgressEventPayload {
                        download_id: download_id.to_string(),
                        phase: progress.phase,
                        action: progress_action.clone(),
                        action_label: progress_action_label.clone(),
                        elapsed_ms: started_at.elapsed().as_millis() as u64,
                        finished: progress.finished as u64,
                        total: progress.total as u64,
                        current_label: progress.current_label,
                        message: progress.message,
                    },
                );
            }
        };

        let result = core_run_cyq_compute(
            &source_path,
            factor,
            start_date.as_deref(),
            end_date.as_deref(),
            download_id.as_ref().map(|_| {
                &progress_cb as &lianghua_rs::download::runner::DownloadProgressCallback<'_>
            }),
        );
        match (&result, download_id.as_deref()) {
            (Ok(run_result), Some(download_id)) => emit_ranking_compute_progress_event(
                &app,
                RankingComputeProgressEventPayload {
                    download_id: download_id.to_string(),
                    phase: "completed".to_string(),
                    action,
                    action_label,
                    elapsed_ms: run_result.elapsed_ms,
                    finished: 1,
                    total: 1,
                    current_label: None,
                    message: format!(
                        "筹码计算完成，写入 {} 条摘要和 {} 条分桶。",
                        run_result.snapshot_rows, run_result.bin_rows
                    ),
                },
            ),
            (Err(error), Some(download_id)) => emit_ranking_compute_progress_event(
                &app,
                RankingComputeProgressEventPayload {
                    download_id: download_id.to_string(),
                    phase: "failed".to_string(),
                    action,
                    action_label,
                    elapsed_ms: started_at.elapsed().as_millis() as u64,
                    finished: 0,
                    total: 0,
                    current_label: None,
                    message: format!("筹码计算失败: {error}"),
                },
            ),
            _ => {}
        }
        result
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
async fn run_cyq_chen_compute(
    app: tauri::AppHandle,
    source_path: String,
    warmup_days: usize,
    bucket_pct: f64,
    start_date: Option<String>,
    end_date: Option<String>,
    download_id: Option<String>,
) -> Result<CyqChenComputeResult, String> {
    let download_id = download_id
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    tauri::async_runtime::spawn_blocking(move || {
        let started_at = Instant::now();
        let action = "cyq-chen".to_string();
        let action_label = "新筹码计算".to_string();
        if let Some(download_id) = download_id.as_deref() {
            emit_ranking_compute_progress_event(
                &app,
                RankingComputeProgressEventPayload {
                    download_id: download_id.to_string(),
                    phase: "started".to_string(),
                    action: action.clone(),
                    action_label: action_label.clone(),
                    elapsed_ms: 0,
                    finished: 0,
                    total: 0,
                    current_label: None,
                    message: "新筹码计算已启动，正在准备数据。".to_string(),
                },
            );
        }

        let progress_app = app.clone();
        let progress_download_id = download_id.clone();
        let progress_action = action.clone();
        let progress_action_label = action_label.clone();
        let progress_cb = move |progress: lianghua_rs::download::runner::DownloadProgress| {
            if let Some(download_id) = progress_download_id.as_deref() {
                emit_ranking_compute_progress_event(
                    &progress_app,
                    RankingComputeProgressEventPayload {
                        download_id: download_id.to_string(),
                        phase: progress.phase,
                        action: progress_action.clone(),
                        action_label: progress_action_label.clone(),
                        elapsed_ms: started_at.elapsed().as_millis() as u64,
                        finished: progress.finished as u64,
                        total: progress.total as u64,
                        current_label: progress.current_label,
                        message: progress.message,
                    },
                );
            }
        };

        core_backup_cyq_chen_strategy_file(&source_path)
            .map_err(|error| format!("创建新筹码计算策略快照失败: {error}"))?;

        let result = core_run_cyq_chen_compute(
            &source_path,
            warmup_days,
            bucket_pct,
            start_date.as_deref(),
            end_date.as_deref(),
            download_id.as_ref().map(|_| {
                &progress_cb as &lianghua_rs::download::runner::DownloadProgressCallback<'_>
            }),
        );
        match (&result, download_id.as_deref()) {
            (Ok(run_result), Some(download_id)) => emit_ranking_compute_progress_event(
                &app,
                RankingComputeProgressEventPayload {
                    download_id: download_id.to_string(),
                    phase: "completed".to_string(),
                    action,
                    action_label,
                    elapsed_ms: run_result.elapsed_ms,
                    finished: 1,
                    total: 1,
                    current_label: None,
                    message: format!(
                        "新筹码计算完成，写入 {} 条摘要和 {} 条分桶。",
                        run_result.snapshot_rows, run_result.bin_rows
                    ),
                },
            ),
            (Err(error), Some(download_id)) => emit_ranking_compute_progress_event(
                &app,
                RankingComputeProgressEventPayload {
                    download_id: download_id.to_string(),
                    phase: "failed".to_string(),
                    action,
                    action_label,
                    elapsed_ms: started_at.elapsed().as_millis() as u64,
                    finished: 0,
                    total: 0,
                    current_label: None,
                    message: format!("新筹码计算失败: {error}"),
                },
            ),
            _ => {}
        }
        result
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
async fn run_cyq_chen_single_stock_test(
    request: CyqChenSingleStockRequest,
) -> Result<CyqChenSingleStockData, String> {
    tauri::async_runtime::spawn_blocking(move || core_run_cyq_chen_single_stock_test(request))
        .await
        .map_err(|error| error.to_string())?
}

fn cyq_chen_active_strategy_path(source_path: &str) -> PathBuf {
    Path::new(source_path).join(CHIP_CHANGE_RULE_FILE_NAME)
}

fn cyq_chen_backup_strategy_path(source_path: &str, backup_id: &str) -> Result<PathBuf, String> {
    let backup_id = backup_id.trim();
    if backup_id.is_empty()
        || backup_id.contains('/')
        || backup_id.contains('\\')
        || backup_id.contains("..")
        || !backup_id.ends_with(".toml")
    {
        return Err("备份文件名不合法".to_string());
    }
    Ok(Path::new(source_path)
        .join(CHIP_CHANGE_BACKUP_DIR_NAME)
        .join(backup_id))
}

fn export_cyq_chen_strategy_file_to_destination(
    app: tauri::AppHandle,
    source_file: PathBuf,
    destination_file: String,
) -> Result<CyqChenStrategyFileExportResult, String> {
    let destination_file = destination_file.trim();
    if destination_file.is_empty() {
        return Err("导出目标文件为空".to_string());
    }
    if !source_file.exists() || !source_file.is_file() {
        return Err(format!(
            "待导出的筹码策略文件不存在: {}",
            source_file.display()
        ));
    }

    let mut source = fs::File::open(&source_file).map_err(|error| {
        format!(
            "打开待导出的筹码策略文件失败: path={}, err={error}",
            source_file.display()
        )
    })?;
    let mut open_options = tauri_plugin_fs::OpenOptions::new();
    open_options.write(true).truncate(true).create(true);
    let destination_path =
        FilePath::from_str(destination_file).map_err(|error| error.to_string())?;
    let destination_label = decode_percent_encoded_path(&destination_path.to_string());
    let mut target = app
        .fs()
        .open(destination_path, open_options)
        .map_err(|error| error.to_string())?;

    let mut buffer = vec![0u8; 1024 * 1024];
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

    Ok(CyqChenStrategyFileExportResult {
        exported_path: destination_label,
    })
}

#[tauri::command]
fn get_cyq_chen_strategy_page(source_path: String) -> Result<CyqChenStrategyPageData, String> {
    core_get_cyq_chen_strategy_page(&source_path)
}

#[tauri::command]
fn save_cyq_chen_strategy_file(
    source_path: String,
    draft: CyqChenStrategyFileDraft,
) -> Result<CyqChenStrategyPageData, String> {
    core_save_cyq_chen_strategy_file(&source_path, draft)
}

#[tauri::command]
fn check_cyq_chen_strategy_file_draft(draft: CyqChenStrategyFileDraft) -> Result<String, String> {
    core_check_cyq_chen_strategy_file_draft(draft)
}

#[tauri::command]
fn backup_cyq_chen_strategy_file(source_path: String) -> Result<CyqChenStrategyPageData, String> {
    core_backup_cyq_chen_strategy_file(&source_path)
}

#[tauri::command]
fn import_cyq_chen_strategy_backup(
    source_path: String,
    source_file: String,
) -> Result<CyqChenStrategyPageData, String> {
    core_import_cyq_chen_strategy_backup(&source_path, &source_file)
}

#[tauri::command]
fn delete_cyq_chen_strategy_backup(
    source_path: String,
    backup_id: String,
) -> Result<CyqChenStrategyPageData, String> {
    core_delete_cyq_chen_strategy_backup(&source_path, &backup_id)
}

#[tauri::command]
async fn export_cyq_chen_active_strategy_file(
    app: tauri::AppHandle,
    source_path: String,
    destination_file: String,
) -> Result<CyqChenStrategyFileExportResult, String> {
    tauri::async_runtime::spawn_blocking(move || {
        let source_path = source_path.trim();
        if source_path.is_empty() {
            return Err("数据目录为空，请先确认当前数据源".to_string());
        }
        export_cyq_chen_strategy_file_to_destination(
            app,
            cyq_chen_active_strategy_path(source_path),
            destination_file,
        )
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
async fn export_cyq_chen_strategy_backup_file(
    app: tauri::AppHandle,
    source_path: String,
    backup_id: String,
    destination_file: String,
) -> Result<CyqChenStrategyFileExportResult, String> {
    tauri::async_runtime::spawn_blocking(move || {
        let source_path = source_path.trim();
        if source_path.is_empty() {
            return Err("数据目录为空，请先确认当前数据源".to_string());
        }
        let source_file = cyq_chen_backup_strategy_path(source_path, &backup_id)?;
        export_cyq_chen_strategy_file_to_destination(app, source_file, destination_file)
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
fn get_cyq_chen_strategy_backup_diff(
    source_path: String,
    backup_id: String,
) -> Result<CyqChenStrategyBackupDiff, String> {
    core_get_cyq_chen_strategy_backup_diff(&source_path, &backup_id)
}

#[tauri::command]
fn activate_cyq_chen_strategy_backup(
    source_path: String,
    backup_id: String,
) -> Result<CyqChenStrategyPageData, String> {
    core_activate_cyq_chen_strategy_backup(&source_path, &backup_id)
}

#[tauri::command]
async fn run_ranking_tiebreak_fill(
    source_path: String,
    strategy_path: Option<String>,
) -> Result<RankComputeRunResult, String> {
    tauri::async_runtime::spawn_blocking(move || {
        core_run_ranking_tiebreak_fill(&source_path, strategy_path.as_deref())
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
async fn run_expression_stock_pick(
    source_path: String,
    board: Option<String>,
    exclude_st_board: Option<bool>,
    reference_trade_date: Option<String>,
    lookback_periods: Option<usize>,
    scope_way: String,
    expression: String,
    consec_threshold: Option<usize>,
) -> Result<ExpressionStockPickResultData, String> {
    let source_path = source_path.trim().to_string();
    if source_path.is_empty() {
        return Err("数据目录为空，请先到数据管理页确认当前目录".to_string());
    }

    tauri::async_runtime::spawn_blocking(move || {
        core_run_expression_stock_pick(
            &source_path,
            board,
            exclude_st_board,
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
    exclude_st_board: Option<bool>,
    trade_date: Option<String>,
    include_areas: Vec<String>,
    include_industries: Vec<String>,
    total_mv_min: Option<f64>,
    total_mv_max: Option<f64>,
    include_concepts: Vec<String>,
    exclude_concepts: Vec<String>,
    match_mode: String,
) -> Result<ConceptStockPickResultData, String> {
    core_run_concept_stock_pick(
        &source_path,
        board,
        exclude_st_board,
        trade_date,
        include_areas,
        include_industries,
        total_mv_min,
        total_mv_max,
        include_concepts,
        exclude_concepts,
        match_mode,
    )
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
    core_hydrate_watch_observe_rows(source_path.as_deref(), &stored_rows, reference_trade_date)
}

#[tauri::command]
fn refresh_watch_observe_rows(
    app: tauri::AppHandle,
    source_path: Option<String>,
    reference_trade_date: Option<String>,
    rows: Option<Vec<WatchObserveUpsertPayload>>,
) -> Result<WatchObserveSnapshotData, String> {
    let stored_rows = match rows {
        Some(rows) => normalize_watch_observe_rows_payload(rows)?,
        None => read_watch_observe_storage(&app)?,
    };
    core_refresh_watch_observe_rows(source_path.as_deref(), &stored_rows, reference_trade_date)
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
        rows[existing_index] = merge_stored_watch_observe_row(incoming, existing.as_ref());
    } else {
        rows.insert(0, incoming);
    }

    write_watch_observe_storage(&app, &rows)?;
    core_hydrate_watch_observe_rows(source_path.as_deref(), &rows, None)
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
    core_hydrate_watch_observe_rows(source_path.as_deref(), &saved_rows, None)
}

#[tauri::command]
fn update_watch_observe_tag(
    app: tauri::AppHandle,
    source_path: Option<String>,
    ts_code: String,
    tag: String,
) -> Result<Vec<CoreWatchObserveRow>, String> {
    let normalized_ts_code =
        core_normalize_watch_observe_ts_code(&ts_code).ok_or_else(|| "自选代码无效".to_string())?;
    let mut rows = read_watch_observe_storage(&app)?;
    let Some(existing_row) = rows
        .iter_mut()
        .find(|item| item.ts_code == normalized_ts_code)
    else {
        return Err(format!("未找到自选记录: {normalized_ts_code}"));
    };

    existing_row.tag = tag.trim().to_string();
    write_watch_observe_storage(&app, &rows)?;
    core_hydrate_watch_observe_rows(source_path.as_deref(), &rows, None)
}

#[tauri::command]
fn remove_watch_observe_rows(
    app: tauri::AppHandle,
    source_path: Option<String>,
    ts_codes: Vec<String>,
) -> Result<Vec<CoreWatchObserveRow>, String> {
    let normalized_codes: Vec<String> = ts_codes
        .into_iter()
        .filter_map(|value| core_normalize_watch_observe_ts_code(&value))
        .collect();
    let mut rows = read_watch_observe_storage(&app)?;
    rows.retain(|item| {
        !normalized_codes
            .iter()
            .any(|ts_code| ts_code == &item.ts_code)
    });
    write_watch_observe_storage(&app, &rows)?;
    core_hydrate_watch_observe_rows(source_path.as_deref(), &rows, None)
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
            import_managed_source_zip,
            get_managed_strategy_assets_status,
            import_managed_strategy_backup,
            auto_backup_managed_active_strategy_on_entry,
            get_managed_strategy_backup_diff,
            backup_managed_active_strategy,
            create_managed_empty_strategy_backup,
            activate_managed_strategy_backup,
            delete_managed_strategy_backup,
            update_managed_strategy_backup_description,
            export_managed_strategy_backup_file,
            export_managed_strategy_bundle,
            get_data_download_status,
            get_indicator_manage_page,
            save_indicator_manage_page,
            run_stock_data_indicator_columns_delete,
            run_stock_data_indicator_columns_rebuild,
            run_data_download,
            run_missing_stock_repair,
            run_ths_concept_download,
            run_concept_performance_repair,
            run_concept_most_related_repair,
            list_stock_lookup_rows,
            get_rank_overview,
            get_rank_trade_date_options,
            get_rank_overview_page,
            get_scene_rank_trade_date_options,
            get_scene_rank_overview_page,
            get_intraday_monitor_page,
            refresh_intraday_monitor_realtime,
            refresh_intraday_monitor_template_tags,
            validate_intraday_monitor_template_expression,
            get_stock_detail_page,
            get_stock_detail_strategy_snapshot,
            get_stock_detail_cyq,
            get_stock_detail_realtime,
            get_stock_similarity_page,
            get_strategy_statistics_page,
            get_strategy_paper_validation_defaults,
            validate_strategy_paper_validation_template_expressions,
            run_strategy_paper_validation,
            get_scene_statistics_page,
            get_strategy_statistics_detail,
            get_strategy_triggered_stocks,
            get_scene_layer_backtest_defaults,
            get_rule_layer_backtest_defaults,
            get_market_analysis,
            get_market_contribution,
            run_rank_layer_backtest,
            run_scene_layer_backtest,
            run_rule_layer_backtest,
            run_transient_rank_layer_backtest,
            run_transient_scene_layer_backtest,
            run_transient_rule_layer_backtest,
            run_rule_expression_validation,
            get_ranking_compute_status,
            preview_ranking_score_calculation_warnings,
            run_ranking_score_calculation,
            run_concept_performance_compute,
            run_cyq_compute,
            run_cyq_chen_compute,
            run_cyq_chen_single_stock_test,
            get_cyq_chen_strategy_page,
            save_cyq_chen_strategy_file,
            check_cyq_chen_strategy_file_draft,
            backup_cyq_chen_strategy_file,
            import_cyq_chen_strategy_backup,
            delete_cyq_chen_strategy_backup,
            export_cyq_chen_active_strategy_file,
            export_cyq_chen_strategy_backup_file,
            get_cyq_chen_strategy_backup_diff,
            activate_cyq_chen_strategy_backup,
            run_ranking_tiebreak_fill,
            get_strategy_manage_page,
            check_strategy_manage_scene_draft,
            create_strategy_manage_scene,
            update_strategy_manage_scene,
            remove_strategy_manage_scene,
            check_strategy_manage_rule_draft,
            create_strategy_manage_rule,
            remove_strategy_manage_rules,
            update_strategy_manage_rule,
            save_strategy_manage_refactor_file,
            get_stock_pick_options,
            get_chart_indicator_settings,
            validate_chart_indicator_settings,
            save_chart_indicator_settings,
            reset_chart_indicator_settings,
            run_expression_stock_pick,
            run_concept_stock_pick,
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
