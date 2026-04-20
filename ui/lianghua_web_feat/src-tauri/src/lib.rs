mod data_download_bridge;
mod managed_source_bridge;

use std::fs;

use lianghua_rs::ui_tools_feat::{
    concept_stock_pick::{
        StockPickResultData as ConceptStockPickResultData,
        run_concept_stock_pick as core_run_concept_stock_pick,
    },
    data_viewer::{StockLookupRow, list_stock_lookup_rows as core_list_stock_lookup_rows},
    details::{
        StockDetailPageData, StockDetailRealtimeData, StockDetailStrategySnapshotData,
        get_stock_detail_page as core_get_stock_detail_page,
        get_stock_detail_realtime as core_get_stock_detail_realtime,
        get_stock_detail_strategy_snapshot as core_get_stock_detail_strategy_snapshot,
    },
    expression_stock_pick::{
        StockPickResultData as ExpressionStockPickResultData,
        run_expression_stock_pick as core_run_expression_stock_pick,
    },
    intraday_monitor::{
        IntradayMonitorPageData, IntradayMonitorRankModeConfig, IntradayMonitorRow,
        IntradayMonitorTemplate, IntradayMonitorTemplateValidationData,
        get_intraday_monitor_page as core_get_intraday_monitor_page,
        refresh_intraday_monitor_template_tags as core_refresh_intraday_monitor_template_tags,
        refresh_intraday_monitor_realtime as core_refresh_intraday_monitor_realtime,
        validate_intraday_monitor_template_expression as core_validate_intraday_monitor_template_expression,
    },
    overview::{
        SceneOverviewPageData,
        get_scene_rank_overview_page as core_get_scene_rank_overview_page,
        get_scene_rank_trade_date_options as core_get_scene_rank_trade_date_options,
    },
    overview_classic::{
        OverviewPageData, OverviewRow,
        get_rank_overview as core_get_rank_overview,
        get_rank_overview_page as core_get_rank_overview_page,
        get_rank_trade_date_options as core_get_rank_trade_date_options,
    },
    ranking_compute::{
        ConceptPerformanceComputeResult, RankComputeRunResult, RankComputeStatus,
        get_ranking_compute_status as core_get_ranking_compute_status,
        run_concept_performance_compute as core_run_concept_performance_compute,
        run_ranking_score_calculation as core_run_ranking_score_calculation,
        run_ranking_tiebreak_fill as core_run_ranking_tiebreak_fill,
    },
    stock_similarity::{
        StockSimilarityPageData, get_stock_similarity_page as core_get_stock_similarity_page,
    },
    stock_pick::{StockPickOptionsData, get_stock_pick_options as core_get_stock_pick_options},
    strategy_manage::{
        StrategyManagePageData, StrategyManageRefactorDraft, StrategyManageRuleDraft, StrategyManageSceneDraft,
        check_strategy_manage_scene_draft as core_check_strategy_manage_scene_draft,
        check_strategy_manage_rule_draft as core_check_strategy_manage_rule_draft,
        create_strategy_manage_scene as core_create_strategy_manage_scene,
        create_strategy_manage_rule as core_create_strategy_manage_rule,
        get_strategy_manage_page as core_get_strategy_manage_page,
        remove_strategy_manage_scene as core_remove_strategy_manage_scene,
        remove_strategy_manage_rules as core_remove_strategy_manage_rules,
        save_strategy_manage_refactor_file as core_save_strategy_manage_refactor_file,
        update_strategy_manage_scene as core_update_strategy_manage_scene,
        update_strategy_manage_rule as core_update_strategy_manage_rule,
    },
    statistics::{
        MarketAnalysisData, MarketContributionData, RuleExpressionValidationData,
        RuleExpressionValidationManualStrategy,
        RuleLayerBacktestData, RuleLayerBacktestDefaultsData, RuleValidationUnknownConfig,
        SceneLayerBacktestData, SceneLayerBacktestDefaultsData, SceneStatisticsPageData,
        StrategyStatisticsDetailData, StrategyStatisticsPageData, TriggeredStockRow,
        get_market_analysis as core_get_market_analysis,
        get_market_contribution as core_get_market_contribution,
        get_rule_layer_backtest_defaults as core_get_rule_layer_backtest_defaults,
        get_scene_layer_backtest_defaults as core_get_scene_layer_backtest_defaults,
        get_scene_statistics_page as core_get_scene_statistics_page,
        get_strategy_statistics_detail as core_get_strategy_statistics_detail,
        get_strategy_statistics_page as core_get_strategy_statistics_page,
        get_strategy_triggered_stocks as core_get_strategy_triggered_stocks,
        run_rule_expression_validation as core_run_rule_expression_validation,
        run_rule_layer_backtest as core_run_rule_layer_backtest,
        run_scene_layer_backtest as core_run_scene_layer_backtest,
    },
    watch_observe::{
        WatchObserveRow as CoreWatchObserveRow, WatchObserveSnapshotData,
        WatchObserveStoredRow, hydrate_watch_observe_rows as core_hydrate_watch_observe_rows,
        normalize_trade_date as core_normalize_watch_observe_trade_date,
        normalize_ts_code as core_normalize_watch_observe_ts_code,
        refresh_watch_observe_rows as core_refresh_watch_observe_rows,
    },
};
use serde::{Deserialize, Serialize};

use data_download_bridge::{
    get_data_download_status, get_indicator_manage_page, run_concept_most_related_repair,
    run_concept_performance_repair, run_data_download, run_missing_stock_repair,
    run_stock_data_indicator_columns_delete, run_stock_data_indicator_columns_rebuild,
    run_ths_concept_download, save_indicator_manage_page,
};
use managed_source_bridge::{
    activate_managed_strategy_backup, allow_import_path, backup_managed_active_strategy,
    copy_import_file_to_appdata, create_managed_empty_strategy_backup,
    delete_managed_strategy_backup,
    export_managed_source_directory, export_managed_source_directory_mobile,
    export_managed_source_file, export_managed_strategy_backup_file,
    export_managed_strategy_bundle, get_managed_strategy_assets_status,
    import_managed_source_zip, import_managed_strategy_backup, preview_managed_source_dataset,
    preview_managed_source_stock_data,
    update_managed_strategy_backup_description,
};

#[cfg(target_os = "android")]
use jni::{JNIEnv, objects::JObject, sys::jboolean};

#[cfg(target_os = "android")]
use rustls_platform_verifier;
use tauri::Manager;

const WATCH_OBSERVE_STORAGE_FILE: &str = "watch_observe.json";

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
fn get_ranking_compute_status(
    source_path: String,
    strategy_path: Option<String>,
) -> Result<RankComputeStatus, String> {
    core_get_ranking_compute_status(&source_path, strategy_path.as_deref())
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
fn refresh_intraday_monitor_realtime(
    source_path: String,
    rows: Vec<IntradayMonitorRow>,
    templates: Vec<IntradayMonitorTemplate>,
    rank_mode_configs: Vec<IntradayMonitorRankModeConfig>,
) -> Result<IntradayMonitorPageData, String> {
    core_refresh_intraday_monitor_realtime(&source_path, rows, templates, rank_mode_configs)
}

#[tauri::command]
fn refresh_intraday_monitor_template_tags(
    source_path: String,
    rows: Vec<IntradayMonitorRow>,
    templates: Vec<IntradayMonitorTemplate>,
    rank_mode_configs: Vec<IntradayMonitorRankModeConfig>,
) -> Result<IntradayMonitorPageData, String> {
    core_refresh_intraday_monitor_template_tags(&source_path, rows, templates, rank_mode_configs)
}

#[tauri::command]
fn validate_intraday_monitor_template_expression(
    expression: String,
) -> Result<IntradayMonitorTemplateValidationData, String> {
    core_validate_intraday_monitor_template_expression(expression)
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
) -> Result<MarketAnalysisData, String> {
    tauri::async_runtime::spawn_blocking(move || {
        core_get_market_analysis(
            source_path,
            lookback_period,
            reference_trade_date,
            board,
            exclude_st_board,
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
    backtest_period: Option<usize>,
) -> Result<SceneLayerBacktestData, String> {
    tauri::async_runtime::spawn_blocking(move || {
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
            backtest_period,
        )
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
    backtest_period: Option<usize>,
) -> Result<RuleLayerBacktestData, String> {
    tauri::async_runtime::spawn_blocking(move || {
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
            backtest_period,
        )
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
    backtest_period: Option<usize>,
    manual_strategy: Option<RuleExpressionValidationManualStrategy>,
    unknown_configs: Option<Vec<RuleValidationUnknownConfig>>,
    sample_limit_per_group: Option<usize>,
) -> Result<RuleExpressionValidationData, String> {
    tauri::async_runtime::spawn_blocking(move || {
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
            backtest_period,
            manual_strategy,
            unknown_configs,
            sample_limit_per_group,
        )
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
    source_path: String,
    strategy_path: Option<String>,
    start_date: String,
    end_date: String,
) -> Result<RankComputeRunResult, String> {
    tauri::async_runtime::spawn_blocking(move || {
        core_run_ranking_score_calculation(
            &source_path,
            strategy_path.as_deref(),
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
    tauri::async_runtime::spawn_blocking(move || {
        core_run_concept_performance_compute(&source_path)
    })
    .await
    .map_err(|error| error.to_string())?
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
    core_hydrate_watch_observe_rows(
        source_path.as_deref(),
        &stored_rows,
        reference_trade_date,
    )
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
    core_refresh_watch_observe_rows(
        source_path.as_deref(),
        &stored_rows,
        reference_trade_date,
    )
}

#[tauri::command]
fn upsert_watch_observe_row(
    app: tauri::AppHandle,
    source_path: Option<String>,
    row: WatchObserveUpsertPayload,
) -> Result<Vec<CoreWatchObserveRow>, String> {
    let incoming = normalize_watch_observe_upsert_payload(row)?;
    let mut rows = read_watch_observe_storage(&app)?;
    if let Some(existing_index) = rows.iter().position(|item| item.ts_code == incoming.ts_code) {
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
    let normalized_ts_code = core_normalize_watch_observe_ts_code(&ts_code)
        .ok_or_else(|| "自选代码无效".to_string())?;
    let mut rows = read_watch_observe_storage(&app)?;
    let Some(existing_row) = rows.iter_mut().find(|item| item.ts_code == normalized_ts_code) else {
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
            get_stock_detail_realtime,
            get_stock_similarity_page,
            get_strategy_statistics_page,
            get_scene_statistics_page,
            get_strategy_statistics_detail,
            get_strategy_triggered_stocks,
            get_scene_layer_backtest_defaults,
            get_rule_layer_backtest_defaults,
            get_market_analysis,
            get_market_contribution,
            run_scene_layer_backtest,
            run_rule_layer_backtest,
            run_rule_expression_validation,
            get_ranking_compute_status,
            run_ranking_score_calculation,
            run_concept_performance_compute,
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
