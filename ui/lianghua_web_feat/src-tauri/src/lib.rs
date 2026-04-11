mod data_download_bridge;
mod managed_source_bridge;

use lianghua_rs::ui_tools_feat::{
    concept_stock_pick::{
        StockPickResultData as ConceptStockPickResultData,
        run_concept_stock_pick as core_run_concept_stock_pick,
    },
    data_viewer::{StockLookupRow, list_stock_lookup_rows as core_list_stock_lookup_rows},
    expression_stock_pick::{
        StockPickResultData as ExpressionStockPickResultData,
        run_expression_stock_pick as core_run_expression_stock_pick,
    },
    ranking_compute::{
        RankComputeRunResult, RankComputeStatus,
        get_ranking_compute_status as core_get_ranking_compute_status,
        run_ranking_score_calculation as core_run_ranking_score_calculation,
        run_ranking_tiebreak_fill as core_run_ranking_tiebreak_fill,
    },
    stock_pick::{StockPickOptionsData, get_stock_pick_options as core_get_stock_pick_options},
    strategy_manage::{
        StrategyManagePageData, StrategyManageRuleDraft, StrategyManageSceneDraft,
        check_strategy_manage_scene_draft as core_check_strategy_manage_scene_draft,
        check_strategy_manage_rule_draft as core_check_strategy_manage_rule_draft,
        create_strategy_manage_scene as core_create_strategy_manage_scene,
        create_strategy_manage_rule as core_create_strategy_manage_rule,
        get_strategy_manage_page as core_get_strategy_manage_page,
        remove_strategy_manage_rules as core_remove_strategy_manage_rules,
        update_strategy_manage_scene as core_update_strategy_manage_scene,
        update_strategy_manage_rule as core_update_strategy_manage_rule,
    },
};

use data_download_bridge::{
    get_data_download_status, get_indicator_manage_page, run_concept_performance_repair,
    run_data_download, run_missing_stock_repair, run_ths_concept_download,
    save_indicator_manage_page,
};
use managed_source_bridge::{
    allow_import_path, copy_import_file_to_appdata, export_managed_source_directory,
    export_managed_source_directory_mobile, export_managed_source_file,
    preview_managed_source_dataset, preview_managed_source_stock_data,
};

#[cfg(target_os = "android")]
use jni::{JNIEnv, objects::JObject, sys::jboolean};

#[cfg(target_os = "android")]
use rustls_platform_verifier;

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

#[tauri::command]
fn list_stock_lookup_rows(source_path: String) -> Result<Vec<StockLookupRow>, String> {
    core_list_stock_lookup_rows(&source_path)
}

#[tauri::command]
fn get_stock_pick_options(source_path: String) -> Result<StockPickOptionsData, String> {
    core_get_stock_pick_options(&source_path)
}

#[tauri::command]
fn get_ranking_compute_status(source_path: String) -> Result<RankComputeStatus, String> {
    core_get_ranking_compute_status(&source_path)
}

#[tauri::command]
fn get_strategy_manage_page(source_path: String) -> Result<StrategyManagePageData, String> {
    core_get_strategy_manage_page(&source_path)
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
async fn run_ranking_score_calculation(
    source_path: String,
    start_date: String,
    end_date: String,
) -> Result<RankComputeRunResult, String> {
    tauri::async_runtime::spawn_blocking(move || {
        core_run_ranking_score_calculation(&source_path, &start_date, &end_date)
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
async fn run_ranking_tiebreak_fill(source_path: String) -> Result<RankComputeRunResult, String> {
    tauri::async_runtime::spawn_blocking(move || core_run_ranking_tiebreak_fill(&source_path))
        .await
        .map_err(|error| error.to_string())?
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
) -> Result<ExpressionStockPickResultData, String> {
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
) -> Result<ConceptStockPickResultData, String> {
    core_run_concept_stock_pick(
        &source_path,
        board,
        trade_date,
        include_concepts,
        exclude_concepts,
        match_mode,
    )
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
            save_indicator_manage_page,
            run_data_download,
            run_missing_stock_repair,
            run_ths_concept_download,
            run_concept_performance_repair,
            list_stock_lookup_rows,
            get_ranking_compute_status,
            run_ranking_score_calculation,
            run_ranking_tiebreak_fill,
            get_strategy_manage_page,
            check_strategy_manage_scene_draft,
            create_strategy_manage_scene,
            update_strategy_manage_scene,
            check_strategy_manage_rule_draft,
            create_strategy_manage_rule,
            remove_strategy_manage_rules,
            update_strategy_manage_rule,
            get_stock_pick_options,
            run_expression_stock_pick,
            run_concept_stock_pick
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
