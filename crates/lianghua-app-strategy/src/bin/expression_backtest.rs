use std::{env, fs, time::Instant};

use lianghua_app_strategy::statistics::{
    RuleExpressionValidationManualStrategy, RuleValidationUnknownConfig,
    run_rule_expression_validation,
};
use lianghua_backtest::simulate::BacktestReturnMetric;
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Request {
    source_path: String,
    #[serde(default)]
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
    return_metric: Option<BacktestReturnMetric>,
    manual_strategy: Option<RuleExpressionValidationManualStrategy>,
    unknown_configs: Option<Vec<RuleValidationUnknownConfig>>,
    sample_limit_per_group: Option<usize>,
    board: Option<String>,
    exclude_st_board: Option<bool>,
    total_mv_min: Option<f64>,
    total_mv_max: Option<f64>,
    walk_forward_folds: Option<usize>,
    core_rule_names: Option<Vec<String>>,
}

fn main() -> Result<(), String> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    if args.len() != 2 {
        return Err("用法: expression_backtest <请求.json> <输出.json>".to_string());
    }
    let request: Request =
        serde_json::from_str(&fs::read_to_string(&args[0]).map_err(|error| error.to_string())?)
            .map_err(|error| error.to_string())?;
    let started = Instant::now();
    let result = run_rule_expression_validation(
        request.source_path,
        request.import_rule_name,
        request.when,
        request.scope_way,
        request.scope_windows,
        request.stock_adj_type,
        request.index_ts_code,
        request.index_beta,
        request.concept_beta,
        request.industry_beta,
        request.start_date,
        request.end_date,
        request.min_samples_per_rule_day,
        request.min_listed_trade_days,
        request.backtest_period,
        request.return_metric,
        request.manual_strategy,
        request.unknown_configs,
        request.sample_limit_per_group,
        request.board,
        request.exclude_st_board,
        request.total_mv_min,
        request.total_mv_max,
        request.walk_forward_folds,
        request.core_rule_names,
    )?;
    fs::write(
        &args[1],
        serde_json::to_vec_pretty(&result).map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())?;
    println!(
        "完成 {} 组表达式回测，耗时 {:.1}s，输出 {}",
        result.combo_results.len(),
        started.elapsed().as_secs_f64(),
        args[1]
    );
    Ok(())
}
