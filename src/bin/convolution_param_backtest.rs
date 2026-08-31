use std::cmp::Ordering;
use std::env;

use duckdb::{Connection, params};
use lianghua_data::data::{result_db_path, source_db_path};

const DEFAULT_LOOKBACK_DAYS: usize = 500;
const DEFAULT_TOP_K: usize = 20;
const DEFAULT_HOLDING_DAYS: usize = 5;
const DEFAULT_TRAIN_RATIO: f64 = 0.7;
const DEFAULT_INDEX: &str = "399300.SZ";
const STOCK_ADJ_TYPE: &str = "qfq";
const WINDOWS: [usize; 8] = [1, 2, 3, 5, 10, 15, 20, 30];
const DECAYS: [f64; 5] = [0.25, 0.5, 0.7, 0.85, 1.0];
const HYBRID_WINDOWS: [usize; 3] = [10, 20, 30];
const LONG_SHARES: [f64; 4] = [0.1, 0.2, 0.3, 0.5];

#[derive(Debug, Clone)]
struct KernelCandidate {
    label: String,
    window: usize,
    decay: f64,
    weights: Vec<f64>,
}

impl KernelCandidate {
    fn label(&self) -> &str {
        &self.label
    }

    fn normalized_weights(&self) -> Vec<f64> {
        let sum = self.weights.iter().sum::<f64>();
        self.weights.iter().map(|weight| weight / sum).collect()
    }
}

#[derive(Debug, Clone)]
struct DailyReturn {
    trade_date: String,
    residual_return: f64,
}

#[derive(Debug, Clone, Default)]
struct PeriodMetrics {
    day_count: usize,
    mean: Option<f64>,
    median: Option<f64>,
    hac_t: Option<f64>,
}

#[derive(Debug, Clone)]
struct CandidateEvaluation {
    candidate: KernelCandidate,
    train: PeriodMetrics,
    validation: PeriodMetrics,
    train_delta: PeriodMetrics,
    validation_delta: PeriodMetrics,
}

fn usage() -> &'static str {
    "用法: cargo run --bin convolution_param_backtest -- \\
     <source_dir> [lookback_days] [top_k] [holding_days] [train_ratio] [index_ts_code]\n\
     示例: cargo run --bin convolution_param_backtest -- \\
     /path/to/source 500 20 5 0.7 399300.SZ"
}

fn parse_arg<T>(args: &[String], index: usize, default: T, label: &str) -> Result<T, String>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    args.get(index)
        .map(|value| {
            value
                .parse::<T>()
                .map_err(|e| format!("解析{label}失败: {value}, err={e}"))
        })
        .transpose()
        .map(|value| value.unwrap_or(default))
}

fn kernel_candidates() -> Vec<KernelCandidate> {
    let mut candidates = vec![KernelCandidate {
        label: "raw".to_string(),
        window: 1,
        decay: 0.0,
        weights: vec![1.0],
    }];
    for window in WINDOWS.into_iter().filter(|window| *window > 1) {
        for decay in DECAYS {
            candidates.push(KernelCandidate {
                label: format!("W{window}-D{decay:.2}"),
                window,
                decay,
                weights: (0..window).map(|lag| decay.powi(lag as i32)).collect(),
            });
        }
    }
    let short_weights = [1.0, 0.7, 0.49];
    let short_sum = short_weights.iter().sum::<f64>();
    for window in HYBRID_WINDOWS {
        for long_share in LONG_SHARES {
            let mut weights = vec![long_share / window as f64; window];
            for (lag, short_weight) in short_weights.iter().enumerate() {
                weights[lag] += (1.0 - long_share) * short_weight / short_sum;
            }
            candidates.push(KernelCandidate {
                label: format!("H{window}-L{:.0}", long_share * 100.0),
                window,
                decay: long_share,
                weights,
            });
        }
    }
    candidates
}

fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn load_score_dates(conn: &Connection, lookback_days: usize) -> Result<Vec<String>, String> {
    let mut stmt = conn
        .prepare(
            "SELECT DISTINCT trade_date
             FROM score_summary
             WHERE total_score IS NOT NULL
             ORDER BY trade_date DESC
             LIMIT ?",
        )
        .map_err(|e| format!("预编译评分日期查询失败: {e}"))?;
    let rows = stmt
        .query_map(params![lookback_days as i64], |row| row.get::<_, String>(0))
        .map_err(|e| format!("查询评分日期失败: {e}"))?;
    let mut dates = rows
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("读取评分日期失败: {e}"))?;
    dates.reverse();
    Ok(dates)
}

fn resolve_extended_start(
    conn: &Connection,
    start_date: &str,
    history_days: usize,
) -> Result<String, String> {
    if history_days == 0 {
        return Ok(start_date.to_string());
    }
    let mut stmt = conn
        .prepare(
            "SELECT DISTINCT trade_date
             FROM score_summary
             WHERE trade_date < ? AND total_score IS NOT NULL
             ORDER BY trade_date DESC
             LIMIT ?",
        )
        .map_err(|e| format!("预编译卷积预热日期查询失败: {e}"))?;
    let rows = stmt
        .query_map(params![start_date, history_days as i64], |row| {
            row.get::<_, String>(0)
        })
        .map_err(|e| format!("查询卷积预热日期失败: {e}"))?;
    let dates = rows
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("读取卷积预热日期失败: {e}"))?;
    Ok(dates
        .last()
        .cloned()
        .unwrap_or_else(|| start_date.to_string()))
}

struct BaseTableInput<'a> {
    source_db: &'a str,
    start_date: &'a str,
    extended_start: &'a str,
    end_date: &'a str,
    holding_days: usize,
    index_ts_code: &'a str,
    max_window: usize,
}

fn build_backtest_base(conn: &Connection, input: &BaseTableInput<'_>) -> Result<usize, String> {
    let attach_sql = format!(
        "ATTACH {} AS market_db (READ_ONLY)",
        sql_string_literal(input.source_db)
    );
    conn.execute(&attach_sql, [])
        .map_err(|e| format!("挂载行情数据库失败: {e}"))?;

    let lag_columns = (1..input.max_window)
        .map(|lag| {
            format!(
                "LAG(TRY_CAST(total_score AS DOUBLE), {lag}) OVER \
                 (PARTITION BY ts_code ORDER BY trade_date) AS score_{lag}"
            )
        })
        .collect::<Vec<_>>()
        .join(",\n                ");
    let lag_columns = if lag_columns.is_empty() {
        String::new()
    } else {
        format!(",\n                {lag_columns}")
    };
    let sql = format!(
        r#"
        CREATE TEMP TABLE convolution_backtest_base AS
        WITH score_lags AS (
            SELECT
                ts_code,
                trade_date,
                TRY_CAST(total_score AS DOUBLE) AS score_0
                {lag_columns}
            FROM score_summary
            WHERE trade_date >= {extended_start}
              AND trade_date <= {end_date}
              AND TRY_CAST(total_score AS DOUBLE) IS NOT NULL
        ),
        stock_window AS (
            SELECT
                ts_code,
                trade_date,
                SUM(TRY_CAST(pct_chg AS DOUBLE)) OVER (
                    PARTITION BY ts_code
                    ORDER BY trade_date
                    ROWS BETWEEN 1 FOLLOWING AND {holding_days} FOLLOWING
                ) AS forward_stock_return,
                COUNT(TRY_CAST(pct_chg AS DOUBLE)) OVER (
                    PARTITION BY ts_code
                    ORDER BY trade_date
                    ROWS BETWEEN 1 FOLLOWING AND {holding_days} FOLLOWING
                ) AS forward_count
            FROM market_db.stock_data
            WHERE adj_type = {stock_adj_type}
              AND trade_date >= {start_date}
        ),
        index_window AS (
            SELECT
                trade_date,
                SUM(TRY_CAST(pct_chg AS DOUBLE)) OVER (
                    ORDER BY trade_date
                    ROWS BETWEEN 1 FOLLOWING AND {holding_days} FOLLOWING
                ) AS forward_index_return,
                COUNT(TRY_CAST(pct_chg AS DOUBLE)) OVER (
                    ORDER BY trade_date
                    ROWS BETWEEN 1 FOLLOWING AND {holding_days} FOLLOWING
                ) AS forward_count
            FROM market_db.stock_data
            WHERE adj_type = 'ind'
              AND ts_code = {index_ts_code}
              AND trade_date >= {start_date}
        )
        SELECT
            scores.*,
            stock.forward_stock_return - idx.forward_index_return AS forward_residual_return
        FROM score_lags scores
        JOIN stock_window stock
          ON stock.ts_code = scores.ts_code
         AND stock.trade_date = scores.trade_date
        JOIN index_window idx
          ON idx.trade_date = scores.trade_date
        WHERE scores.trade_date >= {start_date}
          AND scores.trade_date <= {end_date}
          AND stock.forward_count = {holding_days}
          AND idx.forward_count = {holding_days}
        "#,
        lag_columns = lag_columns,
        extended_start = sql_string_literal(input.extended_start),
        start_date = sql_string_literal(input.start_date),
        end_date = sql_string_literal(input.end_date),
        holding_days = input.holding_days,
        stock_adj_type = sql_string_literal(STOCK_ADJ_TYPE),
        index_ts_code = sql_string_literal(input.index_ts_code),
    );
    conn.execute(&sql, [])
        .map_err(|e| format!("构建卷积回测样本失败: {e}"))?;
    let sample_count = conn
        .query_row(
            "SELECT COUNT(*) FROM convolution_backtest_base",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map(|count| count.max(0) as usize)
        .map_err(|e| format!("统计卷积回测样本失败: {e}"))?;
    if sample_count > 0 {
        return Ok(sample_count);
    }

    let score_count = conn
        .query_row(
            &format!(
                "SELECT COUNT(*) FROM score_summary WHERE trade_date >= {} AND trade_date <= {}",
                sql_string_literal(input.start_date),
                sql_string_literal(input.end_date)
            ),
            [],
            |row| row.get::<_, i64>(0),
        )
        .unwrap_or_default();
    let stock_count = conn
        .query_row(
            &format!(
                "SELECT COUNT(*) FROM market_db.stock_data WHERE adj_type = {} AND trade_date >= {}",
                sql_string_literal(STOCK_ADJ_TYPE),
                sql_string_literal(input.start_date)
            ),
            [],
            |row| row.get::<_, i64>(0),
        )
        .unwrap_or_default();
    let index_count = conn
        .query_row(
            &format!(
                "SELECT COUNT(*) FROM market_db.stock_data WHERE adj_type = 'ind' AND ts_code = {} AND trade_date >= {}",
                sql_string_literal(input.index_ts_code),
                sql_string_literal(input.start_date)
            ),
            [],
            |row| row.get::<_, i64>(0),
        )
        .unwrap_or_default();
    let index_adj_types = conn
        .query_row(
            &format!(
                "SELECT COALESCE(string_agg(DISTINCT adj_type, ','), '-') FROM market_db.stock_data WHERE ts_code = {}",
                sql_string_literal(input.index_ts_code)
            ),
            [],
            |row| row.get::<_, String>(0),
        )
        .unwrap_or_else(|_| "-".to_string());
    let available_indexes = conn
        .query_row(
            "SELECT COALESCE(string_agg(ts_code, ','), '-') FROM (SELECT DISTINCT ts_code FROM market_db.stock_data WHERE adj_type = 'ind' ORDER BY ts_code LIMIT 20)",
            [],
            |row| row.get::<_, String>(0),
        )
        .unwrap_or_else(|_| "-".to_string());
    let joined_count = conn
        .query_row(
            &format!(
                "SELECT COUNT(*) FROM score_summary scores JOIN market_db.stock_data market ON market.ts_code = scores.ts_code AND market.trade_date = scores.trade_date WHERE scores.trade_date >= {} AND scores.trade_date <= {} AND market.adj_type = {}",
                sql_string_literal(input.start_date),
                sql_string_literal(input.end_date),
                sql_string_literal(STOCK_ADJ_TYPE)
            ),
            [],
            |row| row.get::<_, i64>(0),
        )
        .unwrap_or_default();
    Err(format!(
        "卷积回测样本为空: score_rows={score_count}, qfq_market_rows={stock_count}, index_rows={index_count}, index_adj_types={index_adj_types}, available_indexes={available_indexes}, score_market_join={joined_count}"
    ))
}

fn load_candidate_daily_returns(
    conn: &Connection,
    candidate: &KernelCandidate,
    top_k: usize,
) -> Result<Vec<DailyReturn>, String> {
    let score_expression = candidate
        .weights
        .iter()
        .enumerate()
        .map(|(lag, weight)| format!("score_{lag} * {weight:.17}"))
        .collect::<Vec<_>>()
        .join(" + ");
    let complete_window = (0..candidate.window)
        .map(|lag| format!("score_{lag} IS NOT NULL"))
        .collect::<Vec<_>>()
        .join(" AND ");
    let sql = format!(
        r#"
        WITH ranked AS (
            SELECT
                trade_date,
                forward_residual_return,
                ROW_NUMBER() OVER (
                    PARTITION BY trade_date
                    ORDER BY ({score_expression}) DESC, ts_code ASC
                ) AS convolution_rank
            FROM convolution_backtest_base
            WHERE {complete_window}
              AND isfinite(forward_residual_return)
        )
        SELECT
            trade_date,
            AVG(forward_residual_return) AS daily_return
        FROM ranked
        WHERE convolution_rank <= ?
        GROUP BY trade_date
        ORDER BY trade_date ASC
        "#
    );
    let mut stmt = conn
        .prepare(&sql)
        .map_err(|e| format!("预编译候选核{}失败: {e}", candidate.label()))?;
    let rows = stmt
        .query_map(params![top_k as i64], |row| {
            Ok(DailyReturn {
                trade_date: row.get(0)?,
                residual_return: row.get(1)?,
            })
        })
        .map_err(|e| format!("回测候选核{}失败: {e}", candidate.label()))?;
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("读取候选核{}结果失败: {e}", candidate.label()))
}

fn mean(values: &[f64]) -> Option<f64> {
    (!values.is_empty()).then(|| values.iter().sum::<f64>() / values.len() as f64)
}

fn median(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let middle = sorted.len() / 2;
    Some(if sorted.len().is_multiple_of(2) {
        (sorted[middle - 1] + sorted[middle]) / 2.0
    } else {
        sorted[middle]
    })
}

fn newey_west_t(values: &[f64], max_lag: usize) -> Option<f64> {
    if values.len() < 2 {
        return None;
    }
    let avg = mean(values)?;
    let centered = values.iter().map(|value| value - avg).collect::<Vec<_>>();
    let count = centered.len();
    let lag_count = max_lag.min(count - 1);
    let mut long_run_variance =
        centered.iter().map(|value| value * value).sum::<f64>() / count as f64;
    for lag in 1..=lag_count {
        let covariance = centered
            .iter()
            .skip(lag)
            .zip(centered.iter())
            .map(|(current, previous)| current * previous)
            .sum::<f64>()
            / count as f64;
        let bartlett_weight = 1.0 - lag as f64 / (lag_count + 1) as f64;
        long_run_variance += 2.0 * bartlett_weight * covariance;
    }
    if !long_run_variance.is_finite() || long_run_variance <= 1e-12 {
        return None;
    }
    Some(avg / (long_run_variance / count as f64).sqrt())
}

fn summarize(points: &[&DailyReturn], holding_days: usize) -> PeriodMetrics {
    let values = points
        .iter()
        .map(|point| point.residual_return)
        .filter(|value| value.is_finite())
        .collect::<Vec<_>>();
    PeriodMetrics {
        day_count: values.len(),
        mean: mean(&values),
        median: median(&values),
        hac_t: newey_west_t(&values, holding_days.saturating_sub(1)),
    }
}

fn paired_differences(candidate: &[DailyReturn], baseline: &[DailyReturn]) -> Vec<DailyReturn> {
    let baseline_by_date = baseline
        .iter()
        .map(|point| (point.trade_date.as_str(), point.residual_return))
        .collect::<std::collections::HashMap<_, _>>();
    candidate
        .iter()
        .filter_map(|point| {
            baseline_by_date
                .get(point.trade_date.as_str())
                .map(|baseline_return| DailyReturn {
                    trade_date: point.trade_date.clone(),
                    residual_return: point.residual_return - baseline_return,
                })
        })
        .collect()
}

fn metric_or_min(value: Option<f64>) -> f64 {
    value
        .filter(|value| value.is_finite())
        .unwrap_or(f64::NEG_INFINITY)
}

fn compare_evaluations(left: &CandidateEvaluation, right: &CandidateEvaluation) -> Ordering {
    metric_or_min(right.train_delta.hac_t)
        .total_cmp(&metric_or_min(left.train_delta.hac_t))
        .then_with(|| {
            metric_or_min(right.train_delta.mean).total_cmp(&metric_or_min(left.train_delta.mean))
        })
        .then_with(|| {
            metric_or_min(right.train_delta.median)
                .total_cmp(&metric_or_min(left.train_delta.median))
        })
        .then_with(|| left.candidate.window.cmp(&right.candidate.window))
        .then_with(|| left.candidate.decay.total_cmp(&right.candidate.decay))
}

fn format_metric(value: Option<f64>, precision: usize) -> String {
    value
        .map(|value| format!("{value:.precision$}"))
        .unwrap_or_else(|| "-".to_string())
}

fn main() -> Result<(), String> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    if args.is_empty() || args[0] == "--help" || args[0] == "-h" {
        println!("{}", usage());
        return Ok(());
    }

    let source_dir = &args[0];
    let lookback_days = parse_arg(&args, 1, DEFAULT_LOOKBACK_DAYS, "回看交易日数")?;
    let top_k = parse_arg(&args, 2, DEFAULT_TOP_K, "Top-K")?;
    let holding_days = parse_arg(&args, 3, DEFAULT_HOLDING_DAYS, "持有交易日数")?;
    let train_ratio = parse_arg(&args, 4, DEFAULT_TRAIN_RATIO, "训练集比例")?;
    let index_ts_code = args.get(5).map(String::as_str).unwrap_or(DEFAULT_INDEX);
    if lookback_days < 60 || top_k == 0 || holding_days == 0 {
        return Err("lookback_days必须>=60，top_k和holding_days必须>=1".to_string());
    }
    if !(0.5..=0.9).contains(&train_ratio) {
        return Err("训练集比例必须位于[0.5, 0.9]".to_string());
    }

    let result_db = result_db_path(source_dir);
    let source_db = source_db_path(source_dir);
    if !result_db.exists() || !source_db.exists() {
        return Err(format!(
            "数据库缺失: result={}, source={}",
            result_db.display(),
            source_db.display()
        ));
    }
    let conn = Connection::open(&result_db)
        .map_err(|e| format!("打开评分数据库失败: {}, err={e}", result_db.display()))?;
    let dates = load_score_dates(&conn, lookback_days)?;
    if dates.len() < 60 {
        return Err(format!("评分交易日不足60日，实际只有{}日", dates.len()));
    }
    let split_index = ((dates.len() as f64 * train_ratio).floor() as usize)
        .clamp(holding_days + 1, dates.len() - 1);
    let train_end_index = split_index
        .checked_sub(holding_days + 1)
        .ok_or_else(|| "训练区间不足以执行持有期隔离".to_string())?;
    let start_date = dates.first().expect("非空日期");
    let end_date = dates.last().expect("非空日期");
    let train_end = &dates[train_end_index];
    let validation_start = &dates[split_index];
    let max_window = *WINDOWS.iter().max().expect("非空窗口集合");
    let extended_start = resolve_extended_start(&conn, start_date, max_window - 1)?;
    let source_db_str = source_db
        .to_str()
        .ok_or_else(|| "行情数据库路径不是有效UTF-8".to_string())?;

    println!("正在构建回测样本……");
    let sample_count = build_backtest_base(
        &conn,
        &BaseTableInput {
            source_db: source_db_str,
            start_date,
            extended_start: &extended_start,
            end_date,
            holding_days,
            index_ts_code,
            max_window,
        },
    )?;
    println!("区间: {start_date}..{end_date}, 训练截止: {train_end}, 验证开始: {validation_start}");
    println!("样本: {sample_count}, Top-{top_k}, 持有: {holding_days}日, 基准: {index_ts_code}");

    let candidates = kernel_candidates();
    let baseline_daily = load_candidate_daily_returns(&conn, &candidates[0], top_k)?;
    let candidate_count = candidates.len();
    let mut evaluations = Vec::with_capacity(candidates.len());
    for (index, candidate) in candidates.into_iter().enumerate() {
        println!(
            "回测候选 {}/{}: {}",
            index + 1,
            candidate_count,
            candidate.label()
        );
        let daily = if candidate.window == 1 {
            baseline_daily.clone()
        } else {
            load_candidate_daily_returns(&conn, &candidate, top_k)?
        };
        let deltas = paired_differences(&daily, &baseline_daily);
        let train_points = daily
            .iter()
            .filter(|point| point.trade_date.as_str() <= train_end.as_str())
            .collect::<Vec<_>>();
        let validation_points = daily
            .iter()
            .filter(|point| point.trade_date.as_str() >= validation_start.as_str())
            .collect::<Vec<_>>();
        let train_delta_points = deltas
            .iter()
            .filter(|point| point.trade_date.as_str() <= train_end.as_str())
            .collect::<Vec<_>>();
        let validation_delta_points = deltas
            .iter()
            .filter(|point| point.trade_date.as_str() >= validation_start.as_str())
            .collect::<Vec<_>>();
        let mut train_delta = summarize(&train_delta_points, holding_days);
        let mut validation_delta = summarize(&validation_delta_points, holding_days);
        if candidate.window == 1 {
            // 原始榜是增量为零的可选基线；常数差值序列没有可定义的 t 值。
            train_delta.hac_t = Some(0.0);
            validation_delta.hac_t = Some(0.0);
        }
        evaluations.push(CandidateEvaluation {
            candidate,
            train: summarize(&train_points, holding_days),
            validation: summarize(&validation_points, holding_days),
            train_delta,
            validation_delta,
        });
    }
    evaluations.sort_by(compare_evaluations);

    println!("\n训练集排序（选择只看 train，validation 为样本外验证）:");
    println!(
        "{:<4} {:<12} {:>6} {:>10} {:>10} {:>9} {:>10} {:>10} {:>9}",
        "#",
        "kernel",
        "days",
        "train_mean",
        "train_diff",
        "diff_t",
        "valid_mean",
        "valid_diff",
        "diff_t"
    );
    for (index, evaluation) in evaluations.iter().enumerate() {
        println!(
            "{:<4} {:<12} {:>6} {:>10} {:>10} {:>9} {:>10} {:>10} {:>9}",
            index + 1,
            evaluation.candidate.label(),
            evaluation.train.day_count,
            format_metric(evaluation.train.mean, 4),
            format_metric(evaluation.train_delta.mean, 4),
            format_metric(evaluation.train_delta.hac_t, 3),
            format_metric(evaluation.validation.mean, 4),
            format_metric(evaluation.validation_delta.mean, 4),
            format_metric(evaluation.validation_delta.hac_t, 3),
        );
    }

    let winner = evaluations.first().expect("候选核非空");
    println!("\n训练集选中: {}", winner.candidate.label());
    println!(
        "归一化卷积核(当前→过去): {:?}",
        winner.candidate.normalized_weights()
    );
    println!(
        "样本外: mean={}%, 相对原始榜={}%, 增量HAC t={}",
        format_metric(winner.validation.mean, 4),
        format_metric(winner.validation_delta.mean, 4),
        format_metric(winner.validation_delta.hac_t, 3)
    );
    let train_delta_t = winner.train_delta.hac_t.unwrap_or(f64::NEG_INFINITY);
    let validation_delta_t = winner.validation_delta.hac_t.unwrap_or(f64::NEG_INFINITY);
    if winner.candidate.window == 1 {
        println!("结论: 训练集没有卷积核胜过原始榜，保留原始排名。");
    } else if winner.validation.mean.unwrap_or(f64::NEG_INFINITY) <= 0.0
        || winner.validation_delta.mean.unwrap_or(f64::NEG_INFINITY) <= 0.0
    {
        println!("结论: 样本外没有同时满足正超额和优于原始榜，不建议替换当前原始排名。");
    } else if train_delta_t < 1.645 || validation_delta_t < 1.645 {
        println!(
            "结论: 增量方向通过，但未达到单侧95%显著性；将该核作为观察候选，暂不替换生产排名。"
        );
    } else {
        println!("结论: 该核的训练及样本外增量均通过单侧95%显著性，可进入下一轮走步回测。");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{DECAYS, HYBRID_WINDOWS, LONG_SHARES, WINDOWS, kernel_candidates, newey_west_t};
    use lianghua_backtest::simulate::rank::{
        DEFAULT_CONVOLUTION_KERNEL_NAME, default_convolution_kernel,
    };

    #[test]
    fn grid_contains_raw_and_all_window_decay_pairs() {
        let candidates = kernel_candidates();
        assert_eq!(
            candidates.len(),
            1 + (WINDOWS.len() - 1) * DECAYS.len() + HYBRID_WINDOWS.len() * LONG_SHARES.len()
        );
        assert_eq!(candidates[0].weights, vec![1.0]);
        assert!(candidates.iter().all(|candidate| {
            let sum = candidate.normalized_weights().iter().sum::<f64>();
            (sum - 1.0).abs() < 1e-12
        }));
        assert!(
            candidates
                .iter()
                .filter(|candidate| candidate.label.starts_with('H'))
                .all(|candidate| candidate.weights.iter().all(|weight| *weight > 0.0))
        );
    }

    #[test]
    fn hac_t_is_positive_for_positive_series() {
        let value =
            newey_west_t(&[1.0, 2.0, 1.5, 2.5, 2.0], 1).expect("non-constant series has t value");
        assert!(value > 0.0);
    }

    #[test]
    fn production_h30_l50_kernel_is_preserved_in_backtest_grid() {
        let candidates = kernel_candidates();
        let production_candidate = candidates
            .iter()
            .find(|candidate| candidate.label == DEFAULT_CONVOLUTION_KERNEL_NAME)
            .expect("生产卷积核必须保留在参数回测网格中");
        let expected = default_convolution_kernel();
        let actual = production_candidate.normalized_weights();

        assert_eq!(actual.len(), expected.len());
        assert!(
            actual
                .iter()
                .zip(expected.iter())
                .all(|(left, right)| (left - right).abs() < 1e-12)
        );
    }
}
