use duckdb::{Connection, params};
use rayon::prelude::*;
use serde::Serialize;

use crate::{
    data::{load_trade_date_list, result_db_path, source_db_path},
    ui_tools::{build_concepts_map, build_name_map, resolve_trade_date},
    utils::utils::board_category,
};

const DEFAULT_ADJ_TYPE: &str = "qfq";
const BOARD_ALL: &str = "全部";
const DEFAULT_HOLDING_DAYS: u32 = 5;
const STRONG_RETURN_THRESHOLD: f64 = 5.0;
const WEAK_RETURN_THRESHOLD: f64 = -3.0;
const RETURN_BUCKET_LABELS: [&str; 7] = [
    "<-5%", "-5%~-3%", "-3%~-1%", "-1%~1%", "1%~3%", "3%~5%", ">5%",
];

#[derive(Debug, Serialize)]
pub struct ReturnBacktestBucket {
    pub label: String,
    pub count: u32,
}

#[derive(Debug, Serialize, Clone)]
pub struct ReturnBacktestRow {
    pub ts_code: String,
    pub name: Option<String>,
    pub board: String,
    pub rank: Option<i64>,
    pub best_rank: Option<i64>,
    pub total_score: Option<f64>,
    pub concept: Option<String>,
    pub entry_trade_date: Option<String>,
    pub entry_open: Option<f64>,
    pub exit_trade_date: Option<String>,
    pub exit_close: Option<f64>,
    pub return_pct: Option<f64>,
    pub excess_return_pct: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct ReturnBacktestSummary {
    pub selected_top_count: u32,
    pub valid_top_count: u32,
    pub benchmark_sample_count: u32,
    pub benchmark_return_pct: Option<f64>,
    pub top_avg_return_pct: Option<f64>,
    pub top_avg_excess_return_pct: Option<f64>,
    pub top_strong_hit_rate: Option<f64>,
    pub top_weak_hit_rate: Option<f64>,
    pub benchmark_strong_hit_rate: Option<f64>,
    pub benchmark_weak_hit_rate: Option<f64>,
    pub strength_score: Option<f64>,
    pub strength_label: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ReturnBacktestPageData {
    pub resolved_rank_date: Option<String>,
    pub resolved_ref_date: Option<String>,
    pub board: Option<String>,
    pub top_limit: u32,
    pub benchmark_label: Option<String>,
    pub rank_distribution: Option<Vec<ReturnBacktestBucket>>,
    pub benchmark_distribution: Option<Vec<ReturnBacktestBucket>>,
    pub rank_rows: Option<Vec<ReturnBacktestRow>>,
    pub benchmark_rows: Option<Vec<ReturnBacktestRow>>,
    pub summary: Option<ReturnBacktestSummary>,
}

#[derive(Debug, Serialize)]
pub struct ReturnBacktestStrengthHeatmapItem {
    pub rank_date: String,
    pub ref_date: String,
    pub strength_score: Option<f64>,
    pub strength_label: Option<String>,
    pub top_avg_return_pct: Option<f64>,
    pub benchmark_return_pct: Option<f64>,
    pub top_strong_hit_rate: Option<f64>,
    pub top_weak_hit_rate: Option<f64>,
    pub benchmark_strong_hit_rate: Option<f64>,
    pub benchmark_weak_hit_rate: Option<f64>,
    pub valid_top_count: u32,
    pub benchmark_sample_count: u32,
}

#[derive(Debug, Serialize)]
pub struct ReturnBacktestStrengthOverviewData {
    pub holding_days: u32,
    pub top_limit: u32,
    pub board: Option<String>,
    pub latest_rank_date: Option<String>,
    pub strong_days: u32,
    pub weak_days: u32,
    pub flat_days: u32,
    pub items: Option<Vec<ReturnBacktestStrengthHeatmapItem>>,
}

#[derive(Debug)]
struct QueryBacktestRow {
    ts_code: String,
    top_row_num: i64,
    board: String,
    rank: Option<i64>,
    best_rank: Option<i64>,
    total_score: Option<f64>,
    entry_trade_date: String,
    entry_open: f64,
    exit_trade_date: String,
    exit_close: f64,
    return_pct: f64,
}

fn open_result_conn(source_path: &str) -> Result<Connection, String> {
    let result_db = result_db_path(source_path);
    let result_db_str = result_db
        .to_str()
        .ok_or_else(|| "结果库路径不是有效UTF-8".to_string())?;
    Connection::open(result_db_str).map_err(|e| format!("打开结果库失败: {e}"))
}

fn open_source_conn(source_path: &str) -> Result<Connection, String> {
    let source_db = source_db_path(source_path);
    let source_db_str = source_db
        .to_str()
        .ok_or_else(|| "原始库路径不是有效UTF-8".to_string())?;
    Connection::open(source_db_str).map_err(|e| format!("打开原始库失败: {e}"))
}

fn attach_result_db(source_conn: &Connection, source_path: &str) -> Result<(), String> {
    let result_db = result_db_path(source_path);
    let result_db_str = result_db
        .to_str()
        .ok_or_else(|| "结果库路径不是有效UTF-8".to_string())?;
    let escaped = result_db_str.replace('\'', "''");
    source_conn
        .execute_batch(&format!("ATTACH '{}' AS result_db (READ_ONLY);", escaped))
        .map_err(|e| format!("挂载结果库失败: {e}"))
}

fn normalize_board_filter(board: Option<String>) -> Option<String> {
    board.and_then(|value| {
        let trimmed = value.trim().to_string();
        if trimmed.is_empty() || trimmed == BOARD_ALL {
            None
        } else {
            Some(trimmed)
        }
    })
}

fn query_backtest_rows(
    source_conn: &Connection,
    rank_date: &str,
    ref_date: &str,
    board_filter: Option<&str>,
    name_map: &std::collections::HashMap<String, String>,
) -> Result<Vec<QueryBacktestRow>, String> {
    let sql = format!(
        r#"
        WITH rank_pool AS (
            SELECT
                s.ts_code,
                s.rank,
                s.total_score
            FROM result_db.score_summary AS s
            WHERE s.trade_date = ?
        ),
        next_open AS (
            SELECT ts_code, trade_date AS entry_trade_date, entry_open
            FROM (
                SELECT
                    d.ts_code,
                    d.trade_date,
                    TRY_CAST(d.open AS DOUBLE) AS entry_open,
                    ROW_NUMBER() OVER (
                        PARTITION BY d.ts_code
                        ORDER BY d.trade_date ASC
                    ) AS rn
                FROM stock_data AS d
                INNER JOIN rank_pool AS p
                    ON p.ts_code = d.ts_code
                WHERE d.adj_type = ?
                  AND d.trade_date > ?
            ) AS ranked_next_open
            WHERE rn = 1
        ),
        ref_close AS (
            SELECT
                d.ts_code,
                d.trade_date AS exit_trade_date,
                TRY_CAST(d.close AS DOUBLE) AS exit_close
            FROM stock_data AS d
            INNER JOIN rank_pool AS p
                ON p.ts_code = d.ts_code
            WHERE d.adj_type = ?
              AND d.trade_date = ?
        ),
        interval_best_rank AS (
            SELECT
                s.ts_code,
                MIN(s.rank) AS best_rank
            FROM result_db.score_summary AS s
            INNER JOIN next_open AS n
                ON n.ts_code = s.ts_code
            WHERE s.trade_date >= n.entry_trade_date
              AND s.trade_date <= ?
            GROUP BY s.ts_code
        )
        SELECT
            p.ts_code,
            p.rank,
            b.best_rank,
            p.total_score,
            n.entry_trade_date,
            n.entry_open,
            c.exit_trade_date,
            c.exit_close,
            (c.exit_close / n.entry_open - 1.0) * 100.0 AS return_pct
        FROM rank_pool AS p
        INNER JOIN next_open AS n
            ON n.ts_code = p.ts_code
        INNER JOIN ref_close AS c
            ON c.ts_code = p.ts_code
        LEFT JOIN interval_best_rank AS b
            ON b.ts_code = p.ts_code
        WHERE n.entry_open > 0
          AND n.entry_trade_date <= ?
        ORDER BY p.rank ASC NULLS LAST, p.total_score DESC NULLS LAST, p.ts_code ASC
        "#
    );
    let mut stmt = source_conn
        .prepare(&sql)
        .map_err(|e| format!("预编译批量回测查询失败: {e}"))?;
    let mut rows = stmt
        .query(params![
            rank_date,
            DEFAULT_ADJ_TYPE,
            rank_date,
            DEFAULT_ADJ_TYPE,
            ref_date,
            ref_date,
            ref_date
        ])
        .map_err(|e| format!("查询批量回测结果失败: {e}"))?;

    let mut out = Vec::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取批量回测结果失败: {e}"))?
    {
        out.push(QueryBacktestRow {
            ts_code: row.get(0).map_err(|e| format!("读取 ts_code 失败: {e}"))?,
            top_row_num: 0,
            board: String::new(),
            rank: row.get(1).map_err(|e| format!("读取 rank 失败: {e}"))?,
            best_rank: row
                .get(2)
                .map_err(|e| format!("读取 best_rank 失败: {e}"))?,
            total_score: row
                .get(3)
                .map_err(|e| format!("读取 total_score 失败: {e}"))?,
            entry_trade_date: row
                .get(4)
                .map_err(|e| format!("读取 entry_trade_date 失败: {e}"))?,
            entry_open: row
                .get::<_, Option<f64>>(5)
                .map_err(|e| format!("读取 entry_open 失败: {e}"))?
                .ok_or_else(|| "批量回测返回缺失 entry_open".to_string())?,
            exit_trade_date: row
                .get(6)
                .map_err(|e| format!("读取 exit_trade_date 失败: {e}"))?,
            exit_close: row
                .get::<_, Option<f64>>(7)
                .map_err(|e| format!("读取 exit_close 失败: {e}"))?
                .ok_or_else(|| "批量回测返回缺失 exit_close".to_string())?,
            return_pct: row
                .get::<_, Option<f64>>(8)
                .map_err(|e| format!("读取 return_pct 失败: {e}"))?
                .ok_or_else(|| "批量回测返回缺失 return_pct".to_string())?,
        });
    }

    out.retain(|row| {
        let board_value = board_category(
            &row.ts_code,
            name_map.get(&row.ts_code).map(|value| value.as_str()),
        );
        board_filter
            .map(|filter| filter == board_value)
            .unwrap_or(true)
    });
    out.sort_by(|left, right| {
        left.rank
            .cmp(&right.rank)
            .then_with(|| {
                right
                    .total_score
                    .partial_cmp(&left.total_score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .then_with(|| left.ts_code.cmp(&right.ts_code))
    });
    for (index, row) in out.iter_mut().enumerate() {
        row.top_row_num = index as i64 + 1;
        row.board = board_category(
            &row.ts_code,
            name_map.get(&row.ts_code).map(|value| value.as_str()),
        )
        .to_string();
    }

    Ok(out)
}

fn query_rank_trade_date_options_from_conn(conn: &Connection) -> Result<Vec<String>, String> {
    let mut stmt = conn
        .prepare(
            r#"
            SELECT DISTINCT trade_date
            FROM score_summary
            ORDER BY trade_date ASC
            "#,
        )
        .map_err(|e| format!("预编译排名日期列表失败: {e}"))?;
    let mut rows = stmt
        .query([])
        .map_err(|e| format!("查询排名日期列表失败: {e}"))?;
    let mut out = Vec::new();

    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取排名日期列表失败: {e}"))?
    {
        let trade_date: String = row.get(0).map_err(|e| format!("读取排名日期失败: {e}"))?;
        if !trade_date.trim().is_empty() {
            out.push(trade_date);
        }
    }

    Ok(out)
}

fn build_distribution(values: &[f64]) -> Vec<ReturnBacktestBucket> {
    let mut counts = [0u32; 7];
    values.iter().for_each(|value| {
        let bucket_index = if *value < -5.0 {
            0
        } else if *value < -3.0 {
            1
        } else if *value < -1.0 {
            2
        } else if *value <= 1.0 {
            3
        } else if *value <= 3.0 {
            4
        } else if *value <= 5.0 {
            5
        } else {
            6
        };
        counts[bucket_index] += 1;
    });

    RETURN_BUCKET_LABELS
        .iter()
        .enumerate()
        .map(|(index, label)| ReturnBacktestBucket {
            label: (*label).to_string(),
            count: counts[index],
        })
        .collect()
}

fn average(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    Some(values.iter().sum::<f64>() / values.len() as f64)
}

fn hit_rate<F>(values: &[f64], predicate: F) -> Option<f64>
where
    F: Fn(f64) -> bool,
{
    if values.is_empty() {
        return None;
    }
    let hit_count = values
        .iter()
        .copied()
        .filter(|value| predicate(*value))
        .count();
    Some((hit_count as f64 / values.len() as f64) * 100.0)
}

fn classify_strength(strength_score: f64) -> &'static str {
    if strength_score > 0.0 {
        "强于大盘"
    } else if strength_score < 0.0 {
        "弱于大盘"
    } else {
        "持平"
    }
}

fn build_backtest_summary(
    query_rows: &[QueryBacktestRow],
    top_limit: u32,
) -> (ReturnBacktestSummary, Vec<f64>, Vec<f64>, Vec<f64>) {
    let benchmark_returns = query_rows
        .iter()
        .map(|row| row.return_pct)
        .collect::<Vec<_>>();
    let benchmark_return_pct = average(&benchmark_returns);

    let top_return_values = query_rows
        .iter()
        .filter(|row| row.top_row_num <= top_limit as i64)
        .map(|row| row.return_pct)
        .collect::<Vec<_>>();
    let top_excess_values = top_return_values
        .iter()
        .copied()
        .filter_map(|value| benchmark_return_pct.map(|benchmark| value - benchmark))
        .collect::<Vec<_>>();

    let top_strong_hit_rate = hit_rate(&top_return_values, |value| value > STRONG_RETURN_THRESHOLD);
    let top_weak_hit_rate = hit_rate(&top_return_values, |value| value < WEAK_RETURN_THRESHOLD);
    let benchmark_strong_hit_rate =
        hit_rate(&benchmark_returns, |value| value > STRONG_RETURN_THRESHOLD);
    let benchmark_weak_hit_rate =
        hit_rate(&benchmark_returns, |value| value < WEAK_RETURN_THRESHOLD);
    let strength_score = match (
        top_strong_hit_rate,
        top_weak_hit_rate,
        benchmark_strong_hit_rate,
        benchmark_weak_hit_rate,
    ) {
        (
            Some(top_strong_rate),
            Some(top_weak_rate),
            Some(benchmark_strong_rate),
            Some(benchmark_weak_rate),
        ) => {
            Some((top_strong_rate - benchmark_strong_rate) - (top_weak_rate - benchmark_weak_rate))
        }
        _ => None,
    };
    let strength_label = strength_score.map(|value| classify_strength(value).to_string());

    (
        ReturnBacktestSummary {
            selected_top_count: top_limit,
            valid_top_count: top_return_values.len() as u32,
            benchmark_sample_count: benchmark_returns.len() as u32,
            benchmark_return_pct,
            top_avg_return_pct: average(&top_return_values),
            top_avg_excess_return_pct: average(&top_excess_values),
            top_strong_hit_rate,
            top_weak_hit_rate,
            benchmark_strong_hit_rate,
            benchmark_weak_hit_rate,
            strength_score,
            strength_label,
        },
        benchmark_returns,
        top_return_values,
        top_excess_values,
    )
}

fn resolve_ref_trade_date_by_holding_days(
    trade_date_list: &[String],
    rank_date: &str,
    holding_days: u32,
) -> Option<String> {
    let rank_index = trade_date_list.iter().position(|item| item == rank_date)?;
    trade_date_list
        .get(rank_index + holding_days as usize)
        .cloned()
}

fn build_strength_heatmap_item(
    source_path: &str,
    rank_date: &str,
    ref_date: &str,
    board_filter: Option<&str>,
    name_map: &std::collections::HashMap<String, String>,
    top_limit: u32,
) -> Result<ReturnBacktestStrengthHeatmapItem, String> {
    let source_conn = open_source_conn(source_path)?;
    attach_result_db(&source_conn, source_path)?;

    let query_rows =
        query_backtest_rows(&source_conn, rank_date, ref_date, board_filter, name_map)?;
    let (summary, _, _, _) = build_backtest_summary(&query_rows, top_limit);

    Ok(ReturnBacktestStrengthHeatmapItem {
        rank_date: rank_date.to_string(),
        ref_date: ref_date.to_string(),
        strength_score: summary.strength_score,
        strength_label: summary.strength_label,
        top_avg_return_pct: summary.top_avg_return_pct,
        benchmark_return_pct: summary.benchmark_return_pct,
        top_strong_hit_rate: summary.top_strong_hit_rate,
        top_weak_hit_rate: summary.top_weak_hit_rate,
        benchmark_strong_hit_rate: summary.benchmark_strong_hit_rate,
        benchmark_weak_hit_rate: summary.benchmark_weak_hit_rate,
        valid_top_count: summary.valid_top_count,
        benchmark_sample_count: summary.benchmark_sample_count,
    })
}

pub fn get_return_backtest_page(
    source_path: String,
    rank_date: Option<String>,
    ref_date: Option<String>,
    top_limit: Option<u32>,
    board: Option<String>,
) -> Result<ReturnBacktestPageData, String> {
    let result_conn = open_result_conn(&source_path)?;
    let source_conn = open_source_conn(&source_path)?;
    attach_result_db(&source_conn, &source_path)?;

    let resolved_rank_date = resolve_trade_date(&result_conn, rank_date)?;
    let resolved_ref_date = resolve_trade_date(&result_conn, ref_date)?;
    if resolved_ref_date <= resolved_rank_date {
        return Err("参考日必须晚于排名日期".to_string());
    }

    let top_limit = top_limit.unwrap_or(100).max(1);
    let board_filter = normalize_board_filter(board);
    let board_label = board_filter
        .clone()
        .unwrap_or_else(|| BOARD_ALL.to_string());
    let benchmark_label = if board_label == BOARD_ALL {
        "全市场样本".to_string()
    } else {
        format!("{board_label}样本")
    };
    let name_map = build_name_map(&source_path).unwrap_or_default();

    let query_rows = query_backtest_rows(
        &source_conn,
        &resolved_rank_date,
        &resolved_ref_date,
        board_filter.as_deref(),
        &name_map,
    )?;
    let (summary, benchmark_returns, top_return_values, top_excess_values) =
        build_backtest_summary(&query_rows, top_limit);
    let benchmark_return_pct = summary.benchmark_return_pct;
    let concept_map = build_concepts_map(&source_path).unwrap_or_default();

    let mut rank_rows = query_rows
        .iter()
        .filter(|row| row.top_row_num <= top_limit as i64)
        .map(|row| ReturnBacktestRow {
            ts_code: row.ts_code.clone(),
            name: name_map.get(&row.ts_code).cloned(),
            board: row.board.clone(),
            rank: row.rank,
            best_rank: row.best_rank,
            total_score: row.total_score,
            concept: concept_map.get(&row.ts_code).cloned(),
            entry_trade_date: Some(row.entry_trade_date.clone()),
            entry_open: Some(row.entry_open),
            exit_trade_date: Some(row.exit_trade_date.clone()),
            exit_close: Some(row.exit_close),
            return_pct: Some(row.return_pct),
            excess_return_pct: benchmark_return_pct.map(|value| row.return_pct - value),
        })
        .collect::<Vec<_>>();
    let mut benchmark_rows = query_rows
        .into_iter()
        .map(|row| ReturnBacktestRow {
            ts_code: row.ts_code.clone(),
            name: name_map.get(&row.ts_code).cloned(),
            board: row.board.clone(),
            rank: row.rank,
            best_rank: row.best_rank,
            total_score: row.total_score,
            concept: concept_map.get(&row.ts_code).cloned(),
            entry_trade_date: Some(row.entry_trade_date.clone()),
            entry_open: Some(row.entry_open),
            exit_trade_date: Some(row.exit_trade_date.clone()),
            exit_close: Some(row.exit_close),
            return_pct: Some(row.return_pct),
            excess_return_pct: benchmark_return_pct.map(|value| row.return_pct - value),
        })
        .collect::<Vec<_>>();
    rank_rows.sort_by(|left, right| {
        right
            .return_pct
            .partial_cmp(&left.return_pct)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.rank.cmp(&right.rank))
    });
    benchmark_rows.sort_by(|left, right| {
        right
            .return_pct
            .partial_cmp(&left.return_pct)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.rank.cmp(&right.rank))
    });

    Ok(ReturnBacktestPageData {
        resolved_rank_date: Some(resolved_rank_date),
        resolved_ref_date: Some(resolved_ref_date),
        board: Some(board_label),
        top_limit,
        benchmark_label: Some(benchmark_label),
        rank_distribution: Some(build_distribution(&top_return_values)),
        benchmark_distribution: Some(build_distribution(&benchmark_returns)),
        rank_rows: Some(rank_rows),
        benchmark_rows: Some(benchmark_rows),
        summary: Some(ReturnBacktestSummary {
            top_avg_excess_return_pct: average(&top_excess_values),
            ..summary
        }),
    })
}

pub fn get_return_backtest_strength_overview(
    source_path: String,
    holding_days: Option<u32>,
    top_limit: Option<u32>,
    board: Option<String>,
) -> Result<ReturnBacktestStrengthOverviewData, String> {
    let result_conn = open_result_conn(&source_path)?;

    let rank_dates = query_rank_trade_date_options_from_conn(&result_conn)?;
    let mut trade_date_list = load_trade_date_list(&source_path)?;
    trade_date_list.sort();
    trade_date_list.dedup();

    let holding_days = holding_days.unwrap_or(DEFAULT_HOLDING_DAYS).max(1);
    let top_limit = top_limit.unwrap_or(100).max(1);
    let board_filter = normalize_board_filter(board);
    let board_label = board_filter
        .clone()
        .unwrap_or_else(|| BOARD_ALL.to_string());
    let name_map = build_name_map(&source_path).unwrap_or_default();

    let jobs = rank_dates
        .into_iter()
        .filter_map(|rank_date| {
            resolve_ref_trade_date_by_holding_days(&trade_date_list, &rank_date, holding_days)
                .map(|ref_date| (rank_date, ref_date))
        })
        .collect::<Vec<_>>();

    // 每个排名日的格子互相独立，单独开只读连接后可安全并行计算。
    let items = jobs
        .par_iter()
        .map(|(rank_date, ref_date)| {
            build_strength_heatmap_item(
                &source_path,
                rank_date,
                ref_date,
                board_filter.as_deref(),
                &name_map,
                top_limit,
            )
        })
        .collect::<Result<Vec<_>, String>>()?;

    let strong_days = items
        .iter()
        .filter(|item| item.strength_label.as_deref() == Some("强于大盘"))
        .count() as u32;
    let weak_days = items
        .iter()
        .filter(|item| item.strength_label.as_deref() == Some("弱于大盘"))
        .count() as u32;
    let flat_days = items
        .iter()
        .filter(|item| item.strength_label.as_deref() == Some("持平"))
        .count() as u32;
    let latest_rank_date = items.last().map(|item| item.rank_date.clone());

    Ok(ReturnBacktestStrengthOverviewData {
        holding_days,
        top_limit,
        board: Some(board_label),
        latest_rank_date,
        strong_days,
        weak_days,
        flat_days,
        items: Some(items),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs,
        path::{Path, PathBuf},
        time::{SystemTime, UNIX_EPOCH},
    };

    use duckdb::Connection;

    use crate::data::{result_db_path, scoring_data::init_result_db, source_db_path};

    #[test]
    fn build_distribution_places_values_in_symmetric_buckets() {
        let distribution = build_distribution(&[-8.0, -4.2, -2.1, 0.2, 2.8, 4.6, 9.9]);
        let counts = distribution
            .iter()
            .map(|item| item.count)
            .collect::<Vec<_>>();
        assert_eq!(counts, vec![1, 1, 1, 1, 1, 1, 1]);
    }

    #[test]
    fn classify_strength_uses_top_vs_market_extreme_hit_gap() {
        let top_returns = vec![7.2, 6.1, 1.0, -4.4];
        let market_returns = vec![6.2, 1.5, -3.5, -4.2];
        let score = (hit_rate(&top_returns, |value| value > STRONG_RETURN_THRESHOLD).unwrap()
            - hit_rate(&market_returns, |value| value > STRONG_RETURN_THRESHOLD).unwrap())
            - (hit_rate(&top_returns, |value| value < WEAK_RETURN_THRESHOLD).unwrap()
                - hit_rate(&market_returns, |value| value < WEAK_RETURN_THRESHOLD).unwrap());
        assert_eq!(classify_strength(score), "强于大盘");
    }

    fn unique_temp_dir() -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock drift")
            .as_nanos();
        std::env::temp_dir().join(format!("lianghua_rs_return_strength_{stamp}"))
    }

    fn write_strength_fixture_files(source_dir: &Path) {
        fs::create_dir_all(source_dir).expect("create source dir");
        fs::write(
            source_dir.join("trade_calendar.csv"),
            "cal_date\n20240101\n20240102\n20240103\n",
        )
        .expect("write trade_calendar");
    }

    fn write_strength_fixture_source_db(source_dir: &Path) {
        let db_path = source_db_path(source_dir.to_str().expect("utf8 path"));
        let conn = Connection::open(&db_path).expect("open source db");
        conn.execute(
            r#"
            CREATE TABLE stock_data (
                ts_code VARCHAR,
                trade_date VARCHAR,
                adj_type VARCHAR,
                open DOUBLE,
                close DOUBLE
            )
            "#,
            [],
        )
        .expect("create stock_data");

        let rows = [
            ("000001.SZ", "20240102", "qfq", 10.0, 12.0),
            ("000002.SZ", "20240102", "qfq", 10.0, 9.0),
            ("000001.SZ", "20240103", "qfq", 10.0, 8.0),
            ("000002.SZ", "20240103", "qfq", 10.0, 12.0),
        ];
        for (ts_code, trade_date, adj_type, open, close) in rows {
            conn.execute(
                "INSERT INTO stock_data (ts_code, trade_date, adj_type, open, close) VALUES (?, ?, ?, ?, ?)",
                params![ts_code, trade_date, adj_type, open, close],
            )
            .expect("insert stock_data");
        }
    }

    fn write_strength_fixture_result_db(source_dir: &Path) {
        let db_path = result_db_path(source_dir.to_str().expect("utf8 path"));
        init_result_db(&db_path).expect("init result db");
        let conn = Connection::open(&db_path).expect("open result db");

        let summary_rows = [
            ("000001.SZ", "20240101", 90.0, 1_i64),
            ("000002.SZ", "20240101", 80.0, 2_i64),
            ("000001.SZ", "20240102", 88.0, 1_i64),
            ("000002.SZ", "20240102", 75.0, 2_i64),
        ];
        for (ts_code, trade_date, total_score, rank) in summary_rows {
            conn.execute(
                "INSERT INTO score_summary (ts_code, trade_date, total_score, rank) VALUES (?, ?, ?, ?)",
                params![ts_code, trade_date, total_score, rank],
            )
            .expect("insert score_summary");
        }
    }

    #[test]
    fn return_backtest_strength_overview_builds_heatmap_items() {
        let source_dir = unique_temp_dir();
        write_strength_fixture_files(&source_dir);
        write_strength_fixture_source_db(&source_dir);
        write_strength_fixture_result_db(&source_dir);

        let overview = get_return_backtest_strength_overview(
            source_dir.to_str().expect("utf8 path").to_string(),
            Some(1),
            Some(1),
            None,
        )
        .expect("build strength overview");

        let items = overview.items.expect("items");
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].rank_date, "20240101");
        assert_eq!(items[0].ref_date, "20240102");
        assert_eq!(items[0].strength_label.as_deref(), Some("强于大盘"));
        assert_eq!(items[1].rank_date, "20240102");
        assert_eq!(items[1].ref_date, "20240103");
        assert_eq!(items[1].strength_label.as_deref(), Some("弱于大盘"));
        assert_eq!(overview.strong_days, 1);
        assert_eq!(overview.weak_days, 1);
        assert_eq!(overview.flat_days, 0);

        let _ = fs::remove_dir_all(source_dir);
    }
}
