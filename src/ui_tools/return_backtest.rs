use duckdb::{Connection, params};
use serde::Serialize;

use crate::{
    data::{result_db_path, source_db_path},
    ui_tools::{build_concepts_map, build_name_map, resolve_trade_date},
};

const DEFAULT_ADJ_TYPE: &str = "qfq";
const BOARD_ALL: &str = "全部";
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

#[derive(Debug)]
struct QueryBacktestRow {
    ts_code: String,
    top_row_num: i64,
    board: String,
    rank: Option<i64>,
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

fn board_case_sql(ts_code_expr: &str) -> String {
    format!(
        "CASE
            WHEN UPPER({ts_code_expr}) LIKE '%.BJ' THEN '北交所'
            WHEN (UPPER({ts_code_expr}) LIKE '30%.SZ' OR UPPER({ts_code_expr}) LIKE '688%.SH') THEN '创业/科创'
            WHEN (UPPER({ts_code_expr}) LIKE '%.SH' OR UPPER({ts_code_expr}) LIKE '%.SZ') THEN '主板'
            ELSE '其他'
        END"
    )
}

fn query_backtest_rows(
    source_conn: &Connection,
    rank_date: &str,
    ref_date: &str,
    board_filter: Option<&str>,
) -> Result<Vec<QueryBacktestRow>, String> {
    let board_case = board_case_sql("s.ts_code");
    let board_condition = if let Some(board_filter) = board_filter {
        format!("AND {board_case} = '{}'", board_filter.replace('\'', "''"))
    } else {
        String::new()
    };
    let sql = format!(
        r#"
        WITH rank_pool AS (
            SELECT
                s.ts_code,
                s.rank,
                s.total_score,
                {board_case} AS board
            FROM result_db.score_summary AS s
            WHERE s.trade_date = ?
            {board_condition}
        ),
        ranked_pool AS (
            SELECT
                p.*,
                ROW_NUMBER() OVER (
                    ORDER BY COALESCE(p.rank, 999999) ASC, p.total_score DESC, p.ts_code ASC
                ) AS top_row_num
            FROM rank_pool AS p
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
        )
        SELECT
            p.ts_code,
            p.top_row_num,
            p.board,
            p.rank,
            p.total_score,
            n.entry_trade_date,
            n.entry_open,
            c.exit_trade_date,
            c.exit_close,
            (c.exit_close / n.entry_open - 1.0) * 100.0 AS return_pct
        FROM ranked_pool AS p
        INNER JOIN next_open AS n
            ON n.ts_code = p.ts_code
        INNER JOIN ref_close AS c
            ON c.ts_code = p.ts_code
        WHERE n.entry_open > 0
          AND n.entry_trade_date <= ?
        ORDER BY p.top_row_num ASC
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
            top_row_num: row
                .get(1)
                .map_err(|e| format!("读取 top_row_num 失败: {e}"))?,
            board: row.get(2).map_err(|e| format!("读取 board 失败: {e}"))?,
            rank: row.get(3).map_err(|e| format!("读取 rank 失败: {e}"))?,
            total_score: row
                .get(4)
                .map_err(|e| format!("读取 total_score 失败: {e}"))?,
            entry_trade_date: row
                .get(5)
                .map_err(|e| format!("读取 entry_trade_date 失败: {e}"))?,
            entry_open: row
                .get::<_, Option<f64>>(6)
                .map_err(|e| format!("读取 entry_open 失败: {e}"))?
                .ok_or_else(|| "批量回测返回缺失 entry_open".to_string())?,
            exit_trade_date: row
                .get(7)
                .map_err(|e| format!("读取 exit_trade_date 失败: {e}"))?,
            exit_close: row
                .get::<_, Option<f64>>(8)
                .map_err(|e| format!("读取 exit_close 失败: {e}"))?
                .ok_or_else(|| "批量回测返回缺失 exit_close".to_string())?,
            return_pct: row
                .get::<_, Option<f64>>(9)
                .map_err(|e| format!("读取 return_pct 失败: {e}"))?
                .ok_or_else(|| "批量回测返回缺失 return_pct".to_string())?,
        });
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

    let query_rows = query_backtest_rows(
        &source_conn,
        &resolved_rank_date,
        &resolved_ref_date,
        board_filter.as_deref(),
    )?;
    let benchmark_returns = query_rows
        .iter()
        .map(|row| row.return_pct)
        .collect::<Vec<_>>();
    let benchmark_return_pct = average(&benchmark_returns);
    let name_map = build_name_map(&source_path).unwrap_or_default();
    let concept_map = build_concepts_map(&source_path).unwrap_or_default();

    let mut rank_rows = query_rows
        .iter()
        .filter(|row| row.top_row_num <= top_limit as i64)
        .map(|row| ReturnBacktestRow {
            ts_code: row.ts_code.clone(),
            name: name_map.get(&row.ts_code).cloned(),
            board: row.board.clone(),
            rank: row.rank,
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

    let top_return_values = rank_rows
        .iter()
        .filter_map(|row| row.return_pct)
        .collect::<Vec<_>>();
    let top_excess_values = rank_rows
        .iter()
        .filter_map(|row| row.excess_return_pct)
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
        }),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
