use std::env;
use std::path::Path;

use duckdb::{Connection, params, params_from_iter};
use lianghua_backtest::simulate::rank::{calc_convolution_ranking, default_convolution_kernel};
use lianghua_data::data::result_db_path;
use lianghua_scoring::scoring::scoring_data::ScoreSummary;

const DEFAULT_TOP_N: usize = 20;

fn usage() -> &'static str {
    "用法: cargo run --bin convolution_rank_demo -- <source_dir> [trade_date|latest] [top_n] [kernel]\n\
     示例: cargo run --bin convolution_rank_demo -- /path/to/source latest 20\n\
     说明: 默认使用H30-L50双尺度核；自定义kernel从当前日向过去排列，且会自动归一化。"
}

fn parse_kernel(value: Option<&String>) -> Result<Vec<f64>, String> {
    let Some(value) = value else {
        return Ok(default_convolution_kernel());
    };
    let weights = value
        .split(',')
        .map(|item| {
            item.trim()
                .parse::<f64>()
                .map_err(|e| format!("解析卷积核权重失败: {item}, err={e}"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if weights.is_empty() {
        return Err("卷积核不能为空".to_string());
    }
    Ok(weights)
}

fn resolve_trade_date(conn: &Connection, requested: Option<&String>) -> Result<String, String> {
    if let Some(value) = requested.filter(|value| value.as_str() != "latest") {
        return Ok(value.trim().to_string());
    }
    conn.query_row(
        "SELECT MAX(trade_date) FROM score_summary WHERE total_score IS NOT NULL",
        [],
        |row| row.get::<_, String>(0),
    )
    .map_err(|e| format!("读取最新评分日期失败: {e}"))
}

fn load_recent_rows(
    conn: &Connection,
    target_date: &str,
    window_size: usize,
) -> Result<Vec<ScoreSummary>, String> {
    let mut date_stmt = conn
        .prepare(
            "SELECT DISTINCT trade_date
             FROM score_summary
             WHERE trade_date <= ? AND total_score IS NOT NULL
             ORDER BY trade_date DESC
             LIMIT ?",
        )
        .map_err(|e| format!("预编译交易日查询失败: {e}"))?;
    let date_rows = date_stmt
        .query_map(params![target_date, window_size as i64], |row| {
            row.get::<_, String>(0)
        })
        .map_err(|e| format!("查询最近交易日失败: {e}"))?;
    let mut trade_dates = date_rows
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("读取最近交易日失败: {e}"))?;
    trade_dates.reverse();
    if trade_dates.len() < window_size
        || trade_dates.last().map(String::as_str) != Some(target_date)
    {
        return Err(format!(
            "截至{target_date}没有完整的{window_size}个评分交易日"
        ));
    }

    let placeholders = std::iter::repeat_n("?", trade_dates.len())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT ts_code, trade_date, TRY_CAST(total_score AS DOUBLE), rank
         FROM score_summary
         WHERE trade_date IN ({placeholders})
         ORDER BY trade_date ASC, ts_code ASC"
    );
    let mut score_stmt = conn
        .prepare(&sql)
        .map_err(|e| format!("预编译评分查询失败: {e}"))?;
    let score_rows = score_stmt
        .query_map(params_from_iter(trade_dates.iter()), |row| {
            Ok(ScoreSummary {
                ts_code: row.get(0)?,
                trade_date: row.get(1)?,
                total_score: row.get::<_, Option<f64>>(2)?.unwrap_or(f64::NAN),
                rank: row.get(3)?,
            })
        })
        .map_err(|e| format!("查询评分数据失败: {e}"))?;
    score_rows
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("读取评分数据失败: {e}"))
}

fn format_change(change: isize) -> String {
    match change.cmp(&0) {
        std::cmp::Ordering::Greater => format!("+{change}"),
        _ => change.to_string(),
    }
}

fn main() -> Result<(), String> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    if args.is_empty() || args[0] == "--help" || args[0] == "-h" {
        println!("{}", usage());
        return Ok(());
    }

    let source_dir = &args[0];
    let result_db = result_db_path(source_dir);
    if !Path::new(&result_db).exists() {
        return Err(format!("评分数据库不存在: {}", result_db.display()));
    }
    let top_n = args
        .get(2)
        .map(|value| {
            value
                .parse::<usize>()
                .map_err(|e| format!("解析 top_n 失败: {value}, err={e}"))
        })
        .transpose()?
        .unwrap_or(DEFAULT_TOP_N);
    let kernel = parse_kernel(args.get(3))?;

    let conn = Connection::open(&result_db)
        .map_err(|e| format!("打开评分数据库失败: {}, err={e}", result_db.display()))?;
    let trade_date = resolve_trade_date(&conn, args.get(1))?;
    let score_rows = load_recent_rows(&conn, &trade_date, kernel.len())?;
    let ranking = calc_convolution_ranking(&score_rows, &trade_date, &kernel)?;

    println!("交易日: {trade_date}");
    println!("卷积核(当前→过去): {kernel:?}");
    println!("完整窗口股票数: {}", ranking.len());
    println!(
        "{:<8} {:<8} {:<8} {:<8} {:<14} {:>12} {:>12}  {}",
        "conv", "raw", "db", "change", "ts_code", "raw_score", "conv_score", "history(旧→新)"
    );
    for row in ranking.iter().take(top_n) {
        let history = row
            .score_history
            .iter()
            .map(|score| format!("{score:.2}"))
            .collect::<Vec<_>>()
            .join(" → ");
        println!(
            "{:<8} {:<8} {:<8} {:<8} {:<14} {:>12.4} {:>12.4}  {}",
            row.convolution_rank,
            row.raw_rank,
            row.database_rank
                .map(|rank| rank.to_string())
                .unwrap_or_else(|| "-".to_string()),
            format_change(row.rank_change),
            row.ts_code,
            row.raw_score,
            row.convolved_score,
            history
        );
    }

    Ok(())
}
