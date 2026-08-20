use std::time::Instant;

use duckdb::{Connection, params, params_from_iter};
use serde::Serialize;

use crate::{
    data::{result_db_path, scoring_data::ScoreSummary},
    simulate::rank::{
        DEFAULT_CONVOLUTION_KERNEL_NAME, calc_convolution_ranking, default_convolution_kernel,
    },
    ui_tools::{
        build_concepts_map, build_name_map, build_total_mv_map, filter_mv, normalize_trade_date,
        resolve_trade_date,
    },
    utils::utils::board_category,
};

const BOARD_ST: &str = "ST";

#[derive(Debug, Serialize, Clone)]
pub struct ConvolutionRankItem {
    pub ts_code: String,
    pub name: String,
    pub board: String,
    pub concept: String,
    pub total_mv_yi: Option<f64>,
    pub trade_date: String,
    pub database_rank: Option<i64>,
    pub raw_rank: usize,
    pub convolution_rank: usize,
    /// 正数表示经过卷积后名次上升，负数表示下降。
    pub rank_change: isize,
    pub raw_score: f64,
    pub convolution_score: f64,
    /// 从旧到新，与 `history_trade_dates` 一一对应。
    pub score_history: Vec<f64>,
}

#[derive(Debug, Serialize)]
pub struct ConvolutionRankPageData {
    pub rows: Vec<ConvolutionRankItem>,
    pub resolved_trade_date: String,
    pub kernel_name: String,
    /// 从当前交易日向过去排列。
    pub kernel: Vec<f64>,
    /// 从旧到新排列。
    pub history_trade_dates: Vec<String>,
    /// 具备完整窗口、参与全局排名的股票数，先于页面筛选计算。
    pub universe_size: usize,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConvolutionRankComputeResult {
    pub action: String,
    pub kernel_name: String,
    pub window_size: usize,
    pub start_date: String,
    pub end_date: String,
    pub elapsed_ms: u64,
    pub saved_rows: usize,
    pub trade_dates: usize,
}

fn open_result_conn(source_path: &str) -> Result<Connection, String> {
    let result_db = result_db_path(source_path);
    let result_db_str = result_db
        .to_str()
        .ok_or_else(|| "结果库路径不是有效UTF-8".to_string())?;
    Connection::open(result_db_str).map_err(|e| format!("打开结果库失败: {e}"))
}

fn load_recent_score_rows(
    conn: &Connection,
    target_date: &str,
    window_size: usize,
) -> Result<(Vec<String>, Vec<ScoreSummary>), String> {
    let mut date_stmt = conn
        .prepare(
            r#"
            SELECT DISTINCT trade_date
            FROM score_summary
            WHERE trade_date <= ? AND total_score IS NOT NULL
            ORDER BY trade_date DESC
            LIMIT ?
            "#,
        )
        .map_err(|e| format!("预编译卷积交易日查询失败: {e}"))?;
    let date_rows = date_stmt
        .query_map(params![target_date, window_size as i64], |row| {
            row.get::<_, String>(0)
        })
        .map_err(|e| format!("查询卷积交易日失败: {e}"))?;
    let mut trade_dates = date_rows
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("读取卷积交易日失败: {e}"))?;
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
        r#"
        SELECT ts_code, trade_date, TRY_CAST(total_score AS DOUBLE), rank
        FROM score_summary
        WHERE trade_date IN ({placeholders})
        ORDER BY trade_date ASC, ts_code ASC
        "#
    );
    let mut score_stmt = conn
        .prepare(&sql)
        .map_err(|e| format!("预编译卷积评分查询失败: {e}"))?;
    let score_rows = score_stmt
        .query_map(params_from_iter(trade_dates.iter()), |row| {
            Ok(ScoreSummary {
                ts_code: row.get(0)?,
                trade_date: row.get(1)?,
                total_score: row.get::<_, Option<f64>>(2)?.unwrap_or(f64::NAN),
                rank: row.get(3)?,
            })
        })
        .map_err(|e| format!("查询卷积评分失败: {e}"))?;
    let score_rows = score_rows
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("读取卷积评分失败: {e}"))?;

    Ok((trade_dates, score_rows))
}

fn load_stored_convolution_ranking(
    conn: &Connection,
    target_date: &str,
    history_trade_dates: &[String],
    score_rows: &[ScoreSummary],
) -> Result<Option<Vec<crate::simulate::rank::ConvolutionRankRow>>, String> {
    let table_exists = conn
        .query_row(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'convolution_rank'",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map_err(|e| format!("检查卷积排名表失败: {e}"))?;
    if table_exists <= 0 {
        return Ok(None);
    }

    let mut scores_by_stock =
        std::collections::HashMap::<&str, std::collections::HashMap<&str, f64>>::new();
    for row in score_rows {
        if row.total_score.is_finite() {
            scores_by_stock
                .entry(row.ts_code.as_str())
                .or_default()
                .insert(row.trade_date.as_str(), row.total_score);
        }
    }

    let mut stmt = conn
        .prepare(
            r#"
            SELECT
                ts_code,
                trade_date,
                database_rank,
                raw_score,
                convolution_score,
                raw_rank,
                convolution_rank,
                rank_change
            FROM convolution_rank
            WHERE trade_date = ? AND kernel_name = ?
            ORDER BY convolution_rank ASC, ts_code ASC
            "#,
        )
        .map_err(|e| format!("预编译已计算卷积排名查询失败: {e}"))?;
    let mut query_rows = stmt
        .query(params![target_date, DEFAULT_CONVOLUTION_KERNEL_NAME])
        .map_err(|e| format!("查询已计算卷积排名失败: {e}"))?;
    let mut ranking = Vec::new();
    while let Some(row) = query_rows
        .next()
        .map_err(|e| format!("读取已计算卷积排名失败: {e}"))?
    {
        let ts_code: String = row
            .get(0)
            .map_err(|e| format!("读取卷积排名代码失败: {e}"))?;
        let Some(scores_by_date) = scores_by_stock.get(ts_code.as_str()) else {
            continue;
        };
        let Some(score_history) = history_trade_dates
            .iter()
            .map(|trade_date| scores_by_date.get(trade_date.as_str()).copied())
            .collect::<Option<Vec<_>>>()
        else {
            continue;
        };
        let raw_rank = row
            .get::<_, i64>(5)
            .map_err(|e| format!("读取原始名次失败: {e}"))?
            .max(0) as usize;
        let convolution_rank = row
            .get::<_, i64>(6)
            .map_err(|e| format!("读取卷积名次失败: {e}"))?
            .max(0) as usize;
        let rank_change_i64 = row
            .get::<_, i64>(7)
            .map_err(|e| format!("读取卷积名次变化失败: {e}"))?;
        let rank_change = isize::try_from(rank_change_i64)
            .map_err(|_| format!("卷积名次变化超出范围: {rank_change_i64}"))?;

        ranking.push(crate::simulate::rank::ConvolutionRankRow {
            ts_code,
            trade_date: row
                .get(1)
                .map_err(|e| format!("读取卷积排名日期失败: {e}"))?,
            database_rank: row.get(2).map_err(|e| format!("读取数据库名次失败: {e}"))?,
            raw_score: row.get(3).map_err(|e| format!("读取原始分数失败: {e}"))?,
            convolved_score: row.get(4).map_err(|e| format!("读取卷积分数失败: {e}"))?,
            raw_rank,
            convolution_rank,
            rank_change,
            score_history,
        });
    }

    if ranking.is_empty() {
        Ok(None)
    } else {
        Ok(Some(ranking))
    }
}

fn ensure_convolution_rank_table(conn: &Connection) -> Result<(), String> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS convolution_rank (
            ts_code VARCHAR NOT NULL,
            trade_date VARCHAR NOT NULL,
            kernel_name VARCHAR NOT NULL,
            raw_score DOUBLE NOT NULL,
            convolution_score DOUBLE NOT NULL,
            database_rank BIGINT,
            raw_rank BIGINT NOT NULL,
            convolution_rank BIGINT NOT NULL,
            rank_change BIGINT NOT NULL,
            PRIMARY KEY (ts_code, trade_date, kernel_name)
        );
        CREATE INDEX IF NOT EXISTS idx_convolution_rank_date_rank_ts
            ON convolution_rank(trade_date, kernel_name, convolution_rank, ts_code);
        CREATE INDEX IF NOT EXISTS idx_convolution_rank_ts_date
            ON convolution_rank(ts_code, trade_date, kernel_name);
        "#,
    )
    .map_err(|e| format!("初始化卷积排名表失败: {e}"))
}

fn convolution_score_sql(kernel: &[f64]) -> String {
    kernel
        .iter()
        .enumerate()
        .map(|(lag, weight)| format!("score_lag_{lag} * {weight:.17}"))
        .collect::<Vec<_>>()
        .join(" + ")
}

fn compute_convolution_rank_range(
    conn: &mut Connection,
    start_date: &str,
    end_date: &str,
    kernel: &[f64],
) -> Result<(usize, usize), String> {
    ensure_convolution_rank_table(conn)?;

    let warmup_start = conn
        .query_row(
            r#"
            SELECT MIN(trade_date)
            FROM (
                SELECT DISTINCT trade_date
                FROM score_summary
                WHERE trade_date <= ? AND total_score IS NOT NULL
                ORDER BY trade_date DESC
                LIMIT ?
            )
            "#,
            params![start_date, kernel.len() as i64],
            |row| row.get::<_, Option<String>>(0),
        )
        .map_err(|e| format!("查询卷积排名预热日期失败: {e}"))?
        .ok_or_else(|| format!("截至{start_date}没有评分数据"))?;

    let lag_columns = kernel
        .iter()
        .enumerate()
        .map(|(lag, _)| {
            if lag == 0 {
                "TRY_CAST(s.total_score AS DOUBLE) AS score_lag_0".to_string()
            } else {
                format!(
                    "LAG(TRY_CAST(s.total_score AS DOUBLE), {lag}) OVER stock_window AS score_lag_{lag}"
                )
            }
        })
        .collect::<Vec<_>>()
        .join(",\n                ");
    let score_sql = convolution_score_sql(kernel);
    let oldest_lag = kernel.len() - 1;
    let sql = format!(
        r#"
        INSERT INTO convolution_rank
        WITH date_axis AS (
            SELECT
                trade_date,
                ROW_NUMBER() OVER (ORDER BY trade_date) AS date_no
            FROM (
                SELECT DISTINCT trade_date
                FROM score_summary
                WHERE trade_date >= ? AND trade_date <= ? AND total_score IS NOT NULL
            )
        ),
        windowed AS (
            SELECT
                s.ts_code,
                s.trade_date,
                s.rank AS database_rank,
                dates.date_no,
                LAG(dates.date_no, {oldest_lag}) OVER stock_window AS oldest_date_no,
                {lag_columns}
            FROM score_summary AS s
            INNER JOIN date_axis AS dates ON dates.trade_date = s.trade_date
            WHERE s.trade_date >= ? AND s.trade_date <= ? AND s.total_score IS NOT NULL
            WINDOW stock_window AS (PARTITION BY s.ts_code ORDER BY dates.date_no)
        ),
        candidates AS (
            SELECT
                ts_code,
                trade_date,
                database_rank,
                score_lag_0 AS raw_score,
                {score_sql} AS convolution_score
            FROM windowed
            WHERE trade_date >= ?
              AND oldest_date_no = date_no - {oldest_lag}
        ),
        ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY trade_date
                    ORDER BY raw_score DESC, ts_code ASC
                ) AS raw_rank,
                ROW_NUMBER() OVER (
                    PARTITION BY trade_date
                    ORDER BY convolution_score DESC, ts_code ASC
                ) AS convolution_rank
            FROM candidates
        )
        SELECT
            ts_code,
            trade_date,
            ?,
            raw_score,
            convolution_score,
            database_rank,
            raw_rank,
            convolution_rank,
            raw_rank - convolution_rank
        FROM ranked
        "#
    );

    let tx = conn
        .transaction()
        .map_err(|e| format!("创建卷积排名事务失败: {e}"))?;
    tx.execute(
        "DELETE FROM convolution_rank WHERE kernel_name = ? AND trade_date >= ? AND trade_date <= ?",
        params![DEFAULT_CONVOLUTION_KERNEL_NAME, start_date, end_date],
    )
    .map_err(|e| format!("删除区间旧卷积排名失败: {e}"))?;
    let saved_rows = tx
        .execute(
            &sql,
            params![
                warmup_start,
                end_date,
                warmup_start,
                end_date,
                start_date,
                DEFAULT_CONVOLUTION_KERNEL_NAME,
            ],
        )
        .map_err(|e| format!("计算并写入卷积排名失败: {e}"))?;
    if saved_rows == 0 {
        return Err(format!(
            "区间{start_date}至{end_date}没有可写入的卷积排名；每只股票至少需要{}个连续评分交易日",
            kernel.len()
        ));
    }
    let trade_dates = tx
        .query_row(
            "SELECT COUNT(DISTINCT trade_date) FROM convolution_rank WHERE kernel_name = ? AND trade_date >= ? AND trade_date <= ?",
            params![DEFAULT_CONVOLUTION_KERNEL_NAME, start_date, end_date],
            |row| row.get::<_, i64>(0),
        )
        .map_err(|e| format!("统计卷积排名交易日失败: {e}"))?
        .max(0) as usize;
    tx.commit()
        .map_err(|e| format!("提交卷积排名结果失败: {e}"))?;

    Ok((saved_rows, trade_dates))
}

pub fn run_convolution_rank_compute(
    source_path: &str,
    start_date: &str,
    end_date: &str,
) -> Result<ConvolutionRankComputeResult, String> {
    let source_path = source_path.trim();
    if source_path.is_empty() {
        return Err("数据目录为空，请先到数据管理页确认当前目录".to_string());
    }
    let start_date = normalize_trade_date(start_date)
        .ok_or_else(|| "开始日期格式无效，应为 YYYYMMDD 或 YYYY-MM-DD".to_string())?;
    let end_date = normalize_trade_date(end_date)
        .ok_or_else(|| "结束日期格式无效，应为 YYYYMMDD 或 YYYY-MM-DD".to_string())?;
    if start_date > end_date {
        return Err("开始日期不能晚于结束日期".to_string());
    }
    if !result_db_path(source_path).exists() {
        return Err("scoring_result.db 不存在，请先执行排名计算".to_string());
    }

    let started_at = Instant::now();
    let mut conn = open_result_conn(source_path)?;
    let kernel = default_convolution_kernel();
    let (saved_rows, trade_dates) =
        compute_convolution_rank_range(&mut conn, &start_date, &end_date, &kernel)?;

    Ok(ConvolutionRankComputeResult {
        action: "convolution-rank".to_string(),
        kernel_name: DEFAULT_CONVOLUTION_KERNEL_NAME.to_string(),
        window_size: kernel.len(),
        start_date,
        end_date,
        elapsed_ms: started_at.elapsed().as_millis() as u64,
        saved_rows,
        trade_dates,
    })
}

/// 返回回测选定的 H30-L50 卷积排行榜。
///
/// 排名先在完整 30 日窗口股票池中全局计算，再应用页面筛选和条数限制。
pub fn get_convolution_rank_page(
    source_path: String,
    trade_date: Option<String>,
    limit: Option<u32>,
    board: Option<String>,
    exclude_st_board: Option<bool>,
    total_mv_min: Option<f64>,
    total_mv_max: Option<f64>,
) -> Result<ConvolutionRankPageData, String> {
    if let (Some(min_value), Some(max_value)) = (total_mv_min, total_mv_max) {
        if min_value > max_value {
            return Err("总市值最小值不能大于最大值".to_string());
        }
    }

    let conn = open_result_conn(&source_path)?;
    let effective_trade_date = resolve_trade_date(&conn, trade_date)?;
    let kernel = default_convolution_kernel();
    let (history_trade_dates, score_rows) =
        load_recent_score_rows(&conn, &effective_trade_date, kernel.len())?;
    let ranking = load_stored_convolution_ranking(
        &conn,
        &effective_trade_date,
        &history_trade_dates,
        &score_rows,
    )?
    .map(Ok)
    .unwrap_or_else(|| calc_convolution_ranking(&score_rows, &effective_trade_date, &kernel))?;
    let universe_size = ranking.len();

    let name_map = build_name_map(&source_path)?;
    let total_mv_map = build_total_mv_map(&source_path)?;
    let concepts_map = build_concepts_map(&source_path)?;
    let board_filter = board
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty() && value != "全部");
    let exclude_st_board = exclude_st_board.unwrap_or(false);
    let effective_limit = limit.filter(|value| *value > 0).map(|value| value as usize);

    let mut rows = Vec::new();
    for rank_row in ranking {
        let name = name_map.get(&rank_row.ts_code).cloned().unwrap_or_default();
        let board_value = board_category(&rank_row.ts_code, Some(&name)).to_string();
        if exclude_st_board && board_value == BOARD_ST {
            continue;
        }
        if board_filter
            .as_ref()
            .is_some_and(|filter| filter != &board_value)
        {
            continue;
        }
        if !filter_mv(&total_mv_map, &rank_row.ts_code, total_mv_min, total_mv_max) {
            continue;
        }

        rows.push(ConvolutionRankItem {
            total_mv_yi: total_mv_map.get(&rank_row.ts_code).copied(),
            concept: concepts_map
                .get(&rank_row.ts_code)
                .cloned()
                .unwrap_or_default(),
            name,
            board: board_value,
            ts_code: rank_row.ts_code,
            trade_date: rank_row.trade_date,
            database_rank: rank_row.database_rank,
            raw_rank: rank_row.raw_rank,
            convolution_rank: rank_row.convolution_rank,
            rank_change: rank_row.rank_change,
            raw_score: rank_row.raw_score,
            convolution_score: rank_row.convolved_score,
            score_history: rank_row.score_history,
        });

        if effective_limit.is_some_and(|limit| rows.len() >= limit) {
            break;
        }
    }

    Ok(ConvolutionRankPageData {
        rows,
        resolved_trade_date: effective_trade_date,
        kernel_name: DEFAULT_CONVOLUTION_KERNEL_NAME.to_string(),
        kernel,
        history_trade_dates,
        universe_size,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recent_score_rows_use_requested_complete_window() {
        let conn = Connection::open_in_memory().expect("in-memory db should open");
        conn.execute_batch(
            r#"
            CREATE TABLE score_summary (
                ts_code TEXT,
                trade_date TEXT,
                total_score DOUBLE,
                rank BIGINT
            );
            INSERT INTO score_summary VALUES
                ('000001.SZ', '20240101', 10.0, 2),
                ('000001.SZ', '20240102', 20.0, 1),
                ('000001.SZ', '20240103', 30.0, 1),
                ('000002.SZ', '20240102', 15.0, 2),
                ('000002.SZ', '20240103', 25.0, 2);
            "#,
        )
        .expect("fixture should be created");

        let (dates, rows) =
            load_recent_score_rows(&conn, "20240103", 2).expect("window should load");
        assert_eq!(dates, vec!["20240102", "20240103"]);
        assert_eq!(rows.len(), 4);
        assert!(
            load_recent_score_rows(&conn, "20240101", 2)
                .unwrap_err()
                .contains("没有完整的2个评分交易日")
        );
    }

    #[test]
    fn convolution_compute_persists_ranked_date_range() {
        let mut conn = Connection::open_in_memory().expect("in-memory db should open");
        conn.execute_batch(
            r#"
            CREATE TABLE score_summary (
                ts_code TEXT,
                trade_date TEXT,
                total_score DOUBLE,
                rank BIGINT
            );
            INSERT INTO score_summary VALUES
                ('000001.SZ', '20240101', 0.0, 2),
                ('000001.SZ', '20240102', 0.0, 2),
                ('000001.SZ', '20240103', 100.0, 1),
                ('000001.SZ', '20240104', 0.0, 2),
                ('000002.SZ', '20240101', 60.0, 1),
                ('000002.SZ', '20240102', 60.0, 1),
                ('000002.SZ', '20240103', 60.0, 2),
                ('000002.SZ', '20240104', 60.0, 1);
            "#,
        )
        .expect("fixture should be created");

        let (saved_rows, trade_dates) =
            compute_convolution_rank_range(&mut conn, "20240103", "20240104", &[0.5, 0.3, 0.2])
                .expect("convolution range should compute");
        assert_eq!(saved_rows, 4);
        assert_eq!(trade_dates, 2);

        let first_code: String = conn
            .query_row(
                "SELECT ts_code FROM convolution_rank WHERE trade_date = '20240103' ORDER BY convolution_rank LIMIT 1",
                [],
                |row| row.get(0),
            )
            .expect("top convolution row should exist");
        assert_eq!(first_code, "000002.SZ");
        let change: i64 = conn
            .query_row(
                "SELECT rank_change FROM convolution_rank WHERE trade_date = '20240103' AND ts_code = '000001.SZ'",
                [],
                |row| row.get(0),
            )
            .expect("rank change should exist");
        assert_eq!(change, -1);
    }
}
