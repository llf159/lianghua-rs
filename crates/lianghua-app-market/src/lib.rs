use lianghua_core::{expr, utils};
use lianghua_data::data;
use lianghua_download::download;
use lianghua_provider::crawler;
use lianghua_scoring::scoring;

pub mod all_market_monitor;
pub mod dragon_tiger;
pub mod intraday_monitor;
pub mod realtime;
pub mod watch_observe;

mod daily_baseline;
mod scene_stage;
pub mod volume_ratio;

// 监控只读排名；未计算的结果用连接内空表表示，不创建磁盘结果库。
fn open_monitor_result_conn(source_path: &str) -> Result<duckdb::Connection, String> {
    let result_db = data::result_db_path(source_path);
    let conn = if result_db.exists() {
        data::open_db_readonly(&result_db).map_err(|e| format!("打开结果库失败: {e}"))?
    } else {
        duckdb::Connection::open_in_memory().map_err(|e| format!("打开监控查询连接失败: {e}"))?
    };
    for (table, columns) in [
        (
            "score_summary",
            "trade_date VARCHAR, ts_code VARCHAR, rank BIGINT, total_score DOUBLE",
        ),
        (
            "scene_details",
            "trade_date VARCHAR, ts_code VARCHAR, scene_name VARCHAR, direction VARCHAR, stage_score DOUBLE, risk_score DOUBLE, confirm_strength DOUBLE, risk_intensity DOUBLE, stage VARCHAR, scene_rank BIGINT",
        ),
    ] {
        let exists: bool = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema='main' AND table_name=?)",
            [table], |row| row.get(0),
        ).map_err(|e| format!("检查监控排名表失败: {e}"))?;
        if !exists {
            conn.execute_batch(&format!("CREATE TEMP TABLE {table} ({columns})"))
                .map_err(|e| format!("初始化监控空排名表失败: {e}"))?;
        }
    }
    data::attach_similarity_db(&conn, source_path)?;
    Ok(conn)
}

#[cfg(test)]
mod tests {
    #[test]
    fn monitor_result_connection_allows_missing_ranking_data() {
        let source_dir = std::env::temp_dir().join(format!(
            "lianghua-monitor-optional-rank-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&source_dir).unwrap();
        let source_path = source_dir.to_str().unwrap();
        let result_path = super::data::result_db_path(source_path);

        // 缺库和已有库缺表均提供空排名，并且不写回磁盘。
        for existing_db in [false, true] {
            if existing_db {
                drop(duckdb::Connection::open(&result_path).unwrap());
            }
            let conn = super::open_monitor_result_conn(source_path).unwrap();
            let latest_date: Option<String> = conn
                .query_row("SELECT MAX(trade_date) FROM score_summary", [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(latest_date, None);
            let count: i64 = conn.query_row(
                "SELECT COUNT(*) FROM scene_details d LEFT JOIN score_summary s ON d.ts_code=s.ts_code AND d.trade_date=s.trade_date",
                [], |row| row.get(0)
            ).unwrap();
            assert_eq!(count, 0);
            drop(conn);
            assert_eq!(result_path.exists(), existing_db);
        }
        let conn = super::data::open_db_readonly(&result_path).unwrap();
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name IN ('score_summary', 'scene_details')",
            [], |row| row.get(0)
        ).unwrap();
        assert_eq!(count, 0);
        drop(conn);

        let conn = duckdb::Connection::open(&result_path).unwrap();
        conn.execute_batch("CREATE TABLE score_summary (trade_date VARCHAR, ts_code VARCHAR, rank BIGINT, total_score DOUBLE);").unwrap();
        drop(conn);
        let conn = super::open_monitor_result_conn(source_path).unwrap();
        let date: Option<String> = conn
            .query_row("SELECT MAX(trade_date) FROM score_summary", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(date, None);
        drop(conn);
        std::fs::remove_dir_all(source_dir).unwrap();
    }
}
