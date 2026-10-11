use duckdb::{Connection, params};

use super::similarity_db_path;

// 只读挂载独立的相似度结果，视图仅在当前连接存在，不把结果复制进评分库。
pub fn attach_similarity_db(conn: &Connection, source_dir: &str) -> Result<bool, String> {
    let path = similarity_db_path(source_dir);
    if !path.exists() {
        return Ok(false);
    }
    let path = path.to_str().ok_or("相似度库路径不是有效UTF-8")?;
    conn.execute_batch(&format!(
        "ATTACH '{}' AS similarity_db (READ_ONLY)",
        path.replace('\'', "''")
    ))
    .map_err(|e| format!("只读挂载相似度库失败: {e}"))?;
    for table in [
        "strategy_trigger_similarity_rank",
        "strategy_trigger_similarity_rank_meta",
        "strategy_trigger_similarity_active_config",
        "strategy_trigger_similarity_summary",
    ] {
        let exists: bool = conn.query_row("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_catalog='similarity_db' AND table_schema='main' AND table_name=?)", params![table], |row| row.get(0))
            .map_err(|e| format!("检查相似度结果表失败: {e}"))?;
        if exists {
            conn.execute_batch(&format!(
                "CREATE TEMP VIEW {table} AS SELECT * FROM similarity_db.main.{table}"
            ))
            .map_err(|e| format!("挂载相似度结果视图失败: {e}"))?;
        }
    }
    Ok(true)
}
