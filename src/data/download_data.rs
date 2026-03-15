use std::{fs::create_dir_all, path::Path};

use duckdb::Connection;

pub fn init_stock_data_db(db_path: &str) -> Result<(), String> {
    // stock_market_data
    let source_path = Path::new(db_path);
    if let Some(source_parent) = source_path.parent() {
        if !source_parent.as_os_str().is_empty() {
            create_dir_all(source_parent).map_err(|e| format!("创建输出目录失败:{e}"))?;
        }
    }
    let conn = Connection::open(db_path).map_err(|e| format!("打开数据库失败:{e}"))?;
    conn.execute(
        r#"
            CREATE TABLE IF NOT EXISTS stock_list (
                ts_code VARCHAR,
                symbol VARCHAR,
                name VARCHAR,
                area VARCHAR,
                industry VARCHAR,
                list_date VARCHAR,
                trade_date VARCHAR,
                total_share VARCHAR,
                float_share VARCHAR,
                total_mv VARCHAR,
                circ_mv VARCHAR,
                PRIMARY KEY (ts_code, name)
            )
            "#,
        [],
    )
    .map_err(|e| format!("创建stock_data失败:{e}"))?;

    conn.execute(
        r#"
            CREATE TABLE IF NOT EXISTS stock_data (
                ts_code VARCHAR,
                symbol VARCHAR,
                name VARCHAR,
                area VARCHAR,
                industry VARCHAR,
                list_date VARCHAR,
                trade_date VARCHAR,
                total_share VARCHAR,
                float_share VARCHAR,
                total_mv VARCHAR,
                circ_mv VARCHAR,
                PRIMARY KEY (ts_code, name)
            )
            "#,
        [],
    )
    .map_err(|e| format!("创建stock_data失败:{e}"))?;
    Ok(())
}
