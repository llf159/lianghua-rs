use std::{collections::HashSet, fs::create_dir_all};

use duckdb::{Connection, params};

use crate::{
    data::dragon_tiger_db_path,
    download::{TopInstRow, TopListRow},
};

pub fn open_dragon_tiger_db(source_dir: &str) -> Result<Connection, String> {
    let db_path = dragon_tiger_db_path(source_dir);
    if let Some(parent) = db_path.parent() {
        create_dir_all(parent).map_err(|error| {
            format!(
                "创建龙虎榜数据库目录失败: path={}, err={error}",
                parent.display()
            )
        })?;
    }

    let conn = Connection::open(&db_path).map_err(|error| {
        format!(
            "打开龙虎榜数据库失败: path={}, err={error}",
            db_path.display()
        )
    })?;
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS top_list (
            trade_date VARCHAR NOT NULL,
            ts_code VARCHAR NOT NULL,
            name VARCHAR NOT NULL,
            close DOUBLE,
            pct_change DOUBLE,
            turnover_rate DOUBLE,
            amount DOUBLE,
            l_sell DOUBLE,
            l_buy DOUBLE,
            l_amount DOUBLE,
            net_amount DOUBLE,
            net_rate DOUBLE,
            amount_rate DOUBLE,
            float_values DOUBLE,
            reason VARCHAR NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_top_list_date_code
            ON top_list(trade_date, ts_code);

        CREATE TABLE IF NOT EXISTS top_inst (
            trade_date VARCHAR NOT NULL,
            ts_code VARCHAR NOT NULL,
            exalter VARCHAR NOT NULL,
            buy DOUBLE,
            buy_rate DOUBLE,
            sell DOUBLE,
            sell_rate DOUBLE,
            net_buy DOUBLE,
            side VARCHAR NOT NULL,
            reason VARCHAR NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_top_inst_date_code
            ON top_inst(trade_date, ts_code);

        CREATE TABLE IF NOT EXISTS dragon_tiger_sync_log (
            trade_date VARCHAR PRIMARY KEY,
            top_list_row_count BIGINT NOT NULL,
            top_inst_row_count BIGINT NOT NULL,
            synced_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        "#,
    )
    .map_err(|error| format!("初始化龙虎榜数据库失败: {error}"))?;
    Ok(conn)
}

pub fn load_synced_dragon_tiger_trade_dates(conn: &Connection) -> Result<HashSet<String>, String> {
    let mut stmt = conn
        .prepare("SELECT trade_date FROM dragon_tiger_sync_log")
        .map_err(|error| format!("预编译龙虎榜同步日期查询失败: {error}"))?;
    let mut rows = stmt
        .query([])
        .map_err(|error| format!("查询龙虎榜同步日期失败: {error}"))?;
    let mut dates = HashSet::new();

    while let Some(row) = rows
        .next()
        .map_err(|error| format!("读取龙虎榜同步日期失败: {error}"))?
    {
        let trade_date: String = row
            .get(0)
            .map_err(|error| format!("读取龙虎榜交易日期失败: {error}"))?;
        dates.insert(trade_date);
    }
    Ok(dates)
}

fn validate_trade_dates(
    trade_date: &str,
    top_list_rows: &[TopListRow],
    top_inst_rows: &[TopInstRow],
) -> Result<(), String> {
    if let Some(row) = top_list_rows
        .iter()
        .find(|row| row.trade_date != trade_date)
    {
        return Err(format!(
            "top_list 交易日期不匹配: 请求 {trade_date}，返回 {} / {}",
            row.ts_code, row.trade_date
        ));
    }
    if let Some(row) = top_inst_rows
        .iter()
        .find(|row| row.trade_date != trade_date)
    {
        return Err(format!(
            "top_inst 交易日期不匹配: 请求 {trade_date}，返回 {} / {}",
            row.ts_code, row.trade_date
        ));
    }
    Ok(())
}

pub fn replace_dragon_tiger_trade_date(
    conn: &mut Connection,
    trade_date: &str,
    top_list_rows: &[TopListRow],
    top_inst_rows: &[TopInstRow],
) -> Result<(), String> {
    validate_trade_dates(trade_date, top_list_rows, top_inst_rows)?;
    let tx = conn
        .transaction()
        .map_err(|error| format!("创建龙虎榜写入事务失败: {error}"))?;
    tx.execute("DELETE FROM top_list WHERE trade_date = ?", [trade_date])
        .map_err(|error| format!("删除 {trade_date} 旧龙虎榜每日明细失败: {error}"))?;
    tx.execute("DELETE FROM top_inst WHERE trade_date = ?", [trade_date])
        .map_err(|error| format!("删除 {trade_date} 旧龙虎榜席位明细失败: {error}"))?;

    {
        let mut appender = tx
            .appender("top_list")
            .map_err(|error| format!("创建 top_list Appender 失败: {error}"))?;
        for row in top_list_rows {
            appender
                .append_row(params![
                    &row.trade_date,
                    &row.ts_code,
                    &row.name,
                    row.close,
                    row.pct_change,
                    row.turnover_rate,
                    row.amount,
                    row.l_sell,
                    row.l_buy,
                    row.l_amount,
                    row.net_amount,
                    row.net_rate,
                    row.amount_rate,
                    row.float_values,
                    &row.reason,
                ])
                .map_err(|error| {
                    format!(
                        "写入 top_list 失败: trade_date={}, ts_code={}, err={error}",
                        row.trade_date, row.ts_code
                    )
                })?;
        }
        appender
            .flush()
            .map_err(|error| format!("刷新 top_list Appender 失败: {error}"))?;
    }

    {
        let mut appender = tx
            .appender("top_inst")
            .map_err(|error| format!("创建 top_inst Appender 失败: {error}"))?;
        for row in top_inst_rows {
            appender
                .append_row(params![
                    &row.trade_date,
                    &row.ts_code,
                    &row.exalter,
                    row.buy,
                    row.buy_rate,
                    row.sell,
                    row.sell_rate,
                    row.net_buy,
                    &row.side,
                    &row.reason,
                ])
                .map_err(|error| {
                    format!(
                        "写入 top_inst 失败: trade_date={}, ts_code={}, err={error}",
                        row.trade_date, row.ts_code
                    )
                })?;
        }
        appender
            .flush()
            .map_err(|error| format!("刷新 top_inst Appender 失败: {error}"))?;
    }

    tx.execute(
        r#"
        INSERT INTO dragon_tiger_sync_log (
            trade_date,
            top_list_row_count,
            top_inst_row_count,
            synced_at
        )
        VALUES (?, ?, ?, now())
        ON CONFLICT (trade_date) DO UPDATE SET
            top_list_row_count = EXCLUDED.top_list_row_count,
            top_inst_row_count = EXCLUDED.top_inst_row_count,
            synced_at = now()
        "#,
        params![
            trade_date,
            top_list_rows.len() as i64,
            top_inst_rows.len() as i64
        ],
    )
    .map_err(|error| format!("记录 {trade_date} 龙虎榜同步状态失败: {error}"))?;

    tx.commit()
        .map_err(|error| format!("提交 {trade_date} 龙虎榜写入事务失败: {error}"))
}

pub fn checkpoint_dragon_tiger(conn: &Connection) -> Result<(), String> {
    conn.execute_batch("CHECKPOINT")
        .map_err(|error| format!("龙虎榜数据库 CHECKPOINT 失败: {error}"))
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::*;

    fn temp_source_dir() -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        std::env::temp_dir().join(format!("lianghua_dragon_tiger_{nanos}"))
    }

    fn top_list_row(net_amount: f64) -> TopListRow {
        TopListRow {
            trade_date: "20260724".to_string(),
            ts_code: "000011.SZ".to_string(),
            name: "深物业A".to_string(),
            close: Some(8.26),
            pct_change: Some(9.98),
            turnover_rate: Some(4.76),
            amount: Some(299_493_417.0),
            l_sell: Some(46_809_546.66),
            l_buy: Some(74_805_567.96),
            l_amount: Some(121_615_114.62),
            net_amount: Some(net_amount),
            net_rate: Some(9.35),
            amount_rate: Some(40.61),
            float_values: Some(4_519_053_197.54),
            reason: "连续三个交易日内，涨幅偏离值累计达到20%的证券".to_string(),
        }
    }

    fn top_inst_row() -> TopInstRow {
        TopInstRow {
            trade_date: "20260724".to_string(),
            ts_code: "000011.SZ".to_string(),
            exalter: "深股通专用".to_string(),
            buy: Some(29_422_499.6),
            buy_rate: Some(9.82),
            sell: Some(10_352_000.0),
            sell_rate: Some(3.46),
            net_buy: Some(19_070_499.6),
            side: "0".to_string(),
            reason: "连续三个交易日内，涨幅偏离值累计达到20%的证券".to_string(),
        }
    }

    #[test]
    fn replace_trade_date_is_atomic_and_idempotent() {
        let source_dir = temp_source_dir();
        let source_path = source_dir.to_str().expect("utf8 path");
        let mut conn = open_dragon_tiger_db(source_path).expect("open db");

        replace_dragon_tiger_trade_date(
            &mut conn,
            "20260724",
            &[top_list_row(100.0)],
            &[top_inst_row()],
        )
        .expect("first write");
        replace_dragon_tiger_trade_date(
            &mut conn,
            "20260724",
            &[top_list_row(200.0)],
            &[top_inst_row()],
        )
        .expect("replacement");

        let top_list_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM top_list", [], |row| row.get(0))
            .expect("top_list count");
        let top_inst_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM top_inst", [], |row| row.get(0))
            .expect("top_inst count");
        let net_amount: f64 = conn
            .query_row("SELECT net_amount FROM top_list", [], |row| row.get(0))
            .expect("net amount");
        assert_eq!(top_list_count, 1);
        assert_eq!(top_inst_count, 1);
        assert_eq!(net_amount, 200.0);
        assert!(
            load_synced_dragon_tiger_trade_dates(&conn)
                .expect("load sync dates")
                .contains("20260724")
        );
        assert!(source_dir.join("dragon_tiger.db").exists());

        drop(conn);
        fs::remove_dir_all(source_dir).ok();
    }
}
