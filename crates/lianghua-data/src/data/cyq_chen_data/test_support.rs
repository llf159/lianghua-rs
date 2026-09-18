use crate::data::chip_change_rule_path;
use crate::data::stock_list_path;
use crate::data::trade_calendar_path;
use crate::data::{cyq_chen_db_path, source_db_path};
use duckdb::{Connection, params};
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};
pub(in crate::data::cyq_chen_data) fn unique_temp_source_dir() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    std::env::temp_dir().join(format!("lianghua-cyq-chen-test-{nanos}"))
}

pub(in crate::data::cyq_chen_data) fn prepare_source_db(source_dir: &Path) {
    fs::create_dir_all(source_dir).expect("create temp dir");
    fs::write(
        trade_calendar_path(source_dir.to_str().expect("utf8 path")),
        "cal_date\n20260401\n20260402\n20260403\n20260407\n20260408\n",
    )
    .expect("write trade calendar");
    fs::write(
        stock_list_path(source_dir.to_str().expect("utf8 path")),
        "ts_code,symbol,name,area,industry,list_date,market,total_share\n000001.SZ,000001,平安银行,深圳,银行,19910403,主板,20000\n",
    )
    .expect("write stock list");
    (|source_dir: &Path| {
        fs::write(
            chip_change_rule_path(source_dir.to_str().expect("utf8 path")),
            r#"
version = 1

[[strategy]]
name = "主力买入"
holder = "main"
direction = "buy"
when = "C > O AND ZHANG > 0 AND TOTAL_MV_YI > 0"
bias = 1.0

[[strategy]]
name = "散户卖出"
holder = "retail"
direction = "sell"
when = "RATEC > 1"
bias = 1.0
"#,
        )
        .expect("write strategy");
    })(source_dir);

    let source_db = source_db_path(source_dir.to_str().expect("utf8 path"));
    let conn = Connection::open(&source_db).expect("open source db");
    conn.execute(
        r#"
        CREATE TABLE stock_data (
            ts_code VARCHAR,
            trade_date VARCHAR,
            adj_type VARCHAR,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            pre_close DOUBLE,
            change DOUBLE,
            pct_chg DOUBLE,
            vol DOUBLE,
            amount DOUBLE,
            tor DOUBLE
        )
        "#,
        [],
    )
    .expect("create stock_data");

    let rows = [
        ("000001.SZ", "20260401", 10.0, 10.3, 9.8, 10.1, 5.0),
        ("000001.SZ", "20260402", 10.1, 10.4, 10.0, 10.3, 5.0),
        ("000001.SZ", "20260403", 10.3, 10.8, 10.2, 10.6, 5.0),
        ("000001.SZ", "20260407", 10.6, 11.6, 10.5, 11.4, 5.0),
        ("000001.SZ", "20260408", 11.4, 11.8, 11.0, 11.6, 5.0),
        ("000002.SZ", "20260401", 20.0, 20.2, 19.8, 20.1, 3.0),
        ("000002.SZ", "20260402", 20.1, 20.4, 20.0, 20.3, 3.0),
    ];

    for (ts_code, trade_date, open, high, low, close, tor) in rows {
        insert_stock_row(&conn, ts_code, trade_date, open, high, low, close, tor);
    }
}

pub(in crate::data::cyq_chen_data) fn insert_stock_row(
    conn: &Connection,
    ts_code: &str,
    trade_date: &str,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    tor: f64,
) {
    conn.execute(
        r#"
        INSERT INTO stock_data (
            ts_code, trade_date, adj_type, open, high, low, close,
            pre_close, change, pct_chg, vol, amount, tor
        ) VALUES (?, ?, 'qfq', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        "#,
        params![
            ts_code, trade_date, open, high, low, close, close, 0.0_f64, 0.0_f64, 1.0_f64, 1.0_f64,
            tor
        ],
    )
    .expect("insert source row");
}

pub(in crate::data::cyq_chen_data) fn insert_paused_stock_resume_row(source_dir: &Path) {
    let source_db = source_db_path(source_dir.to_str().expect("utf8 path"));
    let conn = Connection::open(&source_db).expect("open source db");
    insert_stock_row(&conn, "000002.SZ", "20260408", 20.3, 21.0, 20.2, 20.8, 4.0);
}

pub(in crate::data::cyq_chen_data) fn snapshot_rows_for_compare(
    source_path: &str,
) -> Vec<(String, String, f64, f64, f64)> {
    let cyq_chen_db = cyq_chen_db_path(source_path);
    let conn = Connection::open(&cyq_chen_db).expect("open cyq chen db");
    let mut stmt = conn
        .prepare(
            r#"
            SELECT ts_code, trade_date, close, main_total, retail_total
            FROM cyq_chen_snapshot
            ORDER BY ts_code ASC, trade_date ASC
            "#,
        )
        .expect("prepare snapshot compare");
    let mut rows = stmt.query([]).expect("query snapshot compare");
    let mut out = Vec::new();
    while let Some(row) = rows.next().expect("read snapshot compare") {
        out.push((
            row.get(0).expect("ts_code"),
            row.get(1).expect("trade_date"),
            row.get(2).expect("close"),
            row.get(3).expect("main_total"),
            row.get(4).expect("retail_total"),
        ));
    }
    out
}

pub(in crate::data::cyq_chen_data) fn bin_rows_for_compare(
    source_path: &str,
) -> Vec<(String, String, i64, f64, f64, f64, f64)> {
    let cyq_chen_db = cyq_chen_db_path(source_path);
    let conn = Connection::open(&cyq_chen_db).expect("open cyq chen db");
    let mut stmt = conn
        .prepare(
            r#"
            SELECT ts_code, trade_date, bin_index, price_low, price_high, main_chip, retail_chip
            FROM cyq_chen_bin
            ORDER BY ts_code ASC, trade_date ASC, bin_index ASC
            "#,
        )
        .expect("prepare bin compare");
    let mut rows = stmt.query([]).expect("query bin compare");
    let mut out = Vec::new();
    while let Some(row) = rows.next().expect("read bin compare") {
        out.push((
            row.get(0).expect("ts_code"),
            row.get(1).expect("trade_date"),
            row.get(2).expect("bin_index"),
            row.get(3).expect("price_low"),
            row.get(4).expect("price_high"),
            row.get(5).expect("main_chip"),
            row.get(6).expect("retail_chip"),
        ));
    }
    out
}

pub(in crate::data::cyq_chen_data) fn meta_rows_for_compare(
    source_path: &str,
) -> Vec<(String, String)> {
    let cyq_chen_db = cyq_chen_db_path(source_path);
    let conn = Connection::open(&cyq_chen_db).expect("open cyq chen db");
    let mut stmt = conn
        .prepare("SELECT key, value FROM cyq_chen_meta ORDER BY key")
        .expect("prepare meta compare");
    let mut rows = stmt.query([]).expect("query meta compare");
    let mut out = Vec::new();
    while let Some(row) = rows.next().expect("read meta compare") {
        out.push((
            row.get(0).expect("meta key"),
            row.get(1).expect("meta value"),
        ));
    }
    out
}

pub(in crate::data::cyq_chen_data) fn index_names_for_compare(source_path: &str) -> Vec<String> {
    let cyq_chen_db = cyq_chen_db_path(source_path);
    let conn = Connection::open(&cyq_chen_db).expect("open cyq chen db");
    let mut stmt = conn
        .prepare(
            "SELECT index_name FROM duckdb_indexes() WHERE table_name IN ('cyq_chen_snapshot', 'cyq_chen_bin') ORDER BY index_name",
        )
        .expect("prepare index compare");
    let mut rows = stmt.query([]).expect("query index compare");
    let mut out = Vec::new();
    while let Some(row) = rows.next().expect("read index compare") {
        out.push(row.get(0).expect("index name"));
    }
    out
}
