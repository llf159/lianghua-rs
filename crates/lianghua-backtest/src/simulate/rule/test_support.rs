//! 共享测试夹具：数值断言辅助与临时数据目录准备。

use crate::data::{result_db_path, source_db_path};
use duckdb::{Connection, params};
use std::fs::{create_dir_all, write};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

pub(super) fn assert_opt_close(left: Option<f64>, right: Option<f64>) {
    match (left, right) {
        (Some(a), Some(b)) => assert!((a - b).abs() < 1e-9, "left={a}, right={b}"),
        (None, None) => {}
        _ => panic!("left={left:?}, right={right:?}"),
    }
}

pub(super) fn temp_source_dir() -> PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    std::env::temp_dir().join(format!("lianghua_rule_layer_{unique}"))
}

pub(super) fn prepare_test_files(source_dir: &str) {
    create_dir_all(source_dir).expect("create source dir");

    write(
        PathBuf::from(source_dir).join("stock_list.csv"),
        concat!(
            "ts_code,symbol,name,area,industry,list_date,trade_date,total_share,float_share,total_mv,circ_mv,fullname,enname,cnspell,market,exchange,curr_type,list_status,delist_date,is_hs,act_name,act_ent_type\n",
            "000001.SZ,,样本股A,,main,20230001,,,,,,,,,,,,,,,,\n",
            "000002.SZ,,样本股B,,main,20230001,,,,,,,,,,,,,,,,\n"
        ),
    )
    .expect("write stock_list.csv");
    let trade_calendar = std::iter::once("cal_date".to_string())
        .chain((1..=70).map(|index| format!("202300{:02}", index)))
        .chain(
            ["20240102", "20240103", "20240104"]
                .into_iter()
                .map(str::to_string),
        )
        .collect::<Vec<_>>()
        .join("\n");
    write(
        PathBuf::from(source_dir).join("trade_calendar.csv"),
        format!("{trade_calendar}\n"),
    )
    .expect("write trade_calendar.csv");
    write(
        PathBuf::from(source_dir).join("stock_concepts.csv"),
        "ts_code,c1,c2,c3,concept\n000001.SZ,,,,concept-a\n000002.SZ,,,,concept-b\n",
    )
    .expect("write stock_concepts.csv");

    let source_conn = Connection::open(source_db_path(source_dir)).expect("open source db");
    source_conn
        .execute(
            r#"
            CREATE TABLE stock_data (
                ts_code VARCHAR,
                trade_date VARCHAR,
                adj_type VARCHAR,
                pct_chg DOUBLE,
                open DOUBLE,
                close DOUBLE
            )
            "#,
            [],
        )
        .expect("create stock_data");

    let mut source_app = source_conn
        .appender("stock_data")
        .expect("stock_data appender");

    source_app
        .append_row(params![
            "000001.SZ",
            "20240102",
            "qfq",
            0.0_f64,
            10.0_f64,
            10.0_f64
        ])
        .expect("stock a row1");
    source_app
        .append_row(params![
            "000001.SZ",
            "20240103",
            "qfq",
            3.0_f64,
            10.0_f64,
            10.3_f64
        ])
        .expect("stock a row2");
    source_app
        .append_row(params![
            "000001.SZ",
            "20240104",
            "qfq",
            5.0_f64,
            10.3_f64,
            10.815_f64
        ])
        .expect("stock a row3");

    source_app
        .append_row(params![
            "000002.SZ",
            "20240102",
            "qfq",
            0.0_f64,
            20.0_f64,
            20.0_f64
        ])
        .expect("stock b row1");
    source_app
        .append_row(params![
            "000002.SZ",
            "20240103",
            "qfq",
            1.0_f64,
            20.0_f64,
            20.2_f64
        ])
        .expect("stock b row2");
    source_app
        .append_row(params![
            "000002.SZ",
            "20240104",
            "qfq",
            -1.0_f64,
            20.2_f64,
            19.998_f64
        ])
        .expect("stock b row3");

    source_app
        .append_row(params![
            "000300.SH",
            "20240102",
            "ind",
            0.0_f64,
            100.0_f64,
            100.0_f64
        ])
        .expect("index row1");
    source_app
        .append_row(params![
            "000300.SH",
            "20240103",
            "ind",
            0.0_f64,
            100.0_f64,
            100.0_f64
        ])
        .expect("index row2");
    source_app
        .append_row(params![
            "000300.SH",
            "20240104",
            "ind",
            0.0_f64,
            100.0_f64,
            100.0_f64
        ])
        .expect("index row3");
    source_app.flush().expect("flush stock_data");

    let result_conn = Connection::open(result_db_path(source_dir)).expect("open result db");
    result_conn
        .execute(
            r#"
            CREATE TABLE score_summary (
                ts_code VARCHAR,
                trade_date VARCHAR,
                total_score DOUBLE,
                rank BIGINT
            )
            "#,
            [],
        )
        .expect("create score_summary");
    result_conn
        .execute(
            r#"
            CREATE TABLE rule_details (
                rule_name VARCHAR,
                ts_code VARCHAR,
                trade_date VARCHAR,
                rule_score DOUBLE
            )
            "#,
            [],
        )
        .expect("create rule_details");

    let mut summary_app = result_conn
        .appender("score_summary")
        .expect("score_summary appender");
    summary_app
        .append_row(params!["000001.SZ", "20240102", 10.0_f64, 1_i64])
        .expect("summary row1");
    summary_app
        .append_row(params!["000002.SZ", "20240102", 9.0_f64, 2_i64])
        .expect("summary row2");
    summary_app
        .append_row(params!["000001.SZ", "20240103", 11.0_f64, 1_i64])
        .expect("summary row3");
    summary_app
        .append_row(params!["000002.SZ", "20240103", 8.0_f64, 2_i64])
        .expect("summary row4");
    summary_app.flush().expect("flush score_summary");

    let mut result_app = result_conn
        .appender("rule_details")
        .expect("rule_details appender");

    result_app
        .append_row(params!["规则A", "000001.SZ", "20240102", 1.0_f64])
        .expect("rule a row1");
    result_app
        .append_row(params!["规则A", "000002.SZ", "20240102", -1.0_f64])
        .expect("rule a row2");
    result_app
        .append_row(params!["规则A", "000001.SZ", "20240103", 2.0_f64])
        .expect("rule a row3");
    result_app
        .append_row(params!["规则A", "000002.SZ", "20240103", -2.0_f64])
        .expect("rule a row4");

    result_app
        .append_row(params!["规则B", "000001.SZ", "20240102", 0.5_f64])
        .expect("rule b row1");
    result_app
        .append_row(params!["规则B", "000002.SZ", "20240102", 0.2_f64])
        .expect("rule b row2");
    result_app
        .append_row(params!["规则B", "000001.SZ", "20240103", 0.4_f64])
        .expect("rule b row3");
    result_app
        .append_row(params!["规则B", "000002.SZ", "20240103", 0.1_f64])
        .expect("rule b row4");
    result_app
        .append_row(params!["规则Zero", "000001.SZ", "20240102", 0.0_f64])
        .expect("rule zero row");
    result_app.flush().expect("flush rule_details");
}
