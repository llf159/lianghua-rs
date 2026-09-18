//! 共享测试夹具：临时数据目录与验证用源库准备。

use crate::data::source_db_path;
use crate::simulate::rule::RuleLayerPoint;
use duckdb::{Connection, params};
use std::collections::HashMap;
use std::fs::{create_dir_all, write};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};
pub(in crate::statistics) fn temp_source_dir() -> PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    std::env::temp_dir().join(format!("lianghua_validation_trigger_scores_{unique}"))
}

pub(in crate::statistics) fn prepare_validation_source_files(source_dir: &str) {
    create_dir_all(source_dir).expect("create source dir");

    write(
        PathBuf::from(source_dir).join("trade_calendar.csv"),
        "cal_date\n20240102\n20240103\n20240104\n",
    )
    .expect("write trade_calendar.csv");

    write(
        PathBuf::from(source_dir).join("stock_list.csv"),
        "ts_code,unused,name\n000001.SZ,,样本股\n",
    )
    .expect("write stock_list.csv");

    let source_conn = Connection::open(source_db_path(source_dir)).expect("open source db");
    source_conn
        .execute(
            r#"
                CREATE TABLE stock_data (
                    ts_code VARCHAR,
                    trade_date VARCHAR,
                    adj_type VARCHAR,
                    open DOUBLE,
                    high DOUBLE,
                    low DOUBLE,
                    close DOUBLE,
                    vol DOUBLE,
                    amount DOUBLE,
                    pre_close DOUBLE,
                    change DOUBLE,
                    pct_chg DOUBLE
                )
                "#,
            [],
        )
        .expect("create stock_data");

    let mut app = source_conn
        .appender("stock_data")
        .expect("stock_data appender");
    app.append_row(params![
        "000001.SZ",
        "20240102",
        "qfq",
        10.0_f64,
        10.5_f64,
        9.8_f64,
        10.2_f64,
        1000.0_f64,
        10000.0_f64,
        10.0_f64,
        0.2_f64,
        2.0_f64,
    ])
    .expect("insert stock row1");
    app.append_row(params![
        "000001.SZ",
        "20240103",
        "qfq",
        10.2_f64,
        11.0_f64,
        10.1_f64,
        10.8_f64,
        1100.0_f64,
        11000.0_f64,
        10.2_f64,
        0.6_f64,
        5.88_f64,
    ])
    .expect("insert stock row2");
    app.append_row(params![
        "000001.SZ",
        "20240104",
        "qfq",
        10.8_f64,
        11.3_f64,
        10.7_f64,
        11.1_f64,
        1200.0_f64,
        12000.0_f64,
        10.8_f64,
        0.3_f64,
        2.78_f64,
    ])
    .expect("insert stock row3");
    app.flush().expect("flush stock_data");
}

pub(in crate::statistics) fn validation_fold_test_point(
    index: usize,
    excess: f64,
) -> RuleLayerPoint {
    RuleLayerPoint {
        trade_date: format!("{index:08}"),
        sample_count: 10,
        avg_rule_score: Some(1.0),
        avg_residual_return: Some(excess),
        avg_excess_residual_return: Some(excess),
        score_weighted_residual_return: Some(excess),
        top_bottom_spread: Some(excess),
        ic: Some(excess),
    }
}

pub(in crate::statistics) fn validation_fold_test_axis(len: usize) -> Vec<RuleLayerPoint> {
    (0..len)
        .map(|index| validation_fold_test_point(index, 0.0))
        .collect()
}

pub(in crate::statistics) fn validation_calendar_of(points: &[RuleLayerPoint]) -> Vec<String> {
    points
        .iter()
        .map(|point| point.trade_date.clone())
        .collect::<Vec<_>>()
}

pub(in crate::statistics) fn validation_points_by_date<'a>(
    axis: &[&'a RuleLayerPoint],
) -> HashMap<&'a str, &'a RuleLayerPoint> {
    axis.iter()
        .map(|point| (point.trade_date.as_str(), *point))
        .collect::<HashMap<_, _>>()
}
