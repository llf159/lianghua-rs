//! 共享测试夹具：样例配置、行情与数值断言。

use crate::data::RowData;
use crate::data::cyq_chen::ChipChangeConfig;
use std::collections::HashMap;
pub(in crate::data::cyq_chen) fn sample_config() -> ChipChangeConfig {
    ChipChangeConfig::from_toml_str(
        r#"
version = 1

[[strategy]]
name = "main buy"
holder = "main"
direction = "buy"
when = "C > O"
bias = 1.0
"#,
    )
    .expect("config should parse")
}

pub(in crate::data::cyq_chen) fn sample_row_data() -> RowData {
    let mut cols = HashMap::new();
    cols.insert(
        "O".to_string(),
        vec![Some(10.0), Some(10.0), Some(10.1), Some(10.6), Some(11.2)],
    );
    cols.insert(
        "H".to_string(),
        vec![Some(10.2), Some(10.2), Some(10.8), Some(11.6), Some(11.8)],
    );
    cols.insert(
        "L".to_string(),
        vec![Some(9.8), Some(9.8), Some(9.9), Some(10.2), Some(10.8)],
    );
    cols.insert(
        "C".to_string(),
        vec![Some(10.1), Some(10.1), Some(10.6), Some(11.4), Some(11.6)],
    );
    cols.insert(
        "TOR".to_string(),
        vec![Some(10.0), Some(10.0), Some(10.0), Some(10.0), Some(10.0)],
    );

    RowData {
        trade_dates: vec![
            "20240102".to_string(),
            "20240103".to_string(),
            "20240104".to_string(),
            "20240105".to_string(),
            "20240108".to_string(),
        ],
        cols,
    }
}

pub(in crate::data::cyq_chen) fn assert_close(actual: f64, expected: f64) {
    assert!(
        (actual - expected).abs() < 1e-6,
        "actual={actual}, expected={expected}"
    );
}

pub(in crate::data::cyq_chen) fn program_contains_call(
    program: &crate::expr::parser::Stmts,
) -> bool {
    program.item.iter().any(|stmt| match stmt {
        crate::expr::parser::Stmt::Assign { value, .. } => {
            crate::data::cyq_chen::config::expr_contains_call(value)
        }
        crate::expr::parser::Stmt::Expr(expr) => {
            crate::data::cyq_chen::config::expr_contains_call(expr)
        }
    })
}
