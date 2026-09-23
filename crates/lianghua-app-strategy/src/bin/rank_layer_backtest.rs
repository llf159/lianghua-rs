use std::{env, fs, process};

use lianghua_app_strategy::statistics::{
    run_rank_layer_backtest, run_transient_rank_layer_backtest,
};

fn main() {
    let arguments = env::args().skip(1).collect::<Vec<_>>();
    if arguments.len() != 6 {
        eprintln!(
            "用法: rank_layer_backtest <数据目录> <database|transient> <开始日期> <结束日期> <持有交易日> <输出JSON>"
        );
        process::exit(2);
    }
    let holding_period = arguments[4].parse::<usize>().unwrap_or_else(|error| {
        eprintln!("持有交易日必须是正整数:{error}");
        process::exit(2);
    });
    let common = (
        arguments[0].clone(),
        Some("qfq".to_string()),
        "000001.SH".to_string(),
        Some(0.5),
        Some(0.1),
        Some(0.1),
        arguments[2].clone(),
        arguments[3].clone(),
        Some(5),
        Some(60),
        Some(holding_period),
        Some(5),
        Some("sample_count".to_string()),
        None,
        Some(false),
    );
    let result = match arguments[1].as_str() {
        "database" => run_rank_layer_backtest(
            common.0, common.1, common.2, common.3, common.4, common.5, common.6, common.7,
            common.8, common.9, common.10, common.11, common.12, common.13, common.14,
        ),
        "transient" => run_transient_rank_layer_backtest(
            common.0, common.1, common.2, common.3, common.4, common.5, common.6, common.7,
            common.8, common.9, common.10, common.11, common.12, common.13, common.14,
        ),
        value => {
            eprintln!("模式必须是 database 或 transient，实际为:{value}");
            process::exit(2);
        }
    }
    .unwrap_or_else(|error| {
        eprintln!("排名整体回测失败:{error}");
        process::exit(1);
    });
    let payload = serde_json::to_vec_pretty(&result).unwrap_or_else(|error| {
        eprintln!("序列化结果失败:{error}");
        process::exit(1);
    });
    fs::write(&arguments[5], payload).unwrap_or_else(|error| {
        eprintln!("写入结果失败:{}:{error}", arguments[5]);
        process::exit(1);
    });
}
