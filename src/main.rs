use std::{env, fs};

use duckdb::Connection;
use lianghua_data::data::{resolve_strategy_path, result_db_path, source_db_path};
use lianghua_model::DownloadProgress;
use lianghua_scoring::scoring::runner::scoring_all_to_db;

fn main() -> Result<(), String> {
    let mut args = env::args().skip(1);
    let Some(source_path) = args.next() else {
        return Err("用法: lianghua-rs <数据目录> [策略文件路径]".to_string());
    };
    if source_path == "--help" || source_path == "-h" {
        println!("用法: lianghua-rs <数据目录> [策略文件路径]");
        return Ok(());
    }
    let strategy_path = args.next();
    if args.next().is_some() {
        return Err("用法: lianghua-rs <数据目录> [策略文件路径]".to_string());
    }

    let source_db = source_db_path(&source_path);
    if !source_db.is_file() {
        return Err(format!("原始行情库不存在: {}", source_db.display()));
    }
    let strategy_file = resolve_strategy_path(&source_path, strategy_path.as_deref());
    if !strategy_file.is_file() {
        return Err(format!("策略文件不存在: {}", strategy_file.display()));
    }

    let result_db = result_db_path(&source_path);
    if !result_db.is_file() {
        return Err(format!(
            "结果库不存在，无法确定一键重算区间: {}",
            result_db.display()
        ));
    }
    let result_db_string = result_db
        .to_str()
        .ok_or_else(|| "结果库路径不是有效 UTF-8".to_string())?;
    let conn = Connection::open(result_db_string).map_err(|e| format!("打开结果库失败: {e}"))?;
    let table_exists = conn
        .query_row(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='main' AND table_name='score_summary'",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map_err(|e| format!("检查结果库表结构失败: {e}"))?;
    if table_exists == 0 {
        return Err("结果库缺少 score_summary，无法确定一键重算区间".to_string());
    }
    let (start_date, end_date): (Option<String>, Option<String>) = conn
        .query_row(
            "SELECT MIN(trade_date), MAX(trade_date) FROM score_summary",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(|e| format!("读取结果库日期范围失败: {e}"))?;
    let start_date = start_date.ok_or_else(|| "结果库没有可用交易日，无法一键重算".to_string())?;
    let end_date = end_date.ok_or_else(|| "结果库没有可用交易日，无法一键重算".to_string())?;
    drop(conn);

    fs::remove_file(&result_db).map_err(|e| format!("删除旧结果库失败: {e}"))?;
    println!("已删除旧结果库，开始重算 {start_date} 至 {end_date}。");

    let progress = |item: DownloadProgress| {
        if item.total == 0 || item.finished == item.total {
            println!("{}", item.message);
        }
    };
    let strategy_file_string = strategy_file.to_string_lossy().into_owned();
    let profile = scoring_all_to_db(
        &source_path,
        Some(&strategy_file_string),
        "qfq",
        &start_date,
        &end_date,
        Some(&progress),
    )?;
    println!(
        "结果库重算完成：{} 只股票，耗时 {} ms。",
        profile.stock_count, profile.total_ms
    );
    for warning in profile.warnings {
        eprintln!("提示：{warning}");
    }
    Ok(())
}
