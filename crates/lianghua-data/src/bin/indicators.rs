use lianghua_data::data::{panel::refresh_with_progress, source_db_path};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let source = args
        .next()
        .ok_or("用法: indicators <source目录> [--rebuild]")?;
    let rebuild = match args.next().as_deref() {
        None => false,
        Some("--rebuild") => true,
        Some(_) => return Err("未知选项，仅支持--rebuild".into()),
    };
    if args.next().is_some() {
        return Err("参数过多".into());
    }
    let path = source_db_path(&source);
    if !path.exists() {
        return Err(format!("原始库不存在：{}", path.display()).into());
    }
    let conn = duckdb::Connection::open(path)?;
    conn.execute_batch("SET threads=1; SET memory_limit='2GB'")?;
    let count = refresh_with_progress(
        &conn,
        &source,
        rebuild,
        Some(&|done, total, name| eprintln!("{done}/{total}：{name}")),
    )?;
    println!("跨股票指标已更新：{count}个交易日；指标由ind.toml定义");
    Ok(())
}
