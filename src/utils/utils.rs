use std::{
    fs,
    path::{Path, PathBuf},
};

// 用于兼容数据是否套有stock_data文件夹
fn choose_existing_or_default(candidates: Vec<PathBuf>) -> PathBuf {
    for p in &candidates {
        if p.exists() {
            return p.clone();
        }
    }
    // candidates 至少传入一个路径
    candidates[0].clone()
}

pub fn source_db_path(source_dir: &str) -> PathBuf {
    let root = Path::new(source_dir);
    choose_existing_or_default(vec![
        root.join("stock_data.db"),
        root.join("stock_data").join("stock_data.db"),
    ])
}

pub fn result_db_path(source_dir: &str) -> PathBuf {
    Path::new(source_dir)
        .join("output")
        .join("scoring_result.db")
}

pub fn result_ths_concepts_path(source_dir: &str) -> PathBuf {
    Path::new(source_dir)
        .join("concepts")
        .join("ths")
        .join("stock_concepts.csv")
}

fn stock_list_path(source_dir: &str) -> PathBuf {
    let root = Path::new(source_dir);
    choose_existing_or_default(vec![
        root.join("stock_list.csv"),
        root.join("stock_data").join("stock_list.csv"),
    ])
}

fn trade_calendar_path(source_dir: &str) -> PathBuf {
    let root = Path::new(source_dir);
    choose_existing_or_default(vec![
        root.join("trade_calendar.csv"),
        root.join("stock_data").join("trade_calendar.csv"),
    ])
}

pub fn load_stock_list(source_dir: &str) -> Result<Vec<Vec<String>>, String> {
    let stock_list_path = stock_list_path(source_dir);
    let text = std::fs::read_to_string(&stock_list_path)
        .map_err(|e| format!("读取stock_list.csv失败:路径:{:?},错误:{e}", stock_list_path))?;
    let mut lines = Vec::new();

    for (idx, line) in text.lines().enumerate() {
        if idx == 0 {
            continue;
        }

        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let line_box = line.split(',').map(|s| s.to_string()).collect();
        lines.push(line_box);
    }
    Ok(lines)
}

pub fn load_trade_date_list(source_dir: &str) -> Result<Vec<String>, String> {
    let path = trade_calendar_path(source_dir);
    let text = fs::read_to_string(path).map_err(|e| format!("读取trade_calendar.csv失败:{e}"))?;
    let mut trade_date_list = Vec::with_capacity(1024);
    for line in text.lines() {
        let line = line.trim();
        if !line.is_empty() {
            trade_date_list.push(line.to_string());
        }
    }
    Ok(trade_date_list)
}

pub fn load_ths_concepts_list(source_dir: &str) -> Result<Vec<Vec<String>>, String> {
    let path = result_ths_concepts_path(source_dir);

    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_path(&path)
        .map_err(|e| format!("打开stock_concepts.csv失败:路径:{:?},错误:{e}", path))?;

    let mut concept_list = Vec::with_capacity(6000);

    for row_result in reader.records() {
        let row = row_result.map_err(|e| format!("解析stock_concepts.csv失败:{e}"))?;
        let cols = row.iter().map(|value| value.to_string()).collect();
        concept_list.push(cols);
    }

    Ok(concept_list)
}

pub fn board_category(ts_code: &str) -> &'static str {
    let ts = ts_code.trim().to_ascii_uppercase();
    if ts.ends_with(".BJ") {
        return "北交所";
    }
    if (ts.ends_with(".SZ") && ts.starts_with("300"))
        || (ts.ends_with(".SH") && ts.starts_with("688"))
    {
        return "创业/科创";
    }
    if ts.ends_with(".SH") || ts.ends_with(".SZ") {
        return "主板";
    }
    "其他"
}
