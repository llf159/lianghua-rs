use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs::create_dir_all,
    path::Path,
};

use duckdb::{Connection, params};
use rayon::prelude::*;

use crate::data::{
    concept_performance_db_path, load_stock_list, load_ths_concepts_list, source_db_path,
    ths_concepts_path,
};

const GDNM_BX_BIAO: &str = "concept_performance";
const MR_FQFS: &str = "qfq";
const GDNM_BX_DATES_PER_CHUNK: usize = 32;
const PERFORMANCE_TYPE_CONCEPT: &str = "concept";
const PERFORMANCE_TYPE_INDUSTRY: &str = "industry";
const PERFORMANCE_TYPE_BOARD: &str = "market";

#[derive(Debug, Clone, PartialEq)]
pub struct GdNmBXRow {
    pub trade_date: String,
    pub performance_type: String,
    pub concept: String,
    pub performance_pct: f64,
}

#[derive(Debug, Default, Clone, Copy)]
struct GdNmJxQr {
    jxqr_vhfu_he: f64,
    qrvs_he: f64,
}

fn round_to_3(value: f64) -> f64 {
    (value * 1000.0).round() / 1000.0
}

pub fn init_concept_performance_db(db_path: &Path) -> Result<(), String> {
    let db_wj = Path::new(db_path);
    if let Some(fu_mulu) = db_wj.parent() {
        if !fu_mulu.as_os_str().is_empty() {
            create_dir_all(fu_mulu).map_err(|e| format!("创建概念表现库目录失败:{e}"))?;
        }
    }

    let conn = Connection::open(db_path).map_err(|e| format!("打开概念表现库失败:{e}"))?;
    conn.execute(
        r#"
        CREATE TABLE IF NOT EXISTS concept_performance (
            trade_date VARCHAR,
            performance_type VARCHAR,
            concept VARCHAR,
            performance_pct DOUBLE,
            PRIMARY KEY (trade_date, performance_type, concept)
        )
        "#,
        [],
    )
    .map_err(|e| format!("创建concept_performance失败:{e}"))?;

    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_concept_perf_type_name_date ON concept_performance(performance_type, concept, trade_date)",
        [],
    )
    .map_err(|e| format!("创建concept_performance索引失败:{e}"))?;
    Ok(())
}

pub fn rebuild_concept_performance_all(source_dir: &str) -> Result<usize, String> {
    let source_db = source_db_path(source_dir);
    if !source_db.exists() {
        return Ok(0);
    }

    let source_db_str = source_db
        .to_str()
        .ok_or_else(|| "source_db路径不是有效UTF-8".to_string())?;
    let source_conn = Connection::open(source_db_str).map_err(|e| format!("打开原始库失败:{e}"))?;

    let biaocunzai = source_conn
        .query_row(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'stock_data'",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map_err(|e| format!("检查stock_data表失败:{e}"))?;
    if biaocunzai <= 0 {
        let bx_db = concept_performance_db_path(source_dir);
        init_concept_performance_db(&bx_db)?;
        clear_concept_performance_all(&bx_db)?;
        return Ok(0);
    }

    let mut stmt = source_conn
        .prepare("SELECT MIN(trade_date), MAX(trade_date) FROM stock_data WHERE adj_type = ?")
        .map_err(|e| format!("预编译概念表现全量日期范围失败:{e}"))?;
    let mut rows = stmt
        .query(params![MR_FQFS])
        .map_err(|e| format!("查询概念表现全量日期范围失败:{e}"))?;
    let Some(row) = rows
        .next()
        .map_err(|e| format!("读取概念表现日期范围失败:{e}"))?
    else {
        return Ok(0);
    };

    let ks_rq: Option<String> = row.get(0).map_err(|e| format!("读取最早交易日失败:{e}"))?;
    let js_rq: Option<String> = row.get(1).map_err(|e| format!("读取最晚交易日失败:{e}"))?;

    match (ks_rq, js_rq) {
        (Some(ks_rq), Some(js_rq)) => rebuild_concept_performance_range(source_dir, &ks_rq, &js_rq),
        _ => {
            let bx_db = concept_performance_db_path(source_dir);
            init_concept_performance_db(&bx_db)?;
            clear_concept_performance_all(&bx_db)?;
            Ok(0)
        }
    }
}

pub fn rebuild_concept_performance_range(
    source_dir: &str,
    start_date: &str,
    end_date: &str,
) -> Result<usize, String> {
    if start_date.trim().is_empty() || end_date.trim().is_empty() {
        return Ok(0);
    }
    if start_date > end_date {
        return Err(format!(
            "概念表现重建日期范围非法: {start_date} > {end_date}"
        ));
    }

    let concept_map = load_concept_map(source_dir)?.unwrap_or_default();
    let industry_map = load_industry_map(source_dir)?;
    let board_map = load_board_map(source_dir)?;

    let uivi_map = load_uivi_map(source_dir)?;
    if uivi_map.is_empty() {
        return Ok(0);
    }

    let mut bx_rows = Vec::new();
    if !concept_map.is_empty() {
        bx_rows.extend(calc_gdnm_bx_rows(
            source_dir,
            start_date,
            end_date,
            PERFORMANCE_TYPE_CONCEPT,
            &concept_map,
            &uivi_map,
        )?);
    }
    if !industry_map.is_empty() {
        bx_rows.extend(calc_gdnm_bx_rows(
            source_dir,
            start_date,
            end_date,
            PERFORMANCE_TYPE_INDUSTRY,
            &industry_map,
            &uivi_map,
        )?);
    }
    if !board_map.is_empty() {
        bx_rows.extend(calc_gdnm_bx_rows(
            source_dir,
            start_date,
            end_date,
            PERFORMANCE_TYPE_BOARD,
            &board_map,
            &uivi_map,
        )?);
    }

    let bx_db = concept_performance_db_path(source_dir);
    init_concept_performance_db(&bx_db)?;
    write_concept_performance_range(&bx_db, start_date, end_date, &bx_rows)?;
    Ok(bx_rows.len())
}

fn clear_concept_performance_all(db_path: &Path) -> Result<(), String> {
    let mut conn = Connection::open(db_path).map_err(|e| format!("打开概念表现库失败:{e}"))?;
    let tx = conn
        .transaction()
        .map_err(|e| format!("创建概念表现事务失败:{e}"))?;
    tx.execute("DELETE FROM concept_performance", [])
        .map_err(|e| format!("清空concept_performance失败:{e}"))?;
    tx.commit()
        .map_err(|e| format!("提交概念表现清空事务失败:{e}"))?;
    Ok(())
}

fn write_concept_performance_range(
    db_path: &Path,
    start_date: &str,
    end_date: &str,
    bx_rows: &[GdNmBXRow],
) -> Result<(), String> {
    let mut conn = Connection::open(db_path).map_err(|e| format!("打开概念表现库失败:{e}"))?;
    let tx = conn
        .transaction()
        .map_err(|e| format!("创建概念表现事务失败:{e}"))?;
    tx.execute(
        "DELETE FROM concept_performance WHERE trade_date >= ? AND trade_date <= ?",
        params![start_date, end_date],
    )
    .map_err(|e| format!("删除概念表现旧数据失败:{e}"))?;

    if !bx_rows.is_empty() {
        let mut app = tx
            .appender(GDNM_BX_BIAO)
            .map_err(|e| format!("创建concept_performance写入器失败:{e}"))?;
        for row in bx_rows {
            app.append_row(params![
                &row.trade_date,
                &row.performance_type,
                &row.concept,
                row.performance_pct
            ])
            .map_err(|e| {
                format!(
                    "写入概念表现失败, trade_date={}, performance_type={}, concept={}: {e}",
                    row.trade_date, row.performance_type, row.concept
                )
            })?;
        }
        app.flush()
            .map_err(|e| format!("刷新concept_performance写入器失败:{e}"))?;
    }

    tx.commit()
        .map_err(|e| format!("提交概念表现事务失败:{e}"))?;
    Ok(())
}

fn calc_gdnm_bx_rows(
    source_dir: &str,
    start_date: &str,
    end_date: &str,
    performance_type: &str,
    gdnm_map: &HashMap<String, Vec<String>>,
    uivi_map: &HashMap<String, f64>,
) -> Result<Vec<GdNmBXRow>, String> {
    let trade_dates = load_trade_dates_in_range(source_dir, start_date, end_date)?;
    if trade_dates.is_empty() {
        return Ok(Vec::new());
    }

    let date_ranges: Vec<(String, String)> = trade_dates
        .chunks(GDNM_BX_DATES_PER_CHUNK)
        .map(|chunk| {
            (
                chunk.first().cloned().unwrap_or_default(),
                chunk.last().cloned().unwrap_or_default(),
            )
        })
        .filter(|(chunk_start, chunk_end)| !chunk_start.is_empty() && !chunk_end.is_empty())
        .collect();

    let chunk_results: Vec<Result<Vec<GdNmBXRow>, String>> = date_ranges
        .par_iter()
        .map(|(chunk_start, chunk_end)| {
            calc_gdnm_bx_rows_chunk(
                source_dir,
                chunk_start,
                chunk_end,
                performance_type,
                gdnm_map,
                uivi_map,
            )
        })
        .collect();

    let mut bx_rows = Vec::new();
    for chunk_result in chunk_results {
        let mut chunk_rows = chunk_result?;
        bx_rows.append(&mut chunk_rows);
    }
    bx_rows.sort_by(|left, right| {
        left.trade_date
            .cmp(&right.trade_date)
            .then_with(|| left.performance_type.cmp(&right.performance_type))
            .then_with(|| left.concept.cmp(&right.concept))
    });

    Ok(bx_rows)
}

fn load_trade_dates_in_range(
    source_dir: &str,
    start_date: &str,
    end_date: &str,
) -> Result<Vec<String>, String> {
    let source_db = source_db_path(source_dir);
    let source_db_str = source_db
        .to_str()
        .ok_or_else(|| "source_db路径不是有效UTF-8".to_string())?;
    let conn = Connection::open(source_db_str).map_err(|e| format!("打开原始库失败:{e}"))?;

    let mut stmt = conn
        .prepare(
            r#"
            SELECT DISTINCT trade_date
            FROM stock_data
            WHERE adj_type = ?
              AND trade_date >= ?
              AND trade_date <= ?
            ORDER BY trade_date ASC
            "#,
        )
        .map_err(|e| format!("预编译概念表现日期列表查询失败:{e}"))?;
    let mut rows = stmt
        .query(params![MR_FQFS, start_date, end_date])
        .map_err(|e| format!("查询概念表现日期列表失败:{e}"))?;

    let mut trade_dates = Vec::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取概念表现日期列表失败:{e}"))?
    {
        let trade_date: String = row
            .get(0)
            .map_err(|e| format!("读取概念表现交易日失败:{e}"))?;
        if !trade_date.trim().is_empty() {
            trade_dates.push(trade_date);
        }
    }

    Ok(trade_dates)
}

fn calc_gdnm_bx_rows_chunk(
    source_dir: &str,
    start_date: &str,
    end_date: &str,
    performance_type: &str,
    gdnm_map: &HashMap<String, Vec<String>>,
    uivi_map: &HashMap<String, f64>,
) -> Result<Vec<GdNmBXRow>, String> {
    let source_db = source_db_path(source_dir);
    let source_db_str = source_db
        .to_str()
        .ok_or_else(|| "source_db路径不是有效UTF-8".to_string())?;
    let conn = Connection::open(source_db_str).map_err(|e| format!("打开原始库失败:{e}"))?;

    let mut stmt = conn
        .prepare(
            r#"
            SELECT
                ts_code,
                trade_date,
                TRY_CAST(pct_chg AS DOUBLE) AS pct_chg
            FROM stock_data
            WHERE adj_type = ?
              AND trade_date >= ?
              AND trade_date <= ?
            ORDER BY trade_date ASC, ts_code ASC
            "#,
        )
        .map_err(|e| format!("预编译概念表现分块查询失败:{e}"))?;
    let mut rows = stmt
        .query(params![MR_FQFS, start_date, end_date])
        .map_err(|e| format!("查询概念表现分块基础数据失败:{e}"))?;

    let mut juhe_map: BTreeMap<(String, String), GdNmJxQr> = BTreeMap::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取概念表现分块基础数据失败:{e}"))?
    {
        let ts_code: String = row.get(0).map_err(|e| format!("读取ts_code失败:{e}"))?;
        let trade_date: String = row.get(1).map_err(|e| format!("读取trade_date失败:{e}"))?;
        let pct_chg: Option<f64> = row.get(2).map_err(|e| format!("读取pct_chg失败:{e}"))?;

        let Some(vhfu) = pct_chg.filter(|v| v.is_finite()) else {
            continue;
        };
        let Some(uivi) = uivi_map.get(&ts_code).copied().filter(|v| *v > 0.0) else {
            continue;
        };
        let Some(gdnm_list) = gdnm_map.get(&ts_code) else {
            continue;
        };

        for gdnm in gdnm_list {
            let juhe = juhe_map
                .entry((trade_date.clone(), gdnm.clone()))
                .or_default();
            juhe.jxqr_vhfu_he += vhfu * uivi;
            juhe.qrvs_he += uivi;
        }
    }

    let mut bx_rows = Vec::with_capacity(juhe_map.len());
    for ((trade_date, concept), juhe) in juhe_map {
        if juhe.qrvs_he <= 0.0 || !juhe.qrvs_he.is_finite() {
            continue;
        }
        let performance_pct = round_to_3(juhe.jxqr_vhfu_he / juhe.qrvs_he);
        if !performance_pct.is_finite() {
            continue;
        }
        bx_rows.push(GdNmBXRow {
            trade_date,
            performance_type: performance_type.to_string(),
            concept,
            performance_pct,
        });
    }

    Ok(bx_rows)
}

fn load_concept_map(source_dir: &str) -> Result<Option<HashMap<String, Vec<String>>>, String> {
    let rows = match load_ths_concepts_list(source_dir) {
        Ok(rows) => rows,
        Err(error) if error.contains("打开stock_concepts.csv失败") => return Ok(None),
        Err(error) => return Err(error),
    };

    let mut gdnm_map = HashMap::with_capacity(rows.len());
    for cols in rows {
        let Some(ts_code) = cols.first().map(|v| v.trim()) else {
            continue;
        };
        let Some(gdnm_raw) = cols.get(2).map(|v| v.trim()) else {
            continue;
        };
        if ts_code.is_empty() || gdnm_raw.is_empty() {
            continue;
        }
        let gdnm_list = split_gdnm_items(gdnm_raw);
        if gdnm_list.is_empty() {
            continue;
        }
        gdnm_map
            .entry(ts_code.to_string())
            .and_modify(|old_list: &mut Vec<String>| {
                old_list.extend(gdnm_list.clone());
                *old_list = split_gdnm_items(&old_list.join(","));
            })
            .or_insert(gdnm_list);
    }

    Ok(Some(gdnm_map))
}

fn load_board_map(source_dir: &str) -> Result<HashMap<String, Vec<String>>, String> {
    let rows = load_stock_list(source_dir)?;
    let mut board_map = HashMap::with_capacity(rows.len());
    for cols in rows {
        let Some(ts_code) = cols.first().map(|v| v.trim()) else {
            continue;
        };
        let Some(board_raw) = cols.get(14).map(|v| v.trim()) else {
            continue;
        };
        if ts_code.is_empty() || board_raw.is_empty() {
            continue;
        }
        let board_list = split_gdnm_items(board_raw);
        if board_list.is_empty() {
            continue;
        }
        board_map
            .entry(ts_code.to_string())
            .and_modify(|old_list: &mut Vec<String>| {
                old_list.extend(board_list.clone());
                *old_list = split_gdnm_items(&old_list.join(","));
            })
            .or_insert(board_list);
    }
    Ok(board_map)
}

fn load_industry_map(source_dir: &str) -> Result<HashMap<String, Vec<String>>, String> {
    let rows = load_stock_list(source_dir)?;
    let mut industry_map = HashMap::with_capacity(rows.len());
    for cols in rows {
        let Some(ts_code) = cols.first().map(|v| v.trim()) else {
            continue;
        };
        let Some(industry_raw) = cols.get(4).map(|v| v.trim()) else {
            continue;
        };
        if ts_code.is_empty() || industry_raw.is_empty() {
            continue;
        }
        let industry_list = split_gdnm_items(industry_raw);
        if industry_list.is_empty() {
            continue;
        }
        industry_map
            .entry(ts_code.to_string())
            .and_modify(|old_list: &mut Vec<String>| {
                old_list.extend(industry_list.clone());
                *old_list = split_gdnm_items(&old_list.join(","));
            })
            .or_insert(industry_list);
    }
    Ok(industry_map)
}

fn load_uivi_map(source_dir: &str) -> Result<HashMap<String, f64>, String> {
    let rows = load_stock_list(source_dir)?;
    let mut uivi_map = HashMap::with_capacity(rows.len());
    for cols in rows {
        let Some(ts_code) = cols.first().map(|v| v.trim()) else {
            continue;
        };
        let Some(total_mv_raw) = cols.get(9).map(|v| v.trim()) else {
            continue;
        };
        let Ok(total_mv) = total_mv_raw.parse::<f64>() else {
            continue;
        };
        if ts_code.is_empty() || total_mv <= 0.0 || !total_mv.is_finite() {
            continue;
        }
        uivi_map.insert(ts_code.to_string(), total_mv);
    }
    Ok(uivi_map)
}

fn split_gdnm_items(value: &str) -> Vec<String> {
    let mut quis_map = HashMap::<String, String>::new();
    for raw in
        value.split(|ch| matches!(ch, ',' | ';' | '，' | '；' | '|' | '、' | '/' | '\n' | '\r'))
    {
        let trimed = raw.trim();
        if trimed.is_empty() {
            continue;
        }
        quis_map
            .entry(trimed.to_ascii_lowercase())
            .or_insert_with(|| trimed.to_string());
    }

    let mut gdnm_list = quis_map.into_values().collect::<Vec<_>>();
    gdnm_list.sort();
    gdnm_list
}

fn cosine_similarity(stock: &[f64], concept: &[f64]) -> Option<f64> {
    if stock.len() != concept.len() || stock.is_empty() {
        return None;
    }

    let mut dot = 0.0_f64;
    let mut stock_norm = 0.0_f64;
    let mut concept_norm = 0.0_f64;

    for (s, c) in stock.iter().zip(concept.iter()) {
        dot += s * c;
        stock_norm += s * s;
        concept_norm += c * c;
    }

    if stock_norm <= f64::EPSILON || concept_norm <= f64::EPSILON {
        return None;
    }

    Some(dot / (stock_norm.sqrt() * concept_norm.sqrt()))
}

#[derive(Debug, Clone, PartialEq)]
pub struct ConceptTrendPoint {
    pub trade_date: String,
    pub performance_pct: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ConceptTrendSeries {
    pub concept: String,
    pub points: Vec<ConceptTrendPoint>,
}

pub fn load_performance_trend_series(
    source_dir: &str,
    performance_type: &str,
    concept: &str,
    start_date: Option<&str>,
    end_date: Option<&str>,
) -> Result<ConceptTrendSeries, String> {
    let concept_name = concept.trim();
    if concept_name.is_empty() {
        return Err("名称不能为空".to_string());
    }
    let performance_type = performance_type.trim();
    if performance_type.is_empty() {
        return Err("表现类型不能为空".to_string());
    }

    let concept_db = concept_performance_db_path(source_dir);
    if !concept_db.exists() {
        return Ok(ConceptTrendSeries {
            concept: concept_name.to_string(),
            points: Vec::new(),
        });
    }

    let concept_db_str = concept_db
        .to_str()
        .ok_or_else(|| "concept_performance_db路径不是有效UTF-8".to_string())?;
    let conn = Connection::open(concept_db_str)
        .map_err(|e| format!("打开 concept_performance.db 失败: {e}"))?;

    let mut stmt = conn
        .prepare(
            r#"
            SELECT trade_date, TRY_CAST(performance_pct AS DOUBLE)
            FROM concept_performance
            WHERE performance_type = ?
              AND concept = ?
              AND (? IS NULL OR trade_date >= ?)
              AND (? IS NULL OR trade_date <= ?)
              AND trade_date IS NOT NULL
            ORDER BY trade_date ASC
            "#,
        )
        .map_err(|e| format!("预编译表现走势查询失败: {e}"))?;

    let mut rows = stmt
        .query(params![
            performance_type,
            concept_name,
            start_date,
            start_date,
            end_date,
            end_date
        ])
        .map_err(|e| format!("查询表现走势失败: {e}"))?;

    let mut points = Vec::new();
    while let Some(row) = rows.next().map_err(|e| format!("读取表现走势失败: {e}"))? {
        let trade_date: String = row
            .get(0)
            .map_err(|e| format!("读取表现走势日期失败: {e}"))?;
        let pct: Option<f64> = row
            .get(1)
            .map_err(|e| format!("读取表现走势涨跌幅失败: {e}"))?;
        let Some(performance_pct) = pct.filter(|v| v.is_finite()) else {
            continue;
        };
        points.push(ConceptTrendPoint {
            trade_date,
            performance_pct,
        });
    }

    Ok(ConceptTrendSeries {
        concept: concept_name.to_string(),
        points,
    })
}

pub fn load_concept_trend_series(
    source_dir: &str,
    concept: &str,
    start_date: Option<&str>,
    end_date: Option<&str>,
) -> Result<ConceptTrendSeries, String> {
    load_performance_trend_series(
        source_dir,
        PERFORMANCE_TYPE_CONCEPT,
        concept,
        start_date,
        end_date,
    )
}

pub fn load_industry_trend_series(
    source_dir: &str,
    industry: &str,
    start_date: Option<&str>,
    end_date: Option<&str>,
) -> Result<ConceptTrendSeries, String> {
    load_performance_trend_series(
        source_dir,
        PERFORMANCE_TYPE_INDUSTRY,
        industry,
        start_date,
        end_date,
    )
}

pub fn rebuild_most_related_concept_csv(source_dir: &str) -> Result<usize, String> {
    let source_db = source_db_path(source_dir);
    let source_db_str = source_db
        .to_str()
        .ok_or_else(|| "source_db路径不是有效UTF-8".to_string())?;
    let conn =
        Connection::open(source_db_str).map_err(|e| format!("打开 stock_data.db 失败: {e}"))?;

    let concept_db = concept_performance_db_path(source_dir);
    let concept_db_str = concept_db
        .to_str()
        .ok_or_else(|| "concept_performance_db路径不是有效UTF-8".to_string())?;
    let concept_conn = Connection::open(concept_db_str)
        .map_err(|e| format!("打开 concept_performance.db 失败: {e}"))?;

    let mut concept_stmt = concept_conn
        .prepare(
            r#"SELECT trade_date, concept, TRY_CAST(performance_pct AS DOUBLE)
               FROM concept_performance
               WHERE performance_type = 'concept'
                 AND trade_date IS NOT NULL
                 AND concept IS NOT NULL
               ORDER BY trade_date ASC"#,
        )
        .map_err(|e| format!("预编译概念表现查询失败: {e}"))?;
    let mut concept_rows = concept_stmt
        .query([])
        .map_err(|e| format!("查询概念表现失败: {e}"))?;

    let mut concept_series_map: HashMap<String, HashMap<String, f64>> = HashMap::new();
    while let Some(row) = concept_rows
        .next()
        .map_err(|e| format!("读取概念表现失败: {e}"))?
    {
        let trade_date: String = row
            .get(0)
            .map_err(|e| format!("读取概念表现交易日失败: {e}"))?;
        let concept: String = row.get(1).map_err(|e| format!("读取概念名称失败: {e}"))?;
        let pct: Option<f64> = row.get(2).map_err(|e| format!("读取概念涨跌幅失败: {e}"))?;
        let Some(pct) = pct.filter(|v| v.is_finite()) else {
            continue;
        };
        concept_series_map
            .entry(concept)
            .or_default()
            .insert(trade_date, pct);
    }

    let concepts_path = ths_concepts_path(source_dir);
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_path(&concepts_path)
        .map_err(|e| format!("打开 stock_concepts.csv 失败: {e}"))?;

    let headers = reader
        .headers()
        .map_err(|e| format!("读取 stock_concepts.csv 表头失败: {e}"))?
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>();

    let mut output_headers = headers;
    let related_col_name = "most_related_concept".to_string();
    let related_idx = output_headers
        .iter()
        .position(|h| h == &related_col_name)
        .unwrap_or_else(|| {
            output_headers.push(related_col_name.clone());
            output_headers.len() - 1
        });

    let mut out_rows: Vec<Vec<String>> = Vec::new();
    let mut ts_code_set: HashSet<String> = HashSet::new();

    for rec in reader.records() {
        let record = rec.map_err(|e| format!("读取 stock_concepts.csv 记录失败: {e}"))?;
        let mut row = record.iter().map(|v| v.to_string()).collect::<Vec<_>>();
        while row.len() <= related_idx {
            row.push(String::new());
        }
        if let Some(ts_code) = row.first().map(|v| v.trim()).filter(|v| !v.is_empty()) {
            ts_code_set.insert(ts_code.to_string());
        }
        out_rows.push(row);
    }

    let mut stock_stmt = conn
        .prepare(
            r#"SELECT ts_code, trade_date, TRY_CAST(pct_chg AS DOUBLE)
               FROM stock_data
               WHERE adj_type = 'qfq' AND trade_date IS NOT NULL
               ORDER BY ts_code ASC, trade_date ASC"#,
        )
        .map_err(|e| format!("预编译个股涨跌幅查询失败: {e}"))?;
    let mut stock_rows = stock_stmt
        .query([])
        .map_err(|e| format!("查询个股涨跌幅失败: {e}"))?;

    let mut stock_series_by_code: HashMap<String, HashMap<String, f64>> = HashMap::new();
    while let Some(row) = stock_rows
        .next()
        .map_err(|e| format!("读取个股涨跌幅失败: {e}"))?
    {
        let ts_code: String = row.get(0).map_err(|e| format!("读取 ts_code 失败: {e}"))?;
        if !ts_code_set.contains(&ts_code) {
            continue;
        }
        let trade_date: String = row
            .get(1)
            .map_err(|e| format!("读取 trade_date 失败: {e}"))?;
        let pct: Option<f64> = row.get(2).map_err(|e| format!("读取 pct_chg 失败: {e}"))?;
        if let Some(pct) = pct.filter(|v| v.is_finite()) {
            stock_series_by_code
                .entry(ts_code)
                .or_default()
                .insert(trade_date, pct);
        }
    }

    let best_map: HashMap<String, String> = out_rows
        .par_iter()
        .filter_map(|row| {
            let ts_code = row.first().map(|v| v.trim()).unwrap_or_default();
            let concept_raw = row.get(2).map(|v| v.trim()).unwrap_or_default();
            if ts_code.is_empty() || concept_raw.is_empty() {
                return None;
            }

            let stock_series_map = stock_series_by_code.get(ts_code)?;
            if stock_series_map.is_empty() {
                return None;
            }

            let candidates = split_gdnm_items(concept_raw);
            if candidates.is_empty() {
                return None;
            }

            let mut best_name = String::new();
            let mut best_sim = f64::NEG_INFINITY;

            for concept_name in candidates {
                let Some(concept_series) = concept_series_map.get(&concept_name) else {
                    continue;
                };

                let mut stock_vec = Vec::new();
                let mut concept_vec = Vec::new();
                for (trade_date, stock_pct) in stock_series_map {
                    if let Some(concept_pct) = concept_series.get(trade_date) {
                        stock_vec.push(*stock_pct);
                        concept_vec.push(*concept_pct);
                    }
                }

                if stock_vec.len() < 20 {
                    continue;
                }

                let Some(sim) = cosine_similarity(&stock_vec, &concept_vec) else {
                    continue;
                };

                if sim > best_sim {
                    best_sim = sim;
                    best_name = concept_name;
                }
            }

            if best_name.is_empty() {
                None
            } else {
                Some((ts_code.to_string(), best_name))
            }
        })
        .collect();

    let mut updated_count = 0usize;
    for row in &mut out_rows {
        let ts_code = row.first().map(|v| v.trim()).unwrap_or_default();
        if let Some(best_name) = best_map.get(ts_code) {
            row[related_idx] = best_name.clone();
            updated_count += 1;
        }
    }

    let mut writer = csv::Writer::from_path(&concepts_path)
        .map_err(|e| format!("重写 stock_concepts.csv 失败: {e}"))?;
    writer
        .write_record(output_headers.iter().map(String::as_str))
        .map_err(|e| format!("写入 stock_concepts.csv 表头失败: {e}"))?;
    for row in out_rows {
        writer
            .write_record(row.iter().map(String::as_str))
            .map_err(|e| format!("写入 stock_concepts.csv 记录失败: {e}"))?;
    }
    writer
        .flush()
        .map_err(|e| format!("刷新 stock_concepts.csv 失败: {e}"))?;

    Ok(updated_count)
}

#[cfg(test)]
mod tests {
    use std::{
        fs::{self, create_dir_all, remove_dir_all},
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    use duckdb::{Connection, params};

    use super::*;

    fn linshi_mulu() -> PathBuf {
        let weiyi = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!("lianghua_gdnm_bx_{weiyi}"))
    }

    fn xieru_stock_list(source_dir: &Path) {
        let neirong = "\
ts_code,symbol,name,area,industry,list_date,trade_date,total_share,float_share,total_mv,circ_mv,fullname,enname,cnspell,market,exchange,curr_type,list_status,delist_date,is_hs,act_name,act_ent_type
000001.SZ,000001,平安银行,深圳,银行,19910403,20240103,0,0,100,80,平安银行,,,,,,,,,,\n\
000002.SZ,000002,万科A,深圳,地产,19910129,20240103,0,0,300,250,万科A,,,,,,,,,,\n\
000003.SZ,000003,测试股,深圳,软件,19910129,20240103,0,0,,50,测试股,,,,,,,,,,\n";
        fs::write(source_dir.join("stock_list.csv"), neirong).expect("write stock_list");
    }

    fn xieru_gdnm_csv(source_dir: &Path) {
        let neirong = "\
ts_code,concepts_code,concepts_name,stock_name
000001.SZ,,算力,平安银行
000002.SZ,,算力,万科A
000003.SZ,,AI,测试股
000001.SZ,,AI/国产替代,平安银行
";
        fs::write(source_dir.join("stock_concepts.csv"), neirong).expect("write concepts");
    }

    fn xieru_source_db(source_dir: &Path) {
        let db_path = source_db_path(source_dir.to_str().expect("utf8 path"));
        let conn = Connection::open(db_path).expect("open source db");
        conn.execute(
            r#"
            CREATE TABLE stock_data (
                ts_code VARCHAR,
                trade_date VARCHAR,
                adj_type VARCHAR,
                pct_chg DOUBLE
            )
            "#,
            [],
        )
        .expect("create stock_data");
        let mut app = conn.appender("stock_data").expect("appender");
        app.append_row(params!["000001.SZ", "20240102", "qfq", 10.0_f64])
            .expect("row1");
        app.append_row(params!["000002.SZ", "20240102", "qfq", 20.0_f64])
            .expect("row2");
        app.append_row(params!["000003.SZ", "20240102", "qfq", 30.0_f64])
            .expect("row3");
        app.append_row(params!["000001.SZ", "20240103", "qfq", -5.0_f64])
            .expect("row4");
        app.append_row(params!["000002.SZ", "20240103", "qfq", 15.0_f64])
            .expect("row5");
        app.flush().expect("flush");
    }

    fn xieru_source_db_many_trade_dates(source_dir: &Path, trade_dates: &[String]) {
        let db_path = source_db_path(source_dir.to_str().expect("utf8 path"));
        let conn = Connection::open(db_path).expect("open source db");
        conn.execute(
            r#"
            CREATE TABLE stock_data (
                ts_code VARCHAR,
                trade_date VARCHAR,
                adj_type VARCHAR,
                pct_chg DOUBLE
            )
            "#,
            [],
        )
        .expect("create stock_data");

        let mut app = conn.appender("stock_data").expect("appender");
        for (index, trade_date) in trade_dates.iter().enumerate() {
            let day_pct = (index + 1) as f64;
            app.append_row(params!["000001.SZ", trade_date, "qfq", day_pct])
                .expect("row stock1");
            app.append_row(params!["000002.SZ", trade_date, "qfq", day_pct * 2.0_f64])
                .expect("row stock2");
        }
        app.flush().expect("flush");
    }

    #[test]
    fn rebuild_concept_performance_range_builds_weighted_rows() {
        let source_dir = linshi_mulu();
        create_dir_all(&source_dir).expect("create dir");
        xieru_stock_list(&source_dir);
        xieru_gdnm_csv(&source_dir);
        xieru_source_db(&source_dir);

        let row_count = rebuild_concept_performance_range(
            source_dir.to_str().expect("utf8 path"),
            "20240102",
            "20240103",
        )
        .expect("rebuild range");
        assert_eq!(row_count, 10);

        let bx_db = concept_performance_db_path(source_dir.to_str().expect("utf8 path"));
        let conn = Connection::open(bx_db).expect("open bx db");
        let mut stmt = conn
            .prepare(
                "SELECT trade_date, performance_type, concept, performance_pct FROM concept_performance ORDER BY trade_date, performance_type, concept",
            )
            .expect("prepare");
        let mut rows = stmt.query([]).expect("query");
        let mut shiji = Vec::new();
        while let Some(row) = rows.next().expect("next") {
            shiji.push(GdNmBXRow {
                trade_date: row.get(0).expect("trade_date"),
                performance_type: row.get(1).expect("performance_type"),
                concept: row.get(2).expect("concept"),
                performance_pct: row.get(3).expect("performance_pct"),
            });
        }

        assert_eq!(
            shiji,
            vec![
                GdNmBXRow {
                    trade_date: "20240102".to_string(),
                    performance_type: PERFORMANCE_TYPE_CONCEPT.to_string(),
                    concept: "AI".to_string(),
                    performance_pct: 10.0,
                },
                GdNmBXRow {
                    trade_date: "20240102".to_string(),
                    performance_type: PERFORMANCE_TYPE_CONCEPT.to_string(),
                    concept: "国产替代".to_string(),
                    performance_pct: 10.0,
                },
                GdNmBXRow {
                    trade_date: "20240102".to_string(),
                    performance_type: PERFORMANCE_TYPE_CONCEPT.to_string(),
                    concept: "算力".to_string(),
                    performance_pct: 17.5,
                },
                GdNmBXRow {
                    trade_date: "20240102".to_string(),
                    performance_type: PERFORMANCE_TYPE_INDUSTRY.to_string(),
                    concept: "地产".to_string(),
                    performance_pct: 20.0,
                },
                GdNmBXRow {
                    trade_date: "20240102".to_string(),
                    performance_type: PERFORMANCE_TYPE_INDUSTRY.to_string(),
                    concept: "银行".to_string(),
                    performance_pct: 10.0,
                },
                GdNmBXRow {
                    trade_date: "20240103".to_string(),
                    performance_type: PERFORMANCE_TYPE_CONCEPT.to_string(),
                    concept: "AI".to_string(),
                    performance_pct: -5.0,
                },
                GdNmBXRow {
                    trade_date: "20240103".to_string(),
                    performance_type: PERFORMANCE_TYPE_CONCEPT.to_string(),
                    concept: "国产替代".to_string(),
                    performance_pct: -5.0,
                },
                GdNmBXRow {
                    trade_date: "20240103".to_string(),
                    performance_type: PERFORMANCE_TYPE_CONCEPT.to_string(),
                    concept: "算力".to_string(),
                    performance_pct: 10.0,
                },
                GdNmBXRow {
                    trade_date: "20240103".to_string(),
                    performance_type: PERFORMANCE_TYPE_INDUSTRY.to_string(),
                    concept: "地产".to_string(),
                    performance_pct: 15.0,
                },
                GdNmBXRow {
                    trade_date: "20240103".to_string(),
                    performance_type: PERFORMANCE_TYPE_INDUSTRY.to_string(),
                    concept: "银行".to_string(),
                    performance_pct: -5.0,
                },
            ]
        );

        let _ = remove_dir_all(source_dir);
    }

    #[test]
    fn rebuild_concept_performance_range_handles_multi_date_chunks() {
        let source_dir = linshi_mulu();
        create_dir_all(&source_dir).expect("create dir");
        xieru_stock_list(&source_dir);
        xieru_gdnm_csv(&source_dir);

        let trade_dates: Vec<String> = (1..=40).map(|day| format!("202402{:02}", day)).collect();
        xieru_source_db_many_trade_dates(&source_dir, &trade_dates);

        let row_count = rebuild_concept_performance_range(
            source_dir.to_str().expect("utf8 path"),
            trade_dates.first().expect("first trade date"),
            trade_dates.last().expect("last trade date"),
        )
        .expect("rebuild range");
        assert_eq!(row_count, 200);

        let bx_db = concept_performance_db_path(source_dir.to_str().expect("utf8 path"));
        let conn = Connection::open(bx_db).expect("open bx db");

        let first_ai: f64 = conn
            .query_row(
                "SELECT performance_pct FROM concept_performance WHERE trade_date = ? AND performance_type = ? AND concept = ?",
                params![
                    trade_dates.first().expect("first trade date"),
                    PERFORMANCE_TYPE_CONCEPT,
                    "AI"
                ],
                |row| row.get(0),
            )
            .expect("query first ai");
        let first_suanli: f64 = conn
            .query_row(
                "SELECT performance_pct FROM concept_performance WHERE trade_date = ? AND performance_type = ? AND concept = ?",
                params![
                    trade_dates.first().expect("first trade date"),
                    PERFORMANCE_TYPE_CONCEPT,
                    "算力"
                ],
                |row| row.get(0),
            )
            .expect("query first suanli");
        let first_bank: f64 = conn
            .query_row(
                "SELECT performance_pct FROM concept_performance WHERE trade_date = ? AND performance_type = ? AND concept = ?",
                params![
                    trade_dates.first().expect("first trade date"),
                    PERFORMANCE_TYPE_INDUSTRY,
                    "银行"
                ],
                |row| row.get(0),
            )
            .expect("query first bank");
        let last_ai: f64 = conn
            .query_row(
                "SELECT performance_pct FROM concept_performance WHERE trade_date = ? AND performance_type = ? AND concept = ?",
                params![
                    trade_dates.last().expect("last trade date"),
                    PERFORMANCE_TYPE_CONCEPT,
                    "AI"
                ],
                |row| row.get(0),
            )
            .expect("query last ai");
        let last_suanli: f64 = conn
            .query_row(
                "SELECT performance_pct FROM concept_performance WHERE trade_date = ? AND performance_type = ? AND concept = ?",
                params![
                    trade_dates.last().expect("last trade date"),
                    PERFORMANCE_TYPE_CONCEPT,
                    "算力"
                ],
                |row| row.get(0),
            )
            .expect("query last suanli");
        let last_bank: f64 = conn
            .query_row(
                "SELECT performance_pct FROM concept_performance WHERE trade_date = ? AND performance_type = ? AND concept = ?",
                params![
                    trade_dates.last().expect("last trade date"),
                    PERFORMANCE_TYPE_INDUSTRY,
                    "银行"
                ],
                |row| row.get(0),
            )
            .expect("query last bank");

        assert_eq!(first_ai, 1.0);
        assert_eq!(first_suanli, 1.75);
        assert_eq!(first_bank, 1.0);
        assert_eq!(last_ai, 40.0);
        assert_eq!(last_suanli, 70.0);
        assert_eq!(last_bank, 40.0);

        let _ = remove_dir_all(source_dir);
    }

    #[test]
    fn rebuild_concept_performance_range_rounds_to_three_decimals() {
        let source_dir = linshi_mulu();
        create_dir_all(&source_dir).expect("create dir");
        xieru_stock_list(&source_dir);
        xieru_gdnm_csv(&source_dir);

        let db_path = source_db_path(source_dir.to_str().expect("utf8 path"));
        let conn = Connection::open(db_path).expect("open source db");
        conn.execute(
            r#"
            CREATE TABLE stock_data (
                ts_code VARCHAR,
                trade_date VARCHAR,
                adj_type VARCHAR,
                pct_chg DOUBLE
            )
            "#,
            [],
        )
        .expect("create stock_data");
        let mut app = conn.appender("stock_data").expect("appender");
        app.append_row(params!["000001.SZ", "20240102", "qfq", 1.0_f64])
            .expect("row1");
        app.append_row(params!["000002.SZ", "20240102", "qfq", 1.111111_f64])
            .expect("row2");
        app.flush().expect("flush");

        rebuild_concept_performance_range(
            source_dir.to_str().expect("utf8 path"),
            "20240102",
            "20240102",
        )
        .expect("rebuild range");

        let bx_db = concept_performance_db_path(source_dir.to_str().expect("utf8 path"));
        let conn = Connection::open(bx_db).expect("open bx db");
        let performance_pct: f64 = conn
            .query_row(
                "SELECT performance_pct FROM concept_performance WHERE trade_date = ? AND performance_type = ? AND concept = ?",
                params!["20240102", PERFORMANCE_TYPE_CONCEPT, "算力"],
                |row| row.get(0),
            )
            .expect("query rounded performance");

        assert_eq!(performance_pct, 1.083);

        let _ = remove_dir_all(source_dir);
    }
}
