use std::{
    collections::HashMap,
    fs,
    path::Path,
    sync::{Mutex, OnceLock},
    time::SystemTime,
};

use duckdb::params_from_iter;

use crate::{
    data::scoring_data::row_into_rt,
    data::{DataReader, IndsData, RowData, ind_toml_path},
    download::ProBarRow,
    expr::eval::Value,
    expr::parser::{Expr, Parser, Stmt, Stmts, lex_all},
    utils::utils::{eval_binary_for_warmup, impl_expr_warmup},
};

const IND_INPUT_KEYS: [&str; 10] = [
    "O",
    "H",
    "L",
    "C",
    "V",
    "AMOUNT",
    "PRE_CLOSE",
    "CHANGE",
    "PCT_CHG",
    "TURNOVER_RATE",
];

#[derive(Clone)]
pub struct IndsCache {
    pub name: String,
    pub expr: Stmts,
    pub perc: usize,
}

#[derive(Clone, PartialEq, Eq)]
struct IndicatorFileStamp {
    modified: Option<SystemTime>,
    len: u64,
}

#[derive(Clone)]
struct IndicatorCacheEntry {
    stamp: IndicatorFileStamp,
    caches: Vec<IndsCache>,
}

static INDICATOR_CACHE: OnceLock<Mutex<HashMap<String, IndicatorCacheEntry>>> = OnceLock::new();

fn round_to(value: f64, scale: usize) -> f64 {
    let factor = 10_f64.powi(scale as i32);
    (value * factor).round() / factor
}

fn round_series(series: Vec<Option<f64>>, scale: usize) -> Vec<Option<f64>> {
    series
        .into_iter()
        .map(|value| value.map(|number| round_to(number, scale)))
        .collect()
}

fn indicator_cache_store() -> &'static Mutex<HashMap<String, IndicatorCacheEntry>> {
    INDICATOR_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn build_indicator_file_stamp(path: &Path) -> Result<IndicatorFileStamp, String> {
    let metadata = fs::metadata(path)
        .map_err(|e| format!("读取指标文件元数据失败: path={}, err={e}", path.display()))?;
    let modified = metadata.modified().ok();

    Ok(IndicatorFileStamp {
        modified,
        len: metadata.len(),
    })
}

fn compile_indicator_defs(inds: Vec<crate::data::IndData>) -> Result<Vec<IndsCache>, String> {
    let mut out = Vec::with_capacity(128);
    for ind in inds {
        let tok = lex_all(&ind.expr);
        let mut parser = Parser::new(tok);
        let stmt = parser
            .parse_main()
            .map_err(|e| format!("表达式解析错误在{}:{}", e.idx, e.msg))?;
        out.push(IndsCache {
            name: ind.name,
            expr: stmt,
            perc: ind.prec,
        });
    }
    Ok(out)
}

fn load_optional_inds(source_dir: &str) -> Result<Option<Vec<crate::data::IndData>>, String> {
    let ind_path = ind_toml_path(source_dir);
    if !ind_path.exists() {
        return Ok(None);
    }

    let raw = fs::read_to_string(&ind_path).map_err(|e| {
        format!(
            "指标文件不存在或不可读: path={}, err={e}",
            ind_path.display()
        )
    })?;
    if raw.trim().is_empty() {
        return Ok(None);
    }

    IndsData::load_inds(source_dir).map(Some)
}

pub fn cache_ind_build(source_dir: &str) -> Result<Vec<IndsCache>, String> {
    // 包含读取ind文件,编译缓存
    let Some(inds) = load_optional_inds(source_dir)? else {
        return Ok(Vec::new());
    };
    compile_indicator_defs(inds)
}

pub fn cache_ind_build_from_path(indicator_path: &Path) -> Result<Vec<IndsCache>, String> {
    let stamp = build_indicator_file_stamp(indicator_path)?;
    let cache_key = indicator_path.to_string_lossy().to_string();
    let cache_store = indicator_cache_store();

    {
        let cache_map = cache_store
            .lock()
            .map_err(|_| "指标缓存锁已中毒".to_string())?;
        if let Some(entry) = cache_map.get(&cache_key) {
            if entry.stamp == stamp {
                return Ok(entry.caches.clone());
            }
        }
    }

    let text = fs::read_to_string(indicator_path).map_err(|e| {
        format!(
            "读取指标文件失败: path={}, err={e}",
            indicator_path.display()
        )
    })?;
    if text.trim().is_empty() {
        return Ok(Vec::new());
    }

    let inds = IndsData::parse_from_text(&text)?;
    let caches = compile_indicator_defs(inds)?;

    let mut cache_map = cache_store
        .lock()
        .map_err(|_| "指标缓存锁已中毒".to_string())?;
    cache_map.insert(
        cache_key,
        IndicatorCacheEntry {
            stamp,
            caches: caches.clone(),
        },
    );

    Ok(caches)
}

pub fn warmup_ind_estimate(source_dir: &str) -> Result<usize, String> {
    let Some(inds) = load_optional_inds(source_dir)? else {
        return Ok(0);
    };
    let mut all_ind_max_need = 0;

    for ind in inds {
        let tok = lex_all(&ind.expr);
        let mut parser = Parser::new(tok);
        let stmts = parser
            .parse_main()
            .map_err(|e| format!("表达式解析错误在{}:{}", e.idx, e.msg))?;
        let mut locals = HashMap::new();
        let mut consts: HashMap<String, usize> = HashMap::new();
        let mut all_expr_need = 0;
        // println!("{:#?}", stmt);

        for stmt in stmts.item {
            match stmt {
                Stmt::Assign { name, value } => match value {
                    Expr::Number(v) => {
                        consts.insert(name, v as usize);
                    }
                    Expr::Binary { op, lhs, rhs } => {
                        if let Some(out) = eval_binary_for_warmup(&op, &lhs, &rhs, &consts)? {
                            consts.insert(name, out as usize);
                        } else {
                            let value_need =
                                impl_expr_warmup(Expr::Binary { op, lhs, rhs }, &locals, &consts)?;
                            locals.insert(name, value_need);
                        }
                    }
                    _ => {
                        let value_need = impl_expr_warmup(value, &locals, &consts)?;
                        locals.insert(name, value_need);
                    }
                },
                Stmt::Expr(v) => {
                    let expr_need = impl_expr_warmup(v, &locals, &consts)?;
                    if expr_need > all_expr_need {
                        all_expr_need = expr_need
                    }
                }
            }
        }

        if all_expr_need > all_ind_max_need {
            all_ind_max_need = all_expr_need;
        }
    }

    Ok(all_ind_max_need)
}

fn load_one_tail_rows_with_warmup_need(
    dr: &DataReader,
    ts_code: &str,
    adj_type: &str,
    end_date: &str,
    warmup_need: usize,
) -> Result<Option<RowData>, String> {
    if warmup_need == 0 {
        return Ok(None);
    }

    match dr.load_one_tail_rows(ts_code, adj_type, end_date, warmup_need) {
        Ok(row_data) => Ok(Some(normalize_row_data_for_indicators(row_data)?)),
        Err(err) if err.contains("trade_dates为空") => Ok(None),
        Err(err) => Err(err),
    }
}

pub fn load_many_tail_rows_with_warmup_need(
    dr: &DataReader,
    adj_type: &str,
    end_dates: &HashMap<String, String>,
    warmup_need: usize,
) -> Result<HashMap<String, RowData>, String> {
    if warmup_need == 0 || end_dates.is_empty() {
        return Ok(HashMap::new());
    }

    let mut requests = end_dates
        .iter()
        .filter_map(|(ts_code, end_date)| {
            let end_date = end_date.trim();
            if end_date.is_empty() {
                None
            } else {
                Some((ts_code.clone(), end_date.to_string()))
            }
        })
        .collect::<Vec<_>>();
    requests.sort_by(|a, b| a.0.cmp(&b.0));
    if requests.is_empty() {
        return Ok(HashMap::new());
    }

    let runtime_to_db = dr
        .cols_table
        .iter()
        .map(|(db_col, key)| (key.as_str(), db_col.as_str()))
        .collect::<HashMap<_, _>>();
    let input_selects = IND_INPUT_KEYS
        .iter()
        .map(|key| match runtime_to_db.get(key) {
            Some(db_col) => {
                format!("TRY_CAST(stock_data.\"{db_col}\" AS DOUBLE) AS \"{key}\"")
            }
            None => format!("CAST(NULL AS DOUBLE) AS \"{key}\""),
        })
        .collect::<Vec<_>>();
    let output_selects = IND_INPUT_KEYS
        .iter()
        .map(|key| format!("ranked.\"{key}\""))
        .collect::<Vec<_>>();
    let values_sql = std::iter::repeat_n("(?, ?)", requests.len())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        r#"
        WITH requested(ts_code, end_date) AS (
            VALUES {values_sql}
        ),
        ranked AS (
            SELECT
                stock_data.ts_code,
                stock_data.trade_date,
                {},
                ROW_NUMBER() OVER (
                    PARTITION BY stock_data.ts_code
                    ORDER BY stock_data.trade_date DESC
                ) AS row_num
            FROM stock_data
            INNER JOIN requested
                ON requested.ts_code = stock_data.ts_code
            WHERE stock_data.adj_type = ?
              AND stock_data.trade_date <= requested.end_date
        )
        SELECT
            ranked.ts_code,
            ranked.trade_date,
            {}
        FROM ranked
        WHERE row_num <= {warmup_need}
        ORDER BY ranked.ts_code ASC, ranked.trade_date ASC
        "#,
        input_selects.join(",\n                "),
        output_selects.join(",\n            "),
    );

    let mut params = Vec::with_capacity(requests.len() * 2 + 1);
    for (ts_code, end_date) in &requests {
        params.push(ts_code.clone());
        params.push(end_date.clone());
    }
    params.push(adj_type.to_string());

    let mut stmt = dr
        .conn
        .prepare(&sql)
        .map_err(|e| format!("预编译批量warmup历史查询失败: {e}"))?;
    let mut rows = stmt
        .query(params_from_iter(params.iter()))
        .map_err(|e| format!("执行批量warmup历史查询失败: {e}"))?;

    let mut trade_dates_by_stock = HashMap::<String, Vec<String>>::new();
    let mut cols_by_stock = HashMap::<String, HashMap<String, Vec<Option<f64>>>>::new();

    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取批量warmup历史查询结果失败: {e}"))?
    {
        let ts_code: String = row.get(0).map_err(|e| format!("读取ts_code失败: {e}"))?;
        let trade_date: String = row.get(1).map_err(|e| format!("读取trade_date失败: {e}"))?;
        trade_dates_by_stock
            .entry(ts_code.clone())
            .or_default()
            .push(trade_date);

        let cols = cols_by_stock.entry(ts_code).or_insert_with(|| {
            IND_INPUT_KEYS
                .iter()
                .map(|key| (key.to_string(), Vec::new()))
                .collect::<HashMap<_, _>>()
        });
        for (idx, key) in IND_INPUT_KEYS.iter().enumerate() {
            let value: Option<f64> = row
                .get(idx + 2)
                .map_err(|e| format!("读取{key}失败: {e}"))?;
            cols.get_mut(*key)
                .expect("indicator input key should exist")
                .push(value);
        }
    }

    let mut out = HashMap::with_capacity(trade_dates_by_stock.len());
    for (ts_code, trade_dates) in trade_dates_by_stock {
        let cols = cols_by_stock
            .remove(&ts_code)
            .ok_or_else(|| format!("批量warmup缺少列数据: {ts_code}"))?;
        let row_data = RowData { trade_dates, cols };
        row_data.validate()?;
        out.insert(ts_code, row_data);
    }

    Ok(out)
}

fn pro_bar_rows_to_row_data(rows: &[ProBarRow]) -> Result<RowData, String> {
    let mut trade_dates = Vec::with_capacity(rows.len());
    let mut cols: HashMap<String, Vec<Option<f64>>> = HashMap::new();

    for key in IND_INPUT_KEYS {
        cols.insert(key.to_string(), Vec::with_capacity(rows.len()));
    }

    for row in rows {
        trade_dates.push(row.trade_date.clone());

        cols.get_mut("O")
            .expect("O should exist")
            .push(Some(row.open));
        cols.get_mut("H")
            .expect("H should exist")
            .push(Some(row.high));
        cols.get_mut("L")
            .expect("L should exist")
            .push(Some(row.low));
        cols.get_mut("C")
            .expect("C should exist")
            .push(Some(row.close));
        cols.get_mut("V")
            .expect("V should exist")
            .push(Some(row.vol));
        cols.get_mut("AMOUNT")
            .expect("AMOUNT should exist")
            .push(Some(row.amount));
        cols.get_mut("PRE_CLOSE")
            .expect("PRE_CLOSE should exist")
            .push(Some(row.pre_close));
        cols.get_mut("CHANGE")
            .expect("CHANGE should exist")
            .push(Some(row.change));
        cols.get_mut("PCT_CHG")
            .expect("PCT_CHG should exist")
            .push(Some(row.pct_chg));
        cols.get_mut("TURNOVER_RATE")
            .expect("TURNOVER_RATE should exist")
            .push(row.turnover_rate);
    }

    let row_data = RowData { trade_dates, cols };
    row_data.validate()?;
    Ok(row_data)
}

fn normalize_row_data_for_indicators(row_data: RowData) -> Result<RowData, String> {
    let len = row_data.trade_dates.len();
    let mut cols = HashMap::with_capacity(IND_INPUT_KEYS.len());

    for key in IND_INPUT_KEYS {
        let series = row_data
            .cols
            .get(key)
            .cloned()
            .unwrap_or_else(|| vec![None; len]);
        cols.insert(key.to_string(), series);
    }

    let out = RowData {
        trade_dates: row_data.trade_dates,
        cols,
    };
    out.validate()?;
    Ok(out)
}

fn merge_history_with_rows(
    history: Option<RowData>,
    rows: &[ProBarRow],
) -> Result<RowData, String> {
    let current = pro_bar_rows_to_row_data(rows)?;

    let Some(mut history) = history else {
        return Ok(current);
    };

    for key in IND_INPUT_KEYS {
        let src = current
            .cols
            .get(key)
            .ok_or_else(|| format!("缺少指标输入列:{key}"))?;
        let dst = history
            .cols
            .get_mut(key)
            .ok_or_else(|| format!("历史数据缺少指标输入列:{key}"))?;
        dst.extend_from_slice(src);
    }
    history.trade_dates.extend(current.trade_dates);
    history.validate()?;
    Ok(history)
}

pub fn calc_increment_inds_from_history(
    inds_cache: &[IndsCache],
    history: Option<RowData>,
    new_rows: &[ProBarRow],
) -> Result<HashMap<String, Vec<Option<f64>>>, String> {
    if new_rows.is_empty() {
        return Err("增量指标计算失败: new_rows为空".to_string());
    }

    let combined = merge_history_with_rows(history, new_rows)?;
    let indicators = calc_inds_with_cache(inds_cache, combined)?;
    let keep_len = new_rows.len();
    let mut out = HashMap::with_capacity(indicators.len());

    for (name, series) in indicators {
        if series.len() < keep_len {
            return Err(format!(
                "指标{}长度不足以切出增量结果: {} < {}",
                name,
                series.len(),
                keep_len
            ));
        }
        out.insert(name, series[series.len() - keep_len..].to_vec());
    }

    Ok(out)
}

pub fn calc_inds_with_cache(
    inds_cache: &[IndsCache],
    row_data: RowData,
) -> Result<HashMap<String, Vec<Option<f64>>>, String> {
    let series_len = row_data.trade_dates.len();
    let mut rt = row_into_rt(row_data)?;
    let mut out = HashMap::with_capacity(inds_cache.len());

    for ind in inds_cache {
        let value = rt
            .eval_program(&ind.expr)
            .map_err(|e| format!("指标{}计算失败: {}", ind.name, e.msg))?;
        let series = Value::as_num_series(&value, series_len)
            .map_err(|e| format!("指标{}结果转序列失败: {}", ind.name, e.msg))?;
        let rounded_series = round_series(series, ind.perc);

        rt.vars
            .insert(ind.name.clone(), Value::NumSeries(rounded_series.clone()));
        out.insert(ind.name.clone(), rounded_series);
    }

    Ok(out)
}

pub fn calc_inds_with_cache_lossy(
    inds_cache: &[IndsCache],
    row_data: RowData,
) -> HashMap<String, Vec<Option<f64>>> {
    let series_len = row_data.trade_dates.len();
    let Ok(mut rt) = row_into_rt(row_data) else {
        return HashMap::new();
    };
    let mut out = HashMap::with_capacity(inds_cache.len());

    for ind in inds_cache {
        let Ok(value) = rt.eval_program(&ind.expr) else {
            continue;
        };
        let Ok(series) = Value::as_num_series(&value, series_len) else {
            continue;
        };
        let rounded_series = round_series(series, ind.perc);

        rt.vars
            .insert(ind.name.clone(), Value::NumSeries(rounded_series.clone()));
        out.insert(ind.name.clone(), rounded_series);
    }

    out
}

pub fn calc_increment_one_stock_inds(
    dr: &DataReader,
    inds_cache: &[IndsCache],
    warmup_need: usize,
    ts_code: &str,
    adj_type: &str,
    history_end_date: Option<&str>,
    new_rows: &[ProBarRow],
) -> Result<HashMap<String, Vec<Option<f64>>>, String> {
    let history = match history_end_date {
        Some(end_date) => {
            load_one_tail_rows_with_warmup_need(dr, ts_code, adj_type, end_date, warmup_need)?
        }
        None => None,
    };
    calc_increment_inds_from_history(inds_cache, history, new_rows)
}

pub fn calc_inds_for_rows_with_cache(
    inds_cache: &[IndsCache],
    rows: &[ProBarRow],
) -> Result<HashMap<String, Vec<Option<f64>>>, String> {
    if rows.is_empty() {
        return Err("指标计算失败: rows为空".to_string());
    }

    calc_inds_with_cache(inds_cache, pro_bar_rows_to_row_data(rows)?)
}

pub fn calc_one_stock_inds(
    source_dir: &str,
    rows: &[ProBarRow],
) -> Result<HashMap<String, Vec<Option<f64>>>, String> {
    if rows.is_empty() {
        return Err("单股指标计算失败: rows为空".to_string());
    }

    let inds_cache = cache_ind_build(source_dir)?;
    if inds_cache.is_empty() {
        return Ok(HashMap::new());
    }
    calc_inds_with_cache(&inds_cache, pro_bar_rows_to_row_data(rows)?)
}
