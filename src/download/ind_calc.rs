use std::{collections::HashMap, fs};

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

pub struct IndsCache {
    pub name: String,
    pub expr: Stmts,
    pub perc: usize,
}

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

fn calc_inds_with_cache(
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

pub fn calc_increment_one_stock_inds(
    dr: &DataReader,
    inds_cache: &[IndsCache],
    warmup_need: usize,
    ts_code: &str,
    adj_type: &str,
    history_end_date: Option<&str>,
    new_rows: &[ProBarRow],
) -> Result<HashMap<String, Vec<Option<f64>>>, String> {
    if new_rows.is_empty() {
        return Err("增量指标计算失败: new_rows为空".to_string());
    }

    let history = match history_end_date {
        Some(end_date) => {
            load_one_tail_rows_with_warmup_need(dr, ts_code, adj_type, end_date, warmup_need)?
        }
        None => None,
    };
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
