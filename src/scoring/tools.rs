use std::collections::HashMap;
use std::collections::HashSet;

use crate::data::{ScopeWay, ScoreRule};
use crate::data::{load_stock_list, load_trade_date_list};
use crate::expr::eval::{Runtime, Value};
use crate::expr::parser::{Expr, Parser, Stmt, lex_all};
use crate::utils::utils::eval_binary_for_warmup;
use crate::utils::utils::impl_expr_warmup;

pub fn load_st_list(source_dir: &str) -> Result<HashSet<String>, String> {
    let rows = load_stock_list(source_dir)?;
    let mut st_list = HashSet::new();
    for cols in rows {
        let ts_code = cols
            .first()
            .ok_or_else(|| "stock_list.csv格式错误: 缺少ts_code列".to_string())?;
        let name = cols
            .get(2)
            .ok_or_else(|| "stock_list.csv格式错误: 缺少name列".to_string())?;

        if name.to_ascii_uppercase().contains("ST") {
            st_list.insert(ts_code.trim().to_string());
        }
    }

    Ok(st_list)
}

pub fn warmup_rows_estimate(source_dir: &str) -> Result<usize, String> {
    // 从拿rule原数据开始计算warmup
    let rules = ScoreRule::load_rules(source_dir)?;
    let mut all_expr_max_need = 0;

    for rule in rules {
        let tok = lex_all(&rule.when); // 变成带序号字符
        let mut p = Parser::new(tok); // 变成基础语句
        let stmts = p
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

        let extra_need = match rule.scope_way {
            ScopeWay::Last => 0,
            ScopeWay::Any => rule.scope_windows - 1,
            ScopeWay::Consec(_) => rule.scope_windows - 1,
            ScopeWay::Each => rule.scope_windows - 1,
            ScopeWay::Recent => rule.scope_windows - 1,
        };

        if extra_need + all_expr_need > all_expr_max_need {
            all_expr_max_need = extra_need + all_expr_need;
        }
    }

    Ok(all_expr_max_need)
}

pub fn calc_query_start_date(
    source_dir: &str,
    warmup_need: usize,
    ori_start_date: &str,
) -> Result<String, String> {
    let trade_dates = load_trade_date_list(source_dir)?;
    let anchor_idx = match trade_dates.binary_search_by(|d| d.as_str().cmp(ori_start_date)) {
        Ok(i) => i,
        Err(i) => i,
    };

    if anchor_idx >= trade_dates.len() {
        return Err(format!("起始日期{ori_start_date}晚于交易日历最后一天"));
    }

    let start_idx = anchor_idx.saturating_sub(warmup_need);
    Ok(trade_dates[start_idx].clone())
}

pub fn calc_query_need_rows(
    source_dir: &str,
    warmup_need: usize,
    start_date: &str,
    end_date: &str,
) -> Result<usize, String> {
    let trade_dates = load_trade_date_list(source_dir)?;
    let start_idx = match trade_dates.binary_search_by(|d| d.as_str().cmp(start_date)) {
        Ok(i) => i,
        Err(i) => i,
    };

    if start_idx >= trade_dates.len() {
        return Err(format!("起始日期{start_date}晚于交易日历最后一天"));
    }

    let end_exclusive = match trade_dates.binary_search_by(|d| d.as_str().cmp(end_date)) {
        Ok(i) => i + 1,
        Err(i) => i,
    };

    let range_need = end_exclusive.saturating_sub(start_idx);
    Ok((warmup_need + range_need).max(1))
}

pub fn rt_max_len(rt: &Runtime) -> usize {
    let mut max_len = 1;
    for v in rt.vars.values() {
        let len = match v {
            Value::Num(_) | Value::Bool(_) => 1,
            Value::NumSeries(ns) => ns.len(),
            Value::BoolSeries(bs) => bs.len(),
        };
        if len > max_len {
            max_len = len;
        }
    }
    max_len
}

pub fn calc_zhang_pct(ts_code: &str, is_st: bool) -> f64 {
    let ts = ts_code.trim().to_ascii_uppercase();
    let (core, suffix) = ts.split_once('.').unwrap_or((ts.as_str(), ""));

    if is_st {
        0.045
    } else if suffix == "BJ" {
        0.295
    } else if core.starts_with("30") || core.starts_with("68") {
        0.195
    } else {
        0.095
    }
}
