use std::collections::HashMap;
use std::collections::HashSet;

use crate::expr::eval::{Runtime, Value};
use crate::expr::parser::{BinaryOp, Expr, Parser, Stmt, lex_all};
use crate::scoring::CachedRule;
use crate::strategy::loader::{ScopeWay, ScoreRule};
use crate::utils::utils::{load_stock_list, load_trade_date_list};

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

fn eval_binary_for_warmup(
    op: &BinaryOp,
    lhs: &Expr,
    rhs: &Expr,
    consts: &HashMap<String, usize>,
) -> Result<Option<f64>, String> {
    let mut out = 0.0;
    let const_pair = match (&*lhs, &*rhs) {
        (Expr::Number(l), Expr::Number(r)) => Some((*l, *r)),
        (Expr::Ident(l), Expr::Number(r)) => consts.get(l).copied().map(|lv| (lv as f64, *r)),
        (Expr::Number(l), Expr::Ident(r)) => consts.get(r).copied().map(|rv| (*l, rv as f64)),
        (Expr::Ident(l), Expr::Ident(r)) => {
            match (consts.get(l).copied(), consts.get(r).copied()) {
                (Some(lv), Some(rv)) => Some((lv as f64, rv as f64)),
                _ => None,
            }
        }
        _ => None,
    };

    if const_pair.is_none() {
        return Ok(None);
    }

    if let Some((v_lhs, v_rhs)) = const_pair {
        out = match op {
            BinaryOp::Add => v_lhs + v_rhs,
            BinaryOp::Sub => v_lhs - v_rhs,
            BinaryOp::Mul => v_lhs * v_rhs,
            BinaryOp::Div => {
                if v_rhs.abs() < f64::EPSILON {
                    return Err("表达式常量赋值不支持除以0".to_string());
                }
                v_lhs / v_rhs
            }
            _ => {
                return Err("表达式常量赋值只支持加减乘除".to_string());
            }
        };

        if out < 0.0 {
            return Err("表达式常量赋值结果不能为负数".to_string());
        }
    }
    Ok(Some(out))
}

fn impl_expr_warmup(
    expr: Expr,
    locals: &HashMap<String, usize>,
    consts: &HashMap<String, usize>,
) -> Result<usize, String> {
    let mut max_need = 0;
    match expr {
        Expr::Binary { op: _, lhs, rhs } => {
            let l_need = impl_expr_warmup(*lhs, locals, consts)?;
            let r_need = impl_expr_warmup(*rhs, locals, consts)?;
            let out = l_need.max(r_need);
            max_need = out;
        }
        Expr::Unary { op: _, rhs } => {
            let r_need = impl_expr_warmup(*rhs, locals, consts)?;
            max_need = r_need;
        }
        Expr::Call { name, args } => {
            let name = name.to_ascii_uppercase();
            match name.as_str() {
                "REF" => {
                    let mut it = args.into_iter();
                    let src = it
                        .next()
                        .ok_or_else(|| format!("{name}缺少第1个参数: src"))?;
                    let win = it
                        .next()
                        .ok_or_else(|| format!("{name}缺少第2个参数: win"))?;

                    let src_need = impl_expr_warmup(src, locals, consts)?;
                    let win_need = match win {
                        Expr::Number(v) => v as usize,
                        Expr::Ident(name) => {
                            let mut ident_need = 0;
                            if let Some(v) = consts.get(&name) {
                                ident_need = *v
                            }
                            ident_need
                        }
                        Expr::Binary { op, lhs, rhs } => {
                            match eval_binary_for_warmup(&op, &*lhs, &*rhs, consts)? {
                                Some(v) => v as usize,
                                None => return Err("REF参数warmup解析错误".to_string()),
                            }
                        }
                        _ => return Err("REF参数warmup解析错误".to_string()),
                    };

                    max_need = src_need + win_need;
                }
                "HHV" | "LLV" | "MA" | "SUM" | "STD" | "COUNT" | "LRANK" | "GRANK" => {
                    let mut it = args.into_iter();
                    let src = it
                        .next()
                        .ok_or_else(|| format!("{name}缺少第1个参数: src"))?;
                    let win = it
                        .next()
                        .ok_or_else(|| format!("{name}缺少第2个参数: win"))?;

                    let src_need = impl_expr_warmup(src, locals, consts)?;
                    let win_need = match win {
                        Expr::Number(v) => v as usize,
                        Expr::Ident(name) => {
                            let mut ident_need = 0;
                            if let Some(v) = consts.get(&name) {
                                ident_need = *v
                            }
                            ident_need
                        }
                        Expr::Binary { op, lhs, rhs } => {
                            match eval_binary_for_warmup(&op, &*lhs, &*rhs, consts)? {
                                Some(v) => v as usize,
                                None => return Err(format!("{name}参数warmup解析错误")),
                            }
                        }
                        _ => return Err(format!("{name}参数warmup解析错误")),
                    };

                    max_need = (src_need + win_need).saturating_sub(1);
                }
                "CROSS" => {
                    let mut it = args.into_iter();
                    let left = it
                        .next()
                        .ok_or_else(|| format!("{name}缺少第1个参数: left"))?;
                    let right = it
                        .next()
                        .ok_or_else(|| format!("{name}缺少第2个参数: right"))?;

                    let l_need = impl_expr_warmup(left, locals, consts)?;
                    let r_need = impl_expr_warmup(right, locals, consts)?;

                    max_need = l_need.max(r_need) + 1;
                }
                "GET" => {
                    let mut it = args.into_iter();
                    let cond = it
                        .next()
                        .ok_or_else(|| format!("{name}缺少第1个参数: cond"))?;
                    let value = it
                        .next()
                        .ok_or_else(|| format!("{name}缺少第2个参数: value"))?;
                    let win = it
                        .next()
                        .ok_or_else(|| format!("{name}缺少第3个参数: win"))?;

                    let cond_need = impl_expr_warmup(cond, locals, consts)?;
                    let value_need = impl_expr_warmup(value, locals, consts)?;
                    let win_need = match win {
                        Expr::Number(v) => v as usize,
                        Expr::Ident(name) => {
                            let mut ident_need = 0;
                            if let Some(v) = consts.get(&name) {
                                ident_need = *v
                            }
                            ident_need
                        }
                        Expr::Binary { op, lhs, rhs } => {
                            match eval_binary_for_warmup(&op, &*lhs, &*rhs, consts)? {
                                Some(v) => v as usize,
                                None => return Err("GET参数warmup解析错误".to_string()),
                            }
                        }
                        _ => return Err("GET参数warmup解析错误".to_string()),
                    };
                    max_need = cond_need.max(value_need) + win_need;
                }
                "ABS" => {
                    let mut it = args.into_iter();
                    let src = it
                        .next()
                        .ok_or_else(|| format!("{name}缺少第1个参数: src"))?;
                    max_need = impl_expr_warmup(src, locals, consts)?;
                }

                "MAX" | "MIN" => {
                    let mut it = args.into_iter();
                    let left = it
                        .next()
                        .ok_or_else(|| format!("{name}缺少第1个参数: left"))?;
                    let right = it
                        .next()
                        .ok_or_else(|| format!("{name}缺少第2个参数: right"))?;

                    let l_need = impl_expr_warmup(left, locals, consts)?;
                    let r_need = impl_expr_warmup(right, locals, consts)?;

                    max_need = l_need.max(r_need);
                }

                "IF" => {
                    let mut it = args.into_iter();
                    let cond = it
                        .next()
                        .ok_or_else(|| format!("{name}缺少第1个参数: cond"))?;
                    let left = it
                        .next()
                        .ok_or_else(|| format!("{name}缺少第2个参数: left"))?;
                    let right = it
                        .next()
                        .ok_or_else(|| format!("{name}缺少第3个参数: right"))?;

                    let c_need = impl_expr_warmup(cond, locals, consts)?;
                    let l_need = impl_expr_warmup(left, locals, consts)?;
                    let r_need = impl_expr_warmup(right, locals, consts)?;

                    max_need = c_need.max(l_need).max(r_need);
                }
                _ => {}
            }
        }
        Expr::Number(_) => {}
        Expr::Ident(name) => {
            if let Some(need) = locals.get(&name) {
                max_need = *need
            }
        }
    }
    Ok(max_need)
}

pub fn warmup_rows_estimate() -> Result<usize, String> {
    let rules = ScoreRule::load_rules()?;
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

pub fn cache_rule_build() -> Result<Vec<CachedRule>, String> {
    let rules = ScoreRule::load_rules()?;
    let mut out = Vec::with_capacity(128);
    for rule in rules {
        let tok = lex_all(&rule.when);
        let mut parser = Parser::new(tok);
        let stmt = parser
            .parse_main()
            .map_err(|e| format!("表达式解析错误在{}:{}", e.idx, e.msg))?;
        out.push(CachedRule {
            name: rule.name,
            scope_windows: rule.scope_windows,
            scope_way: rule.scope_way,
            points: rule.points,
            dist_points: rule.dist_points,
            tag: rule.tag,
            when_src: rule.when,
            when_ast: stmt,
        });
    }
    Ok(out)
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
