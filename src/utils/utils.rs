use std::collections::HashMap;

use crate::expr::parser::{BinaryOp, Expr};

pub fn eval_binary_for_warmup(
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

pub fn impl_expr_warmup(
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
                "GTOPCOUNT" | "LTOPCOUNT" => {
                    let mut it = args.into_iter();
                    let value = it
                        .next()
                        .ok_or_else(|| format!("{name}缺少第1个参数: value"))?;
                    let cond = it
                        .next()
                        .ok_or_else(|| format!("{name}缺少第2个参数: cond"))?;
                    let win = it
                        .next()
                        .ok_or_else(|| format!("{name}缺少第3个参数: win"))?;
                    let _topn = it
                        .next()
                        .ok_or_else(|| format!("{name}缺少第4个参数: topn"))?;

                    let value_need = impl_expr_warmup(value, locals, consts)?;
                    let cond_need = impl_expr_warmup(cond, locals, consts)?;
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

                    max_need = value_need.max(cond_need) + win_need.saturating_sub(1);
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

                "EMA" | "SMA" => {
                    let mut it = args.into_iter();
                    let src = it
                        .next()
                        .ok_or_else(|| format!("{name}缺少第1个参数: src"))?;

                    max_need = impl_expr_warmup(src, locals, consts)?;
                }

                "BARSLAST" => {
                    let mut it = args.into_iter();
                    let cond = it
                        .next()
                        .ok_or_else(|| format!("{name}缺少第1个参数: cond"))?;

                    max_need = impl_expr_warmup(cond, locals, consts)?;
                }

                "RSV" => {
                    let mut it = args.into_iter();
                    let c = it
                        .next()
                        .ok_or_else(|| format!("{name}缺少第1个参数: c"))?;
                    let h = it
                        .next()
                        .ok_or_else(|| format!("{name}缺少第2个参数: h"))?;
                    let l = it
                        .next()
                        .ok_or_else(|| format!("{name}缺少第3个参数: l"))?;
                    let win = it
                        .next()
                        .ok_or_else(|| format!("{name}缺少第4个参数: win"))?;

                    let c_need = impl_expr_warmup(c, locals, consts)?;
                    let h_need = impl_expr_warmup(h, locals, consts)?;
                    let l_need = impl_expr_warmup(l, locals, consts)?;
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

                    max_need = c_need.max(h_need.max(l_need) + win_need.saturating_sub(1));
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

pub fn board_category(ts_code: &str) -> &'static str {
    let ts = ts_code.trim().to_ascii_uppercase();
    if ts.ends_with(".BJ") {
        return "北交所";
    }
    if (ts.ends_with(".SZ") && ts.starts_with("30"))
        || (ts.ends_with(".SH") && ts.starts_with("688"))
    {
        return "创业/科创";
    }
    if ts.ends_with(".SH") || ts.ends_with(".SZ") {
        return "主板";
    }
    "其他"
}
