use std::collections::HashMap;

use crate::{
    data::IndsData,
    expr::parser::{Expr, Parser, Stmt, Stmts, lex_all},
    utils::utils::{eval_binary_for_warmup, impl_expr_warmup},
};

pub struct IndsCache {
    pub name: String,
    pub expr: Stmts,
    pub perc: usize,
}

pub fn cache_ind_build(source_dir: &str) -> Result<Vec<IndsCache>, String> {
    // 包含读取ind文件,编译缓存
    let inds = IndsData::load_inds(source_dir)?;
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
    let inds = IndsData::load_inds(source_dir)?;
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
