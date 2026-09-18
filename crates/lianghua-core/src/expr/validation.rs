//! Shared static checks and metadata calculation for expression programs.
//!
//! Keep parsing error presentation at the caller (where labels such as
//! "买点方程" are known), but keep AST traversal and warmup semantics here so
//! every expression entry point accepts the same functions and assignments.

use std::collections::HashMap;

use super::{
    eval::is_supported_expression_function,
    parser::{Expr, ParseErr, Parser, Stmt, Stmts, lex_all},
};
use crate::utils::utils::{eval_binary_for_warmup, impl_expr_warmup};

/// Parse a complete, possibly multi-statement, expression program.
pub fn parse_expression_program(expression: &str) -> Result<Stmts, ParseErr> {
    Parser::new(lex_all(expression)).parse_main()
}

/// Reject function calls that the evaluator cannot execute.
///
/// The parser deliberately accepts arbitrary call names, so this check must be
/// part of every compile/validation path rather than being deferred to runtime.
pub fn validate_expression_functions(stmts: &Stmts) -> Result<(), String> {
    if let Some(name) = first_unsupported_expression_function(stmts) {
        return Err(format!("表达式引用未知函数: {name}"));
    }
    Ok(())
}

/// Return the first function call that the evaluator cannot execute.
pub fn first_unsupported_expression_function(stmts: &Stmts) -> Option<&str> {
    for stmt in &stmts.item {
        let unsupported = match stmt {
            Stmt::Assign { value, .. } => first_unsupported_expr_function(value),
            Stmt::Expr(expr) => first_unsupported_expr_function(expr),
        };
        if unsupported.is_some() {
            return unsupported;
        }
    }
    None
}

fn first_unsupported_expr_function(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Number(_) | Expr::Ident(_) => None,
        Expr::Call { name, args } => {
            if !is_supported_expression_function(name) {
                return Some(name);
            }
            for arg in args {
                if let Some(name) = first_unsupported_expr_function(arg) {
                    return Some(name);
                }
            }
            None
        }
        Expr::Unary { rhs, .. } => first_unsupported_expr_function(rhs),
        Expr::Binary { lhs, rhs, .. } => {
            first_unsupported_expr_function(lhs).or_else(|| first_unsupported_expr_function(rhs))
        }
    }
}

/// Estimate how many rows before the output range an expression needs.
pub fn estimate_expression_warmup(stmts: &Stmts) -> Result<usize, String> {
    let mut locals = HashMap::new();
    let mut consts: HashMap<String, usize> = HashMap::new();
    let mut expression_need = 0usize;

    for stmt in &stmts.item {
        match stmt {
            Stmt::Assign { name, value } => match value.clone() {
                Expr::Number(value) => {
                    if !value.is_finite() || value < 0.0 {
                        return Err("表达式常量赋值结果不能为负数或非有限值".to_string());
                    }
                    locals.remove(name);
                    consts.insert(name.clone(), value as usize);
                }
                Expr::Binary { op, lhs, rhs } => {
                    if let Some(value) = eval_binary_for_warmup(&op, &lhs, &rhs, &consts)? {
                        locals.remove(name);
                        consts.insert(name.clone(), value as usize);
                    } else {
                        let need =
                            impl_expr_warmup(Expr::Binary { op, lhs, rhs }, &locals, &consts)?;
                        consts.remove(name);
                        locals.insert(name.clone(), need);
                    }
                }
                other => {
                    let need = impl_expr_warmup(other, &locals, &consts)?;
                    consts.remove(name);
                    locals.insert(name.clone(), need);
                }
            },
            Stmt::Expr(expr) => {
                expression_need =
                    expression_need.max(impl_expr_warmup(expr.clone(), &locals, &consts)?);
            }
        }
    }

    Ok(expression_need)
}

#[cfg(test)]
mod tests {
    use super::{
        estimate_expression_warmup, parse_expression_program, validate_expression_functions,
    };
    use crate::expr::parser::{Expr, Stmt, Stmts};

    #[test]
    fn estimates_assignments_and_reassignments_consistently() {
        let program = parse_expression_program(
            "N := 5; X := MA(C, N); N := REF(C, 2); X := REF(X, 1); X > N",
        )
        .expect("expression should parse");

        assert_eq!(estimate_expression_warmup(&program), Ok(5));
    }

    #[test]
    fn rejects_unknown_functions_before_runtime() {
        let program = parse_expression_program("UNKNOWN(C, 5) > 0")
            .expect("unknown calls are syntactically valid");

        assert_eq!(
            validate_expression_functions(&program),
            Err("表达式引用未知函数: UNKNOWN".to_string())
        );
    }

    #[test]
    fn rejects_non_finite_constant_assignments() {
        let program = Stmts {
            item: vec![
                Stmt::Assign {
                    name: "N".to_string(),
                    value: Expr::Number(f64::INFINITY),
                },
                Stmt::Expr(Expr::Ident("N".to_string())),
            ],
        };

        assert_eq!(
            estimate_expression_warmup(&program),
            Err("表达式常量赋值结果不能为负数或非有限值".to_string())
        );
    }
}
