use std::collections::HashMap;

use crate::expr::parser::{BinaryOp, Expr, Stmt, Stmts, UnaryOp};

// One independent adjustment scale per stock and per index. In particular,
// C/I_CLOSE is not invariant when only the stock is adjusted.
pub(super) type Scale = Option<[f64; 8]>;

pub(super) fn program_scale(program: &Stmts, published: &HashMap<String, Scale>) -> Scale {
    let mut vars = published.clone();
    let mut result = Some([0.; 8]);
    for stmt in &program.item {
        result = match stmt {
            Stmt::Assign { name, value } => {
                let scale = expression_scale(value, &vars);
                vars.insert(name.clone(), scale);
                scale
            }
            Stmt::Expr(expr) => expression_scale(expr, &vars),
        };
    }
    result
}

fn expression_scale(expr: &Expr, vars: &HashMap<String, Scale>) -> Scale {
    let independent = [0.; 8];
    match expr {
        Expr::Number(_) => Some(independent),
        Expr::Ident(name) => {
            if let Some(scale) = vars.get(name) {
                return *scale;
            }
            let upper = name.to_ascii_uppercase();
            let (axis, field) = upper
                .split_once('_')
                .and_then(|(prefix, field)| {
                    super::RUNTIME_INDEX_COLS
                        .iter()
                        .position(|(_, key)| *key == prefix)
                        .map(|index| (index + 1, field))
                })
                .unwrap_or((0, upper.as_str()));
            let mut scale = independent;
            if matches!(
                field,
                "O" | "OPEN" | "H" | "HIGH" | "L" | "LOW" | "C" | "CLOSE" | "PRE_CLOSE" | "CHANGE"
            ) {
                scale[axis] = 1.;
            }
            Some(scale)
        }
        Expr::Unary { op, rhs } => {
            let scale = expression_scale(rhs, vars)?;
            match op {
                UnaryOp::Neg => Some(scale),
                UnaryOp::Not => (scale == independent).then_some(independent),
            }
        }
        Expr::Binary { op, lhs, rhs } => {
            let mut left = expression_scale(lhs, vars)?;
            let mut right = expression_scale(rhs, vars)?;
            if !matches!(op, BinaryOp::Mul | BinaryOp::Div) {
                if matches!(lhs.as_ref(), Expr::Number(v) if *v == 0.) {
                    left = right;
                }
                if matches!(rhs.as_ref(), Expr::Number(v) if *v == 0.) {
                    right = left;
                }
            }
            match op {
                BinaryOp::Mul | BinaryOp::Div => {
                    for axis in 0..8 {
                        left[axis] += if matches!(op, BinaryOp::Div) {
                            -right[axis]
                        } else {
                            right[axis]
                        };
                    }
                    Some(left)
                }
                BinaryOp::Add | BinaryOp::Sub => (left == right).then_some(left),
                BinaryOp::And | BinaryOp::Or => {
                    (left == independent && right == independent).then_some(independent)
                }
                _ => (left == right).then_some(independent),
            }
        }
        Expr::Call { name, args } => {
            let scales = args
                .iter()
                .map(|arg| expression_scale(arg, vars))
                .collect::<Option<Vec<_>>>()?;
            let name = name.to_ascii_uppercase();
            // A stock's adjustment factor differs from another stock's. Ratios
            // must cancel before crossing the stock axis, not after aggregation.
            if crate::expr::panel::is_cross_section_function(&name) {
                return scales
                    .iter()
                    .all(|s| *s == independent)
                    .then_some(independent);
            }
            let first = scales.first().copied().unwrap_or(independent);
            match name.as_str() {
                "REF" | "REFD" | "LAST" | "MA" | "MAD" | "EMA" | "SMA" | "HHV" | "HHVD" | "LLV"
                | "LLVD" | "SUM" | "SUMD" | "STD" | "STDD" | "ABS" => scales
                    .iter()
                    .skip(1)
                    .all(|s| *s == independent)
                    .then_some(first),
                "DIV" if scales.len() == 2 => Some(std::array::from_fn(|axis| {
                    scales[0][axis] - scales[1][axis]
                })),
                "RSV" | "RSVD" if scales.len() >= 4 => (scales[..3].iter().all(|s| *s == first)
                    && scales[3..].iter().all(|s| *s == independent))
                .then_some(independent),
                "SQRT" => Some(first.map(|power| power / 2.)),
                "ISVALID" => Some(independent),
                "MIN" | "MAX" => {
                    let scale = args
                        .iter()
                        .zip(&scales)
                        .find(|(arg, _)| !matches!(arg, Expr::Number(v) if *v == 0.))
                        .map_or(independent, |(_, scale)| *scale);
                    args.iter()
                        .zip(&scales)
                        .all(|(arg, s)| *s == scale || matches!(arg, Expr::Number(v) if *v == 0.))
                        .then_some(scale)
                }
                "IF" if args.len() == 3 && scales[0] == independent => {
                    let neutral = |arg: &Expr| {
                        matches!(arg, Expr::Number(v) if *v == 0.)
                            || matches!(arg, Expr::Call { name, .. } if name.eq_ignore_ascii_case("NULL"))
                    };
                    let left = if neutral(&args[1]) {
                        scales[2]
                    } else {
                        scales[1]
                    };
                    let right = if neutral(&args[2]) { left } else { scales[2] };
                    (left == right).then_some(left)
                }
                "CROSS" if scales.len() == 2 => (scales[0] == scales[1]).then_some(independent),
                "LRANK" | "GRANK" => scales
                    .iter()
                    .skip(1)
                    .all(|s| *s == independent)
                    .then_some(independent),
                _ => scales
                    .iter()
                    .all(|s| *s == independent)
                    .then_some(independent),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::validation::parse_expression_program;

    #[test]
    fn relative_prices_cancel_before_cross_section_aggregation() {
        let mut published = HashMap::new();
        let ma = parse_expression_program("MA(C,5)").unwrap();
        published.insert("MY_MA".into(), program_scale(&ma, &published));
        for expression in [
            "CS_MEAN(C/O)",
            "CS_MEAN((C-O)/O)",
            "CS_MEAN(C>O)",
            "CS_MEAN(C/MY_MA)",
            "CS_MEAN(C/REF(C,1)-1)",
            "X:=C/O; CS_MEAN(X)",
        ] {
            assert_eq!(
                program_scale(&parse_expression_program(expression).unwrap(), &published),
                Some([0.; 8]),
                "{expression}"
            );
        }
        for expression in [
            "CS_MEAN(C)",
            "CS_RANK(C)",
            "CS_MEAN(C)/CS_MEAN(O)",
            "CS_MEAN(C/I_CLOSE)",
            "CS_MEAN(C+1)/CS_MEAN(O)",
        ] {
            assert_ne!(
                program_scale(&parse_expression_program(expression).unwrap(), &published),
                Some([0.; 8]),
                "{expression}"
            );
        }
        let examples = crate::data::IndsData::parse_from_text(include_str!(
            "../../../../docs/indicator-market-examples.toml"
        ))
        .unwrap();
        crate::data::panel::validate_definitions(&examples).unwrap();
        // Existing stock formulas must keep their scale when a panel formula
        // later references them (RSI/KDJ and price moving averages in ind.toml).
        for (name, expression) in [
            (
                "RSI6",
                "LC:=REF(C,1); UP:=MAX(C-LC,0); DN:=ABS(C-LC); DIV(SMA(UP,6,1),SMA(DN,6,1))*100",
            ),
            (
                "J",
                "R:=RSV(C,H,L,9); K:=SMA(R,3,1); D:=SMA(K,3,1); 3*K-2*D",
            ),
            (
                "DUOKONG_LONG",
                "DIV(MA(C,14)+MA(C,28)+MA(C,57)+MA(C,114),4)",
            ),
        ] {
            let program = parse_expression_program(expression).unwrap();
            published.insert(name.into(), program_scale(&program, &published));
        }
        for expression in [
            "CS_MEAN(RSI6)",
            "CS_MEAN(J)",
            "CS_MEAN(C/DUOKONG_LONG)",
            "CS_MEAN(DIV(C,O))",
            "CS_MEAN(C/REFD(C,1,5))",
            "CS_MEAN(RSVD(C,H,L,9,20))",
        ] {
            assert_eq!(
                program_scale(&parse_expression_program(expression).unwrap(), &published),
                Some([0.; 8]),
                "{expression}"
            );
        }
        for expression in [
            "CS_MEAN(DIV(C,2))",
            "CS_MEAN(RSV(C,I_HIGH,L,9))",
            "CS_MEAN(MAX(C,1)/O)",
            "CS_MEAN(DUOKONG_LONG)",
        ] {
            assert_ne!(
                program_scale(&parse_expression_program(expression).unwrap(), &published),
                Some([0.; 8]),
                "{expression}"
            );
        }
    }
}
