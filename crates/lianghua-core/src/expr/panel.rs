//! A second evaluation axis: ordinary functions still operate along time;
//! CS_* functions operate across stocks on each date.
use std::{collections::HashMap, sync::Arc};

use super::{
    eval::{Runtime, Value},
    parser::{Expr, Stmt, Stmts},
    validation::expression_input_identifiers,
};

#[derive(Clone, Debug)]
pub enum PanelValue {
    Scalar(f64),
    Market(Arc<Vec<f64>>),
    Stocks(Arc<Vec<Vec<f64>>>),
}

impl PanelValue {
    pub fn at(&self, stock: usize, day: usize) -> Option<f64> {
        let value = match self {
            Self::Scalar(value) => *value,
            Self::Market(values) => values[day],
            Self::Stocks(values) => values[stock][day],
        };
        value.is_finite().then_some(value)
    }

    pub fn runtime_value(&self, stock: usize, days: &[usize]) -> Value {
        match self {
            Self::Scalar(value) => {
                if value.is_finite() {
                    Value::Num(*value)
                } else {
                    Value::Bool(super::eval::Truth::Unknown)
                }
            }
            _ => Value::NumSeries(days.iter().map(|&day| self.at(stock, day)).collect()),
        }
    }
}

pub trait PanelSource {
    fn eval_stock(
        &mut self,
        expr: &Expr,
        vars: &HashMap<String, PanelValue>,
    ) -> Result<PanelValue, String>;
    fn stock_count(&self) -> usize;
    fn date_count(&self) -> usize;
    fn present(&self, stock: usize, day: usize) -> bool;
}

pub fn is_cross_section_function(name: &str) -> bool {
    matches!(
        name.to_ascii_uppercase().as_str(),
        "CS_MEAN"
            | "CS_SUM"
            | "CS_COUNT"
            | "CS_MIN"
            | "CS_MAX"
            | "CS_MEDIAN"
            | "CS_STD"
            | "CS_VAR"
            | "CS_QUANTILE"
            | "CS_RANK"
            | "CS_ZSCORE"
            | "CS_CORR"
            | "CS_SPEARMAN"
            | "CS_WMEAN"
            | "CS_TOPSHARE"
    )
}

pub struct PanelRuntime<S> {
    pub source: S,
    pub vars: HashMap<String, PanelValue>,
    next_temp: usize,
}

impl<S: PanelSource> PanelRuntime<S> {
    pub fn new(source: S) -> Self {
        Self {
            source,
            vars: HashMap::new(),
            next_temp: 0,
        }
    }

    pub fn eval_program(&mut self, program: &Stmts) -> Result<PanelValue, String> {
        let mut output = PanelValue::Scalar(f64::NAN);
        for stmt in &program.item {
            output = match stmt {
                Stmt::Assign { name, value } => {
                    let result = self.eval_expr(value)?;
                    self.vars.insert(name.clone(), result.clone());
                    result
                }
                Stmt::Expr(expr) => self.eval_expr(expr)?,
            };
        }
        Ok(output)
    }

    fn eval_expr(&mut self, expr: &Expr) -> Result<PanelValue, String> {
        let mut temps = Vec::new();
        let expr = self.prepare(expr, &mut temps)?;
        let program = Stmts {
            item: vec![Stmt::Expr(expr.clone())],
        };
        let identifiers = expression_input_identifiers(&program);
        let stock = identifiers.iter().any(|name| {
            !matches!(
                self.vars.get(name),
                Some(PanelValue::Scalar(_) | PanelValue::Market(_))
            )
        });
        let result = if stock {
            self.source.eval_stock(&expr, &self.vars)
        } else {
            let constant = identifiers
                .iter()
                .all(|key| matches!(self.vars.get(key), Some(PanelValue::Scalar(_))));
            let days = (0..self.source.date_count()).collect::<Vec<_>>();
            let mut rt = Runtime::default();
            for name in identifiers {
                rt.vars
                    .insert(name.clone(), self.vars[&name].runtime_value(0, &days));
            }
            rt.eval_program(&program)
                .map_err(|e| e.msg)
                .and_then(|value| {
                    if let Value::Num(value) = value {
                        return Ok(PanelValue::Scalar(value));
                    }
                    if constant && Value::len_of(&value) == 1 {
                        return Value::as_num_series(&value, 1)
                            .map(|values| PanelValue::Scalar(values[0].unwrap_or(f64::NAN)))
                            .map_err(|e| e.msg);
                    }
                    value
                        .into_num_series(days.len())
                        .map(|values| {
                            PanelValue::Market(Arc::new(
                                values.into_iter().map(|v| v.unwrap_or(f64::NAN)).collect(),
                            ))
                        })
                        .map_err(|e| e.msg)
                })
        };
        for name in temps {
            self.vars.remove(&name);
        }
        result
    }

    fn prepare(&mut self, expr: &Expr, temps: &mut Vec<String>) -> Result<Expr, String> {
        Ok(match expr {
            Expr::Call { name, args } if is_cross_section_function(name) => {
                let result = self.aggregate(&name.to_ascii_uppercase(), args)?;
                let key = loop {
                    let key = format!("__CS_INTERNAL_{}", self.next_temp);
                    self.next_temp += 1;
                    if !self.vars.contains_key(&key) {
                        break key;
                    }
                };
                self.vars.insert(key.clone(), result);
                temps.push(key.clone());
                Expr::Ident(key)
            }
            Expr::Call { name, args } => Expr::Call {
                name: name.clone(),
                args: args
                    .iter()
                    .map(|arg| self.prepare(arg, temps))
                    .collect::<Result<_, _>>()?,
            },
            Expr::Unary { op, rhs } => Expr::Unary {
                op: op.clone(),
                rhs: Box::new(self.prepare(rhs, temps)?),
            },
            Expr::Binary { op, lhs, rhs } => Expr::Binary {
                op: op.clone(),
                lhs: Box::new(self.prepare(lhs, temps)?),
                rhs: Box::new(self.prepare(rhs, temps)?),
            },
            other => other.clone(),
        })
    }

    fn aggregate(&mut self, name: &str, args: &[Expr]) -> Result<PanelValue, String> {
        let paired = matches!(name, "CS_CORR" | "CS_SPEARMAN" | "CS_WMEAN");
        let parameter = matches!(name, "CS_QUANTILE" | "CS_TOPSHARE");
        let required = if paired || parameter { 2 } else { 1 };
        if !(required..=required + 2).contains(&args.len()) {
            return Err(format!(
                "{name}需要{required}至{}个参数：数据、可选筛选、可选分组",
                required + 2
            ));
        }
        let mut values = Vec::new();
        for arg in args {
            values.push(self.eval_expr(arg)?);
        }
        let q = if parameter {
            match values[1] {
                PanelValue::Scalar(q) if q.is_finite() && (0.0..=1.0).contains(&q) => q,
                _ => return Err(format!("{name}第二个参数必须是[0,1]内常数")),
            }
        } else {
            0.
        };
        let grouped = args.len() == required + 2;
        let individual = grouped || matches!(name, "CS_RANK" | "CS_ZSCORE");
        let (n, d) = (self.source.stock_count(), self.source.date_count());
        let mut market = vec![f64::NAN; d];
        let mut stocks = if individual {
            vec![vec![f64::NAN; d]; n]
        } else {
            Vec::new()
        };
        for day in 0..d {
            if name == "CS_COUNT" && !grouped && (0..n).any(|stock| self.source.present(stock, day))
            {
                market[day] = 0.;
            }
            let mut groups: HashMap<u64, Vec<(usize, f64, f64)>> = HashMap::new();
            for stock in 0..n {
                if !self.source.present(stock, day) {
                    continue;
                }
                if values
                    .get(required)
                    .is_some_and(|filter| filter.at(stock, day).is_none_or(|v| v == 0.))
                {
                    continue;
                }
                let Some(x) = values[0].at(stock, day) else {
                    continue;
                };
                let y = if paired {
                    let Some(y) = values[1].at(stock, day) else {
                        continue;
                    };
                    y
                } else {
                    0.
                };
                let key = if grouped {
                    let Some(key) = values[required + 1].at(stock, day) else {
                        continue;
                    };
                    if key == 0. { 0 } else { key.to_bits() }
                } else {
                    0
                };
                groups.entry(key).or_default().push((stock, x, y));
            }
            for members in groups.values() {
                let x = members.iter().map(|v| v.1).collect::<Vec<_>>();
                let mean = x.iter().sum::<f64>() / x.len() as f64;
                let variance = (x.len() > 1).then(|| {
                    x.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (x.len() - 1) as f64
                });
                let value = match name {
                    "CS_COUNT" => Some(x.len() as f64),
                    "CS_SUM" => Some(x.iter().sum()),
                    "CS_MEAN" => Some(mean),
                    "CS_MIN" => x.iter().copied().reduce(f64::min),
                    "CS_MAX" => x.iter().copied().reduce(f64::max),
                    "CS_VAR" => variance,
                    "CS_STD" => variance.map(f64::sqrt),
                    "CS_MEDIAN" | "CS_QUANTILE" => {
                        let mut sorted = x.clone();
                        sorted.sort_by(f64::total_cmp);
                        let position =
                            if name == "CS_MEDIAN" { 0.5 } else { q } * (sorted.len() - 1) as f64;
                        let a = position.floor() as usize;
                        let b = position.ceil() as usize;
                        Some(sorted[a] + (sorted[b] - sorted[a]) * (position - a as f64))
                    }
                    "CS_WMEAN" => {
                        let total = members.iter().map(|v| v.2).sum::<f64>();
                        (total > 0. && members.iter().all(|v| v.2 >= 0.))
                            .then(|| members.iter().map(|v| v.1 * v.2).sum::<f64>() / total)
                    }
                    "CS_TOPSHARE" => {
                        let total = x.iter().sum::<f64>();
                        let mut sorted = x.clone();
                        sorted.sort_by(|a, b| b.total_cmp(a));
                        (total > 0. && x.iter().all(|v| *v >= 0.)).then(|| {
                            sorted
                                .iter()
                                .take((q * x.len() as f64).ceil() as usize)
                                .sum::<f64>()
                                / total
                        })
                    }
                    "CS_CORR" | "CS_SPEARMAN" => {
                        let y = members.iter().map(|v| v.2).collect::<Vec<_>>();
                        let (x, y) = if name == "CS_SPEARMAN" {
                            (cross_section_ranks(&x), cross_section_ranks(&y))
                        } else {
                            (x.clone(), y)
                        };
                        let a = x.iter().sum::<f64>() / x.len() as f64;
                        let b = y.iter().sum::<f64>() / y.len() as f64;
                        let norm = (x.iter().map(|v| (v - a).powi(2)).sum::<f64>()
                            * y.iter().map(|v| (v - b).powi(2)).sum::<f64>())
                        .sqrt();
                        (norm > 0.).then(|| {
                            x.iter().zip(y).map(|(x, y)| (x - a) * (y - b)).sum::<f64>() / norm
                        })
                    }
                    "CS_RANK" => {
                        let ranks = cross_section_ranks(&x);
                        for (member, rank) in members.iter().zip(ranks) {
                            stocks[member.0][day] = rank / members.len() as f64;
                        }
                        None
                    }
                    "CS_ZSCORE" => {
                        if let Some(sd) = variance.map(f64::sqrt).filter(|v| *v > 0.) {
                            for member in members {
                                stocks[member.0][day] = (member.1 - mean) / sd;
                            }
                        }
                        None
                    }
                    _ => unreachable!(),
                }
                .filter(|v| v.is_finite());
                if let Some(value) = value {
                    if individual {
                        for member in members {
                            stocks[member.0][day] = value;
                        }
                    } else {
                        market[day] = value;
                    }
                }
            }
        }
        Ok(if individual {
            PanelValue::Stocks(Arc::new(stocks))
        } else {
            PanelValue::Market(Arc::new(market))
        })
    }
}

pub fn cross_section_ranks(values: &[f64]) -> Vec<f64> {
    let mut order = (0..values.len()).collect::<Vec<_>>();
    order.sort_by(|&a, &b| values[a].total_cmp(&values[b]));
    let mut ranks = vec![0.; values.len()];
    let mut start = 0;
    while start < order.len() {
        let mut end = start + 1;
        while end < order.len() && values[order[end]] - values[order[start]] < 1e-12 {
            end += 1;
        }
        for &index in &order[start..end] {
            ranks[index] = (start + 1 + end) as f64 / 2.;
        }
        start = end;
    }
    ranks
}
