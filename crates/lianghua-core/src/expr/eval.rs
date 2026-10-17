use crate::expr::parser::{BinaryOp, Expr, Stmt, Stmts, UnaryOp};
use std::borrow::Cow;
use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

const EPS: f64 = 1e-12;

#[derive(Debug, Clone)]
pub struct EvalErr {
    pub msg: String,
}

#[derive(Debug, Default, Clone)]
pub struct Runtime {
    pub vars: HashMap<String, Value>,
}

/// Boolean results retain missing evidence until the final match boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Truth {
    False,
    True,
    Unknown,
}

impl From<bool> for Truth {
    fn from(value: bool) -> Self {
        if value { Self::True } else { Self::False }
    }
}

impl Truth {
    pub fn from_num(value: Option<f64>) -> Self {
        match value.filter(|value| value.is_finite()) {
            Some(value) => (value != 0.0).into(),
            None => Self::Unknown,
        }
    }

    pub fn as_num(self) -> Option<f64> {
        match self {
            Self::True => Some(1.0),
            Self::False => Some(0.0),
            Self::Unknown => None,
        }
    }

    pub fn is_true(self) -> bool {
        self == Self::True
    }
    pub fn is_known(self) -> bool {
        self != Self::Unknown
    }

    fn and(self, rhs: Self) -> Self {
        match (self, rhs) {
            (Self::False, _) | (_, Self::False) => Self::False,
            (Self::True, Self::True) => Self::True,
            _ => Self::Unknown,
        }
    }

    fn or(self, rhs: Self) -> Self {
        match (self, rhs) {
            (Self::True, _) | (_, Self::True) => Self::True,
            (Self::False, Self::False) => Self::False,
            _ => Self::Unknown,
        }
    }
}

impl std::ops::Not for Truth {
    type Output = Self;
    fn not(self) -> Self {
        match self {
            Self::True => Self::False,
            Self::False => Self::True,
            Self::Unknown => Self::Unknown,
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) enum DayValue {
    Unset,
    Num(Option<f64>),
    Bool(Truth),
}

pub(crate) fn day_value_as_num(value: DayValue) -> Option<f64> {
    match value {
        DayValue::Unset => None,
        DayValue::Num(value) => value.filter(|value| value.is_finite()),
        DayValue::Bool(value) => value.as_num(),
    }
}

fn day_value_as_truth(value: DayValue) -> Truth {
    Truth::from_num(day_value_as_num(value))
}

fn eval_day_binary(op: &BinaryOp, lhs: DayValue, rhs: DayValue) -> DayValue {
    if matches!(op, BinaryOp::And) {
        return DayValue::Bool(day_value_as_truth(lhs).and(day_value_as_truth(rhs)));
    }
    if matches!(op, BinaryOp::Or) {
        return DayValue::Bool(day_value_as_truth(lhs).or(day_value_as_truth(rhs)));
    }
    let pair = day_value_as_num(lhs).zip(day_value_as_num(rhs));
    match op {
        BinaryOp::Add => DayValue::Num(pair.map(|(a, b)| a + b)),
        BinaryOp::Sub => DayValue::Num(pair.map(|(a, b)| a - b)),
        BinaryOp::Mul => DayValue::Num(pair.map(|(a, b)| a * b)),
        BinaryOp::Div => DayValue::Num(pair.map(|(a, b)| if b.abs() < EPS { 0.0 } else { a / b })),
        _ => DayValue::Bool(
            pair.map(|(a, b)| match op {
                BinaryOp::Ge => a > b + EPS || (a - b).abs() <= EPS,
                BinaryOp::Gt => a > b + EPS,
                BinaryOp::Le => a < b - EPS || (a - b).abs() <= EPS,
                BinaryOp::Lt => a < b - EPS,
                BinaryOp::Eq => (a - b).abs() <= EPS,
                BinaryOp::Ne => (a - b).abs() > EPS,
                _ => unreachable!(),
            })
            .map(Truth::from)
            .unwrap_or(Truth::Unknown),
        ),
    }
}

impl Runtime {
    fn dynamic_limit(&mut self, arg: &Expr, fn_name: &str) -> Result<usize, EvalErr> {
        let limit = Value::as_num(&self.eval_expr(arg)?)?;
        if !limit.is_finite() || limit <= 0.0 || limit.fract().abs() > EPS {
            return Err(EvalErr {
                msg: format!("{fn_name}动态周期上限必须是正整数"),
            });
        }
        Ok(limit as usize)
    }

    fn dynamic_period_series(
        &self,
        period: Value,
        len: usize,
        max_period: usize,
        min_period: usize,
    ) -> Result<Vec<Option<usize>>, EvalErr> {
        let period_series = Value::as_num_series(&period, len)?;
        Ok(period_series
            .iter()
            .copied()
            .map(|value| {
                let value = value?;
                if !value.is_finite() {
                    return None;
                }

                if value > max_period as f64 {
                    return None;
                }

                let period = if value as i64 <= 0 {
                    min_period
                } else {
                    value as usize
                };
                Some(period)
            })
            .collect())
    }

    fn dynamic_num_window_args(
        &mut self,
        args: &[Expr],
        fn_name: &str,
    ) -> Result<(Value, Vec<Option<usize>>, usize), EvalErr> {
        if args.len() != 3 {
            return Err(EvalErr {
                msg: format!("{fn_name}需要三个参数"),
            });
        }

        let value = self.eval_expr(&args[0])?;
        let period = self.eval_expr(&args[1])?;
        let len = Value::len_of(&value).max(Value::len_of(&period));
        let max_period = self.dynamic_limit(&args[2], fn_name)?;
        let period_series = self.dynamic_period_series(period, len, max_period, 1)?;

        Ok((value, period_series, len))
    }

    fn impl_extremed(
        &mut self,
        args: &[Expr],
        fn_name: &str,
        greater: bool,
    ) -> Result<Value, EvalErr> {
        let (value, period_series, len) = self.dynamic_num_window_args(args, fn_name)?;
        let num_series = Value::as_num_series(&value, len)?;
        let mut out = Vec::with_capacity(len);

        for i in 0..len {
            let Some(period) = period_series[i] else {
                out.push(None);
                continue;
            };
            if i + 1 < period {
                out.push(None);
                continue;
            }

            let start = i + 1 - period;
            let mut best = match num_series[start] {
                Some(v) => v,
                None => {
                    out.push(None);
                    continue;
                }
            };

            let mut complete = true;
            for value in num_series.iter().take(i + 1).skip(start) {
                let Some(value) = value else {
                    complete = false;
                    break;
                };
                if (greater && *value > best) || (!greater && *value < best) {
                    best = *value;
                }
            }
            out.push(complete.then_some(best));
        }

        Ok(Value::NumSeries(out))
    }

    fn impl_window_sumd(&mut self, args: &[Expr], average: bool) -> Result<Value, EvalErr> {
        let (value, period_series, len) =
            self.dynamic_num_window_args(args, if average { "MAD" } else { "SUMD" })?;
        let num_series = Value::as_num_series(&value, len)?;
        let mut out = Vec::with_capacity(len);

        for i in 0..len {
            let Some(period) = period_series[i] else {
                out.push(None);
                continue;
            };
            if i + 1 < period {
                out.push(None);
                continue;
            }

            let start = i + 1 - period;
            let mut sum = 0.0;
            let mut has_none = false;
            for value in num_series.iter().take(i + 1).skip(start) {
                match value {
                    Some(value) => sum += value,
                    None => {
                        has_none = true;
                        break;
                    }
                }
            }

            if has_none {
                out.push(None);
            } else if average {
                out.push(Some(sum / period as f64));
            } else {
                out.push(Some(sum));
            }
        }

        Ok(Value::NumSeries(out))
    }

    fn impl_stdd(&mut self, args: &[Expr]) -> Result<Value, EvalErr> {
        let (value, period_series, len) = self.dynamic_num_window_args(args, "STDD")?;
        let num_series = Value::as_num_series(&value, len)?;
        let mut out = Vec::with_capacity(len);

        for i in 0..len {
            let Some(period) = period_series[i] else {
                out.push(None);
                continue;
            };
            if i + 1 < period {
                out.push(None);
                continue;
            }

            let start = i + 1 - period;
            let mut sum = 0.0;
            let mut has_none = false;
            for value in num_series.iter().take(i + 1).skip(start) {
                match value {
                    Some(value) => sum += value,
                    None => {
                        has_none = true;
                        break;
                    }
                }
            }
            if has_none {
                out.push(None);
                continue;
            }

            let mean = sum / period as f64;
            let mut sum_sq = 0.0;
            for value in num_series.iter().take(i + 1).skip(start) {
                if let Some(value) = value {
                    sum_sq += (value - mean).powi(2);
                }
            }
            out.push(Some((sum_sq / period as f64).sqrt()));
        }

        Ok(Value::NumSeries(out))
    }

    fn impl_rankd(
        &mut self,
        args: &[Expr],
        fn_name: &str,
        greater_first: bool,
    ) -> Result<Value, EvalErr> {
        let (value, period_series, len) = self.dynamic_num_window_args(args, fn_name)?;
        let num_series = Value::as_num_series(&value, len)?;
        let mut out = Vec::with_capacity(len);

        for i in 0..len {
            let Some(period) = period_series[i] else {
                out.push(None);
                continue;
            };
            if i + 1 < period {
                out.push(None);
                continue;
            }

            let start = i + 1 - period;
            let curr = match num_series[i] {
                Some(v) => v,
                None => {
                    out.push(None);
                    continue;
                }
            };
            let mut count = 1usize;
            let mut bad = false;
            for history in num_series.iter().take(i).skip(start) {
                let Some(history) = history else {
                    bad = true;
                    break;
                };
                if (greater_first && (*history > curr + EPS || (*history - curr).abs() <= EPS))
                    || (!greater_first && (*history < curr - EPS || (*history - curr).abs() <= EPS))
                {
                    count += 1;
                }
            }

            if bad {
                out.push(None);
            } else {
                out.push(Some(count as f64));
            }
        }

        Ok(Value::NumSeries(out))
    }

    fn impl_abs(&mut self, args: &[Expr]) -> Result<Value, EvalErr> {
        if args.len() != 1 {
            return Err(EvalErr {
                msg: "ABS需要一个参数".to_string(),
            });
        }

        let v = self.eval_expr(&args[0])?;
        let len = Value::len_of(&v);
        let vs = Value::as_num_series(&v, len)?;
        let mut out = Vec::with_capacity(len);
        for x in vs.iter().take(len) {
            out.push(x.map(|x| x.abs()));
        }
        Ok(Value::NumSeries(out))
    }

    fn impl_condition_window(
        &mut self,
        args: &[Expr],
        dynamic: bool,
        exists: bool,
    ) -> Result<Value, EvalErr> {
        let name = match (dynamic, exists) {
            (true, true) => "EXISTD",
            (true, false) => "COUNTD",
            (false, true) => "EXIST",
            (false, false) => "COUNT",
        };
        if args.len() != if dynamic { 3 } else { 2 } {
            return Err(EvalErr {
                msg: format!("{name}参数数量错误"),
            });
        }
        let condition = self.eval_expr(&args[0])?;
        let period = self.eval_expr(&args[1])?;
        let len = if dynamic {
            Value::len_of(&condition).max(Value::len_of(&period))
        } else {
            Value::len_of(&condition)
        };
        let conditions = Value::as_truth_series(&condition, len)?;
        let periods = if dynamic {
            let limit = self.dynamic_limit(&args[2], name)?;
            self.dynamic_period_series(period, len, limit, 1)?
        } else {
            let period = Value::as_num(&period)?;
            vec![
                Some(if period as i64 <= 0 {
                    1
                } else {
                    period as usize
                });
                len
            ]
        };
        let mut prefix = Vec::with_capacity(len + 1);
        prefix.push((0usize, 0usize));
        for truth in conditions.iter() {
            let (hits, missing) = *prefix.last().unwrap();
            prefix.push((
                hits + usize::from(truth.is_true()),
                missing + usize::from(!truth.is_known()),
            ));
        }
        let mut counts = Vec::with_capacity(if exists { 0 } else { len });
        let mut truths = Vec::with_capacity(if exists { len } else { 0 });
        for (day, period) in periods.into_iter().enumerate() {
            let Some(period) = period else {
                if exists {
                    truths.push(Truth::Unknown);
                } else {
                    counts.push(None);
                }
                continue;
            };
            let start = (day + 1).saturating_sub(period);
            let hits = prefix[day + 1].0 - prefix[start].0;
            let missing = day + 1 < period || prefix[day + 1].1 != prefix[start].1;
            if exists {
                truths.push(if hits > 0 {
                    Truth::True
                } else if missing {
                    Truth::Unknown
                } else {
                    Truth::False
                });
            } else {
                counts.push((!missing).then_some(hits as f64));
            }
        }
        Ok(if exists {
            Value::BoolSeries(truths)
        } else {
            Value::NumSeries(counts)
        })
    }

    fn impl_countd(&mut self, args: &[Expr]) -> Result<Value, EvalErr> {
        self.impl_condition_window(args, true, false)
    }
    fn impl_existd(&mut self, args: &[Expr]) -> Result<Value, EvalErr> {
        self.impl_condition_window(args, true, true)
    }

    fn impl_max(&mut self, args: &[Expr]) -> Result<Value, EvalErr> {
        if args.len() != 2 {
            return Err(EvalErr {
                msg: "MAX需要两个参数".to_string(),
            });
        }

        let a = self.eval_expr(&args[0])?;
        let b = self.eval_expr(&args[1])?;
        let len = usize::max(Value::len_of(&a), Value::len_of(&b));
        let a_series = Value::as_num_series(&a, len)?;
        let b_series = Value::as_num_series(&b, len)?;
        let mut out = Vec::with_capacity(len);
        for i in 0..len {
            match (a_series[i], b_series[i]) {
                (Some(x), Some(y)) => out.push(Some(x.max(y))),
                _ => out.push(None),
            }
        }
        Ok(Value::NumSeries(out))
    }

    fn impl_refd(&mut self, args: &[Expr]) -> Result<Value, EvalErr> {
        if args.len() != 3 {
            return Err(EvalErr {
                msg: "REFD需要三个参数".to_string(),
            });
        }

        let v = self.eval_expr(&args[0])?;
        let period = self.eval_expr(&args[1])?;
        let len = Value::len_of(&v).max(Value::len_of(&period));
        let num_series = Value::as_num_series(&v, len)?;
        let max_period = self.dynamic_limit(&args[2], "REFD")?;
        let period_series = self.dynamic_period_series(period, len, max_period, 0)?;
        let mut out = Vec::with_capacity(len);

        for i in 0..len {
            let Some(period) = period_series[i] else {
                out.push(None);
                continue;
            };
            if i < period {
                out.push(None);
            } else {
                out.push(num_series[i - period]);
            }
        }

        Ok(Value::NumSeries(out))
    }

    fn impl_min(&mut self, args: &[Expr]) -> Result<Value, EvalErr> {
        if args.len() != 2 {
            return Err(EvalErr {
                msg: "MIN需要两个参数".to_string(),
            });
        }

        let a = self.eval_expr(&args[0])?;
        let b = self.eval_expr(&args[1])?;
        let len = usize::max(Value::len_of(&a), Value::len_of(&b));
        let as_ = Value::as_num_series(&a, len)?;
        let bs_ = Value::as_num_series(&b, len)?;
        let mut out = Vec::with_capacity(len);
        for i in 0..len {
            match (as_[i], bs_[i]) {
                (Some(x), Some(y)) => out.push(Some(x.min(y))),
                _ => out.push(None),
            }
        }
        Ok(Value::NumSeries(out))
    }

    fn impl_hhvd(&mut self, args: &[Expr]) -> Result<Value, EvalErr> {
        self.impl_extremed(args, "HHVD", true)
    }

    fn impl_llvd(&mut self, args: &[Expr]) -> Result<Value, EvalErr> {
        self.impl_extremed(args, "LLVD", false)
    }

    fn impl_div(&mut self, args: &[Expr]) -> Result<Value, EvalErr> {
        if args.len() != 2 {
            return Err(EvalErr {
                msg: "DIV需要两个参数".to_string(),
            });
        }

        let a = self.eval_expr(&args[0])?;
        let b = self.eval_expr(&args[1])?;

        if matches!(a, Value::Num(_) | Value::Bool(_))
            && matches!(b, Value::Num(_) | Value::Bool(_))
        {
            let l = Value::as_num(&a)?;
            let r = Value::as_num(&b)?;
            return if r.abs() < EPS {
                Ok(Value::Num(0.0))
            } else {
                Ok(Value::Num(l / r))
            };
        }

        let len = usize::max(Value::len_of(&a), Value::len_of(&b));
        let ls = Value::as_num_series(&a, len)?;
        let rs = Value::as_num_series(&b, len)?;
        let mut out: Vec<Option<f64>> = Vec::with_capacity(len);

        for i in 0..len {
            match (ls[i], rs[i]) {
                (Some(l), Some(r)) => {
                    if r.abs() < EPS {
                        out.push(Some(0.0));
                    } else {
                        out.push(Some(l / r));
                    }
                }
                _ => out.push(None),
            }
        }

        Ok(Value::NumSeries(out))
    }

    fn impl_count(&mut self, args: &[Expr]) -> Result<Value, EvalErr> {
        self.impl_condition_window(args, false, false)
    }
    fn impl_exist(&mut self, args: &[Expr]) -> Result<Value, EvalErr> {
        self.impl_condition_window(args, false, true)
    }

    fn impl_ma(&mut self, args: &[Expr]) -> Result<Value, EvalErr> {
        if args.len() != 2 {
            return Err(EvalErr {
                msg: "MA需要两个参数".to_string(),
            });
        }

        let values = self.eval_expr(&args[0])?;
        let len = Value::len_of(&values);

        let ori_n = Value::as_num(&self.eval_expr(&args[1])?)?;
        let std_n = { if ori_n as i64 <= 0 { 1 } else { ori_n as usize } };
        let num_series = Value::as_num_series(&values, len)?;
        let mut out = Vec::with_capacity(len);
        for i in 0..len {
            if i + 1 < std_n {
                out.push(None);
                continue;
            }

            let start = i + 1 - std_n;
            let mut sum = 0.0;
            let mut has_none = false;

            for v in &num_series[start..=i] {
                match v {
                    Some(v) => sum += v,
                    None => {
                        has_none = true;
                        break;
                    }
                }
            }
            if has_none {
                out.push(None);
            } else {
                out.push(Some(sum / std_n as f64));
            }
        }

        Ok(Value::NumSeries(out))
    }

    fn impl_ref(&mut self, args: &[Expr]) -> Result<Value, EvalErr> {
        if args.len() != 2 {
            return Err(EvalErr {
                msg: "REF需要两个参数".to_string(),
            });
        }

        let v = self.eval_expr(&args[0])?;
        let len = Value::len_of(&v);
        let num_series = Value::as_num_series(&v, len)?;
        let mut out = Vec::with_capacity(len);
        let ori_n = Value::as_num(&self.eval_expr(&args[1])?)?;
        let std_n = { if ori_n as i64 <= 0 { 0 } else { ori_n as usize } };

        for i in 0..len {
            if i < std_n {
                out.push(None);
            } else {
                out.push(num_series[i - std_n])
            }
        }
        Ok(Value::NumSeries(out))
    }

    fn impl_last(&mut self, args: &[Expr]) -> Result<Value, EvalErr> {
        if args.len() != 2 {
            return Err(EvalErr {
                msg: "LAST需要两个参数".to_string(),
            });
        }

        let v = self.eval_expr(&args[0])?;
        let ori_n = Value::as_num(&self.eval_expr(&args[1])?)?;
        let std_n = if ori_n as i64 <= 0 { 0 } else { ori_n as usize };

        let len = Value::len_of(&v);

        if std_n >= len {
            return Err(EvalErr {
                msg: format!("LAST偏移越界: 序列长度={len}, 偏移={std_n}"),
            });
        }

        let idx = len - 1 - std_n;
        match v {
            Value::Num(n) => Ok(Value::Num(n)),
            Value::Bool(b) => Ok(Value::Bool(b)),
            Value::NumSeries(ns) => match ns[idx] {
                Some(n) => Ok(Value::Num(n)),
                None => Ok(Value::Bool(Truth::Unknown)),
            },
            Value::NumWindow { values, range } => match values[range.start + idx] {
                Some(n) => Ok(Value::Num(n)),
                None => Ok(Value::Bool(Truth::Unknown)),
            },
            Value::SharedNumSeries(ns) => match ns[idx] {
                Some(n) => Ok(Value::Num(n)),
                None => Ok(Value::Bool(Truth::Unknown)),
            },
            Value::BoolSeries(bs) => Ok(Value::Bool(bs[idx])),
        }
    }

    fn impl_hhv(&mut self, args: &[Expr]) -> Result<Value, EvalErr> {
        if args.len() != 2 {
            return Err(EvalErr {
                msg: "HHV需要两个参数".to_string(),
            });
        }

        let v = self.eval_expr(&args[0])?;
        let len = Value::len_of(&v);
        let num_series = Value::as_num_series(&v, len)?;
        let mut out = Vec::with_capacity(len);
        let ori_n = Value::as_num(&self.eval_expr(&args[1])?)?;
        let std_n = { if ori_n as i64 <= 0 { 1 } else { ori_n as usize } };

        for i in 0..len {
            if i + 1 < std_n {
                out.push(None);
                continue;
            }

            let start = i + 1 - std_n;
            let mut max = match num_series[start] {
                Some(v) => v,
                None => {
                    out.push(None);
                    continue;
                }
            };

            let mut complete = true;
            for value in &num_series[start..=i] {
                let Some(value) = value else {
                    complete = false;
                    break;
                };
                if *value > max {
                    max = *value;
                }
            }
            out.push(complete.then_some(max));
        }
        Ok(Value::NumSeries(out))
    }

    fn impl_llv(&mut self, args: &[Expr]) -> Result<Value, EvalErr> {
        if args.len() != 2 {
            return Err(EvalErr {
                msg: "LLV需要两个参数".to_string(),
            });
        }

        let v = self.eval_expr(&args[0])?;
        let len = Value::len_of(&v);
        let num_series = Value::as_num_series(&v, len)?;
        let mut out = Vec::with_capacity(len);
        let ori_n = Value::as_num(&self.eval_expr(&args[1])?)?;
        let std_n = { if ori_n as i64 <= 0 { 1 } else { ori_n as usize } };

        for i in 0..len {
            if i + 1 < std_n {
                out.push(None);
                continue;
            }

            let start = i + 1 - std_n;
            let mut min = match num_series[start] {
                Some(v) => v,
                None => {
                    out.push(None);
                    continue;
                }
            };

            let mut complete = true;
            for value in &num_series[start..=i] {
                let Some(value) = value else {
                    complete = false;
                    break;
                };
                if *value < min {
                    min = *value;
                }
            }
            out.push(complete.then_some(min));
        }
        Ok(Value::NumSeries(out))
    }

    fn impl_sum(&mut self, args: &[Expr]) -> Result<Value, EvalErr> {
        if args.len() != 2 {
            return Err(EvalErr {
                msg: "SUM需要两个参数".to_string(),
            });
        }

        let v = self.eval_expr(&args[0])?;
        let len = Value::len_of(&v);
        let num_series = Value::as_num_series(&v, len)?;
        let mut out = Vec::with_capacity(len);
        let ori_n = Value::as_num(&self.eval_expr(&args[1])?)?;
        let std_n = { if ori_n as i64 <= 0 { 1 } else { ori_n as usize } };

        for i in 0..len {
            if i + 1 < std_n {
                out.push(None);
                continue;
            }

            let start = i + 1 - std_n;
            let mut has_none = false;
            let mut sum = 0.0;

            for j in start..=i {
                match num_series[j] {
                    Some(a) => sum += a,
                    None => {
                        has_none = true;
                        break;
                    }
                }
            }
            if has_none {
                out.push(None);
            } else {
                out.push(Some(sum));
            }
        }
        Ok(Value::NumSeries(out))
    }

    fn impl_std(&mut self, args: &[Expr]) -> Result<Value, EvalErr> {
        if args.len() != 2 {
            return Err(EvalErr {
                msg: "STD需要两个参数".to_string(),
            });
        }

        let v = self.eval_expr(&args[0])?;
        let len = Value::len_of(&v);
        let num_series = Value::as_num_series(&v, len)?;
        let mut out = Vec::with_capacity(len);
        let ori_n = Value::as_num(&self.eval_expr(&args[1])?)?;
        let std_n = { if ori_n as i64 <= 0 { 1 } else { ori_n as usize } };

        for i in 0..len {
            if i + 1 < std_n {
                out.push(None);
                continue;
            }
            let start = i + 1 - std_n;
            let mut sum = 0.0;
            let mut has_none = false;

            for j in start..=i {
                match num_series[j] {
                    Some(x) => sum += x,
                    None => {
                        has_none = true;
                        break;
                    }
                }
            }
            if has_none {
                out.push(None);
                continue;
            }

            let mean = sum / std_n as f64;
            let mut sum_sq = 0.0;
            for j in start..=i {
                if let Some(x) = num_series[j] {
                    sum_sq += (x - mean).powi(2);
                }
            }
            out.push(Some((sum_sq / std_n as f64).sqrt()));
        }
        Ok(Value::NumSeries(out))
    }

    fn impl_if(&mut self, args: &[Expr]) -> Result<Value, EvalErr> {
        if args.len() != 3 {
            return Err(EvalErr {
                msg: "IF需要三个参数".to_string(),
            });
        }

        let cond = self.eval_expr(&args[0])?;
        let l = self.eval_expr(&args[1])?;
        let r = self.eval_expr(&args[2])?;
        let len_cond = Value::len_of(&cond);
        let len_l = Value::len_of(&l);
        let len_r = Value::len_of(&r);
        let len = len_cond.max(len_l).max(len_r);

        let b_series = Value::as_truth_series(&cond, len)?;
        let l_series = Value::as_num_series(&l, len)?;
        let r_series = Value::as_num_series(&r, len)?;
        let mut out = Vec::with_capacity(len);

        for i in 0..len {
            out.push(match b_series[i] {
                Truth::True => l_series[i],
                Truth::False => r_series[i],
                Truth::Unknown => None,
            });
        }
        Ok(Value::NumSeries(out))
    }

    fn impl_cross(&mut self, args: &[Expr]) -> Result<Value, EvalErr> {
        if args.len() != 2 {
            return Err(EvalErr {
                msg: "CROSS需要两个参数".to_string(),
            });
        }

        let l = self.eval_expr(&args[0])?;
        let r = self.eval_expr(&args[1])?;
        let len_l = Value::len_of(&l);
        let len_r = Value::len_of(&r);
        let len = len_l.max(len_r);
        let l_series = Value::as_num_series(&l, len)?;
        let r_series = Value::as_num_series(&r, len)?;
        let mut out = Vec::with_capacity(len);

        if len > 0 {
            out.push(Truth::Unknown);
        }
        for i in 1..len {
            let hit = match (l_series[i], r_series[i], l_series[i - 1], r_series[i - 1]) {
                (Some(a), Some(b), Some(pa), Some(pb))
                    if [a, b, pa, pb].iter().all(|n| n.is_finite()) =>
                {
                    (a > b && pa <= pb).into()
                }
                _ => Truth::Unknown,
            };
            out.push(hit);
        }
        Ok(Value::BoolSeries(out))
    }

    fn impl_ema(&mut self, args: &[Expr]) -> Result<Value, EvalErr> {
        if args.len() != 2 {
            return Err(EvalErr {
                msg: "EMA需要两个参数".to_string(),
            });
        }

        let v = self.eval_expr(&args[0])?;
        let len = Value::len_of(&v);
        let n_series = Value::as_num_series(&v, len)?;
        let ori_n = Value::as_num(&self.eval_expr(&args[1])?)?;
        let std_n = { if ori_n as i64 <= 0 { 1 } else { ori_n as usize } };
        let alpha = 2.0 / (std_n as f64 + 1.0);
        let mut out = Vec::with_capacity(len);
        let mut prev: Option<f64> = None;

        for i in 0..len {
            match n_series[i] {
                None => {
                    out.push(None);
                    prev = None;
                }
                Some(x) => {
                    let ema = match prev {
                        None => x,
                        Some(p) => alpha * x + (1.0 - alpha) * p,
                    };
                    out.push(Some(ema));
                    prev = Some(ema);
                }
            }
        }
        Ok(Value::NumSeries(out))
    }

    fn impl_sma(&mut self, args: &[Expr]) -> Result<Value, EvalErr> {
        if args.len() != 3 {
            return Err(EvalErr {
                msg: "SMA需要三个参数".to_string(),
            });
        }

        let v = self.eval_expr(&args[0])?;
        let len = Value::len_of(&v);
        let n_series = Value::as_num_series(&v, len)?;
        let ori_n = Value::as_num(&self.eval_expr(&args[1])?)?;
        let ori_m = Value::as_num(&self.eval_expr(&args[2])?)?;
        let std_n = { if ori_n as i64 <= 0 { 1.0 } else { ori_n } };
        let std_m = if ori_m < 0.0 { 0.0 } else { ori_m };
        let alpha = (std_m / std_n).clamp(0.0, 1.0);
        let mut out = Vec::with_capacity(len);
        let mut prev: Option<f64> = None;

        for i in 0..len {
            match n_series[i] {
                None => {
                    out.push(None);
                    prev = None;
                }
                Some(x) => {
                    let ema = match prev {
                        None => x,
                        Some(p) => alpha * x + (1.0 - alpha) * p,
                    };
                    out.push(Some(ema));
                    prev = Some(ema);
                }
            }
        }
        Ok(Value::NumSeries(out))
    }

    fn impl_barslast(&mut self, args: &[Expr]) -> Result<Value, EvalErr> {
        if args.len() != 1 {
            return Err(EvalErr {
                msg: "BARSLAST需要一个参数".to_string(),
            });
        }

        let v = self.eval_expr(&args[0])?;
        let len = Value::len_of(&v);
        let b_series = Value::as_truth_series(&v, len)?;
        let mut out = Vec::with_capacity(len);
        let mut age: Option<usize> = None;
        for truth in b_series.iter() {
            age = match truth {
                Truth::True => Some(0),
                Truth::False => age.map(|age| age + 1),
                Truth::Unknown => None,
            };
            out.push(age.map(|age| age as f64));
        }
        Ok(Value::NumSeries(out))
    }

    fn impl_rsv(&mut self, args: &[Expr]) -> Result<Value, EvalErr> {
        if args.len() != 4 {
            return Err(EvalErr {
                msg: "RSV需要四个参数".to_string(),
            });
        }

        let c = self.eval_expr(&args[0])?;
        let h = self.eval_expr(&args[1])?;
        let l = self.eval_expr(&args[2])?;
        let ori_n = Value::as_num(&self.eval_expr(&args[3])?)?;
        let std_n = { if ori_n as i64 <= 0 { 1 } else { ori_n as usize } };

        let len = Value::len_of(&c)
            .max(Value::len_of(&h))
            .max(Value::len_of(&l));
        let c_s = Value::as_num_series(&c, len)?;
        let h_s = Value::as_num_series(&h, len)?;
        let l_s = Value::as_num_series(&l, len)?;
        let mut out = Vec::with_capacity(len);

        for i in 0..len {
            if i + 1 < std_n {
                out.push(None);
                continue;
            }
            let start = i + 1 - std_n;

            let c = match c_s[i] {
                Some(v) => v,
                None => {
                    out.push(None);
                    continue;
                }
            };

            let mut llv = f64::INFINITY;
            let mut hhv = f64::NEG_INFINITY;
            let mut bad = false;

            for j in start..=i {
                match (l_s[j], h_s[j]) {
                    (Some(l), Some(h)) => {
                        if l < llv {
                            llv = l;
                        }
                        if h > hhv {
                            hhv = h;
                        }
                    }
                    _ => {
                        bad = true;
                        break;
                    }
                }
            }
            if bad {
                out.push(None);
                continue;
            }

            let den = hhv - llv;
            if den.abs() < 1e-12 {
                out.push(Some(0.0));
            } else {
                out.push(Some(100.0 * (c - llv) / den));
            }
        }

        Ok(Value::NumSeries(out))
    }

    fn impl_rsvd(&mut self, args: &[Expr]) -> Result<Value, EvalErr> {
        if args.len() != 5 {
            return Err(EvalErr {
                msg: "RSVD需要五个参数".to_string(),
            });
        }

        let c = self.eval_expr(&args[0])?;
        let h = self.eval_expr(&args[1])?;
        let l = self.eval_expr(&args[2])?;
        let period = self.eval_expr(&args[3])?;
        let len = Value::len_of(&c)
            .max(Value::len_of(&h))
            .max(Value::len_of(&l))
            .max(Value::len_of(&period));
        let c_s = Value::as_num_series(&c, len)?;
        let h_s = Value::as_num_series(&h, len)?;
        let l_s = Value::as_num_series(&l, len)?;
        let max_period = self.dynamic_limit(&args[4], "RSVD")?;
        let period_series = self.dynamic_period_series(period, len, max_period, 1)?;
        let mut out = Vec::with_capacity(len);

        for i in 0..len {
            let Some(period) = period_series[i] else {
                out.push(None);
                continue;
            };
            if i + 1 < period {
                out.push(None);
                continue;
            }
            let start = i + 1 - period;
            let c = match c_s[i] {
                Some(v) => v,
                None => {
                    out.push(None);
                    continue;
                }
            };

            let mut llv = f64::INFINITY;
            let mut hhv = f64::NEG_INFINITY;
            let mut bad = false;
            for j in start..=i {
                match (l_s[j], h_s[j]) {
                    (Some(l), Some(h)) => {
                        if l < llv {
                            llv = l;
                        }
                        if h > hhv {
                            hhv = h;
                        }
                    }
                    _ => {
                        bad = true;
                        break;
                    }
                }
            }
            if bad {
                out.push(None);
                continue;
            }

            let den = hhv - llv;
            if den.abs() < EPS {
                out.push(Some(0.0));
            } else {
                out.push(Some(100.0 * (c - llv) / den));
            }
        }

        Ok(Value::NumSeries(out))
    }

    fn impl_grank(&mut self, args: &[Expr]) -> Result<Value, EvalErr> {
        if args.len() != 2 {
            return Err(EvalErr {
                msg: "RANK需要两个参数".to_string(),
            });
        }

        let v = self.eval_expr(&args[0])?;
        let len = Value::len_of(&v);
        let n_series = Value::as_num_series(&v, len)?;
        let ori_n = Value::as_num(&self.eval_expr(&args[1])?)?;
        let std_n = { if ori_n as i64 <= 0 { 1 } else { ori_n as usize } };
        let mut out = Vec::with_capacity(len);

        for i in 0..len {
            if i + 1 < std_n {
                out.push(None);
                continue;
            }
            let start = i + 1 - std_n;
            let mut count: usize = 1;
            let mut bad = false;
            let curr = match n_series[i] {
                Some(v) => v,
                None => {
                    out.push(None);
                    continue;
                }
            };
            for j in start..i {
                let history = match n_series[j] {
                    Some(v) => v,
                    None => {
                        bad = true;
                        break;
                    }
                };
                if history > curr + EPS || (history - curr).abs() <= EPS {
                    count += 1;
                }
            }
            if bad {
                out.push(None);
            } else {
                out.push(Some(count as f64));
            }
        }
        Ok(Value::NumSeries(out))
    }

    fn impl_rank_topcount(
        &mut self,
        args: &[Expr],
        greater_first: bool,
        fn_name: &str,
    ) -> Result<Value, EvalErr> {
        if args.len() != 4 {
            return Err(EvalErr {
                msg: format!("{fn_name}需要四个参数"),
            });
        }

        let value = self.eval_expr(&args[0])?;
        let cond = self.eval_expr(&args[1])?;
        let len = Value::len_of(&value).max(Value::len_of(&cond));
        let value_series = Value::as_num_series(&value, len)?;
        let cond_series = Value::as_truth_series(&cond, len)?;
        let ori_win = Value::as_num(&self.eval_expr(&args[2])?)?;
        let ori_topn = Value::as_num(&self.eval_expr(&args[3])?)?;
        let std_win = if ori_win as i64 <= 0 {
            1
        } else {
            ori_win as usize
        };
        let std_topn = if ori_topn as i64 <= 0 {
            1
        } else {
            ori_topn as usize
        };

        let mut out = Vec::with_capacity(len);
        for i in 0..len {
            if i + 1 < std_win {
                out.push(None);
                continue;
            }

            let start = i + 1 - std_win;
            let mut rows: Vec<(usize, f64, Truth)> = Vec::with_capacity(std_win);
            let mut has_none = false;
            for j in start..=i {
                match value_series[j] {
                    Some(v) => rows.push((j, v, cond_series[j])),
                    None => {
                        has_none = true;
                        break;
                    }
                }
            }

            if has_none {
                out.push(None);
                continue;
            }

            rows.sort_by(|a, b| {
                let primary = if greater_first {
                    b.1.partial_cmp(&a.1)
                } else {
                    a.1.partial_cmp(&b.1)
                };

                primary
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| b.0.cmp(&a.0))
            });

            let keep_n = rows.len().min(std_topn);
            let selected = &rows[..keep_n];
            out.push(
                selected
                    .iter()
                    .all(|(_, _, truth)| truth.is_known())
                    .then(|| {
                        selected
                            .iter()
                            .filter(|(_, _, truth)| truth.is_true())
                            .count() as f64
                    }),
            );
        }

        Ok(Value::NumSeries(out))
    }

    fn impl_rank_topcountd(
        &mut self,
        args: &[Expr],
        greater_first: bool,
        fn_name: &str,
    ) -> Result<Value, EvalErr> {
        if args.len() != 5 {
            return Err(EvalErr {
                msg: format!("{fn_name}需要五个参数"),
            });
        }

        let value = self.eval_expr(&args[0])?;
        let cond = self.eval_expr(&args[1])?;
        let period = self.eval_expr(&args[2])?;
        let len = Value::len_of(&value)
            .max(Value::len_of(&cond))
            .max(Value::len_of(&period));
        let value_series = Value::as_num_series(&value, len)?;
        let cond_series = Value::as_truth_series(&cond, len)?;
        let max_period = self.dynamic_limit(&args[4], fn_name)?;
        let period_series = self.dynamic_period_series(period, len, max_period, 1)?;
        let ori_topn = Value::as_num(&self.eval_expr(&args[3])?)?;
        let std_topn = if ori_topn as i64 <= 0 {
            1
        } else {
            ori_topn as usize
        };

        let mut out = Vec::with_capacity(len);
        for i in 0..len {
            let Some(period) = period_series[i] else {
                out.push(None);
                continue;
            };
            if i + 1 < period {
                out.push(None);
                continue;
            }

            let start = i + 1 - period;
            let mut rows: Vec<(usize, f64, Truth)> = Vec::with_capacity(period);
            let mut has_none = false;
            for j in start..=i {
                match value_series[j] {
                    Some(v) => rows.push((j, v, cond_series[j])),
                    None => {
                        has_none = true;
                        break;
                    }
                }
            }
            if has_none {
                out.push(None);
                continue;
            }

            rows.sort_by(|a, b| {
                let primary = if greater_first {
                    b.1.partial_cmp(&a.1)
                } else {
                    a.1.partial_cmp(&b.1)
                };

                primary
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| b.0.cmp(&a.0))
            });

            let keep_n = rows.len().min(std_topn);
            let selected = &rows[..keep_n];
            out.push(
                selected
                    .iter()
                    .all(|(_, _, truth)| truth.is_known())
                    .then(|| {
                        selected
                            .iter()
                            .filter(|(_, _, truth)| truth.is_true())
                            .count() as f64
                    }),
            );
        }

        Ok(Value::NumSeries(out))
    }

    fn impl_gtopcount(&mut self, args: &[Expr]) -> Result<Value, EvalErr> {
        self.impl_rank_topcount(args, true, "GTOPCOUNT")
    }

    fn impl_ltopcount(&mut self, args: &[Expr]) -> Result<Value, EvalErr> {
        self.impl_rank_topcount(args, false, "LTOPCOUNT")
    }

    fn impl_gtopcountd(&mut self, args: &[Expr]) -> Result<Value, EvalErr> {
        self.impl_rank_topcountd(args, true, "GTOPCOUNTD")
    }

    fn impl_ltopcountd(&mut self, args: &[Expr]) -> Result<Value, EvalErr> {
        self.impl_rank_topcountd(args, false, "LTOPCOUNTD")
    }

    fn impl_lrank(&mut self, args: &[Expr]) -> Result<Value, EvalErr> {
        if args.len() != 2 {
            return Err(EvalErr {
                msg: "RANK需要两个参数".to_string(),
            });
        }

        let v = self.eval_expr(&args[0])?;
        let len = Value::len_of(&v);
        let n_series = Value::as_num_series(&v, len)?;
        let ori_n = Value::as_num(&self.eval_expr(&args[1])?)?;
        let std_n = { if ori_n as i64 <= 0 { 1 } else { ori_n as usize } };
        let mut out = Vec::with_capacity(len);

        for i in 0..len {
            if i + 1 < std_n {
                out.push(None);
                continue;
            }
            let start = i + 1 - std_n;
            let mut count: usize = 1;
            let mut bad = false;
            let a = match n_series[i] {
                Some(v) => v,
                None => {
                    out.push(None);
                    continue;
                }
            };
            for j in start..i {
                let b = match n_series[j] {
                    Some(v) => v,
                    None => {
                        bad = true;
                        break;
                    }
                };
                if b < a - EPS || (b - a).abs() <= EPS {
                    count += 1;
                }
            }
            if bad {
                out.push(None);
            } else {
                out.push(Some(count as f64));
            }
        }
        Ok(Value::NumSeries(out))
    }

    fn impl_get(&mut self, args: &[Expr]) -> Result<Value, EvalErr> {
        if args.len() != 3 {
            return Err(EvalErr {
                msg: "GET需要三个参数".to_string(),
            });
        }

        let cond = self.eval_expr(&args[0])?;
        let v = self.eval_expr(&args[1])?;
        let len = Value::len_of(&cond).max(Value::len_of(&v));
        let cond_series = Value::as_truth_series(&cond, len)?;
        let v_series = Value::as_num_series(&v, len)?;
        let ori_n = Value::as_num(&self.eval_expr(&args[2])?)?;
        let std_n = { if ori_n as i64 <= 0 { 1 } else { ori_n as usize } };
        let mut out = Vec::with_capacity(len);

        for i in 0..len {
            let start = i.saturating_sub(std_n);
            let mut last = None;
            for j in start..i {
                match cond_series[j] {
                    Truth::True => last = v_series[j],
                    Truth::Unknown => last = None,
                    Truth::False => {}
                }
            }
            out.push(last);
        }

        Ok(Value::NumSeries(out))
    }

    fn impl_getd(&mut self, args: &[Expr]) -> Result<Value, EvalErr> {
        if args.len() != 4 {
            return Err(EvalErr {
                msg: "GETD需要四个参数".to_string(),
            });
        }

        let cond = self.eval_expr(&args[0])?;
        let v = self.eval_expr(&args[1])?;
        let period = self.eval_expr(&args[2])?;
        let len = Value::len_of(&cond)
            .max(Value::len_of(&v))
            .max(Value::len_of(&period));
        let cond_series = Value::as_truth_series(&cond, len)?;
        let v_series = Value::as_num_series(&v, len)?;
        let max_period = self.dynamic_limit(&args[3], "GETD")?;
        let period_series = self.dynamic_period_series(period, len, max_period, 1)?;
        let mut out = Vec::with_capacity(len);

        for i in 0..len {
            let Some(period) = period_series[i] else {
                out.push(None);
                continue;
            };
            let start = i.saturating_sub(period);
            let mut last = None;
            for j in start..i {
                match cond_series[j] {
                    Truth::True => last = v_series[j],
                    Truth::Unknown => last = None,
                    Truth::False => {}
                }
            }
            out.push(last);
        }

        Ok(Value::NumSeries(out))
    }
}

macro_rules! define_expression_functions {
    ($($variant:ident => $name:literal),+ $(,)?) => {
        #[derive(Debug, Clone, Copy)]
        pub(crate) enum ExpressionFunction {
            $($variant),+
        }

        impl ExpressionFunction {
            const ALL: &'static [Self] = &[$(Self::$variant),+];

            pub(crate) fn parse(name: &str) -> Option<Self> {
                match name.trim().to_ascii_uppercase().as_str() {
                    $($name => Some(Self::$variant),)+
                    _ => None,
                }
            }

            const fn name(self) -> &'static str {
                match self {
                    $(Self::$variant => $name),+
                }
            }
        }
    };
}

define_expression_functions! {
    Abs => "ABS",
    Max => "MAX",
    Min => "MIN",
    Div => "DIV",
    Hhv => "HHV",
    Hhvd => "HHVD",
    Llv => "LLV",
    Llvd => "LLVD",
    Count => "COUNT",
    Countd => "COUNTD",
    Exist => "EXIST",
    Existd => "EXISTD",
    Ma => "MA",
    Mad => "MAD",
    Ref => "REF",
    Refd => "REFD",
    Last => "LAST",
    Sum => "SUM",
    Sumd => "SUMD",
    Std => "STD",
    Stdd => "STDD",
    If => "IF",
    Cross => "CROSS",
    Ema => "EMA",
    Sma => "SMA",
    Barslast => "BARSLAST",
    Rsv => "RSV",
    Rsvd => "RSVD",
    Grank => "GRANK",
    Grankd => "GRANKD",
    Gtopcount => "GTOPCOUNT",
    Gtopcountd => "GTOPCOUNTD",
    Ltopcount => "LTOPCOUNT",
    Ltopcountd => "LTOPCOUNTD",
    Lrank => "LRANK",
    Lrankd => "LRANKD",
    Get => "GET",
    Getd => "GETD",
}

pub fn supported_expression_functions() -> impl ExactSizeIterator<Item = &'static str> {
    ExpressionFunction::ALL
        .iter()
        .copied()
        .map(ExpressionFunction::name)
}

pub fn is_supported_expression_function(name: &str) -> bool {
    ExpressionFunction::parse(name).is_some()
}

impl Runtime {
    fn eval_expr(&mut self, expr: &Expr) -> Result<Value, EvalErr> {
        match expr {
            Expr::Number(n) => Ok(Value::Num(*n)),
            Expr::Ident(name) => self.vars.get(name).cloned().ok_or_else(|| EvalErr {
                msg: format!("变量不存在:{}", name),
            }),
            Expr::Call { name, args } => self.eval_call(name, args),
            Expr::Unary { op, rhs } => self.eval_unary(op, rhs),
            Expr::Binary { op, lhs, rhs } => self.eval_binary(op, lhs, rhs),
        }
    }

    fn eval_assign(&mut self, name: &str, value: &Expr) -> Result<Value, EvalErr> {
        let v = match self.eval_expr(value)? {
            Value::NumSeries(series) => Value::SharedNumSeries(Arc::new(series)),
            value => value,
        };
        if let Some(slot) = self.vars.get_mut(name) {
            *slot = v.clone();
        } else {
            self.vars.insert(name.to_string(), v.clone());
        }
        Ok(v)
    }

    fn eval_call(&mut self, name: &str, args: &[Expr]) -> Result<Value, EvalErr> {
        let fn_name = name.to_ascii_uppercase();
        let Some(function) = ExpressionFunction::parse(&fn_name) else {
            return Err(EvalErr {
                msg: format!("未定义函数:{fn_name:?}"),
            });
        };
        match function {
            ExpressionFunction::Abs => self.impl_abs(args),
            ExpressionFunction::Max => self.impl_max(args),
            ExpressionFunction::Min => self.impl_min(args),
            ExpressionFunction::Div => self.impl_div(args),
            ExpressionFunction::Hhv => self.impl_hhv(args),
            ExpressionFunction::Hhvd => self.impl_hhvd(args),
            ExpressionFunction::Llv => self.impl_llv(args),
            ExpressionFunction::Llvd => self.impl_llvd(args),
            ExpressionFunction::Count => self.impl_count(args),
            ExpressionFunction::Countd => self.impl_countd(args),
            ExpressionFunction::Exist => self.impl_exist(args),
            ExpressionFunction::Existd => self.impl_existd(args),
            ExpressionFunction::Ma => self.impl_ma(args),
            ExpressionFunction::Mad => self.impl_window_sumd(args, true),
            ExpressionFunction::Ref => self.impl_ref(args),
            ExpressionFunction::Refd => self.impl_refd(args),
            ExpressionFunction::Last => self.impl_last(args),
            ExpressionFunction::Sum => self.impl_sum(args),
            ExpressionFunction::Sumd => self.impl_window_sumd(args, false),
            ExpressionFunction::Std => self.impl_std(args),
            ExpressionFunction::Stdd => self.impl_stdd(args),
            ExpressionFunction::If => self.impl_if(args),
            ExpressionFunction::Cross => self.impl_cross(args),
            ExpressionFunction::Ema => self.impl_ema(args),
            ExpressionFunction::Sma => self.impl_sma(args),
            ExpressionFunction::Barslast => self.impl_barslast(args),
            ExpressionFunction::Rsv => self.impl_rsv(args),
            ExpressionFunction::Rsvd => self.impl_rsvd(args),
            ExpressionFunction::Grank => self.impl_grank(args),
            ExpressionFunction::Grankd => self.impl_rankd(args, "GRANKD", true),
            ExpressionFunction::Gtopcount => self.impl_gtopcount(args),
            ExpressionFunction::Gtopcountd => self.impl_gtopcountd(args),
            ExpressionFunction::Ltopcount => self.impl_ltopcount(args),
            ExpressionFunction::Ltopcountd => self.impl_ltopcountd(args),
            ExpressionFunction::Lrank => self.impl_lrank(args),
            ExpressionFunction::Lrankd => self.impl_rankd(args, "LRANKD", false),
            ExpressionFunction::Get => self.impl_get(args),
            ExpressionFunction::Getd => self.impl_getd(args),
        }
    }

    fn eval_unary(&mut self, op: &UnaryOp, rhs: &Expr) -> Result<Value, EvalErr> {
        let value = self.eval_expr(rhs)?;
        if let Value::Num(number) = value {
            return Ok(match op {
                UnaryOp::Neg => Value::Num(-number),
                UnaryOp::Not => Value::Bool(!Truth::from_num(Some(number))),
            });
        }
        if let Value::Bool(truth) = value {
            return Ok(match op {
                UnaryOp::Neg => truth
                    .as_num()
                    .map(|n| Value::Num(-n))
                    .unwrap_or(Value::Bool(Truth::Unknown)),
                UnaryOp::Not => Value::Bool(!truth),
            });
        }
        if let Value::BoolSeries(series) = value {
            return Ok(match op {
                UnaryOp::Neg => Value::NumSeries(
                    series
                        .into_iter()
                        .map(|truth| truth.as_num().map(|n| -n))
                        .collect(),
                ),
                UnaryOp::Not => Value::BoolSeries(series.into_iter().map(|truth| !truth).collect()),
            });
        }
        let series = Value::as_num_series(&value, Value::len_of(&value))?;
        Ok(match op {
            UnaryOp::Neg => {
                Value::NumSeries(series.iter().map(|value| value.map(|n| -n)).collect())
            }
            UnaryOp::Not => Value::BoolSeries(
                series
                    .iter()
                    .map(|value| !Truth::from_num(*value))
                    .collect(),
            ),
        })
    }

    fn eval_binary(&mut self, op: &BinaryOp, lhs: &Expr, rhs: &Expr) -> Result<Value, EvalErr> {
        let lhs = self.eval_expr(lhs)?;
        let rhs = self.eval_expr(rhs)?;
        if matches!(lhs, Value::Num(_) | Value::Bool(_))
            && matches!(rhs, Value::Num(_) | Value::Bool(_))
        {
            let left = match lhs {
                Value::Num(n) => DayValue::Num(Some(n)),
                Value::Bool(b) => DayValue::Bool(b),
                _ => unreachable!(),
            };
            let right = match rhs {
                Value::Num(n) => DayValue::Num(Some(n)),
                Value::Bool(b) => DayValue::Bool(b),
                _ => unreachable!(),
            };
            return Ok(match eval_day_binary(op, left, right) {
                DayValue::Num(Some(n)) => Value::Num(n),
                DayValue::Bool(b) => Value::Bool(b),
                _ => Value::Bool(Truth::Unknown),
            });
        }
        let len = Value::len_of(&lhs).max(Value::len_of(&rhs));
        if matches!(op, BinaryOp::And | BinaryOp::Or) {
            let left = Value::as_truth_series(&lhs, len)?;
            let right = Value::as_truth_series(&rhs, len)?;
            return Ok(Value::BoolSeries(
                left.iter()
                    .zip(right.iter())
                    .map(|(lhs, rhs)| {
                        if matches!(op, BinaryOp::And) {
                            lhs.and(*rhs)
                        } else {
                            lhs.or(*rhs)
                        }
                    })
                    .collect(),
            ));
        }
        let left = Value::as_num_series(&lhs, len)?;
        let right = Value::as_num_series(&rhs, len)?;
        let evaluated = left
            .iter()
            .zip(right.iter())
            .map(|(lhs, rhs)| eval_day_binary(op, DayValue::Num(*lhs), DayValue::Num(*rhs)));
        Ok(
            if matches!(
                op,
                BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div
            ) {
                Value::NumSeries(evaluated.map(day_value_as_num).collect())
            } else {
                Value::BoolSeries(evaluated.map(day_value_as_truth).collect())
            },
        )
    }

    fn eval_stmt(&mut self, stmt: &Stmt) -> Result<Value, EvalErr> {
        match stmt {
            Stmt::Expr(e) => self.eval_expr(e),
            Stmt::Assign { name, value } => self.eval_assign(name, value),
        }
    }

    fn day_value(&self, name: &str, day_index: usize) -> Result<DayValue, EvalErr> {
        let value = self.vars.get(name).ok_or_else(|| EvalErr {
            msg: format!("变量不存在:{name}"),
        })?;
        match value {
            Value::Num(value) => Ok(DayValue::Num(Some(*value))),
            Value::Bool(value) => Ok(DayValue::Bool(*value)),
            Value::NumSeries(series) => Ok(DayValue::Num(series.get(day_index).copied().flatten())),
            Value::NumWindow { values, range } => Ok(DayValue::Num(
                values[range.clone()].get(day_index).copied().flatten(),
            )),
            Value::SharedNumSeries(series) => {
                Ok(DayValue::Num(series.get(day_index).copied().flatten()))
            }
            Value::BoolSeries(series) => Ok(DayValue::Bool(
                series.get(day_index).copied().unwrap_or(Truth::Unknown),
            )),
        }
    }

    fn eval_expr_bool_at(
        &self,
        expr: &Expr,
        locals: &HashMap<String, DayValue>,
        day_index: usize,
    ) -> Result<Option<DayValue>, EvalErr> {
        match expr {
            Expr::Number(value) => Ok(Some(DayValue::Num(Some(*value)))),
            Expr::Ident(name) => {
                if let Some(value) = locals
                    .get(name)
                    .copied()
                    .filter(|value| !matches!(value, DayValue::Unset))
                {
                    Ok(Some(value))
                } else {
                    self.day_value(name, day_index).map(Some)
                }
            }
            Expr::Call { .. } => Ok(None),
            Expr::Unary { op, rhs } => {
                let Some(rhs) = self.eval_expr_bool_at(rhs, locals, day_index)? else {
                    return Ok(None);
                };
                Ok(Some(match op {
                    UnaryOp::Neg => DayValue::Num(day_value_as_num(rhs).map(|value| -value)),
                    UnaryOp::Not => DayValue::Bool(!day_value_as_truth(rhs)),
                }))
            }
            Expr::Binary { op, lhs, rhs } => {
                let Some(lhs) = self.eval_expr_bool_at(lhs, locals, day_index)? else {
                    return Ok(None);
                };
                let Some(rhs) = self.eval_expr_bool_at(rhs, locals, day_index)? else {
                    return Ok(None);
                };
                let value = eval_day_binary(op, lhs, rhs);
                Ok(Some(value))
            }
        }
    }

    pub(crate) fn eval_program_scalar_at(
        &self,
        stmts: &Stmts,
        day_index: usize,
        locals: &mut HashMap<String, DayValue>,
    ) -> Result<Option<DayValue>, EvalErr> {
        let mut last = DayValue::Num(Some(0.0));
        for stmt in &stmts.item {
            let (name, expr) = match stmt {
                Stmt::Assign { name, value } => (Some(name), value),
                Stmt::Expr(expr) => (None, expr),
            };
            let Some(value) = self.eval_expr_bool_at(expr, locals, day_index)? else {
                return Ok(None);
            };
            if let Some(name) = name {
                if let Some(slot) = locals.get_mut(name) {
                    *slot = value;
                } else {
                    locals.insert(name.clone(), value);
                }
            }
            last = value;
        }
        Ok(Some(last))
    }

    pub fn eval_program_bool_at(
        &self,
        stmts: &Stmts,
        day_index: usize,
    ) -> Result<Option<bool>, EvalErr> {
        self.eval_program_scalar_at(stmts, day_index, &mut HashMap::new())
            .map(|value| value.map(|value| day_value_as_truth(value).is_true()))
    }

    pub fn eval_program(&mut self, stmts: &Stmts) -> Result<Value, EvalErr> {
        let mut last = Value::Num(0.0);
        for stmt in &stmts.item {
            last = self.eval_stmt(stmt)?;
        }
        Ok(last)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Num(f64),
    NumSeries(Vec<Option<f64>>),
    SharedNumSeries(Arc<Vec<Option<f64>>>),
    NumWindow {
        values: Arc<Vec<Option<f64>>>,
        range: Range<usize>,
    },
    Bool(Truth),
    BoolSeries(Vec<Truth>),
}

impl Value {
    /// Share immutable expression inputs and assignment results without copying on reads.
    pub fn into_shared(self) -> Self {
        match self {
            Self::NumSeries(series) => Self::SharedNumSeries(Arc::new(series)),
            Self::BoolSeries(series) => {
                Self::SharedNumSeries(Arc::new(series.into_iter().map(Truth::as_num).collect()))
            }
            other => other,
        }
    }

    pub fn len_of(v: &Value) -> usize {
        match v {
            Value::Num(_) => 1,
            Value::Bool(_) => 1,
            Value::NumSeries(n) => n.len(),
            Value::SharedNumSeries(n) => n.len(),
            Value::NumWindow { range, .. } => range.len(),
            Value::BoolSeries(b) => b.len(),
        }
    }

    pub fn as_num(v: &Value) -> Result<f64, EvalErr> {
        match v {
            Value::Num(n) => Ok(*n),
            Value::Bool(b) => b.as_num().ok_or_else(|| EvalErr {
                msg: "布尔值未知".into(),
            }),
            Value::NumSeries(_) | Value::SharedNumSeries(_) | Value::NumWindow { .. } => {
                Err(EvalErr {
                    msg: "需要标量数字，但拿到数值序列，可用LAST函数转换".to_string(),
                })
            }
            Value::BoolSeries(_) => Err(EvalErr {
                msg: "需要标量数字，但拿到布尔序列".to_string(),
            }),
        }
    }

    pub fn as_bool(v: &Value) -> Result<bool, EvalErr> {
        match v {
            Value::Num(n) => Ok(Truth::from_num(Some(*n)).is_true()),
            Value::Bool(b) => Ok(b.is_true()),
            Value::NumSeries(_) | Value::SharedNumSeries(_) | Value::NumWindow { .. } => {
                Err(EvalErr {
                    msg: "需要布尔，但拿到数值序列".to_string(),
                })
            }
            Value::BoolSeries(_) => Err(EvalErr {
                msg: "需要布尔，但拿到布尔序列，可用LAST函数转换".to_string(),
            }),
        }
    }

    pub fn as_num_series(v: &Value, len: usize) -> Result<Cow<'_, [Option<f64>]>, EvalErr> {
        let series = match v {
            Value::Num(n) => return Ok(Cow::Owned(vec![Some(*n); len])),
            Value::Bool(b) => return Ok(Cow::Owned(vec![b.as_num(); len])),
            Value::NumSeries(ns) => ns.as_slice(),
            Value::SharedNumSeries(ns) => ns.as_slice(),
            Value::NumWindow { values, range } => &values[range.clone()],
            Value::BoolSeries(bs) => {
                if bs.len() != len {
                    return Err(EvalErr {
                        msg: "布尔序列长度不对".to_string(),
                    });
                }
                return Ok(Cow::Owned(bs.iter().copied().map(Truth::as_num).collect()));
            }
        };
        if series.len() != len {
            return Err(EvalErr {
                msg: "数值序列长度不对".to_string(),
            });
        }
        Ok(Cow::Borrowed(series))
    }

    pub fn into_num_series(self, len: usize) -> Result<Vec<Option<f64>>, EvalErr> {
        match self {
            Value::Num(n) => Ok(vec![Some(n); len]),
            Value::Bool(b) => Ok(vec![b.as_num(); len]),
            Value::NumSeries(ns) => {
                if ns.len() == len {
                    Ok(ns)
                } else {
                    Err(EvalErr {
                        msg: "数值序列长度不对".to_string(),
                    })
                }
            }
            Value::SharedNumSeries(ns) => {
                if ns.len() == len {
                    Ok(Arc::try_unwrap(ns).unwrap_or_else(|ns| ns.as_ref().clone()))
                } else {
                    Err(EvalErr {
                        msg: "数值序列长度不对".to_string(),
                    })
                }
            }
            Value::NumWindow { values, range } => {
                if range.len() == len {
                    Ok(values[range].to_vec())
                } else {
                    Err(EvalErr {
                        msg: "数值序列长度不对".to_string(),
                    })
                }
            }
            Value::BoolSeries(bs) => {
                if bs.len() == len {
                    Ok(bs.into_iter().map(Truth::as_num).collect())
                } else {
                    Err(EvalErr {
                        msg: "布尔序列长度不对".to_string(),
                    })
                }
            }
        }
    }

    pub fn into_truth_series(self, len: usize) -> Result<Vec<Truth>, EvalErr> {
        if let Self::BoolSeries(series) = self {
            if series.len() != len {
                return Err(EvalErr {
                    msg: "布尔序列长度不对".into(),
                });
            }
            Ok(series)
        } else {
            Self::as_truth_series(&self, len).map(Cow::into_owned)
        }
    }

    pub fn into_bool_series(self, len: usize) -> Result<Vec<bool>, EvalErr> {
        Self::as_truth_series(&self, len)
            .map(|series| series.iter().map(|truth| truth.is_true()).collect())
    }

    /// Only final matching boundaries may collapse Unknown to false.
    pub fn as_bool_series(v: &Value, len: usize) -> Result<Cow<'_, [bool]>, EvalErr> {
        Ok(Cow::Owned(
            Self::as_truth_series(v, len)?
                .iter()
                .map(|truth| truth.is_true())
                .collect(),
        ))
    }

    pub fn as_truth_series(v: &Value, len: usize) -> Result<Cow<'_, [Truth]>, EvalErr> {
        match v {
            Value::Num(n) => Ok(Cow::Owned(vec![Truth::from_num(Some(*n)); len])),
            Value::Bool(b) => Ok(Cow::Owned(vec![*b; len])),
            Value::BoolSeries(series) => {
                if series.len() != len {
                    return Err(EvalErr {
                        msg: "布尔序列长度不对".into(),
                    });
                }
                Ok(Cow::Borrowed(series))
            }
            _ => Ok(Cow::Owned(
                Self::as_num_series(v, len)?
                    .iter()
                    .copied()
                    .map(Truth::from_num)
                    .collect(),
            )),
        }
    }
}

#[test]
fn call_test() {
    use crate::expr::parser::{Parser, lex_all};

    let expr = "BARSLAST(C > 2);";
    let toks = lex_all(expr);
    let mut p = Parser::new(toks);
    let stmts = p.parse_main().expect("parse failed");
    let mut rt = Runtime::default();

    rt.vars.insert(
        "C".to_string(),
        Value::NumSeries(vec![Some(1.0), Some(2.0), Some(3.0), Some(4.0), Some(1.0)]),
    );
    let out = rt.eval_program(&stmts).expect("eval failed");
    println!("{out:?}");
}

#[test]
fn scalar_binary_keeps_scalar() {
    use crate::expr::parser::{Parser, lex_all};

    let expr = "WINDOW := 10 + 10; REF(C, WINDOW);";
    let toks = lex_all(expr);
    let mut p = Parser::new(toks);
    let stmts = p.parse_main().expect("parse failed");
    let mut rt = Runtime::default();

    rt.vars.insert(
        "C".to_string(),
        Value::NumSeries((1..=25).map(|x| Some(x as f64)).collect()),
    );

    let out = rt.eval_program(&stmts).expect("eval failed");
    match out {
        Value::NumSeries(ns) => {
            assert_eq!(ns.len(), 25);
            assert_eq!(ns[19], None);
            assert_eq!(ns[20], Some(1.0));
            assert_eq!(ns[24], Some(5.0));
        }
        other => panic!("unexpected result: {other:?}"),
    }
}

#[test]
fn eval_program_bool_at_matches_full_series_for_call_free_program() {
    use crate::expr::parser::{Parser, lex_all};

    let expr = "spread := C - O; RATEC > 1 AND spread > 0 AND MAIN_CHIP_RATIO >= 0.5";
    let toks = lex_all(expr);
    let mut p = Parser::new(toks);
    let stmts = p.parse_main().expect("parse failed");

    let mut rt = Runtime::default();
    rt.vars.insert(
        "C".to_string(),
        Value::NumSeries(vec![Some(10.0), Some(11.0), Some(12.0), Some(13.0)]),
    );
    rt.vars.insert(
        "O".to_string(),
        Value::NumSeries(vec![Some(10.1), Some(10.0), Some(11.0), Some(12.0)]),
    );
    rt.vars.insert(
        "RATEC".to_string(),
        Value::NumSeries(vec![Some(0.5), Some(1.5), None, Some(2.0)]),
    );
    rt.vars.insert(
        "MAIN_CHIP_RATIO".to_string(),
        Value::NumSeries(vec![Some(0.4), Some(0.6), Some(0.8), Some(0.3)]),
    );

    for day_index in 0..4 {
        let fast = rt
            .eval_program_bool_at(&stmts, day_index)
            .expect("fast eval")
            .expect("call-free expression should use fast path");
        let mut full_rt = rt.clone();
        let full_value = full_rt.eval_program(&stmts).expect("full eval");
        let full = Value::as_bool_series(&full_value, 4).expect("bool series")[day_index];
        assert_eq!(fast, full, "day_index={day_index}");
    }
}

#[test]
fn eval_program_bool_at_defers_programs_with_calls() {
    use crate::expr::parser::{Parser, lex_all};

    let toks = lex_all("C > MA(C, 3)");
    let mut p = Parser::new(toks);
    let stmts = p.parse_main().expect("parse failed");

    let mut rt = Runtime::default();
    rt.vars.insert(
        "C".to_string(),
        Value::NumSeries(vec![Some(1.0), Some(2.0), Some(3.0), Some(4.0)]),
    );

    assert_eq!(rt.eval_program_bool_at(&stmts, 3).expect("fast eval"), None);
}

#[test]
fn gtopcount_anchors_to_current_window() {
    use crate::expr::parser::{Parser, lex_all};

    let expr = "GTOPCOUNT(V, C > REF(C, 1), 5, 3);";
    let toks = lex_all(expr);
    let mut p = Parser::new(toks);
    let stmts = p.parse_main().expect("parse failed");
    let mut rt = Runtime::default();

    rt.vars.insert(
        "V".to_string(),
        Value::NumSeries(vec![Some(1.0), Some(5.0), Some(2.0), Some(4.0), Some(3.0)]),
    );
    rt.vars.insert(
        "C".to_string(),
        Value::NumSeries(vec![Some(1.0), Some(2.0), Some(1.0), Some(3.0), Some(2.0)]),
    );

    let out = rt.eval_program(&stmts).expect("eval failed");
    match out {
        Value::NumSeries(ns) => {
            assert_eq!(ns[0], None);
            assert_eq!(ns[1], None);
            assert_eq!(ns[2], None);
            assert_eq!(ns[3], None);
            assert_eq!(ns[4], Some(2.0));
        }
        other => panic!("unexpected result: {other:?}"),
    }
}

#[test]
fn ltopcount_anchors_to_current_window() {
    use crate::expr::parser::{Parser, lex_all};

    let expr = "LTOPCOUNT(V, C > REF(C, 1), 5, 3);";
    let toks = lex_all(expr);
    let mut p = Parser::new(toks);
    let stmts = p.parse_main().expect("parse failed");
    let mut rt = Runtime::default();

    rt.vars.insert(
        "V".to_string(),
        Value::NumSeries(vec![Some(5.0), Some(1.0), Some(4.0), Some(2.0), Some(3.0)]),
    );
    rt.vars.insert(
        "C".to_string(),
        Value::NumSeries(vec![Some(1.0), Some(2.0), Some(1.0), Some(3.0), Some(4.0)]),
    );

    let out = rt.eval_program(&stmts).expect("eval failed");
    match out {
        Value::NumSeries(ns) => {
            assert_eq!(ns[0], None);
            assert_eq!(ns[1], None);
            assert_eq!(ns[2], None);
            assert_eq!(ns[3], None);
            assert_eq!(ns[4], Some(3.0));
        }
        other => panic!("unexpected result: {other:?}"),
    }
}

#[test]
fn repeated_ref_comparisons_keep_the_same_reference_low_per_bar() {
    use crate::expr::parser::{Parser, lex_all};

    let expr = "C >= REF(L, 4) AND REF(C, 1) >= REF(L, 4) AND REF(C, 2) >= REF(L, 4);";
    let toks = lex_all(expr);
    let mut p = Parser::new(toks);
    let stmts = p.parse_main().expect("parse failed");
    let mut rt = Runtime::default();

    rt.vars.insert(
        "L".to_string(),
        Value::NumSeries(vec![
            Some(10.0),
            Some(9.0),
            Some(8.0),
            Some(7.0),
            Some(6.0),
            Some(5.0),
            Some(4.0),
        ]),
    );
    rt.vars.insert(
        "C".to_string(),
        Value::NumSeries(vec![
            Some(10.0),
            Some(9.0),
            Some(8.0),
            Some(9.0),
            Some(8.0),
            Some(8.0),
            Some(8.0),
        ]),
    );

    let out = rt.eval_program(&stmts).expect("eval failed");
    match out {
        Value::BoolSeries(bs) => {
            assert_eq!(bs.len(), 7);
            assert!(!bs[5].is_true());
            assert!(bs[6].is_true());
        }
        other => panic!("unexpected result: {other:?}"),
    }
}

#[test]
fn last_returns_latest_or_offset_value_as_scalar() {
    use crate::expr::parser::{Parser, lex_all};

    let expr = "A := LAST(C, 0); B := LAST(C, 2); A - B;";
    let toks = lex_all(expr);
    let mut p = Parser::new(toks);
    let stmts = p.parse_main().expect("parse failed");
    let mut rt = Runtime::default();

    rt.vars.insert(
        "C".to_string(),
        Value::NumSeries(vec![Some(1.0), Some(2.0), Some(3.0), Some(4.0), Some(5.0)]),
    );

    let out = rt.eval_program(&stmts).expect("eval failed");
    assert_eq!(out, Value::Num(2.0));
}

#[test]
fn last_supports_bool_series() {
    use crate::expr::parser::{Parser, lex_all};

    let expr = "LAST(C > 2, 1);";
    let toks = lex_all(expr);
    let mut p = Parser::new(toks);
    let stmts = p.parse_main().expect("parse failed");
    let mut rt = Runtime::default();

    rt.vars.insert(
        "C".to_string(),
        Value::NumSeries(vec![Some(1.0), Some(2.0), Some(3.0), Some(4.0), Some(5.0)]),
    );

    let out = rt.eval_program(&stmts).expect("eval failed");
    assert_eq!(out, Value::Bool(Truth::True));
}

#[test]
fn in_range_supports_inclusive_and_exclusive_bounds() {
    use crate::expr::parser::{Parser, lex_all};

    let expr = "A := C IN [2, 4]; B := C IN (2, 4); A AND NOT(B);";
    let toks = lex_all(expr);
    let mut p = Parser::new(toks);
    let stmts = p.parse_main().expect("parse failed");
    let mut rt = Runtime::default();

    rt.vars.insert(
        "C".to_string(),
        Value::NumSeries(vec![Some(1.0), Some(2.0), Some(3.0), Some(4.0), Some(5.0)]),
    );

    let out = rt.eval_program(&stmts).expect("eval failed");
    assert_eq!(
        out,
        Value::BoolSeries(vec![
            Truth::False,
            Truth::True,
            Truth::False,
            Truth::True,
            Truth::False
        ])
    );
}

#[test]
fn in_range_accepts_expression_bounds() {
    use crate::expr::parser::{Parser, lex_all};

    let expr = "C IN [MA(C, 2), HHV(C, 3)]";
    let toks = lex_all(expr);
    let mut p = Parser::new(toks);
    let stmts = p.parse_main().expect("parse failed");
    let mut rt = Runtime::default();

    rt.vars.insert(
        "C".to_string(),
        Value::NumSeries(vec![Some(1.0), Some(2.0), Some(3.0), Some(2.0), Some(4.0)]),
    );

    let out = rt.eval_program(&stmts).expect("eval failed");
    assert_eq!(
        out,
        Value::BoolSeries(vec![
            Truth::Unknown,
            Truth::Unknown,
            Truth::True,
            Truth::False,
            Truth::True
        ])
    );
}

#[test]
fn exist_returns_true_when_condition_hit_within_window() {
    use crate::expr::parser::{Parser, lex_all};

    let expr = "EXIST(C > 2, 3);";
    let toks = lex_all(expr);
    let mut p = Parser::new(toks);
    let stmts = p.parse_main().expect("parse failed");
    let mut rt = Runtime::default();

    rt.vars.insert(
        "C".to_string(),
        Value::NumSeries(vec![
            Some(1.0),
            Some(3.0),
            Some(1.0),
            Some(1.0),
            Some(4.0),
            Some(1.0),
        ]),
    );

    let out = rt.eval_program(&stmts).expect("eval failed");
    assert_eq!(
        out,
        Value::BoolSeries(vec![
            Truth::Unknown,
            Truth::True,
            Truth::True,
            Truth::True,
            Truth::True,
            Truth::True
        ])
    );
}

#[test]
fn refd_uses_per_bar_offsets_and_keeps_undefined_periods_empty() {
    use crate::expr::parser::{Parser, lex_all};

    let expr = "GAP := REF(BARSLAST(C > 3), 1); REFD(H, GAP + 1, 5);";
    let toks = lex_all(expr);
    let mut p = Parser::new(toks);
    let stmts = p.parse_main().expect("parse failed");
    let mut rt = Runtime::default();

    rt.vars.insert(
        "C".to_string(),
        Value::NumSeries(vec![Some(1.0), Some(2.0), Some(4.0), Some(1.0), Some(1.0)]),
    );
    rt.vars.insert(
        "H".to_string(),
        Value::NumSeries(vec![
            Some(10.0),
            Some(20.0),
            Some(30.0),
            Some(40.0),
            Some(50.0),
        ]),
    );

    let out = rt.eval_program(&stmts).expect("eval failed");
    assert_eq!(
        out,
        Value::NumSeries(vec![None, None, None, Some(30.0), Some(30.0)])
    );
}

#[test]
fn countd_returns_none_when_dynamic_window_exceeds_runtime_cap() {
    use crate::expr::parser::{Parser, lex_all};

    let expr = "COUNTD(C > 0, N, 3);";
    let toks = lex_all(expr);
    let mut p = Parser::new(toks);
    let stmts = p.parse_main().expect("parse failed");
    let mut rt = Runtime::default();

    rt.vars.insert(
        "C".to_string(),
        Value::NumSeries(vec![Some(1.0), Some(1.0), Some(1.0), Some(1.0), Some(1.0)]),
    );
    rt.vars.insert(
        "N".to_string(),
        Value::NumSeries(vec![
            Some(1.0),
            Some(2.0),
            Some(3.0),
            Some(4.0),
            Some(f64::NAN),
        ]),
    );

    let out = rt.eval_program(&stmts).expect("eval failed");
    assert_eq!(
        out,
        Value::NumSeries(vec![Some(1.0), Some(2.0), Some(3.0), None, None])
    );
}

#[test]
fn existd_preserves_unknown_when_window_is_invalid() {
    use crate::expr::parser::{Parser, lex_all};

    let expr = "EXISTD(C > 2, N, 3);";
    let toks = lex_all(expr);
    let mut p = Parser::new(toks);
    let stmts = p.parse_main().expect("parse failed");
    let mut rt = Runtime::default();

    rt.vars.insert(
        "C".to_string(),
        Value::NumSeries(vec![
            Some(1.0),
            Some(3.0),
            Some(1.0),
            Some(1.0),
            Some(4.0),
            Some(1.0),
        ]),
    );
    rt.vars.insert(
        "N".to_string(),
        Value::NumSeries(vec![
            Some(1.0),
            Some(1.0),
            Some(2.0),
            Some(3.0),
            Some(4.0),
            Some(f64::NAN),
        ]),
    );

    let out = rt.eval_program(&stmts).expect("eval failed");
    assert_eq!(
        out,
        Value::BoolSeries(vec![
            Truth::False,
            Truth::True,
            Truth::True,
            Truth::True,
            Truth::Unknown,
            Truth::Unknown
        ])
    );
}

#[test]
fn hhvd_returns_none_when_dynamic_window_exceeds_runtime_cap() {
    use crate::expr::parser::{Parser, lex_all};

    let expr = "HHVD(C, N, 3);";
    let toks = lex_all(expr);
    let mut p = Parser::new(toks);
    let stmts = p.parse_main().expect("parse failed");
    let mut rt = Runtime::default();

    rt.vars.insert(
        "C".to_string(),
        Value::NumSeries(vec![Some(1.0), Some(5.0), Some(3.0), Some(4.0), Some(2.0)]),
    );
    rt.vars.insert(
        "N".to_string(),
        Value::NumSeries(vec![Some(1.0), Some(2.0), Some(3.0), Some(2.0), Some(4.0)]),
    );

    let out = rt.eval_program(&stmts).expect("eval failed");
    assert_eq!(
        out,
        Value::NumSeries(vec![Some(1.0), Some(5.0), Some(5.0), Some(4.0), None])
    );
}

#[test]
fn dynamic_window_exceeding_cap_does_not_match_comparisons() {
    use crate::expr::parser::{Parser, lex_all};

    let expr = "V := HHVD(C, N, 3); (V > 0) OR (V < 0);";
    let toks = lex_all(expr);
    let mut p = Parser::new(toks);
    let stmts = p.parse_main().expect("parse failed");
    let mut rt = Runtime::default();

    rt.vars.insert(
        "C".to_string(),
        Value::NumSeries(vec![Some(1.0), Some(5.0), Some(3.0), Some(4.0), Some(2.0)]),
    );
    rt.vars.insert(
        "N".to_string(),
        Value::NumSeries(vec![Some(1.0), Some(2.0), Some(3.0), Some(2.0), Some(4.0)]),
    );

    let out = rt.eval_program(&stmts).expect("eval failed");
    assert_eq!(
        out,
        Value::BoolSeries(vec![
            Truth::True,
            Truth::True,
            Truth::True,
            Truth::True,
            Truth::Unknown
        ])
    );
}

#[test]
fn dynamic_limit_rejects_fractional_upper_bound() {
    use crate::expr::parser::{Parser, lex_all};

    let expr = "HHVD(C, N, 0.5);";
    let toks = lex_all(expr);
    let mut p = Parser::new(toks);
    let stmts = p.parse_main().expect("parse failed");
    let mut rt = Runtime::default();

    rt.vars.insert(
        "C".to_string(),
        Value::NumSeries(vec![Some(1.0), Some(5.0), Some(3.0)]),
    );
    rt.vars.insert(
        "N".to_string(),
        Value::NumSeries(vec![Some(1.0), Some(2.0), Some(3.0)]),
    );

    let err = rt
        .eval_program(&stmts)
        .expect_err("fractional cap should fail");
    assert_eq!(err.msg, "HHVD动态周期上限必须是正整数");
}

#[test]
fn unknown_survives_negation_functions_and_scalar_evaluation() {
    use crate::expr::validation::parse_expression_program;
    use Truth::{False as F, True as T, Unknown as U};
    let base = Runtime {
        vars: HashMap::from([(
            "C".into(),
            Value::NumSeries(vec![None, Some(9.0), Some(12.0)]),
        )]),
    };
    for (text, expected, scalar) in [
        ("!(C > 10)", [U, T, F], true),
        ("X := C > 10; !X", [U, T, F], true),
        ("(C > 10) OR 1", [T, T, T], true),
        ("(C > 10) AND 0", [F, F, F], true),
        ("!(C > 10 OR 0)", [U, T, F], true),
        ("IF(C > 10,1,2) > 0", [U, T, T], false),
        ("IF(0,C,1) > 0", [T, T, T], false),
        ("!(COUNT(C > 10,2) > 0)", [U, U, F], false),
        ("!EXIST(C > 10,2)", [U, U, F], false),
        ("!CROSS(C,10)", [U, U, F], false),
        ("!(BARSLAST(C > 10) > 0)", [U, U, T], false),
        ("!(GET(C > 10,C,2) > 0)", [U, U, U], false),
        ("!(HHV(C,2) > 10)", [U, U, F], false),
        ("!(MAX(C,10) > 0)", [U, F, F], false),
    ] {
        let program = parse_expression_program(text).unwrap();
        let value = base.clone().eval_program(&program).unwrap();
        assert_eq!(
            Value::as_truth_series(&value, 3).unwrap().as_ref(),
            expected,
            "{text}"
        );
        let matches = Value::as_bool_series(&value, 3).unwrap();
        for day in 0..3 {
            assert_eq!(matches[day], expected[day].is_true(), "{text} day {day}");
            if scalar {
                let actual = base
                    .eval_program_scalar_at(&program, day, &mut HashMap::new())
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    day_value_as_truth(actual),
                    expected[day],
                    "{text} scalar day {day}"
                );
            }
        }
    }
}
