use crate::expr::parser::{BinaryOp, Expr, Stmt, Stmts, UnaryOp};
use std::collections::HashMap;
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

fn to_bool(v: f64) -> bool {
    v != 0.0
}

fn to_num(b: bool) -> f64 {
    if b { 1.0 } else { 0.0 }
}

#[derive(Clone, Copy)]
enum DayValue {
    Num(Option<f64>),
    Bool(bool),
}

fn day_value_as_num(value: DayValue) -> Option<f64> {
    match value {
        DayValue::Num(value) => value,
        DayValue::Bool(value) => Some(to_num(value)),
    }
}

fn day_value_as_bool(value: DayValue) -> bool {
    match value {
        DayValue::Num(Some(value)) => to_bool(value),
        DayValue::Num(None) => false,
        DayValue::Bool(value) => value,
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
            .into_iter()
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
    ) -> Result<(Vec<Option<f64>>, Vec<Option<usize>>, usize), EvalErr> {
        if args.len() != 3 {
            return Err(EvalErr {
                msg: format!("{fn_name}需要三个参数"),
            });
        }

        let value = self.eval_expr(&args[0])?;
        let period = self.eval_expr(&args[1])?;
        let len = Value::len_of(&value).max(Value::len_of(&period));
        let value_series = Value::as_num_series(&value, len)?;
        let max_period = self.dynamic_limit(&args[2], fn_name)?;
        let period_series = self.dynamic_period_series(period, len, max_period, 1)?;

        Ok((value_series, period_series, len))
    }

    fn impl_extremed(
        &mut self,
        args: &[Expr],
        fn_name: &str,
        greater: bool,
    ) -> Result<Value, EvalErr> {
        let (num_series, period_series, len) = self.dynamic_num_window_args(args, fn_name)?;
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

            for value in num_series.iter().take(i + 1).skip(start) {
                if let Some(value) = value {
                    if (greater && *value > best) || (!greater && *value < best) {
                        best = *value;
                    }
                }
            }
            out.push(Some(best));
        }

        Ok(Value::NumSeries(out))
    }

    fn impl_window_sumd(&mut self, args: &[Expr], average: bool) -> Result<Value, EvalErr> {
        let (num_series, period_series, len) =
            self.dynamic_num_window_args(args, if average { "MAD" } else { "SUMD" })?;
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
        let (num_series, period_series, len) = self.dynamic_num_window_args(args, "STDD")?;
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
        let (num_series, period_series, len) = self.dynamic_num_window_args(args, fn_name)?;
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
        // for i in 0..len {
        //     match vs[i] {
        //         Some(a) => out.push(Some(a.abs())),
        //         None => out.push(None),
        //     }
        // }
        for x in vs.iter().take(len) {
            out.push(x.map(|x| x.abs()));
        }
        Ok(Value::NumSeries(out))
    }

    fn impl_countd(&mut self, args: &[Expr]) -> Result<Value, EvalErr> {
        if args.len() != 3 {
            return Err(EvalErr {
                msg: "COUNTD需要三个参数".to_string(),
            });
        }

        let cond = self.eval_expr(&args[0])?;
        let period = self.eval_expr(&args[1])?;
        let len = Value::len_of(&cond).max(Value::len_of(&period));
        let cond_series = Value::as_bool_series(&cond, len)?;
        let max_period = self.dynamic_limit(&args[2], "COUNTD")?;
        let period_series = self.dynamic_period_series(period, len, max_period, 1)?;

        let mut prefix = Vec::with_capacity(len + 1);
        prefix.push(0usize);
        for hit in &cond_series {
            let next = prefix.last().copied().unwrap_or(0) + usize::from(*hit);
            prefix.push(next);
        }

        let mut out = Vec::with_capacity(len);
        for i in 0..len {
            let Some(period) = period_series[i] else {
                out.push(None);
                continue;
            };
            let start = (i + 1).saturating_sub(period);
            out.push(Some((prefix[i + 1] - prefix[start]) as f64));
        }

        Ok(Value::NumSeries(out))
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
        if args.len() != 2 {
            return Err(EvalErr {
                msg: "COUNT需要两个参数".to_string(),
            });
        }

        let cond = self.eval_expr(&args[0])?;
        let len = Value::len_of(&cond);
        let ori_n = Value::as_num(&self.eval_expr(&args[1])?)?;
        let std_n = { if ori_n as i64 <= 0 { 1 } else { ori_n as usize } };
        let cond_series = Value::as_bool_series(&cond, len)?;
        let mut out = Vec::with_capacity(len);
        let mut cnt: usize = 0;

        for i in 0..len {
            if cond_series[i] {
                cnt += 1;
            }
            if i + 1 > std_n {
                let left = i + 1 - std_n;
                if cond_series[left - 1] {
                    cnt -= 1;
                }
            }

            out.push(Some(cnt as f64));
        }
        Ok(Value::NumSeries(out))
    }

    fn impl_exist(&mut self, args: &[Expr]) -> Result<Value, EvalErr> {
        if args.len() != 2 {
            return Err(EvalErr {
                msg: "EXIST需要两个参数".to_string(),
            });
        }

        let cond = self.eval_expr(&args[0])?;
        let len = Value::len_of(&cond);
        let ori_n = Value::as_num(&self.eval_expr(&args[1])?)?;
        let std_n = { if ori_n as i64 <= 0 { 1 } else { ori_n as usize } };
        let cond_series = Value::as_bool_series(&cond, len)?;
        let mut out = Vec::with_capacity(len);
        let mut cnt: usize = 0;

        for i in 0..len {
            if cond_series[i] {
                cnt += 1;
            }
            if i + 1 > std_n {
                let left = i + 1 - std_n;
                if cond_series[left - 1] {
                    cnt -= 1;
                }
            }

            out.push(cnt > 0);
        }
        Ok(Value::BoolSeries(out))
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
                None => Err(EvalErr {
                    msg: "LAST命中的值为空".to_string(),
                }),
            },
            Value::SharedNumSeries(ns) => match ns[idx] {
                Some(n) => Ok(Value::Num(n)),
                None => Err(EvalErr {
                    msg: "LAST命中的值为空".to_string(),
                }),
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

            for j in start..=i {
                match num_series[j] {
                    Some(a) => {
                        if a > max {
                            max = a
                        }
                    }
                    None => {}
                }
            }
            out.push(Some(max));
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

            for j in start..=i {
                match num_series[j] {
                    Some(a) => {
                        if a < min {
                            min = a
                        }
                    }
                    None => {}
                }
            }
            out.push(Some(min));
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

        let b_series = Value::as_bool_series(&cond, len)?;
        let l_series = Value::as_num_series(&l, len)?;
        let r_series = Value::as_num_series(&r, len)?;
        let mut out = Vec::with_capacity(len);

        for i in 0..len {
            if b_series[i] {
                out.push(l_series[i]);
            } else {
                out.push(r_series[i]);
            }
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

        out.push(false);
        for i in 1..len {
            // if l_series[i] > r_series[i]{
            //     if l_series[i - 1] <= r_series[i - 1] {
            //         out.push(true);
            //     } else {
            //         out.push(false)
            //     }
            // } else {
            //     out.push(false);
            // }
            let hit = match (l_series[i], r_series[i], l_series[i - 1], r_series[i - 1]) {
                (Some(a), Some(b), Some(pa), Some(pb)) => a > b && pa <= pb,
                _ => false,
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
        let b_series = Value::as_bool_series(&v, len)?;
        let mut out = Vec::with_capacity(len);
        let mut count = 0;
        let mut has_true = false;
        let mut start = len;

        for i in 0..len {
            if b_series[i] {
                has_true = true;
                start = i + 1;
                out.push(Some(0.0));
                break;
            } else {
                out.push(Some(f64::NAN));
            }
        }
        if has_true {
            for j in start..len {
                match b_series[j] {
                    true => {
                        count = 0;
                    }
                    false => {
                        count += 1;
                    }
                }
                out.push(Some(count as f64));
            }
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
        // 大数字排在前面
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
        // 在第一个参数的排名中, 取满足第二个参数的k线,第三个参数的周期内取第一个参数的第四个参数个数的top
        if args.len() != 4 {
            return Err(EvalErr {
                msg: format!("{fn_name}需要四个参数"),
            });
        }

        let value = self.eval_expr(&args[0])?;
        let cond = self.eval_expr(&args[1])?;
        let len = Value::len_of(&value).max(Value::len_of(&cond));
        let value_series = Value::as_num_series(&value, len)?;
        let cond_series = Value::as_bool_series(&cond, len)?;
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
            let mut rows: Vec<(usize, f64, bool)> = Vec::with_capacity(std_win);
            let mut has_none = false;
            for j in start..=i {
                match value_series[j] {
                    Some(v) => rows.push((j, v, cond_series[j])),
                    None => {
                        has_none = true;
                        break;
                    }
                }
            } // v是需要rank的值,cond_s是是否满足条件, j留着tiebreak

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
            let hit_count = rows.iter().take(keep_n).filter(|(_, _, ok)| *ok).count();
            out.push(Some(hit_count as f64));
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
        let cond_series = Value::as_bool_series(&cond, len)?;
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
            let mut rows: Vec<(usize, f64, bool)> = Vec::with_capacity(period);
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
            let hit_count = rows.iter().take(keep_n).filter(|(_, _, ok)| *ok).count();
            out.push(Some(hit_count as f64));
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
        // 小数字排在前面
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
        let cond_series = Value::as_bool_series(&cond, len)?;
        let v_series = Value::as_num_series(&v, len)?;
        let ori_n = Value::as_num(&self.eval_expr(&args[2])?)?;
        let std_n = { if ori_n as i64 <= 0 { 1 } else { ori_n as usize } };
        let mut out = Vec::with_capacity(len);

        for i in 0..len {
            let start = i.saturating_sub(std_n);
            let mut last = None;
            for j in start..i {
                if cond_series[j] {
                    last = v_series[j];
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
        let cond_series = Value::as_bool_series(&cond, len)?;
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
                if cond_series[j] {
                    last = v_series[j];
                }
            }
            out.push(last);
        }

        Ok(Value::NumSeries(out))
    }
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
        let v = self.eval_expr(value)?;
        self.vars.insert(name.to_string(), v.clone());
        Ok(v)
    }

    fn eval_call(&mut self, name: &str, args: &[Expr]) -> Result<Value, EvalErr> {
        let fn_name = name.to_ascii_uppercase();
        match fn_name.as_str() {
            "ABS" => Ok(self.impl_abs(args)?),
            "MAX" => Ok(self.impl_max(args)?),
            "MIN" => Ok(self.impl_min(args)?),
            "DIV" => Ok(self.impl_div(args)?),
            "HHV" => Ok(self.impl_hhv(args)?),
            "HHVD" => Ok(self.impl_hhvd(args)?),
            "LLV" => Ok(self.impl_llv(args)?),
            "LLVD" => Ok(self.impl_llvd(args)?),
            "COUNT" => Ok(self.impl_count(args)?),
            "COUNTD" => Ok(self.impl_countd(args)?),
            "EXIST" => Ok(self.impl_exist(args)?),
            "MA" => Ok(self.impl_ma(args)?),
            "MAD" => Ok(self.impl_window_sumd(args, true)?),
            "REF" => Ok(self.impl_ref(args)?),
            "REFD" => Ok(self.impl_refd(args)?),
            "LAST" => Ok(self.impl_last(args)?),
            "SUM" => Ok(self.impl_sum(args)?),
            "SUMD" => Ok(self.impl_window_sumd(args, false)?),
            "STD" => Ok(self.impl_std(args)?),
            "STDD" => Ok(self.impl_stdd(args)?),
            "IF" => Ok(self.impl_if(args)?),
            "CROSS" => Ok(self.impl_cross(args)?),
            "EMA" => Ok(self.impl_ema(args)?),
            "SMA" => Ok(self.impl_sma(args)?),
            "BARSLAST" => Ok(self.impl_barslast(args)?),
            "RSV" => Ok(self.impl_rsv(args)?),
            "RSVD" => Ok(self.impl_rsvd(args)?),
            "GRANK" => Ok(self.impl_grank(args)?),
            "GRANKD" => Ok(self.impl_rankd(args, "GRANKD", true)?),
            "GTOPCOUNT" => Ok(self.impl_gtopcount(args)?),
            "GTOPCOUNTD" => Ok(self.impl_gtopcountd(args)?),
            "LTOPCOUNT" => Ok(self.impl_ltopcount(args)?),
            "LTOPCOUNTD" => Ok(self.impl_ltopcountd(args)?),
            "LRANK" => Ok(self.impl_lrank(args)?),
            "LRANKD" => Ok(self.impl_rankd(args, "LRANKD", false)?),
            "GET" => Ok(self.impl_get(args)?),
            "GETD" => Ok(self.impl_getd(args)?),

            other => Err(EvalErr {
                msg: format!("未定义函数:{:?}", other),
            }),
        }
    }

    fn eval_unary(&mut self, op: &UnaryOp, rhs: &Expr) -> Result<Value, EvalErr> {
        let v = self.eval_expr(rhs)?;
        match (op, v) {
            (UnaryOp::Neg, Value::Num(n)) => Ok(Value::Num(-n)),
            (UnaryOp::Neg, Value::Bool(b)) => Ok(Value::Num(-to_num(b))),
            (UnaryOp::Neg, Value::NumSeries(ns)) => Ok(Value::NumSeries(
                ns.into_iter().map(|x| x.map(|n| -n)).collect(),
            )),
            (UnaryOp::Neg, Value::SharedNumSeries(ns)) => {
                Ok(Value::NumSeries(ns.iter().map(|x| x.map(|n| -n)).collect()))
            }
            (UnaryOp::Neg, Value::BoolSeries(bs)) => Ok(Value::NumSeries(
                bs.into_iter().map(|b| Some(-to_num(b))).collect(),
            )),
            (UnaryOp::Not, Value::Num(n)) => Ok(Value::Bool(!to_bool(n))),
            (UnaryOp::Not, Value::Bool(b)) => Ok(Value::Bool(!b)),
            (UnaryOp::Not, Value::NumSeries(ns)) => Ok(Value::BoolSeries(
                ns.into_iter().map(|x| !to_bool(x.unwrap_or(0.0))).collect(),
            )),
            (UnaryOp::Not, Value::SharedNumSeries(ns)) => Ok(Value::BoolSeries(
                ns.iter().map(|x| !to_bool(x.unwrap_or(0.0))).collect(),
            )),
            (UnaryOp::Not, Value::BoolSeries(bs)) => {
                Ok(Value::BoolSeries(bs.into_iter().map(|b| !b).collect()))
            }
        }
    }

    fn eval_binary(&mut self, op: &BinaryOp, lhs: &Expr, rhs: &Expr) -> Result<Value, EvalErr> {
        let lv = self.eval_expr(lhs)?;
        let rv = self.eval_expr(rhs)?;

        if matches!(lv, Value::Num(_) | Value::Bool(_))
            && matches!(rv, Value::Num(_) | Value::Bool(_))
        {
            let l = Value::as_num(&lv)?;
            let r = Value::as_num(&rv)?;

            return match op {
                BinaryOp::Add => Ok(Value::Num(l + r)),
                BinaryOp::Sub => Ok(Value::Num(l - r)),
                BinaryOp::Mul => Ok(Value::Num(l * r)),
                BinaryOp::Div => {
                    if r.abs() < EPS {
                        Ok(Value::Num(0.0))
                    } else {
                        Ok(Value::Num(l / r))
                    }
                }
                BinaryOp::Ge => Ok(Value::Bool(l > r + EPS || (l - r).abs() <= EPS)),
                BinaryOp::Gt => Ok(Value::Bool(l > r + EPS)),
                BinaryOp::Le => Ok(Value::Bool(l < r - EPS || (l - r).abs() <= EPS)),
                BinaryOp::Lt => Ok(Value::Bool(l < r - EPS)),
                BinaryOp::Eq => Ok(Value::Bool((l - r).abs() <= EPS)),
                BinaryOp::Ne => Ok(Value::Bool((l - r).abs() > EPS)),
                BinaryOp::And => Ok(Value::Bool(to_bool(l) && to_bool(r))),
                BinaryOp::Or => Ok(Value::Bool(to_bool(l) || to_bool(r))),
            };
        }

        let len = usize::max(Value::len_of(&lv), Value::len_of(&rv));
        let ls = Value::as_num_series(&lv, len)?;
        let rs = Value::as_num_series(&rv, len)?;

        match op {
            BinaryOp::Add => {
                let mut out: Vec<Option<f64>> = Vec::with_capacity(len);
                for i in 0..len {
                    match (ls[i], rs[i]) {
                        (Some(a), Some(b)) => out.push(Some(a + b)),
                        _ => {
                            out.push(None);
                        }
                    }
                }
                Ok(Value::NumSeries(out))
            }
            BinaryOp::Sub => {
                let mut out: Vec<Option<f64>> = Vec::with_capacity(len);
                for i in 0..len {
                    match (ls[i], rs[i]) {
                        (Some(a), Some(b)) => out.push(Some(a - b)),
                        _ => {
                            out.push(None);
                        }
                    }
                }
                Ok(Value::NumSeries(out))
            }
            BinaryOp::Mul => {
                let mut out: Vec<Option<f64>> = Vec::with_capacity(len);
                for i in 0..len {
                    match (ls[i], rs[i]) {
                        (Some(a), Some(b)) => out.push(Some(a * b)),
                        _ => {
                            out.push(None);
                        }
                    }
                }
                Ok(Value::NumSeries(out))
            }
            BinaryOp::Div => {
                let mut out: Vec<Option<f64>> = Vec::with_capacity(len);
                for i in 0..len {
                    match (ls[i], rs[i]) {
                        (Some(a), Some(b)) => {
                            if b.abs() < EPS {
                                out.push(Some(0.0));
                            } else {
                                out.push(Some(a / b))
                            }
                        }
                        _ => {
                            out.push(None);
                        }
                    }
                }
                Ok(Value::NumSeries(out))
            }

            BinaryOp::Ge => {
                let mut out = Vec::with_capacity(len);
                for i in 0..len {
                    match (ls[i], rs[i]) {
                        (Some(a), Some(b)) => out.push(a > b + EPS || (a - b).abs() <= EPS),
                        _ => {
                            out.push(false);
                        }
                    }
                }
                Ok(Value::BoolSeries(out))
            }
            BinaryOp::Gt => {
                let mut out = Vec::with_capacity(len);
                for i in 0..len {
                    match (ls[i], rs[i]) {
                        (Some(a), Some(b)) => out.push(a > b + EPS),
                        _ => {
                            out.push(false);
                        }
                    }
                }
                Ok(Value::BoolSeries(out))
            }
            BinaryOp::Le => {
                let mut out = Vec::with_capacity(len);
                for i in 0..len {
                    match (ls[i], rs[i]) {
                        (Some(a), Some(b)) => out.push(a < b - EPS || (a - b).abs() <= EPS),
                        _ => {
                            out.push(false);
                        }
                    }
                }
                Ok(Value::BoolSeries(out))
            }
            BinaryOp::Lt => {
                let mut out = Vec::with_capacity(len);
                for i in 0..len {
                    match (ls[i], rs[i]) {
                        (Some(a), Some(b)) => out.push(a < b - EPS),
                        _ => {
                            out.push(false);
                        }
                    }
                }
                Ok(Value::BoolSeries(out))
            }
            BinaryOp::Eq => {
                let mut out = Vec::with_capacity(len);
                for i in 0..len {
                    match (ls[i], rs[i]) {
                        (Some(a), Some(b)) => out.push((a - b).abs() <= EPS),
                        _ => {
                            out.push(false);
                        }
                    }
                }
                Ok(Value::BoolSeries(out))
            }
            BinaryOp::Ne => {
                let mut out = Vec::with_capacity(len);
                for i in 0..len {
                    match (ls[i], rs[i]) {
                        (Some(a), Some(b)) => out.push(!((a - b).abs() <= EPS)),
                        _ => {
                            out.push(false);
                        }
                    }
                }
                Ok(Value::BoolSeries(out))
            }

            BinaryOp::And => {
                let mut out = Vec::with_capacity(len);
                for i in 0..len {
                    match (ls[i], rs[i]) {
                        (Some(a), Some(b)) => out.push(to_bool(a) && to_bool(b)),
                        _ => {
                            out.push(false);
                        }
                    }
                }
                Ok(Value::BoolSeries(out))
            }
            BinaryOp::Or => {
                let mut out = Vec::with_capacity(len);
                for i in 0..len {
                    match (ls[i], rs[i]) {
                        (Some(a), Some(b)) => out.push(to_bool(a) || to_bool(b)),
                        _ => {
                            out.push(false);
                        }
                    }
                }
                Ok(Value::BoolSeries(out))
            }
        }
    }

    fn eval_stmt(&mut self, stmt: &Stmt) -> Result<Value, EvalErr> {
        // 赋值分支和语句分支的选择处理
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
            Value::SharedNumSeries(series) => {
                Ok(DayValue::Num(series.get(day_index).copied().flatten()))
            }
            Value::BoolSeries(series) => Ok(DayValue::Bool(
                series.get(day_index).copied().unwrap_or(false),
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
                if let Some(value) = locals.get(name).copied() {
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
                    UnaryOp::Not => DayValue::Bool(!day_value_as_bool(rhs)),
                }))
            }
            Expr::Binary { op, lhs, rhs } => {
                let Some(lhs) = self.eval_expr_bool_at(lhs, locals, day_index)? else {
                    return Ok(None);
                };
                let Some(rhs) = self.eval_expr_bool_at(rhs, locals, day_index)? else {
                    return Ok(None);
                };
                let lhs_num = day_value_as_num(lhs);
                let rhs_num = day_value_as_num(rhs);
                let value = match op {
                    BinaryOp::Add => {
                        DayValue::Num(lhs_num.zip(rhs_num).map(|(lhs, rhs)| lhs + rhs))
                    }
                    BinaryOp::Sub => {
                        DayValue::Num(lhs_num.zip(rhs_num).map(|(lhs, rhs)| lhs - rhs))
                    }
                    BinaryOp::Mul => {
                        DayValue::Num(lhs_num.zip(rhs_num).map(|(lhs, rhs)| lhs * rhs))
                    }
                    BinaryOp::Div => DayValue::Num(
                        lhs_num.zip(rhs_num).map(
                            |(lhs, rhs)| {
                                if rhs.abs() < EPS { 0.0 } else { lhs / rhs }
                            },
                        ),
                    ),
                    BinaryOp::Ge => DayValue::Bool(
                        lhs_num
                            .zip(rhs_num)
                            .map(|(lhs, rhs)| lhs > rhs + EPS || (lhs - rhs).abs() <= EPS)
                            .unwrap_or(false),
                    ),
                    BinaryOp::Gt => DayValue::Bool(
                        lhs_num
                            .zip(rhs_num)
                            .map(|(lhs, rhs)| lhs > rhs + EPS)
                            .unwrap_or(false),
                    ),
                    BinaryOp::Le => DayValue::Bool(
                        lhs_num
                            .zip(rhs_num)
                            .map(|(lhs, rhs)| lhs < rhs - EPS || (lhs - rhs).abs() <= EPS)
                            .unwrap_or(false),
                    ),
                    BinaryOp::Lt => DayValue::Bool(
                        lhs_num
                            .zip(rhs_num)
                            .map(|(lhs, rhs)| lhs < rhs - EPS)
                            .unwrap_or(false),
                    ),
                    BinaryOp::Eq => DayValue::Bool(
                        lhs_num
                            .zip(rhs_num)
                            .map(|(lhs, rhs)| (lhs - rhs).abs() <= EPS)
                            .unwrap_or(false),
                    ),
                    BinaryOp::Ne => DayValue::Bool(
                        lhs_num
                            .zip(rhs_num)
                            .map(|(lhs, rhs)| (lhs - rhs).abs() > EPS)
                            .unwrap_or(false),
                    ),
                    BinaryOp::And => {
                        DayValue::Bool(day_value_as_bool(lhs) && day_value_as_bool(rhs))
                    }
                    BinaryOp::Or => {
                        DayValue::Bool(day_value_as_bool(lhs) || day_value_as_bool(rhs))
                    }
                };
                Ok(Some(value))
            }
        }
    }

    pub fn eval_program_bool_at(
        &self,
        stmts: &Stmts,
        day_index: usize,
    ) -> Result<Option<bool>, EvalErr> {
        let mut locals = HashMap::new();
        let mut last = DayValue::Num(Some(0.0));

        for stmt in &stmts.item {
            match stmt {
                Stmt::Assign { name, value } => {
                    let Some(value) = self.eval_expr_bool_at(value, &locals, day_index)? else {
                        return Ok(None);
                    };
                    locals.insert(name.clone(), value);
                    last = value;
                }
                Stmt::Expr(expr) => {
                    let Some(value) = self.eval_expr_bool_at(expr, &locals, day_index)? else {
                        return Ok(None);
                    };
                    last = value;
                }
            }
        }

        Ok(Some(day_value_as_bool(last)))
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
    Bool(bool),
    BoolSeries(Vec<bool>),
}

impl Value {
    pub fn len_of(v: &Value) -> usize {
        match v {
            Value::Num(_) => 1,
            Value::Bool(_) => 1,
            Value::NumSeries(n) => n.len(),
            Value::SharedNumSeries(n) => n.len(),
            Value::BoolSeries(b) => b.len(),
        }
    }

    pub fn as_num(v: &Value) -> Result<f64, EvalErr> {
        match v {
            Value::Num(n) => Ok(*n),
            Value::Bool(b) => Ok(if *b { 1.0 } else { 0.0 }),
            Value::NumSeries(_) | Value::SharedNumSeries(_) => Err(EvalErr {
                msg: "需要标量数字，但拿到数值序列，可用LAST函数转换".to_string(),
            }),
            Value::BoolSeries(_) => Err(EvalErr {
                msg: "需要标量数字，但拿到布尔序列".to_string(),
            }),
        }
    }

    pub fn as_bool(v: &Value) -> Result<bool, EvalErr> {
        match v {
            Value::Num(n) => Ok(*n != 0.0),
            Value::Bool(b) => Ok(*b),
            Value::NumSeries(_) | Value::SharedNumSeries(_) => Err(EvalErr {
                msg: "需要布尔，但拿到数值序列".to_string(),
            }),
            Value::BoolSeries(_) => Err(EvalErr {
                msg: "需要布尔，但拿到布尔序列，可用LAST函数转换".to_string(),
            }),
        }
    }

    pub fn as_num_series(v: &Value, len: usize) -> Result<Vec<Option<f64>>, EvalErr> {
        match v {
            Value::Num(n) => Ok(vec![Some(*n); len]),
            Value::Bool(b) => Ok(vec![Some(if *b { 1.0 } else { 0.0 }); len]),
            Value::NumSeries(ns) => {
                if ns.len() == len {
                    Ok(ns.clone())
                } else {
                    Err(EvalErr {
                        msg: "数值序列长度不对".to_string(),
                    })
                }
            }
            Value::SharedNumSeries(ns) => {
                if ns.len() == len {
                    Ok(ns.as_ref().clone())
                } else {
                    Err(EvalErr {
                        msg: "数值序列长度不对".to_string(),
                    })
                }
            }
            Value::BoolSeries(bs) => {
                if bs.len() == len {
                    Ok(bs
                        .iter()
                        .map(|b| Some(if *b { 1.0 } else { 0.0 }))
                        .collect())
                } else {
                    Err(EvalErr {
                        msg: "布尔序列长度不对".to_string(),
                    })
                }
            }
        }
    }

    pub fn as_bool_series(v: &Value, len: usize) -> Result<Vec<bool>, EvalErr> {
        match v {
            Value::Num(n) => Ok(vec![*n != 0.0; len]),
            Value::Bool(b) => Ok(vec![*b; len]),
            Value::NumSeries(ns) => {
                if ns.len() == len {
                    Ok(ns
                        .iter()
                        .map(|n| match n {
                            Some(n) => *n != 0.0,
                            None => false,
                        }) // 在map中match处理Some(n),可用map_or(Some分支, None分支)
                        .collect())
                } else {
                    Err(EvalErr {
                        msg: "数值序列长度不对".to_string(),
                    })
                }
            }
            Value::SharedNumSeries(ns) => {
                if ns.len() == len {
                    Ok(ns
                        .iter()
                        .map(|n| match n {
                            Some(n) => *n != 0.0,
                            None => false,
                        })
                        .collect())
                } else {
                    Err(EvalErr {
                        msg: "数值序列长度不对".to_string(),
                    })
                }
            }
            Value::BoolSeries(bs) => {
                if bs.len() == len {
                    Ok(bs.clone())
                } else {
                    Err(EvalErr {
                        msg: "布尔序列长度不对".to_string(),
                    })
                }
            }
        }
    }
}

#[test]
fn call_test() {
    use crate::expr::parser::{Parser, lex_all};

    // let expr = "C > MA(C, 3);";
    // let expr = "C > HHV(REF(C, 1), 3);";
    // let expr = "SUM(C, 3);";
    // let expr = "NOT(CROSS(C, MA(C, 3)));";
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
            assert!(!bs[5]);
            assert!(bs[6]);
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
    assert_eq!(out, Value::Bool(true));
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
        Value::BoolSeries(vec![false, true, false, true, false])
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
        Value::BoolSeries(vec![false, false, true, false, true])
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
        Value::BoolSeries(vec![false, true, true, true, true, true])
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
    assert_eq!(out, Value::BoolSeries(vec![true, true, true, true, false]));
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
