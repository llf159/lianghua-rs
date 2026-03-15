use crate::expr::parser::{BinaryOp, Expr, Stmt, Stmts, UnaryOp};
use std::collections::HashMap;

const EPS: f64 = 1e-12;

#[derive(Debug, Clone)]
pub struct EvalErr {
    pub msg: String,
}

#[derive(Debug, Default)]
pub struct Runtime {
    pub vars: HashMap<String, Value>,
}

fn to_bool(v: f64) -> bool {
    v != 0.0
}

fn to_num(b: bool) -> f64 {
    if b { 1.0 } else { 0.0 }
}

impl Runtime {
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

    fn impl_gtopcount(&mut self, args: &[Expr]) -> Result<Value, EvalErr> {
        self.impl_rank_topcount(args, true, "GTOPCOUNT")
    }

    fn impl_ltopcount(&mut self, args: &[Expr]) -> Result<Value, EvalErr> {
        self.impl_rank_topcount(args, false, "LTOPCOUNT")
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
            "HHV" => Ok(self.impl_hhv(args)?),
            "LLV" => Ok(self.impl_llv(args)?),
            "COUNT" => Ok(self.impl_count(args)?),
            "MA" => Ok(self.impl_ma(args)?),
            "REF" => Ok(self.impl_ref(args)?),
            "SUM" => Ok(self.impl_sum(args)?),
            "STD" => Ok(self.impl_std(args)?),
            "IF" => Ok(self.impl_if(args)?),
            "CROSS" => Ok(self.impl_cross(args)?),
            "EMA" => Ok(self.impl_ema(args)?),
            "SMA" => Ok(self.impl_sma(args)?),
            "BARSLAST" => Ok(self.impl_barslast(args)?),
            "RSV" => Ok(self.impl_rsv(args)?),
            "GRANK" => Ok(self.impl_grank(args)?),
            "GTOPCOUNT" => Ok(self.impl_gtopcount(args)?),
            "LTOPCOUNT" => Ok(self.impl_ltopcount(args)?),
            "LRANK" => Ok(self.impl_lrank(args)?),
            "GET" => Ok(self.impl_get(args)?),

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
            (UnaryOp::Neg, Value::BoolSeries(bs)) => Ok(Value::NumSeries(
                bs.into_iter().map(|b| Some(-to_num(b))).collect(),
            )),
            (UnaryOp::Not, Value::Num(n)) => Ok(Value::Bool(!to_bool(n))),
            (UnaryOp::Not, Value::Bool(b)) => Ok(Value::Bool(!b)),
            (UnaryOp::Not, Value::NumSeries(ns)) => Ok(Value::BoolSeries(
                ns.into_iter().map(|x| !to_bool(x.unwrap_or(0.0))).collect(),
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
    Bool(bool),
    BoolSeries(Vec<bool>),
}

impl Value {
    pub fn len_of(v: &Value) -> usize {
        match v {
            Value::Num(_) => 1,
            Value::Bool(_) => 1,
            Value::NumSeries(n) => n.len(),
            Value::BoolSeries(b) => b.len(),
        }
    }

    pub fn as_num(v: &Value) -> Result<f64, EvalErr> {
        match v {
            Value::Num(n) => Ok(*n),
            Value::Bool(b) => Ok(if *b { 1.0 } else { 0.0 }),
            Value::NumSeries(_) => Err(EvalErr {
                msg: "需要标量数字，但拿到数值序列".to_string(),
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
            Value::NumSeries(_) => Err(EvalErr {
                msg: "需要布尔，但拿到数值序列".to_string(),
            }),
            Value::BoolSeries(_) => Err(EvalErr {
                msg: "需要布尔，但拿到布尔序列".to_string(),
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
