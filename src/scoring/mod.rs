use crate::{
    expr::{
        eval::{Runtime, Value},
        parser::{Parser, lex_all},
    },
    strategy::loader::{DistPoint, ScopeWay, ScoreRule},
};
pub mod data;

enum ScopeHit {
    Bool(bool),
    Count(usize),
    Recent(Option<usize>),
}

#[derive(Debug, Default)]
pub struct RuleScoreSeries {
    pub name: String,
    pub series: Vec<f64>,
}

fn rt_max_len(rt: &Runtime) -> usize {
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

fn hit_when(when: &str, rt: &mut Runtime) -> Result<Vec<bool>, String> {
    // 得到when的布尔序列
    let tok = lex_all(when);
    let mut expr = Parser::new(tok);
    let stmts = expr
        .parse_main()
        .map_err(|e| format!("表达式解析错误在{}:{}", e.idx, e.msg))?;
    let value = rt
        .eval_program(&stmts)
        .map_err(|e| format!("表达式计算错误:{}", e.msg))?;
    let len = rt_max_len(rt);

    Value::as_bool_series(&value, len).map_err(|e| format!("表达式返回值非布尔:{}", e.msg))
}

fn hit_scopeway(scopeway: ScopeWay, windows: usize, bs: &[bool], i: usize) -> ScopeHit {
    match scopeway {
        ScopeWay::Last => ScopeHit::Bool(bs[i]),
        ScopeWay::Any => {
            let start = (i + 1).saturating_sub(windows);
            for j in start..=i {
                if bs[j] {
                    return ScopeHit::Bool(true);
                }
            }
            ScopeHit::Bool(false)
        }
        ScopeWay::Consec(len) => {
            let start = (i + 1).saturating_sub(windows);
            let mut cur = 0;
            let mut best = 0;
            for j in start..=i {
                if bs[j] {
                    cur += 1;
                } else {
                    cur = 0;
                }
                if cur > best {
                    best = cur;
                }
            }
            ScopeHit::Bool(best >= len)
        }
        ScopeWay::Each => {
            let start = (i + 1).saturating_sub(windows);
            let mut cnt = 0;
            for j in start..=i {
                if bs[j] {
                    cnt += 1
                }
            }
            ScopeHit::Count(cnt)
        }
        ScopeWay::Recent => {
            let start = (i + 1).saturating_sub(windows);
            for j in (start..=i).rev() {
                if bs[j] {
                    return ScopeHit::Recent(Some(i - j));
                }
            }
            ScopeHit::Recent(None)
        }
    }
}

fn impl_scope_at_recent(recent: ScopeHit, dps: &[DistPoint]) -> f64 {
    let last = match recent {
        ScopeHit::Recent(v) => match v {
            Some(n) => n,
            None => {
                return 0.0;
            }
        },
        _ => return 0.0,
    };

    for dp in dps {
        if dp.min <= last && last <= dp.max {
            return dp.points;
        }
    }
    0.0
}

fn score_at(scopeway: ScopeHit, dps: Option<&[DistPoint]>, points: f64) -> f64 {
    // scopeway分发到得分
    match scopeway {
        ScopeHit::Bool(ok) => {
            if ok {
                points
            } else {
                0.0
            }
        }
        ScopeHit::Count(n) => n as f64 * points,
        ScopeHit::Recent(v) => {
            if let Some(dp) = dps {
                impl_scope_at_recent(ScopeHit::Recent(v), dp)
            } else {
                match v {
                    Some(_) => points,
                    None => 0.0,
                }
            }
        }
    }
}

fn score_rule(rule: &ScoreRule, rt: &mut Runtime) -> Result<Vec<f64>, String> {
    let bs = hit_when(&rule.when, rt)?;
    let mut out = Vec::with_capacity(bs.len());

    for i in 0..bs.len() {
        let hit = hit_scopeway(rule.scope_way, rule.scope_windows, &bs, i);
        let s = score_at(hit, rule.dist_points.as_deref(), rule.points);
        out.push(s);
    }

    Ok(out)
}

fn score_total(rules: &[ScoreRule], rt: &mut Runtime) -> Result<Vec<f64>, String> {
    let len = rt_max_len(rt);
    let mut total = vec![0.0; len];

    for rule in rules {
        let single_score = score_rule(rule, rt)?;
        let min_len = usize::min(total.len(), single_score.len());
        for i in 0..min_len {
            total[i] += single_score[i];
        }
    }

    Ok(total)
}

pub fn scoring_rules(rt: &mut Runtime) -> Result<Vec<f64>, String> {
    let rules = ScoreRule::load_rules()?;
    score_total(&rules, rt)
}

pub fn scoring_rules_details(rt: &mut Runtime) -> Result<(Vec<f64>, Vec<RuleScoreSeries>), String> {
    let len = rt_max_len(rt);
    let rules = ScoreRule::load_rules()?;

    let mut total = vec![0.0; len];
    let mut details = Vec::with_capacity(rules.len());

    for rule in rules {
        let score = score_rule(&rule, rt)?;
        let min_len = usize::min(total.len(), score.len());
        for i in 0..min_len {
            total[i] += score[i];
        }

        details.push(RuleScoreSeries {
            name: rule.name.clone(),
            series: score,
        });
    }
    Ok((total, details))
}
