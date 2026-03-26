use duckdb::Connection;

use crate::{
    data::{DistPoint, RuleTag, ScopeWay},
    expr::{
        eval::{Runtime, Value},
        parser::Stmts,
    },
    scoring::tools::rt_max_len,
};

pub mod runner;
pub mod tools;

enum ScopeHit {
    Bool(bool),
    Count(usize),
    Recent(Option<usize>),
}

#[derive(Debug, Clone, Copy)]
pub enum TieBreakWay {
    TsCode,
    KdjJ,
}

#[derive(Debug, Default)]
pub struct RuleScoreSeries {
    pub name: String,
    pub series: Vec<f64>,
}

#[derive(Clone)]
pub struct CachedRule {
    pub name: String,
    pub scope_windows: usize,
    pub scope_way: ScopeWay,
    pub points: f64,
    pub dist_points: Option<Vec<DistPoint>>,
    pub tag: RuleTag,
    pub when_src: String,
    pub when_ast: Stmts,
}

fn hit_when_cache(rule: &CachedRule, rt: &mut Runtime) -> Result<Vec<bool>, String> {
    let value = rt
        .eval_program(&rule.when_ast)
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

fn scoring_rule_cache(rule: &CachedRule, rt: &mut Runtime) -> Result<Vec<f64>, String> {
    let bs = hit_when_cache(&rule, rt)?;
    let mut out = Vec::with_capacity(bs.len());

    for i in 0..bs.len() {
        let hit = hit_scopeway(rule.scope_way, rule.scope_windows, &bs, i);
        let s = score_at(hit, rule.dist_points.as_deref(), rule.points);
        out.push(s);
    }

    Ok(out)
}

pub fn scoring_rules_details_cache(
    rt: &mut Runtime,
    rules_cache: &Vec<CachedRule>,
) -> Result<(Vec<f64>, Vec<RuleScoreSeries>), String> {
    let len = rt_max_len(rt);
    let mut total = vec![50.0; len];
    let mut details = Vec::with_capacity(rules_cache.len());

    for rule in rules_cache {
        let score = scoring_rule_cache(&rule, rt)?;
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

fn build_tirbreak_rank_sql(tie_break: TieBreakWay, adj_type: &str) -> String {
    match tie_break {
        TieBreakWay::TsCode => r#"
            UPDATE score_summary AS s
            SET rank = r.new_rank
            FROM (
                SELECT
                    ts_code,
                    trade_date,
                    ROW_NUMBER() OVER (
                        PARTITION BY trade_date
                        ORDER BY total_score DESC, ts_code ASC
                    ) AS new_rank
                FROM score_summary
            ) AS r
            WHERE s.ts_code = r.ts_code
              AND s.trade_date = r.trade_date
            "#
        .to_string(),
        TieBreakWay::KdjJ => {
            format!(
                r#"
                UPDATE score_summary AS s
                SET rank = r.new_rank
                FROM (
                    SELECT
                        s.ts_code,
                        s.trade_date,
                        ROW_NUMBER() OVER (
                            PARTITION BY s.trade_date
                            ORDER BY
                                s.total_score DESC,
                                src.j ASC NULLS LAST,
                                s.ts_code ASC
                        ) AS new_rank
                    FROM score_summary AS s
                    LEFT JOIN src_db.stock_data AS src
                      ON s.ts_code = src.ts_code
                     AND s.trade_date = src.trade_date
                     AND src.adj_type = '{adj_type}'
                ) AS r
                WHERE s.ts_code = r.ts_code
                  AND s.trade_date = r.trade_date
                "#
            )
        }
    }
}

pub fn build_rank_tiebreak(
    result_db_path: &str,
    source_db_path: &str,
    adj_type: &str,
    tie_break: TieBreakWay,
) -> Result<(), String> {
    let conn = Connection::open(result_db_path).map_err(|e| format!("结果库连接失败:{e}"))?;

    if let TieBreakWay::KdjJ = tie_break {
        let attach_sql = format!("ATTACH '{}' AS src_db", source_db_path);
        conn.execute(&attach_sql, [])
            .map_err(|e| format!("附加原始库失败:{e}"))?;
    }

    let sql = build_tirbreak_rank_sql(tie_break, adj_type);
    conn.execute(&sql, [])
        .map_err(|e| format!("补rank失败:{e}"))?;

    if let TieBreakWay::KdjJ = tie_break {
        let _ = conn.execute("DETACH src_db", []);
    }

    Ok(())
}
