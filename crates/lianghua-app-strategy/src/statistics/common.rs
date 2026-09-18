use crate::data::{RuleKind, ScopeWay, ScoreRule, result_db_path};
use duckdb::Connection;
use std::collections::HashMap;
#[derive(Debug, Clone)]
pub(in crate::statistics) struct RuleMeta {
    pub(in crate::statistics) when: String,
    pub(in crate::statistics) explain: String,
    pub(in crate::statistics) trigger_mode: String,
    pub(in crate::statistics) is_each: bool,
    pub(in crate::statistics) points: f64,
}

#[derive(Debug, Clone, Default)]
pub(in crate::statistics) struct RuleDayAgg {
    pub(in crate::statistics) trigger_count: i64,
    pub(in crate::statistics) contribution_score: f64,
    pub(in crate::statistics) top100_trigger_count: i64,
    pub(in crate::statistics) best_rank: Option<i64>,
}

pub(in crate::statistics) fn open_result_conn(source_path: &str) -> Result<Connection, String> {
    let result_db = result_db_path(source_path);
    let result_db_str = result_db
        .to_str()
        .ok_or_else(|| "结果库路径不是有效UTF-8".to_string())?;
    Connection::open(result_db_str).map_err(|e| format!("打开结果库失败: {e}"))
}

pub(in crate::statistics) fn scope_way_label(scope_way: ScopeWay) -> String {
    match scope_way {
        ScopeWay::Any => "any".to_string(),
        ScopeWay::Last => "last".to_string(),
        ScopeWay::Each => "each".to_string(),
        ScopeWay::Recent => "recent".to_string(),
        ScopeWay::Consec(n) => format!("consec>={n}"),
    }
}

pub(in crate::statistics) fn parse_scope_way_input(
    scope_way_raw: &str,
) -> Result<ScopeWay, String> {
    let normalized = scope_way_raw.trim().to_ascii_uppercase();
    match normalized.as_str() {
        "ANY" => Ok(ScopeWay::Any),
        "LAST" => Ok(ScopeWay::Last),
        "EACH" => Ok(ScopeWay::Each),
        "RECENT" => Ok(ScopeWay::Recent),
        value => {
            let Some(num) = value.strip_prefix("CONSEC>=") else {
                return Err(format!(
                    "scope_way 不支持: {scope_way_raw}，仅支持 ANY/LAST/EACH/RECENT/CONSEC>=N"
                ));
            };
            let threshold = num
                .parse::<usize>()
                .map_err(|_| format!("scope_way 连续阈值非法: {scope_way_raw}"))?;
            if threshold == 0 {
                return Err("scope_way 连续阈值必须 >= 1".to_string());
            }
            Ok(ScopeWay::Consec(threshold))
        }
    }
}

pub(in crate::statistics) fn load_rule_meta(
    source_path: &str,
) -> Result<(Vec<String>, HashMap<String, RuleMeta>), String> {
    let rules = ScoreRule::load_rules(source_path)?;
    let mut order = Vec::with_capacity(rules.len());
    let mut meta_map = HashMap::with_capacity(rules.len());

    for rule in rules {
        order.push(rule.name.clone());
        let when = match rule.kind {
            RuleKind::Single => rule.when.clone(),
            RuleKind::Combination => (|rule: &ScoreRule| -> String {
                let conditions = rule
                    .conditions
                    .iter()
                    .map(|condition| format!("{}: {}", condition.name, condition.when))
                    .collect::<Vec<_>>()
                    .join("；");
                let bonuses = rule
                    .conditions
                    .iter()
                    .filter(|condition| condition.bonus_points != 0.0)
                    .map(|condition| format!("{}: {:+}", condition.name, condition.bonus_points))
                    .collect::<Vec<_>>()
                    .join("；");
                let mut parts = vec![
                    format!("组合条件：{conditions}"),
                    format!(
                        "命中数得分：{:?}",
                        rule.points_by_hits.as_deref().unwrap_or_default()
                    ),
                ];
                if !bonuses.is_empty() {
                    parts.push(format!("额外加分：{bonuses}"));
                }
                parts.join("；")
            })(&rule),
        };
        let points = rule.representative_points();
        meta_map.insert(
            rule.name,
            RuleMeta {
                when,
                explain: rule.explain,
                trigger_mode: scope_way_label(rule.scope_way),
                is_each: rule.kind == RuleKind::Single && matches!(rule.scope_way, ScopeWay::Each),
                points,
            },
        );
    }

    Ok((order, meta_map))
}
