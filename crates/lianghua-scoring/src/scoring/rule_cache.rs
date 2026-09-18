use crate::{
    data::{RuleKind, ScoreRule, collect_assigned_names_from_expr_program},
    expr::validation::{parse_expression_program, validate_expression_functions},
    scoring::{
        CachedCombinationCondition, CachedCombinationRule, CachedRule, CachedRuleExpression,
    },
};

pub fn cache_rule_build(
    source_dir: &str,
    strategy_path: Option<&str>,
) -> Result<Vec<CachedRule>, String> {
    let rules = ScoreRule::load_rules_with_strategy_path(source_dir, strategy_path)?;
    let mut out = Vec::with_capacity(128);
    for rule in rules {
        match rule.kind {
            RuleKind::Single => {
                let expression = build_cached_rule_expression(&rule.name, rule.when)?;
                out.push(CachedRule {
                    name: rule.name,
                    scope_windows: rule.scope_windows,
                    scope_way: rule.scope_way,
                    points: rule.points,
                    dist_points: rule.dist_points,
                    max_points: rule.max_points,
                    tag: rule.tag,
                    when_src: expression.when_src,
                    when_ast: expression.when_ast,
                    assigned_names: expression.assigned_names,
                    combination: None,
                });
            }
            RuleKind::Combination => {
                let conditions = rule
                    .conditions
                    .into_iter()
                    .map(|condition| {
                        let expression =
                            build_cached_rule_expression(&condition.name, condition.when)?;
                        Ok(CachedCombinationCondition {
                            expression,
                            bonus_points: condition.bonus_points,
                        })
                    })
                    .collect::<Result<Vec<_>, String>>()?;
                let first_condition = conditions
                    .first()
                    .ok_or_else(|| format!("组合规则({})没有条件", rule.name))?
                    .expression
                    .clone();
                let points_by_hits = rule
                    .points_by_hits
                    .ok_or_else(|| format!("组合规则({})缺少 points_by_hits", rule.name))?;
                out.push(CachedRule {
                    name: rule.name,
                    scope_windows: rule.scope_windows,
                    scope_way: rule.scope_way,
                    points: 0.0,
                    dist_points: None,
                    max_points: None,
                    tag: rule.tag,
                    when_src: first_condition.when_src,
                    when_ast: first_condition.when_ast,
                    assigned_names: first_condition.assigned_names,
                    combination: Some(CachedCombinationRule {
                        conditions,
                        points_by_hits,
                        max_points: rule.max_points,
                        max_bonus_points: rule.max_bonus_points,
                    }),
                });
            }
        }
    }
    Ok(out)
}

fn build_cached_rule_expression(
    name: &str,
    when_src: String,
) -> Result<CachedRuleExpression, String> {
    let when_ast = parse_expression_program(&when_src)
        .map_err(|error| format!("表达式({name})解析错误在{}:{}", error.idx, error.msg))?;
    validate_expression_functions(&when_ast)?;
    let assigned_names = collect_assigned_names_from_expr_program(&when_ast);
    Ok(CachedRuleExpression {
        name: name.to_string(),
        when_src,
        when_ast,
        assigned_names,
    })
}

#[cfg(test)]
mod tests {
    use std::{
        fs::{create_dir_all, remove_dir_all, write},
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::cache_rule_build;

    #[test]
    fn cache_builder_compiles_all_combination_expressions() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let source_dir = std::env::temp_dir().join(format!("lianghua-combination-cache-{unique}"));
        create_dir_all(&source_dir).expect("create temp source");
        write(
            source_dir.join("score_rule.toml"),
            r#"
version = 1

[[scene]]
name = "趋势启动"
direction = "long"
observe_threshold = 1.0
trigger_threshold = 2.0
confirm_threshold = 3.0
fail_threshold = 1.0

[[rule]]
name = "量价组合"
scene = "趋势启动"
kind = "combination"
stage = "trigger"
scope_windows = 2
scope_way = "ANY"
points_by_hits = [0.0, 1.0, 3.0]
explain = "组合命中计分"

[[rule.condition]]
name = "收红"
when = "C > O"

[[rule.condition]]
name = "放量"
when = "V > REF(V, 1)"
bonus_points = 1.0
"#,
        )
        .expect("write strategy");

        let rules = cache_rule_build(source_dir.to_str().expect("utf8"), None)
            .expect("build combination cache");
        remove_dir_all(&source_dir).expect("remove temp source");

        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].expression_programs().len(), 2);
        let combination = rules[0].combination.as_ref().expect("combination cache");
        assert_eq!(combination.conditions.len(), 2);
        assert_eq!(combination.conditions[0].bonus_points, 0.0);
        assert_eq!(combination.conditions[1].bonus_points, 1.0);
        assert_eq!(combination.points_by_hits, vec![0.0, 1.0, 3.0]);
    }
}
