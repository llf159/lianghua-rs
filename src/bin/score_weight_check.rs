use lianghua_scoring::{
    expr::{
        eval::{Runtime, Value},
        parser::{Parser, lex_all},
    },
    scoring::{rule_cache::cache_rule_build, scoring_rules_details_cache},
};
use serde_json::json;
use std::{env, fs};

fn main() -> Result<(), String> {
    let args: Vec<_> = env::args().skip(1).collect();
    if args.len() != 5 {
        return Err(
            "用法: score_weight_check <source> <原评分> <新评分> <拟合结果.json> <校验输出.json>"
                .into(),
        );
    }
    let old_path = fs::canonicalize(&args[1]).map_err(|e| e.to_string())?;
    let new_path = fs::canonicalize(&args[2]).map_err(|e| e.to_string())?;
    let old = cache_rule_build(&args[0], Some(old_path.to_str().ok_or("路径编码")?))?;
    let new = cache_rule_build(&args[0], Some(new_path.to_str().ok_or("路径编码")?))?;
    if old.len() != new.len() {
        return Err(format!("规则数量改变: {} -> {}", old.len(), new.len()));
    }
    let result: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&args[3]).map_err(|e| e.to_string())?)
            .map_err(|e| e.to_string())?;
    let mut checked = Vec::new();
    for (before, after) in old.iter().zip(&new) {
        if before.name != after.name {
            return Err("规则顺序改变".into());
        }
        let chip = after.combination.as_ref().is_some_and(|c| {
            c.conditions
                .first()
                .is_some_and(|v| v.expression.name.ends_with("_强"))
        });
        if !chip
            && result["selected_old_multipliers"]
                .get(&after.name)
                .is_none()
        {
            continue;
        }
        let mut originals = before.clone();
        let mut fitted = after.clone();
        let dimensions = if chip {
            fitted.combination.as_ref().unwrap().conditions.len() / 4
        } else {
            0
        };
        let len = if chip {
            5_usize.pow(dimensions as u32)
        } else {
            32
        };
        let mut runtime = Runtime::default();
        runtime
            .vars
            .insert("C".into(), Value::NumSeries(vec![Some(1.0); len]));
        let mut expected = vec![50.0; len];
        for rule in [&mut originals, &mut fitted] {
            if let Some(combo) = &mut rule.combination {
                for (i, condition) in combo.conditions.iter_mut().enumerate() {
                    let key = format!("F{i}");
                    condition.expression.when_ast = Parser::new(lex_all(&key))
                        .parse_main()
                        .map_err(|e| format!("{e:?}"))?;
                    condition.expression.assigned_names.clear();
                    let hits: Vec<_> = (0..len)
                        .map(|day| {
                            if chip {
                                day / 5_usize.pow((i / 4) as u32) % 5 == i % 4
                            } else {
                                (day + i * 3) % 7 < 2
                            }
                        })
                        .collect();
                    runtime.vars.insert(key, Value::BoolSeries(hits));
                }
            } else {
                rule.when_ast = Parser::new(lex_all("F"))
                    .parse_main()
                    .map_err(|e| format!("{e:?}"))?;
                rule.assigned_names.clear();
                runtime.vars.insert(
                    "F".into(),
                    Value::BoolSeries((0..len).map(|i| i % 7 < 2).collect()),
                );
            }
        }
        if chip {
            for (i, condition) in after
                .combination
                .as_ref()
                .unwrap()
                .conditions
                .iter()
                .enumerate()
                .step_by(4)
            {
                let name = condition
                    .expression
                    .name
                    .strip_suffix("_强")
                    .ok_or("维度名称")?;
                let weight = result["selected_weights"][name]
                    .as_f64()
                    .ok_or("维度权重缺失")?;
                for (day, value) in expected.iter_mut().enumerate() {
                    let level = [1.0, 0.5, -0.5, -1.0, 0.0][day / 5_usize.pow((i / 4) as u32) % 5];
                    *value += weight * level;
                }
            }
        } else {
            let factor = result["selected_old_multipliers"][&after.name]
                .as_f64()
                .ok_or("旧规则倍率缺失")?;
            let (_, details) = scoring_rules_details_cache(&mut runtime.clone(), &[originals])?;
            for (value, contribution) in expected.iter_mut().zip(&details[0].series) {
                *value += factor * contribution;
            }
        }
        let (actual, _) = scoring_rules_details_cache(&mut runtime, &[fitted])?;
        let error = actual
            .iter()
            .zip(&expected)
            .map(|(a, b)| (a - b).abs())
            .fold(0.0_f64, f64::max);
        if error > 1e-8 {
            return Err(format!("{} 权重导出不一致: {error}", after.name));
        }
        checked.push(json!({"rule":after.name,"patterns":len,"max_error":error}));
    }
    fs::write(&args[4], serde_json::to_string_pretty(&json!({"all_rules_compiled":new.len(),"checks":checked,"scope":"existing-expression compile and score-weight semantics; fixture trigger series, not a new market backtest"})).map_err(|e| e.to_string())?).map_err(|e| e.to_string())?;
    Ok(())
}
