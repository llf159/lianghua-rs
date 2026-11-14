use crate::data::cyq_chen::{
    ChenChipConfig, ChipChangeConfig, ChipDirection, CompiledChipCachedExpr,
    CompiledChipChangeConfig, CompiledChipChangeStrategy,
};

use crate::data::RuntimeKeyCollectOptions;
use crate::data::chip_change_rule_path;
use crate::data::collect_assigned_names_from_expr_program;
use crate::data::collect_runtime_keys_from_expr_programs;
use crate::expr::parser::Expr;
use crate::expr::parser::Stmt;
use crate::expr::parser::Stmts;
use crate::expr::validation::estimate_expression_warmup;
use crate::expr::validation::parse_expression_program;
use crate::expr::validation::validate_expression_functions;
use std::collections::HashMap;
use std::collections::HashSet;
impl CompiledChipChangeConfig {
    pub(super) fn sell_uses_point_rates_for_span(
        &self,
        input_days: usize,
        processed_days: usize,
    ) -> bool {
        // Long replays amortize vector evaluation; short continuations avoid constructing
        // bucket history for input days that will not actually advance inventory.
        self.sell_uses_point_rates && processed_days.saturating_mul(4) <= input_days
    }
}

impl ChipChangeConfig {
    pub fn load(source_dir: &str) -> Result<Self, String> {
        let path = chip_change_rule_path(source_dir);
        let text = std::fs::read_to_string(&path).map_err(|error| {
            format!(
                "筹码变化策略文件不存在或不可读: path={}, err={error}",
                path.display()
            )
        })?;
        Self::from_toml_str(&text)
    }

    pub fn from_toml_str(text: &str) -> Result<Self, String> {
        let mut config: ChipChangeConfig =
            toml::from_str(text).map_err(|error| format!("筹码变化策略文件格式错误: {error}"))?;
        config.normalize_and_validate()?;
        Ok(config)
    }

    pub fn compile(&self) -> Result<CompiledChipChangeConfig, String> {
        self.validate()?;

        let mut strategies = Vec::with_capacity(self.strategy.len());
        for (index, strategy) in self.strategy.iter().enumerate() {
            let n = index + 1;
            let when_ast = parse_strategy_expression(&strategy.when, n, &strategy.name)?;
            let direction = strategy.direction;
            let (mut optimized_when_ast, mut cached_exprs) =
                (|program: &Stmts,
                  direction: ChipDirection,
                  strategy_index: usize|
                 -> (Stmts, Vec<CompiledChipCachedExpr>) {
                    let dynamic_runtime_keys = match direction {
                        ChipDirection::Buy => {
                            (["MAIN_CHIP_RATIO", "MAIN_CHIP_TOTAL", "RETAIL_CHIP_TOTAL"]).as_slice()
                        }
                        ChipDirection::Sell => ([
                            "RATEO",
                            "RATEH",
                            "RATEL",
                            "RATEC",
                            "MAIN_CHIP_RATIO",
                            "MAIN_CHIP_TOTAL",
                            "RETAIL_CHIP_TOTAL",
                        ])
                        .as_slice(),
                    };
                    let mut local_dynamic = HashMap::<String, bool>::new();
                    let mut static_prefix = Vec::<Stmt>::new();
                    let mut cached_exprs = Vec::<CompiledChipCachedExpr>::new();
                    let mut optimized_items = Vec::with_capacity(program.item.len());

                    for stmt in &program.item {
                        match stmt {
                            Stmt::Assign { name, value } => {
                                let is_dynamic = expr_depends_on_dynamic(
                                    value,
                                    &local_dynamic,
                                    dynamic_runtime_keys,
                                );
                                let optimized_value = optimize_strategy_expr(
                                    value,
                                    &local_dynamic,
                                    dynamic_runtime_keys,
                                    &static_prefix,
                                    strategy_index,
                                    &mut cached_exprs,
                                );
                                optimized_items.push(Stmt::Assign {
                                    name: name.clone(),
                                    value: optimized_value,
                                });
                                local_dynamic.insert(name.clone(), is_dynamic);
                                if !is_dynamic {
                                    static_prefix.push(stmt.clone());
                                }
                            }
                            Stmt::Expr(expr) => {
                                optimized_items.push(Stmt::Expr(optimize_strategy_expr(
                                    expr,
                                    &local_dynamic,
                                    dynamic_runtime_keys,
                                    &static_prefix,
                                    strategy_index,
                                    &mut cached_exprs,
                                )));
                            }
                        }
                    }

                    (
                        Stmts {
                            item: optimized_items,
                        },
                        cached_exprs,
                    )
                })(&when_ast, direction, index);
            let sell_uses_bucket_state = direction == ChipDirection::Sell
                && program_depends_on_runtime_keys(
                    &when_ast,
                    &[
                        "RATEO",
                        "RATEH",
                        "RATEL",
                        "RATEC",
                        "MAIN_CHIP_RATIO",
                        "MAIN_CHIP_TOTAL",
                        "RETAIL_CHIP_TOTAL",
                    ],
                );
            // Buy RATEs describe new participants, so they are stock-only inputs.
            let stock_only = match direction {
                ChipDirection::Sell => !sell_uses_bucket_state,
                ChipDirection::Buy => !program_depends_on_runtime_keys(
                    &when_ast,
                    &["MAIN_CHIP_RATIO", "MAIN_CHIP_TOTAL", "RETAIL_CHIP_TOTAL"],
                ),
            };
            if stock_only {
                let key = format!("__CYQ_CHEN_STOCK_RULE_{index}");
                cached_exprs = vec![CompiledChipCachedExpr {
                    key: key.clone(),
                    reuse_key: None,
                    program: when_ast.clone(),
                    assigned_names: collect_assigned_names_from_expr_program(&when_ast),
                }];
                optimized_when_ast = Stmts {
                    item: vec![Stmt::Expr(Expr::Ident(key))],
                };
            }
            let assigned_names = collect_assigned_names_from_expr_program(&optimized_when_ast);
            strategies.push(CompiledChipChangeStrategy {
                name: strategy.name.trim().to_string(),
                holder: strategy.holder,
                direction,
                when: strategy.when.trim().to_string(),
                bias: strategy.bias,
                market_response: strategy.market_response,
                response_target: strategy.response_target,
                confirm_after: strategy.confirm_after,
                when_ast,
                optimized_when_ast,
                cached_exprs,
                assigned_names,
                sell_uses_bucket_state,
            });
        }

        // Compare programs once at compilation, not once per stock.
        let mut shared: Vec<(ChipDirection, Stmts, String)> = Vec::new();
        for strategy in &mut strategies {
            for cached in &mut strategy.cached_exprs {
                if let Some((_, _, key)) = shared.iter().find(|(direction, program, _)| {
                    *direction == strategy.direction && program.item == cached.program.item
                }) {
                    cached.reuse_key = Some(key.clone());
                } else {
                    shared.push((
                        strategy.direction,
                        cached.program.clone(),
                        cached.key.clone(),
                    ));
                }
            }
        }

        let buy_rate_fields = ["RATEO", "RATEH", "RATEL", "RATEC"].map(|key| {
            strategies.iter().any(|strategy| {
                strategy.direction == ChipDirection::Buy
                    && program_depends_on_runtime_keys(&strategy.when_ast, &[key])
            })
        });
        let posterior_horizon = strategies
            .iter()
            .map(|strategy| strategy.confirm_after)
            .max()
            .unwrap_or(0);
        let sell_rate_fields = ["RATEO", "RATEH", "RATEL", "RATEC"].map(|key| {
            strategies.iter().any(|strategy| {
                strategy.direction == ChipDirection::Sell
                    && program_depends_on_runtime_keys(&strategy.when_ast, &[key])
            })
        });
        let sell_uses_main_ratio_history = strategies.iter().any(|strategy| {
            strategy.direction == ChipDirection::Sell
                && ["MAIN_CHIP_RATIO", "main_chip_ratio"]
                    .iter()
                    .any(|key| crate::data::expr_program_uses_runtime_key(&strategy.when_ast, key))
        });
        let sell_uses_bucket_dynamic_state = strategies.iter().any(|strategy| {
            strategy.direction == ChipDirection::Sell
                && [
                    "MAIN_CHIP_RATIO",
                    "main_chip_ratio",
                    "MAIN_CHIP_TOTAL",
                    "main_chip_total",
                    "RETAIL_CHIP_TOTAL",
                    "retail_chip_total",
                ]
                .iter()
                .any(|key| crate::data::expr_program_uses_runtime_key(&strategy.when_ast, key))
        });
        let sell_uses_point_rates = !sell_uses_bucket_dynamic_state
            && strategies
                .iter()
                .filter(|strategy| strategy.direction == ChipDirection::Sell)
                .all(|strategy| {
                    strategy.optimized_when_ast.item.iter().all(|stmt| {
                        expr_supports_point_evaluation(match stmt {
                            Stmt::Assign { value, .. } => value,
                            Stmt::Expr(expr) => expr,
                        })
                    })
                });
        Ok(CompiledChipChangeConfig {
            version: self.version,
            negative_evidence_scale: self.negative_evidence_scale.unwrap_or(3.0),
            independent_buy: self.independent_buy,
            strategies,
            sell_rate_fields,
            buy_rate_fields,
            posterior_horizon,
            sell_uses_point_rates,
            sell_uses_main_ratio_history,
            sell_uses_bucket_dynamic_state,
        })
    }

    fn normalize_and_validate(&mut self) -> Result<(), String> {
        for strategy in &mut self.strategy {
            strategy.name = strategy.name.trim().to_string();
            strategy.when = strategy.when.trim().to_string();
        }
        self.validate()
    }

    fn validate(&self) -> Result<(), String> {
        if let Some(scale) = self.negative_evidence_scale {
            if self.version != 2 || !scale.is_finite() || scale < 0.0 {
                return Err("negative_evidence_scale 仅支持版本 2，且必须为非负有限数值".into());
            }
        }
        if !matches!(self.version, 1 | 2 | 3) {
            return Err(format!(
                "筹码变化策略文件 version 只支持 1、2 或 3，当前为 {}",
                self.version
            ));
        }

        match (self.version, self.independent_buy) {
            (3, Some(model))
                if model.positive_strength.is_finite()
                    && (0.0..=1.0).contains(&model.positive_strength)
                    && model.negative_strength.is_finite()
                    && (0.0..=1.0).contains(&model.negative_strength)
                    && model.initial_main_pct.is_finite()
                    && (0.0..=100.0).contains(&model.initial_main_pct) => {}
            (3, _) => return Err("版本 3 必须配置 independent_buy，正负力度为 [0,1]、初始主力占比为 [0,100] 的有限数值".into()),
            (_, Some(_)) => return Err("independent_buy 仅支持版本 3".into()),
            _ => {}
        }
        if let Some(response) = self
            .independent_buy
            .and_then(|model| model.positive_market_response)
        {
            response.validate()?;
        }
        for (index, strategy) in self.strategy.iter().enumerate() {
            let n = index + 1;
            if strategy.response_target.is_some()
                && (self.version != 3
                    || strategy.holder != super::ChipHolder::Main
                    || strategy.direction != ChipDirection::Buy
                    || strategy.confirm_after != 0)
            {
                return Err(format!("第{n}个策略的响应目标仅支持版本3主力买入"));
            }
            if let Some(response) = strategy.market_response {
                response.validate()?;
                if self.version != 3
                    || strategy.holder != super::ChipHolder::Main
                    || strategy.direction != ChipDirection::Buy
                    || strategy.confirm_after != 0
                {
                    return Err(format!(
                        "第{n}个策略的市场响应仅支持版本3的主力买入方向证据"
                    ));
                }
            }
            if strategy.name.trim().starts_with("RESEARCH_") {
                return Err(format!("第{n}个策略含研究标记，不能导入生产"));
            }
            if matches!(self.version, 2 | 3)
                && (strategy.confirm_after > 0
                    || (strategy.direction == ChipDirection::Buy
                        && strategy.holder != super::ChipHolder::Main))
            {
                return Err(format!(
                    "第{n}个策略不符合方向证据模式：仅支持主力买入方向证据，且不能使用后验归属"
                ));
            }
            if strategy.name.trim().is_empty() {
                return Err(format!("第{n}个strategy的name字段为空"));
            }
            if strategy.when.trim().is_empty() {
                return Err(format!("第{n}个strategy的when字段为空"));
            }
            if !strategy.bias.is_finite() {
                return Err(format!("第{n}个strategy的bias必须是有限数值"));
            }
            let program = parse_strategy_expression(strategy.when.trim(), n, strategy.name.trim())?;
            if strategy.confirm_after > 0 {
                if strategy.direction != ChipDirection::Buy || !(0.0..=1.0).contains(&strategy.bias)
                {
                    return Err(format!(
                        "第{n}个后验规则必须 direction=buy，bias 为 [0,1] 的归属修正比例"
                    ));
                }
                let mut expressions = program
                    .item
                    .iter()
                    .map(|stmt| match stmt {
                        Stmt::Assign { value, .. } => value,
                        Stmt::Expr(expr) => expr,
                    })
                    .collect::<Vec<_>>();
                while let Some(expr) = expressions.pop() {
                    match expr {
                        Expr::Call { name, args } => {
                            if !matches!(
                                name.to_ascii_uppercase().as_str(),
                                "REF"
                                    | "HHV"
                                    | "LLV"
                                    | "MA"
                                    | "SUM"
                                    | "COUNT"
                                    | "ABS"
                                    | "MAX"
                                    | "MIN"
                                    | "CROSS"
                                    | "RSV"
                            ) {
                                return Err(format!(
                                    "第{n}个后验规则只支持有限窗口因果函数，不支持 {name}"
                                ));
                            }
                            expressions.extend(args);
                        }
                        Expr::Unary { rhs, .. } => expressions.push(rhs),
                        Expr::Binary { lhs, rhs, .. } => {
                            expressions.push(lhs);
                            expressions.push(rhs);
                        }
                        _ => {}
                    }
                }
                estimate_expression_warmup(&program)?;
                let keys = collect_runtime_keys_from_expr_programs(
                    &[&program],
                    RuntimeKeyCollectOptions {
                        always_keys: &[],
                        injected_keys: &[],
                        aliases: &[],
                    },
                );
                if keys.iter().any(|key| {
                    key.starts_with("MAIN_CHIP")
                        || key == "RETAIL_CHIP_TOTAL"
                        || key.starts_with("CYQ_")
                }) {
                    return Err(format!(
                        "第{n}个后验规则只能使用行情证据，不能循环依赖筹码归属"
                    ));
                }
            }
        }

        Ok(())
    }
}

pub(super) fn round_ratio(value: f64) -> f64 {
    (value * 1_000_000_000.0).round() / 1_000_000_000.0
}

pub fn load_compiled_chip_change_config(
    source_dir: &str,
) -> Result<CompiledChipChangeConfig, String> {
    ChipChangeConfig::load(source_dir)?.compile()
}

pub fn collect_chen_chip_runtime_keys(chip_config: &CompiledChipChangeConfig) -> HashSet<String> {
    let programs = chip_config
        .strategies
        .iter()
        .map(|strategy| &strategy.when_ast)
        .collect::<Vec<_>>();

    let mut keys = collect_runtime_keys_from_expr_programs(
        &programs,
        RuntimeKeyCollectOptions {
            always_keys: &(["O", "H", "L", "C", "TOR"]),
            injected_keys: &([
                "RATEO",
                "RATEH",
                "RATEL",
                "RATEC",
                "MAIN_CHIP_RATIO",
                "MAIN_CHIP_TOTAL",
                "RETAIL_CHIP_TOTAL",
                "ZHANG",
                "TOTAL_MV_YI",
            ]),
            aliases: &[],
        },
    );
    for response in chip_config
        .strategies
        .iter()
        .filter_map(|strategy| strategy.market_response)
        .chain(
            chip_config
                .independent_buy
                .and_then(|model| model.positive_market_response),
        )
    {
        keys.insert(response.runtime_key().to_string());
    }
    keys
}

pub fn estimate_chen_chip_expression_warmup(
    chip_config: &CompiledChipChangeConfig,
) -> Result<usize, String> {
    let mut max_warmup = 0usize;

    for strategy in &chip_config.strategies {
        max_warmup = max_warmup.max(estimate_expression_warmup(&strategy.when_ast)?);
    }

    Ok(max_warmup)
}

pub(super) fn parse_strategy_expression(
    expression: &str,
    strategy_index: usize,
    strategy_name: &str,
) -> Result<Stmts, String> {
    let program = parse_expression_program(expression).map_err(|error| {
        format!(
            "第{strategy_index}个strategy({strategy_name})表达式解析错误在{}:{}",
            error.idx, error.msg
        )
    })?;
    validate_expression_functions(&program)
        .map_err(|error| format!("第{strategy_index}个strategy({strategy_name}){error}"))?;
    Ok(program)
}

pub(super) fn optimize_strategy_expr(
    expr: &Expr,
    local_dynamic: &HashMap<String, bool>,
    dynamic_runtime_keys: &[&str],
    static_prefix: &[Stmt],
    strategy_index: usize,
    cached_exprs: &mut Vec<CompiledChipCachedExpr>,
) -> Expr {
    if expr_contains_call(expr)
        && !expr_depends_on_dynamic(expr, local_dynamic, dynamic_runtime_keys)
    {
        let cache_index = cached_exprs.len();
        let key = format!("__CYQ_CHEN_CACHE_{strategy_index}_{cache_index}");
        let mut items = Vec::with_capacity(static_prefix.len() + 1);
        items.extend_from_slice(static_prefix);
        items.push(Stmt::Expr(expr.clone()));
        let program = Stmts { item: items };
        let assigned_names = collect_assigned_names_from_expr_program(&program);
        cached_exprs.push(CompiledChipCachedExpr {
            key: key.clone(),
            reuse_key: None,
            program,
            assigned_names,
        });
        return Expr::Ident(key);
    }

    match expr {
        Expr::Number(_) | Expr::Ident(_) => expr.clone(),
        Expr::Call { name, args } => Expr::Call {
            name: name.clone(),
            args: args
                .iter()
                .map(|arg| {
                    optimize_strategy_expr(
                        arg,
                        local_dynamic,
                        dynamic_runtime_keys,
                        static_prefix,
                        strategy_index,
                        cached_exprs,
                    )
                })
                .collect(),
        },
        Expr::Unary { op, rhs } => Expr::Unary {
            op: op.clone(),
            rhs: Box::new(optimize_strategy_expr(
                rhs,
                local_dynamic,
                dynamic_runtime_keys,
                static_prefix,
                strategy_index,
                cached_exprs,
            )),
        },
        Expr::Binary { op, lhs, rhs } => Expr::Binary {
            op: op.clone(),
            lhs: Box::new(optimize_strategy_expr(
                lhs,
                local_dynamic,
                dynamic_runtime_keys,
                static_prefix,
                strategy_index,
                cached_exprs,
            )),
            rhs: Box::new(optimize_strategy_expr(
                rhs,
                local_dynamic,
                dynamic_runtime_keys,
                static_prefix,
                strategy_index,
                cached_exprs,
            )),
        },
    }
}

pub(super) fn expr_contains_call(expr: &Expr) -> bool {
    match expr {
        Expr::Call { .. } => true,
        Expr::Unary { rhs, .. } => expr_contains_call(rhs),
        Expr::Binary { lhs, rhs, .. } => expr_contains_call(lhs) || expr_contains_call(rhs),
        Expr::Number(_) | Expr::Ident(_) => false,
    }
}

pub(super) fn expr_depends_on_dynamic(
    expr: &Expr,
    local_dynamic: &HashMap<String, bool>,
    dynamic_runtime_keys: &[&str],
) -> bool {
    match expr {
        Expr::Number(_) => false,
        Expr::Ident(name) => local_dynamic.get(name).copied().unwrap_or_else(|| {
            let runtime_key = name.to_ascii_uppercase();
            dynamic_runtime_keys.contains(&runtime_key.as_str())
        }),
        Expr::Call { args, .. } => args
            .iter()
            .any(|arg| expr_depends_on_dynamic(arg, local_dynamic, dynamic_runtime_keys)),
        Expr::Unary { rhs, .. } => {
            expr_depends_on_dynamic(rhs, local_dynamic, dynamic_runtime_keys)
        }
        Expr::Binary { lhs, rhs, .. } => {
            expr_depends_on_dynamic(lhs, local_dynamic, dynamic_runtime_keys)
                || expr_depends_on_dynamic(rhs, local_dynamic, dynamic_runtime_keys)
        }
    }
}

fn program_depends_on_runtime_keys(program: &Stmts, runtime_keys: &[&str]) -> bool {
    let mut locals = HashMap::new();
    for stmt in &program.item {
        let (name, expr) = match stmt {
            Stmt::Assign { name, value } => (Some(name), value),
            Stmt::Expr(expr) => (None, expr),
        };
        let dependent = expr_depends_on_dynamic(expr, &locals, runtime_keys);
        if dependent {
            return true;
        }
        if let Some(name) = name {
            locals.insert(name.clone(), false);
        }
    }
    false
}

fn expr_supports_point_evaluation(expr: &Expr) -> bool {
    match expr {
        Expr::Call { name, args } => {
            matches!(name.to_ascii_uppercase().as_str(), "ABS" | "MIN" | "MAX")
                && args.iter().all(expr_supports_point_evaluation)
        }
        Expr::Unary { rhs, .. } => expr_supports_point_evaluation(rhs),
        Expr::Binary { lhs, rhs, .. } => {
            expr_supports_point_evaluation(lhs) && expr_supports_point_evaluation(rhs)
        }
        Expr::Number(_) | Expr::Ident(_) => true,
    }
}

pub(super) fn validate_compute_config(config: ChenChipConfig) -> Result<(), String> {
    if !config.bucket_pct.is_finite() || config.bucket_pct <= 0.0 {
        return Err("bucket_pct必须是有限正数".to_string());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::data::cyq_chen::ChenChipConfig;
    use crate::data::cyq_chen::ChipChangeConfig;
    use crate::data::cyq_chen::ChipDirection;
    use crate::data::cyq_chen::ChipHolder;
    use crate::data::cyq_chen::test_support::*;

    #[test]
    fn chip_strategy_caches_static_history_subexpressions_but_not_rate_history() {
        let config = ChipChangeConfig::from_toml_str(
            r#"
    version = 1

    [[strategy]]
    name = "mixed static history"
    holder = "main"
    direction = "sell"
    when = "M := 2; RATEH > 20 AND HHV(H, M) > REF(C, 1)"
    bias = 1.0

    [[strategy]]
    name = "bucket history"
    holder = "main"
    direction = "sell"
    when = "HHV(RATEO, 10) > 30 AND REF(C, 1) > 0"
    bias = 1.0
    "#,
        )
        .expect("config should parse")
        .compile()
        .expect("config should compile");

        let mixed = &config.strategies[0];
        assert_eq!(mixed.cached_exprs.len(), 1);
        assert!(!program_contains_call(&mixed.optimized_when_ast));

        let bucket_history = &config.strategies[1];
        assert_eq!(bucket_history.cached_exprs.len(), 1);
        assert!(program_contains_call(&bucket_history.optimized_when_ast));
    }

    #[test]
    fn cached_static_subexpressions_preserve_chip_results() {
        let config = ChipChangeConfig::from_toml_str(
            r#"
    version = 1

    [[strategy]]
    name = "mixed sell"
    holder = "retail"
    direction = "sell"
    when = "M := 2; RATEH > 5 AND HHV(H, M) > REF(C, 1)"
    bias = 1.0

    [[strategy]]
    name = "history buy"
    holder = "main"
    direction = "buy"
    when = "COUNT(C > O, 2) > 0"
    bias = 1.0
    "#,
        )
        .expect("config should parse")
        .compile()
        .expect("config should compile");
        let mut uncached_config = config.clone();
        uncached_config.sell_uses_point_rates = false;
        for strategy in &mut uncached_config.strategies {
            strategy.optimized_when_ast = strategy.when_ast.clone();
            strategy.cached_exprs.clear();
        }
        let row_data = sample_row_data();
        let compute_config = ChenChipConfig {
            warmup_days: 2,
            bucket_pct: 5.0,
        };

        let cached = crate::data::cyq_chen::compute_chen_chip_snapshots_with_compiled_config(
            &row_data,
            "20240104",
            &config,
            compute_config,
        )
        .expect("cached compute should succeed");
        let uncached = crate::data::cyq_chen::compute_chen_chip_snapshots_with_compiled_config(
            &row_data,
            "20240104",
            &uncached_config,
            compute_config,
        )
        .expect("uncached compute should succeed");

        assert_eq!(
            serde_json::to_string(&cached).expect("serialize cached"),
            serde_json::to_string(&uncached).expect("serialize uncached")
        );
    }

    #[test]
    fn point_rates_preserve_snapshots_and_production_selects_by_replay_span() {
        let production = ChipChangeConfig::from_toml_str(include_str!(
            "../../../../../docs/chip-independent-buy.toml"
        ))
        .unwrap()
        .compile()
        .unwrap();
        assert!(production.sell_uses_point_rates);
        assert!(production.sell_uses_point_rates_for_span(240, 1));
        assert!(!production.sell_uses_point_rates_for_span(1342, 1037));
        assert_eq!(production.sell_rate_fields, [false, false, false, true]);

        let config = ChipChangeConfig::from_toml_str(
            r#"
version = 1
[[strategy]]
name = "stock main"
holder = "main"
direction = "sell"
when = "C > O"
bias = 0.3
[[strategy]]
name = "bucket main"
holder = "main"
direction = "sell"
when = "R := RATEC; R >= MAX(-5, MIN(5, RATEH)) OR ABS(RATEL) > 1"
bias = 0.7
[[strategy]]
name = "stock retail"
holder = "retail"
direction = "sell"
when = "C > O"
bias = 0.9
[[strategy]]
name = "bucket retail"
holder = "retail"
direction = "sell"
when = "RATEO > REF(C, 1) / C - 1"
bias = -0.1
[[strategy]]
name = "buy"
holder = "main"
direction = "buy"
when = "C > REF(C, 1)"
bias = 0.7
"#,
        )
        .unwrap()
        .compile()
        .unwrap();
        assert!(config.sell_uses_point_rates);
        let mut historical = config.clone();
        historical.sell_uses_point_rates = false;
        let row = sample_row_data();
        let settings = ChenChipConfig {
            warmup_days: 0,
            bucket_pct: 1.0,
        };
        let start = row.trade_dates.last().unwrap();
        let point = crate::data::cyq_chen::compute_chen_chip_snapshots_with_compiled_config(
            &row, start, &config, settings,
        )
        .unwrap();
        let full = crate::data::cyq_chen::compute_chen_chip_snapshots_with_compiled_config(
            &row,
            start,
            &historical,
            settings,
        )
        .unwrap();
        assert!(!point.is_empty());
        assert_eq!(
            serde_json::to_string(&point).unwrap(),
            serde_json::to_string(&full).unwrap()
        );
    }

    #[test]
    fn chip_change_config_parses_and_validates_expression() {
        let config = ChipChangeConfig::from_toml_str(
            r#"
    version = 1

    [[strategy]]
    name = "主力低位承接"
    holder = "main"
    direction = "buy"
    when = "RATEL < -8 AND C > O"
    bias = 1.5
    "#,
        )
        .expect("config should parse");

        assert_eq!(config.strategy.len(), 1);
        assert_eq!(config.strategy[0].holder, ChipHolder::Main);
        assert_eq!(config.strategy[0].direction, ChipDirection::Buy);
        assert_eq!(config.compile().expect("compile").strategies.len(), 1);
    }

    #[test]
    fn chip_change_config_rejects_bad_version() {
        let version_error = ChipChangeConfig::from_toml_str(
            r#"
    version = 4
    strategy = []
    "#,
        )
        .expect_err("version should fail");
        assert!(version_error.contains("version"));
    }

    #[test]
    fn chip_change_config_allows_negative_buy_bias() {
        let config = ChipChangeConfig::from_toml_str(
            r#"
    version = 1

    [[strategy]]
    name = "bad"
    holder = "main"
    direction = "buy"
    when = "C > O"
    bias = -0.5
    "#,
        )
        .expect("negative buy bias should parse");

        assert_eq!(config.strategy[0].bias, -0.5);
    }

    #[test]
    fn chip_change_config_rejects_bad_expression() {
        let error = ChipChangeConfig::from_toml_str(
            r#"
    version = 1

    [[strategy]]
    name = "bad expr"
    holder = "retail"
    direction = "sell"
    when = "C >"
    bias = 1.0
    "#,
        )
        .expect_err("expression should fail");
        assert!(error.contains("表达式解析错误"));
    }
    #[test]
    fn inventory_config_validates_production_rules_and_rejects_unsupported_identity_updates() {
        let mut config = ChipChangeConfig::from_toml_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../docs/chip-inventory-memory.toml"
        )))
        .unwrap();
        assert_eq!(config.version, 2);
        assert_eq!(config.compile().unwrap().strategies.len(), 64);
        assert_eq!(config.compile().unwrap().negative_evidence_scale, 3.0);
        assert!(
            !toml::to_string(&config)
                .unwrap()
                .contains("negative_evidence_scale")
        );
        config.negative_evidence_scale = Some(1.0);
        let restored = ChipChangeConfig::from_toml_str(&toml::to_string(&config).unwrap()).unwrap();
        assert_eq!(restored.compile().unwrap().negative_evidence_scale, 1.0);
        for scale in [-1.0, f64::NAN, f64::INFINITY] {
            config.negative_evidence_scale = Some(scale);
            assert!(
                config
                    .compile()
                    .unwrap_err()
                    .contains("negative_evidence_scale")
            );
        }
        config.negative_evidence_scale = None;
        let buy = config
            .strategy
            .iter()
            .position(|r| r.direction == ChipDirection::Buy)
            .unwrap();
        config.strategy[buy].holder = ChipHolder::Retail;
        assert!(config.compile().unwrap_err().contains("仅支持主力买入"));
        config.strategy[buy].holder = ChipHolder::Main;
        config.strategy[buy].confirm_after = 1;
        assert!(config.compile().unwrap_err().contains("后验归属"));
        config.strategy[buy].confirm_after = 0;
        config.strategy[buy].name = "RESEARCH_BALANCED_MEMORY".into();
        config.version = 1;
        assert!(config.compile().unwrap_err().contains("研究标记"));
    }
    #[test]
    fn independent_buy_requires_explicit_valid_parameters_and_direction_rules() {
        let mut config = super::super::test_support::sample_config();
        config.version = 3;
        assert!(config.compile().unwrap_err().contains("independent_buy"));
        config.independent_buy = Some(super::super::IndependentBuyConfig {
            positive_strength: 0.2,
            negative_strength: 0.7,
            initial_main_pct: 20.0,
            positive_market_response: None,
        });
        assert!(config.compile().is_ok());
        let restored = ChipChangeConfig::from_toml_str(&toml::to_string(&config).unwrap()).unwrap();
        assert_eq!(restored.independent_buy, config.independent_buy);
        for bad in [-1.0, 1.1, f64::NAN, f64::INFINITY] {
            config.independent_buy.as_mut().unwrap().positive_strength = bad;
            assert!(config.compile().is_err());
        }
        config.independent_buy.as_mut().unwrap().positive_strength = 0.2;
        config.independent_buy.as_mut().unwrap().initial_main_pct = 101.0;
        assert!(config.compile().is_err());
        config.independent_buy.as_mut().unwrap().initial_main_pct = 0.0;
        config.strategy[0].holder = super::super::ChipHolder::Retail;
        assert!(config.compile().unwrap_err().contains("仅支持主力买入"));
        config.version = 2;
        assert!(config.compile().unwrap_err().contains("仅支持版本 3"));
    }
}
