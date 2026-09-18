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
            let (optimized_when_ast, cached_exprs) =
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
            let assigned_names = collect_assigned_names_from_expr_program(&optimized_when_ast);
            strategies.push(CompiledChipChangeStrategy {
                name: strategy.name.trim().to_string(),
                holder: strategy.holder,
                direction,
                when: strategy.when.trim().to_string(),
                bias: strategy.bias,
                confirm_after: strategy.confirm_after,
                when_ast,
                optimized_when_ast,
                cached_exprs,
                assigned_names,
            });
        }

        let sell_uses_bucket_rate_series = strategies.iter().any(|strategy| {
            strategy.direction == ChipDirection::Sell
                && (|program: &Stmts, runtime_keys: &[&str]| -> bool {
                    let mut local_dynamic = HashMap::<String, bool>::new();

                    for stmt in &program.item {
                        match stmt {
                            Stmt::Assign { name, value } => {
                                let is_dynamic =
                                    expr_depends_on_dynamic(value, &local_dynamic, runtime_keys);
                                if is_dynamic {
                                    return true;
                                }
                                local_dynamic.insert(name.clone(), false);
                            }
                            Stmt::Expr(expr) => {
                                if expr_depends_on_dynamic(expr, &local_dynamic, runtime_keys) {
                                    return true;
                                }
                            }
                        }
                    }

                    false
                })(&strategy.when_ast, &(["RATEO", "RATEH", "RATEL", "RATEC"]))
        });

        Ok(CompiledChipChangeConfig {
            version: self.version,
            strategies,
            sell_uses_bucket_rate_series,
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
        if self.version != 1 {
            return Err(format!(
                "筹码变化策略文件 version 只支持 1，当前为 {}",
                self.version
            ));
        }

        for (index, strategy) in self.strategy.iter().enumerate() {
            let n = index + 1;
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

    collect_runtime_keys_from_expr_programs(
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
    )
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
    version = 2
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
}
