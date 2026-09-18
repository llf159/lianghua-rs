//! Command-line entry point for strategy dimension research.

use std::{env, process, time::Instant};

use lianghua_app_strategy::dimension_research::{
    get_strategy_dimension_research_defaults, run_strategy_dimension_research,
};

fn main() {
    let arguments = env::args().skip(1).collect::<Vec<_>>();
    if arguments.is_empty() || arguments.len() > 8 {
        eprintln!(
            "用法: strategy_dimension_research <数据目录> [开始日期] [结束日期] [规则数] [非线性样本数] [持有交易日] [顺序] [规则列表]"
        );
        process::exit(2);
    }

    let source_path = arguments[0].clone();
    let defaults = match get_strategy_dimension_research_defaults(source_path.clone()) {
        Ok(value) => value,
        Err(error) => {
            eprintln!("读取研究默认参数失败:{error}");
            process::exit(1);
        }
    };
    let start_date = arguments
        .get(1)
        .cloned()
        .or(defaults.start_date)
        .unwrap_or_else(|| {
            eprintln!("结果库没有可用的开始日期");
            process::exit(1);
        });
    let end_date = arguments
        .get(2)
        .cloned()
        .or(defaults.end_date)
        .unwrap_or_else(|| {
            eprintln!("结果库没有可用的结束日期");
            process::exit(1);
        });
    let rule_count = arguments
        .get(3)
        .map(|value| value.parse::<usize>())
        .transpose()
        .unwrap_or_else(|error| {
            eprintln!("规则数必须是正整数:{error}");
            process::exit(2);
        })
        .unwrap_or(10);
    let nonlinear_sample_limit = arguments
        .get(4)
        .map(|value| value.parse::<usize>())
        .transpose()
        .unwrap_or_else(|error| {
            eprintln!("非线性样本数必须是正整数:{error}");
            process::exit(2);
        })
        .unwrap_or(defaults.default_nonlinear_sample_limit);
    let holding_period = arguments
        .get(5)
        .map(|value| value.parse::<usize>())
        .transpose()
        .unwrap_or_else(|error| {
            eprintln!("持有交易日必须是正整数:{error}");
            process::exit(2);
        })
        .unwrap_or(defaults.default_holding_period);

    let order = arguments.get(6).map(String::as_str).unwrap_or("frequency");
    let (rule_names, order_label) = if let Some(list) = arguments.get(7) {
        let rule_names = list
            .split(',')
            .map(|name| name.trim().to_string())
            .filter(|name| !name.is_empty())
            .collect::<Vec<_>>();
        (rule_names, "explicit".to_string())
    } else {
        let mut rule_options = defaults.rule_options;
        rule_options.sort_by(|left, right| {
            right
                .trigger_count
                .cmp(&left.trigger_count)
                .then_with(|| left.rule_name.cmp(&right.rule_name))
        });
        rule_options.truncate(rule_count);
        match order {
            "frequency" => {}
            "alphabetical" => {
                rule_options.sort_by(|left, right| left.rule_name.cmp(&right.rule_name))
            }
            "reverse_frequency" => rule_options.reverse(),
            _ => {
                eprintln!("顺序必须是 frequency、alphabetical 或 reverse_frequency");
                process::exit(2);
            }
        }
        (
            rule_options
                .into_iter()
                .map(|option| option.rule_name)
                .collect::<Vec<_>>(),
            order.to_string(),
        )
    };

    println!(
        "开始研究:区间={start_date}..{end_date},规则数={},非线性样本上限={nonlinear_sample_limit},持有={holding_period}日,顺序={order_label}",
        rule_names.len()
    );
    let started_at = Instant::now();
    let result = match run_strategy_dimension_research(
        source_path,
        start_date,
        end_date,
        rule_names,
        Some(nonlinear_sample_limit),
        None,
        Some(holding_period),
    ) {
        Ok(value) => value,
        Err(error) => {
            eprintln!("相关性与正交研究失败:{error}");
            process::exit(1);
        }
    };
    if let Ok(output_path) = env::var("LIANGHUA_DIM_RESEARCH_JSON") {
        let payload = match serde_json::to_string(&result) {
            Ok(payload) => payload,
            Err(error) => {
                eprintln!("序列化研究结果失败:{error}");
                process::exit(1);
            }
        };
        if let Err(error) = std::fs::write(&output_path, payload) {
            eprintln!("写入研究结果失败:{output_path}:{error}");
            process::exit(1);
        }
        eprintln!("已写出研究结果:{output_path}");
    }
    println!(
        "完成:耗时={:.3}s,评分宇宙={},策略={},策略对={}",
        started_at.elapsed().as_secs_f64(),
        result.universe_sample_count,
        result.strategies.len(),
        result.pair_metrics.len()
    );
    println!("收益基准指数:{}", result.return_index_ts_code);
    println!("分数加权持仓收益有效日期:");
    for summary in &result.return_summaries {
        println!(
            "  {}: days={}, mean={:.6}, HAC t={:.3}",
            summary.rule_name,
            summary.valid_day_count,
            summary.avg_residual_return.unwrap_or(f64::NAN),
            summary.hac_t_value.unwrap_or(f64::NAN)
        );
    }
    println!("市场依赖 / 收益形态:");
    for exposure in &result.style_exposures {
        println!(
            "  {}: {}",
            exposure.rule_name,
            exposure
                .dimensions
                .iter()
                .map(|dimension| {
                    format!(
                        "{}={:.4}",
                        dimension.key,
                        dimension.value.unwrap_or(f64::NAN)
                    )
                })
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    println!("样本外增量有效样本:");
    for increment in &result.return_increments {
        println!(
            "  {}: train={}, test={}, mean={:.6}, HAC t={:.3}, positive={:.2}%",
            increment.rule_name,
            increment.train_sample_count,
            increment.test_sample_count,
            increment.test_incremental_mean.unwrap_or(f64::NAN),
            increment.test_incremental_hac_t_value.unwrap_or(f64::NAN),
            increment
                .test_incremental_positive_ratio
                .map(|value| value * 100.0)
                .unwrap_or(f64::NAN)
        );
    }

    let mut pairs = result.pair_metrics.iter().collect::<Vec<_>>();
    pairs.sort_by(|left, right| {
        right
            .distance_correlation_daily_mean
            .unwrap_or(f64::NEG_INFINITY)
            .total_cmp(
                &left
                    .distance_correlation_daily_mean
                    .unwrap_or(f64::NEG_INFINITY),
            )
    });
    println!("距离相关最高的策略对:");
    for pair in pairs.into_iter().take(10) {
        println!(
            "  {} / {}: dCor={:.4}, Spearman={:.4}, Pearson={:.4}, Jaccard={:.4}, nonlinear_n={}",
            pair.left_rule_name,
            pair.right_rule_name,
            pair.distance_correlation_daily_mean.unwrap_or(f64::NAN),
            pair.score_spearman_daily_mean.unwrap_or(f64::NAN),
            pair.score_pearson_with_zeros.unwrap_or(f64::NAN),
            pair.jaccard.unwrap_or(f64::NAN),
            pair.nonlinear_sample_count
        );
    }

    println!("按上述规则顺序的线性正交残差:");
    for diagnostic in &result.orthogonal_diagnostics {
        println!(
            "  {}: residual={:.4}, explained={:.4}",
            diagnostic.rule_name,
            diagnostic.residual_variance_ratio.unwrap_or(f64::NAN),
            diagnostic.explained_variance_ratio.unwrap_or(f64::NAN)
        );
    }
    for pending in result.pending_layers {
        println!("待接入:{pending}");
    }
}
