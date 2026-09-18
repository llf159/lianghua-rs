use crate::data::cyq_chen::bars::compute_bucket_rate_series;
use crate::data::cyq_chen::buckets::{
    apply_weighted_sell, find_bucket_containing_price, holder_chip_entries, nearest_bucket_index,
};
use crate::data::cyq_chen::{
    ChenChipBar, ChipBucket, ChipDirection, ChipHolder, CompiledChipChangeConfig, EPS,
    PosteriorChipLot, SellEntry,
};

use crate::expr::eval::Runtime;
use crate::expr::eval::Value;
use crate::expr::parser::Stmts;
use std::sync::Arc;
pub(super) fn apply_posterior_for_day(
    buckets: &mut [ChipBucket],
    bar: &ChenChipBar,
    runtime: &mut Runtime,
    config: &CompiledChipChangeConfig,
    len: usize,
    day: usize,
) -> Result<(), String> {
    let horizon = config
        .strategies
        .iter()
        .map(|r| r.confirm_after)
        .max()
        .unwrap_or(0);
    if horizon == 0 {
        return Ok(());
    }
    let mut decisions = Vec::new();
    for rule in config.strategies.iter().filter(|r| r.confirm_after > 0) {
        let triggered = match runtime
            .eval_program_bool_at(&rule.optimized_when_ast, day)
            .map_err(|e| format!("后验规则 {}: {}", rule.name, e.msg))?
        {
            Some(value) => value,
            None => {
                let value =
                    eval_program_scoped(runtime, &rule.optimized_when_ast, &rule.assigned_names)
                        .map_err(|e| format!("后验规则 {}: {}", rule.name, e.msg))?;
                Value::as_bool_series(&value, len)
                    .map_err(|e| e.msg)?
                    .get(day)
                    .copied()
                    .unwrap_or(false)
            }
        };
        if triggered {
            decisions.push(rule);
        }
    }
    for bucket in buckets {
        for lot in &mut bucket.pending {
            if lot.trade_date == bar.trade_date {
                continue;
            }
            lot.age += 1;
            if let Some(rule) = decisions.iter().find(|r| r.confirm_after == lot.age) {
                let delta = match rule.holder {
                    ChipHolder::Main => lot.retail_chip * rule.bias,
                    ChipHolder::Retail => -lot.main_chip * rule.bias,
                };
                lot.main_chip += delta;
                lot.retail_chip -= delta;
                bucket.main_chip += delta;
                bucket.retail_chip -= delta;
            }
        }
        bucket
            .pending
            .retain(|lot| lot.age < horizon && lot.main_chip + lot.retail_chip > EPS);
    }
    Ok(())
}

pub(super) fn write_main_ratio_for_day(
    buckets: &[ChipBucket],
    main_ratio_history: &mut [Arc<Vec<Option<f64>>>],
    day_index: usize,
) {
    for (bucket, history) in buckets.iter().zip(main_ratio_history.iter_mut()) {
        let total = bucket.total_chip();
        let history_mut = Arc::make_mut(history);
        history_mut[day_index] = if total > EPS {
            Some(bucket.main_chip / total)
        } else {
            Some(0.0)
        };
    }
}

pub(super) fn apply_sell_for_day(
    buckets: &mut [ChipBucket],
    bars: &[Option<ChenChipBar>],
    bucket_runtime: &mut Runtime,
    chip_config: &CompiledChipChangeConfig,
    main_ratio_history: &[Arc<Vec<Option<f64>>>],
    day_index: usize,
    turnover_rate: f64,
) -> Result<(), String> {
    if turnover_rate <= EPS {
        return Ok(());
    }

    let mut entries = Vec::new();
    let mut has_main_bias = false;
    let mut has_retail_bias = false;
    let (main_chip_total, retail_chip_total) = holder_chip_totals(buckets);

    for bucket_index in 0..buckets.len() {
        if buckets[bucket_index].main_chip <= EPS && buckets[bucket_index].retail_chip <= EPS {
            continue;
        }

        (|runtime: &mut Runtime,
          bucket: &ChipBucket,
          main_ratio_history: Arc<Vec<Option<f64>>>,
          main_chip_total: f64,
          retail_chip_total: f64,
          bars: &[Option<ChenChipBar>],
          needs_bucket_rate_series: bool|
         -> Result<(), String> {
            if !bucket.price().is_finite() || bucket.price() <= 0.0 {
                return Err("价格分桶中点价非法".to_string());
            }
            if !main_chip_total.is_finite() || !retail_chip_total.is_finite() {
                return Err("主力/散户总筹码出现非有限数值".to_string());
            }

            if needs_bucket_rate_series {
                match (&bucket.rateo, &bucket.rateh, &bucket.ratel, &bucket.ratec) {
                    (Some(rateo), Some(rateh), Some(ratel), Some(ratec)) => {
                        set_num_series_arc_with_alias(runtime, "RATEO", "rateo", Arc::clone(rateo));
                        set_num_series_arc_with_alias(runtime, "RATEH", "rateh", Arc::clone(rateh));
                        set_num_series_arc_with_alias(runtime, "RATEL", "ratel", Arc::clone(ratel));
                        set_num_series_arc_with_alias(runtime, "RATEC", "ratec", Arc::clone(ratec));
                    }
                    _ => {
                        let cost_price = bucket.price();
                        let (rateo, rateh, ratel, ratec) =
                            compute_bucket_rate_series(bars, cost_price);
                        set_num_series_arc_with_alias(runtime, "RATEO", "rateo", rateo);
                        set_num_series_arc_with_alias(runtime, "RATEH", "rateh", rateh);
                        set_num_series_arc_with_alias(runtime, "RATEL", "ratel", ratel);
                        set_num_series_arc_with_alias(runtime, "RATEC", "ratec", ratec);
                    }
                }
            }

            set_num_series_arc_with_alias(
                runtime,
                "MAIN_CHIP_RATIO",
                "main_chip_ratio",
                main_ratio_history,
            );
            set_num_with_alias(
                runtime,
                "MAIN_CHIP_TOTAL",
                "main_chip_total",
                main_chip_total,
            );
            set_num_with_alias(
                runtime,
                "RETAIL_CHIP_TOTAL",
                "retail_chip_total",
                retail_chip_total,
            );

            Ok(())
        })(
            bucket_runtime,
            &buckets[bucket_index],
            Arc::clone(&main_ratio_history[bucket_index]),
            main_chip_total,
            retail_chip_total,
            bars,
            chip_config.sell_uses_bucket_rate_series,
        )?;
        let (main_bias, retail_bias) = strategy_biases_at(
            bucket_runtime,
            chip_config,
            ChipDirection::Sell,
            bars.len(),
            day_index,
        )?;
        let main_effective = main_bias.max(0.0);
        let retail_effective = retail_bias.max(0.0);

        if main_effective > EPS && buckets[bucket_index].main_chip > EPS {
            has_main_bias = true;
            entries.push(SellEntry {
                bucket_index,
                holder: ChipHolder::Main,
                weight: buckets[bucket_index].main_chip * main_effective,
            });
        }
        if retail_effective > EPS && buckets[bucket_index].retail_chip > EPS {
            has_retail_bias = true;
            entries.push(SellEntry {
                bucket_index,
                holder: ChipHolder::Retail,
                weight: buckets[bucket_index].retail_chip * retail_effective,
            });
        }
    }

    let mut remaining = if entries.is_empty() {
        let retail_entries = holder_chip_entries(buckets, ChipHolder::Retail);
        apply_weighted_sell(buckets, retail_entries, turnover_rate)?
    } else {
        apply_weighted_sell(buckets, entries, turnover_rate)?
    };

    if remaining <= EPS {
        return Ok(());
    }

    let bar = bars[day_index]
        .as_ref()
        .ok_or_else(|| format!("第{day_index}根K线缺少有效数据"))?;
    let retail_trapped_entries = (|buckets: &[ChipBucket], close: f64| -> Vec<SellEntry> {
        buckets
            .iter()
            .enumerate()
            .filter_map(|(bucket_index, bucket)| {
                if bucket.price() > close + EPS && bucket.retail_chip > EPS {
                    Some(SellEntry {
                        bucket_index,
                        holder: ChipHolder::Retail,
                        weight: 1.0,
                    })
                } else {
                    None
                }
            })
            .collect()
    })(buckets, bar.close);
    remaining = apply_weighted_sell(buckets, retail_trapped_entries, remaining)?;

    if remaining <= EPS {
        return Ok(());
    }

    if has_main_bias && !has_retail_bias {
        let retail_entries = holder_chip_entries(buckets, ChipHolder::Retail);
        remaining = apply_weighted_sell(buckets, retail_entries, remaining)?;
    } else if has_retail_bias && !has_main_bias {
        let main_entries = holder_chip_entries(buckets, ChipHolder::Main);
        remaining = apply_weighted_sell(buckets, main_entries, remaining)?;
    } else if !has_main_bias && !has_retail_bias {
        let main_entries = holder_chip_entries(buckets, ChipHolder::Main);
        remaining = apply_weighted_sell(buckets, main_entries, remaining)?;
    }

    if remaining > EPS {
        let all_entries = (|buckets: &[ChipBucket]| -> Vec<SellEntry> {
            let mut entries = Vec::with_capacity(buckets.len() * 2);
            entries.extend(holder_chip_entries(buckets, ChipHolder::Main));
            entries.extend(holder_chip_entries(buckets, ChipHolder::Retail));
            entries
        })(buckets);
        apply_weighted_sell(buckets, all_entries, remaining)?;
    }

    Ok(())
}

pub(super) fn apply_buy_for_day(
    buckets: &mut [ChipBucket],
    bar: &ChenChipBar,
    buy_runtime: &mut Runtime,
    chip_config: &CompiledChipChangeConfig,
    runtime_len: usize,
    day_index: usize,
    turnover_rate: f64,
) -> Result<(), String> {
    if turnover_rate <= EPS {
        return Ok(());
    }

    let weights =
        (|buckets: &[ChipBucket], bar: &ChenChipBar| -> Result<Vec<(usize, f64)>, String> {
            if buckets.is_empty() {
                return Ok(Vec::new());
            }

            if (bar.high - bar.low).abs() <= EPS {
                let index = find_bucket_containing_price(buckets, bar.close)
                    .or_else(|| find_bucket_containing_price(buckets, bar.low))
                    .unwrap_or_else(|| nearest_bucket_index(buckets, bar.close));
                return Ok(vec![(index, 1.0)]);
            }

            let center = (bar.open + bar.high + bar.low + bar.close) / 4.0;
            let center = center.clamp(bar.low, bar.high);
            let start_index = buckets.partition_point(|bucket| bucket.price_high <= bar.low + EPS);
            let mut weights = Vec::new();

            for (index, bucket) in buckets.iter().enumerate().skip(start_index) {
                if bucket.price_low >= bar.high - EPS {
                    break;
                }
                let overlap_low = bucket.price_low.max(bar.low);
                let overlap_high = bucket.price_high.min(bar.high);
                if overlap_high <= overlap_low + EPS {
                    continue;
                }
                let midpoint = (overlap_low + overlap_high) / 2.0;
                let height = (|price: f64, low: f64, center: f64, high: f64| -> f64 {
                    let slope = 2.0 / (high - low);
                    if price <= center {
                        if (center - low).abs() <= EPS {
                            slope
                        } else {
                            (price - low) / (center - low) * slope
                        }
                    } else if (high - center).abs() <= EPS {
                        slope
                    } else {
                        (high - price) / (high - center) * slope
                    }
                })(midpoint, bar.low, center, bar.high);
                let width = overlap_high - overlap_low;
                let weight = (height * width).max(0.0);
                if weight > EPS {
                    weights.push((index, weight));
                }
            }

            let total_weight = weights.iter().map(|(_, weight)| *weight).sum::<f64>();
            if !total_weight.is_finite() {
                return Err("成交价格分布权重出现非有限数值".to_string());
            }
            if total_weight <= EPS {
                let index = find_bucket_containing_price(buckets, center)
                    .unwrap_or_else(|| nearest_bucket_index(buckets, center));
                weights.push((index, 1.0));
            }

            Ok(weights)
        })(buckets, bar)?;
    let total_weight = weights.iter().map(|(_, weight)| *weight).sum::<f64>();
    if total_weight <= EPS {
        return Ok(());
    }
    let (main_chip_total, retail_chip_total) = holder_chip_totals(buckets);
    (|runtime: &mut Runtime, main_chip_total: f64, retail_chip_total: f64| {
        let total = main_chip_total + retail_chip_total;
        let main_ratio = if total > EPS {
            main_chip_total / total
        } else {
            0.5
        };
        set_num_with_alias(runtime, "MAIN_CHIP_RATIO", "main_chip_ratio", main_ratio);
        set_num_with_alias(
            runtime,
            "MAIN_CHIP_TOTAL",
            "main_chip_total",
            main_chip_total,
        );
        set_num_with_alias(
            runtime,
            "RETAIL_CHIP_TOTAL",
            "retail_chip_total",
            retail_chip_total,
        );
    })(buy_runtime, main_chip_total, retail_chip_total);
    let (main_bias, retail_bias) = strategy_biases_at(
        buy_runtime,
        chip_config,
        ChipDirection::Buy,
        runtime_len,
        day_index,
    )?;
    let (main_share, retail_share) = buy_holder_shares(main_bias, retail_bias);

    for (bucket_index, weight) in weights {
        let bucket_buy_amount = turnover_rate * weight / total_weight;

        buckets[bucket_index].main_chip += bucket_buy_amount * main_share;
        buckets[bucket_index].retail_chip += bucket_buy_amount * retail_share;
        if chip_config
            .strategies
            .iter()
            .any(|rule| rule.confirm_after > 0)
        {
            buckets[bucket_index].pending.push(PosteriorChipLot {
                trade_date: bar.trade_date.clone(),
                age: 0,
                main_chip: bucket_buy_amount * main_share,
                retail_chip: bucket_buy_amount * retail_share,
            });
        }
    }

    Ok(())
}

pub(super) fn build_new_participant_buy_runtime(
    base_runtime: &Runtime,
    bars: &[Option<ChenChipBar>],
) -> Result<Runtime, String> {
    let mut rateo = Vec::with_capacity(bars.len());
    let mut rateh = Vec::with_capacity(bars.len());
    let mut ratel = Vec::with_capacity(bars.len());
    let mut ratec = Vec::with_capacity(bars.len());
    let mut last_close: Option<f64> = None;

    for bar in bars {
        let Some(bar) = bar else {
            rateo.push(None);
            rateh.push(None);
            ratel.push(None);
            ratec.push(None);
            continue;
        };
        let reference_price = last_close.unwrap_or(bar.open);
        if !reference_price.is_finite() || reference_price <= 0.0 {
            return Err("新进买方参考价非法".to_string());
        }

        rateo.push(Some((bar.open - reference_price) / reference_price * 100.0));
        rateh.push(Some((bar.high - reference_price) / reference_price * 100.0));
        ratel.push(Some((bar.low - reference_price) / reference_price * 100.0));
        ratec.push(Some(
            (bar.close - reference_price) / reference_price * 100.0,
        ));
        last_close = Some(bar.close);
    }

    let mut runtime = base_runtime.clone();
    set_num_series_with_alias(&mut runtime, "RATEO", "rateo", rateo);
    set_num_series_with_alias(&mut runtime, "RATEH", "rateh", rateh);
    set_num_series_with_alias(&mut runtime, "RATEL", "ratel", ratel);
    set_num_series_with_alias(&mut runtime, "RATEC", "ratec", ratec);
    Ok(runtime)
}

pub(super) fn share_runtime_num_series(runtime: &mut Runtime) {
    for value in runtime.vars.values_mut() {
        if let Value::NumSeries(series) = value {
            *value = Value::SharedNumSeries(Arc::new(std::mem::take(series)));
        }
    }
}

pub(super) fn inject_strategy_expression_caches(
    runtime: &mut Runtime,
    chip_config: &CompiledChipChangeConfig,
    direction: ChipDirection,
) -> Result<(), String> {
    for strategy in chip_config
        .strategies
        .iter()
        .filter(|strategy| strategy.direction == direction)
    {
        for cached_expr in &strategy.cached_exprs {
            let value =
                eval_program_scoped(runtime, &cached_expr.program, &cached_expr.assigned_names)
                    .map_err(|error| {
                        format!("策略 {} 公共表达式预计算错误: {}", strategy.name, error.msg)
                    })?;
            runtime.vars.insert(
                cached_expr.key.clone(),
                (|value: Value| -> Value {
                    match value {
                        Value::NumSeries(series) => Value::SharedNumSeries(Arc::new(series)),
                        Value::BoolSeries(series) => Value::SharedNumSeries(Arc::new(
                            series
                                .into_iter()
                                .map(|value| Some(if value { 1.0 } else { 0.0 }))
                                .collect(),
                        )),
                        other => other,
                    }
                })(value),
            );
        }
    }
    Ok(())
}

pub(super) fn eval_program_scoped(
    runtime: &mut Runtime,
    program: &Stmts,
    assigned_names: &[String],
) -> Result<Value, crate::expr::eval::EvalErr> {
    let snapshots = assigned_names
        .iter()
        .map(|name| runtime.vars.get(name).cloned())
        .collect::<Vec<_>>();
    let result = runtime.eval_program(program);
    for (name, snapshot) in assigned_names.iter().zip(snapshots) {
        match snapshot {
            Some(value) => {
                if let Some(current) = runtime.vars.get_mut(name) {
                    *current = value;
                } else {
                    runtime.vars.insert(name.clone(), value);
                }
            }
            None => {
                runtime.vars.remove(name);
            }
        }
    }
    result
}

pub(super) fn set_runtime_value(runtime: &mut Runtime, key: &str, value: Value) {
    if let Some(current) = runtime.vars.get_mut(key) {
        *current = value;
    } else {
        runtime.vars.insert(key.to_string(), value);
    }
}

pub(super) fn set_num_with_alias(runtime: &mut Runtime, key: &str, alias: &str, value: f64) {
    set_runtime_value(runtime, key, Value::Num(value));
    set_runtime_value(runtime, alias, Value::Num(value));
}

pub(super) fn set_num_series_arc_with_alias(
    runtime: &mut Runtime,
    key: &str,
    alias: &str,
    series: Arc<Vec<Option<f64>>>,
) {
    set_runtime_value(runtime, key, Value::SharedNumSeries(Arc::clone(&series)));
    set_runtime_value(runtime, alias, Value::SharedNumSeries(series));
}

pub(super) fn set_num_series_with_alias(
    runtime: &mut Runtime,
    key: &str,
    alias: &str,
    series: Vec<Option<f64>>,
) {
    let series = Arc::new(series);
    set_num_series_arc_with_alias(runtime, key, alias, series);
}

pub(super) fn strategy_biases_at(
    bucket_runtime: &mut Runtime,
    chip_config: &CompiledChipChangeConfig,
    direction: ChipDirection,
    len: usize,
    day_index: usize,
) -> Result<(f64, f64), String> {
    let mut main_bias = 0.0;
    let mut retail_bias = 0.0;

    for strategy in chip_config
        .strategies
        .iter()
        .filter(|strategy| strategy.direction == direction && strategy.confirm_after == 0)
    {
        let triggered = match bucket_runtime
            .eval_program_bool_at(&strategy.optimized_when_ast, day_index)
            .map_err(|error| format!("策略 {} 表达式计算错误: {}", strategy.name, error.msg))?
        {
            Some(triggered) => triggered,
            None => {
                let value = eval_program_scoped(
                    bucket_runtime,
                    &strategy.optimized_when_ast,
                    &strategy.assigned_names,
                )
                .map_err(|error| format!("策略 {} 表达式计算错误: {}", strategy.name, error.msg))?;
                let triggers = Value::as_bool_series(&value, len).map_err(|error| {
                    format!("策略 {} 表达式返回值非布尔: {}", strategy.name, error.msg)
                })?;
                triggers.get(day_index).copied().unwrap_or(false)
            }
        };

        if triggered {
            match strategy.holder {
                ChipHolder::Main => main_bias += strategy.bias,
                ChipHolder::Retail => retail_bias += strategy.bias,
            }
        }
    }

    Ok((main_bias, retail_bias))
}

pub(super) fn buy_holder_shares(main_bias: f64, retail_bias: f64) -> (f64, f64) {
    let main = main_bias.max(0.0);
    let retail = retail_bias.max(0.0);
    if main > EPS && retail > EPS {
        let total = main + retail;
        (main / total, retail / total)
    } else if main > EPS {
        (1.0, 0.0)
    } else if retail > EPS {
        (0.0, 1.0)
    } else {
        (0.0, 1.0)
    }
}

pub(super) fn holder_chip_totals(buckets: &[ChipBucket]) -> (f64, f64) {
    let main_chip_total = buckets.iter().map(|bucket| bucket.main_chip).sum::<f64>();
    let retail_chip_total = buckets.iter().map(|bucket| bucket.retail_chip).sum::<f64>();
    let total = main_chip_total + retail_chip_total;
    if total <= EPS || !total.is_finite() {
        return (0.0, 0.0);
    }
    let scale = 100.0 / total;
    (main_chip_total * scale, retail_chip_total * scale)
}

#[cfg(test)]
mod tests {
    use crate::data::RowData;
    use crate::data::cyq_chen::ChenChipBar;
    use crate::data::cyq_chen::ChenChipConfig;
    use crate::data::cyq_chen::ChipBucket;
    use crate::data::cyq_chen::ChipChangeConfig;
    use crate::data::cyq_chen::ChipHolder;
    use crate::data::cyq_chen::compute_chen_chip_snapshots_from_row_data;
    use crate::data::cyq_chen::simulate::apply_buy_for_day;
    use crate::data::cyq_chen::simulate::build_new_participant_buy_runtime;
    use crate::data::cyq_chen::simulate::buy_holder_shares;
    use crate::data::cyq_chen::test_support::*;
    use crate::data::runtime::row_into_rt;
    use std::collections::HashMap;

    #[test]
    fn posterior_is_causal_conserves_mass_and_resumes() {
        let mut config = sample_config();
        config.strategy[0].holder = ChipHolder::Retail;
        let mut confirm = config.strategy[0].clone();
        confirm.holder = ChipHolder::Main;
        confirm.name = "confirm".into();
        confirm.when = "C > REF(C, 2)".into();
        confirm.confirm_after = 2;
        confirm.bias = 0.5;
        config.strategy.push(confirm);
        let compiled = config.compile().unwrap();
        let row = sample_row_data();
        let compute = ChenChipConfig {
            warmup_days: 0,
            bucket_pct: 5.0,
        };
        let full = crate::data::cyq_chen::compute_chen_chip_snapshots_with_compiled_config(
            &row,
            &row.trade_dates[0],
            &compiled,
            compute,
        )
        .unwrap();
        let mut prefix = row.clone();
        prefix.trade_dates.truncate(3);
        for values in prefix.cols.values_mut() {
            values.truncate(3);
        }
        let early = crate::data::cyq_chen::compute_chen_chip_snapshots_with_compiled_config(
            &prefix,
            &prefix.trade_dates[0],
            &compiled,
            compute,
        )
        .unwrap();
        assert_eq!(
            serde_json::to_value(&full[..3]).unwrap(),
            serde_json::to_value(&early).unwrap()
        );
        let saved = serde_json::to_string(&early[2].bins).unwrap();
        let bins: Vec<crate::data::cyq_chen::ChenChipBin> = serde_json::from_str(&saved).unwrap();
        let resumed = crate::data::cyq_chen::compute_chen_chip_snapshots_from_initial_bins_with_compiled_config(
            &row,
            &row.trade_dates[3],
            &bins,
            &[],
            &compiled,
            compute,
        )
        .unwrap();
        for (expected, actual) in full[3..].iter().zip(&resumed) {
            assert!((expected.main_total - actual.main_total).abs() < 1e-9);
            assert!((actual.total_chips - 100.0).abs() < 1e-9);
            for bin in &actual.bins {
                assert!(
                    bin.pending.iter().map(|l| l.main_chip).sum::<f64>() <= bin.main_chip + 1e-9
                );
                assert!(
                    bin.pending.iter().map(|l| l.retail_chip).sum::<f64>()
                        <= bin.retail_chip + 1e-9
                );
                assert!(bin.pending.iter().all(|l| l.age < 2));
            }
        }
        config.strategy[1].when = "LAST(C, 0) > C".into();
        assert!(config.compile().unwrap_err().contains("有限窗口因果函数"));
        config.strategy.pop();
        let prior =
            compute_chen_chip_snapshots_from_row_data(&row, &row.trade_dates[0], &config, compute)
                .unwrap();
        assert!((full[0].main_total - prior[0].main_total).abs() < 1e-9);
        assert!((full[1].main_total - prior[1].main_total).abs() < 1e-9);
        assert!(full[2].main_total > prior[2].main_total);
        for (before, after) in prior[..3].iter().zip(&full[..3]) {
            for (b, a) in before.bins.iter().zip(&after.bins) {
                assert!((b.total_chip - a.total_chip).abs() < 1e-9);
            }
        }
    }

    #[test]
    fn buy_holder_shares_use_positive_net_bias_only() {
        let (main_share, retail_share) = buy_holder_shares(1.0, 2.0);
        assert_close(main_share, 1.0 / 3.0);
        assert_close(retail_share, 2.0 / 3.0);

        let (main_share, retail_share) = buy_holder_shares(0.4, -0.6);
        assert_close(main_share, 1.0);
        assert_close(retail_share, 0.0);

        let (main_share, retail_share) = buy_holder_shares(-0.6, 1.2);
        assert_close(main_share, 0.0);
        assert_close(retail_share, 1.0);

        let (main_share, retail_share) = buy_holder_shares(-0.6, -0.2);
        assert_close(main_share, 0.0);
        assert_close(retail_share, 1.0);
    }

    #[test]
    fn buy_strategy_uses_dynamic_global_holder_fields_without_bucket_scan() {
        let chip_config = ChipChangeConfig::from_toml_str(
            r#"
    version = 1

    [[strategy]]
    name = "main keeps adding"
    holder = "main"
    direction = "buy"
    when = "MAIN_CHIP_TOTAL > RETAIL_CHIP_TOTAL AND MAIN_CHIP_TOTAL + RETAIL_CHIP_TOTAL == 100"
    bias = 1.0

    [[strategy]]
    name = "retail adds when stronger"
    holder = "retail"
    direction = "buy"
    when = "RETAIL_CHIP_TOTAL > MAIN_CHIP_TOTAL"
    bias = 1.0
    "#,
        )
        .expect("config should parse")
        .compile()
        .expect("config should compile");
        let row_data = RowData {
            trade_dates: vec!["20240102".to_string()],
            cols: HashMap::from([
                ("O".to_string(), vec![Some(10.0)]),
                ("H".to_string(), vec![Some(10.2)]),
                ("L".to_string(), vec![Some(9.8)]),
                ("C".to_string(), vec![Some(10.1)]),
                ("TOR".to_string(), vec![Some(10.0)]),
            ]),
        };
        let bars = vec![Some(ChenChipBar {
            trade_date: "20240102".to_string(),
            open: 10.0,
            high: 10.2,
            low: 9.8,
            close: 10.1,
            turnover_rate: 10.0,
        })];
        let base_runtime = row_into_rt(row_data).expect("runtime should build");
        let mut buy_runtime =
            build_new_participant_buy_runtime(&base_runtime, &bars).expect("buy runtime");
        let mut buckets = vec![
            ChipBucket {
                price_low: 9.0,
                price_high: 10.0,
                main_chip: 30.0,
                retail_chip: 20.0,
                pending: Vec::new(),
                rateo: None,
                rateh: None,
                ratel: None,
                ratec: None,
            },
            ChipBucket {
                price_low: 10.0,
                price_high: 11.0,
                main_chip: 30.0,
                retail_chip: 20.0,
                pending: Vec::new(),
                rateo: None,
                rateh: None,
                ratel: None,
                ratec: None,
            },
        ];

        apply_buy_for_day(
            &mut buckets,
            bars[0].as_ref().expect("bar"),
            &mut buy_runtime,
            &chip_config,
            bars.len(),
            0,
            10.0,
        )
        .expect("buy should apply");

        let main_total = buckets.iter().map(|bucket| bucket.main_chip).sum::<f64>();
        let retail_total = buckets.iter().map(|bucket| bucket.retail_chip).sum::<f64>();

        assert_close(main_total, 70.0);
        assert_close(retail_total, 40.0);
    }
}
