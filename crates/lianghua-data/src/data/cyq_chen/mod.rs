mod bars;
mod buckets;
mod config;
mod simulate;
use crate::data::cyq_chen::bars::{
    bucket_step, build_validated_bars, ensure_bucket_rate_series, expand_buckets_for_bar,
    validate_row_shape,
};
use crate::data::cyq_chen::buckets::{build_snapshot, normalize_buckets, sanitize_buckets};
use crate::data::cyq_chen::config::validate_compute_config;
use crate::data::cyq_chen::simulate::{
    apply_buy_for_day, apply_posterior_for_day, apply_sell_for_day,
    build_new_participant_buy_runtime, inject_strategy_expression_caches, share_runtime_num_series,
    write_main_ratio_for_day,
};
#[cfg(test)]
mod test_support;

pub use config::{
    collect_chen_chip_runtime_keys, estimate_chen_chip_expression_warmup,
    load_compiled_chip_change_config,
};

use crate::data::RowData;
use crate::data::runtime::row_into_rt;
use crate::expr::parser::Stmts;
use crate::utils::utils::round_f64_to_scale;
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;
pub(super) const EPS: f64 = 1e-10;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChipChangeConfig {
    pub version: u32,
    pub strategy: Vec<ChipChangeStrategy>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChipChangeStrategy {
    pub name: String,
    pub holder: ChipHolder,
    pub direction: ChipDirection,
    pub when: String,
    pub bias: f64,
    #[serde(default)]
    pub confirm_after: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ChipHolder {
    Main,
    Retail,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ChipDirection {
    Buy,
    Sell,
}

#[derive(Debug, Clone)]
pub struct CompiledChipChangeConfig {
    pub version: u32,
    pub strategies: Vec<CompiledChipChangeStrategy>,
    sell_uses_bucket_rate_series: bool,
}

#[derive(Debug, Clone)]
pub struct CompiledChipChangeStrategy {
    pub name: String,
    pub holder: ChipHolder,
    pub direction: ChipDirection,
    pub when: String,
    pub bias: f64,
    pub confirm_after: usize,
    pub when_ast: Stmts,
    optimized_when_ast: Stmts,
    cached_exprs: Vec<CompiledChipCachedExpr>,
    assigned_names: Vec<String>,
}

#[derive(Debug, Clone)]
pub(super) struct CompiledChipCachedExpr {
    key: String,
    program: Stmts,
    assigned_names: Vec<String>,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChenChipConfig {
    pub warmup_days: usize,
    pub bucket_pct: f64,
}

impl Default for ChenChipConfig {
    fn default() -> Self {
        Self {
            warmup_days: (120),
            bucket_pct: (1.0),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChenChipBin {
    pub index: usize,
    pub price: f64,
    pub price_low: f64,
    pub price_high: f64,
    pub main_chip: f64,
    pub retail_chip: f64,
    pub total_chip: f64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pending: Vec<PosteriorChipLot>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChenChipPercentRange {
    pub percent: f64,
    pub price_low: f64,
    pub price_high: f64,
    pub concentration: f64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChenChipSnapshot {
    pub trade_date: Option<String>,
    pub close: f64,
    pub min_price: f64,
    pub max_price: f64,
    pub main_total: f64,
    pub retail_total: f64,
    pub total_chips: f64,
    pub total_profit_ratio: f64,
    pub total_trapped_ratio: f64,
    pub main_profit_ratio: f64,
    pub main_trapped_ratio: f64,
    pub main_avg_cost: f64,
    pub chip_peak_price: f64,
    pub percent_70: ChenChipPercentRange,
    pub percent_90: ChenChipPercentRange,
    pub bins: Vec<ChenChipBin>,
}

pub fn round_chen_chip_value(value: f64) -> f64 {
    let rounded = round_f64_to_scale(value, 4);
    if rounded == 0.0 { 0.0 } else { rounded }
}

pub fn round_chen_chip_snapshot(snapshot: &mut ChenChipSnapshot) {
    snapshot.close = round_chen_chip_value(snapshot.close);
    snapshot.min_price = round_chen_chip_value(snapshot.min_price);
    snapshot.max_price = round_chen_chip_value(snapshot.max_price);
    snapshot.main_total = round_chen_chip_value(snapshot.main_total);
    snapshot.retail_total = round_chen_chip_value(snapshot.retail_total);
    snapshot.total_chips = round_chen_chip_value(snapshot.total_chips);
    let original_ratio_sum = snapshot.total_profit_ratio + snapshot.total_trapped_ratio;
    snapshot.total_profit_ratio = round_chen_chip_value(snapshot.total_profit_ratio);
    snapshot.total_trapped_ratio = if (original_ratio_sum - 1.0).abs() <= 1e-9 {
        round_chen_chip_value(1.0 - snapshot.total_profit_ratio)
    } else {
        round_chen_chip_value(snapshot.total_trapped_ratio)
    };
    let original_main_ratio_sum = snapshot.main_profit_ratio + snapshot.main_trapped_ratio;
    snapshot.main_profit_ratio = round_chen_chip_value(snapshot.main_profit_ratio);
    snapshot.main_trapped_ratio = if (original_main_ratio_sum - 1.0).abs() <= 1e-9 {
        round_chen_chip_value(1.0 - snapshot.main_profit_ratio)
    } else {
        round_chen_chip_value(snapshot.main_trapped_ratio)
    };
    snapshot.main_avg_cost = round_chen_chip_value(snapshot.main_avg_cost);
    snapshot.chip_peak_price = round_chen_chip_value(snapshot.chip_peak_price);
    round_chen_chip_percent_range(&mut snapshot.percent_70);
    round_chen_chip_percent_range(&mut snapshot.percent_90);
    for bin in &mut snapshot.bins {
        (|bin: &mut ChenChipBin| {
            bin.price = round_chen_chip_value(bin.price);
            bin.price_low = round_chen_chip_value(bin.price_low);
            bin.price_high = round_chen_chip_value(bin.price_high);
            bin.main_chip = round_chen_chip_value(bin.main_chip);
            bin.retail_chip = round_chen_chip_value(bin.retail_chip);
            bin.total_chip = round_chen_chip_value(bin.total_chip);
        })(bin);
    }
}

pub(super) fn round_chen_chip_percent_range(range: &mut ChenChipPercentRange) {
    range.percent = round_chen_chip_value(range.percent);
    range.price_low = round_chen_chip_value(range.price_low);
    range.price_high = round_chen_chip_value(range.price_high);
    range.concentration = round_chen_chip_value(range.concentration);
}

#[derive(Debug, Clone)]
pub(super) struct ChenChipBar {
    trade_date: String,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    turnover_rate: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PosteriorChipLot {
    pub trade_date: String,
    pub age: usize,
    pub main_chip: f64,
    pub retail_chip: f64,
}

#[derive(Debug, Clone)]
pub(super) struct ChipBucket {
    price_low: f64,
    price_high: f64,
    main_chip: f64,
    retail_chip: f64,
    pending: Vec<PosteriorChipLot>,
    rateo: Option<Arc<Vec<Option<f64>>>>,
    rateh: Option<Arc<Vec<Option<f64>>>>,
    ratel: Option<Arc<Vec<Option<f64>>>>,
    ratec: Option<Arc<Vec<Option<f64>>>>,
}

#[derive(Debug, Clone)]
pub(super) struct SellEntry {
    bucket_index: usize,
    holder: ChipHolder,
    weight: f64,
}

pub fn compute_chen_chip_snapshots_from_row_data(
    row_data: &RowData,
    output_start_date: &str,
    chip_config: &ChipChangeConfig,
    config: ChenChipConfig,
) -> Result<Vec<ChenChipSnapshot>, String> {
    let compiled = chip_config.compile()?;
    compute_chen_chip_snapshots_with_compiled_config(row_data, output_start_date, &compiled, config)
}

pub fn compute_chen_chip_snapshots_with_compiled_config(
    row_data: &RowData,
    output_start_date: &str,
    chip_config: &CompiledChipChangeConfig,
    config: ChenChipConfig,
) -> Result<Vec<ChenChipSnapshot>, String> {
    validate_compute_config(config)?;
    if chip_config.version != 1 {
        return Err(format!(
            "筹码变化策略文件 version 只支持 1，当前为 {}",
            chip_config.version
        ));
    }

    let output_start_date = output_start_date.trim();
    if output_start_date.is_empty() {
        return Err("output_start_date不能为空".to_string());
    }

    validate_row_shape(row_data)?;
    let Some(output_start_index) = row_data
        .trade_dates
        .iter()
        .position(|trade_date| trade_date == output_start_date)
    else {
        return Err(format!(
            "output_start_date 不在 RowData.trade_dates 中: {output_start_date}"
        ));
    };

    let bars = build_validated_bars(row_data, output_start_index)?;
    if output_start_index < config.warmup_days {
        return Ok(Vec::new());
    }

    let process_start_index = output_start_index.saturating_sub(config.warmup_days);
    let Some(initial_bar) = bars[process_start_index].as_ref() else {
        return Ok(Vec::new());
    };
    let initial_range = (initial_bar.low, initial_bar.high);
    let step = bucket_step(config.bucket_pct);
    let mut buckets =
        (|min_price: f64, max_price: f64, step: f64| -> Result<Vec<ChipBucket>, String> {
            if !min_price.is_finite() || !max_price.is_finite() {
                return Ok(Vec::new());
            }
            if !step.is_finite() || step <= 1.0 {
                return Err("bucket_pct必须是有限正数".to_string());
            }

            let mut boundaries = vec![min_price];
            let mut upper = min_price;
            while upper + EPS < max_price {
                upper *= step;
                if !upper.is_finite() {
                    return Err("动态分桶价格边界出现非有限数值".to_string());
                }
                if upper <= *boundaries.last().expect("boundary exists") + EPS {
                    return Err("动态分桶价格边界未递增".to_string());
                }
                boundaries.push(upper);
            }

            if boundaries.len() == 1 {
                boundaries.push(min_price * step);
            }

            let bucket_count = boundaries.len() - 1;
            let main_each = 50.0 / bucket_count as f64;
            let retail_each = 50.0 / bucket_count as f64;

            let mut buckets = Vec::with_capacity(bucket_count);
            for index in 0..bucket_count {
                buckets.push(ChipBucket {
                    price_low: boundaries[index],
                    price_high: boundaries[index + 1],
                    main_chip: main_each,
                    retail_chip: retail_each,
                    pending: Vec::new(),
                    rateo: None,
                    rateh: None,
                    ratel: None,
                    ratec: None,
                });
            }

            Ok(buckets)
        })(initial_range.0, initial_range.1, step)?;
    if buckets.is_empty() {
        return Ok(Vec::new());
    }
    if chip_config.sell_uses_bucket_rate_series {
        ensure_bucket_rate_series(&mut buckets, &bars);
    }
    let len = row_data.trade_dates.len();
    let mut main_ratio_history: Vec<Arc<Vec<Option<f64>>>> = (0..buckets.len())
        .map(|_| Arc::new(vec![None; len]))
        .collect();
    let mut base_runtime = row_into_rt(row_data.clone())?;
    share_runtime_num_series(&mut base_runtime);
    let mut buy_runtime = build_new_participant_buy_runtime(&base_runtime, &bars)?;
    inject_strategy_expression_caches(&mut base_runtime, chip_config, ChipDirection::Sell)?;
    inject_strategy_expression_caches(&mut buy_runtime, chip_config, ChipDirection::Buy)?;
    let mut snapshots = Vec::with_capacity(len.saturating_sub(output_start_index));

    for day_index in process_start_index..len {
        let Some(bar) = bars[day_index].as_ref() else {
            if day_index < output_start_index {
                return Ok(Vec::new());
            }
            return Err(format!(
                "{} 缺少有效K线数据",
                row_data.trade_dates[day_index]
            ));
        };

        expand_buckets_for_bar(
            &mut buckets,
            &mut main_ratio_history,
            len,
            bar.low,
            bar.high,
            step,
            &bars,
            chip_config.sell_uses_bucket_rate_series,
        )?;
        write_main_ratio_for_day(&buckets, &mut main_ratio_history, day_index);

        apply_sell_for_day(
            &mut buckets,
            &bars,
            &mut base_runtime,
            chip_config,
            &main_ratio_history,
            day_index,
            bar.turnover_rate,
        )?;
        sanitize_buckets(&mut buckets)?;

        apply_buy_for_day(
            &mut buckets,
            bar,
            &mut buy_runtime,
            chip_config,
            len,
            day_index,
            bar.turnover_rate,
        )?;
        apply_posterior_for_day(
            &mut buckets,
            bar,
            &mut buy_runtime,
            chip_config,
            len,
            day_index,
        )?;
        normalize_buckets(&mut buckets)?;

        if day_index >= output_start_index {
            snapshots.push(build_snapshot(bar, &buckets)?);
        }
    }

    Ok(snapshots)
}

pub fn compute_chen_chip_snapshots_from_initial_bins_with_compiled_config(
    row_data: &RowData,
    output_start_date: &str,
    initial_bins: &[ChenChipBin],
    initial_main_ratio_history: &[Arc<Vec<Option<f64>>>],
    chip_config: &CompiledChipChangeConfig,
    config: ChenChipConfig,
) -> Result<Vec<ChenChipSnapshot>, String> {
    validate_compute_config(config)?;
    if chip_config.version != 1 {
        return Err(format!(
            "筹码变化策略文件 version 只支持 1，当前为 {}",
            chip_config.version
        ));
    }

    let output_start_date = output_start_date.trim();
    if output_start_date.is_empty() {
        return Err("output_start_date不能为空".to_string());
    }
    if initial_bins.is_empty() {
        return Err("初始筹码分桶为空，无法续算".to_string());
    }

    validate_row_shape(row_data)?;
    let Some(output_start_index) = row_data
        .trade_dates
        .iter()
        .position(|trade_date| trade_date == output_start_date)
    else {
        return Err(format!(
            "output_start_date 不在 RowData.trade_dates 中: {output_start_date}"
        ));
    };

    let bars = build_validated_bars(row_data, output_start_index)?;
    let step = bucket_step(config.bucket_pct);
    let mut buckets = (|bins: &[ChenChipBin]| -> Result<Vec<ChipBucket>, String> {
        let mut buckets = Vec::with_capacity(bins.len());
        for bin in bins {
            if !bin.price_low.is_finite()
                || !bin.price_high.is_finite()
                || !bin.main_chip.is_finite()
                || !bin.retail_chip.is_finite()
                || bin.price_low <= 0.0
                || bin.price_high <= bin.price_low + EPS
            {
                return Err("初始筹码分桶非法，无法续算".to_string());
            }
            if bin.pending.iter().any(|lot| {
                !lot.main_chip.is_finite()
                    || !lot.retail_chip.is_finite()
                    || lot.main_chip < 0.0
                    || lot.retail_chip < 0.0
            }) || bin.pending.iter().map(|lot| lot.main_chip).sum::<f64>() > bin.main_chip + EPS
                || bin.pending.iter().map(|lot| lot.retail_chip).sum::<f64>()
                    > bin.retail_chip + EPS
            {
                return Err("后验续算状态非法：待确认筹码不能超过对应持有人余额".into());
            }
            buckets.push(ChipBucket {
                price_low: bin.price_low,
                price_high: bin.price_high,
                main_chip: bin.main_chip,
                retail_chip: bin.retail_chip,
                pending: bin.pending.clone(),
                rateo: None,
                rateh: None,
                ratel: None,
                ratec: None,
            });
        }

        for window in buckets.windows(2) {
            if window[0].price_high > window[1].price_low + EPS {
                return Err("初始筹码分桶价格区间重叠，无法续算".to_string());
            }
        }

        normalize_buckets(&mut buckets)?;
        Ok(buckets)
    })(initial_bins)?;
    if chip_config.sell_uses_bucket_rate_series {
        ensure_bucket_rate_series(&mut buckets, &bars);
    }
    let len = row_data.trade_dates.len();
    let mut main_ratio_history = (|initial_history: &[Arc<Vec<Option<f64>>>],
                                   bucket_count: usize,
                                   series_len: usize|
     -> Result<Vec<Arc<Vec<Option<f64>>>>, String> {
        if initial_history.is_empty() {
            return Ok((0..bucket_count)
                .map(|_| Arc::new(vec![None; series_len]))
                .collect());
        }
        if initial_history.len() != bucket_count {
            return Err("初始MAIN_CHIP_RATIO历史分桶数不匹配".to_string());
        }

        for history in initial_history {
            if history.len() != series_len {
                return Err("初始MAIN_CHIP_RATIO历史长度不匹配".to_string());
            }
        }

        Ok(initial_history.to_vec())
    })(initial_main_ratio_history, initial_bins.len(), len)?;
    let mut base_runtime = row_into_rt(row_data.clone())?;
    share_runtime_num_series(&mut base_runtime);
    let mut buy_runtime = build_new_participant_buy_runtime(&base_runtime, &bars)?;
    inject_strategy_expression_caches(&mut base_runtime, chip_config, ChipDirection::Sell)?;
    inject_strategy_expression_caches(&mut buy_runtime, chip_config, ChipDirection::Buy)?;
    let mut snapshots = Vec::with_capacity(len.saturating_sub(output_start_index));

    for day_index in output_start_index..len {
        let Some(bar) = bars[day_index].as_ref() else {
            return Err(format!(
                "{} 缺少有效K线数据",
                row_data.trade_dates[day_index]
            ));
        };

        expand_buckets_for_bar(
            &mut buckets,
            &mut main_ratio_history,
            len,
            bar.low,
            bar.high,
            step,
            &bars,
            chip_config.sell_uses_bucket_rate_series,
        )?;
        write_main_ratio_for_day(&buckets, &mut main_ratio_history, day_index);

        apply_sell_for_day(
            &mut buckets,
            &bars,
            &mut base_runtime,
            chip_config,
            &main_ratio_history,
            day_index,
            bar.turnover_rate,
        )?;
        sanitize_buckets(&mut buckets)?;

        apply_buy_for_day(
            &mut buckets,
            bar,
            &mut buy_runtime,
            chip_config,
            len,
            day_index,
            bar.turnover_rate,
        )?;
        apply_posterior_for_day(
            &mut buckets,
            bar,
            &mut buy_runtime,
            chip_config,
            len,
            day_index,
        )?;
        normalize_buckets(&mut buckets)?;

        snapshots.push(build_snapshot(bar, &buckets)?);
    }

    Ok(snapshots)
}

#[cfg(test)]
mod tests {
    use crate::data::RowData;
    use crate::data::cyq_chen::ChenChipConfig;
    use crate::data::cyq_chen::EPS;
    use crate::data::cyq_chen::compute_chen_chip_snapshots_from_row_data;
    use crate::data::cyq_chen::round_chen_chip_value;
    use crate::data::cyq_chen::test_support::*;
    use std::collections::HashMap;

    #[test]
    fn chen_chip_rounding_matches_four_decimal_format_semantics() {
        for value in [
            1.23445,
            1.23455,
            1.23425,
            -1.23445,
            -1.23455,
            0.00005,
            -0.00005,
            99.99995,
            100.00005,
            0.03125,
            0.09375,
            549_755_813_888.0,
        ] {
            let expected = format!("{value:.4}")
                .parse::<f64>()
                .expect("formatted value should parse");
            let expected = if expected == 0.0 { 0.0 } else { expected };
            assert_eq!(
                round_chen_chip_value(value).to_bits(),
                expected.to_bits(),
                "value={value:.17}"
            );
        }

        assert_eq!(round_chen_chip_value(-0.0).to_bits(), 0.0_f64.to_bits());
        assert!(round_chen_chip_value(f64::NAN).is_nan());
        assert_eq!(round_chen_chip_value(f64::INFINITY), f64::INFINITY);

        let mut state = 0x9876_5432_10fe_dcba_u64;
        for _ in 0..20_000 {
            state = state
                .wrapping_mul(2_862_933_555_777_941_757)
                .wrapping_add(3_037_000_493);
            let fraction_and_sign = state & !(0x7ff_u64 << 52);
            let exponent = ((state >> 1) % 2047) << 52;
            let value = f64::from_bits(fraction_and_sign | exponent);
            if !value.is_finite() {
                continue;
            }
            let expected = format!("{value:.4}")
                .parse::<f64>()
                .expect("formatted value should parse");
            let expected = if expected == 0.0 { 0.0 } else { expected };
            assert_eq!(
                round_chen_chip_value(value).to_bits(),
                expected.to_bits(),
                "value={value:.17e}"
            );
        }
    }

    #[test]
    fn chen_chip_returns_empty_when_warmup_is_insufficient() {
        let snapshots = compute_chen_chip_snapshots_from_row_data(
            &sample_row_data(),
            "20240104",
            &sample_config(),
            ChenChipConfig {
                warmup_days: 3,
                bucket_pct: 5.0,
            },
        )
        .expect("compute should not fail");

        assert!(snapshots.is_empty());
    }

    #[test]
    fn chen_chip_requires_output_start_date_in_row_data() {
        let error = compute_chen_chip_snapshots_from_row_data(
            &sample_row_data(),
            "20240109",
            &sample_config(),
            ChenChipConfig {
                warmup_days: 2,
                bucket_pct: 5.0,
            },
        )
        .expect_err("missing output_start_date should fail");

        assert!(error.contains("output_start_date"));
    }

    #[test]
    fn chen_chip_computes_main_retail_snapshots_and_expands_buckets() {
        let snapshots = compute_chen_chip_snapshots_from_row_data(
            &sample_row_data(),
            "20240104",
            &sample_config(),
            ChenChipConfig {
                warmup_days: 2,
                bucket_pct: 5.0,
            },
        )
        .expect("compute should succeed");

        assert_eq!(snapshots.len(), 3);
        assert_eq!(snapshots[0].trade_date.as_deref(), Some("20240104"));
        assert!(snapshots[0].main_total > snapshots[0].retail_total);
        assert_close(snapshots[0].total_chips, 100.0);
        assert_close(
            snapshots[0].main_total + snapshots[0].retail_total,
            snapshots[0].total_chips,
        );
        assert!(snapshots[1].max_price >= 11.6);
        assert!(snapshots[1].bins.iter().all(|bin| bin.main_chip >= 0.0));
        assert!(snapshots[1].bins.iter().all(|bin| bin.retail_chip >= 0.0));
        assert_close(
            snapshots[1].total_profit_ratio + snapshots[1].total_trapped_ratio,
            1.0,
        );
        assert_close(
            snapshots[1].main_profit_ratio + snapshots[1].main_trapped_ratio,
            1.0,
        );
        assert!(snapshots[1].main_profit_ratio >= 0.0);
        assert!(snapshots[1].main_profit_ratio <= 1.0);
        assert!(snapshots[1].main_avg_cost >= snapshots[1].min_price);
        assert!(snapshots[1].main_avg_cost <= snapshots[1].max_price);
        assert!(snapshots[1].chip_peak_price >= snapshots[1].min_price);
        assert!(snapshots[1].chip_peak_price <= snapshots[1].max_price);
        assert!(snapshots[1].percent_70.price_low <= snapshots[1].percent_70.price_high);
        assert!(snapshots[1].percent_90.price_low <= snapshots[1].percent_90.price_high);
        assert!(
            snapshots[1].percent_90.price_low <= snapshots[1].percent_70.price_low
                || (snapshots[1].percent_90.price_low - snapshots[1].percent_70.price_low).abs()
                    <= EPS
        );
        assert!(
            snapshots[1].percent_90.price_high >= snapshots[1].percent_70.price_high
                || (snapshots[1].percent_90.price_high - snapshots[1].percent_70.price_high).abs()
                    <= EPS
        );
        assert!(
            snapshots[1]
                .bins
                .iter()
                .any(|bin| bin.price_high > 11.0 && bin.total_chip > 0.0)
        );
    }

    #[test]
    fn chen_chip_initial_state_does_not_use_later_warmup_prices() {
        let row_data = RowData {
            trade_dates: vec![
                "20240102".to_string(),
                "20240103".to_string(),
                "20240104".to_string(),
            ],
            cols: HashMap::from([
                ("O".to_string(), vec![Some(10.0), Some(100.0), Some(100.0)]),
                ("H".to_string(), vec![Some(10.2), Some(102.0), Some(102.0)]),
                ("L".to_string(), vec![Some(9.8), Some(98.0), Some(98.0)]),
                ("C".to_string(), vec![Some(10.0), Some(100.0), Some(100.0)]),
                ("TOR".to_string(), vec![Some(0.0), Some(0.0), Some(0.0)]),
            ]),
        };

        let snapshots = compute_chen_chip_snapshots_from_row_data(
            &row_data,
            "20240104",
            &sample_config(),
            ChenChipConfig {
                warmup_days: 2,
                bucket_pct: 5.0,
            },
        )
        .expect("compute should succeed");

        assert_eq!(snapshots.len(), 1);
        assert!(snapshots[0].max_price >= 102.0);
        assert!(snapshots[0].percent_90.price_high < 20.0);
    }

    #[test]
    fn chen_chip_rejects_invalid_output_turnover() {
        let mut row_data = sample_row_data();
        row_data
            .cols
            .get_mut("TOR")
            .expect("tor")
            .get_mut(2)
            .map(|value| *value = Some(101.0));

        let error = compute_chen_chip_snapshots_from_row_data(
            &row_data,
            "20240104",
            &sample_config(),
            ChenChipConfig {
                warmup_days: 2,
                bucket_pct: 5.0,
            },
        )
        .expect_err("invalid turnover should fail");

        assert!(error.contains("换手率"));
    }
}
