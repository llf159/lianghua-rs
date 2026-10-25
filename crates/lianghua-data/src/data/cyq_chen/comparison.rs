use super::{
    ChenChipConfig, ChenChipFeatureState, ChenChipOrigin, ChenChipSnapshot, ChipChangeConfig,
    ChipChangeStrategy, ChipDirection, ChipHolder, CompiledChipChangeConfig, EPS,
    bars::bucket_step, bars::build_initial_buckets, bars::build_validated_bars,
    compute_chen_chip_snapshots_with_compiled_config, replay_chen_chip_snapshots,
};
use crate::data::RowData;
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChenChipMechanismComparison {
    pub simulation_start_date: Option<String>,
    /// Historical float-share observations and corporate actions have not been verified.
    pub universe_verified: bool,
    pub days: Vec<ChenChipMechanismDay>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChenChipMechanismDay {
    pub without_posterior_origin: Option<ChenChipOrigin>,
    pub trade_date: String,
    pub current_avg_cost: f64,
    pub without_posterior_avg_cost: f64,
    pub reference_imputed_avg_cost: f64,
    pub current_profit_ratio: f64,
    pub without_posterior_profit_ratio: f64,
    pub reference_imputed_profit_ratio: f64,
    pub without_posterior_main_total: f64,
    pub posterior_cdf_distance: f64,
    pub reference_cdf_distance: f64,
    pub reference_unknown_ratio: f64,
    pub reference_known_avg_cost: Option<f64>,
    pub reference_profit_ratio_lower: f64,
    pub reference_profit_ratio_upper: f64,
}

impl ChipChangeConfig {
    /// The two holder labels are equal containers, with no identity interpretation.
    pub fn uniform_turnover() -> Self {
        Self {
            version: 1,
            strategy: [ChipDirection::Buy, ChipDirection::Sell]
                .into_iter()
                .flat_map(|direction| {
                    [ChipHolder::Main, ChipHolder::Retail]
                        .into_iter()
                        .map(move |holder| ChipChangeStrategy {
                            name: format!("等概率换手 {holder:?} {direction:?}"),
                            holder,
                            direction,
                            when: "C > 0".into(),
                            bias: 1.0,
                            confirm_after: 0,
                        })
                })
                .collect(),
        }
    }
}

fn distribution_metrics(snapshot: &ChenChipSnapshot) -> (f64, f64) {
    let cost = snapshot
        .bins
        .iter()
        .map(|bin| bin.price * bin.total_chip)
        .sum::<f64>()
        / snapshot.total_chips;
    let profit = snapshot
        .bins
        .iter()
        .filter(|bin| bin.price <= snapshot.close + EPS)
        .map(|bin| bin.total_chip)
        .sum::<f64>()
        / snapshot.total_chips;
    (cost, profit)
}

fn cdf_distance(left: &ChenChipSnapshot, right: &ChenChipSnapshot) -> Result<f64, String> {
    if left.trade_date != right.trade_date || left.bins.len() != right.bins.len() {
        return Err("筹码机制对照的日期或价格网格不一致".into());
    }
    let mut left_cdf = 0.0;
    let mut right_cdf = 0.0;
    let mut distance: f64 = 0.0;
    for (left_bin, right_bin) in left.bins.iter().zip(&right.bins) {
        if left_bin.price_low.to_bits() != right_bin.price_low.to_bits()
            || left_bin.price_high.to_bits() != right_bin.price_high.to_bits()
        {
            return Err("筹码机制对照的价格网格不一致".into());
        }
        left_cdf += left_bin.total_chip / left.total_chips;
        right_cdf += right_bin.total_chip / right.total_chips;
        distance = distance.max((left_cdf - right_cdf).abs());
    }
    Ok(distance)
}

/// Read-only comparison on identical inputs, initialization and price grids.
/// Unknown mass is exact only for the uniform-turnover reference and a fixed universe.
/// The reference's imputed initial prices are removed from the known-cost statistics.
pub fn compute_chen_chip_mechanism_comparison(
    row_data: &RowData,
    output_start_date: &str,
    chip_config: &CompiledChipChangeConfig,
    config: ChenChipConfig,
) -> Result<(Vec<ChenChipSnapshot>, ChenChipMechanismComparison), String> {
    let current = compute_chen_chip_snapshots_with_compiled_config(
        row_data,
        output_start_date,
        chip_config,
        config,
    )?;
    let mut comparison = ChenChipMechanismComparison {
        simulation_start_date: None,
        universe_verified: false,
        days: Vec::with_capacity(current.len()),
    };
    if current.is_empty() {
        return Ok((current, comparison));
    }

    let mut without_posterior = chip_config.clone();
    without_posterior
        .strategies
        .retain(|strategy| strategy.confirm_after == 0);
    // Reduce each emitted snapshot immediately; only the current model retains historical bins.
    replay_chen_chip_snapshots(
        std::borrow::Cow::Borrowed(row_data),
        output_start_date,
        &without_posterior,
        config,
        &mut ChenChipFeatureState::default(),
        false,
        |without| {
            let original = current
                .get(comparison.days.len())
                .ok_or_else(|| "关闭归属修正后快照数量改变，无法比较".to_string())?;
            let (current_avg_cost, current_profit_ratio) = distribution_metrics(original);
            let (without_posterior_avg_cost, without_posterior_profit_ratio) =
                distribution_metrics(&without);
            comparison.days.push(ChenChipMechanismDay {
                without_posterior_origin: without.origin,
                trade_date: original.trade_date.clone().unwrap_or_default(),
                current_avg_cost,
                without_posterior_avg_cost,
                reference_imputed_avg_cost: 0.0,
                current_profit_ratio,
                without_posterior_profit_ratio,
                reference_imputed_profit_ratio: 0.0,
                without_posterior_main_total: without.main_total,
                posterior_cdf_distance: cdf_distance(original, &without)?,
                reference_cdf_distance: 0.0,
                reference_unknown_ratio: 0.0,
                reference_known_avg_cost: None,
                reference_profit_ratio_lower: 0.0,
                reference_profit_ratio_upper: 0.0,
            });
            Ok(())
        },
    )?;
    if comparison.days.len() != current.len() {
        return Err("关闭归属修正后快照数量改变，无法比较".into());
    }

    let neutral = ChipChangeConfig::uniform_turnover().compile()?;
    let output_index = row_data
        .trade_dates
        .iter()
        .position(|date| date == output_start_date.trim())
        .ok_or_else(|| "筹码机制对照缺少输出起点".to_string())?;
    let process_index = output_index.saturating_sub(config.warmup_days);
    let bars = build_validated_bars(row_data, output_index)?;
    let first_bar = bars[process_index]
        .as_ref()
        .ok_or_else(|| "筹码机制对照缺少初始化行情".to_string())?;
    comparison.simulation_start_date = Some(first_bar.trade_date.clone());
    let initial = build_initial_buckets(
        first_bar.low,
        first_bar.high,
        bucket_step(config.bucket_pct),
    )?;
    let initial_avg_cost = initial
        .iter()
        .map(|bucket| bucket.price() * bucket.total_chip())
        .sum::<f64>()
        / 100.0;
    let mut unknown_ratio = 1.0;
    for bar in &bars[process_index..output_index] {
        let bar = bar
            .as_ref()
            .ok_or_else(|| "筹码机制对照预热行情无效".to_string())?;
        unknown_ratio *= 1.0 - bar.turnover_rate / 100.0;
    }
    let mut reference_index = 0;
    replay_chen_chip_snapshots(
        std::borrow::Cow::Borrowed(row_data),
        output_start_date,
        &neutral,
        config,
        &mut ChenChipFeatureState::default(),
        false,
        |reference| {
            let original = current
                .get(reference_index)
                .ok_or_else(|| "等概率参照快照数量改变，无法比较".to_string())?;
            let day = &mut comparison.days[reference_index];
            let bar = bars[output_index + reference_index]
                .as_ref()
                .ok_or_else(|| "筹码机制对照行情无效".to_string())?;
            unknown_ratio *= 1.0 - bar.turnover_rate / 100.0;
            let known_ratio = 1.0 - unknown_ratio;
            let (average, profit) = distribution_metrics(&reference);
            let initial_profit_ratio = initial
                .iter()
                .filter(|bucket| bucket.price() <= reference.close + EPS)
                .map(|bucket| bucket.total_chip())
                .sum::<f64>()
                / 100.0;
            let known_profit =
                (profit - unknown_ratio * initial_profit_ratio).clamp(0.0, known_ratio);
            day.reference_imputed_avg_cost = average;
            day.reference_imputed_profit_ratio = profit;
            day.reference_cdf_distance = cdf_distance(original, &reference)?;
            day.reference_unknown_ratio = unknown_ratio;
            day.reference_known_avg_cost = (known_ratio > EPS)
                .then(|| (average - unknown_ratio * initial_avg_cost) / known_ratio);
            day.reference_profit_ratio_lower = known_profit;
            day.reference_profit_ratio_upper = (known_profit + unknown_ratio).min(1.0);
            reference_index += 1;
            Ok(())
        },
    )?;
    if reference_index != current.len() {
        return Err("等概率参照快照数量改变，无法比较".into());
    }
    Ok((current, comparison))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::cyq_chen::test_support::{assert_close, sample_row_data};

    #[test]
    fn mechanism_comparison_keeps_unknown_mass_and_excludes_imputed_costs() {
        let mut rows = sample_row_data();
        rows.cols.insert(
            "TOR".into(),
            vec![Some(0.0), Some(10.0), Some(0.0), Some(100.0), Some(0.0)],
        );
        for key in ["O", "H", "L", "C"] {
            rows.cols.get_mut(key).unwrap()[1] = Some(14.0);
        }
        let config = ChenChipConfig {
            warmup_days: 1,
            bucket_pct: 1.0,
        };
        let neutral = ChipChangeConfig::uniform_turnover().compile().unwrap();
        let (snapshots, comparison) =
            compute_chen_chip_mechanism_comparison(&rows, "20240103", &neutral, config).unwrap();
        assert_eq!(
            comparison.simulation_start_date.as_deref(),
            Some("20240102")
        );
        assert!(!comparison.universe_verified);
        assert_close(comparison.days[0].reference_unknown_ratio, 0.9);
        assert_close(comparison.days[1].reference_unknown_ratio, 0.9);
        assert_close(comparison.days[2].reference_unknown_ratio, 0.0);
        assert_close(
            comparison.days[2].reference_profit_ratio_lower,
            comparison.days[2].reference_profit_ratio_upper,
        );
        let known_cost = comparison.days[0].reference_known_avg_cost.unwrap();
        assert!((13.8..14.2).contains(&known_cost));
        assert!(known_cost - comparison.days[0].reference_imputed_avg_cost > 3.0);
        for day in &comparison.days {
            assert_close(day.reference_cdf_distance, 0.0);
            assert_close(
                day.reference_profit_ratio_upper - day.reference_profit_ratio_lower,
                day.reference_unknown_ratio,
            );
        }
        for (snapshot, day) in snapshots.iter().zip(&comparison.days) {
            let origin = snapshot.origin.unwrap();
            assert_close(
                (origin.initial_main + origin.initial_retail) / 100.0,
                day.reference_unknown_ratio,
            );
        }
        let mut prefix = rows.clone();
        prefix.trade_dates.truncate(3);
        for values in prefix.cols.values_mut() {
            values.truncate(3);
        }
        let (_, earlier) =
            compute_chen_chip_mechanism_comparison(&prefix, "20240103", &neutral, config).unwrap();
        assert_eq!(
            serde_json::to_value(&earlier.days).unwrap(),
            serde_json::to_value(&comparison.days[..2]).unwrap()
        );
        let flow = snapshots[1].flow.as_ref().unwrap();
        assert_close(flow.main_sold + flow.retail_sold, 0.0);
        assert_close(flow.main_bought + flow.retail_bought, 0.0);

        rows.cols.insert("TOR".into(), vec![Some(0.0); 5]);
        let (_, unknown) =
            compute_chen_chip_mechanism_comparison(&rows, "20240103", &neutral, config).unwrap();
        assert!(
            unknown
                .days
                .iter()
                .all(|day| day.reference_known_avg_cost.is_none())
        );
    }

    #[test]
    fn mechanism_comparison_separates_reclassification_from_later_cost_feedback() {
        let rows = sample_row_data();
        let mut rules = ChipChangeConfig::uniform_turnover();
        rules
            .strategy
            .iter_mut()
            .filter(|rule| rule.direction == ChipDirection::Sell && rule.holder == ChipHolder::Main)
            .for_each(|rule| rule.bias = 0.25);
        rules.strategy.push(ChipChangeStrategy {
            name: "归属确认".into(),
            holder: ChipHolder::Main,
            direction: ChipDirection::Buy,
            when: "C > 0".into(),
            bias: 1.0,
            confirm_after: 1,
        });
        let (snapshots, comparison) = compute_chen_chip_mechanism_comparison(
            &rows,
            "20240102",
            &rules.compile().unwrap(),
            ChenChipConfig {
                warmup_days: 0,
                bucket_pct: 1.0,
            },
        )
        .unwrap();
        let flow = snapshots[1].flow.as_ref().unwrap();
        assert!(flow.reclassified_to_main > 0.0);
        let initial = snapshots[0].origin.unwrap();
        assert_close(initial.initial_main, 48.0);
        assert_close(initial.initial_retail, 42.0);
        assert_close(flow.main_sold + flow.retail_sold, 10.0);
        assert_close(flow.main_bought + flow.retail_bought, 10.0);
        assert_close(
            snapshots[1].main_total - snapshots[0].main_total,
            flow.main_bought - flow.main_sold + flow.reclassified_to_main
                - flow.reclassified_to_retail,
        );
        assert_close(comparison.days[1].posterior_cdf_distance, 0.0);
        assert!(comparison.days[2].posterior_cdf_distance > 0.0);

        // Compare streamed reductions with full replay output for both additional models.
        rules.strategy.retain(|rule| rule.confirm_after == 0);
        let without = compute_chen_chip_snapshots_with_compiled_config(
            &rows,
            "20240102",
            &rules.compile().unwrap(),
            ChenChipConfig {
                warmup_days: 0,
                bucket_pct: 1.0,
            },
        )
        .unwrap();
        let reference = compute_chen_chip_snapshots_with_compiled_config(
            &rows,
            "20240102",
            &ChipChangeConfig::uniform_turnover().compile().unwrap(),
            ChenChipConfig {
                warmup_days: 0,
                bucket_pct: 1.0,
            },
        )
        .unwrap();
        assert_eq!(comparison.days.len(), without.len());
        assert_eq!(comparison.days.len(), reference.len());
        for (((day, original), without), reference) in comparison
            .days
            .iter()
            .zip(&snapshots)
            .zip(&without)
            .zip(&reference)
        {
            assert_eq!(day.without_posterior_origin, without.origin);
            assert_close(
                day.without_posterior_avg_cost,
                distribution_metrics(without).0,
            );
            assert_close(
                day.reference_imputed_avg_cost,
                distribution_metrics(reference).0,
            );
            assert_close(
                day.posterior_cdf_distance,
                cdf_distance(original, without).unwrap(),
            );
            assert_close(
                day.reference_cdf_distance,
                cdf_distance(original, reference).unwrap(),
            );
        }
        // The first confirmation changes only subsequent purchases; the next day's
        // unequal selling can then change the survival of initial inventory.
        let confirmed = snapshots[1].origin.unwrap();
        let unconfirmed = without[1].origin.unwrap();
        assert_close(confirmed.initial_main, unconfirmed.initial_main);
        assert_close(confirmed.initial_retail, unconfirmed.initial_retail);
        assert!(
            (snapshots[2].origin.unwrap().initial_main - without[2].origin.unwrap().initial_main)
                .abs()
                > 1e-6
        );
    }
}
