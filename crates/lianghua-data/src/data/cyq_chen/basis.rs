//! Identity-neutral cost ledger. Masses are fractions of the current float universe.
use super::{
    ChenChipConfig, EPS,
    bars::{
        bucket_step, build_initial_buckets, build_validated_bars, expand_buckets_for_bar,
        validate_row_shape,
    },
    config::validate_compute_config,
    simulate::buy_price_weights,
};
use crate::data::RowData;
use serde::{Deserialize, Serialize};
use std::hash::{Hash, Hasher};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BasisBin {
    pub price_low: f64,
    pub price_high: f64,
    pub price: f64,
    pub mass: f64,
    pub cdf_lower: f64,
    pub cdf_upper: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BasisUnknown {
    pub source: String,
    pub effective_date: String,
    pub mass: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BasisIssue {
    pub detected_date: String,
    pub possible_from: String,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BasisValues {
    pub bins: Vec<BasisBin>,
    pub known_mass: f64,
    pub unknown_mass: f64,
    pub unknown_sources: Vec<BasisUnknown>,
    pub known_avg_cost: Option<f64>,
    pub profit_lower: f64,
    pub profit_upper: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BasisDay {
    pub trade_date: String,
    /// Conditional estimate; capital changes use proportional inheritance.
    pub status: String,
    pub float_observation_available: bool,
    pub issues: Vec<BasisIssue>,
    pub values: Option<BasisValues>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChenChipBasis {
    pub model: String,
    pub input_fingerprint: String,
    pub price_coordinate: String,
    pub simulation_start_date: String,
    pub days: Vec<BasisDay>,
}

/// Pure calculation; does not replace identity-model results or write production databases.
/// Capital changes preserve current-universe K/U ratios; only turnover moves inventory.
pub fn compute_chen_chip_basis(
    rows: &RowData,
    output_start: &str,
    config: ChenChipConfig,
) -> Result<ChenChipBasis, String> {
    validate_compute_config(config)?;
    validate_row_shape(rows)?;
    let output = rows
        .trade_dates
        .iter()
        .position(|d| d == output_start)
        .ok_or("基础成本输出起点不在行情中")?;
    if output < config.warmup_days {
        return Err("基础成本预热行情不足".into());
    }
    let start = output - config.warmup_days;
    let bars = build_validated_bars(rows, start)?;
    let first = bars[start].as_ref().ok_or("基础成本起点行情无效")?;
    // Non-cryptographic identity for local result comparison, never a trust/cache credential.
    let mut fingerprint = std::collections::hash_map::DefaultHasher::new();
    rows.trade_dates.hash(&mut fingerprint);
    for key in ["O", "H", "L", "C", "TOR", "VOL", "V"] {
        key.hash(&mut fingerprint);
        rows.cols
            .get(key)
            .map(|values| {
                values
                    .iter()
                    .map(|v| v.map(f64::to_bits))
                    .collect::<Vec<_>>()
            })
            .hash(&mut fingerprint);
    }
    config.bucket_pct.to_bits().hash(&mut fingerprint);
    config.warmup_days.hash(&mut fingerprint);
    output_start.hash(&mut fingerprint);
    let mut result = ChenChipBasis {
        input_fingerprint: format!("local-v1-{:016x}", fingerprint.finish()),
        model: "neutral-cost-v1".into(),
        price_coordinate: "qfq-restated".into(),
        simulation_start_date: first.trade_date.clone(),
        days: Vec::new(),
    };
    let step = bucket_step(config.bucket_pct);
    let mut buckets = build_initial_buckets(first.low, first.high, step)?;
    for b in &mut buckets {
        b.main_chip = 0.0;
        b.retail_chip = 0.0;
        b.origin = None;
    }
    let mut history = vec![std::sync::Arc::new(Vec::new()); buckets.len()];
    let mut unknown = vec![BasisUnknown {
        source: "initial".into(),
        effective_date: first.trade_date.clone(),
        mass: 1.0,
    }];
    let mut interval: Option<(f64, f64)> = None;
    let mut segment_start = first.trade_date.clone();
    let mut previous_date: Option<String> = None;
    let mut issues = Vec::new();
    for (i, bar) in bars.iter().enumerate().skip(start) {
        let bar = bar.as_ref().ok_or("基础成本模拟区间存在无效行情")?;
        let date = &bar.trade_date;
        if bar.open < bar.low - EPS
            || bar.open > bar.high + EPS
            || bar.close < bar.low - EPS
            || bar.close > bar.high + EPS
        {
            return Err(format!("{} 基础成本OHLC范围不合法", bar.trade_date));
        }
        let vol = rows
            .cols
            .get("V")
            .or_else(|| rows.cols.get("VOL"))
            .and_then(|v| v.get(i))
            .copied()
            .flatten();
        let zero_trade = vol == Some(0.0) && bar.turnover_rate == 0.0;
        let current_interval = vol
            .filter(|v| v.is_finite() && *v > 0.0)
            .filter(|_| bar.turnover_rate > 0.00005)
            .map(|v| {
                (
                    (v - 0.005).max(0.0) / (bar.turnover_rate + 0.00005),
                    (v + 0.005) / (bar.turnover_rate - 0.00005),
                )
            });
        match current_interval {
            Some((lo, hi)) => {
                if let Some((old_lo, old_hi)) = interval {
                    if lo.max(old_lo) > hi.min(old_hi) {
                        issues.push(BasisIssue {
                            detected_date: date.clone(),
                            possible_from: segment_start.clone(),
                            reason: "VOL/TOR提示疑似流通股本变化；V1按比例继承处理".into(),
                        });
                        segment_start = date.clone();
                    }
                }
                interval = Some((lo, hi));
            }
            None if zero_trade => {}
            None => {
                // This observation cannot certify the universe; it is not zero float or no event.
                if issues
                    .last()
                    .is_none_or(|x: &BasisIssue| x.reason != "VOL缺失或TOR过小，无法核对总体")
                {
                    issues.push(BasisIssue {
                        detected_date: date.clone(),
                        possible_from: previous_date.clone().unwrap_or_else(|| date.clone()),
                        reason: "VOL缺失或TOR过小，无法核对总体".into(),
                    });
                }
            }
        }
        expand_buckets_for_bar(
            &mut buckets,
            &mut history,
            0,
            bar.low,
            bar.high,
            step,
            &bars,
            false,
        )?;
        let tau = bar.turnover_rate / 100.0;
        for b in &mut buckets {
            b.main_chip *= 1.0 - tau;
        }
        for u in &mut unknown {
            u.mass *= 1.0 - tau;
        }
        if tau > 0.0 {
            let weights = buy_price_weights(&buckets, bar)?;
            let total = weights.iter().map(|(_, w)| w).sum::<f64>();
            if total <= 0.0 {
                return Err("基础成本成交分配为空".into());
            }
            for (index, weight) in weights {
                buckets[index].main_chip += tau * weight / total;
            }
        }
        let known_mass = buckets.iter().map(|b| b.main_chip).sum::<f64>();
        let unknown_mass = unknown.iter().map(|u| u.mass).sum::<f64>();
        if (known_mass + unknown_mass - 1.0).abs() > 1e-8 {
            return Err("基础成本质量不守恒".into());
        }
        if i >= output {
            let mut cdf = 0.0;
            let bins = buckets
                .iter()
                .map(|b| {
                    cdf += b.main_chip;
                    BasisBin {
                        price_low: b.price_low,
                        price_high: b.price_high,
                        price: b.price(),
                        mass: b.main_chip,
                        cdf_lower: cdf,
                        cdf_upper: (cdf + unknown_mass).min(1.0),
                    }
                })
                .collect();
            let profit = buckets
                .iter()
                .filter(|b| b.price() <= bar.close + EPS)
                .map(|b| b.main_chip)
                .sum::<f64>();
            result.days.push(BasisDay {
                trade_date: date.clone(),
                status: "conditionalUnverified".into(),
                float_observation_available: current_interval.is_some(),
                issues: issues.clone(),
                values: Some(BasisValues {
                    bins,
                    known_mass,
                    unknown_mass,
                    unknown_sources: unknown.clone(),
                    known_avg_cost: (known_mass > EPS).then(|| {
                        buckets.iter().map(|b| b.price() * b.main_chip).sum::<f64>() / known_mass
                    }),
                    profit_lower: profit,
                    profit_upper: (profit + unknown_mass).min(1.0),
                }),
            });
        }
        previous_date = Some(date.clone());
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::cyq_chen::{
        ChipChangeConfig, compute_chen_chip_snapshots_with_compiled_config,
        test_support::{assert_close, sample_row_data},
    };

    #[test]
    fn basis_matches_native_known_cost_and_preserves_prefix() {
        let mut rows = sample_row_data();
        rows.cols.insert("VOL".into(), vec![Some(1000.0); 5]);
        let config = ChenChipConfig {
            warmup_days: 1,
            bucket_pct: 0.3,
        };
        let basis = compute_chen_chip_basis(&rows, "20240103", config).unwrap();
        let native = compute_chen_chip_snapshots_with_compiled_config(
            &rows,
            "20240103",
            &ChipChangeConfig::uniform_turnover().compile().unwrap(),
            config,
        )
        .unwrap();
        for (day, snapshot) in basis.days.iter().zip(native) {
            let v = day.values.as_ref().unwrap();
            assert_close(v.known_mass + v.unknown_mass, 1.0);
            for (bin, old) in v.bins.iter().zip(snapshot.bins) {
                let origin = old.origin.unwrap();
                assert_close(
                    bin.mass,
                    (old.total_chip - origin.initial_main - origin.initial_retail) / 100.0,
                );
            }
        }
        rows.trade_dates.truncate(3);
        for values in rows.cols.values_mut() {
            values.truncate(3);
        }
        let prefix = compute_chen_chip_basis(&rows, "20240103", config).unwrap();
        assert_eq!(
            serde_json::to_value(&prefix.days).unwrap(),
            serde_json::to_value(&basis.days[..2]).unwrap()
        );
    }

    #[test]
    fn basis_capital_jumps_use_proportional_inheritance_and_remain_published() {
        let rows = sample_row_data();
        let config = ChenChipConfig {
            warmup_days: 0,
            bucket_pct: 0.3,
        };
        let without_observation = compute_chen_chip_basis(&rows, "20240102", config).unwrap();
        assert!(
            without_observation
                .days
                .iter()
                .all(|day| day.values.is_some())
        );
        assert!(
            without_observation.days[0]
                .issues
                .iter()
                .any(|issue| { issue.reason == "VOL缺失或TOR过小，无法核对总体" })
        );

        let mut jumped = rows;
        jumped.cols.insert(
            "VOL".into(),
            vec![
                Some(1000.0),
                Some(5000.0),
                Some(5000.0),
                Some(5000.0),
                Some(5000.0),
            ],
        );
        let jumped = compute_chen_chip_basis(&jumped, "20240102", config).unwrap();
        assert!(jumped.days.iter().all(|day| day.values.is_some()));
        assert!(jumped.days[1].issues.iter().any(|issue| {
            issue.reason == "VOL/TOR提示疑似流通股本变化；V1按比例继承处理"
        }));
        for (left, right) in without_observation.days.iter().zip(&jumped.days) {
            let left = left.values.as_ref().unwrap();
            let right = right.values.as_ref().unwrap();
            assert_close(left.known_mass, right.known_mass);
            assert_close(left.unknown_mass, right.unknown_mass);
            assert_eq!(right.unknown_sources.len(), 1);
        }
    }
}
