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
pub struct ChenChipBasisEvent {
    pub id: String,
    /// newFloat, proportionalSplit, or unhandled; shares are absolute shares, not 万股.
    pub kind: String,
    pub effective_date: String,
    pub announcement_date: String,
    pub acquired_date: String,
    pub source: String,
    pub before_shares: Option<f64>,
    pub after_shares: Option<f64>,
}

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
    /// conditionalUnverified or blocked. Processing one event never certifies the whole history.
    pub status: String,
    pub processed_events: Vec<String>,
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
    /// Event metadata identifies a restated calculation, not a historical knowledge-time backtest.
    pub events: Vec<ChenChipBasisEvent>,
    pub days: Vec<BasisDay>,
}

/// Pure calculation; does not replace identity-model results or write production databases.
/// Events before simulation are absorbed in the wholly unknown starting inventory.
pub fn compute_chen_chip_basis(
    rows: &RowData,
    output_start: &str,
    config: ChenChipConfig,
    events: &[ChenChipBasisEvent],
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
    let mut ordered = events.to_vec();
    ordered.sort_by(|a, b| a.effective_date.cmp(&b.effective_date));
    let mut ids = std::collections::HashSet::new();
    for event in &ordered {
        for date in [
            &event.effective_date,
            &event.announcement_date,
            &event.acquired_date,
        ] {
            if date.len() != 8
                || !date.bytes().all(|c| c.is_ascii_digit())
                || chrono::NaiveDate::parse_from_str(date, "%Y%m%d").is_err()
            {
                return Err("事件日期必须为有效YYYYMMDD".into());
            }
        }
        if event.id == "initial"
            || event.id.trim().is_empty()
            || !ids.insert(&event.id)
            || event.source.trim().is_empty()
            || event.announcement_date > event.acquired_date
        {
            return Err("事件ID须非空且唯一，须提供来源，取得日不能早于公告日".into());
        }
        if !["newFloat", "proportionalSplit", "unhandled"].contains(&event.kind.as_str()) {
            return Err("未知股本事件类型".into());
        }
        if event.kind != "unhandled" {
            match (event.before_shares, event.after_shares) {
                (Some(a), Some(b))
                    if a.is_finite()
                        && b.is_finite()
                        && a > 0.0
                        && b > 0.0
                        && (event.kind != "newFloat" || b > a) =>
                {
                    ()
                }
                _ => return Err("已核实事件须提供有效前后股数；新增流通须增加股数".into()),
            }
        }
    }
    if ordered
        .windows(2)
        .any(|w| w[0].effective_date == w[1].effective_date)
    {
        return Err("同日多个股本事件需先核实合并，不能猜测处理顺序".into());
    }
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
    serde_json::to_string(&ordered)
        .map_err(|e| e.to_string())?
        .hash(&mut fingerprint);
    let mut result = ChenChipBasis {
        input_fingerprint: format!("local-v1-{:016x}", fingerprint.finish()),
        model: "neutral-cost-v1".into(),
        price_coordinate: "qfq-restated".into(),
        simulation_start_date: first.trade_date.clone(),
        events: ordered.clone(),
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
    let mut processed = Vec::new();
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
        let due = ordered
            .iter()
            .filter(|e| {
                e.effective_date.as_str() >= first.trade_date.as_str()
                    && e.effective_date.as_str() <= date.as_str()
                    && previous_date
                        .as_ref()
                        .is_none_or(|d| e.effective_date.as_str() > d.as_str())
            })
            .collect::<Vec<_>>();
        // Multiple events between quotations require joint evidence; do not silently compound them.
        if due.len() > 1 {
            issues.push(BasisIssue {
                detected_date: date.clone(),
                possible_from: due[0].effective_date.clone(),
                reason: "两次报价间存在多个事件，需联合核实".into(),
            });
        }
        let single_event = due.len() == 1;
        for event in due {
            let valid = event.kind != "unhandled"
                && single_event
                && match (
                    interval,
                    current_interval,
                    event.before_shares,
                    event.after_shares,
                ) {
                    (Some((lo, hi)), Some((nl, nh)), Some(a), Some(b)) => {
                        (lo..=hi).contains(&(a / 10000.0)) && (nl..=nh).contains(&(b / 10000.0))
                    }
                    (Some((lo, hi)), None, Some(a), Some(_)) if zero_trade => {
                        (lo..=hi).contains(&(a / 10000.0))
                    }
                    _ => false,
                };
            if !valid {
                issues.push(BasisIssue {
                    detected_date: date.clone(),
                    possible_from: event.effective_date.clone(),
                    reason: format!("事件 {} 未处理：类型或前后股本证据不足／不相容", event.id),
                });
                continue;
            }
            if event.kind == "newFloat" {
                let ratio = event.before_shares.unwrap() / event.after_shares.unwrap();
                for b in &mut buckets {
                    b.main_chip *= ratio;
                }
                for u in &mut unknown {
                    u.mass *= ratio;
                }
                unknown.push(BasisUnknown {
                    source: event.id.clone(),
                    effective_date: event.effective_date.clone(),
                    mass: 1.0 - ratio,
                });
            }
            // qfq prices already share one coordinate: do not divide costs again for splits.
            interval = Some((
                event.after_shares.unwrap() / 10000.0,
                event.after_shares.unwrap() / 10000.0,
            ));
            segment_start = date.clone();
            processed.push(event.id.clone());
        }
        match current_interval {
            Some((lo, hi)) => {
                if let Some((old_lo, old_hi)) = interval {
                    if lo.max(old_lo) > hi.min(old_hi) {
                        issues.push(BasisIssue {
                            detected_date: date.clone(),
                            possible_from: segment_start.clone(),
                            reason: "VOL/TOR舍入区间与固定流通股本矛盾".into(),
                        });
                        interval = Some((lo, hi));
                        segment_start = date.clone();
                    } else {
                        interval = Some((lo.max(old_lo), hi.min(old_hi)));
                    }
                } else {
                    interval = Some((lo, hi));
                    segment_start = date.clone();
                }
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
            let values = if issues.is_empty() {
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
                Some(BasisValues {
                    bins,
                    known_mass,
                    unknown_mass,
                    unknown_sources: unknown.clone(),
                    known_avg_cost: (known_mass > EPS).then(|| {
                        buckets.iter().map(|b| b.price() * b.main_chip).sum::<f64>() / known_mass
                    }),
                    profit_lower: profit,
                    profit_upper: (profit + unknown_mass).min(1.0),
                })
            } else {
                None
            };
            result.days.push(BasisDay {
                trade_date: date.clone(),
                status: if values.is_some() {
                    "conditionalUnverified"
                } else {
                    "blocked"
                }
                .into(),
                processed_events: processed.clone(),
                float_observation_available: current_interval.is_some(),
                issues: issues.clone(),
                values,
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
        let basis = compute_chen_chip_basis(&rows, "20240103", config, &[]).unwrap();
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
        let prefix = compute_chen_chip_basis(&rows, "20240103", config, &[]).unwrap();
        assert_eq!(
            serde_json::to_value(&prefix.days).unwrap(),
            serde_json::to_value(&basis.days[..2]).unwrap()
        );
    }

    #[test]
    fn basis_events_migrate_unknown_and_block_unexplained_changes() {
        let mut rows = sample_row_data();
        rows.cols.insert(
            "VOL".into(),
            vec![
                Some(1000.0),
                Some(1500.0),
                Some(1500.0),
                Some(1500.0),
                Some(1500.0),
            ],
        );
        let config = ChenChipConfig {
            warmup_days: 0,
            bucket_pct: 0.3,
        };
        let mut event = ChenChipBasisEvent {
            id: "unlock".into(),
            kind: "newFloat".into(),
            effective_date: "20240103".into(),
            announcement_date: "20240101".into(),
            acquired_date: "20240105".into(),
            source: "verified fixture".into(),
            before_shares: Some(1_000_000.0),
            after_shares: Some(1_500_000.0),
        };
        let without = compute_chen_chip_basis(&rows, "20240102", config, &[]).unwrap();
        assert!(without.days[0].values.is_some());
        assert!(without.days[1..].iter().all(|d| d.values.is_none()));
        let with = compute_chen_chip_basis(&rows, "20240102", config, &[event.clone()]).unwrap();
        let v = with.days[1].values.as_ref().unwrap();
        assert_close(v.known_mass, 0.1 * (2.0 / 3.0) * 0.9 + 0.1);
        assert_close(v.unknown_sources[1].mass, 0.3);
        event.kind = "proportionalSplit".into();
        let split = compute_chen_chip_basis(&rows, "20240102", config, &[event.clone()]).unwrap();
        assert_close(split.days[1].values.as_ref().unwrap().unknown_mass, 0.81);
        rows.cols.get_mut("VOL").unwrap()[1] = Some(0.0);
        rows.cols.get_mut("TOR").unwrap()[1] = Some(0.0);
        event.kind = "newFloat".into();
        let zero = compute_chen_chip_basis(&rows, "20240102", config, &[event.clone()]).unwrap();
        assert_close(
            zero.days[1].values.as_ref().unwrap().known_mass,
            0.1 * 2.0 / 3.0,
        );
        assert_close(
            zero.days[1].values.as_ref().unwrap().unknown_sources[1].mass,
            1.0 / 3.0,
        );
        assert!(!zero.days[1].float_observation_available);
        event.kind = "unhandled".into();
        assert!(
            compute_chen_chip_basis(&rows, "20240102", config, &[event])
                .unwrap()
                .days[1]
                .values
                .is_none()
        );
        rows.cols.remove("VOL");
        assert!(
            compute_chen_chip_basis(&rows, "20240102", config, &[])
                .unwrap()
                .days
                .iter()
                .all(|d| d.values.is_none())
        );
    }
}
