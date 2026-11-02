use super::{ChenChipAgeSummary, ChenChipBar, ChenChipSnapshot, ChipBucket, EPS};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub const CYQ_CHEN_FEATURE_VERSION: u32 = 2;
pub const CYQ_CHEN_AGE_SUMMARY_VERSION: u32 = 1;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ChenChipFeatureState {
    pub version: u32,
    pub trade_date: Option<String>,
    pub observed_days: usize,
    #[serde(default)]
    pub observation_start_date: Option<String>,
    pub(super) buckets: Vec<FeatureBucket>,
    losses: Vec<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct FeatureBucket {
    pub(super) low: f64,
    pub(super) high: f64,
    pub(super) ages: Vec<f64>,
    pub(super) initial: f64,
    loss_run: usize,
    recovered: bool,
    // Outside this interval all ages are zero. Rebuilt lazily for legacy checkpoints.
    #[serde(skip)]
    active_ages: Option<(usize, usize)>,
}

impl FeatureBucket {
    fn age_range(&self) -> std::ops::Range<usize> {
        if let Some((start, end)) = self.active_ages {
            start..end
        } else if let Some(start) = self.ages.iter().position(|q| *q != 0.0) {
            start..self.ages.iter().rposition(|q| *q != 0.0).unwrap() + 1
        } else {
            0..0
        }
    }

    fn age_mass(&self, range: std::ops::Range<usize>) -> f64 {
        let active = self.age_range();
        let start = range.start.max(active.start);
        let end = range.end.min(active.end);
        if start < end {
            self.ages[start..end].iter().sum()
        } else {
            0.0
        }
    }
}

impl ChenChipFeatureState {
    pub fn validate(&self, trade_date: &str, bins: &[super::ChenChipBin]) -> Result<(), String> {
        let total_chip = bins.iter().map(|bin| bin.total_chip).sum::<f64>();
        if bins.is_empty() || !total_chip.is_finite() || (total_chip - 100.0).abs() > 1e-8 {
            return Err("筹码检查点总库存不守恒，请重建该股票筹码".into());
        }
        for (index, bin) in bins.iter().enumerate() {
            if let Some(origin) = bin.origin {
                origin.validate(bin.main_chip, bin.retail_chip, &bin.pending)?;
            }
            if bin.index != index
                || !bin.price_low.is_finite()
                || !bin.price_high.is_finite()
                || !bin.price.is_finite()
                || bin.price_low <= 0.0
                || bin.price_high <= bin.price_low + EPS
                || bin.price < bin.price_low
                || bin.price > bin.price_high
                || !bin.main_chip.is_finite()
                || !bin.retail_chip.is_finite()
                || bin.main_chip < 0.0
                || bin.retail_chip < 0.0
                || !bin.total_chip.is_finite()
                || (bin.total_chip - bin.main_chip - bin.retail_chip).abs() > 1e-8
                || (index > 0 && bins[index - 1].price_high > bin.price_low + EPS)
                || bin.pending.iter().any(|lot| {
                    !lot.main_chip.is_finite()
                        || !lot.retail_chip.is_finite()
                        || lot.main_chip < 0.0
                        || lot.retail_chip < 0.0
                        || lot.trade_date.is_empty()
                        || lot.trade_date.as_str() > trade_date
                })
                || bin.pending.iter().map(|lot| lot.main_chip).sum::<f64>() > bin.main_chip + EPS
                || bin.pending.iter().map(|lot| lot.retail_chip).sum::<f64>()
                    > bin.retail_chip + EPS
            {
                return Err(format!(
                    "筹码检查点分桶或后验库存非法，bin={index}，请重建该股票筹码"
                ));
            }
        }
        if self.version != CYQ_CHEN_FEATURE_VERSION
            || self.trade_date.as_deref() != Some(trade_date)
            || self.observed_days == 0
            || self.losses.len() != self.observed_days.min(20)
            || self
                .losses
                .iter()
                .any(|loss| !loss.is_finite() || *loss < 0.0)
            || self.buckets.len() != bins.len()
        {
            return Err("筹码特征检查点版本、日期或窗口不兼容，请重建该股票筹码".into());
        }
        for (index, (state, bin)) in self.buckets.iter().zip(bins).enumerate() {
            let total = state.ages.iter().sum::<f64>() + state.initial;
            if state.low.to_bits() != bin.price_low.to_bits()
                || state.high.to_bits() != bin.price_high.to_bits()
                || state.ages.len() != 121
                || !state.initial.is_finite()
                || state.initial < 0.0
                || state.ages.iter().any(|q| !q.is_finite() || *q < 0.0)
                || !total.is_finite()
                || (total - bin.main_chip - bin.retail_chip).abs() > 1e-8
                || state.loss_run > 120
            {
                return Err(format!(
                    "筹码特征检查点库存或边界不一致，bin={index}, feature_total={total}, bin_total={}, feature_range=[{}, {}], bin_range=[{}, {}]，请重建该股票筹码",
                    bin.main_chip + bin.retail_chip,
                    state.low,
                    state.high,
                    bin.price_low,
                    bin.price_high
                ));
            }
        }
        Ok(())
    }

    pub(super) fn align(&mut self, buckets: &[ChipBucket]) -> Result<(), String> {
        if self.buckets.len() != buckets.len()
            || self.buckets.iter().zip(buckets).any(|(state, bucket)| {
                state.low.to_bits() != bucket.price_low.to_bits()
                    || state.high.to_bits() != bucket.price_high.to_bits()
            })
        {
            let mut previous = std::mem::take(&mut self.buckets)
                .into_iter()
                .map(|bucket| ((bucket.low.to_bits(), bucket.high.to_bits()), bucket))
                .collect::<HashMap<_, _>>();
            for bucket in buckets {
                let key = (bucket.price_low.to_bits(), bucket.price_high.to_bits());
                let state = previous.remove(&key).unwrap_or_else(|| FeatureBucket {
                    low: bucket.price_low,
                    high: bucket.price_high,
                    ages: vec![0.0; 121],
                    initial: bucket.total_chip(),
                    loss_run: 0,
                    recovered: false,
                    active_ages: None,
                });
                self.buckets.push(state);
            }
            if !previous.is_empty() {
                return Err("筹码扩容丢失了已有特征桶".into());
            }
        }
        for (state, bucket) in self.buckets.iter().zip(buckets) {
            if self.observed_days > 0
                && (state.age_mass(0..121) + state.initial - bucket.total_chip()).abs() > 1e-8
            {
                return Err("筹码特征库存与引擎库存不一致".into());
            }
        }
        Ok(())
    }

    pub(super) fn observe(
        &mut self,
        bar: &ChenChipBar,
        buckets: &[ChipBucket],
        before: &[f64],
        after_sell: &[f64],
        after_buy: &[f64],
    ) -> Result<(), String> {
        if self
            .trade_date
            .as_deref()
            .is_some_and(|date| date >= bar.trade_date.as_str())
        {
            return Err("筹码特征日期必须递增，不能用最新检查点倒算历史".into());
        }
        let mass = before.iter().sum::<f64>();
        if mass <= EPS {
            return Err("筹码特征卖出前总量无效".into());
        }
        let execution = (bar.open + bar.high + bar.low + bar.close) / 4.0;
        let mut loss = 0.0;
        for (index, (state, bucket)) in self.buckets.iter_mut().zip(buckets).enumerate() {
            let sold = before[index] - after_sell[index];
            let bought = after_buy[index] - after_sell[index];
            if sold < -EPS || bought < -EPS {
                return Err("筹码观察阶段出现负卖出或负买入".into());
            }
            loss += sold.max(0.0) / mass
                * ((bucket.price() - execution) / (bucket.price() + execution)).max(0.0);
            let survival = if before[index] > EPS {
                (after_sell[index] / before[index]).clamp(0.0, 1.0)
            } else {
                0.0
            };
            let active = state.age_range();
            state.initial *= survival;
            for q in &mut state.ages[active.clone()] {
                *q *= survival;
            }
            // Keep the checkpoint's logical age order, but move only populated ages.
            if !active.is_empty() {
                if active.end > 119 {
                    state.ages[120] += state.ages[119];
                }
                if active.start < 119 {
                    state
                        .ages
                        .copy_within(active.start..active.end.min(119), active.start + 1);
                }
                if active.start < 120 {
                    state.ages[active.start] = 0.0;
                }
            }
            state.ages[0] = bought.max(0.0);
            state.active_ages = Some(if active.is_empty() {
                if bought > 0.0 { (0, 1) } else { (0, 0) }
            } else {
                (
                    if bought > 0.0 {
                        0
                    } else {
                        (active.start + 1).min(120)
                    },
                    (active.end + 1).min(121),
                )
            });
            let total = state.age_mass(0..121) + state.initial;
            let current = bucket.total_chip();
            if current > EPS && total <= EPS {
                return Err("筹码特征缺少可追踪的库存".into());
            }
            let scale = if total > EPS { current / total } else { 0.0 };
            state.initial *= scale;
            let active = state.age_range();
            for q in &mut state.ages[active] {
                *q *= scale;
            }
            if bucket.price() > bar.close {
                state.loss_run = (state.loss_run + 1).min(120);
            } else {
                state.loss_run = 0;
                state.recovered = true;
            }
        }
        self.losses.push(loss);
        if self.losses.len() > 20 {
            self.losses.remove(0);
        }
        if self.observed_days == 0 {
            self.observation_start_date = Some(bar.trade_date.clone());
        }
        self.observed_days += 1;
        self.version = CYQ_CHEN_FEATURE_VERSION;
        self.trade_date = Some(bar.trade_date.clone());
        Ok(())
    }

    pub(super) fn fill_snapshot(&self, snapshot: &mut ChenChipSnapshot) {
        let mass = snapshot.total_chips;
        let mut masses = [0.0; 6];
        for bucket in &self.buckets {
            masses[0] += bucket.age_mass(0..5);
            masses[1] += bucket.age_mass(5..20);
            masses[2] += bucket.age_mass(20..60);
            masses[3] += bucket.age_mass(60..120);
            masses[4] += bucket.ages[120];
            masses[5] += bucket.initial;
        }
        snapshot.age_summary =
            (self.version == CYQ_CHEN_FEATURE_VERSION && self.observed_days > 0 && mass > EPS)
                .then(|| ChenChipAgeSummary {
                    version: CYQ_CHEN_AGE_SUMMARY_VERSION,
                    trade_date: self.trade_date.clone().unwrap_or_default(),
                    observation_start_date: self.observation_start_date.clone(),
                    observed_days: self.observed_days,
                    clock: "quoteUpdates".into(),
                    model: "mixedProportionalSurvival".into(),
                    total_mass: mass,
                    masses,
                    new5_pct: (self.observed_days >= 5).then_some(masses[0] / mass * 100.0),
                    known_ge20_pct: (self.observed_days >= 21)
                        .then_some(masses[2..5].iter().sum::<f64>() / mass * 100.0),
                    initial_pct: masses[5] / mass * 100.0,
                });
        let mut weighted = 0.0;
        let mut unknown = 0.0;
        for bucket in &self.buckets {
            if bucket.loss_run == 0 {
                continue;
            }
            let depth = (((bucket.low + bucket.high) / 2.0) / snapshot.close - 1.0).max(0.0);
            let active = bucket.age_range();
            for (offset, q) in bucket.ages[active.clone()].iter().enumerate() {
                let age = active.start + offset;
                weighted += q * depth * (age + 1).min(bucket.loss_run).min(120) as f64 / 120.0;
            }
            weighted += bucket.initial * depth * bucket.loss_run as f64 / 120.0;
            if !bucket.recovered && bucket.loss_run < 120 {
                unknown += bucket.initial;
            }
        }
        snapshot.feature_version = Some(self.version);
        snapshot.feature_days = Some(self.observed_days);
        snapshot.unknown_trapped = (mass > EPS).then_some(unknown / mass);
        snapshot.trap_coef = (mass > EPS
            && snapshot.close > EPS
            && weighted.is_finite()
            && self.observed_days >= 120
            && unknown <= EPS)
            .then_some(weighted / mass);
        snapshot.real_loss20 =
            (self.losses.len() == 20).then(|| self.losses.iter().sum::<f64>() / 20.0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::RowData;
    use crate::data::cyq_chen::buckets::build_snapshot;
    use crate::data::cyq_chen::test_support::{assert_close, sample_config};
    use crate::data::cyq_chen::{
        ChenChipConfig,
        compute_chen_chip_snapshots_from_initial_bins_with_compiled_config_with_features,
        compute_chen_chip_snapshots_with_compiled_config_with_features,
    };

    #[test]
    fn active_age_range_matches_dense_ledger_exactly() {
        let mut buckets = (0..12)
            .map(|index| ChipBucket {
                price_low: 8.0 + index as f64,
                price_high: 9.0 + index as f64,
                main_chip: 50.0 / 12.0,
                retail_chip: 50.0 / 12.0,
                pending: Vec::new(),
                origin: None,
                rateo: None,
                rateh: None,
                ratel: None,
                ratec: None,
            })
            .collect::<Vec<_>>();
        let mut state = ChenChipFeatureState::default();
        state.align(&buckets).unwrap();
        // Include sub-EPS mass and cohorts at each published age boundary.
        for (index, bucket) in state.buckets.iter_mut().enumerate() {
            if index % 2 == 0 {
                bucket.ages[[0, 4, 19, 59, 119, 120][index / 2]] = 1e-20;
            }
        }
        for day in 0..700 {
            state.align(&buckets).unwrap();
            let before = buckets
                .iter()
                .map(ChipBucket::total_chip)
                .collect::<Vec<_>>();
            let survival = if day % 173 == 172 {
                0.0
            } else if day % 17 == 0 {
                1.0
            } else {
                0.97
            };
            let after_sell = before.iter().map(|q| q * survival).collect::<Vec<_>>();
            let sold = before.iter().sum::<f64>() - after_sell.iter().sum::<f64>();
            let mut after_buy = after_sell.clone();
            after_buy[(day / 31) % buckets.len()] += sold;
            let normalization = 100.0 / after_buy.iter().sum::<f64>();
            for (bucket, q) in buckets.iter_mut().zip(&after_buy) {
                bucket.main_chip = q * normalization * 0.5;
                bucket.retail_chip = q * normalization * 0.5;
            }
            let mut dense = state.buckets.clone();
            for (index, bucket) in dense.iter_mut().enumerate() {
                let survival = if before[index] > EPS {
                    (after_sell[index] / before[index]).clamp(0.0, 1.0)
                } else {
                    0.0
                };
                // Original full-array update, deliberately independent of the range cache.
                bucket.initial *= survival;
                for q in &mut bucket.ages {
                    *q *= survival;
                }
                bucket.ages[120] += bucket.ages[119];
                bucket.ages.copy_within(0..119, 1);
                bucket.ages[0] = (after_buy[index] - after_sell[index]).max(0.0);
                let total = bucket.ages.iter().sum::<f64>() + bucket.initial;
                let scale = if total > EPS {
                    buckets[index].total_chip() / total
                } else {
                    0.0
                };
                bucket.initial *= scale;
                for q in &mut bucket.ages {
                    *q *= scale;
                }
            }
            let bar = ChenChipBar {
                trade_date: format!("{day:08}"),
                open: 12.0,
                high: 13.0,
                low: 11.0,
                close: if day % 151 < 130 { 10.0 } else { 20.0 },
                turnover_rate: (1.0 - survival) * 100.0,
            };
            state
                .observe(&bar, &buckets, &before, &after_sell, &after_buy)
                .unwrap();
            for (actual, expected) in state.buckets.iter().zip(&dense) {
                assert_eq!(actual.ages, expected.ages, "day={day}");
                assert_eq!(actual.initial, expected.initial, "day={day}");
            }
            let mut snapshot = build_snapshot(&bar, &buckets, true).unwrap();
            state.fill_snapshot(&mut snapshot);
            let mut masses = [0.0; 6];
            let mut weighted = 0.0;
            for bucket in &state.buckets {
                for (slot, range) in [0..5, 5..20, 20..60, 60..120, 120..121]
                    .into_iter()
                    .enumerate()
                {
                    masses[slot] += bucket.ages[range].iter().sum::<f64>();
                }
                masses[5] += bucket.initial;
                if bucket.loss_run > 0 {
                    let depth = (((bucket.low + bucket.high) / 2.0) / bar.close - 1.0).max(0.0);
                    for (age, q) in bucket.ages.iter().enumerate() {
                        weighted +=
                            q * depth * (age + 1).min(bucket.loss_run).min(120) as f64 / 120.0;
                    }
                    weighted += bucket.initial * depth * bucket.loss_run as f64 / 120.0;
                }
            }
            assert_eq!(
                snapshot.age_summary.as_ref().unwrap().masses,
                masses,
                "day={day}"
            );
            if let Some(trap) = snapshot.trap_coef {
                assert_eq!(trap, weighted / snapshot.total_chips, "day={day}");
            }
            if day % 97 == 96 {
                let encoded = serde_json::to_string(&state).unwrap();
                assert!(!encoded.contains("active_ages"));
                state = serde_json::from_str(&encoded).unwrap();
                state.validate(&bar.trade_date, &snapshot.bins).unwrap();
            }
        }
    }

    #[test]
    fn coefficient_tracks_known_time_resets_and_unknown_saturation() {
        let mut buckets = vec![ChipBucket {
            price_low: 11.0,
            price_high: 13.0,
            main_chip: 50.0,
            retail_chip: 50.0,
            pending: Vec::new(),
            origin: None,
            rateo: None,
            rateh: None,
            ratel: None,
            ratec: None,
        }];
        let mut state = ChenChipFeatureState::default();
        let mut bar = ChenChipBar {
            trade_date: "00000001".into(),
            open: 10.0,
            high: 10.0,
            low: 10.0,
            close: 10.0,
            turnover_rate: 0.0,
        };
        for day in 1..=120 {
            bar.trade_date = format!("{day:08}");
            state.align(&buckets).unwrap();
            state
                .observe(&bar, &buckets, &[100.0], &[100.0], &[100.0])
                .unwrap();
            let mut snapshot = build_snapshot(&bar, &buckets, true).unwrap();
            state.fill_snapshot(&mut snapshot);
            if day == 1 {
                let age = snapshot.age_summary.as_ref().unwrap();
                assert_eq!(age.new5_pct, None);
                assert_eq!(age.known_ge20_pct, None);
                assert_close(age.initial_pct, 100.0);
            }
            if day == 19 {
                assert!(snapshot.real_loss20.is_none());
            }
            if day == 20 {
                assert_eq!(snapshot.real_loss20, Some(0.0));
            }
            if day == 119 {
                assert!(snapshot.trap_coef.is_none());
                assert_close(snapshot.unknown_trapped.unwrap(), 1.0);
            }
            if day == 120 {
                assert_close(snapshot.trap_coef.unwrap(), 0.2);
                assert_close(snapshot.unknown_trapped.unwrap(), 0.0);
                assert_eq!(snapshot.real_loss20, Some(0.0));
            }
        }
        for (day, price, expected) in [
            (121, 12.0, 0.0),
            (122, 10.0, 0.2 / 120.0),
            (123, 10.0, 0.2 * 2.0 / 120.0),
        ] {
            bar.trade_date = format!("{day:08}");
            bar.close = price;
            state.align(&buckets).unwrap();
            state
                .observe(&bar, &buckets, &[100.0], &[100.0], &[100.0])
                .unwrap();
            let mut snapshot = build_snapshot(&bar, &buckets, true).unwrap();
            state.fill_snapshot(&mut snapshot);
            assert_close(snapshot.trap_coef.unwrap(), expected);
        }
        bar.trade_date = "00000124".into();
        state.align(&buckets).unwrap();
        state
            .observe(&bar, &buckets, &[100.0], &[50.0], &[100.0])
            .unwrap();
        let mut snapshot = build_snapshot(&bar, &buckets, true).unwrap();
        state.fill_snapshot(&mut snapshot);
        assert_close(
            snapshot.trap_coef.unwrap(),
            0.2 * (0.5 * 3.0 / 120.0 + 0.5 / 120.0),
        );
        assert_close(snapshot.real_loss20.unwrap(), 0.5 * (2.0 / 22.0) / 20.0);
        let age = snapshot.age_summary.as_ref().unwrap();
        assert_close(age.masses[0], 50.0);
        assert_close(age.masses[5], 50.0);
        assert_close(age.initial_pct, 50.0);
        assert_eq!(age.observation_start_date.as_deref(), Some("00000001"));
        assert_close(
            state.buckets[0].initial + state.buckets[0].ages.iter().sum::<f64>(),
            100.0,
        );
        buckets[0].main_chip = 20.0;
        buckets[0].retail_chip = 80.0;
        bar.trade_date = "00000125".into();
        state.align(&buckets).unwrap();
        state
            .observe(&bar, &buckets, &[100.0], &[100.0], &[100.0])
            .unwrap();
        assert_close(*state.losses.last().unwrap(), 0.0);
    }

    #[test]
    fn coefficient_weights_depth_and_time_per_lot_without_market_size_bias() {
        let buckets = vec![
            ChipBucket {
                price_low: 11.0,
                price_high: 13.0,
                main_chip: 60.0,
                retail_chip: 0.0,
                pending: Vec::new(),
                origin: None,
                rateo: None,
                rateh: None,
                ratel: None,
                ratec: None,
            },
            ChipBucket {
                price_low: 19.0,
                price_high: 21.0,
                main_chip: 40.0,
                retail_chip: 0.0,
                pending: Vec::new(),
                origin: None,
                rateo: None,
                rateh: None,
                ratel: None,
                ratec: None,
            },
        ];
        let bar = ChenChipBar {
            trade_date: "20240101".into(),
            open: 10.0,
            high: 10.0,
            low: 10.0,
            close: 10.0,
            turnover_rate: 0.0,
        };
        let mut state = ChenChipFeatureState::default();
        state.align(&buckets).unwrap();
        state.version = CYQ_CHEN_FEATURE_VERSION;
        state.observed_days = 120;
        state.buckets[0].loss_run = 120;
        state.buckets[0].initial = 0.0;
        state.buckets[0].ages[29] = 60.0;
        state.buckets[1].loss_run = 120;
        let mut snapshot = build_snapshot(&bar, &buckets, true).unwrap();
        state.fill_snapshot(&mut snapshot);
        assert_close(snapshot.trap_coef.unwrap(), 0.6 * 0.2 * 0.25 + 0.4 * 1.0);
        let mut scaled = state.clone();
        for bucket in &mut scaled.buckets {
            bucket.low *= 10.0;
            bucket.high *= 10.0;
            bucket.initial *= 10.0;
            for q in &mut bucket.ages {
                *q *= 10.0;
            }
        }
        snapshot.close *= 10.0;
        snapshot.total_chips *= 10.0;
        scaled.fill_snapshot(&mut snapshot);
        assert_close(snapshot.trap_coef.unwrap(), 0.43);
        snapshot.close = 10.0;
        snapshot.total_chips = 100.0;
        state.buckets[0].low = 13.0;
        state.buckets[0].high = 15.0;
        state.fill_snapshot(&mut snapshot);
        assert_close(snapshot.trap_coef.unwrap(), 0.46);
        state.buckets[1].loss_run = 60;
        state.buckets[1].recovered = true;
        state.fill_snapshot(&mut snapshot);
        assert_close(snapshot.trap_coef.unwrap(), 0.26);
        state.buckets[0].initial = 60.0;
        state.buckets[0].ages[29] = 0.0;
        for bucket in &mut state.buckets {
            bucket.low = 49.0;
            bucket.high = 51.0;
            bucket.loss_run = 120;
        }
        state.fill_snapshot(&mut snapshot);
        assert_close(snapshot.trap_coef.unwrap(), 4.0);
    }

    #[test]
    fn feature_checkpoint_resume_matches_replay_with_expansion_and_zero_turnover() {
        let dates = (0..150)
            .map(|day| {
                (chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap() + chrono::Duration::days(day))
                    .format("%Y%m%d")
                    .to_string()
            })
            .collect::<Vec<_>>();
        let prices = (0..150)
            .map(|day| {
                Some(if day < 125 {
                    12.0
                } else if day < 140 {
                    8.0
                } else {
                    15.0
                })
            })
            .collect::<Vec<_>>();
        let row = RowData {
            trade_dates: dates,
            cols: HashMap::from([
                ("O".into(), prices.clone()),
                ("H".into(), prices.clone()),
                ("L".into(), prices.clone()),
                ("C".into(), prices),
                (
                    "TOR".into(),
                    (0..150)
                        .map(|day| {
                            Some(if day == 130 {
                                0.0
                            } else if day == 135 {
                                100.0
                            } else {
                                10.0
                            })
                        })
                        .collect(),
                ),
            ]),
        };
        let config = ChenChipConfig {
            warmup_days: 120,
            bucket_pct: 0.3,
        };
        let chip = sample_config().compile().unwrap();
        let mut full_state = ChenChipFeatureState::default();
        let full = compute_chen_chip_snapshots_with_compiled_config_with_features(
            &row,
            &row.trade_dates[120],
            &chip,
            config,
            &mut full_state,
        )
        .unwrap();
        let split = RowData {
            trade_dates: row.trade_dates[..133].to_vec(),
            cols: row
                .cols
                .iter()
                .map(|(key, values)| (key.clone(), values[..133].to_vec()))
                .collect(),
        };
        let mut split_state = ChenChipFeatureState::default();
        let head = compute_chen_chip_snapshots_with_compiled_config_with_features(
            &split,
            &split.trade_dates[120],
            &chip,
            config,
            &mut split_state,
        )
        .unwrap();
        let last = head.last().unwrap();
        let encoded = serde_json::to_string(&split_state).unwrap();
        let mut restored: ChenChipFeatureState = serde_json::from_str(&encoded).unwrap();
        restored
            .validate(last.trade_date.as_deref().unwrap(), &last.bins)
            .unwrap();
        let tail =
            compute_chen_chip_snapshots_from_initial_bins_with_compiled_config_with_features(
                &row,
                &row.trade_dates[133],
                &last.bins,
                &[],
                &chip,
                config,
                &mut restored,
            )
            .unwrap();
        for (actual, expected) in head.iter().chain(&tail).zip(&full) {
            assert_close(actual.trap_coef.unwrap(), expected.trap_coef.unwrap());
            assert_close(actual.real_loss20.unwrap(), expected.real_loss20.unwrap());
            assert_eq!(actual.feature_days, expected.feature_days);
            assert_eq!(actual.age_summary, expected.age_summary);
            assert_close(actual.total_chips, expected.total_chips);
        }
        assert_eq!(restored.losses, full_state.losses);
        assert_eq!(restored.trade_date, full_state.trade_date);
        let mut legacy = serde_json::to_value(&restored).unwrap();
        legacy
            .as_object_mut()
            .unwrap()
            .remove("observation_start_date");
        let legacy: ChenChipFeatureState = serde_json::from_value(legacy).unwrap();
        legacy
            .validate(
                full.last().unwrap().trade_date.as_deref().unwrap(),
                &full.last().unwrap().bins,
            )
            .unwrap();
        assert!(legacy.observation_start_date.is_none());
        restored.version = 1;
        assert!(
            restored
                .validate(
                    full.last().unwrap().trade_date.as_deref().unwrap(),
                    &full.last().unwrap().bins
                )
                .is_err()
        );
    }
}
