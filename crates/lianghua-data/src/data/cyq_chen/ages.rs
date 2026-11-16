use super::{ChipBucket, EPS};
use crate::expr::eval::{Runtime, Value};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(super) struct HolderAgeState {
    buckets: Vec<HolderAgeBucket>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
struct HolderAgeBucket {
    low: f64,
    high: f64,
    initial: [f64; 2],
    mature: [f64; 2],
    cohorts: VecDeque<(usize, [f64; 2])>,
    scale: [f64; 2],
    #[serde(skip)]
    cohort_total: Option<[f64; 2]>,
}

impl Default for HolderAgeBucket {
    fn default() -> Self {
        Self {
            low: 0.0,
            high: 0.0,
            initial: [0.0; 2],
            mature: [0.0; 2],
            cohorts: VecDeque::new(),
            scale: [1.0; 2],
            cohort_total: None,
        }
    }
}

impl HolderAgeState {
    pub(super) fn align(&mut self, buckets: &[ChipBucket], day: usize) -> Result<(), String> {
        if self.buckets.len() != buckets.len()
            || self.buckets.iter().zip(buckets).any(|(state, bucket)| {
                state.low.to_bits() != bucket.price_low.to_bits()
                    || state.high.to_bits() != bucket.price_high.to_bits()
            })
        {
            let mut previous = std::mem::take(&mut self.buckets)
                .into_iter()
                .map(|b| ((b.low.to_bits(), b.high.to_bits()), b))
                .collect::<HashMap<_, _>>();
            for bucket in buckets {
                let state = previous
                    .remove(&(bucket.price_low.to_bits(), bucket.price_high.to_bits()))
                    .unwrap_or_else(|| HolderAgeBucket {
                        low: bucket.price_low,
                        high: bucket.price_high,
                        initial: [bucket.main_chip, bucket.retail_chip],
                        ..Default::default()
                    });
                self.buckets.push(state);
            }
            if !previous.is_empty() {
                return Err("持有人年龄账本扩容丢失已有成本桶".into());
            }
        }
        for state in &mut self.buckets {
            state.cohort_total.get_or_insert_with(|| {
                state.cohorts.iter().fold([0.0; 2], |mut total, (_, q)| {
                    for holder in 0..2 {
                        total[holder] += q[holder];
                    }
                    total
                })
            });
            while state
                .cohorts
                .front()
                .is_some_and(|(born, _)| day - born >= 120)
            {
                let (_, mass) = state.cohorts.pop_front().unwrap();
                for holder in 0..2 {
                    state.mature[holder] += mass[holder] * state.scale[holder];
                    state.cohort_total.as_mut().unwrap()[holder] -= mass[holder];
                }
            }
            if state.cohorts.is_empty() {
                state.cohort_total = Some([0.0; 2]);
            }
        }
        Ok(())
    }

    pub(super) fn update(
        &mut self,
        buckets: &[ChipBucket],
        day: usize,
        buying: bool,
    ) -> Result<(), String> {
        for (state, bucket) in self.buckets.iter_mut().zip(buckets) {
            for (holder, current) in [bucket.main_chip, bucket.retail_chip]
                .into_iter()
                .enumerate()
            {
                let previous = state.initial[holder]
                    + state.mature[holder]
                    + state.cohort_total.unwrap()[holder] * state.scale[holder];
                if buying {
                    let added = current - previous;
                    if added < -1e-8 {
                        return Err("持有人年龄账本买入阶段库存减少".into());
                    }
                    if added > 0.0 {
                        if state.cohorts.back().is_none_or(|(born, _)| *born != day) {
                            state.cohorts.push_back((day, [0.0; 2]));
                        }
                        let quantity = added / state.scale[holder];
                        state.cohorts.back_mut().unwrap().1[holder] += quantity;
                        state.cohort_total.as_mut().unwrap()[holder] += quantity;
                    }
                } else {
                    if current > previous + 1e-8 {
                        return Err("持有人年龄账本出库阶段库存增加".into());
                    }
                    let ratio = if previous > EPS {
                        (current / previous).clamp(0.0, 1.0)
                    } else {
                        0.0
                    };
                    state.initial[holder] *= ratio;
                    state.mature[holder] *= ratio;
                    state.scale[holder] *= ratio;
                    if state.scale[holder] < 1e-100 || state.scale[holder] > 1e100 {
                        for (_, mass) in &mut state.cohorts {
                            mass[holder] *= state.scale[holder];
                        }
                        state.cohort_total.as_mut().unwrap()[holder] =
                            state.cohorts.iter().map(|(_, q)| q[holder]).sum();
                        state.scale[holder] = 1.0;
                    }
                }
            }
        }
        Ok(())
    }

    pub(super) fn reclassify(
        &mut self,
        bucket: usize,
        day: usize,
        age: usize,
        delta: f64,
    ) -> Result<(), String> {
        let state = &mut self.buckets[bucket];
        let mass = if age >= 120 {
            &mut state.mature
        } else {
            &mut state
                .cohorts
                .iter_mut()
                .find(|(born, _)| *born == day.saturating_sub(age))
                .ok_or("后验身份迁移缺少对应年龄批次")?
                .1
        };
        let scale = if age >= 120 { [1.0; 2] } else { state.scale };
        if delta > mass[1] * scale[1] + 1e-8 || -delta > mass[0] * scale[0] + 1e-8 {
            return Err("后验年龄迁移超过对应身份批次库存".into());
        }
        for (holder, change) in [delta, -delta].into_iter().enumerate() {
            let old = mass[holder];
            mass[holder] = (old + change / scale[holder]).max(0.0);
            if age < 120 {
                if let Some(total) = &mut state.cohort_total {
                    total[holder] += mass[holder] - old;
                }
            }
        }
        Ok(())
    }

    pub(super) fn validate(&self, bins: &[super::ChenChipBin], day: usize) -> Result<(), String> {
        if self.buckets.len() != bins.len() {
            return Err("持有人年龄检查点成本桶数量不匹配，请重建筹码".into());
        }
        for (state, bin) in self.buckets.iter().zip(bins) {
            if state.low.to_bits() != bin.price_low.to_bits()
                || state.high.to_bits() != bin.price_high.to_bits()
                || state.scale.iter().any(|v| !v.is_finite() || *v <= 0.0)
                || state.cohorts.len() > 120
                || state.cohorts.iter().any(|(born, q)| {
                    *born == 0
                        || *born > day
                        || day - born >= 120
                        || q.iter().any(|v| !v.is_finite() || *v < 0.0)
                })
                || state
                    .cohorts
                    .iter()
                    .zip(state.cohorts.iter().skip(1))
                    .any(|(a, b)| a.0 >= b.0)
                || state
                    .initial
                    .iter()
                    .chain(&state.mature)
                    .any(|v| !v.is_finite() || *v < 0.0)
            {
                return Err("持有人年龄检查点无效，请重建筹码".into());
            }
            for (holder, actual) in [bin.main_chip, bin.retail_chip].into_iter().enumerate() {
                let total = state.initial[holder]
                    + state.mature[holder]
                    + state.cohorts.iter().map(|(_, q)| q[holder]).sum::<f64>()
                        * state.scale[holder];
                if (actual - total).abs() > 1e-8 {
                    return Err("持有人年龄检查点库存不守恒，请重建筹码".into());
                }
            }
        }
        Ok(())
    }

    pub(super) fn normalize(&mut self, buckets: &[ChipBucket]) {
        for (state, bucket) in self.buckets.iter_mut().zip(buckets) {
            for (holder, current) in [bucket.main_chip, bucket.retail_chip]
                .into_iter()
                .enumerate()
            {
                let total = state.initial[holder]
                    + state.mature[holder]
                    + state.cohort_total.unwrap()[holder] * state.scale[holder];
                let ratio = if total > 0.0 { current / total } else { 0.0 };
                state.initial[holder] *= ratio;
                state.mature[holder] *= ratio;
                if ratio > 0.0 {
                    state.scale[holder] *= ratio;
                } else {
                    for (_, mass) in &mut state.cohorts {
                        mass[holder] = 0.0;
                    }
                    state.cohort_total.as_mut().unwrap()[holder] = 0.0;
                    state.scale[holder] = 1.0;
                }
            }
        }
    }

    pub(super) fn masses(&self, day: usize, bucket_index: Option<usize>) -> [[f64; 6]; 2] {
        let mut mass = [[0.0; 6]; 2];
        for state in bucket_index.map_or(self.buckets.as_slice(), |index| {
            &self.buckets[index..=index]
        }) {
            for holder in 0..2 {
                mass[holder][4] += state.mature[holder];
                mass[holder][5] += state.initial[holder];
            }
            for (born, q) in &state.cohorts {
                let band = match day - born {
                    0..=4 => 0,
                    5..=19 => 1,
                    20..=59 => 2,
                    _ => 3,
                };
                for holder in 0..2 {
                    mass[holder][band] += q[holder] * state.scale[holder];
                }
            }
        }
        mass
    }

    pub(super) fn inject(
        &self,
        runtime: &mut Runtime,
        day: usize,
        bucket_index: Option<usize>,
        keys: &[String],
    ) {
        if !keys
            .iter()
            .any(|key| key.starts_with("BUCKET_") == bucket_index.is_some())
        {
            return;
        }
        let mass = self.masses(day, bucket_index);
        for key in keys {
            if key.starts_with("BUCKET_") != bucket_index.is_some() {
                continue;
            }
            let field = key.strip_prefix("BUCKET_").unwrap_or(key);
            let (holder, suffix) = if let Some(suffix) = field.strip_prefix("MAIN_AGE_") {
                (0, suffix)
            } else if let Some(suffix) = field.strip_prefix("RETAIL_AGE_") {
                (1, suffix)
            } else {
                continue;
            };
            let quantity = match suffix {
                "NEW5" => mass[holder][0],
                "5_19" => mass[holder][1],
                "20_59" => mass[holder][2],
                "60_119" => mass[holder][3],
                "GE120" => mass[holder][4],
                "INITIAL" => mass[holder][5],
                "KNOWN_GE20" => mass[holder][2..5].iter().sum(),
                _ => continue,
            };
            let total = mass[holder].iter().sum::<f64>();
            let value = if total > EPS {
                Value::Num(100.0 * quantity / total)
            } else {
                Value::Bool(crate::expr::eval::Truth::Unknown)
            };
            super::simulate::set_runtime_value(runtime, key, value.clone());
            super::simulate::set_runtime_value(runtime, &key.to_ascii_lowercase(), value);
        }
    }
}

pub(super) fn age_runtime_keys() -> Vec<String> {
    ["", "BUCKET_"]
        .into_iter()
        .flat_map(|prefix| {
            ["MAIN", "RETAIL"].into_iter().flat_map(move |holder| {
                [
                    "NEW5",
                    "5_19",
                    "20_59",
                    "60_119",
                    "GE120",
                    "INITIAL",
                    "KNOWN_GE20",
                ]
                .into_iter()
                .map(move |band| format!("{prefix}{holder}_AGE_{band}"))
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::{RowData, cyq_chen::*};

    #[test]
    fn holder_age_lazy_scaling_matches_dense_ledger_and_legacy_resume() {
        let mut buckets = (0..4)
            .map(|index| ChipBucket {
                price_low: 9.0 + index as f64,
                price_high: 10.0 + index as f64,
                main_chip: 12.5,
                retail_chip: 12.5,
                pending: Vec::new(),
                origin: None,
                rateo: None,
                rateh: None,
                ratel: None,
                ratec: None,
            })
            .collect::<Vec<_>>();
        let mut state = HolderAgeState::default();
        let mut dense = vec![[[0.0; 2]; 121]; 4];
        let mut initial = [[12.5; 2]; 4];
        for day in 1..=700 {
            state.align(&buckets, day).unwrap();
            for ages in &mut dense {
                for holder in 0..2 {
                    ages[120][holder] += ages[119][holder];
                }
                ages.copy_within(0..119, 1);
                ages[0] = [0.0; 2];
            }
            for (index, bucket) in buckets.iter_mut().enumerate() {
                for (holder, current) in [&mut bucket.main_chip, &mut bucket.retail_chip]
                    .into_iter()
                    .enumerate()
                {
                    let survival = if day % 173 == 0 {
                        0.0
                    } else if (200..330).contains(&day) {
                        1e-4
                    } else {
                        0.95 + holder as f64 * 0.02
                    };
                    *current *= survival;
                    initial[index][holder] *= survival;
                    for mass in &mut dense[index] {
                        mass[holder] *= survival;
                    }
                }
            }
            state.update(&buckets, day, false).unwrap();
            for (index, bucket) in buckets.iter_mut().enumerate() {
                let bought = [0.3 + (day % 7) as f64 * 0.1, 0.6];
                bucket.main_chip += bought[0];
                bucket.retail_chip += bought[1];
                dense[index][0] = bought;
            }
            state.update(&buckets, day, true).unwrap();
            if day > 2 {
                for (index, bucket) in buckets.iter_mut().enumerate() {
                    let delta = dense[index][2][1] * 0.2;
                    state.reclassify(index, day, 2, delta).unwrap();
                    bucket.main_chip += delta;
                    bucket.retail_chip -= delta;
                    dense[index][2][0] += delta;
                    dense[index][2][1] -= delta;
                }
            }
            let normalization = 100.0 / buckets.iter().map(ChipBucket::total_chip).sum::<f64>();
            for (index, bucket) in buckets.iter_mut().enumerate() {
                bucket.main_chip *= normalization;
                bucket.retail_chip *= normalization;
                for holder in 0..2 {
                    initial[index][holder] *= normalization;
                    for mass in &mut dense[index] {
                        mass[holder] *= normalization;
                    }
                }
            }
            state.normalize(&buckets);
            for index in 0..buckets.len() {
                let mut expected = [[0.0; 6]; 2];
                for holder in 0..2 {
                    expected[holder][5] = initial[index][holder];
                    for (age, mass) in dense[index].iter().enumerate() {
                        let band = match age {
                            0..=4 => 0,
                            5..=19 => 1,
                            20..=59 => 2,
                            60..=119 => 3,
                            _ => 4,
                        };
                        expected[holder][band] += mass[holder];
                    }
                }
                let actual = state.masses(day, Some(index));
                for holder in 0..2 {
                    for band in 0..6 {
                        assert!(
                            (actual[holder][band] - expected[holder][band]).abs() < 1e-8,
                            "day={day}, bucket={index}, holder={holder}, band={band}"
                        );
                    }
                }
            }
            if day == 137 || day == 350 {
                // Old checkpoints stored physical cohort quantities without a scale.
                let mut legacy = serde_json::to_value(&state).unwrap();
                for bucket in legacy["buckets"].as_array_mut().unwrap() {
                    let scale = bucket.as_object_mut().unwrap().remove("scale").unwrap();
                    for cohort in bucket["cohorts"].as_array_mut().unwrap() {
                        for holder in 0..2 {
                            cohort[1][holder] = (cohort[1][holder].as_f64().unwrap()
                                * scale[holder].as_f64().unwrap())
                            .into();
                        }
                    }
                }
                state = serde_json::from_value(legacy).unwrap();
            } else if day == 400 {
                state = serde_json::from_str(&serde_json::to_string(&state).unwrap()).unwrap();
            }
        }
    }

    #[test]
    fn holder_age_rules_replay_resume_and_preserve_unknown() {
        let mut row = RowData {
            trade_dates: (1..=26).map(|d| format!("202601{d:02}")).collect(),
            cols: ["O", "H", "L", "C", "TOR"]
                .into_iter()
                .map(|key| (key.into(), vec![Some(10.0); 26]))
                .collect(),
        };
        row.cols.get_mut("TOR").unwrap()[25] = Some(0.0);
        let text = r#"
version=3
[independent_buy]
positive_strength=0.08
negative_strength=0.65
initial_main_pct=50
[[strategy]]
name="main sell"
holder="main"
direction="sell"
when="C>0"
bias=1
[[strategy]]
name="retail sell"
holder="retail"
direction="sell"
when="C>0"
bias=1
[[strategy]]
name="age sell"
holder="main"
direction="sell"
when="BUCKET_MAIN_AGE_KNOWN_GE20>0 AND MAIN_AGE_INITIAL<50"
bias=2
"#;
        let cfg = ChipChangeConfig::from_toml_str(text)
            .unwrap()
            .compile()
            .unwrap();
        assert!(cfg.sell_uses_bucket_dynamic_state);
        assert!(!cfg.sell_uses_point_rates);
        let mut state = ChenChipFeatureState::default();
        let settings = ChenChipConfig {
            warmup_days: 0,
            bucket_pct: 1.0,
        };
        let full = compute_chen_chip_snapshots_with_compiled_config_with_features(
            &row, "20260101", &cfg, settings, &mut state,
        )
        .unwrap();
        let baseline = ChipChangeConfig::from_toml_str(&text.replace("bias=2", "bias=0"))
            .unwrap()
            .compile()
            .unwrap();
        let old =
            compute_chen_chip_snapshots_with_compiled_config(&row, "20260101", &baseline, settings)
                .unwrap();
        assert!((full[19].main_total - old[19].main_total).abs() < 1e-9);
        assert!((full[20].main_total - old[20].main_total).abs() > 1e-4);
        let mut partial = row.clone();
        partial.trade_dates.truncate(20);
        for values in partial.cols.values_mut() {
            values.truncate(20);
        }
        let mut checkpoint = ChenChipFeatureState::default();
        let first = compute_chen_chip_snapshots_with_compiled_config_with_features(
            &partial,
            "20260101",
            &cfg,
            settings,
            &mut checkpoint,
        )
        .unwrap();
        checkpoint = serde_json::from_str(&serde_json::to_string(&checkpoint).unwrap()).unwrap();
        checkpoint
            .validate("20260120", &first.last().unwrap().bins)
            .unwrap();
        let continued =
            compute_chen_chip_snapshots_from_initial_bins_with_compiled_config_with_features(
                &row,
                "20260121",
                &first.last().unwrap().bins,
                &[],
                &cfg,
                settings,
                &mut checkpoint,
            )
            .unwrap();
        assert_eq!(full[0].holder_age_pcts.unwrap()[0][0], None);
        for (a, b) in full[20..].iter().zip(&continued) {
            assert!((a.main_total - b.main_total).abs() < 1e-8);
            for (left, right) in a
                .holder_age_pcts
                .unwrap()
                .into_iter()
                .flatten()
                .zip(b.holder_age_pcts.unwrap().into_iter().flatten())
            {
                assert!((left.unwrap() - right.unwrap()).abs() < 1e-8);
            }
            for ages in a.holder_age_pcts.unwrap() {
                assert!(
                    ages.into_iter()
                        .flatten()
                        .all(|v| (0.0..=100.0).contains(&v))
                );
                assert!(ages.into_iter().flatten().sum::<f64>() <= 100.0 + 1e-8);
            }
        }
        let mut rt = crate::data::runtime::row_to_shared_rt(&row);
        checkpoint
            .holder_ages
            .as_ref()
            .unwrap()
            .inject(&mut rt, 27, None, &age_runtime_keys());
        assert!(
            matches!(rt.vars.get("MAIN_AGE_INITIAL"),Some(Value::Num(v)) if *v>0.0 && *v<100.0)
        );
        assert!(matches!(rt.vars.get("MAIN_AGE_KNOWN_GE20"),Some(Value::Num(v)) if *v>0.0));
        assert_eq!(checkpoint.observed_days, 26);
        assert_eq!(
            continued.last().unwrap().flow.as_ref().unwrap().main_bought,
            0.0
        );
    }

    #[test]
    fn holder_age_expression_scope_and_dynamic_cache() {
        for expression in ["REF(MAIN_AGE_NEW5,1)>0", "X:=MAIN_AGE_NEW5;MA(X,3)>0"] {
            let text = format!(
                "version=1\n[[strategy]]\nname='age'\nholder='main'\ndirection='buy'\nwhen='{expression}'\nbias=1"
            );
            assert!(
                ChipChangeConfig::from_toml_str(&text)
                    .unwrap()
                    .compile()
                    .unwrap_err()
                    .contains("时序")
            );
        }
        let cfg=ChipChangeConfig::from_toml_str("version=1\n[[strategy]]\nname='age'\nholder='main'\ndirection='buy'\nwhen='MAIN_AGE_NEW5>0 AND REF(C,1)>0'\nbias=1").unwrap().compile().unwrap();
        assert_eq!(cfg.age_keys, vec!["MAIN_AGE_NEW5"]);
        let keys = collect_chen_chip_runtime_keys(&cfg);
        assert!(!keys.contains("MAIN_AGE_NEW5"));
        assert!(cfg.strategies[0].cached_exprs.iter().all(|expr| {
            !crate::data::expr_program_uses_runtime_key(&expr.program, "MAIN_AGE_NEW5")
        }));
        let row = RowData {
            trade_dates: vec!["20260101".into()],
            cols: HashMap::new(),
        };
        let mut runtime = crate::data::runtime::row_to_shared_rt(&row);
        HolderAgeState::default().inject(&mut runtime, 1, None, &["MAIN_AGE_NEW5".into()]);
        let expr =
            crate::expr::validation::parse_expression_program("ISVALID(MAIN_AGE_NEW5)").unwrap();
        assert!(matches!(runtime.eval_program(&expr).unwrap(), Value::Num(value) if value == 0.0));
        assert!(matches!(
            runtime.vars.get("MAIN_AGE_NEW5"),
            Some(Value::Bool(crate::expr::eval::Truth::Unknown))
        ));
    }

    #[test]
    fn holder_age_posterior_moves_birth_age_between_holders() {
        let mut state = HolderAgeState {
            buckets: vec![HolderAgeBucket {
                low: 9.,
                high: 11.,
                initial: [40., 40.],
                mature: [0.; 2],
                cohorts: VecDeque::from([(1, [10., 10.])]),
                ..Default::default()
            }],
        };
        state.reclassify(0, 3, 2, 4.).unwrap();
        assert_eq!(state.buckets[0].cohorts[0], (1, [14., 6.]));
        assert_eq!(state.buckets[0].initial, [40., 40.]);
        assert!(state.reclassify(0, 3, 2, 7.).is_err());
        let cfg = ChipChangeConfig::from_toml_str(
            r#"
version=1
[[strategy]]
name="sell"
holder="main"
direction="sell"
when="C>0"
bias=1
[[strategy]]
name="age observation"
holder="main"
direction="buy"
when="MAIN_AGE_NEW5>=0"
bias=0
[[strategy]]
name="posterior"
holder="main"
direction="buy"
when="C>0"
bias=0.5
confirm_after=2
"#,
        )
        .unwrap()
        .compile()
        .unwrap();
        let row = RowData {
            trade_dates: (1..=6).map(|d| format!("202602{d:02}")).collect(),
            cols: ["O", "H", "L", "C", "TOR"]
                .into_iter()
                .map(|key| (key.into(), vec![Some(10.); 6]))
                .collect(),
        };
        let mut features = ChenChipFeatureState::default();
        let snapshots = compute_chen_chip_snapshots_with_compiled_config_with_features(
            &row,
            "20260201",
            &cfg,
            ChenChipConfig {
                warmup_days: 0,
                bucket_pct: 1.,
            },
            &mut features,
        )
        .unwrap();
        assert!(
            snapshots
                .iter()
                .any(|s| s.flow.as_ref().unwrap().reclassified_to_main > 0.)
        );
        features
            .validate("20260206", &snapshots.last().unwrap().bins)
            .unwrap();
    }
}
