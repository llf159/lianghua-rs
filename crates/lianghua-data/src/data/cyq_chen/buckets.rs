use crate::data::cyq_chen::config::round_ratio;
use crate::data::cyq_chen::{
    ChenChipBar, ChenChipBin, ChenChipPercentRange, ChenChipSnapshot, ChipBucket, ChipHolder, EPS,
    SellEntry,
};

pub(super) fn holder_chip_entries(buckets: &[ChipBucket], holder: ChipHolder) -> Vec<SellEntry> {
    buckets
        .iter()
        .enumerate()
        .filter_map(|(bucket_index, bucket)| {
            let chip = bucket.chip(holder);
            if chip > EPS {
                Some(SellEntry {
                    bucket_index,
                    holder,
                    weight: chip,
                })
            } else {
                None
            }
        })
        .collect()
}

pub(super) fn apply_weighted_sell(
    buckets: &mut [ChipBucket],
    entries: Vec<SellEntry>,
    amount: f64,
) -> Result<f64, String> {
    if amount <= EPS || entries.is_empty() {
        return Ok(amount.max(0.0));
    }

    let mut active = entries
        .into_iter()
        .filter(|entry| entry.weight.is_finite() && entry.weight > EPS)
        .collect::<Vec<_>>();
    let mut remaining = amount;

    while remaining > EPS && !active.is_empty() {
        let total_weight = active.iter().map(|entry| entry.weight).sum::<f64>();
        if !total_weight.is_finite() {
            return Err("卖出倾向权重出现非有限数值".to_string());
        }
        if total_weight <= EPS {
            break;
        }

        let mut next_active = Vec::with_capacity(active.len());
        let mut removed = 0.0;

        for entry in active {
            let available = buckets[entry.bucket_index].chip(entry.holder);
            if available <= EPS {
                continue;
            }

            let target = remaining * entry.weight / total_weight;
            let sell_amount = target.min(available);
            if sell_amount > EPS {
                let bucket = &mut buckets[entry.bucket_index];
                let survival = (available - sell_amount) / available;
                for lot in &mut bucket.pending {
                    match entry.holder {
                        ChipHolder::Main => lot.main_chip *= survival,
                        ChipHolder::Retail => lot.retail_chip *= survival,
                    }
                }
                *bucket.chip_mut(entry.holder) -= sell_amount;
                removed += sell_amount;
            }

            if available - sell_amount > EPS {
                next_active.push(entry);
            }
        }

        if removed <= EPS {
            break;
        }
        remaining = (remaining - removed).max(0.0);
        active = next_active;
    }

    Ok(remaining)
}

pub(super) fn find_bucket_containing_price(buckets: &[ChipBucket], price: f64) -> Option<usize> {
    buckets
        .iter()
        .position(|bucket| bucket.price_low <= price + EPS && price <= bucket.price_high + EPS)
}

pub(super) fn nearest_bucket_index(buckets: &[ChipBucket], price: f64) -> usize {
    let mut best_index = 0usize;
    let mut best_distance = f64::INFINITY;
    for (index, bucket) in buckets.iter().enumerate() {
        let distance = (bucket.price() - price).abs();
        if distance < best_distance {
            best_distance = distance;
            best_index = index;
        }
    }
    best_index
}

pub(super) fn sanitize_buckets(buckets: &mut [ChipBucket]) -> Result<(), String> {
    for bucket in buckets {
        if !bucket.price_low.is_finite()
            || !bucket.price_high.is_finite()
            || !bucket.main_chip.is_finite()
            || !bucket.retail_chip.is_finite()
        {
            return Err("筹码分桶出现非有限数值".to_string());
        }
        if bucket.main_chip < 0.0 && bucket.main_chip.abs() <= EPS {
            bucket.main_chip = 0.0;
        }
        if bucket.retail_chip < 0.0 && bucket.retail_chip.abs() <= EPS {
            bucket.retail_chip = 0.0;
        }
        if bucket.main_chip.abs() <= EPS {
            bucket.main_chip = 0.0;
        }
        if bucket.retail_chip.abs() <= EPS {
            bucket.retail_chip = 0.0;
        }
        if bucket.main_chip < -EPS || bucket.retail_chip < -EPS {
            return Err("筹码分桶出现负筹码".to_string());
        }
    }
    Ok(())
}

pub(super) fn normalize_buckets(buckets: &mut [ChipBucket]) -> Result<(), String> {
    sanitize_buckets(buckets)?;
    let total = buckets.iter().map(ChipBucket::total_chip).sum::<f64>();
    if !total.is_finite() {
        return Err("筹码总量出现非有限数值".to_string());
    }
    if total <= EPS {
        return Err("筹码总量为0，无法归一化".to_string());
    }

    let scale = 100.0 / total;
    for bucket in buckets.iter_mut() {
        bucket.main_chip *= scale;
        bucket.retail_chip *= scale;
        for lot in &mut bucket.pending {
            lot.main_chip *= scale;
            lot.retail_chip *= scale;
        }
    }
    sanitize_buckets(buckets)?;
    Ok(())
}

pub(super) fn cost_by_chip(buckets: &[ChipBucket], chip_target: f64) -> f64 {
    let mut sum = 0.0;
    for bucket in buckets {
        let chip = bucket.total_chip();
        if sum + chip > chip_target {
            return bucket.price();
        }
        sum += chip;
    }

    buckets.last().map(ChipBucket::price).unwrap_or(0.0)
}

pub(super) fn build_percent_range(
    percent: f64,
    buckets: &[ChipBucket],
    total_chips: f64,
) -> ChenChipPercentRange {
    let low = if total_chips <= EPS {
        0.0
    } else {
        cost_by_chip(buckets, total_chips * (1.0 - percent) / 2.0)
    };
    let high = if total_chips <= EPS {
        0.0
    } else {
        cost_by_chip(buckets, total_chips * (1.0 + percent) / 2.0)
    };
    let concentration = if (low + high).abs() < EPS {
        0.0
    } else {
        (high - low) / (low + high)
    };

    ChenChipPercentRange {
        percent,
        price_low: low,
        price_high: high,
        concentration: round_ratio(concentration),
    }
}

pub(super) fn build_snapshot(
    bar: &ChenChipBar,
    buckets: &[ChipBucket],
) -> Result<ChenChipSnapshot, String> {
    let mut bins = Vec::with_capacity(buckets.len());
    for (index, bucket) in buckets.iter().enumerate() {
        let total_chip = bucket.total_chip();
        bins.push(ChenChipBin {
            index,
            price: finite_value(bucket.price())?,
            price_low: finite_value(bucket.price_low)?,
            price_high: finite_value(bucket.price_high)?,
            main_chip: finite_value(bucket.main_chip)?,
            retail_chip: finite_value(bucket.retail_chip)?,
            total_chip: finite_value(total_chip)?,
            pending: bucket.pending.clone(),
        });
    }

    let main_total = buckets.iter().map(|bucket| bucket.main_chip).sum::<f64>();
    let retail_total = buckets.iter().map(|bucket| bucket.retail_chip).sum::<f64>();
    let total_chips = main_total + retail_total;
    let profit_chips = buckets
        .iter()
        .filter(|bucket| bucket.price() <= bar.close + EPS)
        .map(ChipBucket::total_chip)
        .sum::<f64>();
    let main_profit_chips = buckets
        .iter()
        .filter(|bucket| bucket.price() <= bar.close + EPS)
        .map(|bucket| bucket.main_chip)
        .sum::<f64>();
    let total_profit_ratio = if total_chips <= EPS {
        0.0
    } else {
        profit_chips / total_chips
    };
    let total_trapped_ratio = if total_chips <= EPS {
        0.0
    } else {
        1.0 - total_profit_ratio
    };
    let main_profit_ratio = if main_total <= EPS {
        0.0
    } else {
        main_profit_chips / main_total
    };
    let main_trapped_ratio = if main_total <= EPS {
        0.0
    } else {
        1.0 - main_profit_ratio
    };
    let main_avg_cost = if main_total <= EPS {
        0.0
    } else {
        buckets
            .iter()
            .map(|bucket| bucket.price() * bucket.main_chip)
            .sum::<f64>()
            / main_total
    };
    let chip_peak_price = buckets
        .iter()
        .fold(None::<&ChipBucket>, |best, bucket| match best {
            Some(best) => {
                let bucket_chip = bucket.total_chip();
                let best_chip = best.total_chip();
                if bucket_chip > best_chip + EPS
                    || ((bucket_chip - best_chip).abs() <= EPS && bucket.price() < best.price())
                {
                    Some(bucket)
                } else {
                    Some(best)
                }
            }
            None => Some(bucket),
        })
        .map(ChipBucket::price)
        .unwrap_or(0.0);
    let min_price = buckets
        .first()
        .map(|bucket| bucket.price_low)
        .ok_or_else(|| "价格分桶为空，无法输出快照".to_string())?;
    let max_price = buckets
        .last()
        .map(|bucket| bucket.price_high)
        .ok_or_else(|| "价格分桶为空，无法输出快照".to_string())?;

    Ok(ChenChipSnapshot {
        trade_date: Some(bar.trade_date.clone()),
        close: finite_value(bar.close)?,
        min_price: finite_value(min_price)?,
        max_price: finite_value(max_price)?,
        main_total: finite_value(main_total)?,
        retail_total: finite_value(retail_total)?,
        total_chips: finite_value(total_chips)?,
        total_profit_ratio: finite_value(round_ratio(total_profit_ratio))?,
        total_trapped_ratio: finite_value(round_ratio(total_trapped_ratio))?,
        main_profit_ratio: finite_value(round_ratio(main_profit_ratio))?,
        main_trapped_ratio: finite_value(round_ratio(main_trapped_ratio))?,
        main_avg_cost: finite_value(main_avg_cost)?,
        chip_peak_price: finite_value(chip_peak_price)?,
        percent_70: build_percent_range(0.7, buckets, total_chips),
        percent_90: build_percent_range(0.9, buckets, total_chips),
        bins,
    })
}

pub(super) fn finite_value(value: f64) -> Result<f64, String> {
    if !value.is_finite() {
        return Err("输出快照出现非有限数值".to_string());
    }
    if value.abs() <= EPS {
        Ok(0.0)
    } else {
        Ok(value)
    }
}

impl ChipBucket {
    pub(super) fn price(&self) -> f64 {
        (self.price_low + self.price_high) / 2.0
    }

    pub(super) fn total_chip(&self) -> f64 {
        self.main_chip + self.retail_chip
    }

    fn chip(&self, holder: ChipHolder) -> f64 {
        match holder {
            ChipHolder::Main => self.main_chip,
            ChipHolder::Retail => self.retail_chip,
        }
    }

    fn chip_mut(&mut self, holder: ChipHolder) -> &mut f64 {
        match holder {
            ChipHolder::Main => &mut self.main_chip,
            ChipHolder::Retail => &mut self.retail_chip,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::data::RowData;
    use crate::data::cyq_chen::ChenChipBar;
    use crate::data::cyq_chen::ChipBucket;
    use crate::data::cyq_chen::ChipChangeConfig;
    use crate::data::cyq_chen::simulate::apply_sell_for_day;
    use crate::data::cyq_chen::test_support::*;
    use crate::data::runtime::row_into_rt;
    use std::collections::HashMap;
    use std::sync::Arc;

    #[test]
    fn sell_shortfall_uses_retail_trapped_chips_before_other_fallbacks() {
        let chip_config = ChipChangeConfig::from_toml_str(
            r#"
    version = 1

    [[strategy]]
    name = "main sell"
    holder = "main"
    direction = "sell"
    when = "C > 0"
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
                ("C".to_string(), vec![Some(10.0)]),
                ("TOR".to_string(), vec![Some(1.0)]),
            ]),
        };
        let bars = vec![Some(ChenChipBar {
            trade_date: "20240102".to_string(),
            open: 10.0,
            high: 10.2,
            low: 9.8,
            close: 10.0,
            turnover_rate: 1.0,
        })];
        let mut base_runtime = row_into_rt(row_data).expect("runtime should build");
        let main_ratio_history = vec![Arc::new(vec![Some(0.0)]), Arc::new(vec![Some(0.0)])];
        let mut buckets = vec![
            ChipBucket {
                price_low: 9.0,
                price_high: 10.0,
                main_chip: 0.1,
                retail_chip: 10.0,
                pending: Vec::new(),
                rateo: None,
                rateh: None,
                ratel: None,
                ratec: None,
            },
            ChipBucket {
                price_low: 10.0,
                price_high: 11.0,
                main_chip: 0.1,
                retail_chip: 10.0,
                pending: Vec::new(),
                rateo: None,
                rateh: None,
                ratel: None,
                ratec: None,
            },
        ];

        apply_sell_for_day(
            &mut buckets,
            &bars,
            &mut base_runtime,
            &chip_config,
            &main_ratio_history,
            0,
            1.0,
        )
        .expect("sell should apply");

        assert_close(buckets[0].main_chip, 0.0);
        assert_close(buckets[1].main_chip, 0.0);
        assert_close(buckets[0].retail_chip, 10.0);
        assert_close(buckets[1].retail_chip, 9.2);
    }
}
