use crate::data::cyq_chen::{ChenChipBar, ChipBucket, EPS};
// 见父模块 mod.rs

use crate::data::RowData;
use std::sync::Arc;
pub(super) fn validate_row_shape(row_data: &RowData) -> Result<(), String> {
    row_data.validate()?;

    for window in row_data.trade_dates.windows(2) {
        if window[0] >= window[1] {
            return Err("RowData.trade_dates必须升序且不可重复".to_string());
        }
    }

    required_series(row_data, "O")?;
    required_series(row_data, "H")?;
    required_series(row_data, "L")?;
    required_series(row_data, "C")?;
    turnover_series(row_data)?;

    Ok(())
}

pub(super) fn required_series<'a>(
    row_data: &'a RowData,
    key: &str,
) -> Result<&'a [Option<f64>], String> {
    row_data
        .cols
        .get(key)
        .map(Vec::as_slice)
        .ok_or_else(|| format!("RowData 缺少 {key} 列"))
}

pub(super) fn turnover_series(row_data: &RowData) -> Result<&[Option<f64>], String> {
    row_data
        .cols
        .get("TOR")
        .map(Vec::as_slice)
        .ok_or_else(|| "RowData 缺少 TOR 列".to_string())
}

pub(super) fn build_validated_bars(
    row_data: &RowData,
    output_start_index: usize,
) -> Result<Vec<Option<ChenChipBar>>, String> {
    let open_series = required_series(row_data, "O")?;
    let high_series = required_series(row_data, "H")?;
    let low_series = required_series(row_data, "L")?;
    let close_series = required_series(row_data, "C")?;
    let turnover_series = turnover_series(row_data)?;

    let mut bars = Vec::with_capacity(row_data.trade_dates.len());
    for index in 0..row_data.trade_dates.len() {
        match (|trade_date: &str,
                open: Option<f64>,
                high: Option<f64>,
                low: Option<f64>,
                close: Option<f64>,
                turnover_rate: Option<f64>|
         -> Result<ChenChipBar, String> {
            let open = required_value(trade_date, "O", open)?;
            let high = required_value(trade_date, "H", high)?;
            let low = required_value(trade_date, "L", low)?;
            let close = required_value(trade_date, "C", close)?;
            let turnover_rate = required_value(trade_date, "TOR", turnover_rate)?;

            for (name, value) in [("O", open), ("H", high), ("L", low), ("C", close)] {
                if !value.is_finite() || value <= 0.0 {
                    return Err(format!("{trade_date} 的{name}必须是有限正数"));
                }
            }
            if high + EPS < low {
                return Err(format!("{trade_date} 的最高价小于最低价"));
            }
            if !turnover_rate.is_finite() || !(0.0..=100.0).contains(&turnover_rate) {
                return Err(format!("{trade_date} 的换手率必须是[0,100]之间的有限数值"));
            }

            Ok(ChenChipBar {
                trade_date: trade_date.to_string(),
                open,
                high,
                low,
                close,
                turnover_rate,
            })
        })(
            row_data.trade_dates[index].as_str(),
            open_series[index],
            high_series[index],
            low_series[index],
            close_series[index],
            turnover_series[index],
        ) {
            Ok(bar) => bars.push(Some(bar)),
            Err(error) if index < output_start_index => {
                let _ = error;
                bars.push(None);
            }
            Err(error) => return Err(error),
        }
    }

    Ok(bars)
}

pub(super) fn required_value(
    trade_date: &str,
    name: &str,
    value: Option<f64>,
) -> Result<f64, String> {
    value.ok_or_else(|| format!("{trade_date} 缺少 {name}"))
}

pub(super) fn bucket_step(bucket_pct: f64) -> f64 {
    1.0 + bucket_pct / 100.0
}

pub(super) fn compute_bucket_rate_series(
    bars: &[Option<ChenChipBar>],
    cost_price: f64,
) -> (
    Arc<Vec<Option<f64>>>,
    Arc<Vec<Option<f64>>>,
    Arc<Vec<Option<f64>>>,
    Arc<Vec<Option<f64>>>,
) {
    let mut rateo = Vec::with_capacity(bars.len());
    let mut rateh = Vec::with_capacity(bars.len());
    let mut ratel = Vec::with_capacity(bars.len());
    let mut ratec = Vec::with_capacity(bars.len());

    for bar in bars {
        if let Some(bar) = bar {
            rateo.push(Some((bar.open - cost_price) / cost_price * 100.0));
            rateh.push(Some((bar.high - cost_price) / cost_price * 100.0));
            ratel.push(Some((bar.low - cost_price) / cost_price * 100.0));
            ratec.push(Some((bar.close - cost_price) / cost_price * 100.0));
        } else {
            rateo.push(None);
            rateh.push(None);
            ratel.push(None);
            ratec.push(None);
        }
    }

    (
        Arc::new(rateo),
        Arc::new(rateh),
        Arc::new(ratel),
        Arc::new(ratec),
    )
}

pub(super) fn ensure_bucket_rate_series(buckets: &mut [ChipBucket], bars: &[Option<ChenChipBar>]) {
    for bucket in buckets {
        if bucket.rateo.is_some() {
            continue;
        }
        let (rateo, rateh, ratel, ratec) = compute_bucket_rate_series(bars, bucket.price());
        bucket.rateo = Some(rateo);
        bucket.rateh = Some(rateh);
        bucket.ratel = Some(ratel);
        bucket.ratec = Some(ratec);
    }
}

pub(super) fn expand_buckets_for_bar(
    buckets: &mut Vec<ChipBucket>,
    main_ratio_history: &mut Vec<Arc<Vec<Option<f64>>>>,
    series_len: usize,
    low: f64,
    high: f64,
    step: f64,
    bars: &[Option<ChenChipBar>],
    needs_bucket_rate_series: bool,
) -> Result<(), String> {
    if buckets.is_empty() {
        return Err("价格分桶为空，无法计算筹码快照".to_string());
    }

    let first_low = buckets.first().expect("bucket exists").price_low;
    let last_high = buckets.last().expect("bucket exists").price_high;

    let low_expand_count = if low + EPS < first_low {
        let mut boundary = first_low;
        let mut count = 0usize;
        while low + EPS < boundary {
            let new_low = boundary / step;
            if !new_low.is_finite() || new_low <= 0.0 || new_low + EPS >= boundary {
                return Err("向下扩展价格分桶失败".to_string());
            }
            boundary = new_low;
            count += 1;
        }
        count
    } else {
        0
    };

    let high_expand_count = if high > last_high + EPS {
        let mut boundary = last_high;
        let mut count = 0usize;
        while high > boundary + EPS {
            let new_high = boundary * step;
            if !new_high.is_finite() || new_high <= boundary + EPS {
                return Err("向上扩展价格分桶失败".to_string());
            }
            boundary = new_high;
            count += 1;
        }
        count
    } else {
        0
    };

    if low_expand_count > 0 {
        let mut new_boundaries = Vec::with_capacity(low_expand_count + 1);
        new_boundaries.push(first_low);
        for _ in 0..low_expand_count {
            let prev = *new_boundaries.last().expect("boundary exists");
            let new_low = prev / step;
            new_boundaries.push(new_low);
        }
        new_boundaries.reverse();

        let mut new_buckets = Vec::with_capacity(low_expand_count);
        let mut new_histories = Vec::with_capacity(low_expand_count);
        for w in new_boundaries.windows(2) {
            let (pl, ph) = (w[0], w[1]);
            let rate_series =
                needs_bucket_rate_series.then(|| compute_bucket_rate_series(bars, (pl + ph) / 2.0));
            new_buckets.push(ChipBucket {
                price_low: pl,
                price_high: ph,
                main_chip: 0.0,
                retail_chip: 0.0,
                pending: Vec::new(),
                rateo: rate_series.as_ref().map(|series| Arc::clone(&series.0)),
                rateh: rate_series.as_ref().map(|series| Arc::clone(&series.1)),
                ratel: rate_series.as_ref().map(|series| Arc::clone(&series.2)),
                ratec: rate_series.map(|series| series.3),
            });
            new_histories.push(Arc::new(vec![None; series_len]));
        }

        let insert_pos = 0;
        buckets.splice(insert_pos..insert_pos, new_buckets);
        main_ratio_history.splice(insert_pos..insert_pos, new_histories);
    }

    if high_expand_count > 0 {
        let mut boundary = last_high;
        let mut new_buckets = Vec::with_capacity(high_expand_count);
        let mut new_histories = Vec::with_capacity(high_expand_count);
        for _ in 0..high_expand_count {
            let new_high = boundary * step;
            let rate_series = needs_bucket_rate_series
                .then(|| compute_bucket_rate_series(bars, (boundary + new_high) / 2.0));
            new_buckets.push(ChipBucket {
                price_low: boundary,
                price_high: new_high,
                main_chip: 0.0,
                retail_chip: 0.0,
                pending: Vec::new(),
                rateo: rate_series.as_ref().map(|series| Arc::clone(&series.0)),
                rateh: rate_series.as_ref().map(|series| Arc::clone(&series.1)),
                ratel: rate_series.as_ref().map(|series| Arc::clone(&series.2)),
                ratec: rate_series.map(|series| series.3),
            });
            new_histories.push(Arc::new(vec![None; series_len]));
            boundary = new_high;
        }
        buckets.extend(new_buckets);
        main_ratio_history.extend(new_histories);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::data::cyq_chen::ChenChipBar;
    use crate::data::cyq_chen::ChipBucket;
    use crate::data::cyq_chen::ChipChangeConfig;
    use crate::data::cyq_chen::bars::expand_buckets_for_bar;
    use std::sync::Arc;

    #[test]
    fn sell_bucket_rate_series_are_enabled_only_when_strategy_uses_rate_fields() {
        let no_rate = ChipChangeConfig::from_toml_str(
            r#"
    version = 1

    [[strategy]]
    name = "plain sell"
    holder = "main"
    direction = "sell"
    when = "C > O"
    bias = 1.0

    [[strategy]]
    name = "rate buy"
    holder = "main"
    direction = "buy"
    when = "rateh > 1"
    bias = 1.0
    "#,
        )
        .expect("config should parse")
        .compile()
        .expect("config should compile");
        assert!(!no_rate.sell_uses_bucket_rate_series);

        let with_rate = ChipChangeConfig::from_toml_str(
            r#"
    version = 1

    [[strategy]]
    name = "rate sell"
    holder = "main"
    direction = "sell"
    when = "HHV(rateo, 2) > 1"
    bias = 1.0
    "#,
        )
        .expect("config should parse")
        .compile()
        .expect("config should compile");
        assert!(with_rate.sell_uses_bucket_rate_series);

        let shadowed_rate = ChipChangeConfig::from_toml_str(
            r#"
    version = 1

    [[strategy]]
    name = "local rate name"
    holder = "main"
    direction = "sell"
    when = "RATEH := C; RATEH > O"
    bias = 1.0
    "#,
        )
        .expect("config should parse")
        .compile()
        .expect("config should compile");
        assert!(!shadowed_rate.sell_uses_bucket_rate_series);
    }

    #[test]
    fn incremental_bucket_expansion_skips_rate_history_when_unused() {
        let bars = vec![
            Some(ChenChipBar {
                trade_date: "20240102".to_string(),
                open: 10.0,
                high: 10.2,
                low: 9.8,
                close: 10.1,
                turnover_rate: 1.0,
            }),
            Some(ChenChipBar {
                trade_date: "20240103".to_string(),
                open: 12.0,
                high: 12.2,
                low: 11.8,
                close: 12.1,
                turnover_rate: 1.0,
            }),
        ];
        let mut buckets = vec![ChipBucket {
            price_low: 9.0,
            price_high: 11.0,
            main_chip: 50.0,
            retail_chip: 50.0,
            pending: Vec::new(),
            rateo: None,
            rateh: None,
            ratel: None,
            ratec: None,
        }];
        let mut main_ratio_history = vec![Arc::new(vec![None; bars.len()])];

        expand_buckets_for_bar(
            &mut buckets,
            &mut main_ratio_history,
            bars.len(),
            11.8,
            12.2,
            1.1,
            &bars,
            false,
        )
        .expect("expand buckets");

        assert!(buckets.len() > 1);
        assert!(buckets.iter().all(|bucket| {
            bucket.rateo.is_none()
                && bucket.rateh.is_none()
                && bucket.ratel.is_none()
                && bucket.ratec.is_none()
        }));
    }
}
