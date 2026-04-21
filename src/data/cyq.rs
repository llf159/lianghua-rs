use serde::Serialize;

use crate::data::RowData;

const DEFAULT_RANGE: usize = 120;
const DEFAULT_FACTOR: usize = 50;
const DEFAULT_MIN_ACCURACY: f64 = 0.01;
const EPS: f64 = 1e-12;

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CyqConfig {
    pub range: usize,
    pub factor: usize,
    pub min_accuracy: f64,
}

impl Default for CyqConfig {
    fn default() -> Self {
        Self {
            range: DEFAULT_RANGE,
            factor: DEFAULT_FACTOR,
            min_accuracy: DEFAULT_MIN_ACCURACY,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CyqBar {
    pub trade_date: Option<String>,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub turnover_rate: f64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CyqBin {
    pub index: usize,
    pub price: f64,
    pub price_low: f64,
    pub price_high: f64,
    pub chip: f64,
    pub chip_pct: f64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CyqPercentRange {
    pub percent: f64,
    pub price_low: f64,
    pub price_high: f64,
    pub concentration: f64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CyqSnapshot {
    pub trade_date: Option<String>,
    pub close: f64,
    pub min_price: f64,
    pub max_price: f64,
    pub accuracy: f64,
    pub total_chips: f64,
    pub benefit_part: f64,
    pub avg_cost: f64,
    pub percent_70: CyqPercentRange,
    pub percent_90: CyqPercentRange,
    pub bins: Vec<CyqBin>,
}

fn round_price(value: f64) -> f64 {
    (value * 100.0).round() / 100.0
}

fn round_ratio(value: f64) -> f64 {
    (value * 1_000_000_000.0).round() / 1_000_000_000.0
}

fn clamp_index(value: isize, upper: usize) -> usize {
    value.clamp(0, upper.saturating_sub(1) as isize) as usize
}

fn required_series<'a>(row_data: &'a RowData, key: &str) -> Result<&'a [Option<f64>], String> {
    row_data
        .cols
        .get(key)
        .map(Vec::as_slice)
        .ok_or_else(|| format!("RowData 缺少 {key} 列"))
}

fn optional_turnover_series<'a>(row_data: &'a RowData) -> Result<&'a [Option<f64>], String> {
    if let Some(series) = row_data.cols.get("TURNOVER_RATE") {
        return Ok(series.as_slice());
    }
    if let Some(series) = row_data.cols.get("TOR") {
        return Ok(series.as_slice());
    }
    Err("RowData 缺少 TURNOVER_RATE/TOR 列".to_string())
}

pub fn build_cyq_bars_from_row_data(row_data: &RowData) -> Result<Vec<CyqBar>, String> {
    row_data.validate()?;

    let open_series = required_series(row_data, "O")?;
    let high_series = required_series(row_data, "H")?;
    let low_series = required_series(row_data, "L")?;
    let close_series = required_series(row_data, "C")?;
    let turnover_series = optional_turnover_series(row_data)?;

    let mut bars = Vec::with_capacity(row_data.trade_dates.len());

    for index in 0..row_data.trade_dates.len() {
        let trade_date = row_data.trade_dates[index].clone();
        let open = open_series[index].ok_or_else(|| format!("{trade_date} 缺少 O"))?;
        let high = high_series[index].ok_or_else(|| format!("{trade_date} 缺少 H"))?;
        let low = low_series[index].ok_or_else(|| format!("{trade_date} 缺少 L"))?;
        let close = close_series[index].ok_or_else(|| format!("{trade_date} 缺少 C"))?;
        let turnover_rate = turnover_series[index].unwrap_or(0.0);

        if !open.is_finite() || !high.is_finite() || !low.is_finite() || !close.is_finite() {
            return Err(format!("{trade_date} 存在非有限数值的 OHLC"));
        }
        if !turnover_rate.is_finite() {
            return Err(format!("{trade_date} 存在非有限数值的换手率"));
        }
        if high + EPS < low {
            return Err(format!("{trade_date} 的最高价小于最低价"));
        }

        bars.push(CyqBar {
            trade_date: Some(trade_date),
            open,
            high,
            low,
            close,
            turnover_rate,
        });
    }

    Ok(bars)
}

fn cost_by_chip(chips: &[f64], chip_target: f64, min_price: f64, accuracy: f64) -> f64 {
    let mut sum = 0.0;
    for (index, chip) in chips.iter().enumerate() {
        if sum + chip > chip_target {
            return round_price(min_price + index as f64 * accuracy);
        }
        sum += chip;
    }

    round_price(min_price + (chips.len().saturating_sub(1)) as f64 * accuracy)
}

fn build_percent_range(
    percent: f64,
    chips: &[f64],
    total_chips: f64,
    min_price: f64,
    accuracy: f64,
) -> CyqPercentRange {
    let low = cost_by_chip(
        chips,
        total_chips * (1.0 - percent) / 2.0,
        min_price,
        accuracy,
    );
    let high = cost_by_chip(
        chips,
        total_chips * (1.0 + percent) / 2.0,
        min_price,
        accuracy,
    );
    let concentration = if (low + high).abs() < EPS {
        0.0
    } else {
        (high - low) / (low + high)
    };

    CyqPercentRange {
        percent,
        price_low: low,
        price_high: high,
        concentration: round_ratio(concentration),
    }
}

pub fn compute_cyq_snapshot(
    bars: &[CyqBar],
    index: usize,
    config: CyqConfig,
) -> Result<CyqSnapshot, String> {
    if bars.is_empty() {
        return Err("bars 不能为空".to_string());
    }
    if index >= bars.len() {
        return Err(format!("index 越界: {index} >= {}", bars.len()));
    }
    if config.factor < 2 {
        return Err("factor 必须 >= 2".to_string());
    }
    if !config.min_accuracy.is_finite() || config.min_accuracy <= 0.0 {
        return Err("min_accuracy 必须是正数".to_string());
    }

    let start = if config.range == 0 {
        0
    } else {
        index.saturating_sub(config.range.saturating_sub(1))
    };
    let window = &bars[start..=index];

    let mut max_price = 0.0;
    let mut min_price = 0.0;
    for (window_index, bar) in window.iter().enumerate() {
        if !bar.high.is_finite()
            || !bar.low.is_finite()
            || !bar.open.is_finite()
            || !bar.close.is_finite()
            || !bar.turnover_rate.is_finite()
        {
            return Err(format!("第 {} 根K线存在非有限数值", start + window_index));
        }
        if bar.high + EPS < bar.low {
            return Err(format!(
                "第 {} 根K线的最高价小于最低价",
                start + window_index
            ));
        }

        max_price = if window_index == 0 {
            bar.high
        } else {
            max_price.max(bar.high)
        };
        min_price = if window_index == 0 {
            bar.low
        } else {
            min_price.min(bar.low)
        };
    }

    let accuracy = config
        .min_accuracy
        .max((max_price - min_price) / (config.factor as f64 - 1.0));
    let mut chips = vec![0.0; config.factor];

    for bar in window {
        let avg = (bar.open + bar.close + bar.high + bar.low) / 4.0;
        let turnover_rate = (bar.turnover_rate / 100.0).clamp(0.0, 1.0);

        for chip in &mut chips {
            *chip *= 1.0 - turnover_rate;
        }

        let high_index = clamp_index(
            ((bar.high - min_price) / accuracy).floor() as isize,
            config.factor,
        );
        let low_index = clamp_index(
            ((bar.low - min_price) / accuracy).ceil() as isize,
            config.factor,
        );
        let avg_index = clamp_index(
            ((avg - min_price) / accuracy).floor() as isize,
            config.factor,
        );

        if (bar.high - bar.low).abs() < EPS {
            chips[avg_index] += (config.factor as f64 - 1.0) * turnover_rate / 2.0;
            continue;
        }

        let slope = 2.0 / (bar.high - bar.low);
        for (offset, chip) in chips[low_index..=high_index].iter_mut().enumerate() {
            let grid_index = low_index + offset;
            let current_price = min_price + accuracy * grid_index as f64;

            if current_price <= avg {
                if (avg - bar.low).abs() < EPS {
                    *chip += slope * turnover_rate;
                } else {
                    *chip += (current_price - bar.low) / (avg - bar.low) * slope * turnover_rate;
                }
            } else if (bar.high - avg).abs() < EPS {
                *chip += slope * turnover_rate;
            } else {
                *chip += (bar.high - current_price) / (bar.high - avg) * slope * turnover_rate;
            }
        }
    }

    for chip in &mut chips {
        if chip.abs() < EPS {
            *chip = 0.0;
        } else if *chip < 0.0 {
            *chip = 0.0;
        }
    }

    let total_chips = chips.iter().sum::<f64>();
    let close = bars[index].close;
    let benefit_sum = chips
        .iter()
        .enumerate()
        .filter_map(|(grid_index, chip)| {
            let grid_price = min_price + accuracy * grid_index as f64;
            if close + EPS >= grid_price {
                Some(*chip)
            } else {
                None
            }
        })
        .sum::<f64>();

    let benefit_part = if total_chips.abs() < EPS {
        0.0
    } else {
        benefit_sum / total_chips
    };
    let avg_cost = if total_chips.abs() < EPS {
        0.0
    } else {
        cost_by_chip(&chips, total_chips * 0.5, min_price, accuracy)
    };

    let mut bins = Vec::with_capacity(config.factor);
    for (grid_index, chip) in chips.iter().enumerate() {
        let price = round_price(min_price + accuracy * grid_index as f64);
        let next_price = if grid_index + 1 < config.factor {
            round_price(min_price + accuracy * (grid_index + 1) as f64)
        } else {
            round_price(max_price)
        };
        let chip_pct = if total_chips.abs() < EPS {
            0.0
        } else {
            chip / total_chips
        };

        bins.push(CyqBin {
            index: grid_index,
            price,
            price_low: price,
            price_high: next_price.max(price),
            chip: round_ratio(*chip),
            chip_pct: round_ratio(chip_pct),
        });
    }

    Ok(CyqSnapshot {
        trade_date: bars[index].trade_date.clone(),
        close: round_price(close),
        min_price: round_price(min_price),
        max_price: round_price(max_price),
        accuracy: round_price(accuracy),
        total_chips: round_ratio(total_chips),
        benefit_part: round_ratio(benefit_part),
        avg_cost,
        percent_70: build_percent_range(0.7, &chips, total_chips, min_price, accuracy),
        percent_90: build_percent_range(0.9, &chips, total_chips, min_price, accuracy),
        bins,
    })
}

pub fn compute_cyq_snapshots(
    bars: &[CyqBar],
    config: CyqConfig,
) -> Result<Vec<CyqSnapshot>, String> {
    let mut snapshots = Vec::with_capacity(bars.len());
    for index in 0..bars.len() {
        snapshots.push(compute_cyq_snapshot(bars, index, config)?);
    }
    Ok(snapshots)
}

pub fn compute_cyq_snapshots_from_row_data(
    row_data: &RowData,
    config: CyqConfig,
) -> Result<Vec<CyqSnapshot>, String> {
    let bars = build_cyq_bars_from_row_data(row_data)?;
    compute_cyq_snapshots(&bars, config)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{
        build_cyq_bars_from_row_data, compute_cyq_snapshot, compute_cyq_snapshots_from_row_data,
        CyqConfig,
    };
    use crate::data::RowData;

    fn sample_row_data() -> RowData {
        let mut cols = HashMap::new();
        cols.insert(
            "O".to_string(),
            vec![Some(10.0), Some(10.2), Some(10.4), Some(10.5)],
        );
        cols.insert(
            "H".to_string(),
            vec![Some(10.3), Some(10.5), Some(10.6), Some(10.8)],
        );
        cols.insert(
            "L".to_string(),
            vec![Some(9.9), Some(10.1), Some(10.2), Some(10.3)],
        );
        cols.insert(
            "C".to_string(),
            vec![Some(10.2), Some(10.4), Some(10.5), Some(10.7)],
        );
        cols.insert(
            "TOR".to_string(),
            vec![Some(1.2), Some(1.6), Some(2.1), Some(2.4)],
        );

        RowData {
            trade_dates: vec![
                "20260401".to_string(),
                "20260402".to_string(),
                "20260403".to_string(),
                "20260407".to_string(),
            ],
            cols,
        }
    }

    #[test]
    fn build_cyq_bars_from_row_data_supports_tor_alias() {
        let bars = build_cyq_bars_from_row_data(&sample_row_data()).expect("build cyq bars");

        assert_eq!(bars.len(), 4);
        assert_eq!(bars[0].trade_date.as_deref(), Some("20260401"));
        assert!((bars[2].turnover_rate - 2.1).abs() < 1e-9);
    }

    #[test]
    fn compute_cyq_snapshot_builds_normalized_bins() {
        let bars = build_cyq_bars_from_row_data(&sample_row_data()).expect("build cyq bars");
        let snapshot = compute_cyq_snapshot(
            &bars,
            3,
            CyqConfig {
                range: 4,
                factor: 20,
                min_accuracy: 0.01,
            },
        )
        .expect("compute cyq snapshot");

        assert_eq!(snapshot.trade_date.as_deref(), Some("20260407"));
        assert_eq!(snapshot.bins.len(), 20);

        let pct_sum = snapshot.bins.iter().map(|bin| bin.chip_pct).sum::<f64>();
        assert!((pct_sum - 1.0).abs() < 1e-6, "pct_sum={pct_sum}");
        assert!(snapshot.avg_cost >= snapshot.min_price);
        assert!(snapshot.avg_cost <= snapshot.max_price);
        assert!(snapshot.percent_90.price_low <= snapshot.percent_90.price_high);
        assert!(snapshot.percent_70.price_low <= snapshot.percent_70.price_high);
    }

    #[test]
    fn compute_cyq_snapshots_from_row_data_returns_one_snapshot_per_day() {
        let snapshots = compute_cyq_snapshots_from_row_data(
            &sample_row_data(),
            CyqConfig {
                range: 3,
                factor: 16,
                min_accuracy: 0.01,
            },
        )
        .expect("compute cyq snapshots");

        assert_eq!(snapshots.len(), 4);
        assert_eq!(snapshots[0].trade_date.as_deref(), Some("20260401"));
        assert_eq!(snapshots[3].trade_date.as_deref(), Some("20260407"));
        assert!(snapshots.iter().all(|item| item.total_chips >= 0.0));
    }
}
