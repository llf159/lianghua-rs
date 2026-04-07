use crate::{
    data::{RowData, scoring_data::row_into_rt},
    download::ind_calc::{cache_ind_build, calc_inds_with_cache},
    expr::eval::Runtime,
};

const EPS: f64 = 1e-12;

#[derive(Debug, Clone)]
pub struct SimulateBarInput {
    pub trade_date: Option<String>,
    pub pct_chg: f64,
    pub volume_ratio: f64,
}

impl SimulateBarInput {
    fn validate(&self) -> Result<(), String> {
        if !self.pct_chg.is_finite() {
            return Err("模拟涨幅必须是有限数字".to_string());
        }
        if !self.volume_ratio.is_finite() {
            return Err("模拟量比必须是有限数字".to_string());
        }
        if self.volume_ratio < 0.0 {
            return Err("模拟量比不能小于0".to_string());
        }
        Ok(())
    }
}

pub fn build_simulated_row_data(
    mut row_data: RowData,
    simulate: &SimulateBarInput,
) -> Result<RowData, String> {
    row_data.validate()?;
    simulate.validate()?;

    let fallback_trade_date = row_data
        .trade_dates
        .last()
        .map(String::as_str)
        .ok_or_else(|| "trade_dates为空".to_string())?;
    let simulated_trade_date = simulate
        .trade_date
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(fallback_trade_date)
        .to_string();

    let latest_value = |keys: &[&str]| -> Option<f64> {
        for key in keys {
            if let Some(series) = row_data.cols.get(*key) {
                if let Some(value) = series.iter().rev().find_map(|value| *value) {
                    return Some(value);
                }
            }
        }
        None
    };

    let previous_close = latest_value(&["C"]).ok_or_else(|| "模拟数据缺少最近的C".to_string())?;
    let previous_volume = latest_value(&["V"]).unwrap_or(0.0);
    let previous_amount = latest_value(&["AMOUNT"]).unwrap_or(0.0);
    let previous_turnover_rate = latest_value(&["TURNOVER_RATE", "TOR"]);

    let simulated_close = previous_close * (1.0 + simulate.pct_chg / 100.0);
    let simulated_open = previous_close;
    let simulated_high = simulated_open.max(simulated_close);
    let simulated_low = simulated_open.min(simulated_close);
    let simulated_change = simulated_close - previous_close;
    let simulated_volume = previous_volume * simulate.volume_ratio;
    let simulated_turnover_rate = previous_turnover_rate.map(|value| value * simulate.volume_ratio);

    let simulated_amount = if simulated_volume.abs() < EPS {
        0.0
    } else if previous_volume.abs() >= EPS && previous_amount.abs() >= EPS {
        let previous_avg_price = previous_amount / previous_volume;
        let close_ratio = if previous_close.abs() >= EPS {
            simulated_close / previous_close
        } else {
            1.0
        };
        simulated_volume * (previous_avg_price * (1.0 + close_ratio) / 2.0).max(0.0)
    } else {
        simulated_volume * ((previous_close + simulated_close) / 2.0).max(0.0)
    };

    row_data.trade_dates.push(simulated_trade_date);

    for series in row_data.cols.values_mut() {
        series.push(series.last().copied().flatten());
    }

    let total_len = row_data.trade_dates.len();
    let has_turnover_col = previous_turnover_rate.is_some()
        || row_data.cols.contains_key("TURNOVER_RATE")
        || row_data.cols.contains_key("TOR");

    for (key, value) in [
        ("O", Some(simulated_open)),
        ("H", Some(simulated_high)),
        ("L", Some(simulated_low)),
        ("C", Some(simulated_close)),
        ("PRE_CLOSE", Some(previous_close)),
        ("CHANGE", Some(simulated_change)),
        ("PCT_CHG", Some(simulate.pct_chg)),
        ("V", Some(simulated_volume)),
        ("AMOUNT", Some(simulated_amount)),
    ] {
        let series = row_data
            .cols
            .entry(key.to_string())
            .or_insert_with(|| vec![None; total_len]);
        if series.len() < total_len {
            series.resize(total_len, None);
        }
        if let Some(last) = series.last_mut() {
            *last = value;
        }
    }

    if has_turnover_col {
        for key in ["TURNOVER_RATE", "TOR"] {
            let series = row_data
                .cols
                .entry(key.to_string())
                .or_insert_with(|| vec![None; total_len]);
            if series.len() < total_len {
                series.resize(total_len, None);
            }
            if let Some(last) = series.last_mut() {
                *last = simulated_turnover_rate;
            }
        }
    }

    row_data.validate()?;
    Ok(row_data)
}

pub fn build_simulated_runtime(
    source_dir: &str,
    row_data: RowData,
    simulate: &SimulateBarInput,
) -> Result<Runtime, String> {
    let mut row_data = build_simulated_row_data(row_data, simulate)?;
    let indicator_cache = cache_ind_build(source_dir)?;

    if !indicator_cache.is_empty() {
        for (name, series) in calc_inds_with_cache(&indicator_cache, row_data.clone())? {
            row_data.cols.insert(name, series);
        }
    }

    row_into_rt(row_data)
}

#[cfg(test)]
mod tests {
    use super::{SimulateBarInput, build_simulated_row_data, build_simulated_runtime};
    use crate::expr::eval::Value;
    use std::collections::HashMap;

    fn sample_row_data() -> crate::data::RowData {
        let mut cols = HashMap::new();
        cols.insert("O".to_string(), vec![Some(9.8), Some(10.0)]);
        cols.insert("H".to_string(), vec![Some(10.1), Some(10.3)]);
        cols.insert("L".to_string(), vec![Some(9.7), Some(9.9)]);
        cols.insert("C".to_string(), vec![Some(10.0), Some(10.2)]);
        cols.insert("V".to_string(), vec![Some(100.0), Some(120.0)]);
        cols.insert("AMOUNT".to_string(), vec![Some(1000.0), Some(1260.0)]);
        cols.insert("PRE_CLOSE".to_string(), vec![Some(9.7), Some(10.0)]);
        cols.insert("CHANGE".to_string(), vec![Some(0.3), Some(0.2)]);
        cols.insert("PCT_CHG".to_string(), vec![Some(3.0928), Some(2.0)]);
        cols.insert("TOR".to_string(), vec![Some(1.0), Some(1.2)]);
        cols.insert("ZHANG".to_string(), vec![Some(0.095), Some(0.095)]);

        crate::data::RowData {
            trade_dates: vec!["20260403".to_string(), "20260407".to_string()],
            cols,
        }
    }

    #[test]
    fn build_simulated_row_data_appends_new_tail() {
        let input = SimulateBarInput {
            trade_date: Some("20260408".to_string()),
            pct_chg: 5.0,
            volume_ratio: 1.5,
        };

        let row_data = build_simulated_row_data(sample_row_data(), &input).expect("simulate row");

        assert_eq!(
            row_data.trade_dates.last().map(String::as_str),
            Some("20260408")
        );
        assert_eq!(
            row_data.cols["PRE_CLOSE"].last().copied().flatten(),
            Some(10.2)
        );
        assert_eq!(row_data.cols["C"].last().copied().flatten(), Some(10.71));
        assert_eq!(row_data.cols["V"].last().copied().flatten(), Some(180.0));
        assert_eq!(row_data.cols["TOR"].last().copied().flatten(), Some(1.8));
        assert_eq!(
            row_data.cols["TURNOVER_RATE"].last().copied().flatten(),
            Some(1.8)
        );
        assert_eq!(
            row_data.cols["ZHANG"].last().copied().flatten(),
            Some(0.095)
        );
    }

    #[test]
    fn build_simulated_runtime_keeps_latest_tail_values() {
        let input = SimulateBarInput {
            trade_date: Some("20260408".to_string()),
            pct_chg: -2.0,
            volume_ratio: 0.5,
        };

        let runtime =
            build_simulated_runtime("/tmp/lianghua-simulate-test", sample_row_data(), &input)
                .expect("simulate runtime");

        let close_series = match runtime.vars.get("C") {
            Some(Value::NumSeries(series)) => series.clone(),
            _ => panic!("missing close series"),
        };
        let volume_series = match runtime.vars.get("V") {
            Some(Value::NumSeries(series)) => series.clone(),
            _ => panic!("missing volume series"),
        };

        assert_eq!(close_series.last().copied().flatten(), Some(9.996));
        assert_eq!(volume_series.last().copied().flatten(), Some(60.0));
    }
}
