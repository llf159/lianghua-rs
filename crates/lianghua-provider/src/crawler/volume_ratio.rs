use serde::Serialize;

use super::{
    intraday::TencentIntradayResponse, intraday::TencentIntradaySymbolWrapper,
    ts_code_to_tencent_code,
};

/// 连续交易已开市分钟数；首分钟按 1 分钟计，午休不累加，盘后不参与。
pub fn intraday_trading_minutes(time: &str) -> Option<f64> {
    let compact = time.replace(':', "");
    if !matches!(compact.len(), 4 | 6) || !compact.bytes().all(|ch| ch.is_ascii_digit()) {
        return None;
    }
    let hour: u32 = compact[..2].parse().ok()?;
    let minute: u32 = compact[2..4].parse().ok()?;
    let second: u32 = if compact.len() == 6 {
        compact[4..].parse().ok()?
    } else {
        0
    };
    if hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    let seconds = hour * 3600 + minute * 60 + second;
    match seconds {
        34200..=41400 => Some(((seconds - 34200) as f64 / 60.0).max(1.0)),
        46800..=54000 => Some(120.0 + (seconds - 46800) as f64 / 60.0),
        _ => None,
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct TencentVolumeRatioPoint {
    pub time: String,
    pub trading_minutes: f64,
    pub volume_ratio: f64,
}

#[derive(Debug, Clone)]
pub struct TencentVolumeRatioData {
    pub trade_date: String,
    pub average_daily_volume: f64,
    pub volume_to_hands: f64,
    pub points: Vec<TencentVolumeRatioPoint>,
}

pub fn parse_tencent_volume_ratio(
    intraday_raw: &str,
    ts_code: &str,
    average_daily_volume: f64,
) -> Result<TencentVolumeRatioData, String> {
    let symbol = ts_code_to_tencent_code(ts_code)?;
    let minute: TencentIntradayResponse =
        serde_json::from_str(intraday_raw).map_err(|e| format!("解析腾讯量比分时失败: {e}"))?;
    if minute.code != 0 {
        return Err(format!("腾讯量比分时请求失败: {}", minute.code));
    }
    let payload: TencentIntradaySymbolWrapper = serde_json::from_value(
        minute
            .data
            .get(&symbol)
            .ok_or("腾讯量比分时缺少标的")?
            .clone(),
    )
    .map_err(|e| format!("解析腾讯量比标的失败: {e}"))?;
    let trade_date = super::intraday::normalize_trade_date(&payload.data.date)?;
    if !average_daily_volume.is_finite() || average_daily_volume <= 0.0 {
        return Err("本地量比五日日均量无效".to_string());
    }
    let mut points = Vec::new();
    let mut previous_volume = 0.0;
    let mut previous_minutes = 0.0;
    let mut shares_per_volume_unit = None;
    for row in &payload.data.data {
        let fields = row.split_whitespace().collect::<Vec<_>>();
        if fields.len() < 3 {
            return Err(format!("腾讯量比分时字段不足: {row}"));
        }
        let Some(minutes) = intraday_trading_minutes(fields[0]) else {
            continue;
        };
        let volume = super::intraday::parse_non_negative_f64(fields[2], "cumulative_vol", row)?;
        if volume < previous_volume || minutes < previous_minutes {
            return Err("腾讯量比分时累计量或时间回退".to_string());
        }
        if shares_per_volume_unit.is_none() && volume > 0.0 {
            if let Some(amount) = fields.get(3) {
                let price = super::intraday::parse_non_negative_f64(fields[1], "price", row)?;
                let amount =
                    super::intraday::parse_non_negative_f64(amount, "cumulative_amount", row)?;
                if price > 0.0 && amount > 0.0 {
                    shares_per_volume_unit = Some(super::intraday::infer_shares_per_volume_unit(
                        price, volume, amount,
                    ));
                }
            } else if symbol.starts_with("bj") {
                shares_per_volume_unit = Some(100.0);
            } else {
                return Err("腾讯量比分时缺少成交额，无法确认成交量单位".to_string());
            }
        }
        let volume_to_hands = shares_per_volume_unit.unwrap_or(100.0) / 100.0;
        points.push(TencentVolumeRatioPoint {
            time: fields[0].to_string(),
            trading_minutes: minutes,
            volume_ratio: volume * volume_to_hands / (average_daily_volume * minutes / 240.0),
        });
        previous_volume = volume;
        previous_minutes = minutes;
    }
    if points.first().map(|point| point.time.as_str()) != Some("0930") {
        return Err("腾讯量比分时缺少开盘起点，无法确认全天基准".to_string());
    }
    Ok(TencentVolumeRatioData {
        trade_date,
        average_daily_volume,
        volume_to_hands: shares_per_volume_unit.unwrap_or(100.0) / 100.0,
        points,
    })
}

pub fn fetch_tencent_volume_ratio(
    http: &reqwest::blocking::Client,
    ts_code: &str,
    average_daily_volume: f64,
) -> Result<TencentVolumeRatioData, String> {
    let intraday_raw = super::intraday::fetch_tencent_intraday_text(http, ts_code)?;
    parse_tencent_volume_ratio(&intraday_raw, ts_code, average_daily_volume)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_hand_and_share_volumes_against_local_hand_baseline() {
        for (symbol, code, raw_volume, expected_factor) in [
            ("sz000001", "000001.SZ", 10, 1.0),
            ("sh688185", "688185.SH", 1000, 0.01),
        ] {
            let raw = serde_json::json!({
                "code": 0,
                "data": {symbol: {"data": {"date": "20261009", "data": [
                    "0930 10 0 0",
                    format!("0931 10 {raw_volume} 10000")
                ]}}}
            })
            .to_string();
            let data = parse_tencent_volume_ratio(&raw, code, 2400.0).unwrap();
            assert_eq!(data.volume_to_hands, expected_factor);
            assert_eq!(data.points[0].volume_ratio, 0.0);
            assert_eq!(data.points[1].volume_ratio, 1.0);
        }
    }

    #[test]
    fn reconstructs_from_open_and_excludes_current_day_lunch_and_post_market() {
        let minute = r#"{"code":0,"data":{"bj920002":{"data":{"date":"20261009","data":["0930 10 10","1000 10 300","1130 10 1200","1200 10 1200","1300 10 1200","1500 10 2400","1530 10 2500"]}}}}"#;
        let data = parse_tencent_volume_ratio(minute, "920002.BJ", 2400.0).unwrap();
        assert_eq!(data.volume_to_hands, 1.0);
        assert_eq!(data.average_daily_volume, 2400.0);
        assert_eq!(data.points.len(), 5);
        assert!(
            data.points
                .iter()
                .all(|point| (point.volume_ratio - 1.0).abs() < 1e-9)
        );
        assert_eq!(intraday_trading_minutes("09:30:00"), Some(1.0));
        assert_eq!(intraday_trading_minutes("10:00:30"), Some(30.5));
        assert_eq!(intraday_trading_minutes("13:00:00"), Some(120.0));
        assert_eq!(intraday_trading_minutes("15:00:01"), None);
        assert_eq!(intraday_trading_minutes("09:60"), None);
        assert!(
            parse_tencent_volume_ratio(&minute.replace("0930", "0931"), "920002.BJ", 2400.0)
                .is_err()
        );
    }
}
