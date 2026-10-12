use serde::Serialize;
use std::{
    collections::HashMap,
    sync::{Mutex, OnceLock},
};

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

/// 东财返回每分钟量（含盘前竞价），先累加后复用严格的分时量比解析。
pub fn parse_eastmoney_volume_ratio(
    raw: &str,
    ts_code: &str,
    average_daily_volume: f64,
) -> Result<TencentVolumeRatioData, String> {
    let response: serde_json::Value =
        serde_json::from_str(raw).map_err(|e| format!("解析东财分时失败: {e}"))?;
    if response["rc"].as_i64() != Some(0) {
        return Err(format!("东财分时返回错误: {}", response["rc"]));
    }
    let symbol = ts_code_to_tencent_code(ts_code)?;
    let data = &response["data"];
    if data["code"].as_str() != Some(&symbol[2..])
        || data["market"].as_i64() != Some(if symbol.starts_with("sh") { 1 } else { 0 })
    {
        return Err(format!("东财分时标的不匹配: {ts_code}"));
    }
    let rows = data["trends"].as_array().ok_or("东财分时缺少数据")?;
    let mut trade_date = None;
    let mut cumulative_vol = 0.0;
    let mut cumulative_amount = 0.0;
    let mut normalized = Vec::new();
    let mut previous_time = String::new();
    for row in rows {
        let raw = row.as_str().ok_or("东财分时行不是文本")?;
        let fields = raw.split(',').collect::<Vec<_>>();
        if fields.len() < 7 {
            return Err(format!("东财分时字段不足: {raw}"));
        }
        let (date, time) = fields[0].split_once(' ').ok_or("东财分时时间格式错误")?;
        let date = super::intraday::normalize_trade_date(&date.replace('-', ""))?;
        if trade_date
            .as_ref()
            .is_some_and(|previous| previous != &date)
        {
            return Err("东财单日分时包含多个日期".to_string());
        }
        trade_date = Some(date);
        let time = time.replace(':', "");
        if time.len() != 4
            || !time.bytes().all(|value| value.is_ascii_digit())
            || (!previous_time.is_empty() && time <= previous_time)
        {
            return Err("东财分时时间乱序或重复".to_string());
        }
        previous_time = time.clone();
        let volume = super::intraday::parse_non_negative_f64(fields[5], "volume", raw)?;
        let amount = super::intraday::parse_non_negative_f64(fields[6], "amount", raw)?;
        // 09:26 的竞价量必须带到 09:30，午休之后也沿用上午累计量。
        if time.as_str() <= "1500" {
            cumulative_vol += volume;
            cumulative_amount += amount;
        }
        if intraday_trading_minutes(&time).is_some() {
            normalized.push(format!(
                "{time} {} {cumulative_vol} {cumulative_amount}",
                fields[2]
            ));
        }
    }
    let trade_date = trade_date.ok_or("东财分时为空")?;
    let normalized = serde_json::json!({
        "code": 0,
        "data": {(symbol): {"data": {"date": trade_date, "data": normalized}}}
    });
    parse_tencent_volume_ratio(&normalized.to_string(), ts_code, average_daily_volume)
}

pub fn fetch_eastmoney_volume_ratio(
    http: &reqwest::blocking::Client,
    ts_code: &str,
    average_daily_volume: f64,
) -> Result<TencentVolumeRatioData, String> {
    let symbol = ts_code_to_tencent_code(ts_code)?;
    let secid = format!(
        "{}.{}",
        if symbol.starts_with("sh") { 1 } else { 0 },
        &symbol[2..]
    );
    let raw = http
        .get("https://push2.eastmoney.com/api/qt/stock/trends2/get")
        .query(&[
            ("secid", secid.as_str()),
            ("fields1", "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11"),
            ("fields2", "f51,f52,f53,f54,f55,f56,f57,f58"),
            ("ndays", "1"),
            ("iscr", "1"),
        ])
        .header("Referer", "https://quote.eastmoney.com/")
        .header("User-Agent", "Mozilla/5.0")
        .send()
        .map_err(|e| super::intraday::format_intraday_request_error("东财", ts_code, e))?
        .error_for_status()
        .map_err(|e| super::intraday::format_intraday_http_error("东财", ts_code, e))?
        .text()
        .map_err(|e| format!("读取东财分时失败: {e:?}"))?;
    parse_eastmoney_volume_ratio(&raw, ts_code, average_daily_volume)
}

/// MSN 分钟量按股累加；显式盘前量保留，分钟桶转换为闭合时间。
/// 数据口径尚未与腾讯完全一致，调用方须标识为近似基准。
pub fn parse_msn_volume_ratio(
    raw: &str,
    ts_code: &str,
    average_daily_volume: f64,
) -> Result<TencentVolumeRatioData, String> {
    let symbol = ts_code_to_tencent_code(ts_code)?;
    let exchange = if symbol.starts_with("sh") {
        "XSHG"
    } else if symbol.starts_with("sz") {
        "XSHE"
    } else {
        return Err("MSN 量比分时暂只支持沪深股票".to_string());
    };
    if !average_daily_volume.is_finite() || average_daily_volume <= 0.0 {
        return Err("本地量比五日日均量无效".to_string());
    }
    let response: serde_json::Value =
        serde_json::from_str(raw).map_err(|e| format!("解析 MSN 分时失败: {e}"))?;
    let data = &response[0];
    if data["symbol"].as_str() != Some(&symbol[2..])
        || data["securityType"].as_str() != Some("stock")
        || data["chartType"].as_str() != Some("1D1M")
    {
        return Err(format!("MSN 分时标的或周期不匹配: {ts_code}"));
    }
    if data["exchangeCode"]
        .as_str()
        .is_some_and(|code| code != exchange)
    {
        return Err(format!("MSN 分时交易所不匹配: {ts_code}"));
    }
    let timezone = chrono::FixedOffset::east_opt(8 * 3600).unwrap();
    let timestamp = |value: &serde_json::Value| {
        chrono::DateTime::parse_from_rfc3339(value.as_str().ok_or("MSN 分时时间不是字符串")?)
            .map(|time| time.with_timezone(&timezone))
            .map_err(|e| format!("MSN 分时时间无效: {e}"))
    };
    let times = data["series"]["timeStamps"]
        .as_array()
        .ok_or("MSN 分时缺少时间数组")?;
    let volumes = data["series"]["volumes"]
        .as_array()
        .ok_or("MSN 分时缺少成交量（不能使用 chartflag=7）")?;
    if times.len() != volumes.len() || times.len() < 5 {
        return Err("MSN 分时成交量长度不匹配或开盘五分钟未完整".to_string());
    }
    let first_time = timestamp(&times[0])?;
    let trade_date = first_time.format("%Y%m%d").to_string();
    if first_time.format("%H%M%S").to_string() != "093000" {
        return Err("MSN 分时缺少 09:30 起点".to_string());
    }
    let pre_times = data["preHoursSeries"]["timeStamps"]
        .as_array()
        .filter(|times| !times.is_empty())
        .ok_or("MSN 分时缺少明确盘前竞价量，不猜测补量")?;
    let pre_volumes = data["preHoursSeries"]["volumes"]
        .as_array()
        .ok_or("MSN 盘前缺少成交量")?;
    if pre_times.len() != pre_volumes.len() {
        return Err("MSN 盘前量与时间长度不匹配".to_string());
    }
    let mut cumulative_volume = 0.0;
    let mut previous = None;
    for (time, volume) in pre_times.iter().zip(pre_volumes) {
        let time = timestamp(time)?;
        if time.date_naive() != first_time.date_naive()
            || time >= first_time
            || previous.is_some_and(|previous| time <= previous)
        {
            return Err("MSN 竞价量日期或时间无效".to_string());
        }
        let volume = volume
            .as_f64()
            .filter(|v| v.is_finite() && *v >= 0.0)
            .ok_or("MSN 竞价成交量无效")?;
        cumulative_volume += volume;
        previous = Some(time);
    }
    let mut points = vec![TencentVolumeRatioPoint {
        time: "0930".to_string(),
        trading_minutes: 1.0,
        volume_ratio: cumulative_volume / 100.0 / (average_daily_volume / 240.0),
    }];
    previous = None;
    for (index, (time, volume)) in times.iter().zip(volumes).enumerate() {
        let time = timestamp(time)?;
        if time.date_naive() != first_time.date_naive()
            || previous.is_some_and(|previous| time <= previous)
            || (index < 5 && time != first_time + chrono::Duration::minutes(index as i64))
        {
            return Err("MSN 分时日期、顺序或开盘五分钟连续性无效".to_string());
        }
        previous = Some(time);
        let volume = volume
            .as_f64()
            .filter(|v| v.is_finite() && *v >= 0.0)
            .ok_or("MSN 分时成交量无效")?;
        // 11:30 的零量标记不能移到午休；连续交易桶按结束时刻使用。
        let start = time.format("%H%M").to_string();
        if !("0930".."1130").contains(&start.as_str())
            && !("1300".."1457").contains(&start.as_str())
        {
            continue;
        }
        cumulative_volume += volume;
        let closed = (time + chrono::Duration::minutes(1))
            .format("%H%M")
            .to_string();
        let minutes = intraday_trading_minutes(&closed).ok_or("MSN 分时分钟边界无效")?;
        let volume_ratio = cumulative_volume / 100.0 / (average_daily_volume * minutes / 240.0);
        if !volume_ratio.is_finite() {
            return Err("MSN 分时累计成交量溢出".to_string());
        }
        points.push(TencentVolumeRatioPoint {
            time: closed,
            trading_minutes: minutes,
            volume_ratio,
        });
    }
    Ok(TencentVolumeRatioData {
        trade_date,
        average_daily_volume,
        volume_to_hands: 0.01,
        points,
    })
}

pub fn fetch_msn_volume_ratio(
    http: &reqwest::blocking::Client,
    ts_code: &str,
    average_daily_volume: f64,
) -> Result<TencentVolumeRatioData, String> {
    let symbol = ts_code_to_tencent_code(ts_code)?;
    let exchange = if symbol.starts_with("sh") {
        "XSHG"
    } else if symbol.starts_with("sz") {
        "XSHE"
    } else {
        return Err("MSN 量比分时暂只支持沪深股票".to_string());
    };
    static IDS: OnceLock<Mutex<HashMap<String, String>>> = OnceLock::new();
    let ids = IDS.get_or_init(|| Mutex::new(HashMap::new()));
    let cached_id = ids
        .lock()
        .map_err(|_| "MSN 标的缓存锁已损坏")?
        .get(&symbol)
        .cloned();
    let apikey = "0QfOX3Vn51YCzitbLaRkTTBadtWpgTN8NZLW0C1SEM";
    let read = |request: reqwest::blocking::RequestBuilder| {
        request
            .send()
            .map_err(|e| super::intraday::format_intraday_request_error("MSN", ts_code, e))?
            .error_for_status()
            .map_err(|e| super::intraday::format_intraday_http_error("MSN", ts_code, e))?
            .text()
            .map_err(|e| format!("读取 MSN 分时失败: {e:?}"))
    };
    let id = if let Some(id) = cached_id {
        id
    } else {
        let raw = read(
            http.get(
                "https://services.bingapis.com/contentservices-finance.csautosuggest/api/v1/Query",
            )
            .query(&[
                ("query", &symbol[2..]),
                ("market", "zh-cn"),
                ("count", "10"),
            ]),
        )?;
        let response: serde_json::Value =
            serde_json::from_str(&raw).map_err(|e| format!("解析 MSN 标的搜索失败: {e}"))?;
        let mut id = None;
        for stock in response["data"]["stocks"]
            .as_array()
            .ok_or("MSN 搜索缺少标的数组")?
        {
            let stock: serde_json::Value =
                serde_json::from_str(stock.as_str().ok_or("MSN 搜索标的不是字符串")?)
                    .map_err(|e| format!("解析 MSN 搜索标的失败: {e}"))?;
            if stock["OS001"].as_str() == Some(&symbol[2..])
                && stock["ExMicCode"].as_str() == Some(exchange)
                && stock["OS010"].as_str() == Some("ST")
            {
                id = stock["SecId"].as_str().map(str::to_string);
                break;
            }
        }
        let id = id.ok_or_else(|| format!("MSN 未找到沪深股票: {ts_code}"))?;
        ids.lock()
            .map_err(|_| "MSN 标的缓存锁已损坏")?
            .insert(symbol, id.clone());
        id
    };
    let raw = read(
        http.get("https://assets.msn.com/service/Finance/Charts")
            .query(&[
                ("apikey", apikey),
                ("ids", id.as_str()),
                ("type", "1D1M"),
                ("wrapodata", "false"),
                ("cm", "zh-cn"),
            ]),
    )?;
    parse_msn_volume_ratio(&raw, ts_code, average_daily_volume)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn msn_includes_explicit_auction_and_closes_five_minute_buckets() {
        let mut raw = serde_json::json!([{
            "symbol": "000001", "exchangeCode": "XSHE", "securityType": "stock", "chartType": "1D1M",
            "preHoursSeries": {"timeStamps": ["2026-10-12T01:25:00Z"], "volumes": [1000]},
            "series": {
                "timeStamps": ["2026-10-12T01:30:00Z", "2026-10-12T01:31:00Z",
                    "2026-10-12T01:32:00Z", "2026-10-12T01:33:00Z", "2026-10-12T01:34:00Z"],
                "volumes": [1000, 2000, 3000, 4000, 5000]
            }
        }]);
        let data = parse_msn_volume_ratio(&raw.to_string(), "000001.SZ", 2400.0).unwrap();
        assert_eq!(data.trade_date, "20261012");
        assert_eq!(data.volume_to_hands, 0.01);
        assert_eq!(data.points.len(), 6);
        assert_eq!(data.points[0].time, "0930");
        assert_eq!(data.points[0].volume_ratio, 1.0);
        assert_eq!(data.points[1].time, "0931");
        assert_eq!(data.points[1].volume_ratio, 2.0);
        assert_eq!(data.points[5].time, "0935");
        assert_eq!(data.points[5].volume_ratio, 3.2);
        assert!(parse_msn_volume_ratio(&raw.to_string(), "000001.SH", 2400.0).is_err());
        assert!(parse_msn_volume_ratio(&raw.to_string(), "600519.SH", 2400.0).is_err());
        raw[0]["series"]["timeStamps"][4] = serde_json::json!("2026-10-12T01:35:00Z");
        assert!(parse_msn_volume_ratio(&raw.to_string(), "000001.SZ", 2400.0).is_err());
        raw[0]["series"]["timeStamps"][4] = serde_json::json!("2026-10-12T01:34:00Z");
        raw[0]["preHoursSeries"] = serde_json::Value::Null;
        assert!(parse_msn_volume_ratio(&raw.to_string(), "000001.SZ", 2400.0).is_err());
    }

    #[test]
    fn empty_intraday_responses_are_errors_not_successful_baselines() {
        for raw in [
            r#"{"code":0,"data":{}}"#,
            r#"{"code":0,"data":{"sz000001":{"data":{"date":"20261012","data":[]}}}}"#,
        ] {
            assert!(parse_tencent_volume_ratio(raw, "000001.SZ", 100000.0).is_err());
        }
        for raw in [
            r#"{"rc":0,"data":null}"#,
            r#"{"rc":0,"data":{"code":"000001","market":0,"trends":[]}}"#,
        ] {
            assert!(parse_eastmoney_volume_ratio(raw, "000001.SZ", 100000.0).is_err());
        }
    }

    #[test]
    fn eastmoney_cumulative_volume_matches_tencent_including_auction() {
        let raw = r#"{"rc":0,"data":{"code":"000001","market":0,"trends":[
            "2026-10-12 09:26,11.62,11.62,11.62,11.62,7746,9000852,11.62",
            "2026-10-12 09:30,11.62,11.62,11.62,11.62,0,0,11.62",
            "2026-10-12 09:31,11.67,11.67,11.67,11.67,29180,34025034.96,11.65",
            "2026-10-12 09:35,11.69,11.69,11.69,11.69,62274,72778505.65,11.67"
        ]}}"#;
        let qq = r#"{"code":0,"data":{"sz000001":{"data":{"date":"20261012","data":[
            "0930 11.62 7746 9000852", "0931 11.67 36926 43025886.96",
            "0935 11.69 99200 115804392.61"
        ]}}}}"#;
        let em = parse_eastmoney_volume_ratio(raw, "000001.SZ", 100000.0).unwrap();
        let qq = parse_tencent_volume_ratio(qq, "000001.SZ", 100000.0).unwrap();
        assert_eq!(em.trade_date, qq.trade_date);
        assert_eq!(em.volume_to_hands, qq.volume_to_hands);
        assert_eq!(em.points.len(), qq.points.len());
        for (em, qq) in em.points.iter().zip(&qq.points) {
            assert_eq!(em.time, qq.time);
            assert!((em.volume_ratio - qq.volume_ratio).abs() < 1e-12);
        }
        assert!(parse_eastmoney_volume_ratio(raw, "600000.SH", 100000.0).is_err());
        assert!(
            parse_eastmoney_volume_ratio(&raw.replace("09:30", "09:29"), "000001.SZ", 100000.0)
                .is_err()
        );
    }

    #[test]
    fn normalizes_hand_and_share_volumes_against_local_hand_baseline() {
        for (symbol, code, raw_volume, expected_factor) in [
            ("sz000001", "000001.SZ", 10, 1.0),
            ("sh688185", "688185.SH", 1000, 0.01),
        ] {
            let raw = serde_json::json!({
                "code": 0,
                "data": {(symbol): {"data": {"date": "20261009", "data": [
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
