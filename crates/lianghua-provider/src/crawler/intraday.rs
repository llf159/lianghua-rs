use serde::{Deserialize, Serialize};

use super::{
    parse_f64_field, parse_optional_f64_field, parse_tencent_amount, parse_tencent_datetime,
    ts_code_to_tencent_code,
};

const TENCENT_INTRADAY_URL: &str = "https://web.ifzq.gtimg.cn/appstock/app/minute/query";

#[derive(Debug, Clone, Serialize)]
pub struct TencentIntradayPoint {
    pub time: String,
    pub price: f64,
    pub average_price: Option<f64>,
    pub vol: f64,
    pub amount: f64,
    pub cumulative_vol: f64,
    pub cumulative_amount: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct TencentIntradaySummary {
    pub name: String,
    pub refreshed_at: Option<String>,
    pub latest_price: f64,
    pub pre_close: f64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub upper_limit: Option<f64>,
    pub lower_limit: Option<f64>,
    pub change_pct: Option<f64>,
    pub average_price: Option<f64>,
    pub total_vol: f64,
    pub total_amount: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct TencentIntradayData {
    pub source: &'static str,
    pub ts_code: String,
    pub trade_date: String,
    pub summary: Option<TencentIntradaySummary>,
    pub points: Vec<TencentIntradayPoint>,
    pub volume_ratio_points: Vec<super::volume_ratio::TencentVolumeRatioPoint>,
    pub volume_ratio_error: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(super) struct TencentIntradayResponse {
    pub(super) code: i64,
    #[serde(default)]
    msg: String,
    #[serde(default)]
    pub(super) data: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Deserialize)]
pub(super) struct TencentIntradaySymbolWrapper {
    pub(super) data: TencentIntradayPayload,
    #[serde(default)]
    qt: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Deserialize)]
pub(super) struct TencentIntradayPayload {
    pub(super) date: String,
    #[serde(default)]
    pub(super) data: Vec<String>,
}

pub(super) fn normalize_trade_date(raw: &str) -> Result<String, String> {
    let trimmed = raw.trim();
    if trimmed.len() == 8 && trimmed.chars().all(|ch| ch.is_ascii_digit()) {
        Ok(trimmed.to_string())
    } else {
        Err(format!("腾讯分时交易日期格式错误: {raw}"))
    }
}

pub(super) fn parse_non_negative_f64(
    raw: &str,
    field_name: &str,
    row: &str,
) -> Result<f64, String> {
    let value = raw
        .parse::<f64>()
        .map_err(|e| format!("腾讯分时字段 {field_name} 解析失败: {row}, {e}"))?;
    if !value.is_finite() || value < 0.0 {
        return Err(format!("腾讯分时字段 {field_name} 非法: {row}"));
    }
    Ok(value)
}

pub(super) fn infer_shares_per_volume_unit(
    price: f64,
    cumulative_vol: f64,
    cumulative_amount: f64,
) -> f64 {
    if price <= 0.0 || cumulative_vol <= 0.0 || cumulative_amount <= 0.0 {
        return 100.0;
    }

    let hand_average_price = cumulative_amount / (cumulative_vol * 100.0);
    let share_average_price = cumulative_amount / cumulative_vol;
    let hand_deviation = (hand_average_price - price).abs() / price;
    let share_deviation = (share_average_price - price).abs() / price;

    if hand_deviation > (0.5) && share_deviation < hand_deviation {
        1.0
    } else {
        100.0
    }
}

pub fn parse_tencent_intraday_text(
    raw: &str,
    ts_code: &str,
) -> Result<TencentIntradayData, String> {
    let symbol = ts_code_to_tencent_code(ts_code)?;
    let normalized_ts_code = ts_code.trim().to_ascii_uppercase();
    let response = serde_json::from_str::<TencentIntradayResponse>(raw)
        .map_err(|e| format!("腾讯分时响应 JSON 解析失败: {e}"))?;

    if response.code != 0 {
        let message = if response.msg.trim().is_empty() {
            "未知错误"
        } else {
            response.msg.trim()
        };
        return Err(format!(
            "腾讯分时接口返回错误: code={}, msg={message}",
            response.code
        ));
    }

    let symbol_value = response
        .data
        .get(&symbol)
        .ok_or_else(|| format!("腾讯分时响应缺少标的: {symbol}"))?
        .clone();
    let wrapper = serde_json::from_value::<TencentIntradaySymbolWrapper>(symbol_value)
        .map_err(|e| format!("腾讯分时标的数据解析失败: {symbol}, {e}"))?;
    let trade_date = normalize_trade_date(&wrapper.data.date)?;
    let points = (|rows: &[String]| -> Result<Vec<TencentIntradayPoint>, String> {
        let mut points = Vec::with_capacity(rows.len());
        let mut previous_vol = 0.0;
        let mut previous_amount = 0.0;
        let mut shares_per_volume_unit = None;

        for row in rows {
            let fields = row.split_whitespace().collect::<Vec<_>>();
            if fields.len() < 4 {
                return Err(format!("腾讯分时记录字段不足，期望至少 4 个字段: {row}"));
            }

            let time = (|raw: &str| -> Result<String, String> {
                let trimmed = raw.trim();
                if trimmed.len() != 4 || !trimmed.chars().all(|ch| ch.is_ascii_digit()) {
                    return Err(format!("腾讯分时时间格式错误: {raw}"));
                }

                let hour = trimmed[0..2]
                    .parse::<u8>()
                    .map_err(|e| format!("腾讯分时时间小时解析失败: {raw}, {e}"))?;
                let minute = trimmed[2..4]
                    .parse::<u8>()
                    .map_err(|e| format!("腾讯分时时间分钟解析失败: {raw}, {e}"))?;
                if hour > 23 || minute > 59 {
                    return Err(format!("腾讯分时时间超出范围: {raw}"));
                }

                Ok(format!("{hour:02}:{minute:02}"))
            })(fields[0])?;
            let price = parse_non_negative_f64(fields[1], "price", row)?;
            let cumulative_vol = parse_non_negative_f64(fields[2], "cumulative_vol", row)?;
            let cumulative_amount = parse_non_negative_f64(fields[3], "cumulative_amount", row)?;

            if cumulative_vol < previous_vol || cumulative_amount < previous_amount {
                return Err(format!("腾讯分时累计量额发生回退: {row}"));
            }

            let vol = cumulative_vol - previous_vol;
            let amount = cumulative_amount - previous_amount;
            let average_price = if cumulative_vol > 0.0 {
                let unit = *shares_per_volume_unit.get_or_insert_with(|| {
                    infer_shares_per_volume_unit(price, cumulative_vol, cumulative_amount)
                });
                Some(cumulative_amount / (cumulative_vol * unit))
            } else {
                None
            };

            points.push(TencentIntradayPoint {
                time,
                price,
                average_price,
                vol,
                amount,
                cumulative_vol,
                cumulative_amount,
            });
            previous_vol = cumulative_vol;
            previous_amount = cumulative_amount;
        }

        Ok(points)
    })(&wrapper.data.data)?;
    let summary = (|qt: &serde_json::Map<String, serde_json::Value>,
                    symbol: &str|
     -> Result<Option<TencentIntradaySummary>, String> {
        let Some(fields_value) = qt.get(symbol) else {
            return Ok(None);
        };
        let fields = fields_value
            .as_array()
            .ok_or_else(|| format!("腾讯分时汇总行情不是数组: {symbol}"))?
            .iter()
            .map(|value| value.as_str().unwrap_or_default())
            .collect::<Vec<_>>();
        if fields.len() <= 51 {
            return Ok(None);
        }

        let latest_price = parse_f64_field(&fields, 3, "now")?;
        let pre_close = parse_f64_field(&fields, 4, "pre_close")?;
        let open = parse_f64_field(&fields, 5, "open")?;
        let high = parse_f64_field(&fields, 33, "high")?;
        let low = parse_f64_field(&fields, 34, "low")?;
        let upper_limit = parse_optional_f64_field(&fields, 47, "upper_limit")?;
        let lower_limit = parse_optional_f64_field(&fields, 48, "lower_limit")?;
        let total_vol = parse_f64_field(&fields, 36, "volume_hand")?;
        let total_amount = parse_tencent_amount(&fields)?;
        let change_pct = parse_optional_f64_field(&fields, 32, "change_pct")?;
        let average_price = parse_optional_f64_field(&fields, 51, "average_price")?;
        let refreshed_at = fields.get(30).and_then(|raw| {
            parse_tencent_datetime(raw)
                .ok()
                .map(|(date, time)| format!("{date} {time}"))
        });

        Ok(Some(TencentIntradaySummary {
            name: fields.get(1).copied().unwrap_or_default().to_string(),
            refreshed_at,
            latest_price,
            pre_close,
            open,
            high,
            low,
            upper_limit,
            lower_limit,
            change_pct,
            average_price,
            total_vol,
            total_amount,
        }))
    })(&wrapper.qt, &symbol)?;

    Ok(TencentIntradayData {
        source: "tencent",
        ts_code: normalized_ts_code,
        trade_date,
        summary,
        points,
        volume_ratio_points: Vec::new(),
        volume_ratio_error: None,
    })
}

pub fn fetch_tencent_intraday(
    http: &reqwest::blocking::Client,
    ts_code: &str,
) -> Result<TencentIntradayData, String> {
    let raw = fetch_tencent_intraday_text(http, ts_code)?;
    parse_tencent_intraday_text(&raw, ts_code)
}

pub(super) fn format_intraday_request_error(
    provider: &str,
    ts_code: &str,
    error: reqwest::Error,
) -> String {
    let kind = if error.is_timeout() {
        "请求超时，尚未取得有效响应"
    } else if error.is_connect() {
        "连接失败，含 DNS/TCP/TLS，请查看底层原因"
    } else {
        "请求发送或响应头读取失败"
    };
    // Display 只给出 error sending request；Debug 保留 source 链，去掉冗长 URL。
    let error = error.without_url();
    format!("请求{provider}分时失败（{kind}）: {ts_code}; 原因: {error:?}")
}

pub(super) fn format_intraday_http_error(
    provider: &str,
    ts_code: &str,
    error: reqwest::Error,
) -> String {
    let status = error.status().map(|status| status.as_u16()).unwrap_or(0);
    let error = error.without_url();
    format!("{provider}分时 HTTP 错误: {ts_code}; HTTP状态={status}; {error}")
}

pub fn fetch_tencent_intraday_text(
    http: &reqwest::blocking::Client,
    ts_code: &str,
) -> Result<String, String> {
    let symbol = ts_code_to_tencent_code(ts_code)?;
    http.get(TENCENT_INTRADAY_URL)
        .query(&[("code", symbol)])
        .header("Accept", "application/json, text/plain, */*")
        .header("Referer", "https://gu.qq.com/")
        .header("User-Agent", "Mozilla/5.0")
        .send()
        .map_err(|e| format_intraday_request_error("腾讯", ts_code, e))?
        .error_for_status()
        .map_err(|e| format_intraday_http_error("腾讯", ts_code, e))?
        .text()
        .map_err(|e| format!("读取腾讯分时行情响应失败: ts_code={ts_code}, err={e:?}"))
}

pub async fn fetch_tencent_intraday_async(
    http: &reqwest::Client,
    ts_code: &str,
) -> Result<TencentIntradayData, String> {
    let raw = async {
        let symbol = ts_code_to_tencent_code(ts_code)?;
        http.get(TENCENT_INTRADAY_URL)
            .query(&[("code", symbol)])
            .header("Accept", "application/json, text/plain, */*")
            .header("Referer", "https://gu.qq.com/")
            .header("User-Agent", "Mozilla/5.0")
            .send()
            .await
            .map_err(|e| format_intraday_request_error("腾讯", ts_code, e))?
            .error_for_status()
            .map_err(|e| format_intraday_http_error("腾讯", ts_code, e))?
            .text()
            .await
            .map_err(|e| format!("读取腾讯分时行情响应失败: ts_code={ts_code}, err={e:?}"))
    }
    .await?;
    parse_tencent_intraday_text(&raw, ts_code)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_close(left: f64, right: f64) {
        assert!((left - right).abs() < 1e-6, "left={left}, right={right}");
    }

    #[test]
    fn parses_tencent_intraday_payload_and_calculates_deltas() {
        let data = parse_tencent_intraday_text(
            r#"{
        "code": 0,
        "msg": "",
        "data": {
            "sz000001": {
                "data": {
                    "data": [
                        "0930 11.31 2670 3019770.00",
                        "0931 11.34 15028 17019975.00"
                    ],
                    "date": "20260811"
                }
            }
        }
    }"#,
            "000001.sz",
        )
        .expect("intraday payload should parse");

        assert_eq!(data.ts_code, "000001.SZ");
        assert_eq!(data.trade_date, "20260811");
        assert_eq!(data.points.len(), 2);
        assert_eq!(data.points[0].time, "09:30");
        assert_eq!(data.points[0].price, 11.31);
        assert_eq!(data.points[0].vol, 2670.0);
        assert_eq!(data.points[0].amount, 3019770.0);
        assert_close(data.points[0].average_price.unwrap(), 11.31);
        assert_eq!(data.points[1].time, "09:31");
        assert_eq!(data.points[1].vol, 12358.0);
        assert_eq!(data.points[1].amount, 14000205.0);
        assert_close(
            data.points[1].average_price.unwrap(),
            11.325_509_049_773_755,
        );
    }

    #[test]
    fn corrects_average_price_when_volume_is_reported_in_shares() {
        let raw = r#"{
            "code": 0,
            "data": {
                "sh688185": {
                    "data": {
                        "data": [
                            "0930 70.06 1457695 102126112.00",
                            "0931 70.50 1600000 112500000.00"
                        ],
                        "date": "20260821"
                    }
                }
            }
        }"#;
        let data = parse_tencent_intraday_text(raw, "688185.SH")
            .expect("share-volume payload should parse");

        assert_close(data.points[0].average_price.unwrap(), 70.060_000_205_804_37);
        assert_close(data.points[1].average_price.unwrap(), 70.3125);
        assert_eq!(data.points[1].vol, 142305.0);
    }

    #[test]
    fn keeps_hand_unit_when_average_price_is_reasonable() {
        assert_eq!(
            infer_shares_per_volume_unit(11.31, 2670.0, 3019770.0),
            100.0
        );
    }

    #[test]
    fn accepts_an_empty_intraday_point_list() {
        let raw = r#"{
            "code": 0,
            "data": {"sh600000": {"data": {"data": [], "date": "20260811"}}}
        }"#;
        let data =
            parse_tencent_intraday_text(raw, "600000.SH").expect("empty point list should parse");

        assert!(data.points.is_empty());
    }

    #[test]
    fn parses_summary_from_the_same_intraday_response() {
        let mut fields = vec![""; 58];
        fields[1] = "平安银行";
        fields[3] = "11.31";
        fields[4] = "11.29";
        fields[5] = "11.30";
        fields[30] = "20260811104757";
        fields[32] = "0.18";
        fields[33] = "11.40";
        fields[34] = "11.25";
        fields[36] = "2670";
        fields[47] = "12.42";
        fields[48] = "10.16";
        fields[51] = "11.31";
        fields[57] = "301.9770";
        let raw = serde_json::json!({
            "code": 0,
            "data": {
                "sz000001": {
                    "data": {
                        "data": ["0930 11.31 2670 3019770.00"],
                        "date": "20260811"
                    },
                    "qt": {"sz000001": fields}
                }
            }
        })
        .to_string();

        let data =
            parse_tencent_intraday_text(&raw, "000001.SZ").expect("intraday summary should parse");
        let summary = data.summary.expect("summary should exist");
        assert_eq!(summary.name, "平安银行");
        assert_eq!(summary.refreshed_at.as_deref(), Some("20260811 10:47:57"));
        assert_eq!(summary.latest_price, 11.31);
        assert_eq!(summary.pre_close, 11.29);
        assert_eq!(summary.upper_limit, Some(12.42));
        assert_eq!(summary.lower_limit, Some(10.16));
        assert_eq!(summary.total_vol, 2670.0);
        assert_close(summary.total_amount, 3019770.0);
    }

    #[test]
    fn rejects_api_errors() {
        let error = parse_tencent_intraday_text(
            r#"{"code": 1, "msg": "bad request", "data": {}}"#,
            "000001.SZ",
        )
        .expect_err("api error should fail");

        assert!(error.contains("code=1"));
        assert!(error.contains("bad request"));
    }

    #[test]
    fn rejects_decreasing_cumulative_values() {
        let raw = r#"{
            "code": 0,
            "data": {
                "sz000001": {
                    "data": {
                        "data": ["0930 11.31 2670 3019770", "0931 11.30 2600 3000000"],
                        "date": "20260811"
                    }
                }
            }
        }"#;
        let error = parse_tencent_intraday_text(raw, "000001.SZ")
            .expect_err("decreasing cumulative values should fail");

        assert!(error.contains("累计量额发生回退"));
    }

    #[test]
    #[ignore = "requires network access"]
    fn live_tencent_intraday_fetch_returns_points() {
        let http = reqwest::blocking::Client::builder()
            .no_proxy()
            .timeout(std::time::Duration::from_secs(15))
            .build()
            .expect("http client should build");
        let data = fetch_tencent_intraday(&http, "000001.SZ")
            .expect("live Tencent intraday request should succeed");

        assert_eq!(data.ts_code, "000001.SZ");
        assert_eq!(data.trade_date.len(), 8);
        assert!(!data.points.is_empty());
        let summary = data.summary.expect("live summary should exist");
        let upper_change_pct =
            (summary.upper_limit.expect("upper limit should exist") / summary.pre_close - 1.0)
                * 100.0;
        let lower_change_pct =
            (summary.lower_limit.expect("lower limit should exist") / summary.pre_close - 1.0)
                * 100.0;
        assert!((8.0..=12.0).contains(&upper_change_pct));
        assert!((-12.0..=-8.0).contains(&lower_change_pct));
    }
}

pub fn normalize_eastmoney_intraday_text(raw: &str, ts_code: &str) -> Result<String, String> {
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
        let date = normalize_trade_date(&date.replace('-', ""))?;
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
        let volume = parse_non_negative_f64(fields[5], "volume", raw)?;
        let amount = parse_non_negative_f64(fields[6], "amount", raw)?;
        // 09:26 的竞价量必须带到 09:30，午休之后也沿用上午累计量。
        if time.as_str() <= "1500" {
            cumulative_vol += volume;
            cumulative_amount += amount;
        }
        if super::volume_ratio::intraday_trading_minutes(&time).is_some() {
            normalized.push(format!(
                "{time} {} {cumulative_vol} {cumulative_amount}",
                fields[2]
            ));
        }
    }
    let trade_date = trade_date.ok_or("东财分时为空")?;
    let mut qt = serde_json::Map::new();
    if let Some(pre_close) = data["preClose"]
        .as_f64()
        .filter(|value| value.is_finite() && *value > 0.0)
    {
        let mut fields = vec![String::new(); 52];
        fields[1] = data["name"].as_str().unwrap_or_default().to_string();
        fields[4] = pre_close.to_string();
        let mut open = None;
        let mut high = f64::NEG_INFINITY;
        let mut low = f64::INFINITY;
        let mut latest = 0.0;
        for row in rows {
            let fields_row = row
                .as_str()
                .ok_or("东财分时行不是文本")?
                .split(',')
                .collect::<Vec<_>>();
            let (_, time) = fields_row[0]
                .split_once(' ')
                .ok_or("东财分时时间格式错误")?;
            if time > "15:00" {
                continue;
            }
            let close =
                parse_non_negative_f64(fields_row[2], "price", row.as_str().unwrap_or_default())?;
            if time <= "09:30" {
                open = Some(close);
            }
            if time >= "09:30" {
                open.get_or_insert(close);
                high = high.max(parse_non_negative_f64(
                    fields_row[3],
                    "high",
                    fields_row[0],
                )?);
                low = low.min(parse_non_negative_f64(fields_row[4], "low", fields_row[0])?);
                latest = close;
            }
        }
        if let Some(open) = open.filter(|_| high.is_finite() && low.is_finite()) {
            fields[3] = latest.to_string();
            fields[5] = open.to_string();
            fields[33] = high.to_string();
            fields[34] = low.to_string();
            fields[36] = cumulative_vol.to_string();
            if cumulative_vol > 0.0 {
                fields[51] = (cumulative_amount / (cumulative_vol * 100.0)).to_string();
            }
            // 腾讯汇总成交额以万元表示。
            fields[37] = (cumulative_amount / 10000.0).to_string();
            fields[32] = ((latest / pre_close - 1.0) * 100.0).to_string();
            qt.insert(symbol.clone(), serde_json::json!(fields));
        }
    }
    let normalized = serde_json::json!({
        "code": 0,
        "data": {(symbol): {"data": {"date": trade_date, "data": normalized}, "qt": qt}}
    });
    Ok(normalized.to_string())
}

pub fn fetch_eastmoney_intraday_text(
    http: &reqwest::blocking::Client,
    ts_code: &str,
) -> Result<String, String> {
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
        .map_err(|e| format_intraday_request_error("东财", ts_code, e))?
        .error_for_status()
        .map_err(|e| format_intraday_http_error("东财", ts_code, e))?
        .text()
        .map_err(|e| format!("读取东财分时失败: {e:?}"))?;
    Ok(raw)
}
