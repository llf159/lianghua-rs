use super::{TencentVolumeRatioData, TencentVolumeRatioPoint, intraday_trading_minutes};
use reqwest::{blocking::Client, header};
use serde_json::Value;
use std::sync::{Mutex, OnceLock};

#[derive(Clone, Copy, Debug)]
pub enum IntradayVolumeSource {
    Tonghuashun,
    Xueqiu,
    Sse,
    Szse,
}

impl IntradayVolumeSource {
    fn market(self, ts_code: &str) -> Result<(&str, &str), String> {
        let (code, exchange) = ts_code.split_once('.').ok_or("分时股票代码格式错误")?;
        if code.len() != 6 || !code.bytes().all(|b| b.is_ascii_digit()) {
            return Err("分时股票代码格式错误".into());
        }
        match (self, exchange) {
            (Self::Tonghuashun, "SH" | "SZ" | "BJ")
            | (Self::Xueqiu, "SH" | "SZ")
            | (Self::Sse, "SH")
            | (Self::Szse, "SZ") => Ok((code, exchange)),
            _ => Err(format!("{self:?} 不支持 {ts_code}")),
        }
    }
}

/// 同一份请求定义供 Rust HTTP 和登录 WebView 使用；Cookie 始终由对应会话管理。
#[derive(serde::Serialize)]
pub struct IntradayRequest {
    pub url: String,
    pub method: String,
    pub headers: std::collections::BTreeMap<String, String>,
    pub body: Option<Value>,
}

impl IntradayRequest {
    pub fn http(&self, http: &Client) -> Result<reqwest::blocking::RequestBuilder, String> {
        let mut request = http.request(
            self.method.parse().map_err(|_| "分时请求方法无效")?,
            &self.url,
        );
        for (key, value) in &self.headers {
            request = request.header(key, value);
        }
        if let Some(body) = &self.body {
            request = request.json(body);
        }
        Ok(request)
    }
}

pub fn volume_ratio_request(
    source: IntradayVolumeSource,
    ts_code: &str,
) -> Result<IntradayRequest, String> {
    let (code, exchange) = source.market(ts_code)?;
    let mut request = IntradayRequest {
        url: String::new(),
        method: "GET".into(),
        headers: Default::default(),
        body: None,
    };
    match source {
        IntradayVolumeSource::Tonghuashun => {
            request.url =
                "https://quota-h.10jqka.com.cn/fuyao/common_hq_aggr/quote/v1/single_trend".into();
            request.method = "POST".into();
            request.headers = [
                ("Content-Type", "application/json"),
                ("X-Fuyao-Auth", "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJhdXRob3JpemVyX25hbWVzcGFjZSI6ImNvbW1vbi1ocS1hZ2dyIiwibGljZW5zZWVfdHlwZSI6IkZST05UX0FQUCIsImxpY2Vuc2VlX25hbWVzcGFjZSI6Imh4a2xpbmUtTkVXU19hcHBOZXdzRmxvd0hvbWVfUGFnZSJ9.ldrvWTheNnGOa_rH_buA6OoUpLtW2bhcdr3fABrGHbk"),
                ("Source-Id", "hxkline-NEWS_appNewsFlowHome_Page"),
                ("Platform", "hxkline"), ("X-Auth-Type", "ths"), ("X-Auth-Version", "1.0"),
                ("X-Auth-ProgId", "7047"), ("X-Auth-AppName", "AINVEST"),
            ].into_iter().map(|(k,v)| (k.into(),v.into())).collect();
            request.body = Some(
                serde_json::json!({"code_list":[{"codes":[code],"market":match exchange {"SH"=>"17","SZ"=>"33",_=>"151"}}],
                "trade_class":"intraday","trade_date":0,"begin_time":0,"end_time":0}),
            );
        }
        IntradayVolumeSource::Xueqiu => {
            request.url = format!(
                "https://stock.xueqiu.com/v5/stock/chart/minute.json?symbol={exchange}{code}&period=1d"
            );
            request
                .headers
                .insert("Referer".into(), "https://xueqiu.com/".into());
        }
        IntradayVolumeSource::Sse => {
            request.url = format!(
                "https://yunhq.sse.com.cn:32042/v1/sh1/line/{code}?select=time,price,volume,avg_price,amount,highest,lowest"
            )
        }
        IntradayVolumeSource::Szse => {
            request.url =
                format!("https://www.szse.cn/api/market/ssjjhq/getTimeData?marketId=1&code={code}");
            request
                .headers
                .insert("Referer".into(), "https://www.szse.cn/".into());
        }
    }
    Ok(request)
}

fn response_json(request: reqwest::blocking::RequestBuilder) -> Result<Value, String> {
    let response = request
        .header(header::USER_AGENT, "Mozilla/5.0")
        .send()
        .map_err(|e| format!("请求分时失败: {e}"))?;
    let status = response.status();
    if !status.is_success() {
        return Err(format!("分时 HTTP 错误; HTTP状态={};", status.as_u16()));
    }
    response
        .json()
        .map_err(|e| format!("解析分时 JSON 失败: {e}"))
}

fn number(value: &Value) -> Result<f64, String> {
    value
        .as_f64()
        .or_else(|| value.as_str()?.parse().ok())
        .filter(|v| v.is_finite() && *v >= 0.0)
        .ok_or_else(|| "分时时间或成交量数值无效".into())
}

fn timestamp(value: &Value) -> Result<(String, String), String> {
    let millis = number(value)?;
    if millis.fract() != 0.0 || millis > i64::MAX as f64 {
        return Err("分时时间戳无效".into());
    }
    let time = chrono::DateTime::from_timestamp_millis(millis as i64)
        .ok_or("分时时间戳越界")?
        .with_timezone(&chrono::FixedOffset::east_opt(8 * 3600).unwrap());
    Ok((
        time.format("%Y%m%d").to_string(),
        time.format("%H%M").to_string(),
    ))
}

/// 四个新源统一为累计股数，再计算量比；不改变实时行情的成交量单位。
pub fn parse_volume_ratio_from_source(
    data: &Value,
    source: IntradayVolumeSource,
    ts_code: &str,
    average_daily_volume: f64,
) -> Result<TencentVolumeRatioData, String> {
    let (code, exchange) = source.market(ts_code)?;
    let mut rows = Vec::new();
    match source {
        IntradayVolumeSource::Tonghuashun => {
            if data["status_code"].as_i64() != Some(0) {
                return Err(format!("同花顺分时业务错误: {}", data["status_msg"]));
            }
            let quotes = data["data"]["quote_data"]
                .as_array()
                .ok_or("同花顺缺少分时标的")?;
            let market = match exchange {
                "SH" => "17",
                "SZ" => "33",
                _ => "151",
            };
            let quote = quotes
                .iter()
                .find(|q| q["code"].as_str() == Some(code) && q["market"].as_str() == Some(market))
                .ok_or("同花顺分时标的不匹配")?;
            let fields = quote["data_fields"]
                .as_array()
                .ok_or("同花顺缺少分时字段")?;
            let time_index = fields
                .iter()
                .position(|v| v.as_str() == Some("1"))
                .ok_or("同花顺缺少时间字段")?;
            let volume_index = fields
                .iter()
                .position(|v| v.as_str() == Some("13"))
                .ok_or("同花顺缺少累计量字段")?;
            for row in quote["value"].as_array().ok_or("同花顺缺少分时序列")? {
                let (date, time) = timestamp(&row[time_index])?;
                rows.push((date, time, number(&row[volume_index])?));
            }
        }
        IntradayVolumeSource::Xueqiu => {
            if data["error_code"].as_i64() != Some(0) {
                return Err(format!(
                    "雪球分时业务错误: {} {}",
                    data["error_code"], data["error_description"]
                ));
            }
            let mut previous_volume = None;
            for row in data["data"]["items"].as_array().ok_or("雪球缺少分时序列")? {
                let (date, time) = timestamp(&row["timestamp"])?;
                // 雪球按桶起点标记，11:30 是午休占位；15:00 为收盘撮合，不能后移。
                let closed = if ("0930".."1130").contains(&time.as_str())
                    || ("1300".."1459").contains(&time.as_str())
                {
                    let t = chrono::NaiveTime::parse_from_str(&time, "%H%M")
                        .map_err(|e| e.to_string())?;
                    (t + chrono::Duration::minutes(1))
                        .format("%H%M")
                        .to_string()
                } else if time == "1500" {
                    time
                } else {
                    continue;
                };
                let volume = if row["volume_total"].is_null() && number(&row["volume"])? == 0.0 {
                    previous_volume.ok_or("雪球首点缺少累计成交量")?
                } else {
                    number(&row["volume_total"])?
                };
                previous_volume = Some(volume);
                rows.push((date, closed, volume));
            }
        }
        IntradayVolumeSource::Sse => {
            if data["code"].as_str() != Some(code) {
                return Err("上交所分时标的不匹配".into());
            }
            let date = data["date"].as_u64().ok_or("上交所缺少日期")?.to_string();
            chrono::NaiveDate::parse_from_str(&date, "%Y%m%d").map_err(|e| e.to_string())?;
            if data["line"][0][0].as_u64() != Some(93000) {
                return Err("上交所分时缺少竞价起点".into());
            }
            let mut cumulative = 0.0;
            for row in data["line"].as_array().ok_or("上交所缺少分时序列")? {
                let time = row[0].as_u64().ok_or("上交所分时时间无效")?;
                if time > 235959 || time % 100 != 0 {
                    return Err("上交所分时时间无效".into());
                }
                let time = format!("{:04}", time / 100);
                if intraday_trading_minutes(&time).is_none() {
                    continue;
                }
                cumulative += number(&row[2])?;
                rows.push((date.clone(), time, cumulative));
            }
        }
        IntradayVolumeSource::Szse => {
            if data["code"].as_str() != Some("0") || data["data"]["code"].as_str() != Some(code) {
                return Err("深交所分时状态或标的不匹配".into());
            }
            let date = data["datetime"]
                .as_str()
                .and_then(|s| s.get(..10))
                .ok_or("深交所缺少日期")?;
            let date = chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d")
                .map_err(|e| e.to_string())?
                .format("%Y%m%d")
                .to_string();
            if data["data"]["picupdata"][0][0].as_str() != Some("09:30") {
                return Err("深交所分时缺少竞价起点".into());
            }
            let mut cumulative = 0.0;
            for row in data["data"]["picupdata"]
                .as_array()
                .ok_or("深交所缺少分时序列")?
            {
                let time = row[0]
                    .as_str()
                    .ok_or("深交所分时时间无效")?
                    .replace(':', "");
                if intraday_trading_minutes(&time).is_none() {
                    continue;
                }
                // 官网分钟量单位为手，统一转换为股；盘后量不进入累计值。
                cumulative += number(&row[5])? * 100.0;
                rows.push((date.clone(), time, cumulative));
            }
        }
    }
    if !average_daily_volume.is_finite() || average_daily_volume <= 0.0 {
        return Err("本地量比五日日均量无效".into());
    }
    let mut data = TencentVolumeRatioData {
        trade_date: String::new(),
        average_daily_volume,
        volume_to_hands: 0.01,
        points: Vec::new(),
    };
    let mut previous_volume = 0.0;
    for (date, time, volume) in rows {
        let Some(minutes) = intraday_trading_minutes(&time) else {
            continue;
        };
        if data.trade_date.is_empty() {
            data.trade_date = date.clone();
        }
        if date != data.trade_date
            || !volume.is_finite()
            || volume < previous_volume
            || data.points.last().is_some_and(|p| p.time >= time)
        {
            return Err("分时日期不一致、时间或累计成交量回退".into());
        }
        let volume_ratio = volume / 100.0 / (average_daily_volume * minutes / 240.0);
        if !volume_ratio.is_finite() {
            return Err("分时量比溢出".into());
        }
        data.points.push(TencentVolumeRatioPoint {
            time,
            trading_minutes: minutes,
            volume_ratio,
        });
        previous_volume = volume;
    }
    if data.points.is_empty() {
        return Err("分时缺少有效交易时段数据".into());
    }
    Ok(data)
}

pub fn fetch_volume_ratio_from_source(
    http: &Client,
    source: IntradayVolumeSource,
    ts_code: &str,
    average_daily_volume: f64,
) -> Result<TencentVolumeRatioData, String> {
    let request = volume_ratio_request(source, ts_code)?;
    let data = match source {
        IntradayVolumeSource::Tonghuashun
        | IntradayVolumeSource::Sse
        | IntradayVolumeSource::Szse => response_json(request.http(http)?)?,
        IntradayVolumeSource::Xueqiu => {
            // 只使用独立匿名会话。锁仅保护初始化/刷新，不串行化分时请求。
            static SESSION: OnceLock<Mutex<Option<(String, u64)>>> = OnceLock::new();
            let session = SESSION.get_or_init(|| Mutex::new(None));
            let mut stale = None;
            let mut result = None;
            for _ in 0..2 {
                let (cookies, generation) = {
                    let mut state = session.lock().map_err(|_| "雪球匿名会话锁损坏")?;
                    if state.is_none()
                        || stale
                            .is_some_and(|version| state.as_ref().is_some_and(|s| s.1 == version))
                    {
                        let response = http
                            .get("https://xueqiu.com/hq")
                            .header(header::USER_AGENT, "Mozilla/5.0")
                            .send()
                            .map_err(|e| format!("初始化雪球匿名会话失败: {e}"))?;
                        if !response.status().is_success() {
                            return Err(format!(
                                "雪球匿名会话 HTTP状态={};",
                                response.status().as_u16()
                            ));
                        }
                        let cookies = response
                            .headers()
                            .get_all(header::SET_COOKIE)
                            .iter()
                            .filter_map(|h| h.to_str().ok()?.split(';').next())
                            .collect::<Vec<_>>()
                            .join("; ");
                        let generation = state.as_ref().map_or(0, |s| s.1 + 1);
                        *state = Some((cookies, generation));
                    }
                    state.as_ref().unwrap().clone()
                };
                let response = response_json(request.http(http)?.header(header::COOKIE, cookies));
                let rejected = match &response {
                    Ok(data) => data["error_code"].as_i64().is_some_and(|c| c != 0),
                    Err(error) => {
                        error.contains("HTTP状态=401;") || error.contains("HTTP状态=403;")
                    }
                };
                result = Some(response);
                if !rejected {
                    break;
                }
                stale = Some(generation);
            }
            result.unwrap()?
        }
    };
    parse_volume_ratio_from_source(&data, source, ts_code, average_daily_volume)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn exchange_minute_units_auction_and_post_market_are_normalized() {
        let sse = json!({"code":"600000", "date":20261106, "line":[
            [93000,9.91,377100], [93100,9.87,4145300], [93200,9.89,844100],
            [93300,9.88,633600], [93400,9.88,607800], [93500,9.91,1018896],
            [153000,9.91,999999999]
        ]});
        let data =
            parse_volume_ratio_from_source(&sse, IntradayVolumeSource::Sse, "600000.SH", 2400.0)
                .unwrap();
        assert!(data.opening_window_complete());
        assert!((data.points[5].volume_ratio - 7626796.0 / 100.0 / 50.0).abs() < 1e-9);
        assert_eq!(data.points.len(), 6);
        assert!(
            parse_volume_ratio_from_source(&sse, IntradayVolumeSource::Sse, "000001.SZ", 2400.0)
                .is_err()
        );
        let mut szse = json!({"code":"0", "datetime":"2026-11-06 15:30", "data":{
        "code":"000001", "picupdata":[
            ["09:30",0,0,0,0,7527], ["09:31",0,0,0,0,44934],
            ["09:32",0,0,0,0,14338], ["09:33",0,0,0,0,12826],
            ["09:34",0,0,0,0,8995], ["09:35",0,0,0,0,13635],
            ["15:30",0,0,0,0,999999999]
        ]}});
        let data =
            parse_volume_ratio_from_source(&szse, IntradayVolumeSource::Szse, "000001.SZ", 2400.0)
                .unwrap();
        assert!(data.opening_window_complete());
        assert_eq!(data.points[5].volume_ratio, 102255.0 / 50.0);
        assert_eq!(data.points.len(), 6);
        szse["data"]["picupdata"][2][5] = json!(-1);
        assert!(
            parse_volume_ratio_from_source(&szse, IntradayVolumeSource::Szse, "000001.SZ", 2400.0)
                .is_err()
        );
    }

    #[test]
    fn cumulative_sources_align_fields_and_closed_buckets_without_inventing_auction() {
        let start = 1793928600000i64; // 2026-11-06 09:30 UTC+8
        let mut ths = json!({"status_code":0,"data":{"quote_data":[{
            "market":"17","code":"600000","data_fields":["13","1"],
            "value":(0..6).map(|i| json!([1000*(i+1), start+i*60000])).collect::<Vec<_>>()
        }]}});
        let data = parse_volume_ratio_from_source(
            &ths,
            IntradayVolumeSource::Tonghuashun,
            "600000.SH",
            2400.0,
        )
        .unwrap();
        assert!(data.opening_window_complete());
        assert_eq!(data.points[5].volume_ratio, 1.2);
        ths["data"]["quote_data"][0]["value"][2][0] = json!(0);
        assert!(
            parse_volume_ratio_from_source(
                &ths,
                IntradayVolumeSource::Tonghuashun,
                "600000.SH",
                2400.0
            )
            .is_err()
        );
        let mut xq = json!({"error_code":0,"data":{
            "items":(0..5).map(|i| json!({"timestamp":start+i*60000,"volume":1000,"volume_total":1000*(i+1)})).collect::<Vec<_>>(),
            "after":[{"timestamp":start,"volume_total":999999999}]
        }});
        let data =
            parse_volume_ratio_from_source(&xq, IntradayVolumeSource::Xueqiu, "600000.SH", 2400.0)
                .unwrap();
        assert!(data.opening_window_complete());
        assert_eq!(data.points[0].time, "0931");
        assert_eq!(data.points[4].time, "0935");
        assert_eq!(data.points[4].volume_ratio, 1.0);
        xq["data"]["items"].as_array_mut().unwrap().remove(2);
        let data =
            parse_volume_ratio_from_source(&xq, IntradayVolumeSource::Xueqiu, "600000.SH", 2400.0)
                .unwrap();
        assert!(!data.opening_window_complete());
    }

    #[test]
    fn xueqiu_handles_lunch_closing_auction_and_invalid_responses() {
        let mut raw = json!({"error_code":0,"data":{"items":[
            {"timestamp":1793935740000i64,"volume_total":1000,"volume":1000},
            {"timestamp":1793935800000i64,"volume_total":null,"volume":0},
            {"timestamp":1793941200000i64,"volume_total":2000,"volume":1000},
            {"timestamp":1793948220000i64,"volume_total":null,"volume":0},
            {"timestamp":1793948340000i64,"volume_total":null,"volume":0},
            {"timestamp":1793948400000i64,"volume_total":3000,"volume":1000}
        ]}});
        let data =
            parse_volume_ratio_from_source(&raw, IntradayVolumeSource::Xueqiu, "000001.SZ", 2400.0)
                .unwrap();
        assert_eq!(
            data.points
                .iter()
                .map(|p| p.time.as_str())
                .collect::<Vec<_>>(),
            ["1130", "1301", "1458", "1500"]
        );
        assert_eq!(
            data.points.last().unwrap().volume_ratio,
            3000.0 / 100.0 / 2400.0
        );
        raw["error_code"] = json!(-400);
        assert!(
            parse_volume_ratio_from_source(&raw, IntradayVolumeSource::Xueqiu, "000001.SZ", 2400.0)
                .is_err()
        );
        for source in [
            IntradayVolumeSource::Tonghuashun,
            IntradayVolumeSource::Xueqiu,
            IntradayVolumeSource::Sse,
            IntradayVolumeSource::Szse,
        ] {
            let code = if matches!(source, IntradayVolumeSource::Szse) {
                "000001.SZ"
            } else {
                "600000.SH"
            };
            assert!(parse_volume_ratio_from_source(&json!({}), source, code, 2400.0).is_err());
        }
    }

    #[test]
    #[ignore = "手动验证四个网站接口，每源仅请求一个标的"]
    fn live_new_volume_ratio_sources() {
        let http = Client::builder()
            .timeout(std::time::Duration::from_secs(12))
            .build()
            .unwrap();
        for (source, code) in [
            (IntradayVolumeSource::Tonghuashun, "600000.SH"),
            (IntradayVolumeSource::Xueqiu, "600000.SH"),
            (IntradayVolumeSource::Sse, "600000.SH"),
            (IntradayVolumeSource::Szse, "000001.SZ"),
        ] {
            let data = fetch_volume_ratio_from_source(&http, source, code, 2400.0).unwrap();
            eprintln!(
                "{source:?} {code}: {} / {} points",
                data.trade_date,
                data.points.len()
            );
            assert!(data.opening_window_complete());
        }
    }
}
