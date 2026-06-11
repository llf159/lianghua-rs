pub mod concept;

use encoding_rs::GBK;
use rayon::prelude::*;
use serde::Serialize;

const SINA_REALTIME_URL: &str = "http://hq.sinajs.cn/";
const TENCENT_REALTIME_URL: &str = "http://qt.gtimg.cn/q=";
pub const DEFAULT_REALTIME_INDEX_TS_CODES: [&str; 10] = [
    "000001.SH", // 上证指数
    "399001.SZ", // 深证成指
    "399006.SZ", // 创业板指
    "899050.BJ", // 北证50
    "000300.SH", // 沪深300
    "000905.SH", // 中证500
    "000852.SH", // 中证1000
    "000510.SH", // 中证A500
    "000688.SH", // 科创50
    "399673.SZ", // 创业板50
];

#[derive(Debug, Clone, Serialize)]
pub struct SinaQuote {
    pub date: String,
    pub time: String,
    pub ts_code: String,
    pub name: String,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub pre_close: f64,
    pub price: f64,
    pub vol: f64,
    pub amount: f64,
    pub change_pct: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TencentQuote {
    pub date: String,
    pub time: String,
    pub ts_code: String,
    pub name: String,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub pre_close: f64,
    pub price: f64,
    pub vol: f64,
    pub amount: f64,
    pub change_pct: Option<f64>,
    pub volume_ratio: Option<f64>,
    pub avg_price: Option<f64>,
}

impl TencentQuote {
    pub fn into_sina_quote(self) -> SinaQuote {
        SinaQuote {
            date: self.date,
            time: self.time,
            ts_code: self.ts_code,
            name: self.name,
            open: self.open,
            high: self.high,
            low: self.low,
            pre_close: self.pre_close,
            price: self.price,
            vol: self.vol,
            amount: self.amount,
            change_pct: self.change_pct,
        }
    }
}

fn ts_code_to_sina_code(ts_code: &str) -> Result<String, String> {
    let std_code = ts_code.trim().to_ascii_lowercase();

    if let Some((code, market)) = std_code.split_once('.') {
        if code.len() != 6 || !code.chars().all(|ch| ch.is_ascii_digit()) {
            return Err(format!("股票代码部分不是6位数字: {ts_code}"));
        }

        let _ = match market {
            "sh" | "sz" | "bj" => {}
            _ => return Err(format!("暂不支持的市场后缀: {market}")),
        };

        return Ok(format!("{market}{code}"));
    } else {
        Err(format!("输入代码非标准:{:?}", std_code))
    }
}

fn sina_code_to_ts_code(sina_code: &str) -> Result<String, String> {
    if let Some(code) = sina_code.strip_prefix("sh") {
        return Ok(format!("{code}.SH"));
    }

    if let Some(code) = sina_code.strip_prefix("sz") {
        return Ok(format!("{code}.SZ"));
    }

    if let Some(code) = sina_code.strip_prefix("bj") {
        return Ok(format!("{code}.BJ"));
    }
    Err(format!("无法识别的新浪symbol: {sina_code}"))
}

fn ts_code_to_tencent_code(ts_code: &str) -> Result<String, String> {
    ts_code_to_sina_code(ts_code)
}

fn tencent_code_to_ts_code(tencent_code: &str) -> Result<String, String> {
    if let Some(code) = tencent_code.strip_prefix("sh") {
        return Ok(format!("{code}.SH"));
    }

    if let Some(code) = tencent_code.strip_prefix("sz") {
        return Ok(format!("{code}.SZ"));
    }

    if let Some(code) = tencent_code.strip_prefix("bj") {
        return Ok(format!("{code}.BJ"));
    }
    Err(format!("无法识别的腾讯symbol: {tencent_code}"))
}

fn parse_f64_field(fields: &[&str], idx: usize, field_name: &str) -> Result<f64, String> {
    let raw = fields
        .get(idx)
        .ok_or_else(|| format!("字段缺失: {field_name}"))?;

    raw.parse::<f64>()
        .map_err(|e| format!("字段 {field_name} 解析失败: {raw}, {e}"))
}

fn parse_optional_f64_field(
    fields: &[&str],
    idx: usize,
    field_name: &str,
) -> Result<Option<f64>, String> {
    let Some(raw) = fields.get(idx).map(|value| value.trim()) else {
        return Ok(None);
    };

    if raw.is_empty() {
        return Ok(None);
    }

    raw.parse::<f64>()
        .map(Some)
        .map_err(|e| format!("字段 {field_name} 解析失败: {raw}, {e}"))
}

fn parse_tencent_amount(fields: &[&str]) -> Result<f64, String> {
    if let Some(amount_10k) = parse_optional_f64_field(fields, 57, "amount_10k_precise")? {
        if amount_10k > 0.0 {
            return Ok(amount_10k * 10000.0);
        }
    }

    if let Some(summary) = fields.get(35) {
        if let Some(raw_amount) = summary.split('/').nth(2) {
            let raw_amount = raw_amount.trim();
            if !raw_amount.is_empty() {
                return raw_amount
                    .parse::<f64>()
                    .map_err(|e| format!("字段 amount_summary 解析失败: {raw_amount}, {e}"));
            }
        }
    }

    parse_f64_field(fields, 37, "amount_10k").map(|amount_10k| amount_10k * 10000.0)
}

fn sina_list_build(ts_codes: &[String], batch_size: usize) -> Result<Vec<String>, String> {
    if batch_size == 0 {
        return Err("batch_size不能为0".to_string());
    }

    let mut symbols = Vec::with_capacity(ts_codes.len());
    for ts_code in ts_codes {
        symbols.push(ts_code_to_sina_code(ts_code)?);
    }

    let mut batches = Vec::new();
    for chunk in symbols.chunks(batch_size) {
        batches.push(chunk.join(","));
    }
    Ok(batches)
}

fn tencent_list_build(ts_codes: &[String], batch_size: usize) -> Result<Vec<String>, String> {
    if batch_size == 0 {
        return Err("batch_size不能为0".to_string());
    }

    let mut symbols = Vec::with_capacity(ts_codes.len());
    for ts_code in ts_codes {
        symbols.push(ts_code_to_tencent_code(ts_code)?);
    }

    let mut batches = Vec::new();
    for chunk in symbols.chunks(batch_size) {
        batches.push(chunk.join(","));
    }
    Ok(batches)
}

fn sina_realtime_url(list: &str) -> String {
    format!("{SINA_REALTIME_URL}?list={list}")
}

fn tencent_realtime_url(list: &str) -> String {
    format!("{TENCENT_REALTIME_URL}{list}")
}

fn fetch_sina_real_time_data(
    http: &reqwest::blocking::Client,
    list: &str,
) -> Result<String, String> {
    let response = http
        .get(sina_realtime_url(list))
        .header("Accept", "*/*")
        .header("Accept-Language", "zh-CN,zh;q=0.9,en;q=0.8")
        .header("Connection", "keep-alive")
        .header("Referer", "https://finance.sina.com.cn")
        .header("User-Agent", "Mozilla/5.0")
        .send()
        .map_err(|e| format!("请求新浪失败: {e}"))?
        .error_for_status()
        .map_err(|e| format!("新浪实时行情返回HTTP错误: {e}"))?;

    let bytes = response
        .bytes()
        .map_err(|e| format!("读取新浪响应字节失败: {e}"))?;

    let (text, _, had_err) = GBK.decode(&bytes);
    if had_err {
        return Err("新浪响应按GBK解码时出现乱码".to_string());
    }
    Ok(text.into_owned())
}

fn fetch_tencent_real_time_data(
    http: &reqwest::blocking::Client,
    list: &str,
) -> Result<String, String> {
    let response = http
        .get(tencent_realtime_url(list))
        .header("Accept", "*/*")
        .header("Accept-Language", "zh-CN,zh;q=0.9,en;q=0.8")
        .header("Connection", "keep-alive")
        .header("Referer", "https://gu.qq.com")
        .header("User-Agent", "Mozilla/5.0")
        .send()
        .map_err(|e| format!("请求腾讯失败: {e}"))?
        .error_for_status()
        .map_err(|e| format!("腾讯实时行情返回HTTP错误: {e}"))?;

    let bytes = response
        .bytes()
        .map_err(|e| format!("读取腾讯响应字节失败: {e}"))?;

    let (text, _, had_err) = GBK.decode(&bytes);
    if had_err {
        return Err("腾讯响应按GBK解码时出现乱码".to_string());
    }
    Ok(text.into_owned())
}

async fn fetch_sina_real_time_data_async(
    http: &reqwest::Client,
    list: &str,
) -> Result<String, String> {
    let response = http
        .get(sina_realtime_url(list))
        .header("Accept", "*/*")
        .header("Accept-Language", "zh-CN,zh;q=0.9,en;q=0.8")
        .header("Connection", "keep-alive")
        .header("Referer", "https://finance.sina.com.cn")
        .header("User-Agent", "Mozilla/5.0")
        .send()
        .await
        .map_err(|e| format!("请求新浪失败: {e}"))?
        .error_for_status()
        .map_err(|e| format!("新浪实时行情返回HTTP错误: {e}"))?;

    let bytes = response
        .bytes()
        .await
        .map_err(|e| format!("读取新浪响应字节失败: {e}"))?;

    let (text, _, had_err) = GBK.decode(&bytes);
    if had_err {
        return Err("新浪响应按GBK解码时出现乱码".to_string());
    }
    Ok(text.into_owned())
}

async fn fetch_tencent_real_time_data_async(
    http: &reqwest::Client,
    list: &str,
) -> Result<String, String> {
    let response = http
        .get(tencent_realtime_url(list))
        .header("Accept", "*/*")
        .header("Accept-Language", "zh-CN,zh;q=0.9,en;q=0.8")
        .header("Connection", "keep-alive")
        .header("Referer", "https://gu.qq.com")
        .header("User-Agent", "Mozilla/5.0")
        .send()
        .await
        .map_err(|e| format!("请求腾讯失败: {e}"))?
        .error_for_status()
        .map_err(|e| format!("腾讯实时行情返回HTTP错误: {e}"))?;

    let bytes = response
        .bytes()
        .await
        .map_err(|e| format!("读取腾讯响应字节失败: {e}"))?;

    let (text, _, had_err) = GBK.decode(&bytes);
    if had_err {
        return Err("腾讯响应按GBK解码时出现乱码".to_string());
    }
    Ok(text.into_owned())
}

fn parse_sina_quote_line(line: &str) -> Result<Option<SinaQuote>, String> {
    let line = line.trim();
    if line.is_empty() {
        return Ok(None);
    }

    let (left, right) = line
        .split_once("=\"")
        .ok_or_else(|| format!("行情行格式错误，缺少 =\": {line}"))?;

    let symbol = left
        .strip_prefix("var hq_str_")
        .ok_or_else(|| format!("行情行前缀错误: {left}"))?;

    let payload = right
        .strip_suffix("\";")
        .ok_or_else(|| format!("行情行结尾错误: {right}"))?;

    if payload.is_empty() {
        return Ok(None);
    }

    let fields: Vec<&str> = payload.split(',').collect();
    if fields.len() < 32 {
        return Err(format!("字段数量不足: {symbol}, len={}", fields.len()));
    }

    let ts_code = sina_code_to_ts_code(symbol)?;
    let name = fields[0].to_string();
    let open = parse_f64_field(&fields, 1, "open")?;
    let pre_close = parse_f64_field(&fields, 2, "pre_close")?;
    let price = parse_f64_field(&fields, 3, "price")?;
    let high = parse_f64_field(&fields, 4, "high")?;
    let low = parse_f64_field(&fields, 5, "low")?;
    // 新浪 level-1 返回的是成交股数；库里的 stock_data.vol 使用“手”，这里统一 /100。
    let vol = parse_f64_field(&fields, 8, "volume")? / 100.0;
    let amount = parse_f64_field(&fields, 9, "amount")?;
    let date = fields[30].to_string();
    let time = fields[31].to_string();
    let change_pct = {
        if pre_close > 0.0 {
            Some((price / pre_close - 1.0) * 100.0)
        } else {
            None
        }
    };

    Ok(Some(SinaQuote {
        date,
        time,
        ts_code,
        name,
        open,
        high,
        low,
        pre_close,
        price,
        vol,
        amount,
        change_pct,
    }))
}

fn parse_tencent_datetime(raw: &str) -> Result<(String, String), String> {
    let digits: String = raw.chars().filter(|ch| ch.is_ascii_digit()).collect();
    if digits.len() < 14 {
        return Err(format!("腾讯datetime字段格式错误: {raw}"));
    }

    Ok((
        digits[0..8].to_string(),
        format!("{}:{}:{}", &digits[8..10], &digits[10..12], &digits[12..14]),
    ))
}

fn parse_tencent_quote_segment(segment: &str) -> Result<Option<TencentQuote>, String> {
    let segment = segment.trim();
    if segment.is_empty() {
        return Ok(None);
    }

    let (left, right) = segment
        .split_once("=\"")
        .ok_or_else(|| format!("腾讯行情段格式错误，缺少 =\": {segment}"))?;

    let symbol = left
        .trim()
        .strip_prefix("v_")
        .ok_or_else(|| format!("腾讯行情段前缀错误: {left}"))?;

    let payload = right
        .strip_suffix('"')
        .ok_or_else(|| format!("腾讯行情段结尾错误: {right}"))?;

    if payload.is_empty() {
        return Ok(None);
    }

    let fields: Vec<&str> = payload.split('~').collect();
    if fields.len() <= 49 {
        return Ok(None);
    }

    let ts_code = tencent_code_to_ts_code(symbol)?;
    let name = fields[1].to_string();
    let price = parse_f64_field(&fields, 3, "now")?;
    let pre_close = parse_f64_field(&fields, 4, "close")?;
    let open = parse_f64_field(&fields, 5, "open")?;
    let high = parse_f64_field(&fields, 33, "high")?;
    let low = parse_f64_field(&fields, 34, "low")?;
    let vol = parse_f64_field(&fields, 36, "volume_hand")?;
    let amount = parse_tencent_amount(&fields)?;
    let change_pct = parse_optional_f64_field(&fields, 32, "change_pct")?;
    let volume_ratio = parse_optional_f64_field(&fields, 49, "volume_ratio")?;
    let avg_price = parse_optional_f64_field(&fields, 51, "avg_price")?;
    let datetime = fields
        .get(30)
        .ok_or_else(|| format!("字段缺失: datetime, {symbol}"))?;
    let (date, time) = parse_tencent_datetime(datetime)?;

    Ok(Some(TencentQuote {
        date,
        time,
        ts_code,
        name,
        open,
        high,
        low,
        pre_close,
        price,
        vol,
        amount,
        change_pct,
        volume_ratio,
        avg_price,
    }))
}

fn parse_sina_quote_text(raw: &str) -> Result<Vec<SinaQuote>, String> {
    let mut quotes = Vec::new();

    for line in raw.lines() {
        if let Some(quote) = parse_sina_quote_line(line)? {
            quotes.push(quote);
        }
    }

    Ok(quotes)
}

fn parse_tencent_quote_text(raw: &str) -> Result<Vec<TencentQuote>, String> {
    let mut quotes = Vec::new();

    for segment in raw.split(';') {
        if let Some(quote) = parse_tencent_quote_segment(segment)? {
            quotes.push(quote);
        }
    }

    Ok(quotes)
}

pub fn fetch_sina_quotes(
    http: &reqwest::blocking::Client,
    ts_codes: &[String],
    batch_size: usize,
) -> Result<Vec<SinaQuote>, String> {
    let batches = sina_list_build(ts_codes, batch_size)?;
    let mut all_quotes = Vec::new();

    for batch in &batches {
        let raw = fetch_sina_real_time_data(http, batch)?;
        let quotes = parse_sina_quote_text(&raw)?;
        all_quotes.extend(quotes);
    }

    Ok(all_quotes)
}

pub fn fetch_sina_index_quotes(
    http: &reqwest::blocking::Client,
    index_ts_codes: &[String],
    batch_size: usize,
) -> Result<Vec<SinaQuote>, String> {
    fetch_sina_quotes(http, index_ts_codes, batch_size)
}

pub fn fetch_default_sina_index_quotes(
    http: &reqwest::blocking::Client,
) -> Result<Vec<SinaQuote>, String> {
    let index_ts_codes = default_realtime_index_ts_codes();
    fetch_sina_index_quotes(http, &index_ts_codes, DEFAULT_REALTIME_INDEX_TS_CODES.len())
}

pub fn fetch_tencent_quotes(
    http: &reqwest::blocking::Client,
    ts_codes: &[String],
    batch_size: usize,
) -> Result<Vec<TencentQuote>, String> {
    let batches = tencent_list_build(ts_codes, batch_size)?;
    let mut all_quotes = Vec::new();

    for batch in &batches {
        let raw = fetch_tencent_real_time_data(http, batch)?;
        let quotes = parse_tencent_quote_text(&raw)?;
        all_quotes.extend(quotes);
    }

    Ok(all_quotes)
}

pub fn fetch_tencent_index_quotes(
    http: &reqwest::blocking::Client,
    index_ts_codes: &[String],
    batch_size: usize,
) -> Result<Vec<TencentQuote>, String> {
    fetch_tencent_quotes(http, index_ts_codes, batch_size)
}

pub fn fetch_default_tencent_index_quotes(
    http: &reqwest::blocking::Client,
) -> Result<Vec<TencentQuote>, String> {
    let index_ts_codes = default_realtime_index_ts_codes();
    fetch_tencent_index_quotes(http, &index_ts_codes, DEFAULT_REALTIME_INDEX_TS_CODES.len())
}

pub fn fetch_sina_quotes_parallel(
    http: &reqwest::blocking::Client,
    ts_codes: &[String],
    batch_size: usize,
) -> Result<Vec<SinaQuote>, String> {
    let batches = sina_list_build(ts_codes, batch_size)?;
    let grouped_quotes = batches
        .par_iter()
        .map(|batch| {
            let raw = fetch_sina_real_time_data(http, batch)?;
            parse_sina_quote_text(&raw)
        })
        .collect::<Result<Vec<_>, String>>()?;

    Ok(grouped_quotes.into_iter().flatten().collect())
}

pub fn fetch_tencent_quotes_parallel(
    http: &reqwest::blocking::Client,
    ts_codes: &[String],
    batch_size: usize,
) -> Result<Vec<TencentQuote>, String> {
    let batches = tencent_list_build(ts_codes, batch_size)?;
    let grouped_quotes = batches
        .par_iter()
        .map(|batch| {
            let raw = fetch_tencent_real_time_data(http, batch)?;
            parse_tencent_quote_text(&raw)
        })
        .collect::<Result<Vec<_>, String>>()?;

    Ok(grouped_quotes.into_iter().flatten().collect())
}

pub fn default_realtime_index_ts_codes() -> Vec<String> {
    DEFAULT_REALTIME_INDEX_TS_CODES
        .iter()
        .map(|ts_code| (*ts_code).to_string())
        .collect()
}

pub async fn fetch_sina_quotes_async(
    http: &reqwest::Client,
    ts_codes: &[String],
    batch_size: usize,
) -> Result<Vec<SinaQuote>, String> {
    let batches = sina_list_build(ts_codes, batch_size)?;
    let mut all_quotes = Vec::new();

    for batch in &batches {
        let raw = fetch_sina_real_time_data_async(http, batch).await?;
        let quotes = parse_sina_quote_text(&raw)?;
        all_quotes.extend(quotes);
    }

    Ok(all_quotes)
}

pub async fn fetch_sina_index_quotes_async(
    http: &reqwest::Client,
    index_ts_codes: &[String],
    batch_size: usize,
) -> Result<Vec<SinaQuote>, String> {
    fetch_sina_quotes_async(http, index_ts_codes, batch_size).await
}

pub async fn fetch_default_sina_index_quotes_async(
    http: &reqwest::Client,
) -> Result<Vec<SinaQuote>, String> {
    let index_ts_codes = default_realtime_index_ts_codes();
    fetch_sina_index_quotes_async(http, &index_ts_codes, DEFAULT_REALTIME_INDEX_TS_CODES.len())
        .await
}

pub async fn fetch_tencent_quotes_async(
    http: &reqwest::Client,
    ts_codes: &[String],
    batch_size: usize,
) -> Result<Vec<TencentQuote>, String> {
    let batches = tencent_list_build(ts_codes, batch_size)?;
    let mut all_quotes = Vec::new();

    for batch in &batches {
        let raw = fetch_tencent_real_time_data_async(http, batch).await?;
        let quotes = parse_tencent_quote_text(&raw)?;
        all_quotes.extend(quotes);
    }

    Ok(all_quotes)
}

pub async fn fetch_tencent_index_quotes_async(
    http: &reqwest::Client,
    index_ts_codes: &[String],
    batch_size: usize,
) -> Result<Vec<TencentQuote>, String> {
    fetch_tencent_quotes_async(http, index_ts_codes, batch_size).await
}

pub async fn fetch_default_tencent_index_quotes_async(
    http: &reqwest::Client,
) -> Result<Vec<TencentQuote>, String> {
    let index_ts_codes = default_realtime_index_ts_codes();
    fetch_tencent_index_quotes_async(http, &index_ts_codes, DEFAULT_REALTIME_INDEX_TS_CODES.len())
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_close(left: f64, right: f64) {
        assert!((left - right).abs() < 1e-6, "left={left}, right={right}");
    }

    #[test]
    fn realtime_url_keeps_comma_separated_symbol_list_unescaped() {
        let url = sina_realtime_url("sh600000,sz000001");
        assert_eq!(url, "http://hq.sinajs.cn/?list=sh600000,sz000001");
        assert!(!url.contains("%2C"));
    }

    #[test]
    fn tencent_realtime_url_keeps_comma_separated_symbol_list_unescaped() {
        let url = tencent_realtime_url("sh600000,sz000001");
        assert_eq!(url, "http://qt.gtimg.cn/q=sh600000,sz000001");
        assert!(!url.contains("%2C"));
    }

    #[test]
    fn default_realtime_index_ts_codes_cover_major_a_share_indexes() {
        assert_eq!(
            default_realtime_index_ts_codes(),
            vec![
                "000001.SH",
                "399001.SZ",
                "399006.SZ",
                "899050.BJ",
                "000300.SH",
                "000905.SH",
                "000852.SH",
                "000510.SH",
                "000688.SH",
                "399673.SZ"
            ]
        );
    }

    #[test]
    fn parse_sina_quote_text_accepts_realtime_index_payload() {
        let raw = r#"var hq_str_sh000001="上证指数,3979.7057,3993.2258,3987.0147,3997.4777,3958.4371,0,0,568545244,1185554610734,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,2026-06-11,15:30:39,00,";"#;

        let quotes = parse_sina_quote_text(raw).expect("sina index quote should parse");

        assert_eq!(quotes.len(), 1);
        let quote = &quotes[0];
        assert_eq!(quote.ts_code, "000001.SH");
        assert_eq!(quote.name, "上证指数");
        assert_eq!(quote.date, "2026-06-11");
        assert_eq!(quote.time, "15:30:39");
        assert_eq!(quote.open, 3979.7057);
        assert_eq!(quote.pre_close, 3993.2258);
        assert_eq!(quote.price, 3987.0147);
        assert_eq!(quote.high, 3997.4777);
        assert_eq!(quote.low, 3958.4371);
        assert_close(quote.vol, 5685452.44);
        assert_eq!(quote.amount, 1185554610734.0);
        assert_close(quote.change_pct.unwrap(), -0.15554091631883038);
    }

    #[test]
    fn parse_tencent_quote_text_accepts_realtime_index_payload() {
        let raw = r#"v_sh000001="1~上证指数~000001~3987.01~3993.23~3979.71~568545244~0~0~0.00~0~0.00~0~0.00~0~0.00~0~0.00~0~0.00~0~0.00~0~0.00~0~0.00~0~0.00~0~~20260611161416~-6.22~-0.16~3997.48~3958.44~3987.01/568545244/1185554610734~568545244~118555461~1.18~17.64~~3997.48~3958.44~0.98~616427.60~664964.51~0.00~-1~-1~0.91~0~3978.73~~~~~~118555461.0734~0.0000~0~ ~ZS~0.46~-1.74~~~~4258.86~3347.65~-2.72~-4.57~-2.65~4818965090870~~0.56~4.19~4818965090870~~~17.19~0.03~~CNY~0~~0.00~0~";"#;

        let quotes = parse_tencent_quote_text(raw).expect("tencent index quote should parse");

        assert_eq!(quotes.len(), 1);
        let quote = &quotes[0];
        assert_eq!(quote.ts_code, "000001.SH");
        assert_eq!(quote.name, "上证指数");
        assert_eq!(quote.date, "20260611");
        assert_eq!(quote.time, "16:14:16");
        assert_eq!(quote.open, 3979.71);
        assert_eq!(quote.pre_close, 3993.23);
        assert_eq!(quote.price, 3987.01);
        assert_eq!(quote.high, 3997.48);
        assert_eq!(quote.low, 3958.44);
        assert_eq!(quote.vol, 568545244.0);
        assert_eq!(quote.amount, 1185554610734.0);
        assert_eq!(quote.change_pct, Some(-0.16));
        assert_eq!(quote.volume_ratio, Some(0.91));
        assert_eq!(quote.avg_price, Some(3978.73));
    }

    #[test]
    fn parse_tencent_quote_text_maps_core_fields_to_sina_quote() {
        let raw = r#"v_sz000001="51~平安银行~000001~11.30~11.32~11.32~1156222~588658~567564~11.29~4714~11.28~6097~11.27~4340~11.26~5332~11.25~3510~11.30~1337~11.31~5129~11.32~12922~11.33~8979~11.34~9660~~20260611161409~-0.02~-0.18~11.39~11.25~11.30/1156222/1308133972~1156222~130813~0.60~5.09~~11.39~11.25~1.24~2192.83~2192.87~0.47~12.45~10.19~1.02~-14034~11.31~3.77~5.14~~~0.39~130813.3972~0.0000~0~ ~GP-A~-0.96~4.44~5.29~7.91~0.71~13.09~10.43~6.00~2.26~3.39~19405600653~19405918198~-22.63~-1.43~19405600653~~~0.43~0.00~~CNY~0~~11.35~-15597~";"#;

        let quotes = parse_tencent_quote_text(raw).expect("tencent quote should parse");

        assert_eq!(quotes.len(), 1);
        let quote = &quotes[0];
        assert_eq!(quote.ts_code, "000001.SZ");
        assert_eq!(quote.name, "平安银行");
        assert_eq!(quote.date, "20260611");
        assert_eq!(quote.time, "16:14:09");
        assert_eq!(quote.price, 11.30);
        assert_eq!(quote.pre_close, 11.32);
        assert_eq!(quote.open, 11.32);
        assert_eq!(quote.high, 11.39);
        assert_eq!(quote.low, 11.25);
        assert_eq!(quote.vol, 1156222.0);
        assert_eq!(quote.amount, 1308133972.0);
        assert_eq!(quote.change_pct, Some(-0.18));
        assert_eq!(quote.volume_ratio, Some(1.02));
        assert_eq!(quote.avg_price, Some(11.31));

        let compatible_quote = quote.clone().into_sina_quote();
        assert_eq!(compatible_quote.ts_code, "000001.SZ");
        assert_eq!(compatible_quote.amount, 1308133972.0);
    }

    #[test]
    fn parse_tencent_quote_text_falls_back_to_integer_amount_field() {
        let raw = r#"v_sz000001="51~平安银行~000001~11.30~11.32~11.32~1156222~588658~567564~11.29~4714~11.28~6097~11.27~4340~11.26~5332~11.25~3510~11.30~1337~11.31~5129~11.32~12922~11.33~8979~11.34~9660~~20260611161409~-0.02~-0.18~11.39~11.25~11.30/1156222/~1156222~130813~0.60~5.09~~11.39~11.25~1.24~2192.83~2192.87~0.47~12.45~10.19~1.02~-14034~11.31~3.77~5.14~~~~0.0000~0~ ~GP-A~-0.96~4.44~5.29~7.91~0.71~13.09~10.43~6.00~2.26~3.39~19405600653~19405918198~-22.63~-1.43~19405600653~~~0.43~0.00~~CNY~0~~11.35~-15597~";"#;

        let quotes = parse_tencent_quote_text(raw).expect("tencent quote should parse");

        assert_eq!(quotes[0].amount, 1308130000.0);
    }

    #[test]
    fn parse_tencent_quote_text_skips_empty_or_short_payloads() {
        let raw = r#"v_sz000001=""; v_sz000002="51~万科A";"#;

        let quotes =
            parse_tencent_quote_text(raw).expect("short tencent payload should be skipped");

        assert!(quotes.is_empty());
    }
}
