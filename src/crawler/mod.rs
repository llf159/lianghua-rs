pub mod concept;

use encoding_rs::GBK;
use serde::Serialize;

const SINA_REALTIME_URL: &str = "http://hq.sinajs.cn/";

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

fn parse_f64_field(fields: &[&str], idx: usize, field_name: &str) -> Result<f64, String> {
    let raw = fields
        .get(idx)
        .ok_or_else(|| format!("字段缺失: {field_name}"))?;

    raw.parse::<f64>()
        .map_err(|e| format!("字段 {field_name} 解析失败: {raw}, {e}"))
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

fn fetch_sina_real_time_data(
    http: &reqwest::blocking::Client,
    list: &str,
) -> Result<String, String> {
    let response = http
        .get(SINA_REALTIME_URL)
        .query(&[("list", list)])
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

async fn fetch_sina_real_time_data_async(
    http: &reqwest::Client,
    list: &str,
) -> Result<String, String> {
    let response = http
        .get(SINA_REALTIME_URL)
        .query(&[("list", list)])
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

fn parse_sina_quote_text(raw: &str) -> Result<Vec<SinaQuote>, String> {
    let mut quotes = Vec::new();

    for line in raw.lines() {
        if let Some(quote) = parse_sina_quote_line(line)? {
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
