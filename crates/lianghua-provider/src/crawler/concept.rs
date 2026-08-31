use std::collections::HashSet;

use encoding_rs::GBK;
use serde::Serialize;

const THS_CONCEPT_URL_PREFIX: &str = "https://basic.10jqka.com.cn";

#[derive(Debug, Clone, Serialize)]
pub struct ThsConceptRow {
    pub ts_code: String,
    pub name: String,
    pub concept: String,
}

#[derive(Debug, Clone)]
pub struct ThsConceptFetchItem {
    pub ts_code: String,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct ThsConceptBatchResult {
    pub rows: Vec<ThsConceptRow>,
    pub processed_count: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct ThsConceptProgress {
    pub phase: String,
    pub finished: usize,
    pub total: usize,
    pub current_label: Option<String>,
    pub message: String,
}

pub type ThsConceptProgressCallback<'a> = dyn Fn(ThsConceptProgress) + Send + Sync + 'a;

fn emit_ths_progress(
    progress_cb: Option<&ThsConceptProgressCallback<'_>>,
    phase: &str,
    finished: usize,
    total: usize,
    current_label: Option<String>,
    message: impl Into<String>,
) {
    if let Some(cb) = progress_cb {
        cb(ThsConceptProgress {
            phase: phase.to_string(),
            finished,
            total,
            current_label,
            message: message.into(),
        });
    }
}

fn ts_code_to_ths_symbol(ts_code: &str) -> Result<String, String> {
    let std_code = ts_code.trim().to_ascii_uppercase();

    let (code, market) = std_code
        .split_once('.')
        .ok_or_else(|| format!("输入代码不是标准 ts_code: {ts_code}"))?;

    if code.len() != 6 || !code.chars().all(|ch| ch.is_ascii_digit()) {
        return Err(format!("股票代码部分不是 6 位数字: {ts_code}"));
    }

    match market {
        "SH" | "SZ" | "BJ" => Ok(code.to_string()),
        _ => Err(format!("暂不支持的市场后缀: {market}")),
    }
}

fn build_ths_concept_url(ts_code: &str) -> Result<String, String> {
    let symbol = ts_code_to_ths_symbol(ts_code)?;
    Ok(format!("{THS_CONCEPT_URL_PREFIX}/{symbol}/concept.html"))
}

pub fn fetch_ths_concept_html(
    http: &reqwest::blocking::Client,
    ts_code: &str,
) -> Result<String, String> {
    let url = build_ths_concept_url(ts_code)?;

    let response = http
        .get(&url)
        .header("Referer", "https://basic.10jqka.com.cn/")
        .header("User-Agent", "Mozilla/5.0")
        .send()
        .map_err(|e| format!("请求同花顺概念页失败: ts_code={ts_code}, err={e}"))?
        .error_for_status()
        .map_err(|e| format!("同花顺概念页返回 HTTP 错误: ts_code={ts_code}, err={e}"))?;

    let bytes = response
        .bytes()
        .map_err(|e| format!("读取同花顺概念页响应字节失败: ts_code={ts_code}, err={e}"))?;

    let (html, _, had_err) = GBK.decode(&bytes);
    if had_err {
        return Err(format!(
            "同花顺概念页按 GBK 解码出现乱码: ts_code={ts_code}"
        ));
    }

    Ok(html.into_owned())
}

fn strip_html_tags(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    let mut in_tag = false;

    for ch in raw.chars() {
        match ch {
            '<' => in_tag = true,
            '>' => in_tag = false,
            _ if !in_tag => out.push(ch),
            _ => {}
        }
    }

    out
}

fn decode_html_entities(raw: &str) -> String {
    raw.replace("&nbsp;", " ")
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
}

fn normalize_html_text(raw: &str) -> String {
    let decoded = decode_html_entities(raw);
    let plain = strip_html_tags(&decoded);
    plain.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn extract_gn_content_table(html: &str) -> Result<&str, String> {
    let start = html
        .find(r#"<table class="gnContent">"#)
        .ok_or_else(|| "未找到 gnContent 概念表格".to_string())?;
    let table_html = &html[start..];
    let end = table_html
        .find("</table>")
        .ok_or_else(|| "概念表格缺少 </table> 结尾".to_string())?;

    Ok(&table_html[..end])
}

fn extract_row_cells(row_html: &str) -> Vec<String> {
    let mut cells = Vec::new();

    for part in row_html.split("<td").skip(1) {
        let Some((_, after_open)) = part.split_once('>') else {
            continue;
        };
        let Some((cell_html, _)) = after_open.split_once("</td>") else {
            continue;
        };

        let text = normalize_html_text(cell_html);
        cells.push(text);
    }

    cells
}

pub fn parse_ths_concept_names(html: &str) -> Result<Vec<String>, String> {
    let table_html = extract_gn_content_table(html)?;
    let mut out = Vec::new();
    let mut seen = HashSet::new();

    for part in table_html.split("<tr").skip(1) {
        let Some((_, row_html)) = part.split_once('>') else {
            continue;
        };
        let cells = extract_row_cells(row_html);
        let Some(index_text) = cells.first() else {
            continue;
        };

        if !index_text.chars().all(|ch| ch.is_ascii_digit()) {
            continue;
        }

        let Some(name) = cells.get(1).map(|value| value.trim()) else {
            continue;
        };
        if name.is_empty() {
            continue;
        }

        let name = name.to_string();
        if seen.insert(name.clone()) {
            out.push(name);
        }
    }

    Ok(out)
}

pub fn parse_ths_concept_text(html: &str) -> Result<String, String> {
    let names = parse_ths_concept_names(html)?;
    Ok(names.join(","))
}

fn is_missing_concept_table_error(error: &str) -> bool {
    error.contains("未找到 gnContent 概念表格")
}

pub fn fetch_one_ths_concept_row(
    http: &reqwest::blocking::Client,
    ts_code: &str,
    name: &str,
) -> Result<ThsConceptRow, String> {
    let normalized_ts_code = ts_code.trim().to_ascii_uppercase();
    if normalized_ts_code.is_empty() {
        return Err("ts_code 不能为空".to_string());
    }

    let normalized_name = name.trim().to_string();
    let html = fetch_ths_concept_html(http, &normalized_ts_code)?;
    let concept = match parse_ths_concept_text(&html) {
        Ok(value) => value,
        Err(error) if is_missing_concept_table_error(&error) => String::new(),
        Err(error) => return Err(error),
    };

    Ok(ThsConceptRow {
        ts_code: normalized_ts_code,
        name: normalized_name,
        concept,
    })
}

pub fn fetch_ths_concept_rows(
    http: &reqwest::blocking::Client,
    items: &[ThsConceptFetchItem],
    progress_cb: Option<&ThsConceptProgressCallback<'_>>,
) -> Result<ThsConceptBatchResult, String> {
    if items.is_empty() {
        return Ok(ThsConceptBatchResult::default());
    }

    emit_ths_progress(
        progress_cb,
        "prepare_ths_concepts",
        0,
        items.len(),
        None,
        format!("开始抓取同花顺概念，共 {} 只股票。", items.len()),
    );

    let mut result = ThsConceptBatchResult {
        rows: Vec::with_capacity(items.len()),
        processed_count: 0,
    };

    for item in items {
        emit_ths_progress(
            progress_cb,
            "fetch_ths_concept",
            result.processed_count,
            items.len(),
            Some(item.ts_code.clone()),
            format!(
                "正在抓取 {}/{}: {} {}",
                result.processed_count + 1,
                items.len(),
                item.ts_code,
                item.name
            ),
        );

        let row = fetch_one_ths_concept_row(http, &item.ts_code, &item.name).map_err(|error| {
            emit_ths_progress(
                progress_cb,
                "failed_ths_concept",
                result.processed_count,
                items.len(),
                Some(item.ts_code.clone()),
                format!("{} {} 抓取失败并停止: {}", item.ts_code, item.name, error),
            );
            format!(
                "抓取中断: ts_code={}, name={}, err={error}",
                item.ts_code, item.name
            )
        })?;
        result.rows.push(row);
        result.processed_count += 1;
        emit_ths_progress(
            progress_cb,
            "fetch_ths_concept",
            result.processed_count,
            items.len(),
            Some(item.ts_code.clone()),
            format!(
                "已完成 {}/{}: {} {}",
                result.processed_count,
                items.len(),
                item.ts_code,
                item.name
            ),
        );
    }

    emit_ths_progress(
        progress_cb,
        "done_ths_concepts",
        result.processed_count,
        items.len(),
        None,
        format!("同花顺概念抓取完成，共 {} 只。", result.processed_count),
    );

    Ok(result)
}
