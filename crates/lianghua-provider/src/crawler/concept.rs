use std::collections::HashSet;

use encoding_rs::GBK;
use serde::Serialize;

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

pub fn fetch_ths_concept_html(
    http: &reqwest::blocking::Client,
    ts_code: &str,
) -> Result<String, String> {
    let url = (|ts_code: &str| -> Result<String, String> {
        let symbol = (|ts_code: &str| -> Result<String, String> {
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
        })(ts_code)?;
        Ok(format!("https://basic.10jqka.com.cn/{symbol}/concept.html"))
    })(ts_code)?;

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

pub fn parse_ths_concept_names(html: &str) -> Result<Vec<String>, String> {
    let table_html = {
        let start = html.find(r#"<table class="gnContent">"#).ok_or_else(|| {
            "未找到 gnContent 概念表格，可能遇到访问验证或页面结构变化".to_string()
        })?;
        let table_html = &html[start..];
        let end = table_html
            .find("</table>")
            .ok_or_else(|| "概念表格缺少 </table> 结尾".to_string())?;

        &table_html[..end]
    };
    let mut out = Vec::new();
    let mut seen = HashSet::new();

    for part in table_html.split("<tr").skip(1) {
        let (row_tag, row_html) = part
            .split_once('>')
            .ok_or_else(|| "概念表格行标签不完整，可能发生页面结构变化".to_string())?;
        let (row_html, _) = row_html
            .split_once("</tr>")
            .ok_or_else(|| "概念表格缺少 </tr> 结尾，可能发生页面结构变化".to_string())?;
        if row_tag.contains(r#"class="extend_content""#) {
            continue;
        }
        let cells = (|row_html: &str| -> Result<Vec<String>, String> {
            let mut cells = Vec::new();

            for part in row_html.split("<td").skip(1) {
                let (_, after_open) = part
                    .split_once('>')
                    .ok_or_else(|| "概念表格单元格标签不完整，可能发生页面结构变化".to_string())?;
                let (cell_html, _) = after_open
                    .split_once("</td>")
                    .ok_or_else(|| "概念表格缺少 </td> 结尾，可能发生页面结构变化".to_string())?;

                let text = (|raw: &str| -> String {
                    let decoded = (|raw: &str| -> String {
                        raw.replace("&nbsp;", " ")
                            .replace("&amp;", "&")
                            .replace("&lt;", "<")
                            .replace("&gt;", ">")
                            .replace("&quot;", "\"")
                            .replace("&#39;", "'")
                    })(raw);
                    let plain = (|raw: &str| -> String {
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
                    })(&decoded);
                    plain.split_whitespace().collect::<Vec<_>>().join(" ")
                })(cell_html);
                cells.push(text);
            }

            Ok(cells)
        })(row_html)?;
        if cells.is_empty() && row_html.contains("<th") {
            continue;
        }
        let index_text = cells
            .first()
            .ok_or_else(|| "概念表格数据行缺少单元格，可能发生页面结构变化".to_string())?;

        if index_text.is_empty() || !index_text.chars().all(|ch| ch.is_ascii_digit()) {
            return Err("概念表格序号不是有效数字，可能发生页面结构变化".to_string());
        }

        let name = cells
            .get(1)
            .map(|value| value.trim())
            .filter(|name| !name.is_empty())
            .ok_or_else(|| {
                format!("概念表格第 {index_text} 行缺少概念名称，可能发生页面结构变化")
            })?;

        let name = name.to_string();
        if seen.insert(name.clone()) {
            out.push(name);
        }
    }

    if out.is_empty() {
        return Err("概念表格未解析到有效概念，可能遇到访问验证、空数据或页面结构变化".to_string());
    }

    Ok(out)
}

pub fn parse_ths_concept_text(html: &str) -> Result<String, String> {
    let names = parse_ths_concept_names(html)?;
    Ok(names.join(","))
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
    let concept = parse_ths_concept_text(&html).map_err(|error| {
        format!("同花顺概念页校验失败: ts_code={normalized_ts_code}, err={error}")
    })?;

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_concept_names_and_deduplicates() {
        let html = r#"<table class="gnContent">
            <tr><th>序号</th><th>概念名称</th></tr>
            <tr><td>1</td><td><a>跨境支付(CIPS)</a></td></tr>
            <tr class="extend_content"><td colspan="4">概念解析</td></tr>
            <tr><td>2</td><td>高股息精选</td></tr>
            <tr><td>3</td><td>高股息精选</td></tr>
        </table>"#;

        assert_eq!(
            parse_ths_concept_text(html).unwrap(),
            "跨境支付(CIPS),高股息精选"
        );
    }

    #[test]
    fn rejects_unverified_or_changed_concept_pages() {
        for (html, expected_error) in [
            ("<html>请完成安全验证</html>", "未找到 gnContent"),
            (r#"<table class="changed"></table>"#, "未找到 gnContent"),
            (r#"<table class="gnContent">"#, "缺少 </table>"),
            (r#"<table class="gnContent"></table>"#, "未解析到有效概念"),
            (
                r#"<table class="gnContent"><tr><td>1</td></tr></table>"#,
                "缺少概念名称",
            ),
            (
                r#"<table class="gnContent"><tr><td>1</td><td>有效概念</td></tr><tr><td>2</td><td></td></tr></table>"#,
                "缺少概念名称",
            ),
            (
                r#"<table class="gnContent"><tr><td>1</td><td>有效概念</td></tr><tr><td>2</td><td>残缺概念</tr></table>"#,
                "缺少 </td>",
            ),
            (
                r#"<table class="gnContent"><tr><td>1</td><td>残缺概念</td></table>"#,
                "缺少 </tr>",
            ),
            (
                r#"<table class="gnContent"><tr><td>1</td><td>有效概念</td></tr><tr><td></td><td>序号缺失</td></tr></table>"#,
                "序号不是有效数字",
            ),
        ] {
            let error = parse_ths_concept_text(html).unwrap_err();
            assert!(error.contains(expected_error), "{error}");
        }
    }
}
