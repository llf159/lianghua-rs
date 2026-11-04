use std::collections::HashMap;
use std::time::Duration;

use chrono::{Datelike, Days, Local, NaiveDate};
use rayon::{ThreadPoolBuilder, prelude::*};
use serde_json::json;

use super::{
    TushareClient, TushareTable,
    runner::{DownloadProgress, DownloadProgressCallback},
};
use crate::data::{
    bulk_write::BulkWriteSettings,
    company_data::{CompanyDownloadWindow, open_company_db, replace_company_batch},
    load_stock_list,
};

// date_field matches the upstream range filter, not necessarily the report period.
pub const COMPANY_DATASETS: &[(&str, &str, &str, usize)] = &[
    ("income", "利润表", "ann_date", 500),
    ("balancesheet", "资产负债表", "ann_date", 100),
    ("cashflow", "现金流量表", "ann_date", 100),
    ("fina_indicator", "财务指标", "end_date", 100),
    ("fina_mainbz", "主营业务构成", "end_date", 150),
    ("forecast", "业绩预告", "ann_date", 3500),
    ("express", "业绩快报", "ann_date", 2000),
    ("dividend", "分红送股", "end_date", 2000),
    ("top10_holders", "十大股东", "end_date", 6000),
    ("top10_floatholders", "十大流通股东", "end_date", 6000),
    ("stk_holdernumber", "股东人数", "ann_date", 3000),
    ("stk_holdertrade", "股东增减持", "ann_date", 3000),
    ("share_float", "限售股解禁", "float_date", 6000),
    ("daily_basic", "每日股本", "trade_date", 6000),
];

#[derive(Clone)]
pub struct CompanyDownloadConfig {
    pub source_dir: String,
    pub token: String,
    pub start_date: String,
    pub end_date: String,
    pub threads: usize,
    pub retry_times: usize,
    pub limit_calls_per_min: usize,
    pub refresh_history: bool,
}

#[derive(Default)]
pub struct CompanyDownloadSummary {
    pub success_count: usize,
    pub skipped_count: usize,
    pub saved_rows: usize,
    pub failed_items: Vec<String>,
}

impl TushareClient {
    /// Fetch a complete window. A full response is split before anything is written.
    pub fn fetch_company_window(
        &self,
        api: &str,
        code: &str,
        start: &str,
        end: &str,
        retry_times: usize,
    ) -> Result<TushareTable, String> {
        let (_, _, date_field, limit) = COMPANY_DATASETS
            .iter()
            .find(|d| d.0 == api)
            .ok_or_else(|| format!("不支持的公司数据接口: {api}"))?;
        let mut pending = vec![(start.to_string(), end.to_string())];
        let mut output = TushareTable {
            fields: Vec::new(),
            items: Vec::new(),
        };
        while let Some((from, to)) = pending.pop() {
            let mut request = json!({"ts_code": code, "start_date": from, "end_date": to});
            if api == "dividend" {
                request = json!({"ts_code": code});
            }
            let mut response = None;
            for attempt in 0..=retry_times {
                lianghua_model::task::checkpoint()?;
                // One budget for the entire company task, in addition to upstream per-API limits.
                self.limiter.wait("company-global")?;
                match self.post_table(
                    api,
                    &request,
                    if api == "daily_basic" {
                        "ts_code,trade_date,total_share,float_share,free_share"
                    } else {
                        ""
                    },
                ) {
                    Ok(table) => {
                        response = Some(table);
                        break;
                    }
                    Err(error) => {
                        lianghua_model::task::checkpoint()?;
                        if error.contains("code=40203")
                            || error.contains("没有权限")
                            || error.contains("访问权限")
                            || error.contains("token")
                            || error.contains("Token")
                            || attempt == retry_times
                        {
                            return Err(error);
                        }
                        lianghua_model::task::wait(Duration::from_secs(
                            if error.contains("每分钟") || error.contains("频率") {
                                60
                            } else {
                                (attempt + 1) as u64
                            },
                        ))?;
                    }
                }
            }
            let table = response.ok_or("公司数据请求没有响应")?;
            if table.items.len() >= *limit {
                if from == to || api == "dividend" {
                    return Err(format!(
                        "{api} {code} {from}..{to} 达到单次 {limit} 行上限，无法确认完整性，保留旧数据"
                    ));
                }
                let first =
                    NaiveDate::parse_from_str(&from, "%Y%m%d").map_err(|e| e.to_string())?;
                let last = NaiveDate::parse_from_str(&to, "%Y%m%d").map_err(|e| e.to_string())?;
                let middle = first + chrono::Duration::days((last - first).num_days() / 2);
                pending.push((
                    (middle + chrono::Duration::days(1))
                        .format("%Y%m%d")
                        .to_string(),
                    to,
                ));
                pending.push((from, middle.format("%Y%m%d").to_string()));
                continue;
            }
            if output.fields.is_empty() {
                output.fields = table.fields.clone();
            }
            if output.fields != table.fields {
                return Err(format!("{api} 分段响应字段不一致"));
            }
            let code_index = table
                .fields
                .iter()
                .position(|f| f == "ts_code")
                .ok_or("公司响应缺少 ts_code")?;
            let date_index = table
                .fields
                .iter()
                .position(|f| f == date_field)
                .ok_or_else(|| format!("{api} 响应缺少 {date_field}"))?;
            for row in table.items {
                if row.len() != table.fields.len() || row[code_index].as_str() != Some(code) {
                    return Err(format!("{api} 返回股票或字段数不匹配"));
                }
                let date = row[date_index]
                    .as_str()
                    .ok_or_else(|| format!("{api} 返回空 {date_field}"))?;
                if api == "dividend" && (date < from.as_str() || date > to.as_str()) {
                    continue;
                }
                if date < from.as_str() || date > to.as_str() {
                    return Err(format!("{api} 返回日期超出请求范围"));
                }
                output.items.push(row);
            }
        }
        Ok(output)
    }
}

pub fn download_company(
    config: &CompanyDownloadConfig,
    progress_cb: Option<&DownloadProgressCallback<'_>>,
) -> Result<CompanyDownloadSummary, String> {
    let start = NaiveDate::parse_from_str(&config.start_date, "%Y%m%d")
        .map_err(|e| format!("开始日期无效: {e}"))?;
    let today = Local::now().date_naive();
    let end = if config.end_date == "today" {
        today
    } else {
        NaiveDate::parse_from_str(&config.end_date, "%Y%m%d")
            .map_err(|e| format!("结束日期无效: {e}"))?
    };
    if start > end {
        return Err("开始日期不能晚于结束日期".into());
    }
    let stocks = load_stock_list(&config.source_dir)?
        .into_iter()
        .filter_map(|r| r.first().filter(|s| !s.is_empty()).cloned())
        .collect::<std::collections::BTreeSet<_>>();
    if stocks.is_empty() {
        return Err("股票列表为空，请先刷新基础数据".into());
    }
    let mut windows = Vec::new();
    let mut from = start;
    while from <= end {
        let to = NaiveDate::from_ymd_opt(from.year(), 12, 31)
            .unwrap()
            .min(end);
        windows.push((
            from.format("%Y%m%d").to_string(),
            to.format("%Y%m%d").to_string(),
        ));
        from = to.checked_add_days(Days::new(1)).ok_or("日期超出范围")?;
    }
    let client = TushareClient::new(config.token.clone(), config.limit_calls_per_min)?;
    let pool = ThreadPoolBuilder::new()
        .num_threads(config.threads.max(1))
        .build()
        .map_err(|e| format!("创建公司数据下载线程池失败: {e}"))?;
    let mut conn = open_company_db(&config.source_dir)?;
    let mut completed = HashMap::<(String, String), Vec<(String, String)>>::new();
    {
        let mut stmt = conn
            .prepare("SELECT api_name, ts_code, start_date, end_date FROM company_sync_log")
            .map_err(|e| e.to_string())?;
        let rows = stmt
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
            .map_err(|e| e.to_string())?;
        for row in rows {
            let (api, code, from, to): (String, String, String, String) =
                row.map_err(|e| e.to_string())?;
            completed.entry((api, code)).or_default().push((from, to));
        }
    }
    let settings = BulkWriteSettings::apply(&conn)?;
    let result = (|| {
        let mut summary = CompanyDownloadSummary::default();
        let total = ((COMPANY_DATASETS.len() - 1) * windows.len() + 1) * stocks.len();
        let mut finished = 0;
        let refresh_from = format!("{}0101", today.year() - 1);
        for &(api, label, date_field, _) in COMPANY_DATASETS {
            // Dividend only accepts exact dates, so fetch its stock history once and filter locally.
            let dividend_window = vec![(
                start.format("%Y%m%d").to_string(),
                end.format("%Y%m%d").to_string(),
            )];
            let api_windows = if api == "dividend" {
                &dividend_window
            } else {
                &windows
            };
            let mut tasks = Vec::new();
            for code in &stocks {
                lianghua_model::task::checkpoint()?;
                let covered = completed.get(&(api.to_string(), code.clone()));
                let mut pending: Vec<(String, String, usize)> = Vec::new();
                for (from, to) in api_windows {
                    if !config.refresh_history
                        && to < &refresh_from
                        && covered
                            .is_some_and(|ranges| ranges.iter().any(|(a, b)| a <= from && b >= to))
                    {
                        summary.skipped_count += 1;
                        finished += 1;
                        continue;
                    }
                    // Coalesce adjacent pending years; split only when the API actually hits its cap.
                    if let Some(last) = pending.last_mut() {
                        let next = (NaiveDate::parse_from_str(&last.1, "%Y%m%d")
                            .map_err(|e| e.to_string())?
                            + chrono::Duration::days(1))
                        .format("%Y%m%d")
                        .to_string();
                        if &next == from {
                            last.1 = to.clone();
                            last.2 += 1;
                            continue;
                        }
                    }
                    pending.push((from.clone(), to.clone(), 1));
                }
                tasks.extend(
                    pending
                        .into_iter()
                        .map(|(from, to, count)| (code.clone(), from, to, count)),
                );
            }
            for (batch_index, batch) in tasks.chunks(pool.current_num_threads()).enumerate() {
                lianghua_model::task::checkpoint()?;
                if let Some(cb) = progress_cb {
                    cb(DownloadProgress {
                        phase: "download_company".into(),
                        finished,
                        total,
                        current_label: Some(format!(
                            "{label} · {} 等 {} 个区间",
                            batch[0].0,
                            batch.len()
                        )),
                        message: format!(
                            "并发抓取；已写入 {} 行，跳过 {} 段，失败 {} 段",
                            summary.saved_rows,
                            summary.skipped_count,
                            summary.failed_items.len()
                        ),
                    });
                }
                let fetched = pool.install(|| {
                    batch
                        .par_iter()
                        .map(|(code, from, to, _)| {
                            client.fetch_company_window(api, code, from, to, config.retry_times)
                        })
                        .collect::<Vec<_>>()
                });
                lianghua_model::task::checkpoint()?;
                let mut prepared = Vec::new();
                let mut api_disabled = false;
                for ((code, from, to, _), result) in batch.iter().zip(fetched) {
                    match result {
                        Ok(table) => prepared.push(CompanyDownloadWindow {
                            api: api.into(),
                            code: code.clone(),
                            date_field: date_field.into(),
                            start: from.clone(),
                            end: to.clone(),
                            fields: table.fields,
                            items: table.items,
                        }),
                        Err(error) => {
                            if error.contains("token") || error.contains("Token") {
                                return Err(error);
                            }
                            api_disabled |= error.contains("code=40203")
                                || error.contains("没有权限")
                                || error.contains("访问权限");
                            summary
                                .failed_items
                                .push(format!("{code} {label} {from}—{to}: {error}"));
                        }
                    }
                }
                if let Some(cb) = progress_cb {
                    cb(DownloadProgress {
                        phase: "write_company".into(),
                        finished,
                        total,
                        current_label: Some(label.into()),
                        message: format!(
                            "本批拉取完成，正在统一提交 {} 个成功区间。",
                            prepared.len()
                        ),
                    });
                }
                replace_company_batch(&mut conn, &prepared)?;
                summary.success_count += prepared.len();
                summary.saved_rows += prepared.iter().map(|w| w.items.len()).sum::<usize>();
                finished += batch.iter().map(|t| t.3).sum::<usize>();
                if api_disabled {
                    finished += tasks[batch_index * pool.current_num_threads() + batch.len()..]
                        .iter()
                        .map(|t| t.3)
                        .sum::<usize>();
                    break;
                }
            }
        }
        if let Some(cb) = progress_cb {
            cb(DownloadProgress {
                phase: "company_done".into(),
                finished: total,
                total,
                current_label: None,
                message: format!(
                    "公司经营与股东下载结束：写入 {} 行，成功 {} 段，跳过 {} 段，失败 {} 项",
                    summary.saved_rows,
                    summary.success_count,
                    summary.skipped_count,
                    summary.failed_items.len()
                ),
            });
        }
        Ok(summary)
    })();
    settings.checkpoint_and_finish(&conn, result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        io::{BufRead, BufReader, Read, Write},
        net::TcpListener,
        time::Instant,
    };

    #[test]
    fn company_http_splits_full_windows_and_limits_requests() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            for index in 0..3 {
                let (mut stream, _) = listener.accept().unwrap();
                let mut reader = BufReader::new(&stream);
                let mut length = 0;
                loop {
                    let mut line = String::new();
                    reader.read_line(&mut line).unwrap();
                    if line == "\r\n" {
                        break;
                    }
                    if let Some((name, value)) = line.split_once(':') {
                        if name.eq_ignore_ascii_case("content-length") {
                            length = value.trim().parse::<usize>().unwrap();
                        }
                    }
                }
                let mut body = vec![0; length];
                reader.read_exact(&mut body).unwrap();
                let request: serde_json::Value = serde_json::from_slice(&body).unwrap();
                assert_eq!(request["api_name"], "balancesheet");
                let from = request["params"]["start_date"].as_str().unwrap();
                let items = if index == 0 {
                    vec![json!(["600519.SH", from, 1.0]); 100]
                } else {
                    vec![json!(["600519.SH", from, 2.0])]
                };
                let body = json!({"code":0,"data":{"fields":["ts_code","ann_date","total_assets"],"items":items}}).to_string();
                write!(stream, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body).unwrap();
            }
        });
        let mut client = TushareClient::new("mock".into(), 1500).unwrap();
        client.base_url = format!("http://{address}");
        let started = Instant::now();
        let table = client
            .fetch_company_window("balancesheet", "600519.SH", "20240101", "20241231", 0)
            .unwrap();
        assert_eq!(table.items.len(), 2);
        assert!(started.elapsed() >= Duration::from_millis(75));
        server.join().unwrap();
    }

    #[test]
    #[ignore = "requires TUSHARE_TOKEN; writes only a temporary database"]
    fn company_live_download_and_resume() {
        let directory =
            std::env::temp_dir().join(format!("lianghua-company-live-{}", std::process::id()));
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(
            directory.join("stock_list.csv"),
            "ts_code\n600519.SH\n000001.SZ\n",
        )
        .unwrap();
        let config = CompanyDownloadConfig {
            source_dir: directory.to_string_lossy().into_owned(),
            token: std::env::var("TUSHARE_TOKEN").unwrap(),
            start_date: "20230101".into(),
            end_date: "20241231".into(),
            threads: 2,
            retry_times: 1,
            limit_calls_per_min: 120,
            refresh_history: false,
        };
        let summary = download_company(
            &config,
            Some(&|p| eprintln!("{}: {}", p.current_label.unwrap_or_default(), p.message)),
        )
        .unwrap();
        assert!(
            summary.failed_items.is_empty(),
            "{:?}",
            summary.failed_items
        );
        assert_eq!(summary.success_count, COMPANY_DATASETS.len() * 2);
        assert!(summary.saved_rows > 300);
        let resumed = download_company(&config, None).unwrap();
        assert_eq!(resumed.skipped_count, (COMPANY_DATASETS.len() * 2 - 1) * 2);
        assert_eq!(resumed.saved_rows, 0);
        std::fs::remove_dir_all(directory).unwrap();
    }
}
