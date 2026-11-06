use std::collections::{HashMap, HashSet};
use std::time::Duration;

use chrono::{Datelike, Days, Local, Months, NaiveDate};
use rayon::{ThreadPoolBuilder, prelude::*};
use serde_json::json;

use super::{
    TushareClient, TushareTable,
    runner::{DownloadProgress, DownloadProgressCallback},
};
use crate::data::{
    bulk_write::BulkWriteSettings,
    company_data::{CompanyDbKind, CompanyDownloadWindow, open_company_db, replace_company_stock},
    load_stock_list,
};

pub const DEFAULT_COMPANY_REFRESH_QUARTERS: usize = 8;

#[derive(Clone, Copy, PartialEq, Eq)]
enum CompanyWindowPolicy {
    Recent,
    WholeRange,
}

// date_field matches the upstream range filter, not necessarily the report period.
const COMPANY_DATASETS: &[(&str, &str, &str, usize, CompanyWindowPolicy, CompanyDbKind)] = &[
    (
        "income",
        "利润表",
        "ann_date",
        500,
        CompanyWindowPolicy::Recent,
        CompanyDbKind::Fundamentals,
    ),
    (
        "balancesheet",
        "资产负债表",
        "ann_date",
        100,
        CompanyWindowPolicy::Recent,
        CompanyDbKind::Fundamentals,
    ),
    (
        "cashflow",
        "现金流量表",
        "ann_date",
        100,
        CompanyWindowPolicy::Recent,
        CompanyDbKind::Fundamentals,
    ),
    (
        "fina_indicator",
        "财务指标",
        "end_date",
        100,
        CompanyWindowPolicy::Recent,
        CompanyDbKind::Fundamentals,
    ),
    (
        "fina_mainbz",
        "主营业务构成",
        "end_date",
        100,
        CompanyWindowPolicy::Recent,
        CompanyDbKind::Fundamentals,
    ),
    (
        "forecast",
        "业绩预告",
        "ann_date",
        3500,
        CompanyWindowPolicy::Recent,
        CompanyDbKind::Fundamentals,
    ),
    (
        "express",
        "业绩快报",
        "ann_date",
        2000,
        CompanyWindowPolicy::Recent,
        CompanyDbKind::Fundamentals,
    ),
    (
        "dividend",
        "分红送股",
        "end_date",
        2000,
        CompanyWindowPolicy::WholeRange,
        CompanyDbKind::Fundamentals,
    ),
    (
        "top10_holders",
        "十大股东",
        "end_date",
        6000,
        CompanyWindowPolicy::Recent,
        CompanyDbKind::Fundamentals,
    ),
    (
        "top10_floatholders",
        "十大流通股东",
        "end_date",
        6000,
        CompanyWindowPolicy::Recent,
        CompanyDbKind::Fundamentals,
    ),
    (
        "stk_holdernumber",
        "股东人数",
        "ann_date",
        3000,
        CompanyWindowPolicy::WholeRange,
        CompanyDbKind::ShareholderCounts,
    ),
    (
        "stk_holdertrade",
        "股东增减持",
        "ann_date",
        3000,
        CompanyWindowPolicy::Recent,
        CompanyDbKind::Fundamentals,
    ),
    (
        "share_float",
        "限售股解禁",
        "float_date",
        6000,
        CompanyWindowPolicy::Recent,
        CompanyDbKind::Fundamentals,
    ),
    (
        "daily_basic",
        "每日股本",
        "trade_date",
        6000,
        CompanyWindowPolicy::Recent,
        CompanyDbKind::Fundamentals,
    ),
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
    pub refresh_quarters: usize,
    pub database: CompanyDbKind,
}

#[derive(Default)]
pub struct CompanyDownloadSummary {
    pub success_count: usize,
    pub failed_count: usize,
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
        let (_, _, date_field, limit, _, _) = COMPANY_DATASETS
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

fn company_refresh_from(today: NaiveDate, refresh_quarters: usize) -> NaiveDate {
    let quarter_month = today.month0() / 3 * 3 + 1;
    let current_quarter = NaiveDate::from_ymd_opt(today.year(), quarter_month, 1)
        .expect("quarter start must be a valid date");
    let months_back = refresh_quarters.saturating_sub(1).saturating_mul(3);
    current_quarter
        .checked_sub_months(Months::new(u32::try_from(months_back).unwrap_or(u32::MAX)))
        .unwrap_or(NaiveDate::MIN)
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
    let refresh_from = company_refresh_from(today, config.refresh_quarters);
    let mut windows = Vec::new();
    let mut from = start;
    while from <= end {
        let mut to = NaiveDate::from_ymd_opt(from.year(), 12, 31)
            .unwrap()
            .min(end);
        if from < refresh_from && refresh_from <= to {
            to = refresh_from.pred_opt().ok_or("日期超出范围")?;
        }
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
    let mut conn = open_company_db(&config.source_dir, config.database)?;
    if config.database == CompanyDbKind::Fundamentals {
        // Old 150-row detection accepted truncated 100-row responses. Invalidate their
        // completion records once, retaining the data until a successful replacement.
        let tx = conn.transaction().map_err(|e| e.to_string())?;
        tx.execute_batch(
            "CREATE TABLE IF NOT EXISTS company_download_migrations (name VARCHAR PRIMARY KEY);
             DELETE FROM company_sync_log
             WHERE api_name = 'fina_mainbz' AND row_count >= 100
               AND NOT EXISTS (SELECT 1 FROM company_download_migrations
                               WHERE name = 'fina_mainbz_limit_100');
             INSERT INTO company_download_migrations VALUES ('fina_mainbz_limit_100')
             ON CONFLICT DO NOTHING;",
        )
        .map_err(|e| format!("更新主营构成下载记录失败: {e}"))?;
        tx.commit().map_err(|e| e.to_string())?;
    }
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
    for ranges in completed.values_mut() {
        ranges.sort_unstable();
    }
    let settings = BulkWriteSettings::apply(&conn)?;
    let result = (|| {
        let mut summary = CompanyDownloadSummary::default();
        let total = stocks.len();
        let mut failed_stocks = HashSet::new();
        let refresh_from = refresh_from.format("%Y%m%d").to_string();
        for &(api, label, date_field, _, policy, database) in COMPANY_DATASETS {
            if database != config.database {
                continue;
            }
            let whole_range = vec![(
                start.format("%Y%m%d").to_string(),
                end.format("%Y%m%d").to_string(),
            )];
            let api_windows = if policy == CompanyWindowPolicy::WholeRange {
                &whole_range
            } else {
                &windows
            };
            let mut finished = 0;
            let mut tasks = Vec::new();
            for code in &stocks {
                lianghua_model::task::checkpoint()?;
                let covered = completed.get(&(api.to_string(), code.clone()));
                let mut pending: Vec<(String, String)> = Vec::new();
                for (from, to) in api_windows {
                    let mut uncovered = Vec::new();
                    if policy == CompanyWindowPolicy::Recent && to < &refresh_from {
                        let mut cursor = from.clone();
                        if let Some(ranges) = covered {
                            for (a, b) in ranges {
                                if b < &cursor {
                                    continue;
                                }
                                if a > to {
                                    break;
                                }
                                if a > &cursor {
                                    uncovered.push((
                                        cursor.clone(),
                                        NaiveDate::parse_from_str(a, "%Y%m%d")
                                            .map_err(|e| e.to_string())?
                                            .pred_opt()
                                            .ok_or("日期超出范围")?
                                            .format("%Y%m%d")
                                            .to_string(),
                                    ));
                                }
                                cursor = NaiveDate::parse_from_str(b, "%Y%m%d")
                                    .map_err(|e| e.to_string())?
                                    .checked_add_days(Days::new(1))
                                    .ok_or("日期超出范围")?
                                    .format("%Y%m%d")
                                    .to_string();
                                if &cursor > to {
                                    break;
                                }
                            }
                        }
                        if &cursor <= to {
                            uncovered.push((cursor, to.clone()));
                        }
                        if uncovered.is_empty() {
                            summary.skipped_count += 1;
                        }
                    } else {
                        uncovered.push((from.clone(), to.clone()));
                    }
                    // Merge adjacent missing history and refresh ranges before requesting.
                    for (from, to) in uncovered {
                        if let Some(last) = pending.last_mut() {
                            let next = NaiveDate::parse_from_str(&last.1, "%Y%m%d")
                                .map_err(|e| e.to_string())?
                                .checked_add_days(Days::new(1))
                                .ok_or("日期超出范围")?
                                .format("%Y%m%d")
                                .to_string();
                            if next == from {
                                last.1 = to;
                                continue;
                            }
                        }
                        pending.push((from, to));
                    }
                }
                if pending.is_empty() {
                    finished += 1;
                } else {
                    tasks.push((code.clone(), pending));
                }
            }
            for batch in tasks.chunks(pool.current_num_threads()) {
                lianghua_model::task::checkpoint()?;
                if let Some(cb) = progress_cb {
                    cb(DownloadProgress {
                        phase: format!("company_{api}"),
                        finished,
                        total,
                        current_label: Some(format!(
                            "{label} · {} 等 {} 只股票",
                            batch[0].0,
                            batch.len()
                        )),
                        message: format!("正在下载 {} 只股票", batch.len()),
                    });
                }
                let fetched = pool.install(|| {
                    batch
                        .par_iter()
                        .map(|(code, ranges)| {
                            let mut windows = Vec::new();
                            for (from, to) in ranges {
                                let table = client.fetch_company_window(
                                    api,
                                    code,
                                    from,
                                    to,
                                    config.retry_times,
                                )?;
                                windows.push(CompanyDownloadWindow {
                                    api: api.into(),
                                    code: code.clone(),
                                    date_field: date_field.into(),
                                    start: from.clone(),
                                    end: to.clone(),
                                    fields: table.fields,
                                    items: table.items,
                                });
                            }
                            Ok::<_, String>(windows)
                        })
                        .collect::<Vec<_>>()
                });
                lianghua_model::task::checkpoint()?;
                let mut api_disabled = false;
                for ((code, _), result) in batch.iter().zip(fetched) {
                    match result {
                        Ok(windows) => {
                            if let Err(error) = replace_company_stock(&mut conn, &windows) {
                                failed_stocks.insert(code.clone());
                                summary
                                    .failed_items
                                    .push(format!("{code} {label}: {error}"));
                                continue;
                            }
                            summary.saved_rows +=
                                windows.iter().map(|w| w.items.len()).sum::<usize>();
                        }
                        Err(error) => {
                            if error.contains("token") || error.contains("Token") {
                                return Err(error);
                            }
                            api_disabled |= error.contains("code=40203")
                                || error.contains("没有权限")
                                || error.contains("访问权限");
                            failed_stocks.insert(code.clone());
                            summary
                                .failed_items
                                .push(format!("{code} {label}: {error}"));
                        }
                    }
                }
                if let Some(cb) = progress_cb {
                    cb(DownloadProgress {
                        phase: format!("company_{api}"),
                        finished,
                        total,
                        current_label: Some(label.into()),
                        message: "本批拉取完成，已按股票逐只提交。".into(),
                    });
                }
                finished += batch.len();
                if let Some(cb) = progress_cb {
                    cb(DownloadProgress {
                        phase: format!("company_{api}"),
                        finished,
                        total,
                        current_label: Some(label.into()),
                        message: format!("已处理 {finished} / {total} 只股票"),
                    });
                }
                if api_disabled {
                    failed_stocks.extend(stocks.iter().cloned());
                    finished = total;
                    break;
                }
            }
            if let Some(cb) = progress_cb {
                cb(DownloadProgress {
                    phase: format!("company_{api}"),
                    finished,
                    total,
                    current_label: Some(label.into()),
                    message: format!("{label}完成"),
                });
            }
        }
        summary.failed_count = failed_stocks.len();
        summary.success_count = stocks.len() - summary.failed_count;
        if let Some(cb) = progress_cb {
            cb(DownloadProgress {
                phase: if config.database == CompanyDbKind::Fundamentals {
                    "company_fundamentals_done".into()
                } else {
                    "shareholder_counts_done".into()
                },
                finished: total,
                total,
                current_label: None,
                message: format!(
                    "{}下载结束：成功 {} 只，失败 {} 只",
                    if config.database == CompanyDbKind::Fundamentals {
                        "基本面数据"
                    } else {
                        "股东人数"
                    },
                    summary.success_count,
                    summary.failed_count
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
    fn company_mainbz_splits_full_windows_and_limits_requests() {
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
                assert_eq!(request["api_name"], "fina_mainbz");
                let from = request["params"]["start_date"].as_str().unwrap();
                let items = if index == 0 {
                    vec![json!(["600519.SH", from, 1.0]); 100]
                } else {
                    vec![json!(["600519.SH", from, 2.0])]
                };
                let body = json!({"code":0,"data":{"fields":["ts_code","end_date","bz_sales"],"items":items}}).to_string();
                write!(stream, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body).unwrap();
            }
        });
        let mut client = TushareClient::new("mock".into(), 1500).unwrap();
        client.base_url = format!("http://{address}");
        let started = Instant::now();
        let table = client
            .fetch_company_window("fina_mainbz", "600519.SH", "20240101", "20241231", 0)
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
            refresh_quarters: DEFAULT_COMPANY_REFRESH_QUARTERS,
            database: CompanyDbKind::Fundamentals,
        };
        let summary = download_company(
            &config,
            Some(&|p| {
                assert_eq!(
                    p.total, 2,
                    "progress must count stocks, not years or API calls"
                );
                assert!(p.finished <= 2);
            }),
        )
        .unwrap();
        assert!(
            summary.failed_items.is_empty(),
            "{:?}",
            summary.failed_items
        );
        assert_eq!(summary.success_count, 2);
        assert!(summary.saved_rows > 300);
        let resumed = download_company(&config, None).unwrap();
        let periodic_window_count = COMPANY_DATASETS
            .iter()
            .filter(|dataset| {
                dataset.4 == CompanyWindowPolicy::Recent && dataset.5 == CompanyDbKind::Fundamentals
            })
            .count()
            * 2
            * 2;
        assert_eq!(resumed.skipped_count, periodic_window_count);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn company_refresh_window_counts_natural_quarters() {
        let today = NaiveDate::from_ymd_opt(2026, 11, 5).unwrap();
        assert_eq!(
            company_refresh_from(today, 1),
            NaiveDate::from_ymd_opt(2026, 10, 1).unwrap()
        );
        assert_eq!(
            company_refresh_from(today, 4),
            NaiveDate::from_ymd_opt(2026, 1, 1).unwrap()
        );
        assert_eq!(
            company_refresh_from(today, 8),
            NaiveDate::from_ymd_opt(2025, 1, 1).unwrap()
        );
    }
}
