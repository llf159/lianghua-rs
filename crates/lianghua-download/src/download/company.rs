use std::collections::HashSet;
use std::time::Duration;

use chrono::{Datelike, Local, Months, NaiveDate};
use rayon::{ThreadPoolBuilder, prelude::*};
use serde_json::json;

use super::{
    TushareClient, TushareTable,
    runner::{DownloadProgress, DownloadProgressCallback},
};
use crate::data::{
    bulk_write::BulkWriteSettings,
    company_data::{CompanyDataWriter, CompanyDbKind, CompanyDownloadWindow, open_company_db},
    load_stock_list,
};

pub const DEFAULT_COMPANY_REFRESH_QUARTERS: usize = 8;

// date_field matches the upstream range filter, not necessarily the report period.
const COMPANY_DATASETS: &[(&str, &str, &str, usize, CompanyDbKind)] = &[
    (
        "income",
        "利润表",
        "ann_date",
        500,
        CompanyDbKind::Fundamentals,
    ),
    (
        "balancesheet",
        "资产负债表",
        "ann_date",
        100,
        CompanyDbKind::Fundamentals,
    ),
    (
        "cashflow",
        "现金流量表",
        "ann_date",
        100,
        CompanyDbKind::Fundamentals,
    ),
    (
        "fina_indicator",
        "财务指标",
        "end_date",
        100,
        CompanyDbKind::Fundamentals,
    ),
    (
        "fina_mainbz",
        "主营业务构成",
        "end_date",
        100,
        CompanyDbKind::Fundamentals,
    ),
    (
        "forecast",
        "业绩预告",
        "ann_date",
        3500,
        CompanyDbKind::Fundamentals,
    ),
    (
        "express",
        "业绩快报",
        "ann_date",
        2000,
        CompanyDbKind::Fundamentals,
    ),
    (
        "dividend",
        "分红送股",
        "end_date",
        2000,
        CompanyDbKind::Fundamentals,
    ),
    (
        "top10_holders",
        "十大股东",
        "end_date",
        6000,
        CompanyDbKind::Fundamentals,
    ),
    (
        "top10_floatholders",
        "十大流通股东",
        "end_date",
        6000,
        CompanyDbKind::Fundamentals,
    ),
    (
        "stk_holdernumber",
        "股东人数",
        "ann_date",
        3000,
        CompanyDbKind::ShareholderCounts,
    ),
    (
        "stk_holdertrade",
        "股东增减持",
        "ann_date",
        3000,
        CompanyDbKind::Fundamentals,
    ),
    (
        "share_float",
        "限售股解禁",
        "float_date",
        6000,
        CompanyDbKind::Fundamentals,
    ),
    (
        "daily_basic",
        "每日股本",
        "trade_date",
        6000,
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
    /// Fetch a window; main business composition intentionally keeps at most 100 rows.
    /// Other full responses are split before anything is written.
    pub fn fetch_company_window(
        &self,
        api: &str,
        code: &str,
        start: &str,
        end: &str,
        retry_times: usize,
    ) -> Result<TushareTable, String> {
        let (_, _, date_field, limit, _) = COMPANY_DATASETS
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
            } else if api == "fina_mainbz" {
                request["limit"] = json!(limit);
            }
            let mut response = None;
            for attempt in 0..=retry_times {
                lianghua_model::task::checkpoint()?;
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
            let mut table = response.ok_or("公司数据请求没有响应")?;
            if api == "fina_mainbz" {
                // The user prefers bounded composition detail over exhaustive history.
                table.items.truncate(*limit);
            }
            if api != "fina_mainbz" && table.items.len() >= *limit {
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

pub fn company_report_start(end: NaiveDate, quarters: usize) -> Result<NaiveDate, String> {
    if quarters == 0 {
        return Err("基本面下载季度数必须大于 0".into());
    }
    let current_quarter = NaiveDate::from_ymd_opt(end.year(), end.month0() / 3 * 3 + 1, 1)
        .expect("quarter start must be a valid date");
    let months_back = u32::try_from(quarters.checked_mul(3).ok_or("下载季度数过大")?)
        .map_err(|_| "下载季度数过大")?;
    current_quarter
        .checked_sub_months(Months::new(months_back))
        .ok_or_else(|| "下载季度数超出日期范围".into())
}

pub fn download_company(
    config: &CompanyDownloadConfig,
    progress_cb: Option<&DownloadProgressCallback<'_>>,
) -> Result<CompanyDownloadSummary, String> {
    let today = Local::now().date_naive();
    let end = if config.end_date == "today" {
        today
    } else {
        NaiveDate::parse_from_str(&config.end_date, "%Y%m%d")
            .map_err(|e| format!("结束日期无效: {e}"))?
    };
    let start = if config.database == CompanyDbKind::Fundamentals {
        company_report_start(end, config.refresh_quarters)?
    } else {
        NaiveDate::parse_from_str(&config.start_date, "%Y%m%d")
            .map_err(|e| format!("开始日期无效: {e}"))?
    };
    let report_end = NaiveDate::from_ymd_opt(end.year(), end.month0() / 3 * 3 + 1, 1)
        .unwrap()
        .pred_opt()
        .ok_or("日期超出范围")?;
    if start > end {
        return Err("开始日期不能晚于结束日期".into());
    }
    let stocks = load_stock_list(&config.source_dir)?
        .into_iter()
        .filter_map(|r| r.first().filter(|s| !s.is_empty()).cloned())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    if stocks.is_empty() {
        return Err("股票列表为空，请先刷新基础数据".into());
    }
    let client = TushareClient::new(config.token.clone(), config.limit_calls_per_min)?;
    let controls = lianghua_model::task::current_controls();
    let pool = ThreadPoolBuilder::new()
        .spawn_handler(move |worker| {
            let controls = controls.clone();
            std::thread::Builder::new().spawn(move || {
                let _active = lianghua_model::task::ActiveTask::enter_isolated(controls);
                worker.run();
            })?;
            Ok(())
        })
        .num_threads(config.threads.max(1))
        .build()
        .map_err(|e| format!("创建公司数据下载线程池失败: {e}"))?;
    let mut conn = open_company_db(&config.source_dir, config.database)?;
    let settings = BulkWriteSettings::apply(&conn)?;
    let result = (|| {
        let mut writer = CompanyDataWriter::new(&mut conn);
        let mut summary = CompanyDownloadSummary::default();
        let total = stocks.len();
        let mut failed_stocks = HashSet::new();
        let datasets = COMPANY_DATASETS
            .iter()
            .filter(|dataset| dataset.4 == config.database)
            .collect::<Vec<_>>();
        let first = start.format("%Y%m%d").to_string();
        let last_report = report_end.format("%Y%m%d").to_string();
        let mut disabled_apis = HashSet::new();
        let mut finished = 0;
        for batch in stocks.chunks(pool.current_num_threads()) {
            lianghua_model::task::checkpoint()?;
            if let Some(cb) = progress_cb {
                cb(DownloadProgress {
                    phase: if config.database == CompanyDbKind::Fundamentals {
                        "company_fundamentals".into()
                    } else {
                        "company_stk_holdernumber".into()
                    },
                    finished,
                    total,
                    current_label: Some(format!("{} 等 {} 只股票", batch[0], batch.len())),
                    message: format!(
                        "本次范围 {} ～ {}，正在并发拉取本批 {} 只股票的各项数据，等待合并提交",
                        first,
                        end.format("%Y%m%d"),
                        batch.len()
                    ),
                });
            }
            let mut tasks = Vec::new();
            for code in batch {
                for &&(api, label, date_field, _, _) in &datasets {
                    if disabled_apis.contains(api) {
                        continue;
                    }
                    tasks.push((
                        api,
                        label,
                        date_field,
                        code,
                        vec![(
                            first.clone(),
                            if config.database == CompanyDbKind::Fundamentals
                                && date_field == "end_date"
                            {
                                last_report.clone()
                            } else {
                                end.format("%Y%m%d").to_string()
                            },
                        )],
                    ));
                }
            }
            let fetched = pool.install(|| {
                tasks
                    .par_iter()
                    .map(|(api, _, date_field, code, ranges)| {
                        let mut windows = Vec::new();
                        for (from, to) in ranges {
                            let mut table = client.fetch_company_window(
                                api,
                                code,
                                from,
                                to,
                                config.retry_times,
                            )?;
                            // Announcement-date queries may return revisions for older reports.
                            // Keep exactly the selected completed report quarters.
                            if config.database == CompanyDbKind::Fundamentals {
                                if let Some(index) =
                                    table.fields.iter().position(|field| field == "end_date")
                                {
                                    table.items.retain(|row| {
                                        row[index].as_str().is_some_and(|period| {
                                            period >= first.as_str()
                                                && period <= last_report.as_str()
                                        })
                                    });
                                }
                            }
                            windows.push(CompanyDownloadWindow {
                                api: (*api).into(),
                                code: (*code).clone(),
                                date_field: (*date_field).into(),
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
            let mut downloaded = Vec::new();
            for ((api, label, _, code, _), result) in tasks.iter().zip(fetched) {
                match result {
                    Ok(windows) => downloaded.extend(windows),
                    Err(error) => {
                        if error.contains("token") || error.contains("Token") {
                            return Err(error);
                        }
                        if error.contains("code=40203")
                            || error.contains("没有权限")
                            || error.contains("访问权限")
                        {
                            disabled_apis.insert(*api);
                            failed_stocks.extend(stocks.iter().cloned());
                        }
                        failed_stocks.insert((*code).clone());
                        summary
                            .failed_items
                            .push(format!("{code} {label}: {error}"));
                    }
                }
            }
            // One writer commits all successful stock/API windows and their logs together.
            writer.replace_batch(&downloaded)?;
            summary.saved_rows += downloaded.iter().map(|w| w.items.len()).sum::<usize>();
            finished += batch.len();
            if let Some(cb) = progress_cb {
                cb(DownloadProgress {
                    phase: if config.database == CompanyDbKind::Fundamentals {
                        "company_fundamentals".into()
                    } else {
                        "company_stk_holdernumber".into()
                    },
                    finished,
                    total,
                    current_label: None,
                    message: format!(
                        "本批各项成功数据已合并提交，已处理 {finished} / {total} 只股票，失败 {} 只",
                        failed_stocks.len()
                    ),
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
    };

    #[test]
    fn company_mainbz_accepts_row_cap_while_other_apis_split() {
        for (api, limit, requests) in [("fina_mainbz", 100, 1), ("income", 500, 3)] {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let server = std::thread::spawn(move || {
                for index in 0..requests {
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
                    assert_eq!(request["api_name"], api);
                    if api == "fina_mainbz" {
                        assert_eq!(request["params"]["limit"], 100);
                    }
                    let from = request["params"]["start_date"].as_str().unwrap();
                    let items = if index == 0 {
                        vec![json!(["600519.SH", from, 1.0]); limit]
                    } else {
                        vec![json!(["600519.SH", from, 2.0])]
                    };
                    let body = json!({"code":0,"data":{"fields":["ts_code",if api == "income" { "ann_date" } else { "end_date" },"bz_sales"],"items":items}}).to_string();
                    write!(stream, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body).unwrap();
                }
            });
            let mut client = TushareClient::new("mock".into(), 1500).unwrap();
            client.base_url = format!("http://{address}");
            let table = client
                .fetch_company_window(api, "600519.SH", "20240101", "20241231", 0)
                .unwrap();
            assert_eq!(
                table.items.len(),
                if api == "fina_mainbz" { 100 } else { 2 }
            );
            let budgets = client.limiter.next_allowed_at.lock().unwrap();
            assert_eq!(budgets.len(), 1);
            assert!(budgets.contains_key(api));
            server.join().unwrap();
        }
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
        assert_eq!(resumed.skipped_count, 0);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn company_download_range_counts_completed_report_quarters() {
        for (end, quarters, expected) in [
            ((2026, 11, 8), 1, (2026, 7, 1)),
            ((2026, 11, 8), 4, (2025, 10, 1)),
            ((2026, 11, 8), 16, (2022, 10, 1)),
            ((2026, 1, 1), 1, (2025, 10, 1)),
        ] {
            assert_eq!(
                company_report_start(
                    NaiveDate::from_ymd_opt(end.0, end.1, end.2).unwrap(),
                    quarters
                )
                .unwrap(),
                NaiveDate::from_ymd_opt(expected.0, expected.1, expected.2).unwrap()
            );
        }
        assert!(company_report_start(NaiveDate::from_ymd_opt(2026, 11, 8).unwrap(), 0).is_err());
    }
}
