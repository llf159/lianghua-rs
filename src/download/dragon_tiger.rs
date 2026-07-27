use std::{collections::HashSet, thread::sleep, time::Duration};

use chrono::Local;

use crate::{
    data::{
        dragon_tiger_data::{
            checkpoint_dragon_tiger, load_synced_dragon_tiger_trade_dates, open_dragon_tiger_db,
            replace_dragon_tiger_trade_date,
        },
        load_trade_date_list,
    },
    download::{
        TopInstRow, TopListRow, TushareClient,
        runner::{DownloadProgress, DownloadProgressCallback},
    },
};

pub const DRAGON_TIGER_FIRST_DATE: &str = "20050101";

#[derive(Debug, Clone)]
pub struct DragonTigerDownloadConfig {
    pub source_dir: String,
    pub token: String,
    pub start_date: String,
    pub end_date: String,
    pub retry_times: usize,
    pub limit_calls_per_min: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DragonTigerDownloadSummary {
    pub planned_trade_dates: usize,
    pub synced_trade_dates: usize,
    pub skipped_trade_dates: usize,
    pub deferred_trade_dates: usize,
    pub top_list_rows: usize,
    pub top_inst_rows: usize,
}

fn resolve_end_date(raw: &str) -> String {
    if raw.eq_ignore_ascii_case("today") {
        Local::now().format("%Y%m%d").to_string()
    } else {
        raw.to_string()
    }
}

fn pending_trade_dates(
    trade_dates: &[String],
    synced_dates: &HashSet<String>,
    start_date: &str,
    end_date: &str,
) -> Vec<String> {
    let effective_start = start_date.max(DRAGON_TIGER_FIRST_DATE);
    trade_dates
        .iter()
        .filter(|date| date.as_str() >= effective_start)
        .filter(|date| date.as_str() <= end_date)
        .filter(|date| !synced_dates.contains(date.as_str()))
        .cloned()
        .collect()
}

fn fetch_with_retries<T, F>(
    trade_date: &str,
    data_label: &str,
    retry_times: usize,
    mut fetch: F,
) -> Result<T, String>
where
    F: FnMut() -> Result<T, String>,
{
    let mut last_error = None;
    for attempt in 0..=retry_times {
        match fetch() {
            Ok(value) => return Ok(value),
            Err(error) => {
                last_error = Some(error);
                if attempt < retry_times {
                    sleep(Duration::from_secs(1));
                }
            }
        }
    }

    Err(format!(
        "交易日 {trade_date} {data_label}下载失败: {}",
        last_error.unwrap_or_else(|| "未知错误".to_string())
    ))
}

fn fetch_trade_date_with_retries(
    client: &TushareClient,
    trade_date: &str,
    retry_times: usize,
) -> Result<(Vec<TopListRow>, Vec<TopInstRow>), String> {
    let top_list_rows =
        fetch_with_retries(trade_date, "龙虎榜每日明细", retry_times, || {
            client.fetch_top_list_by_trade_date(trade_date)
        })?;
    let top_inst_rows =
        fetch_with_retries(trade_date, "龙虎榜席位明细", retry_times, || {
            client.fetch_top_inst_by_trade_date(trade_date)
        })?;
    Ok((top_list_rows, top_inst_rows))
}

pub fn download_dragon_tiger(
    config: &DragonTigerDownloadConfig,
    progress_cb: Option<&DownloadProgressCallback<'_>>,
) -> Result<DragonTigerDownloadSummary, String> {
    let end_date = resolve_end_date(config.end_date.as_str());
    if config.start_date.as_str() > end_date.as_str() {
        return Err("龙虎榜开始日期不能晚于结束日期".to_string());
    }

    let trade_dates = load_trade_date_list(config.source_dir.as_str())?;
    let mut conn = open_dragon_tiger_db(config.source_dir.as_str())?;
    let synced_dates = load_synced_dragon_tiger_trade_dates(&conn)?;
    let pending_dates = pending_trade_dates(
        &trade_dates,
        &synced_dates,
        config.start_date.as_str(),
        end_date.as_str(),
    );
    let in_range_count = trade_dates
        .iter()
        .filter(|date| date.as_str() >= config.start_date.as_str().max(DRAGON_TIGER_FIRST_DATE))
        .filter(|date| date.as_str() <= end_date.as_str())
        .count();
    let mut summary = DragonTigerDownloadSummary {
        planned_trade_dates: pending_dates.len(),
        skipped_trade_dates: in_range_count.saturating_sub(pending_dates.len()),
        ..DragonTigerDownloadSummary::default()
    };

    if pending_dates.is_empty() {
        if let Some(cb) = progress_cb {
            cb(DownloadProgress {
                phase: "dragon_tiger_done".to_string(),
                finished: 0,
                total: 0,
                current_label: None,
                message: "龙虎榜指定区间已经同步，无需重复下载。".to_string(),
            });
        }
        return Ok(summary);
    }

    let client = TushareClient::new(config.token.clone(), config.limit_calls_per_min.max(1))?;
    if let Some(cb) = progress_cb {
        cb(DownloadProgress {
            phase: "download_dragon_tiger".to_string(),
            finished: 0,
            total: pending_dates.len(),
            current_label: pending_dates.first().cloned(),
            message: format!(
                "龙虎榜下载开始，共 {} 个交易日待同步，已跳过 {} 个交易日。",
                pending_dates.len(),
                summary.skipped_trade_dates
            ),
        });
    }

    let today = Local::now().format("%Y%m%d").to_string();
    for (index, trade_date) in pending_dates.iter().enumerate() {
        if let Some(cb) = progress_cb {
            cb(DownloadProgress {
                phase: "download_dragon_tiger".to_string(),
                finished: index,
                total: pending_dates.len(),
                current_label: Some(trade_date.clone()),
                message: format!("正在拉取交易日 {trade_date} 的龙虎榜每日明细和席位明细。"),
            });
        }

        let (top_list_rows, top_inst_rows) =
            fetch_trade_date_with_retries(&client, trade_date, config.retry_times)?;
        if trade_date == &today && top_list_rows.is_empty() && top_inst_rows.is_empty() {
            summary.deferred_trade_dates += 1;
            if let Some(cb) = progress_cb {
                cb(DownloadProgress {
                    phase: "write_dragon_tiger".to_string(),
                    finished: index + 1,
                    total: pending_dates.len(),
                    current_label: Some(trade_date.clone()),
                    message: format!(
                        "交易日 {trade_date} 暂无龙虎榜数据，可能尚未完成晚间更新，本次不标记为已同步。"
                    ),
                });
            }
            continue;
        }

        replace_dragon_tiger_trade_date(&mut conn, trade_date, &top_list_rows, &top_inst_rows)?;
        summary.synced_trade_dates += 1;
        summary.top_list_rows += top_list_rows.len();
        summary.top_inst_rows += top_inst_rows.len();

        if let Some(cb) = progress_cb {
            cb(DownloadProgress {
                phase: "write_dragon_tiger".to_string(),
                finished: index + 1,
                total: pending_dates.len(),
                current_label: Some(trade_date.clone()),
                message: format!(
                    "交易日 {trade_date} 写入每日明细 {} 行、席位明细 {} 行，进度 {}/{}。",
                    top_list_rows.len(),
                    top_inst_rows.len(),
                    index + 1,
                    pending_dates.len()
                ),
            });
        }
    }

    checkpoint_dragon_tiger(&conn)?;
    if let Some(cb) = progress_cb {
        cb(DownloadProgress {
            phase: "dragon_tiger_done".to_string(),
            finished: summary.synced_trade_dates + summary.deferred_trade_dates,
            total: summary.planned_trade_dates,
            current_label: pending_dates.last().cloned(),
            message: format!(
                "龙虎榜下载完成，同步 {} 个交易日，每日明细 {} 行、席位明细 {} 行。",
                summary.synced_trade_dates, summary.top_list_rows, summary.top_inst_rows
            ),
        });
    }
    Ok(summary)
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::*;

    fn temp_source_dir() -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        std::env::temp_dir().join(format!("lianghua_dragon_tiger_download_{nanos}"))
    }

    #[test]
    fn pending_dates_skip_synced_days_and_pre_2005_history() {
        let trade_dates = vec![
            "20041231".to_string(),
            "20050104".to_string(),
            "20050105".to_string(),
        ];
        let synced = HashSet::from(["20050104".to_string()]);
        assert_eq!(
            pending_trade_dates(&trade_dates, &synced, "20000101", "20050105"),
            vec!["20050105".to_string()]
        );
    }

    #[test]
    #[ignore = "requires TUSHARE_TOKEN and network access"]
    fn live_download_writes_both_tables_and_resumes() {
        let token = std::env::var("TUSHARE_TOKEN").expect("TUSHARE_TOKEN is required");
        let source_dir = temp_source_dir();
        fs::create_dir_all(&source_dir).expect("create source dir");
        fs::write(
            source_dir.join("trade_calendar.csv"),
            "cal_date\n20260724\n",
        )
        .expect("write calendar");
        let config = DragonTigerDownloadConfig {
            source_dir: source_dir.to_string_lossy().into_owned(),
            token,
            start_date: "20260724".to_string(),
            end_date: "20260724".to_string(),
            retry_times: 0,
            limit_calls_per_min: 120,
        };

        let first = download_dragon_tiger(&config, None).expect("first download");
        assert_eq!(first.synced_trade_dates, 1);
        assert!(first.top_list_rows > 0);
        assert!(first.top_inst_rows > first.top_list_rows);

        let resumed = download_dragon_tiger(&config, None).expect("resume");
        assert_eq!(resumed.planned_trade_dates, 0);
        assert_eq!(resumed.skipped_trade_dates, 1);

        fs::remove_dir_all(source_dir).ok();
    }
}
