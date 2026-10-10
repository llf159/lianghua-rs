use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;

use chrono::{Local, Timelike};
use duckdb::{AccessMode, Config, Connection};
use serde::{Deserialize, Serialize};

use crate::data::download_state::{
    load_duplicate_stock_rows, load_invalid_stock_rows, load_missing_market_dates,
    load_pending_derived_rebuilds, load_qfq_price_link_issues, load_unfinished_stock_repairs,
};
use crate::data::{load_stock_list, load_trade_date_list, source_db_path};
use lianghua_download::download::runner::INDEX_TS_CODES;

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DatabaseIssue {
    pub id: String,
    pub database: String,
    pub severity: String,
    pub title: String,
    pub detail: String,
    pub ts_code: Option<String>,
    pub stock_name: Option<String>,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub repair_action: Option<String>,
    pub blocks_derived: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DatabaseCheckReport {
    pub source_path: String,
    pub checked_at: String,
    pub source_ready: bool,
    pub target_trade_date: Option<String>,
    pub stock_count: usize,
    pub row_count: u64,
    pub checks: Vec<String>,
    pub issues: Vec<DatabaseIssue>,
    pub limitations: Vec<String>,
}

// 检查与修复前重新检查共用该入口。所有连接只读，扫描不初始化缺失数据库。
pub fn check_databases(source_path: &str) -> Result<DatabaseCheckReport, String> {
    let source_path = source_path.trim();
    if source_path.is_empty() {
        return Err("数据目录为空".to_string());
    }
    let mut report = DatabaseCheckReport {
        source_path: source_path.to_string(), checked_at: Local::now().to_rfc3339(),
        source_ready: false, target_trade_date: None, stock_count: 0, row_count: 0,
        checks: Vec::new(), issues: Vec::new(),
        limitations: vec![
            "本地检查不能证明所有历史行情及复权因子与上游一致；未逐股核对停牌记录，逐股日期较旧不直接判为缺失。".to_string(),
            "复权衔接检查覆盖最近 60 个已保存交易日；历史截短通过待补救记录和评分/筹码历史证据检查。".to_string(),
            "评分、筹码的起始日期允许因预热期晚于行情；未生成的可选结果库不视为行情损坏。".to_string(),
        ],
    };
    let stocks = match load_stock_list(source_path) {
        Ok(stocks) if !stocks.is_empty() => stocks,
        result => {
            report.issues.push(DatabaseIssue {
                id: "stock-list".to_string(),
                database: "stock_list.csv".to_string(),
                severity: "error".to_string(),
                title: "股票列表不可用，无法核对股票覆盖".to_string(),
                detail: result
                    .err()
                    .unwrap_or_else(|| "股票列表为空，请先刷新基础数据。".to_string()),
                ts_code: None,
                stock_name: None,
                start_date: None,
                end_date: None,
                repair_action: None,
                blocks_derived: true,
            });
            Vec::new()
        }
    };
    let names = stocks
        .iter()
        .filter_map(|row| Some((row.first()?.clone(), row.get(2)?.clone())))
        .collect::<HashMap<_, _>>();
    let calendar = load_trade_date_list(source_path).unwrap_or_default();
    let now = Local::now();
    let today = now.format("%Y%m%d").to_string();
    let target = calendar
        .iter()
        .filter(|date| {
            date.as_str() < today.as_str() || (date.as_str() == today.as_str() && now.hour() >= 16)
        })
        .max()
        .cloned();
    report.target_trade_date = target.clone();
    if calendar.is_empty() {
        report.issues.push(DatabaseIssue {
            id: "calendar".to_string(),
            database: "trade_calendar.csv".to_string(),
            severity: "error".to_string(),
            title: "交易日历不可用".to_string(),
            detail: "无法判断整日缺失及最新有效交易日，请先刷新基础数据。".to_string(),
            ts_code: None,
            stock_name: None,
            start_date: None,
            end_date: None,
            repair_action: None,
            blocks_derived: true,
        });
    }
    let source_file = source_db_path(source_path);
    if !source_file.exists() {
        report.issues.push(DatabaseIssue {
            id: "source-missing".to_string(),
            database: "stock_data.db".to_string(),
            severity: "error".to_string(),
            title: "原始行情库不存在".to_string(),
            detail: "请先完成首次行情下载。".to_string(),
            ts_code: None,
            stock_name: None,
            start_date: None,
            end_date: None,
            repair_action: None,
            blocks_derived: true,
        });
        return Ok(report);
    }
    let source = match open_check_connection(&source_file) {
        Ok(source) => source,
        Err(error) => {
            report.issues.push(DatabaseIssue {
                id: "source-unreadable".to_string(),
                database: "stock_data.db".to_string(),
                severity: "error".to_string(),
                title: "原始行情库无法读取".to_string(),
                detail: format!("{error}；先停止其他任务后重试。文件损坏需从备份恢复或重新下载。"),
                ts_code: None,
                stock_name: None,
                start_date: None,
                end_date: None,
                repair_action: None,
                blocks_derived: true,
            });
            return Ok(report);
        }
    };
    source
        .execute_batch("BEGIN TRANSACTION")
        .map_err(|e| format!("创建只读检查快照失败: {e}"))?;
    let mut stmt = match source.prepare("SELECT ts_code, MIN(trade_date), MAX(trade_date), COUNT(*) FROM stock_data WHERE adj_type = 'qfq' GROUP BY ts_code") {
        Ok(stmt) => stmt,
        Err(error) => {
            report.issues.push(DatabaseIssue { id: "source-schema".to_string(), database: "stock_data.db".to_string(), severity: "error".to_string(),
                title: "行情表结构不完整或无法读取".to_string(), detail: error.to_string(), ts_code: None, stock_name: None,
                start_date: None, end_date: None, repair_action: None, blocks_derived: true });
            return Ok(report);
        }
    };
    let ranges = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                (
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, u64>(3)?,
                ),
            ))
        })
        .map_err(|e| format!("扫描行情覆盖失败: {e}"))?
        .collect::<Result<HashMap<_, _>, _>>()
        .map_err(|e| format!("读取行情覆盖失败: {e}"))?;
    drop(stmt);
    report.stock_count = ranges.len();
    report.row_count = ranges.values().map(|(_, _, count)| count).sum();
    let first = ranges.values().map(|(first, _, _)| first).min().cloned();
    let last = ranges.values().map(|(_, last, _)| last).max().cloned();
    let end = last.clone().into_iter().chain(target.clone()).max();
    report.checks.push(format!(
        "原始行情可读性与覆盖：{} 只股票、{} 行",
        report.stock_count, report.row_count
    ));
    for row in &stocks {
        let Some(code) = row.first() else {
            continue;
        };
        if row
            .get(5)
            .zip(end.as_ref())
            .is_some_and(|(listed, end)| listed > end)
        {
            continue;
        }
        if !ranges.contains_key(code) {
            report.issues.push(stock_issue(
                code,
                &names,
                "missing-stock",
                "股票行情完全缺失",
                "股票已上市但原始库没有该股票的 qfq 行情。",
                first
                    .clone()
                    .or(Some("20100101".to_string()))
                    .map(|date| date.max(row.get(5).cloned().unwrap_or_default())),
                end.clone(),
            ));
        }
    }
    if report.row_count == 0 {
        report.issues.push(DatabaseIssue {
            id: "source-empty".to_string(),
            database: "stock_data.db".to_string(),
            severity: "error".to_string(),
            title: "qfq 行情为空".to_string(),
            detail: "请先下载指定历史区间。".to_string(),
            ts_code: None,
            stock_name: None,
            start_date: None,
            end_date: None,
            repair_action: None,
            blocks_derived: true,
        });
    }
    let pending = load_unfinished_stock_repairs(source_path, &source)?;
    for item in &pending {
        report.issues.push(stock_issue(
            &item.ts_code,
            &names,
            "unfinished-stock",
            "历史补救未完成或被截短",
            &format!(
                "需恢复 {} 至 {} 的完整历史，不能仅看最新日期。",
                item.start_date, item.end_date
            ),
            Some(item.start_date.clone()),
            Some(item.end_date.clone()),
        ));
    }
    report
        .checks
        .push("持久补救记录和旧版筹码补救证据".to_string());
    for (kind, title, result) in [
        (
            "invalid-bars",
            "无效价格、日期及成交数据",
            load_invalid_stock_rows(&source),
        ),
        (
            "duplicate-bars",
            "股票交易日重复检查",
            load_duplicate_stock_rows(&source),
        ),
    ] {
        match result {
            Ok(rows) => {
                report.checks.push(title.to_string());
                for (code, date, count) in rows {
                    report.issues.push(stock_issue(&code, &names, kind, title, &format!("检测到 {count} 项，最早问题日期 {date}。将整段重拉该股票，以保持统一复权基准。"), first.clone(), end.clone()));
                }
            }
            Err(error) => report.issues.push(DatabaseIssue {
                id: kind.to_string(),
                database: "stock_data.db".to_string(),
                severity: "error".to_string(),
                title: format!("{title}：检查未完成"),
                detail: error,
                ts_code: None,
                stock_name: None,
                start_date: None,
                end_date: None,
                repair_action: None,
                blocks_derived: true,
            }),
        }
    }
    let missing_dates = load_missing_market_dates(source_path, &source)?;
    if !missing_dates.is_empty() {
        report.issues.push(DatabaseIssue {
            id: "market-gaps".to_string(),
            database: "stock_data.db".to_string(),
            severity: "error".to_string(),
            title: "全市场存在整日缺口".to_string(),
            detail: format!(
                "缺 {} 个交易日：{}。为避免混用复权基准，需要重拉全市场历史区间。",
                missing_dates.len(),
                missing_dates
                    .iter()
                    .take(12)
                    .cloned()
                    .collect::<Vec<_>>()
                    .join("、")
            ),
            ts_code: None,
            stock_name: None,
            start_date: first.clone(),
            end_date: end.clone(),
            repair_action: Some("market".to_string()),
            blocks_derived: true,
        });
    }
    report
        .checks
        .push("全市场交易日缺口（不将单股停牌判为缺失）".to_string());
    if last
        .as_ref()
        .zip(target.as_ref())
        .is_some_and(|(last, target)| last < target)
    {
        report.issues.push(DatabaseIssue {
            id: "source-stale".to_string(),
            database: "stock_data.db".to_string(),
            severity: "warning".to_string(),
            title: "行情尚未更新到当前有效交易日".to_string(),
            detail: format!(
                "当前最新 {}，目标 {}。",
                last.as_deref().unwrap_or("无"),
                target.as_deref().unwrap_or("无")
            ),
            ts_code: None,
            stock_name: None,
            start_date: first.clone(),
            end_date: end.clone(),
            repair_action: Some("update".to_string()),
            blocks_derived: false,
        });
    }
    let links = match load_qfq_price_link_issues(&source) {
        Ok(links) => links,
        Err(error) => {
            report.issues.push(DatabaseIssue {
                id: "price-link-unavailable".to_string(),
                database: "stock_data.db".to_string(),
                severity: "error".to_string(),
                title: "复权衔接检查未完成".to_string(),
                detail: error,
                ts_code: None,
                stock_name: None,
                start_date: None,
                end_date: None,
                repair_action: None,
                blocks_derived: true,
            });
            Vec::new()
        }
    };
    for (code, date, count) in links {
        let mut issue = stock_issue(
            &code,
            &names,
            "price-link",
            "复权衔接待核验",
            &format!(
                "近 60 个交易日有 {count} 处前收盘与上一条收盘不一致，最早 {date}。停牌或上游修订也可能造成此现象，重拉后再检查。"
            ),
            first.clone(),
            end.clone(),
        );
        issue.severity = "warning".to_string();
        report.issues.push(issue);
    }
    report
        .checks
        .push("最近 60 个交易日复权价格衔接".to_string());
    let dirty = load_pending_derived_rebuilds(&source)?;
    for code in INDEX_TS_CODES {
        let range: (Option<String>, Option<String>) = source.query_row("SELECT MIN(trade_date), MAX(trade_date) FROM stock_data WHERE adj_type='ind' AND ts_code=?", [code], |row| Ok((row.get(0)?, row.get(1)?))).map_err(|e| e.to_string())?;
        let mut stmt = source
            .prepare(
                "SELECT DISTINCT trade_date FROM stock_data WHERE adj_type='ind' AND ts_code=?",
            )
            .map_err(|e| e.to_string())?;
        let saved = stmt
            .query_map([code], |row| row.get::<_, String>(0))
            .map_err(|e| e.to_string())?
            .collect::<Result<HashSet<_>, _>>()
            .map_err(|e| e.to_string())?;
        let gap = calendar.iter().any(|date| {
            range.0.as_ref().is_some_and(|first| date >= first)
                && last.as_ref().is_some_and(|last| date <= last)
                && !saved.contains(date)
        });
        if range.1.is_none()
            || range
                .1
                .as_ref()
                .zip(last.as_ref())
                .is_some_and(|(index, source)| index < source)
            || gap
        {
            report.issues.push(DatabaseIssue {
                id: format!("index:{code}"),
                database: "stock_data.db / ind".to_string(),
                severity: "warning".to_string(),
                title: "指数行情缺失或落后".to_string(),
                detail: format!(
                    "指数最新日期 {}；将按该指数重拉保存区间，避免被其他指数的最大日期掩盖。",
                    range.1.as_deref().unwrap_or("无")
                ),
                ts_code: Some(code.to_string()),
                stock_name: None,
                start_date: first.clone(),
                end_date: end.clone(),
                repair_action: Some("index".to_string()),
                blocks_derived: false,
            });
        }
    }
    report
        .checks
        .push("7 个内置指数分别检查日期和缺口".to_string());
    // 旧库没有补救记录时，派生库中的更早历史仍是原始历史被截短的证据。
    for (file, table, component, label) in [
        ("scoring_result.db", "score_summary", "ranking", "评分/排名"),
        (
            "concept_performance.db",
            "concept_performance",
            "concept",
            "概念/行业表现",
        ),
        ("cyq_chen.db", "cyq_chen_snapshot", "chip", "新筹码"),
        ("cyq.db", "cyq_snapshot", "legacy-chip", "旧筹码"),
        (
            "scoring_result.db",
            "strategy_trigger_similarity_rank",
            "similarity",
            "走势相似度",
        ),
    ] {
        let path = Path::new(source_path).join(file);
        if !path.exists() {
            report.checks.push(format!("{label}：尚未生成"));
            if let Some(required) = dirty.iter().find(|item| item.component == component) {
                report.issues.push(DatabaseIssue {
                    id: format!("derived:{component}"),
                    database: file.to_string(),
                    severity: "warning".to_string(),
                    title: format!("{label}需要生成"),
                    detail: "行情修复已完成，需要生成相应后续结果。".to_string(),
                    ts_code: None,
                    stock_name: None,
                    start_date: Some(required.start_date.clone()),
                    end_date: Some(required.end_date.clone()),
                    repair_action: Some(component.to_string()),
                    blocks_derived: false,
                });
            }
            continue;
        }
        let inspected = (|| -> Result<(), String> {
            let conn = open_check_connection(&path)?;
            let exists: bool = conn
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name=?)",
                    [table],
                    |row| row.get(0),
                )
                .map_err(|e| e.to_string())?;
            if !exists {
                report.checks.push(format!("{label}：尚未生成数据表"));
                if let Some(required) = dirty.iter().find(|item| item.component == component) {
                    report.issues.push(DatabaseIssue {
                        id: format!("derived:{component}"),
                        database: file.to_string(),
                        severity: "warning".to_string(),
                        title: format!("{label}需要生成"),
                        detail: "行情修复已完成，需要生成相应后续结果。".to_string(),
                        ts_code: None,
                        stock_name: None,
                        start_date: Some(required.start_date.clone()),
                        end_date: Some(required.end_date.clone()),
                        repair_action: Some(component.to_string()),
                        blocks_derived: false,
                    });
                }
                return Ok(());
            }
            let latest: Option<String> = conn
                .query_row(&format!("SELECT MAX(trade_date) FROM {table}"), [], |row| {
                    row.get(0)
                })
                .map_err(|e| e.to_string())?;
            report.checks.push(format!(
                "{label}可读性与日期：{}",
                latest.as_deref().unwrap_or("空")
            ));
            if component != "concept" && component != "similarity" {
                let mut stmt = conn
                    .prepare(&format!(
                        "SELECT ts_code, MIN(trade_date) FROM {table} GROUP BY ts_code"
                    ))
                    .map_err(|e| e.to_string())?;
                let earlier = stmt
                    .query_map([], |row| {
                        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                    })
                    .map_err(|e| e.to_string())?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| e.to_string())?;
                for (code, history) in earlier {
                    if ranges
                        .get(&code)
                        .is_some_and(|(first, _, _)| first > &history)
                        && names.contains_key(&code)
                    {
                        let id = format!("truncated:{code}");
                        if !report.issues.iter().any(|issue| {
                            issue.id == id || issue.id == format!("unfinished-stock:{code}")
                        }) {
                            report.issues.push(stock_issue(
                                &code,
                                &names,
                                "truncated",
                                "派生库证明原始历史被截短",
                                &format!(
                                    "{label}保留 {history} 的历史，而当前行情从 {} 才开始。",
                                    ranges[&code].0
                                ),
                                first
                                    .clone()
                                    .map(|first| first.min(history.clone()))
                                    .or(Some(history)),
                                end.clone(),
                            ));
                        }
                    }
                }
            }
            let needs_rebuild = dirty.iter().find(|item| item.component == component);
            let chip_pending = if component == "chip" || component == "legacy-chip" {
                crate::data::download_state::load_pending_chip_repairs_for_model(
                    &source,
                    if component == "chip" {
                        "chen"
                    } else {
                        "legacy"
                    },
                )?
            } else {
                Vec::new()
            };
            let pending_chips = !chip_pending.is_empty();
            let raw_problem = report.issues.iter().any(|issue| issue.blocks_derived);
            let stale = latest
                .as_ref()
                .zip(last.as_ref())
                .is_some_and(|(derived, source)| derived < source);
            if needs_rebuild.is_some() || pending_chips || raw_problem || stale || latest.is_none()
            {
                let detail = if let Some(required) = needs_rebuild {
                    format!(
                        "原始行情已改变，需重算 {} 至 {}。即使最大日期相同，旧结果仍不能视为已修复。",
                        required.start_date, required.end_date
                    )
                } else if pending_chips {
                    format!(
                        "{} 只股票有筹码待修复记录，先修复原始行情，再局部重建筹码及维护其余增量。",
                        chip_pending.len()
                    )
                } else if raw_problem {
                    "原始行情尚有异常，当前结果不能认证；原始行情修复后再重建。".to_string()
                } else {
                    format!(
                        "当前最新 {}，行情最新 {}，需补算或重建。",
                        latest.as_deref().unwrap_or("无"),
                        last.as_deref().unwrap_or("无")
                    )
                };
                report.issues.push(DatabaseIssue {
                    id: format!("derived:{component}"),
                    database: file.to_string(),
                    severity: "warning".to_string(),
                    title: format!("{label}需要修复或更新"),
                    detail,
                    ts_code: None,
                    stock_name: None,
                    start_date: first
                        .clone()
                        .into_iter()
                        .chain(needs_rebuild.map(|item| item.start_date.clone()))
                        .min(),
                    end_date: last.clone(),
                    repair_action: Some(component.to_string()),
                    blocks_derived: false,
                });
            }
            Ok(())
        })();
        if let Err(error) = inspected {
            report.issues.push(DatabaseIssue {
                id: format!("unreadable:{component}"),
                database: file.to_string(),
                severity: "error".to_string(),
                title: format!("{label}检查失败"),
                detail: format!("{error}；停止其他任务后重试，损坏文件应从备份恢复。"),
                ts_code: None,
                stock_name: None,
                start_date: None,
                end_date: None,
                repair_action: None,
                blocks_derived: false,
            });
        }
    }
    report.source_ready =
        report.row_count > 0 && !report.issues.iter().any(|issue| issue.blocks_derived);
    report.issues.sort_by(|a, b| {
        b.blocks_derived
            .cmp(&a.blocks_derived)
            .then(a.id.cmp(&b.id))
    });
    Ok(report)
}

fn open_check_connection(path: &Path) -> Result<Connection, String> {
    let config = Config::default()
        .access_mode(AccessMode::ReadOnly)
        .map_err(|e| e.to_string())?
        .threads(2)
        .map_err(|e| e.to_string())?
        .max_memory("512MB")
        .map_err(|e| e.to_string())?;
    Connection::open_with_flags(path, config)
        .map_err(|e| format!("只读打开 {} 失败: {e}", path.display()))
}

fn stock_issue(
    code: &str,
    names: &HashMap<String, String>,
    kind: &str,
    title: &str,
    detail: &str,
    start_date: Option<String>,
    end_date: Option<String>,
) -> DatabaseIssue {
    DatabaseIssue {
        id: format!("{kind}:{code}"),
        database: "stock_data.db / qfq".to_string(),
        severity: "error".to_string(),
        title: title.to_string(),
        detail: detail.to_string(),
        ts_code: Some(code.to_string()),
        stock_name: names.get(code).cloned(),
        start_date,
        end_date,
        repair_action: Some("stock".to_string()),
        blocks_derived: true,
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DatabaseRepairInput {
    pub source_path: String,
    pub issue_ids: Vec<String>,
    pub token: String,
    pub threads: usize,
    pub retry_times: usize,
    pub limit_calls_per_min: usize,
    pub include_turnover: bool,
}

pub fn repair_database_issues(
    input: DatabaseRepairInput,
    progress_cb: Option<&lianghua_download::download::runner::DownloadProgressCallback<'_>>,
) -> Result<crate::download::DataDownloadRunResult, String> {
    use crate::data::download_state::ensure_stock_data_ready_for_derived;
    use lianghua_download::download::runner::{
        DownloadProgress, DownloadRuntimeConfig, download_selected_indices,
        repair_selected_stock_history,
    };
    use lianghua_download::download::{AdjType, DownloadSummary};

    // 不信任前端缓存的日期或股票列表，修复前按当前库重新生成计划。
    let report = check_databases(&input.source_path)?;
    if input.issue_ids.is_empty() {
        return Err("请选择需要修复的问题".to_string());
    }
    let selected = input
        .issue_ids
        .iter()
        .map(|id| {
            report
                .issues
                .iter()
                .find(|issue| &issue.id == id)
                .ok_or_else(|| format!("检查结果已变化，问题 {id} 已不存在，请重新检查。"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let action = selected[0]
        .repair_action
        .as_deref()
        .ok_or_else(|| "该问题不能自动修复，请按检查说明处理。".to_string())?;
    if selected
        .iter()
        .any(|issue| issue.repair_action.as_deref() != Some(action))
    {
        return Err("一次修复只能选择同一种问题类型".to_string());
    }
    if !matches!(action, "stock" | "market" | "index") && !report.source_ready {
        return Err("原始行情检查未通过，禁止修复后续数据库；请先处理所有行情异常。".to_string());
    }
    let mut summary = DownloadSummary::default();
    let mut details = Vec::new();
    match action {
        "stock" | "market" | "index" => {
            if input.token.trim().is_empty() {
                return Err("请先填写 Tushare Token。".to_string());
            }
            let mut plans = BTreeMap::<String, (String, String)>::new();
            for issue in selected {
                let start = issue
                    .start_date
                    .as_ref()
                    .ok_or_else(|| "缺少可用历史起点，请先下载基础行情。".to_string())?;
                let end = issue
                    .end_date
                    .as_ref()
                    .ok_or_else(|| "缺少可用历史终点，请先刷新交易日历。".to_string())?;
                let codes = if action == "market" {
                    load_stock_list(&input.source_path)?
                        .into_iter()
                        .filter(|row| row.get(5).is_none_or(|date| date <= end))
                        .filter_map(|row| row.first().cloned())
                        .collect::<Vec<_>>()
                } else {
                    vec![
                        issue
                            .ts_code
                            .clone()
                            .ok_or_else(|| "缺少受影响代码".to_string())?,
                    ]
                };
                for code in codes {
                    plans
                        .entry(code)
                        .and_modify(|range| {
                            range.0 = range.0.clone().min(start.clone());
                            range.1 = range.1.clone().max(end.clone());
                        })
                        .or_insert((start.clone(), end.clone()));
                }
            }
            let start_date = plans
                .values()
                .map(|range| range.0.clone())
                .min()
                .ok_or_else(|| "没有可修复代码".to_string())?;
            let end_date = plans
                .values()
                .map(|range| range.1.clone())
                .max()
                .ok_or_else(|| "没有可修复代码".to_string())?;
            let codes = plans.into_keys().collect::<Vec<_>>();
            let phase = if action == "index" {
                "repair_index_history"
            } else {
                "repair_stock_history"
            };
            let config = DownloadRuntimeConfig {
                source_dir: input.source_path.clone(),
                adj_type: if action == "index" {
                    AdjType::Ind
                } else {
                    AdjType::Qfq
                },
                token: input.token.trim().to_string(),
                start_date,
                end_date,
                threads: input.threads.max(1),
                retry_times: input.retry_times,
                limit_calls_per_min: input.limit_calls_per_min.max(1),
                include_turnover: input.include_turnover && action != "index",
                allow_stale_stock_list: false,
            };
            let callback = |mut progress: DownloadProgress| {
                if progress.phase == "done" {
                    return;
                }
                if matches!(
                    progress.phase.as_str(),
                    "prepare_trade_calendar" | "prepare_stock_list" | "prepare_index_list"
                ) {
                    progress.finished = 0;
                    progress.total = codes.len();
                }
                progress.phase = phase.to_string();
                if let Some(cb) = progress_cb {
                    cb(progress);
                }
            };
            let batch = if action == "index" {
                download_selected_indices(
                    &config,
                    &codes,
                    &config.start_date,
                    &config.end_date,
                    Some(&callback),
                )?
            } else {
                repair_selected_stock_history(&config, &codes, Some(&callback))?
            };
            if batch.failed_count > 0 || batch.success_count != codes.len() {
                return Err(format!(
                    "仍有代码未成功补回，已停止本次修复；成功提交的数据和未完成记录保留。{}",
                    batch
                        .failed_items
                        .iter()
                        .map(|(code, error)| format!("{code}: {error}"))
                        .collect::<Vec<_>>()
                        .join("；")
                ));
            }
            summary.success_count = batch.success_count;
            summary.saved_rows = batch.saved_rows;
            details.push("仅修复所选行情；行业、筹码、排名须在行情检查通过后分别修复".to_string());
        }
        "concept" => {
            ensure_stock_data_ready_for_derived(&input.source_path)?;
            if let Some(cb) = progress_cb {
                cb(DownloadProgress {
                    phase: "rebuild_concept_performance".to_string(),
                    finished: 0,
                    total: 1,
                    current_label: None,
                    message: "原始行情检查已通过，正在重建概念/行业表现。".to_string(),
                });
            }
            summary.saved_rows =
                crate::data::concept_performance_data::rebuild_concept_performance_all(
                    &input.source_path,
                )?;
            summary.success_count = 1;
        }
        "chip" | "legacy-chip" => {
            ensure_stock_data_ready_for_derived(&input.source_path)?;
            let source = crate::data::download_state::open_source_db_readonly(&input.source_path)?;
            let model = if action == "chip" { "chen" } else { "legacy" };
            let codes =
                crate::data::download_state::load_pending_chip_repairs_for_model(&source, model)?;
            drop(source);
            let callback = |mut progress: DownloadProgress| {
                progress.phase = "maintain_cyq_incremental".to_string();
                // 局部修复与其余增量有不同股票池，不能把局部 100% 当成整库完成。
                progress.finished = 0;
                progress.total = 0;
                if let Some(cb) = progress_cb {
                    cb(progress);
                }
            };
            if action == "chip" {
                if !codes.is_empty() {
                    let repaired = crate::data::cyq_chen_data::repair_cyq_chen_stocks_if_db_exists(
                        &input.source_path,
                        &codes,
                        true,
                        Some(&callback),
                    )?
                    .ok_or_else(|| {
                        "新筹码局部修复未执行，请在计算任务中重建筹码库。".to_string()
                    })?;
                    summary.saved_rows += repaired.snapshot_rows + repaired.bin_rows;
                }
                if let Some(updated) =
                    crate::data::cyq_chen_data::maintain_cyq_chen_incremental_if_db_exists(
                        &input.source_path,
                        true,
                        Some(&callback),
                    )?
                {
                    summary.saved_rows += updated.snapshot_rows + updated.bin_rows;
                }
            } else {
                if !codes.is_empty() {
                    let repaired = crate::data::cyq_data::repair_cyq_stocks_if_db_exists(
                        &input.source_path,
                        &codes,
                        Some(&callback),
                    )?
                    .ok_or_else(|| {
                        "旧筹码局部修复未执行，请在计算任务中重建筹码库。".to_string()
                    })?;
                    summary.saved_rows += repaired.snapshot_rows + repaired.bin_rows;
                }
                if let Some(updated) = crate::data::cyq_data::maintain_cyq_incremental_if_db_exists(
                    &input.source_path,
                )? {
                    summary.saved_rows += updated.snapshot_rows + updated.bin_rows;
                }
            }
            if !codes.is_empty() {
                let source = Connection::open(source_db_path(&input.source_path))
                    .map_err(|e| e.to_string())?;
                crate::data::download_state::clear_completed_chip_repairs(
                    &source,
                    &input.source_path,
                    model,
                    &codes,
                )?;
            }
            summary.success_count = codes.len().max(1);
            details.push("按当前筹码策略修复；策略已变化时执行全量重建".to_string());
        }
        _ => return Err("该结果需使用页面中的评分/相似度计算入口重算。".to_string()),
    }
    Ok(crate::download::DataDownloadRunResult {
        action: "repair-database".to_string(),
        action_label: "数据库针对性修复".to_string(),
        elapsed_ms: 0,
        summary: crate::download::DataDownloadSummary {
            success_count: summary.success_count as u64,
            failed_count: 0,
            saved_rows: summary.saved_rows as u64,
            concept_performance_rows: summary.concept_performance_rows as u64,
            failed_items: Vec::new(),
        },
        completion_details: details,
        status: crate::download::get_data_download_status(&input.source_path)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::download_state::{
        clear_completed_chip_repairs, clear_pending_derived_rebuild,
        load_pending_chip_repairs_for_model, mark_derived_rebuild_required,
    };

    #[test]
    fn database_check_preserves_files_and_tracks_repairs_without_trusting_latest_dates() {
        let directory = std::env::temp_dir().join(format!(
            "lianghua-integrity-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&directory).unwrap();
        let path = directory.to_str().unwrap();
        std::fs::write(directory.join("stock_list.csv"), "ts_code,symbol,name,area,industry,list_date\n000001.SZ,1,A,,,20000101\n000002.SZ,2,B,,,20000101\n000003.SZ,3,C,,,20990101\n000004.SZ,4,D,,,20000101\n").unwrap();
        std::fs::write(
            directory.join("trade_calendar.csv"),
            "cal_date\n20260102\n20260105\n20260106\n",
        )
        .unwrap();
        crate::data::download_data::init_stock_data_db(
            directory.join("stock_data.db").to_str().unwrap(),
        )
        .unwrap();
        let source = Connection::open(directory.join("stock_data.db")).unwrap();
        source.execute_batch("INSERT INTO stock_data(ts_code, trade_date, adj_type, open, high, low, close, pre_close, vol, amount) VALUES
            ('000001.SZ','20260102','qfq',10,10,10,10,10,10,100), ('000001.SZ','20260105','qfq',10,10,10,10,10,10,100), ('000001.SZ','20260106','qfq',10,10,10,10,10,10,100),
            ('000002.SZ','20260105','qfq',10,10,10,10,10,10,100), ('000002.SZ','20260106','qfq',10,10,10,10,10,10,100),
            ('000004.SZ','20260102','qfq',10,10,10,10,10,10,100)").unwrap();
        source
            .execute_batch("UPDATE stock_data SET change=0, pct_chg=0")
            .unwrap();
        drop(source);
        let derived = Connection::open(directory.join("scoring_result.db")).unwrap();
        derived.execute_batch("CREATE TABLE score_summary(ts_code VARCHAR, trade_date VARCHAR); INSERT INTO score_summary VALUES ('000002.SZ','20260102'), ('000002.SZ','20260106')").unwrap();
        drop(derived);
        let before = std::fs::read(directory.join("stock_data.db")).unwrap();
        let report = check_databases(path).unwrap();
        assert!(!report.source_ready);
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.ts_code.as_deref() == Some("000002.SZ") && issue.blocks_derived)
        );
        assert!(
            !report
                .issues
                .iter()
                .any(|issue| issue.ts_code.as_deref() == Some("000003.SZ")
                    || issue.ts_code.as_deref() == Some("000004.SZ"))
        );
        assert_eq!(
            before,
            std::fs::read(directory.join("stock_data.db")).unwrap()
        );
        assert!(!directory.join("cyq.db").exists());
        let source = Connection::open(directory.join("stock_data.db")).unwrap();
        source.execute_batch("INSERT INTO stock_data(ts_code, trade_date, adj_type, open, high, low, close, pre_close, vol, amount) VALUES ('000002.SZ','20260102','qfq',10,10,10,10,10,10,100)").unwrap();
        mark_derived_rebuild_required(&source, "20260102", "20260106").unwrap();
        source
            .execute_batch("UPDATE stock_data SET change=0, pct_chg=0")
            .unwrap();
        drop(source);
        let report = check_databases(path).unwrap();
        assert!(report.source_ready);
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.id == "derived:ranking")
        );
        clear_pending_derived_rebuild(path, "ranking", "20260106", "20260106").unwrap();
        assert!(
            check_databases(path)
                .unwrap()
                .issues
                .iter()
                .any(|issue| issue.id == "derived:ranking")
        );
        clear_pending_derived_rebuild(path, "ranking", "20260102", "20260106").unwrap();
        assert!(
            !check_databases(path)
                .unwrap()
                .issues
                .iter()
                .any(|issue| issue.id == "derived:ranking")
        );
        let source = Connection::open(directory.join("stock_data.db")).unwrap();
        source.execute_batch("CREATE TABLE pending_chip_repair(ts_code VARCHAR PRIMARY KEY); INSERT INTO pending_chip_repair VALUES ('000002.SZ')").unwrap();
        // 两种筹码库的完成状态独立，修复一种不能吞掉另一种的断点。
        std::fs::write(directory.join("cyq.db"), []).unwrap();
        std::fs::write(directory.join("cyq_chen.db"), []).unwrap();
        clear_completed_chip_repairs(&source, path, "chen", &["000002.SZ".to_string()]).unwrap();
        assert!(
            load_pending_chip_repairs_for_model(&source, "chen")
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            load_pending_chip_repairs_for_model(&source, "legacy").unwrap(),
            vec!["000002.SZ"]
        );
        clear_completed_chip_repairs(&source, path, "legacy", &["000002.SZ".to_string()]).unwrap();
        assert!(
            load_pending_chip_repairs_for_model(&source, "legacy")
                .unwrap()
                .is_empty()
        );
        drop(source);
        std::fs::remove_dir_all(directory).unwrap();
    }
}
