use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;

use chrono::{Local, Timelike};
use duckdb::{AccessMode, Config, Connection};
use serde::{Deserialize, Serialize};

use crate::data::download_state::{load_listing_history_gaps, load_pending_stock_repairs};
use crate::data::{load_stock_list, load_trade_date_list, source_db_path};
use lianghua_download::download::runner::INDEX_TS_CODES;

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DatabaseIssue {
    pub id: String,
    pub check_id: String,
    pub database: String,
    pub severity: String,
    pub title: String,
    pub comparisons: Vec<DatabaseComparison>,
    pub ts_code: Option<String>,
    pub stock_name: Option<String>,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub repair_action: Option<String>,
    pub blocks_derived: bool,
}

#[derive(Clone, Serialize)]
pub struct DatabaseComparison {
    pub label: String,
    pub expected: String,
    pub actual: String,
}

#[derive(Serialize)]
pub struct DatabaseCheck {
    pub id: String,
    pub title: String,
    pub status: String,
    pub summary: String,
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
    pub checks: Vec<DatabaseCheck>,
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
            "历史起点按 max(stock_list.csv 上市日，全库历史起点) 对照，仅作提示；停牌、恢复上市也可能导致起点偏晚。".to_string(),
            "下载已检查增量价格衔接并持久登记补救计划；计算事务保证结果整体提交，不重复检查价格衔接、重复行、排名及结果表间完整性，也不复用正式算法抽样重算。".to_string(),
            "只提示影响已有结果的待重算区间；缺失结果和纯新增日期允许存在。策略、指标配置及成分资料变化仍由相应计算入口处理。".to_string(),
            "检测只读，不证明全库数值正确；文件读取失败单独报告，不自动删除或覆盖数据库。".to_string(),
        ],
    };
    let stocks = load_stock_list(source_path)?;
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
    let source_file = source_db_path(source_path);
    let source = open_check_connection(&source_file)?;
    source
        .execute_batch("BEGIN TRANSACTION")
        .map_err(|e| format!("创建只读检查快照失败: {e}"))?;
    let mut stmt = source.prepare("SELECT ts_code, MIN(trade_date), MAX(trade_date), COUNT(*) FROM stock_data WHERE adj_type = 'qfq' GROUP BY ts_code").map_err(|e| format!("读取行情范围失败: {e}"))?;
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
    report.checks.push(DatabaseCheck {
        id: "source".to_string(),
        title: "前复权行情可用性".to_string(),
        status: if report.row_count > 0 {
            "passed"
        } else {
            "error"
        }
        .to_string(),
        summary: if report.row_count > 0 {
            "已有行情可读取"
        } else {
            "没有前复权行情，请先下载"
        }
        .to_string(),
    });
    for gap in load_listing_history_gaps(source_path, &source)? {
        let mut issue = stock_issue(
            &gap.ts_code,
            &names,
            "truncated",
            "行情历史起点待核验",
            vec![DatabaseComparison {
                label: "行情起点".to_string(),
                expected: gap.start_date.clone(),
                actual: ranges
                    .get(&gap.ts_code)
                    .map(|range| range.0.clone())
                    .unwrap_or_else(|| "无".to_string()),
            }],
            Some(gap.start_date),
            end.clone(),
        );
        issue.severity = "warning".to_string();
        issue.blocks_derived = false;
        report.issues.push(issue);
    }
    for pending in load_pending_stock_repairs(&source)? {
        let mut issue = stock_issue(
            &pending.ts_code,
            &names,
            "pending-stock",
            "行情修复尚未完成",
            vec![DatabaseComparison {
                label: "行情修复状态".to_string(),
                expected: "完成".to_string(),
                actual: "未完成".to_string(),
            }],
            Some(pending.start_date),
            Some(pending.end_date),
        );
        issue.check_id = "truncated".to_string();
        report.issues.push(issue);
    }
    report.checks.push(DatabaseCheck {
        id: "truncated".to_string(),
        title: "历史起点与未完成修复".to_string(),
        status: "passed".to_string(),
        summary: format!("{} 只股票", report.stock_count),
    });
    for code in INDEX_TS_CODES.into_iter().filter(|_| last.is_some()) {
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
        let missing_dates = calendar
            .iter()
            .filter(|date| {
                let date = *date;
                range.0.as_ref().is_some_and(|first| date >= first)
                    && last.as_ref().is_some_and(|last| date <= last)
                    && !saved.contains(date)
            })
            .cloned()
            .collect::<Vec<_>>();
        if range.1.is_none()
            || range
                .1
                .as_ref()
                .zip(last.as_ref())
                .is_some_and(|(index, source)| index < source)
            || !missing_dates.is_empty()
        {
            let mut comparisons = Vec::new();
            if range.1.is_none()
                || range
                    .1
                    .as_ref()
                    .zip(last.as_ref())
                    .is_some_and(|(index, source)| index < source)
            {
                comparisons.push(DatabaseComparison {
                    label: "最新日期".to_string(),
                    expected: last.clone().unwrap_or_else(|| "无".to_string()),
                    actual: range.1.clone().unwrap_or_else(|| "无".to_string()),
                });
            }
            if !missing_dates.is_empty() {
                comparisons.push(DatabaseComparison {
                    label: "缺失交易日".to_string(),
                    expected: "0 天".to_string(),
                    actual: format!("{} 天（{}）", missing_dates.len(), missing_dates.join("、")),
                });
            }
            report.issues.push(DatabaseIssue {
                id: format!("index:{code}"),
                check_id: "index".to_string(),
                database: "stock_data.db / ind".to_string(),
                severity: "warning".to_string(),
                title: "指数行情缺失或落后".to_string(),
                comparisons,
                ts_code: Some(code.to_string()),
                stock_name: None,
                start_date: first.clone(),
                end_date: end.clone(),
                repair_action: Some("index".to_string()),
                blocks_derived: false,
            });
        }
    }
    report.checks.push(DatabaseCheck {
        id: "index".to_string(),
        title: "指数日期与缺口".to_string(),
        status: if calendar.is_empty() || last.is_none() {
            "incomplete"
        } else {
            "passed"
        }
        .to_string(),
        summary: if calendar.is_empty() {
            "缺少交易日历，无法核验交易日缺口；请刷新基础资料".to_string()
        } else if last.is_none() {
            "没有前复权行情，无法确定指数覆盖范围".to_string()
        } else {
            format!("{} 个指数，按已保存行情范围核验", INDEX_TS_CODES.len())
        },
    });
    // 跨库提交无法原子清账：结果提交后中断仍可能留下欠账，按已有结果判断。
    // 每个结果库独立报告读取失败，避免一个坏文件吞掉其他项目。
    for (component, file, table, label) in [
        (
            "concept",
            "concept_performance.db",
            "concept_performance",
            "概念／行业表现",
        ),
        (
            "ranking",
            "scoring_result.db",
            "score_summary",
            "评分／排名",
        ),
        (
            "similarity",
            "strategy_similarity.db",
            "strategy_trigger_similarity_rank",
            "走势相似度",
        ),
    ] {
        let check_id = format!("maintenance:{component}");
        let mut check = DatabaseCheck {
            id: check_id.clone(),
            title: format!("{label}读取与重算状态"),
            status: "skipped".to_string(),
            summary: "数据库未生成".to_string(),
        };
        let path = Path::new(source_path).join(file);
        if !path.exists() {
            report.checks.push(check);
            continue;
        }
        let inspected = (|| -> Result<(), String> {
            let results = open_check_connection(&path)?;
            results
                .execute_batch("BEGIN TRANSACTION")
                .map_err(|e| e.to_string())?;
            let exists: bool = results
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name=?)",
                    [table],
                    |row| row.get(0),
                )
                .map_err(|e| e.to_string())?;
            if !exists {
                check.summary = "结果表未生成".to_string();
                return Ok(());
            }
            // 读取日期以确认已有结果可访问，不检查由计算事务保证的行数和表间关系。
            let saved_range: (Option<String>, Option<String>) = results
                .query_row(
                    &format!("SELECT MIN(trade_date), MAX(trade_date) FROM {table}"),
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .map_err(|e| e.to_string())?;
            if saved_range.0.is_none() {
                check.summary = "暂无结果数据".to_string();
                return Ok(());
            }
            check.status = "passed".to_string();
            check.summary = "已有结果可读取，无受影响的重算欠账".to_string();
            let ranges = crate::data::download_state::load_pending_derived_rebuild_ranges(
                source_path,
                component,
            )?;
            for item in ranges {
                let affects_saved: bool = results
                    .query_row(
                        &format!(
                            "SELECT EXISTS(SELECT 1 FROM {table} WHERE trade_date BETWEEN ? AND ?)"
                        ),
                        [&item.start_date, &item.end_date],
                        |row| row.get(0),
                    )
                    .map_err(|e| e.to_string())?;
                if !affects_saved {
                    continue;
                }
                check.summary = "已有结果受行情变更影响，待重算".to_string();
                report.issues.push(DatabaseIssue {
                    id: format!("pending-derived:{component}:{}", item.start_date),
                    check_id: check_id.clone(),
                    database: file.to_string(),
                    severity: "warning".to_string(),
                    title: format!("行情变更后{label}待重算"),
                    comparisons: vec![DatabaseComparison {
                        label: "后续计算状态".to_string(),
                        expected: if component == "concept" {
                            "重算受影响区间的概念／行业表现；再次更新行情也会补做未完成的重算"
                        } else if component == "ranking" {
                            "在排名计算页覆盖待重算区间"
                        } else {
                            "先重算评分，再在相似榜计算页按日重算需要使用的历史结果"
                        }
                        .to_string(),
                        actual: format!(
                            "{} 至 {} 尚未完成；只计算最新一天不会清除其他历史欠账",
                            item.start_date, item.end_date
                        ),
                    }],
                    ts_code: None,
                    stock_name: None,
                    start_date: Some(item.start_date),
                    end_date: Some(item.end_date),
                    repair_action: (component != "similarity").then(|| component.to_string()),
                    blocks_derived: false,
                });
            }
            Ok(())
        })();
        if let Err(error) = inspected {
            check.status = "incomplete".to_string();
            check.summary = "读取失败，请查看问题详情".to_string();
            report.issues.push(DatabaseIssue {
                id: format!("unreadable:{component}"),
                check_id,
                database: file.to_string(),
                severity: "error".to_string(),
                title: format!("{label}读取失败"),
                comparisons: vec![DatabaseComparison {
                    label: "数据库读取".to_string(),
                    expected: "可读取已有结果及重算状态".to_string(),
                    actual: error,
                }],
                ts_code: None,
                stock_name: None,
                start_date: None,
                end_date: None,
                repair_action: None,
                blocks_derived: false,
            });
        }
        report.checks.push(check);
    }
    for check in &mut report.checks {
        let issues = report
            .issues
            .iter()
            .filter(|issue| issue.check_id == check.id)
            .collect::<Vec<_>>();
        if check.status == "passed" && !issues.is_empty() {
            check.status = if issues.iter().any(|issue| issue.blocks_derived) {
                "error"
            } else {
                "warning"
            }
            .to_string();
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
        .threads(4)
        .map_err(|e| e.to_string())?
        .max_memory("2GB")
        .map_err(|e| e.to_string())?;
    Connection::open_with_flags(path, config)
        .map_err(|e| format!("只读打开 {} 失败: {e}", path.display()))
}

fn stock_issue(
    code: &str,
    names: &HashMap<String, String>,
    kind: &str,
    title: &str,
    comparisons: Vec<DatabaseComparison>,
    start_date: Option<String>,
    end_date: Option<String>,
) -> DatabaseIssue {
    DatabaseIssue {
        id: format!("{kind}:{code}"),
        check_id: kind.to_string(),
        database: "stock_data.db / qfq".to_string(),
        severity: "error".to_string(),
        title: title.to_string(),
        comparisons,
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
    if !matches!(action, "stock" | "index") && !report.source_ready {
        return Err("原始行情检查未通过，禁止修复后续数据库；请先处理所有行情异常。".to_string());
    }
    let mut summary = DownloadSummary::default();
    let mut details = Vec::new();
    match action {
        "stock" | "index" => {
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
                let code = issue
                    .ts_code
                    .clone()
                    .ok_or_else(|| "缺少受影响代码".to_string())?;
                plans
                    .entry(code)
                    .and_modify(|range| {
                        range.0 = range.0.clone().min(start.clone());
                        range.1 = range.1.clone().max(end.clone());
                    })
                    .or_insert((start.clone(), end.clone()));
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

    #[test]
    fn database_check_reports_only_uncovered_risks_and_isolates_unreadable_results() {
        let directory = std::env::temp_dir().join(format!(
            "lianghua-integrity-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&directory).unwrap();
        let path = directory.to_str().unwrap();
        std::fs::write(directory.join("stock_list.csv"), "ts_code,symbol,name,area,industry,list_date\n689009.SH,689009,九号公司,,,20201029\n000002.SZ,2,B,,,20000101\n000003.SZ,3,C,,,20201029\n").unwrap();
        std::fs::write(
            directory.join("trade_calendar.csv"),
            "cal_date\n20201029\n20260806\n20260807\n",
        )
        .unwrap();
        crate::data::download_data::init_stock_data_db(
            directory.join("stock_data.db").to_str().unwrap(),
        )
        .unwrap();
        let source = Connection::open(directory.join("stock_data.db")).unwrap();
        source.execute_batch("INSERT INTO stock_data(ts_code, trade_date, adj_type, close, pre_close) VALUES ('689009.SH','20201029','qfq',41.36,41.36), ('689009.SH','20260806','qfq',41.36,41.36), ('689009.SH','20260807','qfq',40.09,41.38), ('000002.SZ','20260806','qfq',10,10)").unwrap();
        drop(source);
        // 已有但不可读取的新筹码库也不进入校验。
        std::fs::write(directory.join("cyq_chen.db"), b"not a database").unwrap();
        let before = std::fs::read(directory.join("stock_data.db")).unwrap();
        let report = check_databases(path).unwrap();
        assert!(report.source_ready);
        crate::data::download_state::ensure_stock_data_ready_for_derived(path).unwrap();
        assert!(
            !report
                .checks
                .iter()
                .any(|check| matches!(check.id.as_str(), "chip" | "legacy-chip" | "ranking"))
        );
        assert!(
            !report
                .issues
                .iter()
                .any(|issue| matches!(issue.check_id.as_str(), "chip" | "legacy-chip" | "ranking"))
        );
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.id == "truncated:000002.SZ"
                    && issue.start_date.as_deref() == Some("20201029")
                    && issue.check_id == "truncated"
                    && issue.comparisons[0].expected == "20201029"
                    && issue.comparisons[0].actual == "20260806"
                    && !issue.blocks_derived)
        );
        assert!(
            report
                .checks
                .iter()
                .any(|check| check.id == "truncated" && check.status == "warning")
        );
        assert!(
            report
                .checks
                .iter()
                .any(|check| check.id == "maintenance:concept" && check.status == "skipped")
        );
        assert!(
            !report
                .checks
                .iter()
                .any(|check| matches!(check.id.as_str(), "price-link" | "concept" | "similarity"))
        );
        assert!(
            !report
                .issues
                .iter()
                .any(|issue| issue.id == "truncated:689009.SH"
                    || issue.ts_code.as_deref() == Some("000003.SZ")
                    || issue.id.starts_with("invalid-bars")
                    || issue.id == "market-gaps")
        );
        assert_eq!(
            before,
            std::fs::read(directory.join("stock_data.db")).unwrap()
        );
        let source = Connection::open(directory.join("stock_data.db")).unwrap();
        source.execute_batch("INSERT INTO stock_data(ts_code, trade_date, adj_type, close, pre_close) VALUES ('000002.SZ','20201029','qfq',10,10)").unwrap();
        drop(source);
        assert!(check_databases(path).unwrap().source_ready);
        crate::data::download_state::ensure_stock_data_ready_for_derived(path).unwrap();
        let source = Connection::open(directory.join("stock_data.db")).unwrap();
        source.execute_batch("CREATE TABLE pending_stock_repair(ts_code VARCHAR PRIMARY KEY, start_date VARCHAR, end_date VARCHAR); INSERT INTO pending_stock_repair VALUES ('000002.SZ','20201029','20260807')").unwrap();
        drop(source);
        let report = check_databases(path).unwrap();
        assert!(!report.source_ready);
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.id == "pending-stock:000002.SZ" && issue.blocks_derived)
        );
        assert!(crate::data::download_state::ensure_stock_data_ready_for_derived(path).is_err());
        let source = Connection::open(directory.join("stock_data.db")).unwrap();
        source
            .execute_batch("DELETE FROM pending_stock_repair")
            .unwrap();
        drop(source);
        // 首次检查不改变文件，也不创建未生成的可选库。
        std::fs::write(directory.join("stock_list.csv"), "ts_code,symbol,name,area,industry,list_date,trade_date,total_share,float_share,total_mv\n689009.SH,689009,九号公司,,X,20201029,20260807,1,1,100\n000002.SZ,2,B,,X,20201029,20260807,1,1,100\n").unwrap();
        let source = Connection::open(directory.join("stock_data.db")).unwrap();
        source
            .execute_batch("UPDATE stock_data SET pct_chg=2, amount=10000, tor=1")
            .unwrap();
        drop(source);
        let derived = Connection::open(directory.join("concept_performance.db")).unwrap();
        derived.execute_batch("CREATE TABLE concept_performance(trade_date VARCHAR, performance_type VARCHAR, concept VARCHAR, performance_pct DOUBLE); INSERT INTO concept_performance VALUES ('20260806','industry','X',2)").unwrap();
        drop(derived);
        // 原算法重算同一输入不是独立校验；不再对数值差异抽检。
        let derived = Connection::open(directory.join("concept_performance.db")).unwrap();
        derived
            .execute_batch("UPDATE concept_performance SET performance_pct=999")
            .unwrap();
        drop(derived);
        assert!(
            check_databases(path)
                .unwrap()
                .checks
                .iter()
                .any(|check| check.id == "maintenance:concept" && check.status == "passed")
        );
        let derived_before = std::fs::read(directory.join("concept_performance.db")).unwrap();
        let before = std::fs::read(directory.join("stock_data.db")).unwrap();
        check_databases(path).unwrap();
        assert_eq!(
            before,
            std::fs::read(directory.join("stock_data.db")).unwrap()
        );
        assert_eq!(
            derived_before,
            std::fs::read(directory.join("concept_performance.db")).unwrap()
        );
        assert!(!directory.join("cyq.db").exists());
        let results = Connection::open(directory.join("scoring_result.db")).unwrap();
        results
            .execute_batch(
                "CREATE TABLE score_summary(trade_date VARCHAR);
            INSERT INTO score_summary VALUES ('20260806'),('20260807')",
            )
            .unwrap();
        drop(results);
        let source = Connection::open(directory.join("stock_data.db")).unwrap();
        source.execute_batch("CREATE TABLE pending_derived_rebuild(component VARCHAR PRIMARY KEY, start_date VARCHAR, end_date VARCHAR);
            INSERT INTO pending_derived_rebuild VALUES ('ranking','20260801','20260809'), ('concept','20260807','20260807')").unwrap();
        drop(source);
        let before = std::fs::read(directory.join("stock_data.db")).unwrap();
        let report = check_databases(path).unwrap();
        assert!(report.source_ready);
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.id == "pending-derived:ranking:20260806"
                    && issue.repair_action.as_deref() == Some("ranking")
                    && !issue.blocks_derived
                    && issue.end_date.as_deref() == Some("20260807"))
        );
        assert_eq!(
            before,
            std::fs::read(directory.join("stock_data.db")).unwrap()
        );
        assert!(
            !report
                .issues
                .iter()
                .any(|issue| issue.id.starts_with("pending-derived:concept:"))
        );
        // 单个结果库损坏不能让行情状态和其他组件欠账消失。
        std::fs::write(directory.join("concept_performance.db"), b"not a database").unwrap();
        let report = check_databases(path).unwrap();
        assert!(report.source_ready);
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.id == "unreadable:concept"
                    && issue.repair_action.is_none()
                    && issue.comparisons[0].actual.contains("失败"))
        );
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.id == "pending-derived:ranking:20260806")
        );
        std::fs::remove_dir_all(directory).unwrap();
    }
}
