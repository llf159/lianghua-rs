use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fs;
use std::io::{BufReader, Read};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::scoring::runner::{ScoringMemoryMode, scoring_all_to_memory_with_mode};
use crate::simulate::comparison::{RankComparisonSummary, compare_rankings_from_score_rows};
use crate::simulate::rank::{RankLayerConfig, RankLayerFromDbInput, RankLayerMethod};

#[derive(Debug, Deserialize, Serialize)]
pub struct RankComparisonRequest {
    pub source_path: String,
    pub candidate_config: String,
    pub stock_adj_type: String,
    pub index_ts_code: String,
    pub index_beta: f64,
    pub concept_beta: f64,
    pub industry_beta: f64,
    pub start_date: String,
    pub end_date: String,
    pub min_listed_trade_days: usize,
    pub backtest_period: usize,
    #[serde(default)]
    pub verify_inputs: bool,
    #[serde(default)]
    pub retain_membership: bool,
    #[serde(default)]
    pub grouping: crate::research_evidence::ResearchGrouping,
    #[serde(default)]
    pub statistics: crate::simulate::research_statistics::ResearchStatisticsSettings,
    #[serde(default)]
    pub task_id: String,
    #[serde(default)]
    pub research_question: String,
    #[serde(default)]
    pub research_notes: serde_json::Value,
    #[serde(default)]
    pub parent_run_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RankComparisonInputDigest {
    pub file: String,
    pub bytes: Option<u64>,
    pub sha256: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct RankComparisonResult {
    pub baseline_config: String,
    pub candidate_config: String,
    pub baseline_rule_names: Vec<String>,
    pub candidate_rule_names: Vec<String>,
    pub baseline_warnings: Vec<String>,
    pub candidate_warnings: Vec<String>,
    pub scoring_start_date: String,
    pub baseline_ranking_days: usize,
    pub candidate_ranking_days: usize,
    pub input_identity: String,
    pub label_algorithm: String,
    pub ranking_algorithm: String,
    pub input_digests: Vec<RankComparisonInputDigest>,
    pub archive_run_id: Option<String>,
    pub archive_error: Option<String>,
    pub membership_saved: bool,
    #[serde(skip)]
    pub membership_staging: Option<std::path::PathBuf>,
    pub summaries: Vec<RankComparisonSummary>,
}

pub fn comparison_input_digests(
    source_path: &str,
    cancelled: &dyn Fn() -> bool,
) -> Result<Vec<RankComparisonInputDigest>, String> {
    let mut out = Vec::new();
    for name in [
        "stock_data.db",
        "stock_data.db.wal",
        "scoring_result.db",
        "scoring_result.db.wal",
        "concept_performance.db",
        "concept_performance.db.wal",
        "strategy_similarity.db",
        "strategy_similarity.db.wal",
        "cyq.db",
        "cyq.db.wal",
        "cyq_chen.db",
        "cyq_chen.db.wal",
        "stock_list.csv",
        "trade_calendar.csv",
        "stock_concepts.csv",
        "score_rule.toml",
        "ind.toml",
        "chip_change_rule.toml",
    ] {
        let path = std::path::Path::new(source_path).join(name);
        let file = match fs::File::open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                out.push(RankComparisonInputDigest {
                    file: name.to_string(),
                    bytes: None,
                    sha256: None,
                });
                continue;
            }
            Err(error) => return Err(format!("读取输入文件 {name} 失败: {error}")),
        };
        let mut reader = BufReader::new(file);
        let mut digest = Sha256::new();
        let mut buffer = vec![0u8; 256 * 1024];
        let mut bytes = 0u64;
        loop {
            if cancelled() {
                return Err("排名对照已取消".to_string());
            }
            let count = reader
                .read(&mut buffer)
                .map_err(|error| format!("校验输入文件 {name} 失败: {error}"))?;
            if count == 0 {
                break;
            }
            digest.update(&buffer[..count]);
            bytes += count as u64;
        }
        out.push(RankComparisonInputDigest {
            file: name.to_string(),
            bytes: Some(bytes),
            sha256: Some(format!("{:x}", digest.finalize())),
        });
    }
    Ok(out)
}

pub fn read_rank_comparison_baseline(source_path: &str) -> Result<String, String> {
    if source_path.trim().is_empty() || !std::path::Path::new(source_path.trim()).is_dir() {
        return Err("数据目录不存在".to_string());
    }
    fs::read_to_string(crate::data::score_rule_path(source_path.trim()))
        .map_err(|e| format!("读取当前策略配置失败: {e}"))
}

pub fn run_rank_comparison(
    request: RankComparisonRequest,
    progress: &dyn Fn(&str) -> Result<(), String>,
    cancelled: &dyn Fn() -> bool,
) -> Result<RankComparisonResult, String> {
    request.grouping.validate()?;
    request.statistics.validate()?;
    let source = request.source_path.trim();
    if source.is_empty() || request.candidate_config.trim().is_empty() {
        return Err("数据目录和候选配置不能为空".to_string());
    }
    if request.start_date.len() != 8
        || request.end_date.len() != 8
        || !request.start_date.bytes().all(|c| c.is_ascii_digit())
        || !request.end_date.bytes().all(|c| c.is_ascii_digit())
        || request.start_date > request.end_date
        || chrono::NaiveDate::parse_from_str(&request.start_date, "%Y%m%d").is_err()
        || chrono::NaiveDate::parse_from_str(&request.end_date, "%Y%m%d").is_err()
        || request.backtest_period == 0
        || request.stock_adj_type.trim().is_empty()
        || request.index_ts_code.trim().is_empty()
        || !request.index_beta.is_finite()
        || !request.concept_beta.is_finite()
        || !request.industry_beta.is_finite()
    {
        return Err("排名对照日期或参数无效".to_string());
    }
    progress(if request.verify_inputs {
        "校验输入文件"
    } else {
        "准备评分配置"
    })?;
    let before = if request.verify_inputs {
        comparison_input_digests(source, cancelled)?
    } else {
        Vec::new()
    };
    let baseline_config = read_rank_comparison_baseline(source)?;
    let mut calendar = crate::data::load_trade_date_list(source)?;
    calendar.sort_unstable();
    calendar.dedup();
    // Trading-calendar gaps do not imply scoring days. Use actual source quote days for warmup.
    let scoring_start_date = {
        let db = crate::data::open_db_readonly(crate::data::source_db_path(source))
            .map_err(|e| e.to_string())?;
        let mut stmt=db.prepare("SELECT DISTINCT trade_date FROM stock_data WHERE adj_type=? AND trade_date<? ORDER BY trade_date DESC LIMIT 2").map_err(|e|e.to_string())?;
        let days = stmt
            .query_map(
                duckdb::params![request.stock_adj_type, request.start_date],
                |row| row.get::<_, String>(0),
            )
            .map_err(|e| e.to_string())?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| e.to_string())?;
        days.last()
            .cloned()
            .unwrap_or_else(|| request.start_date.clone())
    };
    let serial = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| e.to_string())?
        .as_nanos();
    let _archive_access = crate::research_archive::research_archive_lock(source, false)?;
    let archive_root = std::path::Path::new(source).join("research_runs_v1");
    let temporary = archive_root.join(format!(
        ".pending-ranking-{serial:x}-{:x}",
        rand::random::<u64>()
    ));
    fs::create_dir(&temporary).map_err(|e| format!("创建隔离评分配置失败: {e}"))?;
    let membership_staging = archive_root.join(format!(
        ".pending-members-{serial:x}-{:x}.db",
        rand::random::<u64>()
    ));
    let evidence_path = archive_root.join(format!(".pending-evidence-{serial:x}.db"));
    let result = (|| -> Result<RankComparisonResult, String> {
        let baseline_path = temporary.join("baseline.toml");
        let candidate_path = temporary.join("candidate.toml");
        fs::write(&baseline_path, &baseline_config)
            .map_err(|e| format!("写入基线快照失败: {e}"))?;
        fs::write(&candidate_path, &request.candidate_config)
            .map_err(|e| format!("写入候选快照失败: {e}"))?;
        let baseline_path = baseline_path
            .to_str()
            .ok_or_else(|| "基线配置路径不是有效UTF-8".to_string())?;
        let candidate_path = candidate_path
            .to_str()
            .ok_or_else(|| "候选配置路径不是有效UTF-8".to_string())?;
        progress("重算基线评分与跟踪")?;
        let (baseline, baseline_profile) = scoring_all_to_memory_with_mode(
            source,
            Some(baseline_path),
            &request.stock_adj_type,
            &scoring_start_date,
            &request.end_date,
            ScoringMemoryMode::SummaryOnly,
        )?;
        progress("重算候选评分与跟踪")?;
        let (candidate, candidate_profile) = scoring_all_to_memory_with_mode(
            source,
            Some(candidate_path),
            &request.stock_adj_type,
            &scoring_start_date,
            &request.end_date,
            ScoringMemoryMode::SummaryOnly,
        )?;
        let source_db = crate::data::source_db_path(source);
        let source_db = source_db
            .to_str()
            .ok_or_else(|| "原始库路径不是有效UTF-8".to_string())?;
        let conn =
            crate::data::open_db_readonly(source_db).map_err(|e| format!("打开原始库失败: {e}"))?;
        let input = RankLayerFromDbInput {
            stock_adj_type: request.stock_adj_type.clone(),
            index_ts_code: request.index_ts_code.clone(),
            index_beta: request.index_beta,
            concept_beta: request.concept_beta,
            industry_beta: request.industry_beta,
            start_date: request.start_date.clone(),
            end_date: request.end_date.clone(),
            layer_config: RankLayerConfig {
                target: Default::default(),
                min_samples_per_day: 1,
                backtest_period: request.backtest_period,
                min_listed_trade_days: request.min_listed_trade_days,
                layer_count: 5,
                layer_method: RankLayerMethod::Rank,
            },
        };
        progress("计算后验与排名对照")?;
        let mut comparison = compare_rankings_from_score_rows(
            &conn,
            source,
            &input,
            &baseline.summary_rows,
            &candidate.summary_rows,
        )?;
        progress("计算板块、市值和MKT切片")?;
        let evidence = crate::research_evidence::ValidationResearchOutput {
            baseline_diagnostics: true,
            path: evidence_path.clone(),
            grouping: request.grouping.clone(),
            statistics: request.statistics.clone(),
        };
        let codes = baseline
            .summary_rows
            .iter()
            .chain(&candidate.summary_rows)
            .map(|row| row.ts_code.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        evidence.prepare(
            source,
            &request.stock_adj_type,
            &request.start_date,
            &request.end_date,
            request.min_listed_trade_days,
            &codes,
            false,
        )?;
        {
            let db = evidence.open()?;
            let mut dimensions = vec![(
                "board",
                "SELECT trade_date,ts_code,board FROM observations".to_string(),
            )];
            if !request.grouping.market_cap_boundaries.is_empty() {
                dimensions.push((
                    "market_cap",
                    "SELECT trade_date,ts_code,cap_group FROM observations".into(),
                ));
            }
            if !request.grouping.market_metric.is_empty() {
                dimensions.push((request.grouping.market_metric.as_str(),format!("SELECT o.trade_date,o.ts_code,m.bucket FROM observations o JOIN market m USING(trade_date) WHERE m.metric='{}'",request.grouping.market_metric)));
            }
            for (dimension, sql) in dimensions {
                let mut stmt = db.prepare(&sql).map_err(|e| e.to_string())?;
                let mut rows = stmt.query([]).map_err(|e| e.to_string())?;
                let mut groups = std::collections::BTreeMap::<
                    String,
                    std::collections::HashSet<(String, String)>,
                >::new();
                while let Some(row) = rows.next().map_err(|e| e.to_string())? {
                    groups
                        .entry(row.get(2).map_err(|e| e.to_string())?)
                        .or_default()
                        .insert((
                            row.get(0).map_err(|e| e.to_string())?,
                            row.get(1).map_err(|e| e.to_string())?,
                        ));
                }
                for (bucket, members) in groups {
                    progress(&format!("排名切片 {dimension} / {bucket}"))?;
                    let filter = |day: &str, code: &str| {
                        members.contains(&(day.to_string(), code.to_string()))
                    };
                    let mut summaries = crate::simulate::comparison::compare_ranked_samples(
                        &baseline.summary_rows,
                        &candidate.summary_rows,
                        &comparison.labels,
                        &crate::simulate::build_backtest_sample_eligibility(
                            source,
                            request.min_listed_trade_days,
                        )?,
                        &request.start_date,
                        Some(&filter),
                    );
                    for summary in &mut summaries {
                        summary.dimension = dimension.to_string();
                        summary.bucket = bucket.clone();
                    }
                    comparison.summaries.extend(summaries);
                }
            }
        }
        crate::simulate::comparison::complete_rank_comparison_calendar(
            &mut comparison.summaries,
            &calendar,
            &request.start_date,
            &request.end_date,
        );
        let family_size = comparison.summaries.len();
        for summary in &mut comparison.summaries {
            let by_day = summary
                .daily_differences
                .iter()
                .map(|(day, value)| (day.as_str(), *value))
                .collect::<std::collections::HashMap<_, _>>();
            let values = calendar
                .iter()
                .filter(|day| {
                    day.as_str() >= request.start_date.as_str()
                        && day.as_str() <= request.end_date.as_str()
                })
                .filter(|day| {
                    summary.period == "全部"
                        || format!(
                            "{} Q{}",
                            &day[..4],
                            (day[4..6].parse::<usize>().unwrap_or(1) - 1) / 3 + 1
                        ) == summary.period
                })
                .map(|day| by_day.get(day.as_str()).copied().flatten())
                .collect::<Vec<_>>();
            summary.statistics = Some(
                crate::simulate::research_statistics::describe_research_series(
                    &values,
                    request.backtest_period,
                    &request.statistics,
                    family_size,
                )?,
            );
            summary.daily_differences.clear();
        }
        if request.retain_membership {
            progress("保存排名成员与标签")?;
            let eligibility = crate::simulate::build_backtest_sample_eligibility(
                source,
                request.min_listed_trade_days,
            )?;
            {
                let conn = duckdb::Connection::open(&membership_staging)
                    .map_err(|e| format!("创建成员存档失败: {e}"))?;
                conn.execute_batch("CREATE TABLE ranking_members(side VARCHAR NOT NULL, trade_date VARCHAR NOT NULL, ts_code VARCHAR NOT NULL, score DOUBLE NOT NULL, rank BIGINT, eligibility SMALLINT NOT NULL); CREATE TABLE label_windows(ts_code VARCHAR,trade_date VARCHAR,entry_date VARCHAR,label_end_date VARCHAR); CREATE TABLE labels(trade_date VARCHAR NOT NULL, ts_code VARCHAR NOT NULL, residual_return DOUBLE NOT NULL, er_change DOUBLE)")
                .map_err(|e| format!("创建成员存档表失败: {e}"))?;
                {
                    let mut writer = conn
                        .appender("ranking_members")
                        .map_err(|e| format!("准备成员存档失败: {e}"))?;
                    for (side, rows) in [
                        ("baseline", &baseline.summary_rows),
                        ("candidate", &candidate.summary_rows),
                    ] {
                        for row in rows {
                            if cancelled() {
                                return Err("排名对照已取消".to_string());
                            }
                            let status =
                                match eligibility.sample_status(&row.ts_code, &row.trade_date) {
                                    Some(true) => 1i16,
                                    Some(false) => 0i16,
                                    None => -1i16,
                                };
                            writer
                                .append_row(duckdb::params![
                                    side,
                                    row.trade_date,
                                    row.ts_code,
                                    row.total_score,
                                    row.rank,
                                    status
                                ])
                                .map_err(|e| format!("写入排名成员失败: {e}"))?;
                        }
                    }
                    writer
                        .flush()
                        .map_err(|e| format!("刷新排名成员失败: {e}"))?;
                }
                {
                    let mut writer = conn
                        .appender("labels")
                        .map_err(|e| format!("准备标签存档失败: {e}"))?;
                    for row in &comparison.labels {
                        if cancelled() {
                            return Err("排名对照已取消".to_string());
                        }
                        writer
                            .append_row(duckdb::params![
                                row.trade_date,
                                row.ts_code,
                                row.residual_return,
                                row.er_change.is_finite().then_some(row.er_change)
                            ])
                            .map_err(|e| format!("写入后验标签失败: {e}"))?;
                    }
                    writer
                        .flush()
                        .map_err(|e| format!("刷新后验标签失败: {e}"))?;
                }
                {
                    let mut writer = conn.appender("label_windows").map_err(|e| e.to_string())?;
                    for (code, day, entry, end) in &comparison.label_windows {
                        writer
                            .append_row(duckdb::params![
                                code,
                                day,
                                (*entry > 0).then(|| format!("{entry:08}")),
                                (*end > 0).then(|| format!("{end:08}"))
                            ])
                            .map_err(|e| e.to_string())?;
                    }
                    writer.flush().map_err(|e| e.to_string())?;
                }
                let evidence_filename = evidence_path.to_string_lossy().replace('\'', "''");
                conn.execute_batch(&format!("ATTACH '{evidence_filename}' AS evidence (READ_ONLY); CREATE TABLE observations AS SELECT * FROM evidence.observations; CREATE TABLE market AS SELECT * FROM evidence.market; CREATE TABLE research_metadata AS SELECT * FROM evidence.metadata; CREATE TABLE calendar AS SELECT * FROM evidence.calendar; DETACH evidence;")).map_err(|e|e.to_string())?;
                let _ = fs::remove_file(&evidence_path);
                conn.execute_batch("CHECKPOINT")
                    .map_err(|e| format!("写入成员存档失败: {e}"))?;
            }
        }
        if request.verify_inputs {
            progress("复核输入文件")?;
        }
        if request.verify_inputs && comparison_input_digests(source, cancelled)? != before {
            return Err("计算期间输入文件内容发生变化；结果已丢弃，请重新运行".to_string());
        }
        progress("准备结果")?;
        Ok(RankComparisonResult {
            baseline_config,
            candidate_config: request.candidate_config,
            baseline_rule_names: baseline_profile.rule_names,
            candidate_rule_names: candidate_profile.rule_names,
            baseline_warnings: baseline_profile.warnings,
            candidate_warnings: candidate_profile.warnings,
            scoring_start_date,
            baseline_ranking_days: baseline
                .summary_rows
                .iter()
                .filter(|row| row.trade_date >= request.start_date)
                .map(|row| row.trade_date.as_str())
                .collect::<BTreeSet<_>>()
                .len(),
            candidate_ranking_days: candidate
                .summary_rows
                .iter()
                .filter(|row| row.trade_date >= request.start_date)
                .map(|row| row.trade_date.as_str())
                .collect::<BTreeSet<_>>()
                .len(),
            input_identity: if request.verify_inputs {
                "content_checked_not_snapshot"
            } else {
                "unverified"
            }
            .to_string(),
            label_algorithm: "forward_open_compounded_residual_v1".to_string(),
            ranking_algorithm: "native_score_desc_j_asc_nulls_last_code_v1".to_string(),
            input_digests: before,
            archive_run_id: None,
            archive_error: None,
            membership_saved: false,
            membership_staging: request
                .retain_membership
                .then(|| membership_staging.clone()),
            summaries: comparison.summaries,
        })
    })();
    let _ = fs::remove_dir_all(&temporary);
    let _ = fs::remove_file(&evidence_path);
    let _ = fs::remove_file(format!("{}.wal", evidence_path.to_string_lossy()));
    if result.is_err() {
        let _ = fs::remove_file(&membership_staging);
        let _ = fs::remove_file(format!("{}.wal", membership_staging.to_string_lossy()));
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn input_content_check_detects_same_length_change() {
        let root =
            std::env::temp_dir().join(format!("lianghua-input-check-{:x}", rand::random::<u64>()));
        fs::create_dir(&root).unwrap();
        fs::write(root.join("stock_list.csv"), "abcd").unwrap();
        let before = comparison_input_digests(root.to_str().unwrap(), &|| false).unwrap();
        fs::write(root.join("stock_list.csv"), "abce").unwrap();
        let after = comparison_input_digests(root.to_str().unwrap(), &|| false).unwrap();
        assert_ne!(before, after);
        let old = before
            .iter()
            .find(|item| item.file == "stock_list.csv")
            .unwrap();
        let new = after
            .iter()
            .find(|item| item.file == "stock_list.csv")
            .unwrap();
        assert_eq!(old.bytes, new.bytes);
        assert_ne!(old.sha256, new.sha256);
        fs::remove_dir_all(root).unwrap();
    }
}
