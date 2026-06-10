use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::Arc;

use duckdb::{Connection, params, params_from_iter};
use rayon::prelude::*;
use serde::Serialize;

use crate::data::result_db_path;

use super::{
    build_concepts_map, build_industry_map, build_name_map, normalize_trade_date,
    resolve_trade_date,
};

const DEFAULT_WINDOW_TRADE_DAYS: usize = 20;
const DEFAULT_MAX_GAP_TRADE_DAYS: usize = 5;
const DEFAULT_LIMIT: usize = 30;
const MAX_MATCHED_EVENTS_PER_ROW: usize = 20;
const MATCH_WORK_UNIT_TRADE_DAYS: usize = 60;
const SCORE_EPS: f64 = 1e-12;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StrategyTriggerSimilarityTarget {
    pub ts_code: String,
    pub name: Option<String>,
    pub industry: Option<String>,
    pub concept: Option<String>,
    pub start_trade_date: String,
    pub end_trade_date: String,
    pub trigger_count: usize,
    pub rule_names: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StrategyTriggerSimilarityMatchedEvent {
    pub rule_name: String,
    pub target_trade_date: String,
    pub candidate_trade_date: String,
    pub date_gap_trade_days: usize,
    pub event_score: f64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StrategyTriggerSimilarityRow {
    pub ts_code: String,
    pub name: Option<String>,
    pub industry: Option<String>,
    pub concept: Option<String>,
    pub candidate_start_trade_date: String,
    pub candidate_end_trade_date: String,
    pub similarity_score: f64,
    pub matched_event_count: usize,
    pub target_trigger_count: usize,
    pub candidate_trigger_count: usize,
    pub matched_rule_count: usize,
    pub avg_date_gap_trade_days: Option<f64>,
    pub matched_rule_names: Vec<String>,
    pub matched_events: Vec<StrategyTriggerSimilarityMatchedEvent>,
    pub total_score: Option<f64>,
    pub rank: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StrategyTriggerSimilarityPageData {
    pub resolved_trade_date: String,
    pub resolved_ts_code: String,
    pub window_trade_days: usize,
    pub max_gap_trade_days: usize,
    pub target: StrategyTriggerSimilarityTarget,
    pub items: Vec<StrategyTriggerSimilarityRow>,
}

#[derive(Debug, Clone)]
struct TargetEvent {
    index: usize,
    rule_name: String,
    trade_date: String,
    offset: usize,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct CandidateKey {
    ts_code: Arc<str>,
    window_start_index: usize,
}

#[derive(Debug, Clone)]
struct CandidateMatch {
    target_event_index: usize,
    candidate_trade_index: usize,
    date_gap: usize,
    event_score: f64,
}

#[derive(Debug, Default)]
struct CandidateAccumulator {
    best_by_target_event: HashMap<usize, CandidateMatch>,
}

#[derive(Debug, Clone)]
struct MatchWorkUnit {
    rule_name: Arc<str>,
    target_event_indices: Arc<[usize]>,
    start_trade_date: String,
    end_trade_date: String,
}

#[derive(Debug)]
struct SummaryRow {
    total_score: Option<f64>,
    rank: Option<i64>,
}

#[derive(Debug, Default)]
struct CandidateTriggerCounter {
    compact_counts: HashMap<String, (Vec<usize>, Vec<usize>)>,
}

#[derive(Debug)]
struct WorstFirstSimilarityRow(StrategyTriggerSimilarityRow);

impl PartialEq for WorstFirstSimilarityRow {
    fn eq(&self, other: &Self) -> bool {
        compare_similarity_rows(&self.0, &other.0) == Ordering::Equal
    }
}

impl Eq for WorstFirstSimilarityRow {}

impl PartialOrd for WorstFirstSimilarityRow {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for WorstFirstSimilarityRow {
    fn cmp(&self, other: &Self) -> Ordering {
        compare_similarity_rows(&self.0, &other.0)
    }
}

fn normalize_ts_code(ts_code: &str) -> String {
    let normalized = ts_code.trim().to_ascii_uppercase();
    if normalized.contains('.') {
        return normalized;
    }

    if normalized.starts_with("30") || normalized.starts_with("00") {
        format!("{normalized}.SZ")
    } else if normalized.starts_with("60") || normalized.starts_with("68") {
        format!("{normalized}.SH")
    } else {
        format!("{normalized}.BJ")
    }
}

fn open_result_conn(source_path: &str) -> Result<Connection, String> {
    let result_db = result_db_path(source_path);
    let result_db_str = result_db
        .to_str()
        .ok_or_else(|| "结果库路径不是有效UTF-8".to_string())?;
    Connection::open(result_db_str).map_err(|e| format!("打开结果库失败: {e}"))
}

fn resolve_existing_trade_date(
    conn: &Connection,
    trade_date: Option<String>,
) -> Result<String, String> {
    let requested = match trade_date.as_deref().and_then(normalize_trade_date) {
        Some(normalized) => Some(normalized),
        None => trade_date,
    };
    let requested = resolve_trade_date(conn, requested)?;

    let mut stmt = conn
        .prepare(
            r#"
            SELECT MAX(trade_date)
            FROM score_summary
            WHERE trade_date <= ?
            "#,
        )
        .map_err(|e| format!("预编译触发相似交易日解析失败: {e}"))?;
    let mut rows = stmt
        .query(params![requested])
        .map_err(|e| format!("查询触发相似交易日失败: {e}"))?;
    let Some(row) = rows
        .next()
        .map_err(|e| format!("读取触发相似交易日失败: {e}"))?
    else {
        return Err("score_summary 没有可用交易日".to_string());
    };
    let resolved: Option<String> = row
        .get(0)
        .map_err(|e| format!("读取触发相似交易日字段失败: {e}"))?;
    resolved.ok_or_else(|| format!("未找到不晚于 {requested} 的评分交易日"))
}

fn load_all_trade_dates(conn: &Connection) -> Result<Vec<String>, String> {
    let mut stmt = conn
        .prepare(
            r#"
            SELECT DISTINCT trade_date
            FROM score_summary
            ORDER BY trade_date ASC
            "#,
        )
        .map_err(|e| format!("预编译触发相似交易日列表失败: {e}"))?;
    let mut rows = stmt
        .query([])
        .map_err(|e| format!("查询触发相似交易日列表失败: {e}"))?;

    let mut dates = Vec::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取触发相似交易日列表失败: {e}"))?
    {
        let trade_date: String = row
            .get(0)
            .map_err(|e| format!("读取触发相似交易日字段失败: {e}"))?;
        dates.push(trade_date);
    }

    if dates.is_empty() {
        return Err("score_summary 没有可用交易日".to_string());
    }

    Ok(dates)
}

fn target_window_bounds(
    all_trade_dates: &[String],
    end_trade_date: &str,
    window_trade_days: usize,
) -> Result<(usize, usize), String> {
    let end_index = all_trade_dates
        .binary_search_by(|trade_date| trade_date.as_str().cmp(end_trade_date))
        .map_err(|_| format!("交易日不在 score_summary 中: {end_trade_date}"))?;
    let keep = window_trade_days.max(1);
    let start_index = (end_index + 1).saturating_sub(keep);
    Ok((start_index, end_index))
}

fn load_target_events(
    conn: &Connection,
    ts_code: &str,
    start_trade_date: &str,
    end_trade_date: &str,
    target_start_index: usize,
    trade_date_to_index: &HashMap<&str, usize>,
) -> Result<Vec<TargetEvent>, String> {
    let mut stmt = conn
        .prepare(
            r#"
            SELECT rule_name, trade_date
            FROM rule_details
            WHERE ts_code = ?
              AND trade_date >= ?
              AND trade_date <= ?
              AND TRY_CAST(rule_score AS DOUBLE) IS NOT NULL
              AND ABS(TRY_CAST(rule_score AS DOUBLE)) > ?
            ORDER BY trade_date ASC, rule_name ASC
            "#,
        )
        .map_err(|e| format!("预编译目标策略触发查询失败: {e}"))?;
    let mut rows = stmt
        .query(params![
            ts_code,
            start_trade_date,
            end_trade_date,
            SCORE_EPS
        ])
        .map_err(|e| format!("查询目标策略触发失败: {e}"))?;

    let mut out = Vec::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取目标策略触发失败: {e}"))?
    {
        let rule_name: String = row.get(0).map_err(|e| format!("读取规则名失败: {e}"))?;
        let trade_date: String = row.get(1).map_err(|e| format!("读取交易日失败: {e}"))?;
        let Some(trade_index) = trade_date_to_index.get(trade_date.as_str()).copied() else {
            continue;
        };
        out.push(TargetEvent {
            index: out.len(),
            rule_name,
            trade_date,
            offset: trade_index.saturating_sub(target_start_index),
        });
    }

    Ok(out)
}

fn build_target_rule_names(target_events: &[TargetEvent]) -> Vec<String> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    for event in target_events {
        if seen.insert(event.rule_name.clone()) {
            out.push(event.rule_name.clone());
        }
    }
    out
}

fn linear_event_score(date_gap: usize, max_gap_trade_days: usize) -> f64 {
    if date_gap > max_gap_trade_days {
        return 0.0;
    }
    (max_gap_trade_days + 1 - date_gap) as f64 / (max_gap_trade_days + 1) as f64
}

fn insert_best_match(
    accumulators: &mut HashMap<CandidateKey, CandidateAccumulator>,
    candidate_key: CandidateKey,
    candidate_match: CandidateMatch,
) {
    let entry = accumulators.entry(candidate_key).or_default();
    let replace = match entry
        .best_by_target_event
        .get(&candidate_match.target_event_index)
    {
        Some(existing) => {
            candidate_match
                .event_score
                .total_cmp(&existing.event_score)
                .is_gt()
                || (candidate_match.event_score == existing.event_score
                    && candidate_match.date_gap < existing.date_gap)
                || (candidate_match.event_score == existing.event_score
                    && candidate_match.date_gap == existing.date_gap
                    && candidate_match.candidate_trade_index < existing.candidate_trade_index)
        }
        None => true,
    };

    if replace {
        entry
            .best_by_target_event
            .insert(candidate_match.target_event_index, candidate_match);
    }
}

fn build_match_work_units(
    events_by_rule: HashMap<String, Vec<usize>>,
    all_trade_dates: &[String],
    window_trade_days: usize,
    max_gap_trade_days: usize,
) -> Vec<MatchWorkUnit> {
    let chunk_trade_days =
        MATCH_WORK_UNIT_TRADE_DAYS.max(window_trade_days + max_gap_trade_days * 2 + 1);
    let mut event_groups = events_by_rule.into_iter().collect::<Vec<_>>();
    event_groups.sort_by(|left, right| left.0.cmp(&right.0));

    let mut work_units = Vec::new();
    for (rule_name, target_event_indices) in event_groups {
        let rule_name = Arc::<str>::from(rule_name);
        let target_event_indices = Arc::<[usize]>::from(target_event_indices);
        let mut start_index = 0;
        while start_index < all_trade_dates.len() {
            let end_exclusive = usize::min(start_index + chunk_trade_days, all_trade_dates.len());
            let Some(start_trade_date) = all_trade_dates.get(start_index).cloned() else {
                break;
            };
            let Some(end_trade_date) = all_trade_dates.get(end_exclusive - 1).cloned() else {
                break;
            };
            work_units.push(MatchWorkUnit {
                rule_name: Arc::clone(&rule_name),
                target_event_indices: Arc::clone(&target_event_indices),
                start_trade_date,
                end_trade_date,
            });
            start_index = end_exclusive;
        }
    }

    work_units
}

fn load_candidate_matches(
    source_path: &str,
    target_events: &[TargetEvent],
    all_trade_dates: &[String],
    trade_date_to_index: &HashMap<&str, usize>,
    window_trade_days: usize,
    target_ts_code: &str,
    max_gap_trade_days: usize,
) -> Result<HashMap<CandidateKey, CandidateAccumulator>, String> {
    if target_events.is_empty() || all_trade_dates.is_empty() || window_trade_days == 0 {
        return Ok(HashMap::new());
    }
    if all_trade_dates.len() < window_trade_days {
        return Ok(HashMap::new());
    }

    let mut events_by_rule = HashMap::<String, Vec<usize>>::new();
    for event in target_events {
        events_by_rule
            .entry(event.rule_name.clone())
            .or_default()
            .push(event.index);
    }
    let work_units = build_match_work_units(
        events_by_rule,
        all_trade_dates,
        window_trade_days,
        max_gap_trade_days,
    );

    work_units
        .par_iter()
        .map(|work_unit| {
            load_candidate_matches_for_work_unit(
                source_path,
                work_unit,
                target_events,
                all_trade_dates.len(),
                trade_date_to_index,
                window_trade_days,
                target_ts_code,
                max_gap_trade_days,
            )
        })
        .try_reduce(
            HashMap::<CandidateKey, CandidateAccumulator>::new,
            |mut left, right| {
                merge_candidate_accumulators(&mut left, right);
                Ok(left)
            },
        )
}

fn load_candidate_matches_for_work_unit(
    source_path: &str,
    work_unit: &MatchWorkUnit,
    target_events: &[TargetEvent],
    trade_date_count: usize,
    trade_date_to_index: &HashMap<&str, usize>,
    window_trade_days: usize,
    target_ts_code: &str,
    max_gap_trade_days: usize,
) -> Result<HashMap<CandidateKey, CandidateAccumulator>, String> {
    let conn = open_result_conn(source_path)?;
    let last_window_start = trade_date_count - window_trade_days;
    let mut stmt = conn
        .prepare(
            r#"
            SELECT ts_code, trade_date
            FROM rule_details
            WHERE rule_name = ?
              AND ts_code <> ?
              AND trade_date >= ?
              AND trade_date <= ?
              AND TRY_CAST(rule_score AS DOUBLE) IS NOT NULL
              AND ABS(TRY_CAST(rule_score AS DOUBLE)) > ?
            "#,
        )
        .map_err(|e| format!("预编译策略触发相似候选查询失败: {e}"))?;
    let mut rows = stmt
        .query(params![
            work_unit.rule_name.as_ref(),
            target_ts_code,
            work_unit.start_trade_date,
            work_unit.end_trade_date,
            SCORE_EPS
        ])
        .map_err(|e| format!("查询策略触发相似候选失败: {e}"))?;

    let mut out = HashMap::<CandidateKey, CandidateAccumulator>::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取策略触发相似候选失败: {e}"))?
    {
        let candidate_ts_code: String = row.get(0).map_err(|e| format!("读取代码失败: {e}"))?;
        let candidate_ts_code = Arc::<str>::from(candidate_ts_code);
        let candidate_trade_date: String =
            row.get(1).map_err(|e| format!("读取候选交易日失败: {e}"))?;
        let Some(candidate_trade_index) = trade_date_to_index
            .get(candidate_trade_date.as_str())
            .copied()
        else {
            continue;
        };

        for &target_event_index in work_unit.target_event_indices.iter() {
            let Some(target_event) = target_events.get(target_event_index) else {
                continue;
            };
            let base_start = candidate_trade_index as isize - target_event.offset as isize;
            let max_start_raw = base_start + max_gap_trade_days as isize;
            if max_start_raw < 0 {
                continue;
            }
            let min_start = (base_start - max_gap_trade_days as isize).max(0) as usize;
            let max_start = max_start_raw.min(last_window_start as isize) as usize;
            if min_start > max_start {
                continue;
            }

            for window_start_index in min_start..=max_start {
                let candidate_offset = candidate_trade_index.saturating_sub(window_start_index);
                if candidate_offset >= window_trade_days {
                    continue;
                }
                let date_gap = candidate_offset.abs_diff(target_event.offset);
                let event_score = linear_event_score(date_gap, max_gap_trade_days);
                if event_score <= 0.0 {
                    continue;
                }

                insert_best_match(
                    &mut out,
                    CandidateKey {
                        ts_code: Arc::clone(&candidate_ts_code),
                        window_start_index,
                    },
                    CandidateMatch {
                        target_event_index,
                        candidate_trade_index,
                        date_gap,
                        event_score,
                    },
                );
            }
        }
    }

    Ok(out)
}

fn merge_candidate_accumulators(
    out: &mut HashMap<CandidateKey, CandidateAccumulator>,
    partial: HashMap<CandidateKey, CandidateAccumulator>,
) {
    for (candidate_key, accumulator) in partial {
        for candidate_match in accumulator.best_by_target_event.into_values() {
            insert_best_match(out, candidate_key.clone(), candidate_match);
        }
    }
}

impl CandidateTriggerCounter {
    fn count_for(
        &self,
        key: &CandidateKey,
        trade_date_count: usize,
        window_trade_days: usize,
    ) -> usize {
        let window_end_exclusive =
            usize::min(key.window_start_index + window_trade_days, trade_date_count);
        self.compact_counts
            .get(key.ts_code.as_ref())
            .map(|(trade_indices, prefix)| {
                let start_pos = trade_indices
                    .partition_point(|trade_index| *trade_index < key.window_start_index);
                let end_pos = trade_indices
                    .partition_point(|trade_index| *trade_index < window_end_exclusive);
                prefix
                    .get(end_pos)
                    .copied()
                    .unwrap_or(0)
                    .saturating_sub(prefix.get(start_pos).copied().unwrap_or(0))
            })
            .unwrap_or(0)
    }
}

fn load_candidate_trigger_counter<'a, I>(
    conn: &Connection,
    candidate_keys: I,
    trade_date_to_index: &HashMap<&str, usize>,
) -> Result<CandidateTriggerCounter, String>
where
    I: IntoIterator<Item = &'a CandidateKey>,
{
    let mut candidate_codes = candidate_keys
        .into_iter()
        .map(|key| Arc::clone(&key.ts_code))
        .collect::<Vec<_>>();
    candidate_codes.sort_by(|left, right| left.as_ref().cmp(right.as_ref()));
    candidate_codes.dedup_by(|left, right| left.as_ref() == right.as_ref());
    if candidate_codes.is_empty() {
        return Ok(CandidateTriggerCounter::default());
    }

    let placeholders = std::iter::repeat_n("?", candidate_codes.len())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        r#"
        SELECT ts_code, trade_date, COUNT(*) AS trigger_count
        FROM rule_details
        WHERE ts_code IN ({placeholders})
          AND TRY_CAST(rule_score AS DOUBLE) IS NOT NULL
          AND ABS(TRY_CAST(rule_score AS DOUBLE)) > {SCORE_EPS}
        GROUP BY ts_code, trade_date
        "#
    );

    let query_params = candidate_codes
        .iter()
        .map(|code| code.as_ref())
        .collect::<Vec<_>>();

    let mut stmt = conn
        .prepare(&sql)
        .map_err(|e| format!("预编译候选策略触发次数查询失败: {e}"))?;
    let mut rows = stmt
        .query(params_from_iter(query_params.iter()))
        .map_err(|e| format!("查询候选策略触发次数失败: {e}"))?;

    let mut daily_counts = HashMap::<String, Vec<(usize, usize)>>::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取候选策略触发次数失败: {e}"))?
    {
        let ts_code: String = row.get(0).map_err(|e| format!("读取代码失败: {e}"))?;
        let trade_date: String = row.get(1).map_err(|e| format!("读取交易日失败: {e}"))?;
        let trigger_count: i64 = row.get(2).map_err(|e| format!("读取触发次数失败: {e}"))?;
        let Some(trade_index) = trade_date_to_index.get(trade_date.as_str()).copied() else {
            continue;
        };
        daily_counts
            .entry(ts_code)
            .or_default()
            .push((trade_index, trigger_count.max(0) as usize));
    }

    let mut compact_counts = HashMap::<String, (Vec<usize>, Vec<usize>)>::new();
    for (ts_code, mut counts) in daily_counts {
        counts.sort_by_key(|(trade_index, _)| *trade_index);
        let mut trade_indices = Vec::with_capacity(counts.len());
        let mut prefix = Vec::with_capacity(counts.len() + 1);
        prefix.push(0);

        for (trade_index, count) in counts {
            if trade_indices
                .last()
                .copied()
                .map(|last_index| last_index == trade_index)
                .unwrap_or(false)
            {
                if let Some(last_prefix) = prefix.last_mut() {
                    *last_prefix += count;
                }
                continue;
            }

            trade_indices.push(trade_index);
            prefix.push(prefix.last().copied().unwrap_or(0) + count);
        }

        compact_counts.insert(ts_code, (trade_indices, prefix));
    }

    Ok(CandidateTriggerCounter { compact_counts })
}

fn load_row_summary_rows(
    conn: &Connection,
    rows: &[StrategyTriggerSimilarityRow],
) -> Result<HashMap<(String, String), SummaryRow>, String> {
    if rows.is_empty() {
        return Ok(HashMap::new());
    }

    let mut summary_pairs = rows
        .iter()
        .map(|row| (row.ts_code.clone(), row.candidate_end_trade_date.clone()))
        .collect::<Vec<_>>();
    summary_pairs.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    summary_pairs.dedup();
    if summary_pairs.is_empty() {
        return Ok(HashMap::new());
    }

    let values_sql = summary_pairs
        .iter()
        .map(|_| "(?, ?)")
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        r#"
        WITH candidates(ts_code, trade_date) AS (VALUES {values_sql})
        SELECT s.ts_code, s.trade_date, s.total_score, s.rank
        FROM score_summary AS s
        INNER JOIN candidates AS c
          ON c.ts_code = s.ts_code
         AND c.trade_date = s.trade_date
        "#
    );

    let mut query_params = Vec::with_capacity(summary_pairs.len() * 2);
    for (ts_code, end_trade_date) in &summary_pairs {
        query_params.push(ts_code.clone());
        query_params.push(end_trade_date.clone());
    }

    let mut stmt = conn
        .prepare(&sql)
        .map_err(|e| format!("预编译候选评分摘要查询失败: {e}"))?;
    let mut rows = stmt
        .query(params_from_iter(query_params.iter()))
        .map_err(|e| format!("查询候选评分摘要失败: {e}"))?;

    let mut out = HashMap::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取候选评分摘要失败: {e}"))?
    {
        let ts_code: String = row.get(0).map_err(|e| format!("读取代码失败: {e}"))?;
        let trade_date: String = row.get(1).map_err(|e| format!("读取交易日失败: {e}"))?;
        out.insert(
            (ts_code, trade_date),
            SummaryRow {
                total_score: row
                    .get(2)
                    .map_err(|e| format!("读取 total_score 失败: {e}"))?,
                rank: row.get(3).map_err(|e| format!("读取 rank 失败: {e}"))?,
            },
        );
    }

    Ok(out)
}

fn compare_similarity_rows(
    left: &StrategyTriggerSimilarityRow,
    right: &StrategyTriggerSimilarityRow,
) -> Ordering {
    right
        .similarity_score
        .total_cmp(&left.similarity_score)
        .then_with(|| right.matched_event_count.cmp(&left.matched_event_count))
        .then_with(|| {
            left.avg_date_gap_trade_days
                .unwrap_or(f64::MAX)
                .total_cmp(&right.avg_date_gap_trade_days.unwrap_or(f64::MAX))
        })
        .then_with(|| {
            left.candidate_end_trade_date
                .cmp(&right.candidate_end_trade_date)
        })
        .then_with(|| left.ts_code.cmp(&right.ts_code))
}

fn build_row_from_accumulator(
    candidate_key: CandidateKey,
    accumulator: CandidateAccumulator,
    target_events: &[TargetEvent],
    all_trade_dates: &[String],
    window_trade_days: usize,
    target_trigger_count: usize,
    candidate_trigger_count: usize,
    name_map: &HashMap<String, String>,
    industry_map: &HashMap<String, String>,
    concept_map: &HashMap<String, String>,
) -> Option<StrategyTriggerSimilarityRow> {
    let window_start_index = candidate_key.window_start_index;
    let ts_code = candidate_key.ts_code;
    let candidate_start_trade_date = all_trade_dates.get(window_start_index)?.clone();
    let candidate_end_trade_date = all_trade_dates
        .get(window_start_index + window_trade_days - 1)?
        .clone();
    let mut matches = accumulator
        .best_by_target_event
        .into_values()
        .collect::<Vec<_>>();
    if matches.is_empty() {
        return None;
    }

    matches.sort_by(|left, right| {
        target_events
            .get(left.target_event_index)
            .map(|event| event.trade_date.as_str())
            .unwrap_or("")
            .cmp(
                &target_events
                    .get(right.target_event_index)
                    .map(|event| event.trade_date.as_str())
                    .unwrap_or(""),
            )
            .then_with(|| {
                target_events
                    .get(left.target_event_index)
                    .map(|event| event.rule_name.as_str())
                    .unwrap_or("")
                    .cmp(
                        &target_events
                            .get(right.target_event_index)
                            .map(|event| event.rule_name.as_str())
                            .unwrap_or(""),
                    )
            })
            .then_with(|| left.candidate_trade_index.cmp(&right.candidate_trade_index))
    });

    let total_event_score = matches.iter().map(|item| item.event_score).sum::<f64>();
    let matched_event_count = matches.len();
    let f1_denominator = target_trigger_count + candidate_trigger_count;
    let similarity_score = if f1_denominator == 0 {
        0.0
    } else {
        200.0 * total_event_score / f1_denominator as f64
    };
    let avg_date_gap_trade_days = if matched_event_count == 0 {
        None
    } else {
        Some(
            matches.iter().map(|item| item.date_gap).sum::<usize>() as f64
                / matched_event_count as f64,
        )
    };
    let mut matched_rule_names = Vec::new();
    let mut seen_rule_names = HashSet::new();
    for item in &matches {
        let Some(target_event) = target_events.get(item.target_event_index) else {
            continue;
        };
        if seen_rule_names.insert(target_event.rule_name.clone()) {
            matched_rule_names.push(target_event.rule_name.clone());
        }
    }

    let matched_events = matches
        .iter()
        .take(MAX_MATCHED_EVENTS_PER_ROW)
        .filter_map(|item| {
            let target_event = target_events.get(item.target_event_index)?;
            let candidate_trade_date = all_trade_dates.get(item.candidate_trade_index)?;
            Some(StrategyTriggerSimilarityMatchedEvent {
                rule_name: target_event.rule_name.clone(),
                target_trade_date: target_event.trade_date.clone(),
                candidate_trade_date: candidate_trade_date.clone(),
                date_gap_trade_days: item.date_gap,
                event_score: item.event_score,
            })
        })
        .collect::<Vec<_>>();

    Some(StrategyTriggerSimilarityRow {
        name: name_map.get(ts_code.as_ref()).cloned(),
        industry: industry_map.get(ts_code.as_ref()).cloned(),
        concept: concept_map.get(ts_code.as_ref()).cloned(),
        candidate_start_trade_date,
        candidate_end_trade_date,
        candidate_trigger_count,
        total_score: None,
        rank: None,
        ts_code: ts_code.to_string(),
        similarity_score,
        matched_event_count,
        target_trigger_count,
        matched_rule_count: matched_rule_names.len(),
        avg_date_gap_trade_days,
        matched_rule_names,
        matched_events,
    })
}

fn push_top_row(
    heap: &mut BinaryHeap<WorstFirstSimilarityRow>,
    row: StrategyTriggerSimilarityRow,
    limit: usize,
) {
    if limit == 0 {
        return;
    }
    heap.push(WorstFirstSimilarityRow(row));
    if heap.len() > limit {
        heap.pop();
    }
}

fn build_top_rows(
    accumulators: HashMap<CandidateKey, CandidateAccumulator>,
    target_events: &[TargetEvent],
    all_trade_dates: &[String],
    window_trade_days: usize,
    target_trigger_count: usize,
    candidate_trigger_counter: &CandidateTriggerCounter,
    name_map: &HashMap<String, String>,
    industry_map: &HashMap<String, String>,
    concept_map: &HashMap<String, String>,
    limit: usize,
) -> Vec<StrategyTriggerSimilarityRow> {
    if limit == 0 {
        return Vec::new();
    }

    let heap = accumulators
        .into_par_iter()
        .filter_map(|(candidate_key, accumulator)| {
            let candidate_trigger_count = candidate_trigger_counter.count_for(
                &candidate_key,
                all_trade_dates.len(),
                window_trade_days,
            );
            build_row_from_accumulator(
                candidate_key,
                accumulator,
                target_events,
                all_trade_dates,
                window_trade_days,
                target_trigger_count,
                candidate_trigger_count,
                name_map,
                industry_map,
                concept_map,
            )
        })
        .fold(
            BinaryHeap::<WorstFirstSimilarityRow>::new,
            |mut heap, row| {
                push_top_row(&mut heap, row, limit);
                heap
            },
        )
        .reduce(
            BinaryHeap::<WorstFirstSimilarityRow>::new,
            |mut left, right| {
                for row in right.into_vec() {
                    push_top_row(&mut left, row.0, limit);
                }
                left
            },
        );

    let mut rows = heap
        .into_vec()
        .into_iter()
        .map(|row| row.0)
        .collect::<Vec<_>>();
    rows.sort_by(compare_similarity_rows);
    rows
}

#[cfg(test)]
fn build_rows(
    accumulators: HashMap<CandidateKey, CandidateAccumulator>,
    target_events: &[TargetEvent],
    all_trade_dates: &[String],
    window_trade_days: usize,
    target_trigger_count: usize,
    candidate_trigger_counts: &HashMap<CandidateKey, usize>,
    name_map: &HashMap<String, String>,
    industry_map: &HashMap<String, String>,
    concept_map: &HashMap<String, String>,
) -> Vec<StrategyTriggerSimilarityRow> {
    let mut rows = accumulators
        .into_iter()
        .filter_map(|(candidate_key, accumulator)| {
            let candidate_trigger_count = candidate_trigger_counts
                .get(&candidate_key)
                .copied()
                .unwrap_or(0);
            build_row_from_accumulator(
                candidate_key,
                accumulator,
                target_events,
                all_trade_dates,
                window_trade_days,
                target_trigger_count,
                candidate_trigger_count,
                name_map,
                industry_map,
                concept_map,
            )
        })
        .collect::<Vec<_>>();
    rows.sort_by(compare_similarity_rows);
    rows
}

pub fn get_strategy_trigger_similarity_page(
    source_path: String,
    trade_date: Option<String>,
    ts_code: String,
    window_trade_days: Option<u32>,
    max_gap_trade_days: Option<u32>,
    limit: Option<u32>,
) -> Result<StrategyTriggerSimilarityPageData, String> {
    let source_path = source_path.trim().to_string();
    if source_path.is_empty() {
        return Err("source_path 不能为空".to_string());
    }

    let conn = open_result_conn(&source_path)?;
    let resolved_trade_date = resolve_existing_trade_date(&conn, trade_date)?;
    let resolved_ts_code = normalize_ts_code(&ts_code);
    let window_trade_days = window_trade_days
        .map(|value| value as usize)
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_WINDOW_TRADE_DAYS);
    let max_gap_trade_days = max_gap_trade_days
        .map(|value| value as usize)
        .unwrap_or(DEFAULT_MAX_GAP_TRADE_DAYS);
    let limit = limit
        .map(|value| value as usize)
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_LIMIT);

    let all_trade_dates = load_all_trade_dates(&conn)?;
    let (target_start_index, target_end_index) =
        target_window_bounds(&all_trade_dates, &resolved_trade_date, window_trade_days)?;
    let target_window_trade_days = target_end_index + 1 - target_start_index;
    let start_trade_date = all_trade_dates
        .get(target_start_index)
        .cloned()
        .ok_or_else(|| "触发相似交易日窗口为空".to_string())?;
    let trade_date_to_index = all_trade_dates
        .iter()
        .enumerate()
        .map(|(index, trade_date)| (trade_date.as_str(), index))
        .collect::<HashMap<_, _>>();
    let target_events = load_target_events(
        &conn,
        &resolved_ts_code,
        &start_trade_date,
        &resolved_trade_date,
        target_start_index,
        &trade_date_to_index,
    )?;
    let name_map = build_name_map(&source_path).unwrap_or_default();
    let industry_map = build_industry_map(&source_path).unwrap_or_default();
    let concept_map = build_concepts_map(&source_path).unwrap_or_default();
    let target_rule_names = build_target_rule_names(&target_events);

    if target_events.is_empty() {
        return Ok(StrategyTriggerSimilarityPageData {
            resolved_trade_date: resolved_trade_date.clone(),
            resolved_ts_code: resolved_ts_code.clone(),
            window_trade_days: target_window_trade_days,
            max_gap_trade_days,
            target: StrategyTriggerSimilarityTarget {
                ts_code: resolved_ts_code.clone(),
                name: name_map.get(&resolved_ts_code).cloned(),
                industry: industry_map.get(&resolved_ts_code).cloned(),
                concept: concept_map.get(&resolved_ts_code).cloned(),
                start_trade_date,
                end_trade_date: resolved_trade_date,
                trigger_count: 0,
                rule_names: Vec::new(),
            },
            items: Vec::new(),
        });
    }

    let accumulators = load_candidate_matches(
        &source_path,
        &target_events,
        &all_trade_dates,
        &trade_date_to_index,
        target_window_trade_days,
        &resolved_ts_code,
        max_gap_trade_days,
    )?;
    let candidate_trigger_counter =
        load_candidate_trigger_counter(&conn, accumulators.keys(), &trade_date_to_index)?;
    let mut items = build_top_rows(
        accumulators,
        &target_events,
        &all_trade_dates,
        target_window_trade_days,
        target_events.len(),
        &candidate_trigger_counter,
        &name_map,
        &industry_map,
        &concept_map,
        limit,
    );
    let summary_rows = load_row_summary_rows(&conn, &items)?;
    for item in &mut items {
        if let Some(summary) =
            summary_rows.get(&(item.ts_code.clone(), item.candidate_end_trade_date.clone()))
        {
            item.total_score = summary.total_score;
            item.rank = summary.rank;
        }
    }

    Ok(StrategyTriggerSimilarityPageData {
        resolved_trade_date: resolved_trade_date.clone(),
        resolved_ts_code: resolved_ts_code.clone(),
        window_trade_days: target_window_trade_days,
        max_gap_trade_days,
        target: StrategyTriggerSimilarityTarget {
            ts_code: resolved_ts_code.clone(),
            name: name_map.get(&resolved_ts_code).cloned(),
            industry: industry_map.get(&resolved_ts_code).cloned(),
            concept: concept_map.get(&resolved_ts_code).cloned(),
            start_trade_date,
            end_trade_date: resolved_trade_date,
            trigger_count: target_events.len(),
            rule_names: target_rule_names,
        },
        items,
    })
}

#[cfg(test)]
mod tests {
    use super::{
        CandidateAccumulator, CandidateKey, CandidateMatch, TargetEvent, build_rows,
        linear_event_score,
    };
    use std::collections::HashMap;
    use std::sync::Arc;

    #[test]
    fn event_score_decays_by_trade_day_gap() {
        assert!((linear_event_score(0, 5) - 1.0).abs() < f64::EPSILON);
        assert!((linear_event_score(3, 5) - 0.5).abs() < f64::EPSILON);
        assert!((linear_event_score(6, 5) - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn build_rows_uses_bidirectional_f1_similarity() {
        let mut accumulator = CandidateAccumulator::default();
        accumulator.best_by_target_event.insert(
            0,
            CandidateMatch {
                target_event_index: 0,
                candidate_trade_index: 0,
                date_gap: 0,
                event_score: 1.0,
            },
        );
        accumulator.best_by_target_event.insert(
            1,
            CandidateMatch {
                target_event_index: 1,
                candidate_trade_index: 2,
                date_gap: 1,
                event_score: 0.5,
            },
        );

        let candidate_key = CandidateKey {
            ts_code: Arc::from("000002.SZ"),
            window_start_index: 0,
        };
        let all_trade_dates = vec![
            "20240102".to_string(),
            "20240103".to_string(),
            "20240104".to_string(),
        ];
        let target_events = vec![
            TargetEvent {
                index: 0,
                rule_name: "规则A".to_string(),
                trade_date: "20240102".to_string(),
                offset: 0,
            },
            TargetEvent {
                index: 1,
                rule_name: "规则B".to_string(),
                trade_date: "20240103".to_string(),
                offset: 1,
            },
        ];
        let rows = build_rows(
            HashMap::from([(candidate_key.clone(), accumulator)]),
            &target_events,
            &all_trade_dates,
            3,
            3,
            &HashMap::from([(candidate_key, 4)]),
            &HashMap::new(),
            &HashMap::new(),
            &HashMap::new(),
        );

        assert_eq!(rows.len(), 1);
        assert!((rows[0].similarity_score - (300.0 / 7.0)).abs() < 1e-12);
        assert_eq!(rows[0].matched_event_count, 2);
        assert_eq!(rows[0].candidate_trigger_count, 4);
        assert_eq!(rows[0].matched_rule_count, 2);
        assert_eq!(rows[0].avg_date_gap_trade_days, Some(0.5));
        assert_eq!(rows[0].candidate_start_trade_date, "20240102");
        assert_eq!(rows[0].candidate_end_trade_date, "20240104");
    }
}
