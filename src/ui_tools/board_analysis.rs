use std::cmp::Ordering;
use std::collections::HashMap;

use duckdb::{Connection, params};
use serde::Serialize;

use crate::{
    data::{load_trade_date_list, result_db_path, source_db_path},
    ui_tools::{build_concepts_map, build_industry_map, build_name_map, resolve_trade_date},
    utils::utils::board_category,
};

const DEFAULT_ADJ_TYPE: &str = "qfq";
const DEFAULT_WEIGHT_RANGE_START: u32 = 1;
const DEFAULT_WEIGHT_RANGE_END: u32 = 200;
const DEFAULT_BACKTEST_PERIOD_DAYS: u32 = 5;
const CONCEPT_GROUP_LIMIT: usize = 120;
const CONCEPT_RETURN_MIN_SAMPLE_COUNT: u32 = 2;
const LINEAR_TAIL_WEIGHT_FLOOR: f64 = 0.8;

#[derive(Debug, Serialize, Clone)]
pub struct BoardAnalysisGroupRow {
    pub name: String,
    pub sample_count: u32,
    pub strength_score_pct: Option<f64>,
    pub avg_rank: Option<f64>,
    pub avg_return_pct: Option<f64>,
    pub up_ratio_pct: Option<f64>,
    pub top_rank: Option<i64>,
    pub leader_stock_name: Option<String>,
    pub leader_stock_ts_code: Option<String>,
    pub leader_stock_return_pct: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct BoardAnalysisSummary {
    pub rank_sample_count: u32,
    pub return_sample_count: u32,
}

#[derive(Debug, Serialize)]
pub struct BoardAnalysisPageData {
    pub resolved_ref_date: Option<String>,
    pub resolved_backtest_start_date: Option<String>,
    pub weighting_range_start: u32,
    pub weighting_range_end: u32,
    pub backtest_period_days: u32,
    pub industry_strength_rows: Vec<BoardAnalysisGroupRow>,
    pub concept_strength_rows: Vec<BoardAnalysisGroupRow>,
    pub industry_return_rows: Vec<BoardAnalysisGroupRow>,
    pub concept_return_rows: Vec<BoardAnalysisGroupRow>,
    pub summary: Option<BoardAnalysisSummary>,
}

#[derive(Debug, Serialize, Clone)]
pub struct BoardAnalysisStockRow {
    pub ts_code: String,
    pub name: Option<String>,
    pub market_board: String,
    pub industry: Option<String>,
    pub concept: Option<String>,
    pub rank: Option<i64>,
    pub total_score: Option<f64>,
    pub strength_weight: Option<f64>,
    pub return_pct: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct BoardAnalysisGroupDetail {
    pub group_kind: String,
    pub metric_kind: String,
    pub group_name: String,
    pub resolved_ref_date: Option<String>,
    pub resolved_backtest_start_date: Option<String>,
    pub weighting_range_start: u32,
    pub weighting_range_end: u32,
    pub backtest_period_days: u32,
    pub summary: Option<BoardAnalysisGroupRow>,
    pub stocks: Vec<BoardAnalysisStockRow>,
}

#[derive(Debug, Clone)]
struct SnapshotRow {
    ts_code: String,
    name: Option<String>,
    market_board: String,
    industry: Option<String>,
    concept_text: Option<String>,
    concept_items: Vec<String>,
    rank: Option<i64>,
    total_score: Option<f64>,
    strength_weight: Option<f64>,
    return_pct: Option<f64>,
}

#[derive(Debug)]
struct QuerySnapshotRow {
    ts_code: String,
    rank: Option<i64>,
    total_score: Option<f64>,
    start_close: Option<f64>,
    ref_close: Option<f64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GroupKind {
    Industry,
    Concept,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MetricKind {
    Strength,
    Return,
}

impl GroupKind {
    fn parse(value: &str) -> Result<Self, String> {
        match value.trim() {
            "industry" => Ok(Self::Industry),
            "concept" => Ok(Self::Concept),
            other => Err(format!("未知分组类型: {other}")),
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Industry => "industry",
            Self::Concept => "concept",
        }
    }
}

impl MetricKind {
    fn parse(value: &str) -> Result<Self, String> {
        match value.trim() {
            "strength" => Ok(Self::Strength),
            "return" => Ok(Self::Return),
            other => Err(format!("未知榜单类型: {other}")),
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Strength => "strength",
            Self::Return => "return",
        }
    }
}

fn open_result_conn(source_path: &str) -> Result<Connection, String> {
    let result_db = result_db_path(source_path);
    let result_db_str = result_db
        .to_str()
        .ok_or_else(|| "结果库路径不是有效UTF-8".to_string())?;
    Connection::open(result_db_str).map_err(|e| format!("打开结果库失败: {e}"))
}

fn open_source_conn(source_path: &str) -> Result<Connection, String> {
    let source_db = source_db_path(source_path);
    let source_db_str = source_db
        .to_str()
        .ok_or_else(|| "原始库路径不是有效UTF-8".to_string())?;
    Connection::open(source_db_str).map_err(|e| format!("打开原始库失败: {e}"))
}

fn attach_result_db(source_conn: &Connection, source_path: &str) -> Result<(), String> {
    let result_db = result_db_path(source_path);
    let result_db_str = result_db
        .to_str()
        .ok_or_else(|| "结果库路径不是有效UTF-8".to_string())?;
    let escaped = result_db_str.replace('\'', "''");
    source_conn
        .execute_batch(&format!("ATTACH '{}' AS result_db (READ_ONLY);", escaped))
        .map_err(|e| format!("挂载结果库失败: {e}"))
}

fn query_rank_trade_date_options(conn: &Connection) -> Result<Vec<String>, String> {
    let mut stmt = conn
        .prepare(
            r#"
            SELECT DISTINCT trade_date
            FROM score_summary
            ORDER BY trade_date DESC
            "#,
        )
        .map_err(|e| format!("预编译日期列表失败: {e}"))?;
    let mut rows = stmt
        .query([])
        .map_err(|e| format!("查询日期列表失败: {e}"))?;
    let mut out = Vec::new();

    while let Some(row) = rows.next().map_err(|e| format!("读取日期列表失败: {e}"))? {
        let trade_date: String = row.get(0).map_err(|e| format!("读取日期字段失败: {e}"))?;
        if !trade_date.trim().is_empty() {
            out.push(trade_date);
        }
    }

    Ok(out)
}

fn normalize_weight_range(
    weighting_range_start: Option<u32>,
    weighting_range_end: Option<u32>,
) -> (u32, u32) {
    let start = weighting_range_start
        .unwrap_or(DEFAULT_WEIGHT_RANGE_START)
        .max(1);
    let end = weighting_range_end
        .unwrap_or(DEFAULT_WEIGHT_RANGE_END)
        .max(1);
    if start <= end {
        (start, end)
    } else {
        (end, start)
    }
}

fn normalize_backtest_period_days(backtest_period_days: Option<u32>) -> u32 {
    backtest_period_days
        .unwrap_or(DEFAULT_BACKTEST_PERIOD_DAYS)
        .max(1)
}

fn resolve_backtest_start_date(
    source_path: &str,
    ref_date: &str,
    backtest_period_days: u32,
) -> Result<(String, u32), String> {
    let trade_dates = load_trade_date_list(source_path)?;
    if trade_dates.is_empty() {
        return Err("trade_calendar.csv 没有可用交易日".to_string());
    }

    let ref_index = match trade_dates.binary_search_by(|item| item.as_str().cmp(ref_date)) {
        Ok(index) => index,
        Err(insert_index) => insert_index
            .checked_sub(1)
            .ok_or_else(|| format!("参考日{ref_date}早于交易日历起始日"))?,
    };
    let lookback = backtest_period_days.saturating_sub(1) as usize;
    let start_index = ref_index.saturating_sub(lookback);
    let effective_days = (ref_index - start_index + 1) as u32;
    Ok((trade_dates[start_index].clone(), effective_days))
}

fn split_concept_items(value: &str) -> Vec<String> {
    let mut seen = HashMap::<String, String>::new();
    for raw in
        value.split(|ch| matches!(ch, ',' | ';' | '，' | '；' | '|' | '、' | '/' | '\n' | '\r'))
    {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            continue;
        }
        let key = trimmed.to_ascii_lowercase();
        seen.entry(key).or_insert_with(|| trimmed.to_string());
    }

    let mut items: Vec<String> = seen.into_values().collect();
    items.sort_by(|left, right| left.cmp(right));
    items
}

fn compute_non_linear_weight(rank: i64, range_start: u32, range_end: u32) -> Option<f64> {
    let start = range_start as i64;
    let end = range_end as i64;
    if rank <= 0 {
        return None;
    }

    if end <= start {
        if rank <= start {
            return Some(1.0);
        }

        let half_life = start.max(1) as f64;
        let tail_distance = (rank - start) as f64;
        let decay_rate = std::f64::consts::LN_2 / half_life.max(1.0);
        return Some(LINEAR_TAIL_WEIGHT_FLOOR * (-decay_rate * tail_distance).exp());
    }

    if rank <= start {
        return Some(1.0);
    }

    let span = (end - start) as f64;
    if rank <= end {
        let progress = (rank - start) as f64 / span;
        return Some(1.0 - (1.0 - LINEAR_TAIL_WEIGHT_FLOOR) * progress);
    }

    let half_life = (end - start + 1) as f64;
    let tail_distance = (rank - end) as f64;
    let decay_rate = std::f64::consts::LN_2 / half_life.max(1.0);
    Some(LINEAR_TAIL_WEIGHT_FLOOR * (-decay_rate * tail_distance).exp())
}

fn query_snapshot_rows(
    source_conn: &Connection,
    ref_date: &str,
    backtest_start_date: &str,
) -> Result<Vec<QuerySnapshotRow>, String> {
    let sql = r#"
        WITH ref_pool AS (
            SELECT
                s.ts_code,
                s.rank,
                s.total_score
            FROM result_db.score_summary AS s
            WHERE s.trade_date = ?
        ),
        start_close AS (
            SELECT
                d.ts_code,
                TRY_CAST(d.close AS DOUBLE) AS start_close
            FROM stock_data AS d
            INNER JOIN ref_pool AS p
                ON p.ts_code = d.ts_code
            WHERE d.adj_type = ?
              AND d.trade_date = ?
        ),
        ref_close AS (
            SELECT
                d.ts_code,
                TRY_CAST(d.close AS DOUBLE) AS ref_close
            FROM stock_data AS d
            INNER JOIN ref_pool AS p
                ON p.ts_code = d.ts_code
            WHERE d.adj_type = ?
              AND d.trade_date = ?
        )
        SELECT
            p.ts_code,
            p.rank,
            p.total_score,
            s.start_close,
            r.ref_close
        FROM ref_pool AS p
        LEFT JOIN start_close AS s
            ON s.ts_code = p.ts_code
        LEFT JOIN ref_close AS r
            ON r.ts_code = p.ts_code
        ORDER BY COALESCE(p.rank, 999999) ASC, p.total_score DESC, p.ts_code ASC
    "#;

    let mut stmt = source_conn
        .prepare(sql)
        .map_err(|e| format!("预编译板块分析查询失败: {e}"))?;
    let mut rows = stmt
        .query(params![
            ref_date,
            DEFAULT_ADJ_TYPE,
            backtest_start_date,
            DEFAULT_ADJ_TYPE,
            ref_date
        ])
        .map_err(|e| format!("查询板块分析基础数据失败: {e}"))?;

    let mut out = Vec::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取板块分析基础数据失败: {e}"))?
    {
        out.push(QuerySnapshotRow {
            ts_code: row.get(0).map_err(|e| format!("读取 ts_code 失败: {e}"))?,
            rank: row.get(1).map_err(|e| format!("读取 rank 失败: {e}"))?,
            total_score: row
                .get(2)
                .map_err(|e| format!("读取 total_score 失败: {e}"))?,
            start_close: row
                .get(3)
                .map_err(|e| format!("读取 start_close 失败: {e}"))?,
            ref_close: row
                .get(4)
                .map_err(|e| format!("读取 ref_close 失败: {e}"))?,
        });
    }

    Ok(out)
}

fn build_snapshot(
    source_path: &str,
    ref_date: &str,
    backtest_start_date: &str,
    weighting_range_start: u32,
    weighting_range_end: u32,
) -> Result<Vec<SnapshotRow>, String> {
    let source_conn = open_source_conn(source_path)?;
    attach_result_db(&source_conn, source_path)?;
    let query_rows = query_snapshot_rows(&source_conn, ref_date, backtest_start_date)?;
    let name_map = build_name_map(source_path).unwrap_or_default();
    let industry_map = build_industry_map(source_path).unwrap_or_default();
    let concept_map = build_concepts_map(source_path).unwrap_or_default();

    Ok(query_rows
        .into_iter()
        .map(|row| {
            let concept_text = concept_map.get(&row.ts_code).cloned();
            let concept_items = concept_text
                .as_deref()
                .map(split_concept_items)
                .unwrap_or_default();
            let return_pct = match (row.start_close, row.ref_close) {
                (Some(start_close), Some(ref_close)) if start_close > 0.0 => {
                    Some((ref_close / start_close - 1.0) * 100.0)
                }
                _ => None,
            };

            SnapshotRow {
                ts_code: row.ts_code.clone(),
                name: name_map.get(&row.ts_code).cloned(),
                market_board: board_category(
                    &row.ts_code,
                    name_map.get(&row.ts_code).map(|value| value.as_str()),
                )
                .to_string(),
                industry: industry_map.get(&row.ts_code).cloned(),
                concept_text,
                concept_items,
                rank: row.rank,
                total_score: row.total_score,
                strength_weight: row.rank.and_then(|rank| {
                    compute_non_linear_weight(rank, weighting_range_start, weighting_range_end)
                }),
                return_pct,
            }
        })
        .collect())
}

fn average(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    Some(values.iter().sum::<f64>() / values.len() as f64)
}

fn option_f64_desc(left: Option<f64>, right: Option<f64>) -> Ordering {
    match (left, right) {
        (Some(left_value), Some(right_value)) => right_value
            .partial_cmp(&left_value)
            .unwrap_or(Ordering::Equal),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

fn option_f64_asc(left: Option<f64>, right: Option<f64>) -> Ordering {
    match (left, right) {
        (Some(left_value), Some(right_value)) => left_value
            .partial_cmp(&right_value)
            .unwrap_or(Ordering::Equal),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

fn option_i64_asc(left: Option<i64>, right: Option<i64>) -> Ordering {
    match (left, right) {
        (Some(left_value), Some(right_value)) => left_value.cmp(&right_value),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

fn group_names(row: &SnapshotRow, group_kind: GroupKind) -> Vec<String> {
    match group_kind {
        GroupKind::Industry => row
            .industry
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| vec![value.to_string()])
            .unwrap_or_default(),
        GroupKind::Concept => row.concept_items.clone(),
    }
}

fn build_group_summary(
    name: String,
    group_rows: &[&SnapshotRow],
    metric_kind: MetricKind,
) -> BoardAnalysisGroupRow {
    let strength_values = group_rows
        .iter()
        .filter_map(|row| row.strength_weight)
        .collect::<Vec<_>>();
    let ranks = group_rows
        .iter()
        .filter_map(|row| row.rank.map(|value| value as f64))
        .collect::<Vec<_>>();
    let returns = group_rows
        .iter()
        .filter_map(|row| row.return_pct)
        .collect::<Vec<_>>();
    let up_ratio_pct = if returns.is_empty() {
        None
    } else {
        Some(
            (returns.iter().filter(|value| **value > 0.0).count() as f64 / returns.len() as f64)
                * 100.0,
        )
    };
    let top_rank_stock = group_rows
        .iter()
        .filter(|row| row.rank.is_some())
        .min_by(|left, right| {
            option_i64_asc(left.rank, right.rank).then_with(|| left.ts_code.cmp(&right.ts_code))
        })
        .copied();
    let leader_stock = match metric_kind {
        MetricKind::Strength => top_rank_stock,
        MetricKind::Return => group_rows
            .iter()
            .filter(|row| row.return_pct.is_some())
            .max_by(|left, right| {
                left.return_pct
                    .and_then(|left_value| {
                        right
                            .return_pct
                            .map(|right_value| (left_value, right_value))
                    })
                    .and_then(|(left_value, right_value)| left_value.partial_cmp(&right_value))
                    .unwrap_or(Ordering::Equal)
                    .then_with(|| option_i64_asc(left.rank, right.rank).reverse())
            })
            .copied()
            .or(top_rank_stock),
    };

    BoardAnalysisGroupRow {
        name,
        sample_count: group_rows.len() as u32,
        strength_score_pct: average(&strength_values).map(|value| value * 100.0),
        avg_rank: average(&ranks),
        avg_return_pct: average(&returns),
        up_ratio_pct,
        top_rank: top_rank_stock.and_then(|row| row.rank),
        leader_stock_name: leader_stock.and_then(|row| row.name.clone()),
        leader_stock_ts_code: leader_stock.map(|row| row.ts_code.clone()),
        leader_stock_return_pct: leader_stock.and_then(|row| row.return_pct),
    }
}

fn build_group_rows(
    snapshot: &[SnapshotRow],
    group_kind: GroupKind,
    metric_kind: MetricKind,
) -> Vec<BoardAnalysisGroupRow> {
    let mut grouped: HashMap<String, Vec<&SnapshotRow>> = HashMap::new();

    for row in snapshot {
        let include_row = match metric_kind {
            MetricKind::Strength => row.strength_weight.is_some(),
            MetricKind::Return => row.return_pct.is_some(),
        };
        if !include_row {
            continue;
        }

        let names = group_names(row, group_kind);
        if names.is_empty() {
            continue;
        }

        for name in names {
            grouped.entry(name).or_default().push(row);
        }
    }

    let mut rows = grouped
        .into_iter()
        .map(|(name, group_rows)| build_group_summary(name, &group_rows, metric_kind))
        .collect::<Vec<_>>();

    if group_kind == GroupKind::Concept && metric_kind == MetricKind::Return {
        rows.retain(|row| row.sample_count >= CONCEPT_RETURN_MIN_SAMPLE_COUNT);
    }

    rows.sort_by(|left, right| match metric_kind {
        MetricKind::Strength => option_f64_desc(left.strength_score_pct, right.strength_score_pct)
            .then_with(|| right.sample_count.cmp(&left.sample_count))
            .then_with(|| option_f64_asc(left.avg_rank, right.avg_rank))
            .then_with(|| left.name.cmp(&right.name)),
        MetricKind::Return => option_f64_desc(left.avg_return_pct, right.avg_return_pct)
            .then_with(|| option_f64_desc(left.up_ratio_pct, right.up_ratio_pct))
            .then_with(|| right.sample_count.cmp(&left.sample_count))
            .then_with(|| option_f64_desc(left.strength_score_pct, right.strength_score_pct))
            .then_with(|| left.name.cmp(&right.name)),
    });

    if group_kind == GroupKind::Concept && rows.len() > CONCEPT_GROUP_LIMIT {
        rows.truncate(CONCEPT_GROUP_LIMIT);
    }

    rows
}

fn row_matches_group(row: &SnapshotRow, group_kind: GroupKind, group_name: &str) -> bool {
    let group_name = group_name.trim();
    if group_name.is_empty() {
        return false;
    }

    match group_kind {
        GroupKind::Industry => row
            .industry
            .as_deref()
            .map(str::trim)
            .map(|value| value == group_name)
            .unwrap_or(false),
        GroupKind::Concept => row.concept_items.iter().any(|item| item == group_name),
    }
}

fn build_stock_rows(
    snapshot: &[SnapshotRow],
    group_kind: GroupKind,
    metric_kind: MetricKind,
    group_name: &str,
) -> Vec<BoardAnalysisStockRow> {
    let mut rows = snapshot
        .iter()
        .filter(|row| row_matches_group(row, group_kind, group_name))
        .filter(|row| match metric_kind {
            MetricKind::Strength => row.strength_weight.is_some(),
            MetricKind::Return => row.return_pct.is_some(),
        })
        .map(|row| BoardAnalysisStockRow {
            ts_code: row.ts_code.clone(),
            name: row.name.clone(),
            market_board: row.market_board.clone(),
            industry: row.industry.clone(),
            concept: row.concept_text.clone(),
            rank: row.rank,
            total_score: row.total_score,
            strength_weight: row.strength_weight.map(|value| value * 100.0),
            return_pct: row.return_pct,
        })
        .collect::<Vec<_>>();

    rows.sort_by(|left, right| match metric_kind {
        MetricKind::Strength => option_i64_asc(left.rank, right.rank)
            .then_with(|| option_f64_desc(left.return_pct, right.return_pct))
            .then_with(|| left.ts_code.cmp(&right.ts_code)),
        MetricKind::Return => option_f64_desc(left.return_pct, right.return_pct)
            .then_with(|| option_i64_asc(left.rank, right.rank))
            .then_with(|| left.ts_code.cmp(&right.ts_code)),
    });
    rows
}

fn build_context(
    source_path: &str,
    ref_date: Option<String>,
    weighting_range_start: Option<u32>,
    weighting_range_end: Option<u32>,
    backtest_period_days: Option<u32>,
) -> Result<(String, String, u32, u32, u32, Vec<SnapshotRow>), String> {
    let result_conn = open_result_conn(source_path)?;
    let resolved_ref_date = resolve_trade_date(&result_conn, ref_date)?;
    let _date_options = query_rank_trade_date_options(&result_conn)?;
    let (range_start, range_end) =
        normalize_weight_range(weighting_range_start, weighting_range_end);
    let period_days = normalize_backtest_period_days(backtest_period_days);
    let (backtest_start_date, effective_period_days) =
        resolve_backtest_start_date(source_path, &resolved_ref_date, period_days)?;
    let snapshot = build_snapshot(
        source_path,
        &resolved_ref_date,
        &backtest_start_date,
        range_start,
        range_end,
    )?;

    Ok((
        resolved_ref_date,
        backtest_start_date,
        range_start,
        range_end,
        effective_period_days,
        snapshot,
    ))
}

pub fn get_board_analysis_page(
    source_path: String,
    ref_date: Option<String>,
    weighting_range_start: Option<u32>,
    weighting_range_end: Option<u32>,
    backtest_period_days: Option<u32>,
) -> Result<BoardAnalysisPageData, String> {
    let (
        resolved_ref_date,
        backtest_start_date,
        range_start,
        range_end,
        effective_period_days,
        snapshot,
    ) = build_context(
        &source_path,
        ref_date,
        weighting_range_start,
        weighting_range_end,
        backtest_period_days,
    )?;

    let industry_strength_rows =
        build_group_rows(&snapshot, GroupKind::Industry, MetricKind::Strength);
    let concept_strength_rows =
        build_group_rows(&snapshot, GroupKind::Concept, MetricKind::Strength);
    let industry_return_rows = build_group_rows(&snapshot, GroupKind::Industry, MetricKind::Return);
    let concept_return_rows = build_group_rows(&snapshot, GroupKind::Concept, MetricKind::Return);

    Ok(BoardAnalysisPageData {
        resolved_ref_date: Some(resolved_ref_date),
        resolved_backtest_start_date: Some(backtest_start_date),
        weighting_range_start: range_start,
        weighting_range_end: range_end,
        backtest_period_days: effective_period_days,
        industry_strength_rows,
        concept_strength_rows,
        industry_return_rows,
        concept_return_rows,
        summary: Some(BoardAnalysisSummary {
            rank_sample_count: snapshot
                .iter()
                .filter(|row| row.strength_weight.is_some())
                .count() as u32,
            return_sample_count: snapshot
                .iter()
                .filter(|row| row.return_pct.is_some())
                .count() as u32,
        }),
    })
}

pub fn get_board_analysis_group_detail(
    source_path: String,
    ref_date: Option<String>,
    weighting_range_start: Option<u32>,
    weighting_range_end: Option<u32>,
    backtest_period_days: Option<u32>,
    group_kind: String,
    metric_kind: String,
    group_name: String,
) -> Result<BoardAnalysisGroupDetail, String> {
    let group_kind = GroupKind::parse(&group_kind)?;
    let metric_kind = MetricKind::parse(&metric_kind)?;
    let group_name = group_name.trim().to_string();
    if group_name.is_empty() {
        return Err("分组名称不能为空".to_string());
    }

    let (
        resolved_ref_date,
        backtest_start_date,
        range_start,
        range_end,
        effective_period_days,
        snapshot,
    ) = build_context(
        &source_path,
        ref_date,
        weighting_range_start,
        weighting_range_end,
        backtest_period_days,
    )?;

    let stocks = build_stock_rows(&snapshot, group_kind, metric_kind, &group_name);
    if stocks.is_empty() {
        return Err(format!("未找到分组“{group_name}”对应的股票"));
    }

    let group_refs = snapshot
        .iter()
        .filter(|row| row_matches_group(row, group_kind, &group_name))
        .filter(|row| match metric_kind {
            MetricKind::Strength => row.strength_weight.is_some(),
            MetricKind::Return => row.return_pct.is_some(),
        })
        .collect::<Vec<_>>();
    let summary = build_group_summary(group_name.clone(), &group_refs, metric_kind);

    Ok(BoardAnalysisGroupDetail {
        group_kind: group_kind.label().to_string(),
        metric_kind: metric_kind.label().to_string(),
        group_name,
        resolved_ref_date: Some(resolved_ref_date),
        resolved_backtest_start_date: Some(backtest_start_date),
        weighting_range_start: range_start,
        weighting_range_end: range_end,
        backtest_period_days: effective_period_days,
        summary: Some(summary),
        stocks,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compute_non_linear_weight_decays_inside_range() {
        let top_weight = compute_non_linear_weight(1, 1, 100).unwrap();
        let mid_weight = compute_non_linear_weight(50, 1, 100).unwrap();
        let tail_weight = compute_non_linear_weight(100, 1, 100).unwrap();
        assert!(top_weight > mid_weight);
        assert!(mid_weight > tail_weight);
        assert!((top_weight - 1.0).abs() < 1e-9);
        assert!(tail_weight >= 0.8 - 1e-9);
    }

    #[test]
    fn compute_non_linear_weight_uses_exponential_decay_after_range() {
        let end_weight = compute_non_linear_weight(200, 1, 200).unwrap();
        let rank_400_weight = compute_non_linear_weight(400, 1, 200).unwrap();
        let rank_1000_weight = compute_non_linear_weight(1000, 1, 200).unwrap();
        assert!((end_weight - 0.8).abs() < 1e-9);
        assert!((rank_400_weight - 0.4).abs() < 1e-9);
        assert!(rank_1000_weight < rank_400_weight);
    }

    #[test]
    fn split_concept_items_deduplicates_empty_parts() {
        let items = split_concept_items("算力, 东数西算(算力) / 算力 ; ");
        assert_eq!(items.len(), 2);
        assert!(items.iter().any(|item| item == "算力"));
        assert!(items.iter().any(|item| item == "东数西算(算力)"));
    }
}
