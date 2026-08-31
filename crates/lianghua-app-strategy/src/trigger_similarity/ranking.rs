use std::{
    cell::RefCell,
    cmp::{Ordering, Reverse},
    collections::{BinaryHeap, HashMap},
    fs,
    path::Path,
    sync::{Arc, Mutex, OnceLock},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use duckdb::{Connection, params};
use serde::{Deserialize, Serialize};

use super::*;
use crate::data::{ind_toml_path, score_rule_path, stock_list_path};
use crate::utils::utils::board_category;
use lianghua_app_shared::build_total_mv_map;

const ALGORITHM_VERSION: &str = "outcome-reverse-startup-ranking-v8";
const SUCCESS_QUALITY_THRESHOLD: f64 = 0.80;
const FAILURE_QUALITY_THRESHOLD: f64 = 0.20;
const SEMANTIC_DEFINITION_SIGNATURE_PREFIX: &str = "definitions-v1|";

#[derive(Debug, Clone)]
struct OutcomePathRow {
    date_index: usize,
    open: Option<f64>,
    high: Option<f64>,
    low: Option<f64>,
    close: Option<f64>,
    pct_chg: Option<f64>,
}

#[derive(Debug, Clone, Copy)]
struct RawOutcomeLabel {
    stock_index: usize,
    date_index: usize,
    excess_return_pct: f64,
    mfe_pct: f64,
    mae_pct: f64,
    persistence: f64,
}

static RANKING_COMPUTE_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StrategyTriggerRankingMatch {
    pub ts_code: String,
    pub name: Option<String>,
    pub candidate_start_trade_date: String,
    pub candidate_end_trade_date: String,
    pub similarity_score: f64,
    pub forward_excess_return_pct: Option<f64>,
    pub mfe_pct: f64,
    pub mae_pct: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StrategyTriggerRankingRow {
    pub rank: Option<usize>,
    pub ts_code: String,
    pub name: Option<String>,
    pub industry: Option<String>,
    pub concept: Option<String>,
    pub board: Option<String>,
    pub original_score: Option<f64>,
    pub original_rank: Option<i64>,
    pub best_rank_3d: Option<i64>,
    pub ranking_score: Option<f64>,
    pub prediction_signal: Option<f64>,
    pub confidence: f64,
    pub sample_count: usize,
    pub effective_sample_count: f64,
    pub expected_return_pct: Option<f64>,
    pub expected_excess_return_pct: Option<f64>,
    pub shrunk_excess_return_pct: Option<f64>,
    pub excess_positive_rate: Option<f64>,
    pub expected_mfe_pct: Option<f64>,
    pub expected_mae_pct: Option<f64>,
    pub average_similarity: Option<f64>,
    pub best_similarity: Option<f64>,
    pub trigger_count: usize,
    pub total_mv_yi: Option<f64>,
    pub top_matches: Vec<StrategyTriggerRankingMatch>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StrategyTriggerRankingTiming {
    pub label: String,
    pub elapsed_ms: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StrategyTriggerSimilarityActiveConfig {
    pub algorithm_version: String,
    pub window_trade_days: usize,
    pub pool_segments: usize,
    pub outcome_trade_days: usize,
    pub benchmark_index_code: String,
}

#[derive(Debug, Clone)]
struct ActiveConfigRecord {
    config: StrategyTriggerSimilarityActiveConfig,
    config_key: String,
    scope_signature: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StrategyTriggerRankingPageData {
    pub resolved_trade_date: String,
    pub historical_cutoff_date: String,
    pub window_trade_days: usize,
    pub pool_segments: usize,
    pub outcome_trade_days: usize,
    pub benchmark_index_code: String,
    pub algorithm_version: String,
    pub data_signature: String,
    pub generated_at_epoch_seconds: Option<i64>,
    pub is_fresh: bool,
    pub stale_reason: Option<String>,
    pub universe_count: usize,
    pub ranked_count: usize,
    pub candidate_universe_count: usize,
    pub candidate_anchor_count: usize,
    pub evaluated_anchor_count: usize,
    pub elapsed_ms: Option<u64>,
    pub timings: Vec<StrategyTriggerRankingTiming>,
    pub items: Vec<StrategyTriggerRankingRow>,
}

#[derive(Debug, Clone)]
struct ChannelFingerprint {
    vectors: Vec<Option<Vec<f64>>>,
    norms: Vec<f64>,
    has_vectors: bool,
}

#[derive(Debug, Clone)]
struct RankingFingerprint {
    trigger: TriggerFingerprint,
    price_volume: ChannelFingerprint,
    indicators: ChannelFingerprint,
    market: Arc<ChannelFingerprint>,
}

#[derive(Debug, Clone)]
struct RankingSample {
    anchor: Anchor,
    fingerprint: RankingFingerprint,
    trigger_count: usize,
    outcome: Option<Outcome>,
    total_score: Option<f64>,
    original_rank: Option<i64>,
    template_quality_score: Option<f64>,
    template_class: i8,
}

#[derive(Debug, Clone)]
struct OutcomeSelectedAnchor {
    anchor: Anchor,
    quality_score: f64,
    quality_class: i8,
}

#[derive(Debug, Clone, Copy)]
struct ScoredCandidate {
    score: f64,
    trigger_similarity: f64,
    price_volume_similarity: Option<f64>,
    indicator_similarity: Option<f64>,
    market_similarity: Option<f64>,
    candidate_index: usize,
}

#[derive(Default)]
struct RankingTargetScratch {
    candidate_intersection_weights: Vec<f64>,
    candidate_generations: Vec<u32>,
    generation: u32,
    candidate_indices: Vec<usize>,
    success_heap: BinaryHeap<Reverse<ScoredCandidate>>,
    failure_heap: BinaryHeap<Reverse<ScoredCandidate>>,
}

impl RankingTargetScratch {
    fn prepare(&mut self, candidate_count: usize, per_class_limit: usize) {
        self.candidate_intersection_weights
            .resize(candidate_count, 0.0);
        self.candidate_generations.resize(candidate_count, 0);
        self.generation = self.generation.wrapping_add(1);
        if self.generation == 0 {
            self.candidate_generations.fill(0);
            self.generation = 1;
        }
        self.candidate_indices.clear();
        self.success_heap.clear();
        self.failure_heap.clear();
        let required_capacity = per_class_limit + 1;
        if self.success_heap.capacity() < required_capacity {
            self.success_heap.reserve(required_capacity);
        }
        if self.failure_heap.capacity() < required_capacity {
            self.failure_heap.reserve(required_capacity);
        }
    }

    fn add_candidate_rule_weight(&mut self, candidate_index: usize, weight: f64) {
        if self.candidate_generations[candidate_index] != self.generation {
            self.candidate_generations[candidate_index] = self.generation;
            self.candidate_intersection_weights[candidate_index] = weight;
            self.candidate_indices.push(candidate_index);
        } else {
            self.candidate_intersection_weights[candidate_index] += weight;
        }
    }
}

thread_local! {
    static RANKING_TARGET_SCRATCH: RefCell<RankingTargetScratch> =
        RefCell::new(RankingTargetScratch::default());
}

impl PartialEq for ScoredCandidate {
    fn eq(&self, other: &Self) -> bool {
        self.score.total_cmp(&other.score) == Ordering::Equal
            && self.candidate_index == other.candidate_index
    }
}

impl Eq for ScoredCandidate {}

impl PartialOrd for ScoredCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScoredCandidate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.score
            .total_cmp(&other.score)
            .then_with(|| self.candidate_index.cmp(&other.candidate_index))
    }
}

fn push_top_candidate(
    heap: &mut BinaryHeap<Reverse<ScoredCandidate>>,
    scored: ScoredCandidate,
    limit: usize,
) {
    if heap.len() < limit {
        heap.push(Reverse(scored));
    } else if heap.peek().is_some_and(|minimum| scored > minimum.0) {
        heap.pop();
        heap.push(Reverse(scored));
    }
}

fn can_prune_exact_candidate(upper_bound: f64, cutoff: Option<f64>) -> bool {
    // Strict comparison preserves the existing score/index tie-breaking semantics.
    cutoff.is_some_and(|minimum| upper_bound + EPS < minimum)
}

#[derive(Debug, Clone)]
struct RankingMeta {
    data_signature: String,
    generated_at_epoch_seconds: i64,
    historical_cutoff_date: String,
    universe_count: usize,
    ranked_count: usize,
    candidate_universe_count: usize,
    candidate_anchor_count: usize,
    evaluated_anchor_count: usize,
    elapsed_ms: u64,
    timings: Vec<StrategyTriggerRankingTiming>,
}

fn elapsed_ms(started: Instant) -> u64 {
    started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64
}

fn now_epoch_seconds() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .min(i64::MAX as u64) as i64
}

fn config_key(
    window_trade_days: usize,
    pool_segments: usize,
    outcome_trade_days: usize,
    benchmark_index_code: &str,
) -> String {
    (|algorithm_version: &str,
      window_trade_days: usize,
      pool_segments: usize,
      outcome_trade_days: usize,
      benchmark_index_code: &str|
     -> String {
        format!(
            "{algorithm_version}:w{window_trade_days}:p{pool_segments}:h{outcome_trade_days}:b{benchmark_index_code}"
        )
    })(
        ALGORITHM_VERSION,
        window_trade_days,
        pool_segments,
        outcome_trade_days,
        benchmark_index_code,
    )
}

fn file_stamp(path: &Path) -> Result<String, String> {
    let metadata =
        fs::metadata(path).map_err(|e| format!("读取数据文件状态失败 {:?}: {e}", path))?;
    let modified = metadata
        .modified()
        .ok()
        .and_then(|value| value.duration_since(UNIX_EPOCH).ok())
        .map(|value| value.as_nanos())
        .unwrap_or_default();
    Ok(format!("{}:{modified}", metadata.len()))
}

fn load_data_signature(
    conn: &Connection,
    source_path: &str,
    resolved_trade_date: &str,
) -> Result<String, String> {
    let stock_stamp = file_stamp(&source_db_path(source_path))?;
    let stock_list_stamp = (|path: &Path| -> Result<String, String> {
        match file_stamp(path) {
            Ok(stamp) => Ok(stamp),
            Err(_) if !path.exists() => Ok("missing".to_string()),
            Err(error) => Err(error),
        }
    })(&stock_list_path(source_path))?;
    let score_stamp: String = conn
        .query_row(
            r#"
            SELECT concat(
                COUNT(*), ':', COALESCE(MAX(trade_date), ''), ':',
                COALESCE(CAST(bit_xor(hash(ts_code, trade_date, total_score, rank)) AS VARCHAR), '0')
            )
            FROM score_summary WHERE trade_date <= ?
            "#,
            params![resolved_trade_date],
            |row| row.get(0),
        )
        .map_err(|e| format!("读取评分数据水位失败: {e}"))?;
    let rule_stamp: String = conn
        .query_row(
            r#"
            SELECT concat(
                COUNT(*), ':', COALESCE(MAX(trade_date), ''), ':',
                COUNT(DISTINCT rule_name), ':',
                COALESCE(CAST(bit_xor(hash(ts_code, trade_date, rule_name, rule_score)) AS VARCHAR), '0')
            )
            FROM rule_details WHERE trade_date <= ?
            "#,
            params![resolved_trade_date],
            |row| row.get(0),
        )
        .map_err(|e| format!("读取策略触发数据水位失败: {e}"))?;
    Ok(format!(
        "{ALGORITHM_VERSION}|stock={stock_stamp}|stock_list={stock_list_stamp}|score={score_stamp}|rule={rule_stamp}"
    ))
}

fn stable_content_signature(path: &Path) -> Result<String, String> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok("missing".to_string());
        }
        Err(error) => return Err(format!("读取配置文件失败 {}: {error}", path.display())),
    };
    let hash = bytes.iter().fold(0xcbf29ce484222325_u64, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
    });
    Ok(format!("{}:{hash:016x}", bytes.len()))
}

fn parse_active_config_key(
    key: &str,
    _scope_trade_date: String,
    scope_signature: String,
) -> Option<ActiveConfigRecord> {
    let (algorithm_version, suffix) = key.split_once(':')?;
    let mut window = None;
    let mut pool = None;
    let mut outcome = None;
    let mut benchmark = None;
    for part in suffix.split(':') {
        if let Some(value) = part.strip_prefix('w') {
            window = value.parse::<usize>().ok();
        } else if let Some(value) = part.strip_prefix('p') {
            pool = value.parse::<usize>().ok();
        } else if let Some(value) = part.strip_prefix('h') {
            outcome = value.parse::<usize>().ok();
        } else if let Some(value) = part.strip_prefix('b') {
            benchmark = Some(value.to_string());
        }
    }
    Some(ActiveConfigRecord {
        config: StrategyTriggerSimilarityActiveConfig {
            algorithm_version: algorithm_version.to_string(),
            window_trade_days: window?,
            pool_segments: pool?,
            outcome_trade_days: outcome?,
            benchmark_index_code: benchmark?,
        },
        config_key: key.to_string(),
        scope_signature,
    })
}

fn load_active_config_record(conn: &Connection) -> Result<Option<ActiveConfigRecord>, String> {
    if !table_exists(conn, "strategy_trigger_similarity_active_config")? {
        return Ok(None);
    }
    let has_explicit_columns = [
        "algorithm_version",
        "window_trade_days",
        "pool_segments",
        "outcome_trade_days",
        "benchmark_index_code",
    ]
    .into_iter()
    .all(|column| {
        (|conn: &Connection, table_name: &str, column_name: &str| -> bool {
            conn.query_row(
                "SELECT COUNT(*) > 0 FROM information_schema.columns \
         WHERE table_schema='main' AND table_name=? AND column_name=?",
                params![table_name, column_name],
                |row| row.get(0),
            )
            .unwrap_or(false)
        })(conn, "strategy_trigger_similarity_active_config", column)
    });
    let query = if has_explicit_columns {
        "SELECT config_key, scope_trade_date, scope_signature, algorithm_version, \
         window_trade_days, pool_segments, outcome_trade_days, benchmark_index_code \
         FROM strategy_trigger_similarity_active_config WHERE id=1"
    } else {
        "SELECT config_key, scope_trade_date, scope_signature, NULL, NULL, NULL, NULL, NULL \
         FROM strategy_trigger_similarity_active_config WHERE id=1"
    };
    let row = conn
        .query_row(query, [], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, Option<i64>>(4)?,
                row.get::<_, Option<i64>>(5)?,
                row.get::<_, Option<i64>>(6)?,
                row.get::<_, Option<String>>(7)?,
            ))
        })
        .map(Some)
        .or_else(|error| match error {
            duckdb::Error::QueryReturnedNoRows => Ok(None),
            other => Err(other),
        })
        .map_err(|e| format!("读取走势相似生效配置失败: {e}"))?;
    Ok(row.and_then(
        |(key, date, signature, algorithm, window, pool, outcome, benchmark)| {
            let explicit = match (algorithm, window, pool, outcome, benchmark) {
                (Some(algorithm), Some(window), Some(pool), Some(outcome), Some(benchmark)) => {
                    Some(ActiveConfigRecord {
                        config: StrategyTriggerSimilarityActiveConfig {
                            algorithm_version: algorithm,
                            window_trade_days: usize::try_from(window).ok()?,
                            pool_segments: usize::try_from(pool).ok()?,
                            outcome_trade_days: usize::try_from(outcome).ok()?,
                            benchmark_index_code: benchmark,
                        },
                        config_key: key.clone(),
                        scope_signature: signature.clone(),
                    })
                }
                _ => None,
            };
            explicit.or_else(|| parse_active_config_key(&key, date, signature))
        },
    ))
}

pub fn get_strategy_trigger_similarity_active_config(
    conn: &Connection,
) -> Result<Option<StrategyTriggerSimilarityActiveConfig>, String> {
    if let Some(record) = load_active_config_record(conn)? {
        return Ok(Some(record.config));
    }
    // 兼容升级前已经生成过当前算法版本结果的数据库；只作为读取默认值，
    // 首次新写入时仍会补齐 active_config 和清理策略。
    if !table_exists(conn, "strategy_trigger_similarity_rank_meta")? {
        return Ok(None);
    }
    let key: Option<String> = conn
        .query_row(
            "SELECT config_key FROM strategy_trigger_similarity_rank_meta \
             ORDER BY generated_at_epoch_seconds DESC, trade_date DESC LIMIT 1",
            [],
            |row| row.get(0),
        )
        .map(Some)
        .or_else(|error| match error {
            duckdb::Error::QueryReturnedNoRows => Ok(None),
            other => Err(other),
        })
        .map_err(|e| format!("读取最新走势相似配置失败: {e}"))?;
    Ok(key.and_then(|key| {
        parse_active_config_key(&key, String::new(), String::new()).map(|record| record.config)
    }))
}

fn build_channel_fingerprint(vectors: Vec<Option<Vec<f64>>>) -> ChannelFingerprint {
    let mut has_vectors = false;
    let norms = vectors
        .iter()
        .map(|vector| {
            let Some(vector) = vector else {
                return 0.0;
            };
            has_vectors = true;
            vector_norm(vector)
        })
        .collect();
    ChannelFingerprint {
        vectors,
        norms,
        has_vectors,
    }
}

fn share_environment_fingerprints(
    fingerprints: HashMap<String, Vec<Option<Vec<f64>>>>,
) -> HashMap<String, Arc<ChannelFingerprint>> {
    fingerprints
        .into_iter()
        .map(|(trade_date, vectors)| (trade_date, Arc::new(build_channel_fingerprint(vectors))))
        .collect()
}

fn cached_channel_similarity(
    target: &ChannelFingerprint,
    candidate: &ChannelFingerprint,
) -> Option<f64> {
    let mut score_sum = 0.0;
    let mut score_count = 0;
    for (index, (left, right)) in target.vectors.iter().zip(&candidate.vectors).enumerate() {
        let (Some(left), Some(right)) = (left, right) else {
            continue;
        };
        let left_norm = target.norms[index];
        let right_norm = candidate.norms[index];
        let score = if left_norm <= EPS && right_norm <= EPS {
            100.0
        } else if left_norm <= EPS || right_norm <= EPS {
            0.0
        } else {
            let dot = dot_product(left, right);
            (50.0 * (1.0 + dot / (left_norm * right_norm))).clamp(0.0, 100.0)
        };
        score_sum += score;
        score_count += 1;
    }
    (score_count > 0).then(|| score_sum / score_count as f64)
}

#[allow(clippy::too_many_arguments)]
fn build_ranking_samples_for_chunk(
    conn: &Connection,
    anchors: Vec<Anchor>,
    schema: &MarketSchema,
    all_trade_dates: &[String],
    environment_fingerprints: &HashMap<String, Arc<ChannelFingerprint>>,
    benchmark_rows: &HashMap<String, BenchmarkObservation>,
    total_mv_map: &HashMap<String, f64>,
    name_map: &HashMap<String, String>,
    pool_segments: usize,
    outcome_trade_days: usize,
    target_trade_date: &str,
    include_outcome: bool,
) -> Result<Vec<RankingSample>, String> {
    let market_by_anchor = load_market_rows(conn, &anchors, schema)?;
    let rules_by_anchor = load_rule_rows(conn, &anchors)?;
    let summaries = load_summary_rows(conn, &anchors)?;
    let future_by_anchor = if include_outcome {
        load_future_rows(conn, &anchors, outcome_trade_days, target_trade_date)?
    } else {
        HashMap::new()
    };
    Ok(anchors
        .into_par_iter()
        .filter_map(|anchor| {
            let market_rows = market_by_anchor.get(&anchor.id)?;
            if market_rows.len() < 3
                || market_rows.last().map(|row| row.trade_date.as_str())
                    != Some(anchor.end_trade_date.as_str())
            {
                return None;
            }
            let rules = rules_by_anchor
                .get(&anchor.id)
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            let window_dates = window_dates_for_anchor(&anchor, all_trade_dates);
            let outcome = if include_outcome {
                build_outcome(
                    market_rows,
                    future_by_anchor
                        .get(&anchor.id)
                        .map(Vec::as_slice)
                        .unwrap_or(&[]),
                    outcome_trade_days,
                    benchmark_rows,
                )
            } else {
                None
            };
            if include_outcome && outcome.is_none() {
                return None;
            }
            let trigger = (|events: &[RuleEvent],
                            window_dates: &[String],
                            segments: usize|
             -> TriggerFingerprint {
                build_trigger_fingerprint(events, window_dates, segments)
            })(rules, window_dates, pool_segments);
            if trigger.by_rule.is_empty() {
                return None;
            }
            let (total_score, original_rank) =
                summaries.get(&anchor.id).copied().unwrap_or((None, None));
            Some(RankingSample {
                fingerprint: RankingFingerprint {
                    trigger,
                    price_volume: build_channel_fingerprint(build_price_volume_channels(
                        market_rows,
                        pool_segments,
                        total_mv_map.get(&anchor.ts_code).copied(),
                        market_category_features(
                            &anchor.ts_code,
                            name_map.get(&anchor.ts_code).map(String::as_str),
                        ),
                    )),
                    indicators: build_channel_fingerprint(build_indicator_channels(
                        market_rows,
                        schema.indicator_columns.len(),
                        pool_segments,
                    )),
                    market: environment_fingerprints
                        .get(&anchor.end_trade_date)
                        .cloned()
                        .unwrap_or_else(|| -> Arc<ChannelFingerprint> {
                            static EMPTY: OnceLock<Arc<ChannelFingerprint>> = OnceLock::new();
                            Arc::clone(
                                EMPTY.get_or_init(|| {
                                    Arc::new(build_channel_fingerprint(Vec::new()))
                                }),
                            )
                        }),
                },
                trigger_count: rules.len(),
                outcome,
                total_score,
                original_rank,
                template_quality_score: None,
                template_class: 0,
                anchor,
            })
        })
        .collect())
}

#[allow(clippy::too_many_arguments)]
fn load_outcome_selected_anchors(
    conn: &Connection,
    earliest_date: &str,
    cutoff_date: &str,
    target_date: &str,
    all_trade_dates: &[String],
    window_trade_days: usize,
    outcome_trade_days: usize,
    benchmark_index_code: &str,
) -> Result<(Vec<OutcomeSelectedAnchor>, usize), String> {
    let date_index = all_trade_dates
        .iter()
        .enumerate()
        .map(|(index, date)| (date.as_str(), index))
        .collect::<HashMap<_, _>>();
    let earliest_index = date_index
        .get(earliest_date)
        .copied()
        .ok_or_else(|| format!("历史起始日不在评分交易日中: {earliest_date}"))?;
    let cutoff_index = date_index
        .get(cutoff_date)
        .copied()
        .ok_or_else(|| format!("历史截止日不在评分交易日中: {cutoff_date}"))?;

    let benchmark_rows =
        load_benchmark_rows(conn, earliest_date, target_date, benchmark_index_code)?;
    let mut scored_dates = HashMap::<String, Vec<bool>>::new();
    let mut score_stmt = conn
        .prepare(
            "SELECT ts_code, trade_date FROM score_summary \
             WHERE trade_date>=? AND trade_date<=?",
        )
        .map_err(|e| format!("预编译评分日期扫描失败: {e}"))?;
    let mut score_rows = score_stmt
        .query(params![earliest_date, cutoff_date])
        .map_err(|e| format!("查询评分日期扫描失败: {e}"))?;
    while let Some(row) = score_rows
        .next()
        .map_err(|e| format!("读取评分日期扫描失败: {e}"))?
    {
        let ts_code: String = row.get(0).map_err(|e| format!("读取评分股票失败: {e}"))?;
        let trade_date: String = row.get(1).map_err(|e| format!("读取评分日期失败: {e}"))?;
        if let Some(index) = date_index.get(trade_date.as_str()) {
            scored_dates
                .entry(ts_code)
                .or_insert_with(|| vec![false; all_trade_dates.len()])[*index] = true;
        }
    }
    drop(score_rows);
    drop(score_stmt);

    let sql = r#"
        SELECT s.ts_code, s.trade_date,
               TRY_CAST(s.open AS DOUBLE), TRY_CAST(s.high AS DOUBLE),
               TRY_CAST(s.low AS DOUBLE), TRY_CAST(s.close AS DOUBLE),
               TRY_CAST(s.pct_chg AS DOUBLE)
        FROM trigger_market_db.stock_data s
        WHERE s.adj_type='qfq' AND s.trade_date>=? AND s.trade_date<=?
    "#;
    let mut stmt = conn
        .prepare(sql)
        .map_err(|e| format!("预编译线性未来表现扫描失败: {e}"))?;
    let mut rows = stmt
        .query(params![earliest_date, target_date])
        .map_err(|e| format!("查询线性未来表现扫描失败: {e}"))?;
    let mut stock_codes = Vec::<String>::new();
    let mut labels = Vec::<RawOutcomeLabel>::new();
    let mut paths = HashMap::<String, Vec<OutcomePathRow>>::new();

    let flush_stock = |ts_code: &str,
                       path: &mut Vec<OutcomePathRow>,
                       stock_codes: &mut Vec<String>,
                       labels: &mut Vec<RawOutcomeLabel>| {
        if ts_code.is_empty() || path.len() <= outcome_trade_days {
            path.clear();
            return;
        }
        let stock_index = stock_codes.len();
        stock_codes.push(ts_code.to_string());
        path.sort_unstable_by_key(|row| row.date_index);
        let Some(stock_scored_dates) = scored_dates.get(ts_code) else {
            path.clear();
            return;
        };
        for anchor_position in 0..path.len().saturating_sub(outcome_trade_days) {
            let anchor = &path[anchor_position];
            if anchor.date_index < earliest_index || anchor.date_index > cutoff_index {
                continue;
            }
            if !stock_scored_dates[anchor.date_index] {
                continue;
            }
            let future = &path[anchor_position + 1..=anchor_position + outcome_trade_days];
            if future.iter().any(|row| row.close.is_none()) {
                continue;
            }
            let Some(entry) = future[0]
                .open
                .filter(|value| value.is_finite() && value.abs() > EPS)
            else {
                continue;
            };
            let Some(exit) = future
                .last()
                .and_then(|row| row.close)
                .filter(|v| v.is_finite())
            else {
                continue;
            };
            let future_high = future
                .iter()
                .filter_map(|row| row.high.filter(|value| value.is_finite()))
                .fold(f64::NEG_INFINITY, f64::max);
            let future_low = future
                .iter()
                .filter_map(|row| row.low.filter(|value| value.is_finite()))
                .fold(f64::INFINITY, f64::min);
            if !future_high.is_finite() || !future_low.is_finite() {
                continue;
            }
            let Some(benchmark_start) = benchmark_rows.get(&all_trade_dates[future[0].date_index])
            else {
                continue;
            };
            let Some(benchmark_end) =
                benchmark_rows.get(&all_trade_dates[future.last().unwrap().date_index])
            else {
                continue;
            };
            if benchmark_start.open.abs() <= EPS {
                continue;
            }
            let return_pct = (exit / entry - 1.0) * 100.0;
            labels.push(RawOutcomeLabel {
                stock_index,
                date_index: anchor.date_index,
                excess_return_pct: return_pct
                    - (benchmark_end.close / benchmark_start.open - 1.0) * 100.0,
                mfe_pct: (future_high / entry - 1.0) * 100.0,
                mae_pct: (future_low / entry - 1.0) * 100.0,
                persistence: future
                    .iter()
                    .filter(|row| row.pct_chg.is_some_and(|value| value > 0.0))
                    .count() as f64
                    / outcome_trade_days as f64,
            });
        }
        path.clear();
    };

    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取线性未来表现失败: {e}"))?
    {
        let ts_code: String = row.get(0).map_err(|e| format!("读取股票代码失败: {e}"))?;
        let trade_date: String = row.get(1).map_err(|e| format!("读取行情日期失败: {e}"))?;
        let Some(row_date_index) = date_index.get(trade_date.as_str()).copied() else {
            continue;
        };
        paths.entry(ts_code).or_default().push(OutcomePathRow {
            date_index: row_date_index,
            open: row.get(2).map_err(|e| format!("读取开盘价失败: {e}"))?,
            high: row.get(3).map_err(|e| format!("读取最高价失败: {e}"))?,
            low: row.get(4).map_err(|e| format!("读取最低价失败: {e}"))?,
            close: row.get(5).map_err(|e| format!("读取收盘价失败: {e}"))?,
            pct_chg: row.get(6).map_err(|e| format!("读取涨跌幅失败: {e}"))?,
        });
    }
    drop(rows);
    drop(stmt);
    for (ts_code, mut path) in paths {
        flush_stock(&ts_code, &mut path, &mut stock_codes, &mut labels);
    }

    let mut labels_by_date = vec![Vec::<usize>::new(); all_trade_dates.len()];
    for (label_index, label) in labels.iter().enumerate() {
        labels_by_date[label.date_index].push(label_index);
    }
    let quality_pairs = labels_by_date
        .par_iter()
        .flat_map_iter(|indices| {
            if indices.is_empty() {
                return Vec::new();
            }
            let mut ranks = vec![[0.0; 4]; indices.len()];
            for component in 0..4 {
                let mut ordered = indices
                    .iter()
                    .enumerate()
                    .map(|(local_index, label_index)| {
                        let label = labels[*label_index];
                        let value = match component {
                            0 => label.excess_return_pct,
                            1 => label.mfe_pct,
                            2 => label.mae_pct,
                            _ => label.persistence,
                        };
                        (local_index, value)
                    })
                    .collect::<Vec<_>>();
                ordered.sort_unstable_by(|left, right| left.1.total_cmp(&right.1));
                let denominator = ordered.len().saturating_sub(1) as f64;
                let mut position = 0;
                while position < ordered.len() {
                    let mut end = position + 1;
                    while end < ordered.len()
                        && ordered[end].1.total_cmp(&ordered[position].1) == Ordering::Equal
                    {
                        end += 1;
                    }
                    let percent_rank = if denominator <= 0.0 {
                        0.0
                    } else {
                        position as f64 / denominator
                    };
                    for &(local_index, _) in &ordered[position..end] {
                        ranks[local_index][component] = percent_rank;
                    }
                    position = end;
                }
            }
            indices
                .iter()
                .enumerate()
                .map(|(local_index, label_index)| {
                    let rank = ranks[local_index];
                    (
                        *label_index,
                        0.45 * rank[0] + 0.25 * rank[1] + 0.20 * rank[2] + 0.10 * rank[3],
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let mut quality = vec![0.0; labels.len()];
    for (label_index, score) in quality_pairs {
        quality[label_index] = score;
    }

    let mut trigger_dates = HashMap::<String, Vec<bool>>::new();
    let mut trigger_stmt = conn
        .prepare(
            "SELECT ts_code, trade_date FROM rule_details \
             WHERE trade_date>=? AND trade_date<=? \
               AND TRY_CAST(rule_score AS DOUBLE) IS NOT NULL \
               AND ABS(TRY_CAST(rule_score AS DOUBLE)) > ?",
        )
        .map_err(|e| format!("预编译策略触发线性扫描失败: {e}"))?;
    let mut trigger_rows = trigger_stmt
        .query(params![&all_trade_dates[0], cutoff_date, EPS])
        .map_err(|e| format!("查询策略触发线性扫描失败: {e}"))?;
    while let Some(row) = trigger_rows
        .next()
        .map_err(|e| format!("读取策略触发线性扫描失败: {e}"))?
    {
        let ts_code: String = row.get(0).map_err(|e| format!("读取触发股票失败: {e}"))?;
        let trade_date: String = row.get(1).map_err(|e| format!("读取触发日期失败: {e}"))?;
        if let Some(index) = date_index.get(trade_date.as_str()) {
            trigger_dates
                .entry(ts_code)
                .or_insert_with(|| vec![false; all_trade_dates.len()])[*index] = true;
        }
    }

    #[derive(Clone, Copy)]
    struct SelectedLabel {
        stock_index: usize,
        date_index: usize,
        quality_score: f64,
        quality_class: i8,
    }
    let mut selected = Vec::<SelectedLabel>::new();
    let mut previous_stock = usize::MAX;
    let mut previous_quality = None;
    for (label_index, label) in labels.iter().enumerate() {
        if label.stock_index != previous_stock {
            previous_stock = label.stock_index;
            previous_quality = None;
        }
        let score = quality[label_index];
        let quality_class = if score >= SUCCESS_QUALITY_THRESHOLD
            && previous_quality.unwrap_or(0.0) < SUCCESS_QUALITY_THRESHOLD
        {
            1
        } else if score <= FAILURE_QUALITY_THRESHOLD
            && previous_quality.unwrap_or(1.0) > FAILURE_QUALITY_THRESHOLD
        {
            -1
        } else {
            0
        };
        previous_quality = Some(score);
        if quality_class == 0 {
            continue;
        }
        let stock_code = &stock_codes[label.stock_index];
        let window_start = (label.date_index + 1).saturating_sub(window_trade_days);
        let has_trigger = trigger_dates.get(stock_code).is_some_and(|dates| {
            dates[window_start..=label.date_index]
                .iter()
                .any(|triggered| *triggered)
        });
        if has_trigger {
            selected.push(SelectedLabel {
                stock_index: label.stock_index,
                date_index: label.date_index,
                quality_score: score,
                quality_class,
            });
        }
    }
    let universe_count = selected.len();

    fn stable_label_hash(stock: &str, date_index: usize) -> u64 {
        let mut hash = 0xcbf29ce484222325_u64;
        for byte in stock.bytes().chain(date_index.to_le_bytes()) {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
        hash
    }
    let mut chosen = Vec::<SelectedLabel>::new();
    for quality_class in [1_i8, -1_i8] {
        let mut class_rows = selected
            .iter()
            .copied()
            .filter(|row| row.quality_class == quality_class)
            .collect::<Vec<_>>();
        class_rows.sort_unstable_by(|left, right| {
            right.date_index.cmp(&left.date_index).then_with(|| {
                stable_label_hash(&stock_codes[left.stock_index], left.date_index).cmp(
                    &stable_label_hash(&stock_codes[right.stock_index], right.date_index),
                )
            })
        });
        let recent_limit = RECENT_CANDIDATE_ANCHORS / 2;
        let diverse_limit = HISTORY_DIVERSITY_ANCHORS / 2;
        let remaining = class_rows.split_off(class_rows.len().min(recent_limit));
        chosen.extend(class_rows);
        let mut diverse = remaining;
        diverse.sort_unstable_by_key(|row| {
            stable_label_hash(&stock_codes[row.stock_index], row.date_index)
        });
        chosen.extend(diverse.into_iter().take(diverse_limit));
    }

    chosen.sort_unstable_by(|left, right| {
        right
            .date_index
            .cmp(&left.date_index)
            .then_with(|| left.stock_index.cmp(&right.stock_index))
    });
    let anchors = chosen
        .into_iter()
        .enumerate()
        .map(|(id, selected)| {
            let start_index = (selected.date_index + 1).saturating_sub(window_trade_days);
            OutcomeSelectedAnchor {
                anchor: Anchor {
                    id,
                    ts_code: stock_codes[selected.stock_index].clone(),
                    start_trade_date: all_trade_dates[start_index].clone(),
                    end_trade_date: all_trade_dates[selected.date_index].clone(),
                },
                quality_score: selected.quality_score,
                quality_class: selected.quality_class,
            }
        })
        .collect();
    Ok((anchors, universe_count))
}

fn assign_ranks(rows: &mut [StrategyTriggerRankingRow]) {
    rows.sort_by(
        |left, right| match (left.prediction_signal, right.prediction_signal) {
            (Some(left_signal), Some(right_signal)) => right_signal
                .total_cmp(&left_signal)
                .then_with(|| {
                    right
                        .excess_positive_rate
                        .unwrap_or(f64::NEG_INFINITY)
                        .total_cmp(&left.excess_positive_rate.unwrap_or(f64::NEG_INFINITY))
                })
                .then_with(|| {
                    right
                        .best_similarity
                        .unwrap_or(f64::NEG_INFINITY)
                        .total_cmp(&left.best_similarity.unwrap_or(f64::NEG_INFINITY))
                })
                .then_with(|| left.ts_code.cmp(&right.ts_code)),
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => left.ts_code.cmp(&right.ts_code),
        },
    );
    let ranked_count = rows
        .iter()
        .filter(|row| row.prediction_signal.is_some())
        .count();
    for (index, row) in rows.iter_mut().take(ranked_count).enumerate() {
        row.rank = Some(index + 1);
        row.ranking_score = Some(if ranked_count <= 1 {
            100.0
        } else {
            100.0 * (ranked_count - 1 - index) as f64 / (ranked_count - 1) as f64
        });
    }
}

fn ensure_ranking_tables(conn: &Connection) -> Result<(), String> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS strategy_trigger_similarity_active_config (
            id TINYINT NOT NULL,
            config_key VARCHAR NOT NULL,
            algorithm_version VARCHAR NOT NULL,
            window_trade_days BIGINT NOT NULL,
            pool_segments BIGINT NOT NULL,
            outcome_trade_days BIGINT NOT NULL,
            benchmark_index_code VARCHAR NOT NULL,
            scope_trade_date VARCHAR NOT NULL,
            scope_signature VARCHAR NOT NULL,
            updated_at_epoch_seconds BIGINT NOT NULL,
            CONSTRAINT pk_strategy_similarity_active_config PRIMARY KEY (id),
            CONSTRAINT ck_strategy_similarity_active_config_singleton CHECK (id = 1)
        );
        CREATE TABLE IF NOT EXISTS strategy_trigger_similarity_rank_meta (
            trade_date VARCHAR NOT NULL,
            config_key VARCHAR NOT NULL,
            data_signature VARCHAR NOT NULL,
            generated_at_epoch_seconds BIGINT NOT NULL,
            historical_cutoff_date VARCHAR NOT NULL,
            universe_count BIGINT NOT NULL,
            ranked_count BIGINT NOT NULL,
            candidate_universe_count BIGINT NOT NULL,
            candidate_anchor_count BIGINT NOT NULL,
            evaluated_anchor_count BIGINT NOT NULL,
            elapsed_ms BIGINT NOT NULL,
            timings_json VARCHAR NOT NULL
        );
        CREATE TABLE IF NOT EXISTS strategy_trigger_similarity_rank (
            trade_date VARCHAR NOT NULL,
            config_key VARCHAR NOT NULL,
            rank BIGINT,
            ts_code VARCHAR NOT NULL,
            name VARCHAR,
            industry VARCHAR,
            concept VARCHAR,
            original_score DOUBLE,
            original_rank BIGINT,
            ranking_score DOUBLE,
            prediction_signal DOUBLE,
            confidence DOUBLE NOT NULL,
            sample_count BIGINT NOT NULL,
            effective_sample_count DOUBLE NOT NULL,
            expected_return_pct DOUBLE,
            expected_excess_return_pct DOUBLE,
            shrunk_excess_return_pct DOUBLE,
            excess_positive_rate DOUBLE,
            expected_mfe_pct DOUBLE,
            expected_mae_pct DOUBLE,
            average_similarity DOUBLE,
            best_similarity DOUBLE,
            trigger_count BIGINT NOT NULL,
            top_matches_json VARCHAR NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_strategy_similarity_rank_date_config_rank
          ON strategy_trigger_similarity_rank(trade_date, config_key, rank, ts_code);
        ALTER TABLE strategy_trigger_similarity_active_config
          ADD COLUMN IF NOT EXISTS algorithm_version VARCHAR;
        ALTER TABLE strategy_trigger_similarity_active_config
          ADD COLUMN IF NOT EXISTS window_trade_days BIGINT;
        ALTER TABLE strategy_trigger_similarity_active_config
          ADD COLUMN IF NOT EXISTS pool_segments BIGINT;
        ALTER TABLE strategy_trigger_similarity_active_config
          ADD COLUMN IF NOT EXISTS outcome_trade_days BIGINT;
        ALTER TABLE strategy_trigger_similarity_active_config
          ADD COLUMN IF NOT EXISTS benchmark_index_code VARCHAR;
        "#,
    )
    .map_err(|e| format!("创建策略相似排行榜表失败: {e}"))
}

fn table_exists(conn: &Connection, table_name: &str) -> Result<bool, String> {
    conn.query_row(
        "SELECT COUNT(*) > 0 FROM information_schema.tables WHERE table_schema='main' AND table_name=?",
        params![table_name],
        |row| row.get(0),
    )
    .map_err(|e| format!("检查相似排行榜表失败: {e}"))
}

fn parse_config_key(key: &str) -> Option<(usize, usize, usize, String)> {
    let suffix = key.strip_prefix(&format!("{ALGORITHM_VERSION}:"))?;
    let mut window = None;
    let mut pool = None;
    let mut outcome = None;
    let mut benchmark = None;
    for part in suffix.split(':') {
        if let Some(value) = part.strip_prefix('w') {
            window = value.parse::<usize>().ok();
        } else if let Some(value) = part.strip_prefix('p') {
            pool = value.parse::<usize>().ok();
        } else if let Some(value) = part.strip_prefix('h') {
            outcome = value.parse::<usize>().ok();
        } else if let Some(value) = part.strip_prefix('b') {
            benchmark = Some(value.to_string());
        }
    }
    Some((window?, pool?, outcome?, benchmark?))
}

#[allow(clippy::too_many_arguments)]
pub fn get_strategy_trigger_similarity_ranking_page(
    source_path: String,
    trade_date: Option<String>,
    window_trade_days: Option<u32>,
    pool_segments: Option<u32>,
    outcome_trade_days: Option<u32>,
    benchmark_index_code: Option<String>,
    limit: Option<u32>,
    board: Option<String>,
    exclude_st_board: Option<bool>,
    total_mv_min: Option<f64>,
    total_mv_max: Option<f64>,
) -> Result<StrategyTriggerRankingPageData, String> {
    let source_path = source_path.trim().to_string();
    if source_path.is_empty() {
        return Err("source_path 不能为空".to_string());
    }
    let conn = open_result_conn(&source_path)?;
    let resolved_trade_date = resolve_existing_trade_date(&conn, trade_date)?;
    let use_latest_config = window_trade_days.is_none()
        && pool_segments.is_none()
        && outcome_trade_days.is_none()
        && benchmark_index_code.is_none();
    let latest_config = if use_latest_config {
        get_strategy_trigger_similarity_active_config(&conn)?
            .map(|config| {
                (
                    config.window_trade_days,
                    config.pool_segments,
                    config.outcome_trade_days,
                    config.benchmark_index_code,
                )
            })
            .or_else(|| {
                (|conn: &Connection, trade_date: &str| -> Result<Option<String>, String> {
                    if !table_exists(conn, "strategy_trigger_similarity_rank_meta")? {
                        return Ok(None);
                    }
                    let mut stmt = conn
                        .prepare(
                            "SELECT config_key FROM strategy_trigger_similarity_rank_meta \
             WHERE trade_date=? AND config_key LIKE ? \
             ORDER BY generated_at_epoch_seconds DESC LIMIT 1",
                        )
                        .map_err(|e| format!("预编译最新走势相似配置读取失败: {e}"))?;
                    let mut rows = stmt
                        .query(params![trade_date, format!("{ALGORITHM_VERSION}:%")])
                        .map_err(|e| format!("查询最新走势相似配置失败: {e}"))?;
                    let Some(row) = rows
                        .next()
                        .map_err(|e| format!("读取最新走势相似配置失败: {e}"))?
                    else {
                        return Ok(None);
                    };
                    row.get(0)
                        .map(Some)
                        .map_err(|e| format!("读取最新走势相似配置键失败: {e}"))
                })(&conn, &resolved_trade_date)
                .ok()
                .and_then(|key| key.as_deref().and_then(parse_config_key))
            })
    } else {
        None
    };
    let window_trade_days = latest_config
        .as_ref()
        .map(|value| value.0)
        .or_else(|| window_trade_days.map(|v| v as usize).filter(|v| *v >= 3))
        .unwrap_or(DEFAULT_WINDOW_TRADE_DAYS);
    let pool_segments = latest_config
        .as_ref()
        .map(|value| value.1)
        .or_else(|| pool_segments.map(|v| v as usize).filter(|v| *v > 0))
        .unwrap_or(DEFAULT_POOL_SEGMENTS)
        .min(MAX_POOL_SEGMENTS)
        .min(window_trade_days);
    let outcome_trade_days = latest_config
        .as_ref()
        .map(|value| value.2)
        .or_else(|| outcome_trade_days.map(|v| v as usize).filter(|v| *v > 0))
        .unwrap_or(DEFAULT_OUTCOME_TRADE_DAYS);
    let benchmark_index_code = resolve_benchmark_index_code(
        latest_config
            .as_ref()
            .map(|value| value.3.as_str())
            .or(benchmark_index_code.as_deref()),
    )?;
    let limit = limit
        .map(|v| v as usize)
        .filter(|v| *v > 0)
        .unwrap_or(100)
        .min(5_000);
    let all_trade_dates = load_all_trade_dates(&conn)?;
    let target_index = all_trade_dates
        .binary_search(&resolved_trade_date)
        .map_err(|_| format!("参考日不在评分交易日中: {resolved_trade_date}"))?;
    if target_index < outcome_trade_days {
        return Err("参考日前没有足够历史区间".to_string());
    }
    let historical_cutoff_date = all_trade_dates[target_index - outcome_trade_days].clone();
    let key = config_key(
        window_trade_days,
        pool_segments,
        outcome_trade_days,
        &benchmark_index_code,
    );
    let current_signature = load_data_signature(&conn, &source_path, &resolved_trade_date)?;
    let meta = (|conn: &Connection,
                 trade_date: &str,
                 config_key: &str|
     -> Result<Option<RankingMeta>, String> {
        if !table_exists(conn, "strategy_trigger_similarity_rank_meta")? {
            return Ok(None);
        }
        let mut stmt = conn
            .prepare(
                r#"
            SELECT data_signature, generated_at_epoch_seconds, historical_cutoff_date,
                   universe_count, ranked_count, candidate_universe_count,
                   candidate_anchor_count, evaluated_anchor_count, elapsed_ms, timings_json
            FROM strategy_trigger_similarity_rank_meta
            WHERE trade_date=? AND config_key=?
            ORDER BY generated_at_epoch_seconds DESC LIMIT 1
            "#,
            )
            .map_err(|e| format!("预编译相似排行元数据读取失败: {e}"))?;
        let mut rows = stmt
            .query(params![trade_date, config_key])
            .map_err(|e| format!("查询相似排行元数据失败: {e}"))?;
        let Some(row) = rows
            .next()
            .map_err(|e| format!("读取相似排行元数据失败: {e}"))?
        else {
            return Ok(None);
        };
        let timings_json: String = row.get(9).map_err(|e| format!("读取计时信息失败: {e}"))?;
        Ok(Some(RankingMeta {
            data_signature: row.get(0).map_err(|e| format!("读取数据签名失败: {e}"))?,
            generated_at_epoch_seconds: row.get(1).map_err(|e| format!("读取生成时间失败: {e}"))?,
            historical_cutoff_date: row.get(2).map_err(|e| format!("读取历史截止日失败: {e}"))?,
            universe_count: row
                .get::<_, i64>(3)
                .map_err(|e| format!("读取股票池数量失败: {e}"))?
                .max(0) as usize,
            ranked_count: row
                .get::<_, i64>(4)
                .map_err(|e| format!("读取排行数量失败: {e}"))?
                .max(0) as usize,
            candidate_universe_count: row
                .get::<_, i64>(5)
                .map_err(|e| format!("读取候选全集失败: {e}"))?
                .max(0) as usize,
            candidate_anchor_count: row
                .get::<_, i64>(6)
                .map_err(|e| format!("读取候选锚点失败: {e}"))?
                .max(0) as usize,
            evaluated_anchor_count: row
                .get::<_, i64>(7)
                .map_err(|e| format!("读取有效锚点失败: {e}"))?
                .max(0) as usize,
            elapsed_ms: row
                .get::<_, i64>(8)
                .map_err(|e| format!("读取计算耗时失败: {e}"))?
                .max(0) as u64,
            timings: serde_json::from_str(&timings_json).unwrap_or_default(),
        }))
    })(&conn, &resolved_trade_date, &key)?;
    let is_fresh = meta
        .as_ref()
        .is_some_and(|value| value.data_signature == current_signature);
    let stale_reason = match &meta {
        None => Some("尚未计算该日期的全市场排行榜".to_string()),
        Some(value) if value.data_signature != current_signature => {
            Some("行情或策略触发数据已更新，已存排行榜自动失效".to_string())
        }
        Some(_) => None,
    };
    let items = if is_fresh {
        (|conn: &Connection,
          trade_date: &str,
          config_key: &str,
          limit: usize|
         -> Result<Vec<StrategyTriggerRankingRow>, String> {
            if !table_exists(conn, "strategy_trigger_similarity_rank")? {
                return Ok(Vec::new());
            }
            let mut stmt = conn
                .prepare(
                    r#"
            WITH ranked AS (
                SELECT rank, ts_code, name, industry, concept, original_score, original_rank,
                       ranking_score, prediction_signal, confidence, sample_count,
                       effective_sample_count, expected_return_pct, expected_excess_return_pct,
                       shrunk_excess_return_pct, excess_positive_rate, expected_mfe_pct,
                       expected_mae_pct, average_similarity, best_similarity, trigger_count,
                       top_matches_json
                FROM strategy_trigger_similarity_rank
                WHERE trade_date=? AND config_key=?
                ORDER BY rank NULLS LAST, ts_code
                LIMIT ?
            )
            SELECT r.*, b.best_rank_3d
            FROM ranked r
            LEFT JOIN (
                SELECT b3.ts_code, MIN(b3.rank) AS best_rank_3d
                FROM score_summary b3
                WHERE b3.rank IS NOT NULL AND b3.trade_date IN (
                    SELECT DISTINCT trade_date FROM score_summary
                    WHERE trade_date <= ? ORDER BY trade_date DESC LIMIT 3
                )
                GROUP BY b3.ts_code
            ) b ON b.ts_code = r.ts_code
            ORDER BY r.rank NULLS LAST, r.ts_code
            "#,
                )
                .map_err(|e| format!("预编译相似排行读取失败: {e}"))?;
            let mut rows = stmt
                .query(params![trade_date, config_key, limit as i64, trade_date])
                .map_err(|e| format!("查询相似排行失败: {e}"))?;
            let mut out = Vec::new();
            while let Some(row) = rows.next().map_err(|e| format!("读取相似排行失败: {e}"))?
            {
                let matches_json: String =
                    row.get(21).map_err(|e| format!("读取相似事件失败: {e}"))?;
                out.push(StrategyTriggerRankingRow {
                    rank: row
                        .get::<_, Option<i64>>(0)
                        .map_err(|e| format!("读取排名失败: {e}"))?
                        .map(|value| value.max(0) as usize),
                    ts_code: row.get(1).map_err(|e| format!("读取代码失败: {e}"))?,
                    name: row.get(2).map_err(|e| format!("读取名称失败: {e}"))?,
                    industry: row.get(3).map_err(|e| format!("读取行业失败: {e}"))?,
                    concept: row.get(4).map_err(|e| format!("读取概念失败: {e}"))?,
                    board: None,
                    original_score: row.get(5).map_err(|e| format!("读取原始分失败: {e}"))?,
                    original_rank: row.get(6).map_err(|e| format!("读取原始排名失败: {e}"))?,
                    best_rank_3d: row
                        .get(22)
                        .map_err(|e| format!("读取三日优排名失败: {e}"))?,
                    ranking_score: row.get(7).map_err(|e| format!("读取排行分失败: {e}"))?,
                    prediction_signal: row.get(8).map_err(|e| format!("读取预测信号失败: {e}"))?,
                    confidence: row.get(9).map_err(|e| format!("读取置信度失败: {e}"))?,
                    sample_count: row
                        .get::<_, i64>(10)
                        .map_err(|e| format!("读取样本数失败: {e}"))?
                        .max(0) as usize,
                    effective_sample_count: row
                        .get(11)
                        .map_err(|e| format!("读取有效样本失败: {e}"))?,
                    expected_return_pct: row
                        .get(12)
                        .map_err(|e| format!("读取预期收益失败: {e}"))?,
                    expected_excess_return_pct: row
                        .get(13)
                        .map_err(|e| format!("读取预期超额失败: {e}"))?,
                    shrunk_excess_return_pct: row
                        .get(14)
                        .map_err(|e| format!("读取收缩超额失败: {e}"))?,
                    excess_positive_rate: row
                        .get(15)
                        .map_err(|e| format!("读取超额胜率失败: {e}"))?,
                    expected_mfe_pct: row.get(16).map_err(|e| format!("读取MFE失败: {e}"))?,
                    expected_mae_pct: row.get(17).map_err(|e| format!("读取MAE失败: {e}"))?,
                    average_similarity: row
                        .get(18)
                        .map_err(|e| format!("读取平均相似度失败: {e}"))?,
                    best_similarity: row
                        .get(19)
                        .map_err(|e| format!("读取最佳相似度失败: {e}"))?,
                    trigger_count: row
                        .get::<_, i64>(20)
                        .map_err(|e| format!("读取触发数失败: {e}"))?
                        .max(0) as usize,
                    total_mv_yi: None,
                    top_matches: serde_json::from_str(&matches_json).unwrap_or_default(),
                });
            }
            Ok(out)
        })(&conn, &resolved_trade_date, &key, limit)?
    } else {
        Vec::new()
    };
    let name_map = build_name_map(&source_path).unwrap_or_default();
    let total_mv_map = build_total_mv_map(&source_path).unwrap_or_default();
    let exclude_st_board = exclude_st_board.unwrap_or(false);
    let board_filter = board
        .as_ref()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty() && value != "全部");
    let mut items = items;
    items = items
        .into_iter()
        .filter_map(|mut row| {
            let board_value = board_category(
                &row.ts_code,
                name_map.get(&row.ts_code).map(|value| value.as_str()),
            )
            .to_string();
            if exclude_st_board && board_value == "ST" {
                return None;
            }
            if let Some(ref board_value_filter) = board_filter {
                if &board_value != board_value_filter {
                    return None;
                }
            }
            let total_mv = total_mv_map.get(&row.ts_code).copied();
            if let Some(min_v) = total_mv_min {
                if total_mv.unwrap_or(f64::NEG_INFINITY) < min_v {
                    return None;
                }
            }
            if let Some(max_v) = total_mv_max {
                if total_mv.unwrap_or(f64::NEG_INFINITY) > max_v {
                    return None;
                }
            }
            row.board = Some(board_value);
            row.total_mv_yi = total_mv;
            Some(row)
        })
        .collect();
    Ok(StrategyTriggerRankingPageData {
        resolved_trade_date,
        historical_cutoff_date: meta
            .as_ref()
            .map(|value| value.historical_cutoff_date.clone())
            .unwrap_or(historical_cutoff_date),
        window_trade_days,
        pool_segments,
        outcome_trade_days,
        benchmark_index_code,
        algorithm_version: ALGORITHM_VERSION.to_string(),
        data_signature: current_signature,
        generated_at_epoch_seconds: meta.as_ref().map(|value| value.generated_at_epoch_seconds),
        is_fresh,
        stale_reason,
        universe_count: meta.as_ref().map_or(0, |value| value.universe_count),
        ranked_count: meta.as_ref().map_or(0, |value| value.ranked_count),
        candidate_universe_count: meta
            .as_ref()
            .map_or(0, |value| value.candidate_universe_count),
        candidate_anchor_count: meta
            .as_ref()
            .map_or(0, |value| value.candidate_anchor_count),
        evaluated_anchor_count: meta
            .as_ref()
            .map_or(0, |value| value.evaluated_anchor_count),
        elapsed_ms: meta.as_ref().map(|value| value.elapsed_ms),
        timings: meta.map_or_else(Vec::new, |value| value.timings),
        items,
    })
}

#[allow(clippy::too_many_arguments)]
pub fn run_strategy_trigger_similarity_ranking(
    source_path: String,
    trade_date: Option<String>,
    window_trade_days: Option<u32>,
    pool_segments: Option<u32>,
    outcome_trade_days: Option<u32>,
    benchmark_index_code: Option<String>,
    limit: Option<u32>,
    board: Option<String>,
    exclude_st_board: Option<bool>,
    total_mv_min: Option<f64>,
    total_mv_max: Option<f64>,
) -> Result<StrategyTriggerRankingPageData, String> {
    let _guard = RANKING_COMPUTE_LOCK
        .get_or_init(|| Mutex::new(()))
        .try_lock()
        .map_err(|_| "全市场相似排行榜正在计算，请等待当前任务完成".to_string())?;
    let started = Instant::now();
    let source_path = source_path.trim().to_string();
    if source_path.is_empty() {
        return Err("source_path 不能为空".to_string());
    }
    let conn = open_result_conn(&source_path)?;
    let resolved_trade_date = resolve_existing_trade_date(&conn, trade_date)?;
    let active_config = get_strategy_trigger_similarity_active_config(&conn)?;
    let window_trade_days = window_trade_days
        .map(|v| v as usize)
        .filter(|v| *v >= 3)
        .or_else(|| {
            active_config
                .as_ref()
                .map(|config| config.window_trade_days)
        })
        .unwrap_or(DEFAULT_WINDOW_TRADE_DAYS);
    let pool_segments = pool_segments
        .map(|v| v as usize)
        .filter(|v| *v > 0)
        .or_else(|| active_config.as_ref().map(|config| config.pool_segments))
        .unwrap_or(DEFAULT_POOL_SEGMENTS)
        .min(MAX_POOL_SEGMENTS)
        .min(window_trade_days);
    let outcome_trade_days = outcome_trade_days
        .map(|v| v as usize)
        .filter(|v| *v > 0)
        .or_else(|| {
            active_config
                .as_ref()
                .map(|config| config.outcome_trade_days)
        })
        .unwrap_or(DEFAULT_OUTCOME_TRADE_DAYS);
    let benchmark_index_code =
        resolve_benchmark_index_code(benchmark_index_code.as_deref().or_else(|| {
            active_config
                .as_ref()
                .map(|config| config.benchmark_index_code.as_str())
        }))?;
    let initial_signature = load_data_signature(&conn, &source_path, &resolved_trade_date)?;
    let mut timings = Vec::new();

    let phase = Instant::now();
    let all_trade_dates = load_all_trade_dates(&conn)?;
    let target_index = all_trade_dates
        .binary_search(&resolved_trade_date)
        .map_err(|_| format!("参考日不在评分交易日中: {resolved_trade_date}"))?;
    if target_index < outcome_trade_days {
        return Err("参考日前没有足够历史区间".to_string());
    }
    let target_start_index = (target_index + 1).saturating_sub(window_trade_days);
    let target_start_date = all_trade_dates[target_start_index].clone();
    let historical_cutoff_date = all_trade_dates[target_index - outcome_trade_days].clone();
    let earliest_candidate_date = all_trade_dates
        .get(window_trade_days.saturating_sub(1))
        .map(String::as_str)
        .unwrap_or(&all_trade_dates[0]);
    let schema = load_market_schema(&conn)?;
    let first_date = all_trade_dates
        .first()
        .map(String::as_str)
        .unwrap_or(&target_start_date);
    let environment = load_market_environment(&conn, first_date, &resolved_trade_date, &schema)?;
    let environment_fingerprints =
        share_environment_fingerprints(build_environment_fingerprint_map(
            &environment,
            &all_trade_dates,
            window_trade_days,
            pool_segments,
        ));
    let benchmark_rows = load_benchmark_rows(
        &conn,
        first_date,
        &resolved_trade_date,
        &benchmark_index_code,
    )?;
    let total_mv_map = build_total_mv_map(&source_path).unwrap_or_default();
    let name_map = build_name_map(&source_path).unwrap_or_default();
    timings.push(StrategyTriggerRankingTiming {
        label: "市场环境与基准".to_string(),
        elapsed_ms: elapsed_ms(phase),
    });

    let phase = Instant::now();
    let (selected_anchors, candidate_universe_count) = load_outcome_selected_anchors(
        &conn,
        earliest_candidate_date,
        &historical_cutoff_date,
        &resolved_trade_date,
        &all_trade_dates,
        window_trade_days,
        outcome_trade_days,
        &benchmark_index_code,
    )?;
    let selected_quality = selected_anchors
        .iter()
        .map(|selected| {
            (
                selected.anchor.id,
                (selected.quality_score, selected.quality_class),
            )
        })
        .collect::<HashMap<_, _>>();
    let candidate_anchor_count = selected_anchors.len();
    let candidate_anchors = selected_anchors
        .into_iter()
        .map(|selected| selected.anchor)
        .collect::<Vec<_>>();
    let mut candidates = Vec::with_capacity(candidate_anchor_count);
    let mut candidate_anchor_iter = candidate_anchors.into_iter();
    loop {
        let chunk = candidate_anchor_iter
            .by_ref()
            .take(ANCHOR_CHUNK_SIZE)
            .collect::<Vec<_>>();
        if chunk.is_empty() {
            break;
        }
        let mut chunk_samples = build_ranking_samples_for_chunk(
            &conn,
            chunk,
            &schema,
            &all_trade_dates,
            &environment_fingerprints,
            &benchmark_rows,
            &total_mv_map,
            &name_map,
            pool_segments,
            outcome_trade_days,
            &resolved_trade_date,
            true,
        )?;
        for sample in &mut chunk_samples {
            if let Some((quality_score, quality_class)) = selected_quality.get(&sample.anchor.id) {
                sample.template_quality_score = Some(*quality_score);
                sample.template_class = *quality_class;
            }
        }
        candidates.extend(chunk_samples);
    }
    let evaluated_anchor_count = candidates.len();
    timings.push(StrategyTriggerRankingTiming {
        label: "表现反推启动模板与指纹".to_string(),
        elapsed_ms: elapsed_ms(phase),
    });

    let phase = Instant::now();
    let target_anchors = (|conn: &Connection,
                           target_date: &str,
                           start_date: &str|
     -> Result<Vec<Anchor>, String> {
        let mut stmt = conn
        .prepare(
            "SELECT ts_code FROM score_summary WHERE trade_date = ? ORDER BY rank NULLS LAST, ts_code",
        )
        .map_err(|e| format!("预编译当日股票池失败: {e}"))?;
        let rows = stmt
            .query_map(params![target_date], |row| row.get::<_, String>(0))
            .map_err(|e| format!("查询当日股票池失败: {e}"))?;
        rows.enumerate()
            .map(|(id, row)| {
                row.map(|ts_code| Anchor {
                    id,
                    ts_code,
                    start_trade_date: start_date.to_string(),
                    end_trade_date: target_date.to_string(),
                })
                .map_err(|e| format!("读取当日股票池失败: {e}"))
            })
            .collect()
    })(&conn, &resolved_trade_date, &target_start_date)?;
    let target_anchor_count = target_anchors.len();
    let mut targets = Vec::with_capacity(target_anchor_count);
    let mut target_anchor_iter = target_anchors.into_iter();
    loop {
        let chunk = target_anchor_iter
            .by_ref()
            .take(ANCHOR_CHUNK_SIZE)
            .collect::<Vec<_>>();
        if chunk.is_empty() {
            break;
        }
        targets.extend(build_ranking_samples_for_chunk(
            &conn,
            chunk,
            &schema,
            &all_trade_dates,
            &environment_fingerprints,
            &benchmark_rows,
            &total_mv_map,
            &name_map,
            pool_segments,
            outcome_trade_days,
            &resolved_trade_date,
            false,
        )?);
    }
    timings.push(StrategyTriggerRankingTiming {
        label: "当日全市场指纹".to_string(),
        elapsed_ms: elapsed_ms(phase),
    });

    let mut candidate_by_rule = HashMap::<&str, Vec<usize>>::new();
    for (index, candidate) in candidates.iter().enumerate() {
        for rule_name in candidate.fingerprint.trigger.by_rule.keys() {
            candidate_by_rule
                .entry(rule_name.as_str())
                .or_default()
                .push(index);
        }
    }
    let name_map = build_name_map(&source_path).unwrap_or_default();
    let industry_map = build_industry_map(&source_path).unwrap_or_default();
    let concept_map = build_concepts_map(&source_path).unwrap_or_default();
    let rule_weights =
        load_rule_idf_weights(&conn, earliest_candidate_date, &historical_cutoff_date)?;
    let candidate_rule_weight_sums = candidates
        .par_iter()
        .map(|candidate| trigger_rule_weight_sum(&candidate.fingerprint.trigger, &rule_weights))
        .collect::<Vec<_>>();

    let phase = Instant::now();
    let candidate_market_similarities =
        (|targets: &[RankingSample], candidates: &[RankingSample]| -> Vec<Option<f64>> {
            let Some(target) = targets.first() else {
                return vec![None; candidates.len()];
            };
            debug_assert!(targets.iter().all(|other| {
                other.anchor.end_trade_date == target.anchor.end_trade_date
                    && Arc::ptr_eq(&other.fingerprint.market, &target.fingerprint.market)
            }));

            // 市场指纹只由交易日决定。同一批目标共用参考日，因此每个候选交易日只需
            // 计算一次市场相似度，再展开成候选下标数组供精排直接索引。
            let mut similarity_by_date = HashMap::<&str, Option<f64>>::new();
            for candidate in candidates {
                similarity_by_date
                    .entry(candidate.anchor.end_trade_date.as_str())
                    .or_insert_with(|| {
                        cached_channel_similarity(
                            &target.fingerprint.market,
                            &candidate.fingerprint.market,
                        )
                    });
            }
            candidates
                .iter()
                .map(|candidate| {
                    similarity_by_date
                        .get(candidate.anchor.end_trade_date.as_str())
                        .copied()
                        .flatten()
                })
                .collect()
        })(&targets, &candidates);
    let mut ranking_rows = targets
        .par_iter()
        .map(|target| {
            (|target: &RankingSample,
              candidates: &[RankingSample],
              candidate_market_similarities: &[Option<f64>],
              candidate_rule_weight_sums: &[f64],
              candidate_by_rule: &HashMap<&str, Vec<usize>>,
              all_trade_dates: &[String],
              window_trade_days: usize,
              outcome_trade_days: usize,
              name_map: &HashMap<String, String>,
              industry_map: &HashMap<String, String>,
              concept_map: &HashMap<String, String>,
              rule_weights: &HashMap<String, f64>|
             -> StrategyTriggerRankingRow {
                RANKING_TARGET_SCRATCH.with(|scratch| {
                    (|target: &RankingSample,
                      candidates: &[RankingSample],
                      candidate_market_similarities: &[Option<f64>],
                      candidate_rule_weight_sums: &[f64],
                      candidate_by_rule: &HashMap<&str, Vec<usize>>,
                      all_trade_dates: &[String],
                      window_trade_days: usize,
                      outcome_trade_days: usize,
                      name_map: &HashMap<String, String>,
                      industry_map: &HashMap<String, String>,
                      concept_map: &HashMap<String, String>,
                      rule_weights: &HashMap<String, f64>,
                      scratch: &mut RankingTargetScratch|
                     -> StrategyTriggerRankingRow {
                        let target_rule_weight_sum =
                            trigger_rule_weight_sum(&target.fingerprint.trigger, rule_weights);
                        let per_class_limit = (256) / 2;
                        // 线程内复用候选标记、规则交集权重和精排堆。代数标记让每个目标只写入
                        // 实际命中的候选，不再全量清零 candidates.len() 个浮点数。
                        scratch.prepare(candidates.len(), per_class_limit);
                        for rule_name in target.fingerprint.trigger.by_rule.keys() {
                            let Some(indices) = candidate_by_rule.get(rule_name.as_str()) else {
                                continue;
                            };
                            let weight = rule_weight(rule_weights, rule_name);
                            for &candidate_index in indices {
                                scratch.add_candidate_rule_weight(candidate_index, weight);
                            }
                        }

                        for candidate_position in 0..scratch.candidate_indices.len() {
                            let candidate_index = scratch.candidate_indices[candidate_position];
                            let candidate = &candidates[candidate_index];
                            // Leave-one-stock-out：同一股票的滚动窗口会共享真实 K 线、触发和静态
                            // 特征，不能作为自己的历史近邻，否则会形成股票身份与窗口重叠泄漏。
                            if candidate.template_class == 0
                                || candidate.anchor.ts_code == target.anchor.ts_code
                            {
                                continue;
                            }
                            let price_available = target.fingerprint.price_volume.has_vectors
                                && candidate.fingerprint.price_volume.has_vectors;
                            let indicator_available = target.fingerprint.indicators.has_vectors
                                && candidate.fingerprint.indicators.has_vectors;
                            let market_available = target.fingerprint.market.has_vectors
                                && candidate.fingerprint.market.has_vectors;
                            let total_weight = TRIGGER_SIMILARITY_WEIGHT
                                + if price_available {
                                    PRICE_VOLUME_SIMILARITY_WEIGHT
                                } else {
                                    0.0
                                }
                                + if indicator_available {
                                    INDICATOR_SIMILARITY_WEIGHT
                                } else {
                                    0.0
                                }
                                + if market_available {
                                    MARKET_SIMILARITY_WEIGHT
                                } else {
                                    0.0
                                };
                            let cutoff = (|template_class: i8,
                                           success_heap: &BinaryHeap<Reverse<ScoredCandidate>>,
                                           failure_heap: &BinaryHeap<Reverse<ScoredCandidate>>,
                                           limit: usize|
                             -> Option<f64> {
                                let heap = if template_class > 0 {
                                    success_heap
                                } else if template_class < 0 {
                                    failure_heap
                                } else {
                                    return Some(f64::INFINITY);
                                };
                                (heap.len() >= limit).then(|| {
                                    heap.peek().map_or(f64::NEG_INFINITY, |row| row.0.score)
                                })
                            })(
                                candidate.template_class,
                                &scratch.success_heap,
                                &scratch.failure_heap,
                                per_class_limit,
                            );
                            let rule_set = weighted_rule_set_similarity_from_masses(
                                target_rule_weight_sum,
                                candidate_rule_weight_sums[candidate_index],
                                scratch.candidate_intersection_weights[candidate_index],
                            );
                            let aggregate = trigger_aggregate_similarity(
                                &target.fingerprint.trigger,
                                &candidate.fingerprint.trigger,
                            );
                            let trigger_upper_bound =
                                combine_trigger_similarity(rule_set, 1.0, aggregate);
                            let remaining_weight = total_weight - TRIGGER_SIMILARITY_WEIGHT;
                            if can_prune_exact_candidate(
                                (trigger_upper_bound * TRIGGER_SIMILARITY_WEIGHT
                                    + remaining_weight * 100.0)
                                    / total_weight,
                                cutoff,
                            ) {
                                continue;
                            }
                            // 先计算线性点积通道，再由真实通道分反推时序项必须达到的最低分。
                            // 只有仍可能进入堆的候选才执行昂贵的触发序列 DP。
                            let mut channel_weighted_score = 0.0;
                            let mut remaining_weight = total_weight - TRIGGER_SIMILARITY_WEIGHT;

                            let price_volume_similarity = price_available
                                .then(|| {
                                    cached_channel_similarity(
                                        &target.fingerprint.price_volume,
                                        &candidate.fingerprint.price_volume,
                                    )
                                })
                                .flatten();
                            if let Some(score) = price_volume_similarity {
                                channel_weighted_score += score * PRICE_VOLUME_SIMILARITY_WEIGHT;
                                remaining_weight -= PRICE_VOLUME_SIMILARITY_WEIGHT;
                            }
                            if can_prune_exact_candidate(
                                (trigger_upper_bound * TRIGGER_SIMILARITY_WEIGHT
                                    + channel_weighted_score
                                    + remaining_weight * 100.0)
                                    / total_weight,
                                cutoff,
                            ) {
                                continue;
                            }

                            let market_similarity = market_available
                                .then(|| candidate_market_similarities[candidate_index])
                                .flatten();
                            if let Some(score) = market_similarity {
                                channel_weighted_score += score * MARKET_SIMILARITY_WEIGHT;
                                remaining_weight -= MARKET_SIMILARITY_WEIGHT;
                            }
                            if can_prune_exact_candidate(
                                (trigger_upper_bound * TRIGGER_SIMILARITY_WEIGHT
                                    + channel_weighted_score
                                    + remaining_weight * 100.0)
                                    / total_weight,
                                cutoff,
                            ) {
                                continue;
                            }

                            let indicator_similarity = indicator_available
                                .then(|| {
                                    cached_channel_similarity(
                                        &target.fingerprint.indicators,
                                        &candidate.fingerprint.indicators,
                                    )
                                })
                                .flatten();
                            if let Some(score) = indicator_similarity {
                                channel_weighted_score += score * INDICATOR_SIMILARITY_WEIGHT;
                                remaining_weight -= INDICATOR_SIMILARITY_WEIGHT;
                            }
                            if can_prune_exact_candidate(
                                (trigger_upper_bound * TRIGGER_SIMILARITY_WEIGHT
                                    + channel_weighted_score
                                    + remaining_weight * 100.0)
                                    / total_weight,
                                cutoff,
                            ) {
                                continue;
                            }

                            let minimum_timing =
                                cutoff.map_or(f64::NEG_INFINITY, |minimum_score| {
                                    let minimum_trigger = (minimum_score * total_weight
                                        - channel_weighted_score)
                                        / TRIGGER_SIMILARITY_WEIGHT;
                                    (minimum_trigger / 100.0
                                        - rule_set * TRIGGER_RULE_SET_WEIGHT
                                        - aggregate * TRIGGER_AGGREGATE_RHYTHM_WEIGHT)
                                        / TRIGGER_RULE_TIMING_WEIGHT
                                });
                            let Some(timing) = weighted_rule_timing_similarity_with_minimum(
                                &target.fingerprint.trigger,
                                &candidate.fingerprint.trigger,
                                rule_weights,
                                minimum_timing,
                            ) else {
                                continue;
                            };
                            let trigger_similarity =
                                combine_trigger_similarity(rule_set, timing, aggregate);
                            let scored = ScoredCandidate {
                                score: final_similarity(
                                    trigger_similarity,
                                    price_volume_similarity,
                                    indicator_similarity,
                                    market_similarity,
                                ),
                                trigger_similarity,
                                price_volume_similarity,
                                indicator_similarity,
                                market_similarity,
                                candidate_index,
                            };
                            if candidate.template_class > 0 {
                                push_top_candidate(
                                    &mut scratch.success_heap,
                                    scored,
                                    per_class_limit,
                                );
                            } else if candidate.template_class < 0 {
                                push_top_candidate(
                                    &mut scratch.failure_heap,
                                    scored,
                                    per_class_limit,
                                );
                            }
                        }

                        let mut scored_candidates = scratch
                            .success_heap
                            .drain()
                            .chain(scratch.failure_heap.drain())
                            .map(|item| item.0)
                            .collect::<Vec<_>>();
                        scored_candidates.sort_by(|left, right| right.cmp(left));
                        let rating_candidates = (|sorted_candidates: &[ScoredCandidate],
                                                  candidates: &[RankingSample],
                                                  all_trade_dates: &[String],
                                                  window_trade_days: usize,
                                                  outcome_trade_days: usize|
                         -> Vec<ScoredCandidate> {
                            let same_stock_exclusion =
                                window_trade_days.max(outcome_trade_days).max(1);
                            let outcome_exclusion = outcome_trade_days.max(1);
                            let mut selected = Vec::with_capacity(RATING_SAMPLE_LIMIT);
                            let mut selected_end_indices =
                                Vec::<usize>::with_capacity(RATING_SAMPLE_LIMIT);
                            let mut selected_by_stock = HashMap::<&str, Vec<usize>>::new();

                            for scored in sorted_candidates {
                                let candidate = &candidates[scored.candidate_index];
                                if candidate
                                    .outcome
                                    .as_ref()
                                    .and_then(|outcome| outcome.excess_return_pct)
                                    .is_none()
                                {
                                    continue;
                                }
                                let Ok(end_index) =
                                    all_trade_dates.binary_search(&candidate.anchor.end_trade_date)
                                else {
                                    continue;
                                };
                                let nearby_outcome_count = selected_end_indices
                                    .iter()
                                    .filter(|selected_index| {
                                        selected_index.abs_diff(end_index) < outcome_exclusion
                                    })
                                    .count();
                                if nearby_outcome_count >= RATING_MAX_PER_OUTCOME_WINDOW {
                                    continue;
                                }
                                if selected_by_stock
                                    .get(candidate.anchor.ts_code.as_str())
                                    .is_some_and(|indices| {
                                        indices.iter().any(|selected_index| {
                                            selected_index.abs_diff(end_index)
                                                < same_stock_exclusion
                                        })
                                    })
                                {
                                    continue;
                                }
                                selected_end_indices.push(end_index);
                                selected_by_stock
                                    .entry(candidate.anchor.ts_code.as_str())
                                    .or_default()
                                    .push(end_index);
                                selected.push(*scored);
                                if selected.len() >= RATING_SAMPLE_LIMIT {
                                    break;
                                }
                            }
                            selected
                        })(
                            &scored_candidates,
                            candidates,
                            all_trade_dates,
                            window_trade_days,
                            outcome_trade_days,
                        );
                        let rating_sample = rating_candidates
                            .iter()
                            .copied()
                            .filter_map(|scored| {
                                (|candidate: &RankingSample,
              scored: ScoredCandidate,
              target_trigger: &TriggerFingerprint,
              name_map: &HashMap<String, String>|
             -> Option<StrategyTriggerSimilarityRow> {
                let outcome = candidate.outcome.as_ref()?;
                let mut matched_rule_names = target_trigger
                    .by_rule
                    .keys()
                    .filter(|name| candidate.fingerprint.trigger.by_rule.contains_key(*name))
                    .cloned()
                    .collect::<Vec<_>>();
                matched_rule_names.sort();
                Some(StrategyTriggerSimilarityRow {
                    ts_code: candidate.anchor.ts_code.clone(),
                    name: name_map.get(&candidate.anchor.ts_code).cloned(),
                    industry: None,
                    concept: None,
                    candidate_start_trade_date: candidate.anchor.start_trade_date.clone(),
                    candidate_end_trade_date: candidate.anchor.end_trade_date.clone(),
                    outcome_start_trade_date: outcome.start_trade_date.clone(),
                    outcome_end_trade_date: outcome.end_trade_date.clone(),
                    similarity_score: scored.score,
                    trigger_similarity: scored.trigger_similarity,
                    price_volume_similarity: scored.price_volume_similarity,
                    indicator_similarity: scored.indicator_similarity,
                    market_similarity: scored.market_similarity,
                    matched_rule_count: matched_rule_names.len(),
                    matched_rule_names,
                    candidate_trigger_count: candidate.trigger_count,
                    forward_return_pct: outcome.return_pct,
                    forward_excess_return_pct: outcome.excess_return_pct,
                    mfe_pct: outcome.mfe_pct,
                    mae_pct: outcome.mae_pct,
                    total_score: candidate.total_score,
                    rank: candidate.original_rank,
                })
            })(
                &candidates[scored.candidate_index],
                scored,
                &target.fingerprint.trigger,
                name_map,
            )
                            })
                            .collect::<Vec<_>>();
                        let summary = summarize_outcomes(&rating_sample);
                        let confidence = (summary.effective_sample_count
                            / (summary.effective_sample_count + SHRINKAGE_STRENGTH))
                            .max(0.0)
                            .sqrt();
                        let (quality_weighted_sum, quality_weight_sum) = rating_candidates
                            .iter()
                            .filter_map(|scored| {
                                let quality =
                                    candidates[scored.candidate_index].template_quality_score?;
                                let weight = (scored.score / 100.0).powi(2);
                                (weight > EPS).then_some((quality * weight, weight))
                            })
                            .fold((0.0, 0.0), |(value_sum, weight_sum), (value, weight)| {
                                (value_sum + value, weight_sum + weight)
                            });
                        let predicted_quality = (quality_weight_sum > EPS)
                            .then_some(quality_weighted_sum / quality_weight_sum);
                        let prediction_signal = (summary.sample_count >= (5)
                            && summary.effective_sample_count >= (3.0))
                            .then(|| {
                                predicted_quality.map(|quality| (quality - 0.5) * 2.0 * confidence)
                            })
                            .flatten();
                        let average_similarity = (!rating_sample.is_empty()).then(|| {
                            rating_sample
                                .iter()
                                .map(|row| row.similarity_score)
                                .sum::<f64>()
                                / rating_sample.len() as f64
                        });
                        let top_matches = rating_sample
                            .iter()
                            .take(5)
                            .map(|row| StrategyTriggerRankingMatch {
                                ts_code: row.ts_code.clone(),
                                name: row.name.clone(),
                                candidate_start_trade_date: row.candidate_start_trade_date.clone(),
                                candidate_end_trade_date: row.candidate_end_trade_date.clone(),
                                similarity_score: row.similarity_score,
                                forward_excess_return_pct: row.forward_excess_return_pct,
                                mfe_pct: row.mfe_pct,
                                mae_pct: row.mae_pct,
                            })
                            .collect();
                        StrategyTriggerRankingRow {
                            rank: None,
                            ts_code: target.anchor.ts_code.clone(),
                            name: name_map.get(&target.anchor.ts_code).cloned(),
                            industry: industry_map.get(&target.anchor.ts_code).cloned(),
                            concept: concept_map.get(&target.anchor.ts_code).cloned(),
                            board: None,
                            total_mv_yi: None,
                            original_score: target.total_score,
                            original_rank: target.original_rank,
                            best_rank_3d: None,
                            ranking_score: None,
                            prediction_signal,
                            confidence,
                            sample_count: summary.sample_count,
                            effective_sample_count: summary.effective_sample_count,
                            expected_return_pct: summary.weighted_return_pct,
                            expected_excess_return_pct: summary.weighted_excess_return_pct,
                            shrunk_excess_return_pct: summary.shrunk_excess_return_pct,
                            excess_positive_rate: summary.weighted_excess_positive_rate,
                            expected_mfe_pct: summary.weighted_mfe_pct,
                            expected_mae_pct: summary.weighted_mae_pct,
                            average_similarity,
                            best_similarity: rating_sample.first().map(|row| row.similarity_score),
                            trigger_count: target.trigger_count,
                            top_matches,
                        }
                    })(
                        target,
                        candidates,
                        candidate_market_similarities,
                        candidate_rule_weight_sums,
                        candidate_by_rule,
                        all_trade_dates,
                        window_trade_days,
                        outcome_trade_days,
                        name_map,
                        industry_map,
                        concept_map,
                        rule_weights,
                        &mut scratch.borrow_mut(),
                    )
                })
            })(
                target,
                &candidates,
                &candidate_market_similarities,
                &candidate_rule_weight_sums,
                &candidate_by_rule,
                &all_trade_dates,
                window_trade_days,
                outcome_trade_days,
                &name_map,
                &industry_map,
                &concept_map,
                &rule_weights,
            )
        })
        .collect::<Vec<_>>();
    assign_ranks(&mut ranking_rows);
    timings.push(StrategyTriggerRankingTiming {
        label: "全市场近邻精排与后验聚合".to_string(),
        elapsed_ms: elapsed_ms(phase),
    });

    let final_signature = load_data_signature(&conn, &source_path, &resolved_trade_date)?;
    if final_signature != initial_signature {
        return Err(
            "计算期间行情或策略触发数据库发生更新，已放弃提交旧排行榜，请重新计算".to_string(),
        );
    }
    let scope_signature = (|source_path: &str,
                            indicator_columns: &[String]|
     -> Result<String, String> {
        let strategy_signature = stable_content_signature(&score_rule_path(source_path))?;
        let indicator_signature = stable_content_signature(&ind_toml_path(source_path))?;
        Ok(format!(
            "{SEMANTIC_DEFINITION_SIGNATURE_PREFIX}features=market-cap+main-star-growth-bse-st|strategy={strategy_signature}|indicator={indicator_signature}|columns={}",
            indicator_columns.join("\u{1f}")
        ))
    })(&source_path, &schema.indicator_columns)?;
    drop(conn);
    let before_write_elapsed = elapsed_ms(started);
    let phase = Instant::now();
    let key = config_key(
        window_trade_days,
        pool_segments,
        outcome_trade_days,
        &benchmark_index_code,
    );
    (|source_path: &str,
      trade_date: &str,
      config_key: &str,
      signature: &str,
      scope_signature: &str,
      historical_cutoff_date: &str,
      rows: &[StrategyTriggerRankingRow],
      universe_count: usize,
      candidate_universe_count: usize,
      candidate_anchor_count: usize,
      evaluated_anchor_count: usize,
      total_elapsed_ms: u64,
      timings: &[StrategyTriggerRankingTiming]|
     -> Result<(), String> {
        let (window_trade_days, pool_segments, outcome_trade_days, benchmark_index_code) =
            parse_config_key(config_key)
                .ok_or_else(|| format!("无法解析走势相似排行配置: {config_key}"))?;
        let result_path = result_db_path(source_path);
        let mut conn = Connection::open(&result_path)
            .map_err(|e| format!("打开结果库写入相似排行榜失败: {e}"))?;
        ensure_ranking_tables(&conn)?;
        let tx = conn
            .transaction()
            .map_err(|e| format!("创建相似排行榜事务失败: {e}"))?;
        let previous_active = load_active_config_record(&tx)?;
        // 只用策略和指标定义识别语义变化。每天重算复权行情、指标值或评分结果
        // 不会因此清理历史快照。
        if let Some(previous) = previous_active.as_ref() {
            if previous.config_key == config_key
                && previous
                    .scope_signature
                    .starts_with(SEMANTIC_DEFINITION_SIGNATURE_PREFIX)
                && previous.scope_signature != scope_signature
            {
                tx.execute(
                    "DELETE FROM strategy_trigger_similarity_rank WHERE config_key=?",
                    params![config_key],
                )
                .map_err(|e| format!("策略或指标变化后清理相似排行失败: {e}"))?;
                tx.execute(
                    "DELETE FROM strategy_trigger_similarity_rank_meta WHERE config_key=?",
                    params![config_key],
                )
                .map_err(|e| format!("策略或指标变化后清理相似排行元数据失败: {e}"))?;
            }
        }
        // 生产库只允许存在当前生效配置；配置切换本身触发旧配置清理。
        tx.execute(
            "DELETE FROM strategy_trigger_similarity_rank WHERE config_key<>?",
            params![config_key],
        )
        .map_err(|e| format!("清理非生效相似排行配置失败: {e}"))?;
        tx.execute(
            "DELETE FROM strategy_trigger_similarity_rank_meta WHERE config_key<>?",
            params![config_key],
        )
        .map_err(|e| format!("清理非生效相似排行配置元数据失败: {e}"))?;
        tx.execute(
            "DELETE FROM strategy_trigger_similarity_rank WHERE trade_date=? AND config_key=?",
            params![trade_date, config_key],
        )
        .map_err(|e| format!("清理旧相似排行失败: {e}"))?;
        tx.execute(
            "DELETE FROM strategy_trigger_similarity_rank_meta WHERE trade_date=? AND config_key=?",
            params![trade_date, config_key],
        )
        .map_err(|e| format!("清理旧相似排行元数据失败: {e}"))?;
        {
            let mut insert = tx
                .prepare(
                    r#"
                INSERT INTO strategy_trigger_similarity_rank VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
                "#,
                )
                .map_err(|e| format!("预编译相似排行写入失败: {e}"))?;
            for row in rows {
                let top_matches_json = serde_json::to_string(&row.top_matches)
                    .map_err(|e| format!("序列化相似事件失败: {e}"))?;
                insert
                    .execute(params![
                        trade_date,
                        config_key,
                        row.rank.map(|value| value as i64),
                        row.ts_code,
                        row.name,
                        row.industry,
                        row.concept,
                        row.original_score,
                        row.original_rank,
                        row.ranking_score,
                        row.prediction_signal,
                        row.confidence,
                        row.sample_count as i64,
                        row.effective_sample_count,
                        row.expected_return_pct,
                        row.expected_excess_return_pct,
                        row.shrunk_excess_return_pct,
                        row.excess_positive_rate,
                        row.expected_mfe_pct,
                        row.expected_mae_pct,
                        row.average_similarity,
                        row.best_similarity,
                        row.trigger_count as i64,
                        top_matches_json,
                    ])
                    .map_err(|e| format!("写入相似排行失败 {}: {e}", row.ts_code))?;
            }
        }
        let timings_json =
            serde_json::to_string(timings).map_err(|e| format!("序列化相似排行计时失败: {e}"))?;
        tx.execute(
        "INSERT INTO strategy_trigger_similarity_rank_meta VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        params![
            trade_date,
            config_key,
            signature,
            now_epoch_seconds(),
            historical_cutoff_date,
            universe_count as i64,
            rows.iter().filter(|row| row.rank.is_some()).count() as i64,
            candidate_universe_count as i64,
            candidate_anchor_count as i64,
            evaluated_anchor_count as i64,
            total_elapsed_ms as i64,
            timings_json,
        ],
    )
    .map_err(|e| format!("写入相似排行元数据失败: {e}"))?;
        tx.execute(
            "DELETE FROM strategy_trigger_similarity_active_config WHERE id=1",
            [],
        )
        .map_err(|e| format!("清理旧相似排行生效配置失败: {e}"))?;
        tx.execute(
            r#"
        INSERT INTO strategy_trigger_similarity_active_config (
            id, config_key, algorithm_version, window_trade_days, pool_segments,
            outcome_trade_days, benchmark_index_code, scope_trade_date,
            scope_signature, updated_at_epoch_seconds
        ) VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        "#,
            params![
                config_key,
                ALGORITHM_VERSION,
                window_trade_days as i64,
                pool_segments as i64,
                outcome_trade_days as i64,
                benchmark_index_code,
                trade_date,
                scope_signature,
                now_epoch_seconds()
            ],
        )
        .map_err(|e| format!("写入相似排行生效配置失败: {e}"))?;
        tx.commit().map_err(|e| format!("提交相似排行榜失败: {e}"))
    })(
        &source_path,
        &resolved_trade_date,
        &key,
        &initial_signature,
        &scope_signature,
        &historical_cutoff_date,
        &ranking_rows,
        target_anchor_count,
        candidate_universe_count,
        candidate_anchor_count,
        evaluated_anchor_count,
        before_write_elapsed,
        &timings,
    )?;
    timings.push(StrategyTriggerRankingTiming {
        label: "原子写入排行榜".to_string(),
        elapsed_ms: elapsed_ms(phase),
    });

    get_strategy_trigger_similarity_ranking_page(
        source_path,
        Some(resolved_trade_date),
        Some(window_trade_days as u32),
        Some(pool_segments as u32),
        Some(outcome_trade_days as u32),
        Some(benchmark_index_code),
        limit,
        board,
        exclude_st_board,
        total_mv_min,
        total_mv_max,
    )
}

#[cfg(test)]
mod tests {
    use super::StrategyTriggerRankingRow;
    use super::{
        ANCHOR_CHUNK_SIZE, RankingTargetScratch, assign_ranks, build_channel_fingerprint,
        build_environment_fingerprint_map, build_ranking_samples_for_chunk,
        cached_channel_similarity, ensure_ranking_tables,
        get_strategy_trigger_similarity_active_config,
        get_strategy_trigger_similarity_ranking_page, load_all_trade_dates, load_benchmark_rows,
        load_market_environment, load_market_schema, load_outcome_selected_anchors,
        open_result_conn, run_strategy_trigger_similarity_ranking, share_environment_fingerprints,
    };
    use lianghua_app_shared::{build_name_map, build_total_mv_map};
    use std::{
        collections::HashMap,
        fs,
        sync::Arc,
        time::{SystemTime, UNIX_EPOCH},
    };

    use duckdb::{Connection, params};

    fn empty_row(code: &str, signal: Option<f64>) -> StrategyTriggerRankingRow {
        StrategyTriggerRankingRow {
            rank: None,
            ts_code: code.to_string(),
            name: None,
            industry: None,
            concept: None,
            board: None,
            original_score: None,
            original_rank: None,
            best_rank_3d: None,
            ranking_score: None,
            prediction_signal: signal,
            confidence: 0.0,
            sample_count: 0,
            effective_sample_count: 0.0,
            expected_return_pct: None,
            expected_excess_return_pct: None,
            shrunk_excess_return_pct: None,
            excess_positive_rate: None,
            expected_mfe_pct: None,
            expected_mae_pct: None,
            average_similarity: None,
            best_similarity: None,
            trigger_count: 0,
            total_mv_yi: None,
            top_matches: Vec::new(),
        }
    }

    #[test]
    fn market_schema_keeps_all_numeric_indicator_columns() {
        let conn = Connection::open_in_memory().expect("open in-memory DuckDB");
        let indicator_columns = (0..30)
            .map(|index| format!("indicator_{index:02} DOUBLE"))
            .collect::<Vec<_>>()
            .join(", ");
        conn.execute_batch(&format!(
            "ATTACH ':memory:' AS trigger_market_db; \
             CREATE TABLE trigger_market_db.stock_data (\
                 ts_code VARCHAR, trade_date VARCHAR, adj_type VARCHAR, {indicator_columns});"
        ))
        .expect("create market schema");
        let schema = load_market_schema(&conn).expect("load market schema");
        assert_eq!(schema.indicator_columns.len(), 30);
        assert_eq!(
            schema.indicator_columns.first().map(String::as_str),
            Some("indicator_00")
        );
        assert_eq!(
            schema.indicator_columns.last().map(String::as_str),
            Some("indicator_29")
        );
    }

    #[test]
    fn active_config_migrates_from_the_legacy_config_key_row() {
        let conn = Connection::open_in_memory().expect("open in-memory DuckDB");
        conn.execute_batch(
            r#"
            CREATE TABLE strategy_trigger_similarity_active_config (
                id TINYINT PRIMARY KEY,
                config_key VARCHAR NOT NULL,
                scope_trade_date VARCHAR NOT NULL,
                scope_signature VARCHAR NOT NULL,
                updated_at_epoch_seconds BIGINT NOT NULL
            );
            INSERT INTO strategy_trigger_similarity_active_config
            VALUES (1, 'legacy-v3:w20:p3:h5:b000001.SH', '20240110', 'scope', 1);
            "#,
        )
        .expect("create legacy active config");

        ensure_ranking_tables(&conn).expect("migrate active config table");
        let active = get_strategy_trigger_similarity_active_config(&conn)
            .expect("read migrated active config")
            .expect("active config should exist");
        assert_eq!(active.algorithm_version, "legacy-v3");
        assert_eq!(active.window_trade_days, 20);
        assert_eq!(active.pool_segments, 3);
        assert_eq!(active.outcome_trade_days, 5);
        assert_eq!(active.benchmark_index_code, "000001.SH");
    }

    #[test]
    fn channel_fingerprint_supports_more_than_sixty_four_indicators() {
        let target =
            build_channel_fingerprint((0..80).map(|index| Some(vec![index as f64])).collect());
        let candidate =
            build_channel_fingerprint((0..80).map(|index| Some(vec![index as f64])).collect());
        assert!(target.has_vectors);
        assert!(candidate.has_vectors);
        assert!(
            (cached_channel_similarity(&target, &candidate).expect("similarity") - 100.0).abs()
                < 1e-9
        );
    }

    #[test]
    fn market_fingerprints_are_shared_by_trade_date() {
        let shared = share_environment_fingerprints(HashMap::from([(
            "20240102".to_string(),
            vec![Some(vec![1.0, 2.0, 3.0])],
        )]));
        let first = shared.get("20240102").expect("market fingerprint");
        let second = Arc::clone(first);
        assert!(Arc::ptr_eq(first, &second));
        assert_eq!(first.norms, second.norms);
    }

    #[test]
    fn target_scratch_reuses_storage_without_leaking_rule_weights() {
        let mut scratch = RankingTargetScratch::default();
        scratch.prepare(8, 4);
        scratch.add_candidate_rule_weight(3, 1.5);
        scratch.add_candidate_rule_weight(3, 0.5);
        assert_eq!(scratch.candidate_indices, vec![3]);
        assert_eq!(scratch.candidate_intersection_weights[3], 2.0);

        let weights_capacity = scratch.candidate_intersection_weights.capacity();
        let indices_capacity = scratch.candidate_indices.capacity();
        scratch.prepare(8, 4);
        scratch.add_candidate_rule_weight(3, 4.0);
        assert_eq!(scratch.candidate_indices, vec![3]);
        assert_eq!(scratch.candidate_intersection_weights[3], 4.0);
        assert_eq!(
            scratch.candidate_intersection_weights.capacity(),
            weights_capacity
        );
        assert_eq!(scratch.candidate_indices.capacity(), indices_capacity);
    }

    #[test]
    fn rank_assignment_puts_unrated_rows_last() {
        let mut rows = vec![
            empty_row("B", None),
            empty_row("A", Some(2.0)),
            empty_row("C", Some(1.0)),
        ];
        assign_ranks(&mut rows);
        assert_eq!(
            rows.iter()
                .map(|row| row.ts_code.as_str())
                .collect::<Vec<_>>(),
            vec!["A", "C", "B"]
        );
        assert_eq!(rows[0].rank, Some(1));
        assert_eq!(rows[2].rank, None);
    }

    #[test]
    #[ignore = "requires LIANGHUA_BENCH_DATA_DIR and a real dataset"]
    fn benchmark_real_outcome_anchor_scan() {
        let source_path = std::env::var("LIANGHUA_BENCH_DATA_DIR")
            .expect("set LIANGHUA_BENCH_DATA_DIR to a real source directory");
        let conn = open_result_conn(&source_path).expect("open real source databases");
        let all_trade_dates = load_all_trade_dates(&conn).expect("load scoring calendar");
        let target_date = all_trade_dates.last().expect("target date").clone();
        let horizon = 5;
        let cutoff_index = all_trade_dates.len() - 1 - horizon;
        let earliest_date = all_trade_dates[19].clone();
        let cutoff_date = all_trade_dates[cutoff_index].clone();
        let started = std::time::Instant::now();
        let (anchors, universe_count) = load_outcome_selected_anchors(
            &conn,
            &earliest_date,
            &cutoff_date,
            &target_date,
            &all_trade_dates,
            20,
            horizon,
            "000001.SH",
        )
        .expect("scan real outcome labels");
        eprintln!(
            "real outcome scan: elapsed={:?}, selected={}, universe={}",
            started.elapsed(),
            anchors.len(),
            universe_count
        );
        assert!(!anchors.is_empty());
    }

    #[test]
    #[ignore = "requires LIANGHUA_BENCH_DATA_DIR and a real dataset"]
    fn benchmark_real_candidate_fingerprints() {
        let source_path = std::env::var("LIANGHUA_BENCH_DATA_DIR")
            .expect("set LIANGHUA_BENCH_DATA_DIR to a real source directory");
        let conn = open_result_conn(&source_path).expect("open real source databases");
        let all_trade_dates = load_all_trade_dates(&conn).expect("load scoring calendar");
        let target_date = all_trade_dates.last().expect("target date").clone();
        let horizon = 5;
        let cutoff_date = all_trade_dates[all_trade_dates.len() - 1 - horizon].clone();
        let earliest_date = all_trade_dates[19].clone();
        let schema = load_market_schema(&conn).expect("load market schema");
        let environment =
            load_market_environment(&conn, &all_trade_dates[0], &target_date, &schema)
                .expect("load environment");
        let environment_fingerprints = share_environment_fingerprints(
            build_environment_fingerprint_map(&environment, &all_trade_dates, 20, 5),
        );
        let benchmark_rows =
            load_benchmark_rows(&conn, &all_trade_dates[0], &target_date, "000001.SH")
                .expect("load benchmark");
        let total_mv_map = build_total_mv_map(&source_path).unwrap_or_default();
        let name_map = build_name_map(&source_path).unwrap_or_default();
        let (selected, _) = load_outcome_selected_anchors(
            &conn,
            &earliest_date,
            &cutoff_date,
            &target_date,
            &all_trade_dates,
            20,
            horizon,
            "000001.SH",
        )
        .expect("load selected anchors");
        let anchors = selected
            .into_iter()
            .map(|selected| selected.anchor)
            .collect::<Vec<_>>();
        let anchor_count = anchors.len();
        let started = std::time::Instant::now();
        let mut built = 0;
        let mut anchor_iter = anchors.into_iter();
        loop {
            let chunk = anchor_iter
                .by_ref()
                .take(ANCHOR_CHUNK_SIZE)
                .collect::<Vec<_>>();
            if chunk.is_empty() {
                break;
            }
            built += build_ranking_samples_for_chunk(
                &conn,
                chunk,
                &schema,
                &all_trade_dates,
                &environment_fingerprints,
                &benchmark_rows,
                &total_mv_map,
                &name_map,
                5,
                horizon,
                &target_date,
                true,
            )
            .expect("build candidate fingerprints")
            .len();
            eprintln!(
                "candidate fingerprints: elapsed={:?}, built={built}/{}",
                started.elapsed(),
                anchor_count
            );
        }
        assert!(built > 0);
    }

    #[test]
    #[ignore = "requires LIANGHUA_BENCH_DATA_DIR and writes the real ranking tables"]
    fn benchmark_real_full_ranking() {
        let source_path = std::env::var("LIANGHUA_BENCH_DATA_DIR")
            .expect("set LIANGHUA_BENCH_DATA_DIR to a real source directory");
        let started = std::time::Instant::now();
        let page = run_strategy_trigger_similarity_ranking(
            source_path,
            None,
            Some(20),
            Some(5),
            Some(5),
            Some("000001.SH".to_string()),
            Some(100),
            None,
            None,
            None,
            None,
        )
        .expect("compute real full-market ranking");
        eprintln!(
            "real full ranking: elapsed={:?}, universe={}, ranked={}, candidates={}/{}, timings={:?}",
            started.elapsed(),
            page.universe_count,
            page.ranked_count,
            page.evaluated_anchor_count,
            page.candidate_anchor_count,
            page.timings
        );
        assert!(page.is_fresh);
    }

    #[test]
    fn ranking_round_trip_writes_and_revalidates_data_signature() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let source_dir = std::env::temp_dir().join(format!(
            "lianghua-strategy-sim-rank-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&source_dir).expect("create test source directory");
        fs::write(source_dir.join("score_rule.toml"), "version = 1\n")
            .expect("write strategy definition");
        fs::write(source_dir.join("ind.toml"), "[[indicator]]\nname = 'J'\n")
            .expect("write indicator definition");
        let market_path = source_dir.join("stock_data.db");
        let result_path = source_dir.join("scoring_result.db");
        let market = Connection::open(&market_path).expect("open market db");
        market
            .execute_batch(
                r#"
                CREATE TABLE stock_data (
                    ts_code VARCHAR, trade_date VARCHAR, adj_type VARCHAR,
                    open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
                    pct_chg DOUBLE, vol DOUBLE, amount DOUBLE, tor DOUBLE, net_mf_v DOUBLE
                );
                "#,
            )
            .expect("create market table");
        let stocks = [
            "TARGET.SZ",
            "C0.SZ",
            "C1.SZ",
            "C2.SZ",
            "C3.SZ",
            "C4.SZ",
            "C5.SZ",
        ];
        {
            let mut insert = market
                .prepare("INSERT INTO stock_data VALUES (?, ?, 'qfq', ?, ?, ?, ?, ?, ?, ?, ?, ?)")
                .expect("prepare market rows");
            for (stock_index, stock) in stocks.iter().enumerate() {
                for day in 1..=10 {
                    let base = 10.0 + stock_index as f64 + day as f64 * 0.1;
                    insert
                        .execute(params![
                            stock,
                            format!("202401{day:02}"),
                            base,
                            base * 1.03,
                            base * 0.98,
                            base * 1.01,
                            day as f64 * 0.1,
                            1_000.0 + day as f64,
                            10_000.0 + day as f64,
                            2.0,
                            10.0,
                        ])
                        .expect("insert market row");
                }
            }
            let mut insert_index = market
                .prepare("INSERT INTO stock_data VALUES ('000001.SH', ?, 'ind', ?, ?, ?, ?, ?, 0, 0, 0, 0)")
                .expect("prepare benchmark rows");
            for day in 1..=10 {
                let base = 3_000.0 + day as f64;
                insert_index
                    .execute(params![
                        format!("202401{day:02}"),
                        base,
                        base,
                        base,
                        base,
                        0.1
                    ])
                    .expect("insert benchmark row");
            }
        }
        drop(market);

        let result = Connection::open(&result_path).expect("open result db");
        result
            .execute_batch(
                r#"
                CREATE TABLE score_summary (
                    ts_code VARCHAR, trade_date VARCHAR, total_score DOUBLE, rank BIGINT
                );
                CREATE TABLE rule_details (
                    ts_code VARCHAR, trade_date VARCHAR, rule_name VARCHAR, rule_score DOUBLE
                );
                "#,
            )
            .expect("create result tables");
        {
            let mut insert_score = result
                .prepare("INSERT INTO score_summary VALUES (?, ?, ?, ?)")
                .expect("prepare score rows");
            for day in 1..=10 {
                for (index, stock) in stocks.iter().enumerate() {
                    insert_score
                        .execute(params![
                            stock,
                            format!("202401{day:02}"),
                            10.0 - index as f64,
                            index as i64 + 1
                        ])
                        .expect("insert score row");
                }
            }
            let mut insert_rule = result
                .prepare("INSERT INTO rule_details VALUES (?, ?, '启动规则', 1.0)")
                .expect("prepare rule rows");
            for (index, stock) in stocks.iter().skip(1).enumerate() {
                insert_rule
                    .execute(params![stock, format!("202401{:02}", index + 2)])
                    .expect("insert historical trigger");
            }
            insert_rule
                .execute(params!["TARGET.SZ", "20240110"])
                .expect("insert target trigger");
        }
        drop(result);

        let source_path = source_dir.to_string_lossy().to_string();
        run_strategy_trigger_similarity_ranking(
            source_path.clone(),
            Some("20240109".to_string()),
            Some(3),
            Some(2),
            Some(2),
            Some("000001.SH".to_string()),
            Some(100),
            None,
            None,
            None,
            None,
        )
        .expect("compute first historical ranking snapshot");
        let market = Connection::open(source_dir.join("stock_data.db"))
            .expect("reopen market db for daily qfq rebuild simulation");
        market
            .execute(
                "UPDATE stock_data SET close=close+0.01 WHERE ts_code='C0.SZ' AND trade_date='20240102' AND adj_type='qfq'",
                [],
            )
            .expect("simulate recalculated historical qfq value");
        drop(market);
        let result = Connection::open(source_dir.join("scoring_result.db"))
            .expect("reopen result db for daily scoring rebuild simulation");
        result
            .execute(
                "UPDATE score_summary SET total_score=total_score+0.01 WHERE ts_code='C0.SZ' AND trade_date='20240102'",
                [],
            )
            .expect("simulate recalculated historical score");
        result
            .execute(
                "UPDATE rule_details SET rule_score=rule_score+0.01 WHERE ts_code='C0.SZ' AND trade_date='20240102'",
                [],
            )
            .expect("simulate recalculated historical rule result");
        drop(result);
        let computed = run_strategy_trigger_similarity_ranking(
            source_path.clone(),
            Some("20240110".to_string()),
            None,
            None,
            None,
            None,
            Some(100),
            None,
            None,
            None,
            None,
        )
        .expect("compute ranking");
        assert!(computed.is_fresh);
        assert_eq!(computed.universe_count, stocks.len());
        assert!(computed.evaluated_anchor_count > 0);
        assert!(computed.items.iter().any(|row| row.ts_code == "TARGET.SZ"));
        let result = Connection::open(source_dir.join("scoring_result.db"))
            .expect("reopen result db for active config assertions");
        let active = get_strategy_trigger_similarity_active_config(&result)
            .expect("read active config")
            .expect("active config should exist");
        assert_eq!(active.window_trade_days, 3);
        assert_eq!(active.pool_segments, 2);
        assert_eq!(active.outcome_trade_days, 2);
        assert_eq!(active.benchmark_index_code, "000001.SH");
        assert_eq!(
            result
                .query_row(
                    "SELECT COUNT(DISTINCT trade_date) FROM strategy_trigger_similarity_rank_meta",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .expect("count retained snapshots"),
            2,
            "daily qfq and scoring rebuilds must not delete older snapshots"
        );
        drop(result);

        let reread = get_strategy_trigger_similarity_ranking_page(
            source_path.clone(),
            Some("20240110".to_string()),
            Some(3),
            Some(2),
            Some(2),
            Some("000001.SH".to_string()),
            Some(100),
            None,
            None,
            None,
            None,
        )
        .expect("read ranking");
        assert!(reread.is_fresh);
        assert_eq!(reread.items.len(), computed.items.len());

        let result = Connection::open(source_dir.join("scoring_result.db"))
            .expect("reopen result db for mutation");
        result
            .execute(
                "UPDATE rule_details SET rule_score=2.0 WHERE ts_code='TARGET.SZ' AND trade_date='20240110'",
                [],
            )
            .expect("mutate strategy trigger data");
        drop(result);
        let stale = get_strategy_trigger_similarity_ranking_page(
            source_dir.to_string_lossy().to_string(),
            Some("20240110".to_string()),
            Some(3),
            Some(2),
            Some(2),
            Some("000001.SH".to_string()),
            Some(100),
            None,
            None,
            None,
            None,
        )
        .expect("revalidate changed ranking");
        assert!(!stale.is_fresh);
        assert!(stale.items.is_empty());

        fs::write(source_dir.join("score_rule.toml"), "version = 2\n")
            .expect("change strategy definition");

        run_strategy_trigger_similarity_ranking(
            source_dir.to_string_lossy().to_string(),
            Some("20240110".to_string()),
            None,
            None,
            None,
            None,
            Some(100),
            None,
            None,
            None,
            None,
        )
        .expect("recompute after strategy change");
        let result = Connection::open(source_dir.join("scoring_result.db"))
            .expect("reopen result db after strategy change");
        assert_eq!(
            result
                .query_row(
                    "SELECT COUNT(DISTINCT trade_date) FROM strategy_trigger_similarity_rank_meta",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .expect("count snapshots after strategy change"),
            1,
            "strategy changes must clear old snapshots before writing the replacement"
        );
        drop(result);

        run_strategy_trigger_similarity_ranking(
            source_dir.to_string_lossy().to_string(),
            Some("20240110".to_string()),
            Some(3),
            Some(3),
            Some(2),
            Some("000001.SH".to_string()),
            Some(100),
            None,
            None,
            None,
            None,
        )
        .expect("switch active pool configuration");
        let result = Connection::open(source_dir.join("scoring_result.db"))
            .expect("reopen result db after config switch");
        let (config_count, active_pool): (i64, i64) = result
            .query_row(
                "SELECT (SELECT COUNT(DISTINCT config_key) FROM strategy_trigger_similarity_rank_meta), pool_segments \
                 FROM strategy_trigger_similarity_active_config WHERE id=1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("read switched active config");
        assert_eq!(config_count, 1);
        assert_eq!(active_pool, 3);
        drop(result);

        fs::remove_dir_all(&source_dir).expect("remove test source directory");
    }
}
