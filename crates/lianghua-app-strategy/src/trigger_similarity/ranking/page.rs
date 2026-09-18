use crate::trigger_similarity::ranking::store::{
    config_key, get_strategy_trigger_similarity_active_config, load_data_signature,
    parse_config_key, table_exists,
};
use crate::trigger_similarity::ranking::{
    ALGORITHM_VERSION, RankingMeta, StrategyTriggerRankingPageData, StrategyTriggerRankingRow,
};
use crate::trigger_similarity::*;

use crate::trigger_similarity::load::load_all_trade_dates;
use crate::trigger_similarity::load::open_result_conn;
use crate::trigger_similarity::load::resolve_benchmark_index_code;
use crate::trigger_similarity::load::resolve_existing_trade_date;
use crate::utils::utils::board_category;
use duckdb::Connection;
use duckdb::params;
use lianghua_app_shared::build_name_map;
use lianghua_app_shared::build_total_mv_map;
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
    ts_code: Option<String>,
    sample_gap_trade_days: Option<u32>,
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
                    config.sample_gap_trade_days,
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
    let sample_gap_trade_days = sample_gap_trade_days
        .map(|v| v as usize)
        .or_else(|| latest_config.as_ref().map(|value| value.3))
        .unwrap_or(MIN_SAMPLE_GAP_TRADE_DAYS);
    let benchmark_index_code = resolve_benchmark_index_code(
        latest_config
            .as_ref()
            .map(|value| value.4.as_str())
            .or(benchmark_index_code.as_deref()),
    )?;
    let limit = limit
        .map(|v| v as usize)
        .filter(|v| *v > 0)
        .unwrap_or(100)
        .min(5_000);
    let ts_code = ts_code
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .map(|value| value.to_uppercase());
    let all_trade_dates = load_all_trade_dates(&conn)?;
    let target_index = all_trade_dates
        .binary_search(&resolved_trade_date)
        .map_err(|_| format!("参考日不在评分交易日中: {resolved_trade_date}"))?;
    let candidate_gap_trade_days = outcome_trade_days.max(sample_gap_trade_days);
    if target_index < candidate_gap_trade_days {
        return Err("参考日前没有足够历史区间".to_string());
    }
    let historical_cutoff_date = all_trade_dates[target_index - candidate_gap_trade_days].clone();
    let key = config_key(
        window_trade_days,
        pool_segments,
        outcome_trade_days,
        sample_gap_trade_days,
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
                  AND (
                    COALESCE(?, '') = ''
                    OR UPPER(ts_code) = ?
                  )
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
                .query(params![
                    trade_date,
                    config_key,
                    ts_code,
                    ts_code,
                    limit as i64,
                    trade_date
                ])
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
