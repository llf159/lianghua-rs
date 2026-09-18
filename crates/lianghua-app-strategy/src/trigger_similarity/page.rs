use crate::trigger_similarity::channel::{
    build_environment_fingerprint_map, cached_channel_similarity, final_similarity,
    share_environment_fingerprints,
};
use crate::trigger_similarity::fingerprint::{
    load_rule_idf_weights, trigger_fingerprint_similarity_with_masses, trigger_rule_weight_sum,
};
use crate::trigger_similarity::load::{
    load_all_trade_dates, load_market_environment, load_market_schema, open_result_conn,
    resolve_benchmark_index_code, resolve_existing_trade_date, sql_string_literal,
};
use crate::trigger_similarity::sample::{
    OutcomeSummarySample, SampleBuildContext, build_rating_sample, build_samples_for_chunk,
    load_benchmark_rows, summarize_outcomes,
};
use crate::trigger_similarity::{
    ANCHOR_CHUNK_SIZE, Anchor, DEFAULT_LIMIT, DEFAULT_OUTCOME_TRADE_DAYS, DEFAULT_POOL_SEGMENTS,
    DEFAULT_WINDOW_TRADE_DAYS, EPS, HISTORY_DIVERSITY_ANCHORS, KERNEL_NAMES, MAX_POOL_SEGMENTS,
    RECENT_CANDIDATE_ANCHORS, RuleCatalog, RuleEvent, StrategyTriggerSimilarityPageData,
    StrategyTriggerSimilarityRow, StrategyTriggerSimilarityTarget,
};

use duckdb::Connection;
use duckdb::params;
use lianghua_app_shared::build_concepts_map;
use lianghua_app_shared::build_industry_map;
use lianghua_app_shared::build_name_map;
use lianghua_app_shared::build_total_mv_map;
use lianghua_app_shared::canonical_ts_code;
use std::collections::HashMap;
#[allow(clippy::too_many_arguments)]
pub fn get_strategy_trigger_similarity_page(
    source_path: String,
    trade_date: Option<String>,
    ts_code: String,
    window_trade_days: Option<u32>,
    pool_segments: Option<u32>,
    outcome_trade_days: Option<u32>,
    benchmark_index_code: Option<String>,
    limit: Option<u32>,
) -> Result<StrategyTriggerSimilarityPageData, String> {
    let source_path = source_path.trim().to_string();
    if source_path.is_empty() {
        return Err("source_path 不能为空".to_string());
    }
    let conn = open_result_conn(&source_path)?;
    let resolved_trade_date = resolve_existing_trade_date(&conn, trade_date)?;
    let resolved_ts_code = canonical_ts_code(&ts_code);
    let benchmark_index_code = resolve_benchmark_index_code(benchmark_index_code.as_deref())?;
    let window_trade_days = window_trade_days
        .map(|v| v as usize)
        .filter(|v| *v >= 3)
        .unwrap_or(DEFAULT_WINDOW_TRADE_DAYS);
    let pool_segments = pool_segments
        .map(|v| v as usize)
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_POOL_SEGMENTS)
        .min(MAX_POOL_SEGMENTS)
        .min(window_trade_days);
    let outcome_trade_days = outcome_trade_days
        .map(|v| v as usize)
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_OUTCOME_TRADE_DAYS);
    let limit = limit
        .map(|v| v as usize)
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_LIMIT);

    let all_trade_dates = load_all_trade_dates(&conn)?;
    let target_end_index = all_trade_dates
        .binary_search(&resolved_trade_date)
        .map_err(|_| format!("参考日不在评分交易日中: {resolved_trade_date}"))?;
    if target_end_index < outcome_trade_days {
        return Err("参考日前没有足够历史区间构建完整后验样本".to_string());
    }
    let target_start_index = (target_end_index + 1).saturating_sub(window_trade_days);
    let target_start_date = all_trade_dates[target_start_index].clone();
    let historical_cutoff_date = all_trade_dates[target_end_index - outcome_trade_days].clone();
    let mut rule_catalog = RuleCatalog::default();
    let target_events = (|conn: &Connection,
                          ts_code: &str,
                          start_date: &str,
                          end_date: &str|
     -> Result<Vec<RuleEvent>, String> {
        let mut stmt = conn
            .prepare(
                r#"
            SELECT rule_name, trade_date, TRY_CAST(rule_score AS DOUBLE)
            FROM rule_details
            WHERE ts_code = ? AND trade_date >= ? AND trade_date <= ?
              AND TRY_CAST(rule_score AS DOUBLE) IS NOT NULL
              AND ABS(TRY_CAST(rule_score AS DOUBLE)) > ?
            ORDER BY trade_date, rule_name
            "#,
            )
            .map_err(|e| format!("预编译目标触发查询失败: {e}"))?;
        let mut rows = stmt
            .query(params![ts_code, start_date, end_date, EPS])
            .map_err(|e| format!("查询目标触发失败: {e}"))?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().map_err(|e| format!("读取目标触发失败: {e}"))? {
            out.push(RuleEvent {
                rule_id: rule_catalog
                    .intern(row.get(0).map_err(|e| format!("读取规则名失败: {e}"))?),
                trade_date: row.get(1).map_err(|e| format!("读取触发日失败: {e}"))?,
                score: row.get(2).map_err(|e| format!("读取规则分数失败: {e}"))?,
            });
        }
        Ok(out)
    })(
        &conn,
        &resolved_ts_code,
        &target_start_date,
        &resolved_trade_date,
    )?;
    let target_rule_names = (|events: &[RuleEvent]| -> Vec<String> {
        let mut names = events
            .iter()
            .map(|event| rule_catalog.names[event.rule_id].clone())
            .collect::<Vec<_>>();
        names.sort();
        names.dedup();
        names
    })(&target_events);
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
    let target_anchor = Anchor {
        id: 0,
        ts_code: resolved_ts_code.clone(),
        start_trade_date: target_start_date.clone(),
        end_trade_date: resolved_trade_date.clone(),
    };
    let target_context = SampleBuildContext {
        schema: &schema,
        all_trade_dates: &all_trade_dates,
        environment_fingerprints: &environment_fingerprints,
        benchmark_rows: &benchmark_rows,
        total_mv_map: &total_mv_map,
        name_map: &name_map,
        pool_segments,
        outcome_trade_days,
        target_trade_date: &resolved_trade_date,
        include_outcome: false,
        include_summaries: false,
    };
    let target_trigger_count = target_events.len();
    let target_rules = HashMap::from([(target_anchor.id, target_events)]);
    let target_sample = build_samples_for_chunk(
        &conn,
        vec![target_anchor],
        &target_context,
        &mut rule_catalog,
        Some(target_rules),
    )?
    .into_iter()
    .next()
    .ok_or_else(|| format!("{resolved_ts_code} 在 {resolved_trade_date} 没有完整量价窗口"))?;

    let earliest_candidate_date = all_trade_dates
        .get(window_trade_days.saturating_sub(1))
        .map(String::as_str)
        .unwrap_or(&all_trade_dates[0]);
    let (candidate_anchors, candidate_universe_count) =
        (|conn: &Connection,
          target_rule_names: &[String],
          earliest_date: &str,
          cutoff_date: &str,
          all_trade_dates: &[String],
          window_trade_days: usize|
         -> Result<(Vec<Anchor>, usize), String> {
            if target_rule_names.is_empty() {
                return Ok((Vec::new(), 0));
            }
            let target_rule_literals = target_rule_names
                .iter()
                .map(|name| sql_string_literal(name))
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                r#"
        WITH candidates AS (
            SELECT DISTINCT ts_code, trade_date
            FROM rule_details
            WHERE trade_date >= {earliest_date} AND trade_date <= {cutoff_date}
              AND rule_name IN ({target_rule_literals})
              AND TRY_CAST(rule_score AS DOUBLE) IS NOT NULL
              AND ABS(TRY_CAST(rule_score AS DOUBLE)) > {EPS}
        ),
        counted AS (
            SELECT *, COUNT(*) OVER () AS candidate_total
            FROM candidates
        ),
        recent_history AS MATERIALIZED (
            SELECT * FROM counted
            ORDER BY trade_date DESC, hash(ts_code, trade_date)
            LIMIT {recent_limit}
        ),
        diverse_history AS (
            SELECT c.* FROM counted c
            WHERE NOT EXISTS (
                SELECT 1 FROM recent_history t
                WHERE t.ts_code = c.ts_code AND t.trade_date = c.trade_date
            )
            ORDER BY hash(c.ts_code, c.trade_date)
            LIMIT {diversity_limit}
        )
        SELECT ts_code, trade_date, candidate_total FROM recent_history
        UNION ALL
        SELECT ts_code, trade_date, candidate_total FROM diverse_history
        "#,
                cutoff_date = sql_string_literal(cutoff_date),
                earliest_date = sql_string_literal(earliest_date),
                recent_limit = RECENT_CANDIDATE_ANCHORS,
                diversity_limit = HISTORY_DIVERSITY_ANCHORS,
            );
            let mut stmt = conn
                .prepare(&sql)
                .map_err(|e| format!("预编译历史事件锚点查询失败: {e}"))?;
            let mut rows = stmt
                .query([])
                .map_err(|e| format!("查询历史事件锚点失败: {e}"))?;
            let date_index = all_trade_dates
                .iter()
                .enumerate()
                .map(|(index, date)| (date.as_str(), index))
                .collect::<HashMap<_, _>>();
            let mut anchors = Vec::new();
            let mut candidate_universe_count = 0;
            while let Some(row) = rows
                .next()
                .map_err(|e| format!("读取历史事件锚点失败: {e}"))?
            {
                let ts_code: String = row.get(0).map_err(|e| format!("读取锚点代码失败: {e}"))?;
                let end_trade_date: String =
                    row.get(1).map_err(|e| format!("读取锚点日期失败: {e}"))?;
                let total: i64 = row
                    .get(2)
                    .map_err(|e| format!("读取候选全集数量失败: {e}"))?;
                candidate_universe_count = total.max(0) as usize;
                let Some(end_index) = date_index.get(end_trade_date.as_str()).copied() else {
                    continue;
                };
                let start_index = (end_index + 1).saturating_sub(window_trade_days);
                anchors.push(Anchor {
                    id: anchors.len(),
                    ts_code,
                    start_trade_date: all_trade_dates[start_index].clone(),
                    end_trade_date,
                });
            }
            Ok((anchors, candidate_universe_count))
        })(
            &conn,
            &target_rule_names,
            earliest_candidate_date,
            &historical_cutoff_date,
            &all_trade_dates,
            window_trade_days,
        )?;
    let rule_weights = load_rule_idf_weights(
        &conn,
        earliest_candidate_date,
        &historical_cutoff_date,
        &mut rule_catalog,
    )?;
    let target_rule_weight =
        trigger_rule_weight_sum(&target_sample.fingerprint.trigger, &rule_weights);
    let candidate_anchor_count = candidate_anchors.len();
    let candidate_pool_truncated = candidate_universe_count > candidate_anchor_count;
    let candidate_context = SampleBuildContext {
        include_outcome: true,
        include_summaries: true,
        ..target_context
    };
    let industry_map = build_industry_map(&source_path).unwrap_or_default();
    let concept_map = build_concepts_map(&source_path).unwrap_or_default();
    let mut items = Vec::new();
    let mut evaluated_anchor_count = 0;
    let mut market_similarity_by_date = HashMap::<String, Option<f64>>::new();
    let mut candidate_anchor_iter = candidate_anchors.into_iter();
    loop {
        let chunk = candidate_anchor_iter
            .by_ref()
            .take(ANCHOR_CHUNK_SIZE)
            .collect::<Vec<_>>();
        if chunk.is_empty() {
            break;
        }
        let samples =
            build_samples_for_chunk(&conn, chunk, &candidate_context, &mut rule_catalog, None)?;
        evaluated_anchor_count += samples.len();
        for sample in samples {
            if sample.anchor.ts_code == resolved_ts_code {
                continue;
            }
            let candidate_rule_weight =
                trigger_rule_weight_sum(&sample.fingerprint.trigger, &rule_weights);
            let trigger_similarity = trigger_fingerprint_similarity_with_masses(
                &target_sample.fingerprint.trigger,
                &sample.fingerprint.trigger,
                &rule_weights,
                target_rule_weight,
                candidate_rule_weight,
            );
            let price_volume_similarity = cached_channel_similarity(
                &target_sample.fingerprint.price_volume,
                &sample.fingerprint.price_volume,
            );
            let indicator_similarity = cached_channel_similarity(
                &target_sample.fingerprint.indicators,
                &sample.fingerprint.indicators,
            );
            let market_similarity = *market_similarity_by_date
                .entry(sample.anchor.end_trade_date.clone())
                .or_insert_with(|| {
                    cached_channel_similarity(
                        &target_sample.fingerprint.market,
                        &sample.fingerprint.market,
                    )
                });
            let similarity_score = final_similarity(
                trigger_similarity,
                price_volume_similarity,
                indicator_similarity,
                market_similarity,
            );
            let Some(outcome) = sample.outcome else {
                continue;
            };
            let mut matched_rule_names = sample
                .fingerprint
                .trigger
                .by_rule
                .into_keys()
                .filter(|id| target_sample.fingerprint.trigger.by_rule.contains_key(id))
                .map(|id| rule_catalog.names[id].clone())
                .collect::<Vec<_>>();
            matched_rule_names.sort();
            items.push(StrategyTriggerSimilarityRow {
                name: name_map.get(&sample.anchor.ts_code).cloned(),
                industry: industry_map.get(&sample.anchor.ts_code).cloned(),
                concept: concept_map.get(&sample.anchor.ts_code).cloned(),
                ts_code: sample.anchor.ts_code,
                candidate_start_trade_date: sample.anchor.start_trade_date,
                candidate_end_trade_date: sample.anchor.end_trade_date,
                outcome_start_trade_date: outcome.start_trade_date,
                outcome_end_trade_date: outcome.end_trade_date,
                similarity_score,
                trigger_similarity,
                price_volume_similarity,
                indicator_similarity,
                market_similarity,
                matched_rule_count: matched_rule_names.len(),
                matched_rule_names,
                candidate_trigger_count: sample.trigger_count,
                forward_return_pct: outcome.return_pct,
                forward_excess_return_pct: outcome.excess_return_pct,
                mfe_pct: outcome.mfe_pct,
                mae_pct: outcome.mae_pct,
                total_score: sample.total_score,
                rank: sample.rank,
            });
        }
    }
    items.sort_by(|a, b| {
        b.similarity_score
            .total_cmp(&a.similarity_score)
            .then_with(|| b.trigger_similarity.total_cmp(&a.trigger_similarity))
            .then_with(|| b.candidate_end_trade_date.cmp(&a.candidate_end_trade_date))
            .then_with(|| a.ts_code.cmp(&b.ts_code))
    });
    let rating_sample = build_rating_sample(
        &items,
        &all_trade_dates,
        window_trade_days,
        outcome_trade_days,
    );
    let outcome_summary =
        summarize_outcomes(rating_sample.iter().map(|item| OutcomeSummarySample {
            similarity_score: item.similarity_score,
            return_pct: item.forward_return_pct,
            excess_return_pct: item.forward_excess_return_pct,
            mfe_pct: item.mfe_pct,
            mae_pct: item.mae_pct,
        }));
    items.truncate(limit);
    let target_dimension = target_sample.fingerprint.dimension();
    Ok(StrategyTriggerSimilarityPageData {
        resolved_trade_date: resolved_trade_date.clone(),
        resolved_ts_code: resolved_ts_code.clone(),
        window_trade_days: target_end_index + 1 - target_start_index,
        pool_segments,
        outcome_trade_days,
        historical_cutoff_date,
        benchmark_index_code,
        kernel_names: KERNEL_NAMES.iter().map(|v| v.to_string()).collect(),
        indicator_columns: schema.indicator_columns,
        candidate_universe_count,
        candidate_anchor_count,
        evaluated_anchor_count,
        candidate_pool_truncated,
        target: StrategyTriggerSimilarityTarget {
            ts_code: resolved_ts_code.clone(),
            name: name_map.get(&resolved_ts_code).cloned(),
            industry: industry_map.get(&resolved_ts_code).cloned(),
            concept: concept_map.get(&resolved_ts_code).cloned(),
            start_trade_date: target_start_date,
            end_trade_date: resolved_trade_date,
            trigger_count: target_trigger_count,
            rule_names: target_rule_names,
            pooled_feature_dimension: target_dimension,
        },
        outcome_summary,
        items,
    })
}
