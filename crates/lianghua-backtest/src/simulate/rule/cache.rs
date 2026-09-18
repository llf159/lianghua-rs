use crate::simulate::rule::residual::{
    load_most_related_concept_map, load_stock_industry_map, stream_residual_maps,
};
use crate::simulate::rule::{
    RESIDUAL_SERIES_TARGET_POINTS, RESIDUAL_STOCK_BATCH_MAX, RESIDUAL_STOCK_BATCH_MIN,
    ResidualCacheInput, RuleDayBaseSample, RuleDayGroup, RuleLayerConfig, RuleLayerRuntimeCache,
    RuleUniverseRow, TriggeredScoreMap,
};

#[cfg(test)]
use crate::simulate::rule::RuleDbRow;

use crate::data::result_db_path;
use crate::scoring_model::ScoreDetails;
use crate::scoring_model::ScoreSummary;
use duckdb::Connection;
use duckdb::params_from_iter;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
pub fn build_rule_layer_runtime_cache(
    source_conn: &Connection,
    source_dir: &str,
    stock_adj_type: &str,
    index_ts_code: &str,
    index_beta: f64,
    concept_beta: f64,
    industry_beta: f64,
    start_date: &str,
    end_date: &str,
    layer_config: &RuleLayerConfig,
) -> Result<RuleLayerRuntimeCache, String> {
    build_rule_layer_runtime_cache_with_ts_filter(
        source_conn,
        source_dir,
        stock_adj_type,
        index_ts_code,
        index_beta,
        concept_beta,
        industry_beta,
        start_date,
        end_date,
        layer_config,
        None,
    )
}

pub fn build_rule_layer_runtime_cache_with_ts_filter(
    source_conn: &Connection,
    source_dir: &str,
    stock_adj_type: &str,
    index_ts_code: &str,
    index_beta: f64,
    concept_beta: f64,
    industry_beta: f64,
    start_date: &str,
    end_date: &str,
    layer_config: &RuleLayerConfig,
    allowed_ts_codes: Option<&HashSet<String>>,
) -> Result<RuleLayerRuntimeCache, String> {
    validate_rule_common_input(
        stock_adj_type,
        index_ts_code,
        index_beta,
        concept_beta,
        industry_beta,
        start_date,
        end_date,
        layer_config,
    )?;

    let universe_rows = filter_universe_rows_by_ts_codes(
        (|source_dir: &str,
          start_date: &str,
          end_date: &str|
         -> Result<Vec<RuleUniverseRow>, String> {
            let result_db = result_db_path(source_dir);
            if !result_db.exists() {
                return Ok(Vec::new());
            }

            let result_db_str = result_db
                .to_str()
                .ok_or_else(|| "result_db路径不是有效UTF-8".to_string())?;
            let conn = Connection::open(result_db_str)
                .map_err(|e| format!("打开scoring_result.db失败:{e}"))?;

            let mut stmt = conn
                .prepare(
                    r#"
            SELECT
                ts_code,
                trade_date
            FROM score_summary
            WHERE trade_date >= ?
              AND trade_date <= ?
            ORDER BY trade_date ASC, ts_code ASC
            "#,
                )
                .map_err(|e| format!("预编译score_summary查询失败:{e}"))?;

            let mut rows = stmt
                .query(params_from_iter([start_date.trim(), end_date.trim()]))
                .map_err(|e| format!("查询score_summary失败:{e}"))?;

            let mut out = Vec::new();
            while let Some(row) = rows
                .next()
                .map_err(|e| format!("读取score_summary失败:{e}"))?
            {
                let ts_code: String = row.get(0).map_err(|e| format!("读取ts_code失败:{e}"))?;
                let trade_date: String =
                    row.get(1).map_err(|e| format!("读取trade_date失败:{e}"))?;

                if ts_code.trim().is_empty() || trade_date.trim().is_empty() {
                    continue;
                }

                out.push(RuleUniverseRow {
                    ts_code,
                    trade_date,
                });
            }

            Ok(out)
        })(source_dir, start_date, end_date)?,
        allowed_ts_codes,
    );
    if universe_rows.is_empty() {
        return Ok(RuleLayerRuntimeCache::empty());
    }
    build_rule_layer_runtime_cache_from_universe_rows(
        source_conn,
        source_dir,
        universe_rows,
        stock_adj_type,
        index_ts_code,
        index_beta,
        concept_beta,
        industry_beta,
        start_date,
        end_date,
        layer_config,
    )
}

pub fn build_rule_layer_runtime_cache_from_stock_data(
    source_conn: &Connection,
    source_dir: &str,
    stock_adj_type: &str,
    index_ts_code: &str,
    index_beta: f64,
    concept_beta: f64,
    industry_beta: f64,
    start_date: &str,
    end_date: &str,
    layer_config: &RuleLayerConfig,
) -> Result<RuleLayerRuntimeCache, String> {
    build_rule_layer_runtime_cache_from_stock_data_with_ts_filter(
        source_conn,
        source_dir,
        stock_adj_type,
        index_ts_code,
        index_beta,
        concept_beta,
        industry_beta,
        start_date,
        end_date,
        layer_config,
        None,
    )
}

pub fn build_rule_layer_runtime_cache_from_stock_data_with_ts_filter(
    source_conn: &Connection,
    source_dir: &str,
    stock_adj_type: &str,
    index_ts_code: &str,
    index_beta: f64,
    concept_beta: f64,
    industry_beta: f64,
    start_date: &str,
    end_date: &str,
    layer_config: &RuleLayerConfig,
    allowed_ts_codes: Option<&HashSet<String>>,
) -> Result<RuleLayerRuntimeCache, String> {
    validate_rule_common_input(
        stock_adj_type,
        index_ts_code,
        index_beta,
        concept_beta,
        industry_beta,
        start_date,
        end_date,
        layer_config,
    )?;

    let universe_rows = filter_universe_rows_by_ts_codes(
        (|source_conn: &Connection,
          stock_adj_type: &str,
          start_date: &str,
          end_date: &str|
         -> Result<Vec<RuleUniverseRow>, String> {
            let mut stmt = source_conn
                .prepare(
                    r#"
            SELECT DISTINCT
                ts_code,
                trade_date
            FROM stock_data
            WHERE adj_type = ?
              AND trade_date >= ?
              AND trade_date <= ?
            ORDER BY trade_date ASC, ts_code ASC
            "#,
                )
                .map_err(|e| format!("预编译stock_data样本范围查询失败:{e}"))?;

            let mut rows = stmt
                .query(params_from_iter([
                    stock_adj_type.trim(),
                    start_date.trim(),
                    end_date.trim(),
                ]))
                .map_err(|e| format!("查询stock_data样本范围失败:{e}"))?;

            let mut out = Vec::new();
            while let Some(row) = rows
                .next()
                .map_err(|e| format!("读取stock_data样本范围失败:{e}"))?
            {
                let ts_code: String = row.get(0).map_err(|e| format!("读取ts_code失败:{e}"))?;
                let trade_date: String =
                    row.get(1).map_err(|e| format!("读取trade_date失败:{e}"))?;

                if ts_code.trim().is_empty() || trade_date.trim().is_empty() {
                    continue;
                }

                out.push(RuleUniverseRow {
                    ts_code,
                    trade_date,
                });
            }

            Ok(out)
        })(source_conn, stock_adj_type, start_date, end_date)?,
        allowed_ts_codes,
    );
    build_rule_layer_runtime_cache_from_universe_rows(
        source_conn,
        source_dir,
        universe_rows,
        stock_adj_type,
        index_ts_code,
        index_beta,
        concept_beta,
        industry_beta,
        start_date,
        end_date,
        layer_config,
    )
}

pub(crate) fn build_rule_layer_runtime_cache_from_summary_rows(
    source_conn: &Connection,
    source_dir: &str,
    score_summary_rows: &[ScoreSummary],
    stock_adj_type: &str,
    index_ts_code: &str,
    index_beta: f64,
    concept_beta: f64,
    industry_beta: f64,
    start_date: &str,
    end_date: &str,
    layer_config: &RuleLayerConfig,
) -> Result<RuleLayerRuntimeCache, String> {
    build_rule_layer_runtime_cache_from_summary_rows_with_ts_filter(
        source_conn,
        source_dir,
        score_summary_rows,
        stock_adj_type,
        index_ts_code,
        index_beta,
        concept_beta,
        industry_beta,
        start_date,
        end_date,
        layer_config,
        None,
    )
}

pub(crate) fn build_rule_layer_runtime_cache_from_summary_rows_with_ts_filter(
    source_conn: &Connection,
    source_dir: &str,
    score_summary_rows: &[ScoreSummary],
    stock_adj_type: &str,
    index_ts_code: &str,
    index_beta: f64,
    concept_beta: f64,
    industry_beta: f64,
    start_date: &str,
    end_date: &str,
    layer_config: &RuleLayerConfig,
    allowed_ts_codes: Option<&HashSet<String>>,
) -> Result<RuleLayerRuntimeCache, String> {
    validate_rule_common_input(
        stock_adj_type,
        index_ts_code,
        index_beta,
        concept_beta,
        industry_beta,
        start_date,
        end_date,
        layer_config,
    )?;

    let mut universe_rows = score_summary_rows
        .iter()
        .filter_map(|row| {
            if row.trade_date.as_str() < start_date
                || row.trade_date.as_str() > end_date
                || row.ts_code.trim().is_empty()
                || row.trade_date.trim().is_empty()
                || !ts_code_allowed(allowed_ts_codes, &row.ts_code)
            {
                return None;
            }
            Some(RuleUniverseRow {
                ts_code: row.ts_code.clone(),
                trade_date: row.trade_date.clone(),
            })
        })
        .collect::<Vec<_>>();
    universe_rows.sort_by(|left, right| {
        left.trade_date
            .cmp(&right.trade_date)
            .then_with(|| left.ts_code.cmp(&right.ts_code))
    });

    if universe_rows.is_empty() {
        return Ok(RuleLayerRuntimeCache::empty());
    }

    build_rule_layer_runtime_cache_from_universe_rows(
        source_conn,
        source_dir,
        universe_rows,
        stock_adj_type,
        index_ts_code,
        index_beta,
        concept_beta,
        industry_beta,
        start_date,
        end_date,
        layer_config,
    )
}

pub(super) fn build_rule_layer_runtime_cache_from_universe_rows(
    source_conn: &Connection,
    source_dir: &str,
    universe_rows: Vec<RuleUniverseRow>,
    stock_adj_type: &str,
    index_ts_code: &str,
    index_beta: f64,
    concept_beta: f64,
    industry_beta: f64,
    start_date: &str,
    end_date: &str,
    layer_config: &RuleLayerConfig,
) -> Result<RuleLayerRuntimeCache, String> {
    if universe_rows.is_empty() {
        return Ok(RuleLayerRuntimeCache::empty());
    }

    let concept_map = load_most_related_concept_map(source_dir)?;
    let industry_map = load_stock_industry_map(source_dir)?;

    let mut ts_code_names = universe_rows
        .iter()
        .map(|row| row.ts_code.as_str())
        .collect::<Vec<_>>();
    ts_code_names.sort_unstable();
    ts_code_names.dedup();
    let ts_codes = ts_code_names
        .iter()
        .map(|ts_code| Arc::<str>::from(*ts_code))
        .collect::<Vec<_>>();
    let ts_code_ids = ts_code_names
        .into_iter()
        .enumerate()
        .map(|(index, ts_code)| (ts_code.to_string(), index as u32))
        .collect::<HashMap<_, _>>();

    let mut trade_date_names = universe_rows
        .iter()
        .map(|row| row.trade_date.as_str())
        .collect::<Vec<_>>();
    trade_date_names.sort_unstable();
    trade_date_names.dedup();
    let trade_dates = trade_date_names
        .iter()
        .map(|trade_date| Arc::<str>::from(*trade_date))
        .collect::<Vec<_>>();
    let mut day_group_ids = trade_date_names
        .into_iter()
        .enumerate()
        .map(|(index, trade_date)| (trade_date.to_string(), index))
        .collect::<HashMap<_, _>>();

    let stock_count = ts_codes.len();
    let score_column_len = trade_dates.len().saturating_mul(stock_count);
    let mut universe_valid = vec![false; score_column_len];
    let mut sample_capacities = vec![0usize; trade_dates.len()];
    let trade_day_count = trade_dates.len();
    for row in universe_rows {
        let (Some(&ts_code_id), Some(&day_group_id)) = (
            ts_code_ids.get(&row.ts_code),
            day_group_ids.get(&row.trade_date),
        ) else {
            continue;
        };
        let flat_index = day_group_id * stock_count + ts_code_id as usize;
        if !universe_valid[flat_index] {
            universe_valid[flat_index] = true;
            sample_capacities[day_group_id] += 1;
        }
    }
    let mut day_groups = trade_dates
        .into_iter()
        .enumerate()
        .map(|(day_group_id, trade_date)| RuleDayGroup {
            trade_date,
            score_offset: day_group_id * stock_count,
            samples: Vec::with_capacity(sample_capacities[day_group_id]),
        })
        .collect::<Vec<_>>();

    let residual_stock_batch_size = if trade_day_count == 0 {
        RESIDUAL_STOCK_BATCH_MAX
    } else {
        (RESIDUAL_SERIES_TARGET_POINTS / trade_day_count)
            .clamp(RESIDUAL_STOCK_BATCH_MIN, RESIDUAL_STOCK_BATCH_MAX)
    };
    stream_residual_maps(
        source_conn,
        source_dir,
        ts_codes.iter().map(|ts_code| ts_code.to_string()).collect(),
        &concept_map,
        &industry_map,
        &ResidualCacheInput {
            stock_adj_type,
            index_ts_code,
            index_beta,
            concept_beta,
            industry_beta,
            start_date,
            end_date,
            backtest_period: layer_config.backtest_period,
            min_listed_trade_days: layer_config.min_listed_trade_days,
        },
        residual_stock_batch_size,
        |ts_code, residual_map| {
            let Some(&ts_code_id) = ts_code_ids.get(&ts_code) else {
                return Ok(());
            };
            for (trade_date, outcome) in residual_map {
                let Some(&day_group_id) = day_group_ids.get(&trade_date) else {
                    continue;
                };
                let flat_index = day_group_id * stock_count + ts_code_id as usize;
                if !universe_valid[flat_index] {
                    continue;
                }
                day_groups[day_group_id].samples.push(RuleDayBaseSample {
                    ts_code_id,
                    residual_return: outcome.residual_return,
                    er_change: outcome.er_change,
                });
            }
            Ok(())
        },
    )?;
    drop(universe_valid);

    day_groups.retain(|group| !group.samples.is_empty());
    day_group_ids.clear();
    for (day_group_id, group) in day_groups.iter_mut().enumerate() {
        group.samples.shrink_to_fit();
        group.score_offset = day_group_id * stock_count;
        day_group_ids.insert(group.trade_date.to_string(), day_group_id);
    }
    day_groups.shrink_to_fit();
    Ok(RuleLayerRuntimeCache {
        score_column_len: day_groups.len().saturating_mul(stock_count),
        day_groups,
        ts_codes,
        ts_code_ids,
        day_group_ids,
    })
}

pub(super) fn validate_rule_common_input(
    stock_adj_type: &str,
    index_ts_code: &str,
    index_beta: f64,
    concept_beta: f64,
    industry_beta: f64,
    start_date: &str,
    end_date: &str,
    layer_config: &RuleLayerConfig,
) -> Result<(), String> {
    if stock_adj_type.trim().is_empty() {
        return Err("股票复权类型不能为空".to_string());
    }
    if index_ts_code.trim().is_empty() {
        return Err("指数代码不能为空".to_string());
    }
    if start_date.trim().is_empty() || end_date.trim().is_empty() {
        return Err("区间日期不能为空".to_string());
    }
    if start_date > end_date {
        return Err(format!(
            "区间日期非法:start_date({})大于end_date({})",
            start_date, end_date
        ));
    }
    if !index_beta.is_finite() {
        return Err("指数系数必须是有限数字".to_string());
    }
    if !concept_beta.is_finite() {
        return Err("概念系数必须是有限数字".to_string());
    }
    if !industry_beta.is_finite() {
        return Err("行业系数必须是有限数字".to_string());
    }
    layer_config.validate()
}

#[cfg(test)]
pub(super) fn build_triggered_score_map(rule_rows: Vec<RuleDbRow>) -> TriggeredScoreMap {
    let mut rows_by_ts: TriggeredScoreMap = HashMap::new();
    for RuleDbRow {
        ts_code,
        trade_date,
        rule_score,
        ..
    } in rule_rows
    {
        rows_by_ts
            .entry(ts_code)
            .or_default()
            .insert(trade_date, rule_score);
    }
    rows_by_ts
}

pub(super) fn build_triggered_score_maps_from_detail_rows(
    rule_names: &[String],
    score_detail_rows: &[ScoreDetails],
    start_date: &str,
    end_date: &str,
) -> HashMap<String, TriggeredScoreMap> {
    let rule_name_set: HashSet<&str> = rule_names.iter().map(String::as_str).collect();
    let mut rows_by_rule: HashMap<String, TriggeredScoreMap> = HashMap::new();

    for row in score_detail_rows {
        if !rule_name_set.contains(row.rule_name.as_str())
            || row.trade_date.as_str() < start_date
            || row.trade_date.as_str() > end_date
            || !row.rule_score.is_finite()
            || row.ts_code.trim().is_empty()
            || row.trade_date.trim().is_empty()
        {
            continue;
        }

        rows_by_rule
            .entry(row.rule_name.clone())
            .or_default()
            .entry(row.ts_code.clone())
            .or_default()
            .insert(row.trade_date.clone(), row.rule_score);
    }

    rows_by_rule
}

pub(super) fn ts_code_allowed(allowed_ts_codes: Option<&HashSet<String>>, ts_code: &str) -> bool {
    let Some(allowed_ts_codes) = allowed_ts_codes else {
        return true;
    };
    let normalized = ts_code.trim().to_ascii_uppercase();
    allowed_ts_codes.contains(normalized.as_str())
}

pub(super) fn filter_universe_rows_by_ts_codes(
    universe_rows: Vec<RuleUniverseRow>,
    allowed_ts_codes: Option<&HashSet<String>>,
) -> Vec<RuleUniverseRow> {
    if allowed_ts_codes.is_none() {
        return universe_rows;
    }
    universe_rows
        .into_iter()
        .filter(|row| ts_code_allowed(allowed_ts_codes, &row.ts_code))
        .collect()
}

#[cfg(test)]
mod tests {

    use crate::data::result_db_path;
    use crate::data::source_db_path;
    use crate::simulate::rule::RuleLayerConfig;
    use crate::simulate::rule::cache::build_rule_layer_runtime_cache;
    use crate::simulate::rule::cache::build_rule_layer_runtime_cache_from_stock_data;
    use crate::simulate::rule::metrics::calc_rule_layer_metrics_from_cache;
    use crate::simulate::rule::metrics::calc_rule_layer_metrics_with_samples_from_cache;
    use crate::simulate::rule::metrics::calc_rule_layer_metrics_with_triggered_samples_from_cache;
    use crate::simulate::rule::metrics::calc_rule_layer_metrics_with_validation_from_cache;
    use crate::simulate::rule::metrics::collect_triggered_rule_samples_from_cache;
    use crate::simulate::rule::test_support::*;
    use duckdb::Connection;
    use std::collections::HashMap;
    #[test]
    fn runtime_cache_prefers_stored_er_indicator_column() {
        let source_dir = temp_source_dir();
        let source_dir_str = source_dir.to_str().expect("utf8 source dir");
        prepare_test_files(source_dir_str);

        let source_conn = Connection::open(source_db_path(source_dir_str)).expect("open source db");
        source_conn
            .execute("ALTER TABLE stock_data ADD COLUMN ER DOUBLE", [])
            .expect("add ER");
        source_conn
            .execute(
                r#"
                UPDATE stock_data
                SET ER = CASE trade_date
                    WHEN '20240102' THEN 0.10
                    WHEN '20240103' THEN 0.20
                    WHEN '20240104' THEN 0.35
                    ELSE NULL
                END
                WHERE ts_code = '000001.SZ' AND adj_type = 'qfq'
                "#,
                [],
            )
            .expect("update ER");

        let layer_config = RuleLayerConfig {
            min_samples_per_day: 1,
            backtest_period: 1,
            min_listed_trade_days: 0,
        };
        let runtime_cache = build_rule_layer_runtime_cache(
            &source_conn,
            source_dir_str,
            "qfq",
            "000300.SH",
            0.0,
            0.0,
            0.0,
            "20240102",
            "20240104",
            &layer_config,
        )
        .expect("runtime cache");
        let triggered_score_map = HashMap::from([(
            "000001.SZ".to_string(),
            HashMap::from([("20240102".to_string(), 1.0), ("20240103".to_string(), 1.0)]),
        )]);
        let metrics =
            calc_rule_layer_metrics_from_cache(&runtime_cache, &triggered_score_map, &layer_config)
                .expect("metrics");

        assert_opt_close(metrics.avg_er_change, Some(0.125));
        assert_eq!(metrics.er_change_sample_count, 2);
    }

    #[test]
    fn runtime_cache_uses_triggered_residual_mean_and_full_universe_layer_metrics() {
        let source_dir = temp_source_dir();
        let source_dir_str = source_dir.to_str().expect("utf8 source dir");
        prepare_test_files(source_dir_str);

        let source_conn = Connection::open(source_db_path(source_dir_str)).expect("open source db");
        let runtime_cache = build_rule_layer_runtime_cache(
            &source_conn,
            source_dir_str,
            "qfq",
            "000300.SH",
            0.0,
            0.0,
            0.0,
            "20240102",
            "20240104",
            &RuleLayerConfig {
                min_samples_per_day: 1,
                backtest_period: 1,
                min_listed_trade_days: 0,
            },
        )
        .expect("build runtime cache");
        let triggered_score_map = HashMap::from([(
            "000001.SZ".to_string(),
            HashMap::from([("20240102".to_string(), 1.5_f64)]),
        )]);

        let full_samples = calc_rule_layer_metrics_with_samples_from_cache(
            &runtime_cache,
            &triggered_score_map,
            &RuleLayerConfig {
                min_samples_per_day: 1,
                backtest_period: 1,
                min_listed_trade_days: 0,
            },
        )
        .expect("compute full samples");
        let triggered_samples = calc_rule_layer_metrics_with_triggered_samples_from_cache(
            &runtime_cache,
            &triggered_score_map,
            &RuleLayerConfig {
                min_samples_per_day: 1,
                backtest_period: 1,
                min_listed_trade_days: 0,
            },
        )
        .expect("compute triggered samples");
        let validation = calc_rule_layer_metrics_with_validation_from_cache(
            &runtime_cache,
            &triggered_score_map,
            &RuleLayerConfig {
                min_samples_per_day: 1,
                backtest_period: 1,
                min_listed_trade_days: 0,
            },
        )
        .expect("compute validation aggregates");
        let collected_triggered =
            collect_triggered_rule_samples_from_cache(&runtime_cache, &triggered_score_map);

        assert_eq!(full_samples.metrics, triggered_samples.metrics);
        assert_eq!(full_samples.metrics, validation.metrics);
        assert_eq!(full_samples.metrics.points.len(), 1);
        assert_eq!(full_samples.metrics.points[0].trade_date, "20240102");
        assert_eq!(full_samples.metrics.points[0].sample_count, 2);
        assert_opt_close(full_samples.metrics.points[0].avg_rule_score, Some(0.75));
        assert_opt_close(
            full_samples.metrics.points[0].avg_residual_return,
            Some(3.0),
        );
        assert_opt_close(
            full_samples.metrics.points[0].avg_excess_residual_return,
            Some(1.0),
        );
        assert_opt_close(full_samples.metrics.points[0].top_bottom_spread, Some(2.0));
        assert_opt_close(full_samples.metrics.points[0].ic, Some(1.0));
        assert_opt_close(full_samples.metrics.avg_residual_mean, Some(3.0));
        assert_opt_close(full_samples.metrics.avg_excess_residual_mean, Some(1.0));
        assert_opt_close(full_samples.metrics.spread_mean, Some(2.0));
        assert_opt_close(full_samples.metrics.ic_mean, Some(1.0));
        assert_eq!(full_samples.samples.len(), 4);
        assert_eq!(
            full_samples
                .samples
                .iter()
                .filter(|sample| sample.rule_score.abs() < 1e-12)
                .count(),
            3
        );

        assert_eq!(triggered_samples.triggered_samples.len(), 1);
        assert_eq!(triggered_samples.triggered_samples, collected_triggered);
        assert_eq!(validation.triggered_samples, collected_triggered);
        assert_eq!(validation.daily_score_layers.len(), 1);
        assert_eq!(validation.daily_score_layers[0].trade_date, "20240102");
        assert_eq!(validation.daily_score_layers[0].groups.len(), 2);
        assert_eq!(validation.daily_score_layers[0].groups[0].score, 0.0);
        assert_eq!(validation.daily_score_layers[0].groups[0].sample_count, 1);
        assert_eq!(validation.daily_score_layers[0].groups[1].score, 1.5);
        assert_eq!(validation.daily_score_layers[0].groups[1].sample_count, 1);
        assert_eq!(
            validation.return_distribution_counts.iter().sum::<usize>(),
            4
        );
        assert_eq!(triggered_samples.triggered_samples[0].ts_code, "000001.SZ");
        assert_eq!(
            triggered_samples.triggered_samples[0].trade_date,
            "20240102"
        );
        assert!((triggered_samples.triggered_samples[0].rule_score - 1.5).abs() < 1e-12);
    }

    #[test]
    fn stock_data_runtime_cache_is_not_limited_by_score_summary_dates() {
        let source_dir = temp_source_dir();
        let source_dir_str = source_dir.to_str().expect("utf8 source dir");
        prepare_test_files(source_dir_str);

        let result_conn = Connection::open(result_db_path(source_dir_str)).expect("open result db");
        result_conn
            .execute(
                "DELETE FROM score_summary WHERE trade_date = '20240103'",
                [],
            )
            .expect("delete score summary date");

        let source_conn = Connection::open(source_db_path(source_dir_str)).expect("open source db");
        let layer_config = RuleLayerConfig {
            min_samples_per_day: 1,
            backtest_period: 1,
            min_listed_trade_days: 0,
        };
        let triggered_score_map = HashMap::from([
            (
                "000001.SZ".to_string(),
                HashMap::from([
                    ("20240102".to_string(), 1.0_f64),
                    ("20240103".to_string(), 1.0_f64),
                ]),
            ),
            (
                "000002.SZ".to_string(),
                HashMap::from([
                    ("20240102".to_string(), 1.0_f64),
                    ("20240103".to_string(), 1.0_f64),
                ]),
            ),
        ]);

        let result_limited_cache = build_rule_layer_runtime_cache(
            &source_conn,
            source_dir_str,
            "qfq",
            "000300.SH",
            0.0,
            0.0,
            0.0,
            "20240102",
            "20240104",
            &layer_config,
        )
        .expect("build result limited runtime cache");
        let stock_data_cache = build_rule_layer_runtime_cache_from_stock_data(
            &source_conn,
            source_dir_str,
            "qfq",
            "000300.SH",
            0.0,
            0.0,
            0.0,
            "20240102",
            "20240104",
            &layer_config,
        )
        .expect("build stock data runtime cache");

        let result_limited_metrics = calc_rule_layer_metrics_with_samples_from_cache(
            &result_limited_cache,
            &triggered_score_map,
            &layer_config,
        )
        .expect("compute result limited metrics");
        let stock_data_metrics = calc_rule_layer_metrics_with_samples_from_cache(
            &stock_data_cache,
            &triggered_score_map,
            &layer_config,
        )
        .expect("compute stock data metrics");

        assert_eq!(
            result_limited_metrics
                .metrics
                .points
                .iter()
                .map(|point| point.trade_date.as_str())
                .collect::<Vec<_>>(),
            vec!["20240102"]
        );
        assert_eq!(
            stock_data_metrics
                .metrics
                .points
                .iter()
                .map(|point| point.trade_date.as_str())
                .collect::<Vec<_>>(),
            vec!["20240102", "20240103"]
        );
    }
}
