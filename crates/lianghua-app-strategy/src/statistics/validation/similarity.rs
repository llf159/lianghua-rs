//! 表达式组合与既有策略的相似度、重复性与正交指标。
use std::hash::{Hash, Hasher};

use crate::data::result_db_path;
use crate::simulate::dimension::{SignalPairMoments, calc_signal_pair_metrics};
use crate::simulate::fp_utils::pearson_corr;
use crate::simulate::rule::RuleLayerSamplePoint;
use crate::statistics::backtest::RULE_BACKTEST_EPS;
use crate::statistics::common::open_result_conn;
use crate::statistics::validation::RuleValidationSimilarityRow;
use crate::statistics::validation::samples::compare_option_f64_desc;
use duckdb::{Connection, params};
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
#[derive(Debug, Clone)]
pub(in crate::statistics) struct ValidationSimilarityCache {
    pub(in crate::statistics) total_samples: f64,
    pub(in crate::statistics) rule_names: Vec<String>,
    pub(in crate::statistics) rule_hit_counts: Vec<usize>,
    pub(in crate::statistics) pair_to_rule_indices: HashMap<String, Vec<usize>>,
}

pub(in crate::statistics) struct CompactRuleSimilarityCache {
    pub(in crate::statistics) total_samples: f64,
    pub(in crate::statistics) rule_names: Vec<String>,
    pub(in crate::statistics) rule_hit_counts: Vec<usize>,
    pub(in crate::statistics) hits_by_pair: Vec<(u64, u32)>,
}

pub(in crate::statistics) fn validation_pair_key(ts_code: &str, trade_date: &str) -> String {
    let mut key = String::with_capacity(ts_code.len() + trade_date.len() + 1);
    key.push_str(ts_code);
    key.push('\0');
    key.push_str(trade_date);
    key
}

pub(in crate::statistics) fn validation_pair_hash(ts_code: &str, trade_date: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    ts_code.hash(&mut hasher);
    trade_date.hash(&mut hasher);
    hasher.finish()
}

pub(in crate::statistics) fn load_compact_rule_similarity_cache(
    source_path: &str,
    start_date: &str,
    end_date: &str,
) -> Result<CompactRuleSimilarityCache, String> {
    let result_conn = open_result_conn(source_path)?;
    let total_samples = result_conn
        .query_row(
            "SELECT COUNT(*) FROM score_summary WHERE trade_date >= ? AND trade_date <= ?",
            params![start_date, end_date],
            |row| row.get::<_, i64>(0),
        )
        .map_err(|error| format!("读取策略相似度总样本数失败: {error}"))?
        .max(0) as f64;
    let mut stmt = result_conn
        .prepare(
            r#"
            SELECT rule_name, ts_code, trade_date
            FROM rule_details
            WHERE trade_date >= ?
              AND trade_date <= ?
              AND TRY_CAST(rule_score AS DOUBLE) IS NOT NULL
              AND ABS(TRY_CAST(rule_score AS DOUBLE)) > 1e-12
            "#,
        )
        .map_err(|error| format!("预编译策略相似度紧凑缓存查询失败: {error}"))?;
    let mut rows = stmt
        .query(params![start_date, end_date])
        .map_err(|error| format!("查询策略相似度紧凑缓存失败: {error}"))?;
    let mut rule_names = Vec::new();
    let mut rule_name_to_index = HashMap::<String, u32>::new();
    let mut rule_hit_counts = Vec::new();
    let mut hits_by_pair = Vec::new();
    while let Some(row) = rows
        .next()
        .map_err(|error| format!("读取策略相似度紧凑缓存失败: {error}"))?
    {
        let rule_name: String = row
            .get(0)
            .map_err(|error| format!("读取规则名失败: {error}"))?;
        let ts_code: String = row
            .get(1)
            .map_err(|error| format!("读取代码失败: {error}"))?;
        let trade_date: String = row
            .get(2)
            .map_err(|error| format!("读取交易日失败: {error}"))?;
        let rule_index = if let Some(index) = rule_name_to_index.get(&rule_name) {
            *index
        } else {
            let index = u32::try_from(rule_names.len())
                .map_err(|_| "策略数量超过紧凑相似度缓存上限".to_string())?;
            rule_name_to_index.insert(rule_name.clone(), index);
            rule_names.push(rule_name);
            rule_hit_counts.push(0);
            index
        };
        rule_hit_counts[rule_index as usize] += 1;
        hits_by_pair.push((validation_pair_hash(&ts_code, &trade_date), rule_index));
    }
    hits_by_pair.sort_unstable();
    hits_by_pair.shrink_to_fit();
    Ok(CompactRuleSimilarityCache {
        total_samples,
        rule_names,
        rule_hit_counts,
        hits_by_pair,
    })
}

pub(in crate::statistics) fn build_compact_rule_similarity_rows(
    cache: &CompactRuleSimilarityCache,
    triggered_samples: &[crate::simulate::rule::RuleLayerSamplePoint],
    exclude_rule_name: &str,
    explain_map: &HashMap<String, String>,
) -> Vec<RuleValidationSimilarityRow> {
    if triggered_samples.is_empty() {
        return Vec::new();
    }
    let mut overlap_counts = HashMap::<u32, usize>::new();
    for sample in triggered_samples {
        let pair_hash = validation_pair_hash(&sample.ts_code, &sample.trade_date);
        let start = cache
            .hits_by_pair
            .partition_point(|(candidate, _)| *candidate < pair_hash);
        for (_, rule_index) in cache.hits_by_pair[start..]
            .iter()
            .take_while(|(candidate, _)| *candidate == pair_hash)
        {
            *overlap_counts.entry(*rule_index).or_default() += 1;
        }
    }
    let trigger_count = triggered_samples.len() as f64;
    let mut rows = overlap_counts
        .into_iter()
        .filter_map(|(rule_index, overlap_samples)| {
            let rule_name = cache.rule_names.get(rule_index as usize)?;
            if rule_name == exclude_rule_name {
                return None;
            }
            let existing_count = cache
                .rule_hit_counts
                .get(rule_index as usize)
                .copied()
                .unwrap_or(0) as f64;
            Some(RuleValidationSimilarityRow {
                rule_name: rule_name.clone(),
                explain: explain_map.get(rule_name).cloned(),
                overlap_samples,
                overlap_rate_vs_validation: Some(overlap_samples as f64 / trigger_count),
                overlap_rate_vs_existing: (existing_count > 0.0)
                    .then_some(overlap_samples as f64 / existing_count),
                overlap_lift: (cache.total_samples > 0.0 && existing_count > 0.0).then_some(
                    overlap_samples as f64 * cache.total_samples / (trigger_count * existing_count),
                ),
                jaccard: None,
                phi: None,
                score_pearson: None,
                return_pearson: None,
                shared_return_days: 0,
            })
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        right
            .overlap_samples
            .cmp(&left.overlap_samples)
            .then_with(|| left.rule_name.cmp(&right.rule_name))
    });
    rows.truncate(20);
    rows
}

pub(in crate::statistics) fn empty_validation_similarity_cache() -> ValidationSimilarityCache {
    ValidationSimilarityCache {
        total_samples: 0.0,
        rule_names: Vec::new(),
        rule_hit_counts: Vec::new(),
        pair_to_rule_indices: HashMap::new(),
    }
}

#[cfg(test)]
pub(in crate::statistics) fn build_validation_similarity_rows(
    similarity_cache: &ValidationSimilarityCache,
    triggered_samples: &[crate::simulate::rule::RuleLayerSamplePoint],
    exclude_rule_name: Option<&str>,
    explain_map: &HashMap<String, String>,
) -> Vec<RuleValidationSimilarityRow> {
    let mut overlap_hit_count = HashMap::<usize, usize>::new();

    for sample in triggered_samples {
        let Some(rule_indices) = similarity_cache
            .pair_to_rule_indices
            .get(&validation_pair_key(&sample.ts_code, &sample.trade_date))
        else {
            continue;
        };

        for rule_index in rule_indices {
            *overlap_hit_count.entry(*rule_index).or_default() += 1;
        }
    }

    build_validation_similarity_rows_from_overlap(
        similarity_cache,
        triggered_samples.len(),
        overlap_hit_count,
        exclude_rule_name,
        explain_map,
    )
}

pub(in crate::statistics) fn build_validation_similarity_rows_from_overlap(
    similarity_cache: &ValidationSimilarityCache,
    combo_hit_count: usize,
    overlap_hit_count: HashMap<usize, usize>,
    exclude_rule_name: Option<&str>,
    explain_map: &HashMap<String, String>,
) -> Vec<RuleValidationSimilarityRow> {
    let combo_hit_count = combo_hit_count as f64;
    if combo_hit_count <= 0.0 {
        return Vec::new();
    }

    let excluded_rule_name = exclude_rule_name
        .map(str::trim)
        .filter(|value| !value.is_empty());

    let mut out = overlap_hit_count
        .into_iter()
        .filter_map(|(rule_index, overlap_samples)| {
            if overlap_samples == 0 {
                return None;
            }

            let rule_name = similarity_cache.rule_names.get(rule_index)?;
            if excluded_rule_name.is_some_and(|excluded| rule_name == excluded) {
                return None;
            }

            let existing_count = similarity_cache
                .rule_hit_counts
                .get(rule_index)
                .copied()
                .unwrap_or(0) as f64;
            let overlap_rate_vs_validation = Some(overlap_samples as f64 / combo_hit_count);
            let overlap_rate_vs_existing = if existing_count > 0.0 {
                Some(overlap_samples as f64 / existing_count)
            } else {
                None
            };
            let overlap_lift = if similarity_cache.total_samples > 0.0 && existing_count > 0.0 {
                Some(
                    overlap_samples as f64 * similarity_cache.total_samples
                        / (combo_hit_count * existing_count),
                )
            } else {
                None
            };

            Some(RuleValidationSimilarityRow {
                rule_name: rule_name.clone(),
                explain: explain_map.get(rule_name).cloned(),
                overlap_samples,
                overlap_rate_vs_validation,
                overlap_rate_vs_existing,
                overlap_lift,
                jaccard: None,
                phi: None,
                score_pearson: None,
                return_pearson: None,
                shared_return_days: 0,
            })
        })
        .collect::<Vec<_>>();

    out.sort_by(|left, right| {
        right
            .overlap_samples
            .cmp(&left.overlap_samples)
            .then_with(|| left.rule_name.cmp(&right.rule_name))
    });
    out.truncate(20);
    out
}

/// 表达式验证的完整 eligible universe：与回测同口径的全部 `(ts_code, trade_date)` 样本。
///
/// 未触发样本的分数按 0 参与统计，保证 Score Pearson 等横截面相关性使用完整范围。
pub(in crate::statistics) struct ValidationUniverseIndex {
    pub(in crate::statistics) residual_returns: Vec<f64>,
    pub(in crate::statistics) day_indices: Vec<u32>,
    pub(in crate::statistics) trade_dates: Vec<String>,
    pub(in crate::statistics) index_by_pair: HashMap<u64, u32>,
}

impl ValidationUniverseIndex {
    pub(in crate::statistics) fn len(&self) -> usize {
        self.residual_returns.len()
    }
}

pub(in crate::statistics) fn build_validation_universe_index(
    samples: &[RuleLayerSamplePoint],
) -> ValidationUniverseIndex {
    let mut trade_dates = Vec::<String>::new();
    let mut day_index_by_name = HashMap::<String, u32>::new();
    let mut residual_returns = Vec::with_capacity(samples.len());
    let mut day_indices = Vec::with_capacity(samples.len());
    let mut index_by_pair = HashMap::with_capacity(samples.len());

    for sample in samples {
        let day_index = if let Some(index) = day_index_by_name.get(&sample.trade_date) {
            *index
        } else {
            let index = trade_dates.len() as u32;
            trade_dates.push(sample.trade_date.clone());
            day_index_by_name.insert(sample.trade_date.clone(), index);
            index
        };
        let Ok(universe_index) = u32::try_from(residual_returns.len()) else {
            break;
        };
        residual_returns.push(sample.residual_return);
        day_indices.push(day_index);
        index_by_pair.insert(
            validation_pair_hash(&sample.ts_code, &sample.trade_date),
            universe_index,
        );
    }

    ValidationUniverseIndex {
        residual_returns,
        day_indices,
        trade_dates,
        index_by_pair,
    }
}

#[derive(Debug, Clone, Copy)]
pub(in crate::statistics) struct ValidationRuleHit {
    pub(in crate::statistics) pair_hash: u64,
    pub(in crate::statistics) universe_index: u32,
    pub(in crate::statistics) rule_index: u32,
    pub(in crate::statistics) score: f64,
}

/// 结果库里已有策略在 eligible universe 内的紧凑触发索引，按 `pair_hash` 升序排列，
/// 使每个表达式组合都能用一次二分定位同日同股的已有策略分数。
#[derive(Debug, Default)]
pub(in crate::statistics) struct ValidationExistingRuleScoreIndex {
    pub(in crate::statistics) rule_names: Vec<String>,
    pub(in crate::statistics) hit_counts: Vec<usize>,
    pub(in crate::statistics) score_sums: Vec<f64>,
    pub(in crate::statistics) score_square_sums: Vec<f64>,
    pub(in crate::statistics) hits: Vec<ValidationRuleHit>,
}

pub(in crate::statistics) fn load_validation_existing_rule_score_index(
    source_path: &str,
    start_date: &str,
    end_date: &str,
    universe: &ValidationUniverseIndex,
) -> ValidationExistingRuleScoreIndex {
    if !result_db_path(source_path).exists() {
        return ValidationExistingRuleScoreIndex::default();
    }
    let Ok(result_conn) = open_result_conn(source_path) else {
        return ValidationExistingRuleScoreIndex::default();
    };
    // 与原有相似度缓存一致：结果库不可用时只放弃重复性研究，不阻断表达式验证。
    (|result_conn: &Connection| -> Result<ValidationExistingRuleScoreIndex, String> {
        let mut stmt = result_conn
            .prepare(
                r#"
                SELECT
                    rule_name,
                    ts_code,
                    trade_date,
                    TRY_CAST(rule_score AS DOUBLE)
                FROM rule_details
                WHERE trade_date >= ?
                  AND trade_date <= ?
                  AND TRY_CAST(rule_score AS DOUBLE) IS NOT NULL
                  AND ABS(TRY_CAST(rule_score AS DOUBLE)) > 1e-12
                "#,
            )
            .map_err(|error| format!("预编译已有策略分数查询失败: {error}"))?;
        let mut rows = stmt
            .query(params![start_date, end_date])
            .map_err(|error| format!("查询已有策略分数失败: {error}"))?;

        let mut index = ValidationExistingRuleScoreIndex::default();
        let mut rule_index_by_name = HashMap::<String, u32>::new();
        while let Some(row) = rows
            .next()
            .map_err(|error| format!("读取已有策略分数失败: {error}"))?
        {
            let rule_name: String = row.get(0).map_err(|e| format!("读取规则名失败: {e}"))?;
            let ts_code: String = row.get(1).map_err(|e| format!("读取代码失败: {e}"))?;
            let trade_date: String = row.get(2).map_err(|e| format!("读取交易日失败: {e}"))?;
            let score: f64 = row.get(3).map_err(|e| format!("读取规则分失败: {e}"))?;
            let pair_hash = validation_pair_hash(&ts_code, &trade_date);
            let Some(&universe_index) = universe.index_by_pair.get(&pair_hash) else {
                continue;
            };
            let rule_index = if let Some(existing) = rule_index_by_name.get(&rule_name) {
                *existing
            } else {
                let created = index.rule_names.len() as u32;
                rule_index_by_name.insert(rule_name.clone(), created);
                index.rule_names.push(rule_name);
                index.hit_counts.push(0);
                index.score_sums.push(0.0);
                index.score_square_sums.push(0.0);
                created
            };
            index.hit_counts[rule_index as usize] += 1;
            index.score_sums[rule_index as usize] += score;
            index.score_square_sums[rule_index as usize] += score * score;
            index.hits.push(ValidationRuleHit {
                pair_hash,
                universe_index,
                rule_index,
                score,
            });
        }
        index
            .hits
            .sort_unstable_by_key(|hit| (hit.pair_hash, hit.rule_index));
        Ok(index)
    })(&result_conn)
    .unwrap_or_default()
}

/// 已有策略的日度收益，口径与相关性与正交研究一致：`Σ(score × residual) / Σ|score|`，
/// 分母只在 eligible universe 内累计，未触发样本的分数按 0 处理。
#[derive(Clone, Copy)]
pub(in crate::statistics) struct ValidationRuleDailyAgg {
    pub(in crate::statistics) score_residual_sum: f64,
    pub(in crate::statistics) absolute_score_sum: f64,
    pub(in crate::statistics) sample_count: usize,
}

pub(in crate::statistics) fn build_validation_existing_rule_daily_returns(
    universe: &ValidationUniverseIndex,
    index: &ValidationExistingRuleScoreIndex,
    min_samples_per_day: usize,
) -> Vec<HashMap<String, f64>> {
    let mut daily = vec![HashMap::<u32, ValidationRuleDailyAgg>::new(); index.rule_names.len()];
    for hit in &index.hits {
        let residual = universe.residual_returns[hit.universe_index as usize];
        if !residual.is_finite() {
            continue;
        }
        let day_index = universe.day_indices[hit.universe_index as usize];
        let entry =
            daily[hit.rule_index as usize]
                .entry(day_index)
                .or_insert(ValidationRuleDailyAgg {
                    score_residual_sum: 0.0,
                    absolute_score_sum: 0.0,
                    sample_count: 0,
                });
        entry.score_residual_sum += hit.score * residual;
        entry.absolute_score_sum += hit.score.abs();
        entry.sample_count += 1;
    }

    daily
        .into_iter()
        .map(|days| {
            days.into_iter()
                .filter_map(|(day_index, aggregate)| {
                    if aggregate.sample_count < min_samples_per_day
                        || aggregate.absolute_score_sum <= RULE_BACKTEST_EPS
                    {
                        return None;
                    }
                    let trade_date = universe.trade_dates.get(day_index as usize)?;
                    Some((
                        trade_date.clone(),
                        aggregate.score_residual_sum / aggregate.absolute_score_sum,
                    ))
                })
                .collect()
        })
        .collect()
}

/// 表达式组合与已有策略的重复性：Jaccard、Phi、完整 universe 的 Score Pearson
/// 以及日度收益 Pearson（收益口径统一为 `Σ(score × residual) / Σ|score|`）。
/// 只做描述性统计，不产出任何评分或推荐。
pub(in crate::statistics) fn build_validation_expression_similarity_rows(
    universe: &ValidationUniverseIndex,
    index: &ValidationExistingRuleScoreIndex,
    rule_daily_returns: &[HashMap<String, f64>],
    candidate_scores: &HashMap<u64, f64>,
    candidate_daily_returns: &HashMap<String, f64>,
    exclude_rule_name: Option<&str>,
    explain_map: &HashMap<String, String>,
) -> Vec<RuleValidationSimilarityRow> {
    if candidate_scores.is_empty() || index.rule_names.is_empty() {
        return Vec::new();
    }

    let left_trigger_count = candidate_scores.len();
    let left_sum = candidate_scores.values().sum::<f64>();
    let left_square_sum = candidate_scores
        .values()
        .map(|value| value * value)
        .sum::<f64>();
    let mut joint_counts = vec![0usize; index.rule_names.len()];
    let mut cross_sums = vec![0.0_f64; index.rule_names.len()];
    for (pair_hash, score) in candidate_scores {
        let start = index.hits.partition_point(|hit| hit.pair_hash < *pair_hash);
        for hit in index.hits[start..]
            .iter()
            .take_while(|hit| hit.pair_hash == *pair_hash)
        {
            joint_counts[hit.rule_index as usize] += 1;
            cross_sums[hit.rule_index as usize] += score * hit.score;
        }
    }

    let excluded_rule_name = exclude_rule_name
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let universe_count = universe.len();
    let mut rows = index
        .rule_names
        .iter()
        .enumerate()
        .filter_map(|(rule_index, rule_name)| {
            if excluded_rule_name.is_some_and(|excluded| rule_name == excluded) {
                return None;
            }
            let right_trigger_count = index.hit_counts[rule_index];
            let joint_trigger_count = joint_counts[rule_index];
            let metrics = calc_signal_pair_metrics(SignalPairMoments {
                universe_count,
                left_trigger_count,
                right_trigger_count,
                joint_trigger_count,
                left_sum,
                right_sum: index.score_sums[rule_index],
                left_square_sum,
                right_square_sum: index.score_square_sums[rule_index],
                cross_sum: cross_sums[rule_index],
            });

            let mut left_values = Vec::new();
            let mut right_values = Vec::new();
            if let Some(rule_daily) = rule_daily_returns.get(rule_index) {
                for (trade_date, left_value) in candidate_daily_returns {
                    if let Some(right_value) = rule_daily.get(trade_date) {
                        left_values.push(*left_value);
                        right_values.push(*right_value);
                    }
                }
            }

            Some(RuleValidationSimilarityRow {
                rule_name: rule_name.clone(),
                explain: explain_map.get(rule_name).cloned(),
                overlap_samples: joint_trigger_count,
                overlap_rate_vs_validation: (left_trigger_count > 0)
                    .then_some(joint_trigger_count as f64 / left_trigger_count as f64),
                overlap_rate_vs_existing: (right_trigger_count > 0)
                    .then_some(joint_trigger_count as f64 / right_trigger_count as f64),
                overlap_lift: (left_trigger_count > 0 && right_trigger_count > 0).then_some(
                    joint_trigger_count as f64 * universe_count as f64
                        / (left_trigger_count as f64 * right_trigger_count as f64),
                ),
                jaccard: metrics.jaccard,
                phi: metrics.phi,
                score_pearson: metrics.score_pearson,
                return_pearson: pearson_corr(&left_values, &right_values),
                shared_return_days: left_values.len(),
            })
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        compare_option_f64_desc(left.jaccard, right.jaccard)
            .then_with(|| right.overlap_samples.cmp(&left.overlap_samples))
            .then_with(|| left.rule_name.cmp(&right.rule_name))
    });
    rows.truncate(20);
    rows
}

#[cfg(test)]
mod tests {
    use crate::simulate::rule::RuleLayerSamplePoint;
    use crate::statistics::test_support::*;
    use crate::statistics::validation::similarity::CompactRuleSimilarityCache;
    use crate::statistics::validation::similarity::ValidationExistingRuleScoreIndex;
    use crate::statistics::validation::similarity::ValidationRuleHit;
    use crate::statistics::validation::similarity::ValidationSimilarityCache;
    use crate::statistics::validation::similarity::build_compact_rule_similarity_rows;
    use crate::statistics::validation::similarity::build_validation_expression_similarity_rows;
    use crate::statistics::validation::similarity::build_validation_similarity_rows;
    use crate::statistics::validation::similarity::build_validation_universe_index;
    use crate::statistics::validation::similarity::validation_pair_hash;
    use crate::statistics::validation::similarity::validation_pair_key;
    use crate::statistics::validation::walk_forward::build_validation_fold_plan;
    use crate::statistics::validation::walk_forward::build_validation_incremental;
    use crate::statistics::validation::walk_forward::sort_validation_points;
    use std::collections::HashMap;

    #[test]
    fn validation_similarity_rows_use_pair_index_cache() {
        let similarity_cache = ValidationSimilarityCache {
            total_samples: 12.0,
            rule_names: vec!["规则A".to_string(), "规则B".to_string()],
            rule_hit_counts: vec![3, 1],
            pair_to_rule_indices: HashMap::from([
                (validation_pair_key("000001.SZ", "20240102"), vec![0, 1]),
                (validation_pair_key("000002.SZ", "20240103"), vec![0]),
            ]),
        };
        let triggered_samples = vec![
            RuleLayerSamplePoint {
                ts_code: "000001.SZ".to_string(),
                trade_date: "20240102".to_string(),
                rule_score: 1.0,
                residual_return: 0.5,
                er_change: f64::INFINITY,
            },
            RuleLayerSamplePoint {
                ts_code: "000002.SZ".to_string(),
                trade_date: "20240103".to_string(),
                rule_score: 1.0,
                residual_return: 0.3,
                er_change: f64::INFINITY,
            },
        ];
        let explain_map = HashMap::from([("规则A".to_string(), "说明A".to_string())]);

        let rows = build_validation_similarity_rows(
            &similarity_cache,
            &triggered_samples,
            None,
            &explain_map,
        );

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].rule_name, "规则A");
        assert_eq!(rows[0].overlap_samples, 2);
        assert_eq!(rows[0].overlap_rate_vs_validation, Some(1.0));
        assert_eq!(rows[0].overlap_rate_vs_existing, Some(2.0 / 3.0));
        assert_eq!(rows[0].overlap_lift, Some(4.0));
        assert_eq!(rows[0].explain.as_deref(), Some("说明A"));

        assert_eq!(rows[1].rule_name, "规则B");
        assert_eq!(rows[1].overlap_samples, 1);
        assert_eq!(rows[1].overlap_rate_vs_validation, Some(0.5));
        assert_eq!(rows[1].overlap_rate_vs_existing, Some(1.0));
        assert_eq!(rows[1].overlap_lift, Some(6.0));
        assert!(rows[1].explain.is_none());

        let mut hits_by_pair = vec![
            (validation_pair_hash("000001.SZ", "20240102"), 0),
            (validation_pair_hash("000001.SZ", "20240102"), 1),
            (validation_pair_hash("000002.SZ", "20240103"), 0),
        ];
        hits_by_pair.sort_unstable();
        let compact_rows = build_compact_rule_similarity_rows(
            &CompactRuleSimilarityCache {
                total_samples: 12.0,
                rule_names: vec!["规则A".to_string(), "规则B".to_string()],
                rule_hit_counts: vec![3, 1],
                hits_by_pair,
            },
            &triggered_samples,
            "",
            &explain_map,
        );
        assert_eq!(compact_rows.len(), rows.len());
        for (compact, expected) in compact_rows.iter().zip(&rows) {
            assert_eq!(compact.rule_name, expected.rule_name);
            assert_eq!(compact.overlap_samples, expected.overlap_samples);
            assert_eq!(
                compact.overlap_rate_vs_validation,
                expected.overlap_rate_vs_validation
            );
            assert_eq!(
                compact.overlap_rate_vs_existing,
                expected.overlap_rate_vs_existing
            );
            assert_eq!(compact.overlap_lift, expected.overlap_lift);
        }
    }

    #[test]
    fn validation_score_pearson_uses_zero_filled_universe() {
        let samples = ["000001.SZ", "000002.SZ", "000003.SZ", "000004.SZ"]
            .into_iter()
            .map(|ts_code| RuleLayerSamplePoint {
                ts_code: ts_code.to_string(),
                trade_date: "20240102".to_string(),
                rule_score: 0.0,
                residual_return: 0.01,
                er_change: f64::INFINITY,
            })
            .collect::<Vec<_>>();
        let universe = build_validation_universe_index(&samples);
        let mut rule_index = ValidationExistingRuleScoreIndex::default();
        rule_index.rule_names.push("核心策略".to_string());
        rule_index.hit_counts.push(2);
        rule_index.score_sums.push(2.0);
        rule_index.score_square_sums.push(2.0);
        for ts_code in ["000003.SZ", "000004.SZ"] {
            rule_index.hits.push(ValidationRuleHit {
                pair_hash: validation_pair_hash(ts_code, "20240102"),
                universe_index: 0,
                rule_index: 0,
                score: 1.0,
            });
        }
        rule_index
            .hits
            .sort_unstable_by_key(|hit| (hit.pair_hash, hit.rule_index));
        let candidate_scores = ["000001.SZ", "000002.SZ"]
            .into_iter()
            .map(|ts_code| (validation_pair_hash(ts_code, "20240102"), 1.0))
            .collect::<HashMap<_, _>>();

        let rows = build_validation_expression_similarity_rows(
            &universe,
            &rule_index,
            &[],
            &candidate_scores,
            &HashMap::new(),
            None,
            &HashMap::new(),
        );

        // 没有共同触发时，只有把未触发样本按 0 计入完整 universe 才能得到有限相关性。
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].overlap_samples, 0);
        assert_eq!(rows[0].jaccard, Some(0.0));
        assert_eq!(rows[0].phi, Some(-1.0));
        assert_eq!(rows[0].score_pearson, Some(-1.0));
    }

    #[test]
    fn validation_high_similarity_and_positive_increment_can_coexist() {
        let samples = ["000001.SZ", "000002.SZ", "000003.SZ", "000004.SZ"]
            .into_iter()
            .map(|ts_code| RuleLayerSamplePoint {
                ts_code: ts_code.to_string(),
                trade_date: "20240102".to_string(),
                rule_score: 0.0,
                residual_return: 0.01,
                er_change: f64::INFINITY,
            })
            .collect::<Vec<_>>();
        let universe = build_validation_universe_index(&samples);
        let mut rule_index = ValidationExistingRuleScoreIndex::default();
        rule_index.rule_names.push("核心策略".to_string());
        rule_index.hit_counts.push(3);
        rule_index.score_sums.push(3.0);
        rule_index.score_square_sums.push(3.0);
        for ts_code in ["000001.SZ", "000002.SZ", "000003.SZ"] {
            rule_index.hits.push(ValidationRuleHit {
                pair_hash: validation_pair_hash(ts_code, "20240102"),
                universe_index: 0,
                rule_index: 0,
                score: 1.0,
            });
        }
        rule_index
            .hits
            .sort_unstable_by_key(|hit| (hit.pair_hash, hit.rule_index));
        let candidate_scores = ["000001.SZ", "000002.SZ", "000003.SZ"]
            .into_iter()
            .map(|ts_code| (validation_pair_hash(ts_code, "20240102"), 1.0))
            .collect::<HashMap<_, _>>();
        let rows = build_validation_expression_similarity_rows(
            &universe,
            &rule_index,
            &[],
            &candidate_scores,
            &HashMap::new(),
            None,
            &HashMap::new(),
        );
        assert_eq!(rows[0].jaccard, Some(1.0));
        assert_eq!(rows[0].score_pearson, Some(1.0));

        let points = validation_fold_test_axis(120);
        let axis = sort_validation_points(&points);
        let calendar = validation_calendar_of(&points);
        let folds = build_validation_fold_plan(calendar.len(), 1, 0);
        let core_daily = axis
            .iter()
            .enumerate()
            .map(|(index, point)| (point.trade_date.clone(), 1.0 + index as f64))
            .collect::<HashMap<_, _>>();
        let candidate_daily = axis
            .iter()
            .enumerate()
            .map(|(index, point)| {
                let value = 1.0 + index as f64 + if index >= 30 { 3.0 } else { 0.0 };
                (point.trade_date.clone(), value)
            })
            .collect::<HashMap<_, _>>();
        let incremental = build_validation_incremental(
            &calendar,
            &folds,
            0,
            0,
            &candidate_daily,
            &[("核心策略".to_string(), &core_daily)],
        );

        // 与已有策略高度重合，样本外窗口仍然保留正增量，两者互不推导。
        assert_eq!(incremental.positive_folds, 1);
        assert_eq!(incremental.folds[0].status, "ok");
        assert!(
            incremental.folds[0]
                .incremental_mean
                .is_some_and(|value| value > 0.0)
        );
    }

    #[test]
    fn validation_low_similarity_can_still_lack_increment() {
        let samples = ["000001.SZ", "000002.SZ", "000003.SZ", "000004.SZ"]
            .into_iter()
            .map(|ts_code| RuleLayerSamplePoint {
                ts_code: ts_code.to_string(),
                trade_date: "20240102".to_string(),
                rule_score: 0.0,
                residual_return: 0.01,
                er_change: f64::INFINITY,
            })
            .collect::<Vec<_>>();
        let universe = build_validation_universe_index(&samples);
        let mut rule_index = ValidationExistingRuleScoreIndex::default();
        rule_index.rule_names.push("核心策略".to_string());
        rule_index.hit_counts.push(2);
        rule_index.score_sums.push(2.0);
        rule_index.score_square_sums.push(2.0);
        for ts_code in ["000003.SZ", "000004.SZ"] {
            rule_index.hits.push(ValidationRuleHit {
                pair_hash: validation_pair_hash(ts_code, "20240102"),
                universe_index: 0,
                rule_index: 0,
                score: 1.0,
            });
        }
        rule_index
            .hits
            .sort_unstable_by_key(|hit| (hit.pair_hash, hit.rule_index));
        let candidate_scores = ["000001.SZ", "000002.SZ"]
            .into_iter()
            .map(|ts_code| (validation_pair_hash(ts_code, "20240102"), 1.0))
            .collect::<HashMap<_, _>>();
        let rows = build_validation_expression_similarity_rows(
            &universe,
            &rule_index,
            &[],
            &candidate_scores,
            &HashMap::new(),
            None,
            &HashMap::new(),
        );
        assert_eq!(rows[0].jaccard, Some(0.0));

        let points = validation_fold_test_axis(120);
        let axis = sort_validation_points(&points);
        let calendar = validation_calendar_of(&points);
        let folds = build_validation_fold_plan(calendar.len(), 1, 0);
        let core_daily = axis
            .iter()
            .enumerate()
            .map(|(index, point)| (point.trade_date.clone(), 1.0 + index as f64))
            .collect::<HashMap<_, _>>();
        let candidate_daily = axis
            .iter()
            .enumerate()
            .map(|(index, point)| {
                let value = 1.0 + index as f64;
                let candidate = -(value) - if index >= 30 { 3.0 } else { 0.0 };
                (point.trade_date.clone(), candidate)
            })
            .collect::<HashMap<_, _>>();
        let incremental = build_validation_incremental(
            &calendar,
            &folds,
            0,
            0,
            &candidate_daily,
            &[("核心策略".to_string(), &core_daily)],
        );

        // 低相关只说明比较独立，没有观察到稳定新增收益时不能据此判好。
        assert_eq!(incremental.positive_folds, 0);
        assert!(
            incremental.folds[0]
                .incremental_mean
                .is_some_and(|value| value < 0.0)
        );
    }
}
