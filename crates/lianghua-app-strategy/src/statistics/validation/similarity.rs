use std::hash::{Hash, Hasher};

use crate::data::result_db_path;
use crate::simulate::dimension::{SignalPairMoments, calc_signal_pair_metrics};
use crate::simulate::fp_utils::pearson_corr;
use crate::simulate::rule::RuleLayerSamplePoint;
use crate::statistics::backtest::RULE_BACKTEST_EPS;
use crate::statistics::common::open_result_conn;
use crate::statistics::validation::RuleValidationSimilarityRow;
use crate::statistics::validation::VALIDATION_EPS;
use crate::statistics::validation::samples::compare_option_f64_desc;
use duckdb::params;
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashMap, HashSet};
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
                ..Default::default()
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
                ..Default::default()
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

pub(in crate::statistics) struct ValidationUniverseIndex {
    pub(in crate::statistics) residual_returns: Vec<f64>,
    pub(in crate::statistics) day_indices: Vec<u32>,
    pub(in crate::statistics) trade_dates: Vec<String>,
    pub(in crate::statistics) index_by_pair: HashMap<u64, u32>,
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

#[derive(Debug, Default)]
pub(in crate::statistics) struct ValidationExistingRuleScoreIndex {
    pub(in crate::statistics) rule_names: Vec<String>,
    pub(in crate::statistics) hit_counts: Vec<usize>,
    pub(in crate::statistics) score_sums: Vec<f64>,
    pub(in crate::statistics) score_square_sums: Vec<f64>,
    pub(in crate::statistics) hits: Vec<ValidationRuleHit>,
    pub(in crate::statistics) range_trigger_counts: HashMap<String, usize>,
    pub(in crate::statistics) universe_trigger_counts: HashMap<String, usize>,
    pub(in crate::statistics) scored_pairs: Option<HashSet<u64>>,
}

pub(in crate::statistics) fn load_validation_existing_rule_score_index(
    source_path: &str,
    start_date: &str,
    end_date: &str,
    universe: &ValidationUniverseIndex,
    research: Option<&crate::research_evidence::ValidationResearchOutput>,
) -> Result<ValidationExistingRuleScoreIndex, String> {
    let result_conn = if let Some(research) = research {
        research.open()?
    } else {
        if !result_db_path(source_path).exists() {
            return Ok(ValidationExistingRuleScoreIndex::default());
        }
        open_result_conn(source_path)?
    };
    let table = if research.is_some() {
        "baseline_rule_scores"
    } else {
        "rule_details"
    };
    let score_column = if research.is_some() {
        "score"
    } else {
        "rule_score"
    };
    let mut stmt = result_conn.prepare(&format!("SELECT rule_name,ts_code,trade_date,TRY_CAST({score_column} AS DOUBLE) FROM {table} WHERE trade_date>=? AND trade_date<=? AND TRY_CAST({score_column} AS DOUBLE) IS NOT NULL"))
        .map_err(|error| format!("读取基线策略分项失败: {error}"))?;
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
        *index
            .range_trigger_counts
            .entry(rule_name.clone())
            .or_default() += 1;
        let pair_hash = validation_pair_hash(&ts_code, &trade_date);
        let Some(&universe_index) = universe.index_by_pair.get(&pair_hash) else {
            continue;
        };
        *index
            .universe_trigger_counts
            .entry(rule_name.clone())
            .or_default() += 1;
        if score.abs() <= VALIDATION_EPS {
            continue;
        }
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
    if research.is_some() {
        let mut stmt = result_conn.prepare("SELECT ts_code,trade_date FROM baseline_scores WHERE score IS NOT NULL AND trade_date BETWEEN ? AND ?").map_err(|e| e.to_string())?;
        index.scored_pairs = Some(
            stmt.query_map(params![start_date, end_date], |row| {
                let code: String = row.get(0)?;
                let day: String = row.get(1)?;
                Ok(validation_pair_hash(&code, &day))
            })
            .map_err(|e| e.to_string())?
            .collect::<Result<HashSet<_>, _>>()
            .map_err(|e| e.to_string())?,
        );
    }
    Ok(index)
}

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

pub(in crate::statistics) fn build_validation_expression_similarity_rows(
    universe: &ValidationUniverseIndex,
    index: &ValidationExistingRuleScoreIndex,
    rule_daily_returns: &[HashMap<String, f64>],
    candidate_scores: &HashMap<u64, f64>,
    candidate_daily_returns: &HashMap<String, f64>,
    exclude_rule_name: Option<&str>,
    explain_map: &HashMap<String, String>,
    candidate_available: Option<&HashSet<u64>>,
) -> Vec<RuleValidationSimilarityRow> {
    if index.rule_names.is_empty() {
        return Vec::new();
    }
    #[derive(Default)]
    struct Period {
        universe: usize,
        candidate_available: usize,
        baseline_available: usize,
        common: usize,
        left_count: usize,
        left_sum: f64,
        left_square_sum: f64,
        // right count, right sum, right squares, joint count, cross sum per native rule.
        rules: Vec<(usize, f64, f64, usize, f64)>,
    }
    let quarters = universe
        .trade_dates
        .iter()
        .map(|day| {
            let month = day.get(4..6)?.parse::<usize>().ok()?;
            (1..=12)
                .contains(&month)
                .then(|| format!("{} Q{}", &day[..4], (month - 1) / 3 + 1))
        })
        .collect::<Vec<_>>();
    let mut periods = BTreeMap::<&str, Period>::new();
    let mut common_pairs = HashSet::new();
    for (pair, &sample_index) in &universe.index_by_pair {
        let candidate_known = candidate_available.is_none_or(|known| known.contains(pair));
        let baseline_known = index
            .scored_pairs
            .as_ref()
            .is_none_or(|known| known.contains(pair));
        let common = candidate_known && baseline_known;
        if common {
            common_pairs.insert(*pair);
        }
        let left = candidate_scores.get(pair).copied().unwrap_or(0.);
        let day = universe.day_indices[sample_index as usize] as usize;
        for period in std::iter::once("全部").chain(quarters[day].as_deref()) {
            let aggregate = periods.entry(period).or_insert_with(|| Period {
                rules: vec![(0, 0., 0., 0, 0.); index.rule_names.len()],
                ..Default::default()
            });
            aggregate.universe += 1;
            aggregate.candidate_available += usize::from(candidate_known);
            aggregate.baseline_available += usize::from(baseline_known);
            if common {
                aggregate.common += 1;
                aggregate.left_count += usize::from(left.abs() > VALIDATION_EPS);
                aggregate.left_sum += left;
                aggregate.left_square_sum += left * left;
            }
        }
    }
    for hit in &index.hits {
        if !common_pairs.contains(&hit.pair_hash) {
            continue;
        }
        let left = candidate_scores.get(&hit.pair_hash).copied().unwrap_or(0.);
        let day = universe.day_indices[hit.universe_index as usize] as usize;
        for period in std::iter::once("全部").chain(quarters[day].as_deref()) {
            let Some(aggregate) = periods.get_mut(period) else {
                continue;
            };
            let right = &mut aggregate.rules[hit.rule_index as usize];
            right.0 += 1;
            right.1 += hit.score;
            right.2 += hit.score * hit.score;
            right.3 += usize::from(left.abs() > VALIDATION_EPS);
            right.4 += left * hit.score;
        }
    }
    let excluded_rule_name = exclude_rule_name
        .map(str::trim)
        .filter(|name| !name.is_empty());
    let quarter_by_date = universe
        .trade_dates
        .iter()
        .zip(&quarters)
        .filter_map(|(day, quarter)| Some((day.as_str(), quarter.as_deref()?)))
        .collect::<HashMap<_, _>>();
    let mut dates = candidate_daily_returns.keys().collect::<Vec<_>>();
    dates.sort_unstable();
    let mut rows = Vec::new();
    for (period, aggregate) in periods {
        for (rule_index, rule_name) in index.rule_names.iter().enumerate() {
            if excluded_rule_name.is_some_and(|name| name == rule_name) {
                continue;
            }
            let (right_count, right_sum, right_square_sum, joint_count, cross_sum) =
                aggregate.rules[rule_index];
            let metrics = calc_signal_pair_metrics(SignalPairMoments {
                universe_count: aggregate.common,
                left_trigger_count: aggregate.left_count,
                right_trigger_count: right_count,
                joint_trigger_count: joint_count,
                left_sum: aggregate.left_sum,
                right_sum,
                left_square_sum: aggregate.left_square_sum,
                right_square_sum,
                cross_sum,
            });
            let mut left_returns = Vec::new();
            let mut right_returns = Vec::new();
            if let Some(rule_daily) = rule_daily_returns.get(rule_index) {
                for day in &dates {
                    if period != "全部"
                        && quarter_by_date.get(day.as_str()).copied() != Some(period)
                    {
                        continue;
                    }
                    if let Some(right) = rule_daily.get(*day) {
                        left_returns.push(candidate_daily_returns[*day]);
                        right_returns.push(*right);
                    }
                }
            }
            rows.push(RuleValidationSimilarityRow {
                period: period.to_string(),
                universe_samples: Some(aggregate.universe),
                candidate_available_samples: Some(aggregate.candidate_available),
                baseline_available_samples: Some(aggregate.baseline_available),
                shared_score_samples: Some(aggregate.common),
                rule_name: rule_name.clone(),
                explain: explain_map.get(rule_name).cloned(),
                overlap_samples: joint_count,
                overlap_rate_vs_validation: (aggregate.left_count > 0)
                    .then(|| joint_count as f64 / aggregate.left_count as f64),
                overlap_rate_vs_existing: (right_count > 0)
                    .then(|| joint_count as f64 / right_count as f64),
                overlap_lift: (aggregate.left_count > 0 && right_count > 0).then(|| {
                    joint_count as f64 * aggregate.common as f64
                        / (aggregate.left_count as f64 * right_count as f64)
                }),
                jaccard: metrics.jaccard,
                phi: metrics.phi,
                score_pearson: metrics.score_pearson,
                return_pearson: pearson_corr(&left_returns, &right_returns),
                shared_return_days: left_returns.len(),
            });
        }
    }
    rows.sort_by(|a, b| {
        b.period
            .eq("全部")
            .cmp(&a.period.eq("全部"))
            .then_with(|| a.period.cmp(&b.period))
            .then_with(|| compare_option_f64_desc(a.jaccard, b.jaccard))
            .then_with(|| b.overlap_samples.cmp(&a.overlap_samples))
            .then_with(|| a.rule_name.cmp(&b.rule_name))
    });
    let selected = rows
        .iter()
        .filter(|row| row.period == "全部")
        .take(20)
        .map(|row| row.rule_name.clone())
        .collect::<HashSet<_>>();
    rows.retain(|row| selected.contains(&row.rule_name));
    rows
}

#[cfg(test)]
mod tests {
    use crate::data::result_db_path;
    use crate::simulate::rule::RuleLayerSamplePoint;
    use crate::statistics::test_support::*;
    use crate::statistics::validation::similarity::CompactRuleSimilarityCache;
    use crate::statistics::validation::similarity::ValidationExistingRuleScoreIndex;
    use crate::statistics::validation::similarity::ValidationRuleHit;
    use crate::statistics::validation::similarity::ValidationSimilarityCache;
    use crate::statistics::validation::similarity::ValidationUniverseIndex;
    use crate::statistics::validation::similarity::build_compact_rule_similarity_rows;
    use crate::statistics::validation::similarity::build_validation_expression_similarity_rows;
    use crate::statistics::validation::similarity::build_validation_similarity_rows;
    use crate::statistics::validation::similarity::build_validation_universe_index;
    use crate::statistics::validation::similarity::load_validation_existing_rule_score_index;
    use crate::statistics::validation::similarity::validation_pair_hash;
    use crate::statistics::validation::similarity::validation_pair_key;
    use crate::statistics::validation::walk_forward::build_validation_fold_plan;
    use crate::statistics::validation::walk_forward::build_validation_incremental;
    use crate::statistics::validation::walk_forward::sort_validation_points;
    use duckdb::Connection;
    use std::collections::{HashMap, HashSet};
    use std::fs::create_dir_all;

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
    fn existing_rule_score_index_separates_zero_score_from_out_of_universe() {
        let source_dir = temp_source_dir();
        let source_dir_str = source_dir.to_str().expect("utf8 source dir");
        create_dir_all(source_dir_str).expect("create source dir");
        let result_conn = Connection::open(result_db_path(source_dir_str)).expect("open result db");
        result_conn
            .execute_batch(
                r#"
                CREATE TABLE rule_details (
                    ts_code VARCHAR,
                    trade_date VARCHAR,
                    rule_name VARCHAR,
                    rule_score DOUBLE
                );
                INSERT INTO rule_details VALUES
                    ('000001.SZ', '20240103', '零分策略', 0.0),
                    ('000001.SZ', '20240104', '零分策略', 0.0),
                    ('000002.SZ', '20240103', '池外策略', 2.0),
                    ('000001.SZ', '20240103', '有效策略', 1.5);
                "#,
            )
            .expect("prepare rule_details");
        drop(result_conn);

        let universe = ValidationUniverseIndex {
            residual_returns: vec![0.1, 0.2],
            day_indices: vec![0, 1],
            trade_dates: vec!["20240103".to_string(), "20240104".to_string()],
            index_by_pair: HashMap::from([
                (validation_pair_hash("000001.SZ", "20240103"), 0),
                (validation_pair_hash("000001.SZ", "20240104"), 1),
            ]),
        };
        let index = load_validation_existing_rule_score_index(
            source_dir_str,
            "20240102",
            "20240104",
            &universe,
            None,
        )
        .unwrap();

        assert_eq!(index.rule_names, vec!["有效策略".to_string()]);
        assert_eq!(index.universe_trigger_counts.get("零分策略"), Some(&2));
        assert_eq!(index.range_trigger_counts.get("池外策略"), Some(&1));
        assert_eq!(index.universe_trigger_counts.get("池外策略"), None);
        let _ = std::fs::remove_dir_all(&source_dir);
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
            None,
        );

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].period, "全部");
        assert_eq!(rows[1].period, "2024 Q1");
        assert_eq!(rows[0].shared_score_samples, Some(4));
        assert_eq!(rows[0].overlap_samples, 0);
        assert_eq!(rows[0].jaccard, Some(0.0));
        assert_eq!(rows[0].phi, Some(-1.0));
        assert_eq!(rows[0].score_pearson, Some(-1.0));
    }

    #[test]
    fn validation_similarity_retains_quarters_and_masks_unavailable_stock_days() {
        let samples = ["20240102", "20240402"]
            .into_iter()
            .flat_map(|day| {
                ["A", "B", "C", "D"]
                    .into_iter()
                    .map(move |code| RuleLayerSamplePoint {
                        ts_code: code.into(),
                        trade_date: day.into(),
                        rule_score: 0.,
                        residual_return: 1.,
                        er_change: 0.,
                    })
            })
            .collect::<Vec<_>>();
        let universe = build_validation_universe_index(&samples);
        let mut index = ValidationExistingRuleScoreIndex {
            rule_names: vec!["核心".into()],
            scored_pairs: Some(
                universe
                    .index_by_pair
                    .keys()
                    .copied()
                    .filter(|pair| *pair != validation_pair_hash("D", "20240402"))
                    .collect(),
            ),
            ..Default::default()
        };
        let mut candidate_scores = HashMap::new();
        for (sample_index, sample) in samples.iter().enumerate() {
            if sample.ts_code == "A" || sample.ts_code == "B" {
                let pair = validation_pair_hash(&sample.ts_code, &sample.trade_date);
                candidate_scores.insert(pair, 1.);
                index.hits.push(ValidationRuleHit {
                    pair_hash: pair,
                    universe_index: sample_index as u32,
                    rule_index: 0,
                    score: 1.,
                });
            }
        }
        let candidate_available = universe
            .index_by_pair
            .keys()
            .copied()
            .filter(|pair| *pair != validation_pair_hash("B", "20240102"))
            .collect::<HashSet<_>>();
        let returns = HashMap::from([("20240102".to_string(), 1.), ("20240402".to_string(), 2.)]);
        let rows = build_validation_expression_similarity_rows(
            &universe,
            &index,
            &[returns.clone()],
            &candidate_scores,
            &returns,
            None,
            &HashMap::new(),
            Some(&candidate_available),
        );
        assert_eq!(rows.len(), 3);
        let all = &rows[0];
        assert_eq!(all.period, "全部");
        assert_eq!(
            (
                all.universe_samples,
                all.candidate_available_samples,
                all.baseline_available_samples,
                all.shared_score_samples
            ),
            (Some(8), Some(7), Some(7), Some(6))
        );
        assert_eq!(
            (
                all.overlap_samples,
                all.jaccard,
                all.score_pearson,
                all.shared_return_days
            ),
            (3, Some(1.), Some(1.), 2)
        );
        for quarter in rows.iter().filter(|row| row.period != "全部") {
            assert_eq!(quarter.shared_score_samples, Some(3));
            assert_eq!(quarter.score_pearson, Some(1.));
            assert_eq!(quarter.shared_return_days, 1);
            assert_eq!(quarter.return_pearson, None);
        }
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
            None,
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
            None,
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

        assert_eq!(incremental.positive_folds, 0);
        assert!(
            incremental.folds[0]
                .incremental_mean
                .is_some_and(|value| value < 0.0)
        );
    }
}
