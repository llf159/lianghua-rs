use rayon::prelude::*;

use crate::data::DataReader;
use crate::data::RuntimeKeyCollectOptions;
use crate::data::ScopeWay;
use crate::data::collect_assigned_names_from_expr_program;
use crate::data::collect_runtime_keys_from_expr_programs;
use crate::data::expr_program_uses_runtime_key;
use crate::data::result_db_path;
use crate::data::runtime::row_into_rt;
use crate::expr::eval::Runtime;
use crate::expr::eval::Value;
use crate::expr::parser::Stmt;
use crate::expr::parser::Stmts;
use crate::expr::validation::estimate_expression_warmup;
use crate::expr::validation::parse_expression_program;
use crate::expr::validation::validate_expression_functions;
use crate::scoring::CachedRule;
use crate::scoring::evaluate_cached_rule_scores;
use crate::scoring::tools::CyqChenFieldInjector;
use crate::scoring::tools::SimilarityRankFieldInjector;
#[cfg(test)]
use crate::scoring::tools::calc_query_need_rows;
#[cfg(test)]
use crate::scoring::tools::calc_query_start_date;
use crate::scoring::tools::collect_used_cyq_chen_runtime_keys;
use crate::scoring::tools::cyq_chen_runtime_key_names;
use crate::scoring::tools::inject_stock_extra_fields;
#[cfg(test)]
use crate::scoring::tools::load_st_list;
use crate::scoring::tools::load_total_share_map;
use crate::simulate::rule::RuleLayerDailyScoreLayers;
use crate::statistics::backtest::summary::build_rule_decay_validations;
use crate::statistics::backtest::{
    RankLayerBucketSummary, RuleLayerBacktestData, RuleLayerBacktestRunParams,
};
#[cfg(test)]
use crate::statistics::validation::ValidationVariant;
use crate::statistics::validation::{
    PreparedValidationCombo, RuleValidationReturnDistributionBucket, VALIDATION_EPS,
    ValidationTriggeredScoreMap, ValidationTsCodeEvaluation,
};
use duckdb::Connection;
use duckdb::params;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Mutex;
pub(in crate::statistics) fn format_validation_number(value: f64) -> String {
    let rounded = value.round();
    if (value - rounded).abs() < 1e-9 {
        format!("{rounded:.0}")
    } else {
        let mut text = format!("{value:.6}");
        while text.contains('.') && text.ends_with('0') {
            text.pop();
        }
        if text.ends_with('.') {
            text.pop();
        }
        text
    }
}

pub(in crate::statistics) fn estimate_rule_warmup(
    stmts: &Stmts,
    scope_way: ScopeWay,
    scope_windows: usize,
) -> Result<usize, String> {
    let expression_need = estimate_expression_warmup(stmts)?;

    let scope_extra = match scope_way {
        ScopeWay::Last => 0,
        ScopeWay::Any | ScopeWay::Each | ScopeWay::Recent => scope_windows.saturating_sub(1),
        ScopeWay::Consec(threshold) => scope_windows
            .saturating_sub(1)
            .max(threshold.saturating_sub(1)),
    };

    Ok(expression_need + scope_extra)
}

pub(in crate::statistics) fn build_validation_cached_rule(
    rule_name: String,
    scope_way: ScopeWay,
    scope_windows: usize,
    points: f64,
    dist_points: Option<Vec<crate::data::DistPoint>>,
    tag: crate::data::RuleTag,
    formula: &str,
) -> Result<CachedRule, String> {
    let stmts = parse_expression_program(formula)
        .map_err(|e| format!("表达式解析错误在{}:{}", e.idx, e.msg))?;
    validate_expression_functions(&stmts)?;
    let assigned_names = collect_assigned_names_from_expr_program(&stmts);

    Ok(CachedRule {
        name: rule_name,
        scope_windows,
        scope_way,
        points,
        dist_points,
        max_points: None,
        tag,
        when_src: formula.to_string(),
        when_ast: stmts,
        assigned_names,
        combination: None,
    })
}

pub(in crate::statistics) fn collect_validation_assigned_names(stmts: &Stmts) -> Vec<String> {
    let mut assigned = HashSet::new();
    for stmt in &stmts.item {
        if let Stmt::Assign { name, .. } = stmt {
            assigned.insert(name.clone());
        }
    }

    let mut out = assigned.into_iter().collect::<Vec<_>>();
    out.sort();
    out
}

pub(in crate::statistics) fn collect_rule_validation_runtime_keys(
    combos: &[PreparedValidationCombo],
) -> HashSet<String> {
    let programs = combos
        .iter()
        .map(|combo| &combo.cached_rule.when_ast)
        .collect::<Vec<_>>();
    let cyq_chen_keys = cyq_chen_runtime_key_names();
    let injected_keys = (["RANK", "SCORE", "S_RANK", "ZHANG", "TOTAL_MV_YI"])
        .iter()
        .copied()
        .chain(cyq_chen_keys)
        .collect::<Vec<_>>();

    collect_runtime_keys_from_expr_programs(
        &programs,
        RuntimeKeyCollectOptions {
            always_keys: &[],
            injected_keys: &injected_keys,
            aliases: &([]),
        },
    )
}

pub(in crate::statistics) fn snapshot_runtime_values(
    runtime: &Runtime,
    names: &[String],
) -> Vec<(String, Value)> {
    names
        .iter()
        .filter_map(|name| {
            runtime
                .vars
                .get(name)
                .cloned()
                .map(|value| (name.clone(), value))
        })
        .collect()
}

pub(in crate::statistics) fn restore_runtime_values(
    runtime: &mut Runtime,
    values: &[(String, Value)],
) {
    for (name, value) in values {
        runtime.vars.insert(name.clone(), value.clone());
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub(in crate::statistics) struct ValidationRankScoreInfo {
    pub(in crate::statistics) rank: Option<f64>,
    pub(in crate::statistics) score: Option<f64>,
}

pub(in crate::statistics) fn build_validation_triggered_scores_for_combos(
    source_path: &str,
    stock_adj_type: &str,
    query_start_date: &str,
    start_date: &str,
    end_date: &str,
    need_rows: usize,
    ts_codes: &[String],
    st_list: &HashSet<String>,
    combos: &[PreparedValidationCombo],
) -> Result<Vec<ValidationTriggeredScoreMap>, String> {
    if combos.is_empty() {
        return Ok(Vec::new());
    }

    let required_runtime_keys = collect_rule_validation_runtime_keys(combos);
    let used_cyq_chen_keys = (|combos: &[PreparedValidationCombo]| -> HashSet<String> {
        let programs = combos
            .iter()
            .map(|combo| &combo.cached_rule.when_ast)
            .collect::<Vec<_>>();
        collect_used_cyq_chen_runtime_keys(&programs)
    })(combos);
    let total_share_map = load_total_share_map(source_path).unwrap_or_default();
    let needs_rank_score = (|combos: &[PreparedValidationCombo]| -> bool {
        combos.iter().any(|combo| {
            expr_program_uses_runtime_key(&combo.cached_rule.when_ast, "RANK")
                || expr_program_uses_runtime_key(&combo.cached_rule.when_ast, "SCORE")
        })
    })(combos);
    let needs_similarity_rank = combos
        .iter()
        .any(|combo| expr_program_uses_runtime_key(&combo.cached_rule.when_ast, "S_RANK"));
    let rank_score_series_map = if needs_rank_score {
        (|source_path: &str,
          start_date: &str,
          end_date: &str|
         -> HashMap<String, HashMap<String, ValidationRankScoreInfo>> {
            let result_db = result_db_path(source_path);
            if !result_db.exists() {
                return HashMap::new();
            }

            let Some(result_db_str) = result_db.to_str() else {
                return HashMap::new();
            };
            let Ok(conn) = Connection::open(result_db_str) else {
                return HashMap::new();
            };
            let Ok(mut stmt) = conn.prepare(
                r#"
        SELECT ts_code, trade_date, rank, total_score
        FROM score_summary
        WHERE trade_date >= ? AND trade_date <= ?
        "#,
            ) else {
                return HashMap::new();
            };
            let Ok(mut rows) = stmt.query(params![start_date, end_date]) else {
                return HashMap::new();
            };

            let mut out: HashMap<String, HashMap<String, ValidationRankScoreInfo>> = HashMap::new();
            while let Ok(Some(row)) = rows.next() {
                let Ok(ts_code) = row.get::<_, String>(0) else {
                    continue;
                };
                let Ok(trade_date) = row.get::<_, String>(1) else {
                    continue;
                };
                let rank = row
                    .get::<_, Option<i64>>(2)
                    .ok()
                    .flatten()
                    .map(|value| value as f64);
                let score = row.get::<_, Option<f64>>(3).ok().flatten();
                out.entry(ts_code)
                    .or_default()
                    .insert(trade_date, ValidationRankScoreInfo { rank, score });
            }

            out
        })(source_path, query_start_date, end_date)
    } else {
        HashMap::new()
    };
    let combo_triggered_maps = Mutex::new(
        std::iter::repeat_with(HashMap::new)
            .take(combos.len())
            .collect::<Vec<ValidationTriggeredScoreMap>>(),
    );
    let results = ts_codes
        .par_iter()
        .map_init(
            || {
                DataReader::new_with_runtime_keys(source_path, &required_runtime_keys).map(
                    |reader| {
                        (
                            reader,
                            CyqChenFieldInjector::new(source_path, &used_cyq_chen_keys),
                            SimilarityRankFieldInjector::new(source_path, needs_similarity_rank),
                        )
                    },
                )
            },
            |worker_res, ts_code| {
                let (reader, cyq_chen_injector, similarity_rank_injector) =
                    worker_res.as_mut().map_err(|err| err.clone())?;
                let ValidationTsCodeEvaluation {
                    ts_code,
                    combo_hits,
                } = (|reader: &mut DataReader,
                      cyq_chen_injector: &CyqChenFieldInjector,
                      similarity_rank_injector: &SimilarityRankFieldInjector,
                      ts_code: &str,
                      stock_adj_type: &str,
                      start_date: &str,
                      end_date: &str,
                      need_rows: usize,
                      st_list: &HashSet<String>,
                      total_share_map: &HashMap<String, f64>,
                      rank_score_series_map: &HashMap<
                    String,
                    HashMap<String, ValidationRankScoreInfo>,
                >,
                      needs_rank_score: bool,
                      needs_similarity_rank: bool,
                      combos: &[PreparedValidationCombo]|
                 -> Result<ValidationTsCodeEvaluation, String> {
                    let mut row_data =
                        reader.load_one_tail_rows(ts_code, stock_adj_type, end_date, need_rows)?;
                    let _ = cyq_chen_injector.inject(&mut row_data, ts_code);
                    if needs_similarity_rank {
                        similarity_rank_injector.inject(&mut row_data, ts_code)?;
                    }
                    inject_stock_extra_fields(
                        &mut row_data,
                        ts_code,
                        st_list.contains(ts_code),
                        total_share_map.get(ts_code).copied(),
                    )?;
                    if needs_rank_score {
                        (|row_data: &mut crate::data::RowData,
                          ts_code: &str,
                          rank_score_series_map: &HashMap<
                            String,
                            HashMap<String, ValidationRankScoreInfo>,
                        >|
                         -> Result<(), String> {
                            let len = row_data.trade_dates.len();
                            let mut rank_series = vec![None; len];
                            let mut score_series = vec![None; len];

                            if let Some(date_to_values) = rank_score_series_map.get(ts_code) {
                                for (index, trade_date) in row_data.trade_dates.iter().enumerate() {
                                    if let Some(values) = date_to_values.get(trade_date).copied() {
                                        rank_series[index] = values.rank;
                                        score_series[index] = values.score;
                                    }
                                }
                            }

                            row_data.cols.insert("RANK".to_string(), rank_series);
                            row_data.cols.insert("SCORE".to_string(), score_series);
                            row_data.validate()
                        })(&mut row_data, ts_code, rank_score_series_map)?;
                    }

                    let trade_dates = row_data.trade_dates.clone();
                    if trade_dates.is_empty() {
                        return Ok(ValidationTsCodeEvaluation {
                            ts_code: ts_code.to_string(),
                            combo_hits: Vec::new(),
                        });
                    }

                    let keep_from = trade_dates
                        .binary_search_by(|date| date.as_str().cmp(start_date))
                        .unwrap_or_else(|index| index);
                    let mut runtime = row_into_rt(row_data)?;
                    let restore_values = combos
                        .iter()
                        .map(|combo| snapshot_runtime_values(&runtime, &combo.assigned_names))
                        .collect::<Vec<_>>();
                    let mut combo_hits = Vec::new();

                    for (combo_index, combo) in combos.iter().enumerate() {
                        if !restore_values[combo_index].is_empty() {
                            restore_runtime_values(&mut runtime, &restore_values[combo_index]);
                        }

                        let (scores, triggered_flags) =
                            evaluate_cached_rule_scores(&combo.cached_rule, &mut runtime)?;
                        let Some(date_score_map) =
                            (|trade_dates: &[String],
                              keep_from: usize,
                              scores: &[f64],
                              triggered_flags: &[bool],
                              rule_points: f64|
                             -> Option<HashMap<String, f64>> {
                                let min_len = usize::min(
                                    trade_dates.len(),
                                    usize::min(scores.len(), triggered_flags.len()),
                                );
                                if keep_from >= min_len {
                                    return None;
                                }

                                let mut date_score_map = HashMap::new();
                                for index in keep_from..min_len {
                                    let Some(score) =
                                        (|score: f64,
                                          triggered: bool,
                                          rule_points: f64|
                                         -> Option<f64> {
                                            if !score.is_finite() {
                                                return None;
                                            }
                                            if score.abs() > VALIDATION_EPS {
                                                return Some(score);
                                            }
                                            if !triggered {
                                                return None;
                                            }

                                            if rule_points.is_finite()
                                                && rule_points.abs() > VALIDATION_EPS
                                            {
                                                return Some(rule_points.signum());
                                            }
                                            Some(1.0)
                                        })(
                                            scores[index], triggered_flags[index], rule_points
                                        )
                                    else {
                                        continue;
                                    };
                                    date_score_map.insert(trade_dates[index].clone(), score);
                                }

                                if date_score_map.is_empty() {
                                    None
                                } else {
                                    Some(date_score_map)
                                }
                            })(
                                &trade_dates,
                                keep_from,
                                &scores,
                                &triggered_flags,
                                combo.cached_rule.points,
                            )
                        else {
                            continue;
                        };
                        combo_hits.push((combo_index, date_score_map));
                    }

                    Ok(ValidationTsCodeEvaluation {
                        ts_code: ts_code.to_string(),
                        combo_hits,
                    })
                })(
                    reader,
                    cyq_chen_injector,
                    similarity_rank_injector,
                    ts_code,
                    stock_adj_type,
                    start_date,
                    end_date,
                    need_rows,
                    st_list,
                    &total_share_map,
                    &rank_score_series_map,
                    needs_rank_score,
                    needs_similarity_rank,
                    combos,
                )?;

                if !combo_hits.is_empty() {
                    let mut maps = combo_triggered_maps
                        .lock()
                        .map_err(|_| "写入验证触发结果失败:锁已损坏".to_string())?;
                    for (combo_index, date_score_map) in combo_hits {
                        maps[combo_index].insert(ts_code.clone(), date_score_map);
                    }
                }

                Ok::<(), String>(())
            },
        )
        .collect::<Vec<_>>();

    for result in results {
        result?;
    }

    combo_triggered_maps
        .into_inner()
        .map_err(|_| "读取验证触发结果失败:锁已损坏".to_string())
}

#[cfg(test)]
pub(in crate::statistics) fn build_validation_triggered_scores(
    source_path: &str,
    stock_adj_type: &str,
    start_date: &str,
    end_date: &str,
    cached_rule: &CachedRule,
) -> Result<HashMap<String, HashMap<String, f64>>, String> {
    let combo = PreparedValidationCombo {
        variant: ValidationVariant {
            combo_key: cached_rule.name.clone(),
            combo_label: cached_rule.name.clone(),
            formula: cached_rule.when_src.clone(),
            unknown_values: Vec::new(),
        },
        cached_rule: cached_rule.clone(),
        assigned_names: collect_validation_assigned_names(&cached_rule.when_ast),
    };
    let required_runtime_keys = collect_rule_validation_runtime_keys(std::slice::from_ref(&combo));
    let reader = DataReader::new_with_runtime_keys(source_path, &required_runtime_keys)?;
    let ts_codes = reader.list_ts_code(stock_adj_type, start_date, end_date)?;
    let st_list = load_st_list(source_path)?;
    let warmup_need = estimate_rule_warmup(
        &cached_rule.when_ast,
        cached_rule.scope_way,
        cached_rule.scope_windows,
    )?;
    let need_rows = calc_query_need_rows(source_path, warmup_need, start_date, end_date)?;
    let mut triggered_maps = build_validation_triggered_scores_for_combos(
        source_path,
        stock_adj_type,
        &calc_query_start_date(source_path, warmup_need, start_date)?,
        start_date,
        end_date,
        need_rows,
        &ts_codes,
        &st_list,
        &[combo],
    )?;
    Ok(triggered_maps.pop().unwrap_or_default())
}

pub(in crate::statistics) struct ValidationScoreLayerAgg {
    pub(in crate::statistics) score: f64,
    pub(in crate::statistics) point_count: usize,
    pub(in crate::statistics) sample_count: usize,
    pub(in crate::statistics) residual_sum: f64,
}

pub(in crate::statistics) struct ValidationScoreLayerDetails {
    pub(in crate::statistics) spread_mean: Option<f64>,
    pub(in crate::statistics) layer_summaries: Vec<RankLayerBucketSummary>,
}

pub(in crate::statistics) fn mean_f64(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    Some(values.iter().sum::<f64>() / values.len() as f64)
}

pub(in crate::statistics) fn sample_std_f64(values: &[f64]) -> Option<f64> {
    if values.len() < 2 {
        return None;
    }
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    let variance = values
        .iter()
        .map(|value| (value - mean).powi(2))
        .sum::<f64>()
        / (values.len() - 1) as f64;
    variance.is_finite().then_some(variance.sqrt())
}

pub(in crate::statistics) fn build_validation_score_layer_details(
    samples: &[crate::simulate::rule::RuleLayerSamplePoint],
    min_samples_per_day: usize,
) -> ValidationScoreLayerDetails {
    let mut grouped_by_day: std::collections::BTreeMap<
        &str,
        Vec<&crate::simulate::rule::RuleLayerSamplePoint>,
    > = std::collections::BTreeMap::new();
    for sample in samples {
        let trade_date = sample.trade_date.trim();
        if trade_date.is_empty()
            || !sample.rule_score.is_finite()
            || !sample.residual_return.is_finite()
        {
            continue;
        }
        grouped_by_day.entry(trade_date).or_default().push(sample);
    }

    let mut spread_values = Vec::new();
    let mut summary_map = HashMap::<u64, ValidationScoreLayerAgg>::new();

    for day_samples in grouped_by_day.into_values() {
        if day_samples.len() < min_samples_per_day {
            continue;
        }

        let mut ordered = day_samples
            .into_iter()
            .map(|sample| {
                let score = if sample.rule_score.abs() < VALIDATION_EPS {
                    0.0
                } else {
                    sample.rule_score
                };
                (score, sample.residual_return)
            })
            .collect::<Vec<_>>();
        ordered.sort_by(|left, right| {
            left.0
                .partial_cmp(&right.0)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        let mut day_layer_returns = Vec::new();
        let mut index = 0usize;
        while index < ordered.len() {
            let score = ordered[index].0;
            let score_bits = score.to_bits();
            let mut residuals = Vec::new();
            while index < ordered.len() && ordered[index].0.to_bits() == score_bits {
                residuals.push(ordered[index].1);
                index += 1;
            }

            let Some(avg_residual_return) = mean_f64(&residuals) else {
                continue;
            };
            day_layer_returns.push(avg_residual_return);
            let agg = summary_map
                .entry(score_bits)
                .or_insert_with(|| ValidationScoreLayerAgg {
                    score,
                    point_count: 0,
                    sample_count: 0,
                    residual_sum: 0.0,
                });
            agg.point_count += 1;
            agg.sample_count += residuals.len();
            agg.residual_sum += avg_residual_return;
        }

        if let (Some(low), Some(high)) = (day_layer_returns.first(), day_layer_returns.last()) {
            if day_layer_returns.len() >= 2 {
                spread_values.push(high - low);
            }
        }
    }

    let mut layer_summaries = summary_map.into_values().collect::<Vec<_>>();
    layer_summaries.sort_by(|left, right| {
        left.score
            .partial_cmp(&right.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    ValidationScoreLayerDetails {
        spread_mean: mean_f64(&spread_values),
        layer_summaries: layer_summaries
            .into_iter()
            .enumerate()
            .map(|(index, item)| RankLayerBucketSummary {
                layer_index: index + 1,
                layer_label: format_validation_score_layer_label(item.score),
                point_count: item.point_count,
                sample_count: item.sample_count,
                avg_score: Some(item.score),
                avg_residual_return: if item.point_count == 0 {
                    None
                } else {
                    Some(item.residual_sum / item.point_count as f64)
                },
                avg_er_change: None,
            })
            .collect(),
    }
}

pub(in crate::statistics) fn build_validation_score_layer_details_from_daily_layers(
    mut daily_layers: Vec<RuleLayerDailyScoreLayers>,
) -> ValidationScoreLayerDetails {
    daily_layers.sort_by(|left, right| left.trade_date.cmp(&right.trade_date));
    let mut spread_values = Vec::new();
    let mut summary_map = HashMap::<u64, ValidationScoreLayerAgg>::new();

    for day in daily_layers {
        if day.groups.is_empty() {
            continue;
        }
        for group in &day.groups {
            let agg = summary_map.entry(group.score.to_bits()).or_insert_with(|| {
                ValidationScoreLayerAgg {
                    score: group.score,
                    point_count: 0,
                    sample_count: 0,
                    residual_sum: 0.0,
                }
            });
            agg.point_count += 1;
            agg.sample_count += group.sample_count;
            agg.residual_sum += group.avg_residual_return;
        }

        if day.groups.len() >= 2 {
            spread_values.push(
                day.groups
                    .last()
                    .expect("non-empty score groups")
                    .avg_residual_return
                    - day.groups[0].avg_residual_return,
            );
        }
    }

    let mut layer_summaries = summary_map.into_values().collect::<Vec<_>>();
    layer_summaries.sort_by(|left, right| {
        left.score
            .partial_cmp(&right.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    ValidationScoreLayerDetails {
        spread_mean: mean_f64(&spread_values),
        layer_summaries: layer_summaries
            .into_iter()
            .enumerate()
            .map(|(index, item)| RankLayerBucketSummary {
                layer_index: index + 1,
                layer_label: format_validation_score_layer_label(item.score),
                point_count: item.point_count,
                sample_count: item.sample_count,
                avg_score: Some(item.score),
                avg_residual_return: if item.point_count == 0 {
                    None
                } else {
                    Some(item.residual_sum / item.point_count as f64)
                },
                avg_er_change: None,
            })
            .collect(),
    }
}

pub(in crate::statistics) fn build_validation_return_distribution(
    samples: &[crate::simulate::rule::RuleLayerSamplePoint],
) -> Vec<RuleValidationReturnDistributionBucket> {
    let mut counts = [0usize; 7];
    for sample in samples {
        if !sample.residual_return.is_finite() {
            continue;
        }

        let bucket_index = if sample.residual_return <= -10.0 {
            0
        } else if sample.residual_return <= -5.0 {
            1
        } else if sample.residual_return <= -2.0 {
            2
        } else if sample.residual_return <= 2.0 {
            3
        } else if sample.residual_return <= 5.0 {
            4
        } else if sample.residual_return <= 10.0 {
            5
        } else {
            6
        };
        counts[bucket_index] += 1;
    }

    build_validation_return_distribution_from_counts(counts)
}

pub(in crate::statistics) fn build_validation_return_distribution_from_counts(
    counts: [usize; 7],
) -> Vec<RuleValidationReturnDistributionBucket> {
    let total = counts.iter().sum::<usize>();
    ([
        "<= -10%", "-10%~-5%", "-5%~-2%", "-2%~2%", "2%~5%", "5%~10%", ">= 10%",
    ])
    .into_iter()
    .enumerate()
    .map(|(index, label)| RuleValidationReturnDistributionBucket {
        bucket_label: label.to_string(),
        sample_count: counts[index],
        sample_ratio: if total > 0 {
            Some(counts[index] as f64 / total as f64)
        } else {
            None
        },
    })
    .collect()
}

pub(in crate::statistics) fn format_validation_score_layer_label(score: f64) -> String {
    if (score.round() - score).abs() < VALIDATION_EPS {
        format!("得分 {}", score.round() as i64)
    } else {
        format!("得分 {:.4}", score)
    }
}

pub(in crate::statistics) fn build_rule_backtest_payload(
    combo_key: &str,
    params: &RuleLayerBacktestRunParams,
    metrics: crate::simulate::rule::RuleLayerMetrics,
    layer_details: Option<ValidationScoreLayerDetails>,
) -> RuleLayerBacktestData {
    let decay_validations = build_rule_decay_validations(&metrics.points, params.backtest_period);
    let (spread_mean, layer_count, layer_method, layer_method_label, layer_summaries) =
        match layer_details {
            Some(layer_details) => {
                let layer_count = layer_details.layer_summaries.len();
                (
                    layer_details.spread_mean,
                    Some(layer_count),
                    Some("score_value".to_string()),
                    Some("按得分值分层".to_string()),
                    layer_details.layer_summaries,
                )
            }
            None => (None, None, None, None, Vec::new()),
        };

    RuleLayerBacktestData {
        rule_name: combo_key.to_string(),
        stock_adj_type: params.stock_adj_type.clone(),
        index_ts_code: params.index_ts_code.clone(),
        index_beta: params.index_beta,
        concept_beta: params.concept_beta,
        industry_beta: params.industry_beta,
        start_date: params.start_date.clone(),
        end_date: params.end_date.clone(),
        resolved_board: params.resolved_board.clone(),
        exclude_st_board: params.exclude_st_board,
        total_mv_min: params.total_mv_min,
        total_mv_max: params.total_mv_max,
        min_samples_per_rule_day: params.min_samples_per_day,
        min_listed_trade_days: params.min_listed_trade_days,
        backtest_period: params.backtest_period,
        points: Vec::new(),
        avg_residual_mean: metrics.avg_residual_mean,
        avg_excess_residual_mean: metrics.avg_excess_residual_mean,
        decay_validations,
        avg_er_change: metrics.avg_er_change,
        profit_loss_ratio: metrics.profit_loss_ratio,
        spread_mean,
        avg_contribution_score: None,
        avg_contribution_per_trigger: None,
        ic_mean: metrics.ic_mean,
        ic_std: metrics.ic_std,
        icir: metrics.icir,
        ic_t_value: metrics.ic_t_value,
        layer_count,
        layer_method,
        layer_method_label,
        layer_summaries,
        is_all_rules: false,
        all_rule_summaries: Vec::new(),
        rule_validation_details: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use crate::data::DataReader;
    use crate::data::RuleTag;
    use crate::data::ScopeWay;
    use crate::data::result_db_path;
    use crate::scoring::tools::load_st_list;
    use crate::simulate::rule::RuleLayerDailyScoreGroup;
    use crate::simulate::rule::RuleLayerDailyScoreLayers;
    use crate::simulate::rule::RuleLayerSamplePoint;
    use crate::statistics::test_support::*;
    use crate::statistics::validation::PreparedValidationCombo;
    use crate::statistics::validation::ValidationVariant;
    use crate::statistics::validation::scores::build_validation_cached_rule;
    use crate::statistics::validation::scores::build_validation_return_distribution;
    use crate::statistics::validation::scores::build_validation_return_distribution_from_counts;
    use crate::statistics::validation::scores::build_validation_score_layer_details;
    use crate::statistics::validation::scores::build_validation_score_layer_details_from_daily_layers;
    use crate::statistics::validation::scores::build_validation_triggered_scores;
    use crate::statistics::validation::scores::build_validation_triggered_scores_for_combos;
    use crate::statistics::validation::scores::collect_rule_validation_runtime_keys;
    use crate::statistics::validation::scores::collect_validation_assigned_names;
    use duckdb::Connection;
    use duckdb::params;

    #[test]
    fn validation_return_distribution_uses_symmetric_percent_buckets() {
        let samples = [-12.0, -10.0, -7.0, -3.0, -2.0, 0.0, 2.0, 3.0, 8.0, 11.0]
            .into_iter()
            .map(|residual_return| RuleLayerSamplePoint {
                ts_code: "000001.SZ".to_string(),
                trade_date: "20240102".to_string(),
                rule_score: 1.0,
                residual_return,
                er_change: f64::INFINITY,
            })
            .collect::<Vec<_>>();

        let buckets = build_validation_return_distribution(&samples);

        assert_eq!(buckets.len(), 7);
        assert_eq!(
            buckets
                .iter()
                .map(|bucket| bucket.sample_count)
                .collect::<Vec<_>>(),
            vec![2, 1, 2, 2, 1, 1, 1]
        );
        assert_eq!(buckets[0].sample_ratio, Some(0.2));

        let compressed = build_validation_return_distribution_from_counts([2, 1, 2, 2, 1, 1, 1]);
        assert_eq!(
            compressed
                .iter()
                .map(|bucket| (bucket.sample_count, bucket.sample_ratio))
                .collect::<Vec<_>>(),
            buckets
                .iter()
                .map(|bucket| (bucket.sample_count, bucket.sample_ratio))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn compressed_validation_score_layers_match_full_samples() {
        let samples = vec![
            RuleLayerSamplePoint {
                ts_code: "000001.SZ".to_string(),
                trade_date: "20240102".to_string(),
                rule_score: 0.0,
                residual_return: 1.0,
                er_change: 0.0,
            },
            RuleLayerSamplePoint {
                ts_code: "000002.SZ".to_string(),
                trade_date: "20240102".to_string(),
                rule_score: 2.0,
                residual_return: 3.0,
                er_change: 0.0,
            },
            RuleLayerSamplePoint {
                ts_code: "000001.SZ".to_string(),
                trade_date: "20240103".to_string(),
                rule_score: 0.0,
                residual_return: 2.0,
                er_change: 0.0,
            },
            RuleLayerSamplePoint {
                ts_code: "000002.SZ".to_string(),
                trade_date: "20240103".to_string(),
                rule_score: 0.0,
                residual_return: 4.0,
                er_change: 0.0,
            },
        ];
        let full = build_validation_score_layer_details(&samples, 1);
        let compressed = build_validation_score_layer_details_from_daily_layers(vec![
            RuleLayerDailyScoreLayers {
                trade_date: "20240103".to_string(),
                groups: vec![RuleLayerDailyScoreGroup {
                    score: 0.0,
                    sample_count: 2,
                    avg_residual_return: 3.0,
                }],
            },
            RuleLayerDailyScoreLayers {
                trade_date: "20240102".to_string(),
                groups: vec![
                    RuleLayerDailyScoreGroup {
                        score: 0.0,
                        sample_count: 1,
                        avg_residual_return: 1.0,
                    },
                    RuleLayerDailyScoreGroup {
                        score: 2.0,
                        sample_count: 1,
                        avg_residual_return: 3.0,
                    },
                ],
            },
        ]);

        assert_eq!(compressed.spread_mean, full.spread_mean);
        assert_eq!(compressed.layer_summaries.len(), full.layer_summaries.len());
        for (compressed, full) in compressed.layer_summaries.iter().zip(&full.layer_summaries) {
            assert_eq!(compressed.layer_index, full.layer_index);
            assert_eq!(compressed.layer_label, full.layer_label);
            assert_eq!(compressed.point_count, full.point_count);
            assert_eq!(compressed.sample_count, full.sample_count);
            assert_eq!(compressed.avg_score, full.avg_score);
            assert_eq!(compressed.avg_residual_return, full.avg_residual_return);
        }
    }

    #[test]
    fn validation_triggered_scores_cover_full_analysis_window() {
        let source_dir = temp_source_dir();
        let source_dir_str = source_dir.to_str().expect("utf8 source dir");
        prepare_validation_source_files(source_dir_str);

        let cached_rule = build_validation_cached_rule(
            "validation_test_rule".to_string(),
            ScopeWay::Any,
            1,
            1.0,
            None,
            RuleTag::Normal,
            "C > 0",
        )
        .expect("build cached rule");

        let triggered_score_map = build_validation_triggered_scores(
            source_dir_str,
            "qfq",
            "20240102",
            "20240104",
            &cached_rule,
        )
        .expect("build triggered scores");

        let date_score_map = triggered_score_map
            .get("000001.SZ")
            .expect("ts_code should have triggered scores");

        assert_eq!(date_score_map.len(), 3);
        assert!(date_score_map.contains_key("20240102"));
        assert!(date_score_map.contains_key("20240103"));
        assert!(date_score_map.contains_key("20240104"));
        assert_eq!(
            triggered_score_map
                .values()
                .map(|item| item.len())
                .sum::<usize>(),
            3
        );
    }

    #[test]
    fn validation_triggered_scores_inject_uppercase_rank() {
        let source_dir = temp_source_dir();
        let source_dir_str = source_dir.to_str().expect("utf8 source dir");
        prepare_validation_source_files(source_dir_str);
        (|source_dir: &str| {
            let result_conn = Connection::open(result_db_path(source_dir)).expect("open result db");
            result_conn
                .execute(
                    r#"
                    CREATE TABLE score_summary (
                        ts_code VARCHAR,
                        trade_date VARCHAR,
                        total_score DOUBLE,
                        rank BIGINT
                    )
                    "#,
                    [],
                )
                .expect("create score_summary");
            result_conn
                .execute(
                    "INSERT INTO score_summary VALUES (?, ?, ?, ?), (?, ?, ?, ?), (?, ?, ?, ?)",
                    params![
                        "000001.SZ",
                        "20240102",
                        80.0_f64,
                        3_i64,
                        "000001.SZ",
                        "20240103",
                        90.0_f64,
                        2_i64,
                        "000001.SZ",
                        "20240104",
                        100.0_f64,
                        1_i64,
                    ],
                )
                .expect("insert rank rows");
        })(source_dir_str);

        let cached_rule = build_validation_cached_rule(
            "validation_rank_rule".to_string(),
            ScopeWay::Any,
            1,
            1.0,
            None,
            RuleTag::Normal,
            "RANK <= 2",
        )
        .expect("build cached rule");

        let triggered_score_map = build_validation_triggered_scores(
            source_dir_str,
            "qfq",
            "20240102",
            "20240104",
            &cached_rule,
        )
        .expect("build triggered scores");

        let date_score_map = triggered_score_map
            .get("000001.SZ")
            .expect("ts_code should have rank-triggered scores");

        assert_eq!(date_score_map.len(), 2);
        assert!(!date_score_map.contains_key("20240102"));
        assert!(date_score_map.contains_key("20240103"));
        assert!(date_score_map.contains_key("20240104"));
    }

    #[test]
    fn validation_batch_scores_restore_overwritten_base_series() {
        let source_dir = temp_source_dir();
        let source_dir_str = source_dir.to_str().expect("utf8 source dir");
        prepare_validation_source_files(source_dir_str);

        let first_rule = build_validation_cached_rule(
            "validation_combo_001".to_string(),
            ScopeWay::Any,
            1,
            1.0,
            None,
            RuleTag::Normal,
            "C := REF(C, 1); C > 0",
        )
        .expect("build first cached rule");
        let second_rule = build_validation_cached_rule(
            "validation_combo_002".to_string(),
            ScopeWay::Any,
            1,
            1.0,
            None,
            RuleTag::Normal,
            "C := REF(C, 2); C > 0",
        )
        .expect("build second cached rule");

        let expected_first = build_validation_triggered_scores(
            source_dir_str,
            "qfq",
            "20240102",
            "20240104",
            &first_rule,
        )
        .expect("build first triggered scores");
        let expected_second = build_validation_triggered_scores(
            source_dir_str,
            "qfq",
            "20240102",
            "20240104",
            &second_rule,
        )
        .expect("build second triggered scores");

        let reader = DataReader::new(source_dir_str).expect("build reader");
        let ts_codes = reader
            .list_ts_code("qfq", "20240102", "20240104")
            .expect("list ts codes");
        let st_list = load_st_list(source_dir_str).expect("load st list");
        let combos = vec![
            PreparedValidationCombo {
                variant: ValidationVariant {
                    combo_key: first_rule.name.clone(),
                    combo_label: first_rule.name.clone(),
                    formula: first_rule.when_src.clone(),
                    unknown_values: Vec::new(),
                },
                cached_rule: first_rule.clone(),
                assigned_names: collect_validation_assigned_names(&first_rule.when_ast),
            },
            PreparedValidationCombo {
                variant: ValidationVariant {
                    combo_key: second_rule.name.clone(),
                    combo_label: second_rule.name.clone(),
                    formula: second_rule.when_src.clone(),
                    unknown_values: Vec::new(),
                },
                cached_rule: second_rule.clone(),
                assigned_names: collect_validation_assigned_names(&second_rule.when_ast),
            },
        ];

        let batch_results = build_validation_triggered_scores_for_combos(
            source_dir_str,
            "qfq",
            "20240102",
            "20240102",
            "20240104",
            3,
            &ts_codes,
            &st_list,
            &combos,
        )
        .expect("build batch triggered scores");

        assert_eq!(batch_results.len(), 2);
        assert_eq!(batch_results[0], expected_first);
        assert_eq!(batch_results[1], expected_second);
    }

    #[test]
    fn rule_validation_runtime_key_collection_skips_injected_fields() {
        let rule = build_validation_cached_rule(
                "validation_runtime_keys".to_string(),
                ScopeWay::Any,
                1,
                1.0,
                None,
                RuleTag::Normal,
                "M := MA(C, 5); M > MY_VALIDATION_IND AND RANK <= 100 AND SCORE > 0 AND ZHANG > 0 AND TOTAL_MV_YI <= 300 AND S_RANK <= 100 AND CYQ_TPR > 0.6",
            )
            .expect("build cached rule");
        let combo = PreparedValidationCombo {
            variant: ValidationVariant {
                combo_key: rule.name.clone(),
                combo_label: rule.name.clone(),
                formula: rule.when_src.clone(),
                unknown_values: Vec::new(),
            },
            cached_rule: rule.clone(),
            assigned_names: collect_validation_assigned_names(&rule.when_ast),
        };

        let keys = collect_rule_validation_runtime_keys(&[combo]);

        for required_key in ["C", "MY_VALIDATION_IND"] {
            assert!(keys.contains(required_key), "missing {required_key}");
        }
        assert!(!keys.contains("TOTAL_MV"));
        for injected_key in ["RANK", "SCORE", "ZHANG", "TOTAL_MV_YI", "S_RANK", "CYQ_TPR"] {
            assert!(!keys.contains(injected_key), "unexpected {injected_key}");
        }
        assert!(!keys.contains("O"));
    }
}
