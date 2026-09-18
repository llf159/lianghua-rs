use crate::statistics::universe::{ValidationSampleRawRow, ValidationSampleStockMeta};
use crate::statistics::validation::similarity::{ValidationSimilarityCache, validation_pair_key};
use crate::statistics::validation::{
    RuleValidationSampleGroups, RuleValidationSampleRow, RuleValidationSampleStats,
    RuleValidationTriggerCountStats, VALIDATION_EPS,
};
#[cfg(test)]
use rand::Rng;
#[cfg(test)]
use rand::SeedableRng;
use rand::random;
#[cfg(test)]
use rand::rngs::StdRng;
#[cfg(test)]
use rayon::prelude::*;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::collections::HashSet;
pub(in crate::statistics) fn compare_option_f64_desc(
    left: Option<f64>,
    right: Option<f64>,
) -> std::cmp::Ordering {
    match (left, right) {
        (Some(l), Some(r)) => r.partial_cmp(&l).unwrap_or(std::cmp::Ordering::Equal),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => std::cmp::Ordering::Equal,
    }
}

pub(in crate::statistics) struct ValidationSampleAccumulator<'a> {
    pub(in crate::statistics) sample_limit_per_group: usize,
    pub(in crate::statistics) stock_meta_map: &'a HashMap<String, ValidationSampleStockMeta>,
    pub(in crate::statistics) similarity_cache: Option<&'a ValidationSimilarityCache>,
    pub(in crate::statistics) is_each: bool,
    pub(in crate::statistics) trigger_unit_points: f64,
    pub(in crate::statistics) has_dist_points: bool,
    pub(in crate::statistics) total_triggers: usize,
    pub(in crate::statistics) triggered_days: HashSet<String>,
    pub(in crate::statistics) sample_by_stock: HashMap<String, ValidationSampleRawRow>,
    pub(in crate::statistics) overlap_hit_count: HashMap<usize, usize>,
}

impl<'a> ValidationSampleAccumulator<'a> {
    pub(in crate::statistics) fn new(
        sample_limit_per_group: usize,
        stock_meta_map: &'a HashMap<String, ValidationSampleStockMeta>,
        similarity_cache: Option<&'a ValidationSimilarityCache>,
        is_each: bool,
        trigger_unit_points: f64,
        has_dist_points: bool,
    ) -> Self {
        Self {
            sample_limit_per_group,
            stock_meta_map,
            similarity_cache,
            is_each,
            trigger_unit_points,
            has_dist_points,
            total_triggers: 0,
            triggered_days: HashSet::new(),
            sample_by_stock: HashMap::new(),
            overlap_hit_count: HashMap::new(),
        }
    }

    pub(in crate::statistics) fn push(
        &mut self,
        sample: crate::simulate::rule::RuleLayerSamplePointRef<'_>,
    ) {
        self.total_triggers += 1;
        self.triggered_days.insert(sample.trade_date.to_string());
        self.update_similarity_overlap(sample.ts_code, sample.trade_date);

        let row = ValidationSampleRawRow {
            ts_code: sample.ts_code.to_string(),
            trade_date: sample.trade_date.to_string(),
            trigger_count: resolve_validation_trigger_count(
                sample.rule_score,
                self.is_each,
                self.trigger_unit_points,
                self.has_dist_points,
            ),
            rule_score: sample.rule_score,
            residual_return: sample.residual_return,
        };

        let sample_key = format!("{}__{}", row.trigger_count, row.ts_code);
        self.sample_by_stock
            .entry(sample_key)
            .and_modify(|current| {
                if (|current: &ValidationSampleRawRow,
                     candidate: &ValidationSampleRawRow|
                 -> bool {
                    let strength_order = candidate
                        .residual_return
                        .abs()
                        .partial_cmp(&current.residual_return.abs())
                        .unwrap_or(Ordering::Equal);
                    if strength_order != Ordering::Equal {
                        return strength_order == Ordering::Greater;
                    }

                    let date_order = candidate.trade_date.cmp(&current.trade_date);
                    if date_order != Ordering::Equal {
                        return date_order == Ordering::Greater;
                    }

                    candidate
                        .rule_score
                        .abs()
                        .partial_cmp(&current.rule_score.abs())
                        .unwrap_or(Ordering::Equal)
                        == Ordering::Greater
                })(current, &row)
                {
                    *current = row.clone();
                }
            })
            .or_insert(row);
    }

    fn update_similarity_overlap(&mut self, ts_code: &str, trade_date: &str) {
        let Some(similarity_cache) = self.similarity_cache else {
            return;
        };
        let Some(rule_indices) = similarity_cache
            .pair_to_rule_indices
            .get(&validation_pair_key(ts_code, trade_date))
        else {
            return;
        };

        for rule_index in rule_indices {
            *self.overlap_hit_count.entry(*rule_index).or_default() += 1;
        }
    }

    pub(in crate::statistics) fn into_parts(
        self,
    ) -> (
        usize,
        usize,
        RuleValidationSampleStats,
        Vec<RuleValidationTriggerCountStats>,
        RuleValidationSampleGroups,
        HashMap<usize, usize>,
    ) {
        let unique_samples = self.sample_by_stock.into_values().collect::<Vec<_>>();
        let unique_sample_count = unique_samples.len();
        let positive_count = unique_samples
            .iter()
            .filter(|row| row.residual_return > 0.0)
            .count();
        let negative_count = unique_samples
            .iter()
            .filter(|row| row.residual_return < 0.0)
            .count();
        let mut trigger_count_stats_map = HashMap::<usize, RuleValidationTriggerCountStats>::new();
        for row in &unique_samples {
            let stats = trigger_count_stats_map
                .entry(row.trigger_count)
                .or_insert_with(|| RuleValidationTriggerCountStats {
                    trigger_count: row.trigger_count,
                    positive_count: 0,
                    negative_count: 0,
                    random_count: 0,
                    total_samples: 0,
                });
            stats.total_samples += 1;
            stats.random_count += 1;
            if row.residual_return > 0.0 {
                stats.positive_count += 1;
            } else if row.residual_return < 0.0 {
                stats.negative_count += 1;
            }
        }
        let mut trigger_count_stats = trigger_count_stats_map.into_values().collect::<Vec<_>>();
        trigger_count_stats.sort_by_key(|item| item.trigger_count);

        let mut positive_by_board: HashMap<(usize, String), Vec<ValidationSampleRawRow>> =
            HashMap::new();
        let mut negative_by_board: HashMap<(usize, String), Vec<ValidationSampleRawRow>> =
            HashMap::new();
        let mut random_by_board: HashMap<(usize, String), Vec<(u64, ValidationSampleRawRow)>> =
            HashMap::new();

        for row in unique_samples {
            let board = sample_board(&row.ts_code, self.stock_meta_map);
            let bucket_key = (row.trigger_count, board);
            if row.residual_return > 0.0 {
                push_limited_sample(
                    positive_by_board.entry(bucket_key.clone()).or_default(),
                    row.clone(),
                    self.sample_limit_per_group,
                    compare_positive_validation_sample,
                );
            } else if row.residual_return < 0.0 {
                push_limited_sample(
                    negative_by_board.entry(bucket_key.clone()).or_default(),
                    row.clone(),
                    self.sample_limit_per_group,
                    compare_negative_validation_sample,
                );
            }

            push_limited_random_sample(
                random_by_board.entry(bucket_key).or_default(),
                random::<u64>(),
                row,
                self.sample_limit_per_group,
            );
        }

        let mut positive = positive_by_board
            .into_values()
            .flatten()
            .collect::<Vec<_>>();
        let mut negative = negative_by_board
            .into_values()
            .flatten()
            .collect::<Vec<_>>();
        let mut random = random_by_board.into_values().flatten().collect::<Vec<_>>();

        positive.sort_by(compare_positive_validation_sample);
        negative.sort_by(compare_negative_validation_sample);
        random.sort_by(|left, right| {
            left.0
                .cmp(&right.0)
                .then_with(|| compare_random_validation_sample(&left.1, &right.1))
        });

        let groups = RuleValidationSampleGroups {
            positive: validation_sample_rows_to_payload(positive, self.stock_meta_map),
            negative: validation_sample_rows_to_payload(negative, self.stock_meta_map),
            random: validation_sample_rows_to_payload(
                random.into_iter().map(|(_, row)| row),
                self.stock_meta_map,
            ),
        };
        let stats = RuleValidationSampleStats {
            positive_count,
            negative_count,
            random_count: unique_sample_count,
            total_samples: unique_sample_count,
        };

        (
            self.total_triggers,
            self.triggered_days.len(),
            stats,
            trigger_count_stats,
            groups,
            self.overlap_hit_count,
        )
    }
}

pub(in crate::statistics) fn resolve_validation_trigger_count(
    rule_score: f64,
    is_each: bool,
    trigger_unit_points: f64,
    has_dist_points: bool,
) -> usize {
    if !is_each || has_dist_points || trigger_unit_points.abs() <= VALIDATION_EPS {
        return 1;
    }

    let count = (rule_score / trigger_unit_points).abs().round();
    if count.is_finite() && count >= 1.0 {
        count as usize
    } else {
        1
    }
}

pub(in crate::statistics) fn sample_board(
    ts_code: &str,
    stock_meta_map: &HashMap<String, ValidationSampleStockMeta>,
) -> String {
    stock_meta_map
        .get(ts_code)
        .map(|meta| meta.board.clone())
        .unwrap_or_else(|| "其他".to_string())
}

pub(in crate::statistics) fn compare_positive_validation_sample(
    left: &ValidationSampleRawRow,
    right: &ValidationSampleRawRow,
) -> Ordering {
    right
        .residual_return
        .partial_cmp(&left.residual_return)
        .unwrap_or(Ordering::Equal)
        .then_with(|| left.trade_date.cmp(&right.trade_date))
        .then_with(|| left.ts_code.cmp(&right.ts_code))
}

pub(in crate::statistics) fn compare_negative_validation_sample(
    left: &ValidationSampleRawRow,
    right: &ValidationSampleRawRow,
) -> Ordering {
    left.residual_return
        .partial_cmp(&right.residual_return)
        .unwrap_or(Ordering::Equal)
        .then_with(|| left.trade_date.cmp(&right.trade_date))
        .then_with(|| left.ts_code.cmp(&right.ts_code))
}

pub(in crate::statistics) fn compare_random_validation_sample(
    left: &ValidationSampleRawRow,
    right: &ValidationSampleRawRow,
) -> Ordering {
    left.trade_date
        .cmp(&right.trade_date)
        .then_with(|| left.ts_code.cmp(&right.ts_code))
}

pub(in crate::statistics) fn push_limited_sample(
    rows: &mut Vec<ValidationSampleRawRow>,
    row: ValidationSampleRawRow,
    limit: usize,
    compare: fn(&ValidationSampleRawRow, &ValidationSampleRawRow) -> Ordering,
) {
    if limit == 0 {
        return;
    }

    rows.push(row);
    rows.sort_by(compare);
    rows.truncate(limit);
}

pub(in crate::statistics) fn push_limited_random_sample(
    rows: &mut Vec<(u64, ValidationSampleRawRow)>,
    key: u64,
    row: ValidationSampleRawRow,
    limit: usize,
) {
    if limit == 0 {
        return;
    }

    if rows.len() < limit {
        rows.push((key, row));
        return;
    }

    let Some((worst_index, _)) = rows.iter().enumerate().max_by(|(_, left), (_, right)| {
        left.0
            .cmp(&right.0)
            .then_with(|| compare_random_validation_sample(&left.1, &right.1))
    }) else {
        return;
    };

    if key < rows[worst_index].0 {
        rows[worst_index] = (key, row);
    }
}

pub(in crate::statistics) fn validation_sample_rows_to_payload(
    rows: impl IntoIterator<Item = ValidationSampleRawRow>,
    stock_meta_map: &HashMap<String, ValidationSampleStockMeta>,
) -> Vec<RuleValidationSampleRow> {
    rows.into_iter()
        .map(|row| RuleValidationSampleRow {
            name: stock_meta_map
                .get(&row.ts_code)
                .and_then(|meta| meta.name.clone()),
            board: stock_meta_map
                .get(&row.ts_code)
                .map(|meta| meta.board.clone())
                .unwrap_or_else(|| "其他".to_string()),
            volatility_group: stock_meta_map
                .get(&row.ts_code)
                .map(|meta| meta.volatility_group.clone())
                .unwrap_or_else(|| "其他波动".to_string()),
            ts_code: row.ts_code,
            trade_date: row.trade_date,
            trigger_count: row.trigger_count,
            rule_score: row.rule_score,
            residual_return: row.residual_return,
        })
        .collect()
}

#[cfg(test)]
pub(in crate::statistics) fn build_validation_sample_groups(
    samples: &[ValidationSampleRawRow],
    sample_limit_per_group: usize,
    stock_meta_map: &HashMap<String, ValidationSampleStockMeta>,
) -> (RuleValidationSampleStats, RuleValidationSampleGroups) {
    let mut positive = Vec::new();
    let mut negative = Vec::new();

    for row in samples {
        if row.residual_return > 0.0 {
            positive.push(row.clone());
        } else if row.residual_return < 0.0 {
            negative.push(row.clone());
        }
    }

    positive.par_sort_by(|left, right| {
        right
            .residual_return
            .partial_cmp(&left.residual_return)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.trade_date.cmp(&right.trade_date))
            .then_with(|| left.ts_code.cmp(&right.ts_code))
    });
    negative.par_sort_by(|left, right| {
        left.residual_return
            .partial_cmp(&right.residual_return)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.trade_date.cmp(&right.trade_date))
            .then_with(|| left.ts_code.cmp(&right.ts_code))
    });

    let mut rng = StdRng::seed_from_u64(0x9E37_79B9_7F4A_7C15);
    let mut random_pool = samples.to_vec();
    random_pool.par_sort_by(|left, right| {
        left.trade_date
            .cmp(&right.trade_date)
            .then_with(|| left.ts_code.cmp(&right.ts_code))
    });
    if random_pool.len() > 1 {
        for index in (1..random_pool.len()).rev() {
            let swap_index = rng.random_range(0..=index);
            random_pool.swap(index, swap_index);
        }
    }

    let limit_rows_per_board = |rows: Vec<ValidationSampleRawRow>| {
        let mut board_counts = HashMap::<String, usize>::new();
        let mut limited = Vec::new();

        for row in rows {
            let board = stock_meta_map
                .get(&row.ts_code)
                .map(|meta| meta.board.clone())
                .unwrap_or_else(|| "其他".to_string());
            let count = board_counts.entry(board).or_insert(0);
            if *count >= sample_limit_per_group {
                continue;
            }
            *count += 1;
            limited.push(row);
        }

        limited
    };

    let to_payload = |rows: Vec<ValidationSampleRawRow>| {
        limit_rows_per_board(rows)
            .into_iter()
            .map(|row| RuleValidationSampleRow {
                name: stock_meta_map
                    .get(&row.ts_code)
                    .and_then(|meta| meta.name.clone()),
                board: stock_meta_map
                    .get(&row.ts_code)
                    .map(|meta| meta.board.clone())
                    .unwrap_or_else(|| "其他".to_string()),
                volatility_group: stock_meta_map
                    .get(&row.ts_code)
                    .map(|meta| meta.volatility_group.clone())
                    .unwrap_or_else(|| "其他波动".to_string()),
                ts_code: row.ts_code,
                trade_date: row.trade_date,
                trigger_count: row.trigger_count,
                rule_score: row.rule_score,
                residual_return: row.residual_return,
            })
            .collect::<Vec<_>>()
    };

    let stats = RuleValidationSampleStats {
        positive_count: positive.len(),
        negative_count: negative.len(),
        random_count: random_pool.len(),
        total_samples: samples.len(),
    };

    let groups = RuleValidationSampleGroups {
        positive: to_payload(positive),
        negative: to_payload(negative),
        random: to_payload(random_pool),
    };

    (stats, groups)
}

#[cfg(test)]
mod tests {
    use crate::statistics::universe::ValidationSampleRawRow;
    use crate::statistics::universe::ValidationSampleStockMeta;
    use crate::statistics::validation::samples::build_validation_sample_groups;
    use crate::statistics::validation::samples::resolve_validation_trigger_count;
    use std::collections::HashMap;

    #[test]
    fn validation_trigger_count_uses_each_score_multiple() {
        assert_eq!(resolve_validation_trigger_count(3.0, true, 1.0, false), 3);
        assert_eq!(resolve_validation_trigger_count(-4.0, true, -1.0, false), 4);
        assert_eq!(resolve_validation_trigger_count(6.0, true, 2.0, false), 3);
        assert_eq!(resolve_validation_trigger_count(3.0, false, 1.0, false), 1);
        assert_eq!(resolve_validation_trigger_count(3.0, true, 1.0, true), 1);
    }

    #[test]
    fn validation_sample_limit_applies_per_board_and_direction() {
        let samples = vec![
            ValidationSampleRawRow {
                ts_code: "BJ0001.BJ".to_string(),
                trade_date: "20240102".to_string(),
                trigger_count: 1,
                rule_score: 1.0,
                residual_return: 9.0,
            },
            ValidationSampleRawRow {
                ts_code: "BJ0001.BJ".to_string(),
                trade_date: "20240103".to_string(),
                trigger_count: 1,
                rule_score: 1.0,
                residual_return: 8.0,
            },
            ValidationSampleRawRow {
                ts_code: "MB0001.SZ".to_string(),
                trade_date: "20240102".to_string(),
                trigger_count: 1,
                rule_score: 1.0,
                residual_return: 7.0,
            },
            ValidationSampleRawRow {
                ts_code: "MB0001.SZ".to_string(),
                trade_date: "20240103".to_string(),
                trigger_count: 1,
                rule_score: 1.0,
                residual_return: 6.0,
            },
            ValidationSampleRawRow {
                ts_code: "BJ0001.BJ".to_string(),
                trade_date: "20240104".to_string(),
                trigger_count: 1,
                rule_score: 1.0,
                residual_return: -7.0,
            },
            ValidationSampleRawRow {
                ts_code: "BJ0001.BJ".to_string(),
                trade_date: "20240105".to_string(),
                trigger_count: 1,
                rule_score: 1.0,
                residual_return: -8.0,
            },
            ValidationSampleRawRow {
                ts_code: "MB0001.SZ".to_string(),
                trade_date: "20240104".to_string(),
                trigger_count: 1,
                rule_score: 1.0,
                residual_return: -5.0,
            },
            ValidationSampleRawRow {
                ts_code: "MB0001.SZ".to_string(),
                trade_date: "20240105".to_string(),
                trigger_count: 1,
                rule_score: 1.0,
                residual_return: -6.0,
            },
        ];
        let stock_meta_map = HashMap::from([
            (
                "BJ0001.BJ".to_string(),
                ValidationSampleStockMeta {
                    name: Some("北交样本".to_string()),
                    board: "北交所".to_string(),
                    volatility_group: "高波动".to_string(),
                },
            ),
            (
                "MB0001.SZ".to_string(),
                ValidationSampleStockMeta {
                    name: Some("主板样本".to_string()),
                    board: "主板".to_string(),
                    volatility_group: "常规波动".to_string(),
                },
            ),
        ]);

        let (stats, groups) = build_validation_sample_groups(&samples, 1, &stock_meta_map);

        assert_eq!(stats.positive_count, 4);
        assert_eq!(stats.negative_count, 4);
        assert_eq!(stats.random_count, 8);
        assert_eq!(stats.total_samples, 8);

        let count_boards = |rows: &[crate::statistics::validation::RuleValidationSampleRow]| {
            rows.iter()
                .fold(HashMap::<String, usize>::new(), |mut acc, row| {
                    *acc.entry(row.board.clone()).or_insert(0) += 1;
                    acc
                })
        };

        let positive_boards = count_boards(&groups.positive);
        let negative_boards = count_boards(&groups.negative);
        let random_boards = count_boards(&groups.random);

        assert_eq!(groups.positive.len(), 2);
        assert_eq!(positive_boards.get("北交所"), Some(&1));
        assert_eq!(positive_boards.get("主板"), Some(&1));

        assert_eq!(groups.negative.len(), 2);
        assert_eq!(negative_boards.get("北交所"), Some(&1));
        assert_eq!(negative_boards.get("主板"), Some(&1));

        assert_eq!(groups.random.len(), 2);
        assert_eq!(random_boards.get("北交所"), Some(&1));
        assert_eq!(random_boards.get("主板"), Some(&1));
    }
}
