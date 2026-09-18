//! walk-forward fold 划分与增量回归。

use crate::simulate::dimension::calc_linear_orthogonal_diagnostics;
use crate::simulate::fp_utils::calc_newey_west_t_value;
use crate::simulate::fp_utils::pearson_corr;
use crate::simulate::rule::RuleLayerPoint;
use crate::statistics::backtest::RULE_BACKTEST_EPS;
use crate::statistics::validation::scores::mean_f64;
use crate::statistics::validation::{
    RuleValidationIncrementalData, RuleValidationIncrementalFold, RuleValidationWalkForwardData,
    RuleValidationWalkForwardFold,
};
use std::collections::HashMap;
/// 样本外 fold 划分：`train_end_index + 1 ..= test_start_index - 1` 为 purge 区间。
#[derive(Debug, Clone, Copy)]
pub(in crate::statistics) struct ValidationFoldPlan {
    pub(in crate::statistics) train_end_index: usize,
    pub(in crate::statistics) test_start_index: usize,
    pub(in crate::statistics) test_end_index: usize,
}

pub(in crate::statistics) fn build_validation_fold_plan(
    axis_len: usize,
    fold_count: usize,
    purge_days: usize,
) -> Vec<ValidationFoldPlan> {
    if axis_len == 0 {
        return Vec::new();
    }
    // 固定上限 8：再多的 fold 只会让每个测试窗口短到无法支撑 IC 统计。
    let fold_count = fold_count.clamp(1, 8);
    // 首个训练窗口至少占 1/4 长度且不少于 20 个交易日，保证训练期统计不退化。
    let initial_train = (axis_len / 4).max(20).min(axis_len.saturating_sub(1));
    if initial_train == 0 {
        return Vec::new();
    }
    let block = ((axis_len - initial_train) / fold_count).max(1);
    let mut folds = Vec::new();
    for fold_index in 0..fold_count {
        let test_start_index = initial_train + fold_index * block;
        if test_start_index >= axis_len {
            break;
        }
        let test_end_index = if fold_index + 1 == fold_count {
            axis_len - 1
        } else {
            (test_start_index + block - 1).min(axis_len - 1)
        };
        // purge：训练区间末尾与样本外起点之间隔开 holding_period 个交易日，
        // 保证训练样本的前向收益窗口（含 holding_period 当天）不落进 test 区间；
        // HAC 只修正相关性，不能替代隔离。
        let Some(train_end_index) = test_start_index.checked_sub(1 + purge_days) else {
            continue;
        };
        folds.push(ValidationFoldPlan {
            train_end_index,
            test_start_index,
            test_end_index,
        });
    }
    folds
}

/// 分层统计走 fold+reduce 合并，`points` 顺序不保证有序，按交易日排序后再做 fold 划分。
pub(in crate::statistics) fn sort_validation_points(
    points: &[RuleLayerPoint],
) -> Vec<&RuleLayerPoint> {
    let mut axis = points.iter().collect::<Vec<_>>();
    axis.sort_by(|left, right| left.trade_date.cmp(&right.trade_date));
    axis
}

/// 表达式方向：整体触发分为负时按负向解读，与衰减验证保持同一口径。
pub(in crate::statistics) fn validation_axis_direction_sign(axis: &[&RuleLayerPoint]) -> f64 {
    let score_sum = axis
        .iter()
        .filter_map(|point| point.avg_rule_score.filter(|value| value.is_finite()))
        .sum::<f64>();
    if score_sum < 0.0 { -1.0 } else { 1.0 }
}

pub(in crate::statistics) fn build_validation_walk_forward(
    calendar: &[String],
    folds: &[ValidationFoldPlan],
    backtest_period: usize,
    direction_sign: f64,
    points_by_date: &HashMap<&str, &RuleLayerPoint>,
    day_trigger_counts: &HashMap<String, usize>,
) -> RuleValidationWalkForwardData {
    // purge 取 holding period：train 标签的收益实现日不能落在 test 区间内。
    // HAC lag 仍按 holding period - 1 修正重叠窗口带来的序列相关。
    let purge_days = backtest_period;
    let hac_lag = backtest_period.saturating_sub(1);
    let mut rows = Vec::with_capacity(folds.len());
    for (fold_index, plan) in folds.iter().enumerate() {
        let window = &calendar[plan.test_start_index..=plan.test_end_index];
        let daily = window
            .iter()
            .map(|trade_date| points_by_date.get(trade_date.as_str()).copied())
            .collect::<Vec<_>>();
        let valid_days = daily.iter().filter(|point| point.is_some()).count();
        // 与增量研究共用同一门槛：有效日不足 20 的 fold 只标记 insufficient，
        // 不参与正窗口统计；fold 日期对所有参数组合完全一致，不按组合重新切分。
        let sufficient = valid_days >= 20;
        // IC 由“分数与残差”的秩相关给出，Spread 由高分半区减低分半区给出：
        // 两者都已经通过分数符号携带方向（负向规则工作时期望为正），再乘方向符号会把它们翻反。
        // 只有方向盲的残差均值需要按表达式方向翻转。
        let ic_values = daily
            .iter()
            .filter_map(|point| *point)
            .filter_map(|point| point.ic.filter(|value| value.is_finite()))
            .collect::<Vec<_>>();
        let spread_values = daily
            .iter()
            .filter_map(|point| *point)
            .filter_map(|point| point.top_bottom_spread.filter(|value| value.is_finite()))
            .collect::<Vec<_>>();
        let residual_values = daily
            .iter()
            .filter_map(|point| *point)
            .filter_map(|point| {
                point
                    .avg_excess_residual_return
                    .filter(|value| value.is_finite())
            })
            .map(|value| value * direction_sign)
            .collect::<Vec<_>>();
        let (ic_mean, ic_t_value, avg_residual_return, spread_mean) = if sufficient {
            (
                mean_f64(&ic_values),
                calc_newey_west_t_value(&ic_values, hac_lag),
                mean_f64(&residual_values),
                mean_f64(&spread_values),
            )
        } else {
            (None, None, None, None)
        };
        rows.push(RuleValidationWalkForwardFold {
            fold_index,
            status: if sufficient {
                "ok".to_string()
            } else {
                "insufficient".to_string()
            },
            train_start_date: calendar[0].clone(),
            train_end_date: calendar[plan.train_end_index].clone(),
            test_start_date: calendar[plan.test_start_index].clone(),
            test_end_date: calendar[plan.test_end_index].clone(),
            test_day_count: valid_days,
            test_sample_count: window
                .iter()
                .map(|trade_date| day_trigger_counts.get(trade_date).copied().unwrap_or(0))
                .sum(),
            ic_mean,
            ic_t_value,
            avg_residual_return,
            spread_mean,
        });
    }

    RuleValidationWalkForwardData {
        fold_count: rows.len(),
        purge_days,
        ic_positive_folds: rows
            .iter()
            .filter(|row| row.ic_mean.is_some_and(|value| value > 0.0))
            .count(),
        residual_positive_folds: rows
            .iter()
            .filter(|row| row.avg_residual_return.is_some_and(|value| value > 0.0))
            .count(),
        spread_positive_folds: rows
            .iter()
            .filter(|row| row.spread_mean.is_some_and(|value| value > 0.0))
            .count(),
        folds: rows,
    }
}

/// 增量研究：每个 fold 只用 train 做标准化 ridge 回归，再在 test 上用原始尺度系数算增量。
///
/// 标准化后 `(R + λI) β_z = r_xy`，换算回原始尺度得到 `beta_i = β_z_i * std_y / std_x_i`；
/// 样本外增量定义为 `incremental = y_test - Σ beta_i * x_i_test`（不减 train intercept），
/// 均值即控制核心策略后的样本外增量 alpha。均值、标准差与系数全部只来自 train，
/// 岭项让共线核心策略仍然可解。
pub(in crate::statistics) fn build_validation_incremental(
    calendar: &[String],
    folds: &[ValidationFoldPlan],
    purge_days: usize,
    hac_lag: usize,
    candidate_daily_returns: &HashMap<String, f64>,
    core_series: &[(String, &HashMap<String, f64>)],
) -> RuleValidationIncrementalData {
    let mut incremental = RuleValidationIncrementalData {
        core_rule_names: core_series
            .iter()
            .map(|(rule_name, _)| rule_name.clone())
            .collect(),
        purge_days,
        folds: Vec::with_capacity(folds.len()),
        positive_folds: 0,
    };
    if core_series.is_empty() || calendar.is_empty() {
        return incremental;
    }

    let target = calendar
        .iter()
        .map(|trade_date| candidate_daily_returns.get(trade_date).copied())
        .collect::<Vec<_>>();
    let predictors = core_series
        .iter()
        .map(|(_, series)| {
            calendar
                .iter()
                .map(|trade_date| series.get(trade_date).copied())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    for (fold_index, plan) in folds.iter().enumerate() {
        let collect_rows = |from: usize, to: usize| {
            let mut rows = Vec::<Vec<f64>>::new();
            let mut targets = Vec::<f64>::new();
            for index in from..=to {
                let Some(target_value) = target[index] else {
                    continue;
                };
                let Some(row) = predictors
                    .iter()
                    .map(|column| column[index])
                    .collect::<Option<Vec<f64>>>()
                else {
                    continue;
                };
                rows.push(row);
                targets.push(target_value);
            }
            (rows, targets)
        };
        let (train_rows, train_targets) = collect_rows(0, plan.train_end_index);
        let (test_rows, test_targets) = collect_rows(plan.test_start_index, plan.test_end_index);

        let mut fold = RuleValidationIncrementalFold {
            fold_index,
            status: "insufficient".to_string(),
            train_start_date: calendar[0].clone(),
            train_end_date: calendar[plan.train_end_index].clone(),
            test_start_date: calendar[plan.test_start_index].clone(),
            test_end_date: calendar[plan.test_end_index].clone(),
            train_day_count: train_rows.len(),
            test_day_count: test_rows.len(),
            incremental_mean: None,
            incremental_hac_t: None,
            positive_day_ratio: None,
        };

        let column_count = core_series.len();
        // 训练行至少 max(30, predictors × 5)，样本外至少 20 个共同有效日：
        // 与相关性与正交研究的增量门槛保持一致，样本不足时该 fold 不进入正窗口统计。
        let minimum_train_rows = (column_count * 5).max(30);
        if train_rows.len() >= minimum_train_rows && test_rows.len() >= 20 {
            let train_mean_y = train_targets.iter().sum::<f64>() / train_targets.len() as f64;
            let train_mean_x = (0..column_count)
                .map(|column| {
                    train_rows.iter().map(|row| row[column]).sum::<f64>() / train_rows.len() as f64
                })
                .collect::<Vec<_>>();
            let train_std = |column: usize| -> f64 {
                let mean = train_mean_x[column];
                let variance = train_rows
                    .iter()
                    .map(|row| (row[column] - mean) * (row[column] - mean))
                    .sum::<f64>()
                    / (train_rows.len() - 1) as f64;
                if variance > RULE_BACKTEST_EPS {
                    variance.sqrt()
                } else {
                    0.0
                }
            };
            let train_std_y = {
                let variance = train_targets
                    .iter()
                    .map(|value| (value - train_mean_y) * (value - train_mean_y))
                    .sum::<f64>()
                    / (train_targets.len() - 1) as f64;
                if variance > RULE_BACKTEST_EPS {
                    variance.sqrt()
                } else {
                    0.0
                }
            };

            // 标准化相关矩阵：[predictors..., target]，岭系数只作用在 predictor 对角线上。
            let mut matrix = vec![vec![0.0_f64; column_count + 1]; column_count + 1];
            for row in 0..=column_count {
                let left_values = if row < column_count {
                    train_rows.iter().map(|item| item[row]).collect::<Vec<_>>()
                } else {
                    train_targets.clone()
                };
                let left_std = if row < column_count {
                    train_std(row)
                } else {
                    train_std_y
                };
                matrix[row][row] = (left_std > RULE_BACKTEST_EPS) as u8 as f64;
                for column in (row + 1)..=column_count {
                    let right_values = if column < column_count {
                        train_rows
                            .iter()
                            .map(|item| item[column])
                            .collect::<Vec<_>>()
                    } else {
                        train_targets.clone()
                    };
                    let correlation = pearson_corr(&left_values, &right_values).unwrap_or(0.0);
                    matrix[row][column] = correlation;
                    matrix[column][row] = correlation;
                }
            }

            // λ = 0.1 与相关性与正交研究的增量回归保持一致，只作用于标准化 predictor。
            let fitted = if train_std_y > RULE_BACKTEST_EPS {
                calc_linear_orthogonal_diagnostics(&matrix, 0.1)
                    .ok()
                    .and_then(|mut diagnostics| diagnostics.pop())
                    .filter(|diagnostic| diagnostic.residual_variance_ratio.is_some())
            } else {
                None
            };

            if let Some(diagnostic) = fitted {
                let beta = diagnostic
                    .basis_coefficients
                    .into_iter()
                    .enumerate()
                    .map(|(column, coefficient)| {
                        let std = train_std(column);
                        if std > RULE_BACKTEST_EPS {
                            coefficient * train_std_y / std
                        } else {
                            0.0
                        }
                    })
                    .collect::<Vec<_>>();
                let residuals = test_rows
                    .iter()
                    .zip(&test_targets)
                    .map(|(row, target_value)| {
                        // 样本外只按原始尺度系数扣除核心策略贡献，不减 train intercept，
                        // 因此增量均值就是控制核心策略后的 OOS 增量 alpha。
                        let prediction = beta
                            .iter()
                            .zip(row)
                            .map(|(coefficient, value)| coefficient * value)
                            .sum::<f64>();
                        target_value - prediction
                    })
                    .collect::<Vec<_>>();
                fold.status = "ok".to_string();
                fold.test_day_count = residuals.len();
                fold.incremental_mean = mean_f64(&residuals);
                fold.incremental_hac_t = calc_newey_west_t_value(&residuals, hac_lag);
                fold.positive_day_ratio = (!residuals.is_empty()).then(|| {
                    residuals.iter().filter(|value| **value > 0.0).count() as f64
                        / residuals.len() as f64
                });
                if fold.incremental_mean.is_some_and(|value| value > 0.0) {
                    incremental.positive_folds += 1;
                }
            }
        }
        incremental.folds.push(fold);
    }

    incremental
}

#[cfg(test)]
mod tests {
    use crate::simulate::rule::RuleLayerPoint;
    use crate::statistics::test_support::*;
    use crate::statistics::validation::walk_forward::build_validation_fold_plan;
    use crate::statistics::validation::walk_forward::build_validation_incremental;
    use crate::statistics::validation::walk_forward::build_validation_walk_forward;
    use crate::statistics::validation::walk_forward::sort_validation_points;
    use std::collections::HashMap;

    #[test]
    fn validation_fold_plan_purges_holding_period_before_test() {
        let folds = build_validation_fold_plan(100, 4, 2);

        assert_eq!(folds.len(), 4);
        for fold in &folds {
            assert_eq!(fold.train_end_index + 1 + 2, fold.test_start_index);
            assert!(fold.train_end_index < fold.test_start_index);
            assert!(fold.test_start_index <= fold.test_end_index);
        }
        for pair in folds.windows(2) {
            assert!(
                pair[1].test_start_index > pair[0].test_end_index,
                "测试窗口不能重叠"
            );
            assert!(
                pair[1].train_end_index > pair[0].train_end_index,
                "训练窗口必须随 fold 展开"
            );
        }
    }

    #[test]
    fn validation_walk_forward_folds_keep_train_dates_disjoint_from_test() {
        let points = validation_fold_test_axis(200);
        let axis = sort_validation_points(&points);
        let holding_period = 2;
        let calendar = validation_calendar_of(&points);
        let folds = build_validation_fold_plan(calendar.len(), 4, holding_period);
        let day_trigger_counts = axis
            .iter()
            .map(|point| (point.trade_date.clone(), 3usize))
            .collect::<HashMap<_, _>>();
        let walk_forward = build_validation_walk_forward(
            &calendar,
            &folds,
            holding_period,
            1.0,
            &validation_points_by_date(&axis),
            &day_trigger_counts,
        );

        assert_eq!(walk_forward.purge_days, holding_period);
        assert_eq!(walk_forward.folds.len(), 4);
        for fold in &walk_forward.folds {
            assert_eq!(fold.status, "ok");
            assert!(fold.train_end_date < fold.test_start_date);
            assert!(fold.test_start_date <= fold.test_end_date);
            assert!(fold.test_day_count > 0);
            assert_eq!(fold.test_sample_count, fold.test_day_count * 3);
        }
        for pair in walk_forward.folds.windows(2) {
            assert!(pair[1].test_start_date > pair[0].test_end_date);
        }
    }

    #[test]
    fn validation_fold_plan_excludes_test_first_day_labels_with_single_day_holding() {
        // holding period = 1 时 train 末日的收益实现日正好是 test 首日，
        // 因此 purge 至少要排除 1 个交易日，train 不能使用 test 首日行情。
        let points = validation_fold_test_axis(100);
        let axis = sort_validation_points(&points);
        let holding_period = 1;
        let calendar = validation_calendar_of(&points);
        let folds = build_validation_fold_plan(calendar.len(), 4, holding_period);
        let day_trigger_counts = axis
            .iter()
            .map(|point| (point.trade_date.clone(), 1usize))
            .collect::<HashMap<_, _>>();
        let walk_forward = build_validation_walk_forward(
            &calendar,
            &folds,
            holding_period,
            1.0,
            &validation_points_by_date(&axis),
            &day_trigger_counts,
        );

        assert_eq!(walk_forward.purge_days, 1);
        assert!(!walk_forward.folds.is_empty());
        for fold in &walk_forward.folds {
            let train_end_index = calendar
                .iter()
                .position(|trade_date| *trade_date == fold.train_end_date)
                .expect("train end index");
            let test_start_index = calendar
                .iter()
                .position(|trade_date| *trade_date == fold.test_start_date)
                .expect("test start index");
            // train 末日的下一日恰好是被 purge 掉的那一天，其标签收益实现日就是 test 首日。
            assert_eq!(train_end_index + 2, test_start_index);
            assert!(fold.train_end_date < calendar[test_start_index - 1]);
        }
    }

    #[test]
    fn validation_walk_forward_does_not_flip_raw_ic_and_spread_for_negative_rule() {
        // 负向规则（points < 0）工作时的原始 IC/Spread 已经为正：分数符号进入秩相关与
        // 高低分半区，再乘方向符号会把它们翻反。只有方向盲的残差均值需要翻转。
        let points = (0..200)
            .map(|index| RuleLayerPoint {
                trade_date: format!("{index:08}"),
                sample_count: 10,
                avg_rule_score: Some(-1.0),
                avg_residual_return: Some(-0.01),
                avg_excess_residual_return: Some(-0.01),
                score_weighted_residual_return: Some(0.02),
                top_bottom_spread: Some(0.02),
                ic: Some(0.05),
            })
            .collect::<Vec<_>>();
        let axis = sort_validation_points(&points);
        let calendar = validation_calendar_of(&points);
        let folds = build_validation_fold_plan(calendar.len(), 4, 1);
        let day_trigger_counts = axis
            .iter()
            .map(|point| (point.trade_date.clone(), 3usize))
            .collect::<HashMap<_, _>>();

        let walk_forward = build_validation_walk_forward(
            &calendar,
            &folds,
            1,
            -1.0,
            &validation_points_by_date(&axis),
            &day_trigger_counts,
        );

        assert_eq!(walk_forward.folds.len(), 4);
        for fold in &walk_forward.folds {
            assert_eq!(fold.status, "ok");
            assert!(
                fold.ic_mean
                    .is_some_and(|value| (value - 0.05).abs() < 1e-12)
            );
            assert!(
                fold.spread_mean
                    .is_some_and(|value| (value - 0.02).abs() < 1e-12)
            );
            assert!(
                fold.avg_residual_return
                    .is_some_and(|value| (value - 0.01).abs() < 1e-12)
            );
        }
        assert_eq!(walk_forward.ic_positive_folds, 4);
        assert_eq!(walk_forward.residual_positive_folds, 4);
        assert_eq!(walk_forward.spread_positive_folds, 4);
    }

    #[test]
    fn validation_walk_forward_shares_one_calendar_across_combos() {
        // 两个组合的触发密度不同，但 train/test 日期必须完全来自同一套 calendar；
        // 触发稀疏的组合只在对应 fold 标记 insufficient，不重新切日期。
        let points = validation_fold_test_axis(200);
        let axis = sort_validation_points(&points);
        let calendar = validation_calendar_of(&points);
        let folds = build_validation_fold_plan(calendar.len(), 4, 1);
        let dense_counts = axis
            .iter()
            .map(|point| (point.trade_date.clone(), 5usize))
            .collect::<HashMap<_, _>>();
        // 稀疏组合每 10 个交易日只有一个有效日，每个 fold 窗口都不足 20 个有效日。
        let sparse_counts = axis
            .iter()
            .enumerate()
            .filter(|(index, _)| index % 10 == 0)
            .map(|(_, point)| (point.trade_date.clone(), 1usize))
            .collect::<HashMap<_, _>>();
        let sparse_points = axis
            .iter()
            .filter(|point| sparse_counts.contains_key(&point.trade_date))
            .copied()
            .collect::<Vec<_>>();

        let dense = build_validation_walk_forward(
            &calendar,
            &folds,
            1,
            1.0,
            &validation_points_by_date(&axis),
            &dense_counts,
        );
        let sparse = build_validation_walk_forward(
            &calendar,
            &folds,
            1,
            1.0,
            &validation_points_by_date(&sparse_points),
            &sparse_counts,
        );

        assert_eq!(dense.folds.len(), sparse.folds.len());
        for (dense_fold, sparse_fold) in dense.folds.iter().zip(&sparse.folds) {
            assert_eq!(dense_fold.train_start_date, sparse_fold.train_start_date);
            assert_eq!(dense_fold.train_end_date, sparse_fold.train_end_date);
            assert_eq!(dense_fold.test_start_date, sparse_fold.test_start_date);
            assert_eq!(dense_fold.test_end_date, sparse_fold.test_end_date);
            assert_eq!(dense_fold.status, "ok");
            assert_eq!(sparse_fold.status, "insufficient");
            assert!(sparse_fold.test_day_count < 20);
            assert!(sparse_fold.ic_mean.is_none());
            assert!(sparse_fold.avg_residual_return.is_none());
        }
        assert_eq!(sparse.ic_positive_folds, 0);
        assert_eq!(sparse.residual_positive_folds, 0);
        assert_eq!(sparse.spread_positive_folds, 0);
    }

    #[test]
    fn validation_incremental_mean_recovers_constant_alpha() {
        // candidate = core + 5 在 train/test 都成立：样本外增量应回到 constant_alpha，
        // 而不是被 train intercept 吸收成 0。core 在窗口内正负均衡（均值 0），
        // 因此只剩标准化 ridge 的收缩项 0.0909 × x，其窗口均值同样为 0。
        let constant_alpha = 5.0;
        let points = validation_fold_test_axis(120);
        let axis = sort_validation_points(&points);
        let calendar = validation_calendar_of(&points);
        let folds = build_validation_fold_plan(calendar.len(), 1, 0);
        let core_daily = axis
            .iter()
            .enumerate()
            .map(|(index, point)| {
                let core = if index % 2 == 0 { 1.0 } else { -1.0 };
                (point.trade_date.clone(), core)
            })
            .collect::<HashMap<_, _>>();
        let candidate_daily = axis
            .iter()
            .enumerate()
            .map(|(index, point)| {
                let core = if index % 2 == 0 { 1.0 } else { -1.0 };
                (point.trade_date.clone(), core + constant_alpha)
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

        let fold = &incremental.folds[0];
        assert_eq!(fold.status, "ok");
        assert!(
            fold.incremental_mean
                .is_some_and(|value| (value - constant_alpha).abs() < 1e-6),
            "{:?}",
            fold.incremental_mean
        );
        assert!(fold.incremental_mean.is_some_and(|value| value > 0.0));
        assert_eq!(incremental.positive_folds, 1);
    }

    #[test]
    fn validation_incremental_prediction_uses_train_fit_only() {
        let points = validation_fold_test_axis(120);
        let axis = sort_validation_points(&points);
        let calendar = validation_calendar_of(&points);
        let folds = build_validation_fold_plan(calendar.len(), 1, 0);
        let candidate_daily = axis
            .iter()
            .enumerate()
            .map(|(index, point)| {
                let candidate = if index >= 30 {
                    2.0
                } else {
                    2.0 * (1.0 + index as f64)
                };
                (point.trade_date.clone(), candidate)
            })
            .collect::<HashMap<_, _>>();
        let core_daily = axis
            .iter()
            .enumerate()
            .map(|(index, point)| {
                let core = if index >= 30 {
                    100.0
                } else {
                    1.0 + index as f64
                };
                (point.trade_date.clone(), core)
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

        // train 期 y = 2x（斜率 2、均值 2/1），test 期把 x 抬到 100 而 y 保持 2，
        // 因此只有用 train 拟合出的斜率（≈2）外推才会得到明显负增量：
        // 正确预测 ≈ 2 + 2 * (100 - 1) = 200 → 增量 ≈ -198。
        // 若把 test 混入拟合，斜率会被拉到 ≈0，残差接近 0，测试会立刻失败。
        let fold = &incremental.folds[0];
        assert_eq!(fold.status, "ok");
        assert_eq!(fold.train_day_count, 30);
        assert_eq!(fold.test_day_count, 90);
        assert!(
            fold.incremental_mean.is_some_and(|value| value < -150.0),
            "{:?}",
            fold.incremental_mean
        );
        assert_eq!(incremental.positive_folds, 0);
    }

    #[test]
    fn validation_incremental_marks_insufficient_folds_without_counting_windows() {
        // 场景一：候选只在最前面 25 个交易日有效，所有 fold 都达不到
        // max(30, predictors × 5) 的训练行或 20 个共同有效日。
        let points = validation_fold_test_axis(80);
        let axis = sort_validation_points(&points);
        let calendar = validation_calendar_of(&points);
        let folds = build_validation_fold_plan(calendar.len(), 3, 0);
        let candidate_daily = axis
            .iter()
            .enumerate()
            .filter(|(index, _)| *index < 25)
            .map(|(index, point)| (point.trade_date.clone(), 1.0 + index as f64))
            .collect::<HashMap<_, _>>();
        let core_daily = axis
            .iter()
            .enumerate()
            .map(|(index, point)| (point.trade_date.clone(), 1.0 + index as f64))
            .collect::<HashMap<_, _>>();

        let incremental = build_validation_incremental(
            &calendar,
            &folds,
            1,
            0,
            &candidate_daily,
            &[("核心策略".to_string(), &core_daily)],
        );

        assert!(!incremental.folds.is_empty());
        for fold in &incremental.folds {
            assert!(fold.train_day_count < 30, "{}", fold.train_day_count);
            assert_eq!(fold.status, "insufficient");
            assert!(fold.incremental_mean.is_none());
            assert!(fold.incremental_hac_t.is_none());
            assert!(fold.positive_day_ratio.is_none());
        }
        assert!(
            incremental
                .folds
                .iter()
                .any(|fold| fold.test_day_count < 20)
        );
        assert_eq!(incremental.positive_folds, 0);

        // 场景二：训练行足够，但核心策略在样本外只有 15 个共同有效日（< 20），
        // 该 fold 同样标记 insufficient。
        let points = validation_fold_test_axis(120);
        let axis = sort_validation_points(&points);
        let calendar = validation_calendar_of(&points);
        let folds = build_validation_fold_plan(calendar.len(), 1, 0);
        let candidate_daily = axis
            .iter()
            .enumerate()
            .map(|(index, point)| (point.trade_date.clone(), 1.0 + index as f64))
            .collect::<HashMap<_, _>>();
        let short_core_daily = axis
            .iter()
            .enumerate()
            .filter(|(index, _)| *index < 45)
            .map(|(index, point)| (point.trade_date.clone(), 1.0 + index as f64))
            .collect::<HashMap<_, _>>();
        let incremental = build_validation_incremental(
            &calendar,
            &folds,
            0,
            0,
            &candidate_daily,
            &[("核心策略".to_string(), &short_core_daily)],
        );

        let fold = &incremental.folds[0];
        assert_eq!(fold.train_day_count, 30);
        assert_eq!(fold.test_day_count, 15);
        assert_eq!(fold.status, "insufficient");
        assert!(fold.incremental_mean.is_none());
        assert_eq!(incremental.positive_folds, 0);
    }

    #[test]
    fn validation_incremental_ridge_handles_collinear_core_strategies() {
        let points = validation_fold_test_axis(120);
        let axis = sort_validation_points(&points);
        let calendar = validation_calendar_of(&points);
        let folds = build_validation_fold_plan(calendar.len(), 1, 0);
        let candidate_daily = axis
            .iter()
            .enumerate()
            .map(|(index, point)| (point.trade_date.clone(), 1.0 + (index % 7) as f64))
            .collect::<HashMap<_, _>>();
        let collinear = axis
            .iter()
            .enumerate()
            .map(|(index, point)| (point.trade_date.clone(), 1.0 + (index % 7) as f64))
            .collect::<HashMap<_, _>>();

        // 两个完全相同的 predictor 会让普通最小二乘的正规方程退化，标准化 ridge 必须仍然可解。
        let incremental = build_validation_incremental(
            &calendar,
            &folds,
            0,
            0,
            &candidate_daily,
            &[
                ("核心策略A".to_string(), &collinear),
                ("核心策略B".to_string(), &collinear),
            ],
        );

        let fold = &incremental.folds[0];
        assert_eq!(fold.status, "ok");
        assert!(fold.incremental_mean.is_some_and(|value| value.is_finite()));
    }
}
