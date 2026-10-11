use crate::scoring_model::ScoreSummary;
use crate::simulate::rank::RankLayerFromDbInput;
use crate::simulate::rank::RankLayerMethod;
use crate::simulate::rank::calc_rank_layer_metrics_from_rank_samples;
use crate::statistics::backtest::{
    RankLayerMarketValueSummary, RankLayerSampleGroup, RankTopKPeriodSummaryData,
    RankTopKSummaryData,
};
use crate::statistics::universe::{ValidationSampleRawRow, ValidationSampleStockMeta};
use crate::statistics::validation::samples::{
    compare_negative_validation_sample, compare_positive_validation_sample,
    compare_random_validation_sample, push_limited_random_sample, push_limited_sample,
    sample_board, validation_sample_rows_to_payload,
};

use rand::random;
use std::collections::HashMap;
use std::collections::HashSet;
pub(in crate::statistics) const RANK_BACKTEST_LAYER_SAMPLE_LIMIT_PER_GROUP: usize = 5;

pub(in crate::statistics) fn rank_layer_label(layer_index: usize, layer_count: usize) -> String {
    if layer_index == 1 {
        "第1层（低分）".to_string()
    } else if layer_index == layer_count {
        format!("第{layer_index}层（高分）")
    } else {
        format!("第{layer_index}层")
    }
}

pub(in crate::statistics) fn rank_layer_method_label(
    layer_method: RankLayerMethod,
) -> &'static str {
    match layer_method {
        RankLayerMethod::Score => "按分数分层",
        RankLayerMethod::SampleCount => "按样本数分层（同分按数据库排名）",
        RankLayerMethod::Rank => "按数据库排名分层",
    }
}

pub(in crate::statistics) fn rank_top_k_summary_data(
    items: Vec<crate::simulate::rank::RankTopKSummary>,
) -> Vec<RankTopKSummaryData> {
    items
        .into_iter()
        .map(|item| RankTopKSummaryData {
            top_k: item.top_k,
            point_count: item.point_count,
            sample_count: item.sample_count,
            avg_daily_residual_return: item.avg_daily_residual_return,
            median_daily_residual_return: item.median_daily_residual_return,
            positive_day_ratio: item.positive_day_ratio,
            daily_std: item.daily_std,
            hac_t_value: item.hac_t_value,
            hac_lag: item.hac_lag,
        })
        .collect()
}

pub(in crate::statistics) fn rank_top_k_period_summary_data(
    items: Vec<crate::simulate::rank::RankTopKPeriodSummary>,
) -> Vec<RankTopKPeriodSummaryData> {
    items
        .into_iter()
        .map(|item| RankTopKPeriodSummaryData {
            period_label: item.period_label,
            start_date: item.start_date,
            end_date: item.end_date,
            top_k: item.top_k,
            point_count: item.point_count,
            sample_count: item.sample_count,
            avg_daily_residual_return: item.avg_daily_residual_return,
            median_daily_residual_return: item.median_daily_residual_return,
            positive_day_ratio: item.positive_day_ratio,
            hac_t_value: item.hac_t_value,
            hac_lag: item.hac_lag,
        })
        .collect()
}

pub(in crate::statistics) fn build_rank_market_value_summaries(
    source_path: &str,
    input: &RankLayerFromDbInput,
    summary_rows: &[ScoreSummary],
    samples: &[crate::simulate::rank::RankLayerSamplePoint],
) -> Result<Vec<RankLayerMarketValueSummary>, String> {
    let conn = crate::data::open_db_readonly(crate::data::source_db_path(source_path))
        .map_err(|e| e.to_string())?;
    let per_price = crate::data::extras::load_estimated_market_cap_per_price_map(
        &conn,
        source_path,
        &input.stock_adj_type,
    )?;
    let weights = per_price
        .iter()
        .map(|(code, value)| format!("('{}', {})", code.replace('\'', "''"), value))
        .collect::<Vec<_>>();
    if weights.is_empty() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();

    for (group_label, total_mv_min, total_mv_max) in
        (|| -> [(&'static str, Option<f64>, Option<f64>); 3] {
            [
                ("小市值(<50亿)", None, Some(50.0)),
                ("中市值(50-200亿)", Some(50.0), Some(200.0)),
                ("大市值(>=200亿)", Some(200.0), None),
            ]
        })()
    {
        let sql = format!(
            r#"
            WITH weights(ts_code, per_price) AS (VALUES {})
            SELECT s.ts_code, s.trade_date
            FROM stock_data s JOIN weights w ON s.ts_code = w.ts_code
            WHERE s.adj_type = ? AND s.trade_date >= ? AND s.trade_date <= ?
              AND TRY_CAST(s.close AS DOUBLE) > 0 AND isfinite(TRY_CAST(s.close AS DOUBLE) * w.per_price)
              AND (? IS NULL OR TRY_CAST(s.close AS DOUBLE) * w.per_price >= ?)
              AND (? IS NULL OR TRY_CAST(s.close AS DOUBLE) * w.per_price < ?)
        "#,
            weights.join(",")
        );
        let mut stmt = conn
            .prepare(&sql)
            .map_err(|e| format!("预编译历史市值分组失败:{e}"))?;
        let mut rows = stmt
            .query(duckdb::params![
                &input.stock_adj_type,
                &input.start_date,
                &input.end_date,
                total_mv_min,
                total_mv_min,
                total_mv_max,
                total_mv_max
            ])
            .map_err(|e| format!("查询历史市值分组失败:{e}"))?;
        let mut group_pairs = HashSet::new();
        while let Some(row) = rows.next().map_err(|e| e.to_string())? {
            let ts_code: String = row.get(0).map_err(|e| e.to_string())?;
            let date: String = row.get(1).map_err(|e| e.to_string())?;
            group_pairs.insert(
                crate::statistics::validation::similarity::validation_pair_hash(&ts_code, &date),
            );
        }
        let group_rows = summary_rows
            .iter()
            .filter(|row| {
                group_pairs.contains(
                    &crate::statistics::validation::similarity::validation_pair_hash(
                        &row.ts_code,
                        &row.trade_date,
                    ),
                )
            })
            .cloned()
            .collect::<Vec<_>>();
        let group_samples = samples
            .iter()
            .filter(|sample| {
                group_pairs.contains(
                    &crate::statistics::validation::similarity::validation_pair_hash(
                        &sample.ts_code,
                        &sample.trade_date,
                    ),
                )
            })
            .cloned()
            .collect::<Vec<_>>();
        let metrics = calc_rank_layer_metrics_from_rank_samples(
            &group_samples,
            &input.layer_config,
            &group_rows,
        )?;
        out.push(RankLayerMarketValueSummary {
            group_label: group_label.to_string(),
            total_mv_min,
            total_mv_max,
            point_count: metrics.point_count,
            sample_count: metrics.sample_count,
            avg_er_change: metrics.avg_er_change,
            spread_mean: metrics.spread_mean,
            ic_mean: metrics.ic_mean,
            ic_t_value: metrics.ic_t_value,
            icir: metrics.icir,
        });
    }

    Ok(out)
}

#[derive(Default)]
pub(in crate::statistics) struct RankLayerSampleGroupAccumulator {
    pub(in crate::statistics) total_samples: usize,
    pub(in crate::statistics) positive_count: usize,
    pub(in crate::statistics) negative_count: usize,
    pub(in crate::statistics) trade_dates: HashSet<String>,
    pub(in crate::statistics) positive_by_board: HashMap<String, Vec<ValidationSampleRawRow>>,
    pub(in crate::statistics) negative_by_board: HashMap<String, Vec<ValidationSampleRawRow>>,
    pub(in crate::statistics) random_by_board: HashMap<String, Vec<(u64, ValidationSampleRawRow)>>,
}

pub(in crate::statistics) fn build_rank_layer_sample_groups(
    samples: &[crate::simulate::rank::RankLayerSamplePoint],
    layer_count: usize,
    stock_meta_map: &HashMap<String, ValidationSampleStockMeta>,
) -> Vec<RankLayerSampleGroup> {
    let mut groups = (0..layer_count)
        .map(|_| RankLayerSampleGroupAccumulator::default())
        .collect::<Vec<_>>();

    for sample in samples {
        if sample.layer_index == 0 || sample.layer_index > layer_count {
            continue;
        }
        let group = &mut groups[sample.layer_index - 1];
        let row = ValidationSampleRawRow {
            ts_code: sample.ts_code.clone(),
            trade_date: sample.trade_date.clone(),
            trigger_count: 1,
            rule_score: sample.score,
            residual_return: sample.residual_return,
        };

        group.total_samples += 1;
        group.trade_dates.insert(row.trade_date.clone());
        let board = sample_board(&row.ts_code, stock_meta_map);
        if row.residual_return > 0.0 {
            group.positive_count += 1;
            push_limited_sample(
                group.positive_by_board.entry(board.clone()).or_default(),
                row.clone(),
                RANK_BACKTEST_LAYER_SAMPLE_LIMIT_PER_GROUP,
                compare_positive_validation_sample,
            );
        } else if row.residual_return < 0.0 {
            group.negative_count += 1;
            push_limited_sample(
                group.negative_by_board.entry(board.clone()).or_default(),
                row.clone(),
                RANK_BACKTEST_LAYER_SAMPLE_LIMIT_PER_GROUP,
                compare_negative_validation_sample,
            );
        }
        push_limited_random_sample(
            group.random_by_board.entry(board).or_default(),
            random::<u64>(),
            row,
            RANK_BACKTEST_LAYER_SAMPLE_LIMIT_PER_GROUP,
        );
    }

    groups
        .into_iter()
        .enumerate()
        .map(|(index, group)| {
            let triggered_days = group.trade_dates.len();
            let mut positive = group
                .positive_by_board
                .into_values()
                .flatten()
                .collect::<Vec<_>>();
            let mut negative = group
                .negative_by_board
                .into_values()
                .flatten()
                .collect::<Vec<_>>();
            let mut random = group
                .random_by_board
                .into_values()
                .flatten()
                .collect::<Vec<_>>();
            positive.sort_by(compare_positive_validation_sample);
            negative.sort_by(compare_negative_validation_sample);
            random.sort_by(|left, right| {
                left.0
                    .cmp(&right.0)
                    .then_with(|| compare_random_validation_sample(&left.1, &right.1))
            });

            RankLayerSampleGroup {
                layer_index: index + 1,
                layer_label: rank_layer_label(index + 1, layer_count),
                total_samples: group.total_samples,
                triggered_days,
                positive_count: group.positive_count,
                negative_count: group.negative_count,
                random_count: group.total_samples,
                positive: validation_sample_rows_to_payload(positive, stock_meta_map),
                negative: validation_sample_rows_to_payload(negative, stock_meta_map),
                random: validation_sample_rows_to_payload(
                    random.into_iter().map(|(_, row)| row),
                    stock_meta_map,
                ),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use crate::simulate::rank::RankLayerSamplePoint;
    use crate::statistics::backtest::rank_layer::build_rank_layer_sample_groups;
    use crate::statistics::universe::ValidationSampleStockMeta;
    use std::collections::HashMap;

    #[test]
    fn rank_layer_samples_keep_full_observation_counts_and_per_board_limit() {
        let mut samples = Vec::new();
        let mut stock_meta_map = HashMap::new();
        for (board_prefix, board) in [("MB", "主板"), ("CY", "创业板")] {
            for index in 0..6 {
                let ts_code = format!("{board_prefix}{index:04}.SZ");
                stock_meta_map.insert(
                    ts_code.clone(),
                    ValidationSampleStockMeta {
                        name: None,
                        board: board.to_string(),
                        volatility_group: "常规波动".to_string(),
                    },
                );
                samples.push(RankLayerSamplePoint {
                    layer_index: 1,
                    ts_code: ts_code.clone(),
                    trade_date: "20240102".to_string(),
                    score: 10.0,
                    residual_return: index as f64 + 1.0,
                    er_change: f64::INFINITY,
                });
                samples.push(RankLayerSamplePoint {
                    layer_index: 1,
                    ts_code,
                    trade_date: "20240103".to_string(),
                    score: 10.0,
                    residual_return: index as f64 + 11.0,
                    er_change: f64::INFINITY,
                });
            }
        }

        let groups = build_rank_layer_sample_groups(&samples, 1, &stock_meta_map);
        let group = &groups[0];

        assert_eq!(group.total_samples, 24);
        assert_eq!(group.triggered_days, 2);
        assert_eq!(group.positive_count, 24);
        assert_eq!(group.positive.len(), 10);
        assert_eq!(group.positive[0].residual_return, 16.0);
        assert!(
            group
                .positive
                .iter()
                .all(|row| row.trade_date == "20240103")
        );
    }
}
