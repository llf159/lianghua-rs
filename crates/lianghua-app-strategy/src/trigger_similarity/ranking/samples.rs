use crate::trigger_similarity::*;

use super::*;
use crate::trigger_similarity::channel::build_channel_fingerprint;
use crate::trigger_similarity::channel::build_indicator_fingerprint;
use crate::trigger_similarity::channel::build_price_volume_channels;
use crate::trigger_similarity::channel::market_category_features;
use crate::trigger_similarity::fingerprint::build_trigger_fingerprint;
use crate::trigger_similarity::load::load_future_rows;
use crate::trigger_similarity::load::load_market_rows;
use crate::trigger_similarity::load::load_rule_rows;
use crate::trigger_similarity::load::load_summary_rows;
use crate::trigger_similarity::sample::window_dates_for_anchor;
use crate::trigger_similarity::sample::{OutcomeSummarySample, build_outcome, summarize_outcomes};
use duckdb::Connection;
use duckdb::params;
use rayon::prelude::*;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;
#[allow(clippy::too_many_arguments)]
pub(in crate::trigger_similarity) fn build_ranking_samples_for_chunk(
    conn: &Connection,
    anchors: Vec<Anchor>,
    schema: &MarketSchema,
    all_trade_dates: &[String],
    environment_fingerprints: &HashMap<String, Arc<ChannelFingerprint>>,
    benchmark_rows: &HashMap<String, BenchmarkObservation>,
    pool_segments: usize,
    outcome_trade_days: usize,
    target_trade_date: &str,
    include_outcome: bool,
    progress: Option<(&str, usize, usize)>,
    rule_catalog: &mut RuleCatalog,
) -> Result<Vec<RankingSample>, String> {
    if let Some((phase, completed, total)) = progress {
        set_ranking_progress(phase, "正在读取本批股票的行情和指标窗口", completed, total);
    }
    let market_by_anchor = load_market_rows(conn, &anchors, schema)?;
    if let Some((phase, completed, total)) = progress {
        set_ranking_progress(phase, "正在读取本批股票的策略触发窗口", completed, total);
    }
    let rules_by_anchor = load_rule_rows(conn, &anchors, rule_catalog)?;
    let summaries = if include_outcome {
        HashMap::new()
    } else {
        if let Some((phase, completed, total)) = progress {
            set_ranking_progress(phase, "正在读取本批股票的评分摘要", completed, total);
        }
        load_summary_rows(conn, &anchors)?
    };
    let future_by_anchor = if include_outcome {
        if let Some((phase, completed, total)) = progress {
            set_ranking_progress(phase, "正在读取本批历史模板的后验行情", completed, total);
        }
        load_future_rows(conn, &anchors, outcome_trade_days, target_trade_date)?
    } else {
        HashMap::new()
    };
    if let Some((phase, completed, total)) = progress {
        set_ranking_progress(phase, "正在构建本批股票的指纹", completed, total);
    }
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
            if include_outcome
                && outcome
                    .as_ref()
                    .and_then(|item| item.excess_return_pct)
                    .is_none()
            {
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
            let template_class = outcome
                .as_ref()
                .and_then(|item| item.excess_return_pct)
                .map_or(0, |value| {
                    if value > EPS {
                        1
                    } else if value < -EPS {
                        -1
                    } else {
                        0
                    }
                });
            Some(RankingSample {
                fingerprint: RankingFingerprint {
                    trigger,
                    price_volume: build_channel_fingerprint(build_price_volume_channels(
                        market_rows,
                        pool_segments,
                        market_category_features(&anchor.ts_code, None),
                    )),
                    indicators: build_indicator_fingerprint(
                        market_rows,
                        &schema.indicator_columns,
                        pool_segments,
                    ),
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
                template_class,
                anchor,
            })
        })
        .collect())
}

// 入池只依赖当时已知的评分资格和窗口内触发，不查看未来收益。
pub(in crate::trigger_similarity) fn load_history_anchors(
    conn: &Connection,
    earliest_date: &str,
    cutoff_date: &str,
    all_trade_dates: &[String],
    window_trade_days: usize,
) -> Result<(Vec<Anchor>, usize), String> {
    // 窗口内是否有触发只需检查最近一次触发，避免区间 EXISTS 展开同股历史配对。
    let mut stmt = conn
        .prepare(
            r#"
        WITH calendar AS (
            SELECT trade_date, row_number() OVER (ORDER BY trade_date) AS day_index
            FROM (SELECT DISTINCT trade_date FROM score_summary)
        ), triggers AS (
            SELECT DISTINCT r.ts_code, c.day_index
            FROM rule_details r JOIN calendar c USING(trade_date)
            WHERE r.trade_date <= ? AND isfinite(TRY_CAST(r.rule_score AS DOUBLE))
                AND ABS(TRY_CAST(r.rule_score AS DOUBLE)) > ?
        ), scored AS (
            SELECT DISTINCT s.ts_code, s.trade_date, c.day_index
            FROM score_summary s JOIN calendar c USING(trade_date)
            WHERE s.trade_date BETWEEN ? AND ?
        ), eligible AS (
            SELECT s.ts_code, s.trade_date
            FROM scored s ASOF JOIN triggers r
                ON s.ts_code = r.ts_code AND s.day_index >= r.day_index
            WHERE r.day_index >= s.day_index - ? + 1
        )
        SELECT ts_code, trade_date, count(*) OVER () AS universe_count
        FROM eligible
        ORDER BY hash(ts_code, trade_date), ts_code, trade_date LIMIT 50000
    "#,
        )
        .map_err(|e| format!("预编译历史样本抽样失败: {e}"))?;
    let mut rows = stmt
        .query(params![
            cutoff_date,
            EPS,
            earliest_date,
            cutoff_date,
            window_trade_days as i64
        ])
        .map_err(|e| format!("读取历史样本失败: {e}"))?;
    let mut anchors = Vec::new();
    let mut universe_count = 0;
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取历史样本行失败: {e}"))?
    {
        let ts_code: String = row.get(0).map_err(|e| e.to_string())?;
        let end_trade_date: String = row.get(1).map_err(|e| e.to_string())?;
        universe_count = row.get::<_, i64>(2).map_err(|e| e.to_string())?.max(0) as usize;
        let end_index = all_trade_dates
            .binary_search(&end_trade_date)
            .map_err(|_| format!("样本日期不在交易日历: {end_trade_date}"))?;
        anchors.push(Anchor {
            id: anchors.len(),
            ts_code,
            end_trade_date,
            start_trade_date: all_trade_dates[(end_index + 1).saturating_sub(window_trade_days)]
                .clone(),
        });
    }
    Ok((anchors, universe_count))
}

pub(super) fn summarize_ranking_matches(
    rating_candidates: &[ScoredCandidate],
    candidates: &[RankingSample],
    background_similarity_p90: Option<f64>,
) -> (
    StrategyTriggerSimilarityOutcomeSummary,
    f64,
    Option<f64>,
    Option<f64>,
    RankingDiagnostics,
) {
    let summary = summarize_outcomes(rating_candidates.iter().filter_map(|scored| {
        let outcome = candidates[scored.candidate_index].outcome.as_ref()?;
        Some(OutcomeSummarySample {
            similarity_score: scored.score,
            return_pct: outcome.return_pct,
            excess_return_pct: outcome.excess_return_pct,
            mfe_pct: outcome.mfe_pct,
            mae_pct: outcome.mae_pct,
        })
    }));
    let confidence = (summary.effective_sample_count
        / (summary.effective_sample_count + SHRINKAGE_STRENGTH))
        .max(0.0)
        .sqrt();
    let average_similarity = (!rating_candidates.is_empty()).then(|| {
        rating_candidates.iter().map(|row| row.score).sum::<f64>() / rating_candidates.len() as f64
    });
    let weight_sum = rating_candidates
        .iter()
        .map(|row| (row.score / 100.0).powi(2))
        .sum::<f64>();
    let excess_return_std_pct = summary
        .weighted_excess_return_pct
        .filter(|_| weight_sum > EPS)
        .map(|mean| {
            (rating_candidates
                .iter()
                .filter_map(|row| {
                    candidates[row.candidate_index]
                        .outcome
                        .as_ref()
                        .and_then(|outcome| outcome.excess_return_pct)
                        .map(|value| (row.score / 100.0).powi(2) * (value - mean).powi(2))
                })
                .sum::<f64>()
                / weight_sum)
                .sqrt()
        });
    let similarity_lift = average_similarity
        .zip(background_similarity_p90)
        .map(|(near, background)| near - background);
    let supported = summary.effective_sample_count + EPS >= SHRINKAGE_STRENGTH;
    let prediction_signal = supported
        .then_some(summary.shrunk_excess_return_pct)
        .flatten();
    let mut periods = rating_candidates
        .iter()
        .filter_map(|row| {
            let outcome = candidates[row.candidate_index].outcome.as_ref()?;
            Some((
                outcome.end_trade_date.as_str(),
                outcome.start_trade_date.as_str(),
            ))
        })
        .collect::<Vec<_>>();
    // 按真实后验结束日贪心选取最多的不重叠区间，停牌不会被压成固定交易日数。
    periods.sort_unstable();
    let mut period_count = 0;
    let mut last_period = None;
    for (end, start) in periods {
        if last_period.is_none_or(|previous| start > previous) {
            period_count += 1;
            last_period = Some(end);
        }
    }
    let diagnostics = RankingDiagnostics {
        status: if !supported {
            "insufficient_samples"
        } else {
            "available"
        }
        .into(),
        background_similarity_p90: background_similarity_p90,
        similarity_lift,
        excess_return_std_pct,
        outcome_period_count: period_count,
    };
    (
        summary,
        confidence,
        prediction_signal,
        average_similarity,
        diagnostics,
    )
}

pub(super) fn assign_ranks(rows: &mut [StrategyTriggerRankingRow]) {
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

#[cfg(test)]
mod tests {
    use crate::trigger_similarity::load::load_all_trade_dates;
    use crate::trigger_similarity::load::load_market_schema;
    use crate::trigger_similarity::load::open_result_conn;
    use crate::trigger_similarity::ranking::StrategyTriggerRankingRow;
    use crate::trigger_similarity::ranking::samples::assign_ranks;
    use crate::trigger_similarity::ranking::samples::build_ranking_samples_for_chunk;
    use crate::trigger_similarity::ranking::samples::load_history_anchors;
    use std::collections::HashMap;

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
            diagnostics: Default::default(),
        }
    }

    #[test]
    fn sample_builders_skip_unused_summary_and_preloaded_rule_queries() {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute_batch("ATTACH ':memory:' AS trigger_market_db;
            CREATE TABLE trigger_market_db.stock_data(ts_code VARCHAR, trade_date VARCHAR, adj_type VARCHAR, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE);
            INSERT INTO trigger_market_db.stock_data VALUES
            ('A', '20240101', 'qfq', 10, 12, 9, 11),
            ('A', '20240102', 'qfq', 11, 13, 10, 12),
            ('A', '20240103', 'qfq', 12, 14, 11, 13),
            ('A', '20240104', 'qfq', 13, 15, 12, 14);
            CREATE TABLE rule_details(ts_code VARCHAR, trade_date VARCHAR, rule_name VARCHAR, rule_score DOUBLE);
            INSERT INTO rule_details VALUES ('A', '20240103', '启动', 1.0);").unwrap();
        let schema = load_market_schema(&conn).unwrap();
        let dates = (1..=4)
            .map(|day| format!("202401{day:02}"))
            .collect::<Vec<_>>();
        let anchor = crate::trigger_similarity::Anchor {
            id: 7,
            ts_code: "A".into(),
            start_trade_date: dates[0].clone(),
            end_trade_date: dates[2].clone(),
        };
        let benchmark = HashMap::from([(
            dates[3].clone(),
            crate::trigger_similarity::BenchmarkObservation {
                open: 10.0,
                close: 11.0,
            },
        )]);
        let environment = HashMap::new();
        let mut catalog = crate::trigger_similarity::RuleCatalog::default();
        let historical = build_ranking_samples_for_chunk(
            &conn,
            vec![anchor.clone()],
            &schema,
            &dates,
            &environment,
            &benchmark,
            2,
            1,
            &dates[3],
            true,
            None,
            &mut catalog,
        )
        .unwrap();
        assert_eq!(historical.len(), 1);
        assert!(historical[0].total_score.is_none());
        assert!(historical[0].original_rank.is_none());
        assert!(historical[0].outcome.is_some());
        let rules = crate::trigger_similarity::load::load_rule_rows(
            &conn,
            std::slice::from_ref(&anchor),
            &mut catalog,
        )
        .unwrap();
        let context = crate::trigger_similarity::sample::SampleBuildContext {
            schema: &schema,
            all_trade_dates: &dates,
            environment_fingerprints: &environment,
            benchmark_rows: &benchmark,
            pool_segments: 2,
            outcome_trade_days: 1,
            target_trade_date: &dates[3],
            include_outcome: false,
            include_summaries: false,
        };
        let original = crate::trigger_similarity::sample::build_samples_for_chunk(
            &conn,
            vec![anchor.clone()],
            &context,
            &mut catalog,
            None,
        )
        .unwrap();
        conn.execute_batch("DROP TABLE rule_details").unwrap();
        let reused = crate::trigger_similarity::sample::build_samples_for_chunk(
            &conn,
            vec![anchor],
            &context,
            &mut catalog,
            Some(rules),
        )
        .unwrap();
        assert_eq!(reused.len(), 1);
        assert_eq!(reused[0].trigger_count, original[0].trigger_count);
        assert_eq!(
            reused[0].fingerprint.dimension(),
            original[0].fingerprint.dimension()
        );
        assert!(
            (crate::trigger_similarity::fingerprint::trigger_fingerprint_similarity(
                &reused[0].fingerprint.trigger,
                &original[0].fingerprint.trigger,
                &[]
            ) - 100.0)
                .abs()
                < 1e-9
        );
    }

    #[test]
    fn historical_sampling_does_not_require_or_select_by_future_prices() {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE score_summary(ts_code VARCHAR, trade_date VARCHAR);
            CREATE TABLE rule_details(ts_code VARCHAR, trade_date VARCHAR, rule_score DOUBLE);
            INSERT INTO score_summary VALUES ('A','20240101'),('A','20240102'),('A','20240102'),('A','20240103'),
                ('B','20240101'),('B','20240102'),('B','20240103'),
                ('C','20240101'),('C','20240102'),('C','20240103'),
                ('D','20240102'),('D','20240103'),('E','20240103');
            INSERT INTO rule_details VALUES ('A','20240101',1),('A','20240101',2),('B','20240102',-1),
                ('C','20240102',0),('C','20240102','NaN'),('C','20240102','Infinity'),('C','20240102',NULL),
                ('D','20240104',1),('E','20240103',1);").unwrap();
        let dates: Vec<String> = vec!["20240101".into(), "20240102".into(), "20240103".into()];
        let (anchors, count) =
            load_history_anchors(&conn, &dates[1], &dates[2], &dates, 2).unwrap();
        assert_eq!(count, 4);
        let mut keys = anchors
            .iter()
            .map(|row| (row.ts_code.as_str(), row.end_trade_date.as_str()))
            .collect::<Vec<_>>();
        keys.sort_unstable();
        assert_eq!(
            keys,
            vec![
                ("A", "20240102"),
                ("B", "20240102"),
                ("B", "20240103"),
                ("E", "20240103")
            ]
        );
    }

    #[test]
    fn prediction_uses_real_returns_and_keeps_matching_diagnostic_only() {
        use super::*;
        let dates = (1..=10)
            .map(|day| format!("202401{day:02}"))
            .collect::<Vec<_>>();
        let empty_channel = build_channel_fingerprint(Vec::new());
        let mut candidates = (0..8)
            .map(|index| RankingSample {
                anchor: Anchor {
                    id: index,
                    ts_code: format!("C{index}"),
                    start_trade_date: dates[0].clone(),
                    end_trade_date: dates[index].clone(),
                },
                fingerprint: RankingFingerprint {
                    trigger: build_trigger_fingerprint(&[], &dates, 2),
                    price_volume: empty_channel.clone(),
                    indicators: empty_channel.clone(),
                    market: Arc::new(empty_channel.clone()),
                },
                trigger_count: 1,
                outcome: Some(Outcome {
                    start_trade_date: dates[index + 1].clone(),
                    end_trade_date: dates[index + 1].clone(),
                    return_pct: 2.0,
                    excess_return_pct: Some(2.0),
                    mfe_pct: 100.0 * index as f64,
                    mae_pct: -100.0,
                }),
                total_score: None,
                original_rank: None,
                template_class: 0,
            })
            .collect::<Vec<_>>();
        let mut matches = (0..8)
            .map(|index| ScoredCandidate {
                score: 90.0,
                candidate_index: index,
            })
            .collect::<Vec<_>>();
        let (summary, confidence, signal, _, diagnostics) =
            summarize_ranking_matches(&matches, &candidates, Some(60.0));
        assert!((summary.effective_sample_count - 8.0).abs() < 1e-12);
        assert!((signal.unwrap() - 1.0).abs() < 1e-12);
        assert_eq!(diagnostics.status, "available");
        assert_eq!(diagnostics.outcome_period_count, 8);
        assert_eq!(diagnostics.excess_return_std_pct, Some(0.0));
        for row in &mut matches {
            row.score = 50.0;
        }
        let (_, flat_confidence, signal, _, diagnostics) =
            summarize_ranking_matches(&matches, &candidates, Some(50.0));
        assert!((confidence - flat_confidence).abs() < 1e-12);
        assert_eq!(signal, Some(1.0));
        assert_eq!(diagnostics.status, "available");
        assert_eq!(diagnostics.similarity_lift, Some(0.0));
        let (_, _, signal, _, diagnostics) =
            summarize_ranking_matches(&matches, &candidates, Some(60.0));
        assert_eq!(signal, Some(1.0));
        assert_eq!(diagnostics.similarity_lift, Some(-10.0));
        let (_, _, signal, _, diagnostics) = summarize_ranking_matches(&matches, &candidates, None);
        assert_eq!(signal, Some(1.0));
        assert_eq!(diagnostics.similarity_lift, None);
        candidates[0].outcome.as_mut().unwrap().end_trade_date = dates[8].clone();
        let (_, _, _, _, diagnostics) =
            summarize_ranking_matches(&matches, &candidates, Some(50.0));
        assert_eq!(diagnostics.outcome_period_count, 7);
        matches.pop();
        let (_, _, signal, _, diagnostics) =
            summarize_ranking_matches(&matches, &candidates, Some(20.0));
        assert_eq!(signal, None);
        assert_eq!(diagnostics.status, "insufficient_samples");
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
        let horizon = 5;
        let cutoff_index = all_trade_dates.len() - 1 - horizon;
        let earliest_date = all_trade_dates[19].clone();
        let cutoff_date = all_trade_dates[cutoff_index].clone();
        let started = std::time::Instant::now();
        let (anchors, universe_count) =
            load_history_anchors(&conn, &earliest_date, &cutoff_date, &all_trade_dates, 20)
                .expect("scan real outcome labels");
        eprintln!(
            "real outcome scan: elapsed={:?}, selected={}, universe={}",
            started.elapsed(),
            anchors.len(),
            universe_count
        );
        assert!(!anchors.is_empty());
    }
}
