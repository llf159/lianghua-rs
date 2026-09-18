use crate::trigger_similarity::fingerprint::temporal_signature;
use crate::trigger_similarity::{
    ChannelFingerprint, EPS, INDICATOR_SIMILARITY_WEIGHT, MARKET_SIMILARITY_WEIGHT,
    MarketEnvironment, MarketObservation, PRICE_VOLUME_SIMILARITY_WEIGHT,
    TRIGGER_SIMILARITY_WEIGHT,
};

// 见父模块 mod.rs

use std::collections::HashMap;
use std::sync::Arc;
pub(super) fn build_price_volume_channels(
    rows: &[MarketObservation],
    segments: usize,
    total_mv_yi: Option<f64>,
    market_categories: [f64; 5],
) -> Vec<Option<Vec<f64>>> {
    let mut channels = vec![Vec::new(); 13];
    // stock_list.csv 目前提供最新总市值而非逐日历史总市值。用固定对数尺度编码为
    // [0, 1]，作为股票规模这一项基本特征；不把它伪装成时间序列趋势。
    let normalized_market_cap = total_mv_yi
        .filter(|value| value.is_finite() && *value > 0.0)
        .map(|value| (value.log10() / 5.0).clamp(0.0, 1.0));
    let mut previous_close: Option<f64> = None;
    for row in rows {
        channels[0].push(row.close.filter(|v| *v > 0.0).map(f64::ln));
        channels[1].push(row.pct_chg);
        channels[2].push(match (row.open, row.close) {
            (Some(o), Some(c)) if o.abs() > EPS => Some((c / o - 1.0) * 100.0),
            _ => None,
        });
        channels[3].push(match (row.low, row.high) {
            (Some(l), Some(h)) if l.abs() > EPS => Some((h / l - 1.0) * 100.0),
            _ => None,
        });
        channels[4].push(match (row.low, row.high, row.close) {
            (Some(l), Some(h), Some(c)) if (h - l).abs() > EPS => Some((c - l) / (h - l)),
            _ => None,
        });
        channels[5].push(row.vol.filter(|v| *v >= 0.0).map(|v| (1.0 + v).ln()));
        channels[6].push(row.amount.filter(|v| *v >= 0.0).map(|v| (1.0 + v).ln()));
        channels[7].push(row.turnover);
        channels[8].push(match (row.net_flow, row.vol) {
            (Some(f), Some(v)) if v.abs() > EPS => Some(f / v),
            _ => None,
        });
        channels[9].push(match (previous_close, row.open) {
            (Some(previous), Some(open)) if previous.abs() > EPS && open.abs() > EPS => {
                Some((open / previous - 1.0) * 100.0)
            }
            _ => None,
        });
        channels[10].push(match (row.open, row.close, row.low, row.high) {
            (Some(open), Some(close), Some(low), Some(high))
                if (high - low).abs() > EPS && high >= open.max(close) =>
            {
                Some((high - open.max(close)) / (high - low))
            }
            _ => None,
        });
        channels[11].push(match (row.open, row.close, row.low, row.high) {
            (Some(open), Some(close), Some(low), Some(high))
                if (high - low).abs() > EPS && low <= open.min(close) =>
            {
                Some((open.min(close) - low) / (high - low))
            }
            _ => None,
        });
        channels[12].push(normalized_market_cap);
        previous_close = row.close.filter(|value| value.is_finite());
    }
    // 价格水平、成交量和成交额只比较相对形态；跳空、收益、振幅、影线、位置、
    // 换手和资金流保留原始状态，以免把方向、缺口和风险强度标准化掉。
    let standardize = [
        true, false, false, false, false, true, true, false, false, false, false, false, false,
    ];
    let mut fingerprints = channels
        .iter()
        .zip(standardize)
        .map(|(series, standardize)| temporal_signature(series, segments, standardize))
        .collect::<Vec<_>>();
    // 市场类别是静态名义变量，不存在时间趋势或类别远近。保留五维独热向量，
    // 作为一个独立通道直接参与余弦比较，避免扩展成五组重复的时间签名。
    fingerprints.push(Some(market_categories.to_vec()));
    fingerprints
}

// 与全局板块筛选保持一致：主板、科创板、创业板、北交所、ST 五维独热编码。
pub(super) fn market_category_features(ts_code: &str, stock_name: Option<&str>) -> [f64; 5] {
    let mut features = [0.0; 5];
    let index = match crate::utils::utils::board_category(ts_code, stock_name) {
        "主板" => Some(0),
        "科创板" => Some(1),
        "创业板" => Some(2),
        "北交所" => Some(3),
        "ST" => Some(4),
        _ => None,
    };
    if let Some(index) = index {
        features[index] = 1.0;
    }
    features
}

pub(super) fn build_indicator_channels(
    rows: &[MarketObservation],
    count: usize,
    segments: usize,
) -> Vec<Option<Vec<f64>>> {
    (0..count)
        .map(|index| {
            let series = rows
                .iter()
                .map(|row| row.indicators.get(index).copied().flatten())
                .collect::<Vec<_>>();
            temporal_signature(&series, segments, false)
        })
        .collect()
}

pub(super) fn build_environment_fingerprint_map(
    environment: &MarketEnvironment,
    all_trade_dates: &[String],
    window_trade_days: usize,
    segments: usize,
) -> HashMap<String, Vec<Option<Vec<f64>>>> {
    all_trade_dates
        .iter()
        .enumerate()
        .map(|(end_index, end_date)| {
            let start_index = (end_index + 1).saturating_sub(window_trade_days);
            (
                end_date.clone(),
                (|environment: &MarketEnvironment,
                  window_dates: &[String],
                  segments: usize|
                 -> Vec<Option<Vec<f64>>> {
                    (0..environment.channel_count)
                        .map(|index| {
                            let series = window_dates
                                .iter()
                                .map(|date| {
                                    environment
                                        .by_date
                                        .get(date)
                                        .and_then(|v| v.get(index))
                                        .copied()
                                        .flatten()
                                })
                                .collect::<Vec<_>>();
                            temporal_signature(&series, segments, true)
                        })
                        .collect()
                })(
                    environment,
                    &all_trade_dates[start_index..=end_index],
                    segments,
                ),
            )
        })
        .collect()
}

#[cfg(test)]
pub(super) fn cosine_similarity(left: &[f64], right: &[f64]) -> f64 {
    cosine_similarity_with_norms(left, right, vector_norm(left), vector_norm(right))
}

#[inline]
pub(super) fn scalar_dot_product(left: &[f64], right: &[f64]) -> f64 {
    left.iter().zip(right).map(|(a, b)| a * b).sum()
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn avx2_dot_product(left: &[f64], right: &[f64]) -> f64 {
    use std::arch::x86_64::{
        _mm_add_pd, _mm_cvtsd_f64, _mm_unpackhi_pd, _mm256_add_pd, _mm256_castpd256_pd128,
        _mm256_extractf128_pd, _mm256_loadu_pd, _mm256_mul_pd, _mm256_setzero_pd,
    };

    let len = left.len().min(right.len());
    let vectorized_len = len / 4 * 4;
    let mut accumulator = _mm256_setzero_pd();
    let mut index = 0;
    while index < vectorized_len {
        let left_values = unsafe { _mm256_loadu_pd(left.as_ptr().add(index)) };
        let right_values = unsafe { _mm256_loadu_pd(right.as_ptr().add(index)) };
        accumulator = _mm256_add_pd(accumulator, _mm256_mul_pd(left_values, right_values));
        index += 4;
    }
    let low = _mm256_castpd256_pd128(accumulator);
    let high = _mm256_extractf128_pd(accumulator, 1);
    let pair_sums = _mm_add_pd(low, high);
    let mut sum = _mm_cvtsd_f64(pair_sums) + _mm_cvtsd_f64(_mm_unpackhi_pd(pair_sums, pair_sums));
    while index < len {
        sum += left[index] * right[index];
        index += 1;
    }
    sum
}

#[inline]
pub(super) fn dot_product(left: &[f64], right: &[f64]) -> f64 {
    #[cfg(target_arch = "x86_64")]
    if std::arch::is_x86_feature_detected!("avx2") {
        // SAFETY: AVX2 availability is checked at runtime immediately above.
        return unsafe { avx2_dot_product(left, right) };
    }
    scalar_dot_product(left, right)
}

pub(super) fn vector_norm(values: &[f64]) -> f64 {
    dot_product(values, values).sqrt()
}

pub(super) fn cosine_similarity_with_norms(
    left: &[f64],
    right: &[f64],
    left_norm: f64,
    right_norm: f64,
) -> f64 {
    if left.len() != right.len() || left.is_empty() {
        return 0.0;
    }
    let dot = dot_product(left, right);
    if left_norm <= EPS && right_norm <= EPS {
        100.0
    } else if left_norm <= EPS || right_norm <= EPS {
        0.0
    } else {
        (50.0 * (1.0 + dot / (left_norm * right_norm))).clamp(0.0, 100.0)
    }
}

pub(super) fn build_channel_fingerprint(vectors: Vec<Option<Vec<f64>>>) -> ChannelFingerprint {
    let mut channel_offsets = Vec::with_capacity(vectors.len() + 1);
    let mut normalized_values = Vec::with_capacity(
        vectors
            .iter()
            .filter_map(|vector| vector.as_ref().map(Vec::len))
            .sum(),
    );
    let mut channel_states = Vec::with_capacity(vectors.len());
    let mut has_vectors = false;
    let mut all_channels_nonzero = !vectors.is_empty();
    let mut dimension = 0;
    channel_offsets.push(0);
    for vector in vectors {
        let Some(vector) = vector else {
            channel_states.push(0);
            all_channels_nonzero = false;
            channel_offsets.push(normalized_values.len());
            continue;
        };
        has_vectors = true;
        dimension += vector.len();
        let norm = vector_norm(&vector);
        if norm <= EPS {
            channel_states.push(1);
            all_channels_nonzero = false;
            normalized_values.extend(std::iter::repeat_n(0.0, vector.len()));
        } else {
            channel_states.push(2);
            normalized_values.extend(vector.into_iter().map(|value| value / norm));
        }
        channel_offsets.push(normalized_values.len());
    }
    ChannelFingerprint {
        channel_offsets,
        normalized_values,
        channel_states,
        dimension,
        has_vectors,
        all_channels_nonzero,
    }
}

pub(super) fn share_environment_fingerprints(
    fingerprints: HashMap<String, Vec<Option<Vec<f64>>>>,
) -> HashMap<String, Arc<ChannelFingerprint>> {
    fingerprints
        .into_iter()
        .map(|(trade_date, vectors)| (trade_date, Arc::new(build_channel_fingerprint(vectors))))
        .collect()
}

pub(super) fn cached_channel_similarity(
    target: &ChannelFingerprint,
    candidate: &ChannelFingerprint,
) -> Option<f64> {
    if target.all_channels_nonzero
        && candidate.all_channels_nonzero
        && target.channel_offsets == candidate.channel_offsets
    {
        let channel_count = target.channel_states.len();
        let cosine_sum = dot_product(&target.normalized_values, &candidate.normalized_values);
        return (channel_count > 0)
            .then(|| (50.0 * (1.0 + cosine_sum / channel_count as f64)).clamp(0.0, 100.0));
    }

    let mut score_sum = 0.0;
    let mut score_count = 0;
    for index in 0..target
        .channel_states
        .len()
        .min(candidate.channel_states.len())
    {
        let left_state = target.channel_states[index];
        let right_state = candidate.channel_states[index];
        if left_state == 0 || right_state == 0 {
            continue;
        }
        let left = &target.normalized_values
            [target.channel_offsets[index]..target.channel_offsets[index + 1]];
        let right = &candidate.normalized_values
            [candidate.channel_offsets[index]..candidate.channel_offsets[index + 1]];
        let score = if left.len() != right.len() || left.is_empty() {
            0.0
        } else if left_state == 1 && right_state == 1 {
            100.0
        } else if left_state == 1 || right_state == 1 {
            0.0
        } else {
            (50.0 * (1.0 + dot_product(left, right))).clamp(0.0, 100.0)
        };
        score_sum += score;
        score_count += 1;
    }
    (score_count > 0).then(|| score_sum / score_count as f64)
}

pub(super) fn final_similarity(
    trigger: f64,
    price: Option<f64>,
    indicator: Option<f64>,
    market: Option<f64>,
) -> f64 {
    let mut total = trigger * TRIGGER_SIMILARITY_WEIGHT;
    let mut weight = TRIGGER_SIMILARITY_WEIGHT;
    for (score, w) in [
        (price, PRICE_VOLUME_SIMILARITY_WEIGHT),
        (indicator, INDICATOR_SIMILARITY_WEIGHT),
        (market, MARKET_SIMILARITY_WEIGHT),
    ] {
        if let Some(score) = score {
            total += score * w;
            weight += w;
        }
    }
    total / weight
}

#[cfg(test)]
mod tests {
    use crate::trigger_similarity::ANCHOR_CHUNK_SIZE;
    use crate::trigger_similarity::INDICATOR_SIMILARITY_WEIGHT;
    use crate::trigger_similarity::MARKET_SIMILARITY_WEIGHT;
    use crate::trigger_similarity::MIN_SAMPLE_GAP_TRADE_DAYS;
    use crate::trigger_similarity::MarketObservation;
    use crate::trigger_similarity::PRICE_VOLUME_SIMILARITY_WEIGHT;
    use crate::trigger_similarity::TRIGGER_SIMILARITY_WEIGHT;
    use crate::trigger_similarity::channel::build_channel_fingerprint;
    use crate::trigger_similarity::channel::build_environment_fingerprint_map;
    use crate::trigger_similarity::channel::build_price_volume_channels;
    use crate::trigger_similarity::channel::cached_channel_similarity;
    use crate::trigger_similarity::channel::cosine_similarity;
    use crate::trigger_similarity::channel::dot_product;
    use crate::trigger_similarity::channel::final_similarity;
    use crate::trigger_similarity::channel::market_category_features;
    use crate::trigger_similarity::channel::scalar_dot_product;
    use crate::trigger_similarity::channel::share_environment_fingerprints;
    use crate::trigger_similarity::load::load_all_trade_dates;
    use crate::trigger_similarity::load::load_market_environment;
    use crate::trigger_similarity::load::load_market_schema;
    use crate::trigger_similarity::load::open_result_conn;
    use crate::trigger_similarity::ranking::samples::build_ranking_samples_for_chunk;
    use crate::trigger_similarity::ranking::samples::load_outcome_selected_anchors;
    use crate::trigger_similarity::sample::load_benchmark_rows;
    use lianghua_app_shared::build_name_map;
    use lianghua_app_shared::build_total_mv_map;
    use std::collections::HashMap;
    use std::sync::Arc;

    #[test]
    fn cached_channels_preserve_missing_zero_and_mismatched_vector_scores() {
        let channels = vec![
            None,
            Some(vec![]),
            Some(vec![0.0; 3]),
            Some(vec![1.0, -2.0, 3.0]),
            Some(vec![-1.0, 2.0, -3.0]),
            Some(vec![1.0, 2.0]),
        ];
        for left in &channels {
            for right in &channels {
                let target = crate::trigger_similarity::channel::build_channel_fingerprint(vec![
                    left.clone(),
                    None,
                    Some(vec![0.0; 3]),
                ]);
                let candidate =
                    crate::trigger_similarity::channel::build_channel_fingerprint(vec![
                        right.clone(),
                        Some(vec![1.0]),
                        Some(vec![1.0; 3]),
                    ]);
                let expected = match (left, right) {
                    (Some(a), Some(b)) => Some((cosine_similarity(a, b) + 0.0) / 2.0),
                    _ => Some(0.0),
                };
                assert_eq!(
                    crate::trigger_similarity::channel::cached_channel_similarity(
                        &target, &candidate
                    ),
                    expected
                );
            }
        }
        let missing = crate::trigger_similarity::channel::build_channel_fingerprint(vec![None]);
        let present =
            crate::trigger_similarity::channel::build_channel_fingerprint(vec![Some(vec![1.0])]);
        assert_eq!(
            crate::trigger_similarity::channel::cached_channel_similarity(&missing, &present),
            None
        );
    }

    #[test]
    fn vectorized_dot_product_keeps_f64_accuracy() {
        for len in 0..=65 {
            let left = (0..len)
                .map(|index| (index as f64 * 0.137).sin() * 1_000.0)
                .collect::<Vec<_>>();
            let right = (0..len)
                .map(|index| (index as f64 * 0.193).cos() / 7.0)
                .collect::<Vec<_>>();
            let scalar = scalar_dot_product(&left, &right);
            let vectorized = dot_product(&left, &right);
            let tolerance = 1e-12 * scalar.abs().max(1.0);
            assert!((vectorized - scalar).abs() <= tolerance);
        }
    }

    #[test]
    fn price_volume_channels_include_gap_and_wick_details() {
        let row =
            |trade_date: &str, open: f64, high: f64, low: f64, close: f64| -> MarketObservation {
                MarketObservation {
                    trade_date: trade_date.to_string(),
                    open: Some(open),
                    high: Some(high),
                    low: Some(low),
                    close: Some(close),
                    pct_chg: Some(1.0),
                    vol: Some(100.0),
                    amount: Some(1_000.0),
                    turnover: Some(2.0),
                    net_flow: Some(10.0),
                    indicators: Vec::new(),
                }
            };
        let observations = [
            row("20240101", 10.0, 11.0, 9.0, 10.5),
            row("20240102", 12.0, 12.2, 10.8, 11.0),
            row("20240103", 11.0, 15.0, 10.8, 12.0),
        ];
        let channels = build_price_volume_channels(
            &observations,
            3,
            Some(100.0),
            market_category_features("688001.SH", Some("*ST 测试")),
        );

        let gap = channels[9].as_ref().expect("gap channel");
        assert_eq!(gap[0], 0.0);
        assert!((gap[1] - 14.285714285714286).abs() < 1e-12);
        assert!((gap[2] - 0.0).abs() < 1e-12);

        let upper_wick = channels[10].as_ref().expect("upper wick channel");
        assert!((upper_wick[1] - 0.14285714285714285).abs() < 1e-12);
        assert!((upper_wick[2] - 0.7142857142857143).abs() < 1e-12);

        let lower_wick = channels[11].as_ref().expect("lower wick channel");
        assert!((lower_wick[1] - 0.14285714285714285).abs() < 1e-12);
        assert!((lower_wick[2] - 0.047619047619047616).abs() < 1e-12);

        let market_cap = channels[12].as_ref().expect("market cap channel");
        assert!(market_cap.iter().any(|value| *value > 0.0));
        let large_market_cap = build_price_volume_channels(
            &observations,
            3,
            Some(10_000.0),
            market_category_features("688001.SH", Some("*ST 测试")),
        )[12]
            .clone()
            .expect("large market cap channel");
        assert!(cosine_similarity(market_cap, &large_market_cap) < 100.0);
        assert!(
            build_price_volume_channels(
                &observations,
                3,
                None,
                market_category_features("688001.SH", Some("*ST 测试")),
            )[12]
                .is_none()
        );
        assert_eq!(channels.len(), 14);
        assert_eq!(
            channels[13].as_deref(),
            Some([0.0, 0.0, 0.0, 0.0, 1.0].as_slice())
        );
        let main_board = market_category_features("600000.SH", Some("浦发银行"));
        let growth_board = market_category_features("300001.SZ", Some("特锐德"));
        assert_eq!(cosine_similarity(&main_board, &main_board), 100.0);
        assert_eq!(cosine_similarity(&main_board, &growth_board), 50.0);
    }

    #[test]
    fn cosine_similarity_is_percentage_scaled() {
        assert!((cosine_similarity(&[1.0, 0.0], &[1.0, 0.0]) - 100.0).abs() < 1e-9);
        assert!(cosine_similarity(&[1.0, 0.0], &[-1.0, 0.0]).abs() < 1e-9);
        assert!((cosine_similarity(&[1.0, 0.0], &[0.0, 1.0]) - 50.0).abs() < 1e-9);
    }

    #[test]
    fn calibrated_similarity_weights_sum_to_one_and_renormalize_missing_channels() {
        let weight_sum = TRIGGER_SIMILARITY_WEIGHT
            + PRICE_VOLUME_SIMILARITY_WEIGHT
            + INDICATOR_SIMILARITY_WEIGHT
            + MARKET_SIMILARITY_WEIGHT;
        assert!((weight_sum - 1.0).abs() < 1e-12);
        let expected = 100.0 * TRIGGER_SIMILARITY_WEIGHT
            / (TRIGGER_SIMILARITY_WEIGHT + PRICE_VOLUME_SIMILARITY_WEIGHT);
        assert!((final_similarity(100.0, Some(0.0), None, None) - expected).abs() < 1e-9);
    }

    #[test]
    fn channel_fingerprint_supports_more_than_sixty_four_indicators() {
        let target =
            build_channel_fingerprint((0..80).map(|index| Some(vec![index as f64])).collect());
        let candidate =
            build_channel_fingerprint((0..80).map(|index| Some(vec![index as f64])).collect());
        assert!(target.has_vectors);
        assert!(candidate.has_vectors);
        assert!(
            (cached_channel_similarity(&target, &candidate).expect("similarity") - 100.0).abs()
                < 1e-9
        );
    }

    #[test]
    fn market_fingerprints_are_shared_by_trade_date() {
        let shared = share_environment_fingerprints(HashMap::from([(
            "20240102".to_string(),
            vec![Some(vec![1.0, 2.0, 3.0])],
        )]));
        let first = shared.get("20240102").expect("market fingerprint");
        let second = Arc::clone(first);
        assert!(Arc::ptr_eq(first, &second));
        assert_eq!(first.normalized_values, second.normalized_values);
    }

    #[test]
    fn packed_channel_similarity_preserves_missing_and_zero_channel_semantics() {
        let target =
            build_channel_fingerprint(vec![Some(vec![3.0, 4.0]), Some(vec![0.0, 0.0]), None]);
        let candidate = build_channel_fingerprint(vec![
            Some(vec![4.0, 3.0]),
            Some(vec![0.0, 0.0]),
            Some(vec![1.0, 2.0]),
        ]);
        let expected = (98.0 + 100.0) / 2.0;
        assert!(
            (cached_channel_similarity(&target, &candidate).expect("similarity") - expected).abs()
                < 1e-9
        );
    }

    #[test]
    #[ignore = "requires LIANGHUA_BENCH_DATA_DIR and a real dataset"]
    fn benchmark_real_candidate_fingerprints() {
        let source_path = std::env::var("LIANGHUA_BENCH_DATA_DIR")
            .expect("set LIANGHUA_BENCH_DATA_DIR to a real source directory");
        let conn = open_result_conn(&source_path).expect("open real source databases");
        let all_trade_dates = load_all_trade_dates(&conn).expect("load scoring calendar");
        let target_date = all_trade_dates.last().expect("target date").clone();
        let horizon = 5;
        let cutoff_date = all_trade_dates[all_trade_dates.len() - 1 - horizon].clone();
        let earliest_date = all_trade_dates[19].clone();
        let schema = load_market_schema(&conn).expect("load market schema");
        let environment =
            load_market_environment(&conn, &all_trade_dates[0], &target_date, &schema)
                .expect("load environment");
        let environment_fingerprints = share_environment_fingerprints(
            build_environment_fingerprint_map(&environment, &all_trade_dates, 20, 5),
        );
        let benchmark_rows =
            load_benchmark_rows(&conn, &all_trade_dates[0], &target_date, "000001.SH")
                .expect("load benchmark");
        let total_mv_map = build_total_mv_map(&source_path).unwrap_or_default();
        let name_map = build_name_map(&source_path).unwrap_or_default();
        let (selected, _) = load_outcome_selected_anchors(
            &conn,
            &earliest_date,
            &cutoff_date,
            &target_date,
            &all_trade_dates,
            20,
            horizon,
            &benchmark_rows,
            &environment_fingerprints,
            environment_fingerprints.get(&target_date).map(Arc::as_ref),
            MIN_SAMPLE_GAP_TRADE_DAYS,
        )
        .expect("load selected anchors");
        let anchors = selected
            .into_iter()
            .map(|selected| selected.anchor)
            .collect::<Vec<_>>();
        let anchor_count = anchors.len();
        let started = std::time::Instant::now();
        let mut built = 0;
        let mut rule_catalog = crate::trigger_similarity::RuleCatalog::default();
        let mut anchor_iter = anchors.into_iter();
        loop {
            let chunk = anchor_iter
                .by_ref()
                .take(ANCHOR_CHUNK_SIZE)
                .collect::<Vec<_>>();
            if chunk.is_empty() {
                break;
            }
            built += build_ranking_samples_for_chunk(
                &conn,
                chunk,
                &schema,
                &all_trade_dates,
                &environment_fingerprints,
                &benchmark_rows,
                &total_mv_map,
                &name_map,
                5,
                horizon,
                &target_date,
                true,
                None,
                &mut rule_catalog,
            )
            .expect("build candidate fingerprints")
            .len();
            eprintln!(
                "candidate fingerprints: elapsed={:?}, built={built}/{}",
                started.elapsed(),
                anchor_count
            );
        }
        assert!(built > 0);
    }
}
