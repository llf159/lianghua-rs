use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use rayon::prelude::*;
use serde::{Deserialize, Serialize};

use crate::{
    crawler::SinaQuote,
    data::{DataReader, RowData, load_trade_date_list},
    download::ind_calc::warmup_ind_estimate,
    scoring::tools::{calc_zhang_pct, load_st_list, warmup_rows_estimate},
    simulate::{
        SimulationBatchInput, load_simulation_caches, run_simulated_scoring_batch_with_cache,
    },
    ui_tools::{
        build_latest_vol_map,
        overview::{OverviewRow, get_rank_overview},
        realtime::{RealtimeFetchMeta, fetch_realtime_quote_map},
    },
};

const DEFAULT_ADJ_TYPE: &str = "qfq";
const FLAT_PCT_TOLERANCE: f64 = 1.0;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MarketSimulationScenarioInput {
    pub id: String,
    pub label: String,
    pub pct_chg: f64,
    pub volume_ratio: f64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MarketSimulationTriggeredRule {
    pub rule_name: String,
    pub rule_score: f64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MarketSimulationRow {
    pub ts_code: String,
    pub name: String,
    pub concept: String,
    pub reference_rank: Option<i64>,
    pub base_total_score: Option<f64>,
    pub simulated_total_score: f64,
    pub score_delta: f64,
    pub strong_hold: bool,
    pub latest_price: Option<f64>,
    pub latest_change_pct: Option<f64>,
    pub volume_ratio: Option<f64>,
    pub realtime_matched: bool,
    pub triggered_rules: Vec<MarketSimulationTriggeredRule>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MarketSimulationScenarioResult {
    pub id: String,
    pub label: String,
    pub pct_chg: f64,
    pub volume_ratio: f64,
    pub rows: Vec<MarketSimulationRow>,
    pub matched_count: usize,
    pub strong_hold_count: usize,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MarketSimulationPageData {
    pub scenarios: Vec<MarketSimulationScenarioResult>,
    pub requested_count: usize,
    pub effective_count: usize,
    pub fetched_count: usize,
    pub truncated: bool,
    pub refreshed_at: Option<String>,
    pub reference_trade_date: Option<String>,
    pub simulated_trade_date: Option<String>,
    pub sort_mode: String,
    pub strong_score_floor: Option<f64>,
    pub candidate_count: usize,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MarketSimulationRealtimeScenarioInput {
    pub id: String,
    pub pct_chg: f64,
    pub ts_codes: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MarketSimulationRealtimeRowData {
    pub ts_code: String,
    pub latest_price: Option<f64>,
    pub latest_change_pct: Option<f64>,
    pub volume_ratio: Option<f64>,
    pub realtime_matched: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MarketSimulationRealtimeScenarioResult {
    pub id: String,
    pub rows: Vec<MarketSimulationRealtimeRowData>,
    pub matched_count: usize,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MarketSimulationRealtimeRefreshData {
    pub scenarios: Vec<MarketSimulationRealtimeScenarioResult>,
    pub requested_count: usize,
    pub effective_count: usize,
    pub fetched_count: usize,
    pub truncated: bool,
    pub refreshed_at: Option<String>,
    pub quote_trade_date: Option<String>,
    pub quote_time: Option<String>,
}

fn validate_scenarios(
    scenarios: Vec<MarketSimulationScenarioInput>,
) -> Result<Vec<MarketSimulationScenarioInput>, String> {
    if scenarios.is_empty() {
        return Err("至少需要一个模拟场景".to_string());
    }
    if scenarios.len() > 5 {
        return Err("模拟场景最多支持 5 个".to_string());
    }

    let mut out = Vec::with_capacity(scenarios.len());
    for (index, scenario) in scenarios.into_iter().enumerate() {
        let id = scenario.id.trim().to_string();
        let label = scenario.label.trim().to_string();
        if id.is_empty() {
            return Err(format!("第 {} 个模拟场景缺少 id", index + 1));
        }
        if label.is_empty() {
            return Err(format!("第 {} 个模拟场景缺少名称", index + 1));
        }
        if !scenario.pct_chg.is_finite() {
            return Err(format!("第 {} 个模拟场景涨幅非法", index + 1));
        }
        if !scenario.volume_ratio.is_finite() || scenario.volume_ratio < 0.0 {
            return Err(format!("第 {} 个模拟场景量比非法", index + 1));
        }

        out.push(MarketSimulationScenarioInput {
            id,
            label,
            pct_chg: scenario.pct_chg,
            volume_ratio: scenario.volume_ratio,
        });
    }

    Ok(out)
}

fn validate_realtime_refresh_scenarios(
    scenarios: Vec<MarketSimulationRealtimeScenarioInput>,
) -> Result<Vec<MarketSimulationRealtimeScenarioInput>, String> {
    if scenarios.is_empty() {
        return Err("至少需要一个实时刷新场景".to_string());
    }

    let mut out = Vec::with_capacity(scenarios.len());
    for (index, scenario) in scenarios.into_iter().enumerate() {
        let id = scenario.id.trim().to_string();
        if id.is_empty() {
            return Err(format!("第 {} 个实时刷新场景缺少 id", index + 1));
        }
        if !scenario.pct_chg.is_finite() {
            return Err(format!("第 {} 个实时刷新场景涨幅非法", index + 1));
        }

        let ts_codes = scenario
            .ts_codes
            .into_iter()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .collect::<Vec<_>>();

        out.push(MarketSimulationRealtimeScenarioInput {
            id,
            pct_chg: scenario.pct_chg,
            ts_codes,
        });
    }

    Ok(out)
}

fn empty_realtime_fetch_meta() -> RealtimeFetchMeta {
    RealtimeFetchMeta {
        requested_count: 0,
        effective_count: 0,
        fetched_count: 0,
        truncated: false,
        refreshed_at: None,
        quote_trade_date: None,
        quote_time: None,
    }
}

fn resolve_sort_mode(sort_mode: Option<String>) -> String {
    match sort_mode
        .as_deref()
        .map(str::trim)
        .unwrap_or("sim_score")
        .to_ascii_lowercase()
        .as_str()
    {
        "score_delta" => "score_delta".to_string(),
        _ => "sim_score".to_string(),
    }
}

fn resolve_simulated_trade_date(
    source_path: &str,
    reference_trade_date: &str,
) -> Result<String, String> {
    let trade_dates = load_trade_date_list(source_path)?;
    let next_index =
        match trade_dates.binary_search_by(|item| item.as_str().cmp(reference_trade_date)) {
            Ok(index) => index + 1,
            Err(index) => index,
        };

    if let Some(next_trade_date) = trade_dates.get(next_index) {
        Ok(next_trade_date.clone())
    } else {
        Ok(format!("{reference_trade_date}_SIM"))
    }
}

fn realtime_matches_by_price_only(latest_change_pct: Option<f64>, target_pct_chg: f64) -> bool {
    let Some(actual_change_pct) = latest_change_pct.filter(|value| value.is_finite()) else {
        return false;
    };

    if target_pct_chg > FLAT_PCT_TOLERANCE {
        actual_change_pct >= target_pct_chg
    } else if target_pct_chg < -FLAT_PCT_TOLERANCE {
        actual_change_pct <= target_pct_chg
    } else {
        (actual_change_pct - target_pct_chg).abs() <= FLAT_PCT_TOLERANCE
    }
}

fn fill_simulation_extra_fields(
    row_data: &mut RowData,
    ts_code: &str,
    is_st: bool,
) -> Result<(), String> {
    let zhang = calc_zhang_pct(ts_code, is_st);
    row_data.cols.insert(
        "ZHANG".to_string(),
        vec![Some(zhang); row_data.trade_dates.len()],
    );
    row_data.validate()
}

fn load_candidate_rows(
    source_path: &str,
    reference_trade_date: &str,
    overview_rows: &[OverviewRow],
) -> Result<Vec<SimulationBatchInput>, String> {
    if overview_rows.is_empty() {
        return Ok(Vec::new());
    }

    let st_list = load_st_list(source_path)?;
    let need_rows = warmup_rows_estimate(source_path)?
        .max(warmup_ind_estimate(source_path)?)
        .saturating_add(1);

    overview_rows
        .par_chunks(32)
        .map(|chunk| -> Result<Vec<SimulationBatchInput>, String> {
            let reader = DataReader::new(source_path)?;
            let mut out = Vec::with_capacity(chunk.len());

            for row in chunk {
                let mut row_data = reader.load_one_tail_rows(
                    &row.ts_code,
                    DEFAULT_ADJ_TYPE,
                    reference_trade_date,
                    need_rows,
                )?;
                fill_simulation_extra_fields(
                    &mut row_data,
                    &row.ts_code,
                    st_list.contains(&row.ts_code),
                )?;
                out.push(SimulationBatchInput {
                    ts_code: row.ts_code.clone(),
                    row_data,
                });
            }

            Ok(out)
        })
        .collect::<Result<Vec<_>, _>>()
        .map(|groups| groups.into_iter().flatten().collect())
}

fn sort_simulation_rows(
    rows: &mut [MarketSimulationRow],
    sort_mode: &str,
    strong_score_floor: Option<f64>,
) {
    rows.sort_by(|left, right| {
        let left_strong = strong_score_floor
            .map(|floor| {
                left.base_total_score.unwrap_or(f64::NEG_INFINITY) >= floor
                    && left.simulated_total_score >= floor
            })
            .unwrap_or(left.strong_hold);
        let right_strong = strong_score_floor
            .map(|floor| {
                right.base_total_score.unwrap_or(f64::NEG_INFINITY) >= floor
                    && right.simulated_total_score >= floor
            })
            .unwrap_or(right.strong_hold);

        right_strong
            .cmp(&left_strong)
            .then_with(|| {
                if sort_mode == "score_delta" {
                    right
                        .score_delta
                        .partial_cmp(&left.score_delta)
                        .unwrap_or(Ordering::Equal)
                        .then_with(|| {
                            right
                                .simulated_total_score
                                .partial_cmp(&left.simulated_total_score)
                                .unwrap_or(Ordering::Equal)
                        })
                } else {
                    right
                        .simulated_total_score
                        .partial_cmp(&left.simulated_total_score)
                        .unwrap_or(Ordering::Equal)
                        .then_with(|| {
                            right
                                .score_delta
                                .partial_cmp(&left.score_delta)
                                .unwrap_or(Ordering::Equal)
                        })
                }
            })
            .then_with(|| match (left.reference_rank, right.reference_rank) {
                (Some(lv), Some(rv)) => lv.cmp(&rv),
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                (None, None) => left.ts_code.cmp(&right.ts_code),
            })
    });
}

pub fn build_market_simulation_page_from_rows(
    source_path: &str,
    overview_rows: Vec<OverviewRow>,
    quote_map: HashMap<String, SinaQuote>,
    fetch_meta: RealtimeFetchMeta,
    scenarios: Vec<MarketSimulationScenarioInput>,
    sort_mode: Option<String>,
    strong_score_floor: Option<f64>,
) -> Result<MarketSimulationPageData, String> {
    let scenarios = validate_scenarios(scenarios)?;
    let sort_mode = resolve_sort_mode(sort_mode);
    let reference_trade_date = overview_rows.first().and_then(|row| row.trade_date.clone());
    let Some(reference_trade_date_value) = reference_trade_date.clone() else {
        return Ok(MarketSimulationPageData {
            scenarios: Vec::new(),
            requested_count: fetch_meta.requested_count,
            effective_count: fetch_meta.effective_count,
            fetched_count: fetch_meta.fetched_count,
            truncated: fetch_meta.truncated,
            refreshed_at: fetch_meta.refreshed_at,
            reference_trade_date: None,
            simulated_trade_date: None,
            sort_mode,
            strong_score_floor,
            candidate_count: 0,
        });
    };
    let simulated_trade_date =
        resolve_simulated_trade_date(source_path, &reference_trade_date_value)?;
    let latest_vol_map = if quote_map.is_empty() {
        HashMap::new()
    } else {
        build_latest_vol_map(
            source_path,
            &overview_rows
                .iter()
                .map(|row| row.ts_code.clone())
                .collect::<Vec<_>>(),
        )?
    };
    let candidate_rows =
        load_candidate_rows(source_path, &reference_trade_date_value, &overview_rows)?;
    let overview_map: HashMap<String, OverviewRow> = overview_rows
        .into_iter()
        .map(|row| (row.ts_code.clone(), row))
        .collect();
    let caches = load_simulation_caches(source_path)?;

    let mut scenario_results = Vec::with_capacity(scenarios.len());
    for scenario in scenarios {
        let scoring_results = run_simulated_scoring_batch_with_cache(
            candidate_rows
                .iter()
                .map(|item| SimulationBatchInput {
                    ts_code: item.ts_code.clone(),
                    row_data: item.row_data.clone(),
                })
                .collect(),
            &[crate::data::simulate::SimulateBarInput {
                trade_date: Some(simulated_trade_date.clone()),
                pct_chg: scenario.pct_chg,
                volume_ratio: scenario.volume_ratio,
            }],
            &caches,
        )?;

        let mut rows = Vec::with_capacity(scoring_results.len());
        for result in scoring_results {
            let Some(overview_row) = overview_map.get(&result.ts_code) else {
                continue;
            };
            let Some(day) = result.scoring.days.last() else {
                continue;
            };
            let quote = quote_map.get(&result.ts_code);
            let realtime_volume_ratio = match (
                quote.map(|item| item.vol),
                latest_vol_map.get(&result.ts_code).copied(),
            ) {
                (Some(current_vol), Some(previous_vol)) if previous_vol > 0.0 => {
                    Some(current_vol / previous_vol)
                }
                _ => None,
            };
            let base_total_score = overview_row.total_score;
            let simulated_total_score = day.total_score;
            let score_delta = simulated_total_score - base_total_score.unwrap_or(0.0);
            let strong_hold = strong_score_floor
                .map(|floor| {
                    base_total_score.unwrap_or(f64::NEG_INFINITY) >= floor
                        && simulated_total_score >= floor
                })
                .unwrap_or(false);
            let latest_change_pct = quote.and_then(|item| item.change_pct);

            rows.push(MarketSimulationRow {
                ts_code: result.ts_code.clone(),
                name: overview_row.name.clone(),
                concept: overview_row.concept.clone(),
                reference_rank: overview_row.rank,
                base_total_score,
                simulated_total_score,
                score_delta,
                strong_hold,
                latest_price: quote.map(|item| item.price),
                latest_change_pct,
                volume_ratio: realtime_volume_ratio,
                realtime_matched: realtime_matches_by_price_only(
                    latest_change_pct,
                    scenario.pct_chg,
                ),
                triggered_rules: day
                    .rule_scores
                    .iter()
                    .filter(|item| item.triggered)
                    .map(|item| MarketSimulationTriggeredRule {
                        rule_name: item.rule_name.clone(),
                        rule_score: item.rule_score,
                    })
                    .collect(),
            });
        }

        sort_simulation_rows(&mut rows, &sort_mode, strong_score_floor);
        let matched_count = rows.iter().filter(|row| row.realtime_matched).count();
        let strong_hold_count = rows.iter().filter(|row| row.strong_hold).count();

        scenario_results.push(MarketSimulationScenarioResult {
            id: scenario.id,
            label: scenario.label,
            pct_chg: scenario.pct_chg,
            volume_ratio: scenario.volume_ratio,
            rows,
            matched_count,
            strong_hold_count,
        });
    }

    Ok(MarketSimulationPageData {
        scenarios: scenario_results,
        requested_count: fetch_meta.requested_count,
        effective_count: fetch_meta.effective_count,
        fetched_count: fetch_meta.fetched_count,
        truncated: fetch_meta.truncated,
        refreshed_at: fetch_meta.refreshed_at,
        reference_trade_date: Some(reference_trade_date_value),
        simulated_trade_date: Some(simulated_trade_date),
        sort_mode,
        strong_score_floor,
        candidate_count: candidate_rows.len(),
    })
}

pub fn get_market_simulation_page(
    source_path: String,
    reference_trade_date: Option<String>,
    top_limit: Option<u32>,
    scenarios: Vec<MarketSimulationScenarioInput>,
    sort_mode: Option<String>,
    strong_score_floor: Option<f64>,
    fetch_realtime: Option<bool>,
) -> Result<MarketSimulationPageData, String> {
    let limit = top_limit.unwrap_or(50).max(1);
    let overview_rows = get_rank_overview(
        source_path.clone(),
        reference_trade_date,
        Some(limit),
        None,
        None,
        None,
    )?;
    let ts_codes: Vec<String> = overview_rows
        .iter()
        .map(|row| row.ts_code.clone())
        .collect();
    let (quote_map, fetch_meta) = if fetch_realtime.unwrap_or(false) {
        fetch_realtime_quote_map(&ts_codes)?
    } else {
        (HashMap::new(), empty_realtime_fetch_meta())
    };
    build_market_simulation_page_from_rows(
        &source_path,
        overview_rows,
        quote_map,
        fetch_meta,
        scenarios,
        sort_mode,
        strong_score_floor,
    )
}

pub fn refresh_market_simulation_realtime(
    source_path: String,
    scenarios: Vec<MarketSimulationRealtimeScenarioInput>,
) -> Result<MarketSimulationRealtimeRefreshData, String> {
    let scenarios = validate_realtime_refresh_scenarios(scenarios)?;
    let mut seen = HashSet::new();
    let mut ts_codes = Vec::new();
    for scenario in &scenarios {
        for ts_code in &scenario.ts_codes {
            if seen.insert(ts_code.clone()) {
                ts_codes.push(ts_code.clone());
            }
        }
    }

    let (quote_map, fetch_meta) = fetch_realtime_quote_map(&ts_codes)?;
    let latest_vol_map = if ts_codes.is_empty() {
        HashMap::new()
    } else {
        build_latest_vol_map(&source_path, &ts_codes)?
    };

    let scenarios = scenarios
        .into_iter()
        .map(|scenario| {
            let mut rows = Vec::with_capacity(scenario.ts_codes.len());
            for ts_code in scenario.ts_codes {
                let quote = quote_map.get(&ts_code);
                let latest_change_pct = quote.and_then(|item| item.change_pct);
                let volume_ratio = match (
                    quote.map(|item| item.vol),
                    latest_vol_map.get(&ts_code).copied(),
                ) {
                    (Some(current_vol), Some(previous_vol)) if previous_vol > 0.0 => {
                        Some(current_vol / previous_vol)
                    }
                    _ => None,
                };
                rows.push(MarketSimulationRealtimeRowData {
                    ts_code,
                    latest_price: quote.map(|item| item.price),
                    latest_change_pct,
                    volume_ratio,
                    realtime_matched: realtime_matches_by_price_only(
                        latest_change_pct,
                        scenario.pct_chg,
                    ),
                });
            }
            let matched_count = rows.iter().filter(|row| row.realtime_matched).count();
            MarketSimulationRealtimeScenarioResult {
                id: scenario.id,
                rows,
                matched_count,
            }
        })
        .collect();

    Ok(MarketSimulationRealtimeRefreshData {
        scenarios,
        requested_count: fetch_meta.requested_count,
        effective_count: fetch_meta.effective_count,
        fetched_count: fetch_meta.fetched_count,
        truncated: fetch_meta.truncated,
        refreshed_at: fetch_meta.refreshed_at,
        quote_trade_date: fetch_meta.quote_trade_date,
        quote_time: fetch_meta.quote_time,
    })
}
