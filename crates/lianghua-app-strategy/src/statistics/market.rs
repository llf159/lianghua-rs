use crate::data::concept_performance_db_path;
use crate::data::load_stock_list;
use crate::data::load_ths_concepts_list;
use crate::data::source_db_path;
use crate::simulate::DEFAULT_BACKTEST_MIN_LISTED_TRADE_DAYS;
use crate::simulate::build_backtest_sample_eligibility;
use crate::statistics::universe::{
    get_or_build_board_maps, match_board_filter_with_st, resolve_board_filter, split_board_tags,
};
use duckdb::Connection;
use duckdb::params;
use serde::Serialize;
use std::collections::HashMap;
use std::collections::HashSet;
#[derive(Debug, Serialize)]
pub struct MarketRankItem {
    pub name: String,
    pub value: f64,
    pub ts_code: Option<String>,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub concepts: Option<String>,
    pub three_day_gain: Option<f64>,
    pub five_day_gain: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct MarketAnalysisSnapshot {
    pub trade_date: Option<String>,
    pub concept_top: Vec<MarketRankItem>,
    pub industry_top: Vec<MarketRankItem>,
    pub concept_money_flow_top: Vec<MarketRankItem>,
    pub industry_money_flow_top: Vec<MarketRankItem>,
    pub concept_money_outflow_top: Vec<MarketRankItem>,
    pub industry_money_outflow_top: Vec<MarketRankItem>,
    pub gain_top: Vec<MarketRankItem>,
    pub sub_interval_gain_top: Vec<MarketRankItem>,
}

#[derive(Debug, Serialize)]
pub struct MarketAnalysisData {
    pub lookback_period: usize,
    pub stock_rank_limit: usize,
    pub sub_interval_period: usize,
    pub min_board_stock_count: usize,
    pub latest_trade_date: Option<String>,
    pub resolved_reference_trade_date: Option<String>,
    pub board_options: Vec<String>,
    pub resolved_board: Option<String>,
    pub interval: MarketAnalysisSnapshot,
    pub daily: MarketAnalysisSnapshot,
}

#[derive(Debug, Serialize)]
pub struct MarketContributorItem {
    pub ts_code: String,
    pub name: Option<String>,
    pub industry: Option<String>,
    pub contribution_pct: f64,
}

pub(in crate::statistics) fn market_rank_item(name: String, value: f64) -> MarketRankItem {
    MarketRankItem {
        name,
        value,
        ts_code: None,
        start_date: None,
        end_date: None,
        concepts: None,
        three_day_gain: None,
        five_day_gain: None,
    }
}

pub(in crate::statistics) fn market_stock_rank_item(
    stock_name_map: &HashMap<String, String>,
    ts_code: String,
    value: f64,
    start_date: Option<String>,
    end_date: Option<String>,
) -> MarketRankItem {
    let name = stock_name_map
        .get(&ts_code)
        .cloned()
        .unwrap_or_else(|| ts_code.clone());
    MarketRankItem {
        name: format!("{} ({})", name, ts_code),
        value,
        ts_code: Some(ts_code),
        start_date,
        end_date,
        concepts: None,
        three_day_gain: None,
        five_day_gain: None,
    }
}

pub(in crate::statistics) fn trailing_period_gain(
    rows: &[(String, f64)],
    period: usize,
) -> Option<f64> {
    if period == 0 || rows.len() <= period {
        return None;
    }
    let start_close = rows.get(rows.len() - period - 1)?.1;
    let end_close = rows.last()?.1;
    if !start_close.is_finite() || !end_close.is_finite() || start_close <= f64::EPSILON {
        return None;
    }
    let value = (end_close / start_close - 1.0) * 100.0;
    value.is_finite().then_some(value)
}

#[derive(Debug, Serialize)]
pub struct MarketContributionData {
    pub scope: String,
    pub kind: String,
    pub name: String,
    pub trade_date: Option<String>,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub lookback_period: usize,
    pub contributors: Vec<MarketContributorItem>,
}

pub(in crate::statistics) fn build_industry_maps_from_rows(
    stock_rows: Vec<Vec<String>>,
) -> (HashMap<String, Vec<String>>, HashMap<String, usize>) {
    let mut ts_industry_map: HashMap<String, Vec<String>> =
        HashMap::with_capacity(stock_rows.len());
    let mut industry_stocks: HashMap<String, HashSet<String>> = HashMap::new();

    for cols in stock_rows {
        let Some(ts_code) = cols
            .first()
            .map(|value| value.trim().to_ascii_uppercase())
            .filter(|value| !value.is_empty())
        else {
            continue;
        };
        let Some(industry_raw) = cols.get(4).map(|value| value.trim()) else {
            continue;
        };
        if industry_raw.is_empty() {
            continue;
        }

        let industries = split_board_tags(industry_raw);
        for industry in &industries {
            industry_stocks
                .entry(industry.clone())
                .or_default()
                .insert(ts_code.clone());
        }
        if !industries.is_empty() {
            ts_industry_map.insert(ts_code, industries);
        }
    }

    let industry_stock_counts = industry_stocks
        .into_iter()
        .map(|(industry, stocks)| (industry, stocks.len()))
        .collect();
    (ts_industry_map, industry_stock_counts)
}

pub(in crate::statistics) fn has_min_stock_count(
    stock_counts: &HashMap<String, usize>,
    name: &str,
    min_stock_count: usize,
) -> bool {
    min_stock_count <= 1 || stock_counts.get(name).copied().unwrap_or(0) >= min_stock_count
}

pub(in crate::statistics) fn estimate_net_money_flow_yuan(
    net_mf_vol: f64,
    vol: f64,
    amount: f64,
) -> Option<f64> {
    if !net_mf_vol.is_finite()
        || !vol.is_finite()
        || !amount.is_finite()
        || vol <= f64::EPSILON
        || amount < 0.0
    {
        return None;
    }

    let value = net_mf_vol / vol * amount * 1_000.0;
    value.is_finite().then_some(value)
}

pub(in crate::statistics) fn accumulate_board_money_flow(
    acc: &mut HashMap<String, f64>,
    board_map: &HashMap<String, Vec<String>>,
    ts_code: &str,
    net_amount_yuan: f64,
) {
    let Some(boards) = board_map.get(ts_code) else {
        return;
    };
    for board in boards {
        *acc.entry(board.clone()).or_insert(0.0) += net_amount_yuan;
    }
}

pub(in crate::statistics) fn money_flow_rank_items(
    acc: HashMap<String, f64>,
    stock_counts: &HashMap<String, usize>,
    min_stock_count: usize,
) -> Vec<MarketRankItem> {
    let mut items = acc
        .into_iter()
        .filter_map(|(name, value)| {
            if value <= 0.0
                || !value.is_finite()
                || !has_min_stock_count(stock_counts, &name, min_stock_count)
            {
                return None;
            }
            Some(market_rank_item(name, value))
        })
        .collect::<Vec<_>>();
    items.sort_by(|a, b| {
        b.value
            .partial_cmp(&a.value)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.name.cmp(&b.name))
    });
    items.truncate(20);
    items
}

pub(in crate::statistics) fn money_outflow_rank_items(
    acc: HashMap<String, f64>,
    stock_counts: &HashMap<String, usize>,
    min_stock_count: usize,
) -> Vec<MarketRankItem> {
    let mut items = acc
        .into_iter()
        .filter_map(|(name, value)| {
            if value >= 0.0
                || !value.is_finite()
                || !has_min_stock_count(stock_counts, &name, min_stock_count)
            {
                return None;
            }
            Some(market_rank_item(name, value))
        })
        .collect::<Vec<_>>();
    items.sort_by(|a, b| {
        a.value
            .partial_cmp(&b.value)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.name.cmp(&b.name))
    });
    items.truncate(20);
    items
}

pub fn get_market_analysis(
    source_path: String,
    lookback_period: Option<usize>,
    reference_trade_date: Option<String>,
    board: Option<String>,
    exclude_st_board: Option<bool>,
    min_listed_trade_days: Option<usize>,
    stock_rank_limit: Option<usize>,
    sub_interval_period: Option<usize>,
    min_board_stock_count: Option<usize>,
) -> Result<MarketAnalysisData, String> {
    let lookback_period = lookback_period.unwrap_or(20).max(1);
    let stock_rank_limit = stock_rank_limit.unwrap_or(20).clamp(1, 200);
    let sub_interval_period = if lookback_period >= 3 {
        sub_interval_period.unwrap_or(3).max(3).min(lookback_period)
    } else {
        3
    };
    let min_listed_trade_days =
        min_listed_trade_days.unwrap_or(DEFAULT_BACKTEST_MIN_LISTED_TRADE_DAYS);
    let min_board_stock_count = min_board_stock_count.unwrap_or(1).max(1);

    let source_db = source_db_path(&source_path);
    let source_db_str = source_db
        .to_str()
        .ok_or_else(|| "原始库路径不是有效UTF-8".to_string())?;
    let source_conn =
        Connection::open(source_db_str).map_err(|e| format!("打开原始库失败: {e}"))?;

    let latest_trade_date: Option<String> = source_conn
        .query_row(
            "SELECT MAX(trade_date) FROM stock_data WHERE adj_type = 'qfq'",
            [],
            |row| row.get(0),
        )
        .map_err(|e| format!("查询最新交易日失败: {e}"))?;

    let resolved_reference_trade_date = reference_trade_date
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .or_else(|| latest_trade_date.clone());

    let (board_options, ts_board_map) = get_or_build_board_maps(&source_path)?;
    let (ts_concept_map, concept_stock_counts) = (|source_path: &str| -> Result<
        (HashMap<String, Vec<String>>, HashMap<String, usize>),
        String,
    > {
        let rows = match load_ths_concepts_list(source_path) {
            Ok(rows) => rows,
            Err(error) if error.contains("打开stock_concepts.csv失败") => {
                return Ok((HashMap::new(), HashMap::new()));
            }
            Err(error) => return Err(error),
        };
        let mut ts_concept_map: HashMap<String, Vec<String>> = HashMap::new();
        let mut concept_stocks: HashMap<String, HashSet<String>> = HashMap::new();

        for cols in rows {
            let Some(ts_code) = cols
                .first()
                .map(|value| value.trim().to_ascii_uppercase())
                .filter(|value| !value.is_empty())
            else {
                continue;
            };
            let Some(concept_raw) = cols.get(2).map(|value| value.trim()) else {
                continue;
            };
            if concept_raw.is_empty() {
                continue;
            }

            let concepts = split_board_tags(concept_raw);
            for concept in &concepts {
                concept_stocks
                    .entry(concept.clone())
                    .or_default()
                    .insert(ts_code.clone());
            }
            if !concepts.is_empty() {
                ts_concept_map.entry(ts_code).or_default().extend(concepts);
            }
        }

        for concepts in ts_concept_map.values_mut() {
            concepts.sort();
            concepts.dedup();
        }
        let concept_stock_counts = concept_stocks
            .into_iter()
            .map(|(concept, stocks)| (concept, stocks.len()))
            .collect();
        Ok((ts_concept_map, concept_stock_counts))
    })(&source_path)?;
    let (ts_industry_map, industry_stock_counts) = (|source_path: &str| -> Result<
        (HashMap<String, Vec<String>>, HashMap<String, usize>),
        String,
    > {
        let stock_rows = load_stock_list(source_path)?;
        Ok(build_industry_maps_from_rows(stock_rows))
    })(&source_path)?;
    let resolved_board = resolve_board_filter(board, &board_options);
    let exclude_st_board = exclude_st_board.unwrap_or(false);
    let sample_eligibility =
        build_backtest_sample_eligibility(&source_path, min_listed_trade_days)?;

    let Some(ref_date) = resolved_reference_trade_date.clone() else {
        return Ok(MarketAnalysisData {
            lookback_period,
            stock_rank_limit,
            sub_interval_period,
            min_board_stock_count,
            latest_trade_date,
            resolved_reference_trade_date: None,
            board_options,
            resolved_board,
            interval: MarketAnalysisSnapshot {
                trade_date: None,
                concept_top: Vec::new(),
                industry_top: Vec::new(),
                concept_money_flow_top: Vec::new(),
                industry_money_flow_top: Vec::new(),
                concept_money_outflow_top: Vec::new(),
                industry_money_outflow_top: Vec::new(),
                gain_top: Vec::new(),
                sub_interval_gain_top: Vec::new(),
            },
            daily: MarketAnalysisSnapshot {
                trade_date: None,
                concept_top: Vec::new(),
                industry_top: Vec::new(),
                concept_money_flow_top: Vec::new(),
                industry_money_flow_top: Vec::new(),
                concept_money_outflow_top: Vec::new(),
                industry_money_outflow_top: Vec::new(),
                gain_top: Vec::new(),
                sub_interval_gain_top: Vec::new(),
            },
        });
    };

    let mut date_stmt = source_conn
        .prepare(
            r#"
            SELECT trade_date
            FROM (
                SELECT DISTINCT trade_date
                FROM stock_data
                WHERE adj_type = 'qfq'
                  AND trade_date <= ?
                ORDER BY trade_date DESC
                LIMIT ?
            ) AS t
            ORDER BY trade_date ASC
            "#,
        )
        .map_err(|e| format!("预编译市场分析区间日期 SQL 失败: {e}"))?;
    let mut date_rows = date_stmt
        .query(params![&ref_date, lookback_period as i64])
        .map_err(|e| format!("执行市场分析区间日期 SQL 失败: {e}"))?;
    let mut dates = Vec::new();
    while let Some(row) = date_rows
        .next()
        .map_err(|e| format!("读取市场分析区间日期失败: {e}"))?
    {
        let trade_date: String = row.get(0).map_err(|e| format!("读取交易日失败: {e}"))?;
        dates.push(trade_date);
    }

    if dates.is_empty() {
        return Ok(MarketAnalysisData {
            lookback_period,
            stock_rank_limit,
            sub_interval_period,
            min_board_stock_count,
            latest_trade_date,
            resolved_reference_trade_date: Some(ref_date.clone()),
            board_options,
            resolved_board,
            interval: MarketAnalysisSnapshot {
                trade_date: None,
                concept_top: Vec::new(),
                industry_top: Vec::new(),
                concept_money_flow_top: Vec::new(),
                industry_money_flow_top: Vec::new(),
                concept_money_outflow_top: Vec::new(),
                industry_money_outflow_top: Vec::new(),
                gain_top: Vec::new(),
                sub_interval_gain_top: Vec::new(),
            },
            daily: MarketAnalysisSnapshot {
                trade_date: Some(ref_date),
                concept_top: Vec::new(),
                industry_top: Vec::new(),
                concept_money_flow_top: Vec::new(),
                industry_money_flow_top: Vec::new(),
                concept_money_outflow_top: Vec::new(),
                industry_money_outflow_top: Vec::new(),
                gain_top: Vec::new(),
                sub_interval_gain_top: Vec::new(),
            },
        });
    }

    let interval_start = dates.first().cloned().unwrap_or_else(|| ref_date.clone());
    let interval_end = dates.last().cloned().unwrap_or_else(|| ref_date.clone());

    let concept_db = concept_performance_db_path(&source_path);
    let concept_db_str = concept_db
        .to_str()
        .ok_or_else(|| "概念表现库路径不是有效UTF-8".to_string())?;
    let concept_conn =
        Connection::open(concept_db_str).map_err(|e| format!("打开概念表现库失败: {e}"))?;
    let concept_interval_sql = r#"
        SELECT concept, AVG(TRY_CAST(performance_pct AS DOUBLE)) AS avg_pct
        FROM concept_performance
        WHERE performance_type = 'concept'
          AND trade_date >= ?
          AND trade_date <= ?
        GROUP BY 1
        ORDER BY avg_pct DESC NULLS LAST, concept ASC
        "#;

    let mut concept_interval_stmt = concept_conn
        .prepare(concept_interval_sql)
        .map_err(|e| format!("预编译概念区间榜 SQL 失败: {e}"))?;
    let mut concept_interval_rows = concept_interval_stmt
        .query(params![&interval_start, &interval_end])
        .map_err(|e| format!("执行概念区间榜 SQL 失败: {e}"))?;
    let mut interval_concept_top = Vec::new();
    while let Some(row) = concept_interval_rows
        .next()
        .map_err(|e| format!("读取概念区间榜失败: {e}"))?
    {
        let name: String = row.get(0).map_err(|e| format!("读取概念名失败: {e}"))?;
        if !has_min_stock_count(&concept_stock_counts, &name, min_board_stock_count) {
            continue;
        }
        let value: Option<f64> = row.get(1).map_err(|e| format!("读取概念值失败: {e}"))?;
        if let Some(value) = value.filter(|v| v.is_finite()) {
            interval_concept_top.push(market_rank_item(name, value));
        }
    }
    interval_concept_top.truncate(20);

    let mut interval_industry_stmt = source_conn
        .prepare(
            r#"
            SELECT ts_code, AVG(TRY_CAST(pct_chg AS DOUBLE)) AS avg_pct
            FROM stock_data
            WHERE adj_type = 'qfq'
              AND trade_date >= ?
              AND trade_date <= ?
            GROUP BY 1
            "#,
        )
        .map_err(|e| format!("预编译行业区间榜 SQL 失败: {e}"))?;
    let mut interval_industry_rows = interval_industry_stmt
        .query(params![&interval_start, &interval_end])
        .map_err(|e| format!("执行行业区间榜 SQL 失败: {e}"))?;
    let mut interval_industry_acc: HashMap<String, (f64, usize)> = HashMap::new();
    while let Some(row) = interval_industry_rows
        .next()
        .map_err(|e| format!("读取行业区间榜失败: {e}"))?
    {
        let ts_code: String = row.get(0).map_err(|e| format!("读取代码失败: {e}"))?;
        let avg_pct: Option<f64> = row.get(1).map_err(|e| format!("读取行业值失败: {e}"))?;
        let Some(avg_pct) = avg_pct.filter(|v| v.is_finite()) else {
            continue;
        };
        let ts_code = ts_code.to_ascii_uppercase();
        let Some(industry_list) = ts_industry_map.get(&ts_code) else {
            continue;
        };
        for industry in industry_list {
            let entry = interval_industry_acc
                .entry(industry.clone())
                .or_insert((0.0, 0));
            entry.0 += avg_pct;
            entry.1 += 1;
        }
    }
    let mut interval_industry_top = interval_industry_acc
        .into_iter()
        .filter_map(|(name, (sum, cnt))| {
            if cnt == 0 {
                return None;
            }
            if !has_min_stock_count(&industry_stock_counts, &name, min_board_stock_count) {
                return None;
            }
            let value = sum / cnt as f64;
            if !value.is_finite() {
                return None;
            }
            Some(market_rank_item(name, value))
        })
        .collect::<Vec<_>>();
    interval_industry_top.sort_by(|a, b| {
        b.value
            .partial_cmp(&a.value)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.name.cmp(&b.name))
    });
    interval_industry_top.truncate(20);

    let stock_name_map = (|source_path: &str| -> Result<HashMap<String, String>, String> {
        let rows = load_stock_list(source_path)?;
        let mut out = HashMap::with_capacity(rows.len());

        for cols in rows {
            let Some(ts_code) = cols.first().map(|value| value.trim()) else {
                continue;
            };
            let Some(name_raw) = cols.get(2).map(|value| value.trim()) else {
                continue;
            };
            if ts_code.is_empty() || name_raw.is_empty() {
                continue;
            }

            out.insert(ts_code.to_string(), name_raw.to_string());
        }

        Ok(out)
    })(&source_path)?;

    let mut interval_gain_stmt = source_conn
        .prepare(
            r#"
            SELECT ts_code, trade_date, TRY_CAST(close AS DOUBLE) AS close_price
            FROM stock_data
            WHERE adj_type = 'qfq'
              AND trade_date >= ?
              AND trade_date <= ?
            ORDER BY trade_date ASC, ts_code ASC
            "#,
        )
        .map_err(|e| format!("预编译涨幅区间榜 SQL 失败: {e}"))?;
    let mut interval_gain_rows = interval_gain_stmt
        .query(params![&interval_start, &interval_end])
        .map_err(|e| format!("执行涨幅区间榜 SQL 失败: {e}"))?;
    let mut interval_gain_acc: HashMap<String, Vec<(String, f64)>> = HashMap::new();
    while let Some(row) = interval_gain_rows
        .next()
        .map_err(|e| format!("读取涨幅区间榜失败: {e}"))?
    {
        let ts_code: String = row.get(0).map_err(|e| format!("读取代码失败: {e}"))?;
        let trade_date: String = row.get(1).map_err(|e| format!("读取交易日失败: {e}"))?;
        let close_price: Option<f64> = row.get(2).map_err(|e| format!("读取收盘价失败: {e}"))?;
        let Some(close_price) = close_price.filter(|v| v.is_finite() && *v > f64::EPSILON) else {
            continue;
        };
        let ts_code = ts_code.to_ascii_uppercase();
        if !sample_eligibility.allows_sample(&ts_code, &trade_date) {
            continue;
        }
        let Some(board_list) = ts_board_map.get(&ts_code) else {
            continue;
        };
        if !match_board_filter_with_st(board_list, resolved_board.as_deref(), exclude_st_board) {
            continue;
        }
        interval_gain_acc
            .entry(ts_code)
            .or_default()
            .push((trade_date, close_price));
    }
    let mut interval_gain_top = interval_gain_acc
        .iter()
        .filter_map(|(ts_code, rows)| {
            let (start_date, start_close) = rows.first()?;
            let (end_date, end_close) = rows.last()?;
            if *start_close <= f64::EPSILON {
                return None;
            }
            let value = (*end_close / *start_close - 1.0) * 100.0;
            if !value.is_finite() {
                return None;
            }
            let mut rank_item = market_stock_rank_item(
                &stock_name_map,
                ts_code.clone(),
                value,
                Some(start_date.clone()),
                Some(end_date.clone()),
            );
            rank_item.concepts = ts_concept_map
                .get(ts_code)
                .map(|items| items.join(" / "))
                .filter(|value| !value.is_empty());
            Some(rank_item)
        })
        .collect::<Vec<_>>();
    interval_gain_top.sort_by(|a, b| {
        b.value
            .partial_cmp(&a.value)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.name.cmp(&b.name))
    });
    interval_gain_top.truncate(stock_rank_limit);

    let mut sub_interval_gain_top = if dates.len() >= sub_interval_period {
        interval_gain_acc
            .iter()
            .filter_map(|(ts_code, rows)| {
                if rows.len() < sub_interval_period {
                    return None;
                }
                let mut best: Option<(f64, String, String)> = None;
                for window in rows.windows(sub_interval_period) {
                    let Some((start_date, start_close)) = window.first() else {
                        continue;
                    };
                    let Some((end_date, end_close)) = window.last() else {
                        continue;
                    };
                    if *start_close <= f64::EPSILON {
                        continue;
                    }
                    let value = (end_close / start_close - 1.0) * 100.0;
                    if !value.is_finite() {
                        continue;
                    }
                    let should_replace = best
                        .as_ref()
                        .is_none_or(|(best_value, _, _)| value > *best_value);
                    if should_replace {
                        best = Some((value, start_date.clone(), end_date.clone()));
                    }
                }
                let (value, start_date, end_date) = best?;
                Some(market_stock_rank_item(
                    &stock_name_map,
                    ts_code.clone(),
                    value,
                    Some(start_date),
                    Some(end_date),
                ))
            })
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    sub_interval_gain_top.sort_by(|a, b| {
        b.value
            .partial_cmp(&a.value)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.name.cmp(&b.name))
    });
    sub_interval_gain_top.truncate(stock_rank_limit);

    let daily_concept_sql = r#"
        SELECT concept, TRY_CAST(performance_pct AS DOUBLE)
        FROM concept_performance
        WHERE performance_type = 'concept'
          AND trade_date = ?
        ORDER BY TRY_CAST(performance_pct AS DOUBLE) DESC NULLS LAST, concept ASC
        "#;

    let mut daily_concept_stmt = concept_conn
        .prepare(daily_concept_sql)
        .map_err(|e| format!("预编译概念当日榜 SQL 失败: {e}"))?;
    let mut daily_concept_rows = daily_concept_stmt
        .query(params![&ref_date])
        .map_err(|e| format!("执行概念当日榜 SQL 失败: {e}"))?;
    let mut daily_concept_top = Vec::new();
    while let Some(row) = daily_concept_rows
        .next()
        .map_err(|e| format!("读取概念当日榜失败: {e}"))?
    {
        let name: String = row.get(0).map_err(|e| format!("读取概念名失败: {e}"))?;
        if !has_min_stock_count(&concept_stock_counts, &name, min_board_stock_count) {
            continue;
        }
        let value: Option<f64> = row.get(1).map_err(|e| format!("读取概念值失败: {e}"))?;
        if let Some(value) = value.filter(|v| v.is_finite()) {
            daily_concept_top.push(market_rank_item(name, value));
        }
    }
    daily_concept_top.truncate(20);

    let mut daily_industry_stmt = source_conn
        .prepare(
            r#"
            SELECT ts_code, TRY_CAST(pct_chg AS DOUBLE) AS pct
            FROM stock_data
            WHERE adj_type = 'qfq'
              AND trade_date = ?
            "#,
        )
        .map_err(|e| format!("预编译行业当日榜 SQL 失败: {e}"))?;
    let mut daily_industry_rows = daily_industry_stmt
        .query(params![&ref_date])
        .map_err(|e| format!("执行行业当日榜 SQL 失败: {e}"))?;
    let mut daily_industry_acc: HashMap<String, (f64, usize)> = HashMap::new();
    while let Some(row) = daily_industry_rows
        .next()
        .map_err(|e| format!("读取行业当日榜失败: {e}"))?
    {
        let ts_code: String = row.get(0).map_err(|e| format!("读取代码失败: {e}"))?;
        let pct: Option<f64> = row.get(1).map_err(|e| format!("读取行业值失败: {e}"))?;
        let Some(pct) = pct.filter(|v| v.is_finite()) else {
            continue;
        };
        let ts_code = ts_code.to_ascii_uppercase();
        let Some(industry_list) = ts_industry_map.get(&ts_code) else {
            continue;
        };
        for industry in industry_list {
            let entry = daily_industry_acc
                .entry(industry.clone())
                .or_insert((0.0, 0));
            entry.0 += pct;
            entry.1 += 1;
        }
    }
    let mut daily_industry_top = daily_industry_acc
        .into_iter()
        .filter_map(|(name, (sum, cnt))| {
            if cnt == 0 {
                return None;
            }
            if !has_min_stock_count(&industry_stock_counts, &name, min_board_stock_count) {
                return None;
            }
            let value = sum / cnt as f64;
            if !value.is_finite() {
                return None;
            }
            Some(market_rank_item(name, value))
        })
        .collect::<Vec<_>>();
    daily_industry_top.sort_by(|a, b| {
        b.value
            .partial_cmp(&a.value)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.name.cmp(&b.name))
    });
    daily_industry_top.truncate(20);

    let mut trailing_gain_stmt = source_conn
        .prepare(
            r#"
            SELECT ts_code, trade_date, TRY_CAST(close AS DOUBLE) AS close_price
            FROM stock_data
            WHERE adj_type = 'qfq'
              AND trade_date IN (
                  SELECT trade_date
                  FROM (
                      SELECT DISTINCT trade_date
                      FROM stock_data
                      WHERE adj_type = 'qfq'
                        AND trade_date <= ?
                      ORDER BY trade_date DESC
                      LIMIT 6
                  ) AS recent_dates
              )
            ORDER BY ts_code ASC, trade_date ASC
            "#,
        )
        .map_err(|e| format!("预编译当日多周期涨幅 SQL 失败: {e}"))?;
    let mut trailing_gain_rows = trailing_gain_stmt
        .query(params![&ref_date])
        .map_err(|e| format!("执行当日多周期涨幅 SQL 失败: {e}"))?;
    let mut trailing_gain_acc: HashMap<String, Vec<(String, f64)>> = HashMap::new();
    while let Some(row) = trailing_gain_rows
        .next()
        .map_err(|e| format!("读取当日多周期涨幅失败: {e}"))?
    {
        let ts_code: String = row
            .get(0)
            .map_err(|e| format!("读取多周期涨幅代码失败: {e}"))?;
        let trade_date: String = row
            .get(1)
            .map_err(|e| format!("读取多周期涨幅日期失败: {e}"))?;
        let close_price: Option<f64> = row
            .get(2)
            .map_err(|e| format!("读取多周期收盘价失败: {e}"))?;
        let Some(close_price) = close_price.filter(|value| value.is_finite() && *value > 0.0)
        else {
            continue;
        };
        trailing_gain_acc
            .entry(ts_code.trim().to_ascii_uppercase())
            .or_default()
            .push((trade_date, close_price));
    }

    let mut daily_gain_stmt = source_conn
        .prepare(
            r#"
            SELECT ts_code, TRY_CAST(pct_chg AS DOUBLE)
            FROM stock_data
            WHERE adj_type = 'qfq'
              AND trade_date = ?
            ORDER BY TRY_CAST(pct_chg AS DOUBLE) DESC NULLS LAST, ts_code ASC
            "#,
        )
        .map_err(|e| format!("预编译涨幅当日榜 SQL 失败: {e}"))?;
    let mut daily_gain_rows = daily_gain_stmt
        .query(params![&ref_date])
        .map_err(|e| format!("执行涨幅当日榜 SQL 失败: {e}"))?;
    let mut daily_gain_top = Vec::new();
    while let Some(row) = daily_gain_rows
        .next()
        .map_err(|e| format!("读取涨幅当日榜失败: {e}"))?
    {
        let ts_code: String = row.get(0).map_err(|e| format!("读取代码失败: {e}"))?;
        let value: Option<f64> = row.get(1).map_err(|e| format!("读取涨幅值失败: {e}"))?;
        let Some(value) = value.filter(|v| v.is_finite()) else {
            continue;
        };
        let ts_code = ts_code.to_ascii_uppercase();
        if !sample_eligibility.allows_sample(&ts_code, &ref_date) {
            continue;
        }
        let Some(board_list) = ts_board_map.get(&ts_code) else {
            continue;
        };
        if !match_board_filter_with_st(board_list, resolved_board.as_deref(), exclude_st_board) {
            continue;
        }

        let concepts = ts_concept_map
            .get(&ts_code)
            .map(|items| items.join(" / "))
            .filter(|value| !value.is_empty());
        let trailing_rows = trailing_gain_acc
            .get(&ts_code)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let mut rank_item = market_stock_rank_item(
            &stock_name_map,
            ts_code,
            value,
            Some(ref_date.clone()),
            Some(ref_date.clone()),
        );
        rank_item.concepts = concepts;
        rank_item.three_day_gain = trailing_period_gain(trailing_rows, 3);
        rank_item.five_day_gain = trailing_period_gain(trailing_rows, 5);
        daily_gain_top.push(rank_item);
        if daily_gain_top.len() >= stock_rank_limit {
            break;
        }
    }

    let (
        interval_concept_money_flow_top,
        interval_industry_money_flow_top,
        daily_concept_money_flow_top,
        daily_industry_money_flow_top,
        interval_concept_money_outflow_top,
        interval_industry_money_outflow_top,
        daily_concept_money_outflow_top,
        daily_industry_money_outflow_top,
    ) = if (|conn: &Connection| -> Result<bool, String> {
        let mut stmt = conn
            .prepare(
                r#"
            SELECT LOWER(column_name)
            FROM information_schema.columns
            WHERE LOWER(table_name) = 'stock_data'
            "#,
            )
            .map_err(|e| format!("预编译资金流向列检查失败: {e}"))?;
        let mut rows = stmt
            .query([])
            .map_err(|e| format!("执行资金流向列检查失败: {e}"))?;
        let mut columns = HashSet::new();
        while let Some(row) = rows
            .next()
            .map_err(|e| format!("读取资金流向列检查失败: {e}"))?
        {
            let name: String = row
                .get(0)
                .map_err(|e| format!("读取资金流向列名失败: {e}"))?;
            columns.insert(name);
        }
        Ok(["net_mf_v", "vol", "amount"]
            .iter()
            .all(|name| columns.contains(*name)))
    })(&source_conn)?
    {
        let mut money_flow_stmt = source_conn
            .prepare(
                r#"
                SELECT
                    ts_code,
                    trade_date,
                    TRY_CAST(net_mf_v AS DOUBLE) AS net_mf_vol,
                    TRY_CAST(vol AS DOUBLE) AS trade_vol,
                    TRY_CAST(amount AS DOUBLE) AS trade_amount
                FROM stock_data
                WHERE adj_type = 'qfq'
                  AND trade_date >= ?
                  AND trade_date <= ?
                  AND net_mf_v IS NOT NULL
                "#,
            )
            .map_err(|e| format!("预编译资金流向统计 SQL 失败: {e}"))?;
        let mut money_flow_rows = money_flow_stmt
            .query(params![&interval_start, &interval_end])
            .map_err(|e| format!("执行资金流向统计 SQL 失败: {e}"))?;
        let mut interval_concept_acc = HashMap::new();
        let mut interval_industry_acc = HashMap::new();
        let mut daily_concept_acc = HashMap::new();
        let mut daily_industry_acc = HashMap::new();

        while let Some(row) = money_flow_rows
            .next()
            .map_err(|e| format!("读取资金流向统计失败: {e}"))?
        {
            let ts_code: String = row
                .get(0)
                .map_err(|e| format!("读取资金流向代码失败: {e}"))?;
            let trade_date: String = row
                .get(1)
                .map_err(|e| format!("读取资金流向日期失败: {e}"))?;
            let net_mf_vol: Option<f64> =
                row.get(2).map_err(|e| format!("读取净流入量失败: {e}"))?;
            let vol: Option<f64> = row.get(3).map_err(|e| format!("读取成交量失败: {e}"))?;
            let amount: Option<f64> = row.get(4).map_err(|e| format!("读取成交额失败: {e}"))?;
            let Some(net_amount_yuan) =
                net_mf_vol
                    .zip(vol)
                    .zip(amount)
                    .and_then(|((net_mf_vol, vol), amount)| {
                        estimate_net_money_flow_yuan(net_mf_vol, vol, amount)
                    })
            else {
                continue;
            };
            let ts_code = ts_code.trim().to_ascii_uppercase();
            accumulate_board_money_flow(
                &mut interval_concept_acc,
                &ts_concept_map,
                &ts_code,
                net_amount_yuan,
            );
            accumulate_board_money_flow(
                &mut interval_industry_acc,
                &ts_industry_map,
                &ts_code,
                net_amount_yuan,
            );
            if trade_date == ref_date {
                accumulate_board_money_flow(
                    &mut daily_concept_acc,
                    &ts_concept_map,
                    &ts_code,
                    net_amount_yuan,
                );
                accumulate_board_money_flow(
                    &mut daily_industry_acc,
                    &ts_industry_map,
                    &ts_code,
                    net_amount_yuan,
                );
            }
        }

        (
            money_flow_rank_items(
                interval_concept_acc.clone(),
                &concept_stock_counts,
                min_board_stock_count,
            ),
            money_flow_rank_items(
                interval_industry_acc.clone(),
                &industry_stock_counts,
                min_board_stock_count,
            ),
            money_flow_rank_items(
                daily_concept_acc.clone(),
                &concept_stock_counts,
                min_board_stock_count,
            ),
            money_flow_rank_items(
                daily_industry_acc.clone(),
                &industry_stock_counts,
                min_board_stock_count,
            ),
            money_outflow_rank_items(
                interval_concept_acc,
                &concept_stock_counts,
                min_board_stock_count,
            ),
            money_outflow_rank_items(
                interval_industry_acc,
                &industry_stock_counts,
                min_board_stock_count,
            ),
            money_outflow_rank_items(
                daily_concept_acc,
                &concept_stock_counts,
                min_board_stock_count,
            ),
            money_outflow_rank_items(
                daily_industry_acc,
                &industry_stock_counts,
                min_board_stock_count,
            ),
        )
    } else {
        (
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
    };

    Ok(MarketAnalysisData {
        lookback_period,
        stock_rank_limit,
        sub_interval_period,
        min_board_stock_count,
        latest_trade_date,
        resolved_reference_trade_date: Some(ref_date.clone()),
        board_options,
        resolved_board,
        interval: MarketAnalysisSnapshot {
            trade_date: Some(format!("{}~{}", interval_start, interval_end)),
            concept_top: interval_concept_top,
            industry_top: interval_industry_top,
            concept_money_flow_top: interval_concept_money_flow_top,
            industry_money_flow_top: interval_industry_money_flow_top,
            concept_money_outflow_top: interval_concept_money_outflow_top,
            industry_money_outflow_top: interval_industry_money_outflow_top,
            gain_top: interval_gain_top,
            sub_interval_gain_top,
        },
        daily: MarketAnalysisSnapshot {
            trade_date: Some(ref_date),
            concept_top: daily_concept_top,
            industry_top: daily_industry_top,
            concept_money_flow_top: daily_concept_money_flow_top,
            industry_money_flow_top: daily_industry_money_flow_top,
            concept_money_outflow_top: daily_concept_money_outflow_top,
            industry_money_outflow_top: daily_industry_money_outflow_top,
            gain_top: daily_gain_top,
            sub_interval_gain_top: Vec::new(),
        },
    })
}

pub fn get_market_contribution(
    source_path: String,
    scope: String,
    kind: String,
    name: String,
    lookback_period: Option<usize>,
    reference_trade_date: Option<String>,
) -> Result<MarketContributionData, String> {
    let scope = scope.trim().to_ascii_lowercase();
    let kind = kind.trim().to_ascii_lowercase();
    let target_name = name.trim().to_string();
    if !matches!(scope.as_str(), "interval" | "daily") {
        return Err("scope 仅支持 interval/daily".to_string());
    }
    let kind = match kind.as_str() {
        "concept" => "concept".to_string(),
        "industry" | "board" | "market" => "industry".to_string(),
        _ => return Err("kind 仅支持 concept/industry".to_string()),
    };
    if target_name.is_empty() {
        return Err("名称不能为空".to_string());
    }

    let lookback_period = lookback_period.unwrap_or(20).max(1);
    let source_db = source_db_path(&source_path);
    let source_db_str = source_db
        .to_str()
        .ok_or_else(|| "原始库路径不是有效UTF-8".to_string())?;
    let source_conn =
        Connection::open(source_db_str).map_err(|e| format!("打开原始库失败: {e}"))?;

    let latest_trade_date: Option<String> = source_conn
        .query_row(
            "SELECT MAX(trade_date) FROM stock_data WHERE adj_type = 'qfq'",
            [],
            |row| row.get(0),
        )
        .map_err(|e| format!("查询最新交易日失败: {e}"))?;
    let ref_date = reference_trade_date
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .or(latest_trade_date)
        .ok_or_else(|| "缺少有效参考日".to_string())?;

    let mut date_stmt = source_conn
        .prepare(
            r#"
            SELECT trade_date
            FROM (
                SELECT DISTINCT trade_date
                FROM stock_data
                WHERE adj_type = 'qfq'
                  AND trade_date <= ?
                ORDER BY trade_date DESC
                LIMIT ?
            ) AS t
            ORDER BY trade_date ASC
            "#,
        )
        .map_err(|e| format!("预编译市场贡献区间日期 SQL 失败: {e}"))?;
    let mut date_rows = date_stmt
        .query(params![&ref_date, lookback_period as i64])
        .map_err(|e| format!("执行市场贡献区间日期 SQL 失败: {e}"))?;
    let mut dates = Vec::new();
    while let Some(row) = date_rows
        .next()
        .map_err(|e| format!("读取市场贡献区间日期失败: {e}"))?
    {
        let trade_date: String = row.get(0).map_err(|e| format!("读取交易日失败: {e}"))?;
        dates.push(trade_date);
    }

    if dates.is_empty() {
        return Ok(MarketContributionData {
            scope,
            kind,
            name: target_name,
            trade_date: Some(ref_date),
            start_date: None,
            end_date: None,
            lookback_period,
            contributors: Vec::new(),
        });
    }

    let interval_start = dates.first().cloned();
    let interval_end = dates.last().cloned();

    let stock_rows = load_stock_list(&source_path)?;
    let mut ts_name_map: HashMap<String, String> = HashMap::with_capacity(stock_rows.len());
    let mut ts_industry_map: HashMap<String, String> = HashMap::with_capacity(stock_rows.len());
    let mut target_codes: HashSet<String> = HashSet::new();

    for cols in stock_rows {
        let Some(ts_code) = cols.first().map(|value| value.trim()) else {
            continue;
        };
        if ts_code.is_empty() {
            continue;
        }

        let stock_name = cols
            .get(2)
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        if let Some(stock_name) = stock_name {
            ts_name_map.insert(ts_code.to_string(), stock_name);
        }

        let industry_name = cols
            .get(4)
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        if let Some(industry_name) = industry_name.clone() {
            ts_industry_map.insert(ts_code.to_string(), industry_name.clone());
        }

        if kind == "industry" {
            let is_match = industry_name
                .as_deref()
                .map(|value| {
                    value
                        .split(|ch| {
                            matches!(ch, ',' | ';' | '，' | '；' | '|' | '、' | '/' | '\n' | '\r')
                        })
                        .map(|part| part.trim())
                        .any(|part| !part.is_empty() && part == target_name)
                })
                .unwrap_or(false);
            if is_match {
                target_codes.insert(ts_code.to_string());
            }
        }
    }

    if kind == "concept" {
        let concept_rows = load_ths_concepts_list(&source_path)?;
        for cols in concept_rows {
            let Some(ts_code) = cols.first().map(|value| value.trim()) else {
                continue;
            };
            let Some(concept_raw) = cols.get(2).map(|value| value.trim()) else {
                continue;
            };
            if ts_code.is_empty() || concept_raw.is_empty() {
                continue;
            }
            let is_match = concept_raw
                .split(|ch| matches!(ch, ',' | ';' | '，' | '；' | '|' | '、' | '/' | '\n' | '\r'))
                .map(|part| part.trim())
                .any(|part| !part.is_empty() && part == target_name);
            if is_match {
                target_codes.insert(ts_code.to_string());
            }
        }
    }

    if target_codes.is_empty() {
        return Ok(MarketContributionData {
            scope,
            kind,
            name: target_name,
            trade_date: Some(ref_date),
            start_date: interval_start,
            end_date: interval_end,
            lookback_period,
            contributors: Vec::new(),
        });
    }

    let mut contributors = Vec::new();
    if scope == "daily" {
        let mut stmt = source_conn
            .prepare(
                r#"
                SELECT ts_code, TRY_CAST(pct_chg AS DOUBLE) AS pct
                FROM stock_data
                WHERE adj_type = 'qfq'
                  AND trade_date = ?
                "#,
            )
            .map_err(|e| format!("预编译市场贡献当日 SQL 失败: {e}"))?;
        let mut rows = stmt
            .query(params![&ref_date])
            .map_err(|e| format!("执行市场贡献当日 SQL 失败: {e}"))?;

        while let Some(row) = rows
            .next()
            .map_err(|e| format!("读取市场贡献当日数据失败: {e}"))?
        {
            let ts_code: String = row.get(0).map_err(|e| format!("读取代码失败: {e}"))?;
            if !target_codes.contains(&ts_code) {
                continue;
            }
            let pct: Option<f64> = row.get(1).map_err(|e| format!("读取涨幅失败: {e}"))?;
            let Some(contribution_pct) = pct.filter(|v| v.is_finite()) else {
                continue;
            };
            contributors.push(MarketContributorItem {
                ts_code: ts_code.clone(),
                name: ts_name_map.get(&ts_code).cloned(),
                industry: ts_industry_map.get(&ts_code).cloned(),
                contribution_pct,
            });
        }
    } else {
        let start = interval_start.clone().unwrap_or_else(|| ref_date.clone());
        let end = interval_end.clone().unwrap_or_else(|| ref_date.clone());
        let mut stmt = source_conn
            .prepare(
                r#"
                SELECT ts_code, AVG(TRY_CAST(pct_chg AS DOUBLE)) AS avg_pct
                FROM stock_data
                WHERE adj_type = 'qfq'
                  AND trade_date >= ?
                  AND trade_date <= ?
                GROUP BY 1
                "#,
            )
            .map_err(|e| format!("预编译市场贡献区间 SQL 失败: {e}"))?;
        let mut rows = stmt
            .query(params![&start, &end])
            .map_err(|e| format!("执行市场贡献区间 SQL 失败: {e}"))?;

        while let Some(row) = rows
            .next()
            .map_err(|e| format!("读取市场贡献区间数据失败: {e}"))?
        {
            let ts_code: String = row.get(0).map_err(|e| format!("读取代码失败: {e}"))?;
            if !target_codes.contains(&ts_code) {
                continue;
            }
            let pct: Option<f64> = row.get(1).map_err(|e| format!("读取涨幅失败: {e}"))?;
            let Some(contribution_pct) = pct.filter(|v| v.is_finite()) else {
                continue;
            };
            contributors.push(MarketContributorItem {
                ts_code: ts_code.clone(),
                name: ts_name_map.get(&ts_code).cloned(),
                industry: ts_industry_map.get(&ts_code).cloned(),
                contribution_pct,
            });
        }
    }

    contributors.sort_by(|a, b| {
        b.contribution_pct
            .partial_cmp(&a.contribution_pct)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.ts_code.cmp(&b.ts_code))
    });
    contributors.truncate(100);

    Ok(MarketContributionData {
        scope,
        kind,
        name: target_name,
        trade_date: Some(ref_date),
        start_date: interval_start,
        end_date: interval_end,
        lookback_period,
        contributors,
    })
}

#[cfg(test)]
mod tests {
    use crate::statistics::market::build_industry_maps_from_rows;
    use crate::statistics::market::estimate_net_money_flow_yuan;
    use crate::statistics::market::money_flow_rank_items;
    use crate::statistics::market::money_outflow_rank_items;
    use crate::statistics::market::trailing_period_gain;
    use std::collections::HashMap;

    #[test]
    fn market_analysis_industry_map_uses_industry_instead_of_market_board() {
        let rows = vec![
            vec![
                "000001.SZ",
                "000001",
                "平安银行",
                "深圳",
                "银行",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "主板",
            ],
            vec![
                "300001.SZ",
                "300001",
                "特锐德",
                "青岛",
                "专用设备",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "创业板",
            ],
        ]
        .into_iter()
        .map(|row| row.into_iter().map(str::to_string).collect())
        .collect();

        let (industry_map, industry_counts) = build_industry_maps_from_rows(rows);

        assert_eq!(
            industry_map.get("000001.SZ"),
            Some(&vec!["银行".to_string()])
        );
        assert_eq!(
            industry_map.get("300001.SZ"),
            Some(&vec!["专用设备".to_string()])
        );
        assert!(!industry_counts.contains_key("主板"));
        assert!(!industry_counts.contains_key("创业板"));
    }

    #[test]
    fn market_analysis_money_flow_converts_volume_to_yuan() {
        assert_eq!(
            estimate_net_money_flow_yuan(100.0, 1_000.0, 5_000.0),
            Some(500_000.0)
        );
        assert_eq!(estimate_net_money_flow_yuan(100.0, 0.0, 5_000.0), None);
        assert_eq!(
            estimate_net_money_flow_yuan(f64::NAN, 1_000.0, 5_000.0),
            None
        );
    }

    #[test]
    fn market_analysis_money_flow_only_ranks_positive_eligible_boards() {
        let acc = HashMap::from([
            ("算力".to_string(), 200_000_000.0),
            ("机器人".to_string(), 80_000_000.0),
            ("银行".to_string(), -50_000_000.0),
        ]);
        let counts = HashMap::from([
            ("算力".to_string(), 12),
            ("机器人".to_string(), 1),
            ("银行".to_string(), 20),
        ]);

        let items = money_flow_rank_items(acc, &counts, 2);

        assert_eq!(items.len(), 1);
        assert_eq!(items[0].name, "算力");
        assert_eq!(items[0].value, 200_000_000.0);
    }

    #[test]
    fn market_analysis_money_outflow_ranks_largest_outflow_first() {
        let acc = HashMap::from([
            ("算力".to_string(), 20_000_000.0),
            ("机器人".to_string(), -80_000_000.0),
            ("银行".to_string(), -150_000_000.0),
        ]);
        let counts = HashMap::from([
            ("算力".to_string(), 12),
            ("机器人".to_string(), 8),
            ("银行".to_string(), 20),
        ]);

        let items = money_outflow_rank_items(acc, &counts, 2);

        assert_eq!(items.len(), 2);
        assert_eq!(items[0].name, "银行");
        assert_eq!(items[0].value, -150_000_000.0);
        assert_eq!(items[1].name, "机器人");
    }

    #[test]
    fn market_analysis_trailing_gain_uses_requested_trade_day_window() {
        let rows = vec![
            ("20240102".to_string(), 10.0),
            ("20240103".to_string(), 11.0),
            ("20240104".to_string(), 12.0),
            ("20240105".to_string(), 15.0),
            ("20240108".to_string(), 20.0),
            ("20240109".to_string(), 24.0),
        ];

        let three_day = trailing_period_gain(&rows, 3).expect("three day gain");
        let five_day = trailing_period_gain(&rows, 5).expect("five day gain");

        assert!((three_day - 100.0).abs() < 1e-9);
        assert!((five_day - 140.0).abs() < 1e-9);
        assert_eq!(trailing_period_gain(&rows[..5], 5), None);
    }
}
