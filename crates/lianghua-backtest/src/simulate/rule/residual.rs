//! 残差与行情序列的流式计算、截面缓存与规则行读取。

use crate::simulate::rule::{
    EFFICIENCY_RATIO_PERIOD, PCT_CHG_BATCH_SIZE, ResidualCacheInput, RuleBacktestOutcome,
};

#[cfg(test)]
use crate::data::result_db_path;
#[cfg(test)]
use crate::simulate::rule::cache::ts_code_allowed;
#[cfg(test)]
use crate::simulate::rule::{RuleDbRow, RuleLayerFromDbInput};
use crate::data::concept_performance_data::load_concept_trend_series_map;
use crate::data::concept_performance_data::load_industry_trend_series_map;
use crate::data::load_stock_list;
use crate::data::load_ths_concepts_named_map;
use crate::simulate::BacktestSampleEligibility;
use crate::simulate::DailyReturnPoint;
use crate::simulate::ResidualFactorSeriesRefs;
use crate::simulate::ResidualReturnInput;
use crate::simulate::build_backtest_sample_eligibility;
use crate::simulate::calc_forward_residual_return;
use crate::simulate::calc_stock_residual_returns_from_loaded_series;
use crate::simulate::fp_utils::EPS;
use crate::simulate::stock_data_has_open_close;
use duckdb::Connection;
use duckdb::params_from_iter;
use rayon::prelude::*;
use std::collections::HashMap;
use std::collections::HashSet;
pub(super) fn stream_residual_maps<F>(
    source_conn: &Connection,
    source_dir: &str,
    ts_codes: Vec<String>,
    concept_map: &HashMap<String, String>,
    industry_map: &HashMap<String, String>,
    input: &ResidualCacheInput<'_>,
    stock_batch_size: usize,
    mut visit: F,
) -> Result<(), String>
where
    F: FnMut(String, HashMap<String, RuleBacktestOutcome>) -> Result<(), String>,
{
    if ts_codes.is_empty() {
        return Ok(());
    }
    let sample_eligibility =
        build_backtest_sample_eligibility(source_dir, input.min_listed_trade_days)?;

    let mut concept_series_cache = build_concept_series_cache(
        source_dir,
        &ts_codes,
        concept_map,
        input.start_date,
        input.end_date,
        input.concept_beta.abs() > EPS,
    )?;
    concept_series_cache.shrink_to_fit();
    let mut industry_series_cache = build_industry_series_cache(
        source_dir,
        &ts_codes,
        industry_map,
        input.start_date,
        input.end_date,
        input.industry_beta.abs() > EPS,
    )?;
    industry_series_cache.shrink_to_fit();
    let index_series = load_pct_chg_series_cache_for_ts_codes(
        source_conn,
        &[input.index_ts_code.to_string()],
        "ind",
        input.start_date,
        input.end_date,
    )?
    .remove(input.index_ts_code)
    .unwrap_or_default();
    let er_column = (|conn: &Connection, requested: &str| -> Result<Option<String>, String> {
        let mut stmt = conn
            .prepare("DESCRIBE stock_data")
            .map_err(|e| format!("预编译 stock_data 列查询失败:{e}"))?;
        let mut rows = stmt
            .query([])
            .map_err(|e| format!("查询 stock_data 列失败:{e}"))?;
        while let Some(row) = rows
            .next()
            .map_err(|e| format!("读取 stock_data 列失败:{e}"))?
        {
            let name: String = row
                .get(0)
                .map_err(|e| format!("读取 stock_data 列名失败:{e}"))?;
            if name.eq_ignore_ascii_case(requested) {
                return Ok(Some(name));
            }
        }
        Ok(None)
    })(source_conn, "ER")?;

    for ts_code_batch in ts_codes.chunks(stock_batch_size.max(1)) {
        // 只让当前残差计算批次的原始涨跌幅常驻，并把该批残差直接交给最终
        // day_groups。禁止重新引入全量 residual_map_cache，否则会为每一行重复持有
        // 交易日期字符串，导致单策略回测也可能在进入规则计算前耗尽内存。
        let mut stock_series_cache = load_pct_chg_series_cache_for_ts_codes(
            source_conn,
            ts_code_batch,
            input.stock_adj_type,
            input.start_date,
            input.end_date,
        )?;
        shrink_stock_series_cache(&mut stock_series_cache);
        let mut er_series_cache = match er_column.as_deref() {
            Some(column) => {
                (|conn: &Connection,
                  ts_codes: &[String],
                  adj_type: &str,
                  start_date: &str,
                  end_date: &str,
                  indicator_column: &str|
                 -> Result<HashMap<String, HashMap<String, f64>>, String> {
                    if ts_codes.is_empty() {
                        return Ok(HashMap::new());
                    }

                    let mut out =
                        HashMap::<String, HashMap<String, f64>>::with_capacity(ts_codes.len());
                    let quoted_column = (|identifier: &str| -> String {
                        format!("\"{}\"", identifier.replace('"', "\"\""))
                    })(indicator_column);
                    for chunk in ts_codes.chunks(PCT_CHG_BATCH_SIZE) {
                        let placeholders = std::iter::repeat_n("?", chunk.len())
                            .collect::<Vec<_>>()
                            .join(", ");
                        let sql = format!(
                            r#"
            SELECT
                ts_code,
                trade_date,
                TRY_CAST({quoted_column} AS DOUBLE)
            FROM stock_data
            WHERE adj_type = ?
              AND ts_code IN ({placeholders})
              AND trade_date >= ?
              AND trade_date <= ?
            ORDER BY ts_code ASC, trade_date ASC
            "#
                        );
                        let mut stmt = conn
                            .prepare(&sql)
                            .map_err(|e| format!("预编译批量 {indicator_column} 查询失败:{e}"))?;
                        let query_params = std::iter::once(adj_type.trim())
                            .chain(chunk.iter().map(|ts_code| ts_code.trim()))
                            .chain(std::iter::once(start_date.trim()))
                            .chain(std::iter::once(end_date.trim()));
                        let mut rows = stmt
                            .query(params_from_iter(query_params))
                            .map_err(|e| format!("查询批量 {indicator_column} 失败:{e}"))?;

                        while let Some(row) = rows
                            .next()
                            .map_err(|e| format!("读取批量 {indicator_column} 失败:{e}"))?
                        {
                            let ts_code: String =
                                row.get(0).map_err(|e| format!("读取ts_code失败:{e}"))?;
                            let trade_date: String =
                                row.get(1).map_err(|e| format!("读取trade_date失败:{e}"))?;
                            let value: Option<f64> = row
                                .get(2)
                                .map_err(|e| format!("读取{indicator_column}失败:{e}"))?;

                            let Some(value) = value.filter(|value| value.is_finite()) else {
                                continue;
                            };
                            out.entry(ts_code).or_default().insert(trade_date, value);
                        }
                    }

                    Ok(out)
                })(
                    source_conn,
                    ts_code_batch,
                    input.stock_adj_type,
                    input.start_date,
                    input.end_date,
                    column,
                )?
            }
            None => HashMap::new(),
        };
        let fallback_ts_codes = ts_code_batch
            .iter()
            .filter(|ts_code| !er_series_cache.contains_key(ts_code.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        if !fallback_ts_codes.is_empty() {
            let close_series_cache =
                (|conn: &Connection,
                  ts_codes: &[String],
                  adj_type: &str,
                  start_date: &str,
                  end_date: &str|
                 -> Result<HashMap<String, Vec<(String, f64)>>, String> {
                    if ts_codes.is_empty() {
                        return Ok(HashMap::new());
                    }

                    let mut out =
                        HashMap::<String, Vec<(String, f64)>>::with_capacity(ts_codes.len());
                    for chunk in ts_codes.chunks(PCT_CHG_BATCH_SIZE) {
                        let placeholders = std::iter::repeat_n("?", chunk.len())
                            .collect::<Vec<_>>()
                            .join(", ");
                        let sql = format!(
                            r#"
            WITH ranked AS (
                SELECT
                    ts_code,
                    trade_date,
                    TRY_CAST(close AS DOUBLE) AS close_price,
                    ROW_NUMBER() OVER (
                        PARTITION BY
                            ts_code,
                            CASE WHEN trade_date < ? THEN 0 ELSE 1 END
                        ORDER BY trade_date DESC
                    ) AS history_rank
                FROM stock_data
                WHERE adj_type = ?
                  AND ts_code IN ({placeholders})
                  AND trade_date <= ?
            )
            SELECT
                ts_code,
                trade_date,
                close_price
            FROM ranked
            WHERE trade_date >= ?
               OR history_rank <= {EFFICIENCY_RATIO_PERIOD}
            ORDER BY ts_code ASC, trade_date ASC
            "#
                        );
                        let mut stmt = conn
                            .prepare(&sql)
                            .map_err(|e| format!("预编译批量收盘价查询失败:{e}"))?;
                        let query_params = std::iter::once(start_date.trim())
                            .chain(std::iter::once(adj_type.trim()))
                            .chain(chunk.iter().map(|ts_code| ts_code.trim()))
                            .chain(std::iter::once(end_date.trim()))
                            .chain(std::iter::once(start_date.trim()));
                        let mut rows = stmt
                            .query(params_from_iter(query_params))
                            .map_err(|e| format!("查询批量收盘价失败:{e}"))?;

                        while let Some(row) =
                            rows.next().map_err(|e| format!("读取批量收盘价失败:{e}"))?
                        {
                            let ts_code: String =
                                row.get(0).map_err(|e| format!("读取ts_code失败:{e}"))?;
                            let trade_date: String =
                                row.get(1).map_err(|e| format!("读取trade_date失败:{e}"))?;
                            let close: Option<f64> =
                                row.get(2).map_err(|e| format!("读取close失败:{e}"))?;

                            let Some(close) = close.filter(|value| value.is_finite()) else {
                                continue;
                            };
                            out.entry(ts_code).or_default().push((trade_date, close));
                        }
                    }

                    Ok(out)
                })(
                    source_conn,
                    &fallback_ts_codes,
                    input.stock_adj_type,
                    input.start_date,
                    input.end_date,
                )?;
            for (ts_code, close_series) in close_series_cache {
                er_series_cache.insert(
                    ts_code,
                    calc_efficiency_ratio_map(&close_series, EFFICIENCY_RATIO_PERIOD),
                );
            }
        }
        for series in er_series_cache.values_mut() {
            series.shrink_to_fit();
        }
        er_series_cache.shrink_to_fit();

        let batch_results: Vec<Result<(String, HashMap<String, RuleBacktestOutcome>), String>> =
            ts_code_batch
                .par_iter()
                .map(|ts_code| {
                    let residual_map = build_residual_map_for_ts_code(
                        ts_code,
                        &stock_series_cache,
                        &er_series_cache,
                        &index_series,
                        concept_map,
                        industry_map,
                        &concept_series_cache,
                        &industry_series_cache,
                        input,
                        &sample_eligibility,
                    )?;
                    Ok((ts_code.clone(), residual_map))
                })
                .collect();

        drop(er_series_cache);
        drop(stock_series_cache);
        for item in batch_results {
            let (ts_code, residual_map) = item?;
            visit(ts_code, residual_map)?;
        }
    }

    drop(concept_series_cache);
    drop(industry_series_cache);
    drop(index_series);

    Ok(())
}

pub(super) fn build_residual_map_for_ts_code(
    ts_code: &str,
    stock_series_cache: &HashMap<String, HashMap<String, DailyReturnPoint>>,
    er_series_cache: &HashMap<String, HashMap<String, f64>>,
    index_series: &HashMap<String, DailyReturnPoint>,
    concept_map: &HashMap<String, String>,
    industry_map: &HashMap<String, String>,
    concept_series_cache: &HashMap<String, HashMap<String, f64>>,
    industry_series_cache: &HashMap<String, HashMap<String, f64>>,
    input: &ResidualCacheInput<'_>,
    sample_eligibility: &BacktestSampleEligibility,
) -> Result<HashMap<String, RuleBacktestOutcome>, String> {
    let Some(stock_series) = stock_series_cache.get(ts_code) else {
        return Ok(HashMap::new());
    };

    let most_related_concept = concept_map.get(ts_code).cloned().unwrap_or_default();
    let industry = industry_map.get(ts_code).cloned().unwrap_or_default();
    let concept_series = if most_related_concept.trim().is_empty() {
        None
    } else {
        concept_series_cache.get(most_related_concept.trim())
    };
    let industry_series = if industry.trim().is_empty() {
        None
    } else {
        industry_series_cache.get(industry.trim())
    };

    let residual_points = calc_stock_residual_returns_from_loaded_series(
        &ResidualReturnInput {
            ts_code: ts_code.to_string(),
            stock_adj_type: input.stock_adj_type.to_string(),
            index_ts_code: input.index_ts_code.to_string(),
            concept: most_related_concept,
            industry,
            index_beta: input.index_beta,
            concept_beta: input.concept_beta,
            industry_beta: input.industry_beta,
            start_date: input.start_date.to_string(),
            end_date: input.end_date.to_string(),
        },
        stock_series,
        index_series,
        ResidualFactorSeriesRefs {
            concept_series,
            industry_series,
        },
    )?;

    let empty_er_by_date = HashMap::new();
    let er_by_date = er_series_cache.get(ts_code).unwrap_or(&empty_er_by_date);
    let mut residual_map = build_forward_backtest_outcome_map(
        residual_points,
        input.backtest_period,
        er_by_date,
        input.index_beta,
        input.concept_beta,
        input.industry_beta,
    );
    residual_map.retain(|trade_date, _| sample_eligibility.allows_sample(ts_code, trade_date));
    residual_map.shrink_to_fit();
    Ok(residual_map)
}

pub(super) fn build_forward_backtest_outcome_map(
    mut residual_points: Vec<crate::simulate::ResidualReturnPoint>,
    backtest_period: usize,
    er_by_date: &HashMap<String, f64>,
    index_beta: f64,
    concept_beta: f64,
    industry_beta: f64,
) -> HashMap<String, RuleBacktestOutcome> {
    if backtest_period == 0 || residual_points.len() < backtest_period + 1 {
        return HashMap::new();
    }

    residual_points.sort_by(|left, right| left.trade_date.cmp(&right.trade_date));

    let mut out = HashMap::with_capacity(residual_points.len() - backtest_period);
    for index in 0..(residual_points.len() - backtest_period) {
        if let Some(residual_return) = calc_forward_residual_return(
            &residual_points[index + 1..=index + backtest_period],
            index_beta,
            concept_beta,
            industry_beta,
        ) {
            let end_trade_date = &residual_points[index + backtest_period].trade_date;
            let er_change = er_by_date
                .get(end_trade_date)
                .zip(er_by_date.get(&residual_points[index].trade_date))
                .map(|(end_er, start_er)| end_er - start_er)
                .filter(|value| value.is_finite())
                .unwrap_or(f64::INFINITY);
            out.insert(
                residual_points[index].trade_date.clone(),
                RuleBacktestOutcome {
                    residual_return,
                    er_change,
                },
            );
        }
    }

    out
}

pub(super) fn calc_efficiency_ratio_map(
    close_series: &[(String, f64)],
    period: usize,
) -> HashMap<String, f64> {
    if period == 0 || close_series.len() < period + 1 {
        return HashMap::new();
    }

    let mut denominator = close_series[..=period]
        .windows(2)
        .map(|window| (window[1].1 - window[0].1).abs())
        .sum::<f64>();

    let mut out = HashMap::with_capacity(close_series.len() - period);
    for end_index in period..close_series.len() {
        let numerator = close_series[end_index].1 - close_series[end_index - period].1;
        if numerator.is_finite() && denominator.is_finite() && denominator.abs() > EPS {
            let er = numerator / denominator;
            if er.is_finite() {
                out.insert(close_series[end_index].0.clone(), er);
            }
        }

        let add_index = end_index + 1;
        if add_index >= close_series.len() {
            break;
        }
        let remove_change_index = end_index + 1 - period;
        denominator -=
            (close_series[remove_change_index].1 - close_series[remove_change_index - 1].1).abs();
        denominator += (close_series[add_index].1 - close_series[add_index - 1].1).abs();
    }

    out
}

pub(super) fn shrink_stock_series_cache(
    cache: &mut HashMap<String, HashMap<String, DailyReturnPoint>>,
) {
    for series in cache.values_mut() {
        series.shrink_to_fit();
    }
    cache.shrink_to_fit();
}

pub(super) fn load_pct_chg_series_cache_for_ts_codes(
    conn: &Connection,
    ts_codes: &[String],
    adj_type: &str,
    start_date: &str,
    end_date: &str,
) -> Result<HashMap<String, HashMap<String, DailyReturnPoint>>, String> {
    if ts_codes.is_empty() {
        return Ok(HashMap::new());
    }
    if !stock_data_has_open_close(conn)? {
        return Err(
            "stock_data 缺少 open/close 列，无法按次日开盘后的可成交区间回测；请重新同步行情数据"
                .to_string(),
        );
    }

    let mut out =
        HashMap::<String, HashMap<String, DailyReturnPoint>>::with_capacity(ts_codes.len());
    for chunk in ts_codes.chunks(PCT_CHG_BATCH_SIZE) {
        let placeholders = std::iter::repeat_n("?", chunk.len())
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            r#"
            SELECT
                ts_code,
                trade_date,
                TRY_CAST(pct_chg AS DOUBLE),
                TRY_CAST(open AS DOUBLE), TRY_CAST(close AS DOUBLE)
            FROM stock_data
            WHERE adj_type = ?
              AND ts_code IN ({placeholders})
              AND trade_date >= ?
              AND trade_date <= ?
            ORDER BY ts_code ASC, trade_date ASC
            "#
        );
        let mut stmt = conn
            .prepare(&sql)
            .map_err(|e| format!("预编译批量涨跌幅查询失败:{e}"))?;
        let query_params = std::iter::once(adj_type.trim())
            .chain(chunk.iter().map(|ts_code| ts_code.trim()))
            .chain(std::iter::once(start_date.trim()))
            .chain(std::iter::once(end_date.trim()));
        let mut rows = stmt
            .query(params_from_iter(query_params))
            .map_err(|e| format!("查询批量涨跌幅失败:{e}"))?;

        while let Some(row) = rows.next().map_err(|e| format!("读取批量涨跌幅失败:{e}"))? {
            let ts_code: String = row.get(0).map_err(|e| format!("读取ts_code失败:{e}"))?;
            let trade_date: String = row.get(1).map_err(|e| format!("读取trade_date失败:{e}"))?;
            let pct: Option<f64> = row.get(2).map_err(|e| format!("读取pct_chg失败:{e}"))?;
            let open: Option<f64> = row.get(3).map_err(|e| format!("读取open失败:{e}"))?;
            let close: Option<f64> = row.get(4).map_err(|e| format!("读取close失败:{e}"))?;

            let Some(close_pct) = pct.filter(|value| value.is_finite()) else {
                continue;
            };
            let Some(open_pct) = open
                .filter(|value| value.is_finite() && value.abs() > EPS)
                .zip(close.filter(|value| value.is_finite()))
                .map(|(open, close)| (close / open - 1.0) * 100.0)
                .filter(|value| value.is_finite())
            else {
                continue;
            };
            out.entry(ts_code).or_default().insert(
                trade_date,
                DailyReturnPoint {
                    close_pct,
                    open_pct,
                },
            );
        }
    }

    Ok(out)
}

pub(super) fn build_concept_series_cache(
    source_dir: &str,
    ts_codes: &[String],
    concept_map: &HashMap<String, String>,
    start_date: &str,
    end_date: &str,
    enabled: bool,
) -> Result<HashMap<String, HashMap<String, f64>>, String> {
    if !enabled {
        return Ok(HashMap::new());
    }

    let mut names = HashSet::new();
    for ts_code in ts_codes {
        if let Some(name) = concept_map
            .get(ts_code)
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
        {
            names.insert(name.to_string());
        }
    }

    let names = names.into_iter().collect::<Vec<_>>();
    load_concept_trend_series_map(source_dir, &names, start_date.trim(), end_date.trim())
}

pub(super) fn build_industry_series_cache(
    source_dir: &str,
    ts_codes: &[String],
    industry_map: &HashMap<String, String>,
    start_date: &str,
    end_date: &str,
    enabled: bool,
) -> Result<HashMap<String, HashMap<String, f64>>, String> {
    if !enabled {
        return Ok(HashMap::new());
    }

    let mut names = HashSet::new();
    for ts_code in ts_codes {
        if let Some(name) = industry_map
            .get(ts_code)
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
        {
            names.insert(name.to_string());
        }
    }

    let names = names.into_iter().collect::<Vec<_>>();
    load_industry_trend_series_map(source_dir, &names, start_date.trim(), end_date.trim())
}

#[cfg(test)]
pub(super) fn load_rule_rows_filtered(
    source_dir: &str,
    input: &RuleLayerFromDbInput,
    allowed_ts_codes: Option<&HashSet<String>>,
) -> Result<Vec<RuleDbRow>, String> {
    (|source_dir: &str,
      rule_names: &[String],
      start_date: &str,
      end_date: &str|
     -> Result<Vec<RuleDbRow>, String> {
        load_rule_rows_for_names_filtered(source_dir, rule_names, start_date, end_date, None)
    })(
        source_dir,
        std::slice::from_ref(&input.rule_name),
        &input.start_date,
        &input.end_date,
    )
    .map(|rows| {
        if allowed_ts_codes.is_none() {
            return rows;
        }
        rows.into_iter()
            .filter(|row| ts_code_allowed(allowed_ts_codes, &row.ts_code))
            .collect()
    })
}

#[cfg(test)]
pub(super) fn load_rule_rows_for_names_filtered(
    source_dir: &str,
    rule_names: &[String],
    start_date: &str,
    end_date: &str,
    allowed_ts_codes: Option<&HashSet<String>>,
) -> Result<Vec<RuleDbRow>, String> {
    let mut rows = (|source_dir: &str,
                     rule_names: &[String],
                     start_date: &str,
                     end_date: &str|
     -> Result<Vec<RuleDbRow>, String> {
        if rule_names.is_empty() {
            return Ok(Vec::new());
        }

        let result_db = result_db_path(source_dir);
        if !result_db.exists() {
            return Ok(Vec::new());
        }

        let result_db_str = result_db
            .to_str()
            .ok_or_else(|| "result_db路径不是有效UTF-8".to_string())?;
        let conn = Connection::open(result_db_str)
            .map_err(|e| format!("打开scoring_result.db失败:{e}"))?;

        let placeholders = std::iter::repeat_n("?", rule_names.len())
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            r#"
        SELECT
            rule_name,
            ts_code,
            trade_date,
            TRY_CAST(rule_score AS DOUBLE)
        FROM rule_details
        WHERE rule_name IN ({placeholders})
          AND trade_date >= ?
          AND trade_date <= ?
          AND TRY_CAST(rule_score AS DOUBLE) IS NOT NULL
        ORDER BY rule_name ASC, trade_date ASC, ts_code ASC
        "#
        );

        let mut stmt = conn
            .prepare(&sql)
            .map_err(|e| format!("预编译rule_details查询失败:{e}"))?;
        let query_params = rule_names
            .iter()
            .map(|name| name.trim())
            .chain(std::iter::once(start_date.trim()))
            .chain(std::iter::once(end_date.trim()));
        let mut rows = stmt
            .query(params_from_iter(query_params))
            .map_err(|e| format!("查询rule_details失败:{e}"))?;

        let mut out = Vec::new();
        while let Some(row) = rows
            .next()
            .map_err(|e| format!("读取rule_details失败:{e}"))?
        {
            let rule_name: String = row.get(0).map_err(|e| format!("读取rule_name失败:{e}"))?;
            let ts_code: String = row.get(1).map_err(|e| format!("读取ts_code失败:{e}"))?;
            let trade_date: String = row.get(2).map_err(|e| format!("读取trade_date失败:{e}"))?;
            let rule_score: f64 = row.get(3).map_err(|e| format!("读取rule_score失败:{e}"))?;

            if rule_name.trim().is_empty()
                || ts_code.trim().is_empty()
                || trade_date.trim().is_empty()
                || !rule_score.is_finite()
            {
                continue;
            }

            out.push(RuleDbRow {
                rule_name,
                ts_code,
                trade_date,
                rule_score,
            });
        }

        Ok(out)
    })(source_dir, rule_names, start_date, end_date)?;
    if allowed_ts_codes.is_some() {
        rows.retain(|row| ts_code_allowed(allowed_ts_codes, &row.ts_code));
    }
    Ok(rows)
}

pub(super) fn load_most_related_concept_map(
    source_dir: &str,
) -> Result<HashMap<String, String>, String> {
    load_ths_concepts_named_map(source_dir, &["most_related_concept", "concept"])
}

pub(super) fn load_stock_industry_map(source_dir: &str) -> Result<HashMap<String, String>, String> {
    let rows = load_stock_list(source_dir)?;
    let mut map = HashMap::with_capacity(rows.len());

    for row in rows {
        let Some(ts_code) = row.first().map(|v| v.trim()) else {
            continue;
        };
        if ts_code.is_empty() {
            continue;
        }

        let industry = row
            .get(4)
            .map(|v| v.trim())
            .filter(|v| !v.is_empty())
            .unwrap_or("")
            .to_string();

        map.insert(ts_code.to_string(), industry);
    }

    Ok(map)
}

#[cfg(test)]
mod tests {

    use crate::simulate::ResidualReturnPoint;
    use crate::simulate::rule::residual::build_forward_backtest_outcome_map;
    use crate::simulate::rule::residual::calc_efficiency_ratio_map;
    use crate::simulate::rule::test_support::*;
    use std::collections::HashMap;
    #[test]
    fn efficiency_ratio_uses_signed_twenty_period_formula() {
        let rising = (0..=21)
            .map(|index| (format!("d{index:02}"), 100.0 + index as f64))
            .collect::<Vec<_>>();
        let falling = (0..=20)
            .map(|index| (format!("d{index:02}"), 100.0 - index as f64))
            .collect::<Vec<_>>();
        let flat = (0..=20)
            .map(|index| (format!("d{index:02}"), 100.0))
            .collect::<Vec<_>>();

        let rising_er = calc_efficiency_ratio_map(&rising, 20);
        let falling_er = calc_efficiency_ratio_map(&falling, 20);
        let flat_er = calc_efficiency_ratio_map(&flat, 20);

        assert_opt_close(rising_er.get("d20").copied(), Some(1.0));
        assert_opt_close(rising_er.get("d21").copied(), Some(1.0));
        assert_opt_close(falling_er.get("d20").copied(), Some(-1.0));
        assert_eq!(flat_er.get("d20"), None);
    }

    #[test]
    fn forward_outcome_uses_er_change_across_return_range() {
        let residual_points = [0.0, 1.0, 2.0]
            .into_iter()
            .enumerate()
            .map(|(index, residual_pct)| ResidualReturnPoint {
                trade_date: format!("d{index}"),
                stock_pct: residual_pct,
                index_pct: 0.0,
                concept_pct: 0.0,
                industry_pct: 0.0,
                expected_pct: 0.0,
                residual_pct,
                stock_open_pct: residual_pct,
                index_open_pct: 0.0,
            })
            .collect::<Vec<_>>();
        let er_by_date = HashMap::from([("d0".to_string(), -0.10), ("d2".to_string(), 0.25)]);

        let outcomes =
            build_forward_backtest_outcome_map(residual_points, 2, &er_by_date, 0.0, 0.0, 0.0);
        let outcome = outcomes.get("d0").expect("d0 outcome");

        assert!((outcome.residual_return - 3.02).abs() < 1e-12);
        assert!((outcome.er_change - 0.35).abs() < 1e-12);
    }
}
