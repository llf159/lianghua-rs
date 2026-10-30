use super::prepare::CyqChenBatchBuilder;
pub(super) use super::prepare::prepare_cyq_chen_batch_rows;
use crate::data::cyq_chen_data::store::{
    drop_cyq_chen_db_indexes, ensure_cyq_chen_db_indexes, write_cyq_chen_meta,
};
use crate::data::cyq_chen_data::{
    CYQ_CHEN_BIN_TABLE, CYQ_CHEN_SNAPSHOT_TABLE, ComputedCyqChenStock, CyqChenCheckpointRecovery,
    CyqChenInitialState, CyqChenWriteBatch, CyqChenWriteMessage, DEFAULT_ADJ_TYPE,
    PreparedCyqChenWriteBatch, decode_cyq_chen_checkpoint,
};

use crate::data::DataReader;
use crate::data::RowData;
use crate::data::cyq_chen::ChenChipConfig;
use crate::data::cyq_chen::ChenChipFeatureState;
use crate::data::cyq_chen::CompiledChipChangeConfig;
use crate::data::cyq_chen::compute_chen_chip_snapshots_from_initial_bins_with_compiled_config_with_features;
use crate::data::cyq_chen::compute_chen_chip_snapshots_with_compiled_config_with_features;
use crate::data::cyq_chen::estimate_chen_chip_expression_warmup;
use crate::data::cyq_chen::round_chen_chip_value;
use crate::data::extras::inject_stock_extra_fields;
use crate::data::load_trade_date_list;
use duckdb::Appender;
use duckdb::Connection;
use duckdb::params;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::Receiver;
use std::time::{Duration, Instant};
pub(super) fn bucket_history_key(price_low: f64, price_high: f64) -> (u64, u64) {
    (
        round_chen_chip_value(price_low).to_bits(),
        round_chen_chip_value(price_high).to_bits(),
    )
}

pub(super) fn source_stock_data_exists(conn: &Connection) -> Result<bool, String> {
    crate::data::read_with_retry("读取行情表状态", || -> Result<_, String> {
        let table_exists = conn
            .query_row(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'stock_data'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map_err(|e| format!("检查stock_data表失败:{e}"))?;
        Ok(table_exists > 0)
    })
}

pub(super) fn query_source_trade_date_range(
    conn: &Connection,
) -> Result<Option<(String, String)>, String> {
    crate::data::read_with_retry("读取行情日期范围", || -> Result<_, String> {
        let mut stmt = conn
            .prepare("SELECT MIN(trade_date), MAX(trade_date) FROM stock_data WHERE adj_type = ?")
            .map_err(|e| format!("预编译筹码计算日期范围失败:{e}"))?;
        let mut rows = stmt
            .query(params![DEFAULT_ADJ_TYPE])
            .map_err(|e| format!("查询筹码计算日期范围失败:{e}"))?;
        let Some(row) = rows
            .next()
            .map_err(|e| format!("读取筹码计算日期范围失败:{e}"))?
        else {
            return Ok(None);
        };

        let min_trade_date: Option<String> =
            row.get(0).map_err(|e| format!("读取最早日期失败:{e}"))?;
        let max_trade_date: Option<String> =
            row.get(1).map_err(|e| format!("读取最晚日期失败:{e}"))?;

        Ok(match (min_trade_date, max_trade_date) {
            (Some(min_trade_date), Some(max_trade_date)) => Some((min_trade_date, max_trade_date)),
            _ => None,
        })
    })
}

pub(super) fn resolve_cyq_chen_rebuild_trade_date_range(
    conn: &Connection,
    requested_start_date: Option<&str>,
    requested_end_date: Option<&str>,
) -> Result<Option<(String, String)>, String> {
    let Some((source_min_trade_date, source_max_trade_date)) = query_source_trade_date_range(conn)?
    else {
        return Ok(None);
    };

    let requested_start_date = requested_start_date
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let requested_end_date = requested_end_date
        .map(str::trim)
        .filter(|value| !value.is_empty());

    let resolved_start_date = requested_start_date
        .map(|value| value.max(source_min_trade_date.as_str()).to_string())
        .unwrap_or_else(|| source_min_trade_date.clone());
    let resolved_end_date = requested_end_date
        .map(|value| value.min(source_max_trade_date.as_str()).to_string())
        .unwrap_or_else(|| source_max_trade_date.clone());

    if resolved_start_date > resolved_end_date {
        let requested_range = format!(
            "{} 至 {}",
            requested_start_date.unwrap_or(source_min_trade_date.as_str()),
            requested_end_date.unwrap_or(source_max_trade_date.as_str())
        );
        let source_range = format!("{source_min_trade_date} 至 {source_max_trade_date}");
        return Err(format!(
            "所选筹码计算区间 {requested_range} 与原始库可用区间 {source_range} 没有交集"
        ));
    }

    Ok(Some((resolved_start_date, resolved_end_date)))
}

pub fn resolve_cyq_chen_load_start_date(
    source_dir: &str,
    output_start_date: &str,
    output_end_date: &str,
    warmup_days: usize,
) -> Result<Option<String>, String> {
    let trade_dates = load_trade_date_list(source_dir)?;
    if trade_dates.is_empty() {
        return Ok(None);
    }

    let Some(first_output_trade_date_index) = trade_dates.iter().position(|trade_date| {
        let trade_date = trade_date.as_str();
        trade_date >= output_start_date && trade_date <= output_end_date
    }) else {
        return Ok(None);
    };

    if warmup_days == 0 {
        return Ok(Some(trade_dates[first_output_trade_date_index].clone()));
    }

    let load_start_index = first_output_trade_date_index.saturating_sub(warmup_days);
    Ok(Some(trade_dates[load_start_index].clone()))
}

pub(super) fn resolve_first_computable_output_date(
    row_data: &RowData,
    start_date: &str,
    end_date: &str,
    warmup_days: usize,
) -> Option<String> {
    row_data
        .trade_dates
        .iter()
        .enumerate()
        .find_map(|(index, trade_date)| {
            let trade_date_str = trade_date.as_str();
            if trade_date_str < start_date || trade_date_str > end_date {
                return None;
            }
            if index < warmup_days {
                return None;
            }
            Some(trade_date.clone())
        })
}

fn load_cyq_chen_group_rows(
    worker_reader: &DataReader,
    ts_group: &[String],
    load_start_date: &str,
    start_date: &str,
    end_date: &str,
    lookback: usize,
) -> Result<HashMap<String, RowData>, String> {
    let mut rows_map =
        worker_reader.load_batch(ts_group, DEFAULT_ADJ_TYPE, load_start_date, end_date)?;
    for ts_code in ts_group {
        let mut row_data = match rows_map.remove(ts_code.as_str()) {
            Some(r) => r,
            None => {
                let tail = worker_reader.load_one_tail_rows(
                    ts_code,
                    DEFAULT_ADJ_TYPE,
                    end_date,
                    lookback.max(1) * 2,
                )?;
                if tail.trade_dates.is_empty() {
                    RowData {
                        trade_dates: Vec::new(),
                        cols: HashMap::new(),
                    }
                } else {
                    tail
                }
            }
        };

        if !row_data.trade_dates.is_empty()
            && resolve_first_computable_output_date(&row_data, start_date, end_date, lookback)
                .is_none()
        {
            let output_rows = (|row_data: &RowData, start_date: &str, end_date: &str| -> usize {
                row_data
                    .trade_dates
                    .iter()
                    .filter(|trade_date| {
                        let trade_date = trade_date.as_str();
                        trade_date >= start_date && trade_date <= end_date
                    })
                    .count()
            })(&row_data, start_date, end_date);
            if output_rows > 0 {
                let need_rows = lookback.saturating_add(output_rows);
                if need_rows > 0 {
                    let tail = worker_reader.load_one_tail_rows(
                        ts_code,
                        DEFAULT_ADJ_TYPE,
                        end_date,
                        need_rows,
                    )?;
                    if !tail.trade_dates.is_empty() {
                        row_data = tail;
                    }
                }
            }
        }

        if row_data.trade_dates.is_empty() {
            let need_rows = lookback.max(60) * 2;
            if need_rows > 0 {
                let tail = worker_reader.load_one_tail_rows(
                    ts_code,
                    DEFAULT_ADJ_TYPE,
                    end_date,
                    need_rows,
                )?;
                if !tail.trade_dates.is_empty() {
                    row_data = tail;
                }
            }
        }

        rows_map.insert(ts_code.clone(), row_data);
    }
    Ok(rows_map)
}

pub fn compute_cyq_chen_stock_group_batch(
    worker_reader: &DataReader,
    state_conn: Option<&Connection>,
    load_start_date: &str,
    start_date: &str,
    end_date: &str,
    chip_config: &CompiledChipChangeConfig,
    config: ChenChipConfig,
    st_list: &HashSet<String>,
    total_share_map: &HashMap<String, f64>,
    ts_group: &[String],
    on_stock_done: Option<&dyn Fn(&str)>,
    checkpoint_recovery: Option<&CyqChenCheckpointRecovery<'_>>,
) -> Result<CyqChenWriteBatch, String> {
    let lookback = config
        .warmup_days
        .max(estimate_chen_chip_expression_warmup(chip_config)?)
        .max(usize::from(state_conn.is_some()));
    let needs_main_ratio_history = chip_config.sell_uses_main_ratio_history;
    let mut rows_map = load_cyq_chen_group_rows(
        worker_reader,
        ts_group,
        load_start_date,
        start_date,
        end_date,
        lookback,
    )?;
    let mut batch = CyqChenWriteBatch::default();
    for ts_code in ts_group {
        let row_data = rows_map
            .remove(ts_code)
            .ok_or_else(|| format!("缺少已加载行情: {ts_code}"))?;

        let stock = (|mut row_data: RowData,
                      state_conn: Option<&Connection>,
                      ts_code: &str,
                      start_date: &str,
                      end_date: &str,
                      chip_config: &CompiledChipChangeConfig,
                      config: ChenChipConfig,
                      st_list: &HashSet<String>,
                      total_share_map: &HashMap<String, f64>|
         -> Result<ComputedCyqChenStock, String> {
            if row_data.trade_dates.is_empty() {
                return Ok(ComputedCyqChenStock {
                    ts_code: ts_code.to_string(),
                    snapshots: Vec::new(),
                    replace_history: false,
                    feature_state: None,
                });
            }

            inject_stock_extra_fields(
                &mut row_data,
                ts_code,
                st_list.contains(ts_code),
                total_share_map.get(ts_code).copied(),
            )?;

            let Some(output_start_date) =
                resolve_first_computable_output_date(&row_data, start_date, end_date, 0)
            else {
                return Ok(ComputedCyqChenStock {
                    ts_code: ts_code.to_string(),
                    snapshots: Vec::new(),
                    replace_history: false,
                    feature_state: None,
                });
            };

            let output_start_index = row_data
                .trade_dates
                .iter()
                .position(|trade_date| trade_date == &output_start_date)
                .ok_or_else(|| format!("缺少新筹码输出起始日期: {output_start_date}"))?;
            let initial_state = match state_conn {
                Some(conn) => crate::data::read_with_retry("读取新筹码续算状态", || -> Result<Option<CyqChenInitialState>, String> {
                    let output_start_date = output_start_date.as_str();
                    let row_trade_dates = &row_data.trade_dates;
                    let checkpoint_state_date = conn
                        .query_row(
                            "SELECT trade_date FROM cyq_chen_checkpoint WHERE ts_code = ? AND adj_type = ?",
                            params![ts_code, DEFAULT_ADJ_TYPE],
                            |row| row.get::<_, String>(0),
                        )
                        .map(Some)
                        .or_else(|e| match e {
                            duckdb::Error::QueryReturnedNoRows => Ok(None),
                            other => Err(other),
                        })
                        .map_err(|e| format!("查询新筹码检查点日期失败, ts_code={ts_code}: {e}"))?;
                    let latest_state_date = if checkpoint_state_date
                        .as_deref()
                        .is_some_and(|date| date < output_start_date)
                    {
                        checkpoint_state_date
                    } else {
                        conn.query_row(
                            r#"
            SELECT MAX(trade_date)
            FROM cyq_chen_snapshot
            WHERE ts_code = ? AND adj_type = ? AND trade_date < ?
            "#,
                            params![ts_code, DEFAULT_ADJ_TYPE, output_start_date],
                            |row| row.get::<_, Option<String>>(0),
                        )
                        .map_err(|e| format!("查询新筹码最新状态失败, ts_code={ts_code}: {e}"))?
                    };
                    let Some(state_trade_date) = latest_state_date else {
                        return Ok(None);
                    };

                    let (bins_json, feature_json): (String, Option<String>) = conn.query_row(
                        "SELECT bins, feature_state FROM cyq_chen_checkpoint WHERE ts_code = ? AND adj_type = ? AND trade_date = ?",
                        params![ts_code, DEFAULT_ADJ_TYPE, state_trade_date.as_str()],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    ).map_err(|e| format!("读取新筹码续算检查点失败，请重建该股票筹码: {e}"))?;
                    // 不能用展示表的舍入分桶回退续算：它缺少后验库存和特征状态。
                    let (bins, feature_state) = decode_cyq_chen_checkpoint(
                        &state_trade_date, &bins_json, feature_json.as_deref(),
                    )?;
                    if output_start_index == 0
                        || row_trade_dates[output_start_index - 1] != state_trade_date
                    {
                        return Err(format!(
                            "{ts_code} 筹码检查点与行情日期不衔接，请重建该股票筹码"
                        ));
                    }

                    let mut bin_index_by_key = HashMap::new();
                    for (index, bin) in bins.iter().enumerate() {
                        bin_index_by_key
                            .insert(bucket_history_key(bin.price_low, bin.price_high), index);
                    }

                    let mut main_ratio_history: Vec<Arc<Vec<Option<f64>>>> = if needs_main_ratio_history {
                        (0..bins.len()).map(|_| Arc::new(vec![None; row_trade_dates.len()])).collect()
                    } else { Vec::new() };
                    if needs_main_ratio_history && output_start_index > 0 {
                        let history_start_date = row_trade_dates
                            .first()
                            .map(String::as_str)
                            .unwrap_or(state_trade_date.as_str());
                        let mut history_stmt = conn
                            .prepare(
                                r#"
                SELECT trade_date, price_low, price_high, main_chip, retail_chip
                FROM cyq_chen_bin
                WHERE ts_code = ?
                  AND adj_type = ?
                  AND trade_date >= ?
                  AND trade_date < ?
                ORDER BY trade_date ASC, bin_index ASC
                "#,
                            )
                            .map_err(|e| {
                                format!("预编译新筹码历史比例查询失败, ts_code={ts_code}: {e}")
                            })?;
                        let mut history_rows = history_stmt
                            .query(params![
                                ts_code,
                                DEFAULT_ADJ_TYPE,
                                history_start_date,
                                output_start_date
                            ])
                            .map_err(|e| {
                                format!("查询新筹码历史比例失败, ts_code={ts_code}: {e}")
                            })?;
                        let row_index_by_date = row_trade_dates
                            .iter()
                            .take(output_start_index)
                            .enumerate()
                            .map(|(index, trade_date)| (trade_date.as_str(), index))
                            .collect::<HashMap<_, _>>();

                        while let Some(row) = history_rows.next().map_err(|e| {
                            format!("读取新筹码历史比例失败, ts_code={ts_code}: {e}")
                        })? {
                            let trade_date: String = row.get(0).map_err(|e| {
                                format!("读取新筹码历史日期失败, ts_code={ts_code}: {e}")
                            })?;
                            let Some(row_index) =
                                row_index_by_date.get(trade_date.as_str()).copied()
                            else {
                                continue;
                            };
                            let price_low: f64 = row.get(1).map_err(|e| {
                                format!("读取新筹码历史分桶下沿失败, ts_code={ts_code}: {e}")
                            })?;
                            let price_high: f64 = row.get(2).map_err(|e| {
                                format!("读取新筹码历史分桶上沿失败, ts_code={ts_code}: {e}")
                            })?;
                            let main_chip: f64 = row.get(3).map_err(|e| {
                                format!("读取新筹码历史主力筹码失败, ts_code={ts_code}: {e}")
                            })?;
                            let retail_chip: f64 = row.get(4).map_err(|e| {
                                format!("读取新筹码历史散户筹码失败, ts_code={ts_code}: {e}")
                            })?;
                            let Some(bucket_index) = bin_index_by_key
                                .get(&bucket_history_key(price_low, price_high))
                                .copied()
                            else {
                                continue;
                            };
                            let total = main_chip + retail_chip;
                            Arc::make_mut(&mut main_ratio_history[bucket_index])[row_index] =
                                if total > 1e-10 {
                                    Some(main_chip / total)
                                } else {
                                    Some(0.0)
                                };
                        }
                    }

                    Ok(Some(CyqChenInitialState {
                        state_trade_date,
                        feature_state,
                        bins,
                        main_ratio_history,
                    }))
                }),
                None => Ok(None),
            };
            let initial_state = match initial_state {
                Ok(state) => state,
                Err(error) => {
                    let Some(recovery) = checkpoint_recovery else {
                        return Err(error);
                    };
                    (recovery.report)(ts_code, &error);
                    let conn = state_conn.ok_or_else(|| error.clone())?;
                    let first_date = recovery.first_date.get_or_init(|| crate::data::read_with_retry("读取筹码重放起点", || conn.query_row(
                        "SELECT MIN(trade_date) FROM cyq_chen_snapshot WHERE adj_type = ?",
                        [DEFAULT_ADJ_TYPE], |row| row.get(0),
                    ).map_err(|e| format!("读取筹码重放起点失败: {e}")))).clone()?;
                    let replay_load_start = resolve_cyq_chen_load_start_date(
                        recovery.source_dir, &first_date, end_date, lookback,
                    )?.ok_or_else(|| format!("缺少筹码重放交易日历，保留旧结果: {error}"))?;
                    let codes = [ts_code.to_string()];
                    let mut replay = compute_cyq_chen_stock_group_batch(
                        worker_reader, None, &replay_load_start, &first_date, end_date,
                        chip_config, config, st_list, total_share_map, &codes, None, None,
                    )?;
                    let mut stock = replay.stocks.pop().ok_or_else(|| format!(
                        "{ts_code} 重放未生成快照，保留旧结果；原续算错误: {error}"
                    ))?;
                    stock.replace_history = true;
                    return Ok(stock);
                }
            };

            let mut feature_state = ChenChipFeatureState::default();
            let snapshots = if let Some(initial_state) = initial_state {
                feature_state = initial_state.feature_state;
                let Some(continuation_start_date) = row_data
                    .trade_dates
                    .iter()
                    .find(|trade_date| {
                        trade_date.as_str() > initial_state.state_trade_date.as_str()
                    })
                    .cloned()
                else {
                    return Ok(ComputedCyqChenStock {
                        ts_code: ts_code.to_string(),
                        snapshots: Vec::new(),
                        replace_history: false,
                        feature_state: None,
                    });
                };
                compute_chen_chip_snapshots_from_initial_bins_with_compiled_config_with_features(
                    &row_data,
                    &continuation_start_date,
                    &initial_state.bins,
                    &initial_state.main_ratio_history,
                    chip_config,
                    config,
                    &mut feature_state,
                )?
            } else {
                if state_conn.is_none() {
                    if let Some(last_missing_index) = row_data
                        .cols
                        .get("TOR")
                        .and_then(|series| series.iter().rposition(Option::is_none))
                    {
                        row_data.trade_dates.drain(..=last_missing_index);
                        for series in row_data.cols.values_mut() {
                            series.drain(..=last_missing_index);
                        }
                    }
                }
                let Some(output_start_date) = resolve_first_computable_output_date(
                    &row_data,
                    start_date,
                    end_date,
                    config.warmup_days,
                ) else {
                    return Ok(ComputedCyqChenStock {
                        ts_code: ts_code.to_string(),
                        snapshots: Vec::new(),
                        replace_history: false,
                        feature_state: None,
                    });
                };
                compute_chen_chip_snapshots_with_compiled_config_with_features(
                    &row_data,
                    &output_start_date,
                    chip_config,
                    config,
                    &mut feature_state,
                )?
            };
            let snapshots = snapshots
                .into_iter()
                .filter(|snapshot| {
                    snapshot.trade_date.as_deref().is_some_and(|trade_date| {
                        trade_date >= start_date && trade_date <= end_date
                    })
                })
                .collect();

            Ok(ComputedCyqChenStock {
                ts_code: ts_code.to_string(),
                snapshots,
                replace_history: false,
                feature_state: Some(feature_state),
            })
        })(
            row_data,
            state_conn,
            ts_code,
            start_date,
            end_date,
            chip_config,
            config,
            st_list,
            total_share_map,
        )
        .map_err(|error| format!("ts_code={ts_code}: {error}"))?;
        if !stock.snapshots.is_empty() {
            batch.stocks.push(stock);
        }
        if let Some(on_stock_done) = on_stock_done {
            on_stock_done(ts_code);
        }
    }
    Ok(batch)
}

pub(super) fn compute_prepared_cyq_chen_stock_group_batch(
    worker_reader: &DataReader,
    load_start_date: &str,
    start_date: &str,
    end_date: &str,
    chip_config: &CompiledChipChangeConfig,
    config: ChenChipConfig,
    st_list: &HashSet<String>,
    total_share_map: &HashMap<String, f64>,
    ts_group: &[String],
    on_stock_done: Option<&dyn Fn(&str)>,
    continue_on_stock_error: bool,
    on_batch: &mut dyn FnMut(PreparedCyqChenWriteBatch) -> Result<(), String>,
) -> Result<(usize, usize), String> {
    let lookback = config
        .warmup_days
        .max(estimate_chen_chip_expression_warmup(chip_config)?);
    let compute_stock =
        |ts_code: &str, mut row: RowData| -> Result<PreparedCyqChenWriteBatch, String> {
            inject_stock_extra_fields(
                &mut row,
                ts_code,
                st_list.contains(ts_code),
                total_share_map.get(ts_code).copied(),
            )?;
            if let Some(last_missing) = row
                .cols
                .get("TOR")
                .and_then(|series| series.iter().rposition(Option::is_none))
            {
                row.trade_dates.drain(..=last_missing);
                for series in row.cols.values_mut() {
                    series.drain(..=last_missing);
                }
            }

            let mut builder = CyqChenBatchBuilder::new(row.trade_dates.len(), 0, config);
            if let Some(output_start) =
                resolve_first_computable_output_date(&row, start_date, end_date, config.warmup_days)
            {
                let mut state = ChenChipFeatureState::default();
                let mut last = None;
                crate::data::cyq_chen::replay_chen_chip_snapshots(
                    std::borrow::Cow::Owned(row),
                    &output_start,
                    chip_config,
                    config,
                    &mut state,
                    false,
                    |snapshot| {
                        if snapshot
                            .trade_date
                            .as_deref()
                            .is_some_and(|date| date >= start_date && date <= end_date)
                        {
                            if let Some(previous) = last.replace(snapshot) {
                                builder.append_snapshot(ts_code, previous)?;
                            }
                        }
                        Ok(())
                    },
                )?;
                if let Some(last) = last {
                    builder.finish_stock(ts_code, last, Some(&state), false)?;
                }
            }
            let mut batch = builder.finish()?;
            batch.completed_ts_codes = vec![ts_code.to_string()];
            Ok(batch)
        };
    let mut seen = HashSet::with_capacity(ts_group.len());
    let mut deferred = Vec::new();
    let mut snapshot_rows = 0usize;
    let mut bin_rows = 0usize;
    let fatal_error = std::cell::Cell::new(None::<String>);
    let mut handle_row = |ts_code: &str,
                          row: Result<RowData, String>,
                          allow_defer: bool,
                          seen: &mut HashSet<String>,
                          deferred: &mut Vec<String>|
     -> Result<(), String> {
        seen.insert(ts_code.to_string());
        let stock_result = match row {
            Ok(row) if allow_defer && row.trade_dates.len() < lookback => {
                deferred.push(ts_code.to_string());
                None
            }
            Ok(row) => Some(compute_stock(ts_code, row)),
            Err(error) => Some(Err(error)),
        };
        match stock_result {
            Some(Ok(batch)) => {
                snapshot_rows += batch.snapshot_rows;
                bin_rows += batch.bin_rows;
                if let Err(error) = on_batch(batch) {
                    fatal_error.set(Some(error.clone()));
                    return Err(error);
                }
            }
            Some(Err(error)) if continue_on_stock_error => {
                if let Err(error) = on_batch(PreparedCyqChenWriteBatch::with_error(ts_code, error))
                {
                    fatal_error.set(Some(error.clone()));
                    return Err(error);
                }
            }
            Some(Err(error)) => return Err(format!("ts_code={ts_code}: {error}")),
            None => return Ok(()),
        }
        if let Some(done) = on_stock_done {
            done(ts_code);
        }
        Ok(())
    };
    let query_result = worker_reader.load_batch_streaming(
        ts_group,
        DEFAULT_ADJ_TYPE,
        load_start_date,
        end_date,
        |ts_code, row| handle_row(&ts_code, row, true, &mut seen, &mut deferred),
    );
    if let Some(error) = fatal_error.take() {
        return Err(error);
    }
    if let Err(error) = query_result {
        if !continue_on_stock_error {
            return Err(error);
        }
        for ts_code in ts_group {
            if !seen.contains(ts_code) {
                deferred.push(ts_code.clone());
            }
        }
    }
    while let Some(ts_code) = deferred.pop() {
        let row = worker_reader
            .load_one_tail_rows(&ts_code, DEFAULT_ADJ_TYPE, end_date, lookback.max(1) * 2)
            .map_err(|error| format!("ts_code={ts_code}: {error}"));
        handle_row(&ts_code, row, false, &mut seen, &mut deferred)?;
    }
    Ok((snapshot_rows, bin_rows))
}

pub(super) fn append_prepared_cyq_chen_batch_rows(
    conn: &Connection,
    snapshot_app: &mut Appender<'_>,
    bin_app: &mut Appender<'_>,
    batch: PreparedCyqChenWriteBatch,
) -> Result<(usize, usize), String> {
    for ts_code in &batch.replace_ts_codes {
        // 与本轮增量共用事务：先删该股旧历史，再追加重放结果，失败一起回滚。
        for table in [
            "cyq_chen_checkpoint",
            CYQ_CHEN_BIN_TABLE,
            CYQ_CHEN_SNAPSHOT_TABLE,
        ] {
            conn.execute(
                &format!("DELETE FROM {table} WHERE ts_code = ? AND adj_type = ?"),
                params![ts_code, DEFAULT_ADJ_TYPE],
            )
            .map_err(|e| format!("替换重放股票筹码失败, ts_code={ts_code}: {e}"))?;
        }
    }
    if !batch.checkpoints.is_empty() {
        // 大字符串列的逐股 MERGE 更新成本高，删除和追加仍由调用方的事务一起提交。
        conn.execute(
            &format!(
                "DELETE FROM cyq_chen_checkpoint WHERE adj_type = ? AND ts_code IN ({})",
                std::iter::repeat_n("?", batch.checkpoints.len())
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            duckdb::params_from_iter(
                std::iter::once(DEFAULT_ADJ_TYPE).chain(
                    batch
                        .checkpoints
                        .iter()
                        .map(|(ts_code, _, _, _)| ts_code.as_str()),
                ),
            ),
        )
        .map_err(|e| format!("清理新筹码续算检查点失败: {e}"))?;
        let mut checkpoint_app = conn
            .appender("cyq_chen_checkpoint")
            .map_err(|e| format!("创建新筹码续算检查点写入器失败: {e}"))?;
        for (ts_code, trade_date, bins, feature_state) in batch.checkpoints {
            checkpoint_app
                .append_row(params![
                    ts_code,
                    DEFAULT_ADJ_TYPE,
                    trade_date,
                    bins,
                    feature_state
                ])
                .map_err(|e| format!("追加新筹码续算检查点失败: {e}"))?;
        }
        checkpoint_app
            .flush()
            .map_err(|e| format!("刷新新筹码续算检查点失败: {e}"))?;
    }
    if let Some(snapshot_batch) = batch.snapshot_batch {
        snapshot_app
            .append_record_batch(snapshot_batch)
            .map_err(|e| format!("批量写入cyq_chen_snapshot失败:{e}"))?;
    }
    if let Some(bin_batch) = batch.bin_batch {
        bin_app
            .append_record_batch(bin_batch)
            .map_err(|e| format!("批量写入cyq_chen_bin失败:{e}"))?;
    }
    Ok((batch.snapshot_rows, batch.bin_rows))
}

pub(super) fn finish_cyq_chen_write(
    compute_result: Result<(), String>,
    writer_result: Result<(usize, usize), String>,
) -> Result<(usize, usize), String> {
    let rows = writer_result?;
    compute_result?;
    Ok(rows)
}

pub(super) fn rewrite_unverified_cyq_batch(
    conn: &mut Connection,
    batches: &[PreparedCyqChenWriteBatch],
    codes: &[String],
    progress_prefix: Option<&str>,
) -> Result<(), String> {
    let tx = conn.transaction().map_err(|e| e.to_string())?;
    for table in [
        CYQ_CHEN_SNAPSHOT_TABLE,
        CYQ_CHEN_BIN_TABLE,
        "cyq_chen_checkpoint",
    ] {
        tx.execute(
            &format!(
                "DELETE FROM {table} WHERE adj_type = 'qfq' AND ts_code IN ({})",
                vec!["?"; codes.len()].join(",")
            ),
            duckdb::params_from_iter(codes.iter()),
        )
        .map_err(|e| format!("重试当前筹码批次时无法清理旧行，保留现场，表 {table}: {e}"))?;
    }
    if let Some(prefix) = progress_prefix {
        for suffix in ["done", "unverified"] {
            tx.execute(
                &format!(
                    "DELETE FROM {prefix}_{suffix} WHERE ts_code IN ({})",
                    vec!["?"; codes.len()].join(",")
                ),
                duckdb::params_from_iter(codes.iter()),
            )
            .map_err(|e| e.to_string())?;
        }
    }
    {
        let mut snapshots = tx
            .appender(CYQ_CHEN_SNAPSHOT_TABLE)
            .map_err(|e| e.to_string())?;
        let mut bins = tx.appender(CYQ_CHEN_BIN_TABLE).map_err(|e| e.to_string())?;
        for batch in batches {
            if let Some(prefix) = progress_prefix {
                for code in &batch.completed_ts_codes {
                    if prefix == "cyq_chen_maintenance" {
                        tx.execute(
                            "INSERT INTO cyq_chen_maintenance_done VALUES (?, ?)",
                            params![code, batch.replace_ts_codes.contains(code)],
                        )
                        .map_err(|e| e.to_string())?;
                    } else {
                        tx.execute("INSERT INTO cyq_chen_rebuild_done VALUES (?)", [code])
                            .map_err(|e| e.to_string())?;
                    }
                    tx.execute(
                        &format!("INSERT INTO {prefix}_unverified VALUES (?)"),
                        [code],
                    )
                    .map_err(|e| e.to_string())?;
                }
            }
            append_prepared_cyq_chen_batch_rows(&tx, &mut snapshots, &mut bins, batch.clone())?;
        }
        snapshots.flush().map_err(|e| e.to_string())?;
        bins.flush().map_err(|e| e.to_string())?;
    }
    tx.commit()
        .map_err(|e| format!("重新提交当前筹码批次失败:{e}"))
}

pub(super) fn write_cyq_chen_batches_from_channel(
    db_path: &str,
    rx: Receiver<CyqChenWriteMessage>,
    config: ChenChipConfig,
    strategy_hash: String,
    written_stocks: Option<Arc<AtomicUsize>>,
) -> Result<(usize, usize), String> {
    let mut conn = Connection::open(db_path).map_err(|e| format!("打开筹码库失败:{e}"))?;
    let occupied: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM cyq_chen_snapshot LIMIT 1)
            OR EXISTS(SELECT 1 FROM cyq_chen_bin LIMIT 1)
            OR EXISTS(SELECT 1 FROM cyq_chen_checkpoint LIMIT 1)
            OR EXISTS(SELECT 1 FROM cyq_chen_meta LIMIT 1)",
            [],
            |row| row.get(0),
        )
        .map_err(|e| format!("检查新筹码临时库失败:{e}"))?;
    let resumable = std::path::Path::new(db_path).file_name().and_then(|v| v.to_str())
        == Some(".cyq_chen.db.resume.tmp")
        && conn.query_row("SELECT count(*) FROM information_schema.tables WHERE table_name = 'cyq_chen_rebuild_done'", [], |row| row.get::<_, usize>(0))
            .map_err(|e| format!("检查重建进度表失败:{e}"))? == 1;
    if resumable {
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS cyq_chen_rebuild_error(ts_code VARCHAR PRIMARY KEY, error VARCHAR);
             CREATE TABLE IF NOT EXISTS cyq_chen_rebuild_unverified(ts_code VARCHAR PRIMARY KEY)",
        )
        .map_err(|e| format!("创建新筹码重建错误表失败:{e}"))?;
    }
    if occupied && !resumable {
        return Err("新筹码分批重建只允许写入空临时库，禁止覆盖已有数据".to_string());
    }
    let bulk = crate::data::bulk_write::BulkWriteSettings::apply(&conn)?;
    let database_threads = std::thread::available_parallelism()
        .map(|count| count.get().min(8))
        .unwrap_or(4);
    conn.execute_batch(&format!(
        "SET memory_limit = '4GB'; SET threads = {database_threads};"
    ))
    .map_err(|e| format!("设置新筹码写库资源上限失败:{e}"))?;
    drop_cyq_chen_db_indexes(&conn)?;
    let writer_connections = (0..2)
        .map(|_| {
            conn.try_clone()
                .map_err(|e| format!("创建新筹码并行写库连接失败:{e}"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let shared_rx = Arc::new(Mutex::new(rx));
    let batch_access = Arc::new(RwLock::new(()));
    let aborted = Arc::new(AtomicBool::new(false));
    let writer_handles = writer_connections
        .into_iter()
        .map(|writer_conn| {
            let worker_rx = Arc::clone(&shared_rx);
            let written_stocks = written_stocks.clone();
            let batch_access = Arc::clone(&batch_access);
            let aborted = Arc::clone(&aborted);
            let db_path = db_path.to_string();
            std::thread::spawn(move || {
                let result = write_cyq_chen_batch_groups(
                    writer_conn,
                    worker_rx,
                    #[cfg(test)]
                    config,
                    written_stocks,
                    resumable,
                    &db_path,
                    &batch_access,
                    &aborted,
                );
                if result.is_err() {
                    aborted.store(true, Ordering::Relaxed);
                }
                result
            })
        })
        .collect::<Vec<_>>();
    drop(shared_rx);
    let mut snapshot_rows = 0usize;
    let mut bin_rows = 0usize;
    let mut writer_error = None;
    for writer_handle in writer_handles {
        match writer_handle.join() {
            Ok(Ok((snapshots, bins))) => {
                snapshot_rows += snapshots;
                bin_rows += bins;
            }
            Ok(Err(error)) => {
                if writer_error
                    .as_deref()
                    .is_none_or(|previous: &str| previous.starts_with("其他筹码批次失败"))
                {
                    writer_error = Some(error);
                }
            }
            Err(_) => {
                writer_error.get_or_insert("新筹码并行写库线程异常退出".to_string());
            }
        }
    }
    if let Some(error) = writer_error {
        return Err(error);
    }
    // 续算返回整库总量，而非仅本次新增行数。
    if resumable {
        (snapshot_rows, bin_rows) = conn.query_row(
            "SELECT (SELECT count(*) FROM cyq_chen_snapshot), (SELECT count(*) FROM cyq_chen_bin)",
            [], |row| Ok((row.get(0)?, row.get(1)?)),
        ).map_err(|e| format!("统计重建结果失败:{e}"))?;
    }
    let tx = conn
        .transaction()
        .map_err(|e| format!("创建筹码元数据事务失败:{e}"))?;
    write_cyq_chen_meta(&tx, config, &strategy_hash)?;
    tx.commit().map_err(|e| format!("提交筹码元数据失败:{e}"))?;
    let started = Instant::now();
    ensure_cyq_chen_db_indexes(&conn)?;
    if std::env::var_os("LIANGHUA_CYQ_WRITE_PROFILE").is_some() {
        eprintln!("新筹码建索引 index_ms={}", started.elapsed().as_millis());
    }
    let started = Instant::now();
    conn.execute_batch("CHECKPOINT")
        .map_err(|e| format!("检查点新筹码库失败:{e}"))?;
    if std::env::var_os("LIANGHUA_CYQ_WRITE_PROFILE").is_some() {
        eprintln!(
            "新筹码最终落盘 checkpoint_ms={}",
            started.elapsed().as_millis()
        );
    }
    bulk.finish(&conn, Ok((snapshot_rows, bin_rows)))
}

fn write_cyq_chen_batch_groups(
    mut conn: Connection,
    rx: Arc<Mutex<Receiver<CyqChenWriteMessage>>>,
    #[cfg(test)] config: ChenChipConfig,
    written_stocks: Option<Arc<AtomicUsize>>,
    resumable: bool,
    db_path: &str,
    batch_access: &RwLock<()>,
    aborted: &AtomicBool,
) -> Result<(usize, usize), String> {
    let mut snapshot_rows = 0usize;
    let mut bin_rows = 0usize;
    let mut drained = false;
    let profile = std::env::var_os("LIANGHUA_CYQ_WRITE_PROFILE").is_some();
    while !drained {
        if aborted.load(Ordering::Relaxed) {
            return Err("其他筹码批次失败，停止接收后续计算".into());
        }
        let writing = batch_access.read().map_err(|_| "新筹码批次校验锁已损坏")?;
        let tx = conn
            .transaction()
            .map_err(|e| format!("创建筹码批次事务失败:{e}"))?;
        let mut wait_time = Duration::ZERO;
        let mut append_time = Duration::ZERO;
        let mut group_codes = Vec::new();
        let mut retry_batches = Vec::new();
        let (added_stocks, added_snapshot_rows, added_bin_rows, flush_time) = {
            let mut snapshot_app = tx
                .appender(CYQ_CHEN_SNAPSHOT_TABLE)
                .map_err(|e| format!("创建cyq_chen_snapshot写入器失败:{e}"))?;
            let mut bin_app = tx
                .appender(CYQ_CHEN_BIN_TABLE)
                .map_err(|e| format!("创建cyq_chen_bin写入器失败:{e}"))?;
            let mut group_batches = 0usize;
            let mut group_stocks = 0usize;
            let mut group_snapshot_rows = 0usize;
            let mut group_bin_rows = 0usize;
            while group_batches < 128 && group_stocks < 64 && group_bin_rows < 1_000_000 {
                if aborted.load(Ordering::Relaxed) {
                    return Err("其他筹码批次失败，当前未提交批次已回滚".into());
                }
                let started = Instant::now();
                let message = rx
                    .lock()
                    .map_err(|_| "新筹码写库接收队列异常".to_string())?
                    .recv();
                wait_time += started.elapsed();
                let Ok(message) = message else {
                    drained = true;
                    break;
                };
                let batch = match message {
                    #[cfg(test)]
                    CyqChenWriteMessage::Batch(batch) => {
                        prepare_cyq_chen_batch_rows(batch, config)?
                    }
                    CyqChenWriteMessage::PreparedBatch(batch) => batch,
                    CyqChenWriteMessage::Abort(reason) => {
                        return Err(format!("筹码计算中断，临时库不发布:{reason}"));
                    }
                    CyqChenWriteMessage::Finish => {
                        return Err("临时库写入不接受增量提交标记".into());
                    }
                };
                group_codes.extend(batch.completed_ts_codes.iter().cloned());
                group_codes.extend(batch.checkpoints.iter().map(|row| row.0.clone()));
                if !batch.errors.is_empty() {
                    if !resumable {
                        return Err("新筹码个股错误只能保存在重建临时库".to_string());
                    }
                    for error in &batch.errors {
                        tx.execute(
                            "INSERT OR REPLACE INTO cyq_chen_rebuild_error VALUES (?, ?)",
                            params![&error.ts_code, &error.error],
                        )
                        .map_err(|e| {
                            format!("保存新筹码个股错误失败, ts_code={}: {e}", error.ts_code)
                        })?;
                    }
                }
                if resumable {
                    for ts_code in &batch.completed_ts_codes {
                        tx.execute(
                            "DELETE FROM cyq_chen_rebuild_error WHERE ts_code = ?",
                            [ts_code],
                        )
                        .map_err(|e| format!("清理已修复的新筹码个股错误失败:{e}"))?;
                        tx.execute("INSERT INTO cyq_chen_rebuild_done VALUES (?)", [ts_code])
                            .map_err(|e| format!("保存重建股票进度失败:{e}"))?;
                        tx.execute(
                            "INSERT INTO cyq_chen_rebuild_unverified VALUES (?)",
                            [ts_code],
                        )
                        .map_err(|e| format!("保存重建待校验股票失败:{e}"))?;
                    }
                }
                group_stocks += if resumable {
                    batch.completed_ts_codes.len()
                } else {
                    batch.stock_count
                };
                let started = Instant::now();
                retry_batches.push(batch.clone());
                let (snapshots, bins) = append_prepared_cyq_chen_batch_rows(
                    &tx,
                    &mut snapshot_app,
                    &mut bin_app,
                    batch,
                )?;
                append_time += started.elapsed();
                group_batches += 1;
                group_snapshot_rows += snapshots;
                group_bin_rows += bins;
            }
            let started = Instant::now();
            snapshot_app
                .flush()
                .map_err(|e| format!("刷新cyq_chen_snapshot失败:{e}"))?;
            bin_app
                .flush()
                .map_err(|e| format!("刷新cyq_chen_bin失败:{e}"))?;
            (
                group_stocks,
                group_snapshot_rows,
                group_bin_rows,
                started.elapsed(),
            )
        };
        let started = Instant::now();
        tx.commit().map_err(|e| format!("提交筹码批次失败:{e}"))?;
        let commit_time = started.elapsed();
        drop(writing);
        let started = Instant::now();
        if added_stocks > 0 {
            group_codes.sort_unstable();
            group_codes.dedup();
            let _verifying = batch_access.write().map_err(|_| "新筹码批次校验锁已损坏")?;
            for attempt in 0..=2 {
                conn.execute_batch("CHECKPOINT")
                    .map_err(|e| format!("落盘筹码提交批次失败:{e}"))?;
                match crate::data::read_with_retry("回读当前筹码提交批次", || {
                    super::store::verify_cyq_chen_db(
                        std::path::Path::new(db_path),
                        Some(&group_codes),
                    )
                }) {
                    Ok(()) => break,
                    Err(error)
                        if attempt < 2
                            && crate::data::read_retry::is_retryable_read_error(&error) =>
                    {
                        eprintln!(
                            "当前筹码批次校验失败，仅重写本批 {} 只股票，重试 {}/2: {error}",
                            group_codes.len(),
                            attempt + 1
                        );
                        rewrite_unverified_cyq_batch(
                            &mut conn,
                            &retry_batches,
                            &group_codes,
                            resumable.then_some("cyq_chen_rebuild"),
                        )?;
                    }
                    Err(error) => return Err(error),
                }
            }
            if resumable {
                let tx = conn.transaction().map_err(|e| e.to_string())?;
                for code in &group_codes {
                    tx.execute(
                        "DELETE FROM cyq_chen_rebuild_unverified WHERE ts_code = ?",
                        [code],
                    )
                    .map_err(|e| format!("确认重建批次校验完成失败:{e}"))?;
                }
                tx.commit().map_err(|e| e.to_string())?;
            }
        }
        let verify_time = started.elapsed();
        if let Some(written_stocks) = &written_stocks {
            written_stocks.fetch_add(added_stocks, Ordering::Relaxed);
        }
        if profile && added_stocks > 0 {
            eprintln!(
                "新筹码写库 stocks={added_stocks} snapshots={added_snapshot_rows} bins={added_bin_rows} wait_ms={} append_ms={} flush_ms={} commit_ms={} verify_ms={}",
                wait_time.as_millis(),
                append_time.as_millis(),
                flush_time.as_millis(),
                commit_time.as_millis(),
                verify_time.as_millis(),
            );
        }
        snapshot_rows += added_snapshot_rows;
        bin_rows += added_bin_rows;
    }
    Ok((snapshot_rows, bin_rows))
}

#[cfg(test)]
mod tests {
    use crate::data::cyq_chen::ChenChipConfig;
    use crate::data::cyq_chen_data::compute::finish_cyq_chen_write;
    use crate::data::cyq_chen_data::compute::write_cyq_chen_batches_from_channel;
    use crate::data::cyq_chen_data::test_support::*;
    use crate::data::cyq_chen_data::{
        ComputedCyqChenStock, CyqChenWriteBatch, CyqChenWriteMessage, store::init_cyq_chen_db,
    };
    use std::fs;
    use std::sync::mpsc::sync_channel;

    #[test]
    fn batch_retry_and_resume_preserve_other_stocks_without_duplicates() {
        use duckdb::Connection;

        let source_dir = unique_temp_source_dir();
        prepare_source_db(&source_dir);
        let source = source_dir.to_str().unwrap();
        let reader = crate::data::DataReader::new(source).unwrap();
        let config = ChenChipConfig {
            warmup_days: 2,
            bucket_pct: 5.0,
        };
        let chip = crate::data::cyq_chen::load_compiled_chip_change_config(source).unwrap();
        let codes = vec!["000001.SZ".to_string()];
        let batch = super::prepare_cyq_chen_batch_rows(
            super::compute_cyq_chen_stock_group_batch(
                &reader,
                None,
                "20260401",
                "20260401",
                "20260408",
                &chip,
                config,
                &std::collections::HashSet::new(),
                &std::collections::HashMap::new(),
                &codes,
                None,
                None,
            )
            .unwrap(),
            config,
        )
        .unwrap();
        for prefix in ["cyq_chen_rebuild", "cyq_chen_maintenance"] {
            let path = source_dir.join(format!("{prefix}.db"));
            init_cyq_chen_db(&path).unwrap();
            let mut conn = Connection::open(&path).unwrap();
            conn.execute_batch(&format!(
                "CREATE TABLE {prefix}_done(ts_code VARCHAR PRIMARY KEY{});
                 CREATE TABLE {prefix}_unverified(ts_code VARCHAR PRIMARY KEY);
                 INSERT INTO cyq_chen_snapshot(ts_code, trade_date, adj_type) VALUES ('000002.SZ','20260401','qfq');
                 INSERT INTO cyq_chen_bin(ts_code, trade_date, adj_type) VALUES ('000002.SZ','20260401','qfq');
                 INSERT INTO cyq_chen_checkpoint(ts_code, adj_type) VALUES ('000002.SZ','qfq');
                 INSERT INTO {prefix}_done VALUES ('000002.SZ'{});",
                if prefix.ends_with("maintenance") { ", replace_history BOOLEAN" } else { "" },
                if prefix.ends_with("maintenance") { ", false" } else { "" },
            )).unwrap();
            for _ in 0..2 {
                super::rewrite_unverified_cyq_batch(
                    &mut conn,
                    std::slice::from_ref(&batch),
                    &codes,
                    Some(prefix),
                )
                .unwrap();
                for (table, expected) in [
                    ("cyq_chen_snapshot", batch.snapshot_rows),
                    ("cyq_chen_bin", batch.bin_rows),
                    ("cyq_chen_checkpoint", 1),
                ] {
                    let count: usize = conn
                        .query_row(
                            &format!("SELECT count(*) FROM {table} WHERE ts_code = '000001.SZ'"),
                            [],
                            |row| row.get(0),
                        )
                        .unwrap();
                    assert_eq!(count, expected, "{prefix}: {table}");
                }
            }
            conn.execute_batch("CHECKPOINT").unwrap();
            super::super::store::verify_cyq_chen_db(&path, Some(&codes)).unwrap();
            super::super::store::reset_unverified_cyq_batches(&mut conn, prefix).unwrap();
            for table in ["cyq_chen_snapshot", "cyq_chen_bin", "cyq_chen_checkpoint"] {
                let remaining: Vec<String> = conn
                    .prepare(&format!("SELECT ts_code FROM {table}"))
                    .unwrap()
                    .query_map([], |row| row.get(0))
                    .unwrap()
                    .collect::<Result<_, _>>()
                    .unwrap();
                assert_eq!(remaining, vec!["000002.SZ"], "{prefix}: {table}");
            }
            let done: String = conn
                .query_row(&format!("SELECT ts_code FROM {prefix}_done"), [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(done, "000002.SZ");
        }
        drop(reader);
        fs::remove_dir_all(source_dir).unwrap();
    }

    #[test]
    fn streamed_columns_match_collected_snapshots_with_posterior_checkpoint() {
        let source_dir = unique_temp_source_dir();
        prepare_source_db(&source_dir);
        let source = source_dir.to_str().unwrap();
        let reader = crate::data::DataReader::new(source).unwrap();
        let mut chip = crate::data::cyq_chen::ChipChangeConfig::load(source).unwrap();
        let mut confirmation = chip.strategy[0].clone();
        confirmation.name = "later attribution".into();
        confirmation.holder = crate::data::cyq_chen::ChipHolder::Retail;
        confirmation.confirm_after = 2;
        chip.strategy.push(confirmation);
        let compiled = chip.compile().unwrap();
        let config = ChenChipConfig {
            warmup_days: 2,
            bucket_pct: 5.0,
        };
        let codes = vec!["000001.SZ".to_string(), "000002.SZ".to_string()];
        let mut reference = compiled.clone();
        reference.sell_uses_main_ratio_history = true;
        let collected = super::compute_cyq_chen_stock_group_batch(
            &reader,
            None,
            "20260401",
            "20260401",
            "20260408",
            &reference,
            config,
            &std::collections::HashSet::new(),
            &std::collections::HashMap::new(),
            &codes,
            None,
            None,
        )
        .unwrap();
        assert!(
            collected
                .stocks
                .iter()
                .flat_map(|s| &s.snapshots)
                .flat_map(|s| &s.bins)
                .any(|b| !b.pending.is_empty())
        );
        let mut actual = Vec::new();
        super::compute_prepared_cyq_chen_stock_group_batch(
            &reader,
            "20260401",
            "20260401",
            "20260408",
            &compiled,
            config,
            &std::collections::HashSet::new(),
            &std::collections::HashMap::new(),
            &codes,
            None,
            false,
            &mut |batch| {
                actual.push(batch);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(actual.len(), codes.len());
        for ts_code in &codes {
            let one = vec![ts_code.clone()];
            let expected = super::prepare_cyq_chen_batch_rows(
                super::compute_cyq_chen_stock_group_batch(
                    &reader,
                    None,
                    "20260401",
                    "20260401",
                    "20260408",
                    &reference,
                    config,
                    &std::collections::HashSet::new(),
                    &std::collections::HashMap::new(),
                    &one,
                    None,
                    None,
                )
                .unwrap(),
                config,
            )
            .unwrap();
            let actual = actual
                .iter()
                .find(|batch| batch.completed_ts_codes == one)
                .unwrap();
            assert_eq!(actual.snapshot_batch, expected.snapshot_batch);
            assert_eq!(actual.bin_batch, expected.bin_batch);
            assert_eq!(actual.checkpoints, expected.checkpoints);
            assert_eq!(actual.snapshot_rows, expected.snapshot_rows);
            assert_eq!(actual.bin_rows, expected.bin_rows);
            assert_eq!(actual.stock_count, expected.stock_count);
        }
        drop(reader);
        fs::remove_dir_all(source_dir).unwrap();
    }

    #[test]
    fn parallel_full_writer_persists_distinct_stocks() {
        let source_dir = unique_temp_source_dir();
        prepare_source_db(&source_dir);
        let reader = crate::data::DataReader::new(source_dir.to_str().unwrap()).unwrap();
        let row = reader
            .load_one("000001.SZ", "qfq", "20260401", "20260408")
            .unwrap();
        let config = ChenChipConfig {
            warmup_days: 2,
            bucket_pct: 5.0,
        };
        let mut state = crate::data::cyq_chen::ChenChipFeatureState::default();
        let snapshot =
            crate::data::cyq_chen::compute_chen_chip_snapshots_with_compiled_config_with_features(
                &row,
                "20260403",
                &crate::data::cyq_chen::ChipChangeConfig {
                    version: 1,
                    strategy: Vec::new(),
                }
                .compile()
                .unwrap(),
                config,
                &mut state,
            )
            .unwrap()
            .pop()
            .unwrap();
        let stage = source_dir.join("parallel-stage.db");
        init_cyq_chen_db(&stage).unwrap();
        let (tx, rx) = sync_channel(64);
        for index in 0..64 {
            tx.send(CyqChenWriteMessage::Batch(CyqChenWriteBatch {
                stocks: vec![ComputedCyqChenStock {
                    ts_code: format!("{index:06}.SZ"),
                    snapshots: vec![snapshot.clone()],
                    replace_history: false,
                    feature_state: Some(state.clone()),
                }],
            }))
            .unwrap();
        }
        drop(tx);
        let written_stocks = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let result = write_cyq_chen_batches_from_channel(
            stage.to_str().unwrap(),
            rx,
            config,
            "test".to_string(),
            Some(std::sync::Arc::clone(&written_stocks)),
        )
        .unwrap();
        assert_eq!(result, (64, 64 * snapshot.bins.len()));
        assert_eq!(
            written_stocks.load(std::sync::atomic::Ordering::Relaxed),
            64
        );
        let conn = duckdb::Connection::open(&stage).unwrap();
        let stored: i64 = conn
            .query_row("SELECT COUNT(*) FROM cyq_chen_checkpoint", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(stored, 64);
        drop(conn);
        drop(reader);
        fs::remove_dir_all(source_dir).unwrap();
    }

    #[test]
    fn writer_failure_is_not_hidden_by_closed_channel() {
        let source_dir = unique_temp_source_dir();
        fs::create_dir_all(&source_dir).unwrap();
        let (tx, rx) = sync_channel(1);
        let writer_result = write_cyq_chen_batches_from_channel(
            source_dir.to_str().unwrap(),
            rx,
            ChenChipConfig::default(),
            String::new(),
            None,
        );
        let compute_result = tx
            .send(CyqChenWriteMessage::Abort("unused".to_string()))
            .map_err(|e| format!("发送筹码批次失败:{e}"));
        assert!(compute_result.is_err());
        let error = finish_cyq_chen_write(compute_result, writer_result).unwrap_err();
        assert!(error.contains("打开筹码库失败"), "{error}");
        assert!(!error.contains("sending on a closed channel"), "{error}");
        fs::remove_dir_all(source_dir).unwrap();
    }
}
