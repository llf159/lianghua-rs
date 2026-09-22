use crate::data::cyq_chen_data::store::{
    drop_cyq_chen_db_indexes, ensure_cyq_chen_db_indexes, write_cyq_chen_meta,
};
use crate::data::cyq_chen_data::{
    CYQ_CHEN_BIN_TABLE, CYQ_CHEN_FLUSH_BATCH_SIZE, CYQ_CHEN_SNAPSHOT_TABLE, ComputedCyqChenStock,
    CyqChenInitialState, CyqChenWriteBatch, CyqChenWriteMessage, DEFAULT_ADJ_TYPE,
};

use crate::data::DataReader;
use crate::data::RowData;
use crate::data::cyq_chen::ChenChipBin;
use crate::data::cyq_chen::ChenChipConfig;
use crate::data::cyq_chen::ChipDirection;
use crate::data::cyq_chen::CompiledChipChangeConfig;
use crate::data::cyq_chen::compute_chen_chip_snapshots_from_initial_bins_with_compiled_config;
use crate::data::cyq_chen::compute_chen_chip_snapshots_with_compiled_config;
use crate::data::cyq_chen::estimate_chen_chip_expression_warmup;
use crate::data::cyq_chen::round_chen_chip_snapshot;
use crate::data::cyq_chen::round_chen_chip_value;
use crate::data::expr_program_uses_runtime_key;
use crate::data::extras::inject_stock_extra_fields;
use crate::data::load_trade_date_list;
use duckdb::Appender;
use duckdb::Connection;
use duckdb::arrow::array::ArrayRef;
use duckdb::arrow::array::Float64Array;
use duckdb::arrow::array::Int32Array;
use duckdb::arrow::array::StringArray;
use duckdb::arrow::array::builder::StringBuilder;
use duckdb::arrow::datatypes::DataType;
use duckdb::arrow::datatypes::Field;
use duckdb::arrow::datatypes::Schema;
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::params;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::mpsc::Receiver;
pub(super) fn bucket_history_key(price_low: f64, price_high: f64) -> (u64, u64) {
    (
        round_chen_chip_value(price_low).to_bits(),
        round_chen_chip_value(price_high).to_bits(),
    )
}

pub(super) fn source_stock_data_exists(conn: &Connection) -> Result<bool, String> {
    let table_exists = conn
        .query_row(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'stock_data'",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map_err(|e| format!("检查stock_data表失败:{e}"))?;
    Ok(table_exists > 0)
}

pub(super) fn query_source_trade_date_range(
    conn: &Connection,
) -> Result<Option<(String, String)>, String> {
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

    let min_trade_date: Option<String> = row.get(0).map_err(|e| format!("读取最早日期失败:{e}"))?;
    let max_trade_date: Option<String> = row.get(1).map_err(|e| format!("读取最晚日期失败:{e}"))?;

    Ok(match (min_trade_date, max_trade_date) {
        (Some(min_trade_date), Some(max_trade_date)) => Some((min_trade_date, max_trade_date)),
        _ => None,
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

pub(super) fn resolve_cyq_chen_load_start_date(
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

pub(super) fn compute_cyq_chen_stock_group_batch(
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
) -> Result<CyqChenWriteBatch, String> {
    let lookback = config
        .warmup_days
        .max(estimate_chen_chip_expression_warmup(chip_config)?);
    let needs_main_ratio_history = chip_config.strategies.iter().any(|strategy| {
        strategy.direction == ChipDirection::Sell
            && expr_program_uses_runtime_key(&strategy.when_ast, "MAIN_CHIP_RATIO")
    });
    let mut rows_map =
        worker_reader.load_batch(ts_group, DEFAULT_ADJ_TYPE, load_start_date, end_date)?;
    let mut batch = CyqChenWriteBatch::default();
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
                });
            };

            let output_start_index = row_data
                .trade_dates
                .iter()
                .position(|trade_date| trade_date == &output_start_date)
                .ok_or_else(|| format!("缺少新筹码输出起始日期: {output_start_date}"))?;
            let initial_state = match state_conn {
                Some(conn) => (|conn: &Connection,
                                ts_code: &str,
                                output_start_date: &str,
                                row_trade_dates: &[String],
                                output_start_index: usize|
                 -> Result<Option<CyqChenInitialState>, String> {
                    let latest_state_date = conn
                        .query_row(
                            r#"
            SELECT MAX(trade_date)
            FROM cyq_chen_snapshot
            WHERE ts_code = ? AND adj_type = ? AND trade_date < ?
            "#,
                            params![ts_code, DEFAULT_ADJ_TYPE, output_start_date],
                            |row| row.get::<_, Option<String>>(0),
                        )
                        .map_err(|e| format!("查询新筹码最新状态失败, ts_code={ts_code}: {e}"))?;
                    let Some(state_trade_date) = latest_state_date else {
                        return Ok(None);
                    };

                    let checkpoint = conn.query_row(
                        "SELECT bins FROM cyq_chen_checkpoint WHERE ts_code = ? AND adj_type = ? AND trade_date = ?",
                        params![ts_code, DEFAULT_ADJ_TYPE, state_trade_date.as_str()],
                        |row| row.get::<_, String>(0),
                    ).map(Some).or_else(|e| match e { duckdb::Error::QueryReturnedNoRows => Ok(None), other => Err(other) })
                        .map_err(|e| format!("读取新筹码续算检查点失败: {e}"))?;
                    let bins = if let Some(checkpoint) = checkpoint {
                        serde_json::from_str(&checkpoint)
                            .map_err(|e| format!("新筹码检查点损坏: {e}"))?
                    } else {
                        if chip_config
                            .strategies
                            .iter()
                            .any(|rule| rule.confirm_after > 0)
                        {
                            return Err(format!("{ts_code} 缺少后验续算检查点，请重建该股票筹码"));
                        }
                        let mut stmt = conn
                            .prepare(
                                r#"
            SELECT bin_index, price, price_low, price_high, main_chip, retail_chip, total_chip
            FROM cyq_chen_bin
            WHERE ts_code = ? AND adj_type = ? AND trade_date = ?
            ORDER BY bin_index ASC
            "#,
                            )
                            .map_err(|e| {
                                format!("预编译新筹码状态分桶查询失败, ts_code={ts_code}: {e}")
                            })?;
                        let mut rows = stmt
                            .query(params![
                                ts_code,
                                DEFAULT_ADJ_TYPE,
                                state_trade_date.as_str()
                            ])
                            .map_err(|e| {
                                format!("查询新筹码状态分桶失败, ts_code={ts_code}: {e}")
                            })?;
                        let mut bins = Vec::new();
                        while let Some(row) = rows.next().map_err(|e| {
                            format!("读取新筹码状态分桶失败, ts_code={ts_code}: {e}")
                        })? {
                            let index_i64: i64 = row.get(0).map_err(|e| {
                                format!("读取新筹码分桶序号失败, ts_code={ts_code}: {e}")
                            })?;
                            bins.push(ChenChipBin {
                                pending: Vec::new(),
                                index: index_i64.max(0) as usize,
                                price: row.get(1).map_err(|e| {
                                    format!("读取新筹码分桶价格失败, ts_code={ts_code}: {e}")
                                })?,
                                price_low: row.get(2).map_err(|e| {
                                    format!("读取新筹码分桶下沿失败, ts_code={ts_code}: {e}")
                                })?,
                                price_high: row.get(3).map_err(|e| {
                                    format!("读取新筹码分桶上沿失败, ts_code={ts_code}: {e}")
                                })?,
                                main_chip: row.get(4).map_err(|e| {
                                    format!("读取新筹码主力筹码失败, ts_code={ts_code}: {e}")
                                })?,
                                retail_chip: row.get(5).map_err(|e| {
                                    format!("读取新筹码散户筹码失败, ts_code={ts_code}: {e}")
                                })?,
                                total_chip: row.get(6).map_err(|e| {
                                    format!("读取新筹码总筹码失败, ts_code={ts_code}: {e}")
                                })?,
                            });
                        }
                        bins
                    };
                    if bins.is_empty() {
                        return Ok(None);
                    }

                    let mut bin_index_by_key = HashMap::new();
                    for (index, bin) in bins.iter().enumerate() {
                        bin_index_by_key
                            .insert(bucket_history_key(bin.price_low, bin.price_high), index);
                    }

                    let mut main_ratio_history: Vec<Arc<Vec<Option<f64>>>> = (0..bins.len())
                        .map(|_| Arc::new(vec![None; row_trade_dates.len()]))
                        .collect();
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
                        bins,
                        main_ratio_history,
                    }))
                })(
                    conn,
                    ts_code,
                    &output_start_date,
                    &row_data.trade_dates,
                    output_start_index,
                )?,
                None => None,
            };

            let snapshots = if let Some(initial_state) = initial_state {
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
                    });
                };
                compute_chen_chip_snapshots_from_initial_bins_with_compiled_config(
                    &row_data,
                    &continuation_start_date,
                    &initial_state.bins,
                    &initial_state.main_ratio_history,
                    chip_config,
                    config,
                )?
            } else {
                let Some(output_start_date) = resolve_first_computable_output_date(
                    &row_data,
                    start_date,
                    end_date,
                    config.warmup_days,
                ) else {
                    return Ok(ComputedCyqChenStock {
                        ts_code: ts_code.to_string(),
                        snapshots: Vec::new(),
                    });
                };
                compute_chen_chip_snapshots_with_compiled_config(
                    &row_data,
                    &output_start_date,
                    chip_config,
                    config,
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
        )?;
        if !stock.snapshots.is_empty() {
            batch.stocks.push(stock);
        }
        if let Some(on_stock_done) = on_stock_done {
            on_stock_done(ts_code);
        }
    }
    Ok(batch)
}

pub(super) fn append_cyq_chen_batch_rows(
    conn: &Connection,
    snapshot_app: &mut Appender<'_>,
    bin_app: &mut Appender<'_>,
    batch: CyqChenWriteBatch,
    config: ChenChipConfig,
) -> Result<(usize, usize), String> {
    let snapshot_rows = batch
        .stocks
        .iter()
        .map(|stock| stock.snapshots.len())
        .sum::<usize>();
    let bin_rows = batch
        .stocks
        .iter()
        .flat_map(|stock| &stock.snapshots)
        .map(|snapshot| snapshot.bins.len())
        .sum::<usize>();
    let rounded_bucket_pct = round_chen_chip_value(config.bucket_pct);

    let mut snapshot_ts_code =
        StringBuilder::with_capacity(snapshot_rows, snapshot_rows.saturating_mul(12));
    let mut snapshot_trade_date =
        StringBuilder::with_capacity(snapshot_rows, snapshot_rows.saturating_mul(8));
    let mut snapshot_adj_type = StringBuilder::with_capacity(
        snapshot_rows,
        snapshot_rows.saturating_mul(DEFAULT_ADJ_TYPE.len()),
    );
    let mut snapshot_warmup_days = Vec::with_capacity(snapshot_rows);
    let mut snapshot_bucket_pct = Vec::with_capacity(snapshot_rows);
    let mut snapshot_close = Vec::with_capacity(snapshot_rows);
    let mut snapshot_min_price = Vec::with_capacity(snapshot_rows);
    let mut snapshot_max_price = Vec::with_capacity(snapshot_rows);
    let mut snapshot_main_total = Vec::with_capacity(snapshot_rows);
    let mut snapshot_retail_total = Vec::with_capacity(snapshot_rows);
    let mut snapshot_total_chips = Vec::with_capacity(snapshot_rows);
    let mut snapshot_total_profit_ratio = Vec::with_capacity(snapshot_rows);
    let mut snapshot_total_trapped_ratio = Vec::with_capacity(snapshot_rows);
    let mut snapshot_main_avg_cost = Vec::with_capacity(snapshot_rows);
    let mut snapshot_chip_peak_price = Vec::with_capacity(snapshot_rows);
    let mut snapshot_percent_70_price_low = Vec::with_capacity(snapshot_rows);
    let mut snapshot_percent_70_price_high = Vec::with_capacity(snapshot_rows);
    let mut snapshot_percent_70_concentration = Vec::with_capacity(snapshot_rows);
    let mut snapshot_percent_90_price_low = Vec::with_capacity(snapshot_rows);
    let mut snapshot_percent_90_price_high = Vec::with_capacity(snapshot_rows);
    let mut snapshot_percent_90_concentration = Vec::with_capacity(snapshot_rows);
    let mut snapshot_main_profit_ratio = Vec::with_capacity(snapshot_rows);
    let mut snapshot_main_trapped_ratio = Vec::with_capacity(snapshot_rows);

    let mut bin_ts_code = StringBuilder::with_capacity(bin_rows, bin_rows.saturating_mul(12));
    let mut bin_trade_date = StringBuilder::with_capacity(bin_rows, bin_rows.saturating_mul(8));
    let mut bin_adj_type =
        StringBuilder::with_capacity(bin_rows, bin_rows.saturating_mul(DEFAULT_ADJ_TYPE.len()));
    let mut bin_index = Vec::with_capacity(bin_rows);
    let mut bin_price = Vec::with_capacity(bin_rows);
    let mut bin_price_low = Vec::with_capacity(bin_rows);
    let mut bin_price_high = Vec::with_capacity(bin_rows);
    let mut bin_main_chip = Vec::with_capacity(bin_rows);
    let mut bin_retail_chip = Vec::with_capacity(bin_rows);
    let mut bin_total_chip = Vec::with_capacity(bin_rows);

    let mut checkpoint_stmt = conn
        .prepare_cached("INSERT OR REPLACE INTO cyq_chen_checkpoint VALUES (?, ?, ?, ?)")
        .map_err(|e| format!("准备新筹码续算检查点写入失败:{e}"))?;
    for stock in batch.stocks {
        let ts_code = stock.ts_code;
        if let Some(last) = stock.snapshots.last() {
            checkpoint_stmt
                .execute(params![
                    ts_code,
                    DEFAULT_ADJ_TYPE,
                    last.trade_date.as_deref(),
                    serde_json::to_string(&last.bins).map_err(|e| e.to_string())?,
                ])
                .map_err(|e| format!("写入新筹码续算检查点失败: {e}"))?;
        }
        for mut snapshot in stock.snapshots {
            let trade_date = snapshot
                .trade_date
                .take()
                .ok_or_else(|| format!("{ts_code} 的新筹码快照缺少交易日期"))?;
            round_chen_chip_snapshot(&mut snapshot);
            snapshot_ts_code.append_value(&ts_code);
            snapshot_trade_date.append_value(&trade_date);
            snapshot_adj_type.append_value(DEFAULT_ADJ_TYPE);
            snapshot_warmup_days.push(config.warmup_days as i32);
            snapshot_bucket_pct.push(rounded_bucket_pct);
            snapshot_close.push(snapshot.close);
            snapshot_min_price.push(snapshot.min_price);
            snapshot_max_price.push(snapshot.max_price);
            snapshot_main_total.push(snapshot.main_total);
            snapshot_retail_total.push(snapshot.retail_total);
            snapshot_total_chips.push(snapshot.total_chips);
            snapshot_total_profit_ratio.push(snapshot.total_profit_ratio);
            snapshot_total_trapped_ratio.push(snapshot.total_trapped_ratio);
            snapshot_main_avg_cost.push(snapshot.main_avg_cost);
            snapshot_chip_peak_price.push(snapshot.chip_peak_price);
            snapshot_percent_70_price_low.push(snapshot.percent_70.price_low);
            snapshot_percent_70_price_high.push(snapshot.percent_70.price_high);
            snapshot_percent_70_concentration.push(snapshot.percent_70.concentration);
            snapshot_percent_90_price_low.push(snapshot.percent_90.price_low);
            snapshot_percent_90_price_high.push(snapshot.percent_90.price_high);
            snapshot_percent_90_concentration.push(snapshot.percent_90.concentration);
            snapshot_main_profit_ratio.push(snapshot.main_profit_ratio);
            snapshot_main_trapped_ratio.push(snapshot.main_trapped_ratio);

            for bin in snapshot.bins {
                bin_ts_code.append_value(&ts_code);
                bin_trade_date.append_value(&trade_date);
                bin_adj_type.append_value(DEFAULT_ADJ_TYPE);
                bin_index.push(bin.index as i32);
                bin_price.push(bin.price);
                bin_price_low.push(bin.price_low);
                bin_price_high.push(bin.price_high);
                bin_main_chip.push(bin.main_chip);
                bin_retail_chip.push(bin.retail_chip);
                bin_total_chip.push(bin.total_chip);
            }
        }
    }

    if snapshot_rows > 0 {
        snapshot_app
            .append_record_batch((|ts_code: StringArray,
                                   trade_date: StringArray,
                                   adj_type: StringArray,
                                   warmup_days: Vec<i32>,
                                   bucket_pct: Vec<f64>,
                                   close: Vec<f64>,
                                   min_price: Vec<f64>,
                                   max_price: Vec<f64>,
                                   main_total: Vec<f64>,
                                   retail_total: Vec<f64>,
                                   total_chips: Vec<f64>,
                                   total_profit_ratio: Vec<f64>,
                                   total_trapped_ratio: Vec<f64>,
                                   main_avg_cost: Vec<f64>,
                                   chip_peak_price: Vec<f64>,
                                   percent_70_price_low: Vec<f64>,
                                   percent_70_price_high: Vec<f64>,
                                   percent_70_concentration: Vec<f64>,
                                   percent_90_price_low: Vec<f64>,
                                   percent_90_price_high: Vec<f64>,
                                   percent_90_concentration: Vec<f64>,
                                   main_profit_ratio: Vec<f64>,
                                   main_trapped_ratio: Vec<f64>|
             -> Result<RecordBatch, String> {
                let schema = Schema::new(vec![
                    Field::new("ts_code", DataType::Utf8, false),
                    Field::new("trade_date", DataType::Utf8, false),
                    Field::new("adj_type", DataType::Utf8, false),
                    Field::new("warmup_days", DataType::Int32, false),
                    Field::new("bucket_pct", DataType::Float64, false),
                    Field::new("close", DataType::Float64, false),
                    Field::new("min_price", DataType::Float64, false),
                    Field::new("max_price", DataType::Float64, false),
                    Field::new("main_total", DataType::Float64, false),
                    Field::new("retail_total", DataType::Float64, false),
                    Field::new("total_chips", DataType::Float64, false),
                    Field::new("total_profit_ratio", DataType::Float64, false),
                    Field::new("total_trapped_ratio", DataType::Float64, false),
                    Field::new("main_avg_cost", DataType::Float64, false),
                    Field::new("chip_peak_price", DataType::Float64, false),
                    Field::new("percent_70_price_low", DataType::Float64, false),
                    Field::new("percent_70_price_high", DataType::Float64, false),
                    Field::new("percent_70_concentration", DataType::Float64, false),
                    Field::new("percent_90_price_low", DataType::Float64, false),
                    Field::new("percent_90_price_high", DataType::Float64, false),
                    Field::new("percent_90_concentration", DataType::Float64, false),
                    Field::new("main_profit_ratio", DataType::Float64, false),
                    Field::new("main_trapped_ratio", DataType::Float64, false),
                ]);
                RecordBatch::try_new(
                    Arc::new(schema),
                    vec![
                        string_array(ts_code),
                        string_array(trade_date),
                        string_array(adj_type),
                        int32_array(warmup_days),
                        float64_array(bucket_pct),
                        float64_array(close),
                        float64_array(min_price),
                        float64_array(max_price),
                        float64_array(main_total),
                        float64_array(retail_total),
                        float64_array(total_chips),
                        float64_array(total_profit_ratio),
                        float64_array(total_trapped_ratio),
                        float64_array(main_avg_cost),
                        float64_array(chip_peak_price),
                        float64_array(percent_70_price_low),
                        float64_array(percent_70_price_high),
                        float64_array(percent_70_concentration),
                        float64_array(percent_90_price_low),
                        float64_array(percent_90_price_high),
                        float64_array(percent_90_concentration),
                        float64_array(main_profit_ratio),
                        float64_array(main_trapped_ratio),
                    ],
                )
                .map_err(|e| format!("创建cyq_chen_snapshot批次失败:{e}"))
            })(
                snapshot_ts_code.finish(),
                snapshot_trade_date.finish(),
                snapshot_adj_type.finish(),
                snapshot_warmup_days,
                snapshot_bucket_pct,
                snapshot_close,
                snapshot_min_price,
                snapshot_max_price,
                snapshot_main_total,
                snapshot_retail_total,
                snapshot_total_chips,
                snapshot_total_profit_ratio,
                snapshot_total_trapped_ratio,
                snapshot_main_avg_cost,
                snapshot_chip_peak_price,
                snapshot_percent_70_price_low,
                snapshot_percent_70_price_high,
                snapshot_percent_70_concentration,
                snapshot_percent_90_price_low,
                snapshot_percent_90_price_high,
                snapshot_percent_90_concentration,
                snapshot_main_profit_ratio,
                snapshot_main_trapped_ratio,
            )?)
            .map_err(|e| format!("批量写入cyq_chen_snapshot失败:{e}"))?;
    }

    if bin_rows > 0 {
        bin_app
            .append_record_batch((|ts_code: StringArray,
                                   trade_date: StringArray,
                                   adj_type: StringArray,
                                   bin_index: Vec<i32>,
                                   price: Vec<f64>,
                                   price_low: Vec<f64>,
                                   price_high: Vec<f64>,
                                   main_chip: Vec<f64>,
                                   retail_chip: Vec<f64>,
                                   total_chip: Vec<f64>|
             -> Result<RecordBatch, String> {
                let schema = Schema::new(vec![
                    Field::new("ts_code", DataType::Utf8, false),
                    Field::new("trade_date", DataType::Utf8, false),
                    Field::new("adj_type", DataType::Utf8, false),
                    Field::new("bin_index", DataType::Int32, false),
                    Field::new("price", DataType::Float64, false),
                    Field::new("price_low", DataType::Float64, false),
                    Field::new("price_high", DataType::Float64, false),
                    Field::new("main_chip", DataType::Float64, false),
                    Field::new("retail_chip", DataType::Float64, false),
                    Field::new("total_chip", DataType::Float64, false),
                ]);
                RecordBatch::try_new(
                    Arc::new(schema),
                    vec![
                        string_array(ts_code),
                        string_array(trade_date),
                        string_array(adj_type),
                        int32_array(bin_index),
                        float64_array(price),
                        float64_array(price_low),
                        float64_array(price_high),
                        float64_array(main_chip),
                        float64_array(retail_chip),
                        float64_array(total_chip),
                    ],
                )
                .map_err(|e| format!("创建cyq_chen_bin批次失败:{e}"))
            })(
                bin_ts_code.finish(),
                bin_trade_date.finish(),
                bin_adj_type.finish(),
                bin_index,
                bin_price,
                bin_price_low,
                bin_price_high,
                bin_main_chip,
                bin_retail_chip,
                bin_total_chip,
            )?)
            .map_err(|e| format!("批量写入cyq_chen_bin失败:{e}"))?;
    }

    Ok((snapshot_rows, bin_rows))
}

pub(super) fn string_array(values: StringArray) -> ArrayRef {
    Arc::new(values)
}

pub(super) fn int32_array(values: Vec<i32>) -> ArrayRef {
    Arc::new(Int32Array::from(values))
}

pub(super) fn float64_array(values: Vec<f64>) -> ArrayRef {
    Arc::new(Float64Array::from(values))
}

pub(super) fn finish_cyq_chen_write(
    compute_result: Result<(), String>,
    writer_result: Result<(usize, usize), String>,
) -> Result<(usize, usize), String> {
    let rows = writer_result?;
    compute_result?;
    Ok(rows)
}

pub(super) fn write_cyq_chen_batches_from_channel(
    db_path: &str,
    rx: Receiver<CyqChenWriteMessage>,
    config: ChenChipConfig,
    strategy_hash: String,
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
    if occupied {
        return Err("新筹码分批重建只允许写入空临时库，禁止覆盖已有数据".to_string());
    }
    conn.execute_batch("SET memory_limit = '512MB'; SET threads = 2;")
        .map_err(|e| format!("设置新筹码写库资源上限失败:{e}"))?;
    drop_cyq_chen_db_indexes(&conn)?;
    let mut snapshot_rows = 0usize;
    let mut bin_rows = 0usize;
    for message in rx {
        let batch = match message {
            CyqChenWriteMessage::Batch(batch) => batch,
            CyqChenWriteMessage::Abort(reason) => {
                return Err(format!("筹码计算中断，临时库不发布:{reason}"));
            }
        };
        let tx = conn
            .transaction()
            .map_err(|e| format!("创建筹码批次事务失败:{e}"))?;
        let (added_snapshot_rows, added_bin_rows) = {
            let mut snapshot_app = tx
                .appender(CYQ_CHEN_SNAPSHOT_TABLE)
                .map_err(|e| format!("创建cyq_chen_snapshot写入器失败:{e}"))?;
            let mut bin_app = tx
                .appender(CYQ_CHEN_BIN_TABLE)
                .map_err(|e| format!("创建cyq_chen_bin写入器失败:{e}"))?;
            let rows =
                append_cyq_chen_batch_rows(&tx, &mut snapshot_app, &mut bin_app, batch, config)?;
            snapshot_app
                .flush()
                .map_err(|e| format!("刷新cyq_chen_snapshot失败:{e}"))?;
            bin_app
                .flush()
                .map_err(|e| format!("刷新cyq_chen_bin失败:{e}"))?;
            rows
        };
        tx.commit().map_err(|e| format!("提交筹码批次失败:{e}"))?;
        snapshot_rows += added_snapshot_rows;
        bin_rows += added_bin_rows;
    }
    let tx = conn
        .transaction()
        .map_err(|e| format!("创建筹码元数据事务失败:{e}"))?;
    write_cyq_chen_meta(&tx, config, &strategy_hash)?;
    tx.commit().map_err(|e| format!("提交筹码元数据失败:{e}"))?;
    ensure_cyq_chen_db_indexes(&conn)?;
    conn.execute_batch("CHECKPOINT")
        .map_err(|e| format!("检查点新筹码库失败:{e}"))?;
    Ok((snapshot_rows, bin_rows))
}

pub(super) fn write_cyq_chen_incremental_batches_from_channel(
    db_path: &str,
    rx: Receiver<CyqChenWriteMessage>,
    config: ChenChipConfig,
    start_date: &str,
    end_date: &str,
    strategy_hash: String,
) -> Result<(usize, usize), String> {
    let mut conn = Connection::open(db_path).map_err(|e| format!("打开筹码库失败:{e}"))?;
    ensure_cyq_chen_db_indexes(&conn)?;

    let write_result = (|| -> Result<(usize, usize), String> {
        let tx = conn
            .transaction()
            .map_err(|e| format!("创建新筹码增量事务失败:{e}"))?;
        drop_cyq_chen_db_indexes(&tx)?;
        tx.execute(
            "DELETE FROM cyq_chen_bin WHERE adj_type = ? AND trade_date >= ? AND trade_date <= ?",
            params![DEFAULT_ADJ_TYPE, start_date, end_date],
        )
        .map_err(|e| format!("清理增量区间cyq_chen_bin失败:{e}"))?;
        tx.execute(
            "DELETE FROM cyq_chen_snapshot WHERE adj_type = ? AND trade_date >= ? AND trade_date <= ?",
            params![DEFAULT_ADJ_TYPE, start_date, end_date],
        )
        .map_err(|e| format!("清理增量区间cyq_chen_snapshot失败:{e}"))?;

        let mut snapshot_rows = 0usize;
        let mut bin_rows = 0usize;
        let mut batch_count = 0usize;
        let mut abort_reason = None;
        {
            let mut snapshot_app = tx
                .appender(CYQ_CHEN_SNAPSHOT_TABLE)
                .map_err(|e| format!("创建cyq_chen_snapshot写入器失败:{e}"))?;
            let mut bin_app = tx
                .appender(CYQ_CHEN_BIN_TABLE)
                .map_err(|e| format!("创建cyq_chen_bin写入器失败:{e}"))?;

            for message in rx {
                let batch = match message {
                    CyqChenWriteMessage::Batch(batch) => batch,
                    CyqChenWriteMessage::Abort(reason) => {
                        abort_reason = Some(reason);
                        break;
                    }
                };

                let (added_snapshot_rows, added_bin_rows) = append_cyq_chen_batch_rows(
                    &tx,
                    &mut snapshot_app,
                    &mut bin_app,
                    batch,
                    config,
                )?;
                snapshot_rows += added_snapshot_rows;
                bin_rows += added_bin_rows;
                batch_count += 1;

                if batch_count % CYQ_CHEN_FLUSH_BATCH_SIZE == 0 {
                    snapshot_app
                        .flush()
                        .map_err(|e| format!("刷新cyq_chen_snapshot写入器失败:{e}"))?;
                    bin_app
                        .flush()
                        .map_err(|e| format!("刷新cyq_chen_bin写入器失败:{e}"))?;
                }
            }

            if abort_reason.is_none() {
                snapshot_app
                    .flush()
                    .map_err(|e| format!("刷新cyq_chen_snapshot写入器失败:{e}"))?;
                bin_app
                    .flush()
                    .map_err(|e| format!("刷新cyq_chen_bin写入器失败:{e}"))?;
            }
        }

        if let Some(reason) = abort_reason {
            tx.rollback()
                .map_err(|e| format!("新筹码增量计算中断且结果库回滚失败:{reason}; {e}"))?;
            return Err(format!("新筹码增量计算中断，结果库已回滚:{reason}"));
        }

        write_cyq_chen_meta(&tx, config, &strategy_hash)?;
        tx.commit()
            .map_err(|e| format!("提交新筹码增量事务失败:{e}"))?;
        Ok((snapshot_rows, bin_rows))
    })();

    let index_result = ensure_cyq_chen_db_indexes(&conn);
    match (write_result, index_result) {
        (Ok(rows), Ok(())) => Ok(rows),
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use crate::data::cyq_chen::ChenChipConfig;
    use crate::data::cyq_chen_data::CyqChenWriteMessage;
    use crate::data::cyq_chen_data::compute::finish_cyq_chen_write;
    use crate::data::cyq_chen_data::compute::write_cyq_chen_batches_from_channel;
    use crate::data::cyq_chen_data::test_support::*;
    use std::fs;
    use std::sync::mpsc::sync_channel;

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
