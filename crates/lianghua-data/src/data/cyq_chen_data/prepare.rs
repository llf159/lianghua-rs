use super::{ComputedCyqChenStock, CyqChenWriteBatch, DEFAULT_ADJ_TYPE, PreparedCyqChenWriteBatch};
use crate::data::cyq_chen::{
    ChenChipConfig, ChenChipFeatureState, ChenChipSnapshot, round_chen_chip_snapshot,
    round_chen_chip_value,
};
use duckdb::arrow::{
    array::{
        ArrayRef, Float64Array, Int32Array,
        builder::{Float64Builder, Int32Builder, StringBuilder},
    },
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use std::sync::Arc;

pub(super) struct CyqChenBatchBuilder {
    stock_count: usize,
    replace_ts_codes: Vec<String>,
    checkpoints: Vec<(String, String, String, Option<String>)>,
    warmup_days: i32,
    bucket_pct: f64,
    snapshot_ts_code: StringBuilder,
    snapshot_trade_date: StringBuilder,
    snapshot_adj_type: StringBuilder,
    snapshot_warmup_days: Vec<i32>,
    snapshot_bucket_pct: Vec<f64>,
    snapshot_close: Vec<f64>,
    snapshot_min_price: Vec<f64>,
    snapshot_max_price: Vec<f64>,
    snapshot_main_total: Vec<f64>,
    snapshot_retail_total: Vec<f64>,
    snapshot_total_chips: Vec<f64>,
    snapshot_total_profit_ratio: Vec<f64>,
    snapshot_total_trapped_ratio: Vec<f64>,
    snapshot_main_avg_cost: Vec<f64>,
    snapshot_chip_peak_price: Vec<f64>,
    snapshot_percent_70_price_low: Vec<f64>,
    snapshot_percent_70_price_high: Vec<f64>,
    snapshot_percent_70_concentration: Vec<f64>,
    snapshot_percent_90_price_low: Vec<f64>,
    snapshot_percent_90_price_high: Vec<f64>,
    snapshot_percent_90_concentration: Vec<f64>,
    snapshot_main_profit_ratio: Vec<f64>,
    snapshot_main_trapped_ratio: Vec<f64>,
    snapshot_trap_coef: Float64Builder,
    snapshot_real_loss20: Float64Builder,
    snapshot_feature_version: Int32Builder,
    snapshot_feature_days: Int32Builder,
    snapshot_unknown_trapped: Float64Builder,
    bin_ts_code: StringBuilder,
    bin_trade_date: StringBuilder,
    bin_adj_type: StringBuilder,
    bin_index: Vec<i32>,
    bin_price: Vec<f64>,
    bin_price_low: Vec<f64>,
    bin_price_high: Vec<f64>,
    bin_main_chip: Vec<f64>,
    bin_retail_chip: Vec<f64>,
    bin_total_chip: Vec<f64>,
}
impl CyqChenBatchBuilder {
    pub(super) fn new(
        snapshot_capacity: usize,
        bin_capacity: usize,
        config: ChenChipConfig,
    ) -> Self {
        Self {
            stock_count: 0,
            replace_ts_codes: Vec::new(),
            checkpoints: Vec::new(),
            warmup_days: config.warmup_days as i32,
            bucket_pct: round_chen_chip_value(config.bucket_pct),
            snapshot_ts_code: StringBuilder::with_capacity(
                snapshot_capacity,
                snapshot_capacity.saturating_mul(9),
            ),
            snapshot_trade_date: StringBuilder::with_capacity(
                snapshot_capacity,
                snapshot_capacity.saturating_mul(8),
            ),
            snapshot_adj_type: StringBuilder::with_capacity(
                snapshot_capacity,
                snapshot_capacity.saturating_mul(DEFAULT_ADJ_TYPE.len()),
            ),
            snapshot_warmup_days: Vec::with_capacity(snapshot_capacity),
            snapshot_bucket_pct: Vec::with_capacity(snapshot_capacity),
            snapshot_close: Vec::with_capacity(snapshot_capacity),
            snapshot_min_price: Vec::with_capacity(snapshot_capacity),
            snapshot_max_price: Vec::with_capacity(snapshot_capacity),
            snapshot_main_total: Vec::with_capacity(snapshot_capacity),
            snapshot_retail_total: Vec::with_capacity(snapshot_capacity),
            snapshot_total_chips: Vec::with_capacity(snapshot_capacity),
            snapshot_total_profit_ratio: Vec::with_capacity(snapshot_capacity),
            snapshot_total_trapped_ratio: Vec::with_capacity(snapshot_capacity),
            snapshot_main_avg_cost: Vec::with_capacity(snapshot_capacity),
            snapshot_chip_peak_price: Vec::with_capacity(snapshot_capacity),
            snapshot_percent_70_price_low: Vec::with_capacity(snapshot_capacity),
            snapshot_percent_70_price_high: Vec::with_capacity(snapshot_capacity),
            snapshot_percent_70_concentration: Vec::with_capacity(snapshot_capacity),
            snapshot_percent_90_price_low: Vec::with_capacity(snapshot_capacity),
            snapshot_percent_90_price_high: Vec::with_capacity(snapshot_capacity),
            snapshot_percent_90_concentration: Vec::with_capacity(snapshot_capacity),
            snapshot_main_profit_ratio: Vec::with_capacity(snapshot_capacity),
            snapshot_main_trapped_ratio: Vec::with_capacity(snapshot_capacity),
            snapshot_trap_coef: Float64Builder::with_capacity(snapshot_capacity),
            snapshot_real_loss20: Float64Builder::with_capacity(snapshot_capacity),
            snapshot_feature_version: Int32Builder::with_capacity(snapshot_capacity),
            snapshot_feature_days: Int32Builder::with_capacity(snapshot_capacity),
            snapshot_unknown_trapped: Float64Builder::with_capacity(snapshot_capacity),
            bin_ts_code: StringBuilder::with_capacity(bin_capacity, bin_capacity.saturating_mul(9)),
            bin_trade_date: StringBuilder::with_capacity(
                bin_capacity,
                bin_capacity.saturating_mul(8),
            ),
            bin_adj_type: StringBuilder::with_capacity(
                bin_capacity,
                bin_capacity.saturating_mul(DEFAULT_ADJ_TYPE.len()),
            ),
            bin_index: Vec::with_capacity(bin_capacity),
            bin_price: Vec::with_capacity(bin_capacity),
            bin_price_low: Vec::with_capacity(bin_capacity),
            bin_price_high: Vec::with_capacity(bin_capacity),
            bin_main_chip: Vec::with_capacity(bin_capacity),
            bin_retail_chip: Vec::with_capacity(bin_capacity),
            bin_total_chip: Vec::with_capacity(bin_capacity),
        }
    }
    pub(super) fn append_snapshot(
        &mut self,
        ts_code: &str,
        mut snapshot: ChenChipSnapshot,
    ) -> Result<(), String> {
        let trade_date = snapshot
            .trade_date
            .take()
            .ok_or_else(|| format!("{ts_code} 的新筹码快照缺少交易日期"))?;
        round_chen_chip_snapshot(&mut snapshot);
        self.snapshot_ts_code.append_value(&ts_code);
        self.snapshot_trade_date.append_value(&trade_date);
        self.snapshot_adj_type.append_value(DEFAULT_ADJ_TYPE);
        self.snapshot_warmup_days.push(self.warmup_days);
        self.snapshot_bucket_pct.push(self.bucket_pct);
        self.snapshot_close.push(snapshot.close);
        self.snapshot_min_price.push(snapshot.min_price);
        self.snapshot_max_price.push(snapshot.max_price);
        self.snapshot_main_total.push(snapshot.main_total);
        self.snapshot_retail_total.push(snapshot.retail_total);
        self.snapshot_total_chips.push(snapshot.total_chips);
        self.snapshot_total_profit_ratio
            .push(snapshot.total_profit_ratio);
        self.snapshot_total_trapped_ratio
            .push(snapshot.total_trapped_ratio);
        self.snapshot_main_avg_cost.push(snapshot.main_avg_cost);
        self.snapshot_chip_peak_price.push(snapshot.chip_peak_price);
        self.snapshot_percent_70_price_low
            .push(snapshot.percent_70.price_low);
        self.snapshot_percent_70_price_high
            .push(snapshot.percent_70.price_high);
        self.snapshot_percent_70_concentration
            .push(snapshot.percent_70.concentration);
        self.snapshot_percent_90_price_low
            .push(snapshot.percent_90.price_low);
        self.snapshot_percent_90_price_high
            .push(snapshot.percent_90.price_high);
        self.snapshot_percent_90_concentration
            .push(snapshot.percent_90.concentration);
        self.snapshot_main_profit_ratio
            .push(snapshot.main_profit_ratio);
        self.snapshot_main_trapped_ratio
            .push(snapshot.main_trapped_ratio);
        self.snapshot_unknown_trapped
            .append_option(snapshot.unknown_trapped);
        self.snapshot_feature_days
            .append_option(snapshot.feature_days.map(|value| value as i32));
        self.snapshot_feature_version
            .append_option(snapshot.feature_version.map(|value| value as i32));
        self.snapshot_real_loss20
            .append_option(snapshot.real_loss20);
        self.snapshot_trap_coef.append_option(snapshot.trap_coef);

        for bin in snapshot.bins {
            self.bin_ts_code.append_value(&ts_code);
            self.bin_trade_date.append_value(&trade_date);
            self.bin_adj_type.append_value(DEFAULT_ADJ_TYPE);
            self.bin_index.push(bin.index as i32);
            self.bin_price.push(bin.price);
            self.bin_price_low.push(bin.price_low);
            self.bin_price_high.push(bin.price_high);
            self.bin_main_chip.push(bin.main_chip);
            self.bin_retail_chip.push(bin.retail_chip);
            self.bin_total_chip.push(bin.total_chip);
        }
        Ok(())
    }
    pub(super) fn finish_stock(
        &mut self,
        ts_code: &str,
        last: ChenChipSnapshot,
        state: Option<&ChenChipFeatureState>,
        replace_history: bool,
    ) -> Result<(), String> {
        let date = last
            .trade_date
            .as_deref()
            .ok_or_else(|| format!("{ts_code} 的新筹码快照缺少交易日期"))?;
        let state = state.ok_or_else(|| format!("{ts_code} 的新筹码计算缺少特征续算状态"))?;
        state
            .validate(date, &last.bins)
            .map_err(|e| format!("拒绝写入无效新筹码检查点, ts_code={ts_code}: {e}"))?;
        self.checkpoints.push((
            ts_code.to_string(),
            date.to_string(),
            serde_json::to_string(&last.bins).map_err(|e| e.to_string())?,
            Some(serde_json::to_string(state).map_err(|e| e.to_string())?),
        ));
        if replace_history {
            self.replace_ts_codes.push(ts_code.to_string());
        }
        self.stock_count += 1;
        self.append_snapshot(ts_code, last)
    }
    pub(super) fn finish(mut self) -> Result<PreparedCyqChenWriteBatch, String> {
        let snapshot_rows = self.snapshot_close.len();
        let bin_rows = self.bin_index.len();
        let snapshot_batch = if snapshot_rows > 0 {
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
                Field::new("trap_coef", DataType::Float64, true),
                Field::new("real_loss20", DataType::Float64, true),
                Field::new("feature_version", DataType::Int32, true),
                Field::new("feature_days", DataType::Int32, true),
                Field::new("unknown_trapped", DataType::Float64, true),
            ]);
            Some(
                RecordBatch::try_new(
                    Arc::new(schema),
                    vec![
                        Arc::new(self.snapshot_ts_code.finish()) as ArrayRef,
                        Arc::new(self.snapshot_trade_date.finish()) as ArrayRef,
                        Arc::new(self.snapshot_adj_type.finish()) as ArrayRef,
                        Arc::new(Int32Array::from(self.snapshot_warmup_days)) as ArrayRef,
                        Arc::new(Float64Array::from(self.snapshot_bucket_pct)) as ArrayRef,
                        Arc::new(Float64Array::from(self.snapshot_close)) as ArrayRef,
                        Arc::new(Float64Array::from(self.snapshot_min_price)) as ArrayRef,
                        Arc::new(Float64Array::from(self.snapshot_max_price)) as ArrayRef,
                        Arc::new(Float64Array::from(self.snapshot_main_total)) as ArrayRef,
                        Arc::new(Float64Array::from(self.snapshot_retail_total)) as ArrayRef,
                        Arc::new(Float64Array::from(self.snapshot_total_chips)) as ArrayRef,
                        Arc::new(Float64Array::from(self.snapshot_total_profit_ratio)) as ArrayRef,
                        Arc::new(Float64Array::from(self.snapshot_total_trapped_ratio)) as ArrayRef,
                        Arc::new(Float64Array::from(self.snapshot_main_avg_cost)) as ArrayRef,
                        Arc::new(Float64Array::from(self.snapshot_chip_peak_price)) as ArrayRef,
                        Arc::new(Float64Array::from(self.snapshot_percent_70_price_low))
                            as ArrayRef,
                        Arc::new(Float64Array::from(self.snapshot_percent_70_price_high))
                            as ArrayRef,
                        Arc::new(Float64Array::from(self.snapshot_percent_70_concentration))
                            as ArrayRef,
                        Arc::new(Float64Array::from(self.snapshot_percent_90_price_low))
                            as ArrayRef,
                        Arc::new(Float64Array::from(self.snapshot_percent_90_price_high))
                            as ArrayRef,
                        Arc::new(Float64Array::from(self.snapshot_percent_90_concentration))
                            as ArrayRef,
                        Arc::new(Float64Array::from(self.snapshot_main_profit_ratio)) as ArrayRef,
                        Arc::new(Float64Array::from(self.snapshot_main_trapped_ratio)) as ArrayRef,
                        Arc::new(self.snapshot_trap_coef.finish()) as ArrayRef,
                        Arc::new(self.snapshot_real_loss20.finish()) as ArrayRef,
                        Arc::new(self.snapshot_feature_version.finish()) as ArrayRef,
                        Arc::new(self.snapshot_feature_days.finish()) as ArrayRef,
                        Arc::new(self.snapshot_unknown_trapped.finish()) as ArrayRef,
                    ],
                )
                .map_err(|e| format!("创建cyq_chen_snapshot批次失败:{e}"))?,
            )
        } else {
            None
        };
        let bin_batch = if bin_rows > 0 {
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
            Some(
                RecordBatch::try_new(
                    Arc::new(schema),
                    vec![
                        Arc::new(self.bin_ts_code.finish()) as ArrayRef,
                        Arc::new(self.bin_trade_date.finish()) as ArrayRef,
                        Arc::new(self.bin_adj_type.finish()) as ArrayRef,
                        Arc::new(Int32Array::from(self.bin_index)) as ArrayRef,
                        Arc::new(Float64Array::from(self.bin_price)) as ArrayRef,
                        Arc::new(Float64Array::from(self.bin_price_low)) as ArrayRef,
                        Arc::new(Float64Array::from(self.bin_price_high)) as ArrayRef,
                        Arc::new(Float64Array::from(self.bin_main_chip)) as ArrayRef,
                        Arc::new(Float64Array::from(self.bin_retail_chip)) as ArrayRef,
                        Arc::new(Float64Array::from(self.bin_total_chip)) as ArrayRef,
                    ],
                )
                .map_err(|e| format!("创建cyq_chen_bin批次失败:{e}"))?,
            )
        } else {
            None
        };
        Ok(PreparedCyqChenWriteBatch {
            stock_count: self.stock_count,
            replace_ts_codes: self.replace_ts_codes,
            snapshot_rows,
            bin_rows,
            checkpoints: self.checkpoints,
            snapshot_batch,
            bin_batch,
        })
    }
}

pub(super) fn prepare_cyq_chen_batch_rows(
    batch: CyqChenWriteBatch,
    config: ChenChipConfig,
) -> Result<PreparedCyqChenWriteBatch, String> {
    let snapshot_rows = batch.stocks.iter().map(|stock| stock.snapshots.len()).sum();
    let bin_rows = batch
        .stocks
        .iter()
        .flat_map(|stock| &stock.snapshots)
        .map(|snapshot| snapshot.bins.len())
        .sum();
    let mut builder = CyqChenBatchBuilder::new(snapshot_rows, bin_rows, config);
    for ComputedCyqChenStock {
        ts_code,
        mut snapshots,
        feature_state,
        replace_history,
    } in batch.stocks
    {
        if let Some(last) = snapshots.pop() {
            for snapshot in snapshots {
                builder.append_snapshot(&ts_code, snapshot)?;
            }
            builder.finish_stock(&ts_code, last, feature_state.as_ref(), replace_history)?;
        }
    }
    builder.finish()
}
