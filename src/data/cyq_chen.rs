use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use serde::{Deserialize, Serialize};

use crate::{
    data::{
        RowData, RuntimeKeyCollectOptions, chip_change_rule_path,
        collect_runtime_keys_from_expr_programs, scoring_data::row_into_rt,
    },
    expr::{
        eval::{Runtime, Value},
        parser::{Expr, Parser, Stmt, Stmts, lex_all},
    },
    utils::utils::{eval_binary_for_warmup, impl_expr_warmup},
};

const DEFAULT_WARMUP_DAYS: usize = 120;
const DEFAULT_BUCKET_PCT: f64 = 1.0;
const EPS: f64 = 1e-10;
const CHEN_CHIP_ALWAYS_RUNTIME_KEYS: [&str; 5] = ["O", "H", "L", "C", "TURNOVER_RATE"];
const CHEN_CHIP_INJECTED_RUNTIME_KEYS: [&str; 9] = [
    "RATEO",
    "RATEH",
    "RATEL",
    "RATEC",
    "MAIN_CHIP_RATIO",
    "MAIN_CHIP_TOTAL",
    "RETAIL_CHIP_TOTAL",
    "ZHANG",
    "TOTAL_MV_YI",
];

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChipChangeConfig {
    pub version: u32,
    pub strategy: Vec<ChipChangeStrategy>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChipChangeStrategy {
    pub name: String,
    pub holder: ChipHolder,
    pub direction: ChipDirection,
    pub when: String,
    pub bias: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ChipHolder {
    Main,
    Retail,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ChipDirection {
    Buy,
    Sell,
}

#[derive(Debug, Clone)]
pub struct CompiledChipChangeConfig {
    pub version: u32,
    pub strategies: Vec<CompiledChipChangeStrategy>,
}

#[derive(Debug, Clone)]
pub struct CompiledChipChangeStrategy {
    pub name: String,
    pub holder: ChipHolder,
    pub direction: ChipDirection,
    pub when: String,
    pub bias: f64,
    pub when_ast: Stmts,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChenChipConfig {
    pub warmup_days: usize,
    pub bucket_pct: f64,
}

impl Default for ChenChipConfig {
    fn default() -> Self {
        Self {
            warmup_days: DEFAULT_WARMUP_DAYS,
            bucket_pct: DEFAULT_BUCKET_PCT,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChenChipBin {
    pub index: usize,
    pub price: f64,
    pub price_low: f64,
    pub price_high: f64,
    pub main_chip: f64,
    pub retail_chip: f64,
    pub total_chip: f64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChenChipPercentRange {
    pub percent: f64,
    pub price_low: f64,
    pub price_high: f64,
    pub concentration: f64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChenChipSnapshot {
    pub trade_date: Option<String>,
    pub close: f64,
    pub min_price: f64,
    pub max_price: f64,
    pub main_total: f64,
    pub retail_total: f64,
    pub total_chips: f64,
    pub total_profit_ratio: f64,
    pub total_trapped_ratio: f64,
    pub chip_peak_price: f64,
    pub percent_70: ChenChipPercentRange,
    pub percent_90: ChenChipPercentRange,
    pub bins: Vec<ChenChipBin>,
}

pub fn round_chen_chip_value(value: f64) -> f64 {
    if !value.is_finite() {
        return value;
    }

    let rounded = format!("{value:.4}").parse::<f64>().unwrap_or(value);
    if rounded == 0.0 { 0.0 } else { rounded }
}

pub fn round_chen_chip_snapshot(snapshot: &mut ChenChipSnapshot) {
    snapshot.close = round_chen_chip_value(snapshot.close);
    snapshot.min_price = round_chen_chip_value(snapshot.min_price);
    snapshot.max_price = round_chen_chip_value(snapshot.max_price);
    snapshot.main_total = round_chen_chip_value(snapshot.main_total);
    snapshot.retail_total = round_chen_chip_value(snapshot.retail_total);
    snapshot.total_chips = round_chen_chip_value(snapshot.total_chips);
    let original_ratio_sum = snapshot.total_profit_ratio + snapshot.total_trapped_ratio;
    snapshot.total_profit_ratio = round_chen_chip_value(snapshot.total_profit_ratio);
    snapshot.total_trapped_ratio = if (original_ratio_sum - 1.0).abs() <= 1e-9 {
        round_chen_chip_value(1.0 - snapshot.total_profit_ratio)
    } else {
        round_chen_chip_value(snapshot.total_trapped_ratio)
    };
    snapshot.chip_peak_price = round_chen_chip_value(snapshot.chip_peak_price);
    round_chen_chip_percent_range(&mut snapshot.percent_70);
    round_chen_chip_percent_range(&mut snapshot.percent_90);
    for bin in &mut snapshot.bins {
        round_chen_chip_bin(bin);
    }
}

fn round_chen_chip_percent_range(range: &mut ChenChipPercentRange) {
    range.percent = round_chen_chip_value(range.percent);
    range.price_low = round_chen_chip_value(range.price_low);
    range.price_high = round_chen_chip_value(range.price_high);
    range.concentration = round_chen_chip_value(range.concentration);
}

fn round_chen_chip_bin(bin: &mut ChenChipBin) {
    bin.price = round_chen_chip_value(bin.price);
    bin.price_low = round_chen_chip_value(bin.price_low);
    bin.price_high = round_chen_chip_value(bin.price_high);
    bin.main_chip = round_chen_chip_value(bin.main_chip);
    bin.retail_chip = round_chen_chip_value(bin.retail_chip);
    bin.total_chip = round_chen_chip_value(bin.total_chip);
}

#[derive(Debug, Clone)]
struct ChenChipBar {
    trade_date: String,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    turnover_rate: f64,
}

#[derive(Debug, Clone)]
struct ChipBucket {
    price_low: f64,
    price_high: f64,
    main_chip: f64,
    retail_chip: f64,
}

#[derive(Debug, Clone)]
struct SellEntry {
    bucket_index: usize,
    holder: ChipHolder,
    weight: f64,
}

impl ChipChangeConfig {
    pub fn load(source_dir: &str) -> Result<Self, String> {
        let path = chip_change_rule_path(source_dir);
        let text = std::fs::read_to_string(&path).map_err(|error| {
            format!(
                "筹码变化策略文件不存在或不可读: path={}, err={error}",
                path.display()
            )
        })?;
        Self::from_toml_str(&text)
    }

    pub fn from_toml_str(text: &str) -> Result<Self, String> {
        let mut config: ChipChangeConfig =
            toml::from_str(text).map_err(|error| format!("筹码变化策略文件格式错误: {error}"))?;
        config.normalize_and_validate()?;
        Ok(config)
    }

    pub fn compile(&self) -> Result<CompiledChipChangeConfig, String> {
        self.validate()?;

        let mut strategies = Vec::with_capacity(self.strategy.len());
        for (index, strategy) in self.strategy.iter().enumerate() {
            let n = index + 1;
            let when_ast = parse_strategy_expression(&strategy.when, n, &strategy.name)?;
            strategies.push(CompiledChipChangeStrategy {
                name: strategy.name.trim().to_string(),
                holder: strategy.holder,
                direction: strategy.direction,
                when: strategy.when.trim().to_string(),
                bias: strategy.bias,
                when_ast,
            });
        }

        Ok(CompiledChipChangeConfig {
            version: self.version,
            strategies,
        })
    }

    fn normalize_and_validate(&mut self) -> Result<(), String> {
        for strategy in &mut self.strategy {
            strategy.name = strategy.name.trim().to_string();
            strategy.when = strategy.when.trim().to_string();
        }
        self.validate()
    }

    fn validate(&self) -> Result<(), String> {
        if self.version != 1 {
            return Err(format!(
                "筹码变化策略文件 version 只支持 1，当前为 {}",
                self.version
            ));
        }

        for (index, strategy) in self.strategy.iter().enumerate() {
            let n = index + 1;
            if strategy.name.trim().is_empty() {
                return Err(format!("第{n}个strategy的name字段为空"));
            }
            if strategy.when.trim().is_empty() {
                return Err(format!("第{n}个strategy的when字段为空"));
            }
            if !strategy.bias.is_finite() {
                return Err(format!("第{n}个strategy的bias必须是有限数值"));
            }
            parse_strategy_expression(strategy.when.trim(), n, strategy.name.trim())?;
        }

        Ok(())
    }
}

fn round_ratio(value: f64) -> f64 {
    (value * 1_000_000_000.0).round() / 1_000_000_000.0
}

pub fn load_compiled_chip_change_config(
    source_dir: &str,
) -> Result<CompiledChipChangeConfig, String> {
    ChipChangeConfig::load(source_dir)?.compile()
}

pub fn collect_chen_chip_runtime_keys(chip_config: &CompiledChipChangeConfig) -> HashSet<String> {
    let programs = chip_config
        .strategies
        .iter()
        .map(|strategy| &strategy.when_ast)
        .collect::<Vec<_>>();

    collect_runtime_keys_from_expr_programs(
        &programs,
        RuntimeKeyCollectOptions {
            always_keys: &CHEN_CHIP_ALWAYS_RUNTIME_KEYS,
            injected_keys: &CHEN_CHIP_INJECTED_RUNTIME_KEYS,
            aliases: &[],
        },
    )
}

pub fn estimate_chen_chip_expression_warmup(
    chip_config: &CompiledChipChangeConfig,
) -> Result<usize, String> {
    let mut max_warmup = 0usize;

    for strategy in &chip_config.strategies {
        let mut locals = HashMap::new();
        let mut consts: HashMap<String, usize> = HashMap::new();
        let mut expr_need = 0usize;

        for stmt in strategy.when_ast.item.clone() {
            match stmt {
                Stmt::Assign { name, value } => match value {
                    Expr::Number(value) => {
                        if value.is_finite() && value >= 0.0 {
                            consts.insert(name, value as usize);
                        }
                    }
                    Expr::Binary { op, lhs, rhs } => {
                        if let Some(out) = eval_binary_for_warmup(&op, &lhs, &rhs, &consts)? {
                            consts.insert(name, out as usize);
                        } else {
                            let value_need =
                                impl_expr_warmup(Expr::Binary { op, lhs, rhs }, &locals, &consts)?;
                            locals.insert(name, value_need);
                        }
                    }
                    other => {
                        let value_need = impl_expr_warmup(other, &locals, &consts)?;
                        locals.insert(name, value_need);
                    }
                },
                Stmt::Expr(expr) => {
                    expr_need = expr_need.max(impl_expr_warmup(expr, &locals, &consts)?);
                }
            }
        }

        max_warmup = max_warmup.max(expr_need);
    }

    Ok(max_warmup)
}

pub fn compute_chen_chip_snapshots_from_row_data(
    row_data: &RowData,
    output_start_date: &str,
    chip_config: &ChipChangeConfig,
    config: ChenChipConfig,
) -> Result<Vec<ChenChipSnapshot>, String> {
    let compiled = chip_config.compile()?;
    compute_chen_chip_snapshots_with_compiled_config(row_data, output_start_date, &compiled, config)
}

pub fn compute_chen_chip_snapshots_with_compiled_config(
    row_data: &RowData,
    output_start_date: &str,
    chip_config: &CompiledChipChangeConfig,
    config: ChenChipConfig,
) -> Result<Vec<ChenChipSnapshot>, String> {
    validate_compute_config(config)?;
    if chip_config.version != 1 {
        return Err(format!(
            "筹码变化策略文件 version 只支持 1，当前为 {}",
            chip_config.version
        ));
    }

    let output_start_date = output_start_date.trim();
    if output_start_date.is_empty() {
        return Err("output_start_date不能为空".to_string());
    }

    validate_row_shape(row_data)?;
    let Some(output_start_index) = row_data
        .trade_dates
        .iter()
        .position(|trade_date| trade_date == output_start_date)
    else {
        return Err(format!(
            "output_start_date 不在 RowData.trade_dates 中: {output_start_date}"
        ));
    };

    let bars = build_validated_bars(row_data, output_start_index)?;
    if output_start_index < config.warmup_days {
        return Ok(Vec::new());
    }

    let initial_range = resolve_initial_range(&bars, output_start_index, config.warmup_days)?;
    let step = bucket_step(config.bucket_pct);
    let mut buckets = build_initial_buckets(initial_range.0, initial_range.1, step)?;
    if buckets.is_empty() {
        return Ok(Vec::new());
    }
    let len = row_data.trade_dates.len();
    let mut main_ratio_history = vec![vec![None; len]; buckets.len()];
    let mut base_runtime = row_into_rt(row_data.clone())?;
    share_runtime_num_series(&mut base_runtime);
    let buy_runtime = build_new_participant_buy_runtime(&base_runtime, &bars)?;
    let process_start_index = output_start_index.saturating_sub(config.warmup_days);
    let mut snapshots = Vec::with_capacity(len.saturating_sub(output_start_index));

    for day_index in process_start_index..len {
        let Some(bar) = bars[day_index].as_ref() else {
            if day_index < output_start_index {
                return Ok(Vec::new());
            }
            return Err(format!(
                "{} 缺少有效K线数据",
                row_data.trade_dates[day_index]
            ));
        };

        expand_buckets_for_bar(
            &mut buckets,
            &mut main_ratio_history,
            len,
            bar.low,
            bar.high,
            step,
        )?;
        write_main_ratio_for_day(&buckets, &mut main_ratio_history, day_index);

        apply_sell_for_day(
            &mut buckets,
            &bars,
            &base_runtime,
            chip_config,
            &main_ratio_history,
            day_index,
            bar.turnover_rate,
        )?;
        sanitize_buckets(&mut buckets)?;

        apply_buy_for_day(
            &mut buckets,
            bar,
            &buy_runtime,
            chip_config,
            len,
            day_index,
            bar.turnover_rate,
        )?;
        normalize_buckets(&mut buckets)?;

        if day_index >= output_start_index {
            snapshots.push(build_snapshot(bar, &buckets)?);
        }
    }

    Ok(snapshots)
}

pub fn compute_chen_chip_snapshots_from_initial_bins_with_compiled_config(
    row_data: &RowData,
    output_start_date: &str,
    initial_bins: &[ChenChipBin],
    initial_main_ratio_history: &[Vec<Option<f64>>],
    chip_config: &CompiledChipChangeConfig,
    config: ChenChipConfig,
) -> Result<Vec<ChenChipSnapshot>, String> {
    validate_compute_config(config)?;
    if chip_config.version != 1 {
        return Err(format!(
            "筹码变化策略文件 version 只支持 1，当前为 {}",
            chip_config.version
        ));
    }

    let output_start_date = output_start_date.trim();
    if output_start_date.is_empty() {
        return Err("output_start_date不能为空".to_string());
    }
    if initial_bins.is_empty() {
        return Err("初始筹码分桶为空，无法续算".to_string());
    }

    validate_row_shape(row_data)?;
    let Some(output_start_index) = row_data
        .trade_dates
        .iter()
        .position(|trade_date| trade_date == output_start_date)
    else {
        return Err(format!(
            "output_start_date 不在 RowData.trade_dates 中: {output_start_date}"
        ));
    };

    let bars = build_validated_bars(row_data, output_start_index)?;
    let step = bucket_step(config.bucket_pct);
    let mut buckets = build_buckets_from_snapshot_bins(initial_bins)?;
    let len = row_data.trade_dates.len();
    let mut main_ratio_history =
        build_initial_main_ratio_history(initial_main_ratio_history, initial_bins.len(), len)?;
    let mut base_runtime = row_into_rt(row_data.clone())?;
    share_runtime_num_series(&mut base_runtime);
    let buy_runtime = build_new_participant_buy_runtime(&base_runtime, &bars)?;
    let mut snapshots = Vec::with_capacity(len.saturating_sub(output_start_index));

    for day_index in output_start_index..len {
        let Some(bar) = bars[day_index].as_ref() else {
            return Err(format!(
                "{} 缺少有效K线数据",
                row_data.trade_dates[day_index]
            ));
        };

        expand_buckets_for_bar(
            &mut buckets,
            &mut main_ratio_history,
            len,
            bar.low,
            bar.high,
            step,
        )?;
        write_main_ratio_for_day(&buckets, &mut main_ratio_history, day_index);

        apply_sell_for_day(
            &mut buckets,
            &bars,
            &base_runtime,
            chip_config,
            &main_ratio_history,
            day_index,
            bar.turnover_rate,
        )?;
        sanitize_buckets(&mut buckets)?;

        apply_buy_for_day(
            &mut buckets,
            bar,
            &buy_runtime,
            chip_config,
            len,
            day_index,
            bar.turnover_rate,
        )?;
        normalize_buckets(&mut buckets)?;

        snapshots.push(build_snapshot(bar, &buckets)?);
    }

    Ok(snapshots)
}

fn parse_strategy_expression(
    expression: &str,
    strategy_index: usize,
    strategy_name: &str,
) -> Result<Stmts, String> {
    let tokens = lex_all(expression);
    let mut parser = Parser::new(tokens);
    parser.parse_main().map_err(|error| {
        format!(
            "第{strategy_index}个strategy({strategy_name})表达式解析错误在{}:{}",
            error.idx, error.msg
        )
    })
}

fn validate_compute_config(config: ChenChipConfig) -> Result<(), String> {
    if !config.bucket_pct.is_finite() || config.bucket_pct <= 0.0 {
        return Err("bucket_pct必须是有限正数".to_string());
    }
    Ok(())
}

fn validate_row_shape(row_data: &RowData) -> Result<(), String> {
    row_data.validate()?;

    for window in row_data.trade_dates.windows(2) {
        if window[0] >= window[1] {
            return Err("RowData.trade_dates必须升序且不可重复".to_string());
        }
    }

    required_series(row_data, "O")?;
    required_series(row_data, "H")?;
    required_series(row_data, "L")?;
    required_series(row_data, "C")?;
    turnover_series(row_data)?;

    Ok(())
}

fn required_series<'a>(row_data: &'a RowData, key: &str) -> Result<&'a [Option<f64>], String> {
    row_data
        .cols
        .get(key)
        .map(Vec::as_slice)
        .ok_or_else(|| format!("RowData 缺少 {key} 列"))
}

fn turnover_series(row_data: &RowData) -> Result<&[Option<f64>], String> {
    if let Some(series) = row_data.cols.get("TURNOVER_RATE") {
        return Ok(series.as_slice());
    }
    if let Some(series) = row_data.cols.get("TOR") {
        return Ok(series.as_slice());
    }
    Err("RowData 缺少 TURNOVER_RATE/TOR 列".to_string())
}

fn build_validated_bars(
    row_data: &RowData,
    output_start_index: usize,
) -> Result<Vec<Option<ChenChipBar>>, String> {
    let open_series = required_series(row_data, "O")?;
    let high_series = required_series(row_data, "H")?;
    let low_series = required_series(row_data, "L")?;
    let close_series = required_series(row_data, "C")?;
    let turnover_series = turnover_series(row_data)?;

    let mut bars = Vec::with_capacity(row_data.trade_dates.len());
    for index in 0..row_data.trade_dates.len() {
        match parse_bar_at(
            row_data.trade_dates[index].as_str(),
            open_series[index],
            high_series[index],
            low_series[index],
            close_series[index],
            turnover_series[index],
        ) {
            Ok(bar) => bars.push(Some(bar)),
            Err(error) if index < output_start_index => {
                let _ = error;
                bars.push(None);
            }
            Err(error) => return Err(error),
        }
    }

    Ok(bars)
}

fn parse_bar_at(
    trade_date: &str,
    open: Option<f64>,
    high: Option<f64>,
    low: Option<f64>,
    close: Option<f64>,
    turnover_rate: Option<f64>,
) -> Result<ChenChipBar, String> {
    let open = required_value(trade_date, "O", open)?;
    let high = required_value(trade_date, "H", high)?;
    let low = required_value(trade_date, "L", low)?;
    let close = required_value(trade_date, "C", close)?;
    let turnover_rate = required_value(trade_date, "TURNOVER_RATE/TOR", turnover_rate)?;

    for (name, value) in [("O", open), ("H", high), ("L", low), ("C", close)] {
        if !value.is_finite() || value <= 0.0 {
            return Err(format!("{trade_date} 的{name}必须是有限正数"));
        }
    }
    if high + EPS < low {
        return Err(format!("{trade_date} 的最高价小于最低价"));
    }
    if !turnover_rate.is_finite() || !(0.0..=100.0).contains(&turnover_rate) {
        return Err(format!("{trade_date} 的换手率必须是[0,100]之间的有限数值"));
    }

    Ok(ChenChipBar {
        trade_date: trade_date.to_string(),
        open,
        high,
        low,
        close,
        turnover_rate,
    })
}

fn required_value(trade_date: &str, name: &str, value: Option<f64>) -> Result<f64, String> {
    value.ok_or_else(|| format!("{trade_date} 缺少 {name}"))
}

fn resolve_initial_range(
    bars: &[Option<ChenChipBar>],
    output_start_index: usize,
    warmup_days: usize,
) -> Result<(f64, f64), String> {
    let range_start = output_start_index.saturating_sub(warmup_days);
    let range_end_exclusive = if warmup_days == 0 {
        output_start_index + 1
    } else {
        output_start_index
    };

    let mut min_price = f64::INFINITY;
    let mut max_price = f64::NEG_INFINITY;
    for bar in &bars[range_start..range_end_exclusive] {
        let Some(bar) = bar.as_ref() else {
            return Ok((f64::NAN, f64::NAN));
        };
        min_price = min_price.min(bar.low);
        max_price = max_price.max(bar.high);
    }

    if !min_price.is_finite() || !max_price.is_finite() {
        return Ok((f64::NAN, f64::NAN));
    }
    if min_price <= 0.0 || max_price <= 0.0 || max_price + EPS < min_price {
        return Err("预热窗口价格区间非法".to_string());
    }

    Ok((min_price, max_price))
}

fn bucket_step(bucket_pct: f64) -> f64 {
    1.0 + bucket_pct / 100.0
}

fn build_initial_buckets(
    min_price: f64,
    max_price: f64,
    step: f64,
) -> Result<Vec<ChipBucket>, String> {
    if !min_price.is_finite() || !max_price.is_finite() {
        return Ok(Vec::new());
    }
    if !step.is_finite() || step <= 1.0 {
        return Err("bucket_pct必须是有限正数".to_string());
    }

    let mut boundaries = vec![min_price];
    let mut upper = min_price;
    while upper + EPS < max_price {
        upper *= step;
        if !upper.is_finite() {
            return Err("动态分桶价格边界出现非有限数值".to_string());
        }
        if upper <= *boundaries.last().expect("boundary exists") + EPS {
            return Err("动态分桶价格边界未递增".to_string());
        }
        boundaries.push(upper);
    }

    if boundaries.len() == 1 {
        boundaries.push(min_price * step);
    }

    let bucket_count = boundaries.len() - 1;
    let main_each = 50.0 / bucket_count as f64;
    let retail_each = 50.0 / bucket_count as f64;

    let mut buckets = Vec::with_capacity(bucket_count);
    for index in 0..bucket_count {
        buckets.push(ChipBucket {
            price_low: boundaries[index],
            price_high: boundaries[index + 1],
            main_chip: main_each,
            retail_chip: retail_each,
        });
    }

    Ok(buckets)
}

fn build_buckets_from_snapshot_bins(bins: &[ChenChipBin]) -> Result<Vec<ChipBucket>, String> {
    let mut buckets = Vec::with_capacity(bins.len());
    for bin in bins {
        if !bin.price_low.is_finite()
            || !bin.price_high.is_finite()
            || !bin.main_chip.is_finite()
            || !bin.retail_chip.is_finite()
            || bin.price_low <= 0.0
            || bin.price_high <= bin.price_low + EPS
        {
            return Err("初始筹码分桶非法，无法续算".to_string());
        }
        buckets.push(ChipBucket {
            price_low: bin.price_low,
            price_high: bin.price_high,
            main_chip: bin.main_chip,
            retail_chip: bin.retail_chip,
        });
    }

    for window in buckets.windows(2) {
        if window[0].price_high > window[1].price_low + EPS {
            return Err("初始筹码分桶价格区间重叠，无法续算".to_string());
        }
    }

    normalize_buckets(&mut buckets)?;
    Ok(buckets)
}

fn build_initial_main_ratio_history(
    initial_history: &[Vec<Option<f64>>],
    bucket_count: usize,
    series_len: usize,
) -> Result<Vec<Vec<Option<f64>>>, String> {
    if initial_history.is_empty() {
        return Ok(vec![vec![None; series_len]; bucket_count]);
    }
    if initial_history.len() != bucket_count {
        return Err("初始MAIN_CHIP_RATIO历史分桶数不匹配".to_string());
    }

    let mut out = Vec::with_capacity(bucket_count);
    for history in initial_history {
        if history.len() != series_len {
            return Err("初始MAIN_CHIP_RATIO历史长度不匹配".to_string());
        }
        out.push(history.clone());
    }

    Ok(out)
}

fn expand_buckets_for_bar(
    buckets: &mut Vec<ChipBucket>,
    main_ratio_history: &mut Vec<Vec<Option<f64>>>,
    series_len: usize,
    low: f64,
    high: f64,
    step: f64,
) -> Result<(), String> {
    if buckets.is_empty() {
        return Err("价格分桶为空，无法计算筹码快照".to_string());
    }

    while low + EPS < buckets.first().expect("bucket exists").price_low {
        let current_low = buckets.first().expect("bucket exists").price_low;
        let new_low = current_low / step;
        if !new_low.is_finite() || new_low <= 0.0 || new_low + EPS >= current_low {
            return Err("向下扩展价格分桶失败".to_string());
        }
        buckets.insert(
            0,
            ChipBucket {
                price_low: new_low,
                price_high: current_low,
                main_chip: 0.0,
                retail_chip: 0.0,
            },
        );
        main_ratio_history.insert(0, vec![None; series_len]);
    }

    while high > buckets.last().expect("bucket exists").price_high + EPS {
        let current_high = buckets.last().expect("bucket exists").price_high;
        let new_high = current_high * step;
        if !new_high.is_finite() || new_high <= current_high + EPS {
            return Err("向上扩展价格分桶失败".to_string());
        }
        buckets.push(ChipBucket {
            price_low: current_high,
            price_high: new_high,
            main_chip: 0.0,
            retail_chip: 0.0,
        });
        main_ratio_history.push(vec![None; series_len]);
    }

    Ok(())
}

fn write_main_ratio_for_day(
    buckets: &[ChipBucket],
    main_ratio_history: &mut [Vec<Option<f64>>],
    day_index: usize,
) {
    for (bucket, history) in buckets.iter().zip(main_ratio_history.iter_mut()) {
        let total = bucket.total_chip();
        history[day_index] = if total > EPS {
            Some(bucket.main_chip / total)
        } else {
            Some(0.0)
        };
    }
}

fn apply_sell_for_day(
    buckets: &mut [ChipBucket],
    bars: &[Option<ChenChipBar>],
    base_runtime: &Runtime,
    chip_config: &CompiledChipChangeConfig,
    main_ratio_history: &[Vec<Option<f64>>],
    day_index: usize,
    turnover_rate: f64,
) -> Result<(), String> {
    if turnover_rate <= EPS {
        return Ok(());
    }

    let mut entries = Vec::new();
    let mut has_main_bias = false;
    let mut has_retail_bias = false;
    let (main_chip_total, retail_chip_total) = holder_chip_totals(buckets);

    for bucket_index in 0..buckets.len() {
        if buckets[bucket_index].main_chip <= EPS && buckets[bucket_index].retail_chip <= EPS {
            continue;
        }

        let bucket_runtime = build_bucket_runtime(
            base_runtime,
            bars,
            &buckets[bucket_index],
            &main_ratio_history[bucket_index],
            main_chip_total,
            retail_chip_total,
        )?;
        let (main_bias, retail_bias) = strategy_biases_at(
            &bucket_runtime,
            chip_config,
            ChipDirection::Sell,
            bars.len(),
            day_index,
        )?;
        let main_effective = main_bias.max(0.0);
        let retail_effective = retail_bias.max(0.0);

        if main_effective > EPS && buckets[bucket_index].main_chip > EPS {
            has_main_bias = true;
            entries.push(SellEntry {
                bucket_index,
                holder: ChipHolder::Main,
                weight: buckets[bucket_index].main_chip * main_effective,
            });
        }
        if retail_effective > EPS && buckets[bucket_index].retail_chip > EPS {
            has_retail_bias = true;
            entries.push(SellEntry {
                bucket_index,
                holder: ChipHolder::Retail,
                weight: buckets[bucket_index].retail_chip * retail_effective,
            });
        }
    }

    let mut remaining = if entries.is_empty() {
        let retail_entries = holder_chip_entries(buckets, ChipHolder::Retail);
        apply_weighted_sell(buckets, retail_entries, turnover_rate)?
    } else {
        apply_weighted_sell(buckets, entries, turnover_rate)?
    };

    if remaining <= EPS {
        return Ok(());
    }

    let bar = bars[day_index]
        .as_ref()
        .ok_or_else(|| format!("第{day_index}根K线缺少有效数据"))?;
    let retail_trapped_entries = retail_trapped_chip_entries(buckets, bar.close);
    remaining = apply_weighted_sell(buckets, retail_trapped_entries, remaining)?;

    if remaining <= EPS {
        return Ok(());
    }

    if has_main_bias && !has_retail_bias {
        let retail_entries = holder_chip_entries(buckets, ChipHolder::Retail);
        remaining = apply_weighted_sell(buckets, retail_entries, remaining)?;
    } else if has_retail_bias && !has_main_bias {
        let main_entries = holder_chip_entries(buckets, ChipHolder::Main);
        remaining = apply_weighted_sell(buckets, main_entries, remaining)?;
    } else if !has_main_bias && !has_retail_bias {
        let main_entries = holder_chip_entries(buckets, ChipHolder::Main);
        remaining = apply_weighted_sell(buckets, main_entries, remaining)?;
    }

    if remaining > EPS {
        let all_entries = all_chip_entries(buckets);
        apply_weighted_sell(buckets, all_entries, remaining)?;
    }

    Ok(())
}

fn apply_buy_for_day(
    buckets: &mut [ChipBucket],
    bar: &ChenChipBar,
    buy_runtime: &Runtime,
    chip_config: &CompiledChipChangeConfig,
    runtime_len: usize,
    day_index: usize,
    turnover_rate: f64,
) -> Result<(), String> {
    if turnover_rate <= EPS {
        return Ok(());
    }

    let weights = trade_distribution_weight_entries(buckets, bar)?;
    let total_weight = weights.iter().map(|(_, weight)| *weight).sum::<f64>();
    if total_weight <= EPS {
        return Ok(());
    }
    let (main_chip_total, retail_chip_total) = holder_chip_totals(buckets);
    let mut runtime = buy_runtime.clone();
    insert_dynamic_holder_fields(&mut runtime, main_chip_total, retail_chip_total);
    let (main_bias, retail_bias) = strategy_biases_at(
        &runtime,
        chip_config,
        ChipDirection::Buy,
        runtime_len,
        day_index,
    )?;
    let (main_share, retail_share) = buy_holder_shares(main_bias, retail_bias);

    for (bucket_index, weight) in weights {
        let bucket_buy_amount = turnover_rate * weight / total_weight;

        buckets[bucket_index].main_chip += bucket_buy_amount * main_share;
        buckets[bucket_index].retail_chip += bucket_buy_amount * retail_share;
    }

    Ok(())
}

fn insert_dynamic_holder_fields(
    runtime: &mut Runtime,
    main_chip_total: f64,
    retail_chip_total: f64,
) {
    let total = main_chip_total + retail_chip_total;
    let main_ratio = if total > EPS {
        main_chip_total / total
    } else {
        0.5
    };
    insert_num_with_lowercase_alias(runtime, "MAIN_CHIP_RATIO", main_ratio);
    insert_num_with_lowercase_alias(runtime, "MAIN_CHIP_TOTAL", main_chip_total);
    insert_num_with_lowercase_alias(runtime, "RETAIL_CHIP_TOTAL", retail_chip_total);
}

fn build_new_participant_buy_runtime(
    base_runtime: &Runtime,
    bars: &[Option<ChenChipBar>],
) -> Result<Runtime, String> {
    let mut rateo = Vec::with_capacity(bars.len());
    let mut rateh = Vec::with_capacity(bars.len());
    let mut ratel = Vec::with_capacity(bars.len());
    let mut ratec = Vec::with_capacity(bars.len());
    let mut last_close: Option<f64> = None;

    for bar in bars {
        let Some(bar) = bar else {
            rateo.push(None);
            rateh.push(None);
            ratel.push(None);
            ratec.push(None);
            continue;
        };
        let reference_price = last_close.unwrap_or(bar.open);
        if !reference_price.is_finite() || reference_price <= 0.0 {
            return Err("新进买方参考价非法".to_string());
        }

        rateo.push(Some((bar.open - reference_price) / reference_price * 100.0));
        rateh.push(Some((bar.high - reference_price) / reference_price * 100.0));
        ratel.push(Some((bar.low - reference_price) / reference_price * 100.0));
        ratec.push(Some(
            (bar.close - reference_price) / reference_price * 100.0,
        ));
        last_close = Some(bar.close);
    }

    let mut runtime = base_runtime.clone();
    insert_num_series_with_lowercase_alias(&mut runtime, "RATEO", rateo);
    insert_num_series_with_lowercase_alias(&mut runtime, "RATEH", rateh);
    insert_num_series_with_lowercase_alias(&mut runtime, "RATEL", ratel);
    insert_num_series_with_lowercase_alias(&mut runtime, "RATEC", ratec);
    Ok(runtime)
}

fn share_runtime_num_series(runtime: &mut Runtime) {
    for value in runtime.vars.values_mut() {
        if let Value::NumSeries(series) = value {
            *value = Value::SharedNumSeries(Arc::new(std::mem::take(series)));
        }
    }
}

fn build_bucket_runtime(
    base_runtime: &Runtime,
    bars: &[Option<ChenChipBar>],
    bucket: &ChipBucket,
    main_ratio_history: &[Option<f64>],
    main_chip_total: f64,
    retail_chip_total: f64,
) -> Result<Runtime, String> {
    let cost_price = bucket.price();
    if !cost_price.is_finite() || cost_price <= 0.0 {
        return Err("价格分桶中点价非法".to_string());
    }
    if !main_chip_total.is_finite() || !retail_chip_total.is_finite() {
        return Err("主力/散户总筹码出现非有限数值".to_string());
    }
    if main_ratio_history.len() != bars.len() {
        return Err("MAIN_CHIP_RATIO序列长度与交易日长度不一致".to_string());
    }

    let mut rateo = Vec::with_capacity(bars.len());
    let mut rateh = Vec::with_capacity(bars.len());
    let mut ratel = Vec::with_capacity(bars.len());
    let mut ratec = Vec::with_capacity(bars.len());

    for bar in bars {
        if let Some(bar) = bar {
            rateo.push(Some((bar.open - cost_price) / cost_price * 100.0));
            rateh.push(Some((bar.high - cost_price) / cost_price * 100.0));
            ratel.push(Some((bar.low - cost_price) / cost_price * 100.0));
            ratec.push(Some((bar.close - cost_price) / cost_price * 100.0));
        } else {
            rateo.push(None);
            rateh.push(None);
            ratel.push(None);
            ratec.push(None);
        }
    }

    let mut runtime = base_runtime.clone();
    insert_num_series_with_lowercase_alias(&mut runtime, "RATEO", rateo);
    insert_num_series_with_lowercase_alias(&mut runtime, "RATEH", rateh);
    insert_num_series_with_lowercase_alias(&mut runtime, "RATEL", ratel);
    insert_num_series_with_lowercase_alias(&mut runtime, "RATEC", ratec);
    insert_num_series_with_lowercase_alias(
        &mut runtime,
        "MAIN_CHIP_RATIO",
        main_ratio_history.to_vec(),
    );
    insert_num_with_lowercase_alias(&mut runtime, "MAIN_CHIP_TOTAL", main_chip_total);
    insert_num_with_lowercase_alias(&mut runtime, "RETAIL_CHIP_TOTAL", retail_chip_total);

    Ok(runtime)
}

fn insert_num_with_lowercase_alias(runtime: &mut Runtime, key: &str, value: f64) {
    runtime.vars.insert(key.to_string(), Value::Num(value));
    runtime
        .vars
        .insert(key.to_ascii_lowercase(), Value::Num(value));
}

fn insert_num_series_with_lowercase_alias(
    runtime: &mut Runtime,
    key: &str,
    series: Vec<Option<f64>>,
) {
    let series = Arc::new(series);
    runtime
        .vars
        .insert(key.to_string(), Value::SharedNumSeries(Arc::clone(&series)));
    runtime
        .vars
        .insert(key.to_ascii_lowercase(), Value::SharedNumSeries(series));
}

fn strategy_biases_at(
    bucket_runtime: &Runtime,
    chip_config: &CompiledChipChangeConfig,
    direction: ChipDirection,
    len: usize,
    day_index: usize,
) -> Result<(f64, f64), String> {
    let mut main_bias = 0.0;
    let mut retail_bias = 0.0;

    for strategy in chip_config
        .strategies
        .iter()
        .filter(|strategy| strategy.direction == direction)
    {
        let triggered = match bucket_runtime
            .eval_program_bool_at(&strategy.when_ast, day_index)
            .map_err(|error| format!("策略 {} 表达式计算错误: {}", strategy.name, error.msg))?
        {
            Some(triggered) => triggered,
            None => {
                let mut runtime = bucket_runtime.clone();
                let value = runtime.eval_program(&strategy.when_ast).map_err(|error| {
                    format!("策略 {} 表达式计算错误: {}", strategy.name, error.msg)
                })?;
                let triggers = Value::as_bool_series(&value, len).map_err(|error| {
                    format!("策略 {} 表达式返回值非布尔: {}", strategy.name, error.msg)
                })?;
                triggers.get(day_index).copied().unwrap_or(false)
            }
        };

        if triggered {
            match strategy.holder {
                ChipHolder::Main => main_bias += strategy.bias,
                ChipHolder::Retail => retail_bias += strategy.bias,
            }
        }
    }

    Ok((main_bias, retail_bias))
}

fn buy_holder_shares(main_bias: f64, retail_bias: f64) -> (f64, f64) {
    let main = main_bias.max(0.0);
    let retail = retail_bias.max(0.0);
    if main > EPS && retail > EPS {
        let total = main + retail;
        (main / total, retail / total)
    } else if main > EPS {
        (1.0, 0.0)
    } else if retail > EPS {
        (0.0, 1.0)
    } else {
        (0.0, 1.0)
    }
}

fn holder_chip_totals(buckets: &[ChipBucket]) -> (f64, f64) {
    let main_chip_total = buckets.iter().map(|bucket| bucket.main_chip).sum::<f64>();
    let retail_chip_total = buckets.iter().map(|bucket| bucket.retail_chip).sum::<f64>();
    let total = main_chip_total + retail_chip_total;
    if total <= EPS || !total.is_finite() {
        return (0.0, 0.0);
    }
    let scale = 100.0 / total;
    (main_chip_total * scale, retail_chip_total * scale)
}

fn holder_chip_entries(buckets: &[ChipBucket], holder: ChipHolder) -> Vec<SellEntry> {
    buckets
        .iter()
        .enumerate()
        .filter_map(|(bucket_index, bucket)| {
            let chip = bucket.chip(holder);
            if chip > EPS {
                Some(SellEntry {
                    bucket_index,
                    holder,
                    weight: chip,
                })
            } else {
                None
            }
        })
        .collect()
}

fn retail_trapped_chip_entries(buckets: &[ChipBucket], close: f64) -> Vec<SellEntry> {
    buckets
        .iter()
        .enumerate()
        .filter_map(|(bucket_index, bucket)| {
            if bucket.price() > close + EPS && bucket.retail_chip > EPS {
                Some(SellEntry {
                    bucket_index,
                    holder: ChipHolder::Retail,
                    weight: 1.0,
                })
            } else {
                None
            }
        })
        .collect()
}

fn all_chip_entries(buckets: &[ChipBucket]) -> Vec<SellEntry> {
    let mut entries = Vec::with_capacity(buckets.len() * 2);
    entries.extend(holder_chip_entries(buckets, ChipHolder::Main));
    entries.extend(holder_chip_entries(buckets, ChipHolder::Retail));
    entries
}

fn apply_weighted_sell(
    buckets: &mut [ChipBucket],
    entries: Vec<SellEntry>,
    amount: f64,
) -> Result<f64, String> {
    if amount <= EPS || entries.is_empty() {
        return Ok(amount.max(0.0));
    }

    let mut active = entries
        .into_iter()
        .filter(|entry| entry.weight.is_finite() && entry.weight > EPS)
        .collect::<Vec<_>>();
    let mut remaining = amount;

    while remaining > EPS && !active.is_empty() {
        let total_weight = active.iter().map(|entry| entry.weight).sum::<f64>();
        if !total_weight.is_finite() {
            return Err("卖出倾向权重出现非有限数值".to_string());
        }
        if total_weight <= EPS {
            break;
        }

        let mut next_active = Vec::with_capacity(active.len());
        let mut removed = 0.0;

        for entry in active {
            let available = buckets[entry.bucket_index].chip(entry.holder);
            if available <= EPS {
                continue;
            }

            let target = remaining * entry.weight / total_weight;
            let sell_amount = target.min(available);
            if sell_amount > EPS {
                *buckets[entry.bucket_index].chip_mut(entry.holder) -= sell_amount;
                removed += sell_amount;
            }

            if available - sell_amount > EPS {
                next_active.push(entry);
            }
        }

        if removed <= EPS {
            break;
        }
        remaining = (remaining - removed).max(0.0);
        active = next_active;
    }

    Ok(remaining)
}

fn trade_distribution_weight_entries(
    buckets: &[ChipBucket],
    bar: &ChenChipBar,
) -> Result<Vec<(usize, f64)>, String> {
    if buckets.is_empty() {
        return Ok(Vec::new());
    }

    if (bar.high - bar.low).abs() <= EPS {
        let index = find_bucket_containing_price(buckets, bar.close)
            .or_else(|| find_bucket_containing_price(buckets, bar.low))
            .unwrap_or_else(|| nearest_bucket_index(buckets, bar.close));
        return Ok(vec![(index, 1.0)]);
    }

    let center = (bar.open + bar.high + bar.low + bar.close) / 4.0;
    let center = center.clamp(bar.low, bar.high);
    let start_index = buckets.partition_point(|bucket| bucket.price_high <= bar.low + EPS);
    let mut weights = Vec::new();

    for (index, bucket) in buckets.iter().enumerate().skip(start_index) {
        if bucket.price_low >= bar.high - EPS {
            break;
        }
        let overlap_low = bucket.price_low.max(bar.low);
        let overlap_high = bucket.price_high.min(bar.high);
        if overlap_high <= overlap_low + EPS {
            continue;
        }
        let midpoint = (overlap_low + overlap_high) / 2.0;
        let height = triangle_height(midpoint, bar.low, center, bar.high);
        let width = overlap_high - overlap_low;
        let weight = (height * width).max(0.0);
        if weight > EPS {
            weights.push((index, weight));
        }
    }

    let total_weight = weights.iter().map(|(_, weight)| *weight).sum::<f64>();
    if !total_weight.is_finite() {
        return Err("成交价格分布权重出现非有限数值".to_string());
    }
    if total_weight <= EPS {
        let index = find_bucket_containing_price(buckets, center)
            .unwrap_or_else(|| nearest_bucket_index(buckets, center));
        weights.push((index, 1.0));
    }

    Ok(weights)
}

fn triangle_height(price: f64, low: f64, center: f64, high: f64) -> f64 {
    let slope = 2.0 / (high - low);
    if price <= center {
        if (center - low).abs() <= EPS {
            slope
        } else {
            (price - low) / (center - low) * slope
        }
    } else if (high - center).abs() <= EPS {
        slope
    } else {
        (high - price) / (high - center) * slope
    }
}

fn find_bucket_containing_price(buckets: &[ChipBucket], price: f64) -> Option<usize> {
    buckets
        .iter()
        .position(|bucket| bucket.price_low <= price + EPS && price <= bucket.price_high + EPS)
}

fn nearest_bucket_index(buckets: &[ChipBucket], price: f64) -> usize {
    let mut best_index = 0usize;
    let mut best_distance = f64::INFINITY;
    for (index, bucket) in buckets.iter().enumerate() {
        let distance = (bucket.price() - price).abs();
        if distance < best_distance {
            best_distance = distance;
            best_index = index;
        }
    }
    best_index
}

fn sanitize_buckets(buckets: &mut [ChipBucket]) -> Result<(), String> {
    for bucket in buckets {
        if !bucket.price_low.is_finite()
            || !bucket.price_high.is_finite()
            || !bucket.main_chip.is_finite()
            || !bucket.retail_chip.is_finite()
        {
            return Err("筹码分桶出现非有限数值".to_string());
        }
        if bucket.main_chip < 0.0 && bucket.main_chip.abs() <= EPS {
            bucket.main_chip = 0.0;
        }
        if bucket.retail_chip < 0.0 && bucket.retail_chip.abs() <= EPS {
            bucket.retail_chip = 0.0;
        }
        if bucket.main_chip.abs() <= EPS {
            bucket.main_chip = 0.0;
        }
        if bucket.retail_chip.abs() <= EPS {
            bucket.retail_chip = 0.0;
        }
        if bucket.main_chip < -EPS || bucket.retail_chip < -EPS {
            return Err("筹码分桶出现负筹码".to_string());
        }
    }
    Ok(())
}

fn normalize_buckets(buckets: &mut [ChipBucket]) -> Result<(), String> {
    sanitize_buckets(buckets)?;
    let total = buckets.iter().map(ChipBucket::total_chip).sum::<f64>();
    if !total.is_finite() {
        return Err("筹码总量出现非有限数值".to_string());
    }
    if total <= EPS {
        return Err("筹码总量为0，无法归一化".to_string());
    }

    let scale = 100.0 / total;
    for bucket in buckets.iter_mut() {
        bucket.main_chip *= scale;
        bucket.retail_chip *= scale;
    }
    sanitize_buckets(buckets)?;
    Ok(())
}

fn cost_by_chip(buckets: &[ChipBucket], chip_target: f64) -> f64 {
    let mut sum = 0.0;
    for bucket in buckets {
        let chip = bucket.total_chip();
        if sum + chip > chip_target {
            return bucket.price();
        }
        sum += chip;
    }

    buckets.last().map(ChipBucket::price).unwrap_or(0.0)
}

fn build_percent_range(
    percent: f64,
    buckets: &[ChipBucket],
    total_chips: f64,
) -> ChenChipPercentRange {
    let low = if total_chips <= EPS {
        0.0
    } else {
        cost_by_chip(buckets, total_chips * (1.0 - percent) / 2.0)
    };
    let high = if total_chips <= EPS {
        0.0
    } else {
        cost_by_chip(buckets, total_chips * (1.0 + percent) / 2.0)
    };
    let concentration = if (low + high).abs() < EPS {
        0.0
    } else {
        (high - low) / (low + high)
    };

    ChenChipPercentRange {
        percent,
        price_low: low,
        price_high: high,
        concentration: round_ratio(concentration),
    }
}

fn build_snapshot(bar: &ChenChipBar, buckets: &[ChipBucket]) -> Result<ChenChipSnapshot, String> {
    let mut bins = Vec::with_capacity(buckets.len());
    for (index, bucket) in buckets.iter().enumerate() {
        let total_chip = bucket.total_chip();
        bins.push(ChenChipBin {
            index,
            price: finite_value(bucket.price())?,
            price_low: finite_value(bucket.price_low)?,
            price_high: finite_value(bucket.price_high)?,
            main_chip: finite_value(bucket.main_chip)?,
            retail_chip: finite_value(bucket.retail_chip)?,
            total_chip: finite_value(total_chip)?,
        });
    }

    let main_total = buckets.iter().map(|bucket| bucket.main_chip).sum::<f64>();
    let retail_total = buckets.iter().map(|bucket| bucket.retail_chip).sum::<f64>();
    let total_chips = main_total + retail_total;
    let profit_chips = buckets
        .iter()
        .filter(|bucket| bucket.price() <= bar.close + EPS)
        .map(ChipBucket::total_chip)
        .sum::<f64>();
    let total_profit_ratio = if total_chips <= EPS {
        0.0
    } else {
        profit_chips / total_chips
    };
    let total_trapped_ratio = if total_chips <= EPS {
        0.0
    } else {
        1.0 - total_profit_ratio
    };
    let chip_peak_price = buckets
        .iter()
        .fold(None::<&ChipBucket>, |best, bucket| match best {
            Some(best) => {
                let bucket_chip = bucket.total_chip();
                let best_chip = best.total_chip();
                if bucket_chip > best_chip + EPS
                    || ((bucket_chip - best_chip).abs() <= EPS && bucket.price() < best.price())
                {
                    Some(bucket)
                } else {
                    Some(best)
                }
            }
            None => Some(bucket),
        })
        .map(ChipBucket::price)
        .unwrap_or(0.0);
    let min_price = buckets
        .first()
        .map(|bucket| bucket.price_low)
        .ok_or_else(|| "价格分桶为空，无法输出快照".to_string())?;
    let max_price = buckets
        .last()
        .map(|bucket| bucket.price_high)
        .ok_or_else(|| "价格分桶为空，无法输出快照".to_string())?;

    Ok(ChenChipSnapshot {
        trade_date: Some(bar.trade_date.clone()),
        close: finite_value(bar.close)?,
        min_price: finite_value(min_price)?,
        max_price: finite_value(max_price)?,
        main_total: finite_value(main_total)?,
        retail_total: finite_value(retail_total)?,
        total_chips: finite_value(total_chips)?,
        total_profit_ratio: finite_value(round_ratio(total_profit_ratio))?,
        total_trapped_ratio: finite_value(round_ratio(total_trapped_ratio))?,
        chip_peak_price: finite_value(chip_peak_price)?,
        percent_70: build_percent_range(0.7, buckets, total_chips),
        percent_90: build_percent_range(0.9, buckets, total_chips),
        bins,
    })
}

fn finite_value(value: f64) -> Result<f64, String> {
    if !value.is_finite() {
        return Err("输出快照出现非有限数值".to_string());
    }
    if value.abs() <= EPS {
        Ok(0.0)
    } else {
        Ok(value)
    }
}

impl ChipBucket {
    fn price(&self) -> f64 {
        (self.price_low + self.price_high) / 2.0
    }

    fn total_chip(&self) -> f64 {
        self.main_chip + self.retail_chip
    }

    fn chip(&self, holder: ChipHolder) -> f64 {
        match holder {
            ChipHolder::Main => self.main_chip,
            ChipHolder::Retail => self.retail_chip,
        }
    }

    fn chip_mut(&mut self, holder: ChipHolder) -> &mut f64 {
        match holder {
            ChipHolder::Main => &mut self.main_chip,
            ChipHolder::Retail => &mut self.retail_chip,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{
        ChenChipBar, ChenChipConfig, ChipBucket, ChipChangeConfig, ChipDirection, ChipHolder, EPS,
        apply_buy_for_day, apply_sell_for_day, build_new_participant_buy_runtime,
        buy_holder_shares, compute_chen_chip_snapshots_from_row_data, row_into_rt,
    };
    use crate::data::RowData;

    fn sample_config() -> ChipChangeConfig {
        ChipChangeConfig::from_toml_str(
            r#"
version = 1

[[strategy]]
name = "main buy"
holder = "main"
direction = "buy"
when = "C > O"
bias = 1.0
"#,
        )
        .expect("config should parse")
    }

    fn sample_row_data() -> RowData {
        let mut cols = HashMap::new();
        cols.insert(
            "O".to_string(),
            vec![Some(10.0), Some(10.0), Some(10.1), Some(10.6), Some(11.2)],
        );
        cols.insert(
            "H".to_string(),
            vec![Some(10.2), Some(10.2), Some(10.8), Some(11.6), Some(11.8)],
        );
        cols.insert(
            "L".to_string(),
            vec![Some(9.8), Some(9.8), Some(9.9), Some(10.2), Some(10.8)],
        );
        cols.insert(
            "C".to_string(),
            vec![Some(10.1), Some(10.1), Some(10.6), Some(11.4), Some(11.6)],
        );
        cols.insert(
            "TOR".to_string(),
            vec![Some(10.0), Some(10.0), Some(10.0), Some(10.0), Some(10.0)],
        );

        RowData {
            trade_dates: vec![
                "20240102".to_string(),
                "20240103".to_string(),
                "20240104".to_string(),
                "20240105".to_string(),
                "20240108".to_string(),
            ],
            cols,
        }
    }

    fn assert_close(actual: f64, expected: f64) {
        assert!(
            (actual - expected).abs() < 1e-6,
            "actual={actual}, expected={expected}"
        );
    }

    #[test]
    fn chip_change_config_parses_and_validates_expression() {
        let config = ChipChangeConfig::from_toml_str(
            r#"
version = 1

[[strategy]]
name = "主力低位承接"
holder = "main"
direction = "buy"
when = "RATEL < -8 AND C > O"
bias = 1.5
"#,
        )
        .expect("config should parse");

        assert_eq!(config.strategy.len(), 1);
        assert_eq!(config.strategy[0].holder, ChipHolder::Main);
        assert_eq!(config.strategy[0].direction, ChipDirection::Buy);
        assert_eq!(config.compile().expect("compile").strategies.len(), 1);
    }

    #[test]
    fn chip_change_config_rejects_bad_version() {
        let version_error = ChipChangeConfig::from_toml_str(
            r#"
version = 2
strategy = []
"#,
        )
        .expect_err("version should fail");
        assert!(version_error.contains("version"));
    }

    #[test]
    fn chip_change_config_allows_negative_buy_bias() {
        let config = ChipChangeConfig::from_toml_str(
            r#"
version = 1

[[strategy]]
name = "bad"
holder = "main"
direction = "buy"
when = "C > O"
bias = -0.5
"#,
        )
        .expect("negative buy bias should parse");

        assert_eq!(config.strategy[0].bias, -0.5);
    }

    #[test]
    fn buy_holder_shares_use_positive_net_bias_only() {
        let (main_share, retail_share) = buy_holder_shares(1.0, 2.0);
        assert_close(main_share, 1.0 / 3.0);
        assert_close(retail_share, 2.0 / 3.0);

        let (main_share, retail_share) = buy_holder_shares(0.4, -0.6);
        assert_close(main_share, 1.0);
        assert_close(retail_share, 0.0);

        let (main_share, retail_share) = buy_holder_shares(-0.6, 1.2);
        assert_close(main_share, 0.0);
        assert_close(retail_share, 1.0);

        let (main_share, retail_share) = buy_holder_shares(-0.6, -0.2);
        assert_close(main_share, 0.0);
        assert_close(retail_share, 1.0);
    }

    #[test]
    fn buy_strategy_uses_dynamic_global_holder_fields_without_bucket_scan() {
        let chip_config = ChipChangeConfig::from_toml_str(
            r#"
version = 1

[[strategy]]
name = "main keeps adding"
holder = "main"
direction = "buy"
when = "MAIN_CHIP_TOTAL > RETAIL_CHIP_TOTAL AND MAIN_CHIP_TOTAL + RETAIL_CHIP_TOTAL == 100"
bias = 1.0

[[strategy]]
name = "retail adds when stronger"
holder = "retail"
direction = "buy"
when = "RETAIL_CHIP_TOTAL > MAIN_CHIP_TOTAL"
bias = 1.0
"#,
        )
        .expect("config should parse")
        .compile()
        .expect("config should compile");
        let row_data = RowData {
            trade_dates: vec!["20240102".to_string()],
            cols: HashMap::from([
                ("O".to_string(), vec![Some(10.0)]),
                ("H".to_string(), vec![Some(10.2)]),
                ("L".to_string(), vec![Some(9.8)]),
                ("C".to_string(), vec![Some(10.1)]),
                ("TOR".to_string(), vec![Some(10.0)]),
            ]),
        };
        let bars = vec![Some(ChenChipBar {
            trade_date: "20240102".to_string(),
            open: 10.0,
            high: 10.2,
            low: 9.8,
            close: 10.1,
            turnover_rate: 10.0,
        })];
        let base_runtime = row_into_rt(row_data).expect("runtime should build");
        let buy_runtime =
            build_new_participant_buy_runtime(&base_runtime, &bars).expect("buy runtime");
        let mut buckets = vec![
            ChipBucket {
                price_low: 9.0,
                price_high: 10.0,
                main_chip: 30.0,
                retail_chip: 20.0,
            },
            ChipBucket {
                price_low: 10.0,
                price_high: 11.0,
                main_chip: 30.0,
                retail_chip: 20.0,
            },
        ];

        apply_buy_for_day(
            &mut buckets,
            bars[0].as_ref().expect("bar"),
            &buy_runtime,
            &chip_config,
            bars.len(),
            0,
            10.0,
        )
        .expect("buy should apply");

        let main_total = buckets.iter().map(|bucket| bucket.main_chip).sum::<f64>();
        let retail_total = buckets.iter().map(|bucket| bucket.retail_chip).sum::<f64>();

        assert_close(main_total, 70.0);
        assert_close(retail_total, 40.0);
    }

    #[test]
    fn sell_shortfall_uses_retail_trapped_chips_before_other_fallbacks() {
        let chip_config = ChipChangeConfig::from_toml_str(
            r#"
version = 1

[[strategy]]
name = "main sell"
holder = "main"
direction = "sell"
when = "C > 0"
bias = 1.0
"#,
        )
        .expect("config should parse")
        .compile()
        .expect("config should compile");
        let row_data = RowData {
            trade_dates: vec!["20240102".to_string()],
            cols: HashMap::from([
                ("O".to_string(), vec![Some(10.0)]),
                ("H".to_string(), vec![Some(10.2)]),
                ("L".to_string(), vec![Some(9.8)]),
                ("C".to_string(), vec![Some(10.0)]),
                ("TOR".to_string(), vec![Some(1.0)]),
            ]),
        };
        let bars = vec![Some(ChenChipBar {
            trade_date: "20240102".to_string(),
            open: 10.0,
            high: 10.2,
            low: 9.8,
            close: 10.0,
            turnover_rate: 1.0,
        })];
        let base_runtime = row_into_rt(row_data).expect("runtime should build");
        let main_ratio_history = vec![vec![Some(0.0)], vec![Some(0.0)]];
        let mut buckets = vec![
            ChipBucket {
                price_low: 9.0,
                price_high: 10.0,
                main_chip: 0.1,
                retail_chip: 10.0,
            },
            ChipBucket {
                price_low: 10.0,
                price_high: 11.0,
                main_chip: 0.1,
                retail_chip: 10.0,
            },
        ];

        apply_sell_for_day(
            &mut buckets,
            &bars,
            &base_runtime,
            &chip_config,
            &main_ratio_history,
            0,
            1.0,
        )
        .expect("sell should apply");

        assert_close(buckets[0].main_chip, 0.0);
        assert_close(buckets[1].main_chip, 0.0);
        assert_close(buckets[0].retail_chip, 10.0);
        assert_close(buckets[1].retail_chip, 9.2);
    }

    #[test]
    fn chip_change_config_rejects_bad_expression() {
        let error = ChipChangeConfig::from_toml_str(
            r#"
version = 1

[[strategy]]
name = "bad expr"
holder = "retail"
direction = "sell"
when = "C >"
bias = 1.0
"#,
        )
        .expect_err("expression should fail");
        assert!(error.contains("表达式解析错误"));
    }

    #[test]
    fn chen_chip_returns_empty_when_warmup_is_insufficient() {
        let snapshots = compute_chen_chip_snapshots_from_row_data(
            &sample_row_data(),
            "20240104",
            &sample_config(),
            ChenChipConfig {
                warmup_days: 3,
                bucket_pct: 5.0,
            },
        )
        .expect("compute should not fail");

        assert!(snapshots.is_empty());
    }

    #[test]
    fn chen_chip_requires_output_start_date_in_row_data() {
        let error = compute_chen_chip_snapshots_from_row_data(
            &sample_row_data(),
            "20240109",
            &sample_config(),
            ChenChipConfig {
                warmup_days: 2,
                bucket_pct: 5.0,
            },
        )
        .expect_err("missing output_start_date should fail");

        assert!(error.contains("output_start_date"));
    }

    #[test]
    fn chen_chip_computes_main_retail_snapshots_and_expands_buckets() {
        let snapshots = compute_chen_chip_snapshots_from_row_data(
            &sample_row_data(),
            "20240104",
            &sample_config(),
            ChenChipConfig {
                warmup_days: 2,
                bucket_pct: 5.0,
            },
        )
        .expect("compute should succeed");

        assert_eq!(snapshots.len(), 3);
        assert_eq!(snapshots[0].trade_date.as_deref(), Some("20240104"));
        assert!(snapshots[0].main_total > snapshots[0].retail_total);
        assert_close(snapshots[0].total_chips, 100.0);
        assert_close(
            snapshots[0].main_total + snapshots[0].retail_total,
            snapshots[0].total_chips,
        );
        assert!(snapshots[1].max_price >= 11.6);
        assert!(snapshots[1].bins.iter().all(|bin| bin.main_chip >= 0.0));
        assert!(snapshots[1].bins.iter().all(|bin| bin.retail_chip >= 0.0));
        assert_close(
            snapshots[1].total_profit_ratio + snapshots[1].total_trapped_ratio,
            1.0,
        );
        assert!(snapshots[1].chip_peak_price >= snapshots[1].min_price);
        assert!(snapshots[1].chip_peak_price <= snapshots[1].max_price);
        assert!(snapshots[1].percent_70.price_low <= snapshots[1].percent_70.price_high);
        assert!(snapshots[1].percent_90.price_low <= snapshots[1].percent_90.price_high);
        assert!(
            snapshots[1].percent_90.price_low <= snapshots[1].percent_70.price_low
                || (snapshots[1].percent_90.price_low - snapshots[1].percent_70.price_low).abs()
                    <= EPS
        );
        assert!(
            snapshots[1].percent_90.price_high >= snapshots[1].percent_70.price_high
                || (snapshots[1].percent_90.price_high - snapshots[1].percent_70.price_high).abs()
                    <= EPS
        );
        assert!(
            snapshots[1]
                .bins
                .iter()
                .any(|bin| bin.price_high > 11.0 && bin.total_chip > 0.0)
        );
    }

    #[test]
    fn chen_chip_rejects_invalid_output_turnover() {
        let mut row_data = sample_row_data();
        row_data
            .cols
            .get_mut("TOR")
            .expect("tor")
            .get_mut(2)
            .map(|value| *value = Some(101.0));

        let error = compute_chen_chip_snapshots_from_row_data(
            &row_data,
            "20240104",
            &sample_config(),
            ChenChipConfig {
                warmup_days: 2,
                bucket_pct: 5.0,
            },
        )
        .expect_err("invalid turnover should fail");

        assert!(error.contains("换手率"));
    }
}
