use std::{collections::HashMap, env, fs};

use lianghua_data::data::{
    DataReader,
    cyq_chen::{
        ChenChipConfig, ChipChangeConfig, collect_chen_chip_runtime_keys,
        compute_chen_chip_snapshots_with_compiled_config,
    },
};
use serde_json::json;

fn main() -> Result<(), String> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    if args.len() < 6 {
        return Err(
            "用法: chip_behavior_sample <数据目录> <策略.toml> <开始日> <结束日> <输出.json> <股票...>"
                .to_string(),
        );
    }

    let source_dir = &args[0];
    let strategy_path = &args[1];
    let start_date = &args[2];
    let end_date = &args[3];
    let output_path = &args[4];
    let symbols = &args[5..];
    let config: ChipChangeConfig = toml::from_str(
        &fs::read_to_string(strategy_path)
            .map_err(|error| format!("读取策略失败: {strategy_path}: {error}"))?,
    )
    .map_err(|error| format!("解析策略失败: {error}"))?;
    let compiled = config.compile()?;
    let runtime_keys = collect_chen_chip_runtime_keys(&compiled);
    let reader = DataReader::new_with_runtime_keys(source_dir, &runtime_keys)?;
    let mut results = Vec::new();

    for symbol in symbols {
        let row_data = reader.load_one(symbol, "qfq", "20210104", end_date)?;
        let chip_config = ChenChipConfig {
            warmup_days: 120,
            bucket_pct: 1.0,
        };
        let Some(compute_start_date) = row_data.trade_dates.get(chip_config.warmup_days) else {
            results.push(json!({"ts_code": symbol, "rows": []}));
            continue;
        };
        let snapshots = compute_chen_chip_snapshots_with_compiled_config(
            &row_data,
            compute_start_date,
            &compiled,
            chip_config,
        )?;
        let date_index = row_data
            .trade_dates
            .iter()
            .enumerate()
            .map(|(index, date)| (date.as_str(), index))
            .collect::<HashMap<_, _>>();
        let series = |key: &str, index: usize| {
            row_data
                .cols
                .get(key)
                .and_then(|values| values.get(index))
                .copied()
                .flatten()
        };
        let rows = snapshots
            .iter()
            .filter_map(|snapshot| {
                let date = snapshot.trade_date.as_deref()?;
                if date < start_date.as_str() || date > end_date.as_str() {
                    return None;
                }
                let index = *date_index.get(date)?;
                Some(json!({
                    "trade_date": date,
                    "open": series("O", index),
                    "high": series("H", index),
                    "low": series("L", index),
                    "close": series("C", index),
                    "pct_chg": series("PCT_CHG", index),
                    "turnover_rate": series("TOR", index),
                    "volume_ratio": series("VR", index),
                    "rsv_c90": series("RSV_C90", index),
                    "snapshot": {
                        "close": snapshot.close,
                        "minPrice": snapshot.min_price,
                        "maxPrice": snapshot.max_price,
                        "mainTotal": snapshot.main_total,
                        "retailTotal": snapshot.retail_total,
                        "totalChips": snapshot.total_chips,
                        "totalProfitRatio": snapshot.total_profit_ratio,
                        "totalTrappedRatio": snapshot.total_trapped_ratio,
                        "mainProfitRatio": snapshot.main_profit_ratio,
                        "mainTrappedRatio": snapshot.main_trapped_ratio,
                        "mainAvgCost": snapshot.main_avg_cost,
                        "chipPeakPrice": snapshot.chip_peak_price,
                        "percent70": snapshot.percent_70,
                        "percent90": snapshot.percent_90,
                    },
                }))
            })
            .collect::<Vec<_>>();
        results.push(json!({"ts_code": symbol, "rows": rows}));
    }

    let output = json!({
        "source_dir": source_dir,
        "strategy_path": strategy_path,
        "start_date": start_date,
        "end_date": end_date,
        "warmup_days": 120,
        "bucket_pct": 1.0,
        "stocks": results,
    });
    fs::write(
        output_path,
        serde_json::to_vec(&output).map_err(|error| format!("序列化结果失败: {error}"))?,
    )
    .map_err(|error| format!("写入结果失败: {output_path}: {error}"))?;
    Ok(())
}
