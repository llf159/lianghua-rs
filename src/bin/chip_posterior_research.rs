use lianghua_data::data::{
    DataReader,
    cyq_chen::{
        ChenChipConfig, ChipChangeConfig, collect_chen_chip_runtime_keys,
        compute_chen_chip_snapshots_from_initial_bins_with_compiled_config,
        compute_chen_chip_snapshots_with_compiled_config, estimate_chen_chip_expression_warmup,
    },
};
use serde_json::json;
use std::{
    collections::{HashMap, HashSet},
    env, fs,
    time::Instant,
};

fn main() -> Result<(), String> {
    let args: Vec<_> = env::args().skip(1).collect();
    if args.len() < 5 {
        return Err(
            "用法: chip_posterior_research <source> <策略.toml> <end> <输出目录> <股票...>".into(),
        );
    }
    let text = fs::read_to_string(&args[1]).map_err(|e| e.to_string())?;
    let base = ChipChangeConfig::from_toml_str(&text)?;
    let mut variants = Vec::new();
    let mut keys = HashSet::new();
    let configured_only = env::var("LIANGHUA_RESEARCH_SINGLE").as_deref() == Ok("1");
    let settings = if configured_only {
        vec![(None, 0.0)]
    } else {
        [
            (0, 0.0),
            (3, 0.35),
            (3, 0.65),
            (5, 0.35),
            (5, 0.65),
            (10, 0.35),
            (10, 0.65),
        ]
        .into_iter()
        .map(|(days, strength)| (Some(days), strength))
        .collect()
    };
    for (days, strength) in settings {
        let mut config = base.clone();
        if let Some(days) = days {
            if days == 0 {
                config.strategy.retain(|r| r.confirm_after == 0);
            } else {
                for rule in &mut config.strategy {
                    if rule.confirm_after > 0 {
                        rule.when = rule.when.replace(
                            &format!("N := {};", rule.confirm_after),
                            &format!("N := {days};"),
                        );
                        rule.confirm_after = days;
                        rule.bias = strength;
                    }
                }
            }
        }
        let compiled = config.compile()?;
        keys.extend(collect_chen_chip_runtime_keys(&compiled));
        variants.push((
            days.map(|d| format!("d{d}_s{strength}"))
                .unwrap_or_else(|| "configured".into()),
            compiled,
        ));
    }
    let reader = DataReader::new_with_runtime_keys(&args[0], &keys)?;
    fs::create_dir_all(&args[3]).map_err(|e| e.to_string())?;
    let metadata = fs::metadata(format!("{}/stock_data.db", args[0])).map_err(|e| e.to_string())?;
    let provenance = json!({"algorithm": 4, "strategy":text, "source":args[0], "source_bytes":metadata.len(), "source_modified":format!("{:?}",metadata.modified()), "source_modified_ns":metadata.modified().map_err(|e| e.to_string())?.duration_since(std::time::UNIX_EPOCH).map_err(|e| e.to_string())?.as_nanos(), "start":"20210104", "end":args[2], "warmup":120, "bucket_pct":1.0});
    for symbol in &args[4..] {
        let path = format!("{}/{symbol}.json", args[3]);
        if let Ok(bytes) = fs::read(&path) {
            if let Ok(cached) = serde_json::from_slice::<serde_json::Value>(&bytes) {
                if cached["provenance"] == provenance
                    && (!configured_only || cached["variants"].get("configured").is_some())
                    && (configured_only || cached["variants"].get("d0_s0").is_some())
                {
                    eprintln!("{symbol}: cache");
                    continue;
                }
            }
        }
        let start = Instant::now();
        let rows = reader.load_one(symbol, "qfq", "20210104", &args[2])?;
        let Some(first) = rows.trade_dates.get(120) else {
            eprintln!("{symbol}: insufficient history");
            continue;
        };
        let index: HashMap<_, _> = rows
            .trade_dates
            .iter()
            .enumerate()
            .map(|(i, d)| (d.as_str(), i))
            .collect();
        let bars: Vec<_> = rows
            .trade_dates
            .iter()
            .enumerate()
            .map(|(i, d)| {
                json!([
                    d,
                    rows.cols["O"][i],
                    rows.cols["H"][i],
                    rows.cols["L"][i],
                    rows.cols["C"][i],
                    rows.cols["TOR"][i]
                ])
            })
            .collect();
        let mut output = serde_json::Map::new();
        let mut benchmark = serde_json::Value::Null;
        for (name, config) in &variants {
            let compute_start = Instant::now();
            let snapshots = compute_chen_chip_snapshots_with_compiled_config(
                &rows,
                first,
                config,
                ChenChipConfig::default(),
            )?;
            let full_seconds = compute_start.elapsed().as_secs_f64();
            if symbol == &args[4] && name == "d3_s0.35" && snapshots.len() >= 2 {
                let tail_len = 120.max(estimate_chen_chip_expression_warmup(config)?) + 1;
                let tail_start = rows.trade_dates.len().saturating_sub(tail_len);
                let mut tail = rows.clone();
                tail.trade_dates = rows.trade_dates[tail_start..].to_vec();
                for values in tail.cols.values_mut() {
                    *values = values[tail_start..].to_vec();
                }
                let resume_start = Instant::now();
                let resumed = compute_chen_chip_snapshots_from_initial_bins_with_compiled_config(
                    &tail,
                    tail.trade_dates.last().unwrap(),
                    &snapshots[snapshots.len() - 2].bins,
                    &[],
                    config,
                    ChenChipConfig::default(),
                )?;
                let resume_seconds = resume_start.elapsed().as_secs_f64();
                let expected = snapshots.last().unwrap();
                let actual = &resumed[0];
                let error = (expected.main_total - actual.main_total).abs();
                if error > 1e-8 {
                    return Err(format!("真实数据续算不一致: {symbol}: {error}"));
                }
                benchmark = json!({"symbol":symbol, "full_bars":rows.trade_dates.len(), "loaded_tail_bars":tail.trade_dates.len(), "resumed_bars":1, "full_seconds":full_seconds, "resume_seconds":resume_seconds, "main_total_error":error});
            }
            let values: Vec<_> = snapshots
                .iter()
                .map(|s| {
                    json!([
                        index[s.trade_date.as_deref().unwrap()],
                        s.main_total,
                        s.main_avg_cost,
                        s.percent_70.concentration
                    ])
                })
                .collect();
            output.insert(name.clone(), json!(values));
        }
        let output = json!({"provenance":provenance, "symbol":symbol, "bars":bars, "variants":output, "seconds":start.elapsed().as_secs_f64(), "benchmark":benchmark});
        let temporary = format!("{path}.tmp");
        fs::write(
            &temporary,
            serde_json::to_vec(&output).map_err(|e| e.to_string())?,
        )
        .map_err(|e| e.to_string())?;
        fs::rename(temporary, path).map_err(|e| e.to_string())?;
        eprintln!(
            "{symbol}: {:.2}s / {} variants",
            start.elapsed().as_secs_f64(),
            variants.len()
        );
    }
    Ok(())
}
