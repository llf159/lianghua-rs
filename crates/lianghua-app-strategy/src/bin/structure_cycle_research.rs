use lianghua_app_strategy::statistics::{StructureEngine, get_market_structure};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Deserialize)]
struct Bar {
    ts_code: String,
    trade_date: String,
    close: f64,
}

fn tag(value: impl Serialize) -> Result<String, serde_json::Error> {
    Ok(serde_json::to_value(value)?
        .as_str()
        .unwrap_or_default()
        .to_string())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.len() == 4 && args[0] == "--snapshot" {
        let data = get_market_structure(args[1].clone(), Some(args[2].clone()))?;
        std::fs::write(&args[3], serde_json::to_vec_pretty(&data)?)?;
        return Ok(());
    }
    if args.len() != 2 {
        return Err("用法: structure_cycle_research indexes.csv history.csv；或 --snapshot source_dir YYYYMMDD output.json".into());
    }
    let mut reader = csv::Reader::from_path(&args[0])?;
    let mut series = BTreeMap::<String, Vec<Bar>>::new();
    for row in reader.deserialize::<Bar>() {
        let bar = row?;
        series.entry(bar.ts_code.clone()).or_default().push(bar);
    }
    let mut writer = csv::Writer::from_path(&args[1])?;
    writer.write_record([
        "code",
        "trade_date",
        "threshold",
        "degree",
        "close",
        "state",
        "phase",
        "stage",
        "guard",
        "guard_date",
        "guard_known_date",
        "confirmation_bars",
        "breakout",
        "next_trigger",
        "state_since",
        "events",
        "pivots",
    ])?;
    let mut checks = 0;
    for (code, mut bars) in series {
        bars.sort_by(|left, right| left.trade_date.cmp(&right.trade_date));
        for threshold in [0.015, 0.02, 0.03] {
            let mut engine = StructureEngine::new(threshold)?;
            let mut history = Vec::new();
            for bar in &bars {
                let levels = engine.push(&bar.trade_date, bar.close)?;
                for level in &levels {
                    let events = level
                        .events
                        .iter()
                        .filter(|event| event.trade_date == bar.trade_date)
                        .map(|event| tag(event.kind))
                        .collect::<Result<Vec<_>, _>>()?
                        .join("|");
                    writer.write_record([
                        code.clone(),
                        bar.trade_date.clone(),
                        threshold.to_string(),
                        level.degree.to_string(),
                        bar.close.to_string(),
                        tag(level.state)?,
                        level.phase.clone(),
                        level.stage.clone(),
                        level
                            .guard
                            .as_ref()
                            .map(|point| point.price.to_string())
                            .unwrap_or_default(),
                        level
                            .guard
                            .as_ref()
                            .map(|point| point.trade_date.clone())
                            .unwrap_or_default(),
                        level
                            .guard
                            .as_ref()
                            .map(|point| point.confirmed_date.clone())
                            .unwrap_or_default(),
                        level
                            .guard
                            .as_ref()
                            .map(|point| point.confirmation_bars.to_string())
                            .unwrap_or_default(),
                        level
                            .breakout_price
                            .map(|price| price.to_string())
                            .unwrap_or_default(),
                        level
                            .next_trigger_price
                            .map(|price| price.to_string())
                            .unwrap_or_default(),
                        level.state_since.clone().unwrap_or_default(),
                        events,
                        level.pivot_count.to_string(),
                    ])?;
                }
                history.push(levels);
            }
            for cut in [
                bars.len() / 3,
                bars.len() * 2 / 3,
                bars.len().saturating_sub(20),
            ] {
                let mut prefix = StructureEngine::new(threshold)?;
                for (index, bar) in bars[..cut].iter().enumerate() {
                    assert_eq!(prefix.push(&bar.trade_date, bar.close)?, history[index]);
                }
                checks += 1;
            }
        }
    }
    writer.flush()?;
    println!("逐日前缀一致性检查通过: {checks}");
    Ok(())
}
