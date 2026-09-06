use lianghua_data::data::{
    DataReader, ScoreConfig,
    cyq_chen::{
        ChenChipConfig, ChipChangeConfig, collect_chen_chip_runtime_keys,
        compute_chen_chip_snapshots_with_compiled_config,
    },
    runtime::row_into_rt,
};
use lianghua_scoring::scoring::{
    rule_cache::cache_rule_build, scoring_rules_details_cache, scoring_rules_total_cache,
};
use serde_json::json;
use std::{collections::HashSet, env, fs};

fn main() -> Result<(), String> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    if args.len() < 6 {
        return Err("用法: native_chip_strategy_check <源目录> <筹码.toml> <评分.toml> <结束日> <输出.json> <股票...>".into());
    }
    let chip =
        ChipChangeConfig::from_toml_str(&fs::read_to_string(&args[1]).map_err(|e| e.to_string())?)?
            .compile()?;
    let score_path = fs::canonicalize(&args[2]).map_err(|e| e.to_string())?;
    let score_path = score_path.to_str().ok_or("评分路径不是 UTF-8")?;
    let score = ScoreConfig::load_with_strategy_path(&args[0], Some(score_path))?;
    let rules = cache_rule_build(&args[0], Some(score_path))?;
    let reference = env::var("LIANGHUA_REFERENCE_SCORE")
        .ok()
        .map(|path| {
            let path = fs::canonicalize(path).map_err(|e| e.to_string())?;
            cache_rule_build(&args[0], Some(path.to_str().ok_or("参考路径不是 UTF-8")?))
        })
        .transpose()?;
    let mut keys = collect_chen_chip_runtime_keys(&chip);
    keys.extend(["O", "H", "L", "C", "TOR", "AMOUNT"].map(str::to_string));
    let reader = DataReader::new_with_runtime_keys(&args[0], &keys)?;
    let mut output = Vec::new();
    for code in &args[5..] {
        let mut row = reader.load_one(code, "qfq", "20210104", &args[3])?;
        let start = row.trade_dates.get(120).ok_or("至少需要121根K线")?.clone();
        let snapshots = compute_chen_chip_snapshots_with_compiled_config(
            &row,
            &start,
            &chip,
            ChenChipConfig::default(),
        )?;
        for key in ["CYQ_MT", "CYQ_RT", "CYQ_MAC", "CYQ_P70C"] {
            row.cols
                .insert(key.into(), vec![None; row.trade_dates.len()]);
        }
        for snap in snapshots {
            let Some(date) = snap.trade_date else {
                continue;
            };
            let i = row
                .trade_dates
                .binary_search(&date)
                .map_err(|_| "快照日期不存在")?;
            for (key, value) in [
                ("CYQ_MT", snap.main_total),
                ("CYQ_RT", snap.retail_total),
                ("CYQ_MAC", snap.main_avg_cost),
                ("CYQ_P70C", snap.percent_70.concentration),
            ] {
                row.cols.get_mut(key).unwrap()[i] = Some(value);
            }
        }
        let dates = row.trade_dates.clone();
        let closes = row.cols.get("C").ok_or("缺少C")?.clone();
        let mut runtime = row_into_rt(row.clone())?;
        let (totals, details) = scoring_rules_details_cache(&mut runtime, &rules)?;
        if totals
            .iter()
            .any(|v| !v.is_finite() || *v < -1e-8 || *v > 100.0 + 1e-8)
        {
            return Err(format!("{code}: 分数越界"));
        }
        if let Some(reference) = &reference {
            let old = scoring_rules_total_cache(&mut row_into_rt(row.clone())?, reference)?;
            if old.len() != totals.len()
                || old.iter().zip(&totals).any(|(a, b)| (a - b).abs() > 1e-10)
            {
                return Err(format!("{code}: 合并后的评分与参考配置不一致"));
            }
        }
        let cut = dates.len() * 2 / 3;
        row.trade_dates.truncate(cut);
        for values in row.cols.values_mut() {
            values.truncate(cut);
        }
        let prefix = scoring_rules_total_cache(&mut row_into_rt(row)?, &rules)?;
        if prefix
            .iter()
            .zip(&totals)
            .any(|(a, b)| (a - b).abs() > 1e-10)
        {
            return Err(format!("{code}: 评分包含后视数据"));
        }
        let hit_rules = details
            .iter()
            .filter(|r| r.triggered.iter().any(|v| *v))
            .map(|r| r.name.clone())
            .collect::<HashSet<_>>();
        let rows = dates
            .iter()
            .enumerate()
            .filter(|(i, _)| *i >= 280)
            .map(|(i, d)| json!({"date":d,"close":closes[i],"score":totals[i]}))
            .collect::<Vec<_>>();
        output.push(json!({"code":code,"rows":rows,"rules_hit":hit_rules.len(),"prefix_equal":true,"latest_score":totals.last(),"reference_score_equal":reference.as_ref().map(|_|true)}));
        eprintln!(
            "{code}: {} bars, {} rules hit",
            dates.len(),
            hit_rules.len()
        );
    }
    fs::write(&args[4],serde_json::to_string_pretty(&json!({"chip_strategy":args[1],"score_strategy":args[2],"rules":score.rule.len(),"stocks":output,"scope":"native compatibility and causality check, not performance validation"})).map_err(|e|e.to_string())?).map_err(|e|e.to_string())?;
    Ok(())
}
