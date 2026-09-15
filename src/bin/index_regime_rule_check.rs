use std::{env, fs, path::Path};

use lianghua_data::data::{DataReader, runtime::row_into_rt};
use lianghua_scoring::expr::{
    eval::{Runtime, Value},
    validation::parse_expression_program,
};
use lianghua_scoring::scoring::{
    rule_cache::cache_rule_build,
    runner::{ScoringMemoryMode, scoring_all_to_memory_with_mode},
    tools::rt_max_len,
};

fn latch_expr(prefix: &str, down: bool) -> String {
    let head = "
IDX_UP := CROSS({P}DUOKONG_SHORT, {P}DUOKONG_LONG);
IDX_DOWN := CROSS({P}DUOKONG_LONG, {P}DUOKONG_SHORT);
IDX_LU := BARSLAST(IDX_UP);
IDX_LD := BARSLAST(IDX_DOWN);"
        .replace("{P}", prefix);
    let clause = if down {
        "
(
    (IDX_LD >= 0 AND (!(IDX_LU >= 0) OR IDX_LD < IDX_LU))
    OR (!(IDX_LU >= 0) AND !(IDX_LD >= 0) AND {P}DUOKONG_LONG > {P}DUOKONG_SHORT)
)"
    } else {
        "
(
    (IDX_LU >= 0 AND (!(IDX_LD >= 0) OR IDX_LU < IDX_LD))
    OR (!(IDX_LU >= 0) AND !(IDX_LD >= 0) AND {P}DUOKONG_SHORT > {P}DUOKONG_LONG)
)"
    }
    .replace("{P}", prefix);
    format!("{head}{clause}")
}

fn eval_bool_series(rt: &mut Runtime, expr: &str) -> Result<Vec<bool>, String> {
    let stmts =
        parse_expression_program(expr).map_err(|e| format!("锁存表达式解析错误:{}", e.msg))?;
    let value = rt
        .eval_program(&stmts)
        .map_err(|e| format!("锁存表达式计算错误:{}", e.msg))?;
    let len = rt_max_len(rt);
    Value::as_bool_series(&value, len).map_err(|e| format!("锁存表达式返回非布尔:{}", e.msg))
}

fn replay_to_csv(
    source: &str,
    strategy_path: &str,
    label: &str,
    target_rules: &[&str],
    start_date: &str,
    end_date: &str,
    out_path: &Path,
) -> Result<(), String> {
    let started = std::time::Instant::now();
    let (batch, profile) = scoring_all_to_memory_with_mode(
        source,
        Some(strategy_path),
        "qfq",
        start_date,
        end_date,
        ScoringMemoryMode::RuleBacktest,
    )?;
    let rules = cache_rule_build(source, Some(strategy_path))?;
    let targets: std::collections::HashSet<&str> = target_rules.iter().copied().collect();
    let mut out = String::from("ts_code,trade_date,rule_name,rule_score\n");
    let mut kept = 0usize;
    for row in &batch.compact_rule_rows {
        let rule = rules.get(row.rule_id as usize).ok_or("规则序号越界")?;
        if !targets.contains(rule.name.as_str()) {
            continue;
        }
        let summary = batch
            .summary_rows
            .get(row.summary_index as usize)
            .ok_or("总榜行序号越界")?;
        out.push_str(&format!(
            "{},{},{},{}\n",
            summary.ts_code, summary.trade_date, rule.name, row.rule_score
        ));
        kept += 1;
    }
    fs::write(out_path, out).map_err(|e| e.to_string())?;
    eprintln!(
        "[{label}] 股票 {} 只，命中行 {}，耗时 {:.1}s",
        profile.stock_count,
        kept,
        started.elapsed().as_secs_f64()
    );
    Ok(())
}

fn main() -> Result<(), String> {
    let args: Vec<_> = env::args().skip(1).collect();
    if args.len() != 7
        && (args.len() != 8 || (args[7] != "--modified-only" && args[7] != "--validate-only"))
    {
        return Err(
            "用法: index_regime_rule_check <源目录> <原版toml> <改版toml> <锁存探针toml> <开始日> <结束日> <输出目录> [--modified-only|--validate-only]"
                .into(),
        );
    }
    let (source, orig, modified, latch, start, end, outdir) = (
        args[0].as_str(),
        args[1].as_str(),
        args[2].as_str(),
        args[3].as_str(),
        args[4].as_str(),
        args[5].as_str(),
        args[6].as_str(),
    );
    if args.get(7).is_some_and(|mode| mode == "--validate-only") {
        let rules = cache_rule_build(source, Some(modified))?;
        eprintln!("[validate] 解析并编译规则 {} 条", rules.len());
        return Ok(());
    }
    let outdir = Path::new(outdir);
    fs::create_dir_all(outdir).map_err(|e| e.to_string())?;

    let modified_only = args.get(7).is_some_and(|mode| mode == "--modified-only");
    if !modified_only {
        let keys = ["DUOKONG_SHORT", "DUOKONG_LONG"]
            .iter()
            .map(|key| key.to_string())
            .collect::<std::collections::HashSet<_>>();
        let reader = DataReader::new_with_runtime_keys(source, &keys)?;
        let row = reader.load_one("399300.SZ", "ind", "20210104", end)?;
        let dates = row.trade_dates.clone();
        let up = eval_bool_series(&mut row_into_rt(row.clone())?, &latch_expr("", false))?;
        let down = eval_bool_series(&mut row_into_rt(row)?, &latch_expr("", true))?;
        if up.len() != dates.len() || down.len() != dates.len() {
            return Err("指数锁存序列长度与交易日不一致".into());
        }
        let mut output = String::from("trade_date,latch_up,latch_down\n");
        for (i, date) in dates.iter().enumerate() {
            output.push_str(&format!(
                "{},{},{}\n",
                date,
                if up[i] { 1 } else { 0 },
                if down[i] { 1 } else { 0 }
            ));
        }
        fs::write(outdir.join("latch_index.csv"), output).map_err(|e| e.to_string())?;
        eprintln!("[latch_index] 沪深300自身日历锁存已导出");
    }

    let targets = [
        "多炮",
        "高位震荡出货",
        "brick技术性洗盘",
        "brick异动",
        "两根战法",
        "呼吸",
        "极端下跌",
    ];
    if !modified_only {
        replay_to_csv(
            source,
            orig,
            "orig",
            &targets,
            start,
            end,
            &outdir.join("orig_hits.csv"),
        )?;
    }
    replay_to_csv(
        source,
        modified,
        "modified",
        &targets,
        start,
        end,
        &outdir.join("modified_hits.csv"),
    )?;
    if !modified_only {
        replay_to_csv(
            source,
            latch,
            "latch",
            &["I300锁存上行", "I300锁存下行"],
            start,
            end,
            &outdir.join("latch_stocks.csv"),
        )?;
    }
    Ok(())
}
