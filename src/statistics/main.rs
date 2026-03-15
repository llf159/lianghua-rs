use std::{
    collections::HashMap,
    env,
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
};

use crate::{
    data::result_db_path,
    data::{RuleTag, ScopeWay, ScoreConfig, ScoreRule},
};
use csv::Writer;
use duckdb::{Connection, params};
use serde::Serialize;

#[derive(Debug)]
struct AppConfig {
    source_dir: PathBuf,
    rule_file: PathBuf,
    out_dir: PathBuf,
}

#[derive(Debug, Clone)]
struct RuleMeta {
    rule_name: String,
    scope_way: String,
    is_each: bool,
    points: f64,
    tag: String,
    explain: String,
}

#[derive(Debug)]
struct SampleSummary {
    score_summary_rows: i64,
    trade_days: i64,
    stock_count: i64,
    min_trade_date: String,
    max_trade_date: String,
    avg_total_score: f64,
    median_total_score: f64,
    min_total_score: f64,
    max_total_score: f64,
}

#[derive(Debug)]
struct HitSummary {
    trade_date: Option<String>,
    stock_count: i64,
    avg_hit_rule_cnt: f64,
    median_hit_rule_cnt: f64,
    p90_hit_rule_cnt: f64,
    avg_pos_hit_rule_cnt: f64,
    avg_neg_hit_rule_cnt: f64,
    avg_total_score: Option<f64>,
    median_total_score: Option<f64>,
    min_total_score: Option<f64>,
    max_total_score: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
struct RuleStatRow {
    #[serde(rename = "规则名")]
    rule_name: String,
    #[serde(rename = "命中方式")]
    scope_way: String,
    #[serde(rename = "标签")]
    tag: String,
    #[serde(rename = "分值")]
    points: f64,
    #[serde(rename = "说明")]
    explain: String,
    #[serde(rename = "命中样本数")]
    hit_rows: i64,
    #[serde(rename = "命中率")]
    hit_rate: f64,
    #[serde(rename = "平均得分")]
    avg_score: f64,
    #[serde(rename = "命中时平均得分")]
    avg_score_when_hit: Option<f64>,
    #[serde(rename = "总得分贡献")]
    total_score: f64,
    #[serde(rename = "最新日命中样本数")]
    latest_hit_rows: i64,
    #[serde(rename = "最新日命中率")]
    latest_hit_rate: Option<f64>,
    #[serde(rename = "最新日平均得分")]
    latest_avg_score: Option<f64>,
    #[serde(rename = "最新日命中时平均得分")]
    latest_avg_score_when_hit: Option<f64>,
    #[serde(rename = "最新日总得分贡献")]
    latest_total_score: f64,
}

#[derive(Debug, Clone, Serialize)]
struct EachRuleRow {
    #[serde(rename = "规则名")]
    rule_name: String,
    #[serde(rename = "分值")]
    points: f64,
    #[serde(rename = "命中率")]
    hit_rate: f64,
    #[serde(rename = "平均得分")]
    avg_score: f64,
    #[serde(rename = "全样本平均命中次数")]
    avg_hit_count_overall: f64,
    #[serde(rename = "触发时平均命中次数")]
    avg_hit_count_when_hit: Option<f64>,
    #[serde(rename = "最大命中次数")]
    max_hit_count: f64,
    #[serde(rename = "命中次数P90")]
    p90_hit_count_overall: f64,
    #[serde(rename = "最新日命中样本数")]
    latest_hit_rows: i64,
    #[serde(rename = "最新日命中率")]
    latest_hit_rate: Option<f64>,
    #[serde(rename = "最新日全样本平均命中次数")]
    latest_avg_hit_count_overall: Option<f64>,
    #[serde(rename = "最新日触发时平均命中次数")]
    latest_avg_hit_count_when_hit: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
struct DailyStatRow {
    #[serde(rename = "交易日")]
    trade_date: String,
    #[serde(rename = "股票数")]
    stock_count: i64,
    #[serde(rename = "平均命中策略数")]
    avg_hit_rule_cnt: f64,
    #[serde(rename = "命中策略数中位数")]
    median_hit_rule_cnt: f64,
    #[serde(rename = "命中策略数P90")]
    p90_hit_rule_cnt: f64,
    #[serde(rename = "平均正向命中数")]
    avg_pos_hit_rule_cnt: f64,
    #[serde(rename = "平均负向命中数")]
    avg_neg_hit_rule_cnt: f64,
    #[serde(rename = "平均总分")]
    avg_total_score: f64,
    #[serde(rename = "总分中位数")]
    median_total_score: f64,
    #[serde(rename = "最低总分")]
    min_total_score: f64,
    #[serde(rename = "最高总分")]
    max_total_score: f64,
}

#[derive(Debug, Clone, Serialize)]
struct ScopeStatRow {
    #[serde(rename = "命中方式")]
    scope_way: String,
    #[serde(rename = "规则数")]
    rule_count: usize,
    #[serde(rename = "规则平均命中率")]
    avg_rule_hit_rate: f64,
    #[serde(rename = "规则平均得分")]
    avg_rule_avg_score: f64,
    #[serde(rename = "总得分贡献")]
    total_score: f64,
}

const COMBO_MAX_SIZE: usize = 4;
const SUMMARY_TOP_COMBO_COUNT: usize = 8;
const SUMMARY_TOP_PATTERN_COUNT: usize = 10;
const SUMMARY_HIGH_ORDER_PATTERN_MIN_HIT_COUNT: usize = 5;

#[derive(Debug, Clone, Default)]
struct CountScoreAgg {
    sample_count: i64,
    total_score_sum: f64,
}

#[derive(Debug, Clone, Serialize)]
struct ComboStatRow {
    #[serde(rename = "组合阶数")]
    combo_size: usize,
    #[serde(rename = "策略组合")]
    combo_name: String,
    #[serde(rename = "同时触发样本数")]
    co_hit_count: i64,
    #[serde(rename = "同时触发率")]
    co_hit_rate: f64,
    #[serde(rename = "独立预期同时触发率")]
    expected_co_hit_rate: f64,
    #[serde(rename = "同时触发相关偏差")]
    co_hit_corr_gap: f64,
    #[serde(rename = "同时触发Lift")]
    co_hit_lift: Option<f64>,
    #[serde(rename = "同时不触发样本数")]
    co_miss_count: i64,
    #[serde(rename = "同时不触发率")]
    co_miss_rate: f64,
    #[serde(rename = "独立预期同时不触发率")]
    expected_co_miss_rate: f64,
    #[serde(rename = "同时不触发相关偏差")]
    co_miss_corr_gap: f64,
    #[serde(rename = "同时不触发Lift")]
    co_miss_lift: Option<f64>,
    #[serde(rename = "Phi相关系数(仅2阶)")]
    phi_correlation: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
struct TriggerPatternRow {
    #[serde(rename = "触发策略数")]
    hit_rule_count: usize,
    #[serde(rename = "未触发策略数")]
    miss_rule_count: usize,
    #[serde(rename = "触发策略组合")]
    hit_rules: String,
    #[serde(rename = "未触发策略组合")]
    miss_rules: String,
    #[serde(rename = "样本数")]
    sample_count: i64,
    #[serde(rename = "样本占比")]
    sample_rate: f64,
    #[serde(rename = "独立预期样本占比")]
    expected_sample_rate: f64,
    #[serde(rename = "签名相关偏差")]
    pattern_corr_gap: f64,
    #[serde(rename = "签名Lift")]
    pattern_lift: Option<f64>,
    #[serde(rename = "平均总分")]
    avg_total_score: Option<f64>,
}

pub fn statistics_main() -> Result<(), String> {
    let config = parse_args()?;
    run_statistics(&config)
}

fn run_statistics(config: &AppConfig) -> Result<(), String> {
    fs::create_dir_all(&config.out_dir).map_err(|e| {
        format!(
            "创建输出目录失败: path={}, err={e}",
            config.out_dir.display()
        )
    })?;

    let rules = load_rules(&config.rule_file)?;
    let rule_meta_map = build_rule_meta_map(&rules);
    let rule_names: Vec<String> = rules.iter().map(|rule| rule.name.clone()).collect();

    let result_db = result_db_path(
        config
            .source_dir
            .to_str()
            .ok_or_else(|| "source_dir 不是有效 UTF-8".to_string())?,
    );
    let conn = Connection::open(
        result_db
            .to_str()
            .ok_or_else(|| "结果库路径不是有效 UTF-8".to_string())?,
    )
    .map_err(|e| format!("打开结果库失败: {e}"))?;

    let sample_summary = query_sample_summary(&conn)?;
    let latest_trade_date = sample_summary.max_trade_date.clone();
    let overall_hits = query_hit_summary(&conn, None)?;
    let latest_hits = query_hit_summary(&conn, Some(&latest_trade_date))?;
    let mut rule_stats = query_rule_stats(&conn, &latest_trade_date, &rule_meta_map)?;
    let mut each_rule_stats = query_each_rule_stats(&conn, &latest_trade_date, &rule_meta_map)?;
    let daily_stats = query_daily_stats(&conn)?;
    let mut scope_stats = build_scope_stats(&rule_stats);
    let pattern_aggs = query_trigger_pattern_aggs(&conn, &rule_names)?;
    let mut combo_stats = build_combo_stats(&pattern_aggs, &rule_names, COMBO_MAX_SIZE);
    let mut trigger_patterns = build_trigger_pattern_rows(&pattern_aggs, &rule_names);

    rule_stats.sort_by(|a, b| b.total_score.total_cmp(&a.total_score));
    each_rule_stats.sort_by(|a, b| {
        b.avg_hit_count_when_hit
            .unwrap_or(0.0)
            .total_cmp(&a.avg_hit_count_when_hit.unwrap_or(0.0))
    });
    scope_stats.sort_by(|a, b| b.total_score.total_cmp(&a.total_score));
    combo_stats.sort_by(|a, b| {
        a.combo_size
            .cmp(&b.combo_size)
            .then_with(|| b.co_hit_corr_gap.total_cmp(&a.co_hit_corr_gap))
            .then_with(|| {
                b.co_hit_lift
                    .unwrap_or(0.0)
                    .total_cmp(&a.co_hit_lift.unwrap_or(0.0))
            })
            .then_with(|| b.co_hit_count.cmp(&a.co_hit_count))
    });
    trigger_patterns.sort_by(|a, b| {
        b.pattern_corr_gap
            .total_cmp(&a.pattern_corr_gap)
            .then_with(|| {
                b.pattern_lift
                    .unwrap_or(0.0)
                    .total_cmp(&a.pattern_lift.unwrap_or(0.0))
            })
            .then_with(|| b.sample_count.cmp(&a.sample_count))
    });

    write_csv(&config.out_dir.join("rule_stats.csv"), &rule_stats)?;
    write_csv(&config.out_dir.join("each_rules.csv"), &each_rule_stats)?;
    write_csv(&config.out_dir.join("daily_stats.csv"), &daily_stats)?;
    write_csv(&config.out_dir.join("scope_stats.csv"), &scope_stats)?;
    write_csv(
        &config.out_dir.join("combo_correlation_stats.csv"),
        &combo_stats,
    )?;
    write_csv(
        &config.out_dir.join("trigger_pattern_correlation_stats.csv"),
        &trigger_patterns,
    )?;

    let latest_rule_stats = build_latest_rule_stats(&rule_stats);
    write_csv(
        &config.out_dir.join("latest_rule_stats.csv"),
        &latest_rule_stats,
    )?;

    write_summary_md(
        &config.out_dir.join("summary.md"),
        &sample_summary,
        &overall_hits,
        &latest_hits,
        &rule_stats,
        &each_rule_stats,
        &daily_stats,
        &scope_stats,
        &combo_stats,
        &trigger_patterns,
    )?;

    println!("报表输出目录={}", config.out_dir.display());
    println!("最新交易日={latest_trade_date}");
    println!(
        "输出文件=summary.md,rule_stats.csv,each_rules.csv,daily_stats.csv,scope_stats.csv,latest_rule_stats.csv,combo_correlation_stats.csv,trigger_pattern_correlation_stats.csv"
    );
    Ok(())
}

fn parse_args() -> Result<AppConfig, String> {
    let mut source_dir = PathBuf::from("./source");
    let mut rule_file: Option<PathBuf> = None;
    let mut out_dir: Option<PathBuf> = None;

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--source-dir" => {
                let value = args
                    .next()
                    .ok_or_else(|| "--source-dir 缺少参数".to_string())?;
                source_dir = PathBuf::from(value);
            }
            "--rule-file" => {
                let value = args
                    .next()
                    .ok_or_else(|| "--rule-file 缺少参数".to_string())?;
                rule_file = Some(PathBuf::from(value));
            }
            "--out-dir" => {
                let value = args
                    .next()
                    .ok_or_else(|| "--out-dir 缺少参数".to_string())?;
                out_dir = Some(PathBuf::from(value));
            }
            "--help" | "-h" => {
                print_help();
                std::process::exit(0);
            }
            other => {
                return Err(format!("未知参数: {other}"));
            }
        }
    }

    let rule_file = rule_file.unwrap_or_else(|| source_dir.join("score_rule.toml"));
    let out_dir = out_dir.unwrap_or_else(|| source_dir.join("strategy_hit_report"));
    Ok(AppConfig {
        source_dir,
        rule_file,
        out_dir,
    })
}

fn print_help() {
    println!("用法: cargo run -- [--source-dir 目录] [--rule-file 文件] [--out-dir 目录]");
}

fn load_rules(rule_file: &Path) -> Result<Vec<ScoreRule>, String> {
    let text = fs::read_to_string(rule_file)
        .map_err(|e| format!("读取规则文件失败: path={}, err={e}", rule_file.display()))?;
    let cfg: ScoreConfig = toml::from_str(&text).map_err(|e| format!("解析规则文件失败: {e}"))?;
    Ok(cfg.rule)
}

fn build_rule_meta_map(rules: &[ScoreRule]) -> HashMap<String, RuleMeta> {
    let mut out = HashMap::with_capacity(rules.len());
    for rule in rules {
        out.insert(
            rule.name.clone(),
            RuleMeta {
                rule_name: rule.name.clone(),
                scope_way: scope_way_label(rule.scope_way),
                is_each: matches!(rule.scope_way, ScopeWay::Each),
                points: rule.points,
                tag: tag_label(rule.tag).to_string(),
                explain: rule.explain.clone(),
            },
        );
    }
    out
}

fn scope_way_label(scope_way: ScopeWay) -> String {
    match scope_way {
        ScopeWay::Any => "窗口内任一命中".to_string(),
        ScopeWay::Last => "当日命中".to_string(),
        ScopeWay::Each => "窗口累计命中".to_string(),
        ScopeWay::Recent => "最近一次命中".to_string(),
        ScopeWay::Consec(n) => format!("连续命中>={n}"),
    }
}

fn tag_label(tag: RuleTag) -> &'static str {
    match tag {
        RuleTag::Normal => "普通",
        RuleTag::Opportunity => "机会",
    }
}

fn query_sample_summary(conn: &Connection) -> Result<SampleSummary, String> {
    let mut stmt = conn
        .prepare(
            r#"
            SELECT
                COUNT(*) AS score_summary_rows,
                COUNT(DISTINCT trade_date) AS trade_days,
                COUNT(DISTINCT ts_code) AS stock_count,
                MIN(trade_date) AS min_trade_date,
                MAX(trade_date) AS max_trade_date,
                AVG(total_score) AS avg_total_score,
                MEDIAN(total_score) AS median_total_score,
                MIN(total_score) AS min_total_score,
                MAX(total_score) AS max_total_score
            FROM score_summary
            "#,
        )
        .map_err(|e| format!("预编译样本汇总 SQL 失败: {e}"))?;
    let mut rows = stmt
        .query([])
        .map_err(|e| format!("执行样本汇总 SQL 失败: {e}"))?;
    let row = rows
        .next()
        .map_err(|e| format!("读取样本汇总结果失败: {e}"))?
        .ok_or_else(|| "score_summary 没有数据".to_string())?;

    Ok(SampleSummary {
        score_summary_rows: row.get(0).map_err(|e| format!("读取字段失败: {e}"))?,
        trade_days: row.get(1).map_err(|e| format!("读取字段失败: {e}"))?,
        stock_count: row.get(2).map_err(|e| format!("读取字段失败: {e}"))?,
        min_trade_date: row.get(3).map_err(|e| format!("读取字段失败: {e}"))?,
        max_trade_date: row.get(4).map_err(|e| format!("读取字段失败: {e}"))?,
        avg_total_score: row.get(5).map_err(|e| format!("读取字段失败: {e}"))?,
        median_total_score: row.get(6).map_err(|e| format!("读取字段失败: {e}"))?,
        min_total_score: row.get(7).map_err(|e| format!("读取字段失败: {e}"))?,
        max_total_score: row.get(8).map_err(|e| format!("读取字段失败: {e}"))?,
    })
}

fn query_hit_summary(conn: &Connection, trade_date: Option<&str>) -> Result<HitSummary, String> {
    let sql = r#"
        WITH per_stock_day AS (
            SELECT
                trade_date,
                ts_code,
                SUM(CASE WHEN rule_score != 0 THEN 1 ELSE 0 END) AS hit_rule_cnt,
                SUM(CASE WHEN rule_score > 0 THEN 1 ELSE 0 END) AS pos_hit_rule_cnt,
                SUM(CASE WHEN rule_score < 0 THEN 1 ELSE 0 END) AS neg_hit_rule_cnt,
                SUM(rule_score) + 50.0 AS total_score
            FROM score_details
            WHERE (? IS NULL OR trade_date = ?)
            GROUP BY 1, 2
        )
        SELECT
            COUNT(*) AS stock_count,
            AVG(hit_rule_cnt) AS avg_hit_rule_cnt,
            MEDIAN(hit_rule_cnt) AS median_hit_rule_cnt,
            QUANTILE_CONT(hit_rule_cnt, 0.9) AS p90_hit_rule_cnt,
            AVG(pos_hit_rule_cnt) AS avg_pos_hit_rule_cnt,
            AVG(neg_hit_rule_cnt) AS avg_neg_hit_rule_cnt,
            AVG(total_score) AS avg_total_score,
            MEDIAN(total_score) AS median_total_score,
            MIN(total_score) AS min_total_score,
            MAX(total_score) AS max_total_score
        FROM per_stock_day
    "#;

    let mut stmt = conn
        .prepare(sql)
        .map_err(|e| format!("预编译命中汇总 SQL 失败: {e}"))?;
    let mut rows = stmt
        .query(params![trade_date, trade_date])
        .map_err(|e| format!("执行命中汇总 SQL 失败: {e}"))?;
    let row = rows
        .next()
        .map_err(|e| format!("读取命中汇总失败: {e}"))?
        .ok_or_else(|| "score_details 没有数据".to_string())?;

    Ok(HitSummary {
        trade_date: trade_date.map(|v| v.to_string()),
        stock_count: row.get(0).map_err(|e| format!("读取字段失败: {e}"))?,
        avg_hit_rule_cnt: row.get(1).map_err(|e| format!("读取字段失败: {e}"))?,
        median_hit_rule_cnt: row.get(2).map_err(|e| format!("读取字段失败: {e}"))?,
        p90_hit_rule_cnt: row.get(3).map_err(|e| format!("读取字段失败: {e}"))?,
        avg_pos_hit_rule_cnt: row.get(4).map_err(|e| format!("读取字段失败: {e}"))?,
        avg_neg_hit_rule_cnt: row.get(5).map_err(|e| format!("读取字段失败: {e}"))?,
        avg_total_score: row.get(6).map_err(|e| format!("读取字段失败: {e}"))?,
        median_total_score: row.get(7).map_err(|e| format!("读取字段失败: {e}"))?,
        min_total_score: row.get(8).map_err(|e| format!("读取字段失败: {e}"))?,
        max_total_score: row.get(9).map_err(|e| format!("读取字段失败: {e}"))?,
    })
}

fn query_rule_stats(
    conn: &Connection,
    latest_trade_date: &str,
    rule_meta_map: &HashMap<String, RuleMeta>,
) -> Result<Vec<RuleStatRow>, String> {
    let sql = r#"
        SELECT
            rule_name,
            SUM(CASE WHEN rule_score != 0 THEN 1 ELSE 0 END) AS hit_rows,
            AVG(CASE WHEN rule_score != 0 THEN 1.0 ELSE 0.0 END) AS hit_rate,
            AVG(rule_score) AS avg_score,
            AVG(CASE WHEN rule_score != 0 THEN rule_score END) AS avg_score_when_hit,
            SUM(rule_score) AS total_score,
            SUM(CASE WHEN trade_date = ? AND rule_score != 0 THEN 1 ELSE 0 END) AS latest_hit_rows,
            AVG(CASE WHEN trade_date = ? THEN CASE WHEN rule_score != 0 THEN 1.0 ELSE 0.0 END END) AS latest_hit_rate,
            AVG(CASE WHEN trade_date = ? THEN rule_score END) AS latest_avg_score,
            AVG(CASE WHEN trade_date = ? AND rule_score != 0 THEN rule_score END) AS latest_avg_score_when_hit,
            SUM(CASE WHEN trade_date = ? THEN rule_score ELSE 0 END) AS latest_total_score
        FROM score_details
        GROUP BY rule_name
    "#;

    let mut stmt = conn
        .prepare(sql)
        .map_err(|e| format!("预编译规则统计 SQL 失败: {e}"))?;
    let mut rows = stmt
        .query(params![
            latest_trade_date,
            latest_trade_date,
            latest_trade_date,
            latest_trade_date,
            latest_trade_date
        ])
        .map_err(|e| format!("执行规则统计 SQL 失败: {e}"))?;

    let mut out = Vec::with_capacity(rule_meta_map.len());
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取规则统计结果失败: {e}"))?
    {
        let rule_name: String = row
            .get(0)
            .map_err(|e| format!("读取 rule_name 失败: {e}"))?;
        let meta = rule_meta_map
            .get(&rule_name)
            .cloned()
            .unwrap_or_else(|| RuleMeta {
                rule_name: rule_name.clone(),
                scope_way: "未知".to_string(),
                is_each: false,
                points: 0.0,
                tag: "未知".to_string(),
                explain: String::new(),
            });

        out.push(RuleStatRow {
            rule_name: meta.rule_name,
            scope_way: meta.scope_way,
            tag: meta.tag,
            points: meta.points,
            explain: meta.explain,
            hit_rows: row.get(1).map_err(|e| format!("读取字段失败: {e}"))?,
            hit_rate: row.get(2).map_err(|e| format!("读取字段失败: {e}"))?,
            avg_score: row.get(3).map_err(|e| format!("读取字段失败: {e}"))?,
            avg_score_when_hit: row.get(4).map_err(|e| format!("读取字段失败: {e}"))?,
            total_score: row.get(5).map_err(|e| format!("读取字段失败: {e}"))?,
            latest_hit_rows: row.get(6).map_err(|e| format!("读取字段失败: {e}"))?,
            latest_hit_rate: row.get(7).map_err(|e| format!("读取字段失败: {e}"))?,
            latest_avg_score: row.get(8).map_err(|e| format!("读取字段失败: {e}"))?,
            latest_avg_score_when_hit: row.get(9).map_err(|e| format!("读取字段失败: {e}"))?,
            latest_total_score: row.get(10).map_err(|e| format!("读取字段失败: {e}"))?,
        });
    }
    Ok(out)
}

fn query_each_rule_stats(
    conn: &Connection,
    latest_trade_date: &str,
    rule_meta_map: &HashMap<String, RuleMeta>,
) -> Result<Vec<EachRuleRow>, String> {
    let each_rules: Vec<&RuleMeta> = rule_meta_map
        .values()
        .filter(|meta| meta.is_each && meta.points != 0.0)
        .collect();
    let mut out = Vec::with_capacity(each_rules.len());

    for meta in each_rules {
        let sql = r#"
            SELECT
                AVG(CASE WHEN rule_score != 0 THEN 1.0 ELSE 0.0 END) AS hit_rate,
                AVG(rule_score) AS avg_score,
                AVG(rule_score / ?) AS avg_hit_count_overall,
                AVG(CASE WHEN rule_score != 0 THEN rule_score / ? END) AS avg_hit_count_when_hit,
                MAX(rule_score / ?) AS max_hit_count,
                QUANTILE_CONT(rule_score / ?, 0.9) AS p90_hit_count_overall,
                SUM(CASE WHEN trade_date = ? AND rule_score != 0 THEN 1 ELSE 0 END) AS latest_hit_rows,
                AVG(CASE WHEN trade_date = ? THEN CASE WHEN rule_score != 0 THEN 1.0 ELSE 0.0 END END) AS latest_hit_rate,
                AVG(CASE WHEN trade_date = ? THEN rule_score / ? END) AS latest_avg_hit_count_overall,
                AVG(CASE WHEN trade_date = ? AND rule_score != 0 THEN rule_score / ? END) AS latest_avg_hit_count_when_hit
            FROM score_details
            WHERE rule_name = ?
        "#;
        let mut stmt = conn
            .prepare(sql)
            .map_err(|e| format!("预编译 Each 统计 SQL 失败: {e}"))?;
        let mut rows = stmt
            .query(params![
                meta.points,
                meta.points,
                meta.points,
                meta.points,
                latest_trade_date,
                latest_trade_date,
                latest_trade_date,
                meta.points,
                latest_trade_date,
                meta.points,
                meta.rule_name
            ])
            .map_err(|e| format!("执行 Each 统计 SQL 失败: {e}"))?;
        let row = rows
            .next()
            .map_err(|e| format!("读取 Each 统计结果失败: {e}"))?
            .ok_or_else(|| format!("规则 {} 没有统计结果", meta.rule_name))?;

        out.push(EachRuleRow {
            rule_name: meta.rule_name.clone(),
            points: meta.points,
            hit_rate: row.get(0).map_err(|e| format!("读取字段失败: {e}"))?,
            avg_score: row.get(1).map_err(|e| format!("读取字段失败: {e}"))?,
            avg_hit_count_overall: row.get(2).map_err(|e| format!("读取字段失败: {e}"))?,
            avg_hit_count_when_hit: row.get(3).map_err(|e| format!("读取字段失败: {e}"))?,
            max_hit_count: row.get(4).map_err(|e| format!("读取字段失败: {e}"))?,
            p90_hit_count_overall: row.get(5).map_err(|e| format!("读取字段失败: {e}"))?,
            latest_hit_rows: row.get(6).map_err(|e| format!("读取字段失败: {e}"))?,
            latest_hit_rate: row.get(7).map_err(|e| format!("读取字段失败: {e}"))?,
            latest_avg_hit_count_overall: row.get(8).map_err(|e| format!("读取字段失败: {e}"))?,
            latest_avg_hit_count_when_hit: row.get(9).map_err(|e| format!("读取字段失败: {e}"))?,
        });
    }

    Ok(out)
}

fn query_daily_stats(conn: &Connection) -> Result<Vec<DailyStatRow>, String> {
    let sql = r#"
        WITH per_stock_day AS (
            SELECT
                trade_date,
                ts_code,
                SUM(CASE WHEN rule_score != 0 THEN 1 ELSE 0 END) AS hit_rule_cnt,
                SUM(CASE WHEN rule_score > 0 THEN 1 ELSE 0 END) AS pos_hit_rule_cnt,
                SUM(CASE WHEN rule_score < 0 THEN 1 ELSE 0 END) AS neg_hit_rule_cnt,
                SUM(rule_score) + 50.0 AS total_score
            FROM score_details
            GROUP BY 1, 2
        )
        SELECT
            trade_date,
            COUNT(*) AS stock_count,
            AVG(hit_rule_cnt) AS avg_hit_rule_cnt,
            MEDIAN(hit_rule_cnt) AS median_hit_rule_cnt,
            QUANTILE_CONT(hit_rule_cnt, 0.9) AS p90_hit_rule_cnt,
            AVG(pos_hit_rule_cnt) AS avg_pos_hit_rule_cnt,
            AVG(neg_hit_rule_cnt) AS avg_neg_hit_rule_cnt,
            AVG(total_score) AS avg_total_score,
            MEDIAN(total_score) AS median_total_score,
            MIN(total_score) AS min_total_score,
            MAX(total_score) AS max_total_score
        FROM per_stock_day
        GROUP BY trade_date
        ORDER BY trade_date ASC
    "#;

    let mut stmt = conn
        .prepare(sql)
        .map_err(|e| format!("预编译日度统计 SQL 失败: {e}"))?;
    let mut rows = stmt
        .query([])
        .map_err(|e| format!("执行日度统计 SQL 失败: {e}"))?;

    let mut out = Vec::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取日度统计结果失败: {e}"))?
    {
        out.push(DailyStatRow {
            trade_date: row.get(0).map_err(|e| format!("读取字段失败: {e}"))?,
            stock_count: row.get(1).map_err(|e| format!("读取字段失败: {e}"))?,
            avg_hit_rule_cnt: row.get(2).map_err(|e| format!("读取字段失败: {e}"))?,
            median_hit_rule_cnt: row.get(3).map_err(|e| format!("读取字段失败: {e}"))?,
            p90_hit_rule_cnt: row.get(4).map_err(|e| format!("读取字段失败: {e}"))?,
            avg_pos_hit_rule_cnt: row.get(5).map_err(|e| format!("读取字段失败: {e}"))?,
            avg_neg_hit_rule_cnt: row.get(6).map_err(|e| format!("读取字段失败: {e}"))?,
            avg_total_score: row.get(7).map_err(|e| format!("读取字段失败: {e}"))?,
            median_total_score: row.get(8).map_err(|e| format!("读取字段失败: {e}"))?,
            min_total_score: row.get(9).map_err(|e| format!("读取字段失败: {e}"))?,
            max_total_score: row.get(10).map_err(|e| format!("读取字段失败: {e}"))?,
        });
    }
    Ok(out)
}

fn build_scope_stats(rule_stats: &[RuleStatRow]) -> Vec<ScopeStatRow> {
    let mut grouped: HashMap<String, Vec<&RuleStatRow>> = HashMap::new();
    for row in rule_stats {
        grouped.entry(row.scope_way.clone()).or_default().push(row);
    }

    let mut out = Vec::with_capacity(grouped.len());
    for (scope_way, rows) in grouped {
        let rule_count = rows.len();
        let sum_hit_rate: f64 = rows.iter().map(|row| row.hit_rate).sum();
        let sum_avg_score: f64 = rows.iter().map(|row| row.avg_score).sum();
        let total_score: f64 = rows.iter().map(|row| row.total_score).sum();

        out.push(ScopeStatRow {
            scope_way,
            rule_count,
            avg_rule_hit_rate: sum_hit_rate / rule_count as f64,
            avg_rule_avg_score: sum_avg_score / rule_count as f64,
            total_score,
        });
    }
    out
}

fn query_trigger_pattern_aggs(
    conn: &Connection,
    rule_names: &[String],
) -> Result<HashMap<Vec<usize>, CountScoreAgg>, String> {
    let rule_index_map: HashMap<&str, usize> = rule_names
        .iter()
        .enumerate()
        .map(|(idx, name)| (name.as_str(), idx))
        .collect();
    let sql = r#"
        SELECT
            trade_date,
            ts_code,
            rule_name,
            rule_score
        FROM score_details
        ORDER BY trade_date ASC, ts_code ASC
    "#;

    let mut stmt = conn
        .prepare(sql)
        .map_err(|e| format!("预编译触发模式 SQL 失败: {e}"))?;
    let mut rows = stmt
        .query([])
        .map_err(|e| format!("执行触发模式 SQL 失败: {e}"))?;

    let mut out: HashMap<Vec<usize>, CountScoreAgg> = HashMap::new();
    let mut current_trade_date: Option<String> = None;
    let mut current_ts_code: Option<String> = None;
    let mut current_hits: Vec<usize> = Vec::new();
    let mut current_total_score = 50.0;

    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取触发模式结果失败: {e}"))?
    {
        let trade_date: String = row
            .get(0)
            .map_err(|e| format!("读取 trade_date 失败: {e}"))?;
        let ts_code: String = row.get(1).map_err(|e| format!("读取 ts_code 失败: {e}"))?;
        let rule_name: String = row
            .get(2)
            .map_err(|e| format!("读取 rule_name 失败: {e}"))?;
        let rule_score: f64 = row
            .get(3)
            .map_err(|e| format!("读取 rule_score 失败: {e}"))?;

        let sample_changed = current_trade_date.as_deref() != Some(trade_date.as_str())
            || current_ts_code.as_deref() != Some(ts_code.as_str());
        if sample_changed {
            finalize_trigger_pattern_sample(
                &mut out,
                &mut current_hits,
                &mut current_total_score,
                current_trade_date.as_deref(),
                current_ts_code.as_deref(),
            );
            current_trade_date = Some(trade_date);
            current_ts_code = Some(ts_code);
        }

        current_total_score += rule_score;
        if rule_score != 0.0 {
            let rule_idx = *rule_index_map
                .get(rule_name.as_str())
                .ok_or_else(|| format!("规则 {} 不在配置文件中", rule_name))?;
            current_hits.push(rule_idx);
        }
    }

    finalize_trigger_pattern_sample(
        &mut out,
        &mut current_hits,
        &mut current_total_score,
        current_trade_date.as_deref(),
        current_ts_code.as_deref(),
    );
    Ok(out)
}

fn finalize_trigger_pattern_sample(
    out: &mut HashMap<Vec<usize>, CountScoreAgg>,
    current_hits: &mut Vec<usize>,
    current_total_score: &mut f64,
    trade_date: Option<&str>,
    ts_code: Option<&str>,
) {
    if trade_date.is_none() || ts_code.is_none() {
        return;
    }

    current_hits.sort_unstable();
    let entry = out.entry(current_hits.clone()).or_default();
    entry.sample_count += 1;
    entry.total_score_sum += *current_total_score;

    current_hits.clear();
    *current_total_score = 50.0;
}

fn build_combo_stats(
    pattern_aggs: &HashMap<Vec<usize>, CountScoreAgg>,
    rule_names: &[String],
    max_combo_size: usize,
) -> Vec<ComboStatRow> {
    let total_samples: i64 = pattern_aggs.values().map(|agg| agg.sample_count).sum();
    if total_samples == 0 || rule_names.is_empty() {
        return Vec::new();
    }

    let total_samples_f = total_samples as f64;
    let subset_hit_aggs = build_subset_hit_aggs(pattern_aggs, max_combo_size);
    let hit_rates = build_single_hit_rates(&subset_hit_aggs, rule_names.len(), total_samples_f);
    let all_rule_indices: Vec<usize> = (0..rule_names.len()).collect();
    let mut out = Vec::new();

    for combo_size in 2..=max_combo_size.min(rule_names.len()) {
        for_each_combination(&all_rule_indices, combo_size, |combo| {
            let co_hit_count = subset_hit_aggs
                .get(combo)
                .map(|agg| agg.sample_count)
                .unwrap_or(0);
            let co_hit_rate = co_hit_count as f64 / total_samples_f;
            let expected_co_hit_rate = combo.iter().map(|&idx| hit_rates[idx]).product::<f64>();
            let co_hit_corr_gap = co_hit_rate - expected_co_hit_rate;

            let co_miss_count = compute_co_miss_count(combo, &subset_hit_aggs, total_samples);
            let co_miss_rate = co_miss_count as f64 / total_samples_f;
            let expected_co_miss_rate = combo
                .iter()
                .map(|&idx| 1.0 - hit_rates[idx])
                .product::<f64>();
            let co_miss_corr_gap = co_miss_rate - expected_co_miss_rate;

            out.push(ComboStatRow {
                combo_size,
                combo_name: format_rule_list(combo, rule_names),
                co_hit_count,
                co_hit_rate,
                expected_co_hit_rate,
                co_hit_corr_gap,
                co_hit_lift: safe_lift(co_hit_rate, expected_co_hit_rate),
                co_miss_count,
                co_miss_rate,
                expected_co_miss_rate,
                co_miss_corr_gap,
                co_miss_lift: safe_lift(co_miss_rate, expected_co_miss_rate),
                phi_correlation: if combo_size == 2 {
                    compute_phi_correlation(combo, &subset_hit_aggs, total_samples)
                } else {
                    None
                },
            });
        });
    }

    out
}

fn build_subset_hit_aggs(
    pattern_aggs: &HashMap<Vec<usize>, CountScoreAgg>,
    max_combo_size: usize,
) -> HashMap<Vec<usize>, CountScoreAgg> {
    let mut out: HashMap<Vec<usize>, CountScoreAgg> = HashMap::new();

    for (hit_set, pattern_agg) in pattern_aggs {
        let upper = max_combo_size.min(hit_set.len());
        for subset_size in 1..=upper {
            for_each_combination(hit_set, subset_size, |subset| {
                let entry = out.entry(subset.to_vec()).or_default();
                entry.sample_count += pattern_agg.sample_count;
                entry.total_score_sum += pattern_agg.total_score_sum;
            });
        }
    }

    out
}

fn build_single_hit_rates(
    subset_hit_aggs: &HashMap<Vec<usize>, CountScoreAgg>,
    rule_count: usize,
    total_samples: f64,
) -> Vec<f64> {
    (0..rule_count)
        .map(|idx| {
            let key = [idx];
            subset_hit_aggs
                .get(&key[..])
                .map(|agg| agg.sample_count as f64 / total_samples)
                .unwrap_or(0.0)
        })
        .collect()
}

fn compute_co_miss_count(
    combo: &[usize],
    subset_hit_aggs: &HashMap<Vec<usize>, CountScoreAgg>,
    total_samples: i64,
) -> i64 {
    let mut out = total_samples;
    for subset_size in 1..=combo.len() {
        let sign = if subset_size % 2 == 1 { -1 } else { 1 };
        for_each_combination(combo, subset_size, |subset| {
            let hit_count = subset_hit_aggs
                .get(subset)
                .map(|agg| agg.sample_count)
                .unwrap_or(0);
            out += sign * hit_count;
        });
    }
    out
}

fn compute_phi_correlation(
    combo: &[usize],
    subset_hit_aggs: &HashMap<Vec<usize>, CountScoreAgg>,
    total_samples: i64,
) -> Option<f64> {
    if combo.len() != 2 || total_samples <= 0 {
        return None;
    }

    let left = [combo[0]];
    let right = [combo[1]];
    let n11 = subset_hit_aggs
        .get(combo)
        .map(|agg| agg.sample_count)
        .unwrap_or(0) as f64;
    let n1x = subset_hit_aggs
        .get(&left[..])
        .map(|agg| agg.sample_count)
        .unwrap_or(0) as f64;
    let nx1 = subset_hit_aggs
        .get(&right[..])
        .map(|agg| agg.sample_count)
        .unwrap_or(0) as f64;
    let n10 = n1x - n11;
    let n01 = nx1 - n11;
    let n00 = total_samples as f64 - n11 - n10 - n01;
    let denom = (n1x * (total_samples as f64 - n1x) * nx1 * (total_samples as f64 - nx1)).sqrt();

    if denom == 0.0 {
        None
    } else {
        Some((n11 * n00 - n10 * n01) / denom)
    }
}

fn build_trigger_pattern_rows(
    pattern_aggs: &HashMap<Vec<usize>, CountScoreAgg>,
    rule_names: &[String],
) -> Vec<TriggerPatternRow> {
    let total_samples: i64 = pattern_aggs.values().map(|agg| agg.sample_count).sum();
    if total_samples == 0 || rule_names.is_empty() {
        return Vec::new();
    }

    let total_samples_f = total_samples as f64;
    let hit_rates = build_pattern_hit_rates(pattern_aggs, rule_names.len(), total_samples_f);
    let mut out = Vec::with_capacity(pattern_aggs.len());

    for (hit_set, agg) in pattern_aggs {
        let sample_rate = agg.sample_count as f64 / total_samples_f;
        let expected_sample_rate = calc_expected_signature_rate(hit_set, &hit_rates);
        let miss_indices = complement_indices(hit_set, rule_names.len());

        out.push(TriggerPatternRow {
            hit_rule_count: hit_set.len(),
            miss_rule_count: rule_names.len() - hit_set.len(),
            hit_rules: format_rule_list(hit_set, rule_names),
            miss_rules: format_rule_list(&miss_indices, rule_names),
            sample_count: agg.sample_count,
            sample_rate,
            expected_sample_rate,
            pattern_corr_gap: sample_rate - expected_sample_rate,
            pattern_lift: safe_lift(sample_rate, expected_sample_rate),
            avg_total_score: avg_if_any(agg.total_score_sum, agg.sample_count),
        });
    }

    out
}

fn build_pattern_hit_rates(
    pattern_aggs: &HashMap<Vec<usize>, CountScoreAgg>,
    rule_count: usize,
    total_samples: f64,
) -> Vec<f64> {
    let mut hit_counts = vec![0i64; rule_count];
    for (hit_set, agg) in pattern_aggs {
        for &idx in hit_set {
            hit_counts[idx] += agg.sample_count;
        }
    }

    hit_counts
        .into_iter()
        .map(|count| count as f64 / total_samples)
        .collect()
}

fn calc_expected_signature_rate(hit_set: &[usize], hit_rates: &[f64]) -> f64 {
    let mut out = 1.0;
    let mut hit_pos = 0usize;

    for (idx, hit_rate) in hit_rates.iter().copied().enumerate() {
        let is_hit = hit_pos < hit_set.len() && hit_set[hit_pos] == idx;
        let prob = if is_hit { hit_rate } else { 1.0 - hit_rate };
        out *= prob.max(0.0);
        if is_hit {
            hit_pos += 1;
        }
    }

    out
}

fn complement_indices(hit_set: &[usize], total_rule_count: usize) -> Vec<usize> {
    let mut out = Vec::with_capacity(total_rule_count.saturating_sub(hit_set.len()));
    let mut hit_pos = 0usize;

    for idx in 0..total_rule_count {
        if hit_pos < hit_set.len() && hit_set[hit_pos] == idx {
            hit_pos += 1;
            continue;
        }
        out.push(idx);
    }

    out
}

fn format_rule_list(indices: &[usize], rule_names: &[String]) -> String {
    if indices.is_empty() {
        return "-".to_string();
    }

    indices
        .iter()
        .map(|&idx| rule_names[idx].as_str())
        .collect::<Vec<_>>()
        .join(" | ")
}

fn safe_lift(observed_rate: f64, expected_rate: f64) -> Option<f64> {
    if expected_rate > 0.0 {
        Some(observed_rate / expected_rate)
    } else {
        None
    }
}

fn avg_if_any(sum: f64, count: i64) -> Option<f64> {
    if count > 0 {
        Some(sum / count as f64)
    } else {
        None
    }
}

fn for_each_combination<F>(items: &[usize], choose: usize, mut f: F)
where
    F: FnMut(&[usize]),
{
    if choose == 0 || choose > items.len() {
        return;
    }
    let mut current = Vec::with_capacity(choose);
    for_each_combination_inner(items, choose, 0, &mut current, &mut f);
}

fn for_each_combination_inner<F>(
    items: &[usize],
    choose: usize,
    start: usize,
    current: &mut Vec<usize>,
    f: &mut F,
) where
    F: FnMut(&[usize]),
{
    if current.len() == choose {
        f(current);
        return;
    }

    let need = choose - current.len();
    for idx in start..=items.len() - need {
        current.push(items[idx]);
        for_each_combination_inner(items, choose, idx + 1, current, f);
        current.pop();
    }
}

fn build_latest_rule_stats(rule_stats: &[RuleStatRow]) -> Vec<RuleStatRow> {
    let mut out = rule_stats.to_vec();
    out.sort_by(|a, b| {
        b.latest_hit_rate
            .unwrap_or(0.0)
            .total_cmp(&a.latest_hit_rate.unwrap_or(0.0))
            .then_with(|| b.latest_total_score.total_cmp(&a.latest_total_score))
    });
    out
}

fn write_csv<T: Serialize>(path: &Path, rows: &[T]) -> Result<(), String> {
    let mut writer = Writer::from_path(path)
        .map_err(|e| format!("创建 CSV 失败: path={}, err={e}", path.display()))?;
    for row in rows {
        writer
            .serialize(row)
            .map_err(|e| format!("写入 CSV 失败: path={}, err={e}", path.display()))?;
    }
    writer
        .flush()
        .map_err(|e| format!("刷新 CSV 失败: path={}, err={e}", path.display()))?;
    Ok(())
}

fn write_summary_md(
    path: &Path,
    sample_summary: &SampleSummary,
    overall_hits: &HitSummary,
    latest_hits: &HitSummary,
    rule_stats: &[RuleStatRow],
    each_rule_stats: &[EachRuleRow],
    daily_stats: &[DailyStatRow],
    scope_stats: &[ScopeStatRow],
    combo_stats: &[ComboStatRow],
    trigger_patterns: &[TriggerPatternRow],
) -> Result<(), String> {
    let mut file = File::create(path)
        .map_err(|e| format!("创建 Markdown 汇总失败: path={}, err={e}", path.display()))?;

    let top_positive = top_n_rule_stats(rule_stats, 8, true);
    let top_negative = top_n_rule_stats(rule_stats, 8, false);
    let top_latest = top_latest_rule_stats(rule_stats, 10);
    let top_days = top_n_days(daily_stats, 5, true);
    let weak_days = top_n_days(daily_stats, 5, false);
    let top_hit_combo_corr =
        top_combo_stats_by_hit_correlation(combo_stats, SUMMARY_TOP_COMBO_COUNT);
    let top_miss_combo_corr =
        top_combo_stats_by_miss_correlation(combo_stats, SUMMARY_TOP_COMBO_COUNT);
    let top_high_order_patterns = top_pattern_stats(
        trigger_patterns,
        SUMMARY_TOP_PATTERN_COUNT,
        SUMMARY_HIGH_ORDER_PATTERN_MIN_HIT_COUNT,
    );

    writeln!(file, "# 策略命中统计报告").map_err(|e| format!("写入 summary.md 失败: {e}"))?;
    writeln!(file).map_err(|e| format!("写入 summary.md 失败: {e}"))?;
    writeln!(file, "## 概览").map_err(|e| format!("写入 summary.md 失败: {e}"))?;
    writeln!(
        file,
        "- 统计区间: {} -> {}",
        sample_summary.min_trade_date, sample_summary.max_trade_date
    )
    .map_err(|e| format!("写入 summary.md 失败: {e}"))?;
    writeln!(
        file,
        "- 样本规模: {} 个股票日样本，{} 个交易日，{} 只股票",
        sample_summary.score_summary_rows, sample_summary.trade_days, sample_summary.stock_count
    )
    .map_err(|e| format!("写入 summary.md 失败: {e}"))?;
    writeln!(
        file,
        "- 总分概况: 均值 {:.4}，中位数 {:.4}，最低 {:.4}，最高 {:.4}",
        sample_summary.avg_total_score,
        sample_summary.median_total_score,
        sample_summary.min_total_score,
        sample_summary.max_total_score
    )
    .map_err(|e| format!("写入 summary.md 失败: {e}"))?;
    writeln!(
        file,
        "- 平均每股每日命中策略数: 全样本 {:.4}，最新日 {} 为 {:.4}",
        overall_hits.avg_hit_rule_cnt,
        latest_hits.trade_date.clone().unwrap_or_default(),
        latest_hits.avg_hit_rule_cnt
    )
    .map_err(|e| format!("写入 summary.md 失败: {e}"))?;
    writeln!(
        file,
        "- 最新日平均总分: {:.4}，全样本平均总分: {:.4}",
        latest_hits.avg_total_score.unwrap_or(0.0),
        sample_summary.avg_total_score
    )
    .map_err(|e| format!("写入 summary.md 失败: {e}"))?;
    writeln!(file).map_err(|e| format!("写入 summary.md 失败: {e}"))?;

    writeln!(file, "## 命中汇总").map_err(|e| format!("写入 summary.md 失败: {e}"))?;
    write_hit_summary_table(&mut file, overall_hits, latest_hits)?;
    writeln!(file).map_err(|e| format!("写入 summary.md 失败: {e}"))?;

    writeln!(file, "## 命中方式汇总").map_err(|e| format!("写入 summary.md 失败: {e}"))?;
    writeln!(
        file,
        "| 命中方式 | 规则数 | 规则平均命中率 | 规则平均得分 | 总得分贡献 |"
    )
    .map_err(|e| format!("写入 summary.md 失败: {e}"))?;
    writeln!(file, "| --- | ---: | ---: | ---: | ---: |")
        .map_err(|e| format!("写入 summary.md 失败: {e}"))?;
    for row in scope_stats {
        writeln!(
            file,
            "| {} | {} | {:.4} | {:.4} | {:.2} |",
            row.scope_way,
            row.rule_count,
            row.avg_rule_hit_rate,
            row.avg_rule_avg_score,
            row.total_score
        )
        .map_err(|e| format!("写入 summary.md 失败: {e}"))?;
    }
    writeln!(file).map_err(|e| format!("写入 summary.md 失败: {e}"))?;

    writeln!(file, "## 正向贡献最高规则").map_err(|e| format!("写入 summary.md 失败: {e}"))?;
    write_rule_table(&mut file, &top_positive)?;
    writeln!(file).map_err(|e| format!("写入 summary.md 失败: {e}"))?;

    writeln!(file, "## 负向拖分最高规则").map_err(|e| format!("写入 summary.md 失败: {e}"))?;
    write_rule_table(&mut file, &top_negative)?;
    writeln!(file).map_err(|e| format!("写入 summary.md 失败: {e}"))?;

    writeln!(file, "## 最新日活跃规则").map_err(|e| format!("写入 summary.md 失败: {e}"))?;
    write_latest_rule_table(&mut file, &top_latest)?;
    writeln!(file).map_err(|e| format!("写入 summary.md 失败: {e}"))?;

    writeln!(file, "## 共同触发相关性最高组合")
        .map_err(|e| format!("写入 summary.md 失败: {e}"))?;
    writeln!(
        file,
        "按“观测同时触发率 - 独立预期同时触发率”排序，Lift>1 表示比独立假设更容易一起触发。"
    )
    .map_err(|e| format!("写入 summary.md 失败: {e}"))?;
    write_combo_hit_correlation_table(&mut file, &top_hit_combo_corr)?;
    writeln!(file).map_err(|e| format!("写入 summary.md 失败: {e}"))?;

    writeln!(file, "## 共同不触发相关性最高组合")
        .map_err(|e| format!("写入 summary.md 失败: {e}"))?;
    writeln!(
        file,
        "按“观测同时不触发率 - 独立预期同时不触发率”排序，Lift>1 表示比独立假设更容易一起不触发。"
    )
    .map_err(|e| format!("写入 summary.md 失败: {e}"))?;
    write_combo_miss_correlation_table(&mut file, &top_miss_combo_corr)?;
    writeln!(file).map_err(|e| format!("写入 summary.md 失败: {e}"))?;

    writeln!(file, "## 高阶触发签名相关性").map_err(|e| format!("写入 summary.md 失败: {e}"))?;
    writeln!(
        file,
        "这里展示 5 条及以上策略的完整触发/未触发签名，比较观测占比与各策略独立时的理论占比。"
    )
    .map_err(|e| format!("写入 summary.md 失败: {e}"))?;
    write_pattern_correlation_table(&mut file, &top_high_order_patterns)?;
    writeln!(file).map_err(|e| format!("写入 summary.md 失败: {e}"))?;

    writeln!(file, "## 窗口累计命中规则平均命中次数")
        .map_err(|e| format!("写入 summary.md 失败: {e}"))?;
    writeln!(
        file,
        "窗口累计命中类规则的命中次数，可用规则得分除以分值反推，因为评分逻辑保存的是命中次数乘以分值。"
    )
    .map_err(|e| format!("写入 summary.md 失败: {e}"))?;
    writeln!(file).map_err(|e| format!("写入 summary.md 失败: {e}"))?;
    writeln!(file, "| 规则名 | 分值 | 命中率 | 触发时平均命中次数 | 全样本平均命中次数 | 最新日触发时平均命中次数 |")
        .map_err(|e| format!("写入 summary.md 失败: {e}"))?;
    writeln!(file, "| --- | ---: | ---: | ---: | ---: | ---: |")
        .map_err(|e| format!("写入 summary.md 失败: {e}"))?;
    for row in each_rule_stats.iter().take(10) {
        writeln!(
            file,
            "| {} | {:.2} | {:.4} | {} | {:.4} | {} |",
            row.rule_name,
            row.points,
            row.hit_rate,
            fmt_opt4(row.avg_hit_count_when_hit),
            row.avg_hit_count_overall,
            fmt_opt4(row.latest_avg_hit_count_when_hit)
        )
        .map_err(|e| format!("写入 summary.md 失败: {e}"))?;
    }
    writeln!(file).map_err(|e| format!("写入 summary.md 失败: {e}"))?;

    writeln!(file, "## 活跃度最高交易日").map_err(|e| format!("写入 summary.md 失败: {e}"))?;
    write_day_table(&mut file, &top_days)?;
    writeln!(file).map_err(|e| format!("写入 summary.md 失败: {e}"))?;

    writeln!(file, "## 活跃度较低交易日").map_err(|e| format!("写入 summary.md 失败: {e}"))?;
    write_day_table(&mut file, &weak_days)?;
    writeln!(file).map_err(|e| format!("写入 summary.md 失败: {e}"))?;

    Ok(())
}

fn top_n_rule_stats(rule_stats: &[RuleStatRow], n: usize, positive: bool) -> Vec<RuleStatRow> {
    let mut rows = rule_stats.to_vec();
    if positive {
        rows.sort_by(|a, b| b.total_score.total_cmp(&a.total_score));
    } else {
        rows.sort_by(|a, b| a.total_score.total_cmp(&b.total_score));
    }
    rows.truncate(n);
    rows
}

fn top_latest_rule_stats(rule_stats: &[RuleStatRow], n: usize) -> Vec<RuleStatRow> {
    let mut rows = rule_stats.to_vec();
    rows.sort_by(|a, b| {
        b.latest_hit_rate
            .unwrap_or(0.0)
            .total_cmp(&a.latest_hit_rate.unwrap_or(0.0))
            .then_with(|| b.latest_total_score.total_cmp(&a.latest_total_score))
    });
    rows.truncate(n);
    rows
}

fn top_n_days(daily_stats: &[DailyStatRow], n: usize, positive: bool) -> Vec<DailyStatRow> {
    let mut rows = daily_stats.to_vec();
    if positive {
        rows.sort_by(|a, b| {
            b.avg_hit_rule_cnt
                .total_cmp(&a.avg_hit_rule_cnt)
                .then_with(|| b.avg_total_score.total_cmp(&a.avg_total_score))
        });
    } else {
        rows.sort_by(|a, b| {
            a.avg_hit_rule_cnt
                .total_cmp(&b.avg_hit_rule_cnt)
                .then_with(|| a.avg_total_score.total_cmp(&b.avg_total_score))
        });
    }
    rows.truncate(n);
    rows
}

fn top_combo_stats_by_hit_correlation(combo_stats: &[ComboStatRow], n: usize) -> Vec<ComboStatRow> {
    let mut rows: Vec<ComboStatRow> = combo_stats
        .iter()
        .filter(|row| row.co_hit_corr_gap > 0.0)
        .cloned()
        .collect();
    rows.sort_by(|a, b| {
        b.co_hit_corr_gap
            .total_cmp(&a.co_hit_corr_gap)
            .then_with(|| {
                b.co_hit_lift
                    .unwrap_or(0.0)
                    .total_cmp(&a.co_hit_lift.unwrap_or(0.0))
            })
            .then_with(|| b.co_hit_count.cmp(&a.co_hit_count))
    });
    rows.truncate(n);
    rows
}

fn top_combo_stats_by_miss_correlation(
    combo_stats: &[ComboStatRow],
    n: usize,
) -> Vec<ComboStatRow> {
    let mut rows: Vec<ComboStatRow> = combo_stats
        .iter()
        .filter(|row| row.co_miss_corr_gap > 0.0)
        .cloned()
        .collect();
    rows.sort_by(|a, b| {
        b.co_miss_corr_gap
            .total_cmp(&a.co_miss_corr_gap)
            .then_with(|| {
                b.co_miss_lift
                    .unwrap_or(0.0)
                    .total_cmp(&a.co_miss_lift.unwrap_or(0.0))
            })
            .then_with(|| b.co_miss_count.cmp(&a.co_miss_count))
    });
    rows.truncate(n);
    rows
}

fn top_pattern_stats(
    trigger_patterns: &[TriggerPatternRow],
    n: usize,
    min_hit_rule_count: usize,
) -> Vec<TriggerPatternRow> {
    let mut rows: Vec<TriggerPatternRow> = trigger_patterns
        .iter()
        .filter(|row| row.hit_rule_count >= min_hit_rule_count && row.pattern_corr_gap > 0.0)
        .cloned()
        .collect();
    rows.sort_by(|a, b| {
        b.pattern_corr_gap
            .total_cmp(&a.pattern_corr_gap)
            .then_with(|| {
                b.pattern_lift
                    .unwrap_or(0.0)
                    .total_cmp(&a.pattern_lift.unwrap_or(0.0))
            })
            .then_with(|| b.sample_count.cmp(&a.sample_count))
    });
    rows.truncate(n);
    rows
}

fn write_hit_summary_table(
    file: &mut File,
    overall_hits: &HitSummary,
    latest_hits: &HitSummary,
) -> Result<(), String> {
    writeln!(file, "| 范围 | 股票数 | 平均命中策略数 | 命中策略数中位数 | 命中策略数P90 | 平均正向命中数 | 平均负向命中数 | 平均总分 | 总分中位数 | 最低总分 | 最高总分 |")
        .map_err(|e| format!("写入 summary.md 失败: {e}"))?;
    writeln!(
        file,
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
    )
    .map_err(|e| format!("写入 summary.md 失败: {e}"))?;
    write_hit_summary_line(file, "全样本", overall_hits)?;
    write_hit_summary_line(
        file,
        latest_hits.trade_date.as_deref().unwrap_or("最新日"),
        latest_hits,
    )?;
    Ok(())
}

fn write_hit_summary_line(file: &mut File, label: &str, row: &HitSummary) -> Result<(), String> {
    writeln!(
        file,
        "| {} | {} | {:.4} | {:.4} | {:.4} | {:.4} | {:.4} | {} | {} | {} | {} |",
        label,
        row.stock_count,
        row.avg_hit_rule_cnt,
        row.median_hit_rule_cnt,
        row.p90_hit_rule_cnt,
        row.avg_pos_hit_rule_cnt,
        row.avg_neg_hit_rule_cnt,
        fmt_opt4(row.avg_total_score),
        fmt_opt4(row.median_total_score),
        fmt_opt4(row.min_total_score),
        fmt_opt4(row.max_total_score)
    )
    .map_err(|e| format!("写入 summary.md 失败: {e}"))
}

fn write_rule_table(file: &mut File, rows: &[RuleStatRow]) -> Result<(), String> {
    writeln!(
        file,
        "| 规则名 | 命中方式 | 分值 | 命中率 | 平均得分 | 命中时平均得分 | 总得分贡献 |"
    )
    .map_err(|e| format!("写入 summary.md 失败: {e}"))?;
    writeln!(file, "| --- | --- | ---: | ---: | ---: | ---: | ---: |")
        .map_err(|e| format!("写入 summary.md 失败: {e}"))?;
    for row in rows {
        writeln!(
            file,
            "| {} | {} | {:.2} | {:.4} | {:.4} | {} | {:.2} |",
            row.rule_name,
            row.scope_way,
            row.points,
            row.hit_rate,
            row.avg_score,
            fmt_opt4(row.avg_score_when_hit),
            row.total_score
        )
        .map_err(|e| format!("写入 summary.md 失败: {e}"))?;
    }
    Ok(())
}

fn write_latest_rule_table(file: &mut File, rows: &[RuleStatRow]) -> Result<(), String> {
    writeln!(
        file,
        "| 规则名 | 命中方式 | 分值 | 最新日命中率 | 最新日平均得分 | 最新日命中时平均得分 | 最新日总得分贡献 |"
    )
    .map_err(|e| format!("写入 summary.md 失败: {e}"))?;
    writeln!(file, "| --- | --- | ---: | ---: | ---: | ---: | ---: |")
        .map_err(|e| format!("写入 summary.md 失败: {e}"))?;
    for row in rows {
        writeln!(
            file,
            "| {} | {} | {:.2} | {} | {} | {} | {:.2} |",
            row.rule_name,
            row.scope_way,
            row.points,
            fmt_opt4(row.latest_hit_rate),
            fmt_opt4(row.latest_avg_score),
            fmt_opt4(row.latest_avg_score_when_hit),
            row.latest_total_score
        )
        .map_err(|e| format!("写入 summary.md 失败: {e}"))?;
    }
    Ok(())
}

fn write_day_table(file: &mut File, rows: &[DailyStatRow]) -> Result<(), String> {
    writeln!(
        file,
        "| 交易日 | 股票数 | 平均命中策略数 | 平均正向命中数 | 平均负向命中数 | 平均总分 | 总分中位数 |"
    )
    .map_err(|e| format!("写入 summary.md 失败: {e}"))?;
    writeln!(file, "| --- | ---: | ---: | ---: | ---: | ---: | ---: |")
        .map_err(|e| format!("写入 summary.md 失败: {e}"))?;
    for row in rows {
        writeln!(
            file,
            "| {} | {} | {:.4} | {:.4} | {:.4} | {:.4} | {:.4} |",
            row.trade_date,
            row.stock_count,
            row.avg_hit_rule_cnt,
            row.avg_pos_hit_rule_cnt,
            row.avg_neg_hit_rule_cnt,
            row.avg_total_score,
            row.median_total_score
        )
        .map_err(|e| format!("写入 summary.md 失败: {e}"))?;
    }
    Ok(())
}

fn write_combo_hit_correlation_table(file: &mut File, rows: &[ComboStatRow]) -> Result<(), String> {
    if rows.is_empty() {
        writeln!(file, "暂无显著高于独立预期的共同触发组合。")
            .map_err(|e| format!("写入 summary.md 失败: {e}"))?;
        return Ok(());
    }

    writeln!(
        file,
        "| 阶数 | 策略组合 | 观测同时触发率 | 独立预期率 | 相关偏差 | Lift | 同时触发样本数 | Phi(2阶) |"
    )
    .map_err(|e| format!("写入 summary.md 失败: {e}"))?;
    writeln!(
        file,
        "| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |"
    )
    .map_err(|e| format!("写入 summary.md 失败: {e}"))?;

    for row in rows {
        writeln!(
            file,
            "| {} | {} | {} | {} | {} | {} | {} | {} |",
            row.combo_size,
            escape_md_cell(&row.combo_name),
            fmt_prob(row.co_hit_rate),
            fmt_prob(row.expected_co_hit_rate),
            fmt_prob(row.co_hit_corr_gap),
            fmt_opt4(row.co_hit_lift),
            row.co_hit_count,
            fmt_opt4(row.phi_correlation)
        )
        .map_err(|e| format!("写入 summary.md 失败: {e}"))?;
    }

    Ok(())
}

fn write_combo_miss_correlation_table(
    file: &mut File,
    rows: &[ComboStatRow],
) -> Result<(), String> {
    if rows.is_empty() {
        writeln!(file, "暂无显著高于独立预期的共同不触发组合。")
            .map_err(|e| format!("写入 summary.md 失败: {e}"))?;
        return Ok(());
    }

    writeln!(
        file,
        "| 阶数 | 策略组合 | 观测同时不触发率 | 独立预期率 | 相关偏差 | Lift | 同时不触发样本数 | Phi(2阶) |"
    )
    .map_err(|e| format!("写入 summary.md 失败: {e}"))?;
    writeln!(
        file,
        "| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |"
    )
    .map_err(|e| format!("写入 summary.md 失败: {e}"))?;

    for row in rows {
        writeln!(
            file,
            "| {} | {} | {} | {} | {} | {} | {} | {} |",
            row.combo_size,
            escape_md_cell(&row.combo_name),
            fmt_prob(row.co_miss_rate),
            fmt_prob(row.expected_co_miss_rate),
            fmt_prob(row.co_miss_corr_gap),
            fmt_opt4(row.co_miss_lift),
            row.co_miss_count,
            fmt_opt4(row.phi_correlation)
        )
        .map_err(|e| format!("写入 summary.md 失败: {e}"))?;
    }

    Ok(())
}

fn write_pattern_correlation_table(
    file: &mut File,
    rows: &[TriggerPatternRow],
) -> Result<(), String> {
    if rows.is_empty() {
        writeln!(file, "暂无 5 条及以上策略的高阶相关触发签名。")
            .map_err(|e| format!("写入 summary.md 失败: {e}"))?;
        return Ok(());
    }

    writeln!(
        file,
        "| 触发策略数 | 触发策略组合 | 未触发策略数 | 观测占比 | 独立预期占比 | 相关偏差 | Lift | 平均总分 |"
    )
    .map_err(|e| format!("写入 summary.md 失败: {e}"))?;
    writeln!(
        file,
        "| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |"
    )
    .map_err(|e| format!("写入 summary.md 失败: {e}"))?;

    for row in rows {
        writeln!(
            file,
            "| {} | {} | {} | {} | {} | {} | {} | {} |",
            row.hit_rule_count,
            escape_md_cell(&row.hit_rules),
            row.miss_rule_count,
            fmt_prob(row.sample_rate),
            fmt_prob(row.expected_sample_rate),
            fmt_prob(row.pattern_corr_gap),
            fmt_opt4(row.pattern_lift),
            fmt_opt4(row.avg_total_score)
        )
        .map_err(|e| format!("写入 summary.md 失败: {e}"))?;
    }

    Ok(())
}

fn fmt_opt4(v: Option<f64>) -> String {
    match v {
        Some(v) => format!("{v:.4}"),
        None => "-".to_string(),
    }
}

fn fmt_prob(v: f64) -> String {
    if v == 0.0 {
        "0.0000".to_string()
    } else if v.abs() >= 0.0001 {
        format!("{v:.4}")
    } else {
        format!("{v:.3e}")
    }
}

fn escape_md_cell(text: &str) -> String {
    text.replace('|', "\\|")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn approx_eq(left: f64, right: f64) {
        assert!((left - right).abs() < 1e-9, "left={left}, right={right}");
    }

    #[test]
    fn combo_and_pattern_correlation_use_independence_baseline() {
        let rule_names = vec!["A".to_string(), "B".to_string(), "C".to_string()];
        let mut pattern_aggs = HashMap::new();
        pattern_aggs.insert(
            vec![0, 1],
            CountScoreAgg {
                sample_count: 2,
                total_score_sum: 120.0,
            },
        );
        pattern_aggs.insert(
            vec![0, 2],
            CountScoreAgg {
                sample_count: 1,
                total_score_sum: 70.0,
            },
        );
        pattern_aggs.insert(
            Vec::new(),
            CountScoreAgg {
                sample_count: 1,
                total_score_sum: 40.0,
            },
        );

        let combo_stats = build_combo_stats(&pattern_aggs, &rule_names, 3);
        let ab = combo_stats
            .iter()
            .find(|row| row.combo_name == "A | B")
            .expect("missing AB combo");
        approx_eq(ab.co_hit_rate, 0.5);
        approx_eq(ab.expected_co_hit_rate, 0.375);
        approx_eq(ab.co_hit_corr_gap, 0.125);
        approx_eq(ab.co_hit_lift.expect("missing hit lift"), 4.0 / 3.0);
        assert_eq!(ab.co_miss_count, 1);
        approx_eq(ab.co_miss_rate, 0.25);
        approx_eq(ab.expected_co_miss_rate, 0.125);
        approx_eq(ab.co_miss_corr_gap, 0.125);
        approx_eq(ab.co_miss_lift.expect("missing miss lift"), 2.0);
        approx_eq(
            ab.phi_correlation.expect("missing phi correlation"),
            2.0 / 12.0_f64.sqrt(),
        );

        let abc = combo_stats
            .iter()
            .find(|row| row.combo_name == "A | B | C")
            .expect("missing ABC combo");
        assert_eq!(abc.co_hit_count, 0);
        approx_eq(abc.expected_co_hit_rate, 0.09375);
        approx_eq(abc.co_hit_corr_gap, -0.09375);
        approx_eq(abc.co_hit_lift.expect("missing triple hit lift"), 0.0);
        assert_eq!(abc.co_miss_count, 1);
        approx_eq(abc.co_miss_rate, 0.25);
        approx_eq(abc.expected_co_miss_rate, 0.09375);
        approx_eq(abc.co_miss_corr_gap, 0.15625);

        let pattern_rows = build_trigger_pattern_rows(&pattern_aggs, &rule_names);
        let ab_pattern = pattern_rows
            .iter()
            .find(|row| row.hit_rules == "A | B")
            .expect("missing AB pattern");
        assert_eq!(ab_pattern.sample_count, 2);
        approx_eq(ab_pattern.sample_rate, 0.5);
        approx_eq(ab_pattern.expected_sample_rate, 0.28125);
        approx_eq(ab_pattern.pattern_corr_gap, 0.21875);
        approx_eq(
            ab_pattern.pattern_lift.expect("missing pattern lift"),
            16.0 / 9.0,
        );
        approx_eq(ab_pattern.avg_total_score.expect("missing avg score"), 60.0);
    }

    #[test]
    fn statistics_report_writes_correlation_outputs_when_local_db_exists() {
        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let source_dir = manifest_dir.join("source");
        let rule_file = source_dir.join("score_rule.toml");
        let result_db = source_dir.join("scoring_result.db");
        if !result_db.exists() {
            return;
        }

        let out_dir = manifest_dir
            .join("target")
            .join("tmp")
            .join("statistics_correlation_smoke");
        let _ = fs::remove_dir_all(&out_dir);

        run_statistics(&AppConfig {
            source_dir,
            rule_file,
            out_dir: out_dir.clone(),
        })
        .expect("statistics run should succeed");

        assert!(out_dir.join("summary.md").exists());
        assert!(out_dir.join("combo_correlation_stats.csv").exists());
        assert!(
            out_dir
                .join("trigger_pattern_correlation_stats.csv")
                .exists()
        );
    }
}
