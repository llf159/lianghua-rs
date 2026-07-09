use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::io::Write;

use duckdb::{Connection, params};

const SOURCE: &str = "/home/lmingyuanl/.local/share/com.mingyuan.lianghua/source";
const LOOKBACK: usize = 20;
const SUB_WIN: usize = 3;
const REF_DAYS: usize = 60;
const TOP_N: usize = 20;
const MIN_RN: i64 = 60;

#[derive(Debug, Clone)]
struct Meta {
    name: String,
    industry: String,
    area: String,
    board: String,
    total_mv_yi: Option<f64>,
}

#[derive(Debug, Clone, Default)]
struct Bar {
    close: Option<f64>,
    pct: Option<f64>,
    tor: Option<f64>,
    vol: Option<f64>,
    amount: Option<f64>,
    vr: Option<f64>,
    vr20: Option<f64>,
    j: Option<f64>,
    rsi6: Option<f64>,
    ma10: Option<f64>,
    ma20: Option<f64>,
    brick: Option<f64>,
    duokong_short: Option<f64>,
    duokong_long: Option<f64>,
    z_score: Option<f64>,
    vol_sigma: Option<f64>,
    c_sigma: Option<f64>,
    rsv_c90: Option<f64>,
    rsv_c30: Option<f64>,
    rn: i64,
}

#[derive(Debug, Clone)]
struct Winner {
    kind: &'static str,
    ref_date: String,
    start_date: String,
    end_date: String,
    ts_code: String,
    name: String,
    value: f64,
}

#[derive(Debug, Clone)]
struct RuleHit {
    rule_name: String,
    score: f64,
}

#[derive(Debug, Clone, Default)]
struct ScoreInfo {
    rank: Option<i64>,
    total_score: Option<f64>,
}

#[derive(Debug, Clone, Default)]
struct RuleMeta {
    scene: String,
    stage: String,
    points: f64,
    explain: String,
}

#[derive(Debug, Clone, Default)]
struct Agg {
    count: usize,
    sum_value: f64,
}

#[derive(Debug, Clone, Default)]
struct NumStats {
    n: usize,
    sum: f64,
}

impl NumStats {
    fn add(&mut self, v: Option<f64>) {
        if let Some(v) = v.filter(|v| v.is_finite()) {
            self.n += 1;
            self.sum += v;
        }
    }

    fn mean(&self) -> Option<f64> {
        if self.n == 0 {
            None
        } else {
            Some(self.sum / self.n as f64)
        }
    }
}

fn board_category(ts_code: &str, stock_name: &str) -> &'static str {
    let normalized = stock_name.trim().to_ascii_uppercase().replace('＊', "*");
    if normalized.starts_with("*ST") || normalized.starts_with("ST") || normalized.ends_with('退')
    {
        return "ST";
    }
    let ts = ts_code.trim().to_ascii_uppercase();
    if ts.ends_with(".BJ") {
        return "北交所";
    }
    if (ts.ends_with(".SZ") && ts.starts_with("30"))
        || (ts.ends_with(".SH") && ts.starts_with("688"))
    {
        return "创业/科创";
    }
    if ts.ends_with(".SH") || ts.ends_with(".SZ") {
        return "主板";
    }
    "其他"
}

fn load_meta() -> Result<HashMap<String, Meta>, Box<dyn std::error::Error>> {
    let mut rdr = csv::Reader::from_path(format!("{SOURCE}/stock_list.csv"))?;
    let mut out = HashMap::new();
    for row in rdr.records() {
        let row = row?;
        let ts = row.get(0).unwrap_or_default().trim().to_string();
        if ts.is_empty() {
            continue;
        }
        let name = row.get(2).unwrap_or_default().trim().to_string();
        let total_mv_yi = row
            .get(9)
            .and_then(|v| v.trim().parse::<f64>().ok())
            .map(|v| v / 1e4);
        out.insert(
            ts.clone(),
            Meta {
                industry: row.get(4).unwrap_or_default().trim().to_string(),
                area: row.get(3).unwrap_or_default().trim().to_string(),
                board: board_category(&ts, &name).to_string(),
                name,
                total_mv_yi,
            },
        );
    }
    Ok(out)
}

fn load_concepts() -> Result<HashMap<String, Vec<String>>, Box<dyn std::error::Error>> {
    let mut rdr = csv::Reader::from_path(format!("{SOURCE}/stock_concepts.csv"))?;
    let mut out = HashMap::new();
    for row in rdr.records() {
        let row = row?;
        let ts = row.get(0).unwrap_or_default().trim().to_string();
        if ts.is_empty() {
            continue;
        }
        let concepts = row
            .get(2)
            .unwrap_or_default()
            .split(',')
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        out.insert(ts, concepts);
    }
    Ok(out)
}

fn load_rule_meta() -> Result<HashMap<String, RuleMeta>, Box<dyn std::error::Error>> {
    let text = fs::read_to_string(format!("{SOURCE}/score_rule.toml"))?;
    let value: toml::Value = toml::from_str(&text)?;
    let mut out = HashMap::new();
    if let Some(rules) = value.get("rule").and_then(|v| v.as_array()) {
        for rule in rules {
            let Some(name) = rule.get("name").and_then(|v| v.as_str()) else {
                continue;
            };
            out.insert(
                name.to_string(),
                RuleMeta {
                    scene: rule
                        .get("scene")
                        .and_then(|v| v.as_str())
                        .unwrap_or_default()
                        .to_string(),
                    stage: rule
                        .get("stage")
                        .and_then(|v| v.as_str())
                        .unwrap_or_default()
                        .to_string(),
                    points: rule.get("points").and_then(|v| v.as_float()).unwrap_or(0.0),
                    explain: rule
                        .get("explain")
                        .and_then(|v| v.as_str())
                        .unwrap_or_default()
                        .to_string(),
                },
            );
        }
    }
    Ok(out)
}

fn query_dates(conn: &Connection) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut stmt = conn.prepare(
        "SELECT DISTINCT trade_date FROM stock_data WHERE adj_type='qfq' ORDER BY trade_date ASC",
    )?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
    Ok(rows.collect::<Result<Vec<_>, _>>()?)
}

fn load_bars(
    conn: &Connection,
    start: &str,
    end: &str,
) -> Result<HashMap<(String, String), Bar>, Box<dyn std::error::Error>> {
    let sql = r#"
        SELECT *
        FROM (
            SELECT
                ts_code,
                trade_date,
                TRY_CAST(close AS DOUBLE) AS close_price,
                TRY_CAST(pct_chg AS DOUBLE) AS pct,
                TRY_CAST(tor AS DOUBLE) AS tor,
                TRY_CAST(vol AS DOUBLE) AS vol,
                TRY_CAST(amount AS DOUBLE) AS amount,
                TRY_CAST(VR AS DOUBLE) AS vr,
                TRY_CAST(VR_20 AS DOUBLE) AS vr20,
                TRY_CAST(J AS DOUBLE) AS j,
                TRY_CAST(RSI6 AS DOUBLE) AS rsi6,
                TRY_CAST(MA10 AS DOUBLE) AS ma10,
                TRY_CAST(MA20 AS DOUBLE) AS ma20,
                TRY_CAST(BRICK AS DOUBLE) AS brick,
                TRY_CAST(DUOKONG_SHORT AS DOUBLE) AS duokong_short,
                TRY_CAST(DUOKONG_LONG AS DOUBLE) AS duokong_long,
                TRY_CAST(Z_SCORE AS DOUBLE) AS z_score,
                TRY_CAST(VOL_SIGMA AS DOUBLE) AS vol_sigma,
                TRY_CAST(C_SIGMA AS DOUBLE) AS c_sigma,
                TRY_CAST(RSV_C90 AS DOUBLE) AS rsv_c90,
                TRY_CAST(RSV_C30 AS DOUBLE) AS rsv_c30,
                ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY trade_date ASC) AS rn
            FROM stock_data
            WHERE adj_type = 'qfq'
        ) AS t
        WHERE trade_date >= ?
          AND trade_date <= ?
    "#;
    let mut stmt = conn.prepare(sql)?;
    let mut rows = stmt.query(params![start, end])?;
    let mut out = HashMap::new();
    while let Some(row) = rows.next()? {
        let ts: String = row.get(0)?;
        let date: String = row.get(1)?;
        out.insert(
            (ts, date),
            Bar {
                close: row.get(2)?,
                pct: row.get(3)?,
                tor: row.get(4)?,
                vol: row.get(5)?,
                amount: row.get(6)?,
                vr: row.get(7)?,
                vr20: row.get(8)?,
                j: row.get(9)?,
                rsi6: row.get(10)?,
                ma10: row.get(11)?,
                ma20: row.get(12)?,
                brick: row.get(13)?,
                duokong_short: row.get(14)?,
                duokong_long: row.get(15)?,
                z_score: row.get(16)?,
                vol_sigma: row.get(17)?,
                c_sigma: row.get(18)?,
                rsv_c90: row.get(19)?,
                rsv_c30: row.get(20)?,
                rn: row.get(21)?,
            },
        );
    }
    Ok(out)
}

fn is_sample_stock(meta: &HashMap<String, Meta>, ts: &str, bar: &Bar) -> bool {
    bar.rn >= MIN_RN && meta.get(ts).is_some_and(|m| m.board == "主板")
}

fn desc(a: f64, b: f64) -> Ordering {
    b.partial_cmp(&a).unwrap_or(Ordering::Equal)
}

fn top_daily(
    ref_date: &str,
    bars: &HashMap<(String, String), Bar>,
    meta: &HashMap<String, Meta>,
) -> Vec<Winner> {
    let mut rows = bars
        .iter()
        .filter_map(|((ts, date), bar)| {
            if date != ref_date || !is_sample_stock(meta, ts, bar) {
                return None;
            }
            Some((
                ts.clone(),
                meta.get(ts)?.name.clone(),
                bar.pct.filter(|v| v.is_finite())?,
            ))
        })
        .collect::<Vec<_>>();
    rows.sort_by(|a, b| desc(a.2, b.2).then_with(|| a.0.cmp(&b.0)));
    rows.truncate(TOP_N);
    rows.into_iter()
        .map(|(ts_code, name, value)| Winner {
            kind: "daily",
            ref_date: ref_date.to_string(),
            start_date: ref_date.to_string(),
            end_date: ref_date.to_string(),
            ts_code,
            name,
            value,
        })
        .collect()
}

fn top_interval(
    ref_date: &str,
    date_window: &[String],
    bars: &HashMap<(String, String), Bar>,
    meta: &HashMap<String, Meta>,
) -> Vec<Winner> {
    let Some(start_date) = date_window.first() else {
        return Vec::new();
    };
    let Some(end_date) = date_window.last() else {
        return Vec::new();
    };
    let codes = meta.keys().cloned().collect::<Vec<_>>();
    let mut rows = Vec::new();
    for ts in codes {
        let Some(start_bar) = bars.get(&(ts.clone(), start_date.clone())) else {
            continue;
        };
        let Some(end_bar) = bars.get(&(ts.clone(), end_date.clone())) else {
            continue;
        };
        if !is_sample_stock(meta, &ts, end_bar) {
            continue;
        }
        let (Some(s), Some(e)) = (start_bar.close, end_bar.close) else {
            continue;
        };
        if s <= f64::EPSILON || e <= f64::EPSILON {
            continue;
        }
        let value = (e / s - 1.0) * 100.0;
        if value.is_finite() {
            rows.push((ts.clone(), meta[&ts].name.clone(), value));
        }
    }
    rows.sort_by(|a, b| desc(a.2, b.2).then_with(|| a.0.cmp(&b.0)));
    rows.truncate(TOP_N);
    rows.into_iter()
        .map(|(ts_code, name, value)| Winner {
            kind: "interval",
            ref_date: ref_date.to_string(),
            start_date: start_date.clone(),
            end_date: end_date.clone(),
            ts_code,
            name,
            value,
        })
        .collect()
}

fn top_sub_interval(
    ref_date: &str,
    date_window: &[String],
    bars: &HashMap<(String, String), Bar>,
    meta: &HashMap<String, Meta>,
) -> Vec<Winner> {
    if date_window.len() < SUB_WIN {
        return Vec::new();
    }
    let codes = meta.keys().cloned().collect::<Vec<_>>();
    let mut rows = Vec::new();
    for ts in codes {
        let mut best: Option<(f64, String, String)> = None;
        for win in date_window.windows(SUB_WIN) {
            let start = &win[0];
            let end = &win[SUB_WIN - 1];
            let Some(start_bar) = bars.get(&(ts.clone(), start.clone())) else {
                continue;
            };
            let Some(end_bar) = bars.get(&(ts.clone(), end.clone())) else {
                continue;
            };
            if !is_sample_stock(meta, &ts, end_bar) {
                continue;
            }
            let (Some(s), Some(e)) = (start_bar.close, end_bar.close) else {
                continue;
            };
            if s <= f64::EPSILON || e <= f64::EPSILON {
                continue;
            }
            let value = (e / s - 1.0) * 100.0;
            if value.is_finite() && best.as_ref().is_none_or(|(b, _, _)| value > *b) {
                best = Some((value, start.clone(), end.clone()));
            }
        }
        if let Some((value, start_date, end_date)) = best {
            rows.push((
                ts.clone(),
                meta[&ts].name.clone(),
                value,
                start_date,
                end_date,
            ));
        }
    }
    rows.sort_by(|a, b| desc(a.2, b.2).then_with(|| a.0.cmp(&b.0)));
    rows.truncate(TOP_N);
    rows.into_iter()
        .map(|(ts_code, name, value, start_date, end_date)| Winner {
            kind: "sub3",
            ref_date: ref_date.to_string(),
            start_date,
            end_date,
            ts_code,
            name,
            value,
        })
        .collect()
}

fn quoted_in(codes: &HashSet<String>) -> String {
    codes
        .iter()
        .map(|c| format!("'{}'", c.replace('\'', "''")))
        .collect::<Vec<_>>()
        .join(",")
}

fn load_rule_hits(
    conn: &Connection,
    codes: &HashSet<String>,
    start: &str,
    end: &str,
) -> Result<HashMap<(String, String), Vec<RuleHit>>, Box<dyn std::error::Error>> {
    let in_list = quoted_in(codes);
    let sql = format!(
        r#"
        SELECT ts_code, trade_date, rule_name, TRY_CAST(rule_score AS DOUBLE)
        FROM rule_details
        WHERE trade_date >= ?
          AND trade_date <= ?
          AND ts_code IN ({in_list})
          AND rule_score IS NOT NULL
          AND ABS(rule_score) > 1e-12
        "#
    );
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query(params![start, end])?;
    let mut out: HashMap<(String, String), Vec<RuleHit>> = HashMap::new();
    while let Some(row) = rows.next()? {
        let ts: String = row.get(0)?;
        let date: String = row.get(1)?;
        out.entry((ts, date)).or_default().push(RuleHit {
            rule_name: row.get(2)?,
            score: row.get::<_, Option<f64>>(3)?.unwrap_or(0.0),
        });
    }
    Ok(out)
}

fn load_scores(
    conn: &Connection,
    codes: &HashSet<String>,
    start: &str,
    end: &str,
) -> Result<HashMap<(String, String), ScoreInfo>, Box<dyn std::error::Error>> {
    let in_list = quoted_in(codes);
    let sql = format!(
        r#"
        SELECT ts_code, trade_date, rank, TRY_CAST(total_score AS DOUBLE)
        FROM score_summary
        WHERE trade_date >= ?
          AND trade_date <= ?
          AND ts_code IN ({in_list})
        "#
    );
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query(params![start, end])?;
    let mut out = HashMap::new();
    while let Some(row) = rows.next()? {
        let ts: String = row.get(0)?;
        let date: String = row.get(1)?;
        out.insert(
            (ts, date),
            ScoreInfo {
                rank: row.get(2)?,
                total_score: row.get(3)?,
            },
        );
    }
    Ok(out)
}

fn date_range_indices(
    date_index: &HashMap<String, usize>,
    start: &str,
    end: &str,
) -> Option<(usize, usize)> {
    Some((*date_index.get(start)?, *date_index.get(end)?))
}

fn collect_rule_set(
    hits: &HashMap<(String, String), Vec<RuleHit>>,
    dates: &[String],
    ts: &str,
    score_positive_only: bool,
) -> HashSet<String> {
    let mut out = HashSet::new();
    for d in dates {
        if let Some(items) = hits.get(&(ts.to_string(), d.clone())) {
            for hit in items {
                if !score_positive_only || hit.score > 0.0 {
                    out.insert(hit.rule_name.clone());
                }
            }
        }
    }
    out
}

fn agg_rules(
    winners: &[Winner],
    all_dates: &[String],
    date_index: &HashMap<String, usize>,
    hits: &HashMap<(String, String), Vec<RuleHit>>,
    mode: &str,
) -> (BTreeMap<String, Agg>, usize, usize) {
    let mut agg: BTreeMap<String, Agg> = BTreeMap::new();
    let mut samples = 0usize;
    let mut covered = 0usize;
    for w in winners {
        let dates = match mode {
            "same" => vec![w.ref_date.clone()],
            "prev3" => {
                let Some(&i) = date_index.get(&w.ref_date) else {
                    continue;
                };
                let s = i.saturating_sub(3);
                all_dates[s..i].to_vec()
            }
            "pre_start5" => {
                let Some(&i) = date_index.get(&w.start_date) else {
                    continue;
                };
                let s = i.saturating_sub(5);
                all_dates[s..i].to_vec()
            }
            "early5" => {
                let Some((s, e)) = date_range_indices(date_index, &w.start_date, &w.end_date)
                else {
                    continue;
                };
                let end = e.min(s + 4);
                all_dates[s..=end].to_vec()
            }
            _ => Vec::new(),
        };
        samples += 1;
        let rules = collect_rule_set(hits, &dates, &w.ts_code, true);
        if !rules.is_empty() {
            covered += 1;
        }
        for rule in rules {
            let entry = agg.entry(rule).or_default();
            entry.count += 1;
            entry.sum_value += w.value;
        }
    }
    (agg, samples, covered)
}

fn rank_capture(
    winners: &[Winner],
    all_dates: &[String],
    date_index: &HashMap<String, usize>,
    scores: &HashMap<(String, String), ScoreInfo>,
    mode: &str,
    rank_limit: i64,
) -> (usize, usize, NumStats) {
    let mut n = 0usize;
    let mut cap = 0usize;
    let mut score_stats = NumStats::default();
    for w in winners {
        let date = match mode {
            "same" => Some(w.ref_date.clone()),
            "prev1" => date_index
                .get(&w.ref_date)
                .and_then(|i| i.checked_sub(1))
                .and_then(|i| all_dates.get(i).cloned()),
            "start" => Some(w.start_date.clone()),
            "pre_start1" => date_index
                .get(&w.start_date)
                .and_then(|i| i.checked_sub(1))
                .and_then(|i| all_dates.get(i).cloned()),
            _ => None,
        };
        let Some(date) = date else {
            continue;
        };
        n += 1;
        if let Some(info) = scores.get(&(w.ts_code.clone(), date)) {
            if info.rank.is_some_and(|r| r <= rank_limit) {
                cap += 1;
            }
            score_stats.add(info.total_score);
        }
    }
    (n, cap, score_stats)
}

fn add_feature(map: &mut BTreeMap<String, (usize, usize)>, name: &str, yes: bool) {
    let entry = map.entry(name.to_string()).or_default();
    entry.1 += 1;
    if yes {
        entry.0 += 1;
    }
}

fn feature_snapshot(
    sample_keys: &[(String, String)],
    bars: &HashMap<(String, String), Bar>,
    scores: &HashMap<(String, String), ScoreInfo>,
) -> BTreeMap<String, (usize, usize)> {
    let mut out = BTreeMap::new();
    for (ts, date) in sample_keys {
        let Some(bar) = bars.get(&(ts.clone(), date.clone())) else {
            continue;
        };
        add_feature(&mut out, "换手>=10", bar.tor.is_some_and(|v| v >= 10.0));
        add_feature(&mut out, "换手>=16", bar.tor.is_some_and(|v| v >= 16.0));
        add_feature(&mut out, "VR>=2.0", bar.vr.is_some_and(|v| v >= 2.0));
        add_feature(&mut out, "VR<0.7", bar.vr.is_some_and(|v| v < 0.7));
        add_feature(&mut out, "RSI6>=70", bar.rsi6.is_some_and(|v| v >= 70.0));
        add_feature(
            &mut out,
            "RSV_C30>=80",
            bar.rsv_c30.is_some_and(|v| v >= 80.0),
        );
        add_feature(
            &mut out,
            "RSV_C90>=80",
            bar.rsv_c90.is_some_and(|v| v >= 80.0),
        );
        add_feature(
            &mut out,
            "收盘>MA10",
            match (bar.close, bar.ma10) {
                (Some(c), Some(m)) => c > m,
                _ => false,
            },
        );
        add_feature(
            &mut out,
            "收盘>MA20",
            match (bar.close, bar.ma20) {
                (Some(c), Some(m)) => c > m,
                _ => false,
            },
        );
        add_feature(
            &mut out,
            "多空短>长",
            match (bar.duokong_short, bar.duokong_long) {
                (Some(s), Some(l)) => s > l,
                _ => false,
            },
        );
        add_feature(
            &mut out,
            "靠近长期成本3%",
            match (bar.close, bar.duokong_long) {
                (Some(c), Some(l)) if l.abs() > f64::EPSILON => ((c - l).abs() / l) <= 0.03,
                _ => false,
            },
        );
        add_feature(
            &mut out,
            "总榜<=100",
            scores
                .get(&(ts.clone(), date.clone()))
                .and_then(|v| v.rank)
                .is_some_and(|r| r <= 100),
        );
        add_feature(
            &mut out,
            "总榜<=200",
            scores
                .get(&(ts.clone(), date.clone()))
                .and_then(|v| v.rank)
                .is_some_and(|r| r <= 200),
        );
    }
    out
}

fn feature_lift(
    sample: &BTreeMap<String, (usize, usize)>,
    base: &BTreeMap<String, (usize, usize)>,
) -> Vec<(String, f64, f64, f64)> {
    let mut out = Vec::new();
    for (name, (sy, sn)) in sample {
        if *sn == 0 {
            continue;
        }
        let sr = *sy as f64 / *sn as f64;
        let (by, bn) = base.get(name).copied().unwrap_or_default();
        let br = if bn == 0 { 0.0 } else { by as f64 / bn as f64 };
        let lift = if br > 1e-12 { sr / br } else { 0.0 };
        out.push((name.clone(), sr, br, lift));
    }
    out.sort_by(|a, b| desc(a.3, b.3).then_with(|| desc(a.1, b.1)));
    out
}

fn top_counts(items: impl Iterator<Item = String>) -> Vec<(String, usize)> {
    let mut map = BTreeMap::<String, usize>::new();
    for item in items.filter(|v| !v.trim().is_empty()) {
        *map.entry(item).or_default() += 1;
    }
    let mut out = map.into_iter().collect::<Vec<_>>();
    out.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    out
}

fn fmt_pct(v: f64) -> String {
    format!("{:.1}%", v * 100.0)
}

fn write_rule_table(
    out: &mut String,
    title: &str,
    agg: &BTreeMap<String, Agg>,
    samples: usize,
    covered: usize,
    meta: &HashMap<String, RuleMeta>,
    limit: usize,
) {
    out.push_str(&format!("\n### {title}\n\n"));
    out.push_str(&format!(
        "覆盖: {covered}/{samples} = {}\n\n",
        if samples == 0 {
            "--".to_string()
        } else {
            fmt_pct(covered as f64 / samples as f64)
        }
    ));
    out.push_str(
        "|规则|命中样本|覆盖率|平均样本涨幅|场景/阶段|分值|\n|---|---:|---:|---:|---|---:|\n",
    );
    let mut rows = agg.iter().collect::<Vec<_>>();
    rows.sort_by(|a, b| b.1.count.cmp(&a.1.count).then_with(|| a.0.cmp(b.0)));
    for (rule, a) in rows.into_iter().take(limit) {
        let m = meta.get(rule).cloned().unwrap_or_default();
        out.push_str(&format!(
            "|{}|{}|{}|{:.2}%|{}/{}|{:.1}|\n",
            rule,
            a.count,
            if samples == 0 {
                "--".to_string()
            } else {
                fmt_pct(a.count as f64 / samples as f64)
            },
            a.sum_value / a.count.max(1) as f64,
            m.scene,
            m.stage,
            m.points
        ));
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let meta = load_meta()?;
    let concepts = load_concepts()?;
    let rule_meta = load_rule_meta()?;
    let stock_conn = Connection::open(format!("{SOURCE}/stock_data.db"))?;
    let result_conn = Connection::open(format!("{SOURCE}/scoring_result.db"))?;

    let all_dates = query_dates(&stock_conn)?;
    let latest = all_dates.last().cloned().ok_or("no dates")?;
    let start_ref_idx = all_dates.len().saturating_sub(REF_DAYS);
    let ref_dates = all_dates[start_ref_idx..].to_vec();
    let load_start_idx = all_dates.len().saturating_sub(REF_DAYS + LOOKBACK + 10);
    let load_start = all_dates[load_start_idx].clone();
    let bars = load_bars(&stock_conn, &load_start, &latest)?;
    let date_index = all_dates
        .iter()
        .enumerate()
        .map(|(i, d)| (d.clone(), i))
        .collect::<HashMap<_, _>>();

    let mut daily = Vec::new();
    let mut interval = Vec::new();
    let mut sub3 = Vec::new();
    for ref_date in &ref_dates {
        let Some(&ref_i) = date_index.get(ref_date) else {
            continue;
        };
        let win_start = ref_i.saturating_sub(LOOKBACK - 1);
        let win_dates = all_dates[win_start..=ref_i].to_vec();
        daily.extend(top_daily(ref_date, &bars, &meta));
        interval.extend(top_interval(ref_date, &win_dates, &bars, &meta));
        sub3.extend(top_sub_interval(ref_date, &win_dates, &bars, &meta));
    }

    let mut all_winners = Vec::new();
    all_winners.extend(daily.clone());
    all_winners.extend(interval.clone());
    all_winners.extend(sub3.clone());
    let codes = all_winners
        .iter()
        .map(|w| w.ts_code.clone())
        .collect::<HashSet<_>>();
    let signal_start_idx = load_start_idx.saturating_sub(5);
    let signal_start = all_dates[signal_start_idx].clone();
    let rule_hits = load_rule_hits(&result_conn, &codes, &signal_start, &latest)?;
    let scores = load_scores(&result_conn, &codes, &signal_start, &latest)?;

    let (daily_same, daily_same_n, daily_same_cov) =
        agg_rules(&daily, &all_dates, &date_index, &rule_hits, "same");
    let (daily_prev3, daily_prev3_n, daily_prev3_cov) =
        agg_rules(&daily, &all_dates, &date_index, &rule_hits, "prev3");
    let (interval_pre5, interval_pre5_n, interval_pre5_cov) =
        agg_rules(&interval, &all_dates, &date_index, &rule_hits, "pre_start5");
    let (interval_early5, interval_early5_n, interval_early5_cov) =
        agg_rules(&interval, &all_dates, &date_index, &rule_hits, "early5");
    let (sub3_pre5, sub3_pre5_n, sub3_pre5_cov) =
        agg_rules(&sub3, &all_dates, &date_index, &rule_hits, "pre_start5");

    let daily_same_rank100 = rank_capture(&daily, &all_dates, &date_index, &scores, "same", 100);
    let daily_prev_rank100 = rank_capture(&daily, &all_dates, &date_index, &scores, "prev1", 100);
    let interval_start_rank100 =
        rank_capture(&interval, &all_dates, &date_index, &scores, "start", 100);
    let interval_pre_rank100 = rank_capture(
        &interval,
        &all_dates,
        &date_index,
        &scores,
        "pre_start1",
        100,
    );

    let mut latest_daily = daily
        .iter()
        .filter(|w| w.ref_date == latest)
        .cloned()
        .collect::<Vec<_>>();
    latest_daily.sort_by(|a, b| desc(a.value, b.value));

    let daily_prev_keys = daily
        .iter()
        .filter_map(|w| {
            let i = *date_index.get(&w.ref_date)?;
            let d = all_dates.get(i.checked_sub(1)?)?.clone();
            Some((w.ts_code.clone(), d))
        })
        .collect::<Vec<_>>();
    let mut base_prev_keys = Vec::new();
    let daily_ref_prev_dates = daily_prev_keys
        .iter()
        .map(|(_, d)| d.clone())
        .collect::<HashSet<_>>();
    for ((ts, date), bar) in &bars {
        if daily_ref_prev_dates.contains(date) && is_sample_stock(&meta, ts, bar) {
            base_prev_keys.push((ts.clone(), date.clone()));
        }
    }
    let sample_features = feature_snapshot(&daily_prev_keys, &bars, &scores);
    let base_features = feature_snapshot(&base_prev_keys, &bars, &scores);
    let feature_rows = feature_lift(&sample_features, &base_features);

    let industry_counts = top_counts(all_winners.iter().filter_map(|w| {
        meta.get(&w.ts_code)
            .map(|m| format!("{} / {}", m.industry, m.area))
    }));
    let concept_counts = top_counts(all_winners.iter().flat_map(|w| {
        concepts
            .get(&w.ts_code)
            .cloned()
            .unwrap_or_default()
            .into_iter()
    }));

    let mut report = String::new();
    report.push_str("# 主板强势股与策略触发分析报告\n\n");
    report.push_str(&format!(
        "- 数据目录: `{SOURCE}`\n- 实际行情日期: {} ~ {}\n- 本次参考日: 最近 {} 个交易日，{} ~ {}\n- 样本: 主板、非 ST、至少 {} 条 qfq 日线；每个参考日取当日/{}日区间/{}日子区间涨幅前 {}。\n\n",
        all_dates.first().unwrap(),
        latest,
        ref_dates.len(),
        ref_dates.first().unwrap(),
        ref_dates.last().unwrap(),
        MIN_RN,
        LOOKBACK,
        SUB_WIN,
        TOP_N
    ));

    report.push_str("## 结论摘要\n\n");
    report.push_str(&format!(
        "- 当日涨幅榜样本 {} 个；同日正向策略覆盖 {}，前 3 日提前正向策略覆盖 {}。\n",
        daily_same_n,
        fmt_pct(daily_same_cov as f64 / daily_same_n.max(1) as f64),
        fmt_pct(daily_prev3_cov as f64 / daily_prev3_n.max(1) as f64)
    ));
    report.push_str(&format!(
        "- {} 日区间强势样本 {} 个；启动前 5 日覆盖 {}，启动后前 5 日覆盖 {}。\n",
        LOOKBACK,
        interval_pre5_n,
        fmt_pct(interval_pre5_cov as f64 / interval_pre5_n.max(1) as f64),
        fmt_pct(interval_early5_cov as f64 / interval_early5_n.max(1) as f64)
    ));
    report.push_str(&format!(
        "- 总榜捕捉：当日涨幅榜同日 rank<=100 为 {}/{}，前一日 rank<=100 为 {}/{}；{}日强势股启动日 rank<=100 为 {}/{}，启动前一日为 {}/{}。\n",
        daily_same_rank100.1,
        daily_same_rank100.0,
        daily_prev_rank100.1,
        daily_prev_rank100.0,
        LOOKBACK,
        interval_start_rank100.1,
        interval_start_rank100.0,
        interval_pre_rank100.1,
        interval_pre_rank100.0
    ));

    report.push_str("\n## 最新参考日当日主板涨幅榜\n\n");
    report.push_str("|排名|代码|名称|涨幅|行业/地区|总市值亿|\n|---:|---|---|---:|---|---:|\n");
    for (idx, w) in latest_daily.iter().enumerate() {
        let m = &meta[&w.ts_code];
        report.push_str(&format!(
            "|{}|{}|{}|{:.2}%|{} / {}|{}|\n",
            idx + 1,
            w.ts_code,
            w.name,
            w.value,
            m.industry,
            m.area,
            m.total_mv_yi
                .map(|v| format!("{:.0}", v))
                .unwrap_or_else(|| "--".to_string())
        ));
    }

    write_rule_table(
        &mut report,
        "当日涨幅榜：同日策略触发",
        &daily_same,
        daily_same_n,
        daily_same_cov,
        &rule_meta,
        20,
    );
    write_rule_table(
        &mut report,
        "当日涨幅榜：前 3 日提前策略触发",
        &daily_prev3,
        daily_prev3_n,
        daily_prev3_cov,
        &rule_meta,
        20,
    );
    write_rule_table(
        &mut report,
        &format!("{} 日区间强势股：启动前 5 日策略触发", LOOKBACK),
        &interval_pre5,
        interval_pre5_n,
        interval_pre5_cov,
        &rule_meta,
        20,
    );
    write_rule_table(
        &mut report,
        &format!("{} 日区间强势股：启动后前 5 日策略触发", LOOKBACK),
        &interval_early5,
        interval_early5_n,
        interval_early5_cov,
        &rule_meta,
        20,
    );
    write_rule_table(
        &mut report,
        "3 日急涨强势股：启动前 5 日策略触发",
        &sub3_pre5,
        sub3_pre5_n,
        sub3_pre5_cov,
        &rule_meta,
        20,
    );

    report.push_str("\n## 当日涨幅榜前一日指标共性\n\n");
    report.push_str("|特征|强势样本占比|主板基准占比|Lift|\n|---|---:|---:|---:|\n");
    for (name, sr, br, lift) in feature_rows.iter().take(16) {
        report.push_str(&format!(
            "|{}|{}|{}|{:.2}|\n",
            name,
            fmt_pct(*sr),
            fmt_pct(*br),
            lift
        ));
    }

    report.push_str("\n## 行业/概念重复项\n\n");
    report.push_str("行业/地区 Top10：\n\n");
    for (name, count) in industry_counts.iter().take(10) {
        report.push_str(&format!("- {}: {}\n", name, count));
    }
    report.push_str("\n概念 Top15：\n\n");
    for (name, count) in concept_counts.iter().take(15) {
        report.push_str(&format!("- {}: {}\n", name, count));
    }

    report.push_str("\n## 策略修改建议\n\n");
    report.push_str("1. 提高“强势前置”的区分度，而不是只看是否覆盖。最近样本前置覆盖率很高，但主要由低分/宽口径 base 规则贡献；同时强势样本前一日 rank<=100 占比很低，说明规则命中了不少，但没有把它们稳定推到总榜前排。\n\n");
    report.push_str("2. 新增一个低分确认型规则：`总榜靠前 + 趋势在均线上 + 多空短线强于长线 + 换手不过热`。建议先给 3~5 分，放在“趋势启动/confirm”或“量价结构/base”，避免和现有高分异动规则重复。\n\n");
    report.push_str("```toml\n[[rule]]\nname = \"主板强势前置-趋势温和放量\"\nscene = \"趋势启动\"\nstage = \"confirm\"\nscope_windows = 3\nscope_way = \"ANY\"\nwhen = \"\"\"\nRANK <= 200\nAND C > MA10\nAND C > MA20\nAND DUOKONG_SHORT > DUOKONG_LONG\nAND TOR >= 3 AND TOR <= 16\nAND COUNT(PCT_CHG > 6, 5) <= 1\n\"\"\"\npoints = 4.0\nexplain = \"总榜已有强度，价格站上短中均线，多空结构偏强，但换手不过热，作为启动前置确认。\"\n```\n\n");
    report.push_str("3. 新增一个“强势回踩不破”规则，用于区间强势股启动前后前 5 日。现有“长期成本线附近”偏窄，可加一个 MA10/MA20 不破且缩量的版本。\n\n");
    report.push_str("```toml\n[[rule]]\nname = \"强势股缩量回踩不破MA20\"\nscene = \"趋势回调\"\nstage = \"trigger\"\nscope_windows = 5\nscope_way = \"ANY\"\nwhen = \"\"\"\nC > MA20\nAND MA10 > MA20\nAND PCT_CHG < 1.5\nAND PCT_CHG > -4\nAND (VR < 0.8 OR V < MA(V, 5))\nAND COUNT(C > MA20, 5) >= 4\n\"\"\"\npoints = 4.0\nexplain = \"趋势仍在 MA20 上方，回踩日量能收缩，补充不贴长期成本线但仍保持强趋势的主板样本。\"\n```\n\n");
    report.push_str("4. 对高换手规则建议分层：`TOR > 16/22` 不宜一律扣太重。若同时 `RANK <= 100`、`C > MA10`、`PCT_CHG` 未连续过热，可把“换手过高-预警”的扣分从 -6 调低到 -3，或增加豁免条件；否则会压掉一部分主板强势启动样本。\n\n");
    report.push_str(&format!("5. 任何新增规则先用“表达式验证/规则分层回测”跑 5 日、10 日残差收益，并检查与现有规则重叠率；样本来自最近 {} 个参考日，不足以直接作为实盘结论。\n", REF_DAYS));

    fs::create_dir_all("/tmp/lh_market_analyze/out")?;
    fs::write("/tmp/lh_market_analyze/report_expanded.md", &report)?;

    let mut csv = fs::File::create("/tmp/lh_market_analyze/out/winners_expanded.csv")?;
    writeln!(csv, "kind,ref_date,start_date,end_date,ts_code,name,value")?;
    for w in &all_winners {
        writeln!(
            csv,
            "{},{},{},{},{},{},{:.6}",
            w.kind, w.ref_date, w.start_date, w.end_date, w.ts_code, w.name, w.value
        )?;
    }

    println!("{report}");
    Ok(())
}
