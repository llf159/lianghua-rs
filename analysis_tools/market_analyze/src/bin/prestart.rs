use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::io::Write;

use duckdb::{Connection, params};

const SOURCE: &str = "/home/lmingyuanl/.local/share/com.mingyuan.lianghua/source";
const LOOKBACK: usize = 20;
const REF_DAYS: usize = 60;
const TOP_N: usize = 20;
const MIN_RN: i64 = 60;
const PRE_DAYS: usize = 10;

#[derive(Debug, Clone)]
struct Meta {
    name: String,
    industry: String,
    board: String,
}

#[derive(Debug, Clone, Default)]
struct Bar {
    open: Option<f64>,
    high: Option<f64>,
    low: Option<f64>,
    close: Option<f64>,
    pct: Option<f64>,
    tor: Option<f64>,
    vol: Option<f64>,
    vr: Option<f64>,
    vr20: Option<f64>,
    j: Option<f64>,
    rsi6: Option<f64>,
    ma10: Option<f64>,
    ma20: Option<f64>,
    brick: Option<f64>,
    dk_s: Option<f64>,
    dk_l: Option<f64>,
    vol_sigma: Option<f64>,
    rsv90: Option<f64>,
    rsv30: Option<f64>,
    rn: i64,
}

#[derive(Debug, Clone)]
struct StrongWindow {
    ref_date: String,
    start_date: String,
    end_date: String,
    ts_code: String,
    name: String,
    ret20: f64,
}

#[derive(Debug, Clone, Default)]
struct Snapshot {
    ts_code: String,
    start_date: String,
    ret20: Option<f64>,
    features: BTreeMap<String, f64>,
    flags: BTreeMap<String, bool>,
}

#[derive(Debug, Clone, Default)]
struct NumAgg {
    n: usize,
    sum: f64,
    values: Vec<f64>,
}

impl NumAgg {
    fn add(&mut self, v: Option<f64>) {
        if let Some(v) = v.filter(|v| v.is_finite()) {
            self.n += 1;
            self.sum += v;
            self.values.push(v);
        }
    }

    fn mean(&self) -> Option<f64> {
        (self.n > 0).then(|| self.sum / self.n as f64)
    }

    fn median(&mut self) -> Option<f64> {
        if self.values.is_empty() {
            return None;
        }
        self.values
            .sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
        Some(self.values[self.values.len() / 2])
    }
}

#[derive(Debug, Clone, Default)]
struct FlagAgg {
    yes: usize,
    n: usize,
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
        out.insert(
            ts.clone(),
            Meta {
                board: board_category(&ts, &name).to_string(),
                industry: row.get(4).unwrap_or_default().trim().to_string(),
                name,
            },
        );
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
                TRY_CAST(open AS DOUBLE),
                TRY_CAST(high AS DOUBLE),
                TRY_CAST(low AS DOUBLE),
                TRY_CAST(close AS DOUBLE),
                TRY_CAST(pct_chg AS DOUBLE),
                TRY_CAST(tor AS DOUBLE),
                TRY_CAST(vol AS DOUBLE),
                TRY_CAST(VR AS DOUBLE),
                TRY_CAST(VR_20 AS DOUBLE),
                TRY_CAST(J AS DOUBLE),
                TRY_CAST(RSI6 AS DOUBLE),
                TRY_CAST(MA10 AS DOUBLE),
                TRY_CAST(MA20 AS DOUBLE),
                TRY_CAST(BRICK AS DOUBLE),
                TRY_CAST(DUOKONG_SHORT AS DOUBLE),
                TRY_CAST(DUOKONG_LONG AS DOUBLE),
                TRY_CAST(VOL_SIGMA AS DOUBLE),
                TRY_CAST(RSV_C90 AS DOUBLE),
                TRY_CAST(RSV_C30 AS DOUBLE),
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
                open: row.get(2)?,
                high: row.get(3)?,
                low: row.get(4)?,
                close: row.get(5)?,
                pct: row.get(6)?,
                tor: row.get(7)?,
                vol: row.get(8)?,
                vr: row.get(9)?,
                vr20: row.get(10)?,
                j: row.get(11)?,
                rsi6: row.get(12)?,
                ma10: row.get(13)?,
                ma20: row.get(14)?,
                brick: row.get(15)?,
                dk_s: row.get(16)?,
                dk_l: row.get(17)?,
                vol_sigma: row.get(18)?,
                rsv90: row.get(19)?,
                rsv30: row.get(20)?,
                rn: row.get(21)?,
            },
        );
    }
    Ok(out)
}

fn is_main(meta: &HashMap<String, Meta>, ts: &str, bar: &Bar) -> bool {
    bar.rn >= MIN_RN && meta.get(ts).is_some_and(|m| m.board == "主板")
}

fn desc(a: f64, b: f64) -> Ordering {
    b.partial_cmp(&a).unwrap_or(Ordering::Equal)
}

fn build_strong_windows(
    dates: &[String],
    date_index: &HashMap<String, usize>,
    bars: &HashMap<(String, String), Bar>,
    meta: &HashMap<String, Meta>,
) -> Vec<StrongWindow> {
    let start_ref_idx = dates.len().saturating_sub(REF_DAYS);
    let mut out = Vec::new();
    for ref_date in &dates[start_ref_idx..] {
        let Some(&ref_i) = date_index.get(ref_date) else {
            continue;
        };
        let win_start = ref_i.saturating_sub(LOOKBACK - 1);
        let start_date = dates[win_start].clone();
        let end_date = dates[ref_i].clone();
        let mut rows = Vec::new();
        for ts in meta.keys() {
            let Some(sb) = bars.get(&(ts.clone(), start_date.clone())) else {
                continue;
            };
            let Some(eb) = bars.get(&(ts.clone(), end_date.clone())) else {
                continue;
            };
            if !is_main(meta, ts, eb) {
                continue;
            }
            let (Some(s), Some(e)) = (sb.close, eb.close) else {
                continue;
            };
            if s <= f64::EPSILON || e <= f64::EPSILON {
                continue;
            }
            let ret20 = (e / s - 1.0) * 100.0;
            if ret20.is_finite() {
                rows.push((ts.clone(), ret20));
            }
        }
        rows.sort_by(|a, b| desc(a.1, b.1).then_with(|| a.0.cmp(&b.0)));
        for (ts_code, ret20) in rows.into_iter().take(TOP_N) {
            out.push(StrongWindow {
                ref_date: ref_date.clone(),
                start_date: start_date.clone(),
                end_date: end_date.clone(),
                name: meta
                    .get(&ts_code)
                    .map(|v| v.name.clone())
                    .unwrap_or_default(),
                ts_code,
                ret20,
            });
        }
    }
    out
}

fn val_ratio(a: Option<f64>, b: Option<f64>) -> Option<f64> {
    match (a, b) {
        (Some(a), Some(b)) if b.abs() > f64::EPSILON => Some((a / b - 1.0) * 100.0),
        _ => None,
    }
}

fn sum_opt(values: &[Option<f64>]) -> Option<f64> {
    let vals = values.iter().flatten().copied().collect::<Vec<_>>();
    (!vals.is_empty()).then(|| vals.iter().sum())
}

fn avg_opt(values: &[Option<f64>]) -> Option<f64> {
    let vals = values.iter().flatten().copied().collect::<Vec<_>>();
    (!vals.is_empty()).then(|| vals.iter().sum::<f64>() / vals.len() as f64)
}

fn max_opt(values: &[Option<f64>]) -> Option<f64> {
    values
        .iter()
        .flatten()
        .copied()
        .filter(|v| v.is_finite())
        .max_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal))
}

fn count_where(values: &[Option<f64>], f: impl Fn(f64) -> bool) -> f64 {
    values.iter().flatten().copied().filter(|v| f(*v)).count() as f64
}

fn make_snapshot(
    ts: &str,
    start_date: &str,
    ret20: Option<f64>,
    dates: &[String],
    date_index: &HashMap<String, usize>,
    bars: &HashMap<(String, String), Bar>,
) -> Option<Snapshot> {
    let start_i = *date_index.get(start_date)?;
    if start_i < PRE_DAYS {
        return None;
    }
    let d1 = dates.get(start_i - 1)?.clone();
    let pre5 = dates[start_i - 5..start_i].to_vec();
    let pre10 = dates[start_i - 10..start_i].to_vec();
    let b1 = bars.get(&(ts.to_string(), d1.clone()))?;

    let get_series = |ds: &[String], f: fn(&Bar) -> Option<f64>| -> Vec<Option<f64>> {
        ds.iter()
            .map(|d| bars.get(&(ts.to_string(), d.clone())).and_then(f))
            .collect()
    };
    let pct5 = get_series(&pre5, |b| b.pct);
    let pct10 = get_series(&pre10, |b| b.pct);
    let tor5 = get_series(&pre5, |b| b.tor);
    let tor10 = get_series(&pre10, |b| b.tor);
    let vr5 = get_series(&pre5, |b| b.vr);
    let amp5 = pre5
        .iter()
        .map(|d| {
            let b = bars.get(&(ts.to_string(), d.clone()))?;
            val_ratio(b.high, b.low)
        })
        .collect::<Vec<_>>();

    let mut s = Snapshot {
        ts_code: ts.to_string(),
        start_date: start_date.to_string(),
        ret20,
        ..Default::default()
    };
    let mut add = |k: &str, v: Option<f64>| {
        if let Some(v) = v.filter(|v| v.is_finite()) {
            s.features.insert(k.to_string(), v);
        }
    };

    add("t1_pct", b1.pct);
    add("sum_pct5", sum_opt(&pct5));
    add("sum_pct10", sum_opt(&pct10));
    add("max_pct5", max_opt(&pct5));
    add("up_days5", Some(count_where(&pct5, |v| v > 0.0)));
    add("big_up_days5", Some(count_where(&pct5, |v| v > 6.0)));
    add(
        "big_abs_days10",
        Some(count_where(&pct10, |v| v.abs() > 4.0)),
    );
    add("t1_tor", b1.tor);
    add("sum_tor5", sum_opt(&tor5));
    add("sum_tor10", sum_opt(&tor10));
    add("max_tor5", max_opt(&tor5));
    add("t1_vr", b1.vr);
    add("max_vr5", max_opt(&vr5));
    add("low_vr_days5", Some(count_where(&vr5, |v| v < 0.7)));
    add("avg_amp5", avg_opt(&amp5));
    add("t1_amp", val_ratio(b1.high, b1.low));
    add("t1_rsi6", b1.rsi6);
    add("t1_j", b1.j);
    add("t1_rsv30", b1.rsv30);
    add("t1_rsv90", b1.rsv90);
    add("t1_brick", b1.brick);
    add("dist_ma10", val_ratio(b1.close, b1.ma10));
    add("dist_ma20", val_ratio(b1.close, b1.ma20));
    add("dist_dk_long", val_ratio(b1.close, b1.dk_l));
    add("dk_spread", val_ratio(b1.dk_s, b1.dk_l));
    add("vr20", b1.vr20);
    add("vol_sigma_ratio", val_ratio(b1.vol, b1.vol_sigma));

    let f = &s.features;
    let get = |k: &str| f.get(k).copied();
    s.flags.insert(
        "C>MA10".to_string(),
        get("dist_ma10").is_some_and(|v| v > 0.0),
    );
    s.flags.insert(
        "C>MA20".to_string(),
        get("dist_ma20").is_some_and(|v| v > 0.0),
    );
    s.flags.insert(
        "MA20内12%".to_string(),
        get("dist_ma20").is_some_and(|v| v > -3.0 && v < 12.0),
    );
    s.flags.insert(
        "多空短>长".to_string(),
        get("dk_spread").is_some_and(|v| v > 0.0),
    );
    s.flags.insert(
        "多空温和强".to_string(),
        get("dk_spread").is_some_and(|v| v > 0.0 && v < 12.0),
    );
    s.flags.insert(
        "BRICK>=70".to_string(),
        get("t1_brick").is_some_and(|v| v >= 70.0),
    );
    s.flags.insert(
        "BRICK>=80".to_string(),
        get("t1_brick").is_some_and(|v| v >= 80.0),
    );
    s.flags.insert(
        "RSI55-80".to_string(),
        get("t1_rsi6").is_some_and(|v| (55.0..=80.0).contains(&v)),
    );
    s.flags.insert(
        "RSV30>=60".to_string(),
        get("t1_rsv30").is_some_and(|v| v >= 60.0),
    );
    s.flags.insert(
        "RSV90>=50".to_string(),
        get("t1_rsv90").is_some_and(|v| v >= 50.0),
    );
    s.flags.insert(
        "T1换手3-16".to_string(),
        get("t1_tor").is_some_and(|v| (3.0..=16.0).contains(&v)),
    );
    s.flags.insert(
        "T1换手>=10".to_string(),
        get("t1_tor").is_some_and(|v| v >= 10.0),
    );
    s.flags.insert(
        "5日换手15-75".to_string(),
        get("sum_tor5").is_some_and(|v| (15.0..=75.0).contains(&v)),
    );
    s.flags.insert(
        "5日换手>=25".to_string(),
        get("sum_tor5").is_some_and(|v| v >= 25.0),
    );
    s.flags.insert(
        "5日不过热".to_string(),
        get("big_up_days5").is_some_and(|v| v <= 1.0) && get("sum_pct5").is_some_and(|v| v <= 12.0),
    );
    s.flags.insert(
        "5日温和上涨".to_string(),
        get("sum_pct5").is_some_and(|v| (0.0..=15.0).contains(&v)),
    );
    s.flags.insert(
        "T1缩量回踩".to_string(),
        get("t1_pct").is_some_and(|v| (-4.0..=1.5).contains(&v))
            && get("t1_vr").is_some_and(|v| v < 0.9),
    );
    s.flags.insert(
        "近5有放量".to_string(),
        get("max_vr5").is_some_and(|v| v >= 1.5),
    );
    s.flags.insert(
        "近5振幅收敛".to_string(),
        get("avg_amp5").is_some_and(|v| v <= 7.0),
    );

    let trend_active = s.flags.get("C>MA20").copied().unwrap_or(false)
        && s.flags.get("多空短>长").copied().unwrap_or(false)
        && s.flags.get("T1换手3-16").copied().unwrap_or(false)
        && s.flags.get("5日不过热").copied().unwrap_or(false);
    let brick_active = s.flags.get("BRICK>=80").copied().unwrap_or(false)
        && s.flags.get("C>MA20").copied().unwrap_or(false)
        && s.flags.get("多空短>长").copied().unwrap_or(false)
        && s.flags.get("T1换手3-16").copied().unwrap_or(false);
    let pullback_hold = s.flags.get("C>MA20").copied().unwrap_or(false)
        && s.flags.get("T1缩量回踩").copied().unwrap_or(false)
        && s.flags.get("多空短>长").copied().unwrap_or(false);
    let mild_setup = s.flags.get("MA20内12%").copied().unwrap_or(false)
        && s.flags.get("5日温和上涨").copied().unwrap_or(false)
        && s.flags.get("5日换手15-75").copied().unwrap_or(false)
        && s.flags.get("近5振幅收敛").copied().unwrap_or(false);
    s.flags.insert("趋势活跃不过热".to_string(), trend_active);
    s.flags.insert("brick趋势活跃".to_string(), brick_active);
    s.flags.insert("强势回踩不破".to_string(), pullback_hold);
    s.flags.insert("起点前温和蓄势".to_string(), mild_setup);

    Some(s)
}

fn aggregate_nums(samples: &[Snapshot]) -> BTreeMap<String, NumAgg> {
    let mut out = BTreeMap::new();
    for s in samples {
        for (k, v) in &s.features {
            out.entry(k.clone())
                .or_insert_with(NumAgg::default)
                .add(Some(*v));
        }
    }
    out
}

fn aggregate_flags(samples: &[Snapshot]) -> BTreeMap<String, FlagAgg> {
    let mut out: BTreeMap<String, FlagAgg> = BTreeMap::new();
    for s in samples {
        for (k, v) in &s.flags {
            let a = out.entry(k.clone()).or_default();
            a.n += 1;
            if *v {
                a.yes += 1;
            }
        }
    }
    out
}

fn top_industries(samples: &[Snapshot], meta: &HashMap<String, Meta>) -> Vec<(String, usize)> {
    let mut m: BTreeMap<String, usize> = BTreeMap::new();
    for s in samples {
        if let Some(item) = meta.get(&s.ts_code) {
            *m.entry(item.industry.clone()).or_default() += 1;
        }
    }
    let mut rows = m.into_iter().collect::<Vec<_>>();
    rows.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    rows
}

fn pct(v: f64) -> String {
    format!("{:.1}%", v * 100.0)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let meta = load_meta()?;
    let conn = Connection::open(format!("{SOURCE}/stock_data.db"))?;
    let dates = query_dates(&conn)?;
    let date_index = dates
        .iter()
        .enumerate()
        .map(|(i, d)| (d.clone(), i))
        .collect::<HashMap<_, _>>();
    let latest = dates.last().cloned().ok_or("no dates")?;
    let load_start_idx = dates
        .len()
        .saturating_sub(REF_DAYS + LOOKBACK + PRE_DAYS + 5);
    let bars = load_bars(&conn, &dates[load_start_idx], &latest)?;
    let strong_windows = build_strong_windows(&dates, &date_index, &bars, &meta);

    let mut strong_samples = Vec::new();
    let mut strong_by_ref_start: HashMap<(String, String), HashSet<String>> = HashMap::new();
    for w in &strong_windows {
        strong_by_ref_start
            .entry((w.ref_date.clone(), w.start_date.clone()))
            .or_default()
            .insert(w.ts_code.clone());
        if let Some(s) = make_snapshot(
            &w.ts_code,
            &w.start_date,
            Some(w.ret20),
            &dates,
            &date_index,
            &bars,
        ) {
            strong_samples.push(s);
        }
    }

    let mut ordinary_samples = Vec::new();
    let mut used_baseline = HashSet::new();
    for ((ref_date, start_date), excluded) in &strong_by_ref_start {
        let _ = ref_date;
        for ts in meta.keys() {
            if excluded.contains(ts) {
                continue;
            }
            let Some(start_bar) = bars.get(&(ts.clone(), start_date.clone())) else {
                continue;
            };
            if !is_main(&meta, ts, start_bar) {
                continue;
            }
            let key = (ts.clone(), start_date.clone());
            if !used_baseline.insert(key.clone()) {
                continue;
            }
            if let Some(s) = make_snapshot(ts, start_date, None, &dates, &date_index, &bars) {
                ordinary_samples.push(s);
            }
        }
    }

    let mut strong_nums = aggregate_nums(&strong_samples);
    let mut ordinary_nums = aggregate_nums(&ordinary_samples);
    let strong_flags = aggregate_flags(&strong_samples);
    let ordinary_flags = aggregate_flags(&ordinary_samples);

    let mut num_rows = Vec::new();
    for k in strong_nums.keys().cloned().collect::<Vec<_>>() {
        let Some(s_mean) = strong_nums.get(&k).and_then(|v| v.mean()) else {
            continue;
        };
        let Some(o_mean) = ordinary_nums.get(&k).and_then(|v| v.mean()) else {
            continue;
        };
        let s_med = strong_nums
            .get_mut(&k)
            .and_then(|v| v.median())
            .unwrap_or(s_mean);
        let o_med = ordinary_nums
            .get_mut(&k)
            .and_then(|v| v.median())
            .unwrap_or(o_mean);
        num_rows.push((k, s_mean, o_mean, s_med, o_med, s_mean - o_mean));
    }
    num_rows.sort_by(|a, b| {
        b.5.abs()
            .partial_cmp(&a.5.abs())
            .unwrap_or(Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0))
    });

    let mut flag_rows = Vec::new();
    for (k, s) in &strong_flags {
        let Some(o) = ordinary_flags.get(k) else {
            continue;
        };
        if s.n == 0 || o.n == 0 {
            continue;
        }
        let sr = s.yes as f64 / s.n as f64;
        let or = o.yes as f64 / o.n as f64;
        let lift = if or > 1e-12 { sr / or } else { 0.0 };
        flag_rows.push((k.clone(), s.yes, s.n, sr, or, lift));
    }
    flag_rows.sort_by(|a, b| {
        b.5.partial_cmp(&a.5)
            .unwrap_or(Ordering::Equal)
            .then_with(|| b.3.partial_cmp(&a.3).unwrap_or(Ordering::Equal))
    });

    let atomic_flags = [
        "C>MA10",
        "C>MA20",
        "MA20内12%",
        "多空短>长",
        "多空温和强",
        "BRICK>=70",
        "BRICK>=80",
        "RSI55-80",
        "RSV30>=60",
        "RSV90>=50",
        "T1换手3-16",
        "5日换手15-75",
        "5日不过热",
        "5日温和上涨",
        "T1缩量回踩",
        "近5有放量",
        "近5振幅收敛",
    ];
    let mut combo_rows = Vec::new();
    for i in 0..atomic_flags.len() {
        for j in i + 1..atomic_flags.len() {
            for k in j + 1..atomic_flags.len() {
                let combo = [atomic_flags[i], atomic_flags[j], atomic_flags[k]];
                let count = |samples: &[Snapshot]| {
                    samples
                        .iter()
                        .filter(|s| {
                            combo
                                .iter()
                                .all(|c| s.flags.get(*c).copied().unwrap_or(false))
                        })
                        .count()
                };
                let sc = count(&strong_samples);
                let oc = count(&ordinary_samples);
                if sc < 25 || oc < 20 {
                    continue;
                }
                let sr = sc as f64 / strong_samples.len() as f64;
                let or = oc as f64 / ordinary_samples.len() as f64;
                let lift = if or > 1e-12 { sr / or } else { 0.0 };
                if lift >= 1.8 {
                    combo_rows.push((combo.join(" + "), sc, sr, or, lift));
                }
            }
        }
    }
    combo_rows.sort_by(|a, b| {
        b.4.partial_cmp(&a.4)
            .unwrap_or(Ordering::Equal)
            .then_with(|| b.1.cmp(&a.1))
    });

    let industries = top_industries(&strong_samples, &meta);

    let mut report = String::new();
    report.push_str("# 强势票起点前 K 线特征对比报告\n\n");
    report.push_str(&format!(
        "- 数据目录: `{SOURCE}`\n- 口径: 最近 {REF_DAYS} 个参考日、20 日区间主板非 ST 涨幅 Top {TOP_N}\n- 实际参考日: {} ~ {}\n- 强势起点样本: {}；普通对照样本: {}\n- 起点定义: 市场分析 20 日区间的第一天；特征取起点前 T-1/T-5/T-10。\n\n",
        dates[dates.len().saturating_sub(REF_DAYS)].clone(),
        latest,
        strong_samples.len(),
        ordinary_samples.len()
    ));

    report.push_str("## 结论\n\n");
    report.push_str("- 强势票起点前不是单纯低位，也不是纯超跌；更像“趋势结构已经转强 + 换手/量能有活跃迹象 + 价格未严重过热”。\n");
    report.push_str("- 最稳定的差异是换手、RSI/RSV、站上 MA20、多空短线强于长线、BRICK 中高位。\n");
    report.push_str("- 现有 `brick相对位-中/低`、`碗里`、`b1买点` 有价值，但需要用趋势/换手/不过热条件做二次确认，避免宽口径规则抬太多普通票。\n\n");

    report.push_str("## 二值特征 Lift\n\n");
    report.push_str("|特征|强势占比|普通占比|Lift|强势命中|\n|---|---:|---:|---:|---:|\n");
    for (name, yes, n, sr, or, lift) in flag_rows.iter().take(24) {
        report.push_str(&format!(
            "|{}|{}|{}|{:.2}|{}/{}|\n",
            name,
            pct(*sr),
            pct(*or),
            lift,
            yes,
            n
        ));
    }

    report.push_str("\n## 数值特征均值差异\n\n");
    report.push_str(
        "|特征|强势均值|普通均值|强势中位|普通中位|均值差|\n|---|---:|---:|---:|---:|---:|\n",
    );
    for (name, sm, om, smed, omed, diff) in num_rows.iter().take(24) {
        report.push_str(&format!(
            "|{}|{:.2}|{:.2}|{:.2}|{:.2}|{:.2}|\n",
            name, sm, om, smed, omed, diff
        ));
    }

    report.push_str("\n## 高 Lift 三特征组合\n\n");
    report.push_str("|组合|强势占比|普通占比|Lift|强势命中|\n|---|---:|---:|---:|---:|\n");
    for (combo, sc, sr, or, lift) in combo_rows.iter().take(24) {
        report.push_str(&format!(
            "|{}|{}|{}|{:.2}|{}|\n",
            combo,
            pct(*sr),
            pct(*or),
            lift,
            sc
        ));
    }

    report.push_str("\n## 强势样本行业重复\n\n");
    for (name, count) in industries.iter().take(15) {
        report.push_str(&format!("- {}: {}\n", name, count));
    }

    report.push_str("\n## 建议新增策略\n\n");
    report.push_str("### 少量过拟合组合策略\n\n");
    report.push_str("```toml\n[[rule]]\nname = \"起点前-brick趋势活跃\"\nscene = \"趋势启动\"\nstage = \"confirm\"\nscope_windows = 3\nscope_way = \"ANY\"\nwhen = \"\"\"\nBRICK >= 80\nAND C > MA20\nAND DUOKONG_SHORT > DUOKONG_LONG\nAND TOR >= 3 AND TOR <= 16\nAND COUNT(PCT_CHG > 6, 5) <= 1\n\"\"\"\npoints = 5.0\nexplain = \"起点前高 lift 组合：brick 中高位、站上 MA20、多空偏强、换手温和活跃且未连续过热。\"\n```\n\n");
    report.push_str("```toml\n[[rule]]\nname = \"起点前-强势回踩不破\"\nscene = \"趋势回调\"\nstage = \"confirm\"\nscope_windows = 5\nscope_way = \"ANY\"\nwhen = \"\"\"\nC > MA20\nAND DUOKONG_SHORT > DUOKONG_LONG\nAND PCT_CHG >= -4 AND PCT_CHG <= 1.5\nAND VR < 0.9\nAND SUM(TOR, 5) >= 15 AND SUM(TOR, 5) <= 75\n\"\"\"\npoints = 4.0\nexplain = \"趋势不破 MA20，回踩日缩量，前 5 日换手已活跃。\"\n```\n\n");
    report.push_str("```toml\n[[rule]]\nname = \"起点前-温和蓄势\"\nscene = \"量价结构\"\nstage = \"confirm\"\nscope_windows = 3\nscope_way = \"ANY\"\nwhen = \"\"\"\nC > MA10\nAND C > MA20\nAND RSI6 >= 55 AND RSI6 <= 80\nAND RSV_C30 >= 60\nAND SUM(PCT_CHG, 5) >= 0 AND SUM(PCT_CHG, 5) <= 15\nAND COUNT(ABS(PCT_CHG) > 4, 10) <= 4\n\"\"\"\npoints = 4.0\nexplain = \"均线与动量已转强，但 5 日涨幅不过热，适合做前置确认。\"\n```\n\n");

    report.push_str("### 特征类因子策略\n\n");
    report.push_str("```toml\n[[rule]]\nname = \"因子-趋势结构强\"\nscene = \"趋势启动\"\nstage = \"base\"\nscope_windows = 5\nscope_way = \"EACH\"\nwhen = \"C > MA20 AND DUOKONG_SHORT > DUOKONG_LONG AND BRICK >= 70\"\npoints = 1.5\nexplain = \"趋势结构因子，低分连续加权。\"\n```\n\n");
    report.push_str("```toml\n[[rule]]\nname = \"因子-量能活跃不过热\"\nscene = \"量价结构\"\nstage = \"base\"\nscope_windows = 3\nscope_way = \"ANY\"\nwhen = \"SUM(TOR, 5) >= 15 AND SUM(TOR, 5) <= 75 AND COUNT(PCT_CHG > 6, 5) <= 1\"\npoints = 2.5\nexplain = \"起点前常见的温和换手活跃因子。\"\n```\n\n");
    report.push_str("```toml\n[[rule]]\nname = \"因子-动量进入强区\"\nscene = \"趋势启动\"\nstage = \"base\"\nscope_windows = 3\nscope_way = \"ANY\"\nwhen = \"RSI6 >= 55 AND RSI6 <= 80 AND RSV_C30 >= 60 AND RSV_C90 >= 50\"\npoints = 2.0\nexplain = \"动量进入强区但不过极端，作为排序辅助因子。\"\n```\n\n");
    report.push_str("```toml\n[[rule]]\nname = \"风险-起点前过热\"\nscene = \"相对高位\"\nstage = \"risk\"\nscope_windows = 1\nscope_way = \"LAST\"\nwhen = \"SUM(PCT_CHG, 5) > 20 OR COUNT(PCT_CHG > 6, 5) >= 3 OR TOR > 22\"\npoints = -5.0\nexplain = \"避免把已经连续加速、换手极端的票继续顶到前排。\"\n```\n");

    fs::write("/tmp/lh_market_analyze/prestart_feature_report.md", &report)?;
    let mut file = fs::File::create("/tmp/lh_market_analyze/out/prestart_strong_samples.csv")?;
    writeln!(file, "ref_date,start_date,end_date,ts_code,name,ret20")?;
    for w in &strong_windows {
        writeln!(
            file,
            "{},{},{},{},{},{:.6}",
            w.ref_date, w.start_date, w.end_date, w.ts_code, w.name, w.ret20
        )?;
    }
    println!("{report}");
    Ok(())
}
