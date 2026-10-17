use std::{
    collections::{BTreeMap, HashSet, hash_map::DefaultHasher},
    fs,
    hash::{Hash, Hasher},
    path::Path,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use lianghua_core::{
    expr::eval::Value,
    tracking::{TrackDefinition, TrackExpression, TrackProgram, TrackState, value_at},
};
use serde::{Deserialize, Serialize};

use crate::{
    data::{self, DataReader, RuntimeKeyCollectOptions, ScoreConfig},
    scoring,
};

use scoring::tracking::input_stamp;

static TRACKING_RUN: Mutex<()> = Mutex::new(());
// Bump when expression or event semantics change; cached states must then be replayed.
const TRACKING_VERSION: u32 = 4;

#[derive(Serialize)]
pub struct TrackingSource {
    pub id: String,
    pub name: String,
    pub enabled: bool,
}

#[derive(Serialize)]
pub struct TrackingRow {
    pub track_id: String,
    pub ts_code: String,
    pub name: String,
    pub started: String,
    pub age: usize,
    pub observing: bool,
    pub captured: BTreeMap<String, f64>,
    pub source_score: Option<f64>,
    pub completed: Vec<String>,
    pub stopped: Option<String>,
    pub reason: Option<String>,
    pub first_signal: Option<String>,
    pub signal_today: bool,
    pub eligible_today: bool,
    pub score: f64,
}

#[derive(Serialize)]
pub struct TrackingPage {
    pub definitions: Vec<TrackDefinition>,
    pub sources: Vec<TrackingSource>,
    pub as_of: Option<String>,
    pub stale: bool,
    pub computed_symbols: usize,
    pub resumed_symbols: usize,
    pub warnings: Vec<String>,
    pub rows: Vec<TrackingRow>,
}

#[derive(Serialize, Deserialize)]
struct TrackingCheckpoint {
    signature: u64,
    states: BTreeMap<String, TrackState>,
}

#[derive(Serialize, Deserialize)]
struct TrackingSnapshot {
    version: u32,
    #[serde(default)]
    computed_at: u128,
    config: String,
    stamp: u64,
    as_of: String,
    calendar: Vec<String>,
    resumed_symbols: usize,
    warnings: Vec<String>,
    stocks: BTreeMap<String, TrackingCheckpoint>,
}

fn load_snapshot(source: &str) -> Result<Option<TrackingSnapshot>, String> {
    let mut snapshot = match fs::read(Path::new(source).join("strategy_tracking.json")) {
        Ok(bytes) => {
            let saved: TrackingSnapshot = serde_json::from_slice(&bytes).map_err(|e| {
                format!("跟踪快照损坏，请移走 strategy_tracking.json 后重新计算：{e}")
            })?;
            (saved.version == TRACKING_VERSION).then_some(saved)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error.to_string()),
    };
    if data::result_db_path(source).exists() {
        let conn =
            data::open_db_readonly(data::result_db_path(source)).map_err(|e| e.to_string())?;
        let exists: bool = conn.query_row("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name='strategy_tracking_state')", [], |row| row.get(0)).map_err(|e| e.to_string())?;
        if exists {
            let config =
                fs::read_to_string(data::score_rule_path(source)).map_err(|e| e.to_string())?;
            let calendar_json: Option<String> = conn
                .query_row(
                    "SELECT MAX(calendar_json) FROM strategy_tracking_meta",
                    [],
                    |row| row.get(0),
                )
                .map_err(|e| format!("读取排名跟踪日历失败:{e}"))?;
            let calendar: Vec<String> =
                serde_json::from_str(calendar_json.as_deref().unwrap_or("[]"))
                    .map_err(|e| format!("排名跟踪日历损坏，请重新计算排名：{e}"))?;
            let mut stmt = conn.prepare("SELECT ts_code, trade_date, config_hash, input_stamp, state_json FROM strategy_tracking_state").map_err(|e| e.to_string())?;
            let rows = stmt
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                    ))
                })
                .map_err(|e| e.to_string())?;
            let mut ranked: Option<TrackingSnapshot> = None;
            for row in rows {
                let (code, date, hash, stamp, json) = row.map_err(|e| e.to_string())?;
                let checkpoint: scoring::tracking::RankingTrackCheckpoint =
                    serde_json::from_str(&json)
                        .map_err(|e| format!("排名跟踪状态损坏，请重新计算排名：{e}"))?;
                let saved = ranked.get_or_insert_with(|| TrackingSnapshot {
                    version: TRACKING_VERSION,
                    computed_at: checkpoint.computed_at,
                    config: if hash == scoring::tracking::config_hash(&config) {
                        config.clone()
                    } else {
                        String::new()
                    },
                    stamp: stamp.parse().unwrap_or(0),
                    as_of: date.clone(),
                    calendar: calendar.clone(),
                    resumed_symbols: 0,
                    warnings: Vec::new(),
                    stocks: BTreeMap::new(),
                });
                if saved.as_of != date || saved.computed_at != checkpoint.computed_at {
                    return Err("排名跟踪快照不一致，请重新计算排名".into());
                }
                saved.resumed_symbols += usize::from(checkpoint.resumed);
                saved.stocks.insert(
                    code,
                    TrackingCheckpoint {
                        signature: checkpoint.signature,
                        states: checkpoint.states,
                    },
                );
            }
            if let Some(ranked) = ranked.filter(|ranked| {
                snapshot
                    .as_ref()
                    .is_none_or(|old| ranked.computed_at >= old.computed_at)
            }) {
                snapshot = Some(ranked);
            }
        }
    }
    if let Some(saved) = &snapshot {
        if saved.calendar.last() != Some(&saved.as_of)
            || saved.stocks.values().any(|stock| {
                stock
                    .states
                    .values()
                    .filter_map(|state| state.event.as_ref())
                    .any(|event| {
                        std::iter::once(event.started)
                            .chain(event.completed.iter().copied())
                            .chain(event.stopped)
                            .chain(event.first_signal)
                            .chain(event.score_started)
                            .any(|index| index >= saved.calendar.len())
                    })
            })
        {
            return Err("跟踪快照的交易日索引损坏，请重新计算跟踪或排名".into());
        }
    }
    Ok(snapshot)
}

pub fn get_strategy_tracking_page(source: &str) -> Result<TrackingPage, String> {
    let cfg = ScoreConfig::load(source)?;
    let config = fs::read_to_string(data::score_rule_path(source)).map_err(|e| e.to_string())?;
    let snapshot = load_snapshot(source)?;
    let names = lianghua_app_shared::build_name_map(source).unwrap_or_default();
    let mut rows = Vec::new();
    if let Some(snapshot) = &snapshot {
        for (code, checkpoint) in &snapshot.stocks {
            for (id, state) in &checkpoint.states {
                let Some(definition) = cfg.track.iter().find(|track| {
                    track.id == *id
                        && track.enabled
                        && track.source.as_ref().is_none_or(|source| {
                            cfg.rule.iter().any(|rule| {
                                rule.id.as_ref() == Some(source)
                                    && rule.enabled
                                    && cfg
                                        .scene
                                        .iter()
                                        .any(|scene| scene.name == rule.scene_name && scene.enabled)
                            })
                        })
                }) else {
                    continue;
                };
                let Some(event) = &state.event else {
                    continue;
                };
                rows.push(TrackingRow {
                    track_id: id.clone(),
                    ts_code: code.clone(),
                    name: names.get(code).cloned().unwrap_or_default(),
                    started: snapshot.calendar[event.started].clone(),
                    age: snapshot.calendar.len() - 1 - event.started,
                    observing: snapshot.calendar.len() - 1 - event.started <= definition.ttl,
                    captured: event.captured.clone(),
                    source_score: event.source_score,
                    completed: event
                        .completed
                        .iter()
                        .map(|index| snapshot.calendar[*index].clone())
                        .collect(),
                    stopped: event.stopped.map(|index| snapshot.calendar[index].clone()),
                    reason: event.reason.clone(),
                    first_signal: event
                        .first_signal
                        .map(|index| snapshot.calendar[index].clone()),
                    signal_today: event.signal_today,
                    eligible_today: event.eligible_today,
                    score: if event.stopped.is_none() {
                        event.score
                    } else {
                        0.0
                    },
                });
            }
        }
    }
    rows.sort_by(|a, b| {
        b.signal_today
            .cmp(&a.signal_today)
            .then(a.stopped.is_some().cmp(&b.stopped.is_some()))
            .then(a.track_id.cmp(&b.track_id))
            .then(a.ts_code.cmp(&b.ts_code))
    });
    let stamp = input_stamp(source)?;
    Ok(TrackingPage {
        sources: cfg
            .rule
            .iter()
            .map(|rule| TrackingSource {
                id: rule.id.clone().unwrap_or_else(|| rule.name.clone()),
                name: rule.name.clone(),
                enabled: rule.enabled
                    && cfg
                        .scene
                        .iter()
                        .any(|scene| scene.name == rule.scene_name && scene.enabled),
            })
            .collect(),
        definitions: cfg.track,
        as_of: snapshot.as_ref().map(|s| s.as_of.clone()),
        stale: snapshot
            .as_ref()
            .is_none_or(|s| s.config != config || s.stamp != stamp),
        computed_symbols: snapshot.as_ref().map_or(0, |s| s.stocks.len()),
        resumed_symbols: snapshot.as_ref().map_or(0, |s| s.resumed_symbols),
        warnings: snapshot.map_or_else(Vec::new, |s| s.warnings),
        rows,
    })
}

pub fn save_strategy_track(
    source: &str,
    mut draft: TrackDefinition,
) -> Result<TrackingPage, String> {
    let _run = TRACKING_RUN.lock().map_err(|e| e.to_string())?;
    let mut file = super::manage::load_rule_file(source)?;
    let seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| e.to_string())?
        .as_nanos();
    let mut ids: HashSet<_> = file
        .rule
        .iter()
        .filter_map(|rule| rule.id.clone())
        .collect();
    for (index, rule) in file.rule.iter_mut().enumerate() {
        if rule.id.is_none() {
            let mut id = format!("rule_{seed:x}_{index}");
            while ids.contains(&id) {
                id.push('_');
            }
            ids.insert(id.clone());
            rule.id = Some(id);
        }
    }
    if let Some(source) = &draft.source {
        let rule = file
            .rule
            .iter()
            .find(|rule| rule.id.as_ref() == Some(source))
            .or_else(|| file.rule.iter().find(|rule| rule.name == *source))
            .ok_or("原规则不存在")?;
        draft.source = rule.id.clone();
    }
    TrackProgram::compile(&draft)?;
    if let Some(index) = file.track.iter().position(|track| track.id == draft.id) {
        file.track[index] = draft;
    } else {
        file.track.push(draft);
    }
    super::manage::save_rule_file(source, &file)?;
    get_strategy_tracking_page(source)
}

pub fn remove_strategy_track(source: &str, id: &str) -> Result<TrackingPage, String> {
    let _run = TRACKING_RUN.lock().map_err(|e| e.to_string())?;
    let mut file = super::manage::load_rule_file(source)?;
    let before = file.track.len();
    file.track.retain(|track| track.id != id);
    if before == file.track.len() {
        return Err("跟踪策略不存在".into());
    }
    super::manage::save_rule_file(source, &file)?;
    get_strategy_tracking_page(source)
}

pub fn run_strategy_tracking(
    source: &str,
    reference: Option<&str>,
) -> Result<TrackingPage, String> {
    let _run = TRACKING_RUN.lock().map_err(|e| e.to_string())?;
    let stamp = input_stamp(source)?;
    let config = fs::read_to_string(data::score_rule_path(source)).map_err(|e| e.to_string())?;
    let cfg = ScoreConfig::load(source)?;
    let tracks: Vec<_> = cfg
        .track
        .iter()
        .filter(|track| {
            track.enabled
                && track.source.as_ref().is_none_or(|id| {
                    cfg.rule.iter().any(|rule| {
                        rule.id.as_ref() == Some(id)
                            && rule.enabled
                            && cfg
                                .scene
                                .iter()
                                .any(|scene| scene.name == rule.scene_name && scene.enabled)
                    })
                })
        })
        .collect();
    if tracks.is_empty() {
        return Err("请先启用至少一个跟踪策略".into());
    }
    data::download_state::ensure_stock_data_ready_for_derived(source)?;
    let conn = data::open_db_readonly(data::source_db_path(source)).map_err(|e| e.to_string())?;
    let latest: Option<String> = conn
        .query_row(
            "SELECT MAX(trade_date) FROM stock_data WHERE adj_type='qfq'",
            [],
            |row| row.get(0),
        )
        .map_err(|e| e.to_string())?;
    drop(conn);
    let latest = latest.ok_or("没有前复权行情")?;
    let reference = match reference.filter(|value| !value.trim().is_empty()) {
        Some(date) => {
            lianghua_app_shared::normalize_trade_date(date).ok_or("日期须为YYYY-MM-DD或YYYYMMDD")?
        }
        None => latest.clone(),
    };
    if reference > latest {
        return Err(format!("参考日超出行情末日 {latest}"));
    }
    let mut calendar = data::load_trade_date_list(source)?;
    calendar.retain(|date| date <= &reference);
    calendar.sort();
    calendar.dedup();
    let as_of = calendar.last().cloned().ok_or("参考日前没有市场交易日")?;
    let previous = load_snapshot(source)?;
    if previous
        .as_ref()
        .is_some_and(|s| s.stamp == stamp && s.config == config && s.as_of == as_of)
    {
        return get_strategy_tracking_page(source);
    }
    let programs = tracks
        .iter()
        .map(|track| TrackProgram::compile(track))
        .collect::<Result<Vec<_>, _>>()?;
    let source_ids: HashSet<_> = tracks
        .iter()
        .filter_map(|track| track.source.as_deref())
        .collect();
    let source_names: BTreeMap<_, _> = cfg
        .rule
        .iter()
        .filter(|rule| rule.id.as_deref().is_some_and(|id| source_ids.contains(id)))
        .map(|rule| (rule.name.as_str(), rule.id.as_ref().unwrap().as_str()))
        .collect();
    let mut rules = scoring::rule_cache::cache_rule_build(source, None)?;
    rules.retain(|rule| source_names.contains_key(rule.name.as_str()));
    let expressions: Vec<_> = programs
        .iter()
        .flat_map(TrackProgram::expressions)
        .map(TrackExpression::program)
        .chain(rules.iter().flat_map(|rule| rule.expression_programs()))
        .collect();
    let chip_keys = scoring::tools::collect_used_cyq_chen_runtime_keys(&expressions);
    scoring::runner::ensure_expression_dependencies_ready(
        source,
        &expressions,
        &chip_keys,
        &calendar[0],
        &as_of,
        0,
        None,
        "qfq",
    )?;
    let mut injected = scoring::tools::cyq_chen_runtime_key_names()
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>();
    injected.extend(["ZHANG", "TOTAL_MV_YI", "S_RANK"].map(str::to_string));
    injected.extend(
        programs
            .iter()
            .flat_map(|program| program.context.iter().cloned()),
    );
    let injected_refs = injected.iter().map(String::as_str).collect::<Vec<_>>();
    let required = data::collect_runtime_keys_from_expr_programs(
        &expressions,
        RuntimeKeyCollectOptions {
            always_keys: &["C"],
            injected_keys: &injected_refs,
            aliases: &[],
        },
    );
    let reader = DataReader::new_with_runtime_keys(source, &required)?;
    let symbols = reader.list_ts_code("qfq", "00000000", &as_of)?;
    let chips = scoring::tools::CyqChenFieldInjector::new(source, &chip_keys);
    let similarity = scoring::tools::SimilarityRankFieldInjector::new(
        source,
        expressions.iter().any(|expr| {
            crate::expr::validation::expression_input_identifiers(expr).contains("S_RANK")
        }),
    );
    let st_list = scoring::tools::load_st_list(source)?;
    let mut warnings = HashSet::new();
    let mut stocks = BTreeMap::new();
    let mut resumed_symbols = 0;
    let reusable = previous
        .as_ref()
        .filter(|s| s.config == config && s.as_of <= as_of && calendar.starts_with(&s.calendar));
    for code in symbols {
        let mut row = reader.load_one(&code, "qfq", "00000000", &as_of)?;
        scoring::tools::inject_stock_extra_fields(&mut row, &code, st_list.contains(&code), None)?;
        warnings.extend(chips.inject(&mut row, &code));
        similarity.inject(&mut row, &code)?;
        let dates = row.trade_dates.clone();
        let mut rt = data::runtime::row_into_rt(row)?;
        for value in rt.vars.values_mut() {
            if let Value::NumSeries(series) = value {
                *value = Value::SharedNumSeries(Arc::new(std::mem::take(series)));
            }
        }
        let mut scores = BTreeMap::new();
        for rule in &rules {
            let (values, _) = scoring::evaluate_cached_rule_scores(rule, &mut rt)?;
            scores.insert(source_names[rule.name.as_str()].to_string(), values);
        }
        let signature = |limit: &str, sessions: &[String]| {
            let mut hash = DefaultHasher::new();
            sessions.hash(&mut hash);
            let count = dates.partition_point(|date| date.as_str() <= limit);
            dates[..count].hash(&mut hash);
            let mut keys = rt.vars.keys().collect::<Vec<_>>();
            keys.sort();
            for key in keys {
                key.hash(&mut hash);
                for day in 0..count {
                    value_at(&rt.vars[key], day)
                        .map(f64::to_bits)
                        .hash(&mut hash);
                }
            }
            for (id, values) in &scores {
                id.hash(&mut hash);
                for value in values.iter().take(count) {
                    value.to_bits().hash(&mut hash);
                }
            }
            hash.finish()
        };
        let checkpoint = reusable.and_then(|s| {
            s.stocks
                .get(&code)
                .filter(|old| old.signature == signature(&s.as_of, &s.calendar))
                .map(|old| (s.calendar.len(), old.states.clone()))
        });
        let (from, mut states) = checkpoint.unwrap_or_default();
        if from > 0 {
            resumed_symbols += 1;
        }
        for (definition, program) in tracks.iter().zip(&programs) {
            let state = states.entry(definition.id.clone()).or_default();
            let prepared = program.prepare(&rt)?;
            let mut index =
                dates.partition_point(|date| calendar.get(from).is_some_and(|start| date < start));
            for (session, date) in calendar.iter().enumerate().skip(from) {
                while index < dates.len() && dates[index] < *date {
                    index += 1;
                }
                let quoted_index = (dates.get(index) == Some(date)).then_some(index);
                let source_score = if state
                    .event
                    .as_ref()
                    .is_none_or(|event| definition.expired(event, session))
                {
                    match (definition.source.as_deref(), quoted_index) {
                        (Some(id), Some(index)) => Some(scores[id][index]),
                        _ => None,
                    }
                } else {
                    None
                };
                prepared.advance(state, definition, session, quoted_index, &rt, source_score)?;
            }
        }
        stocks.insert(
            code,
            TrackingCheckpoint {
                signature: signature(&as_of, &calendar),
                states,
            },
        );
    }
    if stamp != input_stamp(source)? {
        return Err("计算期间行情或配置发生变化，本次跟踪结果未保存，请重新计算".into());
    }
    let mut warnings = warnings.into_iter().collect::<Vec<_>>();
    warnings.sort();
    let snapshot = TrackingSnapshot {
        version: TRACKING_VERSION,
        computed_at: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| e.to_string())?
            .as_nanos(),
        config,
        stamp,
        as_of,
        calendar,
        resumed_symbols,
        warnings,
        stocks,
    };
    let path = Path::new(source).join("strategy_tracking.json");
    let temp = path.with_extension("json.tmp");
    fs::write(
        &temp,
        serde_json::to_vec(&snapshot).map_err(|e| e.to_string())?,
    )
    .map_err(|e| e.to_string())?;
    fs::rename(&temp, &path).map_err(|e| e.to_string())?;
    get_strategy_tracking_page(source)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn real_source_scores_replay_resume_revisions_and_rule_identity() {
        let source = std::env::temp_dir().join(format!(
            "lianghua_tracking_{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&source).unwrap();
        let dir = source.to_str().unwrap();
        fs::write(
            data::score_rule_path(dir),
            r#"
version = 1
[[scene]]
name = "测试"
direction = "long"
observe_threshold = 1
trigger_threshold = 2
confirm_threshold = 3
fail_threshold = 1
[[rule]]
name = "组合起点"
scene = "测试"
kind = "combination"
stage = "base"
scope_windows = 1
scope_way = "LAST"
points_by_hits = [0.0, 6.0, 6.0]
explain = "fixture"
[[rule.condition]]
name = "发现"
when = "C < O"
[[rule.condition]]
name = "额外证据"
when = "C < O AND V >= 100"
bonus_points = 1.0
"#,
        )
        .unwrap();
        fs::write(
            data::stock_list_path(dir),
            "ts_code,symbol,name\n000001.SZ,000001,测试股\n000002.SZ,000002,参照股\n",
        )
        .unwrap();
        let dates = [
            "20240102", "20240103", "20240104", "20240105", "20240108", "20240109", "20240110",
            "20240111", "20240112", "20240115", "20240116", "20240117", "20240118",
        ];
        fs::write(data::trade_calendar_path(dir), dates.join("\n")).unwrap();
        let conn = duckdb::Connection::open(data::source_db_path(dir)).unwrap();
        conn.execute_batch("CREATE TABLE stock_data(ts_code VARCHAR, trade_date VARCHAR, adj_type VARCHAR, open DOUBLE, close DOUBLE, low DOUBLE, vol DOUBLE, high DOUBLE DEFAULT 20, pre_close DOUBLE DEFAULT 10, change DOUBLE DEFAULT 0, pct_chg DOUBLE DEFAULT 0, amount DOUBLE DEFAULT 10000, j DOUBLE DEFAULT 1)").unwrap();
        for (i, date) in dates.iter().enumerate() {
            conn.execute(
                "INSERT INTO stock_data(ts_code,trade_date,adj_type,open,close,low,vol) VALUES ('000002.SZ',?,'qfq',19,20,18,500)",
                [date],
            )
            .unwrap();
            if i != 7 {
                let (open, close, volume) = match i {
                    0 => (11.0, 10.0, 100.0),
                    6 => (11.0, 10.0, 90.0),
                    1 => (10.0, 11.0, 50.0),
                    3 => (9.0, 8.0, 100.0),
                    _ => (11.0, 12.0, 100.0),
                };
                conn.execute(
                    "INSERT INTO stock_data(ts_code,trade_date,adj_type,open,close,low,vol) VALUES ('000001.SZ',?,'qfq',?,?,9,?)",
                    duckdb::params![date, open, close, volume],
                )
                .unwrap();
            }
        }
        drop(conn);
        let definition = TrackDefinition {
            id: "carry".into(),
            name: "先缩量后反弹".into(),
            enabled: true,
            source: Some("组合起点".into()),
            start: None,
            ttl: 5,
            capture: BTreeMap::from([
                ("A_LOW".into(), "L".into()),
                ("A_CLOSE".into(), "C".into()),
                ("A_VOL".into(), "V".into()),
            ]),
            invalidate: Some("C < A_LOW".into()),
            eligible: None,
            steps: vec![lianghua_core::tracking::TrackStep {
                name: "缩量".into(),
                when: "V < A_VOL".into(),
                points: 2.0,
            }],
            signal: Some("C > A_CLOSE".into()),
            points: 3.0,
            score_until: None,
        };
        let page = save_strategy_track(dir, definition).unwrap();
        let id = page.definitions[0].source.clone().unwrap();
        assert_ne!(id, "组合起点");
        let config = fs::read(data::score_rule_path(dir)).unwrap();
        let page = run_strategy_tracking(dir, Some("2024-01-03")).unwrap();
        assert_eq!(page.rows[0].source_score, Some(7.0));
        assert_eq!(page.rows[0].completed, vec!["20240103"]);
        assert!(!page.rows[0].signal_today);
        assert_eq!(page.rows[0].score, 2.0);
        let page = run_strategy_tracking(dir, Some("20240104")).unwrap();
        assert_eq!(page.resumed_symbols, 2);
        assert!(page.rows[0].signal_today);
        assert_eq!(page.rows[0].score, 5.0);
        let page = run_strategy_tracking(dir, Some("20240105")).unwrap();
        assert_eq!(page.rows[0].reason.as_deref(), Some("invalidated"));
        let continued = load_snapshot(dir).unwrap().unwrap().stocks;
        fs::remove_file(source.join("strategy_tracking.json")).unwrap();
        run_strategy_tracking(dir, Some("20240105")).unwrap();
        let replayed = load_snapshot(dir).unwrap().unwrap().stocks;
        for (code, checkpoint) in &continued {
            assert_eq!(checkpoint.states, replayed[code].states);
            assert_eq!(checkpoint.signature, replayed[code].signature);
        }
        // Revising an old anchor invalidates that stock's checkpoint, even at the same reference date.
        let conn = duckdb::Connection::open(data::source_db_path(dir)).unwrap();
        conn.execute(
            "UPDATE stock_data SET low=7 WHERE ts_code='000001.SZ' AND trade_date='20240102'",
            [],
        )
        .unwrap();
        drop(conn);
        assert!(get_strategy_tracking_page(dir).unwrap().stale);
        let page = run_strategy_tracking(dir, Some("20240105")).unwrap();
        assert_eq!(page.resumed_symbols, 1);
        assert_eq!(page.rows[0].captured["A_LOW"], 7.0);
        assert!(page.rows[0].stopped.is_none());
        let page = run_strategy_tracking(dir, Some("20240112")).unwrap();
        assert_eq!(page.rows[0].started, "20240110");
        assert_eq!(page.rows[0].source_score, Some(6.0));
        assert_eq!(page.rows[0].reason.as_deref(), Some("missing_quote"));
        assert!(!page.rows[0].signal_today);
        assert!(run_strategy_tracking(dir, None).unwrap().rows.is_empty());
        assert_eq!(fs::read(data::score_rule_path(dir)).unwrap(), config);
        assert!(!data::result_db_path(dir).exists());
        // Ranking shares source scores, persists states atomically, and resumes only matching prefixes.
        scoring::runner::scoring_all_to_db(dir, None, "qfq", "20240102", "20240103", None).unwrap();
        scoring::runner::scoring_all_to_db(dir, None, "qfq", "20240104", "20240104", None).unwrap();
        let ranked = get_strategy_tracking_page(dir).unwrap();
        assert_eq!(ranked.as_of.as_deref(), Some("20240104"));
        assert!(!ranked.stale);
        assert_eq!(ranked.resumed_symbols, 2);
        assert_eq!(ranked.rows[0].score, 5.0);
        let result = data::open_db_readonly(data::result_db_path(dir)).unwrap();
        let score: f64 = result.query_row("SELECT total_score FROM score_summary WHERE ts_code='000001.SZ' AND trade_date='20240104'", [], |row| row.get(0)).unwrap();
        assert_eq!(score, 55.0);
        let previous = load_snapshot(dir).unwrap().unwrap().stocks;
        drop(result);
        scoring::runner::scoring_all_to_db(dir, None, "qfq", "20240102", "20240104", None).unwrap();
        let full = load_snapshot(dir).unwrap().unwrap().stocks;
        for (code, checkpoint) in &previous {
            assert_eq!(checkpoint.states, full[code].states);
        }
        let conn = duckdb::Connection::open(data::source_db_path(dir)).unwrap();
        conn.execute(
            "UPDATE stock_data SET low=6 WHERE ts_code='000001.SZ' AND trade_date='20240102'",
            [],
        )
        .unwrap();
        drop(conn);
        scoring::runner::scoring_all_to_db(dir, None, "qfq", "20240105", "20240105", None).unwrap();
        let revised = get_strategy_tracking_page(dir).unwrap();
        assert_eq!(revised.resumed_symbols, 1);
        assert_eq!(revised.rows[0].captured["A_LOW"], 6.0);

        let managed = super::super::manage::get_strategy_manage_page(dir).unwrap();
        let mut draft: super::super::manage::StrategyManageRuleDraft =
            serde_json::from_value(serde_json::to_value(&managed.rules[0]).unwrap()).unwrap();
        draft.name = "改名后".into();
        super::super::manage::update_strategy_manage_rule(dir, "组合起点", draft).unwrap();
        let cfg = ScoreConfig::load(dir).unwrap();
        assert_eq!(cfg.rule[0].id.as_ref(), Some(&id));
        assert_eq!(cfg.track[0].source.as_ref(), Some(&id));
        // Ordinary rule editing owns its follow-up settings and commits both together.
        let managed = super::super::manage::get_strategy_manage_page(dir).unwrap();
        assert_eq!(managed.rules[0].tracking, cfg.track);
        let mut draft: super::super::manage::StrategyManageRuleDraft =
            serde_json::from_value(serde_json::to_value(&managed.rules[0]).unwrap()).unwrap();
        draft.name = "新规则带跟踪".into();
        let mut attached = cfg.track[0].clone();
        attached.id = "attached".into();
        attached.source = Some("self".into());
        draft.tracking = Some(vec![attached]);
        let page = super::super::manage::create_strategy_manage_rule(dir, draft.clone()).unwrap();
        let saved = &page.rules[1].tracking[0];
        assert_ne!(saved.source.as_ref(), Some(&id));
        assert_eq!(saved.source, ScoreConfig::load(dir).unwrap().rule[1].id);
        let bytes = fs::read(data::score_rule_path(dir)).unwrap();
        draft.tracking.as_mut().unwrap()[0].ttl = 0;
        assert!(
            super::super::manage::update_strategy_manage_rule(dir, "新规则带跟踪", draft.clone())
                .is_err()
        );
        assert_eq!(fs::read(data::score_rule_path(dir)).unwrap(), bytes);
        draft.tracking = None;
        let page =
            super::super::manage::update_strategy_manage_rule(dir, "新规则带跟踪", draft).unwrap();
        assert_eq!(page.rules[1].tracking[0].id, "attached");
        let mut draft: super::super::manage::StrategyManageRuleDraft =
            serde_json::from_value(serde_json::to_value(&page.rules[1]).unwrap()).unwrap();
        draft.tracking.as_mut().unwrap()[0].enabled = false;
        let page =
            super::super::manage::update_strategy_manage_rule(dir, "新规则带跟踪", draft.clone())
                .unwrap();
        assert!(!page.rules[1].tracking[0].enabled);
        draft.tracking = Some(Vec::new());
        let page =
            super::super::manage::update_strategy_manage_rule(dir, "新规则带跟踪", draft).unwrap();
        assert!(page.rules[1].tracking.is_empty());
        let page =
            super::super::manage::remove_strategy_manage_rules(dir, &["改名后".into()]).unwrap();
        assert_eq!(page.rules.len(), 1);
        assert!(ScoreConfig::load(dir).unwrap().track.is_empty());
        fs::remove_dir_all(source).unwrap();
    }
}
