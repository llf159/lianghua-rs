use std::{
    collections::{BTreeMap, HashMap, HashSet, hash_map::DefaultHasher},
    fs,
    hash::{Hash, Hasher},
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use lianghua_core::{
    expr::eval::Runtime,
    tracking::{TrackDefinition, TrackExpression, TrackProgram, TrackState, value_at},
};
use lianghua_model::scoring::TrackingScoreState;
use serde::{Deserialize, Serialize};

use super::{CachedRule, RuleScoreSeries, tools::cyq_chen_runtime_key_names};
use crate::data::{self, RuntimeKeyCollectOptions, ScoreConfig};

pub fn config_hash(config: &str) -> String {
    let mut hash = DefaultHasher::new();
    // Invalidate checkpoints when scoring/event semantics change.
    4u32.hash(&mut hash);
    config.hash(&mut hash);
    hash.finish().to_string()
}
pub fn input_stamp(source: &str) -> Result<u64, String> {
    let mut hash = DefaultHasher::new();
    for file in [
        "stock_data.db",
        "stock_data.db.wal",
        "cyq_chen.db",
        "cyq_chen.db.wal",
        "strategy_similarity.db",
        "strategy_similarity.db.wal",
        "stock_list.csv",
        "trade_calendar.csv",
        "score_rule.toml",
    ] {
        file.hash(&mut hash);
        match fs::metadata(Path::new(source).join(file)) {
            Ok(meta) => {
                meta.len().hash(&mut hash);
                meta.modified()
                    .map_err(|e| e.to_string())?
                    .duration_since(UNIX_EPOCH)
                    .map_err(|e| e.to_string())?
                    .as_nanos()
                    .hash(&mut hash);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0u64.hash(&mut hash),
            Err(error) => return Err(error.to_string()),
        }
    }
    Ok(hash.finish())
}

#[derive(Serialize, Deserialize)]
pub struct RankingTrackCheckpoint {
    pub computed_at: u128,
    pub resumed: bool,
    pub signature: u64,
    pub states: BTreeMap<String, TrackState>,
}

pub struct ScoringTrackingPlan {
    tracks: Vec<(TrackDefinition, TrackProgram, Option<usize>)>,
    sources: HashSet<usize>,
    pub(crate) calendar: Vec<String>,
    config_hash: String,
    stamp: String,
    computed_at: u128,
    previous: HashMap<String, (String, RankingTrackCheckpoint)>,
    config_path: std::path::PathBuf,
}

impl ScoringTrackingPlan {
    pub fn load(
        source: &str,
        strategy_path: Option<&str>,
        rules: &[CachedRule],
        end: &str,
    ) -> Result<Self, String> {
        let cfg = ScoreConfig::load_with_strategy_path(source, strategy_path)?;
        let config = fs::read_to_string(data::resolve_strategy_path(source, strategy_path))
            .map_err(|e| e.to_string())?;
        let mut tracks = Vec::new();
        let mut sources = HashSet::new();
        for definition in cfg.track.into_iter().filter(|track| track.enabled) {
            let source_index = if let Some(id) = &definition.source {
                let rule = cfg
                    .rule
                    .iter()
                    .find(|rule| rule.id.as_ref() == Some(id))
                    .ok_or("跟踪原规则不存在")?;
                let Some(index) = rules.iter().position(|cached| cached.name == rule.name) else {
                    continue;
                };
                sources.insert(index);
                Some(index)
            } else {
                if definition.points != 0.0
                    || definition.steps.iter().any(|step| step.points != 0.0)
                {
                    return Err(format!(
                        "跟踪 {} 计分需要关联原规则，以确定场景与评分阶段",
                        definition.name
                    ));
                }
                None
            };
            let program = TrackProgram::compile(&definition)?;
            tracks.push((definition, program, source_index));
        }
        let mut calendar = data::load_trade_date_list(source)?;
        calendar.retain(|date| date.as_str() <= end);
        calendar.sort();
        calendar.dedup();
        let config_hash = config_hash(&config);
        let mut previous = HashMap::new();
        if !tracks.is_empty() && data::result_db_path(source).exists() {
            let conn =
                data::open_db_readonly(data::result_db_path(source)).map_err(|e| e.to_string())?;
            let exists: bool = conn.query_row("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name='strategy_tracking_state')", [], |row| row.get(0)).map_err(|e| e.to_string())?;
            if exists {
                let mut stmt = conn.prepare("SELECT ts_code, trade_date, state_json FROM strategy_tracking_state WHERE config_hash=?").map_err(|e| e.to_string())?;
                let rows = stmt
                    .query_map([&config_hash], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                        ))
                    })
                    .map_err(|e| e.to_string())?;
                for row in rows {
                    let (code, date, state) = row.map_err(|e| e.to_string())?;
                    // An unusable checkpoint is an optimization miss, never a scoring failure.
                    if let Ok(state) = serde_json::from_str(&state) {
                        previous.insert(code, (date, state));
                    }
                }
            }
        }
        Ok(Self {
            config_path: data::resolve_strategy_path(source, strategy_path),
            tracks,
            sources,
            calendar,
            config_hash,
            stamp: input_stamp(source)?.to_string(),
            previous,
            computed_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|e| e.to_string())?
                .as_nanos(),
        })
    }

    pub fn ensure_unchanged(&self, source: &str) -> Result<(), String> {
        if self.is_empty() {
            return Ok(());
        }
        let config = fs::read_to_string(&self.config_path).map_err(|e| e.to_string())?;
        if self.stamp != input_stamp(source)?.to_string()
            || self.config_hash != config_hash(&config)
        {
            return Err("计算期间行情或配置发生变化，排名和跟踪结果均未提交，请重新计算".into());
        }
        Ok(())
    }

    pub fn refresh_stamp_after_maintenance(&self, source: &str) -> Result<(), String> {
        if self.is_empty() {
            return Ok(());
        }
        let stamp = input_stamp(source)?.to_string();
        if stamp != self.stamp {
            let conn = duckdb::Connection::open(data::result_db_path(source))
                .map_err(|e| e.to_string())?;
            conn.execute("UPDATE strategy_tracking_state SET input_stamp=? WHERE input_stamp=? AND config_hash=?", duckdb::params![stamp, self.stamp, self.config_hash])
                .map_err(|e| format!("更新跟踪维护水位失败:{e}"))?;
        }
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.tracks.is_empty()
    }

    pub fn expressions(&self) -> Vec<&crate::expr::parser::Stmts> {
        self.tracks
            .iter()
            .flat_map(|(_, program, _)| program.expressions())
            .map(TrackExpression::program)
            .collect()
    }

    pub fn runtime_keys(&self) -> HashSet<String> {
        let mut injected = cyq_chen_runtime_key_names()
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>();
        injected.extend(["ZHANG", "TOTAL_MV_YI", "S_RANK"].map(str::to_string));
        injected.extend(
            self.tracks
                .iter()
                .flat_map(|(_, program, _)| program.context.iter().cloned()),
        );
        data::collect_runtime_keys_from_expr_programs(
            &self.expressions(),
            RuntimeKeyCollectOptions {
                always_keys: if self.is_empty() { &[] } else { &["C"] },
                injected_keys: &injected.iter().map(String::as_str).collect::<Vec<_>>(),
                aliases: &[],
            },
        )
    }

    pub fn apply(
        &self,
        rt: &Runtime,
        dates: &[String],
        start: &str,
        code: &str,
        total: &mut [f64],
        details: &mut [RuleScoreSeries],
    ) -> Result<Vec<TrackingScoreState>, String> {
        if self.is_empty() {
            return Ok(Vec::new());
        }
        let Some(as_of) = self.calendar.last() else {
            return Ok(Vec::new());
        };
        // All source scores are frozen before adding any follow-up points (no self-feedback).
        let sources = self
            .sources
            .iter()
            .map(|&index| (index, details[index].series.clone()))
            .collect::<HashMap<_, _>>();
        let signature = |limit: &str, sessions: usize| {
            let mut hash = DefaultHasher::new();
            self.calendar[..sessions].hash(&mut hash);
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
            let mut source_indices = sources.keys().collect::<Vec<_>>();
            source_indices.sort();
            for index in source_indices {
                index.hash(&mut hash);
                for value in sources[index].iter().take(count) {
                    value.to_bits().hash(&mut hash);
                }
            }
            hash.finish()
        };
        let checkpoint = self.previous.get(code).and_then(|(date, old)| {
            if date.as_str() >= start {
                return None;
            }
            let session = self.calendar.binary_search(date).ok()?;
            (old.signature == signature(date, session + 1))
                .then_some((session + 1, old.states.clone()))
        });
        let (from, mut states) = checkpoint.unwrap_or_default();
        for (definition, program, source) in &self.tracks {
            let prepared = program.prepare(rt)?;
            let state = states.entry(definition.id.clone()).or_default();
            let mut index = dates
                .partition_point(|date| self.calendar.get(from).is_some_and(|start| date < start));
            for (session, date) in self.calendar.iter().enumerate().skip(from) {
                while index < dates.len() && dates[index] < *date {
                    index += 1;
                }
                let quoted_index = (dates.get(index) == Some(date)).then_some(index);
                let source_score = if state
                    .event
                    .as_ref()
                    .is_none_or(|event| definition.expired(event, session))
                {
                    match (source, quoted_index) {
                        (Some(source), Some(index)) => Some(sources[source][index]),
                        _ => None,
                    }
                } else {
                    None
                };
                let score =
                    prepared.advance(state, definition, session, quoted_index, rt, source_score)?;
                if let (Some(index), Some(source)) = (quoted_index, source) {
                    total[index] += score;
                    details[*source].series[index] += score;
                    details[*source].triggered[index] |= score != 0.0;
                }
            }
        }
        let checkpoint = RankingTrackCheckpoint {
            computed_at: self.computed_at,
            resumed: from > 0,
            signature: signature(as_of, self.calendar.len()),
            states,
        };
        Ok(vec![TrackingScoreState {
            ts_code: code.into(),
            trade_date: as_of.clone(),
            config_hash: self.config_hash.clone(),
            input_stamp: self.stamp.clone(),
            state_json: serde_json::to_string(&checkpoint).map_err(|e| e.to_string())?,
        }])
    }
}
