//! Saved research observations. Queries never reopen production inputs.
use crate::research_archive::{read_research_archive_header, research_artifact_path};
use duckdb::{Connection, params};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct ResearchGrouping {
    #[serde(default)]
    pub market_cap_boundaries: Vec<f64>,
    #[serde(default)]
    pub baseline_score_boundaries: Vec<f64>,
    #[serde(default)]
    pub market_metric: String,
    #[serde(default)]
    pub market_boundaries: Vec<f64>,
}

impl ResearchGrouping {
    pub fn validate(&self) -> Result<(), String> {
        for values in [
            &self.market_cap_boundaries,
            &self.market_boundaries,
            &self.baseline_score_boundaries,
        ] {
            if values.len() > 20
                || values.iter().any(|value| !value.is_finite())
                || values.windows(2).any(|pair| pair[0] >= pair[1])
            {
                return Err("分组边界须为递增、有限的数值，最多20个".into());
            }
        }
        if self.market_cap_boundaries.iter().any(|value| *value <= 0.) {
            return Err("市值边界必须大于零".into());
        }
        if !self.market_metric.is_empty()
            && ![
                "MKT_D10", "MKT_F5", "MKT_CP5", "MKT_VA5", "MKT_LO5", "MKT_AC5", "MKT_IS5",
                "MKT_CF20", "MKT_DT5", "MKT_SC5",
            ]
            .contains(&self.market_metric.as_str())
        {
            return Err("只能使用已发布的十项MKT指标".into());
        }
        Ok(())
    }
    pub fn bucket(value: Option<f64>, boundaries: &[f64]) -> String {
        match value.filter(|value| value.is_finite()) {
            None => "缺失".into(),
            Some(_) if boundaries.is_empty() => "未分组".into(),
            Some(value) => format!(
                "组{}",
                boundaries.partition_point(|boundary| value >= *boundary) + 1
            ),
        }
    }
}

pub struct ValidationResearchOutput {
    pub path: PathBuf,
    pub grouping: ResearchGrouping,
    pub statistics: crate::simulate::research_statistics::ResearchStatisticsSettings,
    pub baseline_diagnostics: bool,
}

impl ValidationResearchOutput {
    pub fn open(&self) -> Result<Connection, String> {
        self.grouping.validate()?;
        self.statistics.validate()?;
        let conn = Connection::open(&self.path).map_err(|e| e.to_string())?;
        conn.execute_batch("SET memory_limit='256MB'; SET threads=1;
            CREATE TABLE IF NOT EXISTS observations(ts_code VARCHAR,trade_date VARCHAR,board VARCHAR,market_cap DOUBLE,cap_group VARCHAR,eligibility SMALLINT,quote_present BOOLEAN);
            CREATE TABLE IF NOT EXISTS labels(horizon BIGINT,ts_code VARCHAR,trade_date VARCHAR,entry_date VARCHAR,label_end_date VARCHAR,residual_return DOUBLE);
            CREATE TABLE IF NOT EXISTS signals(combo_key VARCHAR,ts_code VARCHAR,trade_date VARCHAR,score DOUBLE);
            CREATE TABLE IF NOT EXISTS baseline_rule_scores(rule_name VARCHAR,ts_code VARCHAR,trade_date VARCHAR,score DOUBLE);
            CREATE TABLE IF NOT EXISTS baseline_scores(ts_code VARCHAR,trade_date VARCHAR,score DOUBLE,rank BIGINT,score_group VARCHAR);
            CREATE TABLE IF NOT EXISTS calendar(trade_date VARCHAR);
            CREATE TABLE IF NOT EXISTS market(trade_date VARCHAR,metric VARCHAR,value DOUBLE,bucket VARCHAR);
            CREATE TABLE IF NOT EXISTS metadata(key VARCHAR,value VARCHAR);")
            .map_err(|e|e.to_string())?;
        Ok(conn)
    }

    pub fn summarize(
        &self,
        query: &ResearchEvidenceQuery,
    ) -> Result<Vec<ResearchEvidenceSummary>, String> {
        let (conn, sql) = query.sql_on(self.open()?)?;
        summarize_evidence(conn, sql, query, "all")
    }

    pub fn prepare(
        &self,
        source: &str,
        adj: &str,
        start: &str,
        end: &str,
        min_listed: usize,
        codes: &[String],
        snapshot_baseline_scores: bool,
    ) -> Result<(), String> {
        let conn = self.open()?;
        let prepared: i64 = conn
            .query_row(
                "SELECT count(*) FROM metadata WHERE key='prepared'",
                [],
                |r| r.get(0),
            )
            .map_err(|e| e.to_string())?;
        if prepared > 0 {
            return Ok(());
        }
        let source_conn = crate::data::open_db_readonly(crate::data::source_db_path(source))
            .map_err(|e| e.to_string())?;
        let eligibility = crate::simulate::build_backtest_sample_eligibility(source, min_listed)?;
        let mut calendar = crate::data::load_trade_date_list(source)?;
        calendar.sort_unstable();
        calendar.dedup();
        calendar.retain(|day| day.as_str() >= start && day.as_str() <= end);
        {
            let mut writer = conn.appender("calendar").map_err(|e| e.to_string())?;
            for day in &calendar {
                writer.append_row(params![day]).map_err(|e| e.to_string())?;
            }
            writer.flush().map_err(|e| e.to_string())?;
        }
        let cap_sql = crate::data::extras::estimated_market_cap_sql(&source_conn)?;
        use duckdb::arrow::{
            array::{ArrayRef, BooleanArray, Float64Array, Int16Array, Int64Array, StringArray},
            record_batch::RecordBatch,
        };
        use std::sync::Arc;
        let boards = codes
            .iter()
            .map(|code| crate::utils::utils::board_category(code, None))
            .collect::<Vec<_>>();
        let bucket_labels = std::iter::once("缺失".to_string())
            .chain(std::iter::once("未分组".to_string()))
            .chain(
                (1..=self.grouping.market_cap_boundaries.len() + 1)
                    .map(|index| format!("组{index}")),
            )
            .collect::<Vec<_>>();
        type ObservationRow<'a> = (&'a str, &'a str, u32, Option<f64>, u16, i16, bool);
        let mut batch = Vec::<ObservationRow>::with_capacity(8192);
        let mut writer = conn.appender("observations").map_err(|e| e.to_string())?;
        let mut flush = |batch: &mut Vec<ObservationRow>| -> Result<(), String> {
            if batch.is_empty() {
                return Ok(());
            }
            let record = RecordBatch::try_from_iter([
                (
                    "ts_code",
                    Arc::new(StringArray::from_iter_values(batch.iter().map(|row| row.0)))
                        as ArrayRef,
                ),
                (
                    "trade_date",
                    Arc::new(StringArray::from_iter_values(batch.iter().map(|row| row.1)))
                        as ArrayRef,
                ),
                (
                    "board",
                    Arc::new(StringArray::from_iter_values(
                        batch.iter().map(|row| boards[row.2 as usize]),
                    )) as ArrayRef,
                ),
                (
                    "market_cap",
                    Arc::new(Float64Array::from_iter(batch.iter().map(|row| row.3))) as ArrayRef,
                ),
                (
                    "cap_group",
                    Arc::new(StringArray::from_iter_values(
                        batch
                            .iter()
                            .map(|row| bucket_labels[row.4 as usize].as_str()),
                    )) as ArrayRef,
                ),
                (
                    "eligibility",
                    Arc::new(Int16Array::from_iter(batch.iter().map(|row| row.5))) as ArrayRef,
                ),
                (
                    "quote_present",
                    Arc::new(BooleanArray::from_iter(batch.iter().map(|row| row.6))) as ArrayRef,
                ),
            ])
            .map_err(|e| e.to_string())?;
            writer
                .append_record_batch(record)
                .map_err(|e| e.to_string())?;
            batch.clear();
            Ok(())
        };
        for (chunk_index, chunk) in codes.chunks(128).enumerate() {
            let base = chunk_index * 128;
            let placeholders = std::iter::repeat_n("?", chunk.len())
                .collect::<Vec<_>>()
                .join(",");
            let mut stmt=source_conn.prepare(&format!("SELECT ts_code,trade_date,{cap_sql} FROM stock_data WHERE adj_type=? AND trade_date BETWEEN ? AND ? AND ts_code IN ({placeholders})")).map_err(|e|e.to_string())?;
            let mut rows = stmt
                .query(duckdb::params_from_iter(
                    [adj, start, end]
                        .into_iter()
                        .chain(chunk.iter().map(String::as_str)),
                ))
                .map_err(|e| e.to_string())?;
            let mut quotes = HashMap::<String, HashMap<String, Option<f64>>>::new();
            while let Some(row) = rows.next().map_err(|e| e.to_string())? {
                let code: String = row.get(0).map_err(|e| e.to_string())?;
                let day: String = row.get(1).map_err(|e| e.to_string())?;
                let cap: Option<f64> = row.get(2).map_err(|e| e.to_string())?;
                quotes.entry(code).or_default().insert(day, cap);
            }
            for (offset, code) in chunk.iter().enumerate() {
                for day in &calendar {
                    let quote = quotes
                        .get(code.as_str())
                        .and_then(|days| days.get(day.as_str()));
                    let quote_present = quote.is_some();
                    let cap = quote
                        .copied()
                        .flatten()
                        .filter(|value| value.is_finite() && *value > 0.);
                    let status = match eligibility.sample_status(code, day) {
                        Some(true) => 1i16,
                        Some(false) => 0,
                        None => -1,
                    };
                    let bucket = match cap {
                        None => 0u16,
                        Some(_) if self.grouping.market_cap_boundaries.is_empty() => 1,
                        Some(value) => {
                            2 + self
                                .grouping
                                .market_cap_boundaries
                                .partition_point(|boundary| value >= *boundary)
                                as u16
                        }
                    };
                    batch.push((
                        code.as_str(),
                        day.as_str(),
                        (base + offset) as u32,
                        cap,
                        bucket,
                        status,
                        quote_present,
                    ));
                    if batch.len() >= 8192 {
                        flush(&mut batch)?;
                    }
                }
            }
        }
        flush(&mut batch)?;
        drop(flush);
        writer.flush().map_err(|e| e.to_string())?;
        drop(writer);
        let baseline_status = if !snapshot_baseline_scores {
            conn.execute(
                "INSERT INTO metadata VALUES ('baseline_origin', ?)",
                [if self.baseline_diagnostics {
                    "external_isolated_scoring"
                } else {
                    "not_requested"
                }],
            )
            .map_err(|e| e.to_string())?;
            if self.baseline_diagnostics {
                "排名基线采用本次隔离评分，未复用历史评分".into()
            } else {
                "本次未计算完整策略基线；同类对照不控制原榜分数".into()
            }
        } else {
            let configuration = std::fs::read_to_string(crate::data::score_rule_path(source))
                .map_err(|e| e.to_string())?;
            let temporary = self.path.with_extension("baseline.toml");
            std::fs::write(&temporary, &configuration).map_err(|e| e.to_string())?;
            let scored = crate::scoring::runner::scoring_all_to_memory_with_mode(
                source,
                Some(temporary.to_str().ok_or("基线配置路径不是有效UTF-8")?),
                adj,
                start,
                end,
                crate::scoring::runner::ScoringMemoryMode::RuleBacktest,
            );
            let _ = std::fs::remove_file(&temporary);
            let (scored, profile) = scored?;
            log::info!(
                "研究隔离基线：{}股，准备{}ms，计算{}ms，排名及收尾{}ms",
                profile.stock_count,
                profile.prepare_ms,
                profile.compute_and_send_batches_ms,
                profile
                    .total_ms
                    .saturating_sub(profile.prepare_ms)
                    .saturating_sub(profile.compute_and_send_batches_ms),
            );
            let allowed = codes.iter().map(String::as_str).collect::<HashSet<_>>();
            let baseline_bucket_labels = std::iter::once("缺失".to_string())
                .chain(std::iter::once("未分组".to_string()))
                .chain(
                    (1..=self.grouping.baseline_score_boundaries.len() + 1)
                        .map(|index| format!("组{index}")),
                )
                .collect::<Vec<_>>();
            let mut batch = Vec::<(&str, &str, Option<f64>, Option<i64>, u16)>::with_capacity(8192);
            let mut writer = conn
                .appender("baseline_scores")
                .map_err(|e| e.to_string())?;
            let mut flush = |batch: &mut Vec<(&str, &str, Option<f64>, Option<i64>, u16)>| -> Result<(), String> {
                if batch.is_empty() {
                    return Ok(());
                }
                let record = RecordBatch::try_from_iter([
                    (
                        "ts_code",
                        Arc::new(StringArray::from_iter_values(
                            batch.iter().map(|row| row.0),
                        )) as ArrayRef,
                    ),
                    (
                        "trade_date",
                        Arc::new(StringArray::from_iter_values(
                            batch.iter().map(|row| row.1),
                        )) as ArrayRef,
                    ),
                    (
                        "score",
                        Arc::new(Float64Array::from_iter(batch.iter().map(|row| row.2)))
                            as ArrayRef,
                    ),
                    (
                        "rank",
                        Arc::new(Int64Array::from_iter(batch.iter().map(|row| row.3)))
                            as ArrayRef,
                    ),
                    (
                        "score_group",
                        Arc::new(StringArray::from_iter_values(
                            batch
                                .iter()
                                .map(|row| baseline_bucket_labels[row.4 as usize].as_str()),
                        )) as ArrayRef,
                    ),
                ])
                .map_err(|e| e.to_string())?;
                writer
                    .append_record_batch(record)
                    .map_err(|e| e.to_string())?;
                batch.clear();
                Ok(())
            };
            let mut count = 0;
            for row in &scored.summary_rows {
                if !allowed.contains(row.ts_code.as_str()) {
                    continue;
                }
                let score = row.total_score.is_finite().then_some(row.total_score);
                let bucket = match score {
                    None => 0u16,
                    Some(_) if self.grouping.baseline_score_boundaries.is_empty() => 1,
                    Some(value) => {
                        2 + self
                            .grouping
                            .baseline_score_boundaries
                            .partition_point(|boundary| value >= *boundary)
                            as u16
                    }
                };
                batch.push((
                    row.ts_code.as_str(),
                    row.trade_date.as_str(),
                    score,
                    row.rank,
                    bucket,
                ));
                count += 1;
                if batch.len() >= 8192 {
                    flush(&mut batch)?;
                }
            }
            flush(&mut batch)?;
            drop(flush);
            writer.flush().map_err(|e| e.to_string())?;
            drop(writer);
            let mut batch = Vec::<(&str, &str, &str, f64)>::with_capacity(8192);
            let mut writer = conn
                .appender("baseline_rule_scores")
                .map_err(|e| e.to_string())?;
            let mut flush = |batch: &mut Vec<(&str, &str, &str, f64)>| -> Result<(), String> {
                if batch.is_empty() {
                    return Ok(());
                }
                let record = RecordBatch::try_from_iter([
                    (
                        "rule_name",
                        Arc::new(StringArray::from_iter_values(batch.iter().map(|row| row.0)))
                            as ArrayRef,
                    ),
                    (
                        "ts_code",
                        Arc::new(StringArray::from_iter_values(batch.iter().map(|row| row.1)))
                            as ArrayRef,
                    ),
                    (
                        "trade_date",
                        Arc::new(StringArray::from_iter_values(batch.iter().map(|row| row.2)))
                            as ArrayRef,
                    ),
                    (
                        "score",
                        Arc::new(Float64Array::from_iter(batch.iter().map(|row| row.3)))
                            as ArrayRef,
                    ),
                ])
                .map_err(|e| e.to_string())?;
                writer
                    .append_record_batch(record)
                    .map_err(|e| e.to_string())?;
                batch.clear();
                Ok(())
            };
            for hit in &scored.compact_rule_rows {
                let row = scored
                    .summary_rows
                    .get(hit.summary_index as usize)
                    .ok_or("基线分项索引无效")?;
                if !allowed.contains(row.ts_code.as_str()) {
                    continue;
                }
                let name = profile
                    .rule_names
                    .get(hit.rule_id as usize)
                    .ok_or("基线规则索引无效")?;
                if hit.rule_score.is_finite() {
                    batch.push((
                        name.as_str(),
                        row.ts_code.as_str(),
                        row.trade_date.as_str(),
                        hit.rule_score,
                    ));
                    if batch.len() >= 8192 {
                        flush(&mut batch)?;
                    }
                }
            }
            flush(&mut batch)?;
            drop(flush);
            writer.flush().map_err(|e| e.to_string())?;
            drop(writer);
            for (key, value) in [
                ("baseline_origin", "isolated_native_scoring_v1".to_string()),
                ("baseline_configuration", configuration),
                (
                    "baseline_warnings",
                    serde_json::to_string(&profile.warnings).map_err(|e| e.to_string())?,
                ),
            ] {
                conn.execute("INSERT INTO metadata VALUES (?,?)", params![key, value])
                    .map_err(|e| e.to_string())?;
            }
            format!("本次隔离重算基线{count}个股票日，分项与排名共同留档；未认证运行期间输入一致性")
        };
        let names = [
            "MKT_D10", "MKT_F5", "MKT_CP5", "MKT_VA5", "MKT_LO5", "MKT_AC5", "MKT_IS5", "MKT_CF20",
            "MKT_DT5", "MKT_SC5",
        ]
        .map(str::to_string);
        let market_values =
            crate::data::panel::load_values(&source_conn, source, &names, &[], start, end);
        let (values, market_status) = match market_values {
            Ok(values) => (
                values,
                "已按配置签名及dirty_from过滤；仅已发布日期可用".to_string(),
            ),
            Err(error) => (HashMap::new(), format!("MKT不可用：{error}")),
        };
        let sessions = source_conn
            .prepare("SELECT trade_date FROM indicator_panel_sessions")
            .and_then(|mut stmt| {
                stmt.query_map([], |r| r.get::<_, String>(0))?
                    .collect::<Result<HashSet<_>, _>>()
            })
            .unwrap_or_default();
        let mut writer = conn.appender("market").map_err(|e| e.to_string())?;
        let mut coverage = Vec::new();
        for name in names {
            let mut valid_days = 0usize;
            for day in &calendar {
                let value = values
                    .get(&name)
                    .and_then(|stocks| stocks.get(""))
                    .and_then(|days| days.get(day))
                    .copied()
                    .flatten()
                    .filter(|value| value.is_finite() && sessions.contains(day));
                valid_days += usize::from(value.is_some());
                let bucket = if name == self.grouping.market_metric {
                    ResearchGrouping::bucket(value, &self.grouping.market_boundaries)
                } else {
                    ResearchGrouping::bucket(value, &[])
                };
                writer
                    .append_row(params![day, name, value, bucket])
                    .map_err(|e| e.to_string())?;
            }
            coverage.push(format!("{name}: {valid_days}/{}日", calendar.len()));
        }
        writer.flush().map_err(|e| e.to_string())?;
        drop(writer);
        let market_status = format!(
            "{baseline_status}；{market_status}；{}",
            coverage.join("；")
        );
        for (key, value) in [
            ("market_status", market_status),
            (
                "grouping",
                serde_json::to_string(&self.grouping).map_err(|e| e.to_string())?,
            ),
            (
                "statistics",
                serde_json::to_string(&self.statistics).map_err(|e| e.to_string())?,
            ),
            ("prepared", "1".into()),
        ] {
            conn.execute("INSERT INTO metadata VALUES (?,?)", params![key, value])
                .map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    pub fn save_labels(
        &self,
        horizon: usize,
        cache: &crate::simulate::rule::RuleLayerRuntimeCache,
    ) -> Result<(), String> {
        let conn = self.open()?;
        let mut writer = conn.appender("labels").map_err(|e| e.to_string())?;
        cache.visit_label_windows(|code, day, entry, end, value| {
            writer
                .append_row(params![
                    horizon as i64,
                    code,
                    day,
                    (entry > 0).then(|| format!("{entry:08}")),
                    (end > 0).then(|| format!("{end:08}")),
                    value.is_finite().then_some(value)
                ])
                .map_err(|e| e.to_string())
        })?;
        writer.flush().map_err(|e| e.to_string())?;
        drop(writer);
        conn.execute("INSERT INTO metadata SELECT ?, 'complete' WHERE NOT EXISTS (SELECT 1 FROM metadata WHERE key=?)",params![format!("horizon:{horizon}"),format!("horizon:{horizon}")]).map_err(|e|e.to_string())?;
        Ok(())
    }

    pub fn has_signals(&self, combo: &str) -> Result<bool, String> {
        let conn = self.open()?;
        conn.query_row(
            "SELECT count(*)>0 FROM metadata WHERE key=?",
            [format!("combo:{combo}")],
            |row| row.get(0),
        )
        .map_err(|e| e.to_string())
    }

    pub fn load_signals(
        &self,
        combo: &str,
    ) -> Result<Option<HashMap<String, HashMap<String, f64>>>, String> {
        let conn = self.open()?;
        let present: i64 = conn
            .query_row(
                "SELECT count(*) FROM metadata WHERE key=?",
                [format!("combo:{combo}")],
                |r| r.get(0),
            )
            .map_err(|e| e.to_string())?;
        if present == 0 {
            return Ok(None);
        }
        let mut stmt = conn
            .prepare("SELECT ts_code,trade_date,score FROM signals WHERE combo_key=?")
            .map_err(|e| e.to_string())?;
        let mut rows = stmt.query([combo]).map_err(|e| e.to_string())?;
        let mut map = HashMap::<String, HashMap<String, f64>>::new();
        while let Some(row) = rows.next().map_err(|e| e.to_string())? {
            map.entry(row.get(0).map_err(|e| e.to_string())?)
                .or_default()
                .insert(
                    row.get(1).map_err(|e| e.to_string())?,
                    row.get::<_, Option<f64>>(2)
                        .map_err(|e| e.to_string())?
                        .unwrap_or(f64::NAN),
                );
        }
        Ok(Some(map))
    }

    pub fn save_signals(
        &self,
        combo: &str,
        map: &HashMap<String, HashMap<String, f64>>,
    ) -> Result<(), String> {
        let conn = self.open()?;
        let mut writer = conn.appender("signals").map_err(|e| e.to_string())?;
        use duckdb::arrow::{
            array::{ArrayRef, Float64Array, StringArray},
            record_batch::RecordBatch,
        };
        use std::sync::Arc;
        let mut rows = map.iter().flat_map(|(code, days)| {
            days.iter()
                .map(move |(day, score)| (code.as_str(), day.as_str(), *score))
        });
        loop {
            let batch = rows.by_ref().take(8192).collect::<Vec<_>>();
            if batch.is_empty() {
                break;
            }
            let batch = RecordBatch::try_from_iter([
                (
                    "combo_key",
                    Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
                        combo,
                        batch.len(),
                    ))) as ArrayRef,
                ),
                (
                    "ts_code",
                    Arc::new(StringArray::from_iter_values(batch.iter().map(|row| row.0)))
                        as ArrayRef,
                ),
                (
                    "trade_date",
                    Arc::new(StringArray::from_iter_values(batch.iter().map(|row| row.1)))
                        as ArrayRef,
                ),
                (
                    "score",
                    Arc::new(Float64Array::from_iter(
                        batch.iter().map(|row| row.2.is_finite().then_some(row.2)),
                    )) as ArrayRef,
                ),
            ])
            .map_err(|e| e.to_string())?;
            writer
                .append_record_batch(batch)
                .map_err(|e| e.to_string())?;
        }
        writer.flush().map_err(|e| e.to_string())?;
        drop(writer);
        conn.execute(
            "INSERT INTO metadata VALUES (?, 'complete')",
            [format!("combo:{combo}")],
        )
        .map_err(|e| e.to_string())?;
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ResearchEvidenceQuery {
    pub run_id: String,
    pub combo_key: String,
    pub horizon: usize,
    pub start_date: String,
    pub end_date: String,
    #[serde(default)]
    pub dimension: String,
    #[serde(default)]
    pub bucket: Option<String>,
    #[serde(default)]
    pub state: String,
    #[serde(default)]
    pub offset: usize,
    pub limit: usize,
}

impl ResearchEvidenceQuery {
    pub fn sql(&self, source: &str) -> Result<(Connection, String), String> {
        if self.limit == 0
            || self.limit > 200
            || self.offset > i64::MAX as usize
            || self.horizon == 0
            || self.start_date > self.end_date
            || chrono::NaiveDate::parse_from_str(&self.start_date, "%Y%m%d").is_err()
            || chrono::NaiveDate::parse_from_str(&self.end_date, "%Y%m%d").is_err()
        {
            return Err("研究筛选参数无效".into());
        }
        let header = read_research_archive_header(source, &self.run_id)?;
        if header.status != "completed" || header.kind != "expression_validation" {
            return Err("此运行没有完整表达式明细".into());
        }
        let path = research_artifact_path(source, &self.run_id, "evidence.db")?;
        if !path.is_file() {
            return Err("旧运行未保存全部样本，请重新运行".into());
        }
        let conn = crate::data::open_db_readonly(path).map_err(|e| e.to_string())?;
        self.sql_on(conn)
    }

    fn sql_on(&self, conn: Connection) -> Result<(Connection, String), String> {
        let combo_known: bool = conn
            .query_row(
                "SELECT count(*)>0 FROM metadata WHERE key=?",
                [format!("combo:{}", self.combo_key)],
                |row| row.get(0),
            )
            .map_err(|e| e.to_string())?;
        let horizon_known:bool=conn.query_row("SELECT EXISTS(SELECT 1 FROM metadata WHERE key=?) OR EXISTS(SELECT 1 FROM labels WHERE horizon=?)",params![format!("horizon:{}",self.horizon),self.horizon as i64],|row|row.get(0)).map_err(|e|e.to_string())?;
        if !combo_known || !horizon_known {
            return Err("该运行不存在此参数组合或评价周期".into());
        }
        let grouping = conn
            .query_row(
                "SELECT value FROM metadata WHERE key='grouping'",
                [],
                |row| row.get::<_, String>(0),
            )
            .ok()
            .and_then(|text| serde_json::from_str::<ResearchGrouping>(&text).ok())
            .unwrap_or_default();
        let match_cap = if grouping.market_cap_boundaries.is_empty() {
            "'未启用'"
        } else {
            "o.cap_group"
        };
        let match_score = if grouping.baseline_score_boundaries.is_empty() {
            "'未启用'"
        } else {
            "coalesce(p.score_group,'缺失')"
        };
        let dimension = match self.dimension.as_str() {
            "" | "all" => "'全部'".to_string(),
            "board" => "o.board".into(),
            "market_cap" => "o.cap_group".into(),
            "baseline_score" => "coalesce(p.score_group,'缺失')".into(),
            metric
                if [
                    "MKT_D10", "MKT_F5", "MKT_CP5", "MKT_VA5", "MKT_LO5", "MKT_AC5", "MKT_IS5",
                    "MKT_CF20", "MKT_DT5", "MKT_SC5",
                ]
                .contains(&metric) =>
            {
                format!(
                    "coalesce((SELECT bucket FROM market m WHERE m.trade_date=o.trade_date AND m.metric='{metric}'),'缺失')"
                )
            }
            _ => return Err("不支持的切片".into()),
        };
        let state = match self.state.as_str() {
            "" | "all" => "TRUE",
            "triggered" => "score IS NOT NULL AND abs(score)>1e-12 AND eligibility=1",
            "negative" => {
                "score IS NOT NULL AND abs(score)>1e-12 AND eligibility=1 AND residual_return<0"
            }
            "positive" => {
                "score IS NOT NULL AND abs(score)>1e-12 AND eligibility=1 AND residual_return>0"
            }
            "unknown_eligibility" => "eligibility=-1",
            "ineligible" => "eligibility=0",
            "unavailable" => "eligibility=1 AND quote_present AND score IS NULL",
            "not_triggered" => "eligibility=1 AND score=0",
            "missing_history" => "eligibility=1 AND NOT quote_present",
            "missing_label" => "eligibility=1 AND abs(score)>1e-12 AND residual_return IS NULL",
            _ => return Err("不支持的样本状态".into()),
        };
        let sql = format!(
            "WITH observation_coverage AS (SELECT *, count(*) OVER(PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) AS future_market_days,count(*) FILTER(WHERE quote_present) OVER(PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) AS future_quotes FROM observations), facts AS (SELECT o.*, s.score,l.residual_return,l.entry_date,l.label_end_date,p.score AS baseline_score,p.rank AS baseline_rank,{match_cap} AS match_cap_group,{match_score} AS match_score_group,{dimension} AS bucket FROM observation_coverage o LEFT JOIN baseline_scores p ON p.ts_code=o.ts_code AND p.trade_date=o.trade_date LEFT JOIN signals s ON s.ts_code=o.ts_code AND s.trade_date=o.trade_date AND s.combo_key=? LEFT JOIN labels l ON l.ts_code=o.ts_code AND l.trade_date=o.trade_date AND l.horizon=? WHERE o.trade_date BETWEEN ? AND ?), selected AS (SELECT *, CASE WHEN eligibility=-1 THEN '上市资格未知' WHEN eligibility=0 THEN '未满足上市资格' WHEN NOT quote_present THEN '历史行情缺失' WHEN score IS NULL THEN '指标或表达式窗口不可用' WHEN residual_return IS NULL AND future_market_days<{} THEN '评价区间末端标签未成熟' WHEN residual_return IS NULL AND future_quotes<{} THEN '个股后续行情不足（含停牌与历史缺失）' WHEN residual_return IS NULL THEN '行情无效或指数/概念/行业扣减资料缺失' END AS missing_reason FROM facts WHERE (? IS NULL OR bucket=?) AND {state})",
            self.horizon, self.horizon
        );
        Ok((conn, sql))
    }
}

#[derive(Debug, Serialize)]
pub struct ResearchEvidenceSample {
    pub baseline_score: Option<f64>,
    pub baseline_rank: Option<i64>,
    pub ts_code: String,
    pub trade_date: String,
    pub board: String,
    pub market_cap: Option<f64>,
    pub bucket: String,
    pub score: Option<f64>,
    pub residual_return: Option<f64>,
    pub entry_date: Option<String>,
    pub label_end_date: Option<String>,
    pub missing_reason: Option<String>,
}
#[derive(Debug, Serialize)]
pub struct ResearchEvidencePage {
    pub total: u64,
    pub items: Vec<ResearchEvidenceSample>,
}

pub fn list_research_evidence(
    source: &str,
    query: &ResearchEvidenceQuery,
) -> Result<ResearchEvidencePage, String> {
    let (conn, sql) = query.sql(source)?;
    let total: i64 = conn
        .query_row(
            &format!("{sql} SELECT count(*) FROM selected"),
            params![
                query.combo_key,
                query.horizon as i64,
                query.start_date,
                query.end_date,
                query.bucket,
                query.bucket
            ],
            |r| r.get(0),
        )
        .map_err(|e| e.to_string())?;
    let mut stmt=conn.prepare(&format!("{sql} SELECT ts_code,trade_date,board,market_cap,bucket,score,residual_return,entry_date,label_end_date,missing_reason,baseline_score,baseline_rank FROM selected ORDER BY trade_date,ts_code LIMIT ? OFFSET ?")).map_err(|e|e.to_string())?;
    let items = stmt
        .query_map(
            params![
                query.combo_key,
                query.horizon as i64,
                query.start_date,
                query.end_date,
                query.bucket,
                query.bucket,
                query.limit as i64,
                query.offset as i64
            ],
            |r| {
                Ok(ResearchEvidenceSample {
                    ts_code: r.get(0)?,
                    trade_date: r.get(1)?,
                    board: r.get(2)?,
                    market_cap: r.get(3)?,
                    bucket: r.get(4)?,
                    score: r.get(5)?,
                    residual_return: r.get(6)?,
                    entry_date: r.get(7)?,
                    label_end_date: r.get(8)?,
                    missing_reason: r.get(9)?,
                    baseline_score: r.get(10)?,
                    baseline_rank: r.get(11)?,
                })
            },
        )
        .map_err(|e| e.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())?;
    Ok(ResearchEvidencePage {
        total: total.max(0) as u64,
        items,
    })
}

#[derive(Debug, Serialize)]
pub struct ResearchEvidenceSummary {
    pub market_status: String,
    pub statistics: crate::simulate::research_statistics::ResearchSeriesStatistics,
    pub period: String,
    pub bucket: String,
    pub observations: u64,
    pub triggered: u64,
    pub labeled: u64,
    pub different_stocks: u64,
    pub unknown_eligibility: u64,
    pub ineligible: u64,
    pub unavailable: u64,
    pub missing_history: u64,
    pub missing_label: u64,
    pub immature_label: u64,
    pub insufficient_quote_label: u64,
    pub missing_factor_label: u64,
    pub matched_triggered: u64,
    pub matched_days: u64,
    pub raw_mean: Option<f64>,
    pub stock_day_mean: Option<f64>,
    pub control_mean: Option<f64>,
    pub matched_difference: Option<f64>,
    pub label_end_date: Option<String>,
}

pub fn summarize_research_evidence(
    source: &str,
    query: &ResearchEvidenceQuery,
    frequency: &str,
) -> Result<Vec<ResearchEvidenceSummary>, String> {
    let (conn, sql) = query.sql(source)?;
    summarize_evidence(conn, sql, query, frequency)
}

fn summarize_evidence(
    conn: Connection,
    sql: String,
    query: &ResearchEvidenceQuery,
    frequency: &str,
) -> Result<Vec<ResearchEvidenceSummary>, String> {
    let period = match frequency {
        "all" => "'全部'",
        "quarter" => {
            "substr(trade_date,1,4)||' Q'||cast(cast(ceil(cast(substr(trade_date,5,2) AS DOUBLE)/3) AS BIGINT) AS VARCHAR)"
        }
        "month" => "substr(trade_date,1,6)",
        _ => return Err("不支持的期间".into()),
    };
    // Match only known, qualified observations within the same date, code-board and saved cap group.
    // No future outcome determines the control's identity. Missing labels reduce coverage, never become zero.
    let query_sql = format!(
        "{sql}, controls AS (SELECT trade_date,board,match_cap_group,match_score_group,avg(residual_return) AS control_return FROM facts WHERE eligibility=1 AND score=0 AND match_cap_group<>'缺失' AND match_score_group<>'缺失' GROUP BY trade_date,board,match_cap_group,match_score_group), daily AS (SELECT {period} AS period,bucket,trade_date,count(*) AS n,count(*) FILTER(WHERE eligibility=1 AND abs(score)>1e-12) AS triggered,count(*) FILTER(WHERE eligibility=1 AND abs(score)>1e-12 AND residual_return IS NOT NULL) AS labeled,count(*) FILTER(WHERE eligibility=-1) AS unknown_eligibility,count(*) FILTER(WHERE eligibility=0) AS ineligible,count(*) FILTER(WHERE eligibility=1 AND quote_present AND score IS NULL) AS unavailable,count(*) FILTER(WHERE eligibility=1 AND NOT quote_present) AS missing_history,count(*) FILTER(WHERE eligibility=1 AND abs(score)>1e-12 AND residual_return IS NULL) AS missing_label,count(*) FILTER(WHERE eligibility=1 AND abs(score)>1e-12 AND residual_return IS NULL AND future_market_days<{0}) AS immature_label,count(*) FILTER(WHERE eligibility=1 AND abs(score)>1e-12 AND residual_return IS NULL AND future_market_days>={0} AND future_quotes<{0}) AS insufficient_quote_label,count(*) FILTER(WHERE eligibility=1 AND abs(score)>1e-12 AND residual_return IS NULL AND future_market_days>={0} AND future_quotes>={0}) AS missing_factor_label,avg(residual_return) FILTER(WHERE eligibility=1 AND abs(score)>1e-12) AS raw_mean,sum(residual_return) FILTER(WHERE eligibility=1 AND abs(score)>1e-12) AS raw_sum,max(label_end_date) FILTER(WHERE eligibility=1 AND abs(score)>1e-12) AS label_end_date FROM selected GROUP BY period,bucket,trade_date), matched AS (SELECT {period} AS period,s.bucket,s.trade_date,count(*) AS matched_triggered,avg(s.residual_return-c.control_return) AS difference,avg(c.control_return) AS control_mean FROM selected s JOIN controls c USING(trade_date,board,match_cap_group,match_score_group) WHERE s.eligibility=1 AND abs(s.score)>1e-12 AND s.residual_return IS NOT NULL AND c.control_return IS NOT NULL GROUP BY period,s.bucket,s.trade_date), stocks AS (SELECT {period} AS period,bucket,count(DISTINCT ts_code) AS different_stocks FROM selected WHERE eligibility=1 AND abs(score)>1e-12 GROUP BY period,bucket) SELECT d.period,d.bucket,sum(n)::BIGINT,sum(triggered)::BIGINT,sum(labeled)::BIGINT,coalesce(max(stocks.different_stocks),0)::BIGINT,sum(unknown_eligibility)::BIGINT,sum(ineligible)::BIGINT,sum(unavailable)::BIGINT,sum(missing_history)::BIGINT,sum(missing_label)::BIGINT,coalesce(sum(m.matched_triggered),0)::BIGINT,count(m.difference),avg(d.raw_mean),sum(raw_sum)/nullif(sum(labeled),0),avg(m.control_mean),avg(m.difference),max(d.label_end_date),sum(d.immature_label)::BIGINT,sum(d.insufficient_quote_label)::BIGINT,sum(d.missing_factor_label)::BIGINT FROM daily d LEFT JOIN matched m USING(period,bucket,trade_date) LEFT JOIN stocks USING(period,bucket) GROUP BY d.period,d.bucket ORDER BY d.period,d.bucket",
        query.horizon
    );
    let mut stmt = conn.prepare(&query_sql).map_err(|e| e.to_string())?;
    let mut output = stmt
        .query_map(
            params![
                query.combo_key,
                query.horizon as i64,
                query.start_date,
                query.end_date,
                query.bucket,
                query.bucket
            ],
            |r| {
                Ok(ResearchEvidenceSummary {
                    statistics: Default::default(),
                    market_status: String::new(),
                    period: r.get(0)?,
                    bucket: r.get(1)?,
                    observations: r.get::<_, i64>(2)?.max(0) as u64,
                    triggered: r.get::<_, i64>(3)?.max(0) as u64,
                    labeled: r.get::<_, i64>(4)?.max(0) as u64,
                    different_stocks: r.get::<_, i64>(5)?.max(0) as u64,
                    unknown_eligibility: r.get::<_, i64>(6)?.max(0) as u64,
                    ineligible: r.get::<_, i64>(7)?.max(0) as u64,
                    unavailable: r.get::<_, i64>(8)?.max(0) as u64,
                    missing_history: r.get::<_, i64>(9)?.max(0) as u64,
                    missing_label: r.get::<_, i64>(10)?.max(0) as u64,
                    matched_triggered: r.get::<_, i64>(11)?.max(0) as u64,
                    matched_days: r.get::<_, i64>(12)?.max(0) as u64,
                    raw_mean: r.get(13)?,
                    stock_day_mean: r.get(14)?,
                    control_mean: r.get(15)?,
                    matched_difference: r.get(16)?,
                    label_end_date: r.get(17)?,
                    immature_label: r.get::<_, i64>(18)?.max(0) as u64,
                    insufficient_quote_label: r.get::<_, i64>(19)?.max(0) as u64,
                    missing_factor_label: r.get::<_, i64>(20)?.max(0) as u64,
                })
            },
        )
        .map_err(|e| e.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())?;
    drop(stmt);
    let settings: String = conn
        .query_row(
            "SELECT value FROM metadata WHERE key='statistics'",
            [],
            |r| r.get(0),
        )
        .map_err(|e| e.to_string())?;
    let settings: crate::simulate::research_statistics::ResearchStatisticsSettings =
        serde_json::from_str(&settings).map_err(|e| e.to_string())?;
    let family: i64 = conn
        .query_row(
            "SELECT (SELECT count(*) FROM metadata WHERE key LIKE 'combo:%') * greatest(1,(SELECT count(*) FROM metadata WHERE key LIKE 'horizon:%'),(SELECT count(DISTINCT horizon) FROM labels))",
            [],
            |r| r.get(0),
        )
        .map_err(|e| e.to_string())?;
    let mut axis = conn
        .prepare(
            "SELECT trade_date FROM calendar WHERE trade_date BETWEEN ? AND ? ORDER BY trade_date",
        )
        .map_err(|e| e.to_string())?;
    let axis = axis
        .query_map(params![query.start_date, query.end_date], |r| {
            r.get::<_, String>(0)
        })
        .map_err(|e| e.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())?;
    let daily_sql = format!(
        "{} SELECT period,bucket,trade_date,difference FROM matched",
        query_sql
            .split(" SELECT d.period,d.bucket")
            .next()
            .ok_or("缺少日度诊断")?
    );
    let mut stmt = conn.prepare(&daily_sql).map_err(|e| e.to_string())?;
    let mut rows = stmt
        .query(params![
            query.combo_key,
            query.horizon as i64,
            query.start_date,
            query.end_date,
            query.bucket,
            query.bucket
        ])
        .map_err(|e| e.to_string())?;
    let mut differences = HashMap::<(String, String), HashMap<String, Option<f64>>>::new();
    while let Some(row) = rows.next().map_err(|e| e.to_string())? {
        differences
            .entry((
                row.get(0).map_err(|e| e.to_string())?,
                row.get(1).map_err(|e| e.to_string())?,
            ))
            .or_default()
            .insert(
                row.get(2).map_err(|e| e.to_string())?,
                row.get(3).map_err(|e| e.to_string())?,
            );
    }
    let market_status = conn
        .query_row(
            "SELECT value FROM metadata WHERE key='market_status'",
            [],
            |row| row.get::<_, String>(0),
        )
        .unwrap_or_else(|_| "旧明细未记录MKT覆盖状态".into());
    for item in &mut output {
        item.market_status = market_status.clone();
        let series = differences.get(&(item.period.clone(), item.bucket.clone()));
        let values = axis
            .iter()
            .filter(|day| match frequency {
                "quarter" => {
                    format!(
                        "{} Q{}",
                        &day[..4],
                        (day[4..6].parse::<usize>().unwrap_or(1) - 1) / 3 + 1
                    ) == item.period
                }
                "month" => day.starts_with(&item.period),
                _ => true,
            })
            .map(|day| series.and_then(|series| series.get(day)).copied().flatten())
            .collect::<Vec<_>>();
        item.statistics = crate::simulate::research_statistics::describe_research_series(
            &values,
            query.horizon,
            &settings,
            family.max(1) as usize,
        )?;
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn research_evidence_keeps_unavailable_nonhits_missing_labels_and_matched_denominators() {
        let root =
            std::env::temp_dir().join(format!("research-evidence-{:x}", rand::random::<u64>()));
        std::fs::create_dir(&root).unwrap();
        let source = root.to_str().unwrap();
        let header = crate::research_archive::save_research_archive(
            source,
            None,
            "对照".into(),
            serde_json::json!({"query":{}}),
            serde_json::json!({}),
            "expression_validation",
            "running",
            None,
            None,
        )
        .unwrap();
        let output = ValidationResearchOutput {
            baseline_diagnostics: true,
            path: research_artifact_path(source, &header.run_id, "evidence.db").unwrap(),
            grouping: Default::default(),
            statistics: Default::default(),
        };
        let db = output.open().unwrap();
        db.execute_batch("INSERT INTO calendar VALUES ('20260105'),('20260106');
            INSERT INTO observations VALUES ('A','20260105','主板',10,'组1',1,true),('B','20260105','主板',11,'组1',1,true),('C','20260105','主板',10,'组1',1,true),('D','20260105','主板',10,'组1',1,true),('A','20260106','主板',10,'组1',1,true),('B','20260106','主板',11,'组1',1,true);
            INSERT INTO labels VALUES (3,'A','20260105','20260106','20260108',4),(3,'B','20260105','20260106','20260108',1),(3,'C','20260105','20260106','20260108',99),(3,'A','20260106','20260107','20260109',8),(3,'B','20260106','20260107','20260109',3);
            INSERT INTO signals VALUES ('c','A','20260105',1),('c','B','20260105',0),('c','C','20260105',NULL),('c','D','20260105',1),('c','A','20260106',1),('c','B','20260106',0);").unwrap();
        db.execute(
            "INSERT INTO metadata VALUES ('statistics',?)",
            [serde_json::to_string(&output.statistics).unwrap()],
        )
        .unwrap();
        db.execute("INSERT INTO metadata VALUES ('combo:c','complete')", [])
            .unwrap();
        drop(db);
        crate::research_archive::finish_research_archive(
            source,
            &header.run_id,
            "completed",
            None,
            serde_json::json!({"combo_results":[]}),
        )
        .unwrap();
        let query = ResearchEvidenceQuery {
            run_id: header.run_id.clone(),
            combo_key: "c".into(),
            horizon: 3,
            start_date: "20260105".into(),
            end_date: "20260106".into(),
            dimension: "board".into(),
            bucket: None,
            state: "all".into(),
            offset: 0,
            limit: 50,
        };
        let summary = summarize_research_evidence(source, &query, "quarter").unwrap();
        assert_eq!(summary.len(), 1);
        let row = &summary[0];
        assert_eq!(row.period, "2026 Q1");
        assert_eq!(
            (
                row.triggered,
                row.labeled,
                row.unavailable,
                row.missing_label,
                row.matched_triggered
            ),
            (3, 2, 1, 1, 2)
        );
        assert_eq!(row.matched_difference, Some(4.));
        assert_eq!(row.raw_mean, Some(6.));
        assert_eq!(row.statistics.status, "insufficient");
        assert_eq!(
            serde_json::to_value(output.summarize(&query).unwrap()).unwrap(),
            serde_json::to_value(summarize_research_evidence(source, &query, "all").unwrap())
                .unwrap()
        );
        let db = output.open().unwrap();
        db.execute_batch("INSERT INTO observations VALUES ('E','20260105','主板',NULL,'缺失',1,true); INSERT INTO labels VALUES (3,'E','20260105','20260106','20260108',3); INSERT INTO signals VALUES ('c','E','20260105',0);").unwrap();
        drop(db);
        let no_cap_control = summarize_research_evidence(source, &query, "quarter").unwrap();
        assert_eq!(no_cap_control[0].matched_difference, Some(3.5));
        let db = output.open().unwrap();
        let grouping = ResearchGrouping {
            market_cap_boundaries: vec![50.],
            ..Default::default()
        };
        db.execute(
            "INSERT INTO metadata VALUES ('grouping',?)",
            [serde_json::to_string(&grouping).unwrap()],
        )
        .unwrap();
        drop(db);
        assert_eq!(
            summarize_research_evidence(source, &query, "quarter").unwrap()[0].matched_difference,
            Some(4.)
        );
        let db = output.open().unwrap();
        db.execute_batch("INSERT INTO baseline_scores VALUES ('A','20260105',100,1,'组2'),('A','20260106',100,1,'组2'),('B','20260105',10,2,'组1'),('B','20260106',10,2,'组1'),('E','20260105',100,3,'组2');").unwrap();
        let grouping = ResearchGrouping {
            baseline_score_boundaries: vec![50.],
            ..Default::default()
        };
        db.execute(
            "UPDATE metadata SET value=? WHERE key='grouping'",
            [serde_json::to_string(&grouping).unwrap()],
        )
        .unwrap();
        drop(db);
        let score_control = summarize_research_evidence(source, &query, "quarter").unwrap();
        assert_eq!(
            (
                score_control[0].matched_triggered,
                score_control[0].matched_days,
                score_control[0].matched_difference
            ),
            (1, 1, Some(1.))
        );

        let mut missing = query.clone();
        missing.state = "missing_label".into();
        let page = list_research_evidence(source, &missing).unwrap();
        assert_eq!(page.total, 1);
        assert_eq!(page.items[0].ts_code, "D");
        assert!(
            crate::research_archive::finish_research_archive(
                source,
                &header.run_id,
                "failed",
                None,
                serde_json::json!({})
            )
            .is_err()
        );
        let copy = crate::research_archive::save_research_archive(
            source,
            Some(header.run_id.clone()),
            "复核".into(),
            serde_json::json!({"exploratory_filter":{"dimension":"board","bucket":"主板"}}),
            serde_json::json!({}),
            "expression_validation",
            "completed",
            None,
            Some(&header.run_id),
        )
        .unwrap();
        assert_eq!(
            crate::research_archive::read_research_archive(source, &copy.run_id)
                .unwrap()
                .configuration["exploratory_filter"]["bucket"],
            "主板"
        );
        crate::research_archive::delete_research_archive(source, &header.run_id).unwrap();
        missing.run_id = copy.run_id;
        assert_eq!(list_research_evidence(source, &missing).unwrap().total, 1);
        std::fs::remove_dir_all(root).unwrap();
    }
}
