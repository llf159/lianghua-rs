use super::*;
use crate::trigger_similarity::channel::build_channel_fingerprint;
use crate::trigger_similarity::ranking::store::stable_content_signature;
use duckdb::{Connection, params};
use std::path::Path;

#[derive(Serialize, Deserialize)]
struct CachedSample {
    trigger: TriggerFingerprint,
    rule_names: Vec<(usize, String)>,
    price_volume: ChannelFingerprint,
    indicators: ChannelFingerprint,
    trigger_count: usize,
    outcome: Outcome,
}

// 缓存独立于云端单日快照；不保存参考日相关的市场匹配和 IDF。
pub(super) struct FingerprintCache {
    conn: Connection,
    bulk: crate::data::bulk_write::BulkWriteSettings,
}

pub(super) fn input_signature(
    conn: &Connection,
    source: &str,
    through: &str,
) -> Result<String, String> {
    let market: String = conn.query_row(
        "SELECT concat(count(*), ':', coalesce(CAST(bit_xor(hash(s)) AS VARCHAR), '0')) FROM trigger_market_db.stock_data s WHERE trade_date <= ? AND adj_type='ind'",
        [through], |row| row.get(0),
    ).map_err(|e| format!("校验历史指纹基准失败: {e}"))?;
    let mut parts = vec!["cache-input-v2".to_string(), market];
    let calendar: String = conn.query_row(
        "SELECT coalesce(string_agg(trade_date, ',' ORDER BY trade_date), '') FROM (SELECT DISTINCT trade_date FROM score_summary WHERE trade_date <= ?)",
        [through], |row| row.get(0),
    ).map_err(|e| format!("校验指纹日历失败: {e}"))?;
    parts.push(calendar);
    for file in ["score_rule.toml", "ind.toml"] {
        parts.push(stable_content_signature(&Path::new(source).join(file))?);
    }
    Ok(parts.join("|"))
}

fn stock_input_signatures(
    conn: &Connection,
    through: &str,
) -> Result<HashMap<String, String>, String> {
    let mut stamps = HashMap::<String, String>::new();
    for (table, filter) in [
        ("trigger_market_db.stock_data", "AND adj_type='qfq'"),
        ("rule_details", ""),
    ] {
        let mut stmt = conn.prepare(&format!(
            "SELECT ts_code, concat(count(*), ':', coalesce(CAST(bit_xor(hash(s)) AS VARCHAR), '0')) FROM {table} s WHERE trade_date <= ? {filter} GROUP BY ts_code"
        )).map_err(|e| format!("预编译逐股指纹水位失败: {e}"))?;
        let rows = stmt
            .query_map([through], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(|e| format!("查询逐股指纹水位失败: {e}"))?;
        for row in rows {
            let (code, stamp) = row.map_err(|e| e.to_string())?;
            stamps
                .entry(code)
                .or_default()
                .push_str(&format!("|{table}={stamp}"));
        }
    }
    Ok(stamps)
}

impl FingerprintCache {
    pub fn open(
        input: &Connection,
        source: &str,
        key: &str,
        target_date: &str,
        anchors: &[Anchor],
    ) -> Result<Self, String> {
        let mut conn = Connection::open(Path::new(source).join("strategy_similarity_cache.db"))
            .map_err(|e| format!("打开历史指纹缓存失败: {e}"))?;
        let bulk = crate::data::bulk_write::BulkWriteSettings::apply(&conn)?;
        conn.execute_batch("CREATE TABLE IF NOT EXISTS cache_meta(config_key VARCHAR, through_date VARCHAR, signature VARCHAR, ready BOOLEAN);
            CREATE TABLE IF NOT EXISTS fingerprints(ts_code VARCHAR, start_date VARCHAR, end_date VARCHAR, payload VARCHAR, PRIMARY KEY(ts_code, start_date, end_date));
            CREATE TABLE IF NOT EXISTS cache_stock_stamps(ts_code VARCHAR PRIMARY KEY, signature VARCHAR);
            CREATE TEMP TABLE wanted(id BIGINT, ts_code VARCHAR, start_date VARCHAR, end_date VARCHAR);")
            .map_err(|e| format!("初始化历史指纹缓存失败: {e}"))?;
        let previous = conn
            .query_row(
                "SELECT config_key, through_date, signature, ready FROM cache_meta LIMIT 1",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, bool>(3)?,
                    ))
                },
            )
            .optional()
            .map_err(|e| e.to_string())?;
        let mut current = None;
        let mut previous_stocks = None;
        let mut current_stocks = None;
        let valid = if let Some((old_key, through, stamp, ready)) = previous {
            if ready && old_key == key && through.as_str() <= target_date {
                let actual = input_signature(input, source, &through)?;
                let valid = actual == stamp;
                if valid {
                    previous_stocks = Some(stock_input_signatures(input, &through)?);
                }
                if through == target_date {
                    current = Some(actual);
                    current_stocks = previous_stocks.clone();
                }
                valid
            } else {
                false
            }
        } else {
            false
        };
        let current = match current {
            Some(stamp) => stamp,
            None => input_signature(input, source, target_date)?,
        };
        let current_stocks = match current_stocks {
            Some(stamps) => stamps,
            None => stock_input_signatures(input, target_date)?,
        };
        let mut invalidated = Vec::new();
        if let Some(actual) = previous_stocks {
            let mut stmt = conn
                .prepare("SELECT ts_code, signature FROM cache_stock_stamps")
                .map_err(|e| e.to_string())?;
            let rows = stmt
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })
                .map_err(|e| e.to_string())?;
            for row in rows {
                let (code, stamp) = row.map_err(|e| e.to_string())?;
                if actual.get(&code) != Some(&stamp) {
                    invalidated.push(code);
                }
            }
        }
        let tx = conn.transaction().map_err(|e| e.to_string())?;
        if !valid {
            tx.execute("DELETE FROM fingerprints", [])
                .map_err(|e| e.to_string())?;
        }
        {
            let mut delete = tx
                .prepare("DELETE FROM fingerprints WHERE ts_code=?")
                .map_err(|e| e.to_string())?;
            for code in &invalidated {
                delete.execute([code]).map_err(|e| e.to_string())?;
            }
        }
        log::info!(
            "相似榜指纹缓存: global_valid={}, revised_stocks={}",
            valid,
            invalidated.len()
        );
        tx.execute("DELETE FROM cache_stock_stamps", [])
            .map_err(|e| e.to_string())?;
        {
            let mut appender = tx
                .appender("cache_stock_stamps")
                .map_err(|e| e.to_string())?;
            for (code, stamp) in current_stocks {
                appender
                    .append_row(params![code, stamp])
                    .map_err(|e| e.to_string())?;
            }
            appender.flush().map_err(|e| e.to_string())?;
        }
        tx.execute("DELETE FROM cache_meta", [])
            .map_err(|e| e.to_string())?;
        tx.execute(
            "INSERT INTO cache_meta VALUES (?, ?, ?, false)",
            params![key, target_date, current],
        )
        .map_err(|e| e.to_string())?;
        {
            let mut appender = tx.appender("wanted").map_err(|e| e.to_string())?;
            for anchor in anchors {
                appender
                    .append_row(params![
                        anchor.id as i64,
                        anchor.ts_code,
                        anchor.start_trade_date,
                        anchor.end_trade_date
                    ])
                    .map_err(|e| e.to_string())?;
            }
            appender.flush().map_err(|e| e.to_string())?;
        }
        tx.execute("DELETE FROM fingerprints f WHERE NOT EXISTS (SELECT 1 FROM wanted w WHERE w.ts_code=f.ts_code AND w.start_date=f.start_date AND w.end_date=f.end_date)", [])
            .map_err(|e| e.to_string())?;
        tx.commit().map_err(|e| e.to_string())?;
        // 每批提交以限制内存；中断时 ready=false，下一轮放弃未认证缓存。
        Ok(Self { conn, bulk })
    }

    pub fn load(
        &self,
        anchors: &mut Vec<Anchor>,
        environment: &HashMap<String, Arc<ChannelFingerprint>>,
        rules: &mut RuleCatalog,
        target_date: &str,
    ) -> Result<Vec<RankingSample>, String> {
        let Some(first) = anchors.first() else {
            return Ok(Vec::new());
        };
        let last = anchors.last().unwrap();
        let mut stmt = self.conn.prepare("SELECT w.id, f.payload FROM wanted w JOIN fingerprints f ON w.ts_code=f.ts_code AND w.start_date=f.start_date AND w.end_date=f.end_date WHERE w.id BETWEEN ? AND ? ORDER BY w.id")
            .map_err(|e| e.to_string())?;
        let rows = stmt
            .query_map(params![first.id as i64, last.id as i64], |row| {
                Ok((row.get::<_, i64>(0)? as usize, row.get::<_, String>(1)?))
            })
            .map_err(|e| e.to_string())?;
        let anchors_by_id = anchors
            .iter()
            .map(|anchor| (anchor.id, anchor))
            .collect::<HashMap<_, _>>();
        let mut samples = Vec::new();
        let mut reused = std::collections::HashSet::new();
        for row in rows {
            let (id, payload) = row.map_err(|e| e.to_string())?;
            let Ok(mut cached) = serde_json::from_str::<CachedSample>(&payload) else {
                continue;
            };
            if cached.outcome.end_trade_date.as_str() > target_date {
                continue;
            }
            let Some(&anchor) = anchors_by_id.get(&id) else {
                continue;
            };
            let mut remapped = std::collections::BTreeMap::new();
            let mut complete = true;
            for (old_id, name) in cached.rule_names {
                if let Some(hits) = cached.trigger.by_rule.remove(&old_id) {
                    remapped.insert(rules.intern(name), hits);
                } else {
                    complete = false;
                    break;
                }
            }
            if !complete || !cached.trigger.by_rule.is_empty() {
                continue;
            }
            cached.trigger.by_rule = remapped;
            let excess = cached.outcome.excess_return_pct.unwrap_or(0.0);
            samples.push(RankingSample {
                anchor: anchor.clone(),
                fingerprint: RankingFingerprint {
                    trigger: cached.trigger,
                    price_volume: cached.price_volume,
                    indicators: cached.indicators,
                    market: environment
                        .get(&anchor.end_trade_date)
                        .cloned()
                        .unwrap_or_else(|| Arc::new(build_channel_fingerprint(Vec::new()))),
                },
                trigger_count: cached.trigger_count,
                outcome: Some(cached.outcome),
                total_score: None,
                original_rank: None,
                template_class: if excess > EPS {
                    1
                } else if excess < -EPS {
                    -1
                } else {
                    0
                },
            });
            reused.insert(id);
        }
        anchors.retain(|anchor| !reused.contains(&anchor.id));
        Ok(samples)
    }

    pub fn store(&mut self, samples: &[RankingSample], rules: &RuleCatalog) -> Result<(), String> {
        let tx = self.conn.transaction().map_err(|e| e.to_string())?;
        tx.execute_batch("CREATE OR REPLACE TEMP TABLE cache_batch_keys(ts_code VARCHAR, start_date VARCHAR, end_date VARCHAR)")
            .map_err(|e| e.to_string())?;
        {
            let mut keys = tx.appender("cache_batch_keys").map_err(|e| e.to_string())?;
            for sample in samples {
                if sample.outcome.is_some() {
                    keys.append_row(params![
                        sample.anchor.ts_code,
                        sample.anchor.start_trade_date,
                        sample.anchor.end_trade_date
                    ])
                    .map_err(|e| e.to_string())?;
                }
            }
            keys.flush().map_err(|e| e.to_string())?;
        }
        tx.execute("DELETE FROM fingerprints f USING cache_batch_keys k WHERE f.ts_code=k.ts_code AND f.start_date=k.start_date AND f.end_date=k.end_date", [])
            .map_err(|e| e.to_string())?;
        {
            let mut appender = tx.appender("fingerprints").map_err(|e| e.to_string())?;
            for sample in samples {
                let Some(outcome) = &sample.outcome else {
                    continue;
                };
                let mut names = sample
                    .fingerprint
                    .trigger
                    .by_rule
                    .keys()
                    .map(|id| (*id, rules.names[*id].clone()))
                    .collect::<Vec<_>>();
                names.sort_unstable_by_key(|(id, _)| *id);
                let payload = serde_json::to_string(&CachedSample {
                    trigger: sample.fingerprint.trigger.clone(),
                    rule_names: names,
                    price_volume: sample.fingerprint.price_volume.clone(),
                    indicators: sample.fingerprint.indicators.clone(),
                    trigger_count: sample.trigger_count,
                    outcome: outcome.clone(),
                })
                .map_err(|e| e.to_string())?;
                appender
                    .append_row(params![
                        sample.anchor.ts_code,
                        sample.anchor.start_trade_date,
                        sample.anchor.end_trade_date,
                        payload
                    ])
                    .map_err(|e| e.to_string())?;
            }
            appender.flush().map_err(|e| e.to_string())?;
        }
        tx.execute_batch("DROP TABLE cache_batch_keys")
            .map_err(|e| e.to_string())?;
        tx.commit().map_err(|e| e.to_string())
    }

    pub fn finish(self) -> Result<(), String> {
        let result = self
            .conn
            .execute("UPDATE cache_meta SET ready=true", [])
            .map(|_| ())
            .map_err(|e| e.to_string());
        self.bulk.checkpoint_and_finish(&self.conn, result)
    }
}

use duckdb::OptionalExt;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_reuses_appends_but_invalidates_revisions_and_interruption() {
        let source = std::env::temp_dir().join(format!(
            "similarity-cache-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&source).unwrap();
        let source_path = source.to_str().unwrap();
        let input = Connection::open_in_memory().unwrap();
        input.execute_batch("ATTACH ':memory:' AS trigger_market_db;
            CREATE TABLE trigger_market_db.stock_data(ts_code VARCHAR, trade_date VARCHAR, adj_type VARCHAR, close DOUBLE);
            INSERT INTO trigger_market_db.stock_data VALUES ('A','20240101','qfq',10),('A','20240102','qfq',11),('A','20240103','qfq',12);
            CREATE TABLE score_summary(trade_date VARCHAR);
            INSERT INTO score_summary VALUES ('20240101'),('20240102'),('20240103');
            CREATE TABLE rule_details(ts_code VARCHAR, trade_date VARCHAR, rule_name VARCHAR, rule_score DOUBLE);
            INSERT INTO rule_details VALUES ('A','20240102','规则',1);").unwrap();
        let anchor = Anchor {
            id: 0,
            ts_code: "A".into(),
            start_trade_date: "20240101".into(),
            end_trade_date: "20240102".into(),
        };
        let mut rules = RuleCatalog::default();
        let rule_id = rules.intern("规则".into());
        let trigger = crate::trigger_similarity::fingerprint::build_trigger_fingerprint(
            &[RuleEvent {
                rule_id,
                trade_date: "20240102".into(),
                score: 1.0000000000000002,
            }],
            &["20240101".into(), "20240102".into()],
            2,
        );
        let channel = build_channel_fingerprint(vec![Some(vec![1.0000000000000002, 3.1])]);
        let sample = RankingSample {
            anchor: anchor.clone(),
            fingerprint: RankingFingerprint {
                trigger,
                price_volume: channel.clone(),
                indicators: channel.clone(),
                market: Arc::new(channel),
            },
            trigger_count: 1,
            outcome: Some(Outcome {
                start_trade_date: "20240103".into(),
                end_trade_date: "20240103".into(),
                return_pct: 2.1,
                excess_return_pct: Some(1.1),
                mfe_pct: 3.1,
                mae_pct: -1.1,
            }),
            total_score: None,
            original_rank: None,
            template_class: 1,
        };
        input.execute_batch("INSERT INTO trigger_market_db.stock_data VALUES ('B','20240101','qfq',10),('B','20240102','qfq',11),('B','20240103','qfq',12)").unwrap();
        let mut other = sample.clone();
        other.anchor.id = 1;
        other.anchor.ts_code = "B".into();
        let anchors = vec![anchor, other.anchor.clone()];
        let mut cache =
            FingerprintCache::open(&input, source_path, "test", "20240103", &anchors).unwrap();
        cache.store(&[sample.clone(), other], &rules).unwrap();
        cache.finish().unwrap();
        input
            .execute_batch(
                "INSERT INTO trigger_market_db.stock_data VALUES ('A','20240104','qfq',13);
            INSERT INTO score_summary VALUES ('20240104');
            INSERT INTO rule_details VALUES ('A','20240104','新增',2);",
            )
            .unwrap();
        let cache =
            FingerprintCache::open(&input, source_path, "test", "20240104", &anchors).unwrap();
        let mut remapped_rules = RuleCatalog::default();
        remapped_rules.intern("新增".into());
        let mut missing = anchors.clone();
        let cached = cache
            .load(
                &mut missing,
                &HashMap::new(),
                &mut remapped_rules,
                "20240104",
            )
            .unwrap();
        assert!(missing.is_empty());
        assert_eq!(cached.len(), 2);
        assert_eq!(
            cached[0].fingerprint.trigger.by_rule[&remapped_rules.ids["规则"]][0].score,
            1.0000000000000002
        );
        assert_eq!(
            cached[0].fingerprint.price_volume.normalized_values,
            sample.fingerprint.price_volume.normalized_values
        );
        cache.finish().unwrap();
        input
            .execute_batch(
                "UPDATE trigger_market_db.stock_data SET close=9 WHERE ts_code='A' AND trade_date='20240101'",
            )
            .unwrap();
        let mut cache =
            FingerprintCache::open(&input, source_path, "test", "20240104", &anchors).unwrap();
        let mut missing = anchors.clone();
        let retained = cache
            .load(&mut missing, &HashMap::new(), &mut rules, "20240104")
            .unwrap();
        assert_eq!(retained.len(), 1);
        assert_eq!(retained[0].anchor.ts_code, "B");
        assert_eq!(missing.len(), 1);
        assert_eq!(missing[0].ts_code, "A");
        cache.store(std::slice::from_ref(&sample), &rules).unwrap();
        drop(cache);
        let cache =
            FingerprintCache::open(&input, source_path, "test", "20240104", &anchors).unwrap();
        let mut missing = anchors;
        assert!(
            cache
                .load(&mut missing, &HashMap::new(), &mut rules, "20240104")
                .unwrap()
                .is_empty()
        );
        cache.finish().unwrap();
        std::fs::remove_dir_all(source).unwrap();
    }
}
