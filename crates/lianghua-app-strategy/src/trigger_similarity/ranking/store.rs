use crate::trigger_similarity::ranking::{
    ALGORITHM_VERSION, ActiveConfigRecord, StrategyTriggerSimilarityActiveConfig,
};
use crate::trigger_similarity::*;

// 见父模块 mod.rs

use crate::data::source_db_path;
use crate::data::stock_list_path;
use duckdb::Connection;
use duckdb::params;
use std::fs;
use std::path::Path;
use std::time::UNIX_EPOCH;
pub(super) fn config_key(
    window_trade_days: usize,
    pool_segments: usize,
    outcome_trade_days: usize,
    sample_gap_trade_days: usize,
    benchmark_index_code: &str,
) -> String {
    (|algorithm_version: &str,
      window_trade_days: usize,
      pool_segments: usize,
      outcome_trade_days: usize,
      sample_gap_trade_days: usize,
      benchmark_index_code: &str|
     -> String {
        format!(
            "{algorithm_version}:w{window_trade_days}:p{pool_segments}:h{outcome_trade_days}:g{sample_gap_trade_days}:b{benchmark_index_code}"
        )
    })(
        ALGORITHM_VERSION,
        window_trade_days,
        pool_segments,
        outcome_trade_days,
        sample_gap_trade_days,
        benchmark_index_code,
    )
}

pub(super) fn file_stamp(path: &Path) -> Result<String, String> {
    let metadata =
        fs::metadata(path).map_err(|e| format!("读取数据文件状态失败 {:?}: {e}", path))?;
    let modified = metadata
        .modified()
        .ok()
        .and_then(|value| value.duration_since(UNIX_EPOCH).ok())
        .map(|value| value.as_nanos())
        .unwrap_or_default();
    Ok(format!("{}:{modified}", metadata.len()))
}

pub(super) fn load_data_signature(
    conn: &Connection,
    source_path: &str,
    resolved_trade_date: &str,
) -> Result<String, String> {
    let stock_stamp = file_stamp(&source_db_path(source_path))?;
    let stock_list_stamp = (|path: &Path| -> Result<String, String> {
        match file_stamp(path) {
            Ok(stamp) => Ok(stamp),
            Err(_) if !path.exists() => Ok("missing".to_string()),
            Err(error) => Err(error),
        }
    })(&stock_list_path(source_path))?;
    let score_stamp: String = conn
        .query_row(
            r#"
            SELECT concat(
                COUNT(*), ':', COALESCE(MAX(trade_date), ''), ':',
                COALESCE(CAST(bit_xor(hash(ts_code, trade_date, total_score, rank)) AS VARCHAR), '0')
            )
            FROM score_summary WHERE trade_date <= ?
            "#,
            params![resolved_trade_date],
            |row| row.get(0),
        )
        .map_err(|e| format!("读取评分数据水位失败: {e}"))?;
    let rule_stamp: String = conn
        .query_row(
            r#"
            SELECT concat(
                COUNT(*), ':', COALESCE(MAX(trade_date), ''), ':',
                COUNT(DISTINCT rule_name), ':',
                COALESCE(CAST(bit_xor(hash(ts_code, trade_date, rule_name, rule_score)) AS VARCHAR), '0')
            )
            FROM rule_details WHERE trade_date <= ?
            "#,
            params![resolved_trade_date],
            |row| row.get(0),
        )
        .map_err(|e| format!("读取策略触发数据水位失败: {e}"))?;
    Ok(format!(
        "{ALGORITHM_VERSION}|stock={stock_stamp}|stock_list={stock_list_stamp}|score={score_stamp}|rule={rule_stamp}"
    ))
}

pub(super) fn stable_content_signature(path: &Path) -> Result<String, String> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok("missing".to_string());
        }
        Err(error) => return Err(format!("读取配置文件失败 {}: {error}", path.display())),
    };
    let hash = bytes.iter().fold(0xcbf29ce484222325_u64, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
    });
    Ok(format!("{}:{hash:016x}", bytes.len()))
}

pub(super) fn parse_active_config_key(
    key: &str,
    _scope_trade_date: String,
    scope_signature: String,
) -> Option<ActiveConfigRecord> {
    let (algorithm_version, suffix) = key.split_once(':')?;
    let mut window = None;
    let mut pool = None;
    let mut outcome = None;
    let mut sample_gap = None;
    let mut benchmark = None;
    for part in suffix.split(':') {
        if let Some(value) = part.strip_prefix('w') {
            window = value.parse::<usize>().ok();
        } else if let Some(value) = part.strip_prefix('p') {
            pool = value.parse::<usize>().ok();
        } else if let Some(value) = part.strip_prefix('h') {
            outcome = value.parse::<usize>().ok();
        } else if let Some(value) = part.strip_prefix('g') {
            sample_gap = value.parse::<usize>().ok();
        } else if let Some(value) = part.strip_prefix('b') {
            benchmark = Some(value.to_string());
        }
    }
    Some(ActiveConfigRecord {
        config: StrategyTriggerSimilarityActiveConfig {
            algorithm_version: algorithm_version.to_string(),
            window_trade_days: window?,
            pool_segments: pool?,
            outcome_trade_days: outcome?,
            sample_gap_trade_days: sample_gap.unwrap_or(MIN_SAMPLE_GAP_TRADE_DAYS),
            benchmark_index_code: benchmark?,
        },
        config_key: key.to_string(),
        scope_signature,
    })
}

pub(super) fn load_active_config_record(
    conn: &Connection,
) -> Result<Option<ActiveConfigRecord>, String> {
    if !table_exists(conn, "strategy_trigger_similarity_active_config")? {
        return Ok(None);
    }
    let has_explicit_columns = [
        "algorithm_version",
        "window_trade_days",
        "pool_segments",
        "outcome_trade_days",
        "sample_gap_trade_days",
        "benchmark_index_code",
    ]
    .into_iter()
    .all(|column| {
        (|conn: &Connection, table_name: &str, column_name: &str| -> bool {
            conn.query_row(
                "SELECT COUNT(*) > 0 FROM information_schema.columns \
         WHERE table_schema='main' AND table_name=? AND column_name=?",
                params![table_name, column_name],
                |row| row.get(0),
            )
            .unwrap_or(false)
        })(conn, "strategy_trigger_similarity_active_config", column)
    });
    let query = if has_explicit_columns {
        "SELECT config_key, scope_trade_date, scope_signature, algorithm_version, \
         window_trade_days, pool_segments, outcome_trade_days, sample_gap_trade_days, \
         benchmark_index_code \
         FROM strategy_trigger_similarity_active_config WHERE id=1"
    } else {
        "SELECT config_key, scope_trade_date, scope_signature, NULL, NULL, NULL, NULL, NULL, NULL \
         FROM strategy_trigger_similarity_active_config WHERE id=1"
    };
    let row = conn
        .query_row(query, [], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, Option<i64>>(4)?,
                row.get::<_, Option<i64>>(5)?,
                row.get::<_, Option<i64>>(6)?,
                row.get::<_, Option<i64>>(7)?,
                row.get::<_, Option<String>>(8)?,
            ))
        })
        .map(Some)
        .or_else(|error| match error {
            duckdb::Error::QueryReturnedNoRows => Ok(None),
            other => Err(other),
        })
        .map_err(|e| format!("读取走势相似生效配置失败: {e}"))?;
    Ok(row.and_then(
        |(key, date, signature, algorithm, window, pool, outcome, sample_gap, benchmark)| {
            let explicit = match (algorithm, window, pool, outcome, sample_gap, benchmark) {
                (
                    Some(algorithm),
                    Some(window),
                    Some(pool),
                    Some(outcome),
                    Some(sample_gap),
                    Some(benchmark),
                ) => Some(ActiveConfigRecord {
                    config: StrategyTriggerSimilarityActiveConfig {
                        algorithm_version: algorithm,
                        window_trade_days: usize::try_from(window).ok()?,
                        pool_segments: usize::try_from(pool).ok()?,
                        outcome_trade_days: usize::try_from(outcome).ok()?,
                        sample_gap_trade_days: usize::try_from(sample_gap).ok()?,
                        benchmark_index_code: benchmark,
                    },
                    config_key: key.clone(),
                    scope_signature: signature.clone(),
                }),
                _ => None,
            };
            explicit.or_else(|| parse_active_config_key(&key, date, signature))
        },
    ))
}

pub fn get_strategy_trigger_similarity_active_config(
    conn: &Connection,
) -> Result<Option<StrategyTriggerSimilarityActiveConfig>, String> {
    if let Some(record) = load_active_config_record(conn)? {
        return Ok(Some(record.config));
    }
    // 兼容升级前已经生成过当前算法版本结果的数据库；只作为读取默认值，
    // 首次新写入时仍会补齐 active_config 和清理策略。
    if !table_exists(conn, "strategy_trigger_similarity_rank_meta")? {
        return Ok(None);
    }
    let key: Option<String> = conn
        .query_row(
            "SELECT config_key FROM strategy_trigger_similarity_rank_meta \
             ORDER BY generated_at_epoch_seconds DESC, trade_date DESC LIMIT 1",
            [],
            |row| row.get(0),
        )
        .map(Some)
        .or_else(|error| match error {
            duckdb::Error::QueryReturnedNoRows => Ok(None),
            other => Err(other),
        })
        .map_err(|e| format!("读取最新走势相似配置失败: {e}"))?;
    Ok(key.and_then(|key| {
        parse_active_config_key(&key, String::new(), String::new()).map(|record| record.config)
    }))
}

pub(super) fn ensure_ranking_tables(conn: &Connection) -> Result<(), String> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS strategy_trigger_similarity_active_config (
            id TINYINT NOT NULL,
            config_key VARCHAR NOT NULL,
            algorithm_version VARCHAR NOT NULL,
            window_trade_days BIGINT NOT NULL,
            pool_segments BIGINT NOT NULL,
            outcome_trade_days BIGINT NOT NULL,
            sample_gap_trade_days BIGINT NOT NULL,
            benchmark_index_code VARCHAR NOT NULL,
            scope_trade_date VARCHAR NOT NULL,
            scope_signature VARCHAR NOT NULL,
            updated_at_epoch_seconds BIGINT NOT NULL,
            CONSTRAINT pk_strategy_similarity_active_config PRIMARY KEY (id),
            CONSTRAINT ck_strategy_similarity_active_config_singleton CHECK (id = 1)
        );
        CREATE TABLE IF NOT EXISTS strategy_trigger_similarity_rank_meta (
            trade_date VARCHAR NOT NULL,
            config_key VARCHAR NOT NULL,
            data_signature VARCHAR NOT NULL,
            generated_at_epoch_seconds BIGINT NOT NULL,
            historical_cutoff_date VARCHAR NOT NULL,
            universe_count BIGINT NOT NULL,
            ranked_count BIGINT NOT NULL,
            candidate_universe_count BIGINT NOT NULL,
            candidate_anchor_count BIGINT NOT NULL,
            evaluated_anchor_count BIGINT NOT NULL,
            elapsed_ms BIGINT NOT NULL,
            timings_json VARCHAR NOT NULL
        );
        CREATE TABLE IF NOT EXISTS strategy_trigger_similarity_rank (
            trade_date VARCHAR NOT NULL,
            config_key VARCHAR NOT NULL,
            rank BIGINT,
            ts_code VARCHAR NOT NULL,
            name VARCHAR,
            industry VARCHAR,
            concept VARCHAR,
            original_score DOUBLE,
            original_rank BIGINT,
            ranking_score DOUBLE,
            prediction_signal DOUBLE,
            confidence DOUBLE NOT NULL,
            sample_count BIGINT NOT NULL,
            effective_sample_count DOUBLE NOT NULL,
            expected_return_pct DOUBLE,
            expected_excess_return_pct DOUBLE,
            shrunk_excess_return_pct DOUBLE,
            excess_positive_rate DOUBLE,
            expected_mfe_pct DOUBLE,
            expected_mae_pct DOUBLE,
            average_similarity DOUBLE,
            best_similarity DOUBLE,
            trigger_count BIGINT NOT NULL,
            top_matches_json VARCHAR NOT NULL
        );
        CREATE TABLE IF NOT EXISTS strategy_trigger_similarity_summary (
            trade_date VARCHAR NOT NULL,
            ts_code VARCHAR NOT NULL,
            rank BIGINT,
            CONSTRAINT pk_strategy_similarity_summary PRIMARY KEY (trade_date, ts_code)
        );
        CREATE INDEX IF NOT EXISTS idx_strategy_similarity_rank_date_config_rank
          ON strategy_trigger_similarity_rank(trade_date, config_key, rank, ts_code);
        CREATE INDEX IF NOT EXISTS idx_strategy_similarity_summary_code_date
          ON strategy_trigger_similarity_summary(ts_code, trade_date);
        ALTER TABLE strategy_trigger_similarity_active_config
          ADD COLUMN IF NOT EXISTS algorithm_version VARCHAR;
        ALTER TABLE strategy_trigger_similarity_active_config
          ADD COLUMN IF NOT EXISTS window_trade_days BIGINT;
        ALTER TABLE strategy_trigger_similarity_active_config
          ADD COLUMN IF NOT EXISTS pool_segments BIGINT;
        ALTER TABLE strategy_trigger_similarity_active_config
          ADD COLUMN IF NOT EXISTS outcome_trade_days BIGINT;
        ALTER TABLE strategy_trigger_similarity_active_config
          ADD COLUMN IF NOT EXISTS sample_gap_trade_days BIGINT;
        ALTER TABLE strategy_trigger_similarity_active_config
          ADD COLUMN IF NOT EXISTS benchmark_index_code VARCHAR;
        "#,
    )
    .map_err(|e| format!("创建策略相似排行榜表失败: {e}"))
}

pub(super) fn table_exists(conn: &Connection, table_name: &str) -> Result<bool, String> {
    conn.query_row(
        "SELECT COUNT(*) > 0 FROM information_schema.tables WHERE table_schema='main' AND table_name=?",
        params![table_name],
        |row| row.get(0),
    )
    .map_err(|e| format!("检查相似排行榜表失败: {e}"))
}

pub(super) fn parse_config_key(key: &str) -> Option<(usize, usize, usize, usize, String)> {
    let suffix = key.strip_prefix(&format!("{ALGORITHM_VERSION}:"))?;
    let mut window = None;
    let mut pool = None;
    let mut outcome = None;
    let mut sample_gap = None;
    let mut benchmark = None;
    for part in suffix.split(':') {
        if let Some(value) = part.strip_prefix('w') {
            window = value.parse::<usize>().ok();
        } else if let Some(value) = part.strip_prefix('p') {
            pool = value.parse::<usize>().ok();
        } else if let Some(value) = part.strip_prefix('h') {
            outcome = value.parse::<usize>().ok();
        } else if let Some(value) = part.strip_prefix('g') {
            sample_gap = value.parse::<usize>().ok();
        } else if let Some(value) = part.strip_prefix('b') {
            benchmark = Some(value.to_string());
        }
    }
    Some((
        window?,
        pool?,
        outcome?,
        sample_gap.unwrap_or(MIN_SAMPLE_GAP_TRADE_DAYS),
        benchmark?,
    ))
}

#[cfg(test)]
mod tests {
    use crate::trigger_similarity::ranking::store::ensure_ranking_tables;
    use crate::trigger_similarity::ranking::store::get_strategy_trigger_similarity_active_config;
    use duckdb::Connection;

    #[test]
    fn active_config_migrates_from_the_legacy_config_key_row() {
        let conn = Connection::open_in_memory().expect("open in-memory DuckDB");
        conn.execute_batch(
            r#"
            CREATE TABLE strategy_trigger_similarity_active_config (
                id TINYINT PRIMARY KEY,
                config_key VARCHAR NOT NULL,
                scope_trade_date VARCHAR NOT NULL,
                scope_signature VARCHAR NOT NULL,
                updated_at_epoch_seconds BIGINT NOT NULL
            );
            INSERT INTO strategy_trigger_similarity_active_config
            VALUES (1, 'legacy-v3:w20:p3:h5:b000001.SH', '20240110', 'scope', 1);
            "#,
        )
        .expect("create legacy active config");

        ensure_ranking_tables(&conn).expect("migrate active config table");
        let active = get_strategy_trigger_similarity_active_config(&conn)
            .expect("read migrated active config")
            .expect("active config should exist");
        assert_eq!(active.algorithm_version, "legacy-v3");
        assert_eq!(active.window_trade_days, 20);
        assert_eq!(active.pool_segments, 3);
        assert_eq!(active.outcome_trade_days, 5);
        assert_eq!(active.benchmark_index_code, "000001.SH");
    }
}
