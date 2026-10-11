use crate::trigger_similarity::ranking::{
    ALGORITHM_VERSION, ActiveConfigRecord, StrategyTriggerSimilarityActiveConfig,
};

use crate::data::source_db_path;
use crate::data::stock_list_path;
use duckdb::Connection;
use duckdb::params;
use std::fs;
use std::path::Path;
use std::time::UNIX_EPOCH;
pub(super) fn open_ranking_conn(source_path: &str) -> Result<Connection, String> {
    let conn = Connection::open_in_memory().map_err(|e| format!("打开相似度查询连接失败: {e}"))?;
    for (path, alias) in [
        (crate::data::result_db_path(source_path), "trigger_score_db"),
        (source_db_path(source_path), "trigger_market_db"),
    ] {
        let path = path.to_str().ok_or("相似度输入库路径不是有效UTF-8")?;
        conn.execute_batch(&format!(
            "ATTACH '{}' AS {alias} (READ_ONLY)",
            path.replace('\'', "''")
        ))
        .map_err(|e| format!("挂载相似度输入库失败: {e}"))?;
    }
    conn.execute_batch("CREATE TEMP VIEW score_summary AS SELECT * FROM trigger_score_db.main.score_summary; CREATE TEMP VIEW rule_details AS SELECT * FROM trigger_score_db.main.rule_details")
        .map_err(|e| format!("挂载评分输入表失败: {e}"))?;
    crate::data::attach_similarity_db(&conn, source_path)?;
    Ok(conn)
}

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
            sample_gap_trade_days: sample_gap?,
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
    let query = "SELECT config_key, scope_signature, algorithm_version, \
         window_trade_days, pool_segments, outcome_trade_days, sample_gap_trade_days, \
         benchmark_index_code \
         FROM strategy_trigger_similarity_active_config WHERE id=1";
    let row = conn
        .query_row(query, [], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, i64>(5)?,
                row.get::<_, i64>(6)?,
                row.get::<_, String>(7)?,
            ))
        })
        .map(Some)
        .or_else(|error| match error {
            duckdb::Error::QueryReturnedNoRows => Ok(None),
            other => Err(other),
        })
        .map_err(|e| format!("读取走势相似生效配置失败: {e}"))?;
    row.map(
        |(key, signature, algorithm, window, pool, outcome, sample_gap, benchmark)| {
            Ok(ActiveConfigRecord {
                config: StrategyTriggerSimilarityActiveConfig {
                    algorithm_version: algorithm,
                    window_trade_days: usize::try_from(window).map_err(|_| "相似度窗口参数无效")?,
                    pool_segments: usize::try_from(pool).map_err(|_| "相似度样本池参数无效")?,
                    outcome_trade_days: usize::try_from(outcome)
                        .map_err(|_| "相似度后验参数无效")?,
                    sample_gap_trade_days: usize::try_from(sample_gap)
                        .map_err(|_| "相似度样本间隔无效")?,
                    benchmark_index_code: benchmark,
                },
                config_key: key,
                scope_signature: signature,
            })
        },
    )
    .transpose()
}

pub fn get_strategy_trigger_similarity_active_config(
    conn: &Connection,
) -> Result<Option<StrategyTriggerSimilarityActiveConfig>, String> {
    Ok(load_active_config_record(conn)?.map(|record| record.config))
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
    Some((window?, pool?, outcome?, sample_gap?, benchmark?))
}
