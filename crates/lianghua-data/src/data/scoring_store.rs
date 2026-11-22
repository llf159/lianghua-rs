use std::{
    fs::create_dir_all,
    path::Path,
    sync::{Arc, mpsc::Receiver},
    time,
};

use duckdb::{
    Appender, Connection, Transaction,
    arrow::{
        array::{
            ArrayRef, Float64Array, Int32Array, Int64Array, StringArray, builder::StringBuilder,
        },
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    },
    params,
};
use lianghua_model::scoring::{
    RankTiebreakProfile, SceneDetails, ScoreDetails, ScoreSummary, ScoreWriteMessage,
    ScoreWriteProfile, TieBreakWay,
};
use lianghua_model::{DownloadProgress, DownloadProgressCallback};

const SCORE_SUMMARY_TABLE: &str = "score_summary";
const RULE_DETAILS_TABLE: &str = "rule_details";
const SCENE_DETAILS_TABLE: &str = "scene_details";
const SCORE_SUMMARY_SHADOW_TABLE: &str = "score_summary_write_shadow";
const RULE_DETAILS_SHADOW_TABLE: &str = "rule_details_write_shadow";
const SCENE_DETAILS_SHADOW_TABLE: &str = "scene_details_write_shadow";
const SCORE_SUMMARY_STAGE_TABLE: &str = "score_summary_stage";
const SCENE_DETAILS_STAGE_TABLE: &str = "scene_details_stage";

pub fn init_result_db(db_path: &Path) -> Result<(), String> {
    let db_file = Path::new(db_path);
    if let Some(parent_dir) = db_file.parent() {
        if !parent_dir.as_os_str().is_empty() {
            create_dir_all(parent_dir).map_err(|e| format!("创建输出目录失败:{e}"))?;
        }
    }

    let conn = super::open_db_with_wal_recovery(db_path, || Connection::open(db_path))
        .map_err(|e| format!("打开数据库失败:{e}"))?;

    ensure_result_table_schema(&conn, SCORE_SUMMARY_TABLE)?;
    ensure_result_table_schema(&conn, RULE_DETAILS_TABLE)?;
    ensure_result_table_schema(&conn, SCENE_DETAILS_TABLE)?;
    (|conn: &Connection| -> Result<(), String> {
        conn.execute("DROP INDEX IF EXISTS idx_score_summary_ts_date", [])
            .map_err(|e| format!("删除score_summary冗余索引失败:{e}"))?;
        conn.execute("DROP INDEX IF EXISTS idx_rule_details_ts_date_rule", [])
            .map_err(|e| format!("删除rule_details冗余索引失败:{e}"))?;
        conn.execute("DROP INDEX IF EXISTS idx_scene_details_ts_date_scene", [])
            .map_err(|e| format!("删除scene_details冗余索引失败:{e}"))?;
        conn.execute("DROP INDEX IF EXISTS idx_score_summary_trade_date_ts", [])
            .map_err(|e| format!("删除旧score_summary索引失败:{e}"))?;
        conn.execute("DROP INDEX IF EXISTS idx_scene_details_scene_date_ts", [])
            .map_err(|e| format!("删除scene_details索引失败:{e}"))?;
        Ok(())
    })(&conn)?;
    conn.execute_batch(
        "DROP INDEX IF EXISTS idx_score_summary_trade_date_rank_ts;
         DROP INDEX IF EXISTS idx_rule_details_rule_date_ts;
         DROP INDEX IF EXISTS idx_scene_details_trade_date_scene_rank_ts;",
    )
    .map_err(|e| format!("删除结果库查询索引失败:{e}"))?;

    Ok(())
}

fn create_result_table(conn: &Connection, table_name: &str) -> Result<(), String> {
    let sql = match table_name {
        SCORE_SUMMARY_TABLE => format!(
            r#"
            CREATE TABLE IF NOT EXISTS {table_name} (
                ts_code VARCHAR NOT NULL,
                trade_date VARCHAR NOT NULL,
                total_score DOUBLE,
                rank INTEGER
            )
            "#
        ),
        RULE_DETAILS_TABLE => format!(
            r#"
            CREATE TABLE IF NOT EXISTS {table_name} (
                ts_code VARCHAR NOT NULL,
                trade_date VARCHAR NOT NULL,
                rule_name VARCHAR NOT NULL,
                rule_score DOUBLE
            )
            "#
        ),
        SCENE_DETAILS_TABLE => format!(
            r#"
            CREATE TABLE IF NOT EXISTS {table_name} (
                ts_code VARCHAR NOT NULL,
                trade_date VARCHAR NOT NULL,
                scene_name VARCHAR NOT NULL,
                direction VARCHAR,
                stage VARCHAR,
                stage_score DOUBLE,
                risk_score DOUBLE,
                confirm_strength DOUBLE,
                risk_intensity DOUBLE,
                scene_rank INTEGER
            )
            "#
        ),
        _ => return Err(format!("不支持的结果表:{table_name}")),
    };

    conn.execute(&sql, [])
        .map_err(|e| format!("创建{table_name}失败:{e}"))?;
    Ok(())
}

#[cfg(test)]
fn result_table_has_primary_key(conn: &Connection, table_name: &str) -> Result<bool, String> {
    let sql = format!("SELECT CAST(pk AS BIGINT) AS pk FROM pragma_table_info('{table_name}')");
    let mut stmt = conn
        .prepare(&sql)
        .map_err(|e| format!("准备{table_name}主键检查失败:{e}"))?;
    let mut rows = stmt
        .query([])
        .map_err(|e| format!("查询{table_name}主键信息失败:{e}"))?;
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取{table_name}主键信息失败:{e}"))?
    {
        let pk: i64 = row
            .get(0)
            .map_err(|e| format!("读取{table_name}主键标记失败:{e}"))?;
        if pk > 0 {
            return Ok(true);
        }
    }
    Ok(false)
}

fn ensure_result_table_schema(conn: &Connection, table_name: &str) -> Result<(), String> {
    if !(|conn: &Connection, table_name: &str| -> Result<bool, String> {
        let count = conn
            .query_row(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
                [table_name],
                |row| row.get::<_, i64>(0),
            )
            .map_err(|e| format!("检查{table_name}是否存在失败:{e}"))?;
        Ok(count > 0)
    })(conn, table_name)?
    {
        return create_result_table(conn, table_name);
    }

    let expected_columns = (|table_name: &str| -> Result<Vec<&'static str>, String> {
        match table_name {
            SCORE_SUMMARY_TABLE => Ok(vec!["ts_code", "trade_date", "total_score", "rank"]),
            RULE_DETAILS_TABLE => Ok(vec!["ts_code", "trade_date", "rule_name", "rule_score"]),
            SCENE_DETAILS_TABLE => Ok(vec![
                "ts_code",
                "trade_date",
                "scene_name",
                "direction",
                "stage",
                "stage_score",
                "risk_score",
                "confirm_strength",
                "risk_intensity",
                "scene_rank",
            ]),
            _ => Err(format!("不支持的结果表:{table_name}")),
        }
    })(table_name)?;
    let actual_columns = (|conn: &Connection, table_name: &str| -> Result<Vec<String>, String> {
        let mut stmt = conn
        .prepare(
            "SELECT column_name FROM information_schema.columns WHERE table_name = ? ORDER BY ordinal_position",
        )
        .map_err(|e| format!("准备{table_name}列结构查询失败:{e}"))?;
        let mut rows = stmt
            .query([table_name])
            .map_err(|e| format!("查询{table_name}列结构失败:{e}"))?;
        let mut columns = Vec::new();
        while let Some(row) = rows
            .next()
            .map_err(|e| format!("读取{table_name}列结构失败:{e}"))?
        {
            columns.push(
                row.get::<_, String>(0)
                    .map_err(|e| format!("读取{table_name}列名失败:{e}"))?,
            );
        }
        Ok(columns)
    })(conn, table_name)?;
    let columns_match = actual_columns
        == expected_columns
            .iter()
            .map(|column| column.to_string())
            .collect::<Vec<_>>();
    // Accept both legacy indexed tables and new tables validated before publication.
    if columns_match {
        return Ok(());
    }

    conn.execute(&format!("DROP TABLE {table_name}"), [])
        .map_err(|e| format!("删除旧{table_name}失败:{e}"))?;
    create_result_table(conn, table_name)
}

fn delete_convolution_rank_range(
    tx: &Transaction<'_>,
    start_date: &str,
    end_date: &str,
) -> Result<(), String> {
    let convolution_rank_exists = tx
        .query_row(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'convolution_rank'",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map_err(|e| format!("检查convolution_rank表失败:{e}"))?;
    if convolution_rank_exists > 0 {
        tx.execute(
            "DELETE FROM convolution_rank WHERE trade_date >= ? AND trade_date <= ?",
            params![start_date, end_date],
        )
        .map_err(|e| format!("清理过期卷积排名失败:{e}"))?;
    }
    Ok(())
}

fn score_string_array(values: StringArray) -> ArrayRef {
    Arc::new(values)
}

fn score_float64_array(values: Vec<f64>) -> ArrayRef {
    Arc::new(Float64Array::from(values))
}

fn duckdb_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn scene_stage_rank_weight(stage: Option<&str>) -> i32 {
    match stage {
        Some("confirm") => 3,
        Some("trigger") => 2,
        Some("observe") => 1,
        Some("fail") => 0,
        _ => -1,
    }
}

pub fn rank_summary_rows_by_score(rows: &mut [ScoreSummary]) {
    rows.sort_by(|left, right| {
        left.trade_date
            .cmp(&right.trade_date)
            .then_with(|| {
                right
                    .total_score
                    .partial_cmp(&left.total_score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .then_with(|| left.ts_code.cmp(&right.ts_code))
    });

    let mut current_trade_date: Option<&str> = None;
    let mut current_rank = 0i64;
    for row in rows {
        if current_trade_date != Some(row.trade_date.as_str()) {
            current_trade_date = Some(row.trade_date.as_str());
            current_rank = 1;
        } else {
            current_rank += 1;
        }
        row.rank = Some(current_rank);
    }
}

pub fn rank_scene_rows(rows: &mut [SceneDetails]) {
    rows.sort_by(|left, right| {
        left.trade_date
            .cmp(&right.trade_date)
            .then_with(|| left.scene_name.cmp(&right.scene_name))
            .then_with(|| {
                scene_stage_rank_weight(right.stage.as_deref())
                    .cmp(&scene_stage_rank_weight(left.stage.as_deref()))
            })
            .then_with(|| {
                descending_rank_score(Some(left.confirm_strength), Some(right.confirm_strength))
            })
            .then_with(|| {
                let right_net = right.confirm_strength - right.risk_intensity;
                let left_net = left.confirm_strength - left.risk_intensity;
                descending_rank_score(Some(left_net), Some(right_net))
            })
            .then_with(|| {
                let right_balance = right.stage_score.abs() - right.risk_score.abs();
                let left_balance = left.stage_score.abs() - left.risk_score.abs();
                descending_rank_score(Some(left_balance), Some(right_balance))
            })
            .then_with(|| descending_rank_score(Some(left.total_score), Some(right.total_score)))
            .then_with(|| left.ts_code.cmp(&right.ts_code))
    });

    let mut current_key: Option<(&str, &str)> = None;
    let mut current_rank = 0i64;
    for row in rows {
        let key = (row.trade_date.as_str(), row.scene_name.as_str());
        if current_key != Some(key) {
            current_key = Some(key);
            current_rank = 1;
        } else {
            current_rank += 1;
        }
        row.scene_rank = Some(current_rank);
    }
}

// DuckDB orders NaN above other numbers, treats signed zero as equal, and puts
// NULL last for DESC. Keep both in-memory and persisted scene ranks identical.
fn descending_rank_score(left: Option<f64>, right: Option<f64>) -> std::cmp::Ordering {
    match (left, right) {
        (Some(left), Some(right)) => right
            .partial_cmp(&left)
            .unwrap_or_else(|| right.is_nan().cmp(&left.is_nan())),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => std::cmp::Ordering::Equal,
    }
}

pub(crate) fn build_tiebreak_rank_sql(tie_break: TieBreakWay, adj_type: &str) -> String {
    match tie_break {
        TieBreakWay::TsCode => r#"
            UPDATE score_summary AS s
            SET rank = r.new_rank
            FROM (
                SELECT
                    ts_code,
                    trade_date,
                    ROW_NUMBER() OVER (
                        PARTITION BY trade_date
                        ORDER BY total_score DESC, ts_code ASC
                    ) AS new_rank
                FROM score_summary
            ) AS r
            WHERE s.ts_code = r.ts_code
              AND s.trade_date = r.trade_date
            "#
        .to_string(),
        TieBreakWay::KdjJ => format!(
            r#"
                UPDATE score_summary AS s
                SET rank = r.new_rank
                FROM (
                    SELECT
                        s.ts_code,
                        s.trade_date,
                        ROW_NUMBER() OVER (
                            PARTITION BY s.trade_date
                            ORDER BY
                                s.total_score DESC,
                                src.j ASC NULLS LAST,
                                s.ts_code ASC
                        ) AS new_rank
                    FROM score_summary AS s
                    LEFT JOIN src_db.stock_data AS src
                      ON s.ts_code = src.ts_code
                     AND s.trade_date = src.trade_date
                     AND src.adj_type = '{}'
                ) AS r
                WHERE s.ts_code = r.ts_code
                  AND s.trade_date = r.trade_date
                "#,
            adj_type.replace("'", "''")
        ),
    }
}

pub fn build_rank_tiebreak(
    result_db_path: &str,
    source_db_path: &str,
    adj_type: &str,
    tie_break: TieBreakWay,
) -> Result<RankTiebreakProfile, String> {
    let total_started_at = time::Instant::now();
    let mut profile = RankTiebreakProfile::default();
    let conn = super::open_db_with_wal_recovery(Path::new(result_db_path), || {
        Connection::open(result_db_path)
    })
    .map_err(|e| format!("结果库连接失败:{e}"))?;

    if let TieBreakWay::KdjJ = tie_break {
        let attach_started_at = time::Instant::now();
        let attach_sql = format!(
            "ATTACH {} AS src_db (READ_ONLY)",
            duckdb_string_literal(source_db_path)
        );
        conn.execute(&attach_sql, [])
            .map_err(|e| format!("附加原始库失败:{e}"))?;
        profile.attach_source_db_ms = Some(attach_started_at.elapsed().as_millis() as u64);
    }

    let sql = build_tiebreak_rank_sql(tie_break, adj_type);
    let update_started_at = time::Instant::now();
    conn.execute(&sql, [])
        .map_err(|e| format!("补rank失败:{e}"))?;
    profile.update_rank_ms = update_started_at.elapsed().as_millis() as u64;

    if let TieBreakWay::KdjJ = tie_break {
        let detach_started_at = time::Instant::now();
        let _ = conn.execute("DETACH src_db", []);
        profile.detach_source_db_ms = Some(detach_started_at.elapsed().as_millis() as u64);
    }

    profile.total_ms = total_started_at.elapsed().as_millis() as u64;
    println!(
        "补排名耗时: 总计={}ms；补排名={}ms",
        profile.total_ms, profile.update_rank_ms,
    );
    Ok(profile)
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        sync::mpsc::channel,
        time::{SystemTime, UNIX_EPOCH},
    };

    use duckdb::Connection;

    use super::{
        SceneDetails, ScoreDetails, ScoreSummary, ScoreWriteMessage, build_tiebreak_rank_sql,
        init_result_db, rank_scene_rows, result_table_has_primary_key,
        write_score_batches_from_channel,
    };
    use lianghua_model::scoring::{ScoreBatch, TieBreakWay};

    #[test]
    fn tiebreak_rank_sql_uses_stable_order_and_escapes_adj_type() {
        let ts_code_sql = build_tiebreak_rank_sql(TieBreakWay::TsCode, "qfq");
        assert!(ts_code_sql.contains("total_score DESC, ts_code ASC"));

        let kdj_sql = build_tiebreak_rank_sql(TieBreakWay::KdjJ, "q'f'q");
        assert!(kdj_sql.contains("src.j ASC NULLS LAST"));
        assert!(kdj_sql.contains("src.adj_type = 'q''f''q'"));
    }

    fn scene_row(
        ts_code: &str,
        scene_name: &str,
        stage: &str,
        stage_score: f64,
        risk_score: f64,
        confirm_strength: f64,
        risk_intensity: f64,
        total_score: f64,
    ) -> SceneDetails {
        SceneDetails {
            ts_code: ts_code.to_string(),
            trade_date: "20240102".to_string(),
            scene_name: scene_name.to_string(),
            direction: "long".to_string(),
            stage: Some(stage.to_string()),
            stage_score,
            risk_score,
            confirm_strength,
            risk_intensity,
            total_score,
            scene_rank: None,
        }
    }

    #[test]
    fn completed_scores_resume_ranking_after_publish_is_interrupted() {
        let directory = std::env::temp_dir().join(format!(
            "lianghua-score-resume-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&directory).unwrap();
        let path = directory.join("scoring_result.db");
        let path = path.to_str().unwrap();
        init_result_db(std::path::Path::new(path)).unwrap();
        let (tx, rx) = channel();
        tx.send(ScoreWriteMessage::Begin {
            resume_key: "unfinished".into(),
            resume: false,
        })
        .unwrap();
        drop(tx);
        assert!(
            write_score_batches_from_channel(
                path,
                None,
                "qfq",
                TieBreakWay::TsCode,
                "20240102",
                "20240102",
                rx,
                None
            )
            .unwrap_err()
            .contains("未完整结束")
        );
        assert!(!super::score_compute_checkpoint_matches(path, "unfinished").unwrap());

        let (tx, rx) = channel();
        tx.send(ScoreWriteMessage::Begin {
            resume_key: "same-inputs".into(),
            resume: false,
        })
        .unwrap();
        tx.send(ScoreWriteMessage::StrategySnapshot("saved strategy".into()))
            .unwrap();
        tx.send(ScoreWriteMessage::Batch(ScoreBatch {
            summary_rows: vec![ScoreSummary {
                ts_code: "000001.SZ".into(),
                trade_date: "20240102".into(),
                total_score: 9.0,
                rank: None,
            }],
            detail_rows: vec![ScoreDetails {
                ts_code: "000001.SZ".into(),
                trade_date: "20240102".into(),
                rule_name: "触发".into(),
                rule_score: 9.0,
            }],
            scene_rows: vec![scene_row(
                "000001.SZ",
                "主升",
                "confirm",
                9.0,
                0.0,
                1.0,
                0.0,
                9.0,
            )],
            ..ScoreBatch::default()
        }))
        .unwrap();
        tx.send(ScoreWriteMessage::Finish).unwrap();
        drop(tx);
        let control = std::sync::Arc::new(lianghua_model::task::TaskControl::default());
        let active = lianghua_model::task::ActiveTask::enter_isolated(vec![control.clone()]);
        let progress = |event: lianghua_model::DownloadProgress| {
            if event.phase == "score_publish" {
                let _ = control.request();
            }
        };
        assert!(
            write_score_batches_from_channel(
                path,
                None,
                "qfq",
                TieBreakWay::TsCode,
                "20240101",
                "20240107",
                rx,
                Some(&progress)
            )
            .unwrap_err()
            .contains("任务已停止")
        );
        drop(active);
        assert!(super::score_compute_checkpoint_matches(path, "same-inputs").unwrap());
        assert!(!super::score_compute_checkpoint_matches(path, "changed-inputs").unwrap());
        let conn = Connection::open(path).unwrap();
        let checkpoint = super::read_score_compute_checkpoint(&conn)
            .unwrap()
            .unwrap();
        let parameters = checkpoint.parameters.unwrap();
        assert_eq!(parameters.start_date, "20240101");
        assert_eq!(parameters.end_date, "20240107");
        assert_eq!(parameters.adj_type, "qfq");
        assert_eq!(
            conn.query_row("SELECT COUNT(*) FROM score_summary", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            0
        );
        drop(conn);

        // No score/trigger batches are sent on resume after validation and interrupted publication.
        let (tx, rx) = channel();
        tx.send(ScoreWriteMessage::Begin {
            resume_key: "same-inputs".into(),
            resume: true,
        })
        .unwrap();
        tx.send(ScoreWriteMessage::Finish).unwrap();
        drop(tx);
        write_score_batches_from_channel(
            path,
            None,
            "qfq",
            TieBreakWay::TsCode,
            "20240101",
            "20240107",
            rx,
            None,
        )
        .unwrap();
        assert!(!super::score_compute_checkpoint_matches(path, "same-inputs").unwrap());
        let conn = Connection::open(path).unwrap();
        assert!(
            super::read_score_compute_checkpoint(&conn)
                .unwrap()
                .is_none()
        );
        assert_eq!(
            conn.query_row("SELECT total_score, rank FROM score_summary", [], |row| Ok(
                (row.get::<_, f64>(0)?, row.get::<_, i64>(1)?)
            ))
            .unwrap(),
            (9.0, 1)
        );
        assert_eq!(
            conn.query_row("SELECT scene_rank FROM scene_details", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            1
        );
        assert_eq!(
            conn.query_row("SELECT rule_name FROM rule_details", [], |row| row
                .get::<_, String>(0))
                .unwrap(),
            "触发"
        );
        assert_eq!(
            conn.query_row(
                "SELECT strategy_text FROM rank_compute_strategy",
                [],
                |row| row.get::<_, String>(0)
            )
            .unwrap(),
            "saved strategy"
        );
        drop(conn);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn legacy_score_checkpoint_keeps_resuming_without_inventing_parameters() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE score_compute_checkpoint(resume_key VARCHAR, strategy_snapshot VARCHAR);
            INSERT INTO score_compute_checkpoint VALUES ('legacy-key', 'strategy')",
        )
        .unwrap();
        let checkpoint = super::read_score_compute_checkpoint(&conn)
            .unwrap()
            .unwrap();
        assert!(checkpoint.parameters.is_none());
        assert!(super::score_compute_checkpoint_matches_on(&conn, "legacy-key").unwrap());
        assert!(!super::score_compute_checkpoint_matches_on(&conn, "other-key").unwrap());
    }

    #[test]
    fn scene_rank_batches_preserve_whole_dates_and_match_single_window() {
        let mut conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            r#"
            CREATE TABLE scene_details_stage AS
            SELECT printf('%06d.SZ', i) AS ts_code,
                (20240102 + CASE WHEN i < 120 THEN 0 ELSE (i - 120) // 40 + 1 END)::VARCHAR AS trade_date,
                CASE WHEN i % 2 = 0 THEN '主升·场景' ELSE '防守' END AS scene_name,
                'long' AS direction,
                CASE i % 5 WHEN 0 THEN 'confirm' WHEN 1 THEN 'trigger'
                    WHEN 2 THEN 'observe' WHEN 3 THEN 'fail' ELSE NULL END AS stage,
                (i % 7 - 3)::DOUBLE AS stage_score,
                (i % 3)::DOUBLE AS risk_score,
                CASE WHEN i % 13 = 0 THEN NULL ELSE (i % 4)::DOUBLE END AS confirm_strength,
                (i % 2)::DOUBLE AS risk_intensity,
                CASE WHEN i % 17 = 0 THEN 'NaN'::DOUBLE ELSE (i % 5)::DOUBLE END AS total_score
            FROM range(240) r(i);
            CREATE TABLE expected AS
            SELECT * EXCLUDE(total_score),
                CAST(ROW_NUMBER() OVER (
                    PARTITION BY trade_date, scene_name
                    ORDER BY CASE stage WHEN 'confirm' THEN 3 WHEN 'trigger' THEN 2
                        WHEN 'observe' THEN 1 WHEN 'fail' THEN 0 ELSE -1 END DESC,
                        confirm_strength DESC, (confirm_strength - risk_intensity) DESC,
                        (abs(stage_score) - abs(risk_score)) DESC, total_score DESC, ts_code ASC
                ) AS INTEGER) AS scene_rank
            FROM scene_details_stage;
            CREATE TABLE scene_details_write_shadow AS SELECT * FROM expected WHERE false;
            "#,
        )
        .unwrap();
        let counts = std::sync::Mutex::new(Vec::new());
        let progress = |event: lianghua_model::DownloadProgress| {
            counts.lock().unwrap().push((event.finished, event.total));
        };
        // One oversized date stays whole; the next two dates share a batch; the tail is flushed.
        super::write_scene_rank_batches(&mut conn, 85, Some(&progress)).unwrap();
        let mut counts = counts.into_inner().unwrap();
        counts.dedup();
        assert_eq!(counts, [(0, 4), (1, 4), (3, 4), (4, 4)]);
        assert_eq!(
            conn.query_row(
                "SELECT COUNT(*) FROM ((SELECT * FROM expected EXCEPT ALL SELECT * FROM scene_details_write_shadow)
                 UNION ALL (SELECT * FROM scene_details_write_shadow EXCEPT ALL SELECT * FROM expected))",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
            0
        );
    }

    #[test]
    fn result_key_validation_preserves_date_partitions_and_rejects_duplicates_and_nulls() {
        let mut conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE details(ts_code VARCHAR, trade_date VARCHAR, rule_name VARCHAR);
             INSERT INTO details VALUES ('A', '20240102', '规则'), ('B', '20240102', '规则'),
                 ('A', '20240103', '规则');",
        )
        .unwrap();
        super::validate_result_keys(
            &mut conn,
            "details",
            &["ts_code", "trade_date", "rule_name"],
            1,
            None,
        )
        .unwrap();
        assert!(!result_table_has_primary_key(&conn, "details").unwrap());
        for values in [
            "('A', '20240102', '规则')",
            "(NULL, '20240102', '规则')",
            "('C', '20240102', NULL)",
            "('C', NULL, '规则')",
        ] {
            conn.execute_batch(&format!("INSERT INTO details VALUES {values}"))
                .unwrap();
            assert!(
                super::validate_result_keys(
                    &mut conn,
                    "details",
                    &["ts_code", "trade_date", "rule_name"],
                    1,
                    None,
                )
                .is_err()
            );
            conn.execute_batch(
                "DELETE FROM details WHERE rowid = (SELECT max(rowid) FROM details)",
            )
            .unwrap();
        }
    }

    #[test]
    fn rank_scene_rows_matches_scene_rank_ordering() {
        let mut rows = vec![
            scene_row("000004.SZ", "主升", "trigger", 9.0, 1.0, 1.0, 0.0, 100.0),
            scene_row("000003.SZ", "主升", "confirm", 5.0, 1.0, 2.0, 0.0, 70.0),
            scene_row("000002.SZ", "主升", "confirm", 5.0, 1.0, 2.0, 0.0, 80.0),
            scene_row("000001.SZ", "主升", "confirm", 8.0, 1.0, 2.0, 0.0, 80.0),
            scene_row("000005.SZ", "防守", "observe", 3.0, 0.0, 0.5, 0.0, 50.0),
        ];

        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let db_path = std::env::temp_dir().join(format!("lianghua_scene_rank_{unique}.db"));
        init_result_db(&db_path).unwrap();
        let (tx, rx) = channel();
        for group in rows.chunks(2) {
            tx.send(ScoreWriteMessage::Batch(ScoreBatch {
                scene_rows: group.to_vec(),
                ..ScoreBatch::default()
            }))
            .unwrap();
        }
        drop(tx);
        write_score_batches_from_channel(
            db_path.to_str().unwrap(),
            None,
            "qfq",
            TieBreakWay::TsCode,
            "20240102",
            "20240102",
            rx,
            None,
        )
        .unwrap();

        rank_scene_rows(&mut rows);

        let main_scene = rows
            .iter()
            .filter(|row| row.scene_name == "主升")
            .map(|row| (row.ts_code.as_str(), row.scene_rank))
            .collect::<Vec<_>>();
        assert_eq!(
            main_scene,
            vec![
                ("000001.SZ", Some(1)),
                ("000002.SZ", Some(2)),
                ("000003.SZ", Some(3)),
                ("000004.SZ", Some(4)),
            ]
        );

        let defense_rank = rows
            .iter()
            .find(|row| row.scene_name == "防守")
            .and_then(|row| row.scene_rank);
        assert_eq!(defense_rank, Some(1));
        let conn = Connection::open(&db_path).unwrap();
        for row in &rows {
            let stored_rank: i64 = conn
                .query_row(
                    "SELECT scene_rank FROM scene_details WHERE ts_code = ? AND scene_name = ?",
                    duckdb::params![row.ts_code, row.scene_name],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(Some(stored_rank), row.scene_rank);
        }
        drop(conn);
        fs::remove_file(db_path).unwrap();
    }

    fn explicit_result_index_names(conn: &Connection) -> Vec<String> {
        let mut stmt = conn
            .prepare(
                r#"
                SELECT index_name
                FROM duckdb_indexes()
                WHERE table_name IN ('score_summary', 'rule_details', 'scene_details')
                ORDER BY index_name
                "#,
            )
            .expect("prepare index query");
        stmt.query_map([], |row| row.get::<_, String>(0))
            .expect("query indexes")
            .collect::<Result<Vec<_>, _>>()
            .expect("collect indexes")
    }

    #[test]
    fn init_result_db_removes_primary_key_duplicate_indexes() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        let temp_dir = std::env::temp_dir().join(format!("lianghua_score_indexes_{unique}"));
        fs::create_dir_all(&temp_dir).expect("create temp dir");
        let db_path = temp_dir.join("scoring_result.db");
        init_result_db(&db_path).expect("init db");

        let conn = Connection::open(&db_path).expect("open result db");
        conn.execute_batch(
            r#"
            CREATE INDEX idx_score_summary_trade_date_rank_ts ON score_summary(trade_date, rank, ts_code);
            CREATE INDEX idx_rule_details_rule_date_ts ON rule_details(rule_name, trade_date, ts_code);
            CREATE INDEX idx_scene_details_trade_date_scene_rank_ts ON scene_details(trade_date, scene_name, scene_rank, ts_code);
            CREATE INDEX idx_score_summary_ts_date
                ON score_summary(ts_code, trade_date);
            CREATE INDEX idx_rule_details_ts_date_rule
                ON rule_details(ts_code, trade_date, rule_name);
            CREATE INDEX idx_scene_details_ts_date_scene
                ON scene_details(ts_code, trade_date, scene_name);
            "#,
        )
        .expect("create redundant indexes");
        drop(conn);

        init_result_db(&db_path).expect("migrate indexes");
        let conn = Connection::open(&db_path).expect("reopen result db");
        assert_eq!(explicit_result_index_names(&conn), Vec::<String>::new());

        drop(conn);
        fs::remove_dir_all(temp_dir).expect("remove temp dir");
    }

    #[test]
    fn incremental_score_write_keeps_other_dates_and_indexes() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        let temp_dir = std::env::temp_dir().join(format!("lianghua_score_incremental_{unique}"));
        fs::create_dir_all(&temp_dir).expect("create temp dir");
        let db_path = temp_dir.join("scoring_result.db");
        init_result_db(&db_path).expect("init db");
        let db_path_str = db_path.to_str().expect("db path utf8");

        for (trade_date, total_score) in [("20240102", 1.0), ("20240103", 2.0)] {
            let (tx, rx) = channel();
            tx.send(ScoreWriteMessage::StrategySnapshot(format!(
                "strategy-{trade_date}"
            )))
            .unwrap();
            tx.send(ScoreWriteMessage::Batch(ScoreBatch {
                summary_rows: vec![ScoreSummary {
                    ts_code: "000001.SZ".to_string(),
                    trade_date: trade_date.to_string(),
                    total_score,
                    rank: None,
                }],
                ..ScoreBatch::default()
            }))
            .expect("send batch");
            drop(tx);
            write_score_batches_from_channel(
                db_path_str,
                None,
                "qfq",
                TieBreakWay::TsCode,
                trade_date,
                trade_date,
                rx,
                None,
            )
            .expect("write score batch");
        }

        init_result_db(&db_path).expect("retain unindexed published results");
        let conn = Connection::open(&db_path).expect("open result db");
        assert_eq!(
            conn.query_row("SELECT COUNT(*) FROM score_summary", [], |row| {
                row.get::<_, i64>(0)
            })
            .expect("count summary rows"),
            2
        );
        assert_eq!(conn.query_row("SELECT string_agg(strategy_text, ',' ORDER BY trade_date) FROM rank_compute_strategy", [], |row| row.get::<_, String>(0)).unwrap(), "strategy-20240102,strategy-20240103");
        assert_eq!(explicit_result_index_names(&conn), Vec::<String>::new());
        for table_name in ["score_summary", "rule_details", "scene_details"] {
            assert!(
                !result_table_has_primary_key(&conn, table_name).expect("check primary key"),
                "{table_name} should not build a history-sized index"
            );
        }

        drop(conn);
        fs::remove_dir_all(temp_dir).expect("remove temp dir");
    }

    #[test]
    fn multi_batch_summary_rank_keeps_order_and_cleans_staging_tables() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        let temp_dir = std::env::temp_dir().join(format!("lianghua_score_chunked_{unique}"));
        fs::create_dir_all(&temp_dir).expect("create temp dir");
        let db_path = temp_dir.join("scoring_result.db");
        init_result_db(&db_path).expect("init db");

        let trade_dates = (0..150)
            .map(|index| format!("2024{:02}{:02}", index / 28 + 1, index % 28 + 1))
            .collect::<Vec<_>>();
        let mut summary_rows = Vec::new();
        for trade_date in &trade_dates {
            for (ts_code, total_score) in [("000001.SZ", 1.0), ("000002.SZ", 2.0)] {
                summary_rows.push(ScoreSummary {
                    ts_code: ts_code.to_string(),
                    trade_date: trade_date.clone(),
                    total_score,
                    rank: None,
                });
            }
        }

        let tail_rows = summary_rows.split_off(summary_rows.len() / 2);
        let (tx, rx) = channel();
        for group in [tail_rows, summary_rows] {
            tx.send(ScoreWriteMessage::Batch(ScoreBatch {
                summary_rows: group,
                ..ScoreBatch::default()
            }))
            .expect("send batch");
        }
        drop(tx);
        write_score_batches_from_channel(
            db_path.to_str().expect("db path utf8"),
            None,
            "qfq",
            TieBreakWay::TsCode,
            "20240101",
            "20240611",
            rx,
            None,
        )
        .expect("write score batches");

        let conn = Connection::open(&db_path).expect("open result db");
        let ranked = conn
            .query_row(
                "SELECT COUNT(*) FROM score_summary WHERE rank IS NOT NULL",
                [],
                |row| row.get::<_, i64>(0),
            )
            .expect("count ranked rows");
        assert_eq!(ranked, (trade_dates.len() * 2) as i64);
        for trade_date in [&trade_dates[0], &trade_dates[64], &trade_dates[149]] {
            let ranks = conn
                .query_row(
                    r#"
                    SELECT
                        MAX(CASE WHEN ts_code = '000002.SZ' THEN rank END),
                        MAX(CASE WHEN ts_code = '000001.SZ' THEN rank END)
                    FROM score_summary WHERE trade_date = ?
                    "#,
                    [trade_date],
                    |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
                )
                .expect("read ranks");
            assert_eq!(ranks, (1, 2), "unexpected ranks on {trade_date}");
        }
        assert_eq!(
            conn.query_row(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE '%write_shadow' OR table_name IN ('score_summary_stage', 'scene_details_stage')",
                [],
                |row| row.get::<_, i64>(0),
            )
            .expect("count staging tables"),
            0
        );

        drop(conn);
        fs::remove_dir_all(temp_dir).expect("remove temp dir");
    }

    #[test]
    fn interrupted_full_replace_keeps_official_tables_unchanged() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        let temp_dir = std::env::temp_dir().join(format!("lianghua_score_rollback_{unique}"));
        fs::create_dir_all(&temp_dir).expect("create temp dir");
        let db_path = temp_dir.join("scoring_result.db");
        init_result_db(&db_path).expect("init db");
        let conn = Connection::open(&db_path).expect("open result db");
        conn.execute(
            "INSERT INTO score_summary VALUES ('000001.SZ', '20240102', 9.0, 1)",
            [],
        )
        .expect("insert existing summary");
        conn.execute_batch("CREATE TABLE rank_compute_strategy (trade_date VARCHAR PRIMARY KEY, strategy_text VARCHAR NOT NULL); INSERT INTO rank_compute_strategy VALUES ('20240102', 'old')").unwrap();
        drop(conn);

        let (tx, rx) = channel();
        tx.send(ScoreWriteMessage::StrategySnapshot("new".into()))
            .unwrap();
        tx.send(ScoreWriteMessage::Abort("test abort".to_string()))
            .expect("send abort");
        drop(tx);
        let result = write_score_batches_from_channel(
            db_path.to_str().expect("db path utf8"),
            None,
            "qfq",
            TieBreakWay::TsCode,
            "20240102",
            "20240102",
            rx,
            None,
        );
        assert!(result.is_err());

        let conn = Connection::open(&db_path).expect("reopen result db");
        assert_eq!(
            conn.query_row(
                "SELECT strategy_text FROM rank_compute_strategy",
                [],
                |row| row.get::<_, String>(0)
            )
            .unwrap(),
            "old"
        );
        assert_eq!(
            conn.query_row(
                "SELECT total_score FROM score_summary WHERE ts_code = '000001.SZ' AND trade_date = '20240102'",
                [],
                |row| row.get::<_, f64>(0),
            )
            .expect("read existing summary"),
            9.0
        );
        assert_eq!(
            conn.query_row(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE '%write_shadow' OR table_name IN ('score_summary_stage', 'scene_details_stage')",
                [],
                |row| row.get::<_, i64>(0),
            )
            .expect("count staging tables"),
            0
        );

        drop(conn);
        fs::remove_dir_all(temp_dir).expect("remove temp dir");
    }

    #[test]
    fn write_score_batches_generates_kdj_summary_rank_on_insert() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        let temp_dir = std::env::temp_dir().join(format!("lianghua_score_write_{unique}"));
        fs::create_dir_all(&temp_dir).expect("create temp dir");
        let db_path = temp_dir.join("scoring_result.db");
        init_result_db(&db_path).expect("init db");
        let source_db_path = temp_dir.join("stock_data.db");
        let source_conn = Connection::open(&source_db_path).expect("open source db");
        source_conn
            .execute(
                r#"
                CREATE TABLE stock_data (
                    ts_code VARCHAR,
                    trade_date VARCHAR,
                    adj_type VARCHAR,
                    j DOUBLE
                )
                "#,
                [],
            )
            .expect("create source table");
        source_conn
            .execute(
                r#"
                INSERT INTO stock_data VALUES
                    ('000001.SZ', '20240102', 'qfq', 8.0),
                    ('000002.SZ', '20240102', 'qfq', 9.0),
                    ('000003.SZ', '20240102', 'qfq', 1.0),
                    ('000001.SZ', '20240103', 'qfq', 5.0),
                    ('000002.SZ', '20240103', 'qfq', 7.0),
                    ('000001.SZ', '20230102', 'qfq', -100.0),
                    ('000001.SZ', '20250102', 'qfq', -100.0),
                    ('000001.SZ', '20240102', 'hfq', -100.0)
                "#,
                [],
            )
            .expect("insert source rows");
        drop(source_conn);

        let (tx, rx) = channel();
        tx.send(ScoreWriteMessage::Batch(ScoreBatch {
            summary_rows: vec![
                ScoreSummary {
                    ts_code: "000001.SZ".to_string(),
                    trade_date: "20240102".to_string(),
                    total_score: 1.0,
                    rank: None,
                },
                ScoreSummary {
                    ts_code: "000003.SZ".to_string(),
                    trade_date: "20240102".to_string(),
                    total_score: 3.0,
                    rank: None,
                },
                ScoreSummary {
                    ts_code: "000002.SZ".to_string(),
                    trade_date: "20240102".to_string(),
                    total_score: 3.0,
                    rank: None,
                },
                ScoreSummary {
                    ts_code: "000001.SZ".to_string(),
                    trade_date: "20240103".to_string(),
                    total_score: 2.0,
                    rank: None,
                },
                ScoreSummary {
                    ts_code: "000002.SZ".to_string(),
                    trade_date: "20240103".to_string(),
                    total_score: 5.0,
                    rank: None,
                },
            ],
            detail_rows: vec![ScoreDetails {
                ts_code: "000001.SZ".to_string(),
                trade_date: "20240102".to_string(),
                rule_name: "测试规则".to_string(),
                rule_score: 1.25,
            }],
            compact_rule_rows: Vec::new(),
            scene_rows: vec![
                SceneDetails {
                    ts_code: "000001.SZ".to_string(),
                    trade_date: "20240102".to_string(),
                    scene_name: "场景A".to_string(),
                    direction: "long".to_string(),
                    stage: Some("confirm".to_string()),
                    stage_score: 2.0,
                    risk_score: 0.5,
                    confirm_strength: 0.8,
                    risk_intensity: 0.2,
                    total_score: 1.0,
                    scene_rank: None,
                },
                SceneDetails {
                    ts_code: "000002.SZ".to_string(),
                    trade_date: "20240102".to_string(),
                    scene_name: "场景B".to_string(),
                    direction: "long".to_string(),
                    stage: None,
                    stage_score: 1.0,
                    risk_score: 0.2,
                    confirm_strength: 0.4,
                    risk_intensity: 0.1,
                    total_score: 3.0,
                    scene_rank: None,
                },
            ],
            scene_backtest_rows: Vec::new(),
            tracking_states: Vec::new(),
        }))
        .expect("send batch");
        drop(tx);

        let db_path_str = db_path.to_str().expect("db path utf8");
        let source_db_path_str = source_db_path.to_str().expect("source db path utf8");
        let progress = std::sync::Mutex::new(Vec::new());
        let on_progress = |event: lianghua_model::DownloadProgress| {
            progress.lock().unwrap().push(event);
        };
        write_score_batches_from_channel(
            db_path_str,
            Some(source_db_path_str),
            "qfq",
            TieBreakWay::KdjJ,
            "20240102",
            "20240103",
            rx,
            Some(&on_progress),
        )
        .expect("write score batches");
        let events = progress.into_inner().unwrap();
        let mut completed: Vec<_> = events
            .iter()
            .filter(|event| event.total > 0 && event.finished == event.total)
            .map(|event| event.phase.as_str())
            .collect();
        completed.dedup();
        assert_eq!(
            completed,
            [
                "score_scene_rank",
                "score_summary_rank",
                "score_keys",
                "score_publish",
                "score_checkpoint"
            ]
        );

        let conn = Connection::open(&db_path).expect("open result db");
        let mut stmt = conn
            .prepare(
                r#"
                SELECT ts_code, trade_date, CAST(rank AS BIGINT) AS rank
                FROM score_summary
                ORDER BY trade_date ASC, rank ASC
                "#,
            )
            .expect("prepare query");
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            })
            .expect("query rows")
            .collect::<Result<Vec<_>, _>>()
            .expect("collect rows");

        assert_eq!(
            rows,
            vec![
                ("000003.SZ".to_string(), "20240102".to_string(), 1),
                ("000002.SZ".to_string(), "20240102".to_string(), 2),
                ("000001.SZ".to_string(), "20240102".to_string(), 3),
                ("000002.SZ".to_string(), "20240103".to_string(), 1),
                ("000001.SZ".to_string(), "20240103".to_string(), 2),
            ]
        );

        drop(stmt);
        let detail_count = conn
            .query_row("SELECT COUNT(*) FROM rule_details", [], |row| {
                row.get::<_, i64>(0)
            })
            .expect("count detail rows");
        assert_eq!(detail_count, 1);
        let null_stage_count = conn
            .query_row(
                "SELECT COUNT(*) FROM scene_details WHERE stage IS NULL",
                [],
                |row| row.get::<_, i64>(0),
            )
            .expect("count null scene stages");
        assert_eq!(null_stage_count, 1);
        let ranked_scene_count = conn
            .query_row(
                "SELECT COUNT(*) FROM scene_details WHERE scene_rank = 1",
                [],
                |row| row.get::<_, i64>(0),
            )
            .expect("count ranked scene rows");
        assert_eq!(ranked_scene_count, 2);

        drop(conn);
        fs::remove_dir_all(temp_dir).expect("remove temp dir");
    }
}

fn drop_result_staging_tables(conn: &Connection) -> Result<(), String> {
    conn.execute_batch(&format!(
        r#"
        DROP TABLE IF EXISTS {SCORE_SUMMARY_STAGE_TABLE};
        DROP TABLE IF EXISTS {SCORE_SUMMARY_SHADOW_TABLE};
        DROP TABLE IF EXISTS {RULE_DETAILS_SHADOW_TABLE};
        DROP TABLE IF EXISTS {SCENE_DETAILS_STAGE_TABLE};
        DROP TABLE IF EXISTS {SCENE_DETAILS_SHADOW_TABLE};
        DROP TABLE IF EXISTS strategy_tracking_state_stage;
        DROP TABLE IF EXISTS strategy_tracking_meta_stage;
        DROP TABLE IF EXISTS score_compute_checkpoint;
        "#
    ))
    .map_err(|e| format!("清理结果库暂存表失败:{e}"))
}

// Poll the executing connection without issuing another SQL query or splitting the sort.
fn score_write_stage<T>(
    conn: &mut Connection,
    progress_cb: Option<&DownloadProgressCallback<'_>>,
    phase: &str,
    message: &str,
    action: impl FnOnce(&mut Connection) -> Result<T, String>,
) -> Result<T, String> {
    eprintln!("评分写库阶段 {phase}: {message}");
    let Some(cb) = progress_cb else {
        return action(conn);
    };
    let emit = |finished, total, message: String| {
        cb(DownloadProgress {
            phase: phase.to_string(),
            finished,
            total,
            current_label: None,
            message,
        })
    };
    emit(0, 0, message.to_string());
    let progress = conn.progress_handle();
    let result = std::thread::scope(|scope| {
        let (stop_tx, stop_rx) = std::sync::mpsc::channel::<()>();
        scope.spawn(move || {
            let started = time::Instant::now();
            let mut finished = 0;
            while matches!(
                stop_rx.recv_timeout(time::Duration::from_millis(500)),
                Err(std::sync::mpsc::RecvTimeoutError::Timeout)
            ) {
                // Scene ranking includes a Rust sort; only the summary rank has
                // a single dominant query whose percentage can be displayed.
                if phase == "score_summary_rank" {
                    if let Some(percent) = progress.query_progress().percentage() {
                        finished = finished.max((percent * 100.0).min(9_900.0) as usize);
                    }
                }
                emit(
                    finished,
                    if finished > 0 { 10_000 } else { 0 },
                    format!("{message} 已耗时 {} 秒。", started.elapsed().as_secs()),
                );
            }
        });
        let result = action(conn);
        let _ = stop_tx.send(());
        result
    });
    if result.is_ok() {
        emit(1, 1, format!("{message} 完成。"));
    } else if let Err(error) = &result {
        eprintln!("评分写库阶段 {phase} 失败: {error}");
    }
    result
}

// A batch boundary may only fall between trading dates: each window partition stays whole.
fn write_scene_rank_batches(
    conn: &mut Connection,
    row_budget: u64,
    progress_cb: Option<&DownloadProgressCallback<'_>>,
) -> Result<(), String> {
    let dates = conn
        .prepare(&format!(
            "SELECT trade_date, COUNT(*)::UBIGINT FROM {SCENE_DETAILS_STAGE_TABLE} GROUP BY trade_date ORDER BY trade_date"
        ))
        .map_err(|e| format!("读取场景排名日期失败:{e}"))?
        .query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, u64>(1)?)))
        .map_err(|e| format!("读取场景排名日期失败:{e}"))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("读取场景排名日期失败:{e}"))?;
    let mut begin = 0;
    while begin < dates.len() {
        lianghua_model::task::checkpoint()?;
        let mut end = begin + 1;
        let mut rows = dates[begin].1;
        while end < dates.len() && rows.saturating_add(dates[end].1) <= row_budget {
            rows = rows.saturating_add(dates[end].1);
            end += 1;
        }
        let batch_progress = |mut event: DownloadProgress| {
            // A batch counts only after its transaction commits. Heartbeats retain
            // the previous count so the main progress bar never resets per batch.
            event.finished = if event.total > 0 && event.finished == event.total {
                end
            } else {
                begin
            };
            event.total = dates.len();
            if let Some(cb) = progress_cb {
                cb(event);
            }
        };
        score_write_stage(
            conn,
            progress_cb.map(|_| &batch_progress as &DownloadProgressCallback<'_>),
            "score_scene_rank",
            &format!(
                "正在计算场景排名（{} 至 {}）。",
                dates[begin].0,
                dates[end - 1].0,
            ),
            |conn| {
                let mut keys = conn
                    .prepare(&format!(
                        "SELECT rowid, trade_date, scene_name, ts_code,
                        CASE stage WHEN 'confirm' THEN 3 WHEN 'trigger' THEN 2
                            WHEN 'observe' THEN 1 WHEN 'fail' THEN 0 ELSE -1 END,
                        confirm_strength, confirm_strength - risk_intensity,
                        abs(stage_score) - abs(risk_score), total_score
                     FROM {SCENE_DETAILS_STAGE_TABLE} WHERE trade_date BETWEEN ? AND ?"
                    ))
                    .map_err(|e| format!("准备场景排名键失败:{e}"))?
                    .query_map(params![dates[begin].0, dates[end - 1].0], |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, String>(3)?,
                            row.get::<_, i32>(4)?,
                            [
                                row.get::<_, Option<f64>>(5)?,
                                row.get::<_, Option<f64>>(6)?,
                                row.get::<_, Option<f64>>(7)?,
                                row.get::<_, Option<f64>>(8)?,
                            ],
                        ))
                    })
                    .map_err(|e| format!("读取场景排名键失败:{e}"))?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| format!("读取场景排名键失败:{e}"))?;
                keys.sort_unstable_by(|left, right| {
                    left.1
                        .cmp(&right.1)
                        .then_with(|| left.2.cmp(&right.2))
                        .then_with(|| right.4.cmp(&left.4))
                        .then_with(|| {
                            left.5
                                .iter()
                                .zip(&right.5)
                                .map(|(left, right)| descending_rank_score(*left, *right))
                                .find(|order| !order.is_eq())
                                .unwrap_or(std::cmp::Ordering::Equal)
                        })
                        .then_with(|| left.3.cmp(&right.3))
                });
                lianghua_model::task::checkpoint()?;
                let mut source_rowids = Vec::with_capacity(keys.len());
                let mut ranks = Vec::with_capacity(keys.len());
                let mut current_key = None;
                let mut rank = 0i32;
                for key in &keys {
                    let partition = (&key.1, &key.2);
                    rank = if current_key == Some(partition) {
                        rank + 1
                    } else {
                        1
                    };
                    current_key = Some(partition);
                    source_rowids.push(key.0);
                    ranks.push(rank);
                }
                drop(keys);
                let tx = conn
                    .transaction()
                    .map_err(|e| format!("创建场景排名事务失败:{e}"))?;
                tx.execute_batch(
                    "CREATE TEMP TABLE scene_rank_keys(source_rowid BIGINT, scene_rank INTEGER)",
                )
                .map_err(|e| format!("创建场景排名键暂存失败:{e}"))?;
                {
                    let mut app = tx
                        .appender("scene_rank_keys")
                        .map_err(|e| format!("创建场景排名键写入器失败:{e}"))?;
                    let batch = RecordBatch::try_new(
                        Arc::new(Schema::new(vec![
                            Field::new("source_rowid", DataType::Int64, false),
                            Field::new("scene_rank", DataType::Int32, false),
                        ])),
                        vec![
                            Arc::new(Int64Array::from(source_rowids)),
                            Arc::new(Int32Array::from(ranks)),
                        ],
                    )
                    .map_err(|e| format!("创建场景排名键批次失败:{e}"))?;
                    app.append_record_batch(batch)
                        .map_err(|e| format!("追加场景排名键失败:{e}"))?;
                    app.flush().map_err(|e| format!("刷新场景排名键失败:{e}"))?;
                }
                tx.execute(
                    &format!(
                        r#"
                INSERT INTO {SCENE_DETAILS_SHADOW_TABLE}
                SELECT source.ts_code, source.trade_date, source.scene_name,
                    source.direction, source.stage, source.stage_score,
                    source.risk_score, source.confirm_strength, source.risk_intensity,
                    ranked.scene_rank
                FROM {SCENE_DETAILS_STAGE_TABLE} AS source
                JOIN scene_rank_keys AS ranked ON source.rowid = ranked.source_rowid
                WHERE source.trade_date BETWEEN ? AND ?
            "#
                    ),
                    params![dates[begin].0, dates[end - 1].0],
                )
                .map_err(|e| format!("写入场景排名失败:{e}"))?;
                tx.execute_batch("DROP TABLE scene_rank_keys")
                    .map_err(|e| format!("清理场景排名键失败:{e}"))?;
                tx.commit().map_err(|e| format!("提交场景排名失败:{e}"))
            },
        )?;
        begin = end;
    }
    Ok(())
}

// Date is part of every result key, so checking complete date partitions preserves
// uniqueness without building an ART index whose size grows with all history.
fn validate_result_keys(
    conn: &mut Connection,
    table_name: &str,
    key_columns: &[&str],
    row_budget: u64,
    progress_cb: Option<&DownloadProgressCallback<'_>>,
) -> Result<(), String> {
    let label = match table_name {
        SCORE_SUMMARY_SHADOW_TABLE => "评分汇总",
        RULE_DETAILS_SHADOW_TABLE => "规则明细",
        SCENE_DETAILS_SHADOW_TABLE => "场景明细",
        _ => table_name,
    };
    let dates = conn
        .prepare(&format!(
            "SELECT trade_date, COUNT(*)::UBIGINT FROM {table_name} GROUP BY trade_date ORDER BY trade_date"
        ))
        .map_err(|e| format!("读取{table_name}键校验日期失败:{e}"))?
        .query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, u64>(1)?)))
        .map_err(|e| format!("读取{table_name}键校验日期失败:{e}"))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("读取{table_name}键校验日期失败:{e}"))?;
    let columns = key_columns.join(", ");
    let null_keys = key_columns
        .iter()
        .map(|column| format!("{column} IS NULL"))
        .collect::<Vec<_>>()
        .join(" OR ");
    let mut begin = 0;
    while begin < dates.len() {
        lianghua_model::task::checkpoint()?;
        let mut end = begin + 1;
        let mut rows = dates[begin].1;
        while end < dates.len() && rows.saturating_add(dates[end].1) <= row_budget {
            rows = rows.saturating_add(dates[end].1);
            end += 1;
        }
        score_write_stage(
            conn,
            progress_cb,
            "score_keys",
            &format!(
                "正在校验{label}结果键（{} 至 {}，交易日 {}/{}）。",
                dates[begin].0,
                dates[end - 1].0,
                end,
                dates.len()
            ),
            |conn| {
                let invalid: bool = conn.query_row(
                    &format!(
                        "SELECT EXISTS(SELECT 1 FROM {table_name} WHERE trade_date BETWEEN ? AND ? \
                         GROUP BY {columns} HAVING COUNT(*) > 1 OR {null_keys})"
                    ),
                    params![dates[begin].0, dates[end - 1].0],
                    |row| row.get(0),
                ).map_err(|e| format!("校验{table_name}结果键失败:{e}"))?;
                if invalid {
                    return Err(format!(
                        "{table_name} 在 {} 至 {} 存在重复或空结果键，未发布本轮结果",
                        dates[begin].0,
                        dates[end - 1].0
                    ));
                }
                Ok(())
            },
        )?;
        begin = end;
    }
    Ok(())
}

#[derive(Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ScoreComputeParameters {
    pub start_date: String,
    pub end_date: String,
    pub adj_type: String,
}

pub struct ScoreComputeCheckpoint {
    pub resume_key: String,
    pub parameters: Option<ScoreComputeParameters>,
}

pub fn read_score_compute_checkpoint(
    conn: &Connection,
) -> Result<Option<ScoreComputeCheckpoint>, String> {
    let (columns, parameter_columns): (i64, i64) = conn.query_row(
        "SELECT COUNT(*), COUNT(*) FILTER (WHERE column_name IN ('start_date', 'end_date', 'adj_type')) \
         FROM information_schema.columns WHERE table_name='score_compute_checkpoint'",
        [], |row| Ok((row.get(0)?, row.get(1)?)),
    ).map_err(|e| format!("检查评分断点参数失败:{e}"))?;
    if columns == 0 {
        return Ok(None);
    }
    // Legacy checkpoints still resume with their original input key, but cannot
    // recover requested dates from the first/last trading dates in staged rows.
    let mut stmt = conn
        .prepare(&format!(
            "SELECT resume_key, {} FROM score_compute_checkpoint LIMIT 1",
            if parameter_columns == 3 {
                "start_date, end_date, adj_type"
            } else {
                "NULL, NULL, NULL"
            },
        ))
        .map_err(|e| format!("准备评分断点参数失败:{e}"))?;
    let mut rows = stmt
        .query([])
        .map_err(|e| format!("查询评分断点参数失败:{e}"))?;
    let Some(row) = rows
        .next()
        .map_err(|e| format!("读取评分断点参数失败:{e}"))?
    else {
        return Ok(None);
    };
    let start_date: Option<String> = row
        .get(1)
        .map_err(|e| format!("读取断点开始日期失败:{e}"))?;
    let end_date: Option<String> = row
        .get(2)
        .map_err(|e| format!("读取断点结束日期失败:{e}"))?;
    let adj_type: Option<String> = row
        .get(3)
        .map_err(|e| format!("读取断点复权参数失败:{e}"))?;
    let parameters = match (start_date, end_date, adj_type) {
        (Some(start_date), Some(end_date), Some(adj_type)) => Some(ScoreComputeParameters {
            start_date,
            end_date,
            adj_type,
        }),
        _ => None,
    };
    Ok(Some(ScoreComputeCheckpoint {
        resume_key: row
            .get(0)
            .map_err(|e| format!("读取评分断点指纹失败:{e}"))?,
        parameters,
    }))
}

pub fn score_compute_checkpoint_matches(db_path: &str, resume_key: &str) -> Result<bool, String> {
    let conn = super::open_db_readonly(db_path).map_err(|e| format!("打开评分断点失败:{e}"))?;
    score_compute_checkpoint_matches_on(&conn, resume_key)
}

fn score_compute_checkpoint_matches_on(
    conn: &Connection,
    resume_key: &str,
) -> Result<bool, String> {
    Ok(read_score_compute_checkpoint(conn)?
        .is_some_and(|checkpoint| checkpoint.resume_key == resume_key))
}

pub fn write_score_batches_from_channel(
    db_path: &str,
    source_db_path: Option<&str>,
    adj_type: &str,
    tie_break: TieBreakWay,
    start_date: &str,
    end_date: &str,
    rx: Receiver<ScoreWriteMessage>,
    progress_cb: Option<&DownloadProgressCallback<'_>>,
) -> Result<ScoreWriteProfile, String> {
    let total_started_at = time::Instant::now();
    let mut profile = ScoreWriteProfile::default();
    let mut conn =
        super::open_db_with_wal_recovery(Path::new(db_path), || Connection::open(db_path))
            .map_err(|e| format!("结果库连接失败:{e}"))?;
    if progress_cb.is_some() {
        conn.set_query_progress_tracking(true)
            .map_err(|e| format!("启用排名查询进度失败:{e}"))?;
    }
    let full_replace = (|conn: &Connection,
                         start_date: &str,
                         end_date: &str|
     -> Result<bool, String> {
        for table_name in [SCORE_SUMMARY_TABLE, RULE_DETAILS_TABLE, SCENE_DETAILS_TABLE] {
            let sql = format!(
                "SELECT EXISTS(SELECT 1 FROM {table_name} WHERE trade_date < ? OR trade_date > ? LIMIT 1)"
            );
            let has_rows_outside_range = conn
                .query_row(&sql, params![start_date, end_date], |row| {
                    row.get::<_, bool>(0)
                })
                .map_err(|e| format!("检查{table_name}写入范围失败:{e}"))?;
            if has_rows_outside_range {
                return Ok(false);
            }
        }
        Ok(true)
    })(&conn, start_date, end_date)?;

    let mut source_db_attached = false;
    if let TieBreakWay::KdjJ = tie_break {
        let source_db_path =
            source_db_path.ok_or_else(|| "J值同分排名需要原始库路径".to_string())?;
        let attach_started_at = time::Instant::now();
        let attach_sql = format!(
            "ATTACH {} AS src_db (READ_ONLY)",
            duckdb_string_literal(source_db_path)
        );
        conn.execute(&attach_sql, [])
            .map_err(|e| format!("附加原始库失败:{e}"))?;
        source_db_attached = true;
        profile.attach_source_db_ms = Some(attach_started_at.elapsed().as_millis() as u64);
    }

    let mut messages = rx.into_iter().peekable();
    let (resume_key, resume) = match messages.peek() {
        Some(ScoreWriteMessage::Begin { resume_key, resume }) => {
            (Some(resume_key.clone()), *resume)
        }
        _ => (None, false),
    };
    if resume_key.is_some() {
        messages.next();
    }
    if resume && !score_compute_checkpoint_matches_on(&conn, resume_key.as_deref().unwrap())? {
        return Err("评分断点已失效，请重新计算".into());
    }
    let mut retain_checkpoint = resume;
    let mut strategy_snapshot = if resume {
        conn.query_row(
            "SELECT strategy_snapshot FROM score_compute_checkpoint WHERE resume_key=?",
            [resume_key.as_deref().unwrap()],
            |row| row.get::<_, Option<String>>(0),
        )
        .map_err(|e| format!("读取断点评分策略失败:{e}"))?
    } else {
        None
    };
    let bulk = crate::data::bulk_write::BulkWriteSettings::apply(&conn)?;
    let write_result = (|| -> Result<(), String> {
        let receive_and_append_started_at = time::Instant::now();
        if !resume {
            drop_result_staging_tables(&conn)?;
            conn.execute_batch(&format!(
                r#"
        CREATE TABLE strategy_tracking_meta_stage (calendar_json VARCHAR);
        CREATE TABLE strategy_tracking_state_stage (
            ts_code VARCHAR, trade_date VARCHAR, config_hash VARCHAR,
            input_stamp VARCHAR, state_json VARCHAR
        );
        CREATE TABLE {SCORE_SUMMARY_STAGE_TABLE} (
            ts_code VARCHAR,
            trade_date VARCHAR,
            total_score DOUBLE
        );
        CREATE TABLE {SCORE_SUMMARY_SHADOW_TABLE} (
            ts_code VARCHAR,
            trade_date VARCHAR,
            total_score DOUBLE,
            rank INTEGER
        );
        CREATE TABLE {RULE_DETAILS_SHADOW_TABLE} (
            ts_code VARCHAR,
            trade_date VARCHAR,
            rule_name VARCHAR,
            rule_score DOUBLE
        );
        CREATE TABLE {SCENE_DETAILS_STAGE_TABLE} (
            ts_code VARCHAR,
            trade_date VARCHAR,
            scene_name VARCHAR,
            direction VARCHAR,
            stage VARCHAR,
            stage_score DOUBLE,
            risk_score DOUBLE,
            confirm_strength DOUBLE,
            risk_intensity DOUBLE,
            total_score DOUBLE
        );
        CREATE TABLE {SCENE_DETAILS_SHADOW_TABLE} (
            ts_code VARCHAR,
            trade_date VARCHAR,
            scene_name VARCHAR,
            direction VARCHAR,
            stage VARCHAR,
            stage_score DOUBLE,
            risk_score DOUBLE,
            confirm_strength DOUBLE,
            risk_intensity DOUBLE,
            scene_rank INTEGER
        );
        "#
            ))
            .map_err(|e| format!("创建结果库暂存表失败:{e}"))?;

            let transaction_bytes = conn.memory_budget() / 4;
            let mut drained = false;
            let mut finished = false;
            while !drained {
                lianghua_model::task::checkpoint()?;
                let tx = conn
                    .transaction()
                    .map_err(|e| format!("创建数据库事务失败:{e}"))?;
                {
                    let mut summary_app = tx
                        .appender(SCORE_SUMMARY_STAGE_TABLE)
                        .map_err(|e| format!("score_summary暂存表appender创建失败:{e}"))?;
                    let mut detail_app = tx
                        .appender(RULE_DETAILS_SHADOW_TABLE)
                        .map_err(|e| format!("rule_details appender创建失败:{e}"))?;
                    let mut scene_app = tx
                        .appender(SCENE_DETAILS_STAGE_TABLE)
                        .map_err(|e| format!("场景暂存表appender创建失败:{e}"))?;

                    let mut tracking_app = tx
                        .appender("strategy_tracking_state_stage")
                        .map_err(|e| format!("跟踪状态appender创建失败:{e}"))?;
                    let mut group_batches = 0usize;
                    let mut group_bytes = 0u64;
                    while group_batches < 64 && group_bytes < transaction_bytes {
                        lianghua_model::task::checkpoint()?;
                        let Some(message) = messages.next() else {
                            drained = true;
                            break;
                        };
                        let (batch, _queue_permit) = match message {
                            ScoreWriteMessage::Begin { .. } => {
                                return Err("重复的评分阶段开始消息".into());
                            }
                            ScoreWriteMessage::Finish => {
                                finished = true;
                                drained = true;
                                break;
                            }
                            ScoreWriteMessage::StrategySnapshot(text) => {
                                strategy_snapshot = Some(text);
                                continue;
                            }
                            ScoreWriteMessage::TrackingCalendar(calendar) => {
                                tx.execute(
                                    "INSERT INTO strategy_tracking_meta_stage VALUES (?)",
                                    [calendar],
                                )
                                .map_err(|e| format!("写入跟踪日历失败:{e}"))?;
                                continue;
                            }
                            ScoreWriteMessage::Batch(batch) => (batch, None),
                            ScoreWriteMessage::BudgetedBatch(batch, permit) => {
                                (batch, Some(permit))
                            }
                            ScoreWriteMessage::Abort(reason) => {
                                return Err(format!("评分计算中断，结果库回滚:{reason}"));
                            }
                        };
                        group_bytes = group_bytes.saturating_add(batch.memory_bytes() as u64);
                        group_batches += 1;
                        profile.batch_count += 1;
                        for row in batch.tracking_states {
                            tracking_app
                                .append_row(params![
                                    row.ts_code,
                                    row.trade_date,
                                    row.config_hash,
                                    row.input_stamp,
                                    row.state_json
                                ])
                                .map_err(|e| format!("追加跟踪状态失败:{e}"))?;
                        }

                        (|app: &mut Appender<'_>,
                          rows: Vec<ScoreSummary>|
                         -> Result<(), String> {
                            if rows.is_empty() {
                                return Ok(());
                            }

                            let mut ts_code = StringBuilder::with_capacity(
                                rows.len(),
                                rows.len().saturating_mul(12),
                            );
                            let mut trade_date = StringBuilder::with_capacity(
                                rows.len(),
                                rows.len().saturating_mul(8),
                            );
                            let mut total_score = Vec::with_capacity(rows.len());
                            for row in rows {
                                ts_code.append_value(&row.ts_code);
                                trade_date.append_value(&row.trade_date);
                                total_score.push(row.total_score);
                            }

                            let schema = Schema::new(vec![
                                Field::new("ts_code", DataType::Utf8, false),
                                Field::new("trade_date", DataType::Utf8, false),
                                Field::new("total_score", DataType::Float64, false),
                            ]);
                            let batch = RecordBatch::try_new(
                                Arc::new(schema),
                                vec![
                                    score_string_array(ts_code.finish()),
                                    score_string_array(trade_date.finish()),
                                    score_float64_array(total_score),
                                ],
                            )
                            .map_err(|e| format!("创建score_summary临时批次失败:{e}"))?;
                            app.append_record_batch(batch)
                                .map_err(|e| format!("批量插入score_summary临时表失败:{e}"))
                        })(&mut summary_app, batch.summary_rows)?;
                        (|app: &mut Appender<'_>,
                          rows: Vec<ScoreDetails>|
                         -> Result<(), String> {
                            if rows.is_empty() {
                                return Ok(());
                            }

                            let mut ts_code = StringBuilder::with_capacity(
                                rows.len(),
                                rows.len().saturating_mul(12),
                            );
                            let mut trade_date = StringBuilder::with_capacity(
                                rows.len(),
                                rows.len().saturating_mul(8),
                            );
                            let mut rule_name = StringBuilder::with_capacity(
                                rows.len(),
                                rows.len().saturating_mul(16),
                            );
                            let mut rule_score = Vec::with_capacity(rows.len());
                            for row in rows {
                                ts_code.append_value(&row.ts_code);
                                trade_date.append_value(&row.trade_date);
                                rule_name.append_value(&row.rule_name);
                                rule_score.push(row.rule_score);
                            }

                            let schema = Schema::new(vec![
                                Field::new("ts_code", DataType::Utf8, false),
                                Field::new("trade_date", DataType::Utf8, false),
                                Field::new("rule_name", DataType::Utf8, false),
                                Field::new("rule_score", DataType::Float64, false),
                            ]);
                            let batch = RecordBatch::try_new(
                                Arc::new(schema),
                                vec![
                                    score_string_array(ts_code.finish()),
                                    score_string_array(trade_date.finish()),
                                    score_string_array(rule_name.finish()),
                                    score_float64_array(rule_score),
                                ],
                            )
                            .map_err(|e| format!("创建rule_details批次失败:{e}"))?;
                            app.append_record_batch(batch)
                                .map_err(|e| format!("批量插入rule_details失败:{e}"))
                        })(&mut detail_app, batch.detail_rows)?;
                        (|app: &mut Appender<'_>,
                          rows: Vec<SceneDetails>|
                         -> Result<(), String> {
                            if rows.is_empty() {
                                return Ok(());
                            }

                            let mut ts_code = StringBuilder::with_capacity(
                                rows.len(),
                                rows.len().saturating_mul(12),
                            );
                            let mut trade_date = StringBuilder::with_capacity(
                                rows.len(),
                                rows.len().saturating_mul(8),
                            );
                            let mut scene_name = StringBuilder::with_capacity(
                                rows.len(),
                                rows.len().saturating_mul(16),
                            );
                            let mut direction = StringBuilder::with_capacity(
                                rows.len(),
                                rows.len().saturating_mul(6),
                            );
                            let mut stage = StringBuilder::with_capacity(
                                rows.len(),
                                rows.len().saturating_mul(8),
                            );
                            let mut stage_score = Vec::with_capacity(rows.len());
                            let mut risk_score = Vec::with_capacity(rows.len());
                            let mut confirm_strength = Vec::with_capacity(rows.len());
                            let mut risk_intensity = Vec::with_capacity(rows.len());
                            let mut total_score = Vec::with_capacity(rows.len());
                            for row in rows {
                                ts_code.append_value(&row.ts_code);
                                trade_date.append_value(&row.trade_date);
                                scene_name.append_value(&row.scene_name);
                                direction.append_value(&row.direction);
                                stage.append_option(row.stage.as_deref());
                                stage_score.push(row.stage_score);
                                risk_score.push(row.risk_score);
                                confirm_strength.push(row.confirm_strength);
                                risk_intensity.push(row.risk_intensity);
                                total_score.push(row.total_score);
                            }

                            let schema = Schema::new(vec![
                                Field::new("ts_code", DataType::Utf8, false),
                                Field::new("trade_date", DataType::Utf8, false),
                                Field::new("scene_name", DataType::Utf8, false),
                                Field::new("direction", DataType::Utf8, false),
                                Field::new("stage", DataType::Utf8, true),
                                Field::new("stage_score", DataType::Float64, false),
                                Field::new("risk_score", DataType::Float64, false),
                                Field::new("confirm_strength", DataType::Float64, false),
                                Field::new("risk_intensity", DataType::Float64, false),
                                Field::new("total_score", DataType::Float64, false),
                            ]);
                            let batch = RecordBatch::try_new(
                                Arc::new(schema),
                                vec![
                                    score_string_array(ts_code.finish()),
                                    score_string_array(trade_date.finish()),
                                    score_string_array(scene_name.finish()),
                                    score_string_array(direction.finish()),
                                    score_string_array(stage.finish()),
                                    score_float64_array(stage_score),
                                    score_float64_array(risk_score),
                                    score_float64_array(confirm_strength),
                                    score_float64_array(risk_intensity),
                                    score_float64_array(total_score),
                                ],
                            )
                            .map_err(|e| format!("创建scene_details批次失败:{e}"))?;
                            app.append_record_batch(batch)
                                .map_err(|e| format!("批量插入scene_details失败:{e}"))
                        })(&mut scene_app, batch.scene_rows)?;
                    }

                    tracking_app
                        .flush()
                        .map_err(|e| format!("刷新跟踪状态失败:{e}"))?;
                    summary_app
                        .flush()
                        .map_err(|e| format!("刷新score_summary暂存表失败:{e}"))?;
                    detail_app
                        .flush()
                        .map_err(|e| format!("刷新rule_details失败:{e}"))?;
                    scene_app
                        .flush()
                        .map_err(|e| format!("刷新场景暂存表失败:{e}"))?;
                }
                tx.commit().map_err(|e| format!("提交评分批次失败:{e}"))?;
            }

            if let Some(key) = &resume_key {
                if !finished {
                    return Err("评分计算未完整结束，不能进入排名阶段".into());
                }
                let tx = conn
                    .transaction()
                    .map_err(|e| format!("创建评分断点事务失败:{e}"))?;
                tx.execute_batch("CREATE TABLE score_compute_checkpoint (resume_key VARCHAR, strategy_snapshot VARCHAR, start_date VARCHAR, end_date VARCHAR, adj_type VARCHAR)")
                .map_err(|e| format!("创建评分断点失败:{e}"))?;
                tx.execute(
                    "INSERT INTO score_compute_checkpoint VALUES (?, ?, ?, ?, ?)",
                    params![key, strategy_snapshot, start_date, end_date, adj_type],
                )
                .map_err(|e| format!("保存评分断点失败:{e}"))?;
                tx.commit().map_err(|e| format!("提交评分断点失败:{e}"))?;
                retain_checkpoint = true;
                conn.execute_batch("CHECKPOINT")
                    .map_err(|e| format!("评分阶段断点落盘失败:{e}"))?;
                if let Some(cb) = progress_cb {
                    cb(DownloadProgress {
                        phase: "score_write".into(),
                        finished: 1,
                        total: 1,
                        current_label: None,
                        message: "评分、触发阶段已落盘；后续排名中断可直接继续。".into(),
                    });
                }
            }
        } else {
            match messages.next() {
                Some(ScoreWriteMessage::Finish) => {}
                Some(ScoreWriteMessage::Abort(reason)) => return Err(reason),
                _ => return Err("继续排名前未收到输入确认".into()),
            }
            // Only derived ranks are discarded. Committed scores, triggers and event states survive.
            conn.execute_batch(&format!(
                "DROP TABLE IF EXISTS {SCORE_SUMMARY_SHADOW_TABLE};
                 DROP TABLE IF EXISTS {SCENE_DETAILS_SHADOW_TABLE};
                 CREATE TABLE {SCORE_SUMMARY_SHADOW_TABLE} AS SELECT * FROM {SCORE_SUMMARY_TABLE} WHERE false;
                 CREATE TABLE {SCENE_DETAILS_SHADOW_TABLE} AS SELECT * FROM {SCENE_DETAILS_TABLE} WHERE false;"
            )).map_err(|e| format!("重置排名影子表失败:{e}"))?;
        }
        profile.receive_and_append_batches_ms =
            receive_and_append_started_at.elapsed().as_millis() as u64;
        let scene_rank_started_at = time::Instant::now();
        // Estimate 512 bytes per owned Rust key with 4x headroom for reads and the join.
        let scene_rank_row_budget = (conn.memory_budget() / 2048).max(1);
        write_scene_rank_batches(&mut conn, scene_rank_row_budget, progress_cb)?;
        profile.scene_rank_ms = scene_rank_started_at.elapsed().as_millis() as u64;

        let summary_rank_started_at = time::Instant::now();
        score_write_stage(
            &mut conn,
            progress_cb,
            "score_summary_rank",
            "正在计算总榜排名（含 J 值同分排序）。",
            |conn| {
                let tx = conn
                    .transaction()
                    .map_err(|e| format!("创建数据库事务失败:{e}"))?;
                (|tx: &Transaction<'_>,
                  tie_break: TieBreakWay,
                  adj_type: &str|
                 -> Result<(), String> {
                    match tie_break {
                        TieBreakWay::TsCode => {
                            let sql = format!(
                                r#"
                INSERT INTO {SCORE_SUMMARY_SHADOW_TABLE} (ts_code, trade_date, total_score, rank)
                SELECT
                    ts_code,
                    trade_date,
                    total_score,
                    CAST(
                        ROW_NUMBER() OVER (
                            PARTITION BY trade_date
                            ORDER BY total_score DESC, ts_code ASC
                        ) AS INTEGER
                    ) AS rank
                FROM {SCORE_SUMMARY_STAGE_TABLE}
                "#
                            );
                            tx.execute(&sql, [])
                                .map_err(|e| format!("写入总榜排名失败:{e}"))?;
                        }
                        TieBreakWay::KdjJ => {
                            let sql = format!(
                                r#"
                INSERT INTO {SCORE_SUMMARY_SHADOW_TABLE} (ts_code, trade_date, total_score, rank)
                SELECT
                    st.ts_code,
                    st.trade_date,
                    st.total_score,
                    CAST(
                        ROW_NUMBER() OVER (
                            PARTITION BY st.trade_date
                            ORDER BY st.total_score DESC, src.j ASC NULLS LAST, st.ts_code ASC
                        ) AS INTEGER
                    ) AS rank
                FROM {SCORE_SUMMARY_STAGE_TABLE} AS st
                LEFT JOIN src_db.stock_data AS src
                  ON st.ts_code = src.ts_code
                 AND st.trade_date = src.trade_date
                 AND src.adj_type = ?
                 AND src.trade_date >= ? AND src.trade_date <= ?
                "#
                            );
                            tx.execute(&sql, params![adj_type, start_date, end_date])
                                .map_err(|e| format!("写入J值同分总榜排名失败:{e}"))?;
                        }
                    }
                    Ok(())
                })(&tx, tie_break, adj_type)?;
                tx.commit().map_err(|e| format!("提交总榜排名失败:{e}"))
            },
        )?;
        profile.summary_rank_ms = summary_rank_started_at.elapsed().as_millis() as u64;

        let shadow_key_started_at = time::Instant::now();
        for (table_name, key_columns) in [
            (SCORE_SUMMARY_SHADOW_TABLE, &["ts_code", "trade_date"][..]),
            (
                RULE_DETAILS_SHADOW_TABLE,
                &["ts_code", "trade_date", "rule_name"][..],
            ),
            (
                SCENE_DETAILS_SHADOW_TABLE,
                &["ts_code", "trade_date", "scene_name"][..],
            ),
        ] {
            validate_result_keys(
                &mut conn,
                table_name,
                key_columns,
                scene_rank_row_budget,
                progress_cb,
            )?;
        }
        profile.recreate_indexes_ms = shadow_key_started_at.elapsed().as_millis() as u64;

        score_write_stage(
            &mut conn,
            progress_cb,
            "score_publish",
            "正在发布评分、排名和跟踪结果。",
            |conn| {
                let tx = conn
                    .transaction()
                    .map_err(|e| format!("创建数据库事务失败:{e}"))?;
                lianghua_model::task::checkpoint()?;
                let replace_started_at = time::Instant::now();
                if full_replace {
                    (|tx: &Transaction<'_>| -> Result<(), String> {
                        tx.execute_batch(&format!(
                            r#"
        DROP TABLE {SCORE_SUMMARY_TABLE};
        DROP TABLE {RULE_DETAILS_TABLE};
        DROP TABLE {SCENE_DETAILS_TABLE};

        ALTER TABLE {SCORE_SUMMARY_SHADOW_TABLE} RENAME TO {SCORE_SUMMARY_TABLE};
        ALTER TABLE {RULE_DETAILS_SHADOW_TABLE} RENAME TO {RULE_DETAILS_TABLE};
        ALTER TABLE {SCENE_DETAILS_SHADOW_TABLE} RENAME TO {SCENE_DETAILS_TABLE};
        "#
                        ))
                        .map_err(|e| format!("切换结果库影子表失败:{e}"))?;
                        Ok(())
                    })(&tx)?;
                } else {
                    (|tx: &Transaction<'_>,
                      start_date: &str,
                      end_date: &str|
                     -> Result<(), String> {
                        tx.execute(
                            "DELETE FROM score_summary WHERE trade_date >= ? AND trade_date <= ?",
                            params![start_date, end_date],
                        )
                        .map_err(|e| format!("删除score_summary旧数据失败:{e}"))?;
                        tx.execute(
                            "DELETE FROM rule_details WHERE trade_date >= ? AND trade_date <= ?",
                            params![start_date, end_date],
                        )
                        .map_err(|e| format!("删除rule_details旧数据失败:{e}"))?;
                        tx.execute(
                            "DELETE FROM scene_details WHERE trade_date >= ? AND trade_date <= ?",
                            params![start_date, end_date],
                        )
                        .map_err(|e| format!("删除scene_details旧数据失败:{e}"))?;
                        Ok(())
                    })(&tx, start_date, end_date)?;
                }
                delete_convolution_rank_range(&tx, start_date, end_date)?;
                profile.delete_range_ms = replace_started_at.elapsed().as_millis() as u64;

                if !full_replace {
                    let merge_started_at = time::Instant::now();
                    (|tx: &Transaction<'_>| -> Result<(), String> {
                        tx.execute(
                    &format!(
                        "INSERT INTO {SCORE_SUMMARY_TABLE} (ts_code, trade_date, total_score, rank) \
                         SELECT ts_code, trade_date, total_score, rank FROM {SCORE_SUMMARY_SHADOW_TABLE}"
                    ),
                    [],
                )
                .map_err(|e| format!("写入score_summary失败:{e}"))?;
                        tx.execute(
                    &format!(
                        "INSERT INTO {RULE_DETAILS_TABLE} (ts_code, trade_date, rule_name, rule_score) \
                         SELECT ts_code, trade_date, rule_name, rule_score FROM {RULE_DETAILS_SHADOW_TABLE}"
                    ),
                    [],
                )
                .map_err(|e| format!("写入rule_details失败:{e}"))?;
                        tx.execute(
                    &format!(
                        "INSERT INTO {SCENE_DETAILS_TABLE} (ts_code, trade_date, scene_name, direction, \
                         stage, stage_score, risk_score, confirm_strength, risk_intensity, scene_rank) \
                         SELECT ts_code, trade_date, scene_name, direction, stage, stage_score, risk_score, \
                         confirm_strength, risk_intensity, scene_rank FROM {SCENE_DETAILS_SHADOW_TABLE}"
                    ),
                    [],
                )
                .map_err(|e| format!("写入scene_details失败:{e}"))?;
                        tx.execute_batch(&format!(
                            r#"
        DROP TABLE {SCORE_SUMMARY_SHADOW_TABLE};
        DROP TABLE {RULE_DETAILS_SHADOW_TABLE};
        DROP TABLE {SCENE_DETAILS_SHADOW_TABLE};
        "#
                        ))
                        .map_err(|e| format!("清理结果库暂存表失败:{e}"))?;
                        Ok(())
                    })(&tx)?;
                    profile.receive_and_append_batches_ms +=
                        merge_started_at.elapsed().as_millis() as u64;
                }

                tx.execute_batch("CREATE TABLE IF NOT EXISTS rank_compute_strategy (trade_date VARCHAR PRIMARY KEY, strategy_text VARCHAR NOT NULL)")
                    .map_err(|e| format!("创建排名策略来源表失败:{e}"))?;
                tx.execute(
                    "DELETE FROM rank_compute_strategy WHERE trade_date >= ? AND trade_date <= ?",
                    params![start_date, end_date],
                )
                .map_err(|e| format!("清理排名策略来源失败:{e}"))?;
                if let Some(text) = &strategy_snapshot {
                    tx.execute("INSERT INTO rank_compute_strategy SELECT DISTINCT trade_date, ? FROM score_summary WHERE trade_date >= ? AND trade_date <= ?", params![text, start_date, end_date])
                        .map_err(|e| format!("保存排名策略来源失败:{e}"))?;
                }

                // Publish the final event states in the same transaction as scores and ranks.
                tx.execute_batch(
                    "DROP TABLE IF EXISTS strategy_tracking_state;
            DROP TABLE IF EXISTS strategy_tracking_meta;
            ALTER TABLE strategy_tracking_state_stage RENAME TO strategy_tracking_state;
            ALTER TABLE strategy_tracking_meta_stage RENAME TO strategy_tracking_meta;",
                )
                .map_err(|e| format!("提交跟踪状态失败:{e}"))?;
                tx.execute_batch(&format!(
                    "DROP TABLE {SCORE_SUMMARY_STAGE_TABLE}; DROP TABLE {SCENE_DETAILS_STAGE_TABLE};
                     DROP TABLE IF EXISTS score_compute_checkpoint;"
                ))
                .map_err(|e| format!("清理已发布评分断点失败:{e}"))?;
                lianghua_model::task::checkpoint()?;
                let commit_started_at = time::Instant::now();
                tx.commit().map_err(|e| format!("事务提交错误:{e}"))?;
                profile.commit_ms = commit_started_at.elapsed().as_millis() as u64;
                Ok(())
            },
        )?;

        Ok::<(), String>(())
    })();

    if write_result.is_err() && !retain_checkpoint {
        let _ = drop_result_staging_tables(&conn);
    }

    let detach_source_result = if source_db_attached {
        let detach_started_at = time::Instant::now();
        let result = conn
            .execute("DETACH src_db", [])
            .map(|_| ())
            .map_err(|e| format!("卸载原始库失败:{e}"));
        profile.detach_source_db_ms = Some(detach_started_at.elapsed().as_millis() as u64);
        result
    } else {
        Ok(())
    };
    let checkpoint_started_at = time::Instant::now();
    score_write_stage(
        &mut conn,
        progress_cb,
        "score_checkpoint",
        "正在完成结果库落盘。",
        |conn| bulk.checkpoint_and_finish(conn, write_result),
    )?;
    profile.checkpoint_ms = checkpoint_started_at.elapsed().as_millis() as u64;
    detach_source_result?;
    profile.total_ms = total_started_at.elapsed().as_millis() as u64;

    Ok(profile)
}
