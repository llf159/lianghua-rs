use std::{
    fs::create_dir_all,
    path::Path,
    sync::{Arc, mpsc::Receiver},
    time,
};

use duckdb::{
    Appender, Connection, Transaction,
    arrow::{
        array::{ArrayRef, Float64Array, Int32Array, StringArray, builder::StringBuilder},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    },
    params,
};
use lianghua_model::scoring::{
    RankTiebreakProfile, SceneDetails, ScoreDetails, ScoreSummary, ScoreWriteMessage,
    ScoreWriteProfile, TieBreakWay,
};

const SCORE_SUMMARY_TABLE: &str = "score_summary";
const RULE_DETAILS_TABLE: &str = "rule_details";
const SCENE_DETAILS_TABLE: &str = "scene_details";
const SCORE_SUMMARY_SHADOW_TABLE: &str = "score_summary_write_shadow";
const RULE_DETAILS_SHADOW_TABLE: &str = "rule_details_write_shadow";
const SCENE_DETAILS_SHADOW_TABLE: &str = "scene_details_write_shadow";

pub fn init_result_db(db_path: &Path) -> Result<(), String> {
    let db_file = Path::new(db_path);
    if let Some(parent_dir) = db_file.parent() {
        if !parent_dir.as_os_str().is_empty() {
            create_dir_all(parent_dir).map_err(|e| format!("创建输出目录失败:{e}"))?;
        }
    }

    let conn = Connection::open(db_path).map_err(|e| format!("打开数据库失败:{e}"))?;

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
    ensure_result_db_indexes(&conn)?;

    Ok(())
}

fn create_result_table(conn: &Connection, table_name: &str) -> Result<(), String> {
    let sql = match table_name {
        SCORE_SUMMARY_TABLE => format!(
            r#"
            CREATE TABLE IF NOT EXISTS {table_name} (
                ts_code VARCHAR,
                trade_date VARCHAR,
                total_score DOUBLE,
                rank INTEGER,
                PRIMARY KEY (ts_code, trade_date)
            )
            "#
        ),
        RULE_DETAILS_TABLE => format!(
            r#"
            CREATE TABLE IF NOT EXISTS {table_name} (
                ts_code VARCHAR,
                trade_date VARCHAR,
                rule_name VARCHAR,
                rule_score DOUBLE,
                PRIMARY KEY (ts_code, trade_date, rule_name)
            )
            "#
        ),
        SCENE_DETAILS_TABLE => format!(
            r#"
            CREATE TABLE IF NOT EXISTS {table_name} (
                ts_code VARCHAR,
                trade_date VARCHAR,
                scene_name VARCHAR,
                direction VARCHAR,
                stage VARCHAR,
                stage_score DOUBLE,
                risk_score DOUBLE,
                confirm_strength DOUBLE,
                risk_intensity DOUBLE,
                scene_rank INTEGER,
                PRIMARY KEY (ts_code, trade_date, scene_name)
            )
            "#
        ),
        _ => return Err(format!("不支持的结果表:{table_name}")),
    };

    conn.execute(&sql, [])
        .map_err(|e| format!("创建{table_name}失败:{e}"))?;
    Ok(())
}

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
    let has_primary_key = result_table_has_primary_key(conn, table_name)?;

    if columns_match && has_primary_key {
        return Ok(());
    }

    conn.execute(&format!("DROP TABLE {table_name}"), [])
        .map_err(|e| format!("删除旧{table_name}失败:{e}"))?;
    create_result_table(conn, table_name)
}

fn ensure_result_db_indexes(conn: &Connection) -> Result<(), String> {
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_score_summary_trade_date_rank_ts ON score_summary(trade_date, rank, ts_code)",
        [],
    )
    .map_err(|e| format!("创建score_summary索引失败:{e}"))?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_rule_details_rule_date_ts ON rule_details(rule_name, trade_date, ts_code)",
        [],
    )
    .map_err(|e| format!("创建rule_details索引失败:{e}"))?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_scene_details_trade_date_scene_rank_ts ON scene_details(trade_date, scene_name, scene_rank, ts_code)",
        [],
    )
    .map_err(|e| format!("创建scene_details索引失败:{e}"))?;
    Ok(())
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
                right
                    .confirm_strength
                    .partial_cmp(&left.confirm_strength)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .then_with(|| {
                let right_net = right.confirm_strength - right.risk_intensity;
                let left_net = left.confirm_strength - left.risk_intensity;
                right_net
                    .partial_cmp(&left_net)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .then_with(|| {
                let right_balance = right.stage_score.abs() - right.risk_score.abs();
                let left_balance = left.stage_score.abs() - left.risk_score.abs();
                right_balance
                    .partial_cmp(&left_balance)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .then_with(|| {
                right
                    .total_score
                    .partial_cmp(&left.total_score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
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
    let conn = Connection::open(result_db_path).map_err(|e| format!("结果库连接失败:{e}"))?;

    if let TieBreakWay::KdjJ = tie_break {
        let attach_started_at = time::Instant::now();
        let attach_sql = format!("ATTACH {} AS src_db", duckdb_string_literal(source_db_path));
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
    fn rank_scene_rows_matches_scene_rank_ordering() {
        let mut rows = vec![
            scene_row("000004.SZ", "主升", "trigger", 9.0, 1.0, 1.0, 0.0, 100.0),
            scene_row("000003.SZ", "主升", "confirm", 5.0, 1.0, 2.0, 0.0, 70.0),
            scene_row("000002.SZ", "主升", "confirm", 5.0, 1.0, 2.0, 0.0, 80.0),
            scene_row("000001.SZ", "主升", "confirm", 8.0, 1.0, 2.0, 0.0, 80.0),
            scene_row("000005.SZ", "防守", "observe", 3.0, 0.0, 0.5, 0.0, 50.0),
        ];

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
        assert_eq!(
            explicit_result_index_names(&conn),
            vec![
                "idx_rule_details_rule_date_ts".to_string(),
                "idx_scene_details_trade_date_scene_rank_ts".to_string(),
                "idx_score_summary_trade_date_rank_ts".to_string(),
            ]
        );

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
            )
            .expect("write score batch");
        }

        let conn = Connection::open(&db_path).expect("open result db");
        assert_eq!(
            conn.query_row("SELECT COUNT(*) FROM score_summary", [], |row| {
                row.get::<_, i64>(0)
            })
            .expect("count summary rows"),
            2
        );
        assert_eq!(
            explicit_result_index_names(&conn),
            vec![
                "idx_rule_details_rule_date_ts".to_string(),
                "idx_scene_details_trade_date_scene_rank_ts".to_string(),
                "idx_score_summary_trade_date_rank_ts".to_string(),
            ]
        );
        for table_name in ["score_summary", "rule_details", "scene_details"] {
            assert!(
                result_table_has_primary_key(&conn, table_name).expect("check primary key"),
                "{table_name} should retain its primary key"
            );
        }

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
        drop(conn);

        let (tx, rx) = channel();
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
        );
        assert!(result.is_err());

        let conn = Connection::open(&db_path).expect("reopen result db");
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
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE '%write_shadow'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .expect("count shadow tables"),
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
                    ('000002.SZ', '20240103', 'qfq', 7.0)
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
        }))
        .expect("send batch");
        drop(tx);

        let db_path_str = db_path.to_str().expect("db path utf8");
        let source_db_path_str = source_db_path.to_str().expect("source db path utf8");
        write_score_batches_from_channel(
            db_path_str,
            Some(source_db_path_str),
            "qfq",
            TieBreakWay::KdjJ,
            "20240102",
            "20240103",
            rx,
        )
        .expect("write score batches");

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

pub fn write_score_batches_from_channel(
    db_path: &str,
    source_db_path: Option<&str>,
    adj_type: &str,
    tie_break: TieBreakWay,
    start_date: &str,
    end_date: &str,
    rx: Receiver<ScoreWriteMessage>,
) -> Result<ScoreWriteProfile, String> {
    let total_started_at = time::Instant::now();
    let mut profile = ScoreWriteProfile::default();
    let mut conn = Connection::open(db_path).map_err(|e| format!("结果库连接失败:{e}"))?;
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
        let attach_sql = format!("ATTACH {} AS src_db", duckdb_string_literal(source_db_path));
        conn.execute(&attach_sql, [])
            .map_err(|e| format!("附加原始库失败:{e}"))?;
        source_db_attached = true;
        profile.attach_source_db_ms = Some(attach_started_at.elapsed().as_millis() as u64);
    }

    let write_result = (|| -> Result<(), String> {
        let tx = conn
            .transaction()
            .map_err(|e| format!("创建数据库事务失败:{e}"))?;

        let delete_started_at = time::Instant::now();
        if full_replace {
            (|tx: &Transaction<'_>| -> Result<(), String> {
                tx.execute_batch(&format!(
                    r#"
        DROP TABLE IF EXISTS {SCORE_SUMMARY_SHADOW_TABLE};
        DROP TABLE IF EXISTS {RULE_DETAILS_SHADOW_TABLE};
        DROP TABLE IF EXISTS {SCENE_DETAILS_SHADOW_TABLE};

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
                .map_err(|e| format!("创建结果库影子表失败:{e}"))?;
                Ok(())
            })(&tx)?;
            delete_convolution_rank_range(&tx, start_date, end_date)?;
        } else {
            (|tx: &Transaction<'_>, start_date: &str, end_date: &str| -> Result<(), String> {
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
                delete_convolution_rank_range(tx, start_date, end_date)
            })(&tx, start_date, end_date)?;
        }
        profile.delete_range_ms = delete_started_at.elapsed().as_millis() as u64;
        (|tx: &Transaction<'_>| -> Result<(), String> {
            tx.execute(
                r#"
        CREATE TEMP TABLE score_summary_stage (
            ts_code VARCHAR,
            trade_date VARCHAR,
            total_score DOUBLE
        )
        "#,
                [],
            )
            .map_err(|e| format!("创建score_summary临时表失败:{e}"))?;
            Ok(())
        })(&tx)?;

        let summary_target = if full_replace {
            SCORE_SUMMARY_SHADOW_TABLE
        } else {
            SCORE_SUMMARY_TABLE
        };
        let detail_target = if full_replace {
            RULE_DETAILS_SHADOW_TABLE
        } else {
            RULE_DETAILS_TABLE
        };
        let scene_target = if full_replace {
            SCENE_DETAILS_SHADOW_TABLE
        } else {
            SCENE_DETAILS_TABLE
        };

        let receive_and_append_started_at = time::Instant::now();
        let mut batch_count = 0usize;
        let mut scene_rows = Vec::new();
        {
            let mut summary_app = tx
                .appender("score_summary_stage")
                .map_err(|e| format!("score_summary临时表appender创建失败:{e}"))?;
            let mut detail_app = tx
                .appender(detail_target)
                .map_err(|e| format!("rule_details appender创建失败:{e}"))?;

            for message in rx {
                let batch = match message {
                    ScoreWriteMessage::Batch(batch) => batch,
                    ScoreWriteMessage::Abort(reason) => {
                        return Err(format!("评分计算中断，结果库回滚:{reason}"));
                    }
                };

                (|app: &mut Appender<'_>, rows: &[ScoreSummary]| -> Result<(), String> {
                    if rows.is_empty() {
                        return Ok(());
                    }

                    let mut ts_code =
                        StringBuilder::with_capacity(rows.len(), rows.len().saturating_mul(12));
                    let mut trade_date =
                        StringBuilder::with_capacity(rows.len(), rows.len().saturating_mul(8));
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
                })(&mut summary_app, &batch.summary_rows)?;
                (|app: &mut Appender<'_>, rows: &[ScoreDetails]| -> Result<(), String> {
                    if rows.is_empty() {
                        return Ok(());
                    }

                    let mut ts_code =
                        StringBuilder::with_capacity(rows.len(), rows.len().saturating_mul(12));
                    let mut trade_date =
                        StringBuilder::with_capacity(rows.len(), rows.len().saturating_mul(8));
                    let mut rule_name =
                        StringBuilder::with_capacity(rows.len(), rows.len().saturating_mul(16));
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
                })(&mut detail_app, &batch.detail_rows)?;
                scene_rows.extend(batch.scene_rows);
                batch_count += 1;

                if batch_count % 32 == 0 {
                    summary_app
                        .flush()
                        .map_err(|e| format!("刷新score_summary失败:{e}"))?;
                    detail_app
                        .flush()
                        .map_err(|e| format!("刷新rule_details失败:{e}"))?;
                }
            }

            summary_app
                .flush()
                .map_err(|e| format!("刷新score_summary失败:{e}"))?;
            detail_app
                .flush()
                .map_err(|e| format!("刷新rule_details失败:{e}"))?;
        }
        rank_scene_rows(&mut scene_rows);
        {
            let mut scene_app = tx
                .appender(scene_target)
                .map_err(|e| format!("scene_details appender创建失败:{e}"))?;
            (|app: &mut Appender<'_>, rows: &[SceneDetails]| -> Result<(), String> {
                if rows.is_empty() {
                    return Ok(());
                }

                let mut ts_code =
                    StringBuilder::with_capacity(rows.len(), rows.len().saturating_mul(12));
                let mut trade_date =
                    StringBuilder::with_capacity(rows.len(), rows.len().saturating_mul(8));
                let mut scene_name =
                    StringBuilder::with_capacity(rows.len(), rows.len().saturating_mul(16));
                let mut direction =
                    StringBuilder::with_capacity(rows.len(), rows.len().saturating_mul(6));
                let mut stage =
                    StringBuilder::with_capacity(rows.len(), rows.len().saturating_mul(8));
                let mut stage_score = Vec::with_capacity(rows.len());
                let mut risk_score = Vec::with_capacity(rows.len());
                let mut confirm_strength = Vec::with_capacity(rows.len());
                let mut risk_intensity = Vec::with_capacity(rows.len());
                let mut scene_rank = Vec::with_capacity(rows.len());
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
                    scene_rank.push((|rank: Option<i64>,
                                      label: &str|
                     -> Result<Option<i32>, String> {
                        rank.map(|value| {
                            i32::try_from(value)
                                .map_err(|_| format!("{label}超出INTEGER范围: {value}"))
                        })
                        .transpose()
                    })(
                        row.scene_rank, "scene_details.scene_rank"
                    )?);
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
                    Field::new("scene_rank", DataType::Int32, true),
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
                        (|values: Vec<Option<i32>>| -> ArrayRef {
                            Arc::new(Int32Array::from(values))
                        })(scene_rank),
                    ],
                )
                .map_err(|e| format!("创建scene_details批次失败:{e}"))?;
                app.append_record_batch(batch)
                    .map_err(|e| format!("批量插入scene_details失败:{e}"))
            })(&mut scene_app, &scene_rows)?;
            scene_app
                .flush()
                .map_err(|e| format!("刷新scene_details失败:{e}"))?;
        }
        profile.receive_and_append_batches_ms =
            receive_and_append_started_at.elapsed().as_millis() as u64;
        profile.batch_count = batch_count;

        let summary_rank_started_at = time::Instant::now();
        (|tx: &Transaction<'_>,
          tie_break: TieBreakWay,
          adj_type: &str,
          target_table: &str|
         -> Result<(), String> {
            match tie_break {
                TieBreakWay::TsCode => {
                    let sql = format!(
                        r#"
                INSERT INTO {target_table} (ts_code, trade_date, total_score, rank)
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
                FROM score_summary_stage
                "#
                    );
                    tx.execute(&sql, [])
                        .map_err(|e| format!("写入总榜排名失败:{e}"))?;
                }
                TieBreakWay::KdjJ => {
                    let sql = format!(
                        r#"
                INSERT INTO {target_table} (ts_code, trade_date, total_score, rank)
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
                FROM score_summary_stage AS st
                LEFT JOIN src_db.stock_data AS src
                  ON st.ts_code = src.ts_code
                 AND st.trade_date = src.trade_date
                 AND src.adj_type = ?
                "#
                    );
                    tx.execute(&sql, params![adj_type])
                        .map_err(|e| format!("写入J值同分总榜排名失败:{e}"))?;
                }
            }
            Ok(())
        })(&tx, tie_break, adj_type, summary_target)?;
        profile.summary_rank_ms = summary_rank_started_at.elapsed().as_millis() as u64;

        if full_replace {
            let recreate_indexes_started_at = time::Instant::now();
            (|tx: &Transaction<'_>| -> Result<(), String> {
                tx.execute_batch(&format!(
                    r#"
        ALTER TABLE {SCORE_SUMMARY_SHADOW_TABLE} ADD PRIMARY KEY (ts_code, trade_date);
        ALTER TABLE {RULE_DETAILS_SHADOW_TABLE} ADD PRIMARY KEY (ts_code, trade_date, rule_name);
        ALTER TABLE {SCENE_DETAILS_SHADOW_TABLE} ADD PRIMARY KEY (ts_code, trade_date, scene_name);

        DROP TABLE {SCORE_SUMMARY_TABLE};
        DROP TABLE {RULE_DETAILS_TABLE};
        DROP TABLE {SCENE_DETAILS_TABLE};

        ALTER TABLE {SCORE_SUMMARY_SHADOW_TABLE} RENAME TO {SCORE_SUMMARY_TABLE};
        ALTER TABLE {RULE_DETAILS_SHADOW_TABLE} RENAME TO {RULE_DETAILS_TABLE};
        ALTER TABLE {SCENE_DETAILS_SHADOW_TABLE} RENAME TO {SCENE_DETAILS_TABLE};
        "#
                ))
                .map_err(|e| format!("切换结果库影子表失败:{e}"))?;
                ensure_result_db_indexes(tx)?;
                Ok(())
            })(&tx)?;
            profile.recreate_indexes_ms = recreate_indexes_started_at.elapsed().as_millis() as u64;
        }

        let commit_started_at = time::Instant::now();
        tx.commit().map_err(|e| format!("事务提交错误:{e}"))?;
        profile.commit_ms = commit_started_at.elapsed().as_millis() as u64;

        Ok::<(), String>(())
    })();

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
    write_result?;
    detach_source_result?;
    profile.total_ms = total_started_at.elapsed().as_millis() as u64;

    Ok(profile)
}
