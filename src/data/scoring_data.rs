use duckdb::{Appender, Connection, Transaction, params};
use std::collections::HashSet;
use std::fs::create_dir_all;
use std::path::Path;
use std::sync::mpsc::Receiver;
use std::time;
// use std::fs::File;
// use std::io::{BufWriter, Write};

use crate::data::{RowData, ScoreRule};
use crate::expr::eval::{Runtime, Value};
use crate::expr::parser::{Parser, lex_all};
use crate::scoring::{
    CachedRule, RuleScoreSeries, SceneScoreSeries, TieBreakWay, build_tirbreak_rank_sql,
};

#[derive(Debug, Default)]
pub struct ScoreSummary {
    pub ts_code: String,
    pub trade_date: String,
    pub total_score: f64,
    pub rank: Option<i64>,
}

#[derive(Debug, Default, Clone)]
pub struct ScoreDetails {
    pub ts_code: String,
    pub trade_date: String,
    pub rule_name: String,
    pub rule_score: f64,
}

#[derive(Debug, Default, Clone)]
pub struct SceneDetails {
    pub ts_code: String,
    pub trade_date: String,
    pub scene_name: String,
    pub direction: String,
    pub stage: Option<String>,
    pub stage_score: f64,
    pub risk_score: f64,
    pub confirm_strength: f64,
    pub risk_intensity: f64,
}

#[derive(Debug, Default)]
pub struct ScoreBatch {
    pub summary_rows: Vec<ScoreSummary>,
    pub detail_rows: Vec<ScoreDetails>,
    pub scene_rows: Vec<SceneDetails>,
}

impl ScoreBatch {
    pub fn extend(&mut self, other: ScoreBatch) {
        self.summary_rows.extend(other.summary_rows);
        self.detail_rows.extend(other.detail_rows);
        self.scene_rows.extend(other.scene_rows);
    }
}

#[derive(Debug)]
pub enum ScoreWriteMessage {
    Batch(ScoreBatch),
    Abort(String),
}

#[derive(Debug, Default, Clone)]
pub struct ScoreWriteProfile {
    pub total_ms: u64,
    pub drop_indexes_ms: u64,
    pub delete_range_ms: u64,
    pub receive_and_append_batches_ms: u64,
    pub summary_rank_ms: u64,
    pub scene_rank_ms: u64,
    pub commit_ms: u64,
    pub recreate_indexes_ms: u64,
    pub batch_count: usize,
}

const SCORE_SUMMARY_TABLE: &str = "score_summary";
const RULE_DETAILS_TABLE: &str = "rule_details";
const SCENE_DETAILS_TABLE: &str = "scene_details";

impl ScoreSummary {
    pub fn build(ts_code: &str, trade_dates: &[String], total_scores: &[f64]) -> Vec<Self> {
        let mut sum: Vec<Self> = Vec::new();
        for i in 0..trade_dates.len() {
            let mut score = Self::default();
            score.ts_code = ts_code.to_string();
            score.trade_date = trade_dates[i].clone();
            score.total_score = total_scores[i];
            score.rank = None;
            sum.push(score);
        }
        sum
    }

    // pub fn write_csv(path: &str, rows: &[ScoreSummary]) -> Result<(), String> {
    //     let file = File::create(path).map_err(|e| format!("创建文件失败: {e}"))?;

    //     let mut writer = BufWriter::new(file);

    //     writeln!(writer, "ts_code,trade_date,total_score")
    //         .map_err(|e| format!("写入表头失败: {e}"))?;

    //     for row in rows {
    //         writeln!(
    //             writer,
    //             "{},{},{}",
    //             row.ts_code, row.trade_date, row.total_score
    //         )
    //         .map_err(|e| format!("写入数据行失败: {e}"))?;
    //     }

    //     writer.flush().map_err(|e| format!("刷新文件失败: {e}"))?;

    //     Ok(())
    // }

    pub fn write_db(db_path: &str, rows: &[ScoreSummary]) -> Result<(), String> {
        let mut conn =
            Connection::open(db_path).map_err(|e| format!("summary数据库连接失败:{e}"))?;
        let tx = conn
            .transaction()
            .map_err(|e| format!("创建数据库事务失败:{e}"))?;
        let del_sql = r#"
                DELETE FROM score_summary
                WHERE trade_date = ?
            "#;
        let mut del = tx
            .prepare(del_sql)
            .map_err(|e| format!("预编译del_sql失败:{e}"))?;
        let mut del_dates = HashSet::new();
        for row in rows {
            del_dates.insert(&row.trade_date);
        }
        for day in del_dates {
            let _ = del
                .execute(params![day])
                .map_err(|e| format!("删除数据库旧数据失败:{e}"))?;
        }
        {
            let mut app = tx
                .appender("score_summary")
                .map_err(|e| format!("summary数据库插入错误:{e}"))?;
            for row in rows {
                let _ = app
                    .append_row(params![
                        &row.ts_code,
                        &row.trade_date,
                        &row.total_score,
                        Option::<i64>::None
                    ])
                    .map_err(|e| format!("插入数据库新数据失败:{e}"))?;
            }
            app.flush()
                .map_err(|e| format!("插入数据库新数据失败:{e}"))?;
        }
        tx.commit().map_err(|e| format!("事务提交错误:{e}"))?;
        Ok(())
    }
}

impl ScoreDetails {
    pub fn build(
        ts_code: &str,
        trade_dates: &[String],
        rule_score_series: &[RuleScoreSeries],
    ) -> Vec<ScoreDetails> {
        let mut out = Vec::new();
        for sin_rule in rule_score_series.iter() {
            let rule_name = sin_rule.name.clone();
            if trade_dates.len() == sin_rule.series.len()
                && trade_dates.len() == sin_rule.triggered.len()
            {
                for i in 0..trade_dates.len() {
                    if !sin_rule.triggered[i] {
                        continue;
                    }
                    let rule_score = sin_rule.series[i];
                    let mut rule_details = Self::default();
                    rule_details.ts_code = ts_code.to_string();
                    rule_details.rule_name = rule_name.clone();
                    rule_details.trade_date = trade_dates[i].clone();
                    rule_details.rule_score = rule_score;
                    out.push(rule_details);
                }
            }
        }
        out
    }

    pub fn write_db(db_path: &str, rows: &[ScoreDetails]) -> Result<(), String> {
        let mut conn =
            Connection::open(db_path).map_err(|e| format!("details数据库连接失败:{e}"))?;
        let tx = conn
            .transaction()
            .map_err(|e| format!("事务创建失败:{e}"))?;
        let del_sql = r#"
                DELETE FROM rule_details
                WHERE trade_date = ?
            "#;
        let mut del = tx
            .prepare(del_sql)
            .map_err(|e| format!("预编译del_sql失败:{e}"))?;
        let mut del_dates = HashSet::new();
        for row in rows {
            del_dates.insert(&row.trade_date);
        }
        for day in del_dates {
            let _ = del
                .execute(params![day])
                .map_err(|e| format!("删除数据库旧数据失败:{e}"))?;
        }

        {
            let mut app = tx
                .appender("rule_details")
                .map_err(|e| format!("details数据库插入错误:{e}"))?;
            for row in rows {
                let _ = app
                    .append_row(params![
                        &row.ts_code,
                        &row.trade_date,
                        &row.rule_name,
                        row.rule_score
                    ])
                    .map_err(|e| format!("插入数据库新数据失败:{e}"))?;
            }
            app.flush()
                .map_err(|e| format!("插入数据库新数据失败:{e}"))?;
        }
        tx.commit().map_err(|e| format!("事务提交错误:{e}"))?;
        Ok(())
    }
}

impl SceneDetails {
    pub fn build(
        ts_code: &str,
        trade_dates: &[String],
        scene_score_series: &[SceneScoreSeries],
    ) -> Vec<SceneDetails> {
        let mut out = Vec::new();
        for scene in scene_score_series {
            let scene_name = scene.name.clone();
            if trade_dates.len() != scene.triggered.len()
                || trade_dates.len() != scene.stage_score.len()
                || trade_dates.len() != scene.risk_score.len()
                || trade_dates.len() != scene.confirm_strength.len()
                || trade_dates.len() != scene.risk_intensity.len()
                || trade_dates.len() != scene.stage.len()
            {
                continue;
            }

            for i in 0..trade_dates.len() {
                if !scene.triggered[i] {
                    continue;
                }
                out.push(SceneDetails {
                    ts_code: ts_code.to_string(),
                    trade_date: trade_dates[i].clone(),
                    scene_name: scene_name.clone(),
                    direction: scene.direction.as_str().to_string(),
                    stage: scene.stage[i].clone(),
                    stage_score: scene.stage_score[i],
                    risk_score: scene.risk_score[i],
                    confirm_strength: scene.confirm_strength[i],
                    risk_intensity: scene.risk_intensity[i],
                });
            }
        }
        out
    }
}

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

fn result_table_expected_columns(table_name: &str) -> Result<Vec<&'static str>, String> {
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
}

fn result_table_exists(conn: &Connection, table_name: &str) -> Result<bool, String> {
    let count = conn
        .query_row(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
            [table_name],
            |row| row.get::<_, i64>(0),
        )
        .map_err(|e| format!("检查{table_name}是否存在失败:{e}"))?;
    Ok(count > 0)
}

fn query_result_table_columns(conn: &Connection, table_name: &str) -> Result<Vec<String>, String> {
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
    if !result_table_exists(conn, table_name)? {
        return create_result_table(conn, table_name);
    }

    let expected_columns = result_table_expected_columns(table_name)?;
    let actual_columns = query_result_table_columns(conn, table_name)?;
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

fn drop_result_db_indexes(conn: &Connection) -> Result<(), String> {
    conn.execute(
        "DROP INDEX IF EXISTS idx_score_summary_trade_date_rank_ts",
        [],
    )
    .map_err(|e| format!("删除score_summary索引失败:{e}"))?;
    conn.execute("DROP INDEX IF EXISTS idx_score_summary_ts_date", [])
        .map_err(|e| format!("删除score_summary索引失败:{e}"))?;
    conn.execute("DROP INDEX IF EXISTS idx_rule_details_rule_date_ts", [])
        .map_err(|e| format!("删除rule_details索引失败:{e}"))?;
    conn.execute("DROP INDEX IF EXISTS idx_rule_details_ts_date_rule", [])
        .map_err(|e| format!("删除rule_details索引失败:{e}"))?;
    conn.execute(
        "DROP INDEX IF EXISTS idx_scene_details_trade_date_scene_rank_ts",
        [],
    )
    .map_err(|e| format!("删除scene_details索引失败:{e}"))?;
    conn.execute("DROP INDEX IF EXISTS idx_scene_details_ts_date_scene", [])
        .map_err(|e| format!("删除scene_details索引失败:{e}"))?;
    conn.execute("DROP INDEX IF EXISTS idx_score_summary_trade_date_ts", [])
        .map_err(|e| format!("删除旧score_summary索引失败:{e}"))?;
    conn.execute("DROP INDEX IF EXISTS idx_scene_details_scene_date_ts", [])
        .map_err(|e| format!("删除scene_details索引失败:{e}"))?;
    Ok(())
}

fn ensure_result_db_indexes(conn: &Connection) -> Result<(), String> {
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_score_summary_trade_date_rank_ts ON score_summary(trade_date, rank, ts_code)",
        [],
    )
    .map_err(|e| format!("创建score_summary索引失败:{e}"))?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_score_summary_ts_date ON score_summary(ts_code, trade_date)",
        [],
    )
    .map_err(|e| format!("创建score_summary索引失败:{e}"))?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_rule_details_rule_date_ts ON rule_details(rule_name, trade_date, ts_code)",
        [],
    )
    .map_err(|e| format!("创建rule_details索引失败:{e}"))?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_rule_details_ts_date_rule ON rule_details(ts_code, trade_date, rule_name)",
        [],
    )
    .map_err(|e| format!("创建rule_details索引失败:{e}"))?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_scene_details_trade_date_scene_rank_ts ON scene_details(trade_date, scene_name, scene_rank, ts_code)",
        [],
    )
    .map_err(|e| format!("创建scene_details索引失败:{e}"))?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_scene_details_ts_date_scene ON scene_details(ts_code, trade_date, scene_name)",
        [],
    )
    .map_err(|e| format!("创建scene_details索引失败:{e}"))?;
    Ok(())
}

fn delete_score_range(
    tx: &Transaction<'_>,
    start_date: &str,
    end_date: &str,
) -> Result<(), String> {
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
}

fn append_summary_rows(app: &mut Appender<'_>, rows: &[ScoreSummary]) -> Result<(), String> {
    for row in rows {
        app.append_row(params![
            &row.ts_code,
            &row.trade_date,
            &row.total_score,
            Option::<i64>::None
        ])
        .map_err(|e| format!("插入score_summary失败:{e}"))?;
    }
    Ok(())
}

fn append_detail_rows(app: &mut Appender<'_>, rows: &[ScoreDetails]) -> Result<(), String> {
    for row in rows {
        app.append_row(params![
            &row.ts_code,
            &row.trade_date,
            &row.rule_name,
            row.rule_score
        ])
        .map_err(|e| format!("插入rule_details失败:{e}"))?;
    }
    Ok(())
}

fn append_scene_rows(app: &mut Appender<'_>, rows: &[SceneDetails]) -> Result<(), String> {
    for row in rows {
        app.append_row(params![
            &row.ts_code,
            &row.trade_date,
            &row.scene_name,
            &row.direction,
            &row.stage,
            row.stage_score,
            row.risk_score,
            row.confirm_strength,
            row.risk_intensity,
            Option::<i64>::None
        ])
        .map_err(|e| format!("插入scene_details失败:{e}"))?;
    }
    Ok(())
}

fn compute_summary_rankings_in_tx(tx: &Transaction<'_>) -> Result<(), String> {
    let sql = build_tirbreak_rank_sql(TieBreakWay::TsCode, "");
    tx.execute(&sql, [])
        .map_err(|e| format!("批量更新总榜排名失败:{e}"))?;
    Ok(())
}

pub fn write_score_batches_from_channel(
    db_path: &str,
    start_date: &str,
    end_date: &str,
    rx: Receiver<ScoreWriteMessage>,
) -> Result<ScoreWriteProfile, String> {
    let total_started_at = time::Instant::now();
    let mut profile = ScoreWriteProfile::default();
    let mut conn = Connection::open(db_path).map_err(|e| format!("结果库连接失败:{e}"))?;

    let drop_indexes_started_at = time::Instant::now();
    drop_result_db_indexes(&conn)?;
    profile.drop_indexes_ms = drop_indexes_started_at.elapsed().as_millis() as u64;

    let write_result = (|| -> Result<(), String> {
        let tx = conn
            .transaction()
            .map_err(|e| format!("创建数据库事务失败:{e}"))?;

        let delete_started_at = time::Instant::now();
        delete_score_range(&tx, start_date, end_date)?;
        profile.delete_range_ms = delete_started_at.elapsed().as_millis() as u64;

        let receive_and_append_started_at = time::Instant::now();
        let mut batch_count = 0usize;
        {
            let mut summary_app = tx
                .appender("score_summary")
                .map_err(|e| format!("score_summary appender创建失败:{e}"))?;
            let mut detail_app = tx
                .appender("rule_details")
                .map_err(|e| format!("rule_details appender创建失败:{e}"))?;
            let mut scene_app = tx
                .appender("scene_details")
                .map_err(|e| format!("scene_details appender创建失败:{e}"))?;

            for message in rx {
                let batch = match message {
                    ScoreWriteMessage::Batch(batch) => batch,
                    ScoreWriteMessage::Abort(reason) => {
                        return Err(format!("评分计算中断，结果库回滚:{reason}"));
                    }
                };

                append_summary_rows(&mut summary_app, &batch.summary_rows)?;
                append_detail_rows(&mut detail_app, &batch.detail_rows)?;
                append_scene_rows(&mut scene_app, &batch.scene_rows)?;
                batch_count += 1;

                if batch_count % 32 == 0 {
                    summary_app
                        .flush()
                        .map_err(|e| format!("刷新score_summary失败:{e}"))?;
                    detail_app
                        .flush()
                        .map_err(|e| format!("刷新rule_details失败:{e}"))?;
                    scene_app
                        .flush()
                        .map_err(|e| format!("刷新scene_details失败:{e}"))?;
                }
            }

            summary_app
                .flush()
                .map_err(|e| format!("刷新score_summary失败:{e}"))?;
            detail_app
                .flush()
                .map_err(|e| format!("刷新rule_details失败:{e}"))?;
            scene_app
                .flush()
                .map_err(|e| format!("刷新scene_details失败:{e}"))?;
        }
        profile.receive_and_append_batches_ms =
            receive_and_append_started_at.elapsed().as_millis() as u64;
        profile.batch_count = batch_count;

        let summary_rank_started_at = time::Instant::now();
        compute_summary_rankings_in_tx(&tx)?;
        profile.summary_rank_ms = summary_rank_started_at.elapsed().as_millis() as u64;

        let scene_rank_started_at = time::Instant::now();
        compute_scene_rankings_in_tx(&tx, start_date, end_date)?;
        profile.scene_rank_ms = scene_rank_started_at.elapsed().as_millis() as u64;

        let commit_started_at = time::Instant::now();
        tx.commit().map_err(|e| format!("事务提交错误:{e}"))?;
        profile.commit_ms = commit_started_at.elapsed().as_millis() as u64;

        Ok::<(), String>(())
    })();

    let recreate_indexes_started_at = time::Instant::now();
    let recreate_indexes_result = ensure_result_db_indexes(&conn);
    profile.recreate_indexes_ms = recreate_indexes_started_at.elapsed().as_millis() as u64;

    write_result?;
    recreate_indexes_result?;
    profile.total_ms = total_started_at.elapsed().as_millis() as u64;

    Ok(profile)
}

/// 在当前事务内批量计算Scene排名并回写到scene_details。
fn compute_scene_rankings_in_tx(
    tx: &Transaction<'_>,
    start_date: &str,
    end_date: &str,
) -> Result<(), String> {
    tx.execute(
        r#"
        UPDATE scene_details AS d
        SET scene_rank = r.scene_rank
        FROM (
            SELECT
                d.ts_code,
                d.trade_date,
                d.scene_name,
                ROW_NUMBER() OVER (
                    PARTITION BY d.trade_date, d.scene_name
                    ORDER BY
                        CASE d.stage
                            WHEN 'confirm' THEN 3
                            WHEN 'trigger' THEN 2
                            WHEN 'observe' THEN 1
                            WHEN 'fail' THEN 0
                            ELSE -1
                        END DESC,
                        COALESCE(d.confirm_strength, 0.0) DESC,
                        (COALESCE(d.confirm_strength, 0.0) - COALESCE(d.risk_intensity, 0.0)) DESC,
                        (ABS(d.stage_score) - ABS(d.risk_score)) DESC,
                        s.total_score DESC NULLS LAST,
                        d.ts_code ASC
                ) AS scene_rank
            FROM scene_details AS d
            LEFT JOIN score_summary AS s
              ON d.ts_code = s.ts_code
             AND d.trade_date = s.trade_date
            WHERE d.trade_date BETWEEN ? AND ?
        ) AS r
        WHERE d.ts_code = r.ts_code
          AND d.trade_date = r.trade_date
          AND d.scene_name = r.scene_name
        "#,
        params![start_date, end_date],
    )
    .map_err(|e| format!("批量更新Scene排名失败:{e}"))?;
    Ok(())
}

pub fn row_into_rt(row_data: RowData) -> Result<Runtime, String> {
    let mut rt = Runtime::default();
    // let trade_date = row_data.trade_dates;

    for (name, col) in &row_data.cols {
        let n_series = Value::NumSeries(col.clone());
        rt.vars.insert(name.clone(), n_series);
    }

    Ok(rt)
}

pub fn cache_rule_build(
    source_dir: &str,
    strategy_path: Option<&str>,
) -> Result<Vec<CachedRule>, String> {
    let rules = ScoreRule::load_rules_with_strategy_path(source_dir, strategy_path)?;
    let mut out = Vec::with_capacity(128);
    for rule in rules {
        let tok = lex_all(&rule.when);
        let mut parser = Parser::new(tok);
        let stmt = parser
            .parse_main()
            .map_err(|e| format!("表达式解析错误在{}:{}", e.idx, e.msg))?;
        out.push(CachedRule {
            name: rule.name,
            scope_windows: rule.scope_windows,
            scope_way: rule.scope_way,
            points: rule.points,
            dist_points: rule.dist_points,
            tag: rule.tag,
            when_src: rule.when,
            when_ast: stmt,
        });
    }
    Ok(out)
}
