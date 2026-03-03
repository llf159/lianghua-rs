use duckdb::{Connection, params};
use std::collections::HashSet;
use std::fs::create_dir_all;
use std::time;
use std::{collections::HashMap, path::Path};
// use std::fs::File;
// use std::io::{BufWriter, Write};

use crate::scoring::tools::{calc_zhang_pct, load_st_list};
use crate::{
    expr::eval::{Runtime, Value},
    scoring::RuleScoreSeries,
};

#[derive(Debug, Clone)]
pub struct RowData {
    pub trade_dates: Vec<String>,
    pub cols: HashMap<String, Vec<Option<f64>>>,
}

pub struct DataReader {
    pub conn: duckdb::Connection,
    pub query_sql: String,
    pub cols_table: Vec<(String, String)>,
    pub st_list: HashSet<String>,
}

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

impl RowData {
    pub fn validate(&self) -> Result<(), String> {
        if self.trade_dates.is_empty() {
            return Err("trade_dates为空".to_string());
        }
        let len = self.trade_dates.len();
        for (name, series) in &self.cols {
            if series.len() != len {
                return Err(format!("{name}列长度与交易日长度有差异,数据缺失"));
            }
        }
        Ok(())
    }

    fn count_row(
        db_path: &str,
        ts_code: &str,
        adj_type: &str,
        start_date: &str,
        end_date: &str,
    ) -> Result<usize, String> {
        let conn = Connection::open(db_path).map_err(|e| format!("连接数据库错误:{e}"))?;
        let sql = r#"SELECT COUNT FROM stock_data WHERE ts_code = ? AND adj_type = ? AND trade_date >= ? AND trade_date <= ?"#;
        let cnt: i64 = conn
            .query_row(
                sql,
                params![ts_code, adj_type, start_date, end_date],
                |row| row.get(0),
            )
            .map_err(|e| format!("COUNT查询执行失败:{e}"))?;

        if cnt < 0 {
            return Err(format!("COUNT查询异常:{cnt}"));
        }
        Ok(cnt as usize)
    }
}

impl DataReader {
    pub fn new(db_path: &str) -> Result<DataReader, String> {
        let st_list = load_st_list(db_path)?;
        let conn = Connection::open(db_path).map_err(|e| format!("数据库连接错误:{e}"))?;

        let mut sql_to_colsname = conn
            .prepare("DESCRIBE stock_data")
            .map_err(|e| format!("预编译SQL失败:{e}"))?;
        let mut all_cols = sql_to_colsname
            .query([])
            .map_err(|e| format!("执行查询失败:{e}"))?;

        let mut all_cols_name: Vec<String> = Vec::with_capacity(128);
        while let Some(col) = all_cols.next().map_err(|e| format!("读取表名失败:{e}"))? {
            let name: String = col.get(0).map_err(|e| format!("读取列名失败:{e}"))?;
            all_cols_name.push(name);
        }

        // 基础列检查
        let base_pairs = [
            ("open", "O"),
            ("high", "H"),
            ("low", "L"),
            ("close", "C"),
            ("vol", "V"),
            ("amount", "AMOUNT"),
        ];

        let mut db_cols_table: Vec<(String, String)> = Vec::new();
        for (db_col, pair) in base_pairs {
            let db_cols = all_cols_name
                .iter()
                .find(|c| c.eq_ignore_ascii_case(db_col))
                .cloned()
                .ok_or_else(|| format!("数据库缺少基础列:{db_col}"))?;
            db_cols_table.push((db_cols, pair.to_string()));
        }

        // 导入非基础列
        for col in &all_cols_name {
            let low = col.to_ascii_lowercase();
            if matches!(low.as_str(), "ts_code" | "trade_date" | "adj_type") {
                continue;
            }
            if matches!(
                low.as_str(),
                "open" | "high" | "low" | "close" | "vol" | "amount"
            ) {
                continue;
            }
            db_cols_table.push((col.clone(), col.to_ascii_uppercase()));
        }

        let mut select_inds = vec!["trade_date".to_string()];
        for (db_cols, _) in &db_cols_table {
            select_inds.push(format!(
                "TRY_CAST(\"{}\" AS DOUBLE) AS \"{}\"",
                db_cols, db_cols
            ));
        }

        let sql = format!(
            r#"
                SELECT
                    {}
                FROM stock_data
                WHERE ts_code = ?
                AND adj_type = ?
                AND trade_date >= ?
                AND trade_date <= ?
                ORDER BY trade_date ASC
            "#,
            select_inds.join(",\n")
        );

        Ok(Self {
            conn,
            query_sql: sql,
            cols_table: db_cols_table,
            st_list,
        })
    }

    pub fn load_one(
        &self,
        ts_code: &str,
        adj_type: &str,
        start_date: &str,
        end_date: &str,
    ) -> Result<RowData, String> {
        let mut stmt = self
            .conn
            .prepare_cached(&self.query_sql)
            .map_err(|e| format!("预编译SQL失败:{e}"))?;
        let mut rows = stmt
            .query(params![ts_code, adj_type, start_date, end_date])
            .map_err(|e| format!("执行查询失败:{e}"))?;

        let mut trade_date: Vec<String> = Vec::new();
        let mut cols: HashMap<String, Vec<Option<f64>>> = HashMap::new();
        for (_, key) in &self.cols_table {
            cols.entry(key.clone()).or_default();
        }

        while let Some(row) = rows.next().map_err(|e| format!("读取数据行失败:{e}"))? {
            let d: String = row.get(0).map_err(|e| format!("读取trade_date失败:{e}"))?;
            trade_date.push(d);

            for (i, (_, key)) in self.cols_table.iter().enumerate() {
                let v: Option<f64> = row.get(i + 1).map_err(|e| format!("读取{}失败:{e}", key))?;
                // series是cols里面key对应的那一行的vec
                if let Some(series) = cols.get_mut(key) {
                    series.push(v);
                }
            }
        }

        let is_st = self.st_list.contains(ts_code);
        let zhang = calc_zhang_pct(ts_code, is_st);
        let zhang_series = vec![Some(zhang); trade_date.len()];
        cols.insert("ZHANG".to_string(), zhang_series);

        let out = RowData {
            trade_dates: trade_date,
            cols,
        };
        out.validate()?;
        Ok(out)
    }

    pub fn list_ts_code(
        &self,
        adj_type: &str,
        start_date: &str,
        end_date: &str,
    ) -> Result<Vec<String>, String> {
        let sql = r#"
                SELECT DISTINCT ts_code
                FROM stock_data
                WHERE adj_type = ?
                AND trade_date >= ?
                AND trade_date <= ?
                ORDER BY ts_code ASC
            "#;
        let mut list = Vec::with_capacity(512);
        let mut stmt = self
            .conn
            .prepare(sql)
            .map_err(|e| format!("sql预编译失败:{e}"))?;
        let mut rows = stmt
            .query(params![adj_type, start_date, end_date])
            .map_err(|e| format!("数据库查询失败:{e}"))?;
        while let Some(v) = rows.next().map_err(|e| format!("{e}"))? {
            let tc: String = v.get(0).map_err(|e| format!("{e}"))?;
            list.push(tc);
        }

        Ok(list)
    }
}

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
        let time = time::Instant::now();
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
        println!("summary写入耗时:{:.3?}", time.elapsed());
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
            if trade_dates.len() == sin_rule.series.len() {
                for i in 0..trade_dates.len() {
                    let mut rule_details = Self::default();
                    rule_details.ts_code = ts_code.to_string();
                    rule_details.rule_name = rule_name.clone();
                    rule_details.trade_date = trade_dates[i].clone();
                    rule_details.rule_score = sin_rule.series[i];
                    out.push(rule_details);
                }
            }
        }
        out
    }

    pub fn write_db(db_path: &str, rows: &[ScoreDetails]) -> Result<(), String> {
        let time = time::Instant::now();
        let mut conn =
            Connection::open(db_path).map_err(|e| format!("details数据库连接失败:{e}"))?;
        let tx = conn
            .transaction()
            .map_err(|e| format!("事务创建失败:{e}"))?;
        let del_sql = r#"
                DELETE FROM score_details
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
                .appender("score_details")
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

        println!("details写入耗时:{:.3?}", time.elapsed());
        Ok(())
    }
}

pub fn init_duckdb_database(out_path: &Path) -> Result<(), String> {
    let db_file = Path::new(out_path);
    if let Some(parent_dir) = db_file.parent() {
        if !parent_dir.as_os_str().is_empty() {
            create_dir_all(parent_dir).map_err(|e| format!("创建输出目录失败:{e}"))?;
        }
    }

    let conn = Connection::open(out_path).map_err(|e| format!("打开数据库失败:{e}"))?;

    conn.execute(
        r#"
        CREATE TABLE IF NOT EXISTS score_summary (
            ts_code VARCHAR,
            trade_date VARCHAR,
            total_score DOUBLE,
            rank INTEGER,
            PRIMARY KEY (ts_code, trade_date)
        )
        "#,
        [],
    )
    .map_err(|e| format!("创建score_summary失败:{e}"))?;

    conn.execute(
        r#"
        CREATE TABLE IF NOT EXISTS score_details (
            ts_code VARCHAR,
            trade_date VARCHAR,
            rule_name VARCHAR,
            rule_score DOUBLE,
            PRIMARY KEY (ts_code, trade_date, rule_name)
        )
        "#,
        [],
    )
    .map_err(|e| format!("创建score_details失败:{e}"))?;
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
