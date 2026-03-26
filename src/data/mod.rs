pub mod download_data;
pub mod scoring_data;

use std::{
    collections::{HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
};

use duckdb::{Connection, params};
use serde::{Deserialize, Deserializer, de};

pub fn source_db_path(source_dir: &str) -> PathBuf {
    Path::new(source_dir).join("stock_data.db")
}

pub fn result_db_path(source_dir: &str) -> PathBuf {
    Path::new(source_dir).join("scoring_result.db")
}

pub fn ths_concepts_path(source_dir: &str) -> PathBuf {
    Path::new(source_dir).join("stock_concepts.csv")
}

pub fn stock_list_path(source_dir: &str) -> PathBuf {
    Path::new(source_dir).join("stock_list.csv")
}

pub fn trade_calendar_path(source_dir: &str) -> PathBuf {
    Path::new(source_dir).join("trade_calendar.csv")
}

pub fn score_rule_path(source_dir: &str) -> PathBuf {
    Path::new(source_dir).join("score_rule.toml")
}

pub fn ind_toml_path(source_dir: &str) -> PathBuf {
    Path::new(source_dir).join("ind.toml")
}

pub fn load_stock_list(source_dir: &str) -> Result<Vec<Vec<String>>, String> {
    let path = stock_list_path(source_dir);
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_path(&path)
        .map_err(|e| format!("打开stock_list.csv失败:路径:{:?},错误:{e}", path))?;

    let mut rows = Vec::new();
    for row_result in reader.records() {
        let row = row_result.map_err(|e| format!("解析stock_list.csv失败:{e}"))?;
        rows.push(row.iter().map(|value| value.to_string()).collect());
    }

    Ok(rows)
}

pub fn load_trade_date_list(source_dir: &str) -> Result<Vec<String>, String> {
    let path = trade_calendar_path(source_dir);
    let text = fs::read_to_string(&path).map_err(|e| format!("读取trade_calendar.csv失败:{e}"))?;
    let mut trade_date_list = Vec::with_capacity(1024);

    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.eq_ignore_ascii_case("cal_date") {
            continue;
        }
        trade_date_list.push(line.to_string());
    }

    Ok(trade_date_list)
}

pub fn load_ths_concepts_list(source_dir: &str) -> Result<Vec<Vec<String>>, String> {
    let path = ths_concepts_path(source_dir);

    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_path(&path)
        .map_err(|e| format!("打开stock_concepts.csv失败:路径:{:?},错误:{e}", path))?;

    let mut concept_list = Vec::with_capacity(6000);
    for row_result in reader.records() {
        let row = row_result.map_err(|e| format!("解析stock_concepts.csv失败:{e}"))?;
        concept_list.push(row.iter().map(|value| value.to_string()).collect());
    }

    Ok(concept_list)
}

// ============================================ 原数据部分 ================================================

#[derive(Debug, Clone)]
pub struct RowData {
    pub trade_dates: Vec<String>,
    pub cols: HashMap<String, Vec<Option<f64>>>,
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
}

pub struct DataReader {
    pub conn: Connection,
    pub query_sql: String,
    pub query_tail_rows_sql: String,
    pub cols_table: Vec<(String, String)>, // 数据库列名, runtime规范列名
}

impl DataReader {
    pub fn new(source_dir: &str) -> Result<Self, String> {
        let source_db = source_db_path(source_dir);
        let source_db_str = source_db
            .to_str()
            .ok_or_else(|| "source_db路径不是有效UTF-8".to_string())?;
        let conn = Connection::open(source_db_str).map_err(|e| format!("数据库连接错误:{e}"))?;

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

        let base_pairs = [
            ("open", "O"),
            ("high", "H"),
            ("low", "L"),
            ("close", "C"),
            ("vol", "V"),
            ("amount", "AMOUNT"),
            ("pre_close", "PRE_CLOSE"),
            ("change", "CHANGE"),
            ("pct_chg", "PCT_CHG"),
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

        for col in &all_cols_name {
            let low = col.to_ascii_lowercase();
            if matches!(low.as_str(), "ts_code" | "trade_date" | "adj_type") {
                continue;
            }
            if matches!(
                low.as_str(),
                "open"
                    | "high"
                    | "low"
                    | "close"
                    | "vol"
                    | "amount"
                    | "pre_close"
                    | "change"
                    | "pct_chg"
            ) {
                continue;
            }
            db_cols_table.push((col.clone(), col.to_ascii_uppercase()));
        }

        let mut select_cols = vec!["trade_date".to_string()];
        for (db_col, _) in &db_cols_table {
            select_cols.push(format!(
                "TRY_CAST(\"{}\" AS DOUBLE) AS \"{}\"",
                db_col, db_col
            ));
        }

        let query_sql = format!(
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
            select_cols.join(",\n")
        );

        let query_tail_rows_sql = format!(
            r#"
                SELECT
                    {}
                FROM stock_data
                WHERE ts_code = ?
                  AND adj_type = ?
                  AND trade_date <= ?
                ORDER BY trade_date DESC
                LIMIT ?
            "#,
            select_cols.join(",\n")
        );

        Ok(Self {
            conn,
            query_sql,
            query_tail_rows_sql,
            cols_table: db_cols_table,
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

        let mut trade_dates = Vec::new();
        let mut cols: HashMap<String, Vec<Option<f64>>> = HashMap::new();
        for (_, key) in &self.cols_table {
            cols.entry(key.clone()).or_default();
        }

        while let Some(row) = rows.next().map_err(|e| format!("读取数据行失败:{e}"))? {
            let trade_date: String = row.get(0).map_err(|e| format!("读取trade_date失败:{e}"))?;
            trade_dates.push(trade_date);

            for (i, (_, key)) in self.cols_table.iter().enumerate() {
                let value: Option<f64> =
                    row.get(i + 1).map_err(|e| format!("读取{}失败:{e}", key))?;
                if let Some(series) = cols.get_mut(key) {
                    series.push(value);
                }
            }
        }

        let out = RowData { trade_dates, cols };
        out.validate()?;
        Ok(out)
    }

    pub fn load_one_tail_rows(
        &self,
        ts_code: &str,
        adj_type: &str,
        end_date: &str,
        need_rows: usize,
    ) -> Result<RowData, String> {
        if need_rows == 0 {
            return Err("need_rows不能为0".to_string());
        }

        let mut stmt = self
            .conn
            .prepare_cached(&self.query_tail_rows_sql)
            .map_err(|e| format!("预编译SQL失败:{e}"))?;
        let mut rows = stmt
            .query(params![ts_code, adj_type, end_date, need_rows as i64])
            .map_err(|e| format!("执行查询失败:{e}"))?;

        let mut trade_dates = Vec::new();
        let mut cols: HashMap<String, Vec<Option<f64>>> = HashMap::new();
        for (_, key) in &self.cols_table {
            cols.entry(key.clone()).or_default();
        }

        while let Some(row) = rows.next().map_err(|e| format!("读取数据行失败:{e}"))? {
            let trade_date: String = row.get(0).map_err(|e| format!("读取trade_date失败:{e}"))?;
            trade_dates.push(trade_date);

            for (i, (_, key)) in self.cols_table.iter().enumerate() {
                let value: Option<f64> =
                    row.get(i + 1).map_err(|e| format!("读取{}失败:{e}", key))?;
                if let Some(series) = cols.get_mut(key) {
                    series.push(value);
                }
            }
        }

        trade_dates.reverse();
        for series in cols.values_mut() {
            series.reverse();
        }

        let out = RowData { trade_dates, cols };
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

        while let Some(row) = rows.next().map_err(|e| format!("{e}"))? {
            let ts_code: String = row.get(0).map_err(|e| format!("{e}"))?;
            list.push(ts_code);
        }

        Ok(list)
    }
}

// ============================================ 策略部分 ================================================

// 设计的时候字段要完全适配文本,用Deserialize映射key
impl<'de> Deserialize<'de> for ScopeWay {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        let s = raw.trim().to_ascii_uppercase();

        match s.as_str() {
            "ANY" => Ok(ScopeWay::Any),
            "LAST" => Ok(ScopeWay::Last),
            "EACH" => Ok(ScopeWay::Each),
            "RECENT" => Ok(ScopeWay::Recent),
            _ => {
                if let Some(num) = s.strip_prefix("CONSEC>=") {
                    let k = num
                        .parse::<usize>()
                        .map_err(|_| de::Error::custom("CONSEC>= 后必须是正整数"))?;
                    if k == 0 {
                        return Err(de::Error::custom("CONSEC>=0 无效，必须 >= 1"));
                    }
                    Ok(ScopeWay::Consec(k))
                } else {
                    Err(de::Error::custom(
                        "scope_way 仅支持 ANY/LAST/EACH/RECENT/CONSEC>=N",
                    ))
                }
            }
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct ScoreConfig {
    pub version: u32,
    pub rule: Vec<ScoreRule>,
}

#[derive(Debug, Clone, Copy)]
pub enum ScopeWay {
    Any,
    Last,
    Each,
    Recent,
    Consec(usize),
}

#[derive(Debug, Clone, Copy, Default, serde::Deserialize, serde::Serialize)]
pub enum RuleTag {
    #[default]
    Normal,
    Opportunity,
    Rare,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DistPoint {
    pub min: usize,
    pub max: usize,
    pub points: f64,
}

#[derive(Debug, Deserialize)]
pub struct ScoreRule {
    pub name: String,
    pub scope_windows: usize,
    pub scope_way: ScopeWay,
    pub when: String,
    pub points: f64,
    pub dist_points: Option<Vec<DistPoint>>,
    pub explain: String,
    #[serde(default)]
    pub tag: RuleTag,
}

impl ScoreRule {
    pub fn load_rules(source_dir: &str) -> Result<Vec<ScoreRule>, String> {
        let rule_path = score_rule_path(source_dir);
        let rule_toml = fs::read_to_string(&rule_path).map_err(|e| {
            format!(
                "规则文件不存在或不可读: path={}, err={e}",
                rule_path.display()
            )
        })?;
        let cfg: ScoreConfig =
            toml::from_str(&rule_toml).map_err(|e| format!("规则文件格式错误: {e}"))?;
        Self::validate_rules(&cfg.rule)?;
        Ok(cfg.rule)
    }

    fn validate_rules(rules: &[ScoreRule]) -> Result<(), String> {
        for (i, r) in rules.iter().enumerate() {
            let n = i + 1;
            if r.name.trim().is_empty() {
                return Err(format!("第{:?}个表达式name字段为空", n));
            };
            if r.when.trim().is_empty() {
                return Err(format!("第{:?}个表达式when字段为空", n));
            };

            let has_points = r.points.is_finite();
            let has_dist = matches!(r.dist_points.as_ref(), Some(v) if !v.is_empty());
            if !has_points && !has_dist {
                return Err(format!(
                    "第{n}条规则 points 和 dist_points 不能同时无效/为空"
                ));
            }

            if r.dist_points.is_some() {
                let Some(dist) = &r.dist_points else {
                    return Err(format!("第{:?}个表达式dist_points字段错误", n));
                };
                for (j, v) in dist.iter().enumerate() {
                    if v.min > v.max {
                        return Err(format!("第{n}条规则 dist_points 第{}段 min > max", j + 1));
                    }
                    if !v.points.is_finite() {
                        return Err(format!("第{n}条规则 dist_points 第{}段 points 非法", j + 1));
                    }
                }
                let mut dist: Vec<&DistPoint> = dist.iter().collect();
                dist.sort_by_key(|x| x.min);
                for k in 1..dist.len() {
                    let prev = dist[k - 1];
                    let curr = dist[k];

                    if prev.max >= curr.min {
                        return Err(format!(
                            "区间重叠: [{}-{}] 和 [{}-{}]",
                            prev.min, prev.max, curr.min, curr.max
                        ));
                    }
                }
            }
            if r.scope_windows == 0 {
                return Err(format!("第{:?}个表达式scope_windows字段错误", n));
            };
        }
        Ok(())
    }
}

// ============================================ 指标部分 ================================================

#[derive(Deserialize)]
pub struct IndsData {
    pub version: u32,
    pub ind: Vec<IndData>,
}

#[derive(serde::Deserialize)]
pub struct IndData {
    pub name: String,
    pub expr: String,
    pub prec: usize,
}

impl IndsData {
    pub fn load_inds(source_dir: &str) -> Result<Vec<IndData>, String> {
        let ind_path = ind_toml_path(source_dir);
        let ind_toml = fs::read_to_string(&ind_path).map_err(|e| {
            format!(
                "指标文件不存在或不可读: path={}, err={e}",
                ind_path.display()
            )
        })?;
        let mut cfg: IndsData =
            toml::from_str(&ind_toml).map_err(|e| format!("指标文件格式错误: {e}"))?;

        for ind in &mut cfg.ind {
            ind.name = ind.name.trim().to_ascii_uppercase();
        }

        Self::validate_inds(&cfg.ind)?;
        Ok(cfg.ind)
    }

    fn validate_inds(inds: &[IndData]) -> Result<(), String> {
        let mut seen = HashSet::new();

        for (i, ind) in inds.iter().enumerate() {
            let n = i + 1;
            let name = ind.name.trim();

            if name.is_empty() {
                return Err(format!("第{n}个指标的输出名称为空"));
            } else {
                let mut chars = name.chars();

                let Some(first) = chars.next() else {
                    return Err(format!(
                        "第{n}个指标名称非法: {name}，只允许 ASCII 字母/数字/_，且不能以数字开头"
                    ));
                };

                if !(first.is_ascii_alphabetic() || first == '_') {
                    return Err(format!("第{n}个指标名称非法: {name}，不能以数字开头"));
                }

                if !chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_') {
                    return Err(format!(
                        "第{n}个指标名称非法: {name}，只允许 ASCII 字母/数字/_"
                    ));
                }
            }

            if !seen.insert(name.to_string()) {
                return Err(format!("第{n}个指标名称重复: {name}"));
            }

            if ind.expr.trim().is_empty() {
                return Err(format!("第{n}个指标的表达式为空"));
            }
        }
        Ok(())
    }
}
