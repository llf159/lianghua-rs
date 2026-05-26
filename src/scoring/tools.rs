use std::collections::{HashMap, HashSet};

use duckdb::{params, Connection};

use crate::data::{
    cyq_chen_db_path, load_stock_list, load_trade_date_list, RowData, ScopeWay, ScoreRule,
};
use crate::expr::eval::{Runtime, Value};
use crate::expr::parser::{lex_all, Expr, Parser, Stmt, Stmts};
use crate::utils::utils::eval_binary_for_warmup;
use crate::utils::utils::impl_expr_warmup;

pub const CYQ_CHEN_RUNTIME_FIELDS: [(&str, &str); 14] = [
    ("CYQ_MIN", "min_price"),
    ("CYQ_MAX", "max_price"),
    ("CYQ_MT", "main_total"),
    ("CYQ_RT", "retail_total"),
    ("CYQ_TC", "total_chips"),
    ("CYQ_TPR", "total_profit_ratio"),
    ("CYQ_TTR", "total_trapped_ratio"),
    ("CYQ_PEAK", "chip_peak_price"),
    ("CYQ_P70L", "percent_70_price_low"),
    ("CYQ_P70H", "percent_70_price_high"),
    ("CYQ_P70C", "percent_70_concentration"),
    ("CYQ_P90L", "percent_90_price_low"),
    ("CYQ_P90H", "percent_90_price_high"),
    ("CYQ_P90C", "percent_90_concentration"),
];

const CYQ_CHEN_SNAPSHOT_TABLE: &str = "cyq_chen_snapshot";
const DEFAULT_CYQ_CHEN_ADJ_TYPE: &str = "qfq";

pub fn load_st_list(source_dir: &str) -> Result<HashSet<String>, String> {
    let rows = load_stock_list(source_dir)?;
    let mut st_list = HashSet::new();
    for cols in rows {
        let ts_code = cols
            .first()
            .ok_or_else(|| "stock_list.csv格式错误: 缺少ts_code列".to_string())?;
        let name = cols
            .get(2)
            .ok_or_else(|| "stock_list.csv格式错误: 缺少name列".to_string())?;

        if name.to_ascii_uppercase().contains("ST") {
            st_list.insert(ts_code.trim().to_string());
        }
    }

    Ok(st_list)
}

pub fn warmup_rows_estimate(
    source_dir: &str,
    strategy_path: Option<&str>,
) -> Result<usize, String> {
    // 从拿rule原数据开始计算warmup
    let rules = ScoreRule::load_rules_with_strategy_path(source_dir, strategy_path)?;
    let mut all_expr_max_need = 0;

    for rule in rules {
        let tok = lex_all(&rule.when); // 变成带序号字符
        let mut p = Parser::new(tok); // 变成基础语句
        let stmts = p
            .parse_main()
            .map_err(|e| format!("表达式解析错误在{}:{}", e.idx, e.msg))?;
        let mut locals = HashMap::new();
        let mut consts: HashMap<String, usize> = HashMap::new();
        let mut all_expr_need = 0;
        // println!("{:#?}", stmt);

        for stmt in stmts.item {
            match stmt {
                Stmt::Assign { name, value } => match value {
                    Expr::Number(v) => {
                        consts.insert(name, v as usize);
                    }
                    Expr::Binary { op, lhs, rhs } => {
                        if let Some(out) = eval_binary_for_warmup(&op, &lhs, &rhs, &consts)? {
                            consts.insert(name, out as usize);
                        } else {
                            let value_need =
                                impl_expr_warmup(Expr::Binary { op, lhs, rhs }, &locals, &consts)?;
                            locals.insert(name, value_need);
                        }
                    }
                    _ => {
                        let value_need = impl_expr_warmup(value, &locals, &consts)?;
                        locals.insert(name, value_need);
                    }
                },
                Stmt::Expr(v) => {
                    let expr_need = impl_expr_warmup(v, &locals, &consts)?;
                    if expr_need > all_expr_need {
                        all_expr_need = expr_need
                    }
                }
            }
        }

        let extra_need = match rule.scope_way {
            ScopeWay::Last => 0,
            ScopeWay::Any => rule.scope_windows - 1,
            ScopeWay::Consec(_) => rule.scope_windows - 1,
            ScopeWay::Each => rule.scope_windows - 1,
            ScopeWay::Recent => rule.scope_windows - 1,
        };

        if extra_need + all_expr_need > all_expr_max_need {
            all_expr_max_need = extra_need + all_expr_need;
        }
    }

    Ok(all_expr_max_need)
}

pub fn calc_query_start_date(
    source_dir: &str,
    warmup_need: usize,
    ori_start_date: &str,
) -> Result<String, String> {
    let trade_dates = load_trade_date_list(source_dir)?;
    let anchor_idx = match trade_dates.binary_search_by(|d| d.as_str().cmp(ori_start_date)) {
        Ok(i) => i,
        Err(i) => i,
    };

    if anchor_idx >= trade_dates.len() {
        return Err(format!("起始日期{ori_start_date}晚于交易日历最后一天"));
    }

    let start_idx = anchor_idx.saturating_sub(warmup_need);
    Ok(trade_dates[start_idx].clone())
}

pub fn calc_query_need_rows(
    source_dir: &str,
    warmup_need: usize,
    start_date: &str,
    end_date: &str,
) -> Result<usize, String> {
    let trade_dates = load_trade_date_list(source_dir)?;
    let start_idx = match trade_dates.binary_search_by(|d| d.as_str().cmp(start_date)) {
        Ok(i) => i,
        Err(i) => i,
    };

    if start_idx >= trade_dates.len() {
        return Err(format!("起始日期{start_date}晚于交易日历最后一天"));
    }

    let end_exclusive = match trade_dates.binary_search_by(|d| d.as_str().cmp(end_date)) {
        Ok(i) => i + 1,
        Err(i) => i,
    };

    let range_need = end_exclusive.saturating_sub(start_idx);
    Ok((warmup_need + range_need).max(1))
}

pub fn rt_max_len(rt: &Runtime) -> usize {
    let mut max_len = 1;
    for v in rt.vars.values() {
        let len = match v {
            Value::Num(_) | Value::Bool(_) => 1,
            Value::NumSeries(ns) => ns.len(),
            Value::SharedNumSeries(ns) => ns.len(),
            Value::BoolSeries(bs) => bs.len(),
        };
        if len > max_len {
            max_len = len;
        }
    }
    max_len
}

pub fn cyq_chen_runtime_key_names() -> Vec<&'static str> {
    CYQ_CHEN_RUNTIME_FIELDS
        .iter()
        .map(|(runtime_key, _)| *runtime_key)
        .collect()
}

pub fn collect_used_cyq_chen_runtime_keys(programs: &[&Stmts]) -> HashSet<String> {
    let mut out = HashSet::new();
    for (runtime_key, _) in CYQ_CHEN_RUNTIME_FIELDS {
        if programs
            .iter()
            .any(|program| crate::data::expr_program_uses_runtime_key(program, runtime_key))
        {
            out.insert(runtime_key.to_string());
        }
    }
    out
}

fn used_cyq_chen_fields(used_keys: &HashSet<String>) -> Vec<(&'static str, &'static str)> {
    CYQ_CHEN_RUNTIME_FIELDS
        .iter()
        .copied()
        .filter(|(runtime_key, _)| used_keys.contains(*runtime_key))
        .collect()
}

fn insert_empty_cyq_chen_fields(row_data: &mut RowData, fields: &[(&str, &str)]) {
    let len = row_data.trade_dates.len();
    for (runtime_key, _) in fields {
        row_data
            .cols
            .entry((*runtime_key).to_string())
            .or_insert_with(|| vec![None; len]);
    }
}

pub struct CyqChenFieldInjector {
    conn: Option<Connection>,
    fields: Vec<(&'static str, &'static str)>,
    available_fields: Vec<(&'static str, &'static str)>,
    select_sql: Option<String>,
    unavailable_warning: Option<String>,
}

impl CyqChenFieldInjector {
    pub fn new(source_dir: &str, used_keys: &HashSet<String>) -> Self {
        let fields = used_cyq_chen_fields(used_keys);
        if fields.is_empty() {
            return Self {
                conn: None,
                fields,
                available_fields: Vec::new(),
                select_sql: None,
                unavailable_warning: None,
            };
        }

        let cyq_db = cyq_chen_db_path(source_dir);
        if !cyq_db.exists() {
            return Self::unavailable(fields, "cyq_chen.db 不存在，新筹码字段已按空值注入。");
        }

        let conn = match Connection::open(&cyq_db) {
            Ok(conn) => conn,
            Err(error) => {
                return Self::unavailable(
                    fields,
                    format!("打开 cyq_chen.db 失败: {error}；新筹码字段已按空值注入。"),
                );
            }
        };

        match cyq_chen_table_exists(&conn) {
            Ok(true) => {}
            Ok(false) => {
                return Self::unavailable(
                    fields,
                    "cyq_chen_snapshot 表不存在，新筹码字段已按空值注入。",
                );
            }
            Err(error) => {
                return Self::unavailable(fields, format!("{error}；新筹码字段已按空值注入。"));
            }
        }

        let existing_columns = match cyq_chen_existing_columns(&conn) {
            Ok(columns) => columns,
            Err(error) => {
                return Self::unavailable(fields, format!("{error}；新筹码字段已按空值注入。"));
            }
        };
        let available_fields = fields
            .iter()
            .copied()
            .filter(|(_, db_col)| existing_columns.contains(&db_col.to_ascii_lowercase()))
            .collect::<Vec<_>>();
        if available_fields.is_empty() {
            return Self::unavailable(
                fields,
                "cyq_chen_snapshot 缺少请求的新筹码字段，已按空值注入。",
            );
        }

        let select_sql = Some(build_cyq_chen_select_sql(&available_fields));
        Self {
            conn: Some(conn),
            fields,
            available_fields,
            select_sql,
            unavailable_warning: None,
        }
    }

    fn unavailable(fields: Vec<(&'static str, &'static str)>, warning: impl Into<String>) -> Self {
        Self {
            conn: None,
            fields,
            available_fields: Vec::new(),
            select_sql: None,
            unavailable_warning: Some(warning.into()),
        }
    }

    pub fn inject(&self, row_data: &mut RowData, ts_code: &str) -> Vec<String> {
        if self.fields.is_empty() {
            return Vec::new();
        }
        insert_empty_cyq_chen_fields(row_data, &self.fields);
        if row_data.trade_dates.is_empty() {
            return vec!["RowData 为空，新筹码字段已按空值注入。".to_string()];
        }
        if let Some(warning) = &self.unavailable_warning {
            return vec![warning.clone()];
        }

        let Some(conn) = &self.conn else {
            return vec!["新筹码字段不可用，已按空值注入。".to_string()];
        };
        let Some(sql) = &self.select_sql else {
            return vec!["新筹码字段查询不可用，已按空值注入。".to_string()];
        };
        let first_date = row_data.trade_dates.first().cloned().unwrap_or_default();
        let last_date = row_data.trade_dates.last().cloned().unwrap_or_default();
        let date_index = row_data
            .trade_dates
            .iter()
            .enumerate()
            .map(|(index, trade_date)| (trade_date.as_str(), index))
            .collect::<HashMap<_, _>>();

        let mut stmt = match conn.prepare_cached(sql) {
            Ok(stmt) => stmt,
            Err(error) => {
                return vec![format!(
                    "预编译新筹码字段查询失败: {error}；新筹码字段已按空值注入。"
                )];
            }
        };
        let mut rows = match stmt.query(params![
            ts_code,
            DEFAULT_CYQ_CHEN_ADJ_TYPE,
            first_date.as_str(),
            last_date.as_str()
        ]) {
            Ok(rows) => rows,
            Err(error) => {
                return vec![format!(
                    "查询新筹码字段失败: {error}；新筹码字段已按空值注入。"
                )];
            }
        };

        let mut row_count = 0usize;
        while let Ok(Some(row)) = rows.next() {
            let Ok(trade_date) = row.get::<_, String>(0) else {
                continue;
            };
            let Some(row_index) = date_index.get(trade_date.as_str()).copied() else {
                continue;
            };
            row_count += 1;
            for (field_index, (runtime_key, _)) in self.available_fields.iter().enumerate() {
                let value = row.get::<_, Option<f64>>(field_index + 1).ok().flatten();
                if let Some(series) = row_data.cols.get_mut(*runtime_key) {
                    if let Some(slot) = series.get_mut(row_index) {
                        *slot = value;
                    }
                }
            }
        }

        if row_count == 0 {
            vec![format!(
                "{ts_code} 在新筹码库区间 {first_date} 至 {last_date} 没有匹配数据，新筹码字段已按空值注入。"
            )]
        } else {
            Vec::new()
        }
    }
}

fn build_cyq_chen_select_sql(available_fields: &[(&'static str, &'static str)]) -> String {
    let mut select_cols = vec!["trade_date".to_string()];
    for (_, db_col) in available_fields {
        select_cols.push(format!("TRY_CAST(\"{db_col}\" AS DOUBLE) AS \"{db_col}\""));
    }

    format!(
        r#"
        SELECT {}
        FROM {CYQ_CHEN_SNAPSHOT_TABLE}
        WHERE ts_code = ?
          AND adj_type = ?
          AND trade_date >= ?
          AND trade_date <= ?
        ORDER BY trade_date ASC
        "#,
        select_cols.join(", ")
    )
}

pub fn inject_empty_optional_cyq_chen_fields(
    row_data: &mut RowData,
    used_keys: &HashSet<String>,
) -> Result<(), String> {
    let fields = used_cyq_chen_fields(used_keys);
    insert_empty_cyq_chen_fields(row_data, &fields);
    row_data.validate()
}

fn cyq_chen_table_exists(conn: &Connection) -> Result<bool, String> {
    conn.query_row(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
        [CYQ_CHEN_SNAPSHOT_TABLE],
        |row| row.get::<_, i64>(0),
    )
    .map(|count| count > 0)
    .map_err(|e| format!("检查 cyq_chen_snapshot 表失败: {e}"))
}

fn cyq_chen_existing_columns(conn: &Connection) -> Result<HashSet<String>, String> {
    let mut stmt = conn
        .prepare("SELECT column_name FROM information_schema.columns WHERE table_name = ?")
        .map_err(|e| format!("预编译 cyq_chen_snapshot 字段查询失败: {e}"))?;
    let mut rows = stmt
        .query([CYQ_CHEN_SNAPSHOT_TABLE])
        .map_err(|e| format!("查询 cyq_chen_snapshot 字段失败: {e}"))?;
    let mut out = HashSet::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取 cyq_chen_snapshot 字段失败: {e}"))?
    {
        let name: String = row
            .get(0)
            .map_err(|e| format!("读取 cyq_chen_snapshot 字段名失败: {e}"))?;
        out.insert(name.to_ascii_lowercase());
    }
    Ok(out)
}

fn format_cyq_chen_used_keys(used_keys: &HashSet<String>) -> String {
    let mut keys = used_keys.iter().cloned().collect::<Vec<_>>();
    keys.sort();
    keys.join(", ")
}

pub fn preview_optional_cyq_chen_injection_warnings(
    source_dir: &str,
    start_date: &str,
    end_date: &str,
    warmup_need: usize,
    used_keys: &HashSet<String>,
) -> Vec<String> {
    let fields = used_cyq_chen_fields(used_keys);
    if fields.is_empty() {
        return Vec::new();
    }

    let used_keys_text = format_cyq_chen_used_keys(used_keys);
    let cyq_db = cyq_chen_db_path(source_dir);
    if !cyq_db.exists() {
        return vec![format!(
            "策略使用新筹码字段 {used_keys_text}，但 cyq_chen.db 不存在；本次会按空值注入这些字段。"
        )];
    }

    let mut warnings = Vec::new();
    let conn = match Connection::open(&cyq_db) {
        Ok(conn) => conn,
        Err(error) => {
            return vec![format!(
                "策略使用新筹码字段 {used_keys_text}，但打开 cyq_chen.db 失败: {error}；本次会按空值注入这些字段。"
            )];
        }
    };

    match cyq_chen_table_exists(&conn) {
        Ok(true) => {}
        Ok(false) => {
            return vec![format!(
                "策略使用新筹码字段 {used_keys_text}，但 cyq_chen_snapshot 表不存在；本次会按空值注入这些字段。"
            )];
        }
        Err(error) => {
            return vec![format!(
                "策略使用新筹码字段 {used_keys_text}，但检查 cyq_chen_snapshot 失败: {error}；本次会按空值注入这些字段。"
            )];
        }
    }

    match cyq_chen_existing_columns(&conn) {
        Ok(columns) => {
            let missing = fields
                .iter()
                .filter(|(_, db_col)| !columns.contains(&db_col.to_ascii_lowercase()))
                .map(|(runtime_key, _)| *runtime_key)
                .collect::<Vec<_>>();
            if !missing.is_empty() {
                warnings.push(format!(
                    "cyq_chen_snapshot 缺少字段 {}；对应运行时字段本次会按空值注入。",
                    missing.join(", ")
                ));
            }
        }
        Err(error) => warnings.push(format!(
            "检查 cyq_chen_snapshot 字段失败: {error}；新筹码运行时字段可能按空值注入。"
        )),
    }

    let load_start_date = load_trade_date_list(source_dir)
        .ok()
        .and_then(|trade_dates| {
            let anchor = trade_dates
                .binary_search_by(|date| date.as_str().cmp(start_date))
                .unwrap_or_else(|index| index);
            if anchor >= trade_dates.len() {
                return None;
            }
            Some((
                trade_dates[anchor.saturating_sub(warmup_need)].clone(),
                trade_dates
                    .iter()
                    .filter(|date| {
                        let date = date.as_str();
                        date >= trade_dates[anchor.saturating_sub(warmup_need)].as_str()
                            && date <= end_date
                    })
                    .count(),
            ))
        });
    let (load_start_date, expected_trade_dates) =
        load_start_date.unwrap_or_else(|| (start_date.to_string(), 0));

    let range_info = conn.query_row(
        &format!(
            "SELECT MIN(trade_date), MAX(trade_date), COUNT(DISTINCT trade_date) FROM {CYQ_CHEN_SNAPSHOT_TABLE} WHERE adj_type = ? AND trade_date >= ? AND trade_date <= ?"
        ),
        params![DEFAULT_CYQ_CHEN_ADJ_TYPE, load_start_date.as_str(), end_date],
        |row| {
            Ok((
                row.get::<_, Option<String>>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, i64>(2)?,
            ))
        },
    );
    match range_info {
        Ok((Some(min_date), Some(max_date), actual_dates)) => {
            if min_date.as_str() > load_start_date.as_str() {
                warnings.push(format!(
                    "新筹码库最早可用日期为 {min_date}，晚于表达式预热加载起点 {load_start_date}；更早预热段的新筹码字段会为空。"
                ));
            }
            if max_date.as_str() < end_date {
                warnings.push(format!(
                    "新筹码库最新可用日期为 {max_date}，早于本次结束日期 {end_date}；之后的新筹码字段会为空。"
                ));
            }
            if expected_trade_dates > 0 && (actual_dates.max(0) as usize) < expected_trade_dates {
                warnings.push(format!(
                    "新筹码库在预热/计算区间 {load_start_date} 至 {end_date} 只有 {} 个交易日，交易日历有 {expected_trade_dates} 个；缺口会按空值处理。",
                    actual_dates.max(0)
                ));
            }
        }
        Ok((_, _, _)) => warnings.push(format!(
            "新筹码库在预热/计算区间 {load_start_date} 至 {end_date} 没有数据；相关字段会按空值注入。"
        )),
        Err(error) => warnings.push(format!(
            "读取新筹码库日期范围失败: {error}；相关字段会尽量按空值注入。"
        )),
    }

    warnings
}

pub fn inject_optional_cyq_chen_fields(
    row_data: &mut RowData,
    source_dir: &str,
    ts_code: &str,
    used_keys: &HashSet<String>,
) -> Vec<String> {
    CyqChenFieldInjector::new(source_dir, used_keys).inject(row_data, ts_code)
}

pub fn calc_zhang_pct(ts_code: &str, is_st: bool) -> f64 {
    let ts = ts_code.trim().to_ascii_uppercase();
    let (core, suffix) = ts.split_once('.').unwrap_or((ts.as_str(), ""));

    if is_st {
        0.045
    } else if suffix == "BJ" {
        0.295
    } else if core.starts_with("30") || core.starts_with("68") {
        0.195
    } else {
        0.095
    }
}

pub fn inject_constant_num_fields(
    row_data: &mut RowData,
    fields: &[(&str, Option<f64>)],
) -> Result<(), String> {
    let len = row_data.trade_dates.len();
    for (key, value) in fields {
        row_data.cols.insert((*key).to_string(), vec![*value; len]);
    }
    row_data.validate()
}

pub fn inject_latest_num_fields(
    row_data: &mut RowData,
    fields: &[(&str, Option<f64>)],
) -> Result<(), String> {
    let len = row_data.trade_dates.len();
    for (key, value) in fields {
        let mut series = vec![None; len];
        if let Some(last) = series.last_mut() {
            *last = *value;
        }
        row_data.cols.insert((*key).to_string(), series);
    }
    row_data.validate()
}

pub fn inject_stock_extra_fields(
    row_data: &mut RowData,
    ts_code: &str,
    is_st: bool,
    fallback_total_share: Option<f64>,
) -> Result<(), String> {
    inject_constant_num_fields(row_data, &[("ZHANG", Some(calc_zhang_pct(ts_code, is_st)))])?;

    let len = row_data.trade_dates.len();
    let close_series = row_data.cols.get("C");
    let total_share_series = row_data.cols.get("TOTAL_SHARE");
    let total_mv_yi_series = (0..len)
        .map(|index| {
            let close = close_series
                .and_then(|series| series.get(index).copied().flatten())
                .filter(|value| value.is_finite() && *value > 0.0)?;
            let total_share = total_share_series
                .and_then(|series| series.get(index).copied().flatten())
                .or(fallback_total_share)
                .filter(|value| value.is_finite() && *value > 0.0)?;
            Some(total_share * close / 1e4)
        })
        .collect::<Vec<_>>();

    row_data
        .cols
        .insert("TOTAL_MV_YI".to_string(), total_mv_yi_series);
    row_data.validate()
}

pub fn load_total_share_map(source_dir: &str) -> Result<HashMap<String, f64>, String> {
    let rows = load_stock_list(source_dir)?;
    let mut out = HashMap::with_capacity(rows.len());
    for cols in rows {
        let Some(ts_code) = cols
            .first()
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
        else {
            continue;
        };
        let Some(total_share_raw) = cols
            .get(7)
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
        else {
            continue;
        };
        let Ok(total_share) = total_share_raw.parse::<f64>() else {
            continue;
        };
        if total_share > 0.0 && total_share.is_finite() {
            out.insert(ts_code.to_string(), total_share);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use super::{inject_optional_cyq_chen_fields, inject_stock_extra_fields};
    use crate::data::RowData;

    #[test]
    fn stock_extra_fields_compute_total_mv_yi_from_total_share_and_close() {
        let mut row_data = RowData {
            trade_dates: vec!["20240102".to_string(), "20240103".to_string()],
            cols: HashMap::from([("C".to_string(), vec![Some(10.0), Some(12.0)])]),
        };

        inject_stock_extra_fields(&mut row_data, "000001.SZ", false, Some(20_000.0))
            .expect("inject stock extra fields");

        assert_eq!(
            row_data.cols.get("TOTAL_MV_YI").map(Vec::as_slice),
            Some([Some(20.0), Some(24.0)].as_slice())
        );
    }

    #[test]
    fn stock_extra_fields_prefers_row_total_share_when_present() {
        let mut row_data = RowData {
            trade_dates: vec!["20240102".to_string(), "20240103".to_string()],
            cols: HashMap::from([
                ("C".to_string(), vec![Some(10.0), Some(12.0)]),
                ("TOTAL_SHARE".to_string(), vec![Some(30_000.0), None]),
            ]),
        };

        inject_stock_extra_fields(&mut row_data, "000001.SZ", false, Some(20_000.0))
            .expect("inject stock extra fields");

        assert_eq!(
            row_data.cols.get("TOTAL_MV_YI").map(Vec::as_slice),
            Some([Some(30.0), Some(24.0)].as_slice())
        );
    }

    #[test]
    fn optional_cyq_chen_fields_inject_empty_series_when_db_is_missing() {
        let mut row_data = RowData {
            trade_dates: vec!["20240102".to_string(), "20240103".to_string()],
            cols: HashMap::from([("C".to_string(), vec![Some(10.0), Some(12.0)])]),
        };
        let used_keys = HashSet::from(["CYQ_TPR".to_string()]);
        let missing_source_dir =
            std::env::temp_dir().join(format!("lianghua-missing-cyq-chen-{}", std::process::id()));

        let warnings = inject_optional_cyq_chen_fields(
            &mut row_data,
            missing_source_dir.to_str().expect("utf8 temp path"),
            "000001.SZ",
            &used_keys,
        );

        assert!(!warnings.is_empty());
        assert_eq!(
            row_data.cols.get("CYQ_TPR").map(Vec::as_slice),
            Some([None, None].as_slice())
        );
        row_data.validate().expect("row shape remains valid");
    }
}
