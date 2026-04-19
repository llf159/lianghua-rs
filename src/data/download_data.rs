use std::{collections::HashMap, fs::create_dir_all, path::Path};

use duckdb::{Connection, ToSql, params, params_from_iter};

use crate::{
    crawler::concept::ThsConceptRow,
    data::{source_db_path, stock_list_path, ths_concepts_path, trade_calendar_path},
    download::{AdjType, ProBarRow, StockListRow, TradeCalRow},
};

fn adj_type_name(adj_type: AdjType) -> &'static str {
    match adj_type {
        AdjType::Qfq => "qfq",
        AdjType::Hfq => "hfq",
        AdjType::Raw => "raw",
        AdjType::Ind => "ind",
    }
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn round_to(value: f64, scale: i32) -> f64 {
    let factor = 10_f64.powi(scale);
    (value * factor).round() / factor
}

fn round_opt_to(value: Option<f64>, scale: i32) -> Option<f64> {
    value.map(|v| round_to(v, scale))
}

const STOCK_DATA_TABLE: &str = "stock_data";
const STOCK_DATA_STAGE_TABLE: &str = "stock_data_stage";

const STOCK_DATA_INSERT_COLUMNS: [&str; 13] = [
    "ts_code",
    "trade_date",
    "adj_type",
    "open",
    "high",
    "low",
    "close",
    "pre_close",
    "change",
    "pct_chg",
    "vol",
    "amount",
    "tor",
];

fn validate_indicator_series(
    rows: &[ProBarRow],
    indicators: &HashMap<String, Vec<Option<f64>>>,
) -> Result<Vec<String>, String> {
    let mut indicator_names = indicators.keys().cloned().collect::<Vec<_>>();
    indicator_names.sort();

    for name in &indicator_names {
        let Some(series) = indicators.get(name) else {
            return Err(format!("缺少指标{name}的数据"));
        };
        if series.len() != rows.len() {
            return Err(format!(
                "指标{name}长度与行情行数不一致: {} != {}",
                series.len(),
                rows.len()
            ));
        }
    }

    Ok(indicator_names)
}

fn load_table_columns(conn: &Connection, table_name: &str) -> Result<Vec<String>, String> {
    let mut stmt = conn
        .prepare(&format!("DESCRIBE {table_name}"))
        .map_err(|e| format!("读取{table_name}表结构失败:{e}"))?;
    let mut rows = stmt
        .query([])
        .map_err(|e| format!("查询{table_name}表结构失败:{e}"))?;
    let mut existing = Vec::with_capacity(64);

    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取{table_name}列失败:{e}"))?
    {
        let name: String = row.get(0).map_err(|e| format!("读取列名失败:{e}"))?;
        existing.push(name);
    }

    Ok(existing)
}

fn resolve_appender_columns(
    conn: &Connection,
    table_name: &str,
    requested_columns: &[&str],
) -> Result<Vec<String>, String> {
    let existing = load_table_columns(conn, table_name)?;
    let mut resolved = Vec::with_capacity(requested_columns.len());

    for requested in requested_columns {
        if let Some(actual) = existing
            .iter()
            .find(|name| name.eq_ignore_ascii_case(requested))
        {
            resolved.push(actual.clone());
        } else {
            resolved.push((*requested).to_string());
        }
    }

    Ok(resolved)
}

fn append_rows_to_table(
    conn: &Connection,
    table_name: &str,
    adj_type: AdjType,
    rows: &[ProBarRow],
    indicators: Option<&HashMap<String, Vec<Option<f64>>>>,
) -> Result<(), String> {
    if rows.is_empty() {
        return Ok(());
    }

    let indicator_names = match indicators {
        Some(indicators) if !indicators.is_empty() => validate_indicator_series(rows, indicators)?,
        _ => Vec::new(),
    };
    let adj_type = adj_type_name(adj_type);
    let mut requested_columns = STOCK_DATA_INSERT_COLUMNS.to_vec();
    requested_columns.extend(indicator_names.iter().map(String::as_str));
    let resolved_columns = resolve_appender_columns(conn, table_name, &requested_columns)?;
    let resolved_column_refs = resolved_columns
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();

    let mut appender = conn
        .appender_with_columns(table_name, &resolved_column_refs)
        .map_err(|e| format!("创建 {table_name} Appender失败: {e}"))?;

    for (row_idx, row) in rows.iter().enumerate() {
        let open = round_to(row.open, 2);
        let high = round_to(row.high, 2);
        let low = round_to(row.low, 2);
        let close = round_to(row.close, 2);
        let pre_close = round_to(row.pre_close, 2);
        let change = round_to(row.change, 2);
        let pct_chg = round_to(row.pct_chg, 4);
        let vol = round_to(row.vol, 2);
        let amount = round_to(row.amount, 2);
        let turnover_rate = round_opt_to(row.turnover_rate, 4);

        let mut params: Vec<&dyn ToSql> =
            Vec::with_capacity(STOCK_DATA_INSERT_COLUMNS.len() + indicator_names.len());
        params.push(&row.ts_code);
        params.push(&row.trade_date);
        params.push(&adj_type);
        params.push(&open);
        params.push(&high);
        params.push(&low);
        params.push(&close);
        params.push(&pre_close);
        params.push(&change);
        params.push(&pct_chg);
        params.push(&vol);
        params.push(&amount);
        params.push(&turnover_rate);

        if let Some(indicators) = indicators {
            for name in &indicator_names {
                let series = indicators
                    .get(name)
                    .ok_or_else(|| format!("缺少指标{name}的数据"))?;
                params.push(&series[row_idx]);
            }
        }

        appender.append_row(params.as_slice()).map_err(|e| {
            format!(
                "Appender写入 {table_name} 失败, ts_code={}, trade_date={}: {e}",
                row.ts_code, row.trade_date
            )
        })?;
    }

    appender
        .flush()
        .map_err(|e| format!("刷新 {table_name} Appender失败: {e}"))?;
    Ok(())
}

pub fn init_stock_data_db(db_path: &str) -> Result<(), String> {
    // stock_market_data
    let source_path = Path::new(db_path);
    if let Some(source_parent) = source_path.parent() {
        if !source_parent.as_os_str().is_empty() {
            create_dir_all(source_parent).map_err(|e| format!("创建输出目录失败:{e}"))?;
        }
    }
    let conn = Connection::open(db_path).map_err(|e| format!("打开数据库失败:{e}"))?;

    conn.execute(
        r#"
            CREATE TABLE IF NOT EXISTS stock_data (
                ts_code VARCHAR,
                trade_date VARCHAR,
                adj_type VARCHAR,
                open DECIMAL(10,2),
                high DECIMAL(10,2),
                low DECIMAL(10,2),
                close DECIMAL(10,2),
                pre_close DECIMAL(10,2),
                change DECIMAL(10,2),
                pct_chg DECIMAL(10,4),
                vol DECIMAL(15,2),
                amount DECIMAL(20,2),
                tor DECIMAL(10,4),
                PRIMARY KEY (ts_code, trade_date, adj_type)
            )
            "#,
        [],
    )
    .map_err(|e| format!("创建stock_data失败:{e}"))?;

    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_stock_data_ts_adj_date ON stock_data(ts_code, adj_type, trade_date)",
        [],
    )
    .map_err(|e| format!("创建stock_data索引失败:{e}"))?;
    Ok(())
}

pub fn delete_one_stock_range(
    conn: &Connection,
    ts_code: &str,
    adj_type: AdjType,
    start_date: &str,
    end_date: &str,
) -> Result<(), String> {
    let adj_type = adj_type_name(adj_type);

    conn.execute(
        r#"
        DELETE FROM stock_data
        WHERE ts_code = ?
          AND adj_type = ?
          AND trade_date >= ?
          AND trade_date <= ?
        "#,
        params![ts_code, adj_type, start_date, end_date],
    )
    .map_err(|e| format!("删除旧行情失败: {e}"))?;

    Ok(())
}

pub fn insert_pro_bar_rows(
    conn: &Connection,
    adj_type: AdjType,
    rows: &[ProBarRow],
) -> Result<(), String> {
    append_rows_to_table(conn, STOCK_DATA_TABLE, adj_type, rows, None)
}

pub fn append_stage_pro_bar_rows(
    conn: &Connection,
    adj_type: AdjType,
    rows: &[ProBarRow],
    indicators: &HashMap<String, Vec<Option<f64>>>,
) -> Result<(), String> {
    append_rows_to_table(
        conn,
        STOCK_DATA_STAGE_TABLE,
        adj_type,
        rows,
        Some(indicators),
    )
}

pub fn reset_stock_data_stage_table(conn: &Connection) -> Result<(), String> {
    conn.execute_batch(&format!(
        "DROP TABLE IF EXISTS {STOCK_DATA_STAGE_TABLE};
         CREATE TEMP TABLE {STOCK_DATA_STAGE_TABLE} AS SELECT * FROM {STOCK_DATA_TABLE} LIMIT 0"
    ))
    .map_err(|e| format!("重建临时写库表失败: {e}"))
}

pub fn flush_stock_data_stage_table(conn: &Connection) -> Result<(), String> {
    conn.execute_batch(&format!(
        "INSERT INTO {STOCK_DATA_TABLE} SELECT * FROM {STOCK_DATA_STAGE_TABLE};
         DROP TABLE IF EXISTS {STOCK_DATA_STAGE_TABLE}"
    ))
    .map_err(|e| format!("提交临时写库表失败: {e}"))
}

pub fn checkpoint_stock_data(conn: &Connection) -> Result<(), String> {
    conn.execute_batch("CHECKPOINT")
        .map_err(|e| format!("执行数据库CHECKPOINT失败: {e}"))
}

pub fn replace_one_stock_rows(
    conn: &Connection,
    ts_code: &str,
    adj_type: AdjType,
    start_date: &str,
    end_date: &str,
    rows: &[ProBarRow],
) -> Result<(), String> {
    delete_one_stock_range(conn, ts_code, adj_type, start_date, end_date)?;
    insert_pro_bar_rows(conn, adj_type, rows)?;
    Ok(())
}

pub fn delete_trade_date_rows(
    conn: &Connection,
    adj_type: AdjType,
    trade_date: &str,
) -> Result<(), String> {
    let adj_type_name = adj_type_name(adj_type);

    conn.execute(
        r#"
        DELETE FROM stock_data
        WHERE adj_type = ?
          AND trade_date = ?
        "#,
        params![adj_type_name, trade_date],
    )
    .map_err(|e| format!("删除交易日旧行情失败, trade_date={trade_date}: {e}"))?;

    Ok(())
}

pub fn ensure_indicator_columns(
    conn: &Connection,
    indicator_names: &[String],
) -> Result<(), String> {
    if indicator_names.is_empty() {
        return Ok(());
    }

    let mut stmt = conn
        .prepare("DESCRIBE stock_data")
        .map_err(|e| format!("读取stock_data表结构失败:{e}"))?;
    let mut rows = stmt
        .query([])
        .map_err(|e| format!("查询stock_data表结构失败:{e}"))?;
    let mut existing = Vec::with_capacity(64);
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取stock_data列失败:{e}"))?
    {
        let name: String = row.get(0).map_err(|e| format!("读取列名失败:{e}"))?;
        existing.push(name);
    }

    for name in indicator_names {
        if existing.iter().any(|col| col.eq_ignore_ascii_case(name)) {
            continue;
        }

        let sql = format!(
            "ALTER TABLE stock_data ADD COLUMN {} DOUBLE",
            quote_ident(name)
        );
        conn.execute_batch(&sql)
            .map_err(|e| format!("新增指标列{name}失败:{e}"))?;
        existing.push(name.clone());
    }

    Ok(())
}

pub fn list_stock_data_indicator_columns(conn: &Connection) -> Result<Vec<String>, String> {
    let existing = load_table_columns(conn, STOCK_DATA_TABLE)?;
    let mut indicator_columns = Vec::new();

    for name in existing {
        if STOCK_DATA_INSERT_COLUMNS
            .iter()
            .any(|base| name.eq_ignore_ascii_case(base))
        {
            continue;
        }
        indicator_columns.push(name);
    }

    Ok(indicator_columns)
}

pub fn drop_stock_data_columns(conn: &Connection, column_names: &[String]) -> Result<(), String> {
    for name in column_names {
        let sql = format!("ALTER TABLE stock_data DROP COLUMN {}", quote_ident(name));
        conn.execute_batch(&sql)
            .map_err(|e| format!("删除指标列{name}失败:{e}"))?;
    }

    Ok(())
}

pub fn update_one_stock_indicator_rows(
    conn: &Connection,
    ts_code: &str,
    adj_type: AdjType,
    rows: &[ProBarRow],
    indicators: &HashMap<String, Vec<Option<f64>>>,
) -> Result<(), String> {
    if indicators.is_empty() || rows.is_empty() {
        return Ok(());
    }

    let adj_type = adj_type_name(adj_type);
    let mut indicator_names = indicators.keys().cloned().collect::<Vec<_>>();
    indicator_names.sort();

    for name in &indicator_names {
        let Some(series) = indicators.get(name) else {
            return Err(format!("缺少指标{name}的数据"));
        };
        if series.len() != rows.len() {
            return Err(format!(
                "指标{name}长度与行情行数不一致: {} != {}",
                series.len(),
                rows.len()
            ));
        }
    }

    let set_clause = indicator_names
        .iter()
        .map(|name| format!("{} = ?", quote_ident(name)))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "UPDATE stock_data SET {set_clause} WHERE ts_code = ? AND trade_date = ? AND adj_type = ?"
    );
    let mut stmt = conn
        .prepare(&sql)
        .map_err(|e| format!("预编译批量指标更新SQL失败:{e}"))?;

    for (row_idx, row) in rows.iter().enumerate() {
        let mut values: Vec<&dyn ToSql> = Vec::with_capacity(indicator_names.len() + 3);
        for name in &indicator_names {
            let series = indicators
                .get(name)
                .ok_or_else(|| format!("缺少指标{name}的数据"))?;
            values.push(&series[row_idx]);
        }
        values.push(&ts_code);
        values.push(&row.trade_date);
        values.push(&adj_type);

        stmt.execute(params_from_iter(values)).map_err(|e| {
            format!(
                "批量更新指标失败, ts_code={}, trade_date={}: {e}",
                ts_code, row.trade_date
            )
        })?;
    }

    Ok(())
}

pub fn write_stock_list_csv(source_dir: &str, rows: &[StockListRow]) -> Result<(), String> {
    let path = stock_list_path(source_dir);
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            create_dir_all(parent).map_err(|e| format!("创建stock_list目录失败:{e}"))?;
        }
    }

    let mut writer = csv::Writer::from_path(&path)
        .map_err(|e| format!("创建stock_list.csv失败:路径:{:?},错误:{e}", path))?;

    writer
        .write_record([
            "ts_code",
            "symbol",
            "name",
            "area",
            "industry",
            "list_date",
            "trade_date",
            "total_share",
            "float_share",
            "total_mv",
            "circ_mv",
            "fullname",
            "enname",
            "cnspell",
            "market",
            "exchange",
            "curr_type",
            "list_status",
            "delist_date",
            "is_hs",
            "act_name",
            "act_ent_type",
        ])
        .map_err(|e| format!("写入stock_list.csv表头失败:{e}"))?;

    for row in rows {
        let total_share = row.total_share.map(|v| v.to_string()).unwrap_or_default();
        let float_share = row.float_share.map(|v| v.to_string()).unwrap_or_default();
        let total_mv = row.total_mv.map(|v| v.to_string()).unwrap_or_default();
        let circ_mv = row.circ_mv.map(|v| v.to_string()).unwrap_or_default();

        writer
            .write_record([
                row.ts_code.as_str(),
                row.symbol.as_str(),
                row.name.as_str(),
                row.area.as_str(),
                row.industry.as_str(),
                row.list_date.as_str(),
                row.trade_date.as_str(),
                total_share.as_str(),
                float_share.as_str(),
                total_mv.as_str(),
                circ_mv.as_str(),
                row.fullname.as_str(),
                row.enname.as_str(),
                row.cnspell.as_str(),
                row.market.as_str(),
                row.exchange.as_str(),
                row.curr_type.as_str(),
                row.list_status.as_str(),
                row.delist_date.as_str(),
                row.is_hs.as_str(),
                row.act_name.as_str(),
                row.act_ent_type.as_str(),
            ])
            .map_err(|e| format!("写入stock_list.csv失败, ts_code={}: {e}", row.ts_code))?;
    }

    writer
        .flush()
        .map_err(|e| format!("刷新stock_list.csv失败:{e}"))?;

    Ok(())
}

pub fn write_trade_calendar_csv(source_dir: &str, rows: &[TradeCalRow]) -> Result<(), String> {
    let path = trade_calendar_path(source_dir);
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            create_dir_all(parent).map_err(|e| format!("创建trade_calendar目录失败:{e}"))?;
        }
    }

    let mut writer = csv::Writer::from_path(&path)
        .map_err(|e| format!("创建trade_calendar.csv失败:路径:{:?},错误:{e}", path))?;

    writer
        .write_record(["cal_date"])
        .map_err(|e| format!("写入trade_calendar.csv表头失败:{e}"))?;

    for row in rows {
        writer
            .write_record([row.cal_date.as_str()])
            .map_err(|e| format!("写入trade_calendar.csv失败, cal_date={}: {e}", row.cal_date))?;
    }

    writer
        .flush()
        .map_err(|e| format!("刷新trade_calendar.csv失败:{e}"))?;

    Ok(())
}

fn write_ths_concepts_csv_by_path(path: &Path, rows: &[ThsConceptRow]) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            create_dir_all(parent).map_err(|e| format!("创建概念目录失败:{e}"))?;
        }
    }

    let mut writer = csv::Writer::from_path(path)
        .map_err(|e| format!("创建概念CSV失败:路径:{:?},错误:{e}", path))?;

    writer
        .write_record(["ts_code", "name", "concept"])
        .map_err(|e| format!("写入概念CSV表头失败:{e}"))?;

    for row in rows {
        writer
            .write_record([
                row.ts_code.as_str(),
                row.name.as_str(),
                row.concept.as_str(),
            ])
            .map_err(|e| format!("写入概念CSV失败, ts_code={}: {e}", row.ts_code))?;
    }

    writer.flush().map_err(|e| format!("刷新概念CSV失败:{e}"))?;

    Ok(())
}

pub fn write_ths_concepts_csv(source_dir: &str, rows: &[ThsConceptRow]) -> Result<(), String> {
    let path = ths_concepts_path(source_dir);
    write_ths_concepts_csv_by_path(&path, rows)
}

pub struct LatestCloseRow {
    pub ts_code: String,
    pub trade_date: String,
    pub close: f64,
}

pub fn load_latest_close_map_before(
    source_dir: &str,
    adj_type: &str,
    trade_date: &str,
) -> Result<HashMap<String, LatestCloseRow>, String> {
    let db_path = source_db_path(source_dir);
    let db_path_str = db_path
        .to_str()
        .ok_or_else(|| "source_db路径不是有效UTF-8".to_string())?;
    let conn = Connection::open(db_path_str).map_err(|e| format!("数据库连接错误:{e}"))?;

    let sql = r#"
        SELECT
            d.ts_code,
            d.trade_date,
            CAST(d.close AS DOUBLE)
        FROM stock_data AS d
        INNER JOIN (
            SELECT
                ts_code,
                MAX(trade_date) AS max_trade_date
            FROM stock_data
            WHERE adj_type = ?
              AND trade_date < ?
            GROUP BY ts_code
        ) AS latest
            ON d.ts_code = latest.ts_code
           AND d.trade_date = latest.max_trade_date
        WHERE d.adj_type = ?
    "#;

    let mut stmt = conn
        .prepare(sql)
        .map_err(|e| format!("sql预编译失败:{e}"))?;
    let mut rows = stmt
        .query(params![adj_type, trade_date, adj_type])
        .map_err(|e| format!("数据库查询失败:{e}"))?;

    let mut out = HashMap::new();

    while let Some(row) = rows.next().map_err(|e| format!("读取数据行失败:{e}"))? {
        let ts_code: String = row.get(0).map_err(|e| format!("读取ts_code失败:{e}"))?;
        let trade_date: String = row.get(1).map_err(|e| format!("读取trade_date失败:{e}"))?;
        let close: f64 = row.get(2).map_err(|e| format!("读取close失败:{e}"))?;

        if out.contains_key(&ts_code) {
            return Err(format!("latest close 结果出现重复 ts_code: {ts_code}"));
        }

        out.insert(
            ts_code.clone(),
            LatestCloseRow {
                ts_code,
                trade_date,
                close,
            },
        );
    }

    Ok(out)
}

pub fn load_latest_trade_date(
    source_dir: &str,
    adj_type: AdjType,
) -> Result<Option<String>, String> {
    let db_path = source_db_path(source_dir);
    let db_path_str = db_path
        .to_str()
        .ok_or_else(|| "source_db路径不是有效UTF-8".to_string())?;
    let conn = Connection::open(db_path_str).map_err(|e| format!("数据库连接错误:{e}"))?;

    let table_exists = conn
        .query_row(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'stock_data'",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map_err(|e| format!("检查stock_data表结构失败:{e}"))?;
    if table_exists <= 0 {
        return Ok(None);
    }

    let adj_type = adj_type_name(adj_type);

    let mut stmt = conn
        .prepare("SELECT MAX(trade_date) FROM stock_data WHERE adj_type = ?")
        .map_err(|e| format!("sql预编译失败:{e}"))?;
    let mut rows = stmt
        .query(params![adj_type])
        .map_err(|e| format!("数据库查询失败:{e}"))?;

    let Some(row) = rows.next().map_err(|e| format!("读取数据行失败:{e}"))? else {
        return Ok(None);
    };

    let latest_trade_date: Option<String> = row
        .get(0)
        .map_err(|e| format!("读取latest_trade_date失败:{e}"))?;
    Ok(latest_trade_date)
}

pub fn replace_trade_date_rows(
    conn: &Connection,
    adj_type: AdjType,
    trade_date: &str,
    rows: &[ProBarRow],
) -> Result<(), String> {
    delete_trade_date_rows(conn, adj_type, trade_date)?;
    insert_pro_bar_rows(conn, adj_type, rows)?;
    Ok(())
}
