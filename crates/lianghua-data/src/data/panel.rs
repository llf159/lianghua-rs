use super::{
    DataReader, IndData, IndicatorScope, IndsData, RowData, ind_toml_path, load_trade_date_list,
    stock_list_path,
};
use crate::expr::{
    eval::Value,
    panel::{PanelRuntime, PanelSource, PanelValue, is_cross_section_function},
    parser::{Expr, Stmt, Stmts},
    validation::{
        estimate_expression_warmup, expression_input_identifiers, parse_expression_program,
        validate_expression_functions,
    },
};
use duckdb::{
    Connection,
    arrow::{
        array::{Array, ArrayRef, Float64Array, StringArray, builder::StringBuilder},
        record_batch::RecordBatch,
    },
    params,
};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
};

pub fn definitions(source: &str) -> Result<Vec<IndData>, String> {
    let path = ind_toml_path(source);
    let text = if path.exists() {
        std::fs::read_to_string(path).map_err(|e| e.to_string())?
    } else {
        String::new()
    };
    if text.trim().is_empty() {
        return Ok(Vec::new());
    }
    let defs = IndsData::parse_from_text(&text)?;
    validate_definitions(&defs)?;
    Ok(defs)
}

pub fn validate_definitions(defs: &[IndData]) -> Result<(), String> {
    let names = defs
        .iter()
        .map(|d| (d.name.clone(), d.scope))
        .collect::<HashMap<_, _>>();
    let mut available = HashSet::new();
    for def in defs {
        let program = parse_expression_program(&def.expr)
            .map_err(|e| format!("指标{}解析失败:{}", def.name, e.msg))?;
        validate_expression_functions(&program)?;
        if expression_input_identifiers(&program)
            .iter()
            .any(|key| key.starts_with("__CS_INTERNAL_"))
            || program.item.iter().any(
                |stmt| matches!(stmt,Stmt::Assign{name,..} if name.starts_with("__CS_INTERNAL_")),
            )
        {
            return Err("表达式不能使用内部前缀__CS_INTERNAL_".into());
        }
        if def.scope != IndicatorScope::Stock
            && program.item.iter().any(|stmt| match stmt {
                Stmt::Expr(e) | Stmt::Assign { value: e, .. } => contains_last(e),
            })
        {
            return Err(format!(
                "指标{}不能使用LAST：它将区间末日广播到历史，请使用REF",
                def.name
            ));
        }
        if def.scope == IndicatorScope::Stock
            && program.item.iter().any(|s| match s {
                Stmt::Expr(e) | Stmt::Assign { value: e, .. } => uses_context(e, false),
            })
        {
            return Err(format!(
                "指标{}使用CS_*函数，请选择市场或横截面范围",
                def.name
            ));
        }
        for key in expression_input_identifiers(&program) {
            if let Some(scope) = names.get(&key) {
                if !available.contains(&key) {
                    return Err(format!(
                        "指标{}引用尚未定义的{key}，请调整配置顺序",
                        def.name
                    ));
                }
                if def.scope == IndicatorScope::Stock && *scope != IndicatorScope::Stock {
                    return Err(format!(
                        "指标{}依赖跨股票指标{key}，请将范围改为横截面",
                        def.name
                    ));
                }
            }
        }
        available.insert(def.name.clone());
    }
    Ok(())
}

fn contains_last(expr: &Expr) -> bool {
    match expr {
        Expr::Call { name, args } => {
            name.eq_ignore_ascii_case("LAST") || args.iter().any(contains_last)
        }
        Expr::Unary { rhs, .. } => contains_last(rhs),
        Expr::Binary { lhs, rhs, .. } => contains_last(lhs) || contains_last(rhs),
        _ => false,
    }
}

// Stateful temporal functions require the original prefix, not a guessed warmup.
fn uses_context(expr: &Expr, recursive: bool) -> bool {
    match expr {
        Expr::Call { name, args } => {
            (if recursive {
                matches!(
                    name.to_ascii_uppercase().as_str(),
                    "EMA" | "SMA" | "BARSLAST" | "LAST"
                )
            } else {
                is_cross_section_function(name)
            }) || args.iter().any(|e| uses_context(e, recursive))
        }
        Expr::Unary { rhs, .. } => uses_context(rhs, recursive),
        Expr::Binary { lhs, rhs, .. } => {
            uses_context(lhs, recursive) || uses_context(rhs, recursive)
        }
        _ => false,
    }
}

pub fn config_signature(definitions: &[IndData]) -> Result<String, String> {
    // Invalidate results produced before index invalidation and assignment
    // warmup were fixed, even when the user's formulas have not changed.
    serde_json::to_string(&("panel-v2", definitions)).map_err(|e| e.to_string())
}

pub fn configured_fields(source: &str) -> Result<Vec<(String, IndicatorScope)>, String> {
    Ok(definitions(source)?
        .into_iter()
        .filter(|d| d.scope != IndicatorScope::Stock)
        .map(|d| (d.name, d.scope))
        .collect())
}

pub fn has_values(conn: &Connection) -> Result<bool, String> {
    conn.query_row("SELECT count(*)>0 FROM information_schema.tables WHERE table_name='indicator_panel_values'",[],|r|r.get(0)).map_err(|e|e.to_string())
}

pub fn invalidate(conn: &Connection, start: &str) -> Result<(), String> {
    if has_values(conn)? {
        conn.execute("UPDATE indicator_panel_state SET dirty_from=least(coalesce(dirty_from,?),?) WHERE id=1",params![start,start]).map_err(|e|e.to_string())?;
    }
    Ok(())
}

pub type PanelValues = HashMap<String, HashMap<String, HashMap<String, Option<f64>>>>;

pub fn load_values(
    conn: &Connection,
    source: &str,
    fields: &[String],
    stocks: &[String],
    start: &str,
    end: &str,
) -> Result<PanelValues, String> {
    if fields.is_empty() {
        return Ok(HashMap::new());
    }
    if !has_values(conn)? {
        return Ok(HashMap::new());
    }
    let signature = config_signature(&definitions(source)?)?;
    let names = fields
        .iter()
        .map(|s| format!("'{}'", s.replace('\'', "''")))
        .collect::<Vec<_>>()
        .join(",");
    let codes = std::iter::once(String::new())
        .chain(stocks.iter().cloned())
        .map(|s| format!("'{}'", s.replace('\'', "''")))
        .collect::<Vec<_>>()
        .join(",");
    let mut stmt=conn.prepare(&format!("SELECT v.name,v.ts_code,v.trade_date,v.value FROM indicator_panel_values v JOIN indicator_panel_state s ON s.id=1 AND s.version=1 AND s.signature=? WHERE v.name IN({names}) AND v.ts_code IN({codes}) AND v.trade_date BETWEEN ? AND ? AND (s.dirty_from IS NULL OR v.trade_date<s.dirty_from)")).map_err(|e|e.to_string())?;
    let batches = stmt
        .query_arrow(params![signature, start, end])
        .map_err(|e| e.to_string())?;
    let mut out = PanelValues::new();
    for batch in batches {
        let strings = (0..3)
            .map(|i| {
                batch
                    .column(i)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or("指标结果标识列不是字符串")
            })
            .collect::<Result<Vec<_>, _>>()?;
        let values = batch
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or("指标结果数值列不是DOUBLE")?;
        for row in 0..batch.num_rows() {
            if strings.iter().any(|column| column.is_null(row)) {
                return Err("指标结果的名称、股票代码或日期为空".into());
            }
            let name = strings[0].value(row);
            let stock = strings[1].value(row);
            // Borrow repeated identifiers from Arrow; allocate map keys only
            // on their first occurrence, rather than twice for every value.
            if !out.contains_key(name) {
                out.insert(name.to_owned(), HashMap::new());
            }
            let stocks = out.get_mut(name).expect("inserted field");
            if !stocks.contains_key(stock) {
                stocks.insert(stock.to_owned(), HashMap::new());
            }
            stocks.get_mut(stock).expect("inserted stock").insert(
                strings[2].value(row).to_owned(),
                (!values.is_null(row))
                    .then(|| values.value(row))
                    .filter(|v| v.is_finite()),
            );
        }
    }
    Ok(out)
}

pub fn inject_values(row: &mut RowData, stock: &str, fields: &[String], values: &PanelValues) {
    for field in fields {
        let dates = values
            .get(field)
            .and_then(|stocks| stocks.get(stock).or_else(|| stocks.get("")));
        row.cols.insert(
            field.clone(),
            row.trade_dates
                .iter()
                .map(|date| dates.and_then(|map| map.get(date)).copied().flatten())
                .collect(),
        );
    }
}

pub fn inject_fields(
    conn: &Connection,
    source: &str,
    stock: &str,
    row: &mut RowData,
    requested: &HashSet<String>,
) -> Result<(), String> {
    let fields = configured_fields(source)?
        .into_iter()
        .filter(|(key, _)| requested.contains(key))
        .map(|(key, _)| key)
        .collect::<Vec<_>>();
    if fields.is_empty() {
        return Ok(());
    }
    if let (Some(start), Some(end)) = (row.trade_dates.first(), row.trade_dates.last()) {
        let values = load_values(conn, source, &fields, &[stock.to_string()], start, end)?;
        inject_values(row, stock, &fields, &values);
    }
    Ok(())
}

struct Source {
    reader: DataReader,
    input_columns: Vec<(String, String)>,
    index_columns: Vec<super::RuntimeIndexCol>,
    dates: Vec<String>,
    day_lookup: HashMap<String, usize>,
    sessions: Vec<usize>,
    stocks: Vec<String>,
    presence: Vec<Vec<bool>>,
    listed: BTreeMap<String, usize>,
}

impl PanelSource for Source {
    fn date_count(&self) -> usize {
        self.dates.len()
    }
    fn stock_count(&self) -> usize {
        self.stocks.len()
    }
    fn present(&self, stock: usize, day: usize) -> bool {
        self.presence[stock][day]
    }
    fn eval_stock(
        &mut self,
        expr: &Expr,
        vars: &HashMap<String, PanelValue>,
    ) -> Result<PanelValue, String> {
        let program = Stmts {
            item: vec![Stmt::Expr(expr.clone())],
        };
        let identifiers = expression_input_identifiers(&program);
        let required = identifiers
            .iter()
            .filter(|k| !vars.contains_key(*k) && !matches!(k.as_str(), "SESSION" | "LISTED_DAYS"))
            .cloned()
            .collect::<HashSet<_>>();
        self.reader.cols_table = self
            .input_columns
            .iter()
            .filter(|(_, key)| required.contains(key) || matches!(key.as_str(), "AMOUNT" | "TOR"))
            .cloned()
            .collect();
        self.reader.runtime_index_cols = self
            .index_columns
            .iter()
            .filter(|column| required.contains(&column.runtime_key))
            .cloned()
            .collect();
        let all_days = (0..self.dates.len()).collect::<Vec<_>>();
        let mut out = vec![vec![f64::NAN; self.dates.len()]; self.stocks.len()];
        for (batch_index, codes) in self.stocks.chunks(64).enumerate() {
            let mut batch = if !required.is_empty() {
                self.reader.load_batch(
                    codes,
                    "qfq",
                    &self.dates[0],
                    self.dates.last().expect("dates"),
                )?
            } else {
                HashMap::new()
            };
            for (offset, code) in codes.iter().enumerate() {
                let stock = batch_index * 64 + offset;
                let mut rt = crate::expr::eval::Runtime::default();
                if let Some(mut row) = batch.remove(code) {
                    let days = row
                        .trade_dates
                        .iter()
                        .map(|date| self.day_lookup[date])
                        .collect::<Vec<_>>();
                    if let Expr::Ident(name) = expr
                        && let Some(column) = row.cols.remove(name)
                    {
                        for (&day, value) in days.iter().zip(column) {
                            out[stock][day] = value.filter(|v| v.is_finite()).unwrap_or(f64::NAN);
                        }
                        continue;
                    }
                    for (name, column) in row.cols {
                        let mut full = vec![None; self.dates.len()];
                        for (&day, value) in days.iter().zip(column) {
                            full[day] = value;
                        }
                        rt.vars.insert(name, Value::NumSeries(full));
                    }
                }
                if identifiers.contains("SESSION") {
                    rt.vars.insert(
                        "SESSION".into(),
                        Value::NumSeries(
                            self.sessions.iter().map(|&day| Some(day as f64)).collect(),
                        ),
                    );
                }
                if identifiers.contains("LISTED_DAYS") {
                    let listed = self.listed.get(code).copied().unwrap_or(0);
                    rt.vars.insert(
                        "LISTED_DAYS".into(),
                        Value::NumSeries(
                            self.sessions
                                .iter()
                                .map(|&day| Some(day as f64 - listed as f64 + 1.))
                                .collect(),
                        ),
                    );
                }
                for key in &identifiers {
                    if let Some(value) = vars.get(key) {
                        rt.vars
                            .insert(key.clone(), value.runtime_value(stock, &all_days));
                    }
                }
                let values = rt
                    .eval_program(&program)
                    .map_err(|e| e.msg)?
                    .into_num_series(self.dates.len())
                    .map_err(|e| e.msg)?;
                for (day, value) in values.into_iter().enumerate() {
                    if self.presence[stock][day] {
                        out[stock][day] = value.filter(|v| v.is_finite()).unwrap_or(f64::NAN);
                    }
                }
            }
        }
        Ok(PanelValue::Stocks(Arc::new(out)))
    }
}

pub fn refresh(conn: &Connection, source: &str, rebuild: bool) -> Result<usize, String> {
    refresh_with_progress(conn, source, rebuild, None)
}

pub fn refresh_with_progress(
    conn: &Connection,
    source: &str,
    rebuild: bool,
    progress: Option<&dyn Fn(usize, usize, &str)>,
) -> Result<usize, String> {
    let defs = definitions(source)?;
    let panel_defs = defs
        .iter()
        .filter(|d| d.scope != IndicatorScope::Stock)
        .collect::<Vec<_>>();
    if panel_defs.is_empty() && !has_values(conn)? {
        return Ok(0);
    }
    conn.execute_batch("CREATE TABLE IF NOT EXISTS indicator_panel_values(name VARCHAR,ts_code VARCHAR,trade_date VARCHAR,value DOUBLE,PRIMARY KEY(name,ts_code,trade_date)); CREATE TABLE IF NOT EXISTS indicator_panel_sessions(trade_date VARCHAR PRIMARY KEY); CREATE TABLE IF NOT EXISTS indicator_panel_state(id INTEGER PRIMARY KEY,version INTEGER,signature VARCHAR,dirty_from VARCHAR,listing_json VARCHAR,calendar_json VARCHAR); INSERT INTO indicator_panel_state VALUES(1,1,'',NULL,'{}','[]') ON CONFLICT DO NOTHING;").map_err(|e|e.to_string())?;
    let signature = config_signature(&defs)?;
    if panel_defs.is_empty() {
        conn.execute_batch("BEGIN;DELETE FROM indicator_panel_values;DELETE FROM indicator_panel_sessions;DROP TABLE IF EXISTS market_environment;DROP TABLE IF EXISTS market_environment_state;COMMIT;").map_err(|e|e.to_string())?;
        return Ok(0);
    }
    let (first, last): (Option<String>, Option<String>) = conn
        .query_row(
            "SELECT min(trade_date),max(trade_date) FROM stock_data WHERE adj_type='qfq'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .map_err(|e| e.to_string())?;
    let (Some(first), Some(last)) = (first, last) else {
        return Ok(0);
    };
    let mut calendar = load_trade_date_list(source)?;
    calendar.sort();
    calendar.dedup();
    if !calendar.contains(&first) || !calendar.contains(&last) {
        return Err("交易日历未覆盖行情起止日期".into());
    }
    let mut listing = BTreeMap::new();
    let mut csv = csv::Reader::from_path(stock_list_path(source)).map_err(|e| e.to_string())?;
    let head = csv.headers().map_err(|e| e.to_string())?.clone();
    let code = head
        .iter()
        .position(|s| s == "ts_code")
        .ok_or("股票列表缺少ts_code")?;
    let date = head
        .iter()
        .position(|s| s == "list_date")
        .ok_or("股票列表缺少list_date")?;
    for row in csv.records() {
        let row = row.map_err(|e| e.to_string())?;
        listing.insert(row[code].to_string(), row[date].to_string());
    }
    let (version,old_signature,dirty,old_listing,old_calendar):(i64,String,Option<String>,String,String)=conn.query_row("SELECT version,signature,dirty_from,listing_json,calendar_json FROM indicator_panel_state WHERE id=1",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?))).map_err(|e|e.to_string())?;
    let mut stmt = conn
        .prepare("SELECT trade_date FROM indicator_panel_sessions")
        .map_err(|e| e.to_string())?;
    let saved = stmt
        .query_map([], |r| r.get::<_, String>(0))
        .map_err(|e| e.to_string())?
        .collect::<Result<HashSet<_>, _>>()
        .map_err(|e| e.to_string())?;
    let saved_last = saved.iter().max();
    let old_calendar: Vec<String> =
        serde_json::from_str(&old_calendar).map_err(|e| e.to_string())?;
    let old_listing: BTreeMap<String, String> =
        serde_json::from_str(&old_listing).map_err(|e| e.to_string())?;
    let metadata_changed = old_listing.iter().any(|(k, v)| listing.get(k) != Some(v))
        || listing.iter().any(|(k, v)| {
            !old_listing.contains_key(k) && saved_last.is_some_and(|end| v.is_empty() || v <= end)
        })
        || saved_last.is_some_and(|end| {
            calendar
                .iter()
                .filter(|d| *d <= end)
                .cloned()
                .collect::<Vec<_>>()
                != old_calendar
        });
    let missing = calendar
        .iter()
        .find(|d| *d >= &first && *d <= &last && !saved.contains(*d))
        .cloned();
    let start = if rebuild || version != 1 || old_signature != signature || metadata_changed {
        Some(first.clone())
    } else {
        match (dirty, missing) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    };
    let Some(start) = start else {
        return Ok(0);
    };
    let end_index = calendar.partition_point(|d| d <= &last);
    let output_index = calendar.partition_point(|d| d < &start).min(end_index);
    // Resolve all external inputs before any large history calculation.
    let panel_names = panel_defs
        .iter()
        .map(|def| def.name.as_str())
        .collect::<HashSet<_>>();
    let programs = panel_defs
        .iter()
        .map(|def| parse_expression_program(&def.expr).map_err(|e| e.msg))
        .collect::<Result<Vec<_>, _>>()?;
    let mut raw_inputs = HashSet::new();
    let mut last_use = HashMap::new();
    let mut needs = HashMap::new();
    let mut warmup = 0usize;
    let mut full = false;
    for (index, (def, program)) in panel_defs.iter().zip(&programs).enumerate() {
        let inputs = expression_input_identifiers(program);
        let inherited = inputs
            .iter()
            .filter_map(|key| needs.get(key))
            .copied()
            .max()
            .unwrap_or(0usize);
        let need = inherited
            .checked_add(estimate_expression_warmup(program)?)
            .ok_or("指标预热长度溢出")?;
        needs.insert(def.name.clone(), need);
        warmup = warmup.max(need);
        full |= program.item.iter().any(|stmt| match stmt {
            Stmt::Expr(e) | Stmt::Assign { value: e, .. } => uses_context(e, true),
        });
        for key in &inputs {
            if panel_names.contains(key.as_str()) {
                last_use.insert(key.clone(), index);
            }
        }
        raw_inputs.extend(inputs.into_iter().filter(|key| {
            !panel_names.contains(key.as_str())
                && !matches!(key.as_str(), "SESSION" | "LISTED_DAYS")
        }));
    }
    let reader = DataReader::new_inner(
        source,
        Some(&raw_inputs),
        Some(conn.try_clone().map_err(|e| e.to_string())?),
    )?;
    let warm_index = if full {
        calendar.partition_point(|d| d < &first)
    } else {
        output_index.saturating_sub(warmup.max(1))
    };
    let dates = calendar[warm_index..end_index].to_vec();
    if dates.is_empty() {
        return Ok(0);
    }
    if let Some(cb) = progress {
        cb(
            0,
            panel_defs.len() + 1,
            &format!(
                "输出{}~{}；读取{}~{}（含预热）",
                start, last, dates[0], last
            ),
        );
    }
    // Batched stock predicates use sequential scans in DuckDB. Materialize a
    // suffix once, so each subexpression/batch scans only its warmup range.
    // Keep full replays streaming from the original table to avoid copying
    // the complete history into a second table.
    if warm_index > calendar.partition_point(|d| d < &first) && !raw_inputs.is_empty() {
        let mut columns = vec![
            "ts_code".to_string(),
            "trade_date".to_string(),
            "adj_type".to_string(),
        ];
        columns.extend(reader.cols_table.iter().map(|(column, _)| column.clone()));
        columns.extend(
            reader
                .runtime_index_cols
                .iter()
                .map(|column| column.db_column.clone()),
        );
        columns.sort();
        columns.dedup();
        let columns = columns
            .iter()
            .map(|column| format!("\"{}\"", column.replace('"', "\"\"")))
            .collect::<Vec<_>>()
            .join(",");
        // The shadow table belongs only to this reader's session. Writers and
        // the original connection continue to see the persistent stock_data.
        reader.conn.execute(
            &format!("CREATE TEMP TABLE stock_data AS SELECT {columns} FROM stock_data WHERE adj_type IN ('qfq','ind') AND trade_date BETWEEN ? AND ?"),
            params![dates[0], last],
        ).map_err(|e| format!("准备跨股票指标增量行情失败: {e}"))?;
    }
    let day_lookup = dates
        .iter()
        .enumerate()
        .map(|(i, d)| (d.clone(), i))
        .collect::<HashMap<_, _>>();
    let mut stmt=conn.prepare("SELECT DISTINCT ts_code FROM stock_data WHERE adj_type='qfq' AND trade_date BETWEEN ? AND ? ORDER BY ts_code").map_err(|e|e.to_string())?;
    let stocks = stmt
        .query_map(params![dates[0], last], |r| r.get::<_, String>(0))
        .map_err(|e| e.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())?;
    let stock_lookup = stocks
        .iter()
        .enumerate()
        .map(|(i, s)| (s.clone(), i))
        .collect::<HashMap<_, _>>();
    let mut presence = vec![vec![false; dates.len()]; stocks.len()];
    let mut stmt=conn.prepare("SELECT ts_code,trade_date FROM stock_data WHERE adj_type='qfq' AND trade_date BETWEEN ? AND ?").map_err(|e|e.to_string())?;
    let batches = stmt
        .query_arrow(params![dates[0], last])
        .map_err(|e| e.to_string())?;
    for batch in batches {
        let codes = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("行情股票代码不是字符串")?;
        let dates = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("行情交易日不是字符串")?;
        for row in 0..batch.num_rows() {
            if codes.is_null(row) || dates.is_null(row) {
                return Err("行情的股票代码或交易日为空".into());
            }
            if let Some(&day) = day_lookup.get(dates.value(row)) {
                presence[stock_lookup[codes.value(row)]][day] = true;
            }
        }
    }
    drop(stmt);
    let source_data = Source {
        input_columns: reader.cols_table.clone(),
        index_columns: reader.runtime_index_cols.clone(),
        reader,
        dates,
        day_lookup,
        sessions: (warm_index..end_index).collect(),
        stocks,
        presence,
        listed: listing
            .iter()
            .map(|(k, v)| {
                (
                    k.clone(),
                    if v.is_empty() {
                        0
                    } else {
                        calendar.partition_point(|d| d < v)
                    },
                )
            })
            .collect(),
    };
    let mut rt = PanelRuntime::new(source_data);
    // Hide the output range before the first short write. Partial writes remain
    // invisible until the final metadata publication; retries replace the range.
    conn.execute_batch("BEGIN").map_err(|e| e.to_string())?;
    let prepare = (|| -> Result<(), String> {
        conn.execute("UPDATE indicator_panel_state SET dirty_from=least(coalesce(dirty_from,?),?) WHERE id=1",params![start,start]).map_err(|e|e.to_string())?;
        conn.execute(
            "DELETE FROM indicator_panel_values WHERE trade_date>=?",
            [&start],
        )
        .map_err(|e| e.to_string())?;
        conn.execute(
            "DELETE FROM indicator_panel_sessions WHERE trade_date>=?",
            [&start],
        )
        .map_err(|e| e.to_string())?;
        if old_signature != signature {
            conn.execute("DELETE FROM indicator_panel_values", [])
                .map_err(|e| e.to_string())?;
        }
        conn.execute_batch("COMMIT").map_err(|e| e.to_string())?;
        Ok(())
    })();
    if prepare.is_err() {
        let _ = conn.execute_batch("ROLLBACK");
    }
    prepare?;
    let bulk = crate::data::bulk_write::BulkWriteSettings::apply(conn)?;
    let write_result = (|| -> Result<usize, String> {
        let count = panel_defs.len();
        let output_day = output_index.saturating_sub(warm_index);
        for (index, (def, program)) in panel_defs.iter().zip(&programs).enumerate() {
            if let Some(cb) = progress {
                cb(index, count + 1, &def.name);
            }
            let published = rt.vars.clone();
            let mut value = rt
                .eval_program(program)
                .map_err(|e| format!("指标{}计算失败:{e}", def.name))?;
            // Discard local assignments before rounding, so a unique output can be
            // rounded in place instead of cloning its entire stock/date matrix.
            rt.vars = published;
            rt.vars
                .retain(|key, _| last_use.get(key).is_some_and(|&last| last > index));
            if def.scope == IndicatorScope::Market && matches!(value, PanelValue::Stocks(_)) {
                return Err(format!(
                    "市场指标{}仍包含逐股结果，请使用CS_MEAN等聚合，或改为横截面范围",
                    def.name
                ));
            }
            let scale = 10_f64.powi(def.prec as i32);
            let round = |v: &mut f64| {
                *v = if v.is_finite() {
                    (*v * scale).round() / scale
                } else {
                    f64::NAN
                };
            };
            match &mut value {
                PanelValue::Scalar(v) => round(v),
                PanelValue::Market(v) => Arc::make_mut(v).iter_mut().for_each(round),
                PanelValue::Stocks(v) => {
                    for row in Arc::make_mut(v) {
                        row.iter_mut().for_each(round);
                    }
                }
            }
            if let Some(cb) = progress {
                cb(index, count + 1, &format!("保存{}", def.name));
            }
            let stock_count = if def.scope == IndicatorScope::Market {
                1
            } else {
                rt.source.stocks.len()
            };
            for chunk_start in (0..stock_count).step_by(128) {
                let chunk_end = (chunk_start + 128).min(stock_count);
                let capacity = (chunk_end - chunk_start) * (rt.source.dates.len() - output_day);
                let mut names = StringBuilder::with_capacity(capacity, capacity * def.name.len());
                let mut codes = StringBuilder::with_capacity(capacity, capacity * 9);
                let mut dates = StringBuilder::with_capacity(capacity, capacity * 8);
                let mut values = Vec::with_capacity(capacity);
                for stock in chunk_start..chunk_end {
                    for day in output_day..rt.source.dates.len() {
                        if def.scope == IndicatorScope::Market || rt.source.presence[stock][day] {
                            names.append_value(&def.name);
                            codes.append_value(if def.scope == IndicatorScope::Market {
                                ""
                            } else {
                                &rt.source.stocks[stock]
                            });
                            dates.append_value(&rt.source.dates[day]);
                            values.push(value.at(stock, day));
                        }
                    }
                }
                let batch = RecordBatch::try_from_iter([
                    ("name", Arc::new(names.finish()) as ArrayRef),
                    ("ts_code", Arc::new(codes.finish()) as ArrayRef),
                    ("trade_date", Arc::new(dates.finish()) as ArrayRef),
                    ("value", Arc::new(Float64Array::from(values)) as ArrayRef),
                ])
                .map_err(|e| e.to_string())?;
                conn.execute_batch("BEGIN").map_err(|e| e.to_string())?;
                let write = (|| -> Result<(), String> {
                    let mut app = conn
                        .appender("indicator_panel_values")
                        .map_err(|e| e.to_string())?;
                    app.append_record_batch(batch).map_err(|e| e.to_string())?;
                    app.flush().map_err(|e| e.to_string())?;
                    drop(app);
                    conn.execute_batch("COMMIT").map_err(|e| e.to_string())
                })();
                if write.is_err() {
                    let _ = conn.execute_batch("ROLLBACK");
                }
                write?;
            }
            // Saved outputs need memory only while a later indicator depends on
            // them. Partial rows remain hidden by dirty_from until publication.
            if last_use.get(&def.name).is_some_and(|&last| last > index) {
                rt.vars.insert(def.name.clone(), value);
            }
            if let Some(cb) = progress {
                cb(index + 1, count + 1, &def.name);
            }
        }
        conn.execute_batch("BEGIN").map_err(|e| e.to_string())?;
        let publish = (|| -> Result<(), String> {
            let mut app = conn
                .appender("indicator_panel_sessions")
                .map_err(|e| e.to_string())?;
            for date in &calendar[output_index..end_index] {
                app.append_row(params![date]).map_err(|e| e.to_string())?;
            }
            app.flush().map_err(|e| e.to_string())?;
            drop(app);
            conn.execute("UPDATE indicator_panel_state SET version=1,signature=?,dirty_from=NULL,listing_json=?,calendar_json=? WHERE id=1",params![signature,serde_json::to_string(&listing).map_err(|e|e.to_string())?,serde_json::to_string(&calendar[..end_index]).map_err(|e|e.to_string())?]).map_err(|e|e.to_string())?;
            conn.execute_batch("DROP TABLE IF EXISTS market_environment;DROP TABLE IF EXISTS market_environment_state;COMMIT;").map_err(|e|e.to_string())?;
            Ok(())
        })();
        if publish.is_err() {
            let _ = conn.execute_batch("ROLLBACK");
        }
        publish?;
        Ok(end_index - output_index)
    })();
    let rows = bulk.checkpoint_and_finish(conn, write_result)?;
    if let Some(cb) = progress {
        cb(panel_defs.len() + 1, panel_defs.len() + 1, "完成");
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::{
        download_data::{delete_one_stock_range, init_stock_data_db},
        runtime::row_into_rt,
    };
    use lianghua_model::AdjType;

    #[test]
    fn configured_panel_functions_persistence_and_repair() {
        let path = std::env::temp_dir().join(format!(
            "panel-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&path).unwrap();
        let source = path.to_str().unwrap();
        let dates = (0..100)
            .map(|day| {
                (chrono::NaiveDate::from_ymd_opt(2025, 1, 1).unwrap() + chrono::Duration::days(day))
                    .format("%Y%m%d")
                    .to_string()
            })
            .collect::<Vec<_>>();
        std::fs::write(
            path.join("trade_calendar.csv"),
            format!("cal_date\n{}\n", dates.join("\n")),
        )
        .unwrap();
        std::fs::write(
            path.join("stock_list.csv"),
            "ts_code,list_date\nA,20250101\nB,20250101\nC,20250101\nD,20250101\n",
        )
        .unwrap();
        let config = r#"version=1
[[ind]]
name="MY_MEAN"
scope="market"
expr="CS_MEAN(PCT_CHG,LISTED_DAYS>=60)"
prec=12
[[ind]]
name="MY_RANK"
scope="cross_section"
expr="CS_RANK(PCT_CHG)"
prec=12
[[ind]]
name="MY_GROUP"
scope="cross_section"
expr="CS_MEAN(PCT_CHG,1,IF(TOTAL_MV_YI<0.25,1,2))"
prec=12
[[ind]]
name="MY_STRONG"
scope="market"
expr="MA(CS_MEAN(PCT_CHG,MY_RANK>0.8),3)"
prec=12
[[ind]]
name="MY_NESTED"
scope="market"
expr="P:=PCT_CHG; CS_MEAN(P,CS_RANK(P)>0.8)"
prec=12
[[ind]]
name="MY_EMA"
scope="market"
expr="EMA(MY_MEAN,3)"
prec=12
[[ind]]
name="MY_INDEX"
scope="market"
expr="CS_MEAN(PCT_CHG+I_CLOSE)"
prec=12
[[ind]]
name="MY_ZERO"
scope="market"
expr="cs_count(1,PCT_CHG>10)"
prec=12
[[ind]]
name="MY_STATS"
scope="market"
expr="CS_SUM(PCT_CHG)+CS_MIN(PCT_CHG)+CS_MAX(PCT_CHG)+CS_MEDIAN(PCT_CHG)+CS_VAR(PCT_CHG)+CS_STD(PCT_CHG)+CS_QUANTILE(PCT_CHG,0.25)+CS_WMEAN(PCT_CHG,AMOUNT)+CS_TOPSHARE(AMOUNT,0.5)+CS_CORR(PCT_CHG,AMOUNT)+CS_SPEARMAN(PCT_CHG,AMOUNT)"
prec=12
[[ind]]
name="MY_Z"
scope="cross_section"
expr="CS_ZSCORE(PCT_CHG)"
prec=12
[[ind]]
name="I_CUSTOM"
scope="market"
expr="N:=IF(1,1,2); LN(EXP(N))+SQRT(4)+ISVALID(NULL())"
prec=12
"#;
        std::fs::write(ind_toml_path(source), config).unwrap();
        init_stock_data_db(super::super::source_db_path(source).to_str().unwrap()).unwrap();
        let conn = Connection::open(super::super::source_db_path(source)).unwrap();
        for (day, date) in dates.iter().enumerate().skip(80) {
            conn.execute(
                "INSERT INTO stock_data(ts_code,trade_date,adj_type,close) VALUES('000001.SH',?,'ind',?)",
                params![date, 100. + (day - 80) as f64],
            ).unwrap();
            for (stock, code) in ["A", "B", "C", "D"].iter().enumerate() {
                if stock == 3 && day == 88 {
                    continue;
                }
                conn.execute("INSERT INTO stock_data(ts_code,trade_date,adj_type,open,high,low,close,pct_chg,amount,tor) VALUES(?,?,'qfq',10,12,9,11,?,?,1)",params![code,date,[1.,1.,3.,5.][stock]+(day-80) as f64*0.001,(stock+1) as f64*100.]).unwrap();
            }
        }
        assert_eq!(refresh(&conn, source, false).unwrap(), 20);
        // A repair can precede the remaining quotes' first date (for example,
        // after removing an early quote); full replay still starts at first.
        invalidate(&conn, &dates[0]).unwrap();
        assert_eq!(refresh(&conn, source, false).unwrap(), 100);
        assert_eq!(refresh(&conn, source, false).unwrap(), 0);
        let reader = DataReader::new_with_connection(source, conn.try_clone().unwrap()).unwrap();
        let row = reader.load_one("A", "qfq", &dates[80], &dates[99]).unwrap();
        let mut runtime = row_into_rt(row.clone()).unwrap();
        let condition =
            parse_expression_program("MY_RANK > 0.3 AND MY_GROUP = 1 AND I_CUSTOM = 3").unwrap();
        let value = runtime.eval_program(&condition).unwrap();
        assert!(Value::as_bool_series(&value, row.trade_dates.len()).unwrap()[0]);
        runtime
            .vars
            .insert("GAP".into(), Value::NumSeries(vec![Some(1.), None]));
        let missing = parse_expression_program("HHV(IF(ISVALID(GAP),GAP,NULL()),2)").unwrap();
        assert_eq!(
            runtime
                .eval_program(&missing)
                .unwrap()
                .into_num_series(2)
                .unwrap(),
            vec![None, None]
        );

        let requested = HashSet::from(["I_CUSTOM".to_string()]);
        let selected =
            DataReader::new_inner(source, Some(&requested), Some(conn.try_clone().unwrap()))
                .unwrap();
        assert_eq!(
            selected
                .load_one_tail_rows("A", "qfq", &dates[99], 1)
                .unwrap()
                .cols["I_CUSTOM"][0],
            Some(3.)
        );
        drop(selected);
        assert_eq!(row.cols["MY_MEAN"][0], Some(2.5));
        assert_eq!(row.cols["MY_INDEX"][0], Some(102.5));
        assert_eq!(row.cols["MY_RANK"][0], Some(0.375));
        assert_eq!(row.cols["MY_GROUP"][0], Some(1.));
        assert_eq!(row.cols["MY_NESTED"][0], Some(5.));
        assert_eq!(row.cols["MY_ZERO"][0], Some(0.));
        assert_eq!(row.cols["I_CUSTOM"][0], Some(3.));
        assert!((row.cols["MY_Z"][0].unwrap() + 1.5 / (11_f64 / 3.).sqrt()).abs() < 1e-11);
        let stats = 10.
            + 1.
            + 5.
            + 2.
            + 11. / 3.
            + (11_f64 / 3.).sqrt()
            + 1.
            + 3.2
            + 0.7
            + (49_f64 / 55.).sqrt()
            + (0.9_f64).sqrt();
        assert!(
            (row.cols["MY_STATS"][0].unwrap() - stats).abs() < 1e-11,
            "stats: {} != {stats}",
            row.cols["MY_STATS"][0].unwrap()
        );
        let other = reader.load_one("D", "qfq", &dates[80], &dates[99]).unwrap();
        assert_eq!(other.cols["MY_GROUP"][0], Some(4.));
        assert_eq!(other.cols["MY_MEAN"][0], row.cols["MY_MEAN"][0]);
        conn.execute_batch("BEGIN").unwrap();
        delete_one_stock_range(&conn, "D", AdjType::Qfq, &dates[90], &dates[90]).unwrap();
        conn.execute_batch("ROLLBACK").unwrap();
        assert!(
            reader
                .load_one_tail_rows("A", "qfq", &dates[99], 1)
                .unwrap()
                .cols["MY_MEAN"][0]
                .is_some()
        );
        delete_one_stock_range(&conn, "D", AdjType::Qfq, &dates[90], &dates[90]).unwrap();
        assert!(
            reader
                .load_one_tail_rows("A", "qfq", &dates[99], 1)
                .unwrap()
                .cols["MY_MEAN"][0]
                .is_none()
        );
        assert_eq!(refresh(&conn, source, false).unwrap(), 10);
        let incremental = reader.load_one("A", "qfq", &dates[80], &dates[99]).unwrap();
        refresh(&conn, source, true).unwrap();
        let full = reader.load_one("A", "qfq", &dates[80], &dates[99]).unwrap();
        for (name, _) in configured_fields(source).unwrap() {
            for (a, b) in incremental.cols[&name].iter().zip(&full.cols[&name]) {
                assert_eq!(a.is_some(), b.is_some());
                if let (Some(a), Some(b)) = (a, b) {
                    assert!((a - b).abs() < 1e-11, "{name}: {a} != {b}");
                }
            }
        }
        // A finite-window configuration also supports suffix-only repair.
        let finite = config.replace("EMA(MY_MEAN,3)", "MA(MY_MEAN,3)");
        std::fs::write(ind_toml_path(source), &finite).unwrap();
        assert!(
            reader
                .load_one_tail_rows("A", "qfq", &dates[99], 1)
                .unwrap()
                .cols["MY_MEAN"][0]
                .is_none()
        );
        refresh(&conn, source, false).unwrap();
        invalidate(&conn, &dates[95]).unwrap();
        assert_eq!(refresh(&conn, source, false).unwrap(), 5);
        // The incremental reader's temporary stock_data must neither hide nor
        // replace persistent history (including index inputs) for the caller.
        assert_eq!(
            conn.query_row("SELECT count(*) FROM stock_data", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            98
        );
        let partial = reader.load_one("A", "qfq", &dates[80], &dates[99]).unwrap();
        refresh(&conn, source, true).unwrap();
        let full = reader.load_one("A", "qfq", &dates[80], &dates[99]).unwrap();
        for (name, _) in configured_fields(source).unwrap() {
            for (a, b) in partial.cols[&name].iter().zip(&full.cols[&name]) {
                assert_eq!(a, b, "{name}");
            }
        }
        let invalid = IndsData::parse_from_text(
            &config.replace("CS_MEAN(PCT_CHG,LISTED_DAYS>=60)", "LAST(C,0)"),
        )
        .unwrap();
        assert!(validate_definitions(&invalid).unwrap_err().contains("LAST"));
        let invalid =
            IndsData::parse_from_text(&config.replacen("scope=\"market\"", "scope=\"stock\"", 1))
                .unwrap();
        assert!(validate_definitions(&invalid).unwrap_err().contains("范围"));

        // Cross a write-batch boundary and exercise a matrix alias with its own
        // precision: rounding the alias must not mutate its published input.
        conn.execute(
            "INSERT INTO stock_data(ts_code,trade_date,adj_type,pct_chg,amount,tor) SELECT 'X'||i,?,'qfq',i,100,1 FROM range(65) t(i)",
            [&dates[99]],
        ).unwrap();
        let batched = r#"version=1
[[ind]]
name="MY_BASE"
scope="cross_section"
expr="CS_RANK(PCT_CHG)"
prec=12
[[ind]]
name="MY_ALIAS"
scope="cross_section"
expr="MY_BASE"
prec=2
[[ind]]
name="MY_COPY"
scope="cross_section"
expr="MY_BASE"
prec=12
"#;
        std::fs::write(ind_toml_path(source), batched).unwrap();
        refresh(&conn, source, false).unwrap();
        let (base, alias, copy): (f64, f64, f64) = conn.query_row(
            "SELECT max(value) FILTER(WHERE name='MY_BASE'),max(value) FILTER(WHERE name='MY_ALIAS'),max(value) FILTER(WHERE name='MY_COPY') FROM indicator_panel_values WHERE ts_code='X0'",
            [], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        ).unwrap();
        assert!((base - 1. / 69.).abs() < 1e-12);
        assert_eq!(alias, 0.01);
        assert_eq!(base, copy);
        conn.execute_batch(
            "CREATE TEMP TABLE expected_panel AS SELECT * FROM indicator_panel_values",
        )
        .unwrap();
        let failing = format!(
            "{batched}\n[[ind]]\nname=\"MY_BAD\"\nscope=\"market\"\nexpr=\"CS_QUANTILE(PCT_CHG,2)\"\nprec=12\n"
        );
        std::fs::write(ind_toml_path(source), failing).unwrap();
        let error = refresh(&conn, source, false).unwrap_err();
        assert!(error.contains("[0,1]"), "{error}");
        assert!(
            conn.query_row("SELECT count(*)>0 FROM indicator_panel_values", [], |r| {
                r.get::<_, bool>(0)
            })
            .unwrap()
        );
        assert!(
            load_values(
                &conn,
                source,
                &["MY_BASE".into()],
                &["X0".into()],
                &dates[80],
                &dates[99]
            )
            .unwrap()
            .is_empty()
        );
        std::fs::write(ind_toml_path(source), batched).unwrap();
        refresh(&conn, source, false).unwrap();
        assert_eq!(conn.query_row(
            "SELECT count(*) FROM ((SELECT * FROM expected_panel EXCEPT SELECT * FROM indicator_panel_values) UNION ALL (SELECT * FROM indicator_panel_values EXCEPT SELECT * FROM expected_panel))",
            [], |r| r.get::<_, usize>(0),
        ).unwrap(), 0);
        drop(reader);
        drop(conn);
        std::fs::remove_dir_all(path).unwrap();
    }
}
