use super::{
    DataReader, IndData, IndicatorScope, IndsData, RowData, ind_toml_path, load_trade_date_list,
    runtime::row_into_rt, stock_list_path,
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
use duckdb::{Connection, params};
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
    let mut rows = stmt
        .query(params![signature, start, end])
        .map_err(|e| e.to_string())?;
    let mut out = PanelValues::new();
    while let Some(row) = rows.next().map_err(|e| e.to_string())? {
        let name: String = row.get(0).map_err(|e| e.to_string())?;
        let stock: String = row.get(1).map_err(|e| e.to_string())?;
        let date: String = row.get(2).map_err(|e| e.to_string())?;
        let value = row
            .get::<_, Option<f64>>(3)
            .map_err(|e| e.to_string())?
            .filter(|v| v.is_finite());
        out.entry(name)
            .or_default()
            .entry(stock)
            .or_default()
            .insert(date, value);
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

struct Source<'a> {
    conn: &'a Connection,
    source: &'a str,
    dates: Vec<String>,
    day_lookup: HashMap<String, usize>,
    sessions: Vec<usize>,
    stocks: Vec<String>,
    presence: Vec<Vec<bool>>,
    listed: BTreeMap<String, usize>,
}

impl PanelSource for Source<'_> {
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
        let reader = if required.is_empty() {
            None
        } else {
            Some(DataReader::new_inner(
                self.source,
                Some(&required),
                Some(self.conn.try_clone().map_err(|e| e.to_string())?),
            )?)
        };
        let all_days = (0..self.dates.len()).collect::<Vec<_>>();
        let mut out = vec![vec![f64::NAN; self.dates.len()]; self.stocks.len()];
        for (batch_index, codes) in self.stocks.chunks(64).enumerate() {
            let batch = if let Some(reader) = &reader {
                reader.load_batch(
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
                let days = self.presence[stock]
                    .iter()
                    .enumerate()
                    .filter_map(|(day, present)| present.then_some(day))
                    .collect::<Vec<_>>();
                let mut aligned = RowData {
                    trade_dates: self.dates.clone(),
                    cols: HashMap::new(),
                };
                if let Some(row) = batch.get(code) {
                    for (name, column) in &row.cols {
                        let mut full = vec![None; self.dates.len()];
                        for (date, value) in row.trade_dates.iter().zip(column) {
                            full[self.day_lookup[date]] = *value;
                        }
                        aligned.cols.insert(name.clone(), full);
                    }
                }
                let mut rt = row_into_rt(aligned)?;
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
                for day in days {
                    out[stock][day] = values[day].filter(|v| v.is_finite()).unwrap_or(f64::NAN);
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
    let mut raw_inputs = HashSet::new();
    for def in &panel_defs {
        let program = parse_expression_program(&def.expr).map_err(|e| e.msg)?;
        raw_inputs.extend(
            expression_input_identifiers(&program)
                .into_iter()
                .filter(|key| {
                    !panel_names.contains(key.as_str())
                        && !matches!(key.as_str(), "SESSION" | "LISTED_DAYS")
                }),
        );
    }
    DataReader::new_inner(
        source,
        Some(&raw_inputs),
        Some(conn.try_clone().map_err(|e| e.to_string())?),
    )?;
    let mut warmup = 0usize;
    let mut full = false;
    for def in &panel_defs {
        let p = parse_expression_program(&def.expr).map_err(|e| e.msg)?;
        warmup = warmup
            .checked_add(estimate_expression_warmup(&p)?)
            .ok_or("指标预热长度溢出")?;
        full |= p.item.iter().any(|s| match s {
            Stmt::Expr(e) | Stmt::Assign { value: e, .. } => uses_context(e, true),
        });
    }
    let warm_index = if full {
        calendar.partition_point(|d| d < &first)
    } else {
        output_index.saturating_sub(warmup.max(1))
    };
    let dates = calendar[warm_index..end_index].to_vec();
    if dates.is_empty() {
        return Ok(0);
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
    let mut rows = stmt
        .query(params![dates[0], last])
        .map_err(|e| e.to_string())?;
    while let Some(r) = rows.next().map_err(|e| e.to_string())? {
        let code: String = r.get(0).map_err(|e| e.to_string())?;
        let date: String = r.get(1).map_err(|e| e.to_string())?;
        if let Some(&day) = day_lookup.get(&date) {
            presence[stock_lookup[&code]][day] = true;
        }
    }
    drop(rows);
    drop(stmt);
    let source_data = Source {
        conn,
        source,
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
    let mut results = Vec::new();
    let count = panel_defs.len();
    for (index, def) in panel_defs.into_iter().enumerate() {
        if let Some(cb) = progress {
            cb(index, count + 1, &def.name);
        }
        let program = parse_expression_program(&def.expr).map_err(|e| e.msg)?;
        let published = rt.vars.clone();
        let mut value = rt
            .eval_program(&program)
            .map_err(|e| format!("指标{}计算失败:{e}", def.name))?;
        if def.scope == IndicatorScope::Market && matches!(value, PanelValue::Stocks(_)) {
            return Err(format!(
                "市场指标{}仍包含逐股结果，请使用CS_MEAN等聚合，或改为横截面范围",
                def.name
            ));
        }
        let round = |v: f64| {
            let scale = 10_f64.powi(def.prec as i32);
            if v.is_finite() {
                (v * scale).round() / scale
            } else {
                f64::NAN
            }
        };
        value = match value {
            PanelValue::Scalar(v) => PanelValue::Scalar(round(v)),
            PanelValue::Market(v) => {
                PanelValue::Market(Arc::new(v.iter().map(|v| round(*v)).collect()))
            }
            PanelValue::Stocks(v) => PanelValue::Stocks(Arc::new(
                v.iter()
                    .map(|row| row.iter().map(|v| round(*v)).collect())
                    .collect(),
            )),
        };
        rt.vars = published;
        rt.vars.insert(def.name.clone(), value.clone());
        results.push((def.name.clone(), def.scope, value));
        if let Some(cb) = progress {
            cb(index + 1, count + 1, &def.name);
        }
    }
    if let Some(cb) = progress {
        cb(count, count + 1, "保存指标结果");
    }
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
    for (name, scope, values) in &results {
        if *scope == IndicatorScope::Market {
            let mut app = conn
                .appender("indicator_panel_values")
                .map_err(|e| e.to_string())?;
            for (day, date) in rt
                .source
                .dates
                .iter()
                .enumerate()
                .filter(|(_, d)| *d >= &start)
            {
                app.append_row(params![name, "", date, values.at(0, day)])
                    .map_err(|e| e.to_string())?;
            }
            app.flush().map_err(|e| e.to_string())?;
        } else {
            for chunk in (0..rt.source.stocks.len()).collect::<Vec<_>>().chunks(64) {
                let mut app = conn
                    .appender("indicator_panel_values")
                    .map_err(|e| e.to_string())?;
                for &stock in chunk {
                    for (day, date) in rt
                        .source
                        .dates
                        .iter()
                        .enumerate()
                        .filter(|(_, d)| *d >= &start)
                    {
                        if rt.source.presence[stock][day] {
                            app.append_row(params![
                                name,
                                &rt.source.stocks[stock],
                                date,
                                values.at(stock, day)
                            ])
                            .map_err(|e| e.to_string())?;
                        }
                    }
                }
                app.flush().map_err(|e| e.to_string())?;
            }
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
    if let Some(cb) = progress {
        cb(count + 1, count + 1, "完成");
    }
    Ok(end_index - output_index)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::download_data::{delete_one_stock_range, init_stock_data_db};
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
            for (stock, code) in ["A", "B", "C", "D"].iter().enumerate() {
                if stock == 3 && day == 88 {
                    continue;
                }
                conn.execute("INSERT INTO stock_data(ts_code,trade_date,adj_type,open,high,low,close,pct_chg,amount,tor) VALUES(?,?,'qfq',10,12,9,11,?,?,1)",params![code,date,[1.,1.,3.,5.][stock]+(day-80) as f64*0.001,(stock+1) as f64*100.]).unwrap();
            }
        }
        assert_eq!(refresh(&conn, source, false).unwrap(), 20);
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
        drop(reader);
        drop(conn);
        std::fs::remove_dir_all(path).unwrap();
    }
}
