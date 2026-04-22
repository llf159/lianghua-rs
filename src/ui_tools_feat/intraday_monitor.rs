use std::collections::HashMap;

use duckdb::{Connection, params};
use serde::{Deserialize, Serialize};

use crate::{
    crawler::SinaQuote,
    data::{DataReader, RowData, result_db_path, scoring_data::row_into_rt},
    download::ind_calc::{
        IndsCache, cache_ind_build, calc_inds_with_cache_lossy, warmup_ind_estimate,
    },
    expr::{
        eval::Value,
        parser::{Expr, Parser, Stmt, Stmts, lex_all},
    },
    scoring::tools::{inject_latest_num_fields, inject_stock_extra_fields, rt_max_len},
    ui_tools_feat::{
        build_concepts_map, build_latest_vol_map, build_name_map, build_total_mv_map, filter_mv,
        realtime::{fetch_realtime_quote_map, normalize_quote_trade_date},
    },
    utils::utils::{board_category, eval_binary_for_warmup, impl_expr_warmup},
};

const BOARD_ST: &str = "ST";
const DEFAULT_ADJ_TYPE: &str = "qfq";
const RUNTIME_INPUT_KEYS: [&str; 10] = [
    "O",
    "H",
    "L",
    "C",
    "V",
    "AMOUNT",
    "PRE_CLOSE",
    "CHANGE",
    "PCT_CHG",
    "TURNOVER_RATE",
];

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct IntradayMonitorRow {
    pub rank_mode: String,
    pub ts_code: String,
    pub trade_date: Option<String>,
    pub scene_name: String,
    pub direction: Option<String>,
    pub total_score: Option<f64>,
    pub scene_score: Option<f64>,
    pub risk_score: Option<f64>,
    pub confirm_strength: Option<f64>,
    pub risk_intensity: Option<f64>,
    pub scene_status: Option<String>,
    pub rank: Option<i64>,
    pub name: String,
    pub board: String,
    pub total_mv_yi: Option<f64>,
    pub concept: String,
    pub realtime_price: Option<f64>,
    pub realtime_open: Option<f64>,
    pub realtime_high: Option<f64>,
    pub realtime_low: Option<f64>,
    pub realtime_pre_close: Option<f64>,
    pub realtime_vol: Option<f64>,
    pub realtime_amount: Option<f64>,
    pub realtime_change_pct: Option<f64>,
    pub realtime_change_open_pct: Option<f64>,
    pub realtime_fall_from_high_pct: Option<f64>,
    pub realtime_vol_ratio: Option<f64>,
    pub template_tag_text: Option<String>,
    pub template_tag_tone: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IntradayMonitorPageData {
    pub rows: Vec<IntradayMonitorRow>,
    pub rank_date_options: Option<Vec<String>>,
    pub resolved_rank_date: Option<String>,
    pub scene_options: Option<Vec<String>>,
    pub refreshed_at: Option<String>,
    pub warning_message: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct IntradayMonitorTemplateValidationData {
    pub normalized_expression: String,
    pub warmup_need: usize,
    pub message: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct IntradayMonitorTemplate {
    pub id: String,
    pub name: String,
    pub expression: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct IntradayMonitorRankModeConfig {
    pub mode: String,
    pub scene_name: String,
    pub template_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IntradayRankMode {
    Total,
    Scene,
}

#[derive(Debug, Clone)]
struct ReadyIntradayMonitorTemplate {
    name: String,
    ast: Stmts,
    warmup_need: usize,
}

#[derive(Debug, Clone)]
enum CompiledIntradayMonitorTemplate {
    Ready(ReadyIntradayMonitorTemplate),
    Invalid { name: String, message: String },
}

impl IntradayRankMode {
    fn parse(raw: Option<&str>) -> Result<Self, String> {
        let normalized = raw
            .map(|value| value.trim().to_ascii_lowercase())
            .unwrap_or_else(|| "total".to_string());
        match normalized.as_str() {
            "" | "total" | "summary" | "overall" => Ok(Self::Total),
            "scene" => Ok(Self::Scene),
            _ => Err("rank_mode 仅支持 total 或 scene".to_string()),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Total => "total",
            Self::Scene => "scene",
        }
    }

    fn table_name(self) -> &'static str {
        match self {
            Self::Total => "score_summary",
            Self::Scene => "scene_details",
        }
    }
}

fn open_result_conn(source_path: &str) -> Result<Connection, String> {
    let result_db = result_db_path(source_path);
    let result_db_str = result_db
        .to_str()
        .ok_or_else(|| "结果库路径不是有效UTF-8".to_string())?;
    Connection::open(result_db_str).map_err(|e| format!("打开结果库失败: {e}"))
}

fn resolve_runtime_trade_date(
    row: &IntradayMonitorRow,
    quote: &SinaQuote,
) -> Result<String, String> {
    if let Some(trade_date) = normalize_quote_trade_date(&quote.date) {
        return Ok(trade_date);
    }

    let raw_quote_date = quote.date.trim();
    Err(if raw_quote_date.is_empty() {
        format!(
            "{} 实时行情缺少可用日期，已停止 runtime 计算以避免把最新行情静默写入旧交易日",
            row.ts_code
        )
    } else {
        format!(
            "{} 实时行情日期无法识别: {}，已停止 runtime 计算以避免把最新行情静默写入旧交易日",
            row.ts_code, raw_quote_date
        )
    })
}

fn resolve_trade_date(
    conn: &Connection,
    trade_date: Option<String>,
    rank_mode: IntradayRankMode,
) -> Result<String, String> {
    if let Some(d) = trade_date {
        let d = d.trim().to_string();
        if !d.is_empty() {
            return Ok(d);
        }
    }

    let table_name = rank_mode.table_name();
    let sql = format!("SELECT MAX(trade_date) FROM {table_name}");
    let mut stmt = conn
        .prepare(&sql)
        .map_err(|e| format!("查询最新交易日预编译失败: {e}"))?;
    let mut rows = stmt
        .query([])
        .map_err(|e| format!("查询最新交易日失败: {e}"))?;

    if let Some(row) = rows
        .next()
        .map_err(|e| format!("读取最新交易日结果失败: {e}"))?
    {
        let d: Option<String> = row
            .get(0)
            .map_err(|e| format!("读取最新交易日字段失败: {e}"))?;
        if let Some(v) = d {
            if !v.trim().is_empty() {
                return Ok(v);
            }
        }
    }
    Err(format!("{} 没有可用交易日", table_name))
}

fn query_rank_trade_date_options_from_conn(
    conn: &Connection,
    rank_mode: IntradayRankMode,
) -> Result<Vec<String>, String> {
    let table_name = rank_mode.table_name();
    let sql = format!(
        r#"
            SELECT DISTINCT trade_date
            FROM {table_name}
            ORDER BY trade_date DESC
            "#
    );
    let mut stmt = conn
        .prepare(&sql)
        .map_err(|e| format!("预编译日期列表失败: {e}"))?;
    let mut rows = stmt
        .query([])
        .map_err(|e| format!("查询日期列表失败: {e}"))?;
    let mut out = Vec::new();

    while let Some(row) = rows.next().map_err(|e| format!("读取日期列表失败: {e}"))? {
        let trade_date: String = row.get(0).map_err(|e| format!("读取日期字段失败: {e}"))?;
        if !trade_date.trim().is_empty() {
            out.push(trade_date);
        }
    }

    Ok(out)
}

fn query_scene_options_from_conn(conn: &Connection) -> Result<Vec<String>, String> {
    let mut stmt = conn
        .prepare(
            r#"
            SELECT DISTINCT scene_name
            FROM scene_details
            WHERE scene_name IS NOT NULL
            ORDER BY scene_name ASC
            "#,
        )
        .map_err(|e| format!("预编译场景列表失败: {e}"))?;
    let mut rows = stmt
        .query([])
        .map_err(|e| format!("查询场景列表失败: {e}"))?;
    let mut out = Vec::new();

    while let Some(row) = rows.next().map_err(|e| format!("读取场景列表失败: {e}"))? {
        let scene_name: String = row.get(0).map_err(|e| format!("读取场景名称失败: {e}"))?;
        let scene_name = scene_name.trim();
        if !scene_name.is_empty() {
            out.push(scene_name.to_string());
        }
    }

    Ok(out)
}

fn default_template_name(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        "未命名模板".to_string()
    } else {
        trimmed.to_string()
    }
}

fn estimate_intraday_template_warmup(stmts: &Stmts) -> Result<usize, String> {
    let mut locals = HashMap::new();
    let mut consts: HashMap<String, usize> = HashMap::new();
    let mut expr_need = 0usize;

    for stmt in stmts.item.iter().cloned() {
        match stmt {
            Stmt::Assign { name, value } => match value {
                Expr::Number(v) => {
                    if v < 0.0 {
                        return Err("表达式常量赋值结果不能为负数".to_string());
                    }
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
            Stmt::Expr(expr) => {
                expr_need = expr_need.max(impl_expr_warmup(expr, &locals, &consts)?);
            }
        }
    }

    Ok(expr_need)
}

fn compile_intraday_templates(
    templates: &[IntradayMonitorTemplate],
) -> HashMap<String, CompiledIntradayMonitorTemplate> {
    let mut out = HashMap::with_capacity(templates.len());

    for template in templates {
        let id = template.id.trim();
        if id.is_empty() {
            continue;
        }

        let name = default_template_name(&template.name);
        let expression = template.expression.trim();
        if expression.is_empty() {
            out.insert(
                id.to_string(),
                CompiledIntradayMonitorTemplate::Invalid {
                    name,
                    message: "表达式不能为空".to_string(),
                },
            );
            continue;
        }

        let compiled = (|| -> Result<ReadyIntradayMonitorTemplate, String> {
            let tokens = lex_all(expression);
            let mut parser = Parser::new(tokens);
            let ast = parser
                .parse_main()
                .map_err(|e| format!("表达式解析错误在{}:{}", e.idx, e.msg))?;
            let warmup_need = estimate_intraday_template_warmup(&ast)?;
            Ok(ReadyIntradayMonitorTemplate {
                name: name.clone(),
                ast,
                warmup_need,
            })
        })();

        match compiled {
            Ok(template) => {
                out.insert(
                    id.to_string(),
                    CompiledIntradayMonitorTemplate::Ready(template),
                );
            }
            Err(err) => {
                out.insert(
                    id.to_string(),
                    CompiledIntradayMonitorTemplate::Invalid { name, message: err },
                );
            }
        }
    }

    out
}

pub fn validate_intraday_monitor_template_expression(
    expression: String,
) -> Result<IntradayMonitorTemplateValidationData, String> {
    let normalized = expression.trim().to_string();
    if normalized.is_empty() {
        return Err("表达式不能为空".to_string());
    }

    let tokens = lex_all(&normalized);
    let mut parser = Parser::new(tokens);
    let ast = parser
        .parse_main()
        .map_err(|e| format!("表达式解析错误在{}:{}", e.idx, e.msg))?;
    let warmup_need = estimate_intraday_template_warmup(&ast)?;
    let row_data = build_intraday_template_validation_row_data(warmup_need)?;
    let mut rt = row_into_rt(row_data)?;
    let value = rt
        .eval_program(&ast)
        .map_err(|e| format!("表达式运行错误:{}", e.msg))?;
    let len = rt_max_len(&rt);
    Value::as_bool_series(&value, len).map_err(|e| format!("表达式返回值非布尔:{}", e.msg))?;

    Ok(IntradayMonitorTemplateValidationData {
        normalized_expression: normalized,
        warmup_need,
        message: "表达式可用于实时模板".to_string(),
    })
}

fn resolve_template_id_for_row<'a>(
    row: &IntradayMonitorRow,
    rank_mode_configs: &'a [IntradayMonitorRankModeConfig],
) -> Option<&'a str> {
    let row_mode = row.rank_mode.trim().to_ascii_lowercase();
    if row_mode == "scene" {
        if let Some(config) = rank_mode_configs.iter().find(|item| {
            item.mode.trim().eq_ignore_ascii_case("scene")
                && item.scene_name.trim() == row.scene_name.trim()
                && !item.template_id.trim().is_empty()
        }) {
            return Some(config.template_id.trim());
        }
        return rank_mode_configs
            .iter()
            .find(|item| {
                item.mode.trim().eq_ignore_ascii_case("scene")
                    && item.scene_name.trim() == "全部"
                    && !item.template_id.trim().is_empty()
            })
            .map(|item| item.template_id.trim());
    }

    rank_mode_configs
        .iter()
        .find(|item| {
            item.mode.trim().eq_ignore_ascii_case("total") && !item.template_id.trim().is_empty()
        })
        .map(|item| item.template_id.trim())
}

fn normalize_runtime_row_data(row_data: RowData) -> Result<RowData, String> {
    let len = row_data.trade_dates.len();
    let mut cols = HashMap::with_capacity(RUNTIME_INPUT_KEYS.len());

    for key in RUNTIME_INPUT_KEYS {
        let series = row_data
            .cols
            .get(key)
            .cloned()
            .unwrap_or_else(|| vec![None; len]);
        cols.insert(key.to_string(), series);
    }

    let out = RowData {
        trade_dates: row_data.trade_dates,
        cols,
    };
    out.validate()?;
    Ok(out)
}

fn build_intraday_template_validation_row_data(warmup_need: usize) -> Result<RowData, String> {
    let len = warmup_need.max(2);
    let trade_dates = (0..len)
        .map(|index| format!("202401{:02}", index + 1))
        .collect::<Vec<_>>();
    let mut cols = HashMap::with_capacity(RUNTIME_INPUT_KEYS.len() + 8);

    for key in RUNTIME_INPUT_KEYS {
        let series = (0..len)
            .map(|index| Some(index as f64 + 1.0))
            .collect::<Vec<_>>();
        cols.insert(key.to_string(), series);
    }

    for key in [
        "REALTIME_CHANGE_OPEN_PCT",
        "REALTIME_FALL_FROM_HIGH_PCT",
        "REALTIME_VOL_RATIO",
        "VOL_RATIO",
    ] {
        let mut series = vec![None; len];
        if let Some(last) = series.last_mut() {
            *last = Some(1.0);
        }
        cols.insert(key.to_string(), series);
    }

    cols.insert("ZHANG".to_string(), vec![Some(0.095); len]);
    cols.insert("TOTAL_MV_YI".to_string(), vec![Some(100.0); len]);

    let mut rank_series = (0..len)
        .map(|index| Some((index + 1) as f64))
        .collect::<Vec<_>>();
    if let Some(last) = rank_series.last_mut() {
        *last = None;
    }
    cols.insert("RANK".to_string(), rank_series.clone());
    cols.insert("rank".to_string(), rank_series);

    let out = RowData { trade_dates, cols };
    out.validate()?;
    Ok(out)
}

fn build_quote_only_runtime_row_data(
    quote: &SinaQuote,
    trade_date: &str,
) -> Result<RowData, String> {
    let trade_date = trade_date.trim();
    if trade_date.is_empty() {
        return Err("缺少可用日期，无法仅用实时行情构建 runtime".to_string());
    }
    let change = quote.price - quote.pre_close;
    let pct_chg = if quote.pre_close.abs() > f64::EPSILON {
        Some(change / quote.pre_close * 100.0)
    } else {
        quote.change_pct
    };
    let mut cols = HashMap::with_capacity(RUNTIME_INPUT_KEYS.len());
    cols.insert("O".to_string(), vec![Some(quote.open)]);
    cols.insert("H".to_string(), vec![Some(quote.high)]);
    cols.insert("L".to_string(), vec![Some(quote.low)]);
    cols.insert("C".to_string(), vec![Some(quote.price)]);
    cols.insert("V".to_string(), vec![Some(quote.vol)]);
    cols.insert("AMOUNT".to_string(), vec![Some(quote.amount)]);
    cols.insert("PRE_CLOSE".to_string(), vec![Some(quote.pre_close)]);
    cols.insert("CHANGE".to_string(), vec![Some(change)]);
    cols.insert("PCT_CHG".to_string(), vec![pct_chg]);
    cols.insert("TURNOVER_RATE".to_string(), vec![None]);

    let out = RowData {
        trade_dates: vec![trade_date.to_string()],
        cols,
    };
    out.validate()?;
    Ok(out)
}

fn merge_realtime_quote_into_row_data(
    row_data: &mut RowData,
    quote: &SinaQuote,
    trade_date: &str,
) -> Result<(), String> {
    let trade_date = trade_date.trim();
    if trade_date.is_empty() {
        return Err("缺少可用日期，无法拼接实时行情".to_string());
    }
    let replace_last = row_data
        .trade_dates
        .last()
        .map(|value| value == trade_date)
        .unwrap_or(false);

    if !replace_last {
        row_data.trade_dates.push(trade_date.to_string());
        for series in row_data.cols.values_mut() {
            series.push(None);
        }
    }

    let last_index = row_data.trade_dates.len().saturating_sub(1);
    let change = quote.price - quote.pre_close;
    let pct_chg = if quote.pre_close.abs() > f64::EPSILON {
        Some(change / quote.pre_close * 100.0)
    } else {
        quote.change_pct
    };

    for (key, value) in [
        ("O", Some(quote.open)),
        ("H", Some(quote.high)),
        ("L", Some(quote.low)),
        ("C", Some(quote.price)),
        ("V", Some(quote.vol)),
        ("AMOUNT", Some(quote.amount)),
        ("PRE_CLOSE", Some(quote.pre_close)),
        ("CHANGE", Some(change)),
        ("PCT_CHG", pct_chg),
        ("TURNOVER_RATE", None),
    ] {
        let series = row_data
            .cols
            .entry(key.to_string())
            .or_insert_with(|| vec![None; row_data.trade_dates.len()]);
        if series.len() < row_data.trade_dates.len() {
            series.resize(row_data.trade_dates.len(), None);
        }
        series[last_index] = value;
    }

    row_data.validate()
}

fn attach_runtime_extra_series(
    row_data: &mut RowData,
    row: &IntradayMonitorRow,
) -> Result<(), String> {
    let is_st = row.board.trim() == BOARD_ST;
    inject_stock_extra_fields(row_data, &row.ts_code, is_st, row.total_mv_yi)?;
    inject_latest_num_fields(
        row_data,
        &[
            ("REALTIME_CHANGE_OPEN_PCT", row.realtime_change_open_pct),
            (
                "REALTIME_FALL_FROM_HIGH_PCT",
                row.realtime_fall_from_high_pct,
            ),
            ("REALTIME_VOL_RATIO", row.realtime_vol_ratio),
            ("VOL_RATIO", row.realtime_vol_ratio),
        ],
    )
}

fn load_runtime_rank_series_map(
    conn: &Connection,
    row: &IntradayMonitorRow,
    start_date: &str,
    end_date: &str,
) -> Result<HashMap<String, Option<f64>>, String> {
    let rank_mode = IntradayRankMode::parse(Some(&row.rank_mode))?;
    let (sql, params): (&str, Vec<String>) = match rank_mode {
        IntradayRankMode::Total => (
            r#"
            SELECT trade_date, rank
            FROM score_summary
            WHERE ts_code = ?
              AND trade_date >= ?
              AND trade_date <= ?
            "#,
            vec![
                row.ts_code.clone(),
                start_date.to_string(),
                end_date.to_string(),
            ],
        ),
        IntradayRankMode::Scene => (
            r#"
            SELECT trade_date, scene_rank
            FROM scene_details
            WHERE ts_code = ?
              AND scene_name = ?
              AND trade_date >= ?
              AND trade_date <= ?
            "#,
            vec![
                row.ts_code.clone(),
                row.scene_name.clone(),
                start_date.to_string(),
                end_date.to_string(),
            ],
        ),
    };

    let mut stmt = conn
        .prepare(sql)
        .map_err(|e| format!("预编译 rank 序列失败: {e}"))?;
    let mut rows = stmt
        .query(duckdb::params_from_iter(params.iter()))
        .map_err(|e| format!("查询 rank 序列失败: {e}"))?;
    let mut out = HashMap::new();

    while let Some(db_row) = rows
        .next()
        .map_err(|e| format!("读取 rank 序列失败: {e}"))?
    {
        let trade_date: String = db_row
            .get(0)
            .map_err(|e| format!("读取 rank 日期失败: {e}"))?;
        let rank = db_row
            .get::<_, Option<i64>>(1)
            .map_err(|e| format!("读取 rank 值失败: {e}"))?
            .map(|value| value as f64);
        out.insert(trade_date, rank);
    }

    Ok(out)
}

fn inject_runtime_rank_series(
    row_data: &mut RowData,
    rank_series_map: &HashMap<String, Option<f64>>,
) -> Result<(), String> {
    let len = row_data.trade_dates.len();
    let mut rank_series = vec![None; len];

    for (index, trade_date) in row_data.trade_dates.iter().enumerate() {
        rank_series[index] = rank_series_map.get(trade_date).copied().flatten();
    }

    if let Some(last) = rank_series.last_mut() {
        *last = None;
    }

    row_data
        .cols
        .insert("RANK".to_string(), rank_series.clone());
    row_data.cols.insert("rank".to_string(), rank_series);
    row_data.validate()
}

fn build_intraday_runtime_row_data(
    reader: &DataReader,
    result_conn: &Connection,
    row: &IntradayMonitorRow,
    quote: &SinaQuote,
    need_rows: usize,
    indicator_cache: &[IndsCache],
) -> Result<RowData, String> {
    let end_date = resolve_runtime_trade_date(row, quote)?;

    let mut row_data = match reader.load_one_tail_rows(
        &row.ts_code,
        DEFAULT_ADJ_TYPE,
        &end_date,
        need_rows.max(1),
    ) {
        Ok(history) => {
            let mut normalized = normalize_runtime_row_data(history)?;
            merge_realtime_quote_into_row_data(&mut normalized, quote, &end_date)?;
            normalized
        }
        Err(err) if err.contains("trade_dates为空") => {
            build_quote_only_runtime_row_data(quote, &end_date)?
        }
        Err(err) => return Err(format!("读取 runtime 历史K线失败: {err}")),
    };

    attach_runtime_extra_series(&mut row_data, row)?;
    let start_date = row_data
        .trade_dates
        .first()
        .cloned()
        .ok_or_else(|| "runtime trade_dates 为空".to_string())?;
    let end_date = row_data
        .trade_dates
        .last()
        .cloned()
        .ok_or_else(|| "runtime trade_dates 为空".to_string())?;
    let rank_series_map = load_runtime_rank_series_map(result_conn, row, &start_date, &end_date)?;
    inject_runtime_rank_series(&mut row_data, &rank_series_map)?;

    if !indicator_cache.is_empty() {
        for (name, series) in calc_inds_with_cache_lossy(indicator_cache, row_data.clone()) {
            row_data.cols.insert(name, series);
        }
    }

    row_data.validate()?;
    Ok(row_data)
}

fn set_template_tag(row: &mut IntradayMonitorRow, text: impl Into<String>, tone: &str) {
    row.template_tag_text = Some(text.into());
    row.template_tag_tone = Some(tone.to_string());
}

fn build_template_warning_message(rows: &[IntradayMonitorRow]) -> Option<String> {
    let failed_rows = rows
        .iter()
        .filter(|row| row.template_tag_tone.as_deref() == Some("down"))
        .filter_map(|row| {
            row.template_tag_text
                .as_ref()
                .map(|text| text.trim())
                .filter(|text| !text.is_empty())
                .map(|text| format!("{}: {}", row.ts_code, text))
        })
        .collect::<Vec<_>>();

    match failed_rows.as_slice() {
        [] => None,
        [first] => Some(format!("模板计算异常: {first}")),
        [first, ..] => Some(format!(
            "模板计算异常共 {} 条，首条: {}",
            failed_rows.len(),
            first
        )),
    }
}

fn apply_intraday_template_tags(
    source_path: &str,
    rows: &mut [IntradayMonitorRow],
    quote_map: &HashMap<String, SinaQuote>,
    templates: &[IntradayMonitorTemplate],
    rank_mode_configs: &[IntradayMonitorRankModeConfig],
) -> Option<String> {
    for row in rows.iter_mut() {
        row.template_tag_text = None;
        row.template_tag_tone = None;
    }

    if templates.is_empty() || rank_mode_configs.is_empty() {
        return None;
    }

    let compiled_templates = compile_intraday_templates(templates);
    let template_warmup_need = compiled_templates
        .values()
        .filter_map(|item| match item {
            CompiledIntradayMonitorTemplate::Ready(template) => Some(template.warmup_need),
            CompiledIntradayMonitorTemplate::Invalid { .. } => None,
        })
        .max()
        .unwrap_or(0);
    let indicator_cache = cache_ind_build(source_path).unwrap_or_default();
    let indicator_warmup_need = if indicator_cache.is_empty() {
        0
    } else {
        warmup_ind_estimate(source_path).unwrap_or(0)
    };
    let need_rows = template_warmup_need.max(indicator_warmup_need).max(1);

    let reader = match DataReader::new(source_path) {
        Ok(reader) => reader,
        Err(err) => {
            for row in rows.iter_mut() {
                if resolve_template_id_for_row(row, rank_mode_configs).is_some() {
                    set_template_tag(row, format!("runtime 初始化失败: {err}"), "down");
                }
            }
            return build_template_warning_message(rows);
        }
    };
    let result_conn = match open_result_conn(source_path) {
        Ok(conn) => conn,
        Err(err) => {
            for row in rows.iter_mut() {
                if resolve_template_id_for_row(row, rank_mode_configs).is_some() {
                    set_template_tag(row, format!("runtime 初始化失败: {err}"), "down");
                }
            }
            return build_template_warning_message(rows);
        }
    };

    for row in rows.iter_mut() {
        let Some(template_id) = resolve_template_id_for_row(row, rank_mode_configs) else {
            continue;
        };
        let Some(compiled_template) = compiled_templates.get(template_id) else {
            set_template_tag(row, "模板缺失", "down");
            continue;
        };
        let Some(quote) = quote_map.get(&row.ts_code) else {
            let template_name = match compiled_template {
                CompiledIntradayMonitorTemplate::Ready(template) => template.name.as_str(),
                CompiledIntradayMonitorTemplate::Invalid { name, .. } => name.as_str(),
            };
            set_template_tag(row, format!("{template_name} · 无实时"), "neutral");
            continue;
        };

        match compiled_template {
            CompiledIntradayMonitorTemplate::Invalid { name, message } => {
                set_template_tag(row, format!("{name} · {message}"), "down");
            }
            CompiledIntradayMonitorTemplate::Ready(template) => {
                let result = (|| -> Result<bool, String> {
                    let row_data = build_intraday_runtime_row_data(
                        &reader,
                        &result_conn,
                        row,
                        quote,
                        need_rows,
                        &indicator_cache,
                    )?;
                    let mut rt = row_into_rt(row_data)?;
                    let value = rt
                        .eval_program(&template.ast)
                        .map_err(|e| format!("表达式计算错误:{}", e.msg))?;
                    let len = rt_max_len(&rt);
                    let series = Value::as_bool_series(&value, len)
                        .map_err(|e| format!("表达式返回值非布尔:{}", e.msg))?;
                    Ok(series.last().copied().unwrap_or(false))
                })();

                match result {
                    Ok(true) => set_template_tag(row, format!("{} · 命中", template.name), "up"),
                    Ok(false) => {
                        set_template_tag(row, format!("{} · 未命中", template.name), "neutral")
                    }
                    Err(err) => {
                        set_template_tag(row, format!("{} · {}", template.name, err), "down")
                    }
                }
            }
        }
    }

    build_template_warning_message(rows)
}

fn build_quote_from_row(row: &IntradayMonitorRow) -> Option<SinaQuote> {
    let price = row.realtime_price?;
    let open = row.realtime_open?;
    let high = row.realtime_high?;
    let low = row.realtime_low?;
    let pre_close = row.realtime_pre_close?;
    let vol = row.realtime_vol?;
    let amount = row.realtime_amount?;

    Some(SinaQuote {
        date: row.trade_date.clone().unwrap_or_default(),
        time: "00:00:00".to_string(),
        ts_code: row.ts_code.clone(),
        name: row.name.clone(),
        open,
        high,
        low,
        pre_close,
        price,
        vol,
        amount,
        change_pct: row.realtime_change_pct,
    })
}

pub fn refresh_intraday_monitor_realtime(
    source_path: &str,
    rows: Vec<IntradayMonitorRow>,
    templates: Vec<IntradayMonitorTemplate>,
    rank_mode_configs: Vec<IntradayMonitorRankModeConfig>,
) -> Result<IntradayMonitorPageData, String> {
    if rows.is_empty() {
        return Ok(IntradayMonitorPageData {
            rows,
            rank_date_options: None,
            resolved_rank_date: None,
            scene_options: None,
            refreshed_at: None,
            warning_message: None,
        });
    }

    let ts_codes = rows
        .iter()
        .map(|item| item.ts_code.clone())
        .collect::<Vec<_>>();
    let latest_vol_map = build_latest_vol_map(source_path, &ts_codes).unwrap_or_default();
    let (quote_map, fetch_meta) = fetch_realtime_quote_map(&ts_codes)?;

    let mut next_rows = rows;
    for row in &mut next_rows {
        if let Some(quote) = quote_map.get(&row.ts_code) {
            row.realtime_price = Some(quote.price);
            row.realtime_open = Some(quote.open);
            row.realtime_high = Some(quote.high);
            row.realtime_low = Some(quote.low);
            row.realtime_pre_close = Some(quote.pre_close);
            row.realtime_vol = Some(quote.vol);
            row.realtime_amount = Some(quote.amount);
            row.realtime_change_pct = quote.change_pct;
            row.realtime_change_open_pct = if quote.open > 0.0 {
                Some((quote.price / quote.open - 1.0) * 100.0)
            } else {
                None
            };
            row.realtime_fall_from_high_pct = if quote.high > 0.0 {
                Some(((quote.high - quote.price) / quote.high).max(0.0) * 100.0)
            } else {
                None
            };
            row.realtime_vol_ratio = latest_vol_map
                .get(&row.ts_code)
                .copied()
                .filter(|value| *value > 0.0)
                .map(|value| quote.vol / value);
        } else {
            row.realtime_price = None;
            row.realtime_open = None;
            row.realtime_high = None;
            row.realtime_low = None;
            row.realtime_pre_close = None;
            row.realtime_vol = None;
            row.realtime_amount = None;
            row.realtime_change_pct = None;
            row.realtime_change_open_pct = None;
            row.realtime_fall_from_high_pct = None;
            row.realtime_vol_ratio = None;
        }
    }

    let warning_message = apply_intraday_template_tags(
        source_path,
        &mut next_rows,
        &quote_map,
        &templates,
        &rank_mode_configs,
    );

    Ok(IntradayMonitorPageData {
        rows: next_rows,
        rank_date_options: None,
        resolved_rank_date: None,
        scene_options: None,
        refreshed_at: fetch_meta.refreshed_at,
        warning_message,
    })
}

pub fn refresh_intraday_monitor_template_tags(
    source_path: &str,
    rows: Vec<IntradayMonitorRow>,
    templates: Vec<IntradayMonitorTemplate>,
    rank_mode_configs: Vec<IntradayMonitorRankModeConfig>,
) -> Result<IntradayMonitorPageData, String> {
    if rows.is_empty() {
        return Ok(IntradayMonitorPageData {
            rows,
            rank_date_options: None,
            resolved_rank_date: None,
            scene_options: None,
            refreshed_at: None,
            warning_message: None,
        });
    }

    let mut next_rows = rows;
    let quote_map = next_rows
        .iter()
        .filter_map(build_quote_from_row)
        .map(|quote| (quote.ts_code.clone(), quote))
        .collect::<HashMap<_, _>>();

    let warning_message = apply_intraday_template_tags(
        source_path,
        &mut next_rows,
        &quote_map,
        &templates,
        &rank_mode_configs,
    );

    Ok(IntradayMonitorPageData {
        rows: next_rows,
        rank_date_options: None,
        resolved_rank_date: None,
        scene_options: None,
        refreshed_at: None,
        warning_message,
    })
}

pub fn get_intraday_monitor_page(
    source_path: &str,
    rank_mode: Option<String>,
    rank_date: Option<String>,
    scene_name: Option<String>,
    limit: Option<u32>,
    board: Option<String>,
    exclude_st_board: Option<bool>,
    total_mv_min: Option<f64>,
    total_mv_max: Option<f64>,
) -> Result<IntradayMonitorPageData, String> {
    if let (Some(min_v), Some(max_v)) = (total_mv_min, total_mv_max) {
        if min_v > max_v {
            return Err("总市值最小值不能大于最大值".to_string());
        }
    }

    let conn = open_result_conn(source_path)?;
    let rank_mode = IntradayRankMode::parse(rank_mode.as_deref())?;
    let effective_rank_date = resolve_trade_date(&conn, rank_date, rank_mode)?;
    let rank_date_options = query_rank_trade_date_options_from_conn(&conn, rank_mode)?;
    let scene_options = if rank_mode == IntradayRankMode::Scene {
        Some(query_scene_options_from_conn(&conn)?)
    } else {
        None
    };
    let scene_filter = if rank_mode == IntradayRankMode::Scene {
        scene_name
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
    } else {
        None
    };
    if let (IntradayRankMode::Scene, Some(wanted_scene_name), Some(scene_names)) =
        (rank_mode, scene_filter.as_ref(), scene_options.as_ref())
    {
        if !scene_names.iter().any(|item| item == wanted_scene_name) {
            return Err(format!("场景不存在: {wanted_scene_name}"));
        }
    }

    let name_map = build_name_map(source_path)?;
    let total_mv_map = build_total_mv_map(source_path)?;
    let concepts_map = build_concepts_map(source_path)?;

    let board_filter = board
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty() && value != "全部");
    let exclude_st_board = exclude_st_board.unwrap_or(false);

    let limit = limit.filter(|value| *value > 0).map(|value| value as usize);
    let mut base_rows = Vec::new();

    match rank_mode {
        IntradayRankMode::Total => {
            let mut stmt = conn
                .prepare(
                    r#"
                    SELECT
                        ts_code,
                        trade_date,
                        total_score,
                        rank
                    FROM score_summary
                    WHERE trade_date = ?
                    ORDER BY
                        COALESCE(rank, 999999) ASC,
                        total_score DESC,
                        ts_code ASC
                    "#,
                )
                .map_err(|e| format!("预编译总榜查询失败: {e}"))?;
            let mut db_rows = stmt
                .query(params![effective_rank_date])
                .map_err(|e| format!("查询总榜失败: {e}"))?;

            while let Some(row) = db_rows.next().map_err(|e| format!("读取总榜失败: {e}"))? {
                let ts_code: String = row.get(0).map_err(|e| format!("读取 ts_code 失败: {e}"))?;
                let board_value =
                    board_category(&ts_code, name_map.get(&ts_code).map(|value| value.as_str()))
                        .to_string();

                if exclude_st_board && board_value == BOARD_ST {
                    continue;
                }

                if let Some(ref board_value_filter) = board_filter {
                    if &board_value != board_value_filter {
                        continue;
                    }
                }
                if !filter_mv(&total_mv_map, &ts_code, total_mv_min, total_mv_max) {
                    continue;
                }

                base_rows.push(IntradayMonitorRow {
                    rank_mode: rank_mode.as_str().to_string(),
                    ts_code: ts_code.clone(),
                    trade_date: Some(
                        row.get(1)
                            .map_err(|e| format!("读取 trade_date 失败: {e}"))?,
                    ),
                    scene_name: "总榜".to_string(),
                    direction: None,
                    total_score: row
                        .get(2)
                        .map_err(|e| format!("读取 total_score 失败: {e}"))?,
                    scene_score: None,
                    risk_score: None,
                    confirm_strength: None,
                    risk_intensity: None,
                    scene_status: None,
                    rank: row.get(3).map_err(|e| format!("读取 rank 失败: {e}"))?,
                    name: name_map.get(&ts_code).cloned().unwrap_or_default(),
                    board: board_value,
                    total_mv_yi: total_mv_map.get(&ts_code).copied(),
                    concept: concepts_map.get(&ts_code).cloned().unwrap_or_default(),
                    realtime_price: None,
                    realtime_open: None,
                    realtime_high: None,
                    realtime_low: None,
                    realtime_pre_close: None,
                    realtime_vol: None,
                    realtime_amount: None,
                    realtime_change_pct: None,
                    realtime_change_open_pct: None,
                    realtime_fall_from_high_pct: None,
                    realtime_vol_ratio: None,
                    template_tag_text: None,
                    template_tag_tone: None,
                });

                if let Some(limit_value) = limit {
                    if base_rows.len() >= limit_value {
                        break;
                    }
                }
            }
        }
        IntradayRankMode::Scene => {
            let mut per_scene_count = std::collections::HashMap::<String, usize>::new();
            let (sql, query_scene_name): (&str, Option<String>) = if scene_filter.is_some() {
                (
                    r#"
                    SELECT
                        d.ts_code,
                        d.trade_date,
                        d.scene_name,
                        d.direction,
                        d.stage_score,
                        d.risk_score,
                        d.confirm_strength,
                        d.risk_intensity,
                        d.stage,
                        d.scene_rank,
                        s.total_score
                    FROM scene_details AS d
                    LEFT JOIN score_summary AS s
                      ON d.ts_code = s.ts_code
                     AND d.trade_date = s.trade_date
                    WHERE d.trade_date = ?
                      AND d.scene_name = ?
                    ORDER BY
                        COALESCE(d.scene_rank, 999999) ASC,
                        COALESCE(d.confirm_strength, 0.0) DESC,
                        COALESCE(s.total_score, -1e18) DESC,
                        d.ts_code ASC
                    "#,
                    scene_filter.clone(),
                )
            } else {
                (
                    r#"
                    SELECT
                        d.ts_code,
                        d.trade_date,
                        d.scene_name,
                        d.direction,
                        d.stage_score,
                        d.risk_score,
                        d.confirm_strength,
                        d.risk_intensity,
                        d.stage,
                        d.scene_rank,
                        s.total_score
                    FROM scene_details AS d
                    LEFT JOIN score_summary AS s
                      ON d.ts_code = s.ts_code
                     AND d.trade_date = s.trade_date
                    WHERE d.trade_date = ?
                    ORDER BY
                        COALESCE(d.scene_rank, 999999) ASC,
                        d.scene_name ASC,
                        COALESCE(d.confirm_strength, 0.0) DESC,
                        COALESCE(s.total_score, -1e18) DESC,
                        d.ts_code ASC
                    "#,
                    None,
                )
            };

            let mut stmt = conn
                .prepare(sql)
                .map_err(|e| format!("预编译场景榜查询失败: {e}"))?;
            let mut db_rows = if let Some(scene_name) = query_scene_name {
                stmt.query(params![effective_rank_date, scene_name])
            } else {
                stmt.query(params![effective_rank_date])
            }
            .map_err(|e| format!("查询场景榜失败: {e}"))?;

            while let Some(row) = db_rows.next().map_err(|e| format!("读取场景榜失败: {e}"))?
            {
                let ts_code: String = row.get(0).map_err(|e| format!("读取 ts_code 失败: {e}"))?;
                let this_scene_name: String = row
                    .get(2)
                    .map_err(|e| format!("读取 scene_name 失败: {e}"))?;

                let board_value =
                    board_category(&ts_code, name_map.get(&ts_code).map(|value| value.as_str()))
                        .to_string();

                if exclude_st_board && board_value == BOARD_ST {
                    continue;
                }

                if let Some(ref board_value_filter) = board_filter {
                    if &board_value != board_value_filter {
                        continue;
                    }
                }

                if !filter_mv(&total_mv_map, &ts_code, total_mv_min, total_mv_max) {
                    continue;
                }

                base_rows.push(IntradayMonitorRow {
                    rank_mode: rank_mode.as_str().to_string(),
                    ts_code: ts_code.clone(),
                    trade_date: Some(
                        row.get(1)
                            .map_err(|e| format!("读取 trade_date 失败: {e}"))?,
                    ),
                    scene_name: this_scene_name.clone(),
                    direction: row
                        .get(3)
                        .map_err(|e| format!("读取 direction 失败: {e}"))?,
                    total_score: row
                        .get(10)
                        .map_err(|e| format!("读取 total_score 失败: {e}"))?,
                    scene_score: row
                        .get(4)
                        .map_err(|e| format!("读取 scene_score 失败: {e}"))?,
                    risk_score: row
                        .get(5)
                        .map_err(|e| format!("读取 risk_score 失败: {e}"))?,
                    confirm_strength: row
                        .get(6)
                        .map_err(|e| format!("读取 confirm_strength 失败: {e}"))?,
                    risk_intensity: row
                        .get(7)
                        .map_err(|e| format!("读取 risk_intensity 失败: {e}"))?,
                    scene_status: row
                        .get::<_, Option<String>>(8)
                        .map_err(|e| format!("读取 scene_status 失败: {e}"))?,
                    rank: row.get(9).map_err(|e| format!("读取 rank 失败: {e}"))?,
                    name: name_map.get(&ts_code).cloned().unwrap_or_default(),
                    board: board_value,
                    total_mv_yi: total_mv_map.get(&ts_code).copied(),
                    concept: concepts_map.get(&ts_code).cloned().unwrap_or_default(),
                    realtime_price: None,
                    realtime_open: None,
                    realtime_high: None,
                    realtime_low: None,
                    realtime_pre_close: None,
                    realtime_vol: None,
                    realtime_amount: None,
                    realtime_change_pct: None,
                    realtime_change_open_pct: None,
                    realtime_fall_from_high_pct: None,
                    realtime_vol_ratio: None,
                    template_tag_text: None,
                    template_tag_tone: None,
                });

                if let Some(limit_value) = limit {
                    if scene_filter.is_some() {
                        if base_rows.len() >= limit_value {
                            break;
                        }
                    } else {
                        let next_count =
                            per_scene_count.get(&this_scene_name).copied().unwrap_or(0);
                        if next_count >= limit_value {
                            let _ = base_rows.pop();
                            continue;
                        }
                        per_scene_count.insert(this_scene_name, next_count + 1);
                    }
                }
            }
        }
    }

    Ok(IntradayMonitorPageData {
        rows: base_rows,
        rank_date_options: Some(rank_date_options),
        resolved_rank_date: Some(effective_rank_date),
        scene_options,
        refreshed_at: None,
        warning_message: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_quote() -> SinaQuote {
        SinaQuote {
            date: "".to_string(),
            time: "09:35:00".to_string(),
            ts_code: "000001.SZ".to_string(),
            name: "平安银行".to_string(),
            open: 10.0,
            high: 10.5,
            low: 9.8,
            pre_close: 9.9,
            price: 10.2,
            vol: 1234.0,
            amount: 5678.0,
            change_pct: Some(3.03),
        }
    }

    fn sample_row_data() -> RowData {
        let mut cols = HashMap::new();
        for key in RUNTIME_INPUT_KEYS {
            cols.insert(key.to_string(), vec![Some(1.0)]);
        }
        RowData {
            trade_dates: vec!["20240401".to_string()],
            cols,
        }
    }

    #[test]
    fn resolve_runtime_trade_date_requires_quote_date() {
        let row = IntradayMonitorRow {
            rank_mode: "total".to_string(),
            ts_code: "000001.SZ".to_string(),
            trade_date: Some("20240401".to_string()),
            scene_name: String::new(),
            direction: None,
            total_score: None,
            scene_score: None,
            risk_score: None,
            confirm_strength: None,
            risk_intensity: None,
            scene_status: None,
            rank: None,
            name: Some("平安银行".to_string()),
            board: BOARD_MAIN.to_string(),
            total_mv_yi: None,
            concept: None,
            realtime_price: None,
            realtime_open: None,
            realtime_high: None,
            realtime_low: None,
            realtime_pre_close: None,
            realtime_vol: None,
            realtime_amount: None,
            realtime_change_pct: None,
            realtime_change_open_pct: None,
            realtime_fall_from_high_pct: None,
            realtime_vol_ratio: None,
            template_tag_text: None,
            template_tag_tone: None,
        };
        let error = resolve_runtime_trade_date(&row, &sample_quote())
            .expect_err("missing quote date should fail");

        assert!(error.contains("实时行情缺少可用日期"));
        assert!(error.contains("避免把最新行情静默写入旧交易日"));
    }

    #[test]
    fn build_quote_only_runtime_row_data_accepts_explicit_trade_date() {
        let row_data = build_quote_only_runtime_row_data(&sample_quote(), "20240401")
            .expect("quote-only runtime row data");

        assert_eq!(row_data.trade_dates, vec!["20240401".to_string()]);
        assert_eq!(
            row_data.cols.get("C").and_then(|series| series[0]),
            Some(10.2)
        );
    }

    #[test]
    fn merge_realtime_quote_into_row_data_accepts_explicit_trade_date() {
        let mut row_data = sample_row_data();

        merge_realtime_quote_into_row_data(&mut row_data, &sample_quote(), "20240401")
            .expect("merge realtime row");

        assert_eq!(row_data.trade_dates, vec!["20240401".to_string()]);
        assert_eq!(
            row_data.cols.get("C").and_then(|series| series[0]),
            Some(10.2)
        );
        assert_eq!(
            row_data.cols.get("PRE_CLOSE").and_then(|series| series[0]),
            Some(9.9)
        );
    }
}
