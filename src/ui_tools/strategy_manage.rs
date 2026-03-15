use serde::{Deserialize, Serialize};
use std::fs;

use crate::{
    data::{DataReader, RowData, score_rule_path},
    data::scoring_data::row_into_rt,
    expr::{
        eval::Value,
        parser::{Expr, Parser, Stmt, Stmts, lex_all},
    },
    scoring::tools::{calc_zhang_pct, load_st_list, rt_max_len},
    utils::utils::eval_binary_for_warmup,
    utils::utils::impl_expr_warmup,
};

const DEFAULT_ADJ_TYPE: &str = "qfq";

#[derive(Debug, Serialize)]
pub struct StrategyManageRuleItem {
    pub index: usize,
    pub name: String,
    pub scope_way: String,
    pub scope_windows: usize,
    pub points: f64,
    pub explain: String,
    pub when: String,
    pub tag: String,
    pub dist_points: Option<Vec<StrategyManageDistPoint>>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StrategyManageDistPoint {
    pub min: usize,
    pub max: usize,
    pub points: f64,
}

#[derive(Debug, Serialize)]
pub struct StrategyManagePageData {
    pub rules: Vec<StrategyManageRuleItem>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StrategyManageRuleDraft {
    pub name: String,
    pub scope_way: String,
    pub scope_windows: usize,
    pub when: String,
    pub points: f64,
    pub dist_points: Option<Vec<StrategyManageDistPoint>>,
    pub explain: String,
    pub tag: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct StrategyRuleFile {
    version: u32,
    rule: Vec<StrategyRuleFileRule>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct StrategyRuleFileRule {
    name: String,
    scope_windows: usize,
    scope_way: String,
    when: String,
    points: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    dist_points: Option<Vec<StrategyRuleFileDistPoint>>,
    explain: String,
    #[serde(default, skip_serializing_if = "is_normal_tag")]
    tag: StrategyRuleFileTag,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct StrategyRuleFileDistPoint {
    min: usize,
    max: usize,
    points: f64,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
enum StrategyRuleFileTag {
    #[default]
    Normal,
    Opportunity,
}

#[derive(Debug, Clone, Copy)]
enum StrategyScopeWay {
    Any,
    Last,
    Each,
    Recent,
    Consec(usize),
}

fn is_normal_tag(tag: &StrategyRuleFileTag) -> bool {
    matches!(tag, StrategyRuleFileTag::Normal)
}

fn load_rule_file(source_path: &str) -> Result<StrategyRuleFile, String> {
    let path = score_rule_path(source_path);
    let text = fs::read_to_string(&path)
        .map_err(|e| format!("读取策略规则文件失败: path={}, err={e}", path.display()))?;
    toml::from_str(&text).map_err(|e| format!("解析策略规则文件失败: {e}"))
}

fn parse_scope_way(scope_way: &str) -> Result<StrategyScopeWay, String> {
    match scope_way.trim().to_ascii_uppercase().as_str() {
        "ANY" => Ok(StrategyScopeWay::Any),
        "LAST" => Ok(StrategyScopeWay::Last),
        "EACH" => Ok(StrategyScopeWay::Each),
        "RECENT" => Ok(StrategyScopeWay::Recent),
        value => {
            let Some(num) = value.strip_prefix("CONSEC>=") else {
                return Err(format!("scope_way 不支持: {scope_way}"));
            };
            let threshold = num
                .parse::<usize>()
                .map_err(|_| format!("scope_way 连续阈值非法: {scope_way}"))?;
            if threshold == 0 {
                return Err("scope_way 连续阈值必须 >= 1".to_string());
            }
            Ok(StrategyScopeWay::Consec(threshold))
        }
    }
}

fn parse_tag(tag: &str) -> Result<StrategyRuleFileTag, String> {
    match tag.trim().to_ascii_lowercase().as_str() {
        "" | "normal" => Ok(StrategyRuleFileTag::Normal),
        "opportunity" => Ok(StrategyRuleFileTag::Opportunity),
        other => Err(format!("tag 不支持: {other}")),
    }
}

fn estimate_rule_warmup(stmts: &Stmts, scope_way: StrategyScopeWay, scope_windows: usize) -> Result<usize, String> {
    let mut locals = std::collections::HashMap::new();
    let mut consts: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    let mut expr_need = 0usize;

    for stmt in stmts.item.clone() {
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
                        let need =
                            impl_expr_warmup(Expr::Binary { op, lhs, rhs }, &locals, &consts)?;
                        locals.insert(name, need);
                    }
                }
                other => {
                    let need = impl_expr_warmup(other, &locals, &consts)?;
                    locals.insert(name, need);
                }
            },
            Stmt::Expr(expr) => {
                expr_need = expr_need.max(impl_expr_warmup(expr, &locals, &consts)?);
            }
        }
    }

    let scope_extra = match scope_way {
        StrategyScopeWay::Last => 0,
        StrategyScopeWay::Any | StrategyScopeWay::Each | StrategyScopeWay::Recent => {
            scope_windows.saturating_sub(1)
        }
        StrategyScopeWay::Consec(threshold) => scope_windows
            .saturating_sub(1)
            .max(threshold.saturating_sub(1)),
    };

    Ok(expr_need + scope_extra)
}

fn fill_validation_extra_fields(
    row_data: &mut RowData,
    ts_code: &str,
    is_st: bool,
) -> Result<(), String> {
    let zhang = calc_zhang_pct(ts_code, is_st);
    let zhang_series = vec![Some(zhang); row_data.trade_dates.len()];
    row_data.cols.insert("ZHANG".to_string(), zhang_series);
    row_data.validate()
}

fn validate_rule_definition(
    source_path: &str,
    reader: Option<&DataReader>,
    sample_ts_code: Option<&str>,
    latest_trade_date: Option<&str>,
    st_list: Option<&std::collections::HashSet<String>>,
    rule: &StrategyRuleFileRule,
) -> Result<(), String> {
    if rule.name.trim().is_empty() {
        return Err("策略名不能为空".to_string());
    }
    if rule.when.trim().is_empty() {
        return Err(format!("策略 {} 的表达式不能为空", rule.name));
    }
    if rule.explain.trim().is_empty() {
        return Err(format!("策略 {} 的说明不能为空", rule.name));
    }
    if rule.scope_windows == 0 {
        return Err(format!("策略 {} 的 scope_windows 必须 >= 1", rule.name));
    }
    if !rule.points.is_finite() {
        return Err(format!("策略 {} 的 points 非法", rule.name));
    }
    if let Some(dist_points) = &rule.dist_points {
        for (index, item) in dist_points.iter().enumerate() {
            if item.min > item.max {
                return Err(format!("策略 {} 的 dist_points 第{}段 min > max", rule.name, index + 1));
            }
            if !item.points.is_finite() {
                return Err(format!("策略 {} 的 dist_points 第{}段 points 非法", rule.name, index + 1));
            }
        }
    }

    let scope_way = parse_scope_way(&rule.scope_way)?;
    let tokens = lex_all(&rule.when);
    let mut parser = Parser::new(tokens);
    let stmts = parser
        .parse_main()
        .map_err(|e| format!("策略 {} 表达式解析错误在{}:{}", rule.name, e.idx, e.msg))?;

    if let (Some(reader), Some(sample_ts_code), Some(latest_trade_date), Some(st_list)) =
        (reader, sample_ts_code, latest_trade_date, st_list)
    {
        let warmup_need = estimate_rule_warmup(&stmts, scope_way, rule.scope_windows)?;
        let need_rows = (warmup_need + rule.scope_windows).max(1);
        let mut row_data =
            reader.load_one_tail_rows(sample_ts_code, DEFAULT_ADJ_TYPE, latest_trade_date, need_rows)?;
        fill_validation_extra_fields(&mut row_data, sample_ts_code, st_list.contains(sample_ts_code))?;
        let mut runtime = row_into_rt(row_data)?;
        let value = runtime
            .eval_program(&stmts)
            .map_err(|e| format!("策略 {} 表达式不支持: {}", rule.name, e.msg))?;
        let len = rt_max_len(&runtime);
        let is_num_series = Value::as_num_series(&value, len).is_ok();
        let is_bool_series = Value::as_bool_series(&value, len).is_ok();
        if !is_num_series && !is_bool_series {
            return Err(format!("策略 {} 表达式返回值非法", rule.name));
        }
    } else {
        let _ = source_path;
    }

    Ok(())
}

fn validate_rule_file(source_path: &str, rule_file: &StrategyRuleFile) -> Result<(), String> {
    let reader = DataReader::new(source_path).ok();
    let latest_trade_date = reader.as_ref().and_then(|reader| {
        let mut stmt = reader
            .conn
            .prepare(
                r#"
                SELECT MAX(trade_date)
                FROM stock_data
                WHERE adj_type = ?
                "#,
            )
            .ok()?;
        let mut rows = stmt.query([DEFAULT_ADJ_TYPE]).ok()?;
        let row = rows.next().ok()??;
        row.get::<_, Option<String>>(0).ok().flatten()
    });
    let sample_ts_code = if let (Some(reader), Some(latest_trade_date)) = (&reader, &latest_trade_date) {
        DataReader::list_ts_code(reader, DEFAULT_ADJ_TYPE, latest_trade_date, latest_trade_date)
            .ok()
            .and_then(|values| values.into_iter().next())
    } else {
        None
    };
    let st_list = if sample_ts_code.is_some() {
        load_st_list(source_path).ok()
    } else {
        None
    };

    for rule in &rule_file.rule {
        validate_rule_definition(
            source_path,
            reader.as_ref(),
            sample_ts_code.as_deref(),
            latest_trade_date.as_deref(),
            st_list.as_ref(),
            rule,
        )?;
    }

    Ok(())
}

fn build_rule_file_rule(draft: StrategyManageRuleDraft) -> Result<StrategyRuleFileRule, String> {
    Ok(StrategyRuleFileRule {
        name: draft.name.trim().to_string(),
        scope_windows: draft.scope_windows,
        scope_way: draft.scope_way.trim().to_ascii_uppercase(),
        when: draft.when.trim().to_string(),
        points: draft.points,
        dist_points: draft.dist_points.map(|items| {
            items.into_iter()
                .map(|item| StrategyRuleFileDistPoint {
                    min: item.min,
                    max: item.max,
                    points: item.points,
                })
                .collect()
        }),
        explain: draft.explain.trim().to_string(),
        tag: parse_tag(&draft.tag)?,
    })
}

fn write_rule_file(source_path: &str, rule_file: &StrategyRuleFile) -> Result<(), String> {
    let path = score_rule_path(source_path);
    let text =
        toml::to_string_pretty(rule_file).map_err(|e| format!("序列化策略规则文件失败: {e}"))?;
    fs::write(&path, text)
        .map_err(|e| format!("写入策略规则文件失败: path={}, err={e}", path.display()))
}

fn build_rule_item(index: usize, rule: StrategyRuleFileRule) -> StrategyManageRuleItem {
    StrategyManageRuleItem {
        index,
        name: rule.name,
        scope_way: rule.scope_way,
        scope_windows: rule.scope_windows,
        points: rule.points,
        explain: rule.explain,
        when: rule.when,
        tag: match rule.tag {
            StrategyRuleFileTag::Normal => "Normal".to_string(),
            StrategyRuleFileTag::Opportunity => "Opportunity".to_string(),
        },
        dist_points: rule.dist_points.map(|items| {
            items.into_iter()
                .map(|item| StrategyManageDistPoint {
                    min: item.min,
                    max: item.max,
                    points: item.points,
                })
                .collect()
        }),
    }
}

fn next_strategy_name(existing_names: &[String]) -> String {
    let base = "新策略";
    if !existing_names.iter().any(|item| item == base) {
        return base.to_string();
    }

    for index in 1..=999 {
        let candidate = format!("{base}{index:02}");
        if !existing_names.iter().any(|item| item == &candidate) {
            return candidate;
        }
    }

    format!("{base}{}", existing_names.len() + 1)
}

pub fn get_strategy_manage_page(source_path: &str) -> Result<StrategyManagePageData, String> {
    let rule_file = load_rule_file(source_path)?;
    let rules = rule_file
        .rule
        .into_iter()
        .enumerate()
        .map(|(index, rule)| build_rule_item(index + 1, rule))
        .collect();

    Ok(StrategyManagePageData { rules })
}

pub fn add_strategy_manage_rule(source_path: &str) -> Result<StrategyManagePageData, String> {
    let mut rule_file = load_rule_file(source_path)?;
    let existing_names: Vec<String> = rule_file.rule.iter().map(|item| item.name.clone()).collect();
    let next_name = next_strategy_name(&existing_names);

    rule_file.rule.push(StrategyRuleFileRule {
        name: next_name,
        scope_windows: 1,
        scope_way: "LAST".to_string(),
        when: "C > O".to_string(),
        points: 0.0,
        dist_points: None,
        explain: "待补充".to_string(),
        tag: StrategyRuleFileTag::Normal,
    });

    validate_rule_file(source_path, &rule_file)?;
    write_rule_file(source_path, &rule_file)?;
    get_strategy_manage_page(source_path)
}

pub fn remove_strategy_manage_rules(
    source_path: &str,
    names: &[String],
) -> Result<StrategyManagePageData, String> {
    let mut rule_file = load_rule_file(source_path)?;
    rule_file.rule.retain(|item| !names.iter().any(|name| name == &item.name));
    validate_rule_file(source_path, &rule_file)?;
    write_rule_file(source_path, &rule_file)?;
    get_strategy_manage_page(source_path)
}

pub fn update_strategy_manage_rule(
    source_path: &str,
    original_name: &str,
    draft: StrategyManageRuleDraft,
) -> Result<StrategyManagePageData, String> {
    let mut rule_file = load_rule_file(source_path)?;
    let target_name = original_name.trim();
    let Some(target_index) = rule_file.rule.iter().position(|item| item.name == target_name) else {
        return Err(format!("未找到待修改策略: {target_name}"));
    };

    let next_rule = build_rule_file_rule(draft)?;
    if rule_file
        .rule
        .iter()
        .enumerate()
        .any(|(index, item)| index != target_index && item.name == next_rule.name)
    {
        return Err(format!("策略名重复: {}", next_rule.name));
    }

    let previous_rule = rule_file.rule[target_index].clone();
    rule_file.rule[target_index] = next_rule;

    if let Err(error) = validate_rule_file(source_path, &rule_file) {
        rule_file.rule[target_index] = previous_rule;
        return Err(format!("保存已回滚: {error}"));
    }

    write_rule_file(source_path, &rule_file)?;
    get_strategy_manage_page(source_path)
}
