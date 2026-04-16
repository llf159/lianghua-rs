use std::{
    collections::{HashMap, HashSet},
    fs,
    path::PathBuf,
};

use serde::{Deserialize, Serialize};

use crate::data::RowData;
use crate::data::scoring_data::row_into_rt;
use crate::expr::eval::Value;
use crate::expr::parser::{Expr, Stmt, Stmts};
use crate::{
    data::{DataReader, RuleStage, SceneDirection, ScoreConfig, score_rule_path},
    expr::parser::{Parser, lex_all},
    scoring::tools::{calc_zhang_pct, load_st_list, rt_max_len},
    utils::utils::{eval_binary_for_warmup, impl_expr_warmup},
};

const DEFAULT_ADJ_TYPE: &str = "qfq";

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StrategyManageDistPoint {
    pub min: usize,
    pub max: usize,
    pub points: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct StrategyManageSceneItem {
    pub index: usize,
    pub name: String,
    pub direction: String,
    pub observe_threshold: f64,
    pub trigger_threshold: f64,
    pub confirm_threshold: f64,
    pub fail_threshold: f64,
    pub rule_count: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StrategyManageSceneDraft {
    pub name: String,
    pub direction: String,
    pub observe_threshold: f64,
    pub trigger_threshold: f64,
    pub confirm_threshold: f64,
    pub fail_threshold: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct StrategyManageRuleItem {
    pub index: usize,
    pub name: String,
    pub scene_name: String,
    pub stage: String,
    pub scope_way: String,
    pub scope_windows: usize,
    pub points: f64,
    pub explain: String,
    pub when: String,
    pub dist_points: Option<Vec<StrategyManageDistPoint>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StrategyManageRuleDraft {
    pub name: String,
    pub scene_name: String,
    pub stage: String,
    pub scope_way: String,
    pub scope_windows: usize,
    pub when: String,
    pub points: f64,
    pub dist_points: Option<Vec<StrategyManageDistPoint>>,
    pub explain: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct StrategyManagePageData {
    pub scenes: Vec<StrategyManageSceneItem>,
    pub rules: Vec<StrategyManageRuleItem>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StrategyManageRefactorDraft {
    pub scenes: Vec<StrategyManageSceneDraft>,
    pub rules: Vec<StrategyManageRuleDraft>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct StrategyRuleFile {
    version: u32,
    scene: Vec<StrategyRuleFileScene>,
    rule: Vec<StrategyRuleFileRule>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct StrategyRuleFileScene {
    name: String,
    direction: SceneDirection,
    observe_threshold: f64,
    trigger_threshold: f64,
    confirm_threshold: f64,
    fail_threshold: f64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct StrategyRuleFileRule {
    name: String,
    #[serde(rename = "scene")]
    scene_name: String,
    stage: RuleStage,
    scope_windows: usize,
    scope_way: String,
    when: String,
    points: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    dist_points: Option<Vec<StrategyManageDistPoint>>,
    explain: String,
}

#[derive(Debug, Clone, Copy)]
enum StrategyScopeWay {
    Any,
    Last,
    Each,
    Recent,
    Consec(usize),
}

fn load_rule_file(source_path: &str) -> Result<StrategyRuleFile, String> {
    let path = score_rule_path(source_path);
    let text = fs::read_to_string(&path)
        .map_err(|e| format!("读取策略规则文件失败: path={}, err={e}", path.display()))?;
    parse_rule_file_text(&text).map_err(|e| format!("解析策略规则文件失败: {e}"))
}

fn parse_rule_file_text(text: &str) -> Result<StrategyRuleFile, toml::de::Error> {
    toml::from_str(text)
}

fn rule_file_output_path(source_path: &str, file_name: &str) -> Result<PathBuf, String> {
    let trimmed = file_name.trim();
    if trimmed.is_empty() {
        return Err("策略文件名不能为空".to_string());
    }
    if !trimmed.ends_with(".toml") {
        return Err("策略文件名必须以 .toml 结尾".to_string());
    }
    if trimmed.contains('/') || trimmed.contains('\\') {
        return Err("策略文件名不能包含路径分隔符".to_string());
    }
    Ok(std::path::Path::new(source_path).join(trimmed))
}

fn save_rule_file(source_path: &str, file: &StrategyRuleFile) -> Result<(), String> {
    let path = score_rule_path(source_path);
    let text = toml::to_string_pretty(file).map_err(|e| format!("序列化策略规则文件失败: {e}"))?;
    fs::write(&path, text)
        .map_err(|e| format!("写入策略规则文件失败: path={}, err={e}", path.display()))
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

fn normalize_scope_way(scope_way: &str) -> Result<String, String> {
    let parsed = parse_scope_way(scope_way)?;
    Ok(match parsed {
        StrategyScopeWay::Any => "ANY".to_string(),
        StrategyScopeWay::Last => "LAST".to_string(),
        StrategyScopeWay::Each => "EACH".to_string(),
        StrategyScopeWay::Recent => "RECENT".to_string(),
        StrategyScopeWay::Consec(n) => format!("CONSEC>={n}"),
    })
}

fn parse_rule_stage(stage: &str) -> Result<RuleStage, String> {
    match stage.trim().to_ascii_lowercase().as_str() {
        "base" => Ok(RuleStage::Base),
        "trigger" => Ok(RuleStage::Trigger),
        "confirm" => Ok(RuleStage::Confirm),
        "risk" => Ok(RuleStage::Risk),
        "fail" => Ok(RuleStage::Fail),
        other => Err(format!("stage 不支持: {other}")),
    }
}

fn parse_scene_direction(direction: &str) -> Result<SceneDirection, String> {
    match direction.trim().to_ascii_lowercase().as_str() {
        "long" => Ok(SceneDirection::Long),
        "short" => Ok(SceneDirection::Short),
        other => Err(format!(
            "scene direction 不支持: {other}，仅支持 long/short"
        )),
    }
}

fn format_rule_stage(stage: RuleStage) -> String {
    match stage {
        RuleStage::Base => "base",
        RuleStage::Trigger => "trigger",
        RuleStage::Confirm => "confirm",
        RuleStage::Risk => "risk",
        RuleStage::Fail => "fail",
    }
    .to_string()
}

fn estimate_rule_warmup(
    stmts: &Stmts,
    scope_way: StrategyScopeWay,
    scope_windows: usize,
) -> Result<usize, String> {
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

fn validate_scene_values(draft: &StrategyManageSceneDraft) -> Result<(), String> {
    let name = draft.name.trim();
    if name.is_empty() {
        return Err("scene 名称不能为空".to_string());
    }
    parse_scene_direction(&draft.direction)?;
    for (label, value) in [
        ("observe_threshold", draft.observe_threshold),
        ("trigger_threshold", draft.trigger_threshold),
        ("confirm_threshold", draft.confirm_threshold),
        ("fail_threshold", draft.fail_threshold),
    ] {
        if !value.is_finite() {
            return Err(format!("{label} 非法"));
        }
    }
    for (label, value) in [
        ("observe_threshold", draft.observe_threshold),
        ("trigger_threshold", draft.trigger_threshold),
        ("confirm_threshold", draft.confirm_threshold),
        ("fail_threshold", draft.fail_threshold),
    ] {
        if value <= 0.0 {
            return Err(format!("{label} 必须 > 0"));
        }
    }
    Ok(())
}

fn validate_scene_draft_basic(
    source_path: &str,
    original_name: Option<&str>,
    draft: &StrategyManageSceneDraft,
) -> Result<String, String> {
    let name = draft.name.trim();
    validate_scene_values(draft)?;

    let config = ScoreConfig::load(source_path)?;
    let original_name = original_name.map(str::trim);
    if config.scene.iter().any(|item| {
        item.name.trim() == name && original_name.is_none_or(|old| old != item.name.trim())
    }) {
        return Err(format!("scene 名称重复: {name}"));
    }

    Ok("scene 草稿检查通过".to_string())
}

fn validate_rule_definition(
    _source_path: &str,
    reader: Option<&DataReader>,
    sample_ts_code: Option<&str>,
    latest_trade_date: Option<&str>,
    st_list: Option<&HashSet<String>>,
    rule: &StrategyRuleFileRule,
    scenes: &[StrategyRuleFileScene],
) -> Result<(), String> {
    if rule.name.trim().is_empty() {
        return Err("策略名不能为空".to_string());
    }
    if !scenes
        .iter()
        .any(|scene| scene.name.trim() == rule.scene_name.trim())
    {
        return Err(format!("规则 {} 引用的 scene 不存在", rule.name));
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
                return Err(format!(
                    "策略 {} 的 dist_points 第{}段 min > max",
                    rule.name,
                    index + 1
                ));
            }
            if !item.points.is_finite() {
                return Err(format!(
                    "策略 {} 的 dist_points 第{}段 points 非法",
                    rule.name,
                    index + 1
                ));
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
        let mut row_data = reader.load_one_tail_rows(
            sample_ts_code,
            DEFAULT_ADJ_TYPE,
            latest_trade_date,
            need_rows,
        )?;
        fill_validation_extra_fields(
            &mut row_data,
            sample_ts_code,
            st_list.contains(sample_ts_code),
        )?;
        let mut rt = row_into_rt(row_data)?;
        let value = rt
            .eval_program(&stmts)
            .map_err(|e| format!("策略 {} 表达式运行错误:{}", rule.name, e.msg))?;
        let len = rt_max_len(&rt);
        Value::as_bool_series(&value, len)
            .map_err(|e| format!("策略 {} 表达式返回值非布尔:{}", rule.name, e.msg))?;
    }

    Ok(())
}

fn map_dist_points(
    values: Option<Vec<StrategyManageDistPoint>>,
) -> Option<Vec<StrategyManageDistPoint>> {
    values.filter(|items| !items.is_empty())
}

fn load_validation_context(
    source_path: &str,
) -> Result<(DataReader, Option<String>, Option<String>, HashSet<String>), String> {
    let reader = DataReader::new(source_path)?;
    let latest_trade_date = reader
        .conn
        .query_row(
            "SELECT MAX(trade_date) FROM stock_data WHERE adj_type = ?",
            [DEFAULT_ADJ_TYPE],
            |row| row.get::<_, Option<String>>(0),
        )
        .map_err(|e| format!("读取最新交易日失败: {e}"))?;

    let sample_ts_code = latest_trade_date.as_deref().and_then(|trade_date| {
        reader
            .conn
            .query_row(
                "SELECT ts_code FROM stock_data WHERE adj_type = ? AND trade_date = ? ORDER BY ts_code LIMIT 1",
                [DEFAULT_ADJ_TYPE, trade_date],
                |row| row.get::<_, String>(0),
            )
            .ok()
    });
    let st_list = load_st_list(source_path)?;
    Ok((reader, sample_ts_code, latest_trade_date, st_list))
}

fn draft_to_rule(draft: StrategyManageRuleDraft) -> Result<StrategyRuleFileRule, String> {
    Ok(StrategyRuleFileRule {
        name: draft.name.trim().to_string(),
        scene_name: draft.scene_name.trim().to_string(),
        stage: parse_rule_stage(&draft.stage)?,
        scope_windows: draft.scope_windows.max(1),
        scope_way: normalize_scope_way(&draft.scope_way)?,
        when: draft.when.trim().to_string(),
        points: draft.points,
        dist_points: map_dist_points(draft.dist_points),
        explain: draft.explain.trim().to_string(),
    })
}

fn scene_draft_to_file(draft: StrategyManageSceneDraft) -> Result<StrategyRuleFileScene, String> {
    Ok(StrategyRuleFileScene {
        name: draft.name.trim().to_string(),
        direction: parse_scene_direction(&draft.direction)?,
        observe_threshold: draft.observe_threshold,
        trigger_threshold: draft.trigger_threshold,
        confirm_threshold: draft.confirm_threshold,
        fail_threshold: draft.fail_threshold,
    })
}

fn build_page_data(config: &StrategyRuleFile) -> StrategyManagePageData {
    let mut rule_count_map: HashMap<&str, usize> = HashMap::new();
    for rule in &config.rule {
        *rule_count_map.entry(rule.scene_name.trim()).or_default() += 1;
    }

    let scenes = config
        .scene
        .iter()
        .enumerate()
        .map(|(index, scene)| StrategyManageSceneItem {
            index,
            name: scene.name.clone(),
            direction: scene.direction.as_str().to_string(),
            observe_threshold: scene.observe_threshold,
            trigger_threshold: scene.trigger_threshold,
            confirm_threshold: scene.confirm_threshold,
            fail_threshold: scene.fail_threshold,
            rule_count: rule_count_map.get(scene.name.trim()).copied().unwrap_or(0),
        })
        .collect();

    let rules = config
        .rule
        .iter()
        .enumerate()
        .map(|(index, rule)| StrategyManageRuleItem {
            index,
            name: rule.name.clone(),
            scene_name: rule.scene_name.clone(),
            stage: format_rule_stage(rule.stage),
            scope_way: rule.scope_way.clone(),
            scope_windows: rule.scope_windows,
            points: rule.points,
            explain: rule.explain.clone(),
            when: rule.when.clone(),
            dist_points: rule.dist_points.clone(),
        })
        .collect();

    StrategyManagePageData { scenes, rules }
}

pub fn get_strategy_manage_page(source_path: &str) -> Result<StrategyManagePageData, String> {
    let config = load_rule_file(source_path)?;
    Ok(build_page_data(&config))
}

pub fn check_strategy_manage_scene_draft(
    source_path: &str,
    original_name: Option<&str>,
    draft: StrategyManageSceneDraft,
) -> Result<String, String> {
    validate_scene_draft_basic(source_path, original_name, &draft)
}

pub fn create_strategy_manage_scene(
    source_path: &str,
    draft: StrategyManageSceneDraft,
) -> Result<StrategyManagePageData, String> {
    validate_scene_draft_basic(source_path, None, &draft)?;
    let mut config = load_rule_file(source_path)?;
    config.scene.push(scene_draft_to_file(draft)?);
    save_rule_file(source_path, &config)?;
    get_strategy_manage_page(source_path)
}

pub fn update_strategy_manage_scene(
    source_path: &str,
    original_name: &str,
    draft: StrategyManageSceneDraft,
) -> Result<StrategyManagePageData, String> {
    validate_scene_draft_basic(source_path, Some(original_name), &draft)?;
    let mut config = load_rule_file(source_path)?;
    let Some(scene) = config
        .scene
        .iter_mut()
        .find(|item| item.name.trim() == original_name.trim())
    else {
        return Err(format!("scene 不存在: {}", original_name.trim()));
    };

    let new_name = draft.name.trim().to_string();
    scene.name = new_name.clone();
    scene.direction = parse_scene_direction(&draft.direction)?;
    scene.observe_threshold = draft.observe_threshold;
    scene.trigger_threshold = draft.trigger_threshold;
    scene.confirm_threshold = draft.confirm_threshold;
    scene.fail_threshold = draft.fail_threshold;

    if new_name != original_name.trim() {
        for rule in &mut config.rule {
            if rule.scene_name.trim() == original_name.trim() {
                rule.scene_name = new_name.clone();
            }
        }
    }

    save_rule_file(source_path, &config)?;
    get_strategy_manage_page(source_path)
}

pub fn remove_strategy_manage_scene(
    source_path: &str,
    name: &str,
) -> Result<StrategyManagePageData, String> {
    let trimmed_name = name.trim();
    if trimmed_name.is_empty() {
        return Err("scene 名称不能为空".to_string());
    }

    let mut config = load_rule_file(source_path)?;
    if config
        .rule
        .iter()
        .any(|item| item.scene_name.trim() == trimmed_name)
    {
        return Err(format!("scene 仍被 rule 引用，不能删除: {trimmed_name}"));
    }

    let original_len = config.scene.len();
    config.scene.retain(|item| item.name.trim() != trimmed_name);
    if config.scene.len() == original_len {
        return Err(format!("scene 不存在: {trimmed_name}"));
    }

    save_rule_file(source_path, &config)?;
    get_strategy_manage_page(source_path)
}

pub fn check_strategy_manage_rule_draft(
    source_path: &str,
    original_name: Option<&str>,
    draft: StrategyManageRuleDraft,
) -> Result<String, String> {
    let config = load_rule_file(source_path)?;
    let rule = draft_to_rule(draft)?;
    if config.rule.iter().any(|item| {
        item.name.trim() == rule.name.trim()
            && original_name.is_none_or(|old| old != item.name.trim())
    }) {
        return Err(format!("规则名称重复: {}", rule.name));
    }
    let (reader, sample_ts_code, latest_trade_date, st_list) =
        load_validation_context(source_path)?;
    validate_rule_definition(
        source_path,
        Some(&reader),
        sample_ts_code.as_deref(),
        latest_trade_date.as_deref(),
        Some(&st_list),
        &rule,
        &config.scene,
    )?;
    Ok("rule 草稿检查通过".to_string())
}

pub fn create_strategy_manage_rule(
    source_path: &str,
    draft: StrategyManageRuleDraft,
) -> Result<StrategyManagePageData, String> {
    check_strategy_manage_rule_draft(source_path, None, draft.clone())?;
    let mut config = load_rule_file(source_path)?;
    config.rule.push(draft_to_rule(draft)?);
    save_rule_file(source_path, &config)?;
    get_strategy_manage_page(source_path)
}

pub fn remove_strategy_manage_rules(
    source_path: &str,
    names: &[String],
) -> Result<StrategyManagePageData, String> {
    let name_set: HashSet<String> = names
        .iter()
        .map(|item| item.trim().to_string())
        .filter(|item| !item.is_empty())
        .collect();
    let mut config = load_rule_file(source_path)?;
    config
        .rule
        .retain(|item| !name_set.contains(item.name.trim()));
    save_rule_file(source_path, &config)?;
    get_strategy_manage_page(source_path)
}

pub fn update_strategy_manage_rule(
    source_path: &str,
    original_name: &str,
    draft: StrategyManageRuleDraft,
) -> Result<StrategyManagePageData, String> {
    check_strategy_manage_rule_draft(source_path, Some(original_name), draft.clone())?;
    let mut config = load_rule_file(source_path)?;
    let Some(rule) = config
        .rule
        .iter_mut()
        .find(|item| item.name.trim() == original_name.trim())
    else {
        return Err(format!("规则不存在: {}", original_name.trim()));
    };
    *rule = draft_to_rule(draft)?;
    save_rule_file(source_path, &config)?;
    get_strategy_manage_page(source_path)
}

pub fn save_strategy_manage_refactor_file(
    source_path: &str,
    file_name: &str,
    draft: StrategyManageRefactorDraft,
) -> Result<String, String> {
    if draft.scenes.is_empty() {
        return Err("至少需要一个 scene".to_string());
    }
    if draft.rules.is_empty() {
        return Err("至少需要一条 rule".to_string());
    }

    let output_path = rule_file_output_path(source_path, file_name)?;

    let mut scene_name_set: HashSet<String> = HashSet::new();
    let mut scene_items = Vec::with_capacity(draft.scenes.len());
    for scene in draft.scenes {
        let checked = StrategyManageSceneDraft {
            name: scene.name.trim().to_string(),
            direction: scene.direction.trim().to_string(),
            observe_threshold: scene.observe_threshold,
            trigger_threshold: scene.trigger_threshold,
            confirm_threshold: scene.confirm_threshold,
            fail_threshold: scene.fail_threshold,
        };
        if !scene_name_set.insert(checked.name.clone()) {
            return Err(format!("scene 名称重复: {}", checked.name));
        }
        validate_scene_values(&checked)?;
        scene_items.push(scene_draft_to_file(checked)?);
    }

    let (reader, sample_ts_code, latest_trade_date, st_list) =
        load_validation_context(source_path)?;
    let mut rule_name_set: HashSet<String> = HashSet::new();
    let mut rule_items = Vec::with_capacity(draft.rules.len());
    for rule_draft in draft.rules {
        let rule = draft_to_rule(rule_draft)?;
        if !rule_name_set.insert(rule.name.clone()) {
            return Err(format!("规则名称重复: {}", rule.name));
        }
        validate_rule_definition(
            source_path,
            Some(&reader),
            sample_ts_code.as_deref(),
            latest_trade_date.as_deref(),
            Some(&st_list),
            &rule,
            &scene_items,
        )?;
        rule_items.push(rule);
    }

    let file = StrategyRuleFile {
        version: 1,
        scene: scene_items,
        rule: rule_items,
    };

    let text = toml::to_string_pretty(&file).map_err(|e| format!("序列化策略规则文件失败: {e}"))?;
    fs::write(&output_path, text).map_err(|e| {
        format!(
            "写入策略规则文件失败: path={}, err={e}",
            output_path.display()
        )
    })?;

    Ok(output_path.display().to_string())
}

#[cfg(test)]
mod tests {
    use super::parse_rule_file_text;

    #[test]
    fn parse_strategy_rule_file_in_current_format() {
        let text = r#"
version = 1

[[scene]]
name = "趋势启动"
direction = "long"
observe_threshold = 1.0
trigger_threshold = 2.0
confirm_threshold = 3.0
fail_threshold = 1.0

[[rule]]
name = "启动测试"
scene = "趋势启动"
stage = "base"
scope_windows = 1
scope_way = "LAST"
when = "C > O"
points = 2.0
explain = "test"
"#;

        let file = parse_rule_file_text(text).expect("new-format file should parse");
        assert_eq!(file.scene.len(), 1);
        assert_eq!(file.rule.len(), 1);
        assert_eq!(file.rule[0].name, "启动测试");
    }

    #[test]
    fn parse_strategy_rule_file_with_legacy_weight() {
        let text = r#"
version = 1

[[scene]]
name = "趋势启动"
direction = "long"
observe_threshold = 1.0
trigger_threshold = 2.0
confirm_threshold = 3.0
fail_threshold = 1.0

[[rule]]
name = "启动测试"
scene = "趋势启动"
stage = "base"
scope_windows = 1
scope_way = "LAST"
when = "C > O"
weight = 1.5
points = 2.0
explain = "test"
"#;

        let file = parse_rule_file_text(text).expect("legacy-weight file should parse");
        assert_eq!(file.rule.len(), 1);
        assert_eq!(file.rule[0].name, "启动测试");
    }
}
