use std::{
    collections::{HashMap, HashSet},
    fs,
    path::PathBuf,
};

use serde::{Deserialize, Serialize};

use crate::data::scoring_data::row_into_rt;
use crate::expr::eval::Value;
use crate::expr::{
    parser::Stmts,
    validation::{
        estimate_expression_warmup, parse_expression_program, validate_expression_functions,
    },
};
use crate::{
    data::{DataReader, RuleStage, SceneDirection, ScoreConfig, score_rule_path},
    scoring::tools::{
        collect_used_cyq_chen_runtime_keys, inject_optional_cyq_chen_fields,
        inject_stock_extra_fields, load_st_list, load_total_share_map, rt_max_len,
    },
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
    let expression_need = estimate_expression_warmup(stmts)?;

    let scope_extra = match scope_way {
        StrategyScopeWay::Last => 0,
        StrategyScopeWay::Any | StrategyScopeWay::Each | StrategyScopeWay::Recent => {
            scope_windows.saturating_sub(1)
        }
        StrategyScopeWay::Consec(threshold) => scope_windows
            .saturating_sub(1)
            .max(threshold.saturating_sub(1)),
    };

    Ok(expression_need + scope_extra)
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
    source_path: &str,
    reader: Option<&DataReader>,
    sample_ts_code: Option<&str>,
    latest_trade_date: Option<&str>,
    st_list: Option<&HashSet<String>>,
    total_share_map: Option<&HashMap<String, f64>>,
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
    let scope_way = parse_scope_way(&rule.scope_way)?;
    if let Some(dist_points) = &rule.dist_points {
        if !dist_points.is_empty() && !strategy_scope_way_supports_dist_points(scope_way) {
            return Err(format!(
                "策略 {} 的 scope_way 不支持 dist_points，仅 EACH/RECENT 支持区间字典分",
                rule.name
            ));
        }
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
        let mut sorted = dist_points.iter().collect::<Vec<_>>();
        sorted.sort_by_key(|item| item.min);
        for index in 1..sorted.len() {
            let prev = sorted[index - 1];
            let curr = sorted[index];
            if prev.max >= curr.min {
                return Err(format!(
                    "策略 {} 的 dist_points 区间重叠: [{}-{}] 和 [{}-{}]",
                    rule.name, prev.min, prev.max, curr.min, curr.max
                ));
            }
        }
    }

    let stmts = parse_expression_program(&rule.when)
        .map_err(|e| format!("策略 {} 表达式解析错误在{}:{}", rule.name, e.idx, e.msg))?;
    validate_expression_functions(&stmts).map_err(|error| format!("策略 {} {error}", rule.name))?;

    if let (
        Some(reader),
        Some(sample_ts_code),
        Some(latest_trade_date),
        Some(st_list),
        Some(total_share_map),
    ) = (
        reader,
        sample_ts_code,
        latest_trade_date,
        st_list,
        total_share_map,
    ) {
        let warmup_need = estimate_rule_warmup(&stmts, scope_way, rule.scope_windows)?;
        let need_rows = (warmup_need + rule.scope_windows).max(1);
        let mut row_data = reader.load_one_tail_rows(
            sample_ts_code,
            DEFAULT_ADJ_TYPE,
            latest_trade_date,
            need_rows,
        )?;
        inject_stock_extra_fields(
            &mut row_data,
            sample_ts_code,
            st_list.contains(sample_ts_code),
            total_share_map.get(sample_ts_code).copied(),
        )?;
        let used_cyq_chen_keys = collect_used_cyq_chen_runtime_keys(&[&stmts]);
        inject_optional_cyq_chen_fields(
            &mut row_data,
            source_path,
            sample_ts_code,
            &used_cyq_chen_keys,
        );
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

fn strategy_scope_way_supports_dist_points(scope_way: StrategyScopeWay) -> bool {
    matches!(scope_way, StrategyScopeWay::Each | StrategyScopeWay::Recent)
}

fn map_dist_points(
    values: Option<Vec<StrategyManageDistPoint>>,
) -> Option<Vec<StrategyManageDistPoint>> {
    values.filter(|items| !items.is_empty())
}

fn load_validation_context(
    source_path: &str,
) -> Result<
    (
        DataReader,
        Option<String>,
        Option<String>,
        HashSet<String>,
        HashMap<String, f64>,
    ),
    String,
> {
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
    let total_share_map = load_total_share_map(source_path).unwrap_or_default();
    Ok((
        reader,
        sample_ts_code,
        latest_trade_date,
        st_list,
        total_share_map,
    ))
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
    let (reader, sample_ts_code, latest_trade_date, st_list, total_share_map) =
        load_validation_context(source_path)?;
    validate_rule_definition(
        source_path,
        Some(&reader),
        sample_ts_code.as_deref(),
        latest_trade_date.as_deref(),
        Some(&st_list),
        Some(&total_share_map),
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

    let (reader, sample_ts_code, latest_trade_date, st_list, total_share_map) =
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
            Some(&total_share_map),
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
    use std::{
        fs::{create_dir_all, remove_dir_all, write},
        time::{SystemTime, UNIX_EPOCH},
    };

    use duckdb::{Connection, params};

    use super::{StrategyManageRuleDraft, check_strategy_manage_rule_draft, parse_rule_file_text};
    use crate::data::source_db_path;

    fn temp_source_dir() -> std::path::PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!("lianghua_strategy_manage_{unique}"))
    }

    fn prepare_strategy_validation_source(source_dir: &std::path::Path) {
        create_dir_all(source_dir).expect("create source dir");
        write(
            source_dir.join("stock_list.csv"),
            "ts_code,unused,name\n000001.SZ,,样本股\n",
        )
        .expect("write stock_list.csv");
        write(
            source_dir.join("score_rule.toml"),
            r#"
version = 1

[[scene]]
name = "趋势启动"
direction = "long"
observe_threshold = 1.0
trigger_threshold = 2.0
confirm_threshold = 3.0
fail_threshold = 1.0

[[rule]]
name = "基础规则"
scene = "趋势启动"
stage = "base"
scope_windows = 1
scope_way = "LAST"
when = "C > O"
points = 1.0
explain = "test"
"#,
        )
        .expect("write score_rule.toml");

        let conn = Connection::open(source_db_path(source_dir.to_str().expect("utf8")))
            .expect("open source db");
        conn.execute(
            r#"
            CREATE TABLE stock_data (
                ts_code VARCHAR,
                trade_date VARCHAR,
                adj_type VARCHAR,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                vol DOUBLE,
                amount DOUBLE,
                pre_close DOUBLE,
                change DOUBLE,
                pct_chg DOUBLE
            )
            "#,
            [],
        )
        .expect("create stock_data");
        for (trade_date, open, high, low, close, pre_close, change, pct_chg) in [
            ("20240102", 10.0, 10.5, 9.8, 10.2, 10.0, 0.2, 2.0),
            ("20240103", 10.2, 11.0, 10.1, 10.8, 10.2, 0.6, 5.88),
        ] {
            conn.execute(
                r#"
                INSERT INTO stock_data VALUES (?, ?, 'qfq', ?, ?, ?, ?, 1000, 10000, ?, ?, ?)
                "#,
                params![
                    "000001.SZ",
                    trade_date,
                    open,
                    high,
                    low,
                    close,
                    pre_close,
                    change,
                    pct_chg
                ],
            )
            .expect("insert stock row");
        }
    }

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

    #[test]
    fn rule_draft_validation_accepts_optional_cyq_chen_fields() {
        let source_dir = temp_source_dir();
        prepare_strategy_validation_source(&source_dir);

        let result = check_strategy_manage_rule_draft(
            source_dir.to_str().expect("utf8"),
            None,
            StrategyManageRuleDraft {
                name: "攻击K".to_string(),
                scene_name: "趋势启动".to_string(),
                stage: "trigger".to_string(),
                scope_way: "LAST".to_string(),
                scope_windows: 1,
                when: r#"
Y_TRAPPED := REF(CYQ_TTR, 1);
RELEASED := Y_TRAPPED - CYQ_TTR;
Y_TRAPPED >= 0.60
AND RELEASED >= 0.30
AND CYQ_TPR >= 0.70
AND PCT_CHG >= 4
AND C >= H * 0.98
"#
                .to_string(),
                points: 6.0,
                dist_points: None,
                explain: "大量解放套牢盘".to_string(),
            },
        );

        remove_dir_all(&source_dir).expect("remove source dir");
        assert_eq!(result.as_deref(), Ok("rule 草稿检查通过"));
    }
}
