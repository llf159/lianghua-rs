use std::collections::{HashMap, VecDeque};

use lianghua_model::scoring::SceneResolvedStage;

use crate::{
    data::{DistPoint, RuleStage, RuleTag, SceneDirection, ScopeWay, ScoreScene},
    expr::{
        eval::{Runtime, Truth, Value},
        parser::Stmts,
    },
    scoring::tools::rt_max_len,
};

pub mod result_build;
pub mod rule_cache;
pub mod runner;
pub mod tools;
pub mod tracking;

#[derive(Debug, PartialEq)]
enum ScopeHit {
    Bool(bool),
    Count(usize),
    Recent(Option<usize>),
}

#[derive(Default)]
struct ScopeWindowState {
    hit_count: usize,
    last_hit: Option<usize>,
    consecutive: usize,
    last_consecutive: Option<usize>,
}

impl ScopeWindowState {
    fn advance(&mut self, scopeway: ScopeWay, windows: usize, bs: &[Truth], i: usize) -> ScopeHit {
        let hit = bs[i].is_true();
        if matches!(scopeway, ScopeWay::Last) {
            return ScopeHit::Bool(hit);
        }
        self.hit_count += usize::from(hit);
        if i >= windows {
            self.hit_count -= usize::from(bs[i - windows].is_true());
        }
        if hit {
            self.last_hit = Some(i);
            self.consecutive += 1;
        } else {
            self.consecutive = 0;
        }
        let start = (i + 1).saturating_sub(windows);
        match scopeway {
            ScopeWay::Last => ScopeHit::Bool(hit),
            ScopeWay::Any => ScopeHit::Bool(self.hit_count > 0),
            ScopeWay::Each => ScopeHit::Count(self.hit_count),
            ScopeWay::Recent => {
                ScopeHit::Recent(self.last_hit.filter(|&day| day >= start).map(|day| i - day))
            }
            ScopeWay::Consec(len) => {
                if self.consecutive >= len {
                    self.last_consecutive = Some(i);
                }
                ScopeHit::Bool(
                    len == 0
                        || self
                            .last_consecutive
                            .is_some_and(|end| end >= start && end - start + 1 >= len),
                )
            }
        }
    }
}

#[derive(Debug, Default)]
pub struct RuleScoreSeries {
    pub name: String,
    pub series: Vec<f64>,
    pub triggered: Vec<bool>,
}

#[derive(Debug, Default)]
pub struct SceneScoreSeries {
    pub name: String,
    pub direction: SceneDirection,
    pub stage: Vec<Option<SceneResolvedStage>>,
    pub stage_score: Vec<f64>,
    pub risk_score: Vec<f64>,
    pub confirm_strength: Vec<f64>,
    pub risk_intensity: Vec<f64>,
    pub triggered: Vec<bool>,
}

#[derive(Debug, Clone)]
pub struct RuleSceneMeta {
    pub scene_name: String,
    pub stage: RuleStage,
}

#[derive(Clone)]
pub struct CachedRule {
    pub name: String,
    pub scope_windows: usize,
    pub scope_way: ScopeWay,
    pub points: f64,
    pub dist_points: Option<Vec<DistPoint>>,
    pub max_points: Option<f64>,
    pub tag: RuleTag,
    pub when_src: String,
    pub when_ast: Stmts,
    pub assigned_names: Vec<String>,
    pub combination: Option<CachedCombinationRule>,
}

#[derive(Clone)]
pub struct CachedRuleExpression {
    pub name: String,
    pub when_src: String,
    pub when_ast: Stmts,
    pub assigned_names: Vec<String>,
}

#[derive(Clone)]
pub struct CachedCombinationCondition {
    pub expression: CachedRuleExpression,
    pub bonus_points: f64,
}

#[derive(Clone)]
pub struct CachedCombinationRule {
    pub conditions: Vec<CachedCombinationCondition>,
    pub points_by_hits: Vec<f64>,
    pub max_points: Option<f64>,
    pub max_bonus_points: Option<f64>,
}

impl CachedRule {
    pub fn expression_programs(&self) -> Vec<&Stmts> {
        let Some(combination) = &self.combination else {
            return vec![&self.when_ast];
        };

        combination
            .conditions
            .iter()
            .map(|condition| &condition.expression.when_ast)
            .collect()
    }
}

#[derive(Clone)]
enum RuntimeSnapshotEntry {
    Existing(String, Value),
    Missing(String),
}

fn snapshot_runtime_values(rt: &Runtime, names: &[String]) -> Vec<RuntimeSnapshotEntry> {
    names
        .iter()
        .map(|name| match rt.vars.get(name).cloned() {
            Some(value) => RuntimeSnapshotEntry::Existing(name.clone(), value),
            None => RuntimeSnapshotEntry::Missing(name.clone()),
        })
        .collect()
}

fn restore_runtime_values(rt: &mut Runtime, snapshots: &[RuntimeSnapshotEntry]) {
    for snapshot in snapshots {
        match snapshot {
            RuntimeSnapshotEntry::Existing(name, value) => {
                rt.vars.insert(name.clone(), value.clone());
            }
            RuntimeSnapshotEntry::Missing(name) => {
                rt.vars.remove(name);
            }
        }
    }
}

#[cfg(test)]
fn hit_scopeway(scopeway: ScopeWay, windows: usize, bs: &[Truth], i: usize) -> ScopeHit {
    match scopeway {
        ScopeWay::Last => ScopeHit::Bool(bs[i].is_true()),
        ScopeWay::Any => {
            let start = (i + 1).saturating_sub(windows);
            for j in start..=i {
                if bs[j].is_true() {
                    return ScopeHit::Bool(true);
                }
            }
            ScopeHit::Bool(false)
        }
        ScopeWay::Consec(len) => {
            let start = (i + 1).saturating_sub(windows);
            let mut cur = 0;
            let mut best = 0;
            for j in start..=i {
                if bs[j].is_true() {
                    cur += 1;
                } else {
                    cur = 0;
                }
                if cur > best {
                    best = cur;
                }
            }
            ScopeHit::Bool(best >= len)
        }
        ScopeWay::Each => {
            let start = (i + 1).saturating_sub(windows);
            ScopeHit::Count(bs[start..=i].iter().filter(|truth| truth.is_true()).count())
        }
        ScopeWay::Recent => {
            let start = (i + 1).saturating_sub(windows);
            for j in (start..=i).rev() {
                if bs[j].is_true() {
                    return ScopeHit::Recent(Some(i - j));
                }
            }
            ScopeHit::Recent(None)
        }
    }
}

fn score_from_dist_points(value: usize, dps: &[DistPoint]) -> f64 {
    for dp in dps {
        if dp.min <= value && value <= dp.max {
            return dp.points;
        }
    }
    0.0
}

fn scope_hit_triggered(hit: &ScopeHit) -> bool {
    match hit {
        ScopeHit::Bool(ok) => *ok,
        ScopeHit::Count(count) => *count > 0,
        ScopeHit::Recent(value) => value.is_some(),
    }
}

fn clamp_score(value: f64, cap: Option<f64>) -> f64 {
    match cap {
        Some(cap) => value.clamp(-cap, cap),
        None => value,
    }
}

fn combination_score_parts(
    combination: &CachedCombinationRule,
    mut condition_hit: impl FnMut(usize) -> bool,
) -> (f64, f64, bool) {
    let hit_count = (0..combination.conditions.len())
        .filter(|index| condition_hit(*index))
        .count();
    if hit_count == 0 {
        return (0.0, 0.0, false);
    }
    let base_score = combination.points_by_hits[hit_count];

    let bonus_score = combination
        .conditions
        .iter()
        .enumerate()
        .filter_map(|(index, condition)| condition_hit(index).then_some(condition.bonus_points))
        .sum::<f64>();
    (base_score, bonus_score, true)
}

fn scoring_rule_cache(
    rule: &CachedRule,
    rt: &mut Runtime,
    mut availability: Option<&mut Vec<bool>>,
) -> Result<(Vec<f64>, Vec<bool>), String> {
    if let Some(combination) = &rule.combination {
        return (|rule: &CachedRule,
                 combination: &CachedCombinationRule,
                 rt: &mut Runtime|
         -> Result<(Vec<f64>, Vec<bool>), String> {
            let condition_hits = combination
                .conditions
                .iter()
                .map(|condition| {
                    (|expression: &CachedRuleExpression,
                      rt: &mut Runtime|
                     -> Result<Vec<Truth>, String> {
                        let snapshots = snapshot_runtime_values(rt, &expression.assigned_names);
                        let result = rt
                            .eval_program(&expression.when_ast)
                            .map_err(|error| {
                                format!("表达式({})计算错误:{}", expression.name, error.msg)
                            })
                            .and_then(|value| {
                                let len = rt_max_len(rt);
                                value.into_truth_series(len).map_err(|error| {
                                    format!("表达式({})返回值非布尔:{}", expression.name, error.msg)
                                })
                            });
                        restore_runtime_values(rt, &snapshots);
                        result
                    })(&condition.expression, rt)
                })
                .collect::<Result<Vec<_>, _>>()?;
            let len = rt_max_len(rt);
            let mut missing_prefix = Vec::with_capacity(len + 1);
            missing_prefix.push(0usize);
            for day in 0..len {
                missing_prefix.push(
                    missing_prefix[day]
                        + usize::from(condition_hits.iter().any(|hits| !hits[day].is_known())),
                );
            }
            let mut scores = Vec::with_capacity(len);
            let mut triggered = Vec::with_capacity(len);
            let mut daily_parts = if matches!(rule.scope_way, ScopeWay::Any | ScopeWay::Each) {
                vec![None; len]
            } else {
                Vec::new()
            };
            let mut best_days = VecDeque::<(usize, usize)>::new();
            let mut condition_states = if matches!(rule.scope_way, ScopeWay::Consec(_)) {
                (0..combination.conditions.len())
                    .map(|_| ScopeWindowState::default())
                    .collect::<Vec<_>>()
            } else {
                Vec::new()
            };
            let mut consecutive_hits = vec![false; combination.conditions.len()];

            for index in 0..len {
                let start = if matches!(rule.scope_way, ScopeWay::Last) {
                    index
                } else {
                    (index + 1).saturating_sub(rule.scope_windows)
                };
                if matches!(rule.scope_way, ScopeWay::Consec(_)) {
                    for (condition, state) in condition_states.iter_mut().enumerate() {
                        consecutive_hits[condition] = scope_hit_triggered(&state.advance(
                            rule.scope_way,
                            rule.scope_windows,
                            &condition_hits[condition],
                            index,
                        ));
                    }
                }
                if missing_prefix[index + 1] != missing_prefix[start] {
                    best_days.clear();
                    scores.push(0.0);
                    triggered.push(false);
                    continue;
                }
                let (base_score, bonus_score, is_triggered) = match rule.scope_way {
                    ScopeWay::Last => combination_score_parts(combination, |condition| {
                        condition_hits[condition][index].is_true()
                    }),
                    ScopeWay::Any => {
                        while best_days.front().is_some_and(|&(day, _)| day < start) {
                            best_days.pop_front();
                        }
                        // After an incomplete window, rebuild only when a usable window returns.
                        let first_day = if best_days.is_empty() { start } else { index };
                        for day in first_day..=index {
                            let count = condition_hits
                                .iter()
                                .filter(|hits| hits[day].is_true())
                                .count();
                            while best_days
                                .back()
                                .is_some_and(|&(_, previous)| previous <= count)
                            {
                                best_days.pop_back();
                            }
                            best_days.push_back((day, count));
                        }
                        let best_day = best_days.front().map_or(index, |&(day, _)| day);
                        *daily_parts[best_day].get_or_insert_with(|| {
                            combination_score_parts(combination, |condition| {
                                condition_hits[condition][best_day].is_true()
                            })
                        })
                    }
                    ScopeWay::Each => {
                        let mut base_total = 0.0;
                        let mut bonus_total = 0.0;
                        let mut any_triggered = false;
                        for day in start..=index {
                            let (day_base, day_bonus, day_triggered) = *daily_parts[day]
                                .get_or_insert_with(|| {
                                    combination_score_parts(combination, |condition| {
                                        condition_hits[condition][day].is_true()
                                    })
                                });
                            if day_triggered {
                                base_total += day_base;
                                bonus_total += day_bonus;
                                any_triggered = true;
                            }
                        }
                        (base_total, bonus_total, any_triggered)
                    }
                    ScopeWay::Consec(_) => combination_score_parts(combination, |condition| {
                        consecutive_hits[condition]
                    }),
                    ScopeWay::Recent => {
                        return Err("组合策略不支持 RECENT scope_way".to_string());
                    }
                };

                triggered.push(is_triggered);
                scores.push(if is_triggered {
                    (|combination: &CachedCombinationRule,
                      base_score: f64,
                      bonus_score: f64|
                     -> f64 {
                        let bonus_score = clamp_score(bonus_score, combination.max_bonus_points);
                        clamp_score(base_score + bonus_score, combination.max_points)
                    })(combination, base_score, bonus_score)
                } else {
                    0.0
                });
            }

            Ok((scores, triggered))
        })(rule, combination, rt);
    }

    let snapshots = snapshot_runtime_values(rt, &rule.assigned_names);
    let bs_result = (|rule: &CachedRule, rt: &mut Runtime| -> Result<Vec<Truth>, String> {
        let value = rt
            .eval_program(&rule.when_ast)
            .map_err(|e| format!("表达式计算错误:{}", e.msg))?;
        let len = rt_max_len(rt);

        value
            .into_truth_series(len)
            .map_err(|e| format!("表达式返回值非布尔:{}", e.msg))
    })(rule, rt);
    restore_runtime_values(rt, &snapshots);
    let truths = bs_result?;
    let mut missing_prefix = Vec::with_capacity(truths.len() + 1);
    missing_prefix.push(0usize);
    for truth in &truths {
        missing_prefix.push(missing_prefix.last().unwrap() + usize::from(!truth.is_known()));
    }
    let bs = truths;
    let mut out = Vec::with_capacity(bs.len());
    let mut triggered = Vec::with_capacity(bs.len());
    let mut scope_state = ScopeWindowState::default();

    for i in 0..bs.len() {
        let start = if matches!(rule.scope_way, ScopeWay::Last) {
            i
        } else {
            (i + 1).saturating_sub(rule.scope_windows)
        };
        let complete = missing_prefix[i + 1] == missing_prefix[start];
        if let Some(availability) = availability.as_deref_mut() {
            availability.push(complete);
        }
        let hit = scope_state.advance(rule.scope_way, rule.scope_windows, &bs, i);
        if !complete {
            out.push(0.0);
            triggered.push(false);
            continue;
        }
        triggered.push(scope_hit_triggered(&hit));
        let s = (|scopeway: ScopeHit, dps: Option<&[DistPoint]>, points: f64| -> f64 {
            match scopeway {
                ScopeHit::Bool(ok) => {
                    if ok {
                        points
                    } else {
                        0.0
                    }
                }
                ScopeHit::Count(count) => {
                    if count == 0 {
                        0.0
                    } else if let Some(dp) = dps {
                        // Preserve the original chronological floating-point addition order.
                        (start..=i)
                            .filter(|&day| bs[day].is_true())
                            .map(|day| score_from_dist_points(i - day, dp))
                            .sum::<f64>()
                    } else {
                        count as f64 * points
                    }
                }
                ScopeHit::Recent(v) => {
                    if let Some(dp) = dps {
                        match v {
                            Some(last) => score_from_dist_points(last, dp),
                            None => 0.0,
                        }
                    } else {
                        match v {
                            Some(_) => points,
                            None => 0.0,
                        }
                    }
                }
            }
        })(hit, rule.dist_points.as_deref(), rule.points);
        out.push(clamp_score(s, rule.max_points));
    }

    Ok((out, triggered))
}

pub fn evaluate_cached_rule_scores(
    rule: &CachedRule,
    rt: &mut Runtime,
) -> Result<(Vec<f64>, Vec<bool>), String> {
    scoring_rule_cache(rule, rt, None)
}

/// Research needs to distinguish an unknown input window from a known non-hit.
/// The scoring and scope calculation are shared with production.
pub fn evaluate_cached_rule_scores_with_availability(
    rule: &CachedRule,
    rt: &mut Runtime,
) -> Result<(Vec<f64>, Vec<bool>, Vec<bool>), String> {
    if rule.combination.is_some() {
        return Err("单表达式研究不支持组合规则".into());
    }
    let mut available = Vec::new();
    let (scores, triggered) = scoring_rule_cache(rule, rt, Some(&mut available))?;
    Ok((scores, triggered, available))
}

pub fn scoring_rules_details_cache(
    rt: &mut Runtime,
    rules_cache: &[CachedRule],
) -> Result<(Vec<f64>, Vec<RuleScoreSeries>), String> {
    let len = rt_max_len(rt);
    let mut total = vec![50.0; len];
    let mut details = Vec::with_capacity(rules_cache.len());

    for rule in rules_cache {
        let (score, triggered) = scoring_rule_cache(&rule, rt, None)?;
        let min_len = usize::min(total.len(), score.len());
        for i in 0..min_len {
            total[i] += score[i];
        }

        details.push(RuleScoreSeries {
            name: rule.name.clone(),
            series: score,
            triggered,
        });
    }

    Ok((total, details))
}

pub fn scoring_rules_total_cache(
    rt: &mut Runtime,
    rules_cache: &[CachedRule],
) -> Result<Vec<f64>, String> {
    let len = rt_max_len(rt);
    let mut total = vec![50.0; len];

    for rule in rules_cache {
        let (score, _) = scoring_rule_cache(rule, rt, None)?;
        let min_len = usize::min(total.len(), score.len());
        for i in 0..min_len {
            total[i] += score[i];
        }
    }

    Ok(total)
}

fn cross_stage_threshold(direction: SceneDirection, score: f64, threshold: f64) -> bool {
    if !score.is_finite() || !threshold.is_finite() {
        return false;
    }
    match direction {
        SceneDirection::Long => score >= threshold,
        SceneDirection::Short => score <= -threshold,
    }
}

fn calc_intensity(score: f64, threshold: f64) -> f64 {
    if !score.is_finite() || !threshold.is_finite() || threshold.abs() < (1e-12) {
        return 0.0;
    }
    score.abs() / threshold.abs()
}

pub fn build_scene_score_series(
    rule_scene_meta: &[RuleSceneMeta],
    rule_details: &[RuleScoreSeries],
    scenes: &[ScoreScene],
) -> Vec<SceneScoreSeries> {
    if scenes.is_empty() || rule_details.is_empty() {
        return Vec::new();
    }

    let len = rule_details
        .first()
        .map(|item| item.series.len())
        .unwrap_or_default();
    let mut scene_index = HashMap::with_capacity(scenes.len());
    let mut out: Vec<SceneScoreSeries> = scenes
        .iter()
        .enumerate()
        .map(|(index, scene)| {
            scene_index.insert(scene.name.clone(), index);
            SceneScoreSeries {
                name: scene.name.clone(),
                direction: scene.direction,
                stage: vec![None; len],
                stage_score: vec![0.0; len],
                risk_score: vec![0.0; len],
                confirm_strength: vec![0.0; len],
                risk_intensity: vec![0.0; len],
                triggered: vec![false; len],
            }
        })
        .collect();

    let mut has_trigger_rule = vec![vec![false; len]; scenes.len()];
    let mut has_confirm_rule = vec![vec![false; len]; scenes.len()];
    let mut has_fail_rule = vec![vec![false; len]; scenes.len()];

    for (rule_meta, detail) in rule_scene_meta.iter().zip(rule_details.iter()) {
        let Some(&scene_pos) = scene_index.get(&rule_meta.scene_name) else {
            continue;
        };
        let scene_row = &mut out[scene_pos];
        let min_len = usize::min(detail.series.len(), detail.triggered.len()).min(len);

        for i in 0..min_len {
            if !detail.triggered[i] {
                continue;
            }

            scene_row.triggered[i] = true;

            match rule_meta.stage {
                RuleStage::Base => {
                    scene_row.stage_score[i] += detail.series[i];
                }
                RuleStage::Trigger => {
                    scene_row.stage_score[i] += detail.series[i];
                    has_trigger_rule[scene_pos][i] = true;
                }
                RuleStage::Confirm => {
                    scene_row.stage_score[i] += detail.series[i];
                    has_confirm_rule[scene_pos][i] = true;
                }
                RuleStage::Risk => {
                    scene_row.risk_score[i] += detail.series[i];
                }
                RuleStage::Fail => {
                    scene_row.risk_score[i] += detail.series[i];
                    has_fail_rule[scene_pos][i] = true;
                }
            }
        }
    }

    for (scene_pos, scene) in scenes.iter().enumerate() {
        for i in 0..len {
            if !out[scene_pos].triggered[i] {
                continue;
            }
            let stage_score = out[scene_pos].stage_score[i];
            let risk_score = out[scene_pos].risk_score[i];
            out[scene_pos].confirm_strength[i] =
                calc_intensity(stage_score, scene.confirm_threshold);
            out[scene_pos].risk_intensity[i] = calc_intensity(risk_score, scene.fail_threshold);
            out[scene_pos].stage[i] = (|scene: &ScoreScene,
                                        stage_score: f64,
                                        risk_score: f64,
                                        has_trigger: bool,
                                        has_confirm: bool,
                                        has_fail: bool|
             -> Option<SceneResolvedStage> {
                if has_fail
                    && (|direction: SceneDirection, risk_score: f64, threshold: f64| -> bool {
                        if !risk_score.is_finite() || !threshold.is_finite() {
                            return false;
                        }
                        match direction {
                            SceneDirection::Long => risk_score <= -threshold,
                            SceneDirection::Short => risk_score >= threshold,
                        }
                    })(scene.direction, risk_score, scene.fail_threshold)
                {
                    return Some(SceneResolvedStage::Fail);
                }
                if has_confirm
                    && cross_stage_threshold(scene.direction, stage_score, scene.confirm_threshold)
                {
                    return Some(SceneResolvedStage::Confirm);
                }
                if has_trigger
                    && cross_stage_threshold(scene.direction, stage_score, scene.trigger_threshold)
                {
                    return Some(SceneResolvedStage::Trigger);
                }
                if has_trigger
                    && cross_stage_threshold(scene.direction, stage_score, scene.observe_threshold)
                {
                    return Some(SceneResolvedStage::Observe);
                }
                None
            })(
                scene,
                stage_score,
                risk_score,
                has_trigger_rule[scene_pos][i],
                has_confirm_rule[scene_pos][i],
                has_fail_rule[scene_pos][i],
            );
        }
    }

    out
}

#[cfg(test)]
mod tests {
    use super::{
        CachedCombinationCondition, CachedCombinationRule, CachedRule, CachedRuleExpression,
        evaluate_cached_rule_scores,
    };
    use crate::{
        data::{ScopeWay, collect_assigned_names_from_expr_program},
        expr::{
            eval::{Runtime, Value},
            parser::{Parser, lex_all},
        },
    };

    fn cached_rule(expression: &str) -> CachedRule {
        let tokens = lex_all(expression);
        let mut parser = Parser::new(tokens);
        let when_ast = parser.parse_main().expect("expression should parse");
        let assigned_names = collect_assigned_names_from_expr_program(&when_ast);

        CachedRule {
            name: expression.to_string(),
            scope_windows: 1,
            scope_way: ScopeWay::Last,
            points: 1.0,
            dist_points: None,
            max_points: None,
            tag: crate::data::RuleTag::Normal,
            when_src: expression.to_string(),
            when_ast,
            assigned_names,
            combination: None,
        }
    }

    fn combination_rule(
        condition_expressions: &[&str],
        points_by_hits: Vec<f64>,
        bonus_points: &[f64],
        max_points: Option<f64>,
        max_bonus_points: Option<f64>,
    ) -> CachedRule {
        let conditions = condition_expressions
            .iter()
            .enumerate()
            .map(|(index, expression)| CachedCombinationCondition {
                expression: (|name: &str, expression: &str| -> CachedRuleExpression {
                    let tokens = lex_all(expression);
                    let mut parser = Parser::new(tokens);
                    let when_ast = parser.parse_main().expect("expression should parse");
                    let assigned_names = collect_assigned_names_from_expr_program(&when_ast);
                    CachedRuleExpression {
                        name: name.to_string(),
                        when_src: expression.to_string(),
                        when_ast,
                        assigned_names,
                    }
                })(&format!("condition_{}", index + 1), expression),
                bonus_points: bonus_points.get(index).copied().unwrap_or(0.0),
            })
            .collect::<Vec<_>>();
        let first = conditions[0].expression.clone();
        CachedRule {
            name: "combination".to_string(),
            scope_windows: 1,
            scope_way: ScopeWay::Last,
            points: 0.0,
            dist_points: None,
            max_points: None,
            tag: crate::data::RuleTag::Normal,
            when_src: first.when_src,
            when_ast: first.when_ast,
            assigned_names: first.assigned_names,
            combination: Some(CachedCombinationRule {
                conditions,
                points_by_hits,
                max_points,
                max_bonus_points,
            }),
        }
    }

    fn runtime_with_close_series(close_values: &[f64]) -> Runtime {
        let mut runtime = Runtime::default();
        runtime.vars.insert(
            "C".to_string(),
            Value::NumSeries(close_values.iter().map(|value| Some(*value)).collect()),
        );
        runtime
    }

    #[test]
    fn sliding_scope_matches_window_scan() {
        use super::{ScopeWindowState, hit_scopeway};
        use crate::expr::eval::Truth;

        // All short three-state histories, including zero/oversized windows and runs.
        for pattern in 0..3usize.pow(6) {
            let mut code = pattern;
            let truths = (0..6)
                .map(|_| {
                    let truth = [Truth::False, Truth::True, Truth::Unknown][code % 3];
                    code /= 3;
                    truth
                })
                .collect::<Vec<_>>();
            for window in [0, 1, 2, 4, 8] {
                for way in [
                    ScopeWay::Last,
                    ScopeWay::Any,
                    ScopeWay::Each,
                    ScopeWay::Recent,
                    ScopeWay::Consec(0),
                    ScopeWay::Consec(1),
                    ScopeWay::Consec(3),
                    ScopeWay::Consec(8),
                ] {
                    let mut state = ScopeWindowState::default();
                    for i in 0..truths.len() {
                        let expected = hit_scopeway(way, window, &truths, i);
                        let actual = state.advance(way, window, &truths, i);
                        assert_eq!(
                            actual, expected,
                            "pattern={pattern}, window={window}, way={way:?}, day={i}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn cached_rule_restores_overwritten_base_series() {
        let mut runtime = runtime_with_close_series(&[1.0, 2.0, 3.0]);
        let overwrite_rule = cached_rule("C := REF(C, 1); C > 0");
        let check_rule = cached_rule("C > 2");

        evaluate_cached_rule_scores(&overwrite_rule, &mut runtime)
            .expect("overwrite rule should evaluate");
        let (_, triggered) =
            evaluate_cached_rule_scores(&check_rule, &mut runtime).expect("check rule evaluates");

        assert_eq!(triggered, vec![false, false, true]);
    }

    #[test]
    fn cached_rule_removes_temporary_assignments_after_eval() {
        let mut runtime = runtime_with_close_series(&[1.0, 2.0, 3.0]);
        let temp_rule = cached_rule("TMP := 1; TMP > 0");
        let check_rule = cached_rule("TMP > 0");

        evaluate_cached_rule_scores(&temp_rule, &mut runtime).expect("temp rule should evaluate");
        assert!(!runtime.vars.contains_key("TMP"));

        let error = evaluate_cached_rule_scores(&check_rule, &mut runtime)
            .expect_err("TMP should not leak into next rule");
        assert!(error.contains("变量不存在:TMP"));
    }

    #[test]
    fn dynamic_window_over_cap_does_not_trigger_rule() {
        let mut runtime = runtime_with_close_series(&[1.0, 5.0, 3.0, 4.0, 2.0]);
        runtime.vars.insert(
            "N".to_string(),
            Value::NumSeries(vec![Some(1.0), Some(2.0), Some(3.0), Some(2.0), Some(4.0)]),
        );
        let rule = cached_rule("V := HHVD(C, N, 3); V > 0");

        let (scores, triggered) =
            evaluate_cached_rule_scores(&rule, &mut runtime).expect("rule evaluates");

        assert_eq!(triggered, vec![true, true, true, true, false]);
        assert_eq!(scores, vec![1.0, 1.0, 1.0, 1.0, 0.0]);
    }

    #[test]
    fn single_each_rule_applies_final_score_absolute_cap() {
        let mut runtime = runtime_with_close_series(&[1.0, 2.0, 3.0]);
        let mut rule = cached_rule("C > 0");
        rule.scope_way = ScopeWay::Each;
        rule.scope_windows = 3;
        rule.points = 2.0;
        rule.max_points = Some(3.0);

        let (scores, triggered) =
            evaluate_cached_rule_scores(&rule, &mut runtime).expect("single EACH evaluates");

        assert_eq!(scores, vec![2.0, 3.0, 3.0]);
        assert_eq!(triggered, vec![true, true, true]);

        rule.points = -2.0;
        let (scores, _) =
            evaluate_cached_rule_scores(&rule, &mut runtime).expect("negative EACH evaluates");
        assert_eq!(scores, vec![-2.0, -3.0, -3.0]);
    }

    #[test]
    fn combination_rule_scores_distinct_hits_and_bonus() {
        let mut runtime = runtime_with_close_series(&[0.0, 2.0, 3.0, 4.0, 5.0]);
        let rule = combination_rule(
            &["C > 1", "C > 2", "C > 3"],
            vec![0.0, 1.0, 3.0, 5.0],
            &[0.0, 0.0, 2.0],
            None,
            None,
        );

        let (scores, triggered) =
            evaluate_cached_rule_scores(&rule, &mut runtime).expect("combination evaluates");

        assert_eq!(scores, vec![0.0, 1.0, 3.0, 7.0, 7.0]);
        assert_eq!(triggered, vec![false, true, true, true, true]);
    }

    #[test]
    fn combination_bonus_applies_without_base_score() {
        let mut runtime = runtime_with_close_series(&[3.0, 11.0]);
        let rule = combination_rule(
            &["C > 1", "C > 10"],
            vec![0.0, 0.0, 3.0],
            &[2.0, 0.0],
            None,
            None,
        );

        let (scores, triggered) =
            evaluate_cached_rule_scores(&rule, &mut runtime).expect("combination evaluates");

        assert_eq!(scores, vec![2.0, 5.0]);
        assert_eq!(triggered, vec![true, true]);
    }

    #[test]
    fn combination_without_any_hit_does_not_trigger() {
        let mut runtime = runtime_with_close_series(&[0.5, 0.5]);
        let rule = combination_rule(
            &["C > 1", "C > 10"],
            vec![0.0, 0.0, 3.0],
            &[-2.0, -4.0],
            None,
            None,
        );

        let (scores, triggered) =
            evaluate_cached_rule_scores(&rule, &mut runtime).expect("combination evaluates");

        assert_eq!(scores, vec![0.0, 0.0]);
        assert_eq!(triggered, vec![false, false]);
    }

    #[test]
    fn combination_bonus_follows_condition_hit() {
        let mut runtime = runtime_with_close_series(&[5.0, 11.0]);
        let rule = combination_rule(
            &["C > 10", "C > 1"],
            vec![0.0, 1.0, 2.0],
            &[3.0, 0.0],
            None,
            None,
        );

        let (scores, triggered) =
            evaluate_cached_rule_scores(&rule, &mut runtime).expect("combination evaluates");

        assert_eq!(scores, vec![1.0, 5.0]);
        assert_eq!(triggered, vec![true, true]);
    }

    #[test]
    fn combination_any_scores_the_most_hits_day_and_prefers_the_latest_tie() {
        let mut runtime = runtime_with_close_series(&[1.0, 2.0, 3.0]);
        let mut rule = combination_rule(
            &["C > 0", "C < 1.5", "C > 2.5"],
            vec![0.0, 1.0, 3.0, 6.0],
            &[0.0, 10.0, 20.0],
            None,
            None,
        );
        rule.scope_way = ScopeWay::Any;
        rule.scope_windows = 3;

        let (scores, triggered) =
            evaluate_cached_rule_scores(&rule, &mut runtime).expect("combination evaluates");

        assert_eq!(scores, vec![13.0, 13.0, 23.0]);
        assert_eq!(triggered, vec![true, true, true]);

        runtime.vars.insert(
            "C".into(),
            Value::NumSeries(vec![
                Some(1.0),
                None,
                Some(1.0),
                Some(2.0),
                Some(2.0),
                Some(3.0),
            ]),
        );
        let (scores, triggered) = evaluate_cached_rule_scores(&rule, &mut runtime).unwrap();
        assert_eq!(scores, vec![13.0, 0.0, 0.0, 0.0, 13.0, 23.0]);
        assert_eq!(triggered, vec![true, false, false, false, true, true]);
    }

    #[test]
    fn combination_each_sums_each_days_score_in_the_window() {
        let mut runtime = runtime_with_close_series(&[1.0, 2.0, 3.0]);
        let mut rule = combination_rule(
            &["C > 0", "C < 1.5", "C > 2.5"],
            vec![0.0, 1.0, 3.0, 6.0],
            &[0.0, 10.0, 20.0],
            None,
            None,
        );
        rule.scope_way = ScopeWay::Each;
        rule.scope_windows = 3;

        let (scores, triggered) =
            evaluate_cached_rule_scores(&rule, &mut runtime).expect("combination evaluates");

        assert_eq!(scores, vec![13.0, 14.0, 37.0]);
        assert_eq!(triggered, vec![true, true, true]);
    }

    #[test]
    fn combination_recent_is_rejected_at_runtime() {
        let mut runtime = runtime_with_close_series(&[1.0]);
        let mut rule = combination_rule(&["C > 0"], vec![0.0, 1.0], &[0.0], None, None);
        rule.scope_way = ScopeWay::Recent;

        let error = evaluate_cached_rule_scores(&rule, &mut runtime)
            .expect_err("combination RECENT must be rejected");

        assert!(error.contains("不支持 RECENT"));
    }

    #[test]
    fn combination_rule_applies_bonus_and_total_caps() {
        let mut runtime = runtime_with_close_series(&[5.0]);
        let rule = combination_rule(
            &["C > 1", "C > 2"],
            vec![0.0, 1.0, 5.0],
            &[2.0, 2.0],
            Some(5.5),
            Some(1.0),
        );

        let (scores, triggered) =
            evaluate_cached_rule_scores(&rule, &mut runtime).expect("combination evaluates");

        assert_eq!(scores, vec![5.5]);
        assert_eq!(triggered, vec![true]);
    }
    #[test]
    fn unknown_conditions_never_create_scores_or_invent_combination_hit_counts() {
        let mut runtime = Runtime {
            vars: std::collections::HashMap::from([(
                "C".into(),
                Value::NumSeries(vec![None, Some(9.0), Some(12.0)]),
            )]),
        };
        for (rule, expected) in [
            (cached_rule("!(C > 10)"), vec![0.0, 1.0, 0.0]),
            (cached_rule("IF(0,C,1) > 0"), vec![1.0, 1.0, 1.0]),
            (
                combination_rule(&["1", "!(C > 10)"], vec![0.0, 7.0, 2.0], &[], None, None),
                vec![0.0, 2.0, 7.0],
            ),
        ] {
            let (scores, triggered) = evaluate_cached_rule_scores(&rule, &mut runtime).unwrap();
            assert_eq!(scores, expected);
            assert_eq!(
                triggered,
                expected
                    .iter()
                    .map(|score| *score != 0.0)
                    .collect::<Vec<_>>()
            );
        }
        let mut windowed = cached_rule("!(C > 10)");
        windowed.scope_way = ScopeWay::Each;
        windowed.scope_windows = 2;
        assert_eq!(
            evaluate_cached_rule_scores(&windowed, &mut runtime)
                .unwrap()
                .0,
            vec![0.0, 0.0, 1.0]
        );
    }
}
