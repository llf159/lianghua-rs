use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::expr::{
    eval::{Runtime, Value},
    parser::{BinaryOp, Expr, Stmt, Stmts, UnaryOp},
    validation::{
        estimate_expression_warmup, expression_input_identifiers, parse_expression_program,
        validate_expression_functions,
    },
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TrackDefinition {
    pub id: String,
    pub name: String,
    #[serde(default = "track_enabled")]
    pub enabled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start: Option<String>,
    pub ttl: usize,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub capture: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub invalidate: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub eligible: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub steps: Vec<TrackStep>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signal: Option<String>,
    /// Awarded once after the first signal, then held until the event ends.
    #[serde(default)]
    pub points: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub score_until: Option<String>,
}

fn track_enabled() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TrackStep {
    pub name: String,
    pub when: String,
    #[serde(default)]
    pub points: f64,
}

#[derive(Debug, Clone)]
pub struct TrackExpression {
    program: Stmts,
    inputs: HashSet<String>,
    warmup: usize,
    /// 每条语句求值前，已经由跟踪上下文（AGE、冻结值，或它们派生的局部量）决定的局部变量名。
    /// 这些值每天都会变，不能在整段历史上预计算。
    derived_locals: Vec<HashSet<String>>,
}

impl TrackExpression {
    pub fn compile(text: &str, context: &HashSet<String>) -> Result<Self, String> {
        let program = parse_expression_program(text)
            .map_err(|e| format!("表达式位置{}：{}", e.idx, e.msg))?;
        validate_expression_functions(&program)?;
        if program.item.is_empty() {
            return Err("跟踪表达式不能为空".to_string());
        }
        // 保留赋值语句本身，复用的局部量只求值一次；只有上下文派生的局部量被记下来，
        // 因为它们无法在整段历史上预计算。
        let mut derived = HashSet::new();
        let mut derived_locals = Vec::with_capacity(program.item.len());
        for stmt in &program.item {
            let (name, expr) = statement_parts(stmt);
            if name.is_some_and(|name| context.contains(name) || name.starts_with("__TRACK_")) {
                return Err("不能覆盖跟踪上下文变量".to_string());
            }
            derived_locals.push(derived.clone());
            validate_track_calls(expr, context, &derived)?;
            if let Some(name) = name
                && depends_on_track_context(expr, context, &derived)
            {
                derived.insert(name.clone());
            }
        }
        let inputs = expression_input_identifiers(&program);
        let warmup = estimate_expression_warmup(&program)?;
        Ok(Self {
            program,
            inputs,
            warmup,
            derived_locals,
        })
    }

    pub fn program(&self) -> &Stmts {
        &self.program
    }

    pub fn prepare(
        &self,
        rt: &Runtime,
        context: &HashSet<String>,
    ) -> Result<PreparedTrackExpression, String> {
        let mut values = HashMap::new();
        // 不依赖当日跟踪上下文的语句在整段历史上先算一次，后面的语句就能复用它。
        let mut full = rt.clone();
        let mut statements = Vec::with_capacity(self.program.item.len());
        for (index, stmt) in self.program.item.iter().enumerate() {
            let (name, expr) = statement_parts(stmt);
            let derived = &self.derived_locals[index];
            let contextual = depends_on_track_context(expr, context, derived);
            let mut pending = vec![expr];
            let mut checks = Vec::new();
            while let Some(node) = pending.pop() {
                match node {
                    Expr::Call { args, .. } => pending.extend(args),
                    Expr::Unary { rhs, .. } => pending.push(rhs),
                    Expr::Binary { lhs, rhs, .. } => {
                        pending.push(lhs);
                        pending.push(rhs);
                    }
                    _ => {}
                }
                // Preserve unavailable numeric intermediates before legacy boolean conversion can erase NULL.
                if matches!(
                    node,
                    Expr::Call { .. }
                        | Expr::Unary {
                            op: UnaryOp::Neg,
                            ..
                        }
                        | Expr::Binary {
                            op: BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div,
                            ..
                        }
                ) {
                    checks.push(Stmts {
                        item: vec![Stmt::Expr(prepare_expression(
                            node.clone(),
                            &full,
                            context,
                            derived,
                            &mut values,
                        )?)],
                    });
                }
            }
            let expr = prepare_expression(expr.clone(), &full, context, derived, &mut values)?;
            let program = Stmts {
                item: vec![match name {
                    Some(name) => Stmt::Assign {
                        name: name.clone(),
                        value: expr,
                    },
                    None => Stmt::Expr(expr),
                }],
            };
            if let Some(name) = name
                && !contextual
            {
                for (key, value) in &values {
                    full.vars.insert(key.clone(), value.clone());
                }
                full.eval_program(&program).map_err(|e| e.msg)?;
                if let Some(value) = full.vars.get_mut(name)
                    && let Value::NumSeries(series) = value
                {
                    *value = Value::SharedNumSeries(Arc::new(std::mem::take(series)));
                }
            }
            statements.push(PreparedTrackStatement { program, checks });
        }
        for input in self.inputs.iter().cloned() {
            if !context.contains(&input) && !values.contains_key(&input) {
                values.insert(
                    input.clone(),
                    rt.vars
                        .get(&input)
                        .cloned()
                        .ok_or_else(|| format!("跟踪引用未知字段：{input}"))?,
                );
            }
        }
        let required = self
            .inputs
            .iter()
            .filter(|name| !context.contains(*name))
            .map(|name| {
                rt.vars
                    .get(name)
                    .cloned()
                    .map(|value| (name.clone(), value))
                    .ok_or_else(|| format!("跟踪引用未知字段：{name}"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(PreparedTrackExpression {
            statements,
            values,
            required,
            warmup: self.warmup,
        })
    }
}

fn statement_parts(stmt: &Stmt) -> (Option<&String>, &Expr) {
    match stmt {
        Stmt::Assign { name, value } => (Some(name), value),
        Stmt::Expr(expr) => (None, expr),
    }
}

/// 表达式是否依赖当日跟踪上下文：直接引用上下文变量，或引用由上下文派生的局部量。
fn depends_on_track_context(
    expr: &Expr,
    context: &HashSet<String>,
    derived_locals: &HashSet<String>,
) -> bool {
    match expr {
        Expr::Ident(name) => context.contains(name) || derived_locals.contains(name),
        Expr::Call { args, .. } => args
            .iter()
            .any(|expr| depends_on_track_context(expr, context, derived_locals)),
        Expr::Unary { rhs, .. } => depends_on_track_context(rhs, context, derived_locals),
        Expr::Binary { lhs, rhs, .. } => {
            depends_on_track_context(lhs, context, derived_locals)
                || depends_on_track_context(rhs, context, derived_locals)
        }
        Expr::Number(_) => false,
    }
}

fn validate_track_calls(
    expr: &Expr,
    context: &HashSet<String>,
    derived_locals: &HashSet<String>,
) -> Result<(), String> {
    match expr {
        Expr::Call { name, args } => {
            let name = name.to_ascii_uppercase();
            if name == "LAST" {
                return Err("跟踪不能使用LAST读取整段末值，请用当前字段或REF".to_string());
            }
            if depends_on_track_context(expr, context, derived_locals)
                && matches!(name.as_str(), "EMA" | "SMA" | "BARSLAST" | "GET" | "GETD")
            {
                return Err(format!(
                    "{name}不能包含跟踪上下文；请先计算行情指标，再与冻结值比较"
                ));
            }
            // AGE and captures are current-event scalars, not historical series.
            if matches!(name.as_str(), "REF" | "REFD" | "CROSS")
                && args
                    .iter()
                    .any(|expr| depends_on_track_context(expr, context, derived_locals))
            {
                return Err(format!("{name}不能回看AGE或冻结值；请先回看行情再比较"));
            }
            if matches!(
                name.as_str(),
                "MA" | "SUM" | "STD" | "HHV" | "LLV" | "COUNT" | "EXIST" | "GRANK" | "LRANK"
            ) && args
                .get(1)
                .is_some_and(|expr| depends_on_track_context(expr, context, derived_locals))
            {
                return Err(format!(
                    "{name}的周期不能使用跟踪变量，请改用带固定上限的D函数"
                ));
            }
            if matches!(name.as_str(), "GTOPCOUNT" | "LTOPCOUNT")
                && args
                    .get(2)
                    .is_some_and(|expr| depends_on_track_context(expr, context, derived_locals))
            {
                return Err(format!(
                    "{name}的周期不能使用跟踪变量，请改用带固定上限的D函数"
                ));
            }
            if name == "RSV"
                && args
                    .get(3)
                    .is_some_and(|expr| depends_on_track_context(expr, context, derived_locals))
            {
                return Err("RSV的周期不能使用跟踪变量，请改用RSVD并提供固定上限".into());
            }
            for arg in args {
                validate_track_calls(arg, context, derived_locals)?;
            }
        }
        Expr::Unary { rhs, .. } => validate_track_calls(rhs, context, derived_locals)?,
        Expr::Binary { lhs, rhs, .. } => {
            validate_track_calls(lhs, context, derived_locals)?;
            validate_track_calls(rhs, context, derived_locals)?;
        }
        _ => {}
    }
    Ok(())
}

fn prepare_expression(
    expr: Expr,
    rt: &Runtime,
    context: &HashSet<String>,
    derived_locals: &HashSet<String>,
    values: &mut HashMap<String, Value>,
) -> Result<Expr, String> {
    if matches!(&expr, Expr::Call { .. })
        && !depends_on_track_context(&expr, context, derived_locals)
    {
        let mut runtime = rt.clone();
        let value = runtime
            .eval_program(&Stmts {
                item: vec![Stmt::Expr(expr)],
            })
            .map_err(|e| e.msg)?;
        let key = format!("__TRACK_{}", values.len());
        let value = match value {
            Value::NumSeries(series) => Value::SharedNumSeries(Arc::new(series)),
            other => other,
        };
        values.insert(key.clone(), value);
        return Ok(Expr::Ident(key));
    }
    Ok(match expr {
        Expr::Call { name, args } => Expr::Call {
            name,
            args: args
                .into_iter()
                .map(|expr| prepare_expression(expr, rt, context, derived_locals, values))
                .collect::<Result<_, _>>()?,
        },
        Expr::Unary { op, rhs } => Expr::Unary {
            op,
            rhs: Box::new(prepare_expression(
                *rhs,
                rt,
                context,
                derived_locals,
                values,
            )?),
        },
        Expr::Binary { op, lhs, rhs } => Expr::Binary {
            op,
            lhs: Box::new(prepare_expression(
                *lhs,
                rt,
                context,
                derived_locals,
                values,
            )?),
            rhs: Box::new(prepare_expression(
                *rhs,
                rt,
                context,
                derived_locals,
                values,
            )?),
        },
        other => other,
    })
}

struct PreparedTrackStatement {
    /// 恰好一条语句，逐日按声明顺序执行。
    program: Stmts,
    /// 这条语句里必须在当日可用的数值中间量。
    checks: Vec<Stmts>,
}

pub struct PreparedTrackExpression {
    statements: Vec<PreparedTrackStatement>,
    values: HashMap<String, Value>,
    required: Vec<(String, Value)>,
    warmup: usize,
}

pub fn value_at(value: &Value, index: usize) -> Option<f64> {
    match value {
        Value::Num(value) => Some(*value),
        Value::Bool(value) => Some(if *value { 1.0 } else { 0.0 }),
        Value::NumSeries(values) => values.get(index).copied().flatten(),
        Value::SharedNumSeries(values) => values.get(index).copied().flatten(),
        Value::BoolSeries(values) => values
            .get(index)
            .map(|value| if *value { 1.0 } else { 0.0 }),
    }
    .filter(|value| value.is_finite())
}

impl PreparedTrackExpression {
    pub fn evaluate(
        &self,
        index: usize,
        context: &BTreeMap<String, f64>,
    ) -> Result<Option<f64>, String> {
        if index < self.warmup {
            return Ok(None);
        }
        let start = index - self.warmup;
        // Missing evidence must not turn into a successful NOT/! comparison.
        if self
            .required
            .iter()
            .any(|(_, value)| (start..=index).any(|day| value_at(value, day).is_none()))
        {
            return Ok(None);
        }
        let mut rt = Runtime::default();
        for (name, value) in &self.values {
            let value = match value {
                Value::NumSeries(series) => {
                    let Some(slice) = series.get(start..=index) else {
                        return Ok(None);
                    };
                    Value::NumSeries(slice.to_vec())
                }
                Value::SharedNumSeries(series) => {
                    let Some(slice) = series.get(start..=index) else {
                        return Ok(None);
                    };
                    Value::NumSeries(slice.to_vec())
                }
                Value::BoolSeries(series) => {
                    let Some(slice) = series.get(start..=index) else {
                        return Ok(None);
                    };
                    Value::BoolSeries(slice.to_vec())
                }
                other => other.clone(),
            };
            rt.vars.insert(name.clone(), value);
        }
        for (name, value) in context {
            rt.vars.insert(name.clone(), Value::Num(*value));
        }
        let mut result = None;
        for statement in &self.statements {
            let value = rt.eval_program(&statement.program).map_err(|e| e.msg)?;
            for check in &statement.checks {
                let value = rt.eval_program(check).map_err(|e| e.msg)?;
                if value_at(&value, Value::len_of(&value).saturating_sub(1)).is_none() {
                    return Ok(None);
                }
            }
            result = value_at(&value, Value::len_of(&value).saturating_sub(1));
        }
        Ok(result)
    }
}

#[derive(Debug, Clone)]
pub struct TrackProgram {
    pub start: Option<TrackExpression>,
    pub capture: BTreeMap<String, TrackExpression>,
    pub invalidate: TrackExpression,
    pub eligible: TrackExpression,
    pub steps: Vec<TrackExpression>,
    pub signal: TrackExpression,
    pub score_until: Option<TrackExpression>,
    pub context: HashSet<String>,
}

impl TrackProgram {
    pub fn compile(definition: &TrackDefinition) -> Result<Self, String> {
        if definition.id.trim().is_empty()
            || !definition
                .id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
        {
            return Err("跟踪ID只能包含字母、数字、下划线或连字符".to_string());
        }
        if definition.name.trim().is_empty() || definition.ttl == 0 {
            return Err("跟踪名称不能为空，期限必须大于0".to_string());
        }
        if definition
            .source
            .as_deref()
            .is_some_and(|s| s.trim().is_empty())
            || definition
                .start
                .as_deref()
                .is_some_and(|s| s.trim().is_empty())
            || definition.source.is_some() == definition.start.is_some()
        {
            return Err("跟踪须选择原规则或填写开始表达式，且只能选择一种起点".to_string());
        }
        for name in definition.capture.keys() {
            if !name.starts_with("A_")
                || !name
                    .bytes()
                    .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
            {
                return Err("冻结变量名必须以A_开头，只包含大写字母、数字和下划线".to_string());
            }
        }
        let mut context = HashSet::from(
            [
                "AGE",
                "TRIGGER_SCORE",
                "SCORE_AGE",
                "SIGNAL_AGE",
                "COMPLETED_STEPS",
            ]
            .map(str::to_string),
        );
        context.extend(definition.capture.keys().cloned());
        let empty = HashSet::new();
        let start = definition
            .start
            .as_deref()
            .map(|s| TrackExpression::compile(s, &empty))
            .transpose()?;
        let capture = definition
            .capture
            .iter()
            .map(|(name, expr)| Ok((name.clone(), TrackExpression::compile(expr, &empty)?)))
            .collect::<Result<BTreeMap<_, _>, String>>()?;
        if start
            .iter()
            .chain(capture.values())
            .any(|expr| expr.inputs.iter().any(|name| context.contains(name)))
        {
            return Err("起点和冻结表达式不能读取尚未创建的跟踪上下文".to_string());
        }
        if !definition.points.is_finite()
            || definition.steps.iter().any(|step| !step.points.is_finite())
            || !definition
                .steps
                .iter()
                .fold(definition.points.abs(), |sum, step| sum + step.points.abs())
                .is_finite()
        {
            return Err("跟踪分值及其合计必须为有限数值".into());
        }
        let mut names = HashSet::new();
        let steps = definition
            .steps
            .iter()
            .map(|step| {
                if step.name.trim().is_empty() || !names.insert(step.name.trim()) {
                    return Err("观察步骤名称不能为空或重复".to_string());
                }
                TrackExpression::compile(&step.when, &context)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            score_until: definition
                .score_until
                .as_deref()
                .map(|s| TrackExpression::compile(s, &context))
                .transpose()?,
            start,
            capture,
            context: context.clone(),
            steps,
            invalidate: TrackExpression::compile(
                definition.invalidate.as_deref().unwrap_or("0"),
                &context,
            )?,
            eligible: TrackExpression::compile(
                definition.eligible.as_deref().unwrap_or("1"),
                &context,
            )?,
            signal: TrackExpression::compile(
                definition.signal.as_deref().unwrap_or("1"),
                &context,
            )?,
        })
    }

    pub fn expressions(&self) -> Vec<&TrackExpression> {
        self.start
            .iter()
            .chain(self.capture.values())
            .chain([&self.invalidate, &self.eligible, &self.signal])
            .chain(self.steps.iter())
            .chain(self.score_until.iter())
            .collect()
    }
}

impl TrackDefinition {
    pub fn expired(&self, event: &TrackEvent, session: usize) -> bool {
        session.saturating_sub(event.started) > self.ttl
            && (event.stopped.is_some()
                || self.score_until.is_none()
                || event.score_started.is_none())
    }
}

pub struct PreparedTrackProgram {
    start: Option<PreparedTrackExpression>,
    capture: BTreeMap<String, PreparedTrackExpression>,
    invalidate: PreparedTrackExpression,
    eligible: PreparedTrackExpression,
    steps: Vec<PreparedTrackExpression>,
    signal: PreparedTrackExpression,
    score_until: Option<PreparedTrackExpression>,
}

impl TrackProgram {
    pub fn prepare(&self, rt: &Runtime) -> Result<PreparedTrackProgram, String> {
        Ok(PreparedTrackProgram {
            start: self
                .start
                .as_ref()
                .map(|expr| expr.prepare(rt, &self.context))
                .transpose()?,
            capture: self
                .capture
                .iter()
                .map(|(name, expr)| Ok((name.clone(), expr.prepare(rt, &self.context)?)))
                .collect::<Result<_, String>>()?,
            invalidate: self.invalidate.prepare(rt, &self.context)?,
            eligible: self.eligible.prepare(rt, &self.context)?,
            steps: self
                .steps
                .iter()
                .map(|expr| expr.prepare(rt, &self.context))
                .collect::<Result<_, _>>()?,
            signal: self.signal.prepare(rt, &self.context)?,
            score_until: self
                .score_until
                .as_ref()
                .map(|expr| expr.prepare(rt, &self.context))
                .transpose()?,
        })
    }
}

impl PreparedTrackProgram {
    pub fn advance(
        &self,
        state: &mut TrackState,
        definition: &TrackDefinition,
        session: usize,
        index: Option<usize>,
        rt: &Runtime,
        source_score: Option<f64>,
    ) -> Result<f64, String> {
        let mut evidence = TrackEvidence {
            quoted: index.is_some_and(|i| rt.vars.get("C").and_then(|v| value_at(v, i)).is_some()),
            ..Default::default()
        };
        let existing = state
            .event
            .as_ref()
            .filter(|event| !definition.expired(event, session));
        if let Some(index) = index.filter(|_| evidence.quoted) {
            if let Some(event) = existing {
                if event.stopped.is_none() {
                    let mut context = event.captured.clone();
                    context.insert("AGE".into(), (session - event.started) as f64);
                    context.insert("TRIGGER_SCORE".into(), event.source_score.unwrap_or(0.0));
                    context.insert(
                        "SCORE_AGE".into(),
                        event
                            .score_started
                            .map_or(-1.0, |started| (session - started) as f64),
                    );
                    context.insert(
                        "SIGNAL_AGE".into(),
                        event
                            .first_signal
                            .map_or(-1.0, |started| (session - started) as f64),
                    );
                    context.insert("COMPLETED_STEPS".into(), event.completed.len() as f64);
                    evidence.invalidate =
                        self.invalidate.evaluate(index, &context)?.map(|v| v != 0.0);
                    if evidence.invalidate == Some(false) {
                        evidence.score_until = self
                            .score_until
                            .as_ref()
                            .map(|expr| expr.evaluate(index, &context))
                            .transpose()?
                            .flatten()
                            .map(|v| v != 0.0);
                        if session - event.started <= definition.ttl {
                            evidence.eligible =
                                self.eligible.evaluate(index, &context)?.map(|v| v != 0.0);
                            if evidence.eligible == Some(true) {
                                if let Some(step) = self.steps.get(event.completed.len()) {
                                    evidence.step =
                                        step.evaluate(index, &context)?.map(|v| v != 0.0);
                                } else {
                                    evidence.signal =
                                        self.signal.evaluate(index, &context)?.map(|v| v != 0.0);
                                }
                            }
                        }
                    }
                }
            } else {
                evidence.source_score = source_score.filter(|v| v.is_finite());
                evidence.start = if let Some(start) = &self.start {
                    start
                        .evaluate(index, &BTreeMap::new())?
                        .is_some_and(|v| v != 0.0)
                } else {
                    evidence.source_score.is_some_and(|v| v > 0.0)
                };
                if evidence.start {
                    evidence.captured = self
                        .capture
                        .iter()
                        .map(|(name, expr)| {
                            expr.evaluate(index, &BTreeMap::new())
                                .map(|v| v.map(|v| (name.clone(), v)))
                        })
                        .collect::<Result<Option<BTreeMap<_, _>>, _>>()?;
                }
            }
        }
        state.advance(session, definition, evidence);
        Ok(state
            .event
            .as_ref()
            .filter(|event| event.stopped.is_none())
            .map_or(0.0, |event| event.score))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct TrackState {
    pub event: Option<TrackEvent>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TrackEvent {
    pub started: usize,
    pub captured: BTreeMap<String, f64>,
    pub source_score: Option<f64>,
    pub completed: Vec<usize>,
    pub stopped: Option<usize>,
    pub reason: Option<String>,
    pub first_signal: Option<usize>,
    pub signal_today: bool,
    pub eligible_today: bool,
    #[serde(default)]
    pub score: f64,
    #[serde(default)]
    pub score_started: Option<usize>,
}

#[derive(Default)]
pub struct TrackEvidence {
    pub quoted: bool,
    pub start: bool,
    pub captured: Option<BTreeMap<String, f64>>,
    pub source_score: Option<f64>,
    pub invalidate: Option<bool>,
    pub eligible: Option<bool>,
    pub step: Option<bool>,
    pub signal: Option<bool>,
    pub score_until: Option<bool>,
}

impl TrackState {
    pub fn advance(
        &mut self,
        session: usize,
        definition: &TrackDefinition,
        evidence: TrackEvidence,
    ) {
        if self
            .event
            .as_ref()
            .is_some_and(|event| definition.expired(event, session))
        {
            self.event = None;
        }
        let Some(event) = self.event.as_mut() else {
            if evidence.quoted
                && evidence.start
                && let Some(captured) = evidence.captured
            {
                self.event = Some(TrackEvent {
                    started: session,
                    captured,
                    source_score: evidence.source_score,
                    completed: Vec::new(),
                    stopped: None,
                    reason: None,
                    first_signal: None,
                    signal_today: false,
                    eligible_today: false,
                    score: 0.0,
                    score_started: None,
                });
            }
            return;
        };
        event.signal_today = false;
        event.eligible_today = false;
        if event.stopped.is_some() || session == event.started {
            return;
        }
        let observing = session - event.started <= definition.ttl;
        let ready_before_today = event.completed.len() == definition.steps.len();
        let reason = if !evidence.quoted {
            Some("missing_quote")
        } else if evidence.invalidate.is_none() {
            Some("missing_evidence")
        } else if evidence.invalidate == Some(true) {
            Some("invalidated")
        } else if definition.score_until.is_some() && evidence.score_until.is_none() {
            Some("missing_evidence")
        } else if evidence.score_until == Some(true) {
            Some("score_ended")
        } else if observing && evidence.eligible.is_none() {
            Some("missing_evidence")
        } else if observing
            && evidence.eligible == Some(true)
            && ((ready_before_today && evidence.signal.is_none())
                || (!ready_before_today && evidence.step.is_none()))
        {
            Some("missing_evidence")
        } else {
            None
        };
        if let Some(reason) = reason {
            event.stopped = Some(session);
            event.reason = Some(reason.to_string());
            return;
        }
        if !observing {
            return;
        }
        event.eligible_today = evidence.eligible == Some(true);
        if !event.eligible_today {
            return;
        }
        if ready_before_today && evidence.signal == Some(true) {
            event.signal_today = true;
            if event.first_signal.is_none() {
                event.first_signal = Some(session);
                event.score += definition.points;
                if definition.points != 0.0 {
                    event.score_started.get_or_insert(session);
                }
            }
        }
        if !ready_before_today && evidence.step == Some(true) {
            let points = definition.steps[event.completed.len()].points;
            event.score += points;
            if points != 0.0 {
                event.score_started.get_or_insert(session);
            }
            event.completed.push(session);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_order_invalidation_missing_and_resume() {
        let definition = TrackDefinition {
            id: "test".into(),
            name: "test".into(),
            enabled: true,
            source: None,
            start: Some("1".into()),
            ttl: 5,
            capture: BTreeMap::new(),
            invalidate: None,
            eligible: None,
            steps: vec![TrackStep {
                name: "step".into(),
                when: "1".into(),
                points: 0.0,
            }],
            signal: None,
            points: 0.0,
            score_until: None,
        };
        let mut state = TrackState::default();
        state.advance(
            0,
            &definition,
            TrackEvidence {
                quoted: true,
                start: true,
                captured: Some(BTreeMap::from([("A_LOW".into(), 10.0)])),
                source_score: Some(7.0),
                ..Default::default()
            },
        );
        let day = || TrackEvidence {
            quoted: true,
            start: true,
            captured: Some(BTreeMap::new()),
            invalidate: Some(false),
            eligible: Some(true),
            step: Some(true),
            signal: Some(true),
            ..Default::default()
        };
        state.advance(1, &definition, day());
        assert_eq!(state.event.as_ref().unwrap().completed, vec![1]);
        assert!(!state.event.as_ref().unwrap().signal_today);
        let mut resumed = state.clone();
        state.advance(2, &definition, day());
        resumed.advance(2, &definition, day());
        assert_eq!(state, resumed);
        assert_eq!(state.event.as_ref().unwrap().captured["A_LOW"], 10.0);
        assert!(state.event.as_ref().unwrap().signal_today);
        state.advance(
            3,
            &definition,
            TrackEvidence {
                invalidate: Some(true),
                ..day()
            },
        );
        state.advance(4, &definition, day());
        assert_eq!(
            state.event.as_ref().unwrap().reason.as_deref(),
            Some("invalidated")
        );
        assert!(!state.event.as_ref().unwrap().signal_today);
        state.advance(6, &definition, day());
        assert_eq!(state.event.as_ref().unwrap().started, 6);
        state.advance(7, &definition, TrackEvidence::default());
        assert_eq!(
            state.event.as_ref().unwrap().reason.as_deref(),
            Some("missing_quote")
        );
        state.advance(12, &definition, TrackEvidence::default());
        assert!(state.event.is_none());
    }

    #[test]
    fn awards_latch_and_end_by_expression_after_observation_window() {
        let definition = TrackDefinition {
            id: "hold".into(),
            name: "hold".into(),
            enabled: true,
            source: None,
            start: Some("C = 10".into()),
            ttl: 2,
            capture: BTreeMap::new(),
            invalidate: Some("C < 5".into()),
            eligible: Some("C > 8".into()),
            steps: vec![TrackStep {
                name: "progress".into(),
                when: "C > 10".into(),
                points: 2.0,
            }],
            signal: Some("C > 11".into()),
            points: 3.0,
            score_until: Some("SCORE_AGE >= 4".into()),
        };
        let rt = Runtime {
            vars: HashMap::from([(
                "C".into(),
                Value::NumSeries(vec![
                    Some(10.0),
                    Some(11.0),
                    Some(12.0),
                    Some(7.0),
                    Some(9.0),
                    Some(9.0),
                ]),
            )]),
        };
        let program = TrackProgram::compile(&definition)
            .unwrap()
            .prepare(&rt)
            .unwrap();
        let mut state = TrackState::default();
        let scores = (0..6)
            .map(|day| {
                program
                    .advance(&mut state, &definition, day, Some(day), &rt, None)
                    .unwrap()
            })
            .collect::<Vec<_>>();
        assert_eq!(scores, vec![0.0, 2.0, 5.0, 5.0, 5.0, 0.0]);
        assert_eq!(
            state.event.as_ref().unwrap().reason.as_deref(),
            Some("score_ended")
        );
        // The same retained points must disappear immediately on invalidation or missing quotes.
        for evidence in [
            TrackEvidence {
                quoted: true,
                invalidate: Some(true),
                ..Default::default()
            },
            TrackEvidence::default(),
        ] {
            let mut invalid = TrackState {
                event: Some(TrackEvent {
                    started: 0,
                    captured: BTreeMap::new(),
                    source_score: None,
                    completed: vec![1],
                    stopped: None,
                    reason: None,
                    first_signal: Some(2),
                    signal_today: true,
                    eligible_today: true,
                    score: 5.0,
                    score_started: Some(1),
                }),
            };
            invalid.advance(3, &definition, evidence);
            assert!(invalid.event.as_ref().unwrap().stopped.is_some());
        }
    }

    #[test]
    fn expressions_are_causal_and_missing_is_unknown() {
        let rt = Runtime {
            vars: HashMap::from([(
                "C".into(),
                Value::SharedNumSeries(Arc::new(vec![
                    Some(10.0),
                    Some(8.0),
                    Some(12.0),
                    None,
                    Some(99.0),
                ])),
            )]),
        };
        let context = HashSet::from(["A_LOW".into(), "AGE".into()]);
        let compiled = TrackExpression::compile(
            "X := MA(C,2); X > A_LOW AND COUNT(C > A_LOW,2) >= 1",
            &context,
        )
        .unwrap();
        let prepared = compiled.prepare(&rt, &context).unwrap();
        let values = BTreeMap::from([("A_LOW".into(), 9.0), ("AGE".into(), 2.0)]);
        assert_eq!(prepared.evaluate(2, &values).unwrap(), Some(1.0));
        assert_eq!(prepared.evaluate(3, &values).unwrap(), None);
        let missing = TrackExpression::compile("!(C > A_LOW)", &context)
            .unwrap()
            .prepare(&rt, &context)
            .unwrap();
        assert_eq!(missing.evaluate(3, &values).unwrap(), None);
        let unavailable = TrackExpression::compile("!(GET(C > 100,C,2) > A_LOW)", &context)
            .unwrap()
            .prepare(&rt, &context)
            .unwrap();
        assert_eq!(unavailable.evaluate(2, &values).unwrap(), None);
        let beyond_limit = TrackExpression::compile("!COUNTD(C > A_LOW, AGE + 1, 2)", &context)
            .unwrap()
            .prepare(&rt, &context)
            .unwrap();
        assert_eq!(beyond_limit.evaluate(2, &values).unwrap(), None);
        assert!(TrackExpression::compile("LAST(C,0) > 1", &context).is_err());
        assert!(TrackExpression::compile("REF(AGE,1)>0", &context).is_err());
        let prefix = Runtime {
            vars: HashMap::from([(
                "C".into(),
                Value::NumSeries(vec![Some(10.0), Some(8.0), Some(12.0)]),
            )]),
        };
        assert_eq!(
            compiled
                .prepare(&prefix, &context)
                .unwrap()
                .evaluate(2, &values)
                .unwrap(),
            prepared.evaluate(2, &values).unwrap()
        );
    }
}
