use std::cell::{OnceCell, RefCell};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::expr::{
    eval::{DayValue, Runtime, Value, day_value_as_num},
    parser::{Expr, Stmt, Stmts},
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
    /// Optional explicit trigger; source still identifies the score's scene and stage.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start: Option<String>,
    pub ttl: usize,
    /// Maximum event age from its anchor; omitted legacy values use ttl.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_age: Option<usize>,
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
        let mut full = Runtime::default();
        for name in self.inputs.iter().filter(|name| !context.contains(*name)) {
            let value = rt
                .vars
                .get(name)
                .ok_or_else(|| format!("跟踪引用未知字段：{name}"))?;
            full.vars.insert(name.clone(), value.clone().into_shared());
        }
        let required = full
            .vars
            .iter()
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect::<Vec<_>>();
        let mut statements = Vec::with_capacity(self.program.item.len());
        for (index, stmt) in self.program.item.iter().enumerate() {
            let (name, expr) = statement_parts(stmt);
            let derived = &self.derived_locals[index];
            let contextual = depends_on_track_context(expr, context, derived);
            let expr = prepare_expression(expr.clone(), &mut full, context, derived, &mut values)?;
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
                if let Some(value) = full.vars.get_mut(name) {
                    *value = std::mem::replace(value, Value::Num(0.0)).into_shared();
                }
            }
            statements.push(program);
        }
        for (name, value) in &required {
            values.entry(name.clone()).or_insert_with(|| value.clone());
        }
        // Remaining calls depend on event context and still need historical windows.
        let scalar = statements.iter().all(|program| {
            let mut pending = program
                .item
                .iter()
                .map(|stmt| statement_parts(stmt).1)
                .collect::<Vec<_>>();
            while let Some(expr) = pending.pop() {
                match expr {
                    Expr::Call { .. } => return false,
                    Expr::Unary { rhs, .. } => pending.push(rhs),
                    Expr::Binary { lhs, rhs, .. } => {
                        pending.push(lhs);
                        pending.push(rhs);
                    }
                    _ => {}
                }
            }
            true
        });
        Ok(PreparedTrackExpression {
            statements,
            scalar,
            locals: RefCell::new(HashMap::with_capacity(
                context.len() + self.program.item.len(),
            )),
            runtime: RefCell::new(Runtime {
                vars: values.clone(),
            }),
            values,
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
    rt: &mut Runtime,
    context: &HashSet<String>,
    derived_locals: &HashSet<String>,
    values: &mut HashMap<String, Value>,
) -> Result<Expr, String> {
    if matches!(&expr, Expr::Call { .. })
        && !depends_on_track_context(&expr, context, derived_locals)
    {
        let value = rt
            .eval_program(&Stmts {
                item: vec![Stmt::Expr(expr)],
            })
            .map_err(|e| e.msg)?;
        let key = format!("__TRACK_{}", values.len());
        let value = value.into_shared();
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

pub struct PreparedTrackExpression {
    statements: Vec<Stmts>,
    values: HashMap<String, Value>,
    runtime: RefCell<Runtime>,
    scalar: bool,
    locals: RefCell<HashMap<String, DayValue>>,
    warmup: usize,
}

pub fn value_at(value: &Value, index: usize) -> Option<f64> {
    match value {
        Value::Num(value) => Some(*value),
        Value::Bool(value) => value.as_num(),
        Value::NumSeries(values) => values.get(index).copied().flatten(),
        Value::SharedNumSeries(values) => values.get(index).copied().flatten(),
        Value::NumWindow { values, range } => values[range.clone()].get(index).copied().flatten(),
        Value::BoolSeries(values) => values.get(index).and_then(|value| value.as_num()),
    }
    .filter(|value| value.is_finite())
}

impl PreparedTrackExpression {
    pub fn evaluate(
        &self,
        index: usize,
        context: &BTreeMap<String, f64>,
    ) -> Result<Option<f64>, String> {
        let start = index.saturating_sub(self.warmup);
        if self.scalar {
            let rt = self.runtime.borrow();
            let mut locals = self.locals.borrow_mut();
            // Retain allocated names while preventing prior-day assignments from shadowing inputs.
            locals
                .values_mut()
                .for_each(|value| *value = DayValue::Unset);
            for (name, value) in context {
                if let Some(slot) = locals.get_mut(name) {
                    *slot = DayValue::Num(Some(*value));
                } else {
                    locals.insert(name.clone(), DayValue::Num(Some(*value)));
                }
            }
            let mut result = None;
            for statement in &self.statements {
                result = rt
                    .eval_program_scalar_at(statement, index, &mut locals)
                    .map_err(|e| e.msg)?
                    .and_then(day_value_as_num);
            }
            return Ok(result.filter(|value| value.is_finite()));
        }
        let mut rt = self.runtime.borrow_mut();
        for (name, value) in &self.values {
            let value = match value {
                Value::SharedNumSeries(series) => {
                    if index >= series.len() {
                        return Ok(None);
                    }
                    Value::NumWindow {
                        values: Arc::clone(series),
                        range: start..index + 1,
                    }
                }
                Value::NumWindow { values, range } => {
                    if index >= range.len() {
                        return Ok(None);
                    }
                    Value::NumWindow {
                        values: Arc::clone(values),
                        range: range.start + start..range.start + index + 1,
                    }
                }
                other => other.clone(),
            };
            if let Some(slot) = rt.vars.get_mut(name) {
                *slot = value;
            } else {
                rt.vars.insert(name.clone(), value);
            }
        }
        for (name, value) in context {
            if let Some(slot) = rt.vars.get_mut(name) {
                *slot = Value::Num(*value);
            } else {
                rt.vars.insert(name.clone(), Value::Num(*value));
            }
        }
        let mut result = None;
        for statement in &self.statements {
            let value = rt.eval_program(statement).map_err(|e| e.msg)?;
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
    /// Replays the last max_age market sessions independently for each output date.
    /// Parallel window states preserve single-event suppression inside each window,
    /// without letting an older suppressed trigger determine today's anchor.
    pub fn rolling_scores(
        &self,
        definition: &TrackDefinition,
        rt: &Runtime,
        dates: &[String],
        calendar: &[String],
        start: &str,
        source_scores: Option<&[f64]>,
    ) -> Result<(Vec<f64>, TrackState), String> {
        let mut scores = vec![0.0; dates.len()];
        let first_output = calendar.partition_point(|date| date.as_str() < start);
        let begin = first_output.saturating_sub(definition.max_age());
        let prepared = self.prepare(rt)?;
        let mut windows = VecDeque::<(usize, TrackState)>::new();
        let mut index =
            dates.partition_point(|date| calendar.get(begin).is_some_and(|first| date < first));
        for (session, date) in calendar.iter().enumerate().skip(begin) {
            while index < dates.len() && dates[index] < *date {
                index += 1;
            }
            let quoted_index = (dates.get(index) == Some(date)).then_some(index);
            let window_start = session.saturating_sub(definition.max_age());
            while windows
                .front()
                .is_some_and(|(origin, _)| *origin < window_start)
            {
                windows.pop_front();
            }
            if session
                <= calendar
                    .len()
                    .saturating_sub(1)
                    .saturating_sub(definition.max_age())
            {
                windows.push_back((session, TrackState::default()));
            }
            for (window, (_, state)) in windows.iter_mut().enumerate() {
                let score = prepared.advance(
                    state,
                    definition,
                    session,
                    quoted_index,
                    rt,
                    quoted_index
                        .and_then(|i| source_scores.and_then(|values| values.get(i)))
                        .copied(),
                )?;
                if window == 0
                    && session >= first_output
                    && let Some(index) = quoted_index
                {
                    // The oldest surviving window is exactly [session-max_age, session].
                    // Later windows are retained only to produce future output days.
                    scores[index] = score;
                }
            }
        }
        Ok((
            scores,
            windows
                .pop_front()
                .map(|(_, state)| state)
                .unwrap_or_default(),
        ))
    }

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
        if definition.max_age() < definition.ttl {
            return Err("跟踪总期限不能小于观察期限".to_string());
        }
        if definition
            .source
            .as_deref()
            .is_some_and(|s| s.trim().is_empty())
            || definition
                .start
                .as_deref()
                .is_some_and(|s| s.trim().is_empty())
            || (definition.source.is_none() && definition.start.is_none())
        {
            return Err(
                "跟踪须关联原规则或填写开始表达式；已填写时以开始表达式建立起点".to_string(),
            );
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
    pub fn score_name(&self) -> String {
        format!("跟踪·{}", self.name)
    }

    pub fn max_age(&self) -> usize {
        self.max_age.unwrap_or(self.ttl)
    }

    pub fn expired(&self, event: &TrackEvent, session: usize) -> bool {
        let age = session.saturating_sub(event.started);
        age > self.max_age()
            || (age > self.ttl && (event.stopped.is_some() || event.score_started.is_none()))
    }
}

pub struct PreparedTrackProgram<'a> {
    start: Option<PreparedTrackExpression>,
    capture: &'a BTreeMap<String, TrackExpression>,
    context: &'a HashSet<String>,
    prepared_capture: OnceCell<BTreeMap<String, PreparedTrackExpression>>,
    event_context: RefCell<BTreeMap<String, f64>>,
    invalidate: PreparedTrackExpression,
    eligible: PreparedTrackExpression,
    steps: Vec<PreparedTrackExpression>,
    signal: PreparedTrackExpression,
    score_until: Option<PreparedTrackExpression>,
}

impl TrackProgram {
    pub fn prepare(&self, rt: &Runtime) -> Result<PreparedTrackProgram<'_>, String> {
        Ok(PreparedTrackProgram {
            start: self
                .start
                .as_ref()
                .map(|expr| expr.prepare(rt, &self.context))
                .transpose()?,
            capture: &self.capture,
            context: &self.context,
            prepared_capture: OnceCell::new(),
            event_context: RefCell::new(BTreeMap::new()),
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

impl PreparedTrackProgram<'_> {
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
                    let mut context = self.event_context.borrow_mut();
                    for (name, value) in &event.captured {
                        if let Some(slot) = context.get_mut(name) {
                            *slot = *value;
                        } else {
                            context.insert(name.clone(), *value);
                        }
                    }
                    for (name, value) in [
                        ("AGE", (session - event.started) as f64),
                        ("TRIGGER_SCORE", event.source_score.unwrap_or(0.0)),
                        (
                            "SCORE_AGE",
                            event
                                .score_started
                                .map_or(-1.0, |started| (session - started) as f64),
                        ),
                        (
                            "SIGNAL_AGE",
                            event
                                .first_signal
                                .map_or(-1.0, |started| (session - started) as f64),
                        ),
                        ("COMPLETED_STEPS", event.completed.len() as f64),
                    ] {
                        if let Some(slot) = context.get_mut(name) {
                            *slot = value;
                        } else {
                            context.insert(name.into(), value);
                        }
                    }
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
                    if self.prepared_capture.get().is_none() {
                        let prepared = self
                            .capture
                            .iter()
                            .map(|(name, expr)| Ok((name.clone(), expr.prepare(rt, self.context)?)))
                            .collect::<Result<_, String>>()?;
                        let _ = self.prepared_capture.set(prepared);
                    }
                    evidence.captured = self
                        .prepared_capture
                        .get()
                        .unwrap()
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
            max_age: None,
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
            source: Some("discovery".into()),
            start: Some("C = 10".into()),
            ttl: 2,
            max_age: Some(6),
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
        let compiled = TrackProgram::compile(&definition).unwrap();
        let program = compiled.prepare(&rt).unwrap();
        let mut state = TrackState::default();
        let scores = (0..6)
            .map(|day| {
                program
                    .advance(&mut state, &definition, day, Some(day), &rt, Some(0.0))
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
    fn awarded_points_survive_ttl_until_logic_is_invalidated() {
        let mut definition = TrackDefinition {
            id: "logic".into(),
            name: "logic".into(),
            enabled: true,
            source: Some("rule".into()),
            start: None,
            ttl: 2,
            max_age: Some(4),
            capture: BTreeMap::from([("A_LOW".into(), "C * 0.9".into())]),
            invalidate: Some("C < A_LOW".into()),
            eligible: Some("C <= 11".into()),
            steps: vec![TrackStep {
                name: "progress".into(),
                when: "C > 10".into(),
                points: 2.0,
            }],
            signal: Some("C > 11".into()),
            points: 3.0,
            score_until: None,
        };
        let rt = Runtime {
            vars: HashMap::from([(
                "C".into(),
                Value::NumSeries(vec![
                    Some(10.0),
                    Some(11.0),
                    Some(10.0),
                    Some(12.0),
                    Some(8.0),
                    Some(10.0),
                ]),
            )]),
        };
        let compiled = TrackProgram::compile(&definition).unwrap();
        let program = compiled.prepare(&rt).unwrap();
        let mut state = TrackState::default();
        let mut resumed = TrackState::default();
        for day in 0..6 {
            let score = program
                .advance(&mut state, &definition, day, Some(day), &rt, Some(6.0))
                .unwrap();
            assert_eq!(score, [0.0, 2.0, 2.0, 2.0, 0.0, 0.0][day]);
            if day == 2 {
                resumed = state.clone();
            } else if day > 2 {
                compiled
                    .prepare(&rt)
                    .unwrap()
                    .advance(&mut resumed, &definition, day, Some(day), &rt, Some(6.0))
                    .unwrap();
                assert_eq!(state, resumed);
            }
            if day == 3 {
                let event = state.event.as_ref().unwrap();
                assert_eq!(event.started, 0);
                assert_eq!(event.completed, vec![1]);
                assert_eq!(event.first_signal, None);
                assert!(!definition.expired(event, day));
            }
            if day == 4 {
                assert_eq!(
                    state.event.as_ref().unwrap().reason.as_deref(),
                    Some("invalidated")
                );
            }
        }
        assert_eq!(state.event.as_ref().unwrap().started, 5);
        // A zero-point observation still expires; TTL does not create an immortal waiting event.
        definition.steps[0].points = 0.0;
        let compiled = TrackProgram::compile(&definition).unwrap();
        let program = compiled.prepare(&rt).unwrap();
        let mut state = TrackState::default();
        for day in 0..4 {
            program
                .advance(&mut state, &definition, day, Some(day), &rt, Some(6.0))
                .unwrap();
        }
        assert_eq!(state.event.as_ref().unwrap().started, 3);
    }

    #[test]
    fn bounded_tracking_expires_and_daily_replay_matches_ranges() {
        let mut definition = TrackDefinition {
            id: "bounded".into(),
            name: "bounded".into(),
            enabled: true,
            source: Some("rule".into()),
            start: None,
            ttl: 2,
            max_age: Some(4),
            capture: BTreeMap::new(),
            invalidate: None,
            eligible: None,
            steps: Vec::new(),
            signal: None,
            points: 3.0,
            score_until: None,
        };
        let calendar = (0..12).map(|i| format!("{i:08}")).collect::<Vec<_>>();
        let rt = Runtime {
            vars: HashMap::from([("C".into(), Value::NumSeries(vec![Some(10.0); 12]))]),
        };
        let sources = vec![1.0; 12];
        let program = TrackProgram::compile(&definition).unwrap();
        let prepared = program.prepare(&rt).unwrap();
        let mut state = TrackState::default();
        for day in 0..=5 {
            let score = prepared
                .advance(&mut state, &definition, day, Some(day), &rt, Some(1.0))
                .unwrap();
            assert_eq!(score, if day == 0 || day == 5 { 0.0 } else { 3.0 });
        }
        assert_eq!(state.event.as_ref().unwrap().started, 5);
        let (range, _) = program
            .rolling_scores(
                &definition,
                &rt,
                &calendar,
                &calendar,
                &calendar[3],
                Some(&sources),
            )
            .unwrap();
        for day in 3..12 {
            let (daily, _) = program
                .rolling_scores(
                    &definition,
                    &rt,
                    &calendar[..=day],
                    &calendar[..=day],
                    &calendar[day],
                    Some(&sources[..=day]),
                )
                .unwrap();
            assert_eq!(range[day], daily[day]);
        }
        // A stop before the rolling window must not suppress a recent trigger.
        definition.invalidate = Some("AGE >= 2".into());
        let program = TrackProgram::compile(&definition).unwrap();
        let (range, _) = program
            .rolling_scores(
                &definition,
                &rt,
                &calendar,
                &calendar,
                &calendar[0],
                Some(&sources),
            )
            .unwrap();
        for day in 0..12 {
            let (daily, _) = program
                .rolling_scores(
                    &definition,
                    &rt,
                    &calendar[..=day],
                    &calendar[..=day],
                    &calendar[day],
                    Some(&sources[..=day]),
                )
                .unwrap();
            assert_eq!(range[day], daily[day]);
        }
        let dates = calendar
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != 5 && *i != 6)
            .map(|(_, date)| date.clone())
            .collect::<Vec<_>>();
        let (range, _) = program
            .rolling_scores(
                &definition,
                &rt,
                &dates,
                &calendar,
                &calendar[0],
                Some(&sources),
            )
            .unwrap();
        for (index, date) in dates.iter().enumerate() {
            let day = calendar.binary_search(date).unwrap();
            let (daily, _) = program
                .rolling_scores(
                    &definition,
                    &rt,
                    &dates[..=index],
                    &calendar[..=day],
                    date,
                    Some(&sources),
                )
                .unwrap();
            assert_eq!(range[index], daily[index]);
        }
        definition.max_age = Some(1);
        assert!(TrackProgram::compile(&definition).is_err());
        definition.max_age = None;
        assert_eq!(definition.max_age(), definition.ttl);
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
    #[test]
    fn reused_expression_state_matches_fresh_evaluation() {
        let series = Arc::new((1..=8).map(|day| Some(day as f64)).collect::<Vec<_>>());
        let rt = Runtime {
            vars: HashMap::from([("X".into(), Value::SharedNumSeries(Arc::clone(&series)))]),
        };
        let context = HashSet::from(["AGE".into(), "A_LOW".into()]);
        for (text, scalar) in [
            ("X := X + AGE; X > A_LOW", true),
            ("X := X + AGE; COUNTD(X > A_LOW, AGE, 3)", false),
        ] {
            let compiled = TrackExpression::compile(text, &context).unwrap();
            let prepared = compiled.prepare(&rt, &context).unwrap();
            assert_eq!(prepared.scalar, scalar);
            for day in [3, 5, 4, 7] {
                for age in [1.0, 2.0] {
                    let values = BTreeMap::from([("AGE".into(), age), ("A_LOW".into(), 5.0)]);
                    assert_eq!(
                        prepared.evaluate(day, &values).unwrap(),
                        compiled
                            .prepare(&rt, &context)
                            .unwrap()
                            .evaluate(day, &values)
                            .unwrap()
                    );
                }
            }
        }
        let window = Value::NumWindow {
            values: Arc::clone(&series),
            range: 2..5,
        };
        let borrowed = Value::as_num_series(&window, 3).unwrap();
        assert!(matches!(borrowed, std::borrow::Cow::Borrowed(_)));
        assert_eq!(borrowed.as_ptr(), series[2..5].as_ptr());
    }

    #[test]
    fn captures_prepare_only_on_new_anchor_and_are_reused() {
        let definition = TrackDefinition {
            id: "lazy".into(),
            name: "lazy".into(),
            enabled: true,
            source: Some("rule".into()),
            start: None,
            ttl: 2,
            max_age: Some(4),
            capture: BTreeMap::from([("A_MEAN".into(), "MA(C,2)".into())]),
            invalidate: None,
            eligible: None,
            steps: Vec::new(),
            signal: None,
            points: 0.0,
            score_until: None,
        };
        let rt = Runtime {
            vars: HashMap::from([(
                "C".into(),
                Value::NumSeries(vec![
                    Some(10.0),
                    Some(20.0),
                    Some(30.0),
                    Some(40.0),
                    Some(50.0),
                ]),
            )]),
        };
        let compiled = TrackProgram::compile(&definition).unwrap();
        let program = compiled.prepare(&rt).unwrap();
        let mut state = TrackState::default();
        program
            .advance(&mut state, &definition, 0, Some(0), &rt, None)
            .unwrap();
        assert!(program.prepared_capture.get().is_none());
        program
            .advance(&mut state, &definition, 1, Some(1), &rt, Some(2.0))
            .unwrap();
        assert!(program.prepared_capture.get().is_some());
        assert_eq!(state.event.as_ref().unwrap().captured["A_MEAN"], 15.0);
        let resumed = compiled.prepare(&rt).unwrap();
        resumed
            .advance(&mut state, &definition, 2, Some(2), &rt, Some(2.0))
            .unwrap();
        assert!(resumed.prepared_capture.get().is_none());
        assert_eq!(state.event.as_ref().unwrap().captured["A_MEAN"], 15.0);
        program
            .advance(&mut state, &definition, 4, Some(4), &rt, Some(2.0))
            .unwrap();
        assert_eq!(state.event.as_ref().unwrap().captured["A_MEAN"], 45.0);
    }
}
