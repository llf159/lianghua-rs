use super::market_activity::{MarketActivityData, load_market_activity};
use crate::data::source_db_path;
use duckdb::{Connection, params};
use serde::Serialize;
use std::collections::VecDeque;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StructureState {
    Up,
    Down,
    UpBroken,
    DownBroken,
    Insufficient,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StructureEventKind {
    UpStarted,
    DownStarted,
    UpRenewed,
    DownRenewed,
    UpInvalidated,
    DownInvalidated,
    UpReclaimed,
    DownReclaimed,
    FailedUpBreakout,
    FailedDownBreakout,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct StructureEvent {
    pub trade_date: String,
    pub kind: StructureEventKind,
    pub price: f64,
    pub level_price: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct StructurePoint {
    pub price: f64,
    pub trade_date: String,
    pub confirmed_date: String,
    pub confirmation_bars: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct MarketStructureLevel {
    pub degree: usize,
    pub state: StructureState,
    pub phase: String,
    pub stage: String,
    pub can_restore_previous: bool,
    pub state_since: Option<String>,
    pub guard: Option<StructurePoint>,
    pub breakout_price: Option<f64>,
    pub next_trigger_price: Option<f64>,
    pub guard_distance_pct: Option<f64>,
    pub child_state: Option<StructureState>,
    pub parent_state: Option<StructureState>,
    pub pivot_count: usize,
    pub events: Vec<StructureEvent>,
}

#[derive(Clone)]
struct Pivot {
    high: bool,
    index: usize,
    point: StructurePoint,
}

impl Pivot {
    fn observed(index: usize, date: &str, price: f64, high: bool) -> Self {
        Self {
            high,
            index,
            point: StructurePoint {
                price,
                trade_date: date.to_string(),
                confirmed_date: date.to_string(),
                confirmation_bars: 0,
            },
        }
    }
}

#[derive(Clone)]
struct Frame {
    up: bool,
    valid: bool,
    pending: bool,
    guard: Pivot,
    extreme: Pivot,
    started_index: usize,
    breakout: f64,
    previous: Option<Box<Frame>>,
}

#[derive(Default)]
struct Level {
    pivots: Vec<Pivot>,
    frame: Option<Frame>,
    last_pair: Option<(bool, usize, usize)>,
    last_parent_index: Option<usize>,
    state_since: Option<String>,
    events: VecDeque<StructureEvent>,
}

impl Level {
    fn state(&self) -> StructureState {
        match self.frame.as_ref() {
            Some(frame) if frame.up && frame.valid => StructureState::Up,
            Some(frame) if !frame.up && frame.valid => StructureState::Down,
            Some(frame) if frame.up => StructureState::UpBroken,
            Some(_) => StructureState::DownBroken,
            None => StructureState::Insufficient,
        }
    }

    fn insert(&mut self, pivot: Pivot, date: &str, index: usize) -> Option<Pivot> {
        if let Some(last) = self
            .pivots
            .last_mut()
            .filter(|last| last.high == pivot.high)
        {
            if (pivot.high && pivot.point.price > last.point.price)
                || (!pivot.high && pivot.point.price < last.point.price)
            {
                *last = pivot;
            } else {
                return None;
            }
        } else {
            self.pivots.push(pivot);
        }
        let start = self.pivots.len().checked_sub(5)?;
        let points = &self.pivots[start..];
        let middle = &points[2];
        let turning = if middle.high {
            middle.point.price > points[0].point.price
                && middle.point.price >= points[4].point.price
        } else {
            middle.point.price < points[0].point.price
                && middle.point.price <= points[4].point.price
        };
        if !turning || self.last_parent_index == Some(middle.index) {
            return None;
        }
        self.last_parent_index = Some(middle.index);
        let mut parent = middle.clone();
        parent.point.confirmed_date = date.to_string();
        parent.point.confirmation_bars = index - parent.index;
        Some(parent)
    }

    fn step(&mut self, price: f64, previous_price: f64, date: &str, index: usize) {
        let before = self.state();
        let mut changes = Vec::new();
        let mut boundary_change = false;
        if let Some(mut frame) = self.frame.take() {
            let failed = if frame.up {
                price <= frame.breakout
            } else {
                price >= frame.breakout
            };
            if frame.pending && failed {
                if let Some(prior) = frame.previous.take() {
                    changes.push((
                        if frame.up {
                            StructureEventKind::FailedUpBreakout
                        } else {
                            StructureEventKind::FailedDownBreakout
                        },
                        frame.breakout,
                    ));
                    frame = *prior;
                }
            }
            if (frame.up && price > frame.extreme.point.price)
                || (!frame.up && price < frame.extreme.point.price)
            {
                frame.extreme = Pivot::observed(index, date, price, frame.up);
            }
            let valid = if frame.up {
                price >= frame.guard.point.price
            } else {
                price <= frame.guard.point.price
            };
            let lost = frame.valid && !valid;
            if valid != frame.valid {
                let kind = match (frame.up, valid) {
                    (true, true) => StructureEventKind::UpReclaimed,
                    (true, false) => StructureEventKind::UpInvalidated,
                    (false, true) => StructureEventKind::DownReclaimed,
                    (false, false) => StructureEventKind::DownInvalidated,
                };
                changes.push((kind, frame.guard.point.price));
            }
            frame.valid = valid;
            if lost {
                let up = !frame.up;
                let breakout = frame.guard.point.price;
                let mut guard = frame.extreme.clone();
                guard.point.confirmed_date = date.to_string();
                guard.point.confirmation_bars = index - guard.index;
                frame.previous = None;
                frame = Frame {
                    up,
                    valid: true,
                    pending: true,
                    guard,
                    extreme: Pivot::observed(index, date, price, up),
                    started_index: index,
                    breakout,
                    previous: Some(Box::new(frame)),
                };
                changes.push((
                    if up {
                        StructureEventKind::UpStarted
                    } else {
                        StructureEventKind::DownStarted
                    },
                    breakout,
                ));
                boundary_change = true;
            }
            self.frame = Some(frame);
        }
        if self.pivots.len() >= 2 && !boundary_change {
            let origin = self.pivots.last().unwrap().clone();
            let target = &self.pivots[self.pivots.len() - 2];
            let up = !origin.high;
            let pair = (up, target.index, origin.index);
            let same = self.frame.as_ref().is_some_and(|frame| frame.up == up);
            let beyond = if up {
                price > target.point.price
            } else {
                price < target.point.price
            };
            let crossed = if up {
                previous_price <= target.point.price
            } else {
                previous_price >= target.point.price
            };
            let newly_known =
                origin.point.confirmed_date == date || target.point.confirmed_date == date;
            let mut qualifies = beyond && (crossed || newly_known);
            let mut breakout = target.point.price;
            if let Some(frame) = self.frame.as_ref().filter(|_| !same) {
                let defense_crossed = if up {
                    previous_price <= frame.guard.point.price && price > frame.guard.point.price
                } else {
                    previous_price >= frame.guard.point.price && price < frame.guard.point.price
                };
                qualifies = !frame.valid && (qualifies || defense_crossed);
                if defense_crossed {
                    breakout = frame.guard.point.price;
                }
            }
            qualifies &= if up {
                price >= origin.point.price
            } else {
                price <= origin.point.price
            };
            if let Some(frame) = self.frame.as_ref().filter(|_| same) {
                qualifies &= target.index >= frame.started_index && origin.index > target.index;
            }
            if qualifies && (!same || self.last_pair != Some(pair)) {
                self.last_pair = Some(pair);
                if let Some(frame) = self.frame.as_mut().filter(|frame| same && frame.valid) {
                    if (up && origin.point.price > frame.guard.point.price)
                        || (!up && origin.point.price < frame.guard.point.price)
                    {
                        frame.guard = origin;
                    }
                    frame.pending = false;
                    frame.previous = None;
                    frame.breakout = breakout;
                    changes.push((
                        if up {
                            StructureEventKind::UpRenewed
                        } else {
                            StructureEventKind::DownRenewed
                        },
                        breakout,
                    ));
                } else {
                    let previous = self.frame.take().map(|mut prior| {
                        prior.previous = None;
                        Box::new(prior)
                    });
                    self.frame = Some(Frame {
                        up,
                        valid: true,
                        pending: true,
                        guard: origin,
                        extreme: Pivot::observed(index, date, price, up),
                        started_index: index,
                        breakout,
                        previous,
                    });
                    changes.push((
                        if up {
                            StructureEventKind::UpStarted
                        } else {
                            StructureEventKind::DownStarted
                        },
                        breakout,
                    ));
                }
            }
        }
        if before != self.state() {
            self.state_since = Some(date.to_string());
        }
        for (kind, level_price) in changes {
            self.events.push_back(StructureEvent {
                trade_date: date.to_string(),
                kind,
                price,
                level_price,
            });
        }
        while self.events.len() > 8 {
            self.events.pop_front();
        }
    }
}

pub struct StructureEngine {
    threshold: f64,
    index: usize,
    previous: Option<f64>,
    leg: i8,
    high: Option<Pivot>,
    low: Option<Pivot>,
    levels: [Level; 3],
}

impl StructureEngine {
    pub fn new(threshold: f64) -> Result<Self, String> {
        if !threshold.is_finite() || threshold <= 0.0 || threshold >= 1.0 {
            return Err("波段幅度必须在 0 与 1 之间。".to_string());
        }
        Ok(Self {
            threshold,
            index: 0,
            previous: None,
            leg: 0,
            high: None,
            low: None,
            levels: std::array::from_fn(|_| Level::default()),
        })
    }

    pub fn push(&mut self, date: &str, price: f64) -> Result<Vec<MarketStructureLevel>, String> {
        if !price.is_finite() || price <= 0.0 {
            return Err("结构判断需要有效的正收盘价。".to_string());
        }
        let current = Pivot::observed(self.index, date, price, false);
        if self
            .high
            .as_ref()
            .is_none_or(|point| price > point.point.price)
        {
            let mut high = current.clone();
            high.high = true;
            self.high = Some(high);
        }
        if self
            .low
            .as_ref()
            .is_none_or(|point| price < point.point.price)
        {
            self.low = Some(current.clone());
        }
        let mut pivot = None;
        if self.leg >= 0
            && price <= self.high.as_ref().unwrap().point.price * (1.0 - self.threshold)
        {
            pivot = self.high.clone();
            self.leg = -1;
            self.low = Some(current.clone());
        } else if self.leg <= 0
            && price >= self.low.as_ref().unwrap().point.price * (1.0 + self.threshold)
        {
            pivot = self.low.clone();
            self.leg = 1;
            let mut high = current;
            high.high = true;
            self.high = Some(high);
        }
        if let Some(mut point) = pivot {
            point.point.confirmed_date = date.to_string();
            point.point.confirmation_bars = self.index - point.index;
            let mut next = Some(point);
            for level in &mut self.levels {
                if let Some(point) = next.take() {
                    next = level.insert(point, date, self.index);
                }
            }
        }
        for level in &mut self.levels {
            level.step(price, self.previous.unwrap_or(price), date, self.index);
        }
        let mut result = Vec::new();
        for (degree, level) in self.levels.iter().enumerate() {
            let state = level.state();
            let leg_up = if degree == 0 {
                self.leg >= 0
            } else {
                self.levels[degree - 1]
                    .frame
                    .as_ref()
                    .map_or(self.leg >= 0, |frame| frame.up == frame.valid)
            };
            let phase = match state {
                StructureState::Up if leg_up => "advance",
                StructureState::Up => "pullback",
                StructureState::Down if leg_up => "rebound",
                StructureState::Down => "decline",
                StructureState::Insufficient => "unknown",
                _ => "transition",
            };
            let stage = match level.frame.as_ref() {
                Some(frame) if frame.valid && frame.pending => "started",
                Some(frame) if frame.valid => "continued",
                _ => "unresolved",
            };
            let guard = level.frame.as_ref().map(|frame| frame.guard.point.clone());
            result.push(MarketStructureLevel {
                degree,
                state,
                phase: phase.to_string(),
                stage: stage.to_string(),
                can_restore_previous: level
                    .frame
                    .as_ref()
                    .is_some_and(|frame| frame.pending && frame.previous.is_some()),
                state_since: level.state_since.clone(),
                guard_distance_pct: guard
                    .as_ref()
                    .map(|point| (price / point.price - 1.0) * 100.0),
                guard,
                breakout_price: level.frame.as_ref().map(|frame| frame.breakout),
                next_trigger_price: (level.pivots.len() >= 2)
                    .then(|| level.pivots[level.pivots.len() - 2].point.price),
                child_state: degree
                    .checked_sub(1)
                    .map(|child| self.levels[child].state()),
                parent_state: self.levels.get(degree + 1).map(Level::state),
                pivot_count: level.pivots.len(),
                events: level.events.iter().cloned().collect(),
            });
        }
        self.previous = Some(price);
        self.index += 1;
        Ok(result)
    }
}

#[derive(Debug, Serialize)]
pub struct MarketStructureChartPoint {
    pub trade_date: String,
    pub close: f64,
    pub guards: [Option<f64>; 3],
    pub states: [StructureState; 3],
}

#[derive(Debug, Serialize)]
pub struct MarketStructureIndex {
    pub code: String,
    pub name: String,
    pub trade_date: Option<String>,
    pub close: Option<f64>,
    pub current: bool,
    pub levels: Vec<MarketStructureLevel>,
    pub chart: VecDeque<MarketStructureChartPoint>,
}

#[derive(Debug, Serialize)]
pub struct MarketStructureConsensus {
    pub degree: usize,
    pub up_count: usize,
    pub down_count: usize,
    pub broken_count: usize,
    pub unavailable_count: usize,
    pub description: String,
}

#[derive(Debug, Serialize)]
pub struct MarketStructureData {
    pub trade_date: Option<String>,
    pub latest_trade_date: Option<String>,
    pub base_swing_pct: f64,
    pub summary: String,
    pub consensus: Vec<MarketStructureConsensus>,
    pub indexes: Vec<MarketStructureIndex>,
    pub activity: MarketActivityData,
}

pub fn get_market_structure(
    source_path: String,
    reference_trade_date: Option<String>,
) -> Result<MarketStructureData, String> {
    let path = source_db_path(&source_path);
    if !path.is_file() {
        return Err("尚无本地行情库，请先下载日线行情。".to_string());
    }
    let requested = reference_trade_date
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    if requested.as_ref().is_some_and(|date| {
        chrono::NaiveDate::parse_from_str(date, "%Y%m%d").is_err() || date.len() != 8
    }) {
        return Err("参考日需要有效的 YYYYMMDD 日期。".to_string());
    }
    let conn = Connection::open(path).map_err(|error| format!("打开周期行情库失败: {error}"))?;
    let latest_trade_date: Option<String> = conn
        .query_row(
            "SELECT MAX(trade_date) FROM stock_data WHERE adj_type IN ('qfq', 'ind')",
            [],
            |row| row.get(0),
        )
        .map_err(|error| error.to_string())?;
    let reference = requested
        .or_else(|| latest_trade_date.clone())
        .unwrap_or_default();
    let trade_date: Option<String> = conn.query_row("SELECT MAX(trade_date) FROM stock_data WHERE adj_type IN ('qfq', 'ind') AND trade_date <= ?", params![reference], |row| row.get(0)).map_err(|error| error.to_string())?;
    let mut statement = conn.prepare("SELECT trade_date, close::DOUBLE FROM stock_data WHERE adj_type = 'ind' AND ts_code = ? AND trade_date <= ? ORDER BY trade_date").map_err(|error| error.to_string())?;
    let mut indexes = Vec::new();
    for (code, name) in [
        ("399300.SZ", "沪深300"),
        ("399905.SZ", "中证500"),
        ("000852.SH", "中证1000"),
    ] {
        let mut engine = StructureEngine::new(0.02)?;
        let mut index = MarketStructureIndex {
            code: code.to_string(),
            name: name.to_string(),
            trade_date: None,
            close: None,
            current: false,
            levels: Vec::new(),
            chart: VecDeque::new(),
        };
        let mut rows = statement
            .query(params![code, trade_date.as_deref().unwrap_or_default()])
            .map_err(|error| error.to_string())?;
        while let Some(row) = rows.next().map_err(|error| error.to_string())? {
            let date: String = row.get(0).map_err(|error| error.to_string())?;
            let close: Option<f64> = row.get(1).map_err(|error| error.to_string())?;
            if let Some(close) = close.filter(|value| value.is_finite() && *value > 0.0) {
                index.levels = engine.push(&date, close)?;
                index.chart.push_back(MarketStructureChartPoint {
                    trade_date: date.clone(),
                    close,
                    guards: std::array::from_fn(|degree| {
                        index.levels[degree].guard.as_ref().map(|point| point.price)
                    }),
                    states: std::array::from_fn(|degree| index.levels[degree].state),
                });
                while index.chart.len() > 120 {
                    index.chart.pop_front();
                }
                index.close = Some(close);
                index.trade_date = Some(date);
            }
        }
        index.current = index.trade_date.is_some() && index.trade_date == trade_date;
        indexes.push(index);
    }
    let mut consensus = Vec::new();
    for degree in 0..3 {
        let mut counts = [0usize; 4];
        for index in &indexes {
            let state = index
                .levels
                .get(degree)
                .filter(|_| index.current)
                .map(|level| level.state)
                .unwrap_or(StructureState::Insufficient);
            counts[match state {
                StructureState::Up => 0,
                StructureState::Down => 1,
                StructureState::UpBroken | StructureState::DownBroken => 2,
                StructureState::Insufficient => 3,
            }] += 1;
        }
        let pending = indexes.iter().filter(|index| index.current).any(|index| {
            index
                .levels
                .get(degree)
                .is_some_and(|level| level.stage == "started")
        });
        let description = if counts[3] == 3 {
            "结构尚未形成"
        } else if counts[0] == 3 && pending {
            "三个指数向上，含转换尝试"
        } else if counts[1] == 3 && pending {
            "三个指数向下，含转换尝试"
        } else if counts[0] == 3 {
            "三个指数上涨结构有效"
        } else if counts[1] == 3 {
            "三个指数下降结构有效"
        } else {
            "指数结构分歧"
        };
        consensus.push(MarketStructureConsensus {
            degree,
            up_count: counts[0],
            down_count: counts[1],
            broken_count: counts[2],
            unavailable_count: counts[3],
            description: description.to_string(),
        });
    }
    let summary = consensus
        .iter()
        .map(|level| {
            format!(
                "{}周期：{}",
                ["小", "中", "大"][level.degree],
                level.description
            )
        })
        .collect::<Vec<_>>()
        .join("；");
    let activity =
        load_market_activity(&conn, trade_date.as_deref().unwrap_or_default(), &indexes)?;
    Ok(MarketStructureData {
        trade_date,
        latest_trade_date,
        base_swing_pct: 2.0,
        summary,
        consensus,
        indexes,
        activity,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn structure_break_reclaim_hierarchy_and_prefix_are_causal() {
        let mut engine = StructureEngine::new(0.02).unwrap();
        let mut snapshots = Vec::new();
        let prices = [
            100.0, 110.0, 105.0, 112.0, 108.0, 115.0, 107.0, 108.0, 116.0,
        ];
        for (i, price) in prices.into_iter().enumerate() {
            snapshots.push(engine.push(&format!("{i:08}"), price).unwrap());
        }
        assert_eq!(snapshots[5][0].guard.as_ref().unwrap().price, 108.0);
        assert_eq!(snapshots[6][0].state, StructureState::Down);
        assert!(snapshots[6][0].can_restore_previous);
        assert!(!snapshots[3][0].can_restore_previous);
        assert_eq!(snapshots[7][0].state, StructureState::Up);
        assert!(
            snapshots[7][0]
                .events
                .iter()
                .any(|event| event.kind == StructureEventKind::FailedDownBreakout)
        );
        let mut replay = StructureEngine::new(0.02).unwrap();
        for (i, price) in prices[..8].iter().enumerate() {
            assert_eq!(
                replay.push(&format!("{i:08}"), *price).unwrap(),
                snapshots[i]
            );
        }
        let mut hierarchy = StructureEngine::new(0.02).unwrap();
        let mut output = Vec::new();
        for (i, price) in [
            100.0, 110.0, 105.0, 112.0, 103.0, 111.0, 106.0, 115.0, 108.0, 114.0, 109.0, 117.0,
        ]
        .into_iter()
        .enumerate()
        {
            output = hierarchy.push(&format!("{i:08}"), price).unwrap();
            for level in &output {
                if let Some(guard) = &level.guard {
                    assert!(guard.confirmed_date <= format!("{i:08}"));
                }
            }
        }
        assert!(output[1].pivot_count > 0);
        assert!(output[1].pivot_count < output[0].pivot_count);
        output = hierarchy.push("00000012", 102.0).unwrap();
        assert_eq!(output[1].state, StructureState::Down);
        assert_eq!(output[1].stage, "started");
        assert_eq!(output[1].guard.as_ref().unwrap().price, 117.0);
        assert_eq!(output[1].guard.as_ref().unwrap().confirmed_date, "00000012");
        assert!(hierarchy.push("00000012", f64::NAN).is_err());
    }

    #[test]
    fn structure_reference_excludes_future_and_stale_indexes() {
        let directory = crate::statistics::test_support::temp_source_dir();
        std::fs::create_dir_all(&directory).unwrap();
        let conn = Connection::open(source_db_path(directory.to_str().unwrap())).unwrap();
        conn.execute_batch("CREATE TABLE stock_data(ts_code VARCHAR, trade_date VARCHAR, adj_type VARCHAR, close DOUBLE, vol DOUBLE, amount DOUBLE)").unwrap();
        {
            let mut appender = conn.appender("stock_data").unwrap();
            for (day, close) in [
                100.0, 110.0, 105.0, 112.0, 108.0, 115.0, 107.0, 109.0, 200.0,
            ]
            .into_iter()
            .enumerate()
            {
                let date = format!("202401{:02}", day + 2);
                for code in ["399300.SZ", "399905.SZ", "000852.SH"] {
                    if code == "399905.SZ" && day == 7 {
                        continue;
                    }
                    appender
                        .append_row(params![code, &date, "ind", close, 0.0, 0.0])
                        .unwrap();
                }
            }
        }
        let data = get_market_structure(
            directory.to_str().unwrap().to_string(),
            Some("20240109".to_string()),
        )
        .unwrap();
        assert_eq!(data.trade_date.as_deref(), Some("20240109"));
        assert_eq!(data.latest_trade_date.as_deref(), Some("20240110"));
        assert_eq!(data.indexes[0].close, Some(109.0));
        assert_eq!(data.indexes[0].levels[0].state, StructureState::Up);
        assert_eq!(
            data.indexes[0].levels[0].guard.as_ref().unwrap().price,
            108.0
        );
        assert!(!data.indexes[1].current);
        assert_eq!(data.consensus[0].up_count, 2);
        assert_eq!(data.consensus[0].unavailable_count, 1);
        assert!(
            data.indexes[0]
                .chart
                .iter()
                .all(|point| point.trade_date.as_str() <= "20240109")
        );
        let empty = get_market_structure(
            directory.to_str().unwrap().to_string(),
            Some("20230101".to_string()),
        )
        .unwrap();
        assert!(empty.trade_date.is_none());
        assert!(empty.indexes.iter().all(|index| index.levels.is_empty()));
        assert!(
            get_market_structure(
                directory.to_str().unwrap().to_string(),
                Some("20240230".to_string())
            )
            .is_err()
        );
        drop(conn);
        std::fs::remove_dir_all(directory).unwrap();
    }
}
