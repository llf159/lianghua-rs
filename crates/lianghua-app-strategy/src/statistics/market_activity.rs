use super::structure_cycle::{MarketStructureIndex, StructureEventKind, StructureState};
use duckdb::{Connection, params};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Serialize)]
pub struct MarketActivityMeasure {
    pub total: Option<f64>,
    pub day_ratio: Option<f64>,
    pub week_ratio: Option<f64>,
    pub week_change: Option<f64>,
}

impl MarketActivityMeasure {
    fn from_totals(totals: &[Option<f64>]) -> Self {
        let mut ratios = [None; 3];
        for (slot, (top_days, bottom_days, offset)) in
            [(1, 20, 1), (5, 20, 5), (5, 5, 5)].into_iter().enumerate()
        {
            if totals.len() < bottom_days + offset {
                continue;
            }
            let numerator = &totals[totals.len() - top_days..];
            let denominator = &totals[totals.len() - offset - bottom_days..totals.len() - offset];
            if let (Some(top), Some(bottom)) = (
                numerator.iter().copied().sum::<Option<f64>>(),
                denominator.iter().copied().sum::<Option<f64>>(),
            ) {
                let ratio = (top / numerator.len() as f64) / (bottom / denominator.len() as f64);
                ratios[slot] = (bottom > 0.0 && ratio.is_finite()).then_some(ratio);
            }
        }
        Self {
            total: totals.last().copied().flatten(),
            day_ratio: ratios[0],
            week_ratio: ratios[1],
            week_change: ratios[2],
        }
    }
}

#[derive(Debug, Serialize)]
pub struct MarketActivityScope {
    pub scope: String,
    pub observed_stocks: usize,
    pub invalid_volume_stocks: usize,
    pub invalid_amount_stocks: usize,
    pub volume: MarketActivityMeasure,
    pub amount: MarketActivityMeasure,
}

#[derive(Debug, Serialize)]
pub struct MarketActivityOutlook {
    pub horizon: usize,
    pub expected_return_pct: f64,
    pub activity_adjustment_pp: f64,
}

#[derive(Debug, Serialize)]
pub struct MarketActivityData {
    pub scopes: Vec<MarketActivityScope>,
    pub outlook: Vec<MarketActivityOutlook>,
    pub outlook_status: String,
    pub model_fit_end: String,
    pub validation_data_asof: String,
}

#[derive(Deserialize)]
struct FrozenActivityModels {
    fit_end: String,
    validation_data_asof: String,
    models: Vec<FrozenActivityModel>,
}

#[derive(Deserialize)]
struct FrozenActivityModel {
    horizon: usize,
    columns: Vec<String>,
    impute: Vec<f64>,
    mean: Vec<f64>,
    scale: Vec<f64>,
    intercept: f64,
    coefficients: Vec<f64>,
}

impl FrozenActivityModel {
    fn predict(
        &self,
        features: &HashMap<String, Option<f64>>,
        neutral_activity: bool,
    ) -> Result<f64, String> {
        let size = self.columns.len();
        if [
            self.impute.len(),
            self.mean.len(),
            self.scale.len(),
            self.coefficients.len(),
        ]
        .iter()
        .any(|length| *length != size)
        {
            return Err("量能模型参数长度不一致。".to_string());
        }
        let mut prediction = self.intercept;
        for (i, column) in self.columns.iter().enumerate() {
            let input = features
                .get(column)
                .ok_or_else(|| format!("量能模型缺少特征: {column}"))?;
            let value = if neutral_activity && column.starts_with("log_all_a_") {
                0.0
            } else {
                input
                    .filter(|value| value.is_finite())
                    .unwrap_or(self.impute[i])
            };
            prediction += (value - self.mean[i]) / self.scale[i] * self.coefficients[i];
        }
        if !prediction.is_finite() {
            return Err("量能模型估计值无效。".to_string());
        }
        Ok(prediction)
    }
}

pub(super) fn load_market_activity(
    conn: &Connection,
    reference: &str,
    indexes: &[MarketStructureIndex],
) -> Result<MarketActivityData, String> {
    let frozen: FrozenActivityModels =
        serde_json::from_str(include_str!("market_activity_model.json"))
            .map_err(|error| format!("读取量能模型失败: {error}"))?;
    let mut data = MarketActivityData {
        scopes: Vec::new(),
        outlook: Vec::new(),
        outlook_status: "需要完整量能窗口与三个指数最近61个市场交易日的价格。".to_string(),
        model_fit_end: frozen.fit_end,
        validation_data_asof: frozen.validation_data_asof,
    };
    let mut statement = conn.prepare("SELECT DISTINCT trade_date FROM stock_data WHERE adj_type IN ('qfq','ind') AND trade_date<=? ORDER BY trade_date DESC LIMIT 61")
        .map_err(|error| format!("读取量能交易日失败: {error}"))?;
    let mut dates = statement
        .query_map(params![reference], |row| row.get::<_, String>(0))
        .map_err(|error| error.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())?;
    dates.reverse();
    let Some(last_date) = dates.last() else {
        return Ok(data);
    };
    let start = &dates[dates.len().saturating_sub(25)];
    let mut statement = conn.prepare("WITH dates AS (
        SELECT DISTINCT trade_date FROM stock_data WHERE adj_type IN ('qfq','ind')
          AND trade_date BETWEEN ? AND ?
        ), scopes(scope) AS (VALUES ('all_a'),('sh_sz'))
        SELECT scope,d.trade_date,count(s.ts_code)::BIGINT,
          count(s.ts_code) FILTER(WHERE s.vol IS NULL OR NOT isfinite(s.vol::DOUBLE) OR s.vol<0)::BIGINT,
          count(s.ts_code) FILTER(WHERE s.amount IS NULL OR NOT isfinite(s.amount::DOUBLE) OR s.amount<0)::BIGINT,
          CASE WHEN count(s.ts_code)=0 OR count(s.ts_code) FILTER(WHERE s.vol IS NULL OR NOT isfinite(s.vol::DOUBLE) OR s.vol<0)>0 THEN NULL ELSE sum(s.vol)::DOUBLE END,
          CASE WHEN count(s.ts_code)=0 OR count(s.ts_code) FILTER(WHERE s.amount IS NULL OR NOT isfinite(s.amount::DOUBLE) OR s.amount<0)>0 THEN NULL ELSE sum(s.amount)::DOUBLE*1000 END
        FROM dates d CROSS JOIN scopes
        LEFT JOIN stock_data s ON s.trade_date=d.trade_date AND s.adj_type='qfq'
          AND s.trade_date BETWEEN ? AND ?
          AND (right(s.ts_code,2) IN ('SH','SZ') OR (scope='all_a' AND right(s.ts_code,2)='BJ'))
        GROUP BY scope,d.trade_date ORDER BY scope,d.trade_date")
        .map_err(|error| format!("准备市场总量查询失败: {error}"))?;
    let mut rows = statement
        .query(params![start, last_date, start, last_date])
        .map_err(|error| format!("查询市场总量失败: {error}"))?;
    let mut volume = Vec::new();
    let mut amount = Vec::new();
    while let Some(row) = rows.next().map_err(|error| error.to_string())? {
        let scope: String = row.get(0).map_err(|error| error.to_string())?;
        if data.scopes.last().is_none_or(|last| last.scope != scope) {
            volume.clear();
            amount.clear();
            data.scopes.push(MarketActivityScope {
                scope,
                observed_stocks: 0,
                invalid_volume_stocks: 0,
                invalid_amount_stocks: 0,
                volume: MarketActivityMeasure::from_totals(&[]),
                amount: MarketActivityMeasure::from_totals(&[]),
            });
        }
        let current = data.scopes.last_mut().unwrap();
        current.observed_stocks = row.get::<_, i64>(2).map_err(|error| error.to_string())? as usize;
        current.invalid_volume_stocks =
            row.get::<_, i64>(3).map_err(|error| error.to_string())? as usize;
        current.invalid_amount_stocks =
            row.get::<_, i64>(4).map_err(|error| error.to_string())? as usize;
        volume.push(
            row.get::<_, Option<f64>>(5)
                .map_err(|error| error.to_string())?,
        );
        amount.push(
            row.get::<_, Option<f64>>(6)
                .map_err(|error| error.to_string())?,
        );
        current.volume = MarketActivityMeasure::from_totals(&volume);
        current.amount = MarketActivityMeasure::from_totals(&amount);
    }
    if reference < "20210101" {
        data.outlook_status = "模型拟合至2020年末，仅提供2021年起的走势估计。".to_string();
        return Ok(data);
    }
    if dates.len() != 61
        || indexes.len() != 3
        || indexes.iter().any(|index| {
            !index.current
                || index.levels.len() != 3
                || !index
                    .chart
                    .iter()
                    .rev()
                    .take(61)
                    .map(|point| &point.trade_date)
                    .eq(dates.iter().rev())
        })
    {
        return Ok(data);
    }
    let Some(scope) = data.scopes.iter().find(|scope| scope.scope == "all_a") else {
        return Ok(data);
    };
    let mut features = HashMap::new();
    for (measure, ratios) in [
        (
            "volume",
            [
                scope.volume.day_ratio,
                scope.volume.week_ratio,
                scope.volume.week_change,
            ],
        ),
        (
            "amount",
            [
                scope.amount.day_ratio,
                scope.amount.week_ratio,
                scope.amount.week_change,
            ],
        ),
    ] {
        for (name, value) in ["day20", "week20", "week_change"].into_iter().zip(ratios) {
            let Some(value) = value.filter(|value| *value > 0.0) else {
                data.outlook_status =
                    "量能窗口缺失、存在无效数据或零量，暂不计算走势估计。".to_string();
                return Ok(data);
            };
            features.insert(
                format!("log_all_a_{measure}_total_{name}"),
                Some(value.ln()),
            );
        }
    }
    let prices: Vec<Vec<f64>> = indexes
        .iter()
        .map(|index| {
            let mut values: Vec<_> = index
                .chart
                .iter()
                .rev()
                .take(61)
                .map(|point| point.close)
                .collect();
            values.reverse();
            values
        })
        .collect();
    for period in [5, 20, 60] {
        features.insert(
            format!("past{period}"),
            Some(
                prices
                    .iter()
                    .map(|p| p[60] / p[60 - period] - 1.0)
                    .sum::<f64>()
                    / 3.0,
            ),
        );
    }
    features.insert(
        "style20".to_string(),
        Some(prices[2][60] / prices[2][40] - prices[0][60] / prices[0][40]),
    );
    let returns: Vec<_> = (41..61)
        .map(|day| {
            prices
                .iter()
                .map(|p| p[day] / p[day - 1] - 1.0)
                .sum::<f64>()
                / 3.0
        })
        .collect();
    let mean = returns.iter().sum::<f64>() / 20.0;
    features.insert(
        "sigma20".to_string(),
        Some((returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / 19.0).sqrt()),
    );
    for degree in 0..3 {
        let mut sums: HashMap<&str, (f64, usize)> = HashMap::new();
        for index in indexes {
            let level = &index.levels[degree];
            for (name, value) in [
                ("up", Some(f64::from(level.state == StructureState::Up))),
                ("down", Some(f64::from(level.state == StructureState::Down))),
                (
                    "up_broken",
                    Some(f64::from(level.state == StructureState::UpBroken)),
                ),
                (
                    "down_broken",
                    Some(f64::from(level.state == StructureState::DownBroken)),
                ),
                (
                    "insufficient",
                    Some(f64::from(level.state == StructureState::Insufficient)),
                ),
                (
                    "pending_up",
                    Some(f64::from(
                        level.state == StructureState::Up && level.stage == "started",
                    )),
                ),
                (
                    "pending_down",
                    Some(f64::from(
                        level.state == StructureState::Down && level.stage == "started",
                    )),
                ),
                ("pullback", Some(f64::from(level.phase == "pullback"))),
                ("rebound", Some(f64::from(level.phase == "rebound"))),
                (
                    "guard_gap",
                    level
                        .guard
                        .as_ref()
                        .map(|guard| (index.close.unwrap() / guard.price).ln()),
                ),
            ] {
                let entry = sums.entry(name).or_default();
                if let Some(value) = value {
                    entry.0 += value;
                    entry.1 += 1;
                }
            }
            for (name, kind) in [
                ("up_invalidated", StructureEventKind::UpInvalidated),
                ("down_invalidated", StructureEventKind::DownInvalidated),
                ("up_reclaimed", StructureEventKind::UpReclaimed),
                ("down_reclaimed", StructureEventKind::DownReclaimed),
                ("failed_up_breakout", StructureEventKind::FailedUpBreakout),
                (
                    "failed_down_breakout",
                    StructureEventKind::FailedDownBreakout,
                ),
            ] {
                let entry = sums.entry(name).or_default();
                entry.0 += f64::from(
                    level
                        .events
                        .iter()
                        .any(|event| event.trade_date == *last_date && event.kind == kind),
                );
                entry.1 += 1;
            }
        }
        for (name, (sum, count)) in sums {
            features.insert(
                format!("{name}{degree}"),
                (count > 0).then_some(sum / count as f64),
            );
        }
    }
    for model in frozen.models {
        let prediction = model.predict(&features, false)?;
        let neutral = model.predict(&features, true)?;
        data.outlook.push(MarketActivityOutlook {
            horizon: model.horizon,
            expected_return_pct: prediction * 100.0,
            activity_adjustment_pp: (prediction - neutral) * 100.0,
        });
    }
    data.outlook_status = "已按当前价格结构与全市场总体量能计算。".to_string();
    Ok(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn activity_windows_use_stock_totals_and_exclude_future_and_invalid_data() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE stock_data(ts_code VARCHAR,trade_date VARCHAR,adj_type VARCHAR,vol DOUBLE,amount DOUBLE)").unwrap();
        for day in 1..=26 {
            for (code, vol, amount) in [
                ("600000.SH", day as f64, day as f64 * 10.0),
                ("000001.SZ", day as f64 * 2.0, day as f64 * 20.0),
                ("920001.BJ", day as f64 * 3.0, day as f64 * 30.0),
            ] {
                conn.execute(
                    "INSERT INTO stock_data VALUES (?,?,'qfq',?,?)",
                    params![code, format!("202601{day:02}"), vol, amount],
                )
                .unwrap();
            }
            conn.execute(
                "INSERT INTO stock_data VALUES ('000001.SH',?,'ind',1000000,1000000)",
                params![format!("202601{day:02}")],
            )
            .unwrap();
        }
        let data = load_market_activity(&conn, "20260125", &[]).unwrap();
        let all = &data.scopes[0];
        assert_eq!(all.observed_stocks, 3);
        assert_eq!(all.volume.total, Some(150.0));
        assert_eq!(all.amount.total, Some(1500000.0));
        assert!((all.volume.day_ratio.unwrap() - 25.0 / 14.5).abs() < 1e-12);
        assert!((all.volume.week_ratio.unwrap() - 23.0 / 10.5).abs() < 1e-12);
        assert!((all.volume.week_change.unwrap() - 23.0 / 18.0).abs() < 1e-12);
        assert_eq!(data.scopes[1].volume.total, Some(75.0));
        assert!(data.outlook.is_empty());
        conn.execute(
            "UPDATE stock_data SET vol=NULL WHERE ts_code='920001.BJ' AND trade_date='20260124'",
            [],
        )
        .unwrap();
        let invalid = load_market_activity(&conn, "20260125", &[]).unwrap();
        assert_eq!(invalid.scopes[0].volume.total, Some(150.0));
        assert!(invalid.scopes[0].volume.week_ratio.is_none());
        assert!(invalid.scopes[0].volume.day_ratio.is_none());
        assert!(invalid.scopes[0].amount.week_ratio.is_some());
        assert!(invalid.scopes[1].volume.week_ratio.is_some());
    }
}
