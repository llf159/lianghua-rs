use crate::data::{load_trade_date_list, source_db_path, stock_list_path};
use duckdb::{AccessMode, Config, Connection, params};
use serde::Serialize;
use std::collections::{HashMap, HashSet};

const ANALYSIS_WINDOW_SESSIONS: usize = 80;
const COVERAGE_FALLBACK_SESSIONS: usize = 20;
const MIN_COVERAGE_RATIO: f64 = 0.90;
const MIN_EXPECTED_STOCKS: usize = 20;
const HISTORY_SESSIONS: usize = 20;

#[derive(Debug, Clone, Copy)]
struct Listing {
    listed_index: usize,
    delist_index: Option<usize>,
}

impl Listing {
    fn eligible(&self, session: usize) -> bool {
        session >= self.listed_index.saturating_add(59)
            && self.delist_index.is_none_or(|index| session < index)
    }
}

#[derive(Debug, Clone, Copy)]
struct Bar {
    high: f64,
    low: f64,
    close: f64,
    logret: f64,
    ret: f64,
    intraday: f64,
    closepos: f64,
    amount: f64,
    size_yi: f64,
    log_impact: f64,
}

struct StockSeries {
    rows: Vec<Option<Bar>>,
}

#[derive(Debug, Default)]
struct DailySamples {
    follow: usize,
    closepos: usize,
    volume: usize,
    leader: usize,
    amount: usize,
    impact: usize,
    tail: usize,
}

#[derive(Debug, Default)]
struct DailyRaw {
    covered: bool,
    breadth_10d_pct: Option<f64>,
    feedback: [EventCounts; 4],
    ret: Option<f64>,
    dispersion2: Option<f64>,
    follow: Option<f64>,
    closepos: Option<f64>,
    volume_alignment: Option<f64>,
    leader_overlap: Option<f64>,
    amount_concentration: Option<f64>,
    impact_shock: Option<f64>,
    downside_tail: Option<f64>,
    small_ret: Option<f64>,
    large_ret: Option<f64>,
    samples: DailySamples,
}

#[derive(Debug, Default)]
struct EventCounts {
    stocks: Vec<usize>,
    successes: usize,
}

#[derive(Debug, Serialize)]
pub struct MarketEnvironmentFeedback {
    pub key: String,
    pub label: String,
    pub rate_pct: Option<f64>,
    pub change_5d_pp: Option<f64>,
    pub events: usize,
    pub successes: usize,
    pub stocks: usize,
    pub description: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketEnvironmentGroup {
    Direction,
    Volume,
    Risk,
}

#[derive(Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketEnvironmentStatus {
    Ok,
    Insufficient,
}

#[derive(Debug, Serialize)]
pub struct MarketEnvironmentPoint {
    pub trade_date: String,
    pub value: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct MarketEnvironmentDimension {
    pub group: MarketEnvironmentGroup,
    pub key: String,
    pub label: String,
    pub unit: String,
    pub value: Option<f64>,
    pub change_5d: Option<f64>,
    pub interpretation: String,
    pub sample_count: usize,
    pub sample_label: String,
    pub history: Vec<MarketEnvironmentPoint>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MarketEnvironmentCoverage {
    pub expected_stocks: usize,
    pub quoted_stocks: usize,
    pub suspended_or_gapped_stocks: usize,
    pub missing_stocks: usize,
    pub unknown_listing_stocks: usize,
    pub ratio: Option<f64>,
    pub required_ratio: f64,
}

#[derive(Debug, Serialize)]
pub struct MarketEnvironmentData {
    pub reference_trade_date: Option<String>,
    pub analysis_trade_date: Option<String>,
    pub latest_trade_date: Option<String>,
    pub fallback_reason: Option<String>,
    pub coverage: Option<MarketEnvironmentCoverage>,
    pub return_5d_pct: Option<f64>,
    pub return_10d_pct: Option<f64>,
    pub breadth_10d_pct: Option<f64>,
    pub feedback: Vec<MarketEnvironmentFeedback>,
    pub phase: Option<String>,
    pub headline: String,
    pub summary: String,
    pub status: MarketEnvironmentStatus,
    pub dimensions: Vec<MarketEnvironmentDimension>,
}

pub fn get_market_environment(
    source_path: String,
    reference_trade_date: Option<String>,
) -> Result<MarketEnvironmentData, String> {
    let requested = reference_trade_date
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    if let Some(date) = requested.as_ref() {
        if date.len() != 8 || chrono::NaiveDate::parse_from_str(date, "%Y%m%d").is_err() {
            return Err("参考日需要有效的 YYYYMMDD 日期。".to_string());
        }
    }

    let database_path = source_db_path(&source_path);
    if !database_path.is_file() {
        return Err("尚无本地行情库，请先下载日线行情。".to_string());
    }
    let mut calendar =
        load_trade_date_list(&source_path).map_err(|error| format!("读取交易日历失败: {error}"))?;
    calendar.retain(|date| date.len() == 8);
    calendar.sort();
    calendar.dedup();
    if calendar.is_empty() {
        return Err("交易日历为空，无法分析市场环境。".to_string());
    }

    let config = Config::default()
        .access_mode(AccessMode::ReadOnly)
        .map_err(|error| format!("配置行情库只读模式失败: {error}"))?;
    let connection = Connection::open_with_flags(&database_path, config)
        .map_err(|error| format!("打开行情库失败: {error}"))?;
    let latest: Option<String> = connection
        .query_row(
            "SELECT MAX(trade_date) FROM stock_data WHERE adj_type = 'qfq'",
            [],
            |row| row.get(0),
        )
        .map_err(|error| format!("读取最新行情日失败: {error}"))?;
    let Some(latest) = latest else {
        return Ok(insufficient_data(
            requested.as_deref(),
            None,
            "本地行情库暂无可用的前复权日线。".to_string(),
        ));
    };
    if calendar.binary_search(&latest).is_err() {
        return Ok(insufficient_data(
            requested.as_deref().or(Some(latest.as_str())),
            Some(latest.as_str()),
            format!("交易日历未覆盖最新行情日 {latest}，请先更新交易日历。"),
        ));
    }
    let reference = requested.unwrap_or_else(|| latest.clone());
    let reference_index = calendar
        .partition_point(|date| date.as_str() <= reference.as_str())
        .checked_sub(1);
    let Some(reference_index) = reference_index else {
        return Ok(insufficient_data(
            Some(reference.as_str()),
            Some(latest.as_str()),
            "参考日早于交易日历起点，无法分析市场环境。".to_string(),
        ));
    };
    let latest_index = calendar
        .partition_point(|date| date.as_str() <= latest.as_str())
        .saturating_sub(1);
    let end_index = reference_index.min(latest_index);
    let start_index = (end_index + 1).saturating_sub(ANALYSIS_WINDOW_SESSIONS);
    if end_index + 1 - start_index < 40 {
        return Ok(insufficient_data(
            Some(reference.as_str()),
            Some(latest.as_str()),
            "参考日之前的交易日不足 40 个，无法完成市场环境计算。".to_string(),
        ));
    }
    let window_dates = calendar[start_index..=end_index].to_vec();
    let date_index = window_dates
        .iter()
        .enumerate()
        .map(|(index, date)| (date.as_str(), index))
        .collect::<HashMap<_, _>>();

    let (listings, unknown_listing_stocks) = load_listings(&source_path, &calendar)?;
    let mut codes = listings.keys().cloned().collect::<Vec<_>>();
    codes.sort();
    let code_index = codes
        .iter()
        .enumerate()
        .map(|(index, code)| (code.as_str(), index))
        .collect::<HashMap<_, _>>();
    let mut stocks = codes
        .iter()
        .map(|_| StockSeries {
            rows: vec![None; window_dates.len()],
        })
        .collect::<Vec<_>>();
    let mut unlisted_quotes = vec![0usize; window_dates.len()];

    let mut statement = connection
        .prepare(
            "SELECT ts_code, trade_date, open::DOUBLE, high::DOUBLE, low::DOUBLE,
                    close::DOUBLE, pct_chg::DOUBLE, amount::DOUBLE, tor::DOUBLE
             FROM stock_data
             WHERE adj_type = 'qfq' AND trade_date BETWEEN ? AND ?",
        )
        .map_err(|error| format!("准备市场行情查询失败: {error}"))?;
    let mut rows = statement
        .query(params![
            window_dates.first().map(String::as_str).unwrap_or_default(),
            window_dates.last().map(String::as_str).unwrap_or_default()
        ])
        .map_err(|error| format!("查询市场行情失败: {error}"))?;
    while let Some(row) = rows.next().map_err(|error| error.to_string())? {
        let code: String = row.get(0).map_err(|error| error.to_string())?;
        let date: String = row.get(1).map_err(|error| error.to_string())?;
        let Some(&window_index) = date_index.get(date.as_str()) else {
            continue;
        };
        let Some(&stock_index) = code_index.get(code.as_str()) else {
            unlisted_quotes[window_index] += 1;
            continue;
        };
        let open = row
            .get::<_, Option<f64>>(2)
            .map_err(|error| error.to_string())?
            .unwrap_or(f64::NAN);
        let high = row
            .get::<_, Option<f64>>(3)
            .map_err(|error| error.to_string())?
            .unwrap_or(f64::NAN);
        let low = row
            .get::<_, Option<f64>>(4)
            .map_err(|error| error.to_string())?
            .unwrap_or(f64::NAN);
        let close = row
            .get::<_, Option<f64>>(5)
            .map_err(|error| error.to_string())?
            .unwrap_or(f64::NAN);
        let ret = row
            .get::<_, Option<f64>>(6)
            .map_err(|error| error.to_string())?
            .unwrap_or(f64::NAN);
        let amount = row
            .get::<_, Option<f64>>(7)
            .map_err(|error| error.to_string())?
            .unwrap_or(f64::NAN);
        let tor = row
            .get::<_, Option<f64>>(8)
            .map_err(|error| error.to_string())?
            .unwrap_or(f64::NAN);
        let valid_price = open.is_finite()
            && high.is_finite()
            && low.is_finite()
            && close.is_finite()
            && open > 0.0
            && low > 0.0
            && high >= close
            && low <= close
            && high >= open
            && low <= open;
        stocks[stock_index].rows[window_index] = Some(Bar {
            high: if valid_price { high } else { f64::NAN },
            low: if valid_price { low } else { f64::NAN },
            close: if valid_price { close } else { f64::NAN },
            logret: if ret.is_finite() && ret > -100.0 {
                (1.0 + ret / 100.0).ln()
            } else {
                f64::NAN
            },
            ret,
            intraday: if valid_price {
                (close / open - 1.0) * 100.0
            } else {
                f64::NAN
            },
            closepos: if valid_price && high > low {
                (2.0 * close - high - low) / (high - low)
            } else {
                f64::NAN
            },
            amount,
            size_yi: if amount > 0.0 && tor > 0.0 {
                amount / tor / 1000.0
            } else {
                f64::NAN
            },
            log_impact: if amount > 0.0 && ret.is_finite() {
                (ret.abs().max(0.01) / amount).ln()
            } else {
                f64::NAN
            },
        });
    }
    drop(rows);
    drop(statement);
    drop(connection);

    let mut data = analyze_window(
        &reference,
        &latest,
        &window_dates,
        start_index,
        &codes,
        &listings,
        &stocks,
        unknown_listing_stocks,
        &unlisted_quotes,
    );
    if data.fallback_reason.is_none()
        && data.analysis_trade_date.as_deref() != Some(reference.as_str())
        && let Some(analysis) = data.analysis_trade_date.clone()
    {
        data.fallback_reason = Some(format!(
            "参考日 {reference} 不是交易日或尚无完整行情，使用截至该日的最近交易日 {analysis}。"
        ));
    }
    Ok(data)
}

fn analyze_window(
    reference: &str,
    latest: &str,
    window_dates: &[String],
    start_index: usize,
    codes: &[String],
    listings: &HashMap<String, Listing>,
    stocks: &[StockSeries],
    unknown_listing_stocks: usize,
    unlisted_quotes: &[usize],
) -> MarketEnvironmentData {
    let last = window_dates.len() - 1;
    let mut chosen = None;
    let mut last_coverage = None;
    for offset in 0..=COVERAGE_FALLBACK_SESSIONS.min(last) {
        let index = last - offset;
        let coverage = day_coverage(
            codes,
            listings,
            stocks,
            start_index,
            index,
            unknown_listing_stocks + unlisted_quotes.get(index).copied().unwrap_or(0),
        );
        let sufficient = coverage.expected_stocks >= MIN_EXPECTED_STOCKS
            && coverage
                .ratio
                .is_some_and(|ratio| ratio >= MIN_COVERAGE_RATIO);
        if offset == 0 {
            last_coverage = Some(coverage.clone());
        }
        if sufficient {
            chosen = Some((index, coverage));
            break;
        }
    }
    let Some((analysis_index, coverage)) = chosen else {
        let detail = last_coverage
            .as_ref()
            .and_then(|coverage| coverage.ratio)
            .map(|ratio| format!("最新日覆盖率 {:.1}%", ratio * 100.0))
            .unwrap_or_else(|| "最新日无可用报价".to_string());
        return MarketEnvironmentData {
            reference_trade_date: Some(reference.to_string()),
            analysis_trade_date: None,
            latest_trade_date: Some(latest.to_string()),
            fallback_reason: None,
            coverage: last_coverage,
            return_5d_pct: None,
            return_10d_pct: None,
            breadth_10d_pct: None,
            feedback: Vec::new(),
            phase: None,
            headline: "数据不足，暂不能判断全局".to_string(),
            summary: format!(
                "最近 {} 个交易日没有覆盖充分的全市场行情（{detail}，要求 {:.0}%）。",
                COVERAGE_FALLBACK_SESSIONS + 1,
                MIN_COVERAGE_RATIO * 100.0
            ),
            status: MarketEnvironmentStatus::Insufficient,
            dimensions: Vec::new(),
        };
    };

    let daily = (0..window_dates.len())
        .map(|index| {
            let coverage = day_coverage(codes, listings, stocks, start_index, index, 0);
            if coverage.expected_stocks < MIN_EXPECTED_STOCKS
                || !coverage
                    .ratio
                    .is_some_and(|ratio| ratio >= MIN_COVERAGE_RATIO)
            {
                DailyRaw::default()
            } else {
                day_raw(codes, listings, stocks, start_index, index)
            }
        })
        .collect::<Vec<_>>();
    let returns = daily.iter().map(|day| day.ret).collect::<Vec<_>>();
    let dispersion = daily.iter().map(|day| day.dispersion2).collect::<Vec<_>>();
    let follow = daily.iter().map(|day| day.follow).collect::<Vec<_>>();
    let closepos = daily.iter().map(|day| day.closepos).collect::<Vec<_>>();
    let volume = daily
        .iter()
        .map(|day| day.volume_alignment)
        .collect::<Vec<_>>();
    let leader = daily
        .iter()
        .map(|day| day.leader_overlap)
        .collect::<Vec<_>>();
    let concentration = daily
        .iter()
        .map(|day| day.amount_concentration)
        .collect::<Vec<_>>();
    let impact = daily.iter().map(|day| day.impact_shock).collect::<Vec<_>>();
    let tail = daily
        .iter()
        .map(|day| day.downside_tail)
        .collect::<Vec<_>>();
    let conflict = daily
        .iter()
        .map(|day| match (day.small_ret, day.large_ret) {
            (Some(small), Some(large)) => Some(if small * large < 0.0 { 1.0 } else { 0.0 }),
            _ => None,
        })
        .collect::<Vec<_>>();

    let trend = (0..window_dates.len())
        .map(|index| trend_efficiency(&returns, index, 10))
        .collect::<Vec<_>>();
    let follow5 = rolling_series(&follow, 5);
    let closepos5 = rolling_series(&closepos, 5);
    let volume5 = rolling_series(&volume, 5);
    let leader5 = rolling_series(&leader, 5);
    let concentration5 = rolling_series(&concentration, 5);
    let impact5 = rolling_series(&impact, 5);
    let tail5 = rolling_series(&tail, 5);
    let conflict5 = rolling_series(&conflict, 5);
    let common = (0..window_dates.len())
        .map(|index| common_fraction(&returns, &dispersion, index))
        .collect::<Vec<_>>();
    let return5 = (0..window_dates.len())
        .map(|index| market_return(&returns, index, 5))
        .collect::<Vec<_>>();
    let return10 = (0..window_dates.len())
        .map(|index| market_return(&returns, index, 10))
        .collect::<Vec<_>>();

    let raw_trend = trend[analysis_index];
    let raw_follow = follow5[analysis_index];
    let raw_common = common[analysis_index];
    let raw_tail = tail5[analysis_index];
    let market_return5 = return5[analysis_index];
    let market_return10 = return10[analysis_index];
    let phase = match (raw_trend, raw_follow) {
        (Some(trend), Some(follow)) => Some(format!(
            "{}·{}",
            if trend > 0.2 {
                "上行"
            } else if trend < -0.2 {
                "下行"
            } else {
                "震荡"
            },
            if follow >= 0.0 {
                "强者延续"
            } else {
                "强弱反转"
            }
        )),
        _ => None,
    };
    let samples = &daily[analysis_index].samples;
    let dimensions = vec![
        environment_dimension(
            MarketEnvironmentGroup::Direction,
            "direction",
            "方向顺畅度（10日）",
            "",
            &trend,
            analysis_index,
            1.0,
            raw_trend
                .map(|value| dimension_interpretation("direction", value, market_return10))
                .unwrap_or_default(),
            10,
            "个交易日",
            window_dates,
        ),
        environment_dimension(
            MarketEnvironmentGroup::Direction,
            "follow",
            "强弱延续度（5日）",
            "个百分点",
            &follow5,
            analysis_index,
            1.0,
            raw_follow
                .map(|value| dimension_interpretation("follow", value, market_return10))
                .unwrap_or_default(),
            samples.follow,
            "只有效股票",
            window_dates,
        ),
        environment_dimension(
            MarketEnvironmentGroup::Direction,
            "closepos",
            "收盘位置（5日）",
            "",
            &closepos5,
            analysis_index,
            1.0,
            closepos5[analysis_index]
                .map(|value| dimension_interpretation("closepos", value, market_return10))
                .unwrap_or_default(),
            samples.closepos,
            "只有效股票",
            window_dates,
        ),
        environment_dimension(
            MarketEnvironmentGroup::Volume,
            "volume_alignment",
            "量价配合（5日）",
            "",
            &volume5,
            analysis_index,
            1.0,
            volume5[analysis_index]
                .map(|value| dimension_interpretation("volume_alignment", value, market_return10))
                .unwrap_or_default(),
            samples.volume,
            "只有效股票",
            window_dates,
        ),
        environment_dimension(
            MarketEnvironmentGroup::Volume,
            "leader_overlap",
            "领涨保留（5日）",
            "%",
            &leader5,
            analysis_index,
            100.0,
            leader5[analysis_index]
                .map(|value| dimension_interpretation("leader_overlap", value, market_return10))
                .unwrap_or_default(),
            samples.leader,
            "只有效股票",
            window_dates,
        ),
        environment_dimension(
            MarketEnvironmentGroup::Volume,
            "amount_concentration",
            "成交集中（5日）",
            "%",
            &concentration5,
            analysis_index,
            100.0,
            concentration5[analysis_index]
                .map(|value| {
                    dimension_interpretation("amount_concentration", value, market_return10)
                })
                .unwrap_or_default(),
            samples.amount,
            "只有效股票",
            window_dates,
        ),
        environment_dimension(
            MarketEnvironmentGroup::Volume,
            "impact_shock",
            "交易冲击（5日）",
            "",
            &impact5,
            analysis_index,
            1.0,
            impact5[analysis_index]
                .map(|value| dimension_interpretation("impact_shock", value, market_return10))
                .unwrap_or_default(),
            samples.impact,
            "只有效股票",
            window_dates,
        ),
        environment_dimension(
            MarketEnvironmentGroup::Risk,
            "common_fraction",
            "共同波动（20日）",
            "%",
            &common,
            analysis_index,
            100.0,
            raw_common
                .map(|value| dimension_interpretation("common_fraction", value, market_return10))
                .unwrap_or_default(),
            20,
            "个交易日",
            window_dates,
        ),
        environment_dimension(
            MarketEnvironmentGroup::Risk,
            "downside_tail",
            "下跌尾部（5日）",
            "百分点",
            &tail5,
            analysis_index,
            100.0,
            raw_tail
                .map(|value| dimension_interpretation("downside_tail", value, market_return10))
                .unwrap_or_default(),
            samples.tail,
            "只有效股票",
            window_dates,
        ),
        environment_dimension(
            MarketEnvironmentGroup::Risk,
            "style_conflict",
            "大小盘分歧（5日）",
            "%",
            &conflict5,
            analysis_index,
            100.0,
            conflict5[analysis_index]
                .map(|value| dimension_interpretation("style_conflict", value, market_return10))
                .unwrap_or_default(),
            5,
            "个交易日",
            window_dates,
        ),
    ];
    let feedback = feedback_readings(&daily, analysis_index);
    let breadth_10d_pct = daily[analysis_index].breadth_10d_pct;
    let (headline, summary) = summarize(&dimensions, &feedback, market_return5, breadth_10d_pct);
    let fallback_reason = (analysis_index != last).then(|| {
        format!(
            "{} 的全市场覆盖不足，已回退到最近覆盖充分的交易日 {}。",
            window_dates[last], window_dates[analysis_index]
        )
    });
    MarketEnvironmentData {
        reference_trade_date: Some(reference.to_string()),
        analysis_trade_date: Some(window_dates[analysis_index].clone()),
        latest_trade_date: Some(latest.to_string()),
        fallback_reason,
        coverage: Some(coverage),
        return_5d_pct: market_return5,
        return_10d_pct: market_return10,
        breadth_10d_pct,
        feedback,
        phase,
        headline,
        summary,
        status: MarketEnvironmentStatus::Ok,
        dimensions,
    }
}

fn load_listings(
    source_path: &str,
    calendar: &[String],
) -> Result<(HashMap<String, Listing>, usize), String> {
    let path = stock_list_path(source_path);
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_path(&path)
        .map_err(|error| format!("打开 stock_list.csv 失败: {error}"))?;
    let headers = reader
        .headers()
        .map_err(|error| format!("读取 stock_list.csv 表头失败: {error}"))?
        .iter()
        .map(|value| value.trim().to_string())
        .collect::<Vec<_>>();
    let code_index = headers
        .iter()
        .position(|header| header.eq_ignore_ascii_case("ts_code"))
        .ok_or_else(|| "stock_list.csv 缺少 ts_code 列。".to_string())?;
    let list_index = headers
        .iter()
        .position(|header| header.eq_ignore_ascii_case("list_date"))
        .ok_or_else(|| "stock_list.csv 缺少 list_date 列。".to_string())?;
    let delist_index = headers
        .iter()
        .position(|header| header.eq_ignore_ascii_case("delist_date"));
    let mut listings = HashMap::new();
    let mut unknown = 0usize;
    for row in reader.records() {
        let row = row.map_err(|error| format!("解析 stock_list.csv 失败: {error}"))?;
        let Some(code) = row
            .get(code_index)
            .map(str::trim)
            .filter(|value| !value.is_empty())
        else {
            continue;
        };
        let list_date = row
            .get(list_index)
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let Some(list_date) = list_date.filter(|value| {
            value.len() == 8 && chrono::NaiveDate::parse_from_str(value, "%Y%m%d").is_ok()
        }) else {
            unknown += 1;
            continue;
        };
        let listed_index = calendar
            .binary_search_by(|value| value.as_str().cmp(list_date))
            .unwrap_or_else(|index| index);
        let delist_index = delist_index
            .and_then(|index| row.get(index))
            .map(str::trim)
            .filter(|value| {
                value.len() == 8 && chrono::NaiveDate::parse_from_str(value, "%Y%m%d").is_ok()
            })
            .map(|value| {
                calendar
                    .binary_search_by(|date| date.as_str().cmp(value))
                    .unwrap_or_else(|index| index)
            });
        listings.insert(
            code.to_string(),
            Listing {
                listed_index,
                delist_index,
            },
        );
    }
    if listings.is_empty() {
        return Err("stock_list.csv 中没有可确认上市日期的股票。".to_string());
    }
    Ok((listings, unknown))
}

fn day_coverage(
    codes: &[String],
    listings: &HashMap<String, Listing>,
    stocks: &[StockSeries],
    start_index: usize,
    window_index: usize,
    unknown_listing_stocks: usize,
) -> MarketEnvironmentCoverage {
    let session = start_index + window_index;
    let mut expected = 0usize;
    let mut quoted = 0usize;
    let mut suspended = 0usize;
    let mut missing = 0usize;
    for (index, code) in codes.iter().enumerate() {
        let Some(listing) = listings.get(code) else {
            continue;
        };
        if !listing.eligible(session) {
            continue;
        }
        expected += 1;
        if stocks[index].rows[window_index].is_some() {
            quoted += 1;
        } else if stocks[index].rows[..window_index]
            .iter()
            .rev()
            .take(20)
            .any(Option::is_some)
        {
            suspended += 1;
        } else {
            missing += 1;
        }
    }
    MarketEnvironmentCoverage {
        expected_stocks: expected,
        quoted_stocks: quoted,
        suspended_or_gapped_stocks: suspended,
        missing_stocks: missing,
        unknown_listing_stocks,
        ratio: (expected > 0).then(|| quoted as f64 / expected as f64),
        required_ratio: MIN_COVERAGE_RATIO,
    }
}

fn day_raw(
    codes: &[String],
    listings: &HashMap<String, Listing>,
    stocks: &[StockSeries],
    start_index: usize,
    window_index: usize,
) -> DailyRaw {
    let session = start_index + window_index;
    let mut returns = Vec::new();
    let mut closepos = Vec::new();
    let mut follow = Vec::new();
    let mut volume = Vec::new();
    let mut leader = Vec::new();
    let mut amounts = Vec::new();
    let mut impacts = Vec::new();
    let mut small_returns = Vec::new();
    let mut large_returns = Vec::new();
    let mut down = 0usize;
    let mut up = 0usize;
    let mut tail_samples = 0usize;
    let mut past10 = Vec::new();
    let mut feedback: [EventCounts; 4] = Default::default();
    for (index, code) in codes.iter().enumerate() {
        let Some(listing) = listings.get(code) else {
            continue;
        };
        if !listing.eligible(session) {
            continue;
        }
        let rows = &stocks[index].rows;
        let Some(bar) = rows.get(window_index).copied().flatten() else {
            continue;
        };
        if contiguous(rows, window_index, 10)
            && let Some(ret) = cumulative_return(rows, window_index - 9, window_index)
        {
            past10.push(ret);
        }
        if bar.close.is_finite() {
            if let Some((high, low)) = prior_range(rows, window_index) {
                if bar.high > high {
                    feedback[0].stocks.push(index);
                    feedback[0].successes += usize::from(bar.close > high);
                }
                if bar.low < low {
                    feedback[1].stocks.push(index);
                    feedback[1].successes += usize::from(bar.close >= low);
                }
            }
            if window_index > 0
                && listing.eligible(session - 1)
                && let Some((high, low)) = prior_range(rows, window_index - 1)
                && let Some(previous) = rows[window_index - 1]
            {
                if previous.high > high && previous.close > high {
                    feedback[2].stocks.push(index);
                    feedback[2].successes += usize::from(bar.close > high);
                }
                if previous.low < low && previous.close < low {
                    feedback[3].stocks.push(index);
                    feedback[3].successes += usize::from(bar.close >= low);
                }
            }
        }
        if bar.ret.is_finite() {
            returns.push(bar.ret);
            tail_samples += 1;
            if bar.ret < -3.0 {
                down += 1;
            } else if bar.ret > 3.0 {
                up += 1;
            }
            if bar.size_yi.is_finite() {
                if bar.size_yi < 100.0 {
                    small_returns.push(bar.ret);
                } else if bar.size_yi >= 500.0 {
                    large_returns.push(bar.ret);
                }
            }
        }
        if bar.closepos.is_finite() {
            closepos.push(bar.closepos);
        }
        if bar.amount.is_finite() && bar.amount >= 0.0 {
            amounts.push(bar.amount);
        }
        if contiguous(rows, window_index, 7) {
            if let Some(earlier) = cumulative_return(rows, window_index - 5, window_index - 1) {
                if bar.intraday.is_finite() {
                    follow.push((earlier, bar.intraday));
                }
            }
        }
        if contiguous(rows, window_index, 21) {
            let mut amount_sum = 0.0;
            let mut amount_count = 0usize;
            let mut impact_sum = 0.0;
            let mut impact_count = 0usize;
            for prior in &rows[window_index - 20..window_index] {
                let Some(prior) = prior else {
                    continue;
                };
                if prior.amount.is_finite() {
                    amount_sum += prior.amount;
                    amount_count += 1;
                }
                if prior.log_impact.is_finite() {
                    impact_sum += prior.log_impact;
                    impact_count += 1;
                }
            }
            if bar.ret.is_finite() && amount_count > 0 {
                let baseline = amount_sum / amount_count as f64;
                if baseline > 0.0 {
                    let ratio = bar.amount / baseline;
                    if ratio.is_finite() {
                        volume.push((bar.ret, ratio));
                    }
                }
            }
            if bar.log_impact.is_finite() && impact_count > 0 {
                impacts.push(bar.log_impact - impact_sum / impact_count as f64);
            }
        }
        if contiguous(rows, window_index, 11) {
            if let (Some(past), Some(old)) = (
                cumulative_return(rows, window_index - 4, window_index),
                cumulative_return(rows, window_index - 9, window_index - 5),
            ) {
                leader.push((past, old));
            }
        }
    }

    let mut raw = DailyRaw {
        covered: true,
        breadth_10d_pct: (past10.len() >= MIN_EXPECTED_STOCKS
            && past10.len() as f64 >= returns.len() as f64 * MIN_COVERAGE_RATIO)
            .then(|| {
                100.0 * past10.iter().filter(|ret| **ret > 0.0).count() as f64 / past10.len() as f64
            }),
        feedback,
        ret: (!returns.is_empty()).then(|| mean(&returns)),
        dispersion2: sample_variance(&returns),
        closepos: (!closepos.is_empty()).then(|| mean(&closepos)),
        amount_concentration: concentration(&mut amounts),
        impact_shock: median(&mut impacts),
        downside_tail: (tail_samples > 0).then(|| (down as f64 - up as f64) / tail_samples as f64),
        small_ret: (small_returns.len() >= 20).then(|| mean(&small_returns)),
        large_ret: (large_returns.len() >= 20).then(|| mean(&large_returns)),
        ..Default::default()
    };
    raw.samples.closepos = closepos.len();
    raw.samples.amount = amounts.len();
    raw.samples.impact = impacts.len();
    raw.samples.tail = tail_samples;
    if follow.len() >= 20 {
        let earlier = follow.iter().map(|pair| pair.0).collect::<Vec<_>>();
        let intraday = follow.iter().map(|pair| pair.1).collect::<Vec<_>>();
        let percentile = percentile_ranks(&earlier);
        let mut high_sum = 0.0;
        let mut high_count = 0usize;
        let mut low_sum = 0.0;
        let mut low_count = 0usize;
        for (index, rank) in percentile.iter().enumerate() {
            if *rank > 0.8 {
                high_sum += intraday[index];
                high_count += 1;
            } else if *rank <= 0.2 {
                low_sum += intraday[index];
                low_count += 1;
            }
        }
        if high_count > 0 && low_count > 0 {
            raw.follow = Some(high_sum / high_count as f64 - low_sum / low_count as f64);
        }
        raw.samples.follow = follow.len();
    }
    if volume.len() >= 20 {
        let returns = volume.iter().map(|pair| pair.0).collect::<Vec<_>>();
        let ratios = volume.iter().map(|pair| pair.1).collect::<Vec<_>>();
        raw.volume_alignment = rank_correlation(&returns, &ratios);
        raw.samples.volume = volume.len();
    }
    if leader.len() >= 20 {
        let past = leader.iter().map(|pair| pair.0).collect::<Vec<_>>();
        let old = leader.iter().map(|pair| pair.1).collect::<Vec<_>>();
        let past_rank = percentile_ranks(&past);
        let old_rank = percentile_ranks(&old);
        let mut intersection = 0usize;
        let mut union = 0usize;
        for (past, old) in past_rank.iter().zip(old_rank.iter()) {
            let past_top = *past > 0.8;
            let old_top = *old > 0.8;
            intersection += usize::from(past_top && old_top);
            union += usize::from(past_top || old_top);
        }
        if union > 0 {
            raw.leader_overlap = Some(intersection as f64 / union as f64);
        }
        raw.samples.leader = leader.len();
    }
    raw
}

fn contiguous(rows: &[Option<Bar>], end: usize, days: usize) -> bool {
    end + 1 >= days && rows[end + 1 - days..=end].iter().all(Option::is_some)
}

fn prior_range(rows: &[Option<Bar>], end: usize) -> Option<(f64, f64)> {
    if !contiguous(rows, end, 21) {
        return None;
    }
    let mut high = f64::NEG_INFINITY;
    let mut low = f64::INFINITY;
    for row in &rows[end - 20..end] {
        let bar = row.as_ref()?;
        if !bar.high.is_finite() || !bar.low.is_finite() || bar.low <= 0.0 {
            return None;
        }
        high = high.max(bar.high);
        low = low.min(bar.low);
    }
    Some((high, low))
}

fn feedback_readings(daily: &[DailyRaw], end: usize) -> Vec<MarketEnvironmentFeedback> {
    [
        (
            "attack",
            "上攻站稳",
            "盘中超过此前20日高点，收盘仍高于该位置。",
        ),
        (
            "defense",
            "下探收回",
            "盘中跌破此前20日低点，收盘回到该位置。",
        ),
        (
            "kept",
            "成果保留",
            "前日已经上攻站稳，次日收盘仍高于前日所用参考位。",
        ),
        (
            "repair",
            "跌破修复",
            "前日已经收盘跌破，次日收盘回到前日所用参考位。",
        ),
    ]
    .into_iter()
    .enumerate()
    .map(|(kind, (key, label, description))| {
        let reading = |end: usize| {
            let start = (end + 1).saturating_sub(5);
            let window = &daily[start..=end];
            let mut stocks = HashSet::new();
            let mut events = 0;
            let mut successes = 0;
            for day in window {
                events += day.feedback[kind].stocks.len();
                successes += day.feedback[kind].successes;
                stocks.extend(day.feedback[kind].stocks.iter().copied());
            }
            let covered = window.len() == 5 && window.iter().all(|day| day.covered);
            let rate = (covered && events >= 50).then(|| successes as f64 / events as f64 * 100.0);
            (rate, events, successes, stocks.len(), covered)
        };
        let (rate, events, successes, stocks, covered) = reading(end);
        let change = end.checked_sub(5).and_then(|previous| {
            rate.zip(reading(previous).0)
                .map(|(current, previous)| current - previous)
        });
        MarketEnvironmentFeedback {
            key: key.to_string(),
            label: label.to_string(),
            rate_pct: rate,
            change_5d_pp: change,
            events,
            successes,
            stocks,
            description: if !covered {
                format!("{description}近5日行情覆盖不足，暂不判断。")
            } else if events < 50 {
                format!("{description}近5日仅{events}次事件，样本不足。")
            } else {
                description.to_string()
            },
        }
    })
    .collect()
}

fn cumulative_return(rows: &[Option<Bar>], start: usize, end: usize) -> Option<f64> {
    let mut sum = 0.0;
    for row in &rows[start..=end] {
        let logret = row.as_ref()?.logret;
        if !logret.is_finite() {
            return None;
        }
        sum += logret;
    }
    Some((sum.exp() - 1.0) * 100.0)
}

fn mean(values: &[f64]) -> f64 {
    values.iter().sum::<f64>() / values.len() as f64
}

fn sample_variance(values: &[f64]) -> Option<f64> {
    if values.len() < 2 {
        return None;
    }
    let center = mean(values);
    Some(
        values
            .iter()
            .map(|value| (value - center).powi(2))
            .sum::<f64>()
            / (values.len() - 1) as f64,
    )
}

fn median(values: &mut [f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    values.sort_by(|left, right| left.total_cmp(right));
    let middle = values.len() / 2;
    Some(if values.len() % 2 == 0 {
        (values[middle - 1] + values[middle]) / 2.0
    } else {
        values[middle]
    })
}

fn concentration(amounts: &mut [f64]) -> Option<f64> {
    if amounts.is_empty() {
        return None;
    }
    amounts.sort_by(|left, right| left.total_cmp(right));
    let total = amounts.iter().sum::<f64>();
    if total <= 0.0 {
        return None;
    }
    let top = ((amounts.len() as f64) * 0.1).ceil() as usize;
    let top = top.max(1).min(amounts.len());
    Some(amounts[amounts.len() - top..].iter().sum::<f64>() / total)
}

fn percentile_ranks(values: &[f64]) -> Vec<f64> {
    let mut order = (0..values.len()).collect::<Vec<_>>();
    order.sort_by(|left, right| {
        values[*left]
            .total_cmp(&values[*right])
            .then_with(|| left.cmp(right))
    });
    let mut ranks = vec![0.0; values.len()];
    let mut start = 0usize;
    while start < order.len() {
        let mut end = start + 1;
        while end < order.len() && (values[order[end]] - values[order[start]]).abs() < 1e-12 {
            end += 1;
        }
        let rank = ((start + 1 + end) as f64 / 2.0) / values.len() as f64;
        for position in &order[start..end] {
            ranks[*position] = rank;
        }
        start = end;
    }
    ranks
}

fn rank_correlation(left: &[f64], right: &[f64]) -> Option<f64> {
    let left = percentile_ranks(left);
    let right = percentile_ranks(right);
    let left_mean = mean(&left);
    let right_mean = mean(&right);
    let mut covariance = 0.0;
    let mut left_variance = 0.0;
    let mut right_variance = 0.0;
    for (left, right) in left.iter().zip(right.iter()) {
        covariance += (left - left_mean) * (right - right_mean);
        left_variance += (left - left_mean).powi(2);
        right_variance += (right - right_mean).powi(2);
    }
    let denominator = (left_variance * right_variance).sqrt();
    (denominator > 0.0).then(|| covariance / denominator)
}

fn rolling_series(values: &[Option<f64>], window: usize) -> Vec<Option<f64>> {
    (0..values.len())
        .map(|end| {
            if end + 1 < window {
                return None;
            }
            let mut sum = 0.0;
            for value in &values[end + 1 - window..=end] {
                sum += (*value)?;
            }
            Some(sum / window as f64)
        })
        .collect()
}

fn trend_efficiency(returns: &[Option<f64>], end: usize, window: usize) -> Option<f64> {
    if end + 1 < window {
        return None;
    }
    let mut sum = 0.0;
    let mut absolute = 0.0;
    for ret in &returns[end + 1 - window..=end] {
        let ret = (*ret)?;
        if ret <= -100.0 {
            return None;
        }
        let logret = (1.0 + ret / 100.0).ln();
        sum += logret;
        absolute += logret.abs();
    }
    (absolute > 0.0).then(|| sum / absolute)
}

fn market_return(returns: &[Option<f64>], end: usize, window: usize) -> Option<f64> {
    if end + 1 < window {
        return None;
    }
    let mut sum = 0.0;
    for ret in &returns[end + 1 - window..=end] {
        let ret = (*ret)?;
        if ret <= -100.0 {
            return None;
        }
        sum += (1.0 + ret / 100.0).ln();
    }
    Some((sum.exp() - 1.0) * 100.0)
}

fn common_fraction(returns: &[Option<f64>], dispersion: &[Option<f64>], end: usize) -> Option<f64> {
    if end + 1 < 20 {
        return None;
    }
    let mut rets = Vec::with_capacity(20);
    let mut dispersions = Vec::with_capacity(20);
    for index in end + 1 - 20..=end {
        rets.push((*returns.get(index)?)?);
        dispersions.push((*dispersion.get(index)?)?);
    }
    let variance = sample_variance(&rets)?;
    let dispersion = mean(&dispersions);
    let denominator = variance + dispersion;
    (denominator > 0.0).then(|| variance / denominator)
}

fn environment_dimension(
    group: MarketEnvironmentGroup,
    key: &str,
    label: &str,
    unit: &str,
    series: &[Option<f64>],
    end: usize,
    scale: f64,
    interpretation: String,
    sample_count: usize,
    sample_label: &str,
    dates: &[String],
) -> MarketEnvironmentDimension {
    let value = series[end].map(|value| value * scale);
    let change_5d = end.checked_sub(5).and_then(|previous| {
        series[previous]
            .zip(series[end])
            .map(|(previous, current)| (current - previous) * scale)
    });
    let start = (end + 1).saturating_sub(HISTORY_SESSIONS);
    let history = (start..=end)
        .map(|index| MarketEnvironmentPoint {
            trade_date: dates[index].clone(),
            value: series[index].map(|value| value * scale),
        })
        .collect();
    MarketEnvironmentDimension {
        group,
        key: key.to_string(),
        label: label.to_string(),
        unit: unit.to_string(),
        value,
        change_5d,
        interpretation: if value.is_some() {
            interpretation
        } else {
            "数据不足，暂不解释。".to_string()
        },
        sample_count,
        sample_label: sample_label.to_string(),
        history,
    }
}

fn dimension_interpretation(key: &str, value: f64, return10: Option<f64>) -> String {
    match key {
        "direction" => {
            let path = if value >= 0.2 {
                "近10日向上推进较顺畅"
            } else if value <= -0.2 {
                "近10日向下推进较顺畅"
            } else {
                "近10日涨跌来回折返，方向性不强"
            };
            match (value >= 0.2, value <= -0.2, return10) {
                (true, _, Some(ret)) if ret < 0.0 => {
                    format!("{path}，但近10日累计收益仍为负")
                }
                (_, true, Some(ret)) if ret > 0.0 => {
                    format!("{path}，但近10日累计收益仍为正")
                }
                _ => path.to_string(),
            }
        }
        "follow" if value >= 0.3 => "原强势股继续明显占优，强弱延续".to_string(),
        "follow" if value > 0.0 => "原强势股略占优，强弱小幅延续".to_string(),
        "follow" if value == 0.0 => "原强势股与弱势股表现接近".to_string(),
        "follow" if value <= -0.3 => "原强势股明显转弱，强弱发生轮换".to_string(),
        "follow" => "原强势股略转弱，强弱偏向反转".to_string(),
        "closepos" if value > 0.1 => "收盘多落在日内上半区，日内承接偏强".to_string(),
        "closepos" if value < -0.1 => "收盘多落在日内下半区，日内承接偏弱".to_string(),
        "closepos" => "收盘位置接近日内中位".to_string(),
        "volume_alignment" if value > 0.25 => "相对放量更集中在上涨股".to_string(),
        "volume_alignment" if value < 0.12 => "放量没有明显偏向上涨股，量价关系偏弱".to_string(),
        "volume_alignment" => "量价关系中性偏正".to_string(),
        "leader_overlap" if value > 0.2 => "相邻5日领涨成员保留较多".to_string(),
        "leader_overlap" if value < 0.1 => "领涨成员快速轮换".to_string(),
        "leader_overlap" => "领涨成员部分保留".to_string(),
        "amount_concentration" if value > 0.6 => "成交额更多集中在前10%股票".to_string(),
        "amount_concentration" if value < 0.5 => "成交额分布较分散".to_string(),
        "amount_concentration" => "成交集中度居中".to_string(),
        "impact_shock" if value > 0.0 => "单位成交额对应的波动高于自身20日常态".to_string(),
        "impact_shock" if value < 0.0 => "单位成交额对应的波动低于自身20日常态".to_string(),
        "impact_shock" => "交易冲击接近自身常态".to_string(),
        "common_fraction" if value > 0.3 => "共同波动占比偏高，个股更易同涨同跌".to_string(),
        "common_fraction" if value < 0.12 => "共同波动占比较低，个股走势更分散".to_string(),
        "common_fraction" => "共同波动占比居中".to_string(),
        "downside_tail" if value > 0.05 => "跌超3%的股票明显多于涨超3%，下跌尾部偏重".to_string(),
        "downside_tail" if value < -0.05 => "涨超3%的股票明显多于跌超3%，上涨尾部偏重".to_string(),
        "downside_tail" if value > 0.0 => "下跌尾部略占优".to_string(),
        "downside_tail" if value < 0.0 => "上涨尾部略占优".to_string(),
        "downside_tail" => "涨跌尾部接近".to_string(),
        "style_conflict" if value > 0.4 => "小盘与大盘方向相反的日子较多".to_string(),
        "style_conflict" if value < 0.2 => "小盘与大盘多数同向".to_string(),
        "style_conflict" => "大小盘方向时有分歧".to_string(),
        _ => String::new(),
    }
}

fn summarize(
    dimensions: &[MarketEnvironmentDimension],
    feedback: &[MarketEnvironmentFeedback],
    return5: Option<f64>,
    breadth: Option<f64>,
) -> (String, String) {
    let value = |key: &str| {
        dimensions
            .iter()
            .find(|item| item.key == key)
            .and_then(|item| item.value)
    };
    let rate = |key: &str| {
        feedback
            .iter()
            .find(|item| item.key == key)
            .and_then(|item| item.rate_pct)
    };
    let Some(trend) = value("direction") else {
        return (
            "方向数据尚不完整".to_string(),
            "近10日方向数据不足，暂不能形成全局判断。已有事件证据可展开查看。".to_string(),
        );
    };
    let up = trend > 0.2;
    let down = trend < -0.2;
    let pullback = up && return5.is_some_and(|ret| ret < 0.0);
    let rebound = down && return5.is_some_and(|ret| ret > 0.0);
    let attack = rate("attack");
    let defense = rate("defense");
    let kept = rate("kept");
    let repair = rate("repair");
    let headline = if pullback {
        "上行中的回撤，短线转弱"
    } else if rebound {
        "下行中的反弹，整体仍偏弱"
    } else if up && breadth.is_some_and(|v| v < 50.0) {
        "平均上涨，多数股票尚未跟上"
    } else if down && breadth.is_some_and(|v| v > 50.0) {
        "平均下行，多数股票近10日仍上涨"
    } else if down && defense.is_some_and(|v| v > 50.0) && repair.is_some_and(|v| v < 50.0) {
        "下探有抵抗，整体仍偏弱"
    } else if up && kept.is_some_and(|v| v > 50.0) {
        "整体上行，上攻成果多数保住"
    } else if up {
        "市场整体偏强"
    } else if down {
        "市场整体偏弱"
    } else {
        "市场来回拉锯，方向不集中"
    };
    let mut direction = if pullback {
        "近10日仍以上行为主，最近5日已回落。"
    } else if rebound {
        "最近5日有所回升，近10日仍以下行为主。"
    } else if up {
        "近10日等权市场向上推进，整体方向偏强。"
    } else if down {
        "近10日等权市场向下推进，整体方向偏弱。"
    } else {
        "近10日涨跌来回抵消，整体仍是震荡。"
    }
    .to_string();
    if let Some(breadth) = breadth {
        direction.push_str(&format!(
            "同期个股累计上涨的占{breadth:.0}%，{}。",
            if breadth > 50.0 {
                "多数股票近10日仍有上涨"
            } else if breadth < 50.0 {
                "上涨尚未覆盖多数股票"
            } else {
                "上涨股票约占一半"
            }
        ));
    }
    let attack_text = match (attack, kept) {
        (Some(a), Some(k)) => format!(
            "近5日上攻尝试中，{a:.0}%在收盘站稳；此前已站稳的股票，次日{k:.0}%保住原参考位。{}",
            if a < 50.0 && k > 50.0 {
                "新上攻多数未能站稳，已取得的成果则多数保住。"
            } else if a > 50.0 && k > 50.0 {
                "上攻站稳与后续保留都超过一半。"
            } else if a > 50.0 && k < 50.0 {
                "上攻当日多数站稳，后续成果却多数未能保留。"
            } else if a < 50.0 && k < 50.0 {
                "上攻站稳与后续保留都不足一半。"
            } else {
                "上攻与后续保留仍有分化。"
            }
        ),
        (Some(a), None) => format!("近5日上攻尝试中，{a:.0}%在收盘站稳；后续成果保留样本不足。"),
        (None, Some(k)) => format!("此前已上攻站稳者，次日{k:.0}%保住原参考位；新上攻样本不足。"),
        _ => "近期上攻与成果保留样本不足，暂不判断。".to_string(),
    };
    let repair_text = match (defense, repair) {
        (Some(d), Some(r)) => format!(
            "下探尝试中，{d:.0}%在当日收回；前日已收盘跌破的股票，次日{r:.0}%回到原参考位。{}",
            if d > 50.0 && r < 50.0 {
                "下探时存在抵抗，已经形成的跌破多数仍未修复。"
            } else if d > 50.0 && r > 50.0 {
                "当日收回与既有跌破的次日修复都超过一半。"
            } else if d < 50.0 && r > 50.0 {
                "当日下探多数未收回，前日跌破者则多数得到修复。"
            } else if d < 50.0 && r < 50.0 {
                "当日收回与既有跌破的次日修复都不足一半。"
            } else {
                "抵抗与修复的表现尚不一致。"
            }
        ),
        (Some(d), None) => format!("下探尝试中，{d:.0}%在当日收回；既有跌破的次日修复样本不足。"),
        (None, Some(r)) => format!("前日已收盘跌破者，次日{r:.0}%修复；当日下探样本不足。"),
        _ => "近期下探与修复样本不足，暂不判断。".to_string(),
    };
    (
        headline.to_string(),
        format!("{direction}\n\n{attack_text}\n\n{repair_text}"),
    )
}

fn insufficient_data(
    reference: Option<&str>,
    latest: Option<&str>,
    summary: String,
) -> MarketEnvironmentData {
    MarketEnvironmentData {
        reference_trade_date: reference.map(str::to_string),
        analysis_trade_date: None,
        latest_trade_date: latest.map(str::to_string),
        fallback_reason: None,
        coverage: None,
        return_5d_pct: None,
        return_10d_pct: None,
        breadth_10d_pct: None,
        feedback: Vec::new(),
        phase: None,
        headline: "数据不足，暂不能判断全局".to_string(),
        summary,
        status: MarketEnvironmentStatus::Insufficient,
        dimensions: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn synthetic_series(len: usize, day: usize, missing: bool, ret: f64) -> StockSeries {
        let mut rows = vec![None; len];
        for (index, row) in rows.iter_mut().enumerate() {
            if missing && index == day {
                continue;
            }
            *row = Some(Bar {
                high: 11.0,
                low: 9.0,
                close: 10.0,
                logret: if ret > -100.0 {
                    (1.0 + ret / 100.0).ln()
                } else {
                    f64::NAN
                },
                ret,
                intraday: ret * 0.5,
                closepos: 0.0,
                amount: 1000.0,
                size_yi: 300.0,
                log_impact: (ret.abs().max(0.01) / 1000.0).ln(),
            });
        }
        StockSeries { rows }
    }

    #[test]
    fn overview_separates_direction_participation_and_repair() {
        for (trend, recent, breadth, expected) in [
            (0.6, -1.0, 70.0, "上行中的回撤"),
            (-0.6, 1.0, 30.0, "下行中的反弹"),
            (0.6, 1.0, 30.0, "平均上涨，多数股票尚未跟上"),
            (-0.6, -1.0, 30.0, "下探有抵抗"),
        ] {
            let dimensions = [MarketEnvironmentDimension {
                group: MarketEnvironmentGroup::Direction,
                key: "direction".to_string(),
                label: String::new(),
                unit: String::new(),
                value: Some(trend),
                change_5d: None,
                interpretation: String::new(),
                sample_count: 10,
                sample_label: String::new(),
                history: Vec::new(),
            }];
            let feedback = [
                ("attack", 40.0),
                ("kept", 60.0),
                ("defense", 55.0),
                ("repair", 25.0),
            ]
            .into_iter()
            .map(|(key, rate)| MarketEnvironmentFeedback {
                key: key.to_string(),
                label: String::new(),
                rate_pct: Some(rate),
                change_5d_pp: None,
                events: 100,
                successes: rate as usize,
                stocks: 60,
                description: String::new(),
            })
            .collect::<Vec<_>>();
            let (headline, summary) =
                summarize(&dimensions, &feedback, Some(recent), Some(breadth));
            assert!(headline.starts_with(expected), "{headline}");
            assert!(summary.contains("新上攻多数未能站稳"));
            assert!(summary.contains("已经形成的跌破多数仍未修复"));
        }
        assert_eq!(summarize(&[], &[], None, None).0, "方向数据尚不完整");
    }

    #[test]
    fn event_feedback_keeps_original_reference_and_does_not_skip_gaps() {
        let codes = (0..60).map(|n| format!("{n:06}.SZ")).collect::<Vec<_>>();
        let listings = codes
            .iter()
            .map(|code| {
                (
                    code.clone(),
                    Listing {
                        listed_index: 0,
                        delist_index: None,
                    },
                )
            })
            .collect::<HashMap<_, _>>();
        let mut stocks = (0..60)
            .map(|_| synthetic_series(40, 40, false, 0.0))
            .collect::<Vec<_>>();
        for (index, stock) in stocks.iter_mut().enumerate() {
            for (day, high, low, close) in [
                (24, 10.0, 8.0, 8.5),
                (25, 11.0, 8.0, if index == 0 { 8.7 } else { 9.5 }),
                (26, 12.0, 9.0, 11.5),
                (27, 12.0, 10.0, 11.3),
            ] {
                let bar = stock.rows[day].as_mut().unwrap();
                bar.high = high;
                bar.low = low;
                bar.close = close;
            }
        }
        let daily = (0..29)
            .map(|day| day_raw(&codes, &listings, &stocks, 100, day))
            .collect::<Vec<_>>();
        let readings = feedback_readings(&daily, 28);
        assert_eq!(readings[0].events, 60);
        assert_eq!(readings[2].rate_pct, Some(100.0));
        assert_eq!(readings[3].events, 60);
        assert_eq!(readings[3].successes, 59);
        assert_eq!(readings[3].stocks, 60);
        stocks[0].rows[25] = None;
        assert!(
            day_raw(&codes, &listings, &stocks, 100, 26).feedback[3]
                .stocks
                .is_empty()
        );
        let mut incomplete = daily;
        incomplete[26].covered = false;
        assert!(feedback_readings(&incomplete, 28)[3].rate_pct.is_none());
    }

    #[test]
    fn coverage_fallback_is_by_session_and_unknown_listing_is_reported() {
        let len = 80;
        let calendar = (0..len)
            .map(|index| format!("2025{index:04}"))
            .collect::<Vec<_>>();
        let mut listings = HashMap::new();
        let mut stocks = Vec::new();
        for index in 0..24 {
            let code = format!("{index:06}.SZ");
            listings.insert(
                code,
                Listing {
                    listed_index: 0,
                    delist_index: None,
                },
            );
            stocks.push(synthetic_series(len, len - 1, index < 5, 0.5));
        }
        let codes = listings.keys().cloned().collect::<Vec<_>>();
        let data = analyze_window(
            "20250080",
            "20250080",
            &calendar,
            0,
            &codes,
            &listings,
            &stocks,
            1,
            &vec![0usize; len],
        );
        assert_eq!(data.analysis_trade_date.as_deref(), Some("20250078"));
        assert_eq!(
            data.coverage
                .as_ref()
                .map(|coverage| coverage.unknown_listing_stocks),
            Some(1)
        );
        assert_eq!(data.status, MarketEnvironmentStatus::Ok);
        assert!(data.fallback_reason.is_some());
    }

    #[test]
    fn missing_return_is_excluded_from_tail_denominator() {
        let len = 40;
        let stocks = [-4.0, 2.0, f64::NAN].map(|ret| synthetic_series(len, len, false, ret));
        let raw = day_raw(
            &[
                "000001.SZ".to_string(),
                "000002.SZ".to_string(),
                "000003.SZ".to_string(),
            ],
            &HashMap::from([
                (
                    "000001.SZ".to_string(),
                    Listing {
                        listed_index: 0,
                        delist_index: None,
                    },
                ),
                (
                    "000002.SZ".to_string(),
                    Listing {
                        listed_index: 0,
                        delist_index: None,
                    },
                ),
                (
                    "000003.SZ".to_string(),
                    Listing {
                        listed_index: 0,
                        delist_index: None,
                    },
                ),
            ]),
            &stocks,
            20,
            39,
        );
        assert_eq!(raw.samples.tail, 2);
        assert_eq!(raw.downside_tail, Some(0.5));
    }
}
