use std::{
    collections::HashMap,
    sync::{Arc, Mutex, OnceLock},
    time::{Duration, Instant},
};

use duckdb::{Connection, params};
use lianghua_data::data::{load_trade_date_list, source_db_path};
use lianghua_provider::crawler::{
    SinaQuote,
    volume_ratio::{TencentVolumeRatioData, fetch_tencent_volume_ratio, intraday_trading_minutes},
};

fn load_local_volume_averages(
    conn: &Connection,
    dates: &[String],
) -> Result<HashMap<String, f64>, String> {
    if dates.len() != 5 {
        return Err("本地交易日历不足五日".to_string());
    }
    let mut stmt = conn
        .prepare(
            "SELECT ts_code, AVG(TRY_CAST(vol AS DOUBLE)) FROM stock_data
         WHERE adj_type='qfq' AND trade_date IN (?, ?, ?, ?, ?)
         GROUP BY ts_code
         HAVING COUNT(DISTINCT trade_date)=5 AND COUNT(*)=5
            AND COUNT(TRY_CAST(vol AS DOUBLE))=5 AND MIN(TRY_CAST(vol AS DOUBLE))>=0
            AND isfinite(AVG(TRY_CAST(vol AS DOUBLE))) AND AVG(TRY_CAST(vol AS DOUBLE))>0",
        )
        .map_err(|e| format!("预编译本地量比基准失败: {e}"))?;
    let mut rows = stmt
        .query(params![dates[0], dates[1], dates[2], dates[3], dates[4]])
        .map_err(|e| format!("查询本地量比基准失败: {e}"))?;
    let mut values = HashMap::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取本地量比基准失败: {e}"))?
    {
        values.insert(
            row.get(0)
                .map_err(|e| format!("读取本地量比代码失败: {e}"))?,
            row.get(1)
                .map_err(|e| format!("读取本地量比均量失败: {e}"))?,
        );
    }
    Ok(values)
}

struct Baseline {
    data: TencentVolumeRatioData,
    observed_high: f64,
    observed_opening_high: f64,
    next_point: usize,
    loaded_at: Instant,
}

impl Baseline {
    fn apply_quote(&mut self, quote: &SinaQuote) -> Option<VolumeRatioValue> {
        let quote_minute = quote.time.replace(':', "");
        let quote_minute = quote_minute.get(..4)?;
        let (minutes, value) = if let Some(minutes) = intraday_trading_minutes(&quote.time) {
            if !quote.vol.is_finite() || quote.vol < 0.0 {
                return None;
            }
            (
                minutes,
                quote.vol * self.data.volume_to_hands
                    / (self.data.average_daily_volume * minutes / 240.0),
            )
        } else {
            // 午休冻结上午值，盘后冻结收盘值，不能把休市时间或盘后量混入计算。
            let minutes = if quote.time.as_str() > "15:00:00" {
                240.0
            } else if quote.time.as_str() > "11:30:00" && quote.time.as_str() < "13:00:00" {
                120.0
            } else {
                return None;
            };
            let end = self.data.points.partition_point(|point| {
                point.trading_minutes <= minutes && point.time.as_str() <= quote_minute
            });
            let last = self.data.points.get(end.checked_sub(1)?)?;
            (minutes, last.volume_ratio)
        };
        // 曲线时间单调递增，每个历史点只归并一次；实时值每轮仍重新计算。
        while let Some(point) = self.data.points.get(self.next_point) {
            if point.trading_minutes > minutes || point.time.as_str() > quote_minute {
                break;
            }
            self.observed_high = self.observed_high.max(point.volume_ratio);
            if point.trading_minutes <= 5.0 {
                self.observed_opening_high = self.observed_opening_high.max(point.volume_ratio);
            }
            self.next_point += 1;
        }
        let high = self.observed_high.max(value);
        self.observed_high = high;
        if minutes <= 5.0 {
            self.observed_opening_high = self.observed_opening_high.max(value);
        }
        // 分时覆盖到 09:35 后才确认开盘五分钟完整；09:35 的端点仍计入基准。
        let opening_high = (minutes > 5.0
            && self
                .data
                .points
                .last()
                .is_some_and(|point| point.trading_minutes >= 5.0))
        .then_some(self.observed_opening_high);
        Some(VolumeRatioValue {
            current: value,
            high,
            opening_high,
        })
    }
}

enum Entry {
    Pending,
    Ready(Baseline),
    Failed {
        at: Instant,
        error: String,
        baseline: Option<Baseline>,
    },
}

#[derive(Default)]
struct Cache {
    source_path: String,
    date: String,
    revision: u64,
    generation: u64,
    last_poll: Option<Instant>,
    entries: HashMap<String, Entry>,
    local_averages: HashMap<String, f64>,
    local_loaded_at: Option<Instant>,
    local_error: Option<String>,
}

#[derive(Debug, PartialEq)]
pub struct VolumeRatioValue {
    pub current: f64,
    pub high: f64,
    pub opening_high: Option<f64>,
}

pub struct VolumeRatioSnapshot {
    pub values: HashMap<String, VolumeRatioValue>,
    pub revision: u64,
    pub status: Option<String>,
}

static CACHE: OnceLock<Arc<Mutex<Cache>>> = OnceLock::new();
static WORKERS: OnceLock<Result<(rayon::ThreadPool, reqwest::blocking::Client), String>> =
    OnceLock::new();

pub fn snapshot(
    source_path: &str,
    quotes: &HashMap<String, SinaQuote>,
    trade_date: &str,
) -> Result<VolumeRatioSnapshot, String> {
    let cache = CACHE.get_or_init(|| Arc::new(Mutex::new(Cache::default())));
    let (workers, http) = WORKERS
        .get_or_init(|| {
            let workers = rayon::ThreadPoolBuilder::new()
                .num_threads(4)
                .thread_name(|index| format!("volume-ratio-{index}"))
                .build()
                .map_err(|e| format!("创建量比补齐线程失败: {e}"))?;
            let http = reqwest::blocking::Client::builder()
                .connect_timeout(Duration::from_secs(6))
                .timeout(Duration::from_secs(12))
                .build()
                .map_err(|e| format!("创建量比客户端失败: {e}"))?;
            Ok((workers, http))
        })
        .as_ref()
        .map_err(Clone::clone)?;
    let mut state = cache.lock().map_err(|_| "量比缓存锁已损坏".to_string())?;
    if state.source_path != source_path
        || state.date != trade_date
        || state
            .last_poll
            .is_some_and(|at| at.elapsed() >= Duration::from_secs(60))
    {
        state.date = trade_date.to_string();
        state.source_path = source_path.to_string();
        state.entries.clear();
        state.revision += 1;
        state.generation += 1;
        state.local_loaded_at = None;
    }
    state.last_poll = Some(Instant::now());
    if state
        .local_loaded_at
        .is_none_or(|at| at.elapsed() >= Duration::from_secs(60))
    {
        let result = load_trade_date_list(source_path).and_then(|mut dates| {
            dates.retain(|date| date.as_str() < trade_date);
            dates.sort_unstable_by(|a, b| b.cmp(a));
            dates.dedup();
            dates.truncate(5);
            let conn = Connection::open(source_db_path(source_path))
                .map_err(|e| format!("打开本地量比日线失败: {e}"))?;
            load_local_volume_averages(&conn, &dates)
        });
        match result {
            Ok(values) => {
                state.local_averages = values;
                state.local_error = None;
            }
            Err(error) => {
                state.local_averages.clear();
                state.local_error = Some(error);
            }
        }
        state.local_loaded_at = Some(Instant::now());
    }
    let mut pending = 0;
    let mut failed = 0;
    let mut first_error = None;
    let mut values = HashMap::new();
    for (ts_code, quote) in quotes {
        if quote.date.replace('-', "") != trade_date {
            continue;
        }
        let retry = match state.entries.get(ts_code) {
            None => true,
            Some(Entry::Failed { at, .. }) => at.elapsed() >= Duration::from_secs(60),
            Some(Entry::Ready(baseline)) => {
                quote.time.as_str() > "09:35:00"
                    && baseline
                        .data
                        .points
                        .last()
                        .is_none_or(|point| point.trading_minutes < 5.0)
                    && baseline.loaded_at.elapsed() >= Duration::from_secs(60)
            }
            _ => false,
        };
        if retry {
            let previous = match state.entries.insert(ts_code.clone(), Entry::Pending) {
                Some(Entry::Ready(baseline)) => Some(baseline),
                Some(Entry::Failed { baseline, .. }) => baseline,
                _ => None,
            };
            let cache = Arc::clone(cache);
            let ts_code = ts_code.clone();
            let date = trade_date.to_string();
            let http = http.clone();
            let generation = state.generation;
            let local_average = state.local_averages.get(&ts_code).copied();
            let local_error = state.local_error.clone();
            workers.spawn(move || {
                // 跨日、停止轮询后丢弃排队任务，暂停期间不持续抓取全市场。
                if let Ok(mut state) = cache.lock() {
                    if state.generation != generation {
                        return;
                    }
                    if state
                        .last_poll
                        .is_none_or(|at| at.elapsed() >= Duration::from_secs(60))
                    {
                        state.entries.remove(&ts_code);
                        state.revision += 1;
                        return;
                    }
                } else {
                    return;
                }
                let result = if let Some(average) = local_average {
                    fetch_tencent_volume_ratio(&http, &ts_code, average)
                } else {
                    Err(local_error.unwrap_or_else(|| {
                        format!("{ts_code} 本地日线缺少 {date} 前五个交易日，请先更新日线")
                    }))
                }
                .and_then(|data| {
                    if data.trade_date != date {
                        return Err(format!(
                            "分时日期 {} 与行情日期 {date} 不一致",
                            data.trade_date
                        ));
                    }
                    Ok(data)
                });
                if let Ok(mut state) = cache.lock() {
                    if state.generation != generation {
                        return;
                    }
                    state.entries.insert(
                        ts_code,
                        match result {
                            Ok(data) => Entry::Ready(Baseline {
                                data,
                                observed_high: previous
                                    .as_ref()
                                    .map_or(0.0, |baseline| baseline.observed_high),
                                observed_opening_high: previous
                                    .as_ref()
                                    .map_or(0.0, |baseline| baseline.observed_opening_high),
                                next_point: 0,
                                loaded_at: Instant::now(),
                            }),
                            Err(error) => Entry::Failed {
                                at: Instant::now(),
                                error,
                                baseline: previous,
                            },
                        },
                    );
                    state.revision += 1;
                }
            });
        }
        match state.entries.get_mut(ts_code) {
            Some(Entry::Ready(baseline)) => {
                if let Some(value) = baseline.apply_quote(quote) {
                    if value.opening_high.is_none() {
                        pending += 1;
                    }
                    values.insert(ts_code.clone(), value);
                }
            }
            Some(Entry::Pending) => pending += 1,
            Some(Entry::Failed { error, .. }) => {
                failed += 1;
                first_error.get_or_insert(error.clone());
            }
            None => {}
        }
    }
    let status = if pending > 0 || failed > 0 {
        Some(format!(
            "开盘五分钟量比基准：已补齐 {} 只，等待开盘五分钟结束或补齐中 {pending} 只，失败 {failed} 只；基准未完整的股票暂不提示量比新高{}",
            values
                .values()
                .filter(|value| value.opening_high.is_some())
                .count(),
            first_error
                .map(|error| format!("。首条错误：{error}（60 秒后重试）"))
                .unwrap_or_default()
        ))
    } else {
        None
    };
    Ok(VolumeRatioSnapshot {
        values,
        revision: state.revision,
        status,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use lianghua_provider::crawler::volume_ratio::TencentVolumeRatioPoint;

    #[test]
    fn local_baseline_requires_exact_five_dates_and_keeps_hand_units() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE stock_data(ts_code VARCHAR, trade_date VARCHAR, adj_type VARCHAR, vol DOUBLE);
            INSERT INTO stock_data VALUES
                ('920002.BJ','20260924','qfq',100),('920002.BJ','20260928','qfq',200),
                ('920002.BJ','20260929','qfq',300),('920002.BJ','20260930','qfq',400),
                ('920002.BJ','20261008','qfq',500),('920002.BJ','20261009','qfq',99999),
                ('920002.BJ','20261008','raw',99999),('920003.BJ','20260924','qfq',100);
            INSERT INTO stock_data SELECT code,trade_date,adj_type,vol FROM stock_data
                CROSS JOIN (VALUES ('000001.SZ'),('600000.SH'),('688185.SH')) AS codes(code)
                WHERE ts_code='920002.BJ';").unwrap();
        let mut dates =
            ["20260924", "20260928", "20260929", "20260930", "20261008"].map(str::to_string);
        let values = load_local_volume_averages(&conn, &dates).unwrap();
        assert_eq!(values.len(), 4);
        for code in ["920002.BJ", "000001.SZ", "600000.SH", "688185.SH"] {
            assert_eq!(values[code], 300.0);
        }
        dates[4] = "20261012".to_string();
        assert!(
            load_local_volume_averages(&conn, &dates)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn quote_uses_full_day_baseline_without_future_points_or_post_market_volume() {
        let mut baseline = Baseline {
            data: TencentVolumeRatioData {
                trade_date: "20261009".to_string(),
                average_daily_volume: 2400.0,
                volume_to_hands: 1.0,
                points: vec![
                    TencentVolumeRatioPoint {
                        time: "0930".to_string(),
                        trading_minutes: 1.0,
                        volume_ratio: 5.0,
                    },
                    TencentVolumeRatioPoint {
                        time: "1130".to_string(),
                        trading_minutes: 120.0,
                        volume_ratio: 1.0,
                    },
                    TencentVolumeRatioPoint {
                        time: "1400".to_string(),
                        trading_minutes: 180.0,
                        volume_ratio: 8.0,
                    },
                    TencentVolumeRatioPoint {
                        time: "1500".to_string(),
                        trading_minutes: 240.0,
                        volume_ratio: 2.0,
                    },
                ],
            },
            observed_high: 0.0,
            observed_opening_high: 0.0,
            next_point: 0,
            loaded_at: Instant::now(),
        };
        let mut quote = SinaQuote {
            date: "20261009".to_string(),
            time: "10:00:00".to_string(),
            ts_code: "000001.SZ".to_string(),
            name: String::new(),
            open: 10.0,
            high: 10.0,
            low: 10.0,
            pre_close: 10.0,
            price: 10.0,
            vol: 300.0,
            amount: 300000.0,
            change_pct: Some(0.0),
        };
        assert_eq!(
            baseline.apply_quote(&quote),
            Some(VolumeRatioValue {
                current: 1.0,
                high: 5.0,
                opening_high: Some(5.0)
            })
        );
        baseline.data.volume_to_hands = 0.01;
        quote.vol *= 100.0;
        assert_eq!(
            baseline.apply_quote(&quote),
            Some(VolumeRatioValue {
                current: 1.0,
                high: 5.0,
                opening_high: Some(5.0)
            })
        );
        baseline.data.volume_to_hands = 1.0;
        quote.vol /= 100.0;
        quote.time = "12:00:00".to_string();
        assert_eq!(
            baseline.apply_quote(&quote),
            Some(VolumeRatioValue {
                current: 1.0,
                high: 5.0,
                opening_high: Some(5.0)
            })
        );
        quote.time = "13:00:00".to_string();
        quote.vol = 7200.0;
        assert_eq!(
            baseline.apply_quote(&quote),
            Some(VolumeRatioValue {
                current: 6.0,
                high: 6.0,
                opening_high: Some(5.0)
            })
        );
        quote.time = "15:30:00".to_string();
        quote.vol = 999999.0;
        assert_eq!(
            baseline.apply_quote(&quote),
            Some(VolumeRatioValue {
                current: 2.0,
                high: 8.0,
                opening_high: Some(5.0)
            })
        );
    }

    #[test]
    fn quote_opening_baseline_waits_for_complete_five_minutes_and_keeps_live_peak() {
        let mut baseline = Baseline {
            data: TencentVolumeRatioData {
                trade_date: "20261009".to_string(),
                average_daily_volume: 2400.0,
                volume_to_hands: 1.0,
                points: vec![
                    TencentVolumeRatioPoint {
                        time: "0930".to_string(),
                        trading_minutes: 1.0,
                        volume_ratio: 2.0,
                    },
                    TencentVolumeRatioPoint {
                        time: "0934".to_string(),
                        trading_minutes: 4.0,
                        volume_ratio: 3.0,
                    },
                ],
            },
            observed_high: 0.0,
            observed_opening_high: 0.0,
            next_point: 0,
            loaded_at: Instant::now(),
        };
        let mut quote = SinaQuote {
            date: "20261009".to_string(),
            time: "09:34:30".to_string(),
            ts_code: "000001.SZ".to_string(),
            name: String::new(),
            open: 10.0,
            high: 10.0,
            low: 10.0,
            pre_close: 10.0,
            price: 10.0,
            vol: 180.0,
            amount: 180000.0,
            change_pct: Some(0.0),
        };
        assert_eq!(
            baseline.apply_quote(&quote),
            Some(VolumeRatioValue {
                current: 4.0,
                high: 4.0,
                opening_high: None,
            })
        );
        quote.time = "09:36:00".to_string();
        quote.vol = 300.0;
        assert_eq!(baseline.apply_quote(&quote).unwrap().opening_high, None);
        baseline.data.points.push(TencentVolumeRatioPoint {
            time: "0935".to_string(),
            trading_minutes: 5.0,
            volume_ratio: 3.5,
        });
        quote.time = "09:35:00".to_string();
        quote.vol = 200.0;
        assert_eq!(baseline.apply_quote(&quote).unwrap().opening_high, None);
        quote.time = "09:36:00".to_string();
        quote.vol = 300.0;
        assert_eq!(
            baseline.apply_quote(&quote),
            Some(VolumeRatioValue {
                current: 5.0,
                high: 5.0,
                opening_high: Some(4.0),
            })
        );
        quote.time = "09:37:00".to_string();
        assert_eq!(
            baseline.apply_quote(&quote).unwrap().opening_high,
            Some(4.0)
        );
    }
}
