pub mod ind_calc;
pub mod runner;

use rayon::prelude::*;
use std::{
    collections::HashMap,
    sync::Mutex,
    thread::sleep,
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};

use crate::data::download_data::{write_stock_list_csv, write_trade_calendar_csv};
use crate::download::ind_calc::calc_one_stock_inds;

pub struct DownloadConfig {
    pub start_date: String,
    pub end_date: String,
    pub adj_type: AdjType,
    pub token: String,
    pub source_dir: String,
    pub stock_list: Vec<String>,
}

#[derive(Serialize)]
struct DailyParams<'a> {
    ts_code: &'a str,
    start_date: &'a str,
    end_date: &'a str,
}

#[derive(Serialize)]
struct TradeDateParams<'a> {
    trade_date: &'a str,
}

#[derive(Serialize)]
struct StockBasicParams<'a> {
    exchange: &'a str,
    list_status: &'a str,
}

#[derive(Serialize)]
struct DailyBasicTradeDateParams<'a> {
    trade_date: &'a str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdjType {
    Qfq,
    Hfq,
    Raw,
    Ind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BarFreq {
    Daily,
    Weekly,
    Monthly,
}

#[derive(Debug, Clone)]
pub struct BarRow {
    pub ts_code: String,
    pub trade_date: String,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub pre_close: f64,
    pub change: f64,
    pub pct_chg: f64,
    pub vol: f64,
    pub amount: f64,
}

#[derive(Debug, Clone)]
pub struct AdjFactorRow {
    pub ts_code: String,
    pub trade_date: String,
    pub adj_factor: f64,
}

#[derive(Debug, Clone)]
pub struct DailyBasicRow {
    pub ts_code: String,
    pub trade_date: String,
    pub turnover_rate: Option<f64>,
    pub volume_ratio: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct StockBasicRow {
    pub ts_code: String,
    pub symbol: String,
    pub name: String,
    pub area: String,
    pub industry: String,
    pub fullname: String,
    pub enname: String,
    pub cnspell: String,
    pub market: String,
    pub exchange: String,
    pub curr_type: String,
    pub list_status: String,
    pub list_date: String,
    pub delist_date: String,
    pub is_hs: String,
    pub act_name: String,
    pub act_ent_type: String,
}

#[derive(Debug, Clone)]
pub struct DailyBasicSnapshotRow {
    pub ts_code: String,
    pub trade_date: String,
    pub total_share: Option<f64>,
    pub float_share: Option<f64>,
    pub total_mv: Option<f64>,
    pub circ_mv: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct StockListRow {
    pub ts_code: String,
    pub symbol: String,
    pub name: String,
    pub area: String,
    pub industry: String,
    pub list_date: String,
    pub trade_date: String,
    pub total_share: Option<f64>,
    pub float_share: Option<f64>,
    pub total_mv: Option<f64>,
    pub circ_mv: Option<f64>,
    pub fullname: String,
    pub enname: String,
    pub cnspell: String,
    pub market: String,
    pub exchange: String,
    pub curr_type: String,
    pub list_status: String,
    pub delist_date: String,
    pub is_hs: String,
    pub act_name: String,
    pub act_ent_type: String,
}

#[derive(Debug, Clone)]
pub struct TradeCalRow {
    // 交易日列表返回值结构
    pub exchange: String,
    pub cal_date: String,
    pub is_open: String,
    pub pretrade_date: String,
}

#[derive(Serialize)]
struct TradeCalParams<'a> {
    exchange: &'a str,
    start_date: &'a str,
    end_date: &'a str,
    is_open: &'a str,
}

pub struct TushareClient {
    // 主要连接
    pub base_url: String,
    pub token: String,
    pub http: reqwest::blocking::Client,
    pub limiter: RateLimiter,
}

pub struct RateLimiter {
    pub min_interval: Duration,
    pub next_allowed_at: Mutex<HashMap<String, Instant>>,
}

impl RateLimiter {
    pub fn new(calls_per_min: usize) -> Result<Self, String> {
        if calls_per_min == 0 {
            return Err("calls_per_min 不能为 0".to_string());
        }

        let interval_ms = 60_000u64 / calls_per_min as u64;

        Ok(Self {
            min_interval: Duration::from_millis(interval_ms.max(1)),
            next_allowed_at: Mutex::new(HashMap::new()),
        })
    }

    pub fn wait(&self, api_name: &str) -> Result<(), String> {
        let now = Instant::now();
        let sleep_for = {
            let mut next_allowed_at = self
                .next_allowed_at
                .lock()
                .map_err(|_| "限频器锁已中毒".to_string())?;

            let reserved_at = match next_allowed_at.get(api_name).copied() {
                Some(next_at) if next_at > now => next_at,
                _ => now,
            };

            next_allowed_at.insert(api_name.to_string(), reserved_at + self.min_interval);
            reserved_at.saturating_duration_since(now)
        };

        if !sleep_for.is_zero() {
            sleep(sleep_for);
        }

        Ok(())
    }
}

#[derive(Serialize, Default)]
struct TushareRequest<'a, T> {
    api_name: &'a str,
    token: &'a str,
    params: T,
    fields: &'a str,
}

#[derive(Deserialize)]
struct TushareEnvelope {
    code: i32,
    msg: Option<String>,
    data: Option<TushareTable>,
}

#[derive(Deserialize)]
pub struct TushareTable {
    pub fields: Vec<String>,
    pub items: Vec<Vec<serde_json::Value>>,
}

#[derive(Debug, Default)]
pub struct DownloadSummary {
    pub success_count: usize,
    pub failed_count: usize,
    pub saved_rows: usize,
    pub failed_items: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
pub struct PreparedStockDownload {
    pub ts_code: String,
    pub start_date: String,
    pub end_date: String,
    pub adj_type: AdjType,
    pub rows: Vec<ProBarRow>,
    pub indicators: HashMap<String, Vec<Option<f64>>>,
}

#[derive(Debug, Default)]
pub struct PreparedDownloadBatch {
    pub prepared_items: Vec<PreparedStockDownload>,
    pub failed_items: Vec<(String, String)>,
}

impl PreparedDownloadBatch {
    pub fn summary(&self) -> DownloadSummary {
        DownloadSummary {
            success_count: self.prepared_items.len(),
            failed_count: self.failed_items.len(),
            saved_rows: self.prepared_items.iter().map(|item| item.rows.len()).sum(),
            failed_items: self.failed_items.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct DownloadTask {
    pub ts_code: String,
    pub start_date: String,
    pub end_date: String,
    pub freq: BarFreq,
    pub adj_type: AdjType,
    pub with_factors: bool,
}

impl TushareClient {
    pub fn new(token: String, calls_per_min: usize) -> Result<Self, String> {
        let http = reqwest::blocking::Client::builder()
            .build()
            .map_err(|e| format!("创建HTTP客户端失败: {e}"))?;
        Ok(Self {
            base_url: "http://api.tushare.pro".to_string(),
            token,
            http,
            limiter: RateLimiter::new(calls_per_min)?,
        })
    }

    fn post_table<P: Serialize>(
        &self,
        api_name: &str,
        params: &P,
        fields: &str,
    ) -> Result<TushareTable, String> {
        self.limiter.wait(api_name)?;

        let json_body = TushareRequest {
            api_name,
            token: &self.token,
            params,
            fields,
        };
        let envelope: TushareEnvelope = self
            .http
            .post(&self.base_url)
            .json(&json_body)
            .send()
            .map_err(|e| format!("请求 {api_name} 失败: {e}"))?
            .error_for_status()
            .map_err(|e| format!("{api_name} 返回 HTTP 错误: {e}"))?
            .json()
            .map_err(|e| format!("解析 {api_name} 响应失败: {e}"))?;

        if envelope.code != 0 {
            return Err(format!(
                "Tushare接口 {} 返回业务错误: code={}, msg={}",
                api_name,
                envelope.code,
                envelope.msg.unwrap_or_default()
            ));
        }

        envelope
            .data
            .ok_or_else(|| format!("Tushare接口 {} 没有返回 data", api_name))
    }

    pub fn fetch_stock_basic_table(
        // 基础信息列, 一次调用basic
        &self,
        exchange: &str,
        list_status: &str,
    ) -> Result<TushareTable, String> {
        let params = StockBasicParams {
            exchange,
            list_status,
        };

        self.post_table(
            "stock_basic",
            &params,
            "ts_code,symbol,name,area,industry,fullname,enname,cnspell,market,exchange,curr_type,list_status,list_date,delist_date,is_hs,act_name,act_ent_type",
        )
    }

    pub fn fetch_all_stock_basic_rows(&self) -> Result<Vec<StockBasicRow>, String> {
        let mut by_ts_code: HashMap<String, StockBasicRow> = HashMap::new();

        for list_status in ["L", "P"] {
            let table = self.fetch_stock_basic_table("", list_status)?;
            let rows = parse_stock_basic_rows(&table)?;

            for row in rows {
                by_ts_code.insert(row.ts_code.clone(), row);
            }
        }

        let mut rows: Vec<StockBasicRow> = by_ts_code.into_values().collect();
        rows.sort_by(|a, b| a.ts_code.cmp(&b.ts_code));
        Ok(rows)
    }

    pub fn fetch_daily_basic_snapshot_table(
        &self,
        trade_date: &str,
    ) -> Result<TushareTable, String> {
        let params = DailyBasicTradeDateParams { trade_date };

        self.post_table(
            "daily_basic",
            &params,
            "ts_code,trade_date,total_share,float_share,total_mv,circ_mv",
        )
    }

    pub fn fetch_stock_list_rows(&self, trade_date: &str) -> Result<Vec<StockListRow>, String> {
        let basic_rows = self.fetch_all_stock_basic_rows()?;
        let snap_table = self.fetch_daily_basic_snapshot_table(trade_date)?;
        let snap_rows = parse_daily_basic_snapshot_rows(&snap_table)?;
        let snap_map = build_daily_basic_snapshot_map(snap_rows)?;
        merge_stock_list_rows(basic_rows, &snap_map, trade_date)
    }

    pub fn download_stock_list_csv(
        &self,
        source_dir: &str,
        trade_date: &str,
    ) -> Result<usize, String> {
        let rows = self.fetch_stock_list_rows(trade_date)?;
        write_stock_list_csv(source_dir, &rows)?;
        Ok(rows.len())
    }

    pub fn fetch_trade_cal_table(
        &self,
        exchange: &str,
        start_date: &str,
        end_date: &str,
        is_open: &str,
    ) -> Result<TushareTable, String> {
        let params = TradeCalParams {
            exchange,
            start_date,
            end_date,
            is_open,
        };

        self.post_table(
            "trade_cal",
            &params,
            "exchange,cal_date,is_open,pretrade_date",
        )
    }

    fn fetch_open_trade_cal_rows(
        &self,
        start_date: &str,
        end_date: &str,
    ) -> Result<Vec<TradeCalRow>, String> {
        let table = self.fetch_trade_cal_table("", start_date, end_date, "1")?;
        let mut rows = parse_trade_cal_rows(&table)?;
        rows.sort_by(|a, b| a.cal_date.cmp(&b.cal_date));
        rows.dedup_by(|a, b| a.cal_date == b.cal_date);
        Ok(rows)
    }

    pub fn download_trade_calendar_csv(
        &self,
        source_dir: &str,
        start_date: &str,
        end_date: &str,
    ) -> Result<usize, String> {
        let rows = self.fetch_open_trade_cal_rows(start_date, end_date)?;
        write_trade_calendar_csv(source_dir, &rows)?;
        Ok(rows.len())
    }

    fn fetch_single_daily_all(
        &self,
        ts_code: &str,
        start_date: &str,
        end_date: &str,
    ) -> Result<TushareTable, String> {
        let params = DailyParams {
            ts_code,
            start_date,
            end_date,
        };

        self.post_table(
            "daily",
            &params,
            "ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount",
        )
    }

    fn fetch_single_index_daily_all(
        &self,
        ts_code: &str,
        start_date: &str,
        end_date: &str,
    ) -> Result<TushareTable, String> {
        let params = DailyParams {
            ts_code,
            start_date,
            end_date,
        };

        self.post_table(
            "index_daily",
            &params,
            "ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount",
        )
    }

    fn fetch_single_adj_factor(
        &self,
        ts_code: &str,
        start_date: &str,
        end_date: &str,
    ) -> Result<TushareTable, String> {
        let params = DailyParams {
            ts_code,
            start_date,
            end_date,
        };

        self.post_table("adj_factor", &params, "ts_code,trade_date,adj_factor")
    }

    fn fetch_single_daily_basic_all(
        &self,
        ts_code: &str,
        start_date: &str,
        end_date: &str,
    ) -> Result<TushareTable, String> {
        let params = DailyParams {
            ts_code,
            start_date,
            end_date,
        };

        self.post_table(
            "daily_basic",
            &params,
            "ts_code,trade_date,turnover_rate,volume_ratio",
        )
    }

    fn fetch_single_weekly_all(
        &self,
        ts_code: &str,
        start_date: &str,
        end_date: &str,
    ) -> Result<TushareTable, String> {
        let params = DailyParams {
            ts_code,
            start_date,
            end_date,
        };

        self.post_table(
            "weekly",
            &params,
            "ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount",
        )
    }

    fn fetch_single_monthly_all(
        &self,
        ts_code: &str,
        start_date: &str,
        end_date: &str,
    ) -> Result<TushareTable, String> {
        let params = DailyParams {
            ts_code,
            start_date,
            end_date,
        };

        self.post_table(
            "monthly",
            &params,
            "ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount",
        )
    }

    fn fetch_base_bar_table(
        &self,
        ts_code: &str,
        start_date: &str,
        end_date: &str,
        freq: BarFreq,
    ) -> Result<TushareTable, String> {
        match freq {
            BarFreq::Daily => self.fetch_single_daily_all(ts_code, start_date, end_date),
            BarFreq::Weekly => self.fetch_single_weekly_all(ts_code, start_date, end_date),
            BarFreq::Monthly => self.fetch_single_monthly_all(ts_code, start_date, end_date),
        }
    }

    pub fn fetch_single_pro_bar(
        &self,
        ts_code: &str,
        start_date: &str,
        end_date: &str,
        freq: BarFreq,
        adj_type: AdjType,
        with_factors: bool,
    ) -> Result<Vec<ProBarRow>, String> {
        //单股下载总函数

        let bar_table = self.fetch_base_bar_table(ts_code, start_date, end_date, freq)?;
        let bar_rows = parse_bar_rows(&bar_table)?;

        let mut rows = if with_factors && freq == BarFreq::Daily {
            let basic_table = self.fetch_single_daily_basic_all(ts_code, start_date, end_date)?;
            let basic_rows = parse_daily_basic_rows(&basic_table)?;
            let basic_map = build_single_basic_map(basic_rows)?;
            build_single_basic_with_basiccol(bar_rows, &basic_map)?
        } else {
            build_pro_bar_rows(bar_rows)
        };

        rows.sort_by(|a, b| a.trade_date.cmp(&b.trade_date));
        if adj_type != AdjType::Raw {
            let adj_table = self.fetch_single_adj_factor(ts_code, start_date, end_date)?;
            let adj_rows = parse_adj_factor_rows(&adj_table)?;
            let adj_map = build_adj_factor_map(adj_rows)?;
            apply_adj_to_rows(&mut rows, &adj_type, &adj_map)?;
        }

        normalize_stock_rows_like_pro_bar(&mut rows);

        Ok(rows)
    }

    pub fn fetch_single_index_bar(
        &self,
        ts_code: &str,
        start_date: &str,
        end_date: &str,
    ) -> Result<Vec<ProBarRow>, String> {
        let bar_table = self.fetch_single_index_daily_all(ts_code, start_date, end_date)?;
        let bar_rows = parse_bar_rows(&bar_table)?;
        let mut rows = build_pro_bar_rows(bar_rows);
        rows.sort_by(|a, b| a.trade_date.cmp(&b.trade_date));
        recalc_change_fields(&mut rows);
        Ok(rows)
    }

    pub fn prepare_one_stock_download(
        &self,
        source_dir: String,
        ts_code: String,
        start_date: String,
        end_date: String,
        freq: BarFreq,
        adj_type: AdjType,
        with_factors: bool,
    ) -> Result<PreparedStockDownload, String> {
        let rows = self.fetch_single_pro_bar(
            &ts_code,
            &start_date,
            &end_date,
            freq,
            adj_type,
            with_factors,
        )?;
        let indicators = calc_one_stock_inds(&source_dir, &rows)?;

        println!("下载完成:{:?}", &ts_code);
        Ok(PreparedStockDownload {
            ts_code: ts_code.to_string(),
            start_date: start_date.to_string(),
            end_date: end_date.to_string(),
            adj_type,
            rows,
            indicators,
        })
    }

    pub fn prepare_one_index_download(
        &self,
        source_dir: String,
        ts_code: String,
        start_date: String,
        end_date: String,
    ) -> Result<PreparedStockDownload, String> {
        let rows = self.fetch_single_index_bar(&ts_code, &start_date, &end_date)?;
        let indicators = calc_one_stock_inds(&source_dir, &rows)?;

        println!("指数下载完成:{:?}", &ts_code);
        Ok(PreparedStockDownload {
            ts_code,
            start_date,
            end_date,
            adj_type: AdjType::Ind,
            rows,
            indicators,
        })
    }

    pub fn prepare_stock_downloads(
        &self,
        source_dir: &str,
        tasks: &[DownloadTask],
    ) -> PreparedDownloadBatch {
        let results = tasks
            .par_iter()
            .map(|task| {
                self.prepare_one_stock_download(
                    source_dir.to_string(),
                    task.ts_code.to_string(),
                    task.start_date.to_string(),
                    task.end_date.to_string(),
                    task.freq,
                    task.adj_type,
                    task.with_factors,
                )
                .map_err(|err| (task.ts_code.to_string(), err))
            })
            .collect::<Vec<_>>();

        let mut batch = PreparedDownloadBatch::default();
        for result in results {
            match result {
                Ok(prepared) => {
                    batch.prepared_items.push(prepared);
                }
                Err((ts_code, err)) => {
                    batch.failed_items.push((ts_code, err));
                }
            }
        }

        batch
    }

    pub fn prepare_index_downloads(
        &self,
        source_dir: &str,
        ts_codes: &[String],
        start_date: &str,
        end_date: &str,
    ) -> PreparedDownloadBatch {
        let results = ts_codes
            .par_iter()
            .map(|ts_code| {
                self.prepare_one_index_download(
                    source_dir.to_string(),
                    ts_code.to_string(),
                    start_date.to_string(),
                    end_date.to_string(),
                )
                .map_err(|err| (ts_code.to_string(), err))
            })
            .collect::<Vec<_>>();

        let mut batch = PreparedDownloadBatch::default();
        for result in results {
            match result {
                Ok(prepared) => batch.prepared_items.push(prepared),
                Err((ts_code, err)) => batch.failed_items.push((ts_code, err)),
            }
        }

        batch
    }

    fn fetch_daily_by_trade_date(&self, trade_date: &str) -> Result<TushareTable, String> {
        let params = TradeDateParams { trade_date };

        self.post_table(
            "daily",
            &params,
            "ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount",
        )
    }

    fn fetch_daily_basic_by_trade_date(&self, trade_date: &str) -> Result<TushareTable, String> {
        let params = TradeDateParams { trade_date };

        self.post_table(
            "daily_basic",
            &params,
            "ts_code,trade_date,turnover_rate,volume_ratio",
        )
    }

    pub fn fetch_market_daily(
        &self,
        trade_date: &str,
        with_factors: bool,
    ) -> Result<Vec<ProBarRow>, String> {
        let bar_table = self.fetch_daily_by_trade_date(trade_date)?;
        let bar_rows = parse_bar_rows(&bar_table)?;

        if with_factors {
            let basic_table = self.fetch_daily_basic_by_trade_date(trade_date)?;
            let basic_rows = parse_daily_basic_rows(&basic_table)?;
            let basic_map = build_market_basic_map(basic_rows)?;
            let mut rows = build_market_basic_with_basiccol(bar_rows, &basic_map)?;
            normalize_stock_rows_like_pro_bar(&mut rows);
            Ok(rows)
        } else {
            let mut rows = build_pro_bar_rows(bar_rows);
            normalize_stock_rows_like_pro_bar(&mut rows);
            Ok(rows)
        }
    }
}

impl TushareTable {
    fn field_index(&self, field_name: &str) -> Result<usize, String> {
        self.fields
            .iter()
            .position(|name| name == field_name)
            .ok_or_else(|| format!("Tushare返回缺少字段: {field_name}"))
    }

    fn value_as_string(value: &serde_json::Value, field_name: &str) -> Result<String, String> {
        if value.is_null() {
            return Ok(String::new());
        }
        if let Some(v) = value.as_str() {
            return Ok(v.to_string());
        }
        if let Some(v) = value.as_i64() {
            return Ok(v.to_string());
        }
        if let Some(v) = value.as_u64() {
            return Ok(v.to_string());
        }
        if let Some(v) = value.as_f64() {
            return Ok(v.to_string());
        }
        if let Some(v) = value.as_bool() {
            return Ok(v.to_string());
        }

        Err(format!("{field_name} 不是可转字符串的值"))
    }

    fn value_as_f64(value: &serde_json::Value, field_name: &str) -> Result<f64, String> {
        value
            .as_f64()
            .ok_or_else(|| format!("{field_name} 不是数字"))
    }

    fn value_as_opt_f64(
        value: &serde_json::Value,
        field_name: &str,
    ) -> Result<Option<f64>, String> {
        if value.is_null() {
            return Ok(None);
        }

        value
            .as_f64()
            .map(Some)
            .ok_or_else(|| format!("{field_name} 不是数字或null"))
    }
}

pub fn parse_bar_rows(table: &TushareTable) -> Result<Vec<BarRow>, String> {
    let ts_code_idx = table.field_index("ts_code")?;
    let trade_date_idx = table.field_index("trade_date")?;
    let open_idx = table.field_index("open")?;
    let high_idx = table.field_index("high")?;
    let low_idx = table.field_index("low")?;
    let close_idx = table.field_index("close")?;
    let pre_close_idx = table.field_index("pre_close")?;
    let change_idx = table.field_index("change")?;
    let pct_chg_idx = table.field_index("pct_chg")?;
    let vol_idx = table.field_index("vol")?;
    let amount_idx = table.field_index("amount")?;

    let mut rows = Vec::with_capacity(table.items.len());

    for item in &table.items {
        rows.push(BarRow {
            ts_code: TushareTable::value_as_string(&item[ts_code_idx], "ts_code")?,
            trade_date: TushareTable::value_as_string(&item[trade_date_idx], "trade_date")?,
            open: TushareTable::value_as_f64(&item[open_idx], "open")?,
            high: TushareTable::value_as_f64(&item[high_idx], "high")?,
            low: TushareTable::value_as_f64(&item[low_idx], "low")?,
            close: TushareTable::value_as_f64(&item[close_idx], "close")?,
            pre_close: TushareTable::value_as_f64(&item[pre_close_idx], "pre_close")?,
            change: TushareTable::value_as_f64(&item[change_idx], "change")?,
            pct_chg: TushareTable::value_as_f64(&item[pct_chg_idx], "pct_chg")?,
            vol: TushareTable::value_as_f64(&item[vol_idx], "vol")?,
            amount: TushareTable::value_as_f64(&item[amount_idx], "amount")?,
        });
    }

    Ok(rows)
}

pub fn parse_adj_factor_rows(table: &TushareTable) -> Result<Vec<AdjFactorRow>, String> {
    let ts_code_idx = table.field_index("ts_code")?;
    let trade_date_idx = table.field_index("trade_date")?;
    let adj_factor_idx = table.field_index("adj_factor")?;

    let mut rows = Vec::with_capacity(table.items.len());

    for item in &table.items {
        rows.push(AdjFactorRow {
            ts_code: TushareTable::value_as_string(&item[ts_code_idx], "ts_code")?,
            trade_date: TushareTable::value_as_string(&item[trade_date_idx], "trade_date")?,
            adj_factor: TushareTable::value_as_f64(&item[adj_factor_idx], "adj_factor")?,
        });
    }

    Ok(rows)
}

pub fn parse_daily_basic_rows(table: &TushareTable) -> Result<Vec<DailyBasicRow>, String> {
    let ts_code_idx = table.field_index("ts_code")?;
    let trade_date_idx = table.field_index("trade_date")?;
    let turnover_rate_idx = table.field_index("turnover_rate")?;
    let volume_ratio_idx = table.field_index("volume_ratio")?;

    let mut rows = Vec::with_capacity(table.items.len());

    for item in &table.items {
        rows.push(DailyBasicRow {
            ts_code: TushareTable::value_as_string(&item[ts_code_idx], "ts_code")?,
            trade_date: TushareTable::value_as_string(&item[trade_date_idx], "trade_date")?,
            turnover_rate: TushareTable::value_as_opt_f64(
                &item[turnover_rate_idx],
                "turnover_rate",
            )?,
            volume_ratio: TushareTable::value_as_opt_f64(&item[volume_ratio_idx], "volume_ratio")?,
        });
    }

    Ok(rows)
}

pub fn parse_daily_basic_snapshot_rows(
    table: &TushareTable,
) -> Result<Vec<DailyBasicSnapshotRow>, String> {
    let ts_code_idx = table.field_index("ts_code")?;
    let trade_date_idx = table.field_index("trade_date")?;
    let total_share_idx = table.field_index("total_share")?;
    let float_share_idx = table.field_index("float_share")?;
    let total_mv_idx = table.field_index("total_mv")?;
    let circ_mv_idx = table.field_index("circ_mv")?;

    let mut rows = Vec::with_capacity(table.items.len());

    for item in &table.items {
        rows.push(DailyBasicSnapshotRow {
            ts_code: TushareTable::value_as_string(&item[ts_code_idx], "ts_code")?,
            trade_date: TushareTable::value_as_string(&item[trade_date_idx], "trade_date")?,
            total_share: TushareTable::value_as_opt_f64(&item[total_share_idx], "total_share")?,
            float_share: TushareTable::value_as_opt_f64(&item[float_share_idx], "float_share")?,
            total_mv: TushareTable::value_as_opt_f64(&item[total_mv_idx], "total_mv")?,
            circ_mv: TushareTable::value_as_opt_f64(&item[circ_mv_idx], "circ_mv")?,
        });
    }

    Ok(rows)
}

pub fn parse_stock_basic_rows(table: &TushareTable) -> Result<Vec<StockBasicRow>, String> {
    let ts_code_idx = table.field_index("ts_code")?;
    let symbol_idx = table.field_index("symbol")?;
    let name_idx = table.field_index("name")?;
    let area_idx = table.field_index("area")?;
    let industry_idx = table.field_index("industry")?;
    let fullname_idx = table.field_index("fullname")?;
    let enname_idx = table.field_index("enname")?;
    let cnspell_idx = table.field_index("cnspell")?;
    let market_idx = table.field_index("market")?;
    let exchange_idx = table.field_index("exchange")?;
    let curr_type_idx = table.field_index("curr_type")?;
    let list_status_idx = table.field_index("list_status")?;
    let list_date_idx = table.field_index("list_date")?;
    let delist_date_idx = table.field_index("delist_date")?;
    let is_hs_idx = table.field_index("is_hs")?;
    let act_name_idx = table.field_index("act_name")?;
    let act_ent_type_idx = table.field_index("act_ent_type")?;

    let mut rows = Vec::with_capacity(table.items.len());

    for item in &table.items {
        rows.push(StockBasicRow {
            ts_code: TushareTable::value_as_string(&item[ts_code_idx], "ts_code")?,
            symbol: TushareTable::value_as_string(&item[symbol_idx], "symbol")?,
            name: TushareTable::value_as_string(&item[name_idx], "name")?,
            area: TushareTable::value_as_string(&item[area_idx], "area")?,
            industry: TushareTable::value_as_string(&item[industry_idx], "industry")?,
            fullname: TushareTable::value_as_string(&item[fullname_idx], "fullname")?,
            enname: TushareTable::value_as_string(&item[enname_idx], "enname")?,
            cnspell: TushareTable::value_as_string(&item[cnspell_idx], "cnspell")?,
            market: TushareTable::value_as_string(&item[market_idx], "market")?,
            exchange: TushareTable::value_as_string(&item[exchange_idx], "exchange")?,
            curr_type: TushareTable::value_as_string(&item[curr_type_idx], "curr_type")?,
            list_status: TushareTable::value_as_string(&item[list_status_idx], "list_status")?,
            list_date: TushareTable::value_as_string(&item[list_date_idx], "list_date")?,
            delist_date: TushareTable::value_as_string(&item[delist_date_idx], "delist_date")?,
            is_hs: TushareTable::value_as_string(&item[is_hs_idx], "is_hs")?,
            act_name: TushareTable::value_as_string(&item[act_name_idx], "act_name")?,
            act_ent_type: TushareTable::value_as_string(&item[act_ent_type_idx], "act_ent_type")?,
        });
    }

    Ok(rows)
}

pub fn build_daily_basic_snapshot_map(
    rows: Vec<DailyBasicSnapshotRow>,
) -> Result<HashMap<String, DailyBasicSnapshotRow>, String> {
    let mut map = HashMap::with_capacity(rows.len());

    for row in rows {
        if map.contains_key(&row.ts_code) {
            return Err(format!(
                "daily_basic snapshot 出现重复 ts_code: {}",
                row.ts_code
            ));
        }
        map.insert(row.ts_code.clone(), row);
    }

    Ok(map)
}

pub fn merge_stock_list_rows(
    basic_rows: Vec<StockBasicRow>,
    snap_map: &HashMap<String, DailyBasicSnapshotRow>,
    fallback_trade_date: &str,
) -> Result<Vec<StockListRow>, String> {
    let mut out = Vec::with_capacity(basic_rows.len());

    for row in basic_rows {
        let snap = snap_map.get(&row.ts_code);
        let trade_date = snap
            .map(|item| item.trade_date.clone())
            .unwrap_or_else(|| fallback_trade_date.to_string());
        let total_share = snap.and_then(|item| item.total_share);
        let float_share = snap.and_then(|item| item.float_share);
        let total_mv = snap.and_then(|item| item.total_mv);
        let circ_mv = snap.and_then(|item| item.circ_mv);

        out.push(StockListRow {
            ts_code: row.ts_code,
            symbol: row.symbol,
            name: row.name,
            area: row.area,
            industry: row.industry,
            list_date: row.list_date,
            trade_date,
            total_share,
            float_share,
            total_mv,
            circ_mv,
            fullname: row.fullname,
            enname: row.enname,
            cnspell: row.cnspell,
            market: row.market,
            exchange: row.exchange,
            curr_type: row.curr_type,
            list_status: row.list_status,
            delist_date: row.delist_date,
            is_hs: row.is_hs,
            act_name: row.act_name,
            act_ent_type: row.act_ent_type,
        });
    }

    Ok(out)
}

pub fn parse_trade_cal_rows(table: &TushareTable) -> Result<Vec<TradeCalRow>, String> {
    let exchange_idx = table.field_index("exchange")?;
    let cal_date_idx = table.field_index("cal_date")?;
    let is_open_idx = table.field_index("is_open")?;
    let pretrade_date_idx = table.field_index("pretrade_date")?;

    let mut rows = Vec::with_capacity(table.items.len());

    for item in &table.items {
        rows.push(TradeCalRow {
            exchange: TushareTable::value_as_string(&item[exchange_idx], "exchange")?,
            cal_date: TushareTable::value_as_string(&item[cal_date_idx], "cal_date")?,
            is_open: TushareTable::value_as_string(&item[is_open_idx], "is_open")?,
            pretrade_date: TushareTable::value_as_string(
                &item[pretrade_date_idx],
                "pretrade_date",
            )?,
        });
    }

    Ok(rows)
}

#[derive(Debug, Clone)]
pub struct ProBarRow {
    pub ts_code: String,
    pub trade_date: String,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub pre_close: f64,
    pub change: f64,
    pub pct_chg: f64,
    pub vol: f64,
    pub amount: f64,
    pub turnover_rate: Option<f64>,
    pub volume_ratio: Option<f64>,
}

pub fn build_single_basic_map(
    rows: Vec<DailyBasicRow>,
) -> Result<HashMap<String, DailyBasicRow>, String> {
    let mut map = HashMap::with_capacity(rows.len());

    for row in rows {
        let trade_date = row.trade_date.clone();

        if map.contains_key(&trade_date) {
            return Err(format!("daily_basic 出现重复 trade_date: {trade_date}"));
        }

        map.insert(trade_date, row);
    }

    Ok(map)
}

pub fn build_single_basic_with_basiccol(
    bar_rows: Vec<BarRow>,
    basic_map: &HashMap<String, DailyBasicRow>,
) -> Result<Vec<ProBarRow>, String> {
    let mut out = Vec::with_capacity(bar_rows.len());

    for bar in bar_rows {
        let (turnover_rate, volume_ratio) = match basic_map.get(&bar.trade_date) {
            Some(basic) => {
                if basic.ts_code != bar.ts_code {
                    return Err(format!(
                        "trade_date={} 的 daily_basic ts_code 不匹配: {} != {}",
                        bar.trade_date, basic.ts_code, bar.ts_code
                    ));
                }

                (basic.turnover_rate, basic.volume_ratio)
            }
            None => (None, None),
        };

        out.push(ProBarRow {
            ts_code: bar.ts_code,
            trade_date: bar.trade_date,
            open: bar.open,
            high: bar.high,
            low: bar.low,
            close: bar.close,
            pre_close: bar.pre_close,
            change: bar.change,
            pct_chg: bar.pct_chg,
            vol: bar.vol,
            amount: bar.amount,
            turnover_rate,
            volume_ratio,
        });
    }

    Ok(out)
}

pub fn build_adj_factor_map(
    rows: Vec<AdjFactorRow>,
) -> Result<HashMap<String, AdjFactorRow>, String> {
    let mut map = HashMap::with_capacity(rows.len());

    for row in rows {
        let trade_date = row.trade_date.clone();

        if map.contains_key(&trade_date) {
            return Err(format!("adj_factor 出现重复 trade_date: {trade_date}"));
        }

        map.insert(trade_date, row);
    }

    Ok(map)
}

fn build_aligned_adj_factors(
    rows: &[ProBarRow],
    adj_map: &HashMap<String, AdjFactorRow>,
) -> Result<Vec<f64>, String> {
    let mut factors = Vec::with_capacity(rows.len());

    for row in rows {
        let factor = adj_map.get(&row.trade_date).map(|v| {
            if v.ts_code != row.ts_code {
                return Err(format!(
                    "adj_factor ts_code 不匹配: {} != {}",
                    v.ts_code, row.ts_code
                ));
            }
            Ok(v.adj_factor)
        });

        match factor {
            Some(v) => factors.push(Some(v?)),
            None => factors.push(None),
        }
    }

    for i in (0..factors.len()).rev() {
        if factors[i].is_none() {
            if i + 1 < factors.len() {
                factors[i] = factors[i + 1];
            }
        }
    }

    let mut out = Vec::with_capacity(factors.len());
    for (idx, factor) in factors.into_iter().enumerate() {
        let Some(factor) = factor else {
            return Err(format!(
                "无法为 trade_date={} 补齐 adj_factor",
                rows[idx].trade_date
            ));
        };
        out.push(factor);
    }

    Ok(out)
}

pub fn build_pro_bar_rows(bar_rows: Vec<BarRow>) -> Vec<ProBarRow> {
    let mut out = Vec::with_capacity(bar_rows.len());

    for bar in bar_rows {
        out.push(ProBarRow {
            ts_code: bar.ts_code,
            trade_date: bar.trade_date,
            open: bar.open,
            high: bar.high,
            low: bar.low,
            close: bar.close,
            pre_close: bar.pre_close,
            change: bar.change,
            pct_chg: bar.pct_chg,
            vol: bar.vol,
            amount: bar.amount,
            turnover_rate: None,
            volume_ratio: None,
        });
    }

    out
}

pub fn resolve_qfq_base_factor(
    rows: &[ProBarRow],
    adj_map: &HashMap<String, AdjFactorRow>,
) -> Result<f64, String> {
    let last_row = rows
        .last()
        .ok_or_else(|| "没有可用于前复权的行情数据".to_string())?;

    let factor_row = adj_map
        .get(&last_row.trade_date)
        .ok_or_else(|| format!("缺少前复权基准日期的 adj_factor: {}", last_row.trade_date))?;

    if factor_row.ts_code != last_row.ts_code {
        return Err(format!(
            "前复权基准日 ts_code 不匹配: {} != {}",
            factor_row.ts_code, last_row.ts_code
        ));
    }

    Ok(factor_row.adj_factor)
}

pub fn apply_adj_to_rows(
    rows: &mut [ProBarRow],
    adj_type: &AdjType,
    adj_map: &HashMap<String, AdjFactorRow>,
) -> Result<(), String> {
    let factors = build_aligned_adj_factors(rows, adj_map)?;

    let qfq_base = match adj_type {
        AdjType::Qfq => Some(
            *factors
                .last()
                .ok_or_else(|| "没有可用于前复权的因子".to_string())?,
        ),
        AdjType::Hfq | AdjType::Raw | AdjType::Ind => None,
    };

    for (row, factor) in rows.iter_mut().zip(factors.into_iter()) {
        let scale = match adj_type {
            AdjType::Raw => 1.0,
            AdjType::Hfq => factor,
            AdjType::Qfq => factor / qfq_base.expect("qfq_base should exist"),
            AdjType::Ind => 1.0,
        };

        row.open *= scale;
        row.high *= scale;
        row.low *= scale;
        row.close *= scale;
        row.pre_close *= scale;
    }
    Ok(())
}

fn pro_bar_format(value: f64, scale: usize) -> f64 {
    if !value.is_finite() {
        return value;
    }

    format!("{value:.precision$}", precision = scale)
        .parse::<f64>()
        .unwrap_or(value)
}

fn normalize_stock_price_fields_like_pro_bar(rows: &mut [ProBarRow]) {
    for row in rows {
        row.open = pro_bar_format(row.open, 2);
        row.high = pro_bar_format(row.high, 2);
        row.low = pro_bar_format(row.low, 2);
        row.close = pro_bar_format(row.close, 2);
        row.pre_close = pro_bar_format(row.pre_close, 2);
    }
}

fn recalc_change_fields(rows: &mut [ProBarRow]) {
    for row in rows.iter_mut() {
        row.change = row.close - row.pre_close;
        row.pct_chg = if row.pre_close.abs() < f64::EPSILON {
            0.0
        } else {
            row.change / row.pre_close * 100.0
        };
    }
}

fn normalize_stock_rows_like_pro_bar(rows: &mut [ProBarRow]) {
    normalize_stock_price_fields_like_pro_bar(rows);

    for row in rows.iter_mut() {
        let change = row.close - row.pre_close;
        row.change = pro_bar_format(change, 2);
        row.pct_chg = if row.pre_close.abs() < f64::EPSILON {
            0.0
        } else {
            pro_bar_format(change / row.pre_close * 100.0, 2)
        };
    }
}

pub fn build_daily_basic_map(
    rows: Vec<DailyBasicRow>,
) -> Result<HashMap<String, DailyBasicRow>, String> {
    let mut map = HashMap::with_capacity(rows.len());

    for row in rows {
        if map.contains_key(&row.ts_code) {
            return Err(format!("daily_basic 出现重复 ts_code: {}", row.ts_code));
        }
        map.insert(row.ts_code.clone(), row);
    }

    Ok(map)
}

pub fn build_daily_basic_with_basiccol(
    bar_rows: Vec<BarRow>,
    basic_map: &HashMap<String, DailyBasicRow>,
) -> Result<Vec<ProBarRow>, String> {
    let mut out = Vec::with_capacity(bar_rows.len());

    for bar in bar_rows {
        let (turnover_rate, volume_ratio) = match basic_map.get(&bar.ts_code) {
            Some(basic) => {
                if basic.trade_date != bar.trade_date {
                    return Err(format!(
                        "ts_code={} 的 trade_date 不匹配: {} != {}",
                        bar.ts_code, basic.trade_date, bar.trade_date
                    ));
                }

                (basic.turnover_rate, basic.volume_ratio)
            }
            None => (None, None),
        };

        out.push(ProBarRow {
            ts_code: bar.ts_code,
            trade_date: bar.trade_date,
            open: bar.open,
            high: bar.high,
            low: bar.low,
            close: bar.close,
            pre_close: bar.pre_close,
            change: bar.change,
            pct_chg: bar.pct_chg,
            vol: bar.vol,
            amount: bar.amount,
            turnover_rate,
            volume_ratio,
        });
    }

    Ok(out)
}

pub fn build_market_basic_map(
    rows: Vec<DailyBasicRow>,
) -> Result<HashMap<String, DailyBasicRow>, String> {
    let mut map = HashMap::with_capacity(rows.len());

    for row in rows {
        if map.contains_key(&row.ts_code) {
            return Err(format!("daily_basic 出现重复 ts_code: {}", row.ts_code));
        }

        map.insert(row.ts_code.clone(), row);
    }

    Ok(map)
}

pub fn build_market_basic_with_basiccol(
    bar_rows: Vec<BarRow>,
    basic_map: &HashMap<String, DailyBasicRow>,
) -> Result<Vec<ProBarRow>, String> {
    let mut out = Vec::with_capacity(bar_rows.len());

    for bar in bar_rows {
        let (turnover_rate, volume_ratio) = match basic_map.get(&bar.ts_code) {
            Some(basic) => {
                if basic.trade_date != bar.trade_date {
                    return Err(format!(
                        "ts_code={} 的 daily_basic trade_date 不匹配: {} != {}",
                        bar.ts_code, basic.trade_date, bar.trade_date
                    ));
                }

                (basic.turnover_rate, basic.volume_ratio)
            }
            None => (None, None),
        };

        out.push(ProBarRow {
            ts_code: bar.ts_code,
            trade_date: bar.trade_date,
            open: bar.open,
            high: bar.high,
            low: bar.low,
            close: bar.close,
            pre_close: bar.pre_close,
            change: bar.change,
            pct_chg: bar.pct_chg,
            vol: bar.vol,
            amount: bar.amount,
            turnover_rate,
            volume_ratio,
        });
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn probar_row(ts_code: &str, trade_date: &str, close: f64, pre_close: f64) -> ProBarRow {
        ProBarRow {
            ts_code: ts_code.to_string(),
            trade_date: trade_date.to_string(),
            open: close,
            high: close,
            low: close,
            close,
            pre_close,
            change: close - pre_close,
            pct_chg: 0.0,
            vol: 0.0,
            amount: 0.0,
            turnover_rate: None,
            volume_ratio: None,
        }
    }

    #[test]
    fn qfq_rows_match_tushare_pro_bar_price_rounding_before_change() {
        let mut rows = vec![
            probar_row("000739.SZ", "20180104", 6.56, 6.50),
            probar_row("000739.SZ", "20260429", 18.47, 18.58),
        ];
        let adj_map = build_adj_factor_map(vec![
            AdjFactorRow {
                ts_code: "000739.SZ".to_string(),
                trade_date: "20180104".to_string(),
                adj_factor: 11.941,
            },
            AdjFactorRow {
                ts_code: "000739.SZ".to_string(),
                trade_date: "20260429".to_string(),
                adj_factor: 13.7498,
            },
        ])
        .expect("adj factor map");

        apply_adj_to_rows(&mut rows, &AdjType::Qfq, &adj_map).expect("apply qfq");
        normalize_stock_rows_like_pro_bar(&mut rows);

        assert_eq!(rows[0].close, 5.70);
        assert_eq!(rows[0].pre_close, 5.64);
        assert_eq!(rows[0].change, 0.06);
        assert_eq!(rows[0].pct_chg, 1.06);
    }

    #[test]
    fn pro_bar_format_matches_python_percent_format_boundaries() {
        assert_eq!(pro_bar_format(2.675, 2), 2.67);
        assert_eq!(pro_bar_format(1.045, 2), 1.04);
        assert_eq!(pro_bar_format(1.055, 2), 1.05);
    }

    #[test]
    fn pct_chg_uses_unformatted_change_between_formatted_prices() {
        let mut rows = vec![probar_row("000739.SZ", "20040116", 0.99, 0.96)];

        normalize_stock_rows_like_pro_bar(&mut rows);

        assert_eq!(rows[0].change, 0.03);
        assert_eq!(rows[0].pct_chg, 3.13);
    }
}
