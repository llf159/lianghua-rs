//! Shared market-data contracts used by storage, ingestion, and scoring crates.

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

#[derive(Debug, Clone, PartialEq)]
pub struct MoneyflowRow {
    pub ts_code: String,
    pub trade_date: String,
    pub b_sm_v: Option<f64>,
    pub s_sm_v: Option<f64>,
    pub b_md_v: Option<f64>,
    pub s_md_v: Option<f64>,
    pub b_lg_v: Option<f64>,
    pub s_lg_v: Option<f64>,
    pub b_elg_v: Option<f64>,
    pub s_elg_v: Option<f64>,
    pub net_mf_v: Option<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TopListRow {
    pub trade_date: String,
    pub ts_code: String,
    pub name: String,
    pub close: Option<f64>,
    pub pct_change: Option<f64>,
    pub turnover_rate: Option<f64>,
    pub amount: Option<f64>,
    pub l_sell: Option<f64>,
    pub l_buy: Option<f64>,
    pub l_amount: Option<f64>,
    pub net_amount: Option<f64>,
    pub net_rate: Option<f64>,
    pub amount_rate: Option<f64>,
    pub float_values: Option<f64>,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TopInstRow {
    pub trade_date: String,
    pub ts_code: String,
    pub exalter: String,
    pub buy: Option<f64>,
    pub buy_rate: Option<f64>,
    pub sell: Option<f64>,
    pub sell_rate: Option<f64>,
    pub net_buy: Option<f64>,
    pub side: String,
    pub reason: String,
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
    pub exchange: String,
    pub cal_date: String,
    pub is_open: String,
    pub pretrade_date: String,
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
    pub moneyflow: Option<MoneyflowRow>,
}

#[derive(Debug, Clone)]
pub struct DownloadProgress {
    pub phase: String,
    pub finished: usize,
    pub total: usize,
    pub current_label: Option<String>,
    pub message: String,
}

pub type DownloadProgressCallback<'a> = dyn Fn(DownloadProgress) + Send + Sync + 'a;
