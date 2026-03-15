pub mod ind_calc;

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

pub struct DownloadConfig {
    pub start_date: String,
    pub end_date: String,
    pub adj_type: AdjType,
    pub token: String,
    pub source_dir: String,
    pub stock_list: Vec<String>,
}

enum AdjType {
    Qfq,
    Hfq,
    Raw,
}

enum DownloadStrategy {
    First,
    Pending,
    Skip,
}

pub struct TushareClient {
    pub base_url: String,
    pub token: String,
    pub http: reqwest::blocking::Client,
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
struct TushareTable {
    fields: Vec<String>,
    items: Vec<Vec<serde_json::Value>>,
}

#[derive(Deserialize)]
struct PostTable<P>{
    api_name: String,
    params: P,
    fields: HashMap<String, String>,
}


impl TushareClient {
    pub fn new(token: String) -> Result<Self, String> {
        let http = reqwest::blocking::Client::builder()
            .build()
            .map_err(|e| format!("创建HTTP客户端失败: {e}"))?;
        Ok(Self {
            base_url: "http://api.tushare.pro".to_string(),
            token,
            http,
        })
    }

    pub fn fetch_daily_basic_raw(&self, trade_date: &str) -> Result<String, String> {
        // 返回行个数和数据库还不配合
        let json_body = TushareRequest {
            api_name: "daily_basic",
            token: &self.token,
            params: trade_date,
            fields: "",
        };

        self.http
            .post(&self.base_url)
            .json(&json_body)
            .send()
            .map_err(|e| format!("请求 daily_basic 失败: {e}"))?
            .error_for_status()
            .map_err(|e| format!("daily_basic 返回 HTTP 错误: {e}"))?
            .text()
            .map_err(|e| format!("读取 daily_basic 响应文本失败: {e}"))
    }

    // pub fn fetch_one_stock_all(&self, ts_code: &str, config: &DownloadConfig) -> Result<String, String> {
    //     let json_body = TushareRequest {
    //         api_name: "daily",
    //         token: &self.token,
    //         params: [ts_code, &config.start_date, &config.end_date],
    //         fields: "",
    //     };

    //     self.http
    //         .post(&self.base_url)
    //         .json(&json_body)
    //         .send()
    //         .map_err(|e| format!("请求 daily_basic 失败: {e}"))?
    //         .error_for_status()
    //         .map_err(|e| format!("daily_basic 返回 HTTP 错误: {e}"))?
    //         .text()
    //         .map_err(|e| format!("读取 daily_basic 响应文本失败: {e}"))

    // }
}
