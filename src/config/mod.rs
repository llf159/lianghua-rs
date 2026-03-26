use serde::Deserialize;

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct AppConfig {
    pub data: DataConfig,
    pub output: OutputConfig,
    pub download: DownloadConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct DataConfig {
    pub source_db: String,
    pub adj_type: String,
}

impl Default for DataConfig {
    fn default() -> Self {
        Self {
            source_db: "./source/stock_data.db".to_string(),
            adj_type: "qfq".to_string(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct OutputConfig {
    pub dir: String,
    pub result_db: String,
}

impl Default for OutputConfig {
    fn default() -> Self {
        Self {
            dir: "./source".to_string(),
            result_db: "scoring_result.db".to_string(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct DownloadConfig {
    pub token: String,
    pub start_date: String,
    pub end_date: String,
    pub threads: usize,
    pub retry_times: usize,
    pub limit_calls_per_min: usize,
    pub refresh_stock_list: bool,
    pub include_turnover: bool,
}

impl Default for DownloadConfig {
    fn default() -> Self {
        Self {
            token: String::new(),
            start_date: "20240101".to_string(),
            end_date: "today".to_string(),
            threads: 4,
            retry_times: 3,
            limit_calls_per_min: 190,
            refresh_stock_list: true,
            include_turnover: true,
        }
    }
}
