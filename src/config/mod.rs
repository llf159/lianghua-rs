use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct AppConfig {
    pub data: DataConfig,
    pub output: OutputConfig,
}

#[derive(Debug, Deserialize)]
pub struct DataConfig {
    pub source_db: String,
    pub adj_type: String,
}

#[derive(Debug, Deserialize)]
pub struct OutputConfig {
    pub dir: String,
    pub result_db: String,
}
