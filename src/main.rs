use lianghua_rs::{
    config::{AppConfig, DataConfig, DownloadConfig, OutputConfig},
    download::runner::download,
};

fn main() -> Result<(), String> {
    let config = AppConfig {
        data: DataConfig {
            source_db: "./source/stock_data.db".to_string(),
            adj_type: "qfq".to_string(),
        },
        output: OutputConfig {
            dir: "./source".to_string(),
            result_db: "scoring_result.db".to_string(),
        },
        download: DownloadConfig {
            token: "".to_string(),
            start_date: "20240101".to_string(),
            end_date: "today".to_string(),
            threads: 16,
            retry_times: 3,
            limit_calls_per_min: 190,
            refresh_stock_list: true,
            include_turnover: true,
        },
    };

    download(&config, None)?;

    Ok(())
}
