// use lianghua_rs::{
//     config::{AppConfig, DataConfig, DownloadConfig, OutputConfig},
//     download::runner::download,
// };

// fn main() -> Result<(), String> {
//     let config = AppConfig {
//         data: DataConfig {
//             source_db: "./source/stock_data.db".to_string(),
//             adj_type: "qfq".to_string(),
//         },
//         output: OutputConfig {
//             dir: "./source".to_string(),
//             result_db: "scoring_result.db".to_string(),
//         },
//         download: DownloadConfig {
//             token: "".to_string(),
//             start_date: "20240101".to_string(),
//             end_date: "today".to_string(),
//             threads: 16,
//             retry_times: 3,
//             limit_calls_per_min: 190,
//             refresh_stock_list: true,
//             include_turnover: true,
//         },
//     };

//     download(&config, None)?;

//     Ok(())
// }

// use std::{
//     fs::{OpenOptions, create_dir_all, read_to_string, remove_file, write},
//     path::Path,
// };

// use lianghua_rs::{
//     crawler::concept::{
//         ThsConceptBatchResult, ThsConceptFetchConfig, ThsConceptFetchItem, ThsConceptRow,
//         fetch_ths_concept_rows,
//     },
//     data::load_stock_list,
// };

// fn load_test_items(source_dir: &str, limit: usize) -> Result<Vec<ThsConceptFetchItem>, String> {
//     let rows = load_stock_list(source_dir)?;
//     let mut items = Vec::new();

//     for cols in rows {
//         let Some(ts_code) = cols.first().map(|value| value.trim()) else {
//             continue;
//         };
//         let Some(name) = cols.get(2).map(|value| value.trim()) else {
//             continue;
//         };
//         if ts_code.is_empty() || name.is_empty() {
//             continue;
//         }

//         items.push(ThsConceptFetchItem {
//             ts_code: ts_code.to_string(),
//             name: name.to_string(),
//         });

//         if items.len() >= limit {
//             break;
//         }
//     }

//     Ok(items)
// }

// fn append_test_concepts_csv(
//     path: &Path,
//     rows: &[ThsConceptRow],
//     append: bool,
// ) -> Result<(), String> {
//     if let Some(parent) = path.parent() {
//         if !parent.as_os_str().is_empty() {
//             create_dir_all(parent).map_err(|e| format!("创建测试输出目录失败: {e}"))?;
//         }
//     }

//     let file = OpenOptions::new()
//         .create(true)
//         .write(true)
//         .truncate(!append)
//         .append(append)
//         .open(path)
//         .map_err(|e| format!("打开测试概念文件失败: path={}, err={e}", path.display()))?;
//     let mut writer = csv::WriterBuilder::new()
//         .has_headers(false)
//         .from_writer(file);

//     if !append {
//         writer
//             .write_record(["ts_code", "name", "concept"])
//             .map_err(|e| format!("写入测试概念表头失败: {e}"))?;
//     }

//     for row in rows {
//         writer
//             .write_record([
//                 row.ts_code.as_str(),
//                 row.name.as_str(),
//                 row.concept.as_str(),
//             ])
//             .map_err(|e| format!("写入测试概念数据失败: ts_code={}, err={e}", row.ts_code))?;
//     }

//     writer
//         .flush()
//         .map_err(|e| format!("刷新测试概念文件失败: {e}"))?;

//     Ok(())
// }

// fn load_checkpoint(path: &Path) -> Result<usize, String> {
//     if !path.exists() {
//         return Ok(0);
//     }

//     let raw = read_to_string(path)
//         .map_err(|e| format!("读取 checkpoint 失败: path={}, err={e}", path.display()))?;
//     let trimmed = raw.trim();
//     if trimmed.is_empty() {
//         return Ok(0);
//     }

//     trimmed
//         .parse::<usize>()
//         .map_err(|e| format!("解析 checkpoint 失败: path={}, err={e}", path.display()))
// }

// fn save_checkpoint(path: &Path, next_index: usize) -> Result<(), String> {
//     if let Some(parent) = path.parent() {
//         if !parent.as_os_str().is_empty() {
//             create_dir_all(parent).map_err(|e| format!("创建 checkpoint 目录失败: {e}"))?;
//         }
//     }

//     write(path, next_index.to_string())
//         .map_err(|e| format!("写入 checkpoint 失败: path={}, err={e}", path.display()))
// }

// fn main() -> Result<(), String> {
//     let source_dir = "/home/lmingyuanl/.local/share/com.lmingyuanl.lianghua/source";
//     let sample_limit = usize::MAX;
//     let batch_size = 50;
//     let fetch_config = ThsConceptFetchConfig {
//         retry_times: 3,
//         retry_sleep_secs: 10,
//     };
//     let output_path = Path::new(source_dir).join("test_stock_concepts.csv");
//     let checkpoint_path = Path::new(source_dir).join("test_stock_concepts.checkpoint");

//     let items = load_test_items(source_dir, sample_limit)?;
//     let start_index = load_checkpoint(&checkpoint_path)?;
//     if start_index > items.len() {
//         return Err(format!(
//             "checkpoint 超出股票列表范围: checkpoint={}, total={}",
//             start_index,
//             items.len()
//         ));
//     }
//     if start_index > 0 && !output_path.exists() {
//         return Err(format!(
//             "checkpoint 存在但输出文件缺失: checkpoint={}, path={}",
//             start_index,
//             output_path.display()
//         ));
//     }

//     let http = reqwest::blocking::Client::builder()
//         .build()
//         .map_err(|e| format!("创建 HTTP 客户端失败: {e}"))?;
//     let mut total_success = start_index;
//     let mut next_index = start_index;
//     let mut first_write = !output_path.exists() || start_index == 0;

//     while next_index < items.len() {
//         let end_index = (next_index + batch_size).min(items.len());
//         let batch_items = &items[next_index..end_index];
//         println!(
//             "batch_start: {} batch_end: {} batch_count: {}",
//             next_index,
//             end_index,
//             batch_items.len()
//         );

//         let batch_result: ThsConceptBatchResult =
//             fetch_ths_concept_rows(&http, batch_items, fetch_config)?;
//         append_test_concepts_csv(&output_path, &batch_result.rows, !first_write)?;
//         first_write = false;
//         next_index = end_index;
//         total_success += batch_result.processed_count;
//         save_checkpoint(&checkpoint_path, next_index)?;
//     }

//     if checkpoint_path.exists() {
//         remove_file(&checkpoint_path).map_err(|e| {
//             format!(
//                 "删除 checkpoint 失败: path={}, err={e}",
//                 checkpoint_path.display()
//             )
//         })?;
//     }

//     println!("source_dir: {source_dir}");
//     println!("sample_limit: {sample_limit}");
//     println!("batch_size: {batch_size}");
//     println!(
//         "retry: times={} sleep_secs={}",
//         fetch_config.retry_times, fetch_config.retry_sleep_secs
//     );
//     println!("input_count: {}", items.len());
//     println!("resume_from: {start_index}");
//     println!("success_count: {total_success}");
//     println!("output_path: {}", output_path.display());

//     Ok(())
// }

fn main() {}
