// use lianghua_rs::scoring::{data::RowData, runner::{scoring_all, scoring_single_period}};

// fn main() -> Result<(), String> {
// let row_data = RowData::load_data("./stock_data/stock_data.db", "600968.SH", )?;
// let result = scoring_all(
//     "./stock_data/stock_data.db",
//     "qfq",
//     "20250901",
//     "20250902",
// )?;
// println!("{:#?}", result);
// Ok(())
// }

use std::time::Instant;

use lianghua_rs::scoring::runner::scoring_all_to_db;

fn main() -> Result<(), String> {
    let source_db_path = "./stock_data/stock_data.db";
    let result_db_path = "./output/scoring_result.db";
    let ts_code = "002432.SZ";
    let adj_type = "qfq";
    let start_date = "20260227";
    let end_date = "20260227";

    let total_start = Instant::now();

    let scoring_start = Instant::now();
    // let (a, b) = scoring_single_period(source_db_path, ts_code, adj_type, start_date, end_date)?;
    scoring_all_to_db(source_db_path, adj_type, start_date, end_date)?;
    println!("scoring_all_to_db took: {:.3?}", scoring_start.elapsed());

    // let rank_start = Instant::now();
    // build_rank_tiebreak(result_db_path, source_db_path, adj_type, TieBreakWay::KdjJ)?;
    // println!("backfill_rank took: {:.3?}", rank_start.elapsed());

    println!("total took: {:.3?}", total_start.elapsed());
    // let n = estimate_warmup_rows()?;
    // let _ = list_trade_date("./stock_data").map_err(|e| format!("{e}"));
    // println!("{:#?}\n", n);
    // println!("{:#?}\n{:#?}", n, m);
    // println!("{:#?}", a);
    Ok(())
}
