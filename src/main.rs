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

use lianghua_rs::{
    scoring::{TieBreakWay, build_rank_tiebreak, runner::scoring_all_to_db, tools::load_st_list},
    utils::utils::load_ths_concepts_list,
};

fn main() -> Result<(), String> {
    let source_dir = "./source";
    let result_db_path = "./source/output/scoring_result.db";
    // let ts_code = "002432.SZ";
    let adj_type = "qfq";
    let start_date = "20260201";
    let end_date = "20260306";

    let total_start = Instant::now();

    let scoring_start = Instant::now();
    // let st_list = load_st_list(source_dir);
    // let (a, b) = scoring_single_period(source_dir, ts_code, adj_type, start_date, end_date)?;
    scoring_all_to_db(source_dir, adj_type, start_date, end_date)?;
    build_rank_tiebreak(
        result_db_path,
        "./source/stock_data.db",
        adj_type,
        TieBreakWay::KdjJ,
    )?;
    // println!("{:#?}", st_list);
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
