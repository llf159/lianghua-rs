// use lianghua_rs::expr;
use lianghua_rs::expr::eval::{Runtime, Value};
// use lianghua_rs::expr::lexer::Lexer;
// use lianghua_rs::expr::parser::{Expr, Parser, lex_all};
// use lianghua_rs::strategy::loader::IndConfig;
// use std::fs;
use lianghua_rs::scoring::data::{DataRow, ScoreDetails, ScoreSummary};
use lianghua_rs::scoring::scoring_rules_details;

fn main() {
    // 读取toml测试
    // let ind_toml = fs::read_to_string("ind.toml").expect("msg");
    // let cfg: IndConfig = toml::from_str(&ind_toml).expect("msg");
    // println!("{:#?}{:#?}", ind_toml, cfg);
    // for x in &cfg.ind {
    //     println!("{} {} {}", x.output_name, x.expr, x.prec);
    // }
    // let expr_str = cfg.ind[0].expr.clone();
    // let tok2 = lex_all(&expr_str);
    // let mut expr_test2 = Parser::new(tok2);
    // println!("{:?}", expr_str);

    // 字段解析测试
    // let mut ls = Lexer::new("    a");
    // println!("{:?}", ls);
    // let tok = ls.next_token();
    // println!("{tok:?}");
    // println!("{:?}", ls);

    // 解析测试
    // let tok_test = String::from("N := 20;MA(C, N) < C AND C > ABS(O)");
    // let tok = lex_all(&tok_test);
    // let mut expr_test = Parser::new(tok);

    // let expr3 = expr_test.parse_primary();
    // let expr4 = expr_test.parse_main();
    // println!("{:#?}", expr4);

    // 错误处理测试
    // match expr_test.parse_main() {
    //     Ok(x) => println!("{:#?}", x),
    //     Err(e) => println!("err at {}: {}", e.idx, e.msg),
    // }
    // let stmt = match expr_test.parse_main() {
    //     Ok(x) => x,
    //     Err(e) => {
    //         println!("err at {}: {}", e.idx, e.msg);
    //         return;
    //     }
    // };

    // 示例表达式测试
    // let tok2 = lex_all(&expr_str);
    // // println!("{:?}", tok2);
    // let mut expr1 = Parser::new(tok2);
    // let expr2 = expr1.parse_primary();
    // println!("{:?}", expr2);

    // 参数导入测试
    // let mut rt = Runtime::default();
    // rt.vars.insert("C".to_string(), Value::Num(12.3));
    // rt.vars.insert("O".to_string(), Value::Num(11.8));
    // match rt.eval_program(&stmt) {
    //     Ok(v) => println!("eval result = {:?}", v),
    //     Err(e) => println!("{}", e.msg),
    // }

    // 打分测试
    // use lianghua_rs::expr::eval::{Runtime, Value};
    // use lianghua_rs::scoring::scoring_rules_details;

    // let mut rt = Runtime::default();

    // rt.vars.insert(
    //     "C".to_string(),
    //     Value::NumSeries(vec![
    //         Some(10.0),
    //         Some(10.5),
    //         Some(10.2),
    //         Some(10.8),
    //         Some(11.0),
    //     ]),
    // );
    // rt.vars.insert(
    //     "O".to_string(),
    //     Value::NumSeries(vec![
    //         Some(9.8),
    //         Some(10.6),
    //         Some(10.1),
    //         Some(10.7),
    //         Some(10.9),
    //     ]),
    // );

    // match scoring_rules_details(&mut rt) {
    //     Ok((total, details)) => {
    //         println!("total = {:?}", total);
    //         for d in details {
    //             println!("{} => {:?}", d.name, d.series);
    //         }
    //     }
    //     Err(e) => {
    //         println!("scoring err: {}", e);
    //     }
    // }

    // 计算流程测试
    let ts_code = "600968.SH".to_string();
    let data = DataRow::load_data(
        "./stock_data/stock_data.db",
        &ts_code,
        "qfq",
        "20250825",
        "20250901",
    );
    // let mut rt:HashMap<&str, Vec<Option<f64>>> = HashMap::new();
    let mut rt = Runtime::default();
    let mut trade_date: Vec<String> = Vec::new();
    match data {
        // Ok(v) => println!("{:?}", v),
        Ok(v) => {
            trade_date = v.trade_dates;
            for (name, col) in &v.cols {
                let n_series = Value::NumSeries(col.clone());
                rt.vars.insert(name.clone(), n_series);

                // rt.insert(name, col.to_vec());
            }
            // println!("{:?}", &rt);
        }
        Err(e) => println!("有错:{e}"),
    }
    let mut scoring_result = ScoreDetails::default();
    if let Ok((v, t)) = scoring_rules_details(&mut rt) {
        // println!("{:?}{:?}", v, t);
        let summary = ScoreSummary::build(&ts_code, &trade_date, &v);
        let details = ScoreDetails::build(&ts_code, &trade_date, &t);
        println!("{:#?}", summary);
        println!("{:#?}", details);
        // let _ = ScoreSummary::write_csv("./111.csv", &summary);
    }
}
