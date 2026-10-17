use lianghua_core::{
    expr::eval::{Runtime, Value},
    tracking::{TrackDefinition, TrackProgram, TrackState},
};
use lianghua_scoring::scoring::{evaluate_cached_rule_scores, rule_cache::cache_rule_build};
use std::{
    collections::BTreeMap,
    env, fs,
    io::{BufRead, BufReader, BufWriter, Write},
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    if args.first().is_some_and(|arg| arg == "--validate-config") {
        let source = args
            .get(1)
            .ok_or("--validate-config <source> [score.toml]")?;
        let config = lianghua_scoring::data::ScoreConfig::load_with_strategy_path(
            source,
            args.get(2).map(String::as_str),
        )?;
        println!(
            "validated {} rules, {} tracks",
            config.rule.len(),
            config.track.len()
        );
        for track in config.track {
            println!(
                "{} enabled={} source={:?} points={}",
                track.name, track.enabled, track.source, track.points
            );
        }
        return Ok(());
    }
    if args.len() != 4 {
        return Err(
            "tracking_research <source> <bars.csv> <candidates.json> <output-prefix>".into(),
        );
    }
    let definitions: Vec<TrackDefinition> = serde_json::from_str(&fs::read_to_string(&args[2])?)?;
    let programs = definitions
        .iter()
        .map(TrackProgram::compile)
        .collect::<Result<Vec<_>, _>>()?;
    let names = ["跨板块旧倍量点火守价", "高效回调组合", "单针洗盘"];
    let rules = cache_rule_build(&args[0], None)?;
    let selected = names
        .iter()
        .map(|name| rules.iter().find(|r| r.name == *name).unwrap())
        .collect::<Vec<_>>();
    let calendar = lianghua_scoring::data::load_trade_date_list(&args[0])?;
    let mut calendar = calendar;
    calendar.sort();
    calendar.dedup();
    let conn =
        lianghua_scoring::data::open_db_readonly(lianghua_scoring::data::source_db_path(&args[0]))?;
    let as_of: String = conn.query_row(
        "SELECT max(trade_date) FROM stock_data WHERE adj_type='qfq'",
        [],
        |row| row.get(0),
    )?;
    calendar.retain(|date| date <= &as_of);

    let input = fs::File::open(&args[1])?;
    let mut lines = BufReader::new(input).lines();
    let header = lines
        .next()
        .ok_or("empty input")??
        .split(',')
        .map(str::to_string)
        .collect::<Vec<_>>();
    let mut output = BufWriter::new(fs::File::create(format!("{}.csv", args[3]))?);
    write!(output, "ts_code,trade_date,s0,s1,s2")?;
    for d in &definitions {
        write!(output, ",{}", d.id)?;
    }
    writeln!(output)?;
    let mut events = BufWriter::new(fs::File::create(format!("{}_events.csv", args[3]))?);
    writeln!(
        events,
        "ts_code,candidate,anchor_session,session,reason,score"
    )?;
    let mut pending = lines.next().transpose()?;
    let st_list = lianghua_scoring::data::extras::load_st_list(&args[0])?;
    let mut stocks = 0;
    let mut audits = BTreeMap::<String, usize>::new();
    while let Some(first) = pending.take() {
        let code = first.split(',').next().ok_or("missing code")?.to_string();
        let mut rows = vec![first];
        for line in lines.by_ref() {
            let line = line?;
            if line.split(',').next() != Some(code.as_str()) {
                pending = Some(line);
                break;
            }
            rows.push(line);
        }
        let mut dates = Vec::new();
        let mut cols = vec![Vec::new(); header.len() - 2];
        for row in rows {
            let parts = row.split(',').collect::<Vec<_>>();
            if parts.len() != header.len() {
                return Err("bad csv width".into());
            }
            dates.push(parts[1].to_string());
            for (col, value) in cols.iter_mut().zip(&parts[2..]) {
                col.push(if value.is_empty() {
                    None
                } else {
                    Some(value.parse::<f64>()?)
                });
            }
        }
        let mut rt = Runtime::default();
        for (key, values) in header[2..].iter().zip(cols) {
            rt.vars.insert(key.clone(), Value::NumSeries(values));
        }
        rt.vars.insert(
            "ZHANG".into(),
            Value::NumSeries(vec![
                Some(lianghua_scoring::data::extras::calc_zhang_pct(
                    &code,
                    st_list.contains(&code),
                ));
                dates.len()
            ]),
        );
        let mut scores = Vec::new();
        for rule in &selected {
            scores.push(evaluate_cached_rule_scores(rule, &mut rt)?.0);
        }
        let mut extras = vec![vec![0.0; dates.len()]; definitions.len()];
        for (k, (definition, program)) in definitions.iter().zip(&programs).enumerate() {
            let prepared = program.prepare(&rt)?;
            let mut state = TrackState::default();
            let mut index = 0;
            let source: usize = definition.source.as_ref().ok_or("source")?.parse()?;
            let begin = calendar.partition_point(|d| d < &dates[0]);
            let end = calendar.len();
            let mut last_stopped = None;
            for session in begin..end {
                while index < dates.len() && dates[index] < calendar[session] {
                    index += 1;
                }
                let qi = (dates.get(index) == Some(&calendar[session])).then_some(index);
                let score = prepared.advance(
                    &mut state,
                    definition,
                    session,
                    qi,
                    &rt,
                    qi.map(|i| scores[source][i]),
                )?;
                if let Some(i) = qi {
                    extras[k][i] = score;
                }
                if let Some(e) = &state.event {
                    if e.started == session {
                        *audits
                            .entry(format!("{}_started", definition.id))
                            .or_default() += 1;
                    }
                    if e.first_signal == Some(session) {
                        *audits
                            .entry(format!("{}_confirmed", definition.id))
                            .or_default() += 1;
                    }
                    if e.stopped.is_some() && e.stopped != last_stopped {
                        let reason = e.reason.as_deref().unwrap_or("unknown");
                        *audits
                            .entry(format!("{}_{reason}", definition.id))
                            .or_default() += 1;
                        writeln!(
                            events,
                            "{code},{},{},{session},{reason},{}",
                            definition.id, e.started, e.score
                        )?;
                    }
                    last_stopped = e.stopped;
                }
            }
            if stocks < 3 && (k % 12 == 0 || k % 12 == 9) {
                let cut = dates.len() / 2;
                let mut short = Runtime::default();
                for (key, value) in &rt.vars {
                    let values = (0..cut)
                        .map(|i| lianghua_core::tracking::value_at(value, i))
                        .collect();
                    short.vars.insert(key.clone(), Value::NumSeries(values));
                }
                let short_score = evaluate_cached_rule_scores(selected[source], &mut short)?.0;
                let probe = program.prepare(&short)?;
                let mut state = TrackState::default();
                let mut i = 0;
                for session in begin..calendar.partition_point(|d| d <= &dates[cut - 1]) {
                    while i < cut && dates[i] < calendar[session] {
                        i += 1;
                    }
                    let qi = (i < cut && dates[i] == calendar[session]).then_some(i);
                    let s = probe.advance(
                        &mut state,
                        definition,
                        session,
                        qi,
                        &short,
                        qi.map(|i| short_score[i]),
                    )?;
                    if let Some(i) = qi {
                        if s != extras[k][i] {
                            return Err(format!("prefix mismatch {code} {k} {i}").into());
                        }
                    }
                }
                *audits.entry("prefix_checks".into()).or_default() += 1;
            }
        }
        for (i, date) in dates.iter().enumerate() {
            write!(
                output,
                "{code},{date},{},{},{}",
                scores[0][i], scores[1][i], scores[2][i]
            )?;
            for extra in &extras {
                write!(output, ",{}", extra[i])?;
            }
            writeln!(output)?;
        }
        stocks += 1;
        if stocks % 500 == 0 {
            eprintln!("native replay {stocks} {code}");
        }
    }
    audits.insert("stocks".into(), stocks);
    fs::write(
        format!("{}_audit.json", args[3]),
        serde_json::to_string_pretty(&audits)?,
    )?;
    Ok(())
}
