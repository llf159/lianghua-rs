use crate::data::load_stock_list;
use crate::utils::utils::board_category;
use lianghua_app_shared::{build_total_mv_map, filter_mv};
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
#[derive(Debug, Clone)]
pub(in crate::statistics) struct ValidationSampleRawRow {
    pub(in crate::statistics) ts_code: String,
    pub(in crate::statistics) trade_date: String,
    pub(in crate::statistics) trigger_count: usize,
    pub(in crate::statistics) rule_score: f64,
    pub(in crate::statistics) residual_return: f64,
}

#[derive(Debug, Clone)]
pub(in crate::statistics) struct ValidationSampleStockMeta {
    pub(in crate::statistics) name: Option<String>,
    pub(in crate::statistics) board: String,
    pub(in crate::statistics) volatility_group: String,
}

pub(in crate::statistics) fn load_validation_sample_stock_meta_map(
    source_path: &str,
) -> Result<HashMap<String, ValidationSampleStockMeta>, String> {
    let rows = load_stock_list(source_path)?;
    let mut out = HashMap::with_capacity(rows.len());

    for cols in rows {
        let Some(ts_code_raw) = cols.first().map(|value| value.trim()) else {
            continue;
        };
        if ts_code_raw.is_empty() {
            continue;
        }

        let ts_code = ts_code_raw.to_ascii_uppercase();
        let stock_name = cols
            .get(2)
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .map(|value| value.to_string());
        let board = resolve_validation_sample_board_label(
            &ts_code,
            stock_name.as_deref(),
            cols.get(14)
                .map(|value| value.trim())
                .filter(|value| !value.is_empty()),
        );

        out.insert(
            ts_code,
            ValidationSampleStockMeta {
                name: stock_name,
                volatility_group: derive_validation_volatility_group(&board).to_string(),
                board,
            },
        );
    }

    Ok(out)
}

pub(in crate::statistics) fn resolve_validation_sample_board_label(
    ts_code: &str,
    stock_name: Option<&str>,
    market_label: Option<&str>,
) -> String {
    let category_board = board_category(ts_code, stock_name);
    if category_board == "ST" {
        return category_board.to_string();
    }

    if let Some(board) = market_label.and_then(|market_label: &str| -> Option<String> {
        let market_label = market_label.trim();
        if market_label.is_empty() {
            return None;
        }

        if market_label.contains("北交") {
            return Some("北交所".to_string());
        }
        if market_label.contains("科创") {
            return Some("科创板".to_string());
        }
        if market_label.contains("创业") {
            return Some("创业板".to_string());
        }
        if market_label.contains("主板") {
            return Some("主板".to_string());
        }

        Some(market_label.to_string())
    }) {
        return board;
    }

    category_board.to_string()
}

pub(in crate::statistics) fn derive_validation_volatility_group(board: &str) -> &'static str {
    let board = board.trim();
    if board.contains("北交") || board.contains("创业") || board.contains("科创") {
        "高波动"
    } else if board == "ST" {
        "其他波动"
    } else if board.contains("主板") {
        "常规波动"
    } else {
        "其他波动"
    }
}

pub(in crate::statistics) fn split_board_tags(board_raw: &str) -> Vec<String> {
    board_raw
        .split(|ch| matches!(ch, ',' | ';' | '，' | '；' | '|' | '、' | '/' | '\n' | '\r'))
        .map(|part| part.trim())
        .filter(|part| !part.is_empty())
        .map(|part| part.to_string())
        .collect()
}

pub(in crate::statistics) static BOARD_MAPS_CACHE: Mutex<
    Option<(String, Vec<String>, HashMap<String, Vec<String>>)>,
> = Mutex::new(None);

pub(in crate::statistics) fn get_or_build_board_maps(
    source_path: &str,
) -> Result<(Vec<String>, HashMap<String, Vec<String>>), String> {
    {
        let cache = BOARD_MAPS_CACHE
            .lock()
            .map_err(|e| format!("读取板块映射缓存失败: {e}"))?;
        if let Some((cached_path, board_options, ts_board_map)) = cache.as_ref() {
            if cached_path == source_path {
                return Ok((board_options.clone(), ts_board_map.clone()));
            }
        }
    }
    let (board_options, ts_board_map) = build_board_maps(source_path)?;
    let mut cache = BOARD_MAPS_CACHE
        .lock()
        .map_err(|e| format!("写入板块映射缓存失败: {e}"))?;
    *cache = Some((
        source_path.to_string(),
        board_options.clone(),
        ts_board_map.clone(),
    ));
    Ok((board_options, ts_board_map))
}

pub(in crate::statistics) fn build_board_maps(
    source_path: &str,
) -> Result<(Vec<String>, HashMap<String, Vec<String>>), String> {
    let stock_rows = load_stock_list(source_path)?;
    let mut ts_board_map: HashMap<String, Vec<String>> = HashMap::with_capacity(stock_rows.len());
    let mut board_set: HashSet<String> = HashSet::new();

    for cols in stock_rows {
        let Some(ts_code_raw) = cols.first().map(|value| value.trim()) else {
            continue;
        };
        let ts_code = ts_code_raw.to_ascii_uppercase();
        let stock_name = cols
            .get(2)
            .map(|value| value.trim())
            .filter(|value| !value.is_empty());

        let mut board_list = Vec::new();
        let category_board = board_category(&ts_code, stock_name).to_string();
        board_set.insert(category_board.clone());
        board_list.push(category_board);

        if let Some(board_raw) = cols.get(14).map(|value| value.trim()) {
            if !board_raw.is_empty() {
                let detail_boards = split_board_tags(board_raw);
                for board in detail_boards {
                    if board_list.iter().any(|item| item == &board) {
                        continue;
                    }
                    board_set.insert(board.clone());
                    board_list.push(board);
                }
            }
        }

        ts_board_map.insert(ts_code, board_list);
    }

    let mut board_options = board_set.into_iter().collect::<Vec<_>>();
    board_options.sort();

    Ok((board_options, ts_board_map))
}

pub(in crate::statistics) fn resolve_board_filter(
    requested: Option<String>,
    board_options: &[String],
) -> Option<String> {
    let requested = requested
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    if let Some(board) = requested {
        if board_options.iter().any(|item| item == &board) {
            return Some(board);
        }
    }
    None
}

pub(in crate::statistics) fn match_board_filter_with_st(
    board_list: &[String],
    selected_board: Option<&str>,
    exclude_st_board: bool,
) -> bool {
    if exclude_st_board && board_list.iter().any(|board| board == "ST") {
        return false;
    }
    (|board_list: &[String], selected_board: Option<&str>| -> bool {
        let Some(selected_board) = selected_board else {
            return true;
        };
        board_list.iter().any(|board| board == selected_board)
    })(board_list, selected_board)
}

pub(in crate::statistics) fn build_backtest_stock_filter(
    source_path: &str,
    board: Option<String>,
    exclude_st_board: Option<bool>,
    total_mv_min: Option<f64>,
    total_mv_max: Option<f64>,
) -> Result<
    (
        Option<String>,
        bool,
        Option<f64>,
        Option<f64>,
        Option<HashSet<String>>,
    ),
    String,
> {
    let exclude_st_board = exclude_st_board.unwrap_or(false);
    let requested_board = board
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty() && value != "全部");
    let (total_mv_min, total_mv_max) = (|total_mv_min: Option<f64>,
                                         total_mv_max: Option<f64>|
     -> Result<(Option<f64>, Option<f64>), String> {
        let total_mv_min = total_mv_min.filter(|value| value.is_finite());
        let total_mv_max = total_mv_max.filter(|value| value.is_finite());
        if let (Some(min_v), Some(max_v)) = (total_mv_min, total_mv_max) {
            if min_v > max_v {
                return Err("总市值最小值不能大于最大值".to_string());
            }
        }
        Ok((total_mv_min, total_mv_max))
    })(total_mv_min, total_mv_max)?;
    let has_mv_filter = total_mv_min.is_some() || total_mv_max.is_some();

    if requested_board.is_none() && !exclude_st_board && !has_mv_filter {
        return Ok((None, false, None, None, None));
    }

    let (board_options, ts_board_map) = get_or_build_board_maps(source_path)?;
    let resolved_board = resolve_board_filter(requested_board, &board_options);
    let total_mv_map = if has_mv_filter {
        build_total_mv_map(source_path)?
    } else {
        HashMap::new()
    };
    let allowed_ts_codes = ts_board_map
        .into_iter()
        .filter_map(|(ts_code, board_list)| {
            if match_board_filter_with_st(&board_list, resolved_board.as_deref(), exclude_st_board)
                && filter_mv(&total_mv_map, &ts_code, total_mv_min, total_mv_max)
            {
                Some(ts_code)
            } else {
                None
            }
        })
        .collect::<HashSet<_>>();

    Ok((
        resolved_board,
        exclude_st_board,
        total_mv_min,
        total_mv_max,
        Some(allowed_ts_codes),
    ))
}

pub(in crate::statistics) fn ts_code_allowed_by_filter(
    allowed_ts_codes: Option<&HashSet<String>>,
    ts_code: &str,
) -> bool {
    let Some(allowed_ts_codes) = allowed_ts_codes else {
        return true;
    };
    allowed_ts_codes.contains(ts_code.trim())
        || allowed_ts_codes.contains(ts_code.trim().to_ascii_uppercase().as_str())
}

#[cfg(test)]
mod tests {
    use crate::statistics::universe::derive_validation_volatility_group;
    use crate::statistics::universe::resolve_validation_sample_board_label;

    #[test]
    fn validation_sample_board_prefers_market_label_and_derives_group() {
        assert_eq!(
            resolve_validation_sample_board_label("688001.SH", Some("样本股"), Some("科创板")),
            "科创板"
        );
        assert_eq!(derive_validation_volatility_group("科创板"), "高波动");
    }

    #[test]
    fn validation_sample_board_keeps_st_override() {
        assert_eq!(
            resolve_validation_sample_board_label("000001.SZ", Some("*ST样本"), Some("主板")),
            "ST"
        );
        assert_eq!(derive_validation_volatility_group("ST"), "其他波动");
    }
}
