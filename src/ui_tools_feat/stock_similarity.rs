use std::collections::{HashMap, HashSet};

use duckdb::{Connection, params, params_from_iter};
use serde::Serialize;

use crate::data::result_db_path;

use super::{build_concepts_map, build_industry_map, build_name_map, resolve_trade_date};

const CONCEPT_WEIGHT: f64 = 40.0;
const INDUSTRY_WEIGHT: f64 = 40.0;
const SCENE_WEIGHT: f64 = 30.0;
const DEFAULT_LIMIT: usize = 30;

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StockSimilarityTarget {
    pub ts_code: String,
    pub name: Option<String>,
    pub industry: Option<String>,
    pub concept: Option<String>,
    pub concept_items: Vec<String>,
    pub trigger_scene_names: Vec<String>,
    pub available_score: f64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StockSimilarityRow {
    pub ts_code: String,
    pub name: Option<String>,
    pub industry: Option<String>,
    pub concept: Option<String>,
    pub total_score: Option<f64>,
    pub rank: Option<i64>,
    pub similarity_score: f64,
    pub concept_score: f64,
    pub industry_score: f64,
    pub scene_score: f64,
    pub same_industry: bool,
    pub matched_concepts: Vec<String>,
    pub matched_scene_names: Vec<String>,
    pub concept_match_ratio: Option<f64>,
    pub scene_match_ratio: Option<f64>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StockSimilarityPageData {
    pub resolved_trade_date: String,
    pub resolved_ts_code: String,
    pub target: StockSimilarityTarget,
    pub items: Vec<StockSimilarityRow>,
}

#[derive(Debug)]
struct SummaryRow {
    ts_code: String,
    total_score: Option<f64>,
    rank: Option<i64>,
}

pub(crate) struct StockSimilarityMaps<'a> {
    pub name_map: &'a HashMap<String, String>,
    pub concept_map: &'a HashMap<String, String>,
    pub industry_map: &'a HashMap<String, String>,
}

fn normalize_ts_code(ts_code: &str) -> String {
    let normalized = ts_code.trim().to_ascii_uppercase();
    if normalized.contains('.') {
        return normalized;
    }

    if normalized.starts_with("30") || normalized.starts_with("00") {
        format!("{normalized}.SZ")
    } else if normalized.starts_with("60") || normalized.starts_with("68") {
        format!("{normalized}.SH")
    } else {
        format!("{normalized}.BJ")
    }
}

fn split_match_items(value: &str) -> Vec<String> {
    let normalized = value.trim();
    if normalized.is_empty() {
        return Vec::new();
    }

    let mut out = Vec::new();
    let mut seen = HashSet::new();
    for item in
        normalized.split(|ch| matches!(ch, ';' | ',' | '，' | '；' | '|' | '、' | '/' | '\n'))
    {
        let item = item.trim();
        if item.is_empty() {
            continue;
        }
        if seen.insert(item.to_string()) {
            out.push(item.to_string());
        }
    }

    if out.is_empty() {
        vec![normalized.to_string()]
    } else {
        out
    }
}

fn open_result_conn(source_path: &str) -> Result<Connection, String> {
    let result_db = result_db_path(source_path);
    let result_db_str = result_db
        .to_str()
        .ok_or_else(|| "结果库路径不是有效UTF-8".to_string())?;
    Connection::open(result_db_str).map_err(|e| format!("打开结果库失败: {e}"))
}

fn load_target_summary_row(
    conn: &Connection,
    trade_date: &str,
    ts_code: &str,
) -> Result<Option<SummaryRow>, String> {
    let mut stmt = conn
        .prepare(
            r#"
            SELECT ts_code, total_score, rank
            FROM score_summary
            WHERE trade_date = ? AND ts_code = ?
            LIMIT 1
            "#,
        )
        .map_err(|e| format!("预编译相似股目标查询失败: {e}"))?;
    let mut rows = stmt
        .query(params![trade_date, ts_code])
        .map_err(|e| format!("查询相似股目标失败: {e}"))?;

    let Some(row) = rows
        .next()
        .map_err(|e| format!("读取相似股目标失败: {e}"))?
    else {
        return Ok(None);
    };

    Ok(Some(SummaryRow {
        ts_code: row.get(0).map_err(|e| format!("读取 ts_code 失败: {e}"))?,
        total_score: row
            .get(1)
            .map_err(|e| format!("读取 total_score 失败: {e}"))?,
        rank: row.get(2).map_err(|e| format!("读取 rank 失败: {e}"))?,
    }))
}

fn load_candidate_summary_rows(
    conn: &Connection,
    trade_date: &str,
    candidate_codes: &HashSet<String>,
) -> Result<Vec<SummaryRow>, String> {
    if candidate_codes.is_empty() {
        return Ok(Vec::new());
    }

    let mut sorted_codes = candidate_codes.iter().cloned().collect::<Vec<_>>();
    sorted_codes.sort();
    let placeholders = std::iter::repeat_n("(?)", sorted_codes.len())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        r#"
        WITH candidates(ts_code) AS (VALUES {placeholders})
        SELECT s.ts_code, s.total_score, s.rank
        FROM score_summary AS s
        INNER JOIN candidates AS c ON c.ts_code = s.ts_code
        WHERE s.trade_date = ?
        ORDER BY s.rank ASC NULLS LAST, s.total_score DESC NULLS LAST, s.ts_code ASC
        "#
    );
    let mut params = sorted_codes;
    params.push(trade_date.to_string());
    let mut stmt = conn
        .prepare(&sql)
        .map_err(|e| format!("预编译相似股候选查询失败: {e}"))?;
    let mut rows = stmt
        .query(params_from_iter(params.iter()))
        .map_err(|e| format!("查询相似股候选失败: {e}"))?;

    let mut out = Vec::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取相似股候选失败: {e}"))?
    {
        out.push(SummaryRow {
            ts_code: row.get(0).map_err(|e| format!("读取 ts_code 失败: {e}"))?,
            total_score: row
                .get(1)
                .map_err(|e| format!("读取 total_score 失败: {e}"))?,
            rank: row.get(2).map_err(|e| format!("读取 rank 失败: {e}"))?,
        });
    }

    Ok(out)
}

fn push_unique(values: &mut Vec<String>, value: String) {
    if !values.iter().any(|existing| existing == &value) {
        values.push(value);
    }
}

fn load_target_trigger_scenes(
    conn: &Connection,
    trade_date: &str,
    ts_code: &str,
) -> Result<Vec<String>, String> {
    let mut stmt = conn
        .prepare(
            r#"
            SELECT scene_name
            FROM scene_details
            WHERE trade_date = ? AND ts_code = ?
              AND stage IN ('trigger', 'confirm')
            ORDER BY scene_name ASC
            "#,
        )
        .map_err(|e| format!("预编译相似股目标场景查询失败: {e}"))?;
    let mut rows = stmt
        .query(params![trade_date, ts_code])
        .map_err(|e| format!("查询相似股目标场景失败: {e}"))?;

    let mut out = Vec::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取相似股目标场景失败: {e}"))?
    {
        let scene_name: String = row.get(0).map_err(|e| format!("读取场景名称失败: {e}"))?;
        let scene_name = scene_name.trim();
        if scene_name.is_empty() {
            continue;
        }
        push_unique(&mut out, scene_name.to_string());
    }

    Ok(out)
}

fn load_matching_scene_map(
    conn: &Connection,
    trade_date: &str,
    ts_code: &str,
) -> Result<HashMap<String, Vec<String>>, String> {
    let mut stmt = conn
        .prepare(
            r#"
            WITH target_scene AS (
                SELECT DISTINCT scene_name
                FROM scene_details
                WHERE trade_date = ?
                  AND ts_code = ?
                  AND stage IN ('trigger', 'confirm')
                  AND scene_name IS NOT NULL
                  AND TRIM(scene_name) <> ''
            )
            SELECT cand.ts_code, cand.scene_name
            FROM scene_details AS cand
            INNER JOIN target_scene AS target
              ON target.scene_name = cand.scene_name
            WHERE cand.trade_date = ?
              AND cand.ts_code <> ?
              AND cand.stage IN ('trigger', 'confirm')
            ORDER BY cand.ts_code ASC, cand.scene_name ASC
            "#,
        )
        .map_err(|e| format!("预编译相似股匹配场景查询失败: {e}"))?;
    let mut rows = stmt
        .query(params![trade_date, ts_code, trade_date, ts_code])
        .map_err(|e| format!("查询相似股匹配场景失败: {e}"))?;

    let mut out = HashMap::<String, Vec<String>>::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("读取相似股匹配场景失败: {e}"))?
    {
        let candidate_ts_code: String = row
            .get(0)
            .map_err(|e| format!("读取场景 ts_code 失败: {e}"))?;
        let scene_name: String = row.get(1).map_err(|e| format!("读取场景名称失败: {e}"))?;
        let scene_name = scene_name.trim();
        if scene_name.is_empty() {
            continue;
        }
        push_unique(
            out.entry(candidate_ts_code).or_default(),
            scene_name.to_string(),
        );
    }

    Ok(out)
}

fn build_available_score(
    target_industry: Option<&String>,
    target_concepts: &[String],
    target_trigger_scenes: &[String],
) -> f64 {
    let mut out = 0.0;
    if !target_concepts.is_empty() {
        out += CONCEPT_WEIGHT;
    }
    if target_industry
        .map(|value| !value.trim().is_empty())
        .unwrap_or(false)
    {
        out += INDUSTRY_WEIGHT;
    }
    if !target_trigger_scenes.is_empty() {
        out += SCENE_WEIGHT;
    }
    out
}

fn calc_ratio_score(
    weight: f64,
    sample_items: &[String],
    candidate_items: &[String],
) -> (f64, Vec<String>) {
    if sample_items.is_empty() {
        return (0.0, Vec::new());
    }

    let candidate_set: HashSet<&str> = candidate_items.iter().map(|item| item.as_str()).collect();
    let matched_items: Vec<String> = sample_items
        .iter()
        .filter(|item| candidate_set.contains(item.as_str()))
        .cloned()
        .collect();

    let score = weight * matched_items.len() as f64 / sample_items.len() as f64;
    (score, matched_items)
}

pub(crate) fn get_stock_similarity_page_with_conn(
    conn: &Connection,
    source_path: &str,
    effective_trade_date: &str,
    ts_code: &str,
    limit: Option<u32>,
) -> Result<StockSimilarityPageData, String> {
    let name_map = build_name_map(source_path).unwrap_or_default();
    let concept_map = build_concepts_map(source_path).unwrap_or_default();
    let industry_map = build_industry_map(source_path).unwrap_or_default();
    get_stock_similarity_page_with_conn_and_maps(
        conn,
        effective_trade_date,
        ts_code,
        limit,
        StockSimilarityMaps {
            name_map: &name_map,
            concept_map: &concept_map,
            industry_map: &industry_map,
        },
    )
}

pub(crate) fn get_stock_similarity_page_with_conn_and_maps(
    conn: &Connection,
    effective_trade_date: &str,
    ts_code: &str,
    limit: Option<u32>,
    maps: StockSimilarityMaps<'_>,
) -> Result<StockSimilarityPageData, String> {
    let normalized_ts_code = normalize_ts_code(ts_code);
    if load_target_summary_row(conn, effective_trade_date, &normalized_ts_code)?.is_none() {
        return Err(format!(
            "未找到 {} 在 {} 的评分结果",
            normalized_ts_code, effective_trade_date
        ));
    }

    let target_concept_text = maps.concept_map.get(&normalized_ts_code).cloned();
    let target_industry = maps.industry_map.get(&normalized_ts_code).cloned();
    let target_concept_items = target_concept_text
        .as_deref()
        .map(split_match_items)
        .unwrap_or_default();
    let target_trigger_scenes =
        load_target_trigger_scenes(conn, effective_trade_date, &normalized_ts_code)?;
    let available_score = build_available_score(
        target_industry.as_ref(),
        &target_concept_items,
        &target_trigger_scenes,
    );

    if available_score <= 0.0 {
        return Err(format!(
            "{} 在 {} 没有可比对的概念、行业或 trigger 以上场景",
            normalized_ts_code, effective_trade_date
        ));
    }

    let limit = limit
        .map(|value| value as usize)
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_LIMIT);

    let matching_scene_map = if target_trigger_scenes.is_empty() {
        HashMap::new()
    } else {
        load_matching_scene_map(conn, effective_trade_date, &normalized_ts_code)?
    };
    let mut candidate_codes = HashSet::new();
    if let Some(target_industry) = target_industry.as_deref().map(str::trim) {
        if !target_industry.is_empty() {
            for (candidate_ts_code, industry) in maps.industry_map {
                if candidate_ts_code != &normalized_ts_code && industry.trim() == target_industry {
                    candidate_codes.insert(candidate_ts_code.clone());
                }
            }
        }
    }
    if !target_concept_items.is_empty() {
        let target_concept_set = target_concept_items
            .iter()
            .map(String::as_str)
            .collect::<HashSet<_>>();
        for (candidate_ts_code, concept_text) in maps.concept_map {
            if candidate_ts_code == &normalized_ts_code {
                continue;
            }
            if split_match_items(concept_text)
                .iter()
                .any(|item| target_concept_set.contains(item.as_str()))
            {
                candidate_codes.insert(candidate_ts_code.clone());
            }
        }
    }
    for candidate_ts_code in matching_scene_map.keys() {
        candidate_codes.insert(candidate_ts_code.clone());
    }

    let summary_rows = load_candidate_summary_rows(conn, effective_trade_date, &candidate_codes)?;
    let mut items = Vec::new();
    for row in summary_rows {
        if row.ts_code == normalized_ts_code {
            continue;
        }

        let candidate_concept_text = maps.concept_map.get(&row.ts_code).cloned();
        let candidate_industry = maps.industry_map.get(&row.ts_code).cloned();
        let candidate_concept_items = candidate_concept_text
            .as_deref()
            .map(split_match_items)
            .unwrap_or_default();
        let candidate_trigger_scenes = matching_scene_map
            .get(&row.ts_code)
            .cloned()
            .unwrap_or_default();

        let (concept_score, matched_concepts) = calc_ratio_score(
            CONCEPT_WEIGHT,
            &target_concept_items,
            &candidate_concept_items,
        );
        let (scene_score, matched_scene_names) = calc_ratio_score(
            SCENE_WEIGHT,
            &target_trigger_scenes,
            &candidate_trigger_scenes,
        );

        let same_industry = match (target_industry.as_deref(), candidate_industry.as_deref()) {
            (Some(left), Some(right)) => {
                let left = left.trim();
                let right = right.trim();
                !left.is_empty() && left == right
            }
            _ => false,
        };
        let industry_score = if same_industry { INDUSTRY_WEIGHT } else { 0.0 };
        let similarity_score = concept_score + industry_score + scene_score;

        if similarity_score <= 0.0 {
            continue;
        }

        items.push(StockSimilarityRow {
            ts_code: row.ts_code.clone(),
            name: maps.name_map.get(&row.ts_code).cloned(),
            industry: candidate_industry,
            concept: candidate_concept_text,
            total_score: row.total_score,
            rank: row.rank,
            similarity_score,
            concept_score,
            industry_score,
            scene_score,
            same_industry,
            matched_concepts,
            matched_scene_names,
            concept_match_ratio: if target_concept_items.is_empty() {
                None
            } else {
                Some(concept_score / CONCEPT_WEIGHT)
            },
            scene_match_ratio: if target_trigger_scenes.is_empty() {
                None
            } else {
                Some(scene_score / SCENE_WEIGHT)
            },
        });
    }

    items.sort_by(|left, right| {
        right
            .similarity_score
            .total_cmp(&left.similarity_score)
            .then_with(|| right.scene_score.total_cmp(&left.scene_score))
            .then_with(|| right.concept_score.total_cmp(&left.concept_score))
            .then_with(|| right.industry_score.total_cmp(&left.industry_score))
            .then_with(|| {
                left.rank
                    .unwrap_or(i64::MAX)
                    .cmp(&right.rank.unwrap_or(i64::MAX))
            })
            .then_with(|| left.ts_code.cmp(&right.ts_code))
    });
    if items.len() > limit {
        items.truncate(limit);
    }

    let target_name = maps.name_map.get(&normalized_ts_code).cloned();

    Ok(StockSimilarityPageData {
        resolved_trade_date: effective_trade_date.to_string(),
        resolved_ts_code: normalized_ts_code.clone(),
        target: StockSimilarityTarget {
            ts_code: normalized_ts_code,
            name: target_name,
            industry: target_industry,
            concept: target_concept_text,
            concept_items: target_concept_items,
            trigger_scene_names: target_trigger_scenes,
            available_score,
        },
        items,
    })
}

pub fn get_stock_similarity_page(
    source_path: String,
    trade_date: Option<String>,
    ts_code: String,
    limit: Option<u32>,
) -> Result<StockSimilarityPageData, String> {
    let conn = open_result_conn(&source_path)?;
    let effective_trade_date = resolve_trade_date(&conn, trade_date)?;
    get_stock_similarity_page_with_conn(&conn, &source_path, &effective_trade_date, &ts_code, limit)
}

#[cfg(test)]
mod tests {
    use super::{CONCEPT_WEIGHT, SCENE_WEIGHT, calc_ratio_score, split_match_items};

    #[test]
    fn split_match_items_dedup_and_trim() {
        assert_eq!(
            split_match_items("芯片, 算力；芯片|机器人 / 算力"),
            vec!["芯片", "算力", "机器人"]
        );
    }

    #[test]
    fn ratio_score_follows_input_sample_count() {
        let sample = vec![
            "概念1".to_string(),
            "概念2".to_string(),
            "概念3".to_string(),
            "概念4".to_string(),
            "概念5".to_string(),
        ];
        let candidate = vec![
            "概念2".to_string(),
            "概念3".to_string(),
            "概念4".to_string(),
            "概念5".to_string(),
        ];

        let (score, matched) = calc_ratio_score(CONCEPT_WEIGHT, &sample, &candidate);
        assert_eq!(matched.len(), 4);
        assert!((score - 32.0).abs() < f64::EPSILON);
    }

    #[test]
    fn scene_score_uses_declared_weight() {
        let sample = vec![
            "场景1".to_string(),
            "场景2".to_string(),
            "场景3".to_string(),
            "场景4".to_string(),
            "场景5".to_string(),
        ];
        let candidate = vec![
            "场景1".to_string(),
            "场景2".to_string(),
            "场景3".to_string(),
            "场景4".to_string(),
        ];

        let (score, matched) = calc_ratio_score(SCENE_WEIGHT, &sample, &candidate);
        assert_eq!(matched.len(), 4);
        assert!((score - 24.0).abs() < f64::EPSILON);
    }
}
