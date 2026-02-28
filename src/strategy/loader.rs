use serde::Deserialize;
use serde::de::{self, Deserializer};
use std::fs;

// 设计的时候字段要完全适配文本,用Deserialize映射key
#[derive(Debug, Deserialize)]
pub struct IndConfig {
    pub version: u32,
    pub ind: Vec<IndDef>,
}

#[derive(Debug, Deserialize)]
pub struct IndDef {
    pub output_name: String,
    pub expr: String,
    pub prec: u8,
}

#[derive(Debug, Deserialize)]
pub struct ScoreConfig {
    pub version: u32,
    pub rule: Vec<ScoreRule>,
}

#[derive(Debug, Deserialize)]
pub struct ScoreRule {
    pub name: String,
    pub scope_windows: usize,
    pub scope_way: ScopeWay,
    pub when: String,
    pub points: f64,
    pub dist_points: Option<Vec<DistPoint>>,
    pub explain: String,
    pub tag: RuleTag,
}

#[derive(Debug, Clone, Copy)]
pub enum ScopeWay {
    Any,
    Last,
    Each,
    Recent,
    Consec(usize),
}

#[derive(Debug, Clone, Copy, serde::Deserialize)]
pub enum RuleTag {
    Opportunity,
}

#[derive(Debug, Deserialize)]
pub struct DistPoint {
    pub min: usize,
    pub max: usize,
    pub points: f64,
}

impl ScoreRule {
    pub fn load_rules() -> Result<Vec<ScoreRule>, String> {
        let rule_toml = fs::read_to_string("score_rule.toml")
            .map_err(|e| format!("规则文件不存在或不可读: {e}"))?;
        let cfg: ScoreConfig =
            toml::from_str(&rule_toml).map_err(|e| format!("规则文件格式错误: {e}"))?;
        Self::validate_rules(&cfg.rule)?;
        Ok(cfg.rule)
    }

    fn validate_rules(rules: &[ScoreRule]) -> Result<(), String> {
        for (i, r) in rules.iter().enumerate() {
            let n = i + 1;
            if r.name.trim().is_empty() {
                return Err(format!("第{:?}个表达式name字段为空", n));
            };
            if r.when.trim().is_empty() {
                return Err(format!("第{:?}个表达式when字段为空", n));
            };

            let has_points = r.points.is_finite();
            let has_dist = matches!(r.dist_points.as_ref(), Some(v) if !v.is_empty());
            if !has_points && !has_dist {
                return Err(format!(
                    "第{n}条规则 points 和 dist_points 不能同时无效/为空"
                ));
            }

            if r.dist_points.is_some() {
                let Some(dist) = &r.dist_points else {
                    return Err(format!("第{:?}个表达式dist_points字段错误", n));
                };
                for (j, v) in dist.iter().enumerate() {
                    if v.min > v.max {
                        return Err(format!("第{n}条规则 dist_points 第{}段 min > max", j + 1));
                    }
                    if !v.points.is_finite() {
                        return Err(format!("第{n}条规则 dist_points 第{}段 points 非法", j + 1));
                    }
                }
                let mut dist: Vec<&DistPoint> = dist.iter().collect();
                dist.sort_by_key(|x| x.min);
                for k in 1..dist.len() {
                    let prev = dist[k - 1];
                    let curr = dist[k];

                    if prev.max >= curr.min {
                        return Err(format!(
                            "区间重叠: [{}-{}] 和 [{}-{}]",
                            prev.min, prev.max, curr.min, curr.max
                        ));
                    }
                }
            }
            if r.scope_windows == 0 {
                return Err(format!("第{:?}个表达式scope_windows字段错误", n));
            };
        }
        Ok(())
    }
}

impl IndConfig {
    pub fn load_inds() -> Result<Vec<IndDef>, String> {
        let ind_toml =
            fs::read_to_string("ind.toml").map_err(|e| format!("指标文件不存在或不可读: {e}"))?;
        let cfg: IndConfig =
            toml::from_str(&ind_toml).map_err(|e| format!("指标文件格式错误: {e}"))?;
        Self::validate_inds(&cfg.ind)?;
        Ok(cfg.ind)
    }

    fn validate_inds(inds: &[IndDef]) -> Result<(), String> {
        for (i, x) in inds.iter().enumerate() {
            let n = i + 1;
            if x.output_name.trim().is_empty() {
                return Err(format!("第{n}个指标 output_name 为空"));
            }
            if x.expr.trim().is_empty() {
                return Err(format!("第{n}个指标 expr 为空"));
            }
        }
        Ok(())
    }
}

impl<'de> Deserialize<'de> for ScopeWay {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        let s = raw.trim().to_ascii_uppercase();

        match s.as_str() {
            "ANY" => Ok(ScopeWay::Any),
            "LAST" => Ok(ScopeWay::Last),
            "EACH" => Ok(ScopeWay::Each),
            "RECENT" => Ok(ScopeWay::Recent),
            _ => {
                if let Some(num) = s.strip_prefix("CONSEC>=") {
                    let k = num
                        .parse::<usize>()
                        .map_err(|_| de::Error::custom("CONSEC>= 后必须是正整数"))?;
                    if k == 0 {
                        return Err(de::Error::custom("CONSEC>=0 无效，必须 >= 1"));
                    }
                    Ok(ScopeWay::Consec(k))
                } else {
                    Err(de::Error::custom(
                        "scope_way 仅支持 ANY/LAST/EACH/RECENT/CONSEC>=N",
                    ))
                }
            }
        }
    }
}
