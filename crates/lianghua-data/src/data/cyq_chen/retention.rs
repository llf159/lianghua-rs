use super::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChenChipRetentionRequest {
    pub anchor_date: String,
    pub evaluation_date: String,
    pub price_low: f64,
    pub price_high: f64,
}

/// A single continuous replay freezes whole anchor buckets, then reads mixed surviving lots.
/// Masses are per 100 model shares; ratios are 0–1. Identity includes the exact strategy.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChenChipRetention {
    pub request: ChenChipRetentionRequest,
    pub strategy: ChipChangeConfig,
    pub model: String,
    pub warmup_days: usize,
    pub bucket_pct: f64,
    pub output_start_date: String,
    pub simulation_start_date: Option<String>,
    pub anchor_observation: Option<usize>,
    pub evaluation_observation: Option<usize>,
    pub quote_updates: Option<usize>,
    pub actual_price_low: Option<f64>,
    pub actual_price_high: Option<f64>,
    pub anchor_mass: Option<f64>,
    pub old_remaining: Option<f64>,
    pub current_mass: Option<f64>,
    pub added_mass: Option<f64>,
    pub retention_ratio: Option<f64>,
    pub added_ratio: Option<f64>,
    pub unavailable_reason: Option<String>,
}

pub(in crate::data) struct ChenChipRetentionCapture<'a> {
    pub request: &'a ChenChipRetentionRequest,
    pub result: &'a mut ChenChipRetention,
    pub query_only: bool,
}

impl ChenChipRetention {
    pub(super) fn new(
        output_start_date: &str,
        chip_config: &CompiledChipChangeConfig,
        config: ChenChipConfig,
        request: &ChenChipRetentionRequest,
    ) -> Result<Self, String> {
        if request.anchor_date > request.evaluation_date
            || !request.price_low.is_finite()
            || !request.price_high.is_finite()
            || request.price_low <= 0.0
            || request.price_high <= request.price_low
        {
            return Err("留存查询需要锚点不晚于评价日，且成本区间为正的递增边界".into());
        }
        for date in [&request.anchor_date, &request.evaluation_date] {
            chrono::NaiveDate::parse_from_str(date, "%Y%m%d")
                .map_err(|_| "留存查询日期必须为有效的YYYYMMDD".to_string())?;
        }
        Ok(Self {
            request: request.clone(),
            strategy: ChipChangeConfig {
                version: chip_config.version,
                strategy: chip_config
                    .strategies
                    .iter()
                    .map(|strategy| ChipChangeStrategy {
                        name: strategy.name.clone(),
                        holder: strategy.holder,
                        direction: strategy.direction,
                        when: strategy.when.clone(),
                        bias: strategy.bias,
                        confirm_after: strategy.confirm_after,
                    })
                    .collect(),
            },
            model: "mixedProportionalSurvival-v1".into(),
            warmup_days: config.warmup_days,
            bucket_pct: config.bucket_pct,
            output_start_date: output_start_date.into(),
            simulation_start_date: None,
            anchor_observation: None,
            evaluation_observation: None,
            quote_updates: None,
            actual_price_low: None,
            actual_price_high: None,
            anchor_mass: None,
            old_remaining: None,
            current_mass: None,
            added_mass: None,
            retention_ratio: None,
            added_ratio: None,
            unavailable_reason: Some("评价日不在本次模拟的有效报价中".into()),
        })
    }
}

pub fn query_chen_chip_retention(
    row_data: &RowData,
    output_start_date: &str,
    chip_config: &CompiledChipChangeConfig,
    config: ChenChipConfig,
    request: &ChenChipRetentionRequest,
) -> Result<ChenChipRetention, String> {
    let mut result = ChenChipRetention::new(output_start_date, chip_config, config, request)?;
    replay_chen_chip_snapshots(
        Cow::Borrowed(row_data),
        output_start_date,
        chip_config,
        config,
        &mut ChenChipFeatureState::default(),
        false,
        Some(ChenChipRetentionCapture {
            request,
            result: &mut result,
            query_only: true,
        }),
        |_| Ok(()),
    )?;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::cyq_chen::test_support::assert_close;
    use std::collections::HashMap;

    #[test]
    fn fixed_interval_distinguishes_replacement_and_honors_age_limit() {
        let row = RowData {
            trade_dates: (0..122)
                .map(|day| {
                    (chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()
                        + chrono::Duration::days(day))
                    .format("%Y%m%d")
                    .to_string()
                })
                .collect(),
            cols: HashMap::from([
                ("O".into(), vec![Some(10.0); 122]),
                ("H".into(), vec![Some(10.0); 122]),
                ("L".into(), vec![Some(10.0); 122]),
                ("C".into(), vec![Some(10.0); 122]),
                (
                    "TOR".into(),
                    (0..122)
                        .map(|day| Some(if day == 1 { 60.0 } else { 0.0 }))
                        .collect(),
                ),
            ]),
        };
        let chip = ChipChangeConfig {
            version: 2,
            strategy: vec![],
        }
        .compile()
        .unwrap();
        let config = ChenChipConfig {
            warmup_days: 0,
            bucket_pct: 5.0,
        };
        let snapshots = compute_chen_chip_snapshots_with_compiled_config(
            &row,
            &row.trade_dates[0],
            &chip,
            config,
        )
        .unwrap();
        for (distance, remaining) in [(0, 100.0), (1, 40.0), (120, 40.0), (121, 0.0)] {
            let result = query_chen_chip_retention(
                &row,
                &row.trade_dates[0],
                &chip,
                config,
                &ChenChipRetentionRequest {
                    anchor_date: row.trade_dates[0].clone(),
                    evaluation_date: row.trade_dates[distance].clone(),
                    price_low: 9.0,
                    price_high: 11.0,
                },
            )
            .unwrap();
            let (combined, captured) = compute_chen_chip_snapshots_with_retention(
                &row,
                &row.trade_dates[0],
                &chip,
                config,
                Some(&result.request),
            )
            .unwrap();
            assert_eq!(
                serde_json::to_value(&captured.unwrap()).unwrap(),
                serde_json::to_value(&result).unwrap()
            );
            assert_eq!(
                serde_json::to_value(&combined).unwrap(),
                serde_json::to_value(&snapshots).unwrap()
            );
            assert_eq!(result.quote_updates, Some(distance));
            if distance == 121 {
                assert!(result.unavailable_reason.is_some());
                assert!(result.old_remaining.is_none());
            } else {
                assert!(result.unavailable_reason.is_none());
                assert_close(result.anchor_mass.unwrap(), 100.0);
                assert_close(result.old_remaining.unwrap(), remaining);
                assert_close(result.current_mass.unwrap(), 100.0);
                assert_close(result.added_mass.unwrap(), 100.0 - remaining);
                assert_close(result.retention_ratio.unwrap(), remaining / 100.0);
            }
        }
        let mut expanded = row.clone();
        for key in ["O", "H", "L", "C"] {
            expanded.cols.get_mut(key).unwrap()[1..].fill(Some(20.0));
        }
        let outside = query_chen_chip_retention(
            &expanded,
            &row.trade_dates[0],
            &chip,
            config,
            &ChenChipRetentionRequest {
                anchor_date: row.trade_dates[0].clone(),
                evaluation_date: row.trade_dates[1].clone(),
                price_low: 9.0,
                price_high: 11.0,
            },
        )
        .unwrap();
        assert_close(outside.old_remaining.unwrap(), 40.0);
        assert_close(outside.current_mass.unwrap(), 40.0);
        assert_close(outside.added_mass.unwrap(), 0.0);
        let empty = query_chen_chip_retention(
            &expanded,
            &row.trade_dates[0],
            &chip,
            config,
            &ChenChipRetentionRequest {
                anchor_date: row.trade_dates[1].clone(),
                evaluation_date: row.trade_dates[2].clone(),
                price_low: 14.0,
                price_high: 15.0,
            },
        )
        .unwrap();
        assert!(empty.unavailable_reason.is_none());
        assert_close(empty.anchor_mass.unwrap(), 0.0);
        assert!(empty.retention_ratio.is_none());
        assert!(empty.added_ratio.is_none());
        let at120 = snapshots[120].age_summary.as_ref().unwrap();
        assert_close(at120.masses[3], 60.0);
        let at121 = snapshots[121].age_summary.as_ref().unwrap();
        assert_close(at121.masses[4], 60.0);
        assert_close(at121.masses[5], 40.0);
    }
}
