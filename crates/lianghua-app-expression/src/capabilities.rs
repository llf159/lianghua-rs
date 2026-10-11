use serde::Serialize;

use crate::expr::eval::supported_expression_functions;

pub const RT_OPEN_CHANGE_PCT: &str = "RT_OP";
pub const RT_FALL_FROM_HIGH_PCT: &str = "RT_FH";
pub const RT_VOLUME_RATIO: &str = "RT_VR";
pub const RT_OPENING_VOLUME_RATIO_HIGH: &str = "RT_VR5H";
pub const RT_AVERAGE_PRICE: &str = "RT_AVG";

#[derive(Debug, Clone, Copy)]
pub struct ExpressionFieldDefinition {
    pub name: &'static str,
    pub description: &'static str,
    pub example: &'static str,
}

pub const INTRADAY_REALTIME_FIELDS: &[ExpressionFieldDefinition] = &[
    ExpressionFieldDefinition {
        name: RT_OPEN_CHANGE_PCT,
        description: "实体涨幅，计算口径为 (当前价 - 今开) / 昨收 × 100%，单位是百分比。",
        example: "RT_OP >= 2",
    },
    ExpressionFieldDefinition {
        name: RT_FALL_FROM_HIGH_PCT,
        description: "当前价相对于今日高点的回落幅度，单位是百分比；返回值恒为非负数。",
        example: "RT_FH <= 1.5",
    },
    ExpressionFieldDefinition {
        name: RT_VOLUME_RATIO,
        description: "盘中量比；监控量比基准补齐后使用本地五日日均量计算值，否则使用行情源返回值。新浪源没有该字段时为空。",
        example: "RT_VR >= 2",
    },
    ExpressionFieldDefinition {
        name: RT_OPENING_VOLUME_RATIO_HIGH,
        description: "监控专用：开盘前五分钟（09:30–09:35，含端点）的最高量比，基于本地前五个交易日日均量及腾讯分时计算；仅腾讯行情源可用，窗口未结束或数据未补齐时为空，历史日为空。",
        example: "RT_VR > RT_VR5H",
    },
    ExpressionFieldDefinition {
        name: RT_AVERAGE_PRICE,
        description: "行情源返回的均价；新浪源没有该字段时为空。",
        example: "C > RT_AVG",
    },
];

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExpressionFieldData {
    pub name: String,
    pub description: String,
    pub example: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExpressionCapabilitiesData {
    pub supported_functions: Vec<String>,
    pub intraday_realtime_fields: Vec<ExpressionFieldData>,
}

pub fn get_expression_capabilities() -> ExpressionCapabilitiesData {
    ExpressionCapabilitiesData {
        supported_functions: supported_expression_functions()
            .map(str::to_string)
            .collect(),
        intraday_realtime_fields: INTRADAY_REALTIME_FIELDS
            .iter()
            .map(|field| ExpressionFieldData {
                name: field.name.to_string(),
                description: field.description.to_string(),
                example: field.example.to_string(),
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        INTRADAY_REALTIME_FIELDS, RT_AVERAGE_PRICE, RT_FALL_FROM_HIGH_PCT, RT_OPEN_CHANGE_PCT,
        RT_OPENING_VOLUME_RATIO_HIGH, RT_VOLUME_RATIO, get_expression_capabilities,
    };

    #[test]
    fn capabilities_are_derived_from_runtime_field_definitions() {
        let capabilities = get_expression_capabilities();
        let names = capabilities
            .intraday_realtime_fields
            .iter()
            .map(|field| field.name.as_str())
            .collect::<Vec<_>>();

        assert_eq!(INTRADAY_REALTIME_FIELDS.len(), 5);
        assert_eq!(
            names,
            [
                RT_OPEN_CHANGE_PCT,
                RT_FALL_FROM_HIGH_PCT,
                RT_VOLUME_RATIO,
                RT_OPENING_VOLUME_RATIO_HIGH,
                RT_AVERAGE_PRICE,
            ]
        );
    }
}
