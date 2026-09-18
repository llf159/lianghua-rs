import { useMemo } from "react";
import type { RuleValidationComboResult } from "../../../../apis/strategyTrigger";
import {
  TableSortButton,
  getAriaSort,
  useTableSort,
  type SortDefinition,
} from "../../../../shared/tableSort";
import { formatNumber, formatPercent } from "../../../../shared/backtestFormat";
import {
  formatUnknownValuesForCombo,
  formatValidationInsufficientHint,
  formatValidationWindowRatio,
} from "../../../../shared/expressionValidation";

type ComboRobustnessSortKey =
  | "params"
  | "trigger_samples"
  | "ic_mean"
  | "ic_t_value"
  | "spread_mean"
  | "oos_ic"
  | "oos_residual"
  | "incremental";

export function ExpressionParameterRobustness({
  combos,
  selectedComboKey,
  onSelectCombo,
}: {
  combos: RuleValidationComboResult[];
  selectedComboKey: string;
  onSelectCombo: (comboKey: string) => void;
}) {
  const sortDefinitions = useMemo(
    () =>
      ({
        params: {
          value: (row: RuleValidationComboResult) => formatUnknownValuesForCombo(row),
        },
        trigger_samples: {
          value: (row: RuleValidationComboResult) => row.trigger_samples,
        },
        ic_mean: {
          value: (row: RuleValidationComboResult) => row.backtest.ic_mean,
        },
        ic_t_value: {
          value: (row: RuleValidationComboResult) => row.backtest.ic_t_value,
        },
        spread_mean: {
          value: (row: RuleValidationComboResult) => row.backtest.spread_mean,
        },
        oos_ic: {
          value: (row: RuleValidationComboResult) => row.walk_forward?.ic_positive_folds,
        },
        oos_residual: {
          value: (row: RuleValidationComboResult) => row.walk_forward?.residual_positive_folds,
        },
        incremental: {
          value: (row: RuleValidationComboResult) => row.incremental?.positive_folds,
        },
      }) satisfies Partial<Record<ComboRobustnessSortKey, SortDefinition<RuleValidationComboResult>>>,
    [],
  );

  const {
    sortKey,
    sortDirection,
    sortedRows,
    toggleSort,
  } = useTableSort<RuleValidationComboResult, ComboRobustnessSortKey>(combos, sortDefinitions);

  function renderSortHeader(key: ComboRobustnessSortKey, label: string, title: string) {
    return (
      <th aria-sort={getAriaSort(sortKey === key, sortDirection)}>
        <TableSortButton
          label={label}
          isActive={sortKey === key && sortDirection !== null}
          direction={sortDirection}
          onClick={() => toggleSort(key)}
          title={title}
        />
      </th>
    );
  }

  return (
    <div className="expression-backtest-section">
      <h3>参数鲁棒性</h3>
      <p className="expression-backtest-caption">
        默认按后端返回的参数枚举顺序展示，点击表头才排序；点击任意一行，下方 walk-forward、分层、
        重复性、增量与样本检查会全部切到该参数组合。系统不会自动挑选最佳参数，请比较相邻参数是否方向一致、
        是否只有单点异常突出。所有组合共用同一套 walk-forward calendar（同一次请求里 train/test 日期完全一致），
        样本不足的 fold 计入分母但不计为正窗口。
      </p>
      <div className="expression-backtest-table-wrap">
        <table className="expression-backtest-table expression-backtest-robustness-table">
          <thead>
            <tr>
              {renderSortHeader("params", "参数", "按参数字典序排序")}
              {renderSortHeader("trigger_samples", "样本", "按触发样本数排序")}
              {renderSortHeader("ic_mean", "IC", "按 IC 均值排序")}
              {renderSortHeader("ic_t_value", "IC t", "按 IC t值排序")}
              {renderSortHeader("spread_mean", "Spread", "按分层差均值排序")}
              {renderSortHeader("oos_ic", "OOS IC", "按样本外 IC 正窗口数量排序")}
              {renderSortHeader("oos_residual", "OOS Residual", "按样本外残差正窗口数量排序")}
              {renderSortHeader("incremental", "Incremental", "按样本外增量正窗口数量排序")}
            </tr>
          </thead>
          <tbody>
            {sortedRows.map((combo) => {
              const isActive = combo.combo_key === selectedComboKey;
              const unknownValueText = formatUnknownValuesForCombo(combo);
              return (
                <tr
                  key={combo.combo_key}
                  className={
                    isActive
                      ? "expression-backtest-robustness-row-active expression-backtest-row-selectable"
                      : "expression-backtest-row-selectable"
                  }
                  onClick={() => onSelectCombo(combo.combo_key)}
                >
                  <td>
                    <span
                      className="expression-backtest-robustness-params"
                      title={`${combo.combo_label}：${unknownValueText}`}
                    >
                      {unknownValueText}
                    </span>
                  </td>
                  <td>{combo.trigger_samples}</td>
                  <td>{formatNumber(combo.backtest.ic_mean)}</td>
                  <td>{formatNumber(combo.backtest.ic_t_value)}</td>
                  <td>{formatPercent(combo.backtest.spread_mean)}</td>
                  <td title={formatValidationInsufficientHint(combo, "walk_forward")}>
                    {formatValidationWindowRatio(combo, "ic")}
                  </td>
                  <td title={formatValidationInsufficientHint(combo, "walk_forward")}>
                    {formatValidationWindowRatio(combo, "residual")}
                  </td>
                  <td title={formatValidationInsufficientHint(combo, "incremental")}>
                    {formatValidationWindowRatio(combo, "incremental")}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}
