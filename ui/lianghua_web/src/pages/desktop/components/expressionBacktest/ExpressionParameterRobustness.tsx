import type { RuleValidationComboResult } from "../../../../apis/strategyTrigger";

export function ExpressionParameterRobustness({combos, selectedComboKey, onSelectCombo}: {
  combos: RuleValidationComboResult[]; selectedComboKey: string; onSelectCombo: (comboKey: string) => void;
}) {
  const selected = combos.find((item) => item.combo_key === selectedComboKey) ?? combos[0];
  return <section className="expression-backtest-section">
    <h3>参数敏感性分析</h3>
    <p className="research-meta">共 {combos.length} 个参数组合。切换参数后，上方绩效摘要、季度统计、诊断及正负样本同步更新；比较相邻参数可检查效果是否依赖单个取值。</p>
    <div className="research-form-grid">{selected?.unknown_values.map((parameter) => <label className="research-field" key={parameter.name}>{parameter.name}<select value={parameter.value} onChange={(event) => {
      const next = combos.find((combo) => selected.unknown_values.every((item) => combo.unknown_values.some((value) => value.name === item.name && value.value === (item.name === parameter.name ? Number(event.target.value) : item.value))));
      if (next) onSelectCombo(next.combo_key);
    }}>{Array.from(new Set(combos.flatMap((combo) => combo.unknown_values.filter((item) => item.name === parameter.name).map((item) => item.value)))).sort((left, right) => left - right).map((value) => <option key={value} value={value}>{value}</option>)}</select></label>)}</div>
  </section>;
}
