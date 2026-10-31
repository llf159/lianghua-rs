import type { ResearchGrouping } from "../../../apis/strategyTrigger";

export function ResearchGroupingFields({ value, onChange, disabled, showBaselineControl = false }: { showBaselineControl?: boolean; value: ResearchGrouping; onChange: (value: ResearchGrouping) => void; disabled?: boolean }) {
  const [capText, marketText] = [value.market_cap_boundaries.join(", "), value.market_boundaries.join(", ")];
  const baselineText = (value.baseline_score_boundaries ?? []).join(", ");
  return <fieldset className="research-parameter-group"><legend>分组设置</legend>
    <div className="research-form-grid">

    <label className="research-field">市值边界 · 亿元<input title="边界递增，用逗号分隔；留空不分组" key={capText} defaultValue={capText} disabled={disabled} onBlur={(event) => onChange({ ...value, market_cap_boundaries: event.target.value.trim() ? event.target.value.split(/[,，]/).map(Number) : [] })} /></label>{" "}
    {showBaselineControl ? <label className="research-field">原榜分数档 · 可选<input key={baselineText} defaultValue={baselineText} disabled={disabled} onBlur={(event) => onChange({...value, baseline_score_boundaries: event.target.value.trim() ? event.target.value.split(/[,，]/).map(Number) : []})} /></label> : null}{" "}
    <label className="research-field">市场指标 <select value={value.market_metric} disabled={disabled} onChange={(event) => onChange({ ...value, market_metric: event.target.value })}>
      <option value="">不分组</option>{["D10","F5","CP5","VA5","LO5","AC5","IS5","CF20","DT5","SC5"].map((name) => <option key={name} value={`MKT_${name}`}>MKT_{name}</option>)}
    </select></label>{" "}
    <label className="research-field">市场指标边界 <input key={marketText} defaultValue={marketText} disabled={disabled} onBlur={(event) => onChange({ ...value, market_boundaries: event.target.value.trim() ? event.target.value.split(/[,，]/).map(Number) : [] })} /></label>
    </div>
  </fieldset>;
}
