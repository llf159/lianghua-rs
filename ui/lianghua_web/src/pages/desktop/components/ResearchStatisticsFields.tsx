import type { ResearchStatisticsSettings } from "../../../apis/strategyTrigger";
export function ResearchStatisticsFields({value, onChange, disabled}: {value: ResearchStatisticsSettings; onChange: (value: ResearchStatisticsSettings) => void; disabled?: boolean}) {
  return <details className="research-disclosure"><summary>统计设置</summary>
    <div className="research-form-grid">
    <label className="research-field">日期块长<input type="number" min={1} max={252} value={value.block_length} disabled={disabled} onChange={(event) => onChange({...value, block_length: Number(event.target.value)})} /></label>
    <label className="research-field">重采样次数<input type="number" min={100} max={5000} value={value.resamples} disabled={disabled} onChange={(event) => onChange({...value, resamples: Number(event.target.value)})} /></label>
    <label className="research-field">随机种子<input type="number" min={0} max={Number.MAX_SAFE_INTEGER} value={value.seed} disabled={disabled} onChange={(event) => onChange({...value, seed: Number(event.target.value)})} /></label>
    </div>
    <p className="research-help">输出95%描述区间；不足20个有效日不作推断。统计校正不能消除反复查看历史带来的偏差。</p>
  </details>;
}
