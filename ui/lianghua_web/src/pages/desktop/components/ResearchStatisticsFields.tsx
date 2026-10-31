import { useEffect, useState } from "react";
import type { ResearchStatisticsSettings } from "../../../apis/strategyTrigger";
export function ResearchStatisticsFields({value, onChange, disabled}: {value: ResearchStatisticsSettings; onChange: (value: ResearchStatisticsSettings) => void; disabled?: boolean}) {
  const [blockLengthText, setBlockLengthText] = useState(String(value.block_length));
  const [resamplesText, setResamplesText] = useState(String(value.resamples));
  const [seedText, setSeedText] = useState(String(value.seed));
  useEffect(() => {
    setBlockLengthText(String(value.block_length));
    setResamplesText(String(value.resamples));
    setSeedText(String(value.seed));
  }, [value.block_length, value.resamples, value.seed]);
  return <fieldset className="research-parameter-group"><legend>统计设置</legend>
    <div className="research-form-grid">
    <label className="research-field">日期块长<input type="number" min={1} max={252} value={blockLengthText} disabled={disabled} onChange={(event) => {
      const text = event.target.value;
      setBlockLengthText(text);
      if (text.trim() && Number.isFinite(Number(text))) onChange({...value, block_length: Number(text)});
    }} onBlur={() => {if (!blockLengthText.trim() || !Number.isFinite(Number(blockLengthText))) setBlockLengthText(String(value.block_length));}} /></label>
    <label className="research-field">重采样次数<input type="number" min={100} max={5000} value={resamplesText} disabled={disabled} onChange={(event) => {
      const text = event.target.value;
      setResamplesText(text);
      if (text.trim() && Number.isFinite(Number(text))) onChange({...value, resamples: Number(text)});
    }} onBlur={() => {if (!resamplesText.trim() || !Number.isFinite(Number(resamplesText))) setResamplesText(String(value.resamples));}} /></label>
    <label className="research-field">随机种子<input type="number" min={0} max={Number.MAX_SAFE_INTEGER} value={seedText} disabled={disabled} onChange={(event) => {
      const text = event.target.value;
      setSeedText(text);
      if (text.trim() && Number.isFinite(Number(text))) onChange({...value, seed: Number(text)});
    }} onBlur={() => {if (!seedText.trim() || !Number.isFinite(Number(seedText))) setSeedText(String(value.seed));}} /></label>
    </div>
  </fieldset>;
}
