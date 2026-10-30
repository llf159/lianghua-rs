export type ResearchNotes = {
  hypothesis: string;
  scope: string;
  explorationStart: string;
  explorationEnd: string;
  verificationStart: string;
  verificationEnd: string;
  historyReviewed: boolean;
  sortingEvidence: string;
  returnEvidence: string;
  conclusion: string;
};

export function ResearchNotesFields({ value, onChange }: {
  value: ResearchNotes;
  onChange: (value: ResearchNotes) => void;
}) {
  return <details className="research-disclosure">
    <summary>研究备注 · 可选</summary>
    <div className="research-form-grid">
      <label className="research-field">可检验假设 <input value={value.hypothesis} onChange={(event) => onChange({ ...value, hypothesis: event.target.value })} /></label>
      <label className="research-field">适用范围 <input value={value.scope} onChange={(event) => onChange({ ...value, scope: event.target.value })} /></label>
      <label className="research-field">探索期开始 <input type="date" value={value.explorationStart} onChange={(event) => onChange({ ...value, explorationStart: event.target.value })} /></label>
      <label className="research-field">探索期结束 <input type="date" value={value.explorationEnd} onChange={(event) => onChange({ ...value, explorationEnd: event.target.value })} /></label>
      <label className="research-field">检验期开始 <input type="date" value={value.verificationStart} onChange={(event) => onChange({ ...value, verificationStart: event.target.value })} /></label>
      <label className="research-field">检验期结束 <input type="date" value={value.verificationEnd} onChange={(event) => onChange({ ...value, verificationEnd: event.target.value })} /></label>
      <label className="research-checkbox"><input type="checkbox" checked={value.historyReviewed} onChange={(event) => onChange({ ...value, historyReviewed: event.target.checked })} />这段历史已反复查看</label>
      <label className="research-field">排序价值 <select value={value.sortingEvidence} onChange={(event) => onChange({ ...value, sortingEvidence: event.target.value })}>
        <option value="">未填写</option><option value="support">支持</option><option value="local">局部支持</option><option value="insufficient">证据不足</option><option value="unsupported">不支持</option>
      </select></label>
      <label className="research-field">收益价值 <select value={value.returnEvidence} onChange={(event) => onChange({ ...value, returnEvidence: event.target.value })}>
        <option value="">未填写</option><option value="support">支持</option><option value="local">局部支持</option><option value="insufficient">证据不足</option><option value="unsupported">不支持</option>
      </select></label>
      <label className="research-field">结论与不足 <input value={value.conclusion} onChange={(event) => onChange({ ...value, conclusion: event.target.value })} /></label>
    </div>
  </details>;
}
