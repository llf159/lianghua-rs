import { useEffect, useState } from "react";
import { listRankComparisonMembers, type RankComparisonMember, type RankComparisonSummary } from "../../../apis/strategyTrigger";
import { compactDateToInput, formatBacktestValue, formatDateLabel, formatNumber, normalizeDateInput } from "../../../shared/backtestFormat";
import DetailsLink from "../../../shared/DetailsLink";

export function RankComparisonSamples({ sourcePath, runId, startDate, endDate, summary, onClose }: {
  sourcePath: string;
  runId: string;
  startDate: string;
  endDate: string;
  summary: RankComparisonSummary;
  onClose: () => void;
}) {
  const match = /^(\d{4}) Q([1-4])$/.exec(summary.period);
  const start = match ? `${match[1]}${["0101", "0401", "0701", "1001"][Number(match[2]) - 1]}` : startDate;
  const end = match ? `${match[1]}${["0331", "0630", "0930", "1231"][Number(match[2]) - 1]}` : endDate;
  const [from, setFrom] = useState(compactDateToInput(start > startDate ? start : startDate));
  const [to, setTo] = useState(compactDateToInput(end < endDate ? end : endDate));
  const [membership, setMembership] = useState("all");
  const [offset, setOffset] = useState(0);
  const [page, setPage] = useState<{key: string; total: number; items: RankComparisonMember[]; error: string}>({key: "", total: 0, items: [], error: ""});
  const pageSize = 50;
  const first = normalizeDateInput(from);
  const last = normalizeDateInput(to);
  const dateError = !first || !last || first > last ? "请选择有效日期区间。" : "";
  const pageKey = JSON.stringify([sourcePath,runId,first,last,summary.pool_kind,summary.top_k,summary.dimension,summary.bucket,membership,offset]);
  const loading = !dateError && page.key !== pageKey;
  const items = !dateError && page.key === pageKey ? page.items : [];
  const total = !dateError && page.key === pageKey ? page.total : 0;
  const error = dateError || (page.key === pageKey ? page.error : "");

  useEffect(() => {
    if (dateError) return;
    let cancelled = false;
    void listRankComparisonMembers({ sourcePath, runId, startDate: first, endDate: last,
      dimension: summary.dimension === "all" ? undefined : summary.dimension, bucket: summary.dimension === "all" ? undefined : summary.bucket,
      poolKind: summary.pool_kind || "daily", topK: summary.top_k, membership, offset, limit: pageSize,
    }).then((response) => {
      if (!cancelled) setPage({key: pageKey, total: response.total, items: response.items, error: ""});
    }).catch((failure) => {
      if (!cancelled) setPage({key: pageKey, total: 0, items: [], error: `读取成员失败：${String(failure)}`});
    });
    return () => { cancelled = true; };
  }, [sourcePath, runId, first, last, dateError, pageKey, summary.pool_kind, summary.top_k, summary.dimension, summary.bucket, membership, offset]);

  return <div className="scene-layer-layer-summary">
    <h3 title={runId}>排名成员</h3>
    <p className="scene-layer-caption">评分与后验来自存档；详情图使用当前行情。</p>
    <div className="research-toolbar">
      <label className="research-field">开始 <input type="date" value={from} onChange={(event) => { setFrom(event.target.value); setOffset(0); }} /></label>
      <label className="research-field">结束 <input type="date" value={to} onChange={(event) => { setTo(event.target.value); setOffset(0); }} /></label>
      <label className="research-field">成员 <select value={membership} onChange={(event) => { setMembership(event.target.value); setOffset(0); }}>
        <option value="all">全部</option><option value="added">新增</option><option value="removed">移除</option><option value="shared">共同</option><option value="missing_label">缺标签</option>
      </select></label>
      <button type="button" onClick={onClose}>关闭成员</button>
    </div>
    {error ? <p role="alert">{error}</p> : null}
    <div className="scene-layer-contrib-table-wrap"><table className="scene-layer-contrib-table">
      <thead><tr><th>信号日</th><th>股票</th><th>变化</th><th>基线分 / 名次</th><th>候选分 / 名次</th><th>入场 / 标签末日</th><th>后验残差</th><th>缺失原因</th></tr></thead>
      <tbody>{items.map((item) => <tr key={`${item.trade_date}-${item.ts_code}`}>
        <td>{formatDateLabel(item.trade_date)}</td><td><DetailsLink tsCode={item.ts_code} tradeDate={item.trade_date} sourcePath={sourcePath}>{item.ts_code}</DetailsLink></td>
        <td>{item.membership === "added" ? "新增" : item.membership === "removed" ? "移除" : "共同"}</td>
        <td>{formatNumber(item.baseline_score)} / {item.baseline_rank ?? "—"}</td><td>{formatNumber(item.candidate_score)} / {item.candidate_rank ?? "—"}</td>
        <td>{item.entry_date ?? "—"} / {item.label_end_date ?? "—"}</td><td>{formatBacktestValue(item.residual_return, "residual_return")}</td><td>{item.missing_reason ?? "—"}</td>
      </tr>)}</tbody>
    </table></div>
    <div className="research-toolbar">
      <button type="button" disabled={loading || offset === 0} onClick={() => setOffset(Math.max(0, offset - pageSize))}>上一页</button>
      <span>{loading ? "读取中…" : `${total ? offset + 1 : 0}–${Math.min(offset + pageSize, total)} / ${total}`}</span>
      <button type="button" disabled={loading || offset + pageSize >= total} onClick={() => setOffset(offset + pageSize)}>下一页</button>
    </div>
  </div>;
}
