import { useEffect, useRef, useState } from "react";
import { formatProgressDuration, useProgressTiming } from "../../../shared/dataTaskProgressUtils";

export function ResearchRunProgress({ phase, cancelling, onCancel }: {
  phase: string;
  cancelling: boolean;
  onCancel: () => void;
}) {
  const startedAt = useRef(0);
  const [sample, setSample] = useState<{ phase: string; finished: number; total: number; elapsedMs: number } | null>(null);
  const match = /^(计算|统计|已完成)参数组合 (\d+)\/(\d+)$/.exec(phase);
  const total = match ? Number(match[3]) : 0;
  const finished = match ? Math.max(0, Number(match[2]) - (match[1] === "已完成" ? 0 : 1)) : 0;
  useEffect(() => {
    const frame = window.requestAnimationFrame(() => {
      const now = Date.now();
      if (!startedAt.current) startedAt.current = now;
      setSample({ phase: total ? "combos" : phase, finished, total, elapsedMs: now - startedAt.current });
    });
    return () => window.cancelAnimationFrame(frame);
  }, [phase, finished, total]);
  const timing = useProgressTiming(true, sample ? { action: "research", ...sample } : null);

  return <div className="research-run-progress" role="status">
    <div className="research-run-progress-head">
      <strong>{phase || "准备运行"}</strong>
      <button type="button" disabled={cancelling || phase.startsWith("正在保存")} onClick={onCancel}>取消</button>
    </div>
    <progress aria-label={total ? "当前评价周期参数组合进度" : "回测进行中"} max={total || 1} {...(total ? { value: finished } : {})} />
    <div className="research-run-progress-detail">
      <span>已用时 {formatProgressDuration(timing.elapsedMs)}</span>
      {total > 0 ? <span>当前评价周期 {finished} / {total}</span> : null}
      {total > 1 && finished < total ? <span>当前组合阶段预计剩余 {formatProgressDuration(timing.estimatedRemainingMs)}</span> : null}
    </div>
  </div>;
}
