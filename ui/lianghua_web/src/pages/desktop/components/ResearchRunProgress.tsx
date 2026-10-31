import { useEffect, useRef, useState } from "react";
import DataTaskProgress from "../../../shared/DataTaskProgress";
import { formatProgressDuration, useAnimatedProgressPercent, useProgressTiming } from "../../../shared/dataTaskProgressUtils";
import "../css/DataDownloadPage.css";

export function ResearchRunProgress({ actionLabel, phase, cancelling, onCancel }: {
  actionLabel: string;
  phase: string;
  cancelling: boolean;
  onCancel: (rollback: boolean) => void;
}) {
  const startedAt = useRef(0);
  const [sample, setSample] = useState<{ phase: string; finished: number; total: number; elapsedMs: number } | null>(null);
  const match = /^(计算|统计|已完成)参数组合 (\d+)\/(\d+)$/.exec(phase);
  const scoring = /^(基线评分|候选评分|研究基线评分|本批组合信号计算) (\d+)\/(\d+)$/.exec(phase);
  const total = Number((match ?? scoring)?.[3] ?? 0);
  const finished = match ? Math.max(0, Number(match[2]) - (match[1] === "已完成" ? 0 : 1)) : Number(scoring?.[2] ?? 0);
  const phaseKey = match ? "参数组合" : scoring?.[1] ?? phase;
  useEffect(() => {
    const frame = window.requestAnimationFrame(() => {
      const now = Date.now();
      if (!startedAt.current) startedAt.current = now;
      setSample({ phase: phaseKey, finished, total, elapsedMs: now - startedAt.current });
    });
    return () => window.cancelAnimationFrame(frame);
  }, [phaseKey, finished, total]);
  const timing = useProgressTiming(true, sample ? { action: "research", ...sample } : null);
  const percent = total > 0 ? Math.min(100, finished / total * 100) : null;
  const shownPercent = useAnimatedProgressPercent(true, percent);

  return <div className="research-run-progress" role="status">
    <DataTaskProgress
      actionLabel={actionLabel}
      phaseLabel={phaseKey || "准备运行"}
      phaseStepPillText=""
      phaseStepStatText=""
      progressPercent={percent}
      shownProgressPercent={percent === null ? 20 : shownPercent}
      elapsedText={`已用时 ${formatProgressDuration(timing.elapsedMs)}`}
      estimatedRemainingText={total > 1 && finished < total ? formatProgressDuration(timing.estimatedRemainingMs) : undefined}
      progressCounterText={total ? `${match ? "当前评价周期" : "已完成股票"} ${finished} / ${total}` : "等待进度反馈"}
      message={phase || null}
      fallbackMessage="准备运行"
      onCancel={() => onCancel(false)}
      onRollback={() => onCancel(true)}
      cancelDisabled={cancelling || phase.startsWith("正在保存")}
    />
  </div>;
}
