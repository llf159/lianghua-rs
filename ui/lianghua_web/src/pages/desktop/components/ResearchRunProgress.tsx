import { useEffect, useRef, useState } from "react";
import DataTaskProgress, { type DataTaskProgressSegment } from "../../../shared/DataTaskProgress";
import { formatProgressDuration, useAnimatedProgressPercent, useProgressTiming } from "../../../shared/dataTaskProgressUtils";
import "../css/DataDownloadPage.css";

export function ResearchRunProgress({ actionLabel, mode, phase, cancelling, onCancel }: {
  actionLabel: string;
  mode: "expression" | "comparison";
  phase: string;
  cancelling: boolean;
  onCancel: (rollback: boolean) => void;
}) {
  const startedAt = useRef(0);
  const [sample, setSample] = useState<{ phase: string; finished: number; total: number; elapsedMs: number } | null>(null);
  const [activeStage, setActiveStage] = useState(0);
  const combination = /^(计算|统计|已完成)参数组合 (\d+)\/(\d+)$/.exec(phase);
  const batch = /^本批组合信号计算 (\d+)\/(\d+)$/.exec(phase);
  const scoring = /^(基线评分|候选评分|研究基线评分) (\d+)\/(\d+)$/.exec(phase);
  const total = Number((combination ?? batch ?? scoring)?.[3] ?? 0);
  const finished = combination
    ? Math.max(0, Number(combination[2]) - (combination[1] === "已完成" ? 0 : 1))
    : Number((batch ?? scoring)?.[2] ?? 0);
  const phaseKey = combination
    ? "参数组合"
    : batch?.[0] ?? scoring?.[1] ?? phase;
  const localRatio = total > 0 ? Math.max(0, Math.min(1, finished / total)) : null;
  let detectedStage: number | null = null;
  if (mode === "comparison") {
    if (/^(重算基线评分与跟踪|基线评分 \d+\/\d+)$/.test(phase)) detectedStage = 1;
    else if (/^(重算候选评分与跟踪|候选评分 \d+\/\d+)$/.test(phase)) detectedStage = 2;
    else if (/^(计算后验与排名对照|计算板块、市值和MKT切片|排名切片 )/.test(phase)) detectedStage = 3;
    else if (/^(保存排名成员与标签|复核输入文件|准备结果|正在保存)/.test(phase)) detectedStage = 4;
    else if (/^(等待仓库可用|校验输入文件|准备评分配置)/.test(phase)) detectedStage = 0;
  } else if (/^(计算参数组合|统计参数组合|已完成参数组合|本批组合信号计算|等待并行后验数据准备完成)/.test(phase)) {
    detectedStage = 2;
  } else if (/^(整理结果|汇总分组统计)/.test(phase)) {
    detectedStage = 3;
  } else if (/^(提交研究汇总与正负样本|正在保存)/.test(phase)) {
    detectedStage = 4;
  } else if (/^(研究基线评分|准备\d+日评价|准备后验与股票日证据|并行准备后验与股票日证据)/.test(phase)) {
    detectedStage = 1;
  } else if (/^(等待仓库可用|检查表达式和输入)/.test(phase)) {
    detectedStage = 0;
  }
  const progressSegments: DataTaskProgressSegment[] =
    (mode === "expression"
      ? ["检查输入", "准备评分", "策略计算", "汇总统计", "保存结果"]
      : ["准备数据", "基线评分", "候选评分", "对照分析", "保存结果"]
    ).map((label, index) => ({
      key: label,
      label,
      state: index < activeStage ? "done" : index === activeStage ? "active" : "pending",
      progress: index === activeStage && localRatio !== null ? localRatio * 100 : undefined,
    }));
  useEffect(() => {
    if (detectedStage !== null) setActiveStage(detectedStage);
  }, [detectedStage]);
  useEffect(() => {
    const frame = window.requestAnimationFrame(() => {
      const now = Date.now();
      if (!startedAt.current) startedAt.current = now;
      setSample({ phase: phaseKey, finished, total, elapsedMs: now - startedAt.current });
    });
    return () => window.cancelAnimationFrame(frame);
  }, [phaseKey, finished, total]);
  const timing = useProgressTiming(true, sample ? { action: "research", ...sample } : null);
  const percent = Math.min(99.9, (activeStage + (localRatio ?? 0)) / progressSegments.length * 100);
  const shownPercent = useAnimatedProgressPercent(true, percent);

  return <div className="research-run-progress" role="status">
    <DataTaskProgress
      actionLabel={actionLabel}
      phaseLabel={phaseKey || "准备运行"}
      phaseStepPillText=""
      phaseStepStatText=""
      progressPercent={percent}
      progressSegments={progressSegments}
      shownProgressPercent={shownPercent}
      elapsedText={`已用时 ${formatProgressDuration(timing.elapsedMs)}`}
      estimatedRemainingText={total > 1 && finished < total ? formatProgressDuration(timing.estimatedRemainingMs) : undefined}
      progressCounterText={total ? `${combination ? "当前评价周期" : "已完成股票"} ${finished} / ${total}` : "等待进度反馈"}
      message={phase || null}
      fallbackMessage="准备运行"
      onCancel={() => onCancel(false)}
      onRollback={() => onCancel(true)}
      cancelDisabled={cancelling || phase.startsWith("正在保存")}
    />
  </div>;
}
