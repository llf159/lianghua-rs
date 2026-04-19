type DataTaskProgressProps = {
  phaseLabel: string
  phaseStepPillText: string
  phaseStepStatText: string
  actionLabel: string
  progressPercent: number | null
  elapsedText: string
  shownProgressPercent: number
  progressCounterText: string
  currentObjectText: string
  message: string | null | undefined
  fallbackMessage: string
}

export default function DataTaskProgress({
  phaseLabel,
  phaseStepPillText,
  phaseStepStatText,
  actionLabel,
  progressPercent,
  elapsedText,
  shownProgressPercent,
  progressCounterText,
  currentObjectText,
  message,
  fallbackMessage,
}: DataTaskProgressProps) {
  return (
    <div className="data-download-progress">
      <div className="data-download-progress-head">
        <div className="data-download-progress-title">
          <span className="data-download-progress-phase-pill">
            {phaseLabel}
            {phaseStepPillText}
          </span>
          <strong>{actionLabel}</strong>
        </div>
        <div className="data-download-progress-value">
          <strong>{progressPercent === null ? '--' : `${progressPercent}%`}</strong>
          <span>{elapsedText}</span>
        </div>
      </div>
      <div className="data-download-progress-bar">
        <div
          className={`data-download-progress-bar-fill ${progressPercent === null ? 'is-indeterminate' : ''}`}
          style={{ width: `${Math.max(shownProgressPercent, 10)}%` }}
        />
      </div>
      <div className="data-download-progress-stats">
        <div className="data-download-progress-stat">
          <span>阶段</span>
          <strong>
            {phaseLabel}
            {phaseStepStatText}
          </strong>
        </div>
        <div className="data-download-progress-stat">
          <span>进度</span>
          <strong>{progressCounterText}</strong>
        </div>
        <div className="data-download-progress-stat data-download-progress-stat-wide">
          <span>当前对象</span>
          <strong>{currentObjectText}</strong>
        </div>
      </div>
      <div className="data-download-progress-text">{message ?? fallbackMessage}</div>
    </div>
  )
}