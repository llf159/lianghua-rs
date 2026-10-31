export type DataTaskProgressSegment = {
  key: string
  label: string
  state: 'done' | 'active' | 'pending'
  progress?: number | null
}

type DataTaskProgressProps = {
  phaseLabel: string
  phaseStepPillText: string
  phaseStepStatText: string
  actionLabel: string
  progressPercent: number | null
  progressSegments?: DataTaskProgressSegment[] | null
  elapsedText: string
  estimatedRemainingText?: string
  estimatedTotalText?: string
  shownProgressPercent: number
  progressCounterText: string
  currentObjectText?: string
  message: string | null | undefined
  fallbackMessage: string
  onCancel?: () => void
  onRollback?: () => void
  cancelDisabled?: boolean
}

export default function DataTaskProgress({
  phaseLabel,
  phaseStepPillText,
  phaseStepStatText,
  actionLabel,
  progressPercent,
  progressSegments,
  elapsedText,
  estimatedRemainingText,
  estimatedTotalText,
  shownProgressPercent,
  progressCounterText,
  currentObjectText,
  message,
  fallbackMessage,
  onCancel,
  onRollback,
  cancelDisabled,
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
        <div className="data-download-progress-head-end">
          <div className="data-download-progress-value">
            <strong>{progressPercent === null ? '--' : `${Math.floor(shownProgressPercent * 10) / 10}%`}</strong>
            <span>{elapsedText}</span>
          </div>
        </div>
      </div>
      <div
        className="data-download-progress-bar"
        role="progressbar"
        aria-label={`${actionLabel} · ${phaseLabel}`}
        aria-valuemin={0}
        aria-valuemax={100}
        aria-valuenow={progressPercent ?? undefined}
      >
        <div
          className={`data-download-progress-bar-fill ${progressPercent === null ? 'is-indeterminate' : ''}`}
          style={{ width: `${Math.max(0, Math.min(100, shownProgressPercent))}%` }}
        />
        {progressSegments && progressSegments.length > 1 ? (
          <div
            className="data-download-progress-segments"
            style={{ gridTemplateColumns: `repeat(${progressSegments.length}, minmax(0, 1fr))` }}
          >
            {progressSegments.map((segment) => (
              <span
                key={segment.key}
                className={`data-download-progress-segment is-${segment.state}`}
                title={segment.label}
                aria-label={segment.label}
              >
                {segment.progress !== undefined && segment.progress !== null && segment.progress > 0 ? (
                  <span
                    className="data-download-progress-segment-fill"
                    style={{ width: `${Math.max(0, Math.min(100, segment.progress))}%` }}
                  />
                ) : null}
              </span>
            ))}
          </div>
        ) : null}
      </div>
      <div className="data-download-progress-stats">
        <div className="data-download-progress-stat">
          <span>阶段</span>
          <strong>
            {phaseLabel}
            {phaseStepStatText}
          </strong>
        </div>
        <div className="data-download-progress-stat data-download-progress-stat-mid">
          <span>进度</span>
          <strong>{progressCounterText}</strong>
        </div>
        {currentObjectText !== undefined ? (
          <div className="data-download-progress-stat data-download-progress-stat-wide">
            <span>当前对象</span>
            <strong title={currentObjectText}>{currentObjectText}</strong>
          </div>
        ) : null}
        {estimatedRemainingText !== undefined ? (
          <div className="data-download-progress-stat">
            <span>阶段预计剩余</span>
            <strong>{estimatedRemainingText}</strong>
          </div>
        ) : null}
        {estimatedTotalText !== undefined ? (
          <div className="data-download-progress-stat">
            <span>阶段预计总耗时</span>
            <strong>{estimatedTotalText}</strong>
          </div>
        ) : null}
      </div>
      <div className="data-download-progress-footer">
        <div className="data-download-progress-text">{message ?? fallbackMessage}</div>
        {onCancel || onRollback ? (
          <div className="data-download-progress-actions">
            {onCancel ? (
              <button
                type="button"
                className="data-download-progress-action-btn"
                disabled={cancelDisabled}
                title="停止任务并保留已提交结果"
                onClick={onCancel}
              >
                停止
              </button>
            ) : null}
            {onRollback ? (
              <button
                type="button"
                className="data-download-progress-action-btn is-danger"
                disabled={cancelDisabled}
                title="停止任务并撤回本次研究记录"
                onClick={onRollback}
              >
                撤回
              </button>
            ) : null}
          </div>
        ) : null}
      </div>
    </div>
  )
}
