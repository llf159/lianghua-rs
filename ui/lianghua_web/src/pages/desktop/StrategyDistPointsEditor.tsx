import { useId } from 'react'
import {
  validateDistPointRows,
  type DistPointRow,
} from '../../shared/strategyDistPoints'
import './css/StrategyDistPointsEditor.css'

export default function StrategyDistPointsEditor({
  rows,
  onChange,
  scopeWay,
  windowText,
  showHelp = true,
}: {
  rows: DistPointRow[]
  onChange: (rows: DistPointRow[]) => void
  scopeWay: string
  windowText: string
  showHelp?: boolean
}) {
  const id = useId()
  const { points, errors } = validateDistPointRows(rows)
  const valid = Object.keys(errors).length === 0
  const window = Number(windowText)
  const validWindow = Number.isSafeInteger(window) && window > 0
  const gaps: Array<{ min: number; max: number }> = []
  if (valid && validWindow) {
    let start = 0
    for (const point of points) {
      if (point.min >= window) break
      if (point.min > start) gaps.push({ min: start, max: point.min - 1 })
      start = Math.max(start, point.max + 1)
    }
    if (start < window) gaps.push({ min: start, max: window - 1 })
  }

  function updateRow(index: number, field: keyof DistPointRow, value: string) {
    onChange(
      rows.map((row, rowIndex) =>
        rowIndex === index ? { ...row, [field]: value } : row,
      ),
    )
  }

  return (
    <div className="strategy-dist-editor strategy-manage-field-span-full">
      <div className="strategy-dist-editor-heading">
        <strong>区间字典（距今报价日）</strong>
        <span>{rows.length} 个区间</span>
      </div>
      <p className="strategy-manage-note" id={`${id}-help`}>
        0 是当天，区间包含两端。
      </p>
      <div
        className="strategy-dist-editor-rows"
        aria-describedby={`${id}-help`}
      >
        {rows.map((row, index) => (
          <div
            className={`strategy-dist-editor-row${errors[index] ? ' is-invalid' : ''}`}
            key={index}
          >
            <span className="strategy-dist-editor-index">{index + 1}</span>
            <label className="strategy-manage-field">
              <span>起始日（含）</span>
              <input
                aria-label={`区间 ${index + 1} 起始日`}
                aria-invalid={Boolean(errors[index])}
                aria-describedby={
                  errors[index] ? `${id}-error-${index}` : undefined
                }
                type="number"
                min={0}
                step={1}
                value={row.min}
                onChange={(event) =>
                  updateRow(index, 'min', event.target.value)
                }
              />
            </label>
            <label className="strategy-manage-field">
              <span>结束日（含）</span>
              <input
                aria-label={`区间 ${index + 1} 结束日`}
                aria-invalid={Boolean(errors[index])}
                aria-describedby={
                  errors[index] ? `${id}-error-${index}` : undefined
                }
                type="number"
                min={0}
                step={1}
                value={row.max}
                onChange={(event) =>
                  updateRow(index, 'max', event.target.value)
                }
              />
            </label>
            <label className="strategy-manage-field">
              <span>分值</span>
              <input
                aria-label={`区间 ${index + 1} 分值`}
                aria-invalid={Boolean(errors[index])}
                aria-describedby={
                  errors[index] ? `${id}-error-${index}` : undefined
                }
                type="number"
                step="any"
                placeholder="可填负数、小数"
                value={row.points}
                onChange={(event) =>
                  updateRow(index, 'points', event.target.value)
                }
              />
            </label>
            <button
              className="strategy-manage-inline-btn is-danger"
              type="button"
              aria-label={`删除区间 ${index + 1}`}
              onClick={() =>
                onChange(rows.filter((_, rowIndex) => rowIndex !== index))
              }
            >
              删除
            </button>
            {errors[index] && (
              <p
                className="strategy-dist-editor-error"
                id={`${id}-error-${index}`}
              >
                {errors[index]}
              </p>
            )}
          </div>
        ))}
      </div>
      <div className="strategy-dist-editor-actions">
        <button
          type="button"
          className="strategy-manage-inline-btn"
          onClick={() => {
            const start = rows
              .filter(
                (row) =>
                  row.max.trim() &&
                  Number.isSafeInteger(Number(row.max)) &&
                  Number(row.max) >= 0,
              )
              .reduce((next, row) => Math.max(next, Number(row.max) + 1), 0)
            onChange([
              ...rows,
              { min: String(start), max: String(start), points: '' },
            ])
          }}
        >
          + 添加区间
        </button>
        <button
          type="button"
          className="strategy-manage-inline-btn"
          disabled={!valid || rows.length < 2}
          onClick={() =>
            onChange(
              [...rows].sort(
                (left, right) => Number(left.min) - Number(right.min),
              ),
            )
          }
        >
          按起始日排序
        </button>
        {gaps.length > 0 && (
          <button
            type="button"
            className="strategy-manage-inline-btn"
            onClick={() =>
              onChange(
                [
                  ...rows,
                  ...gaps.map((gap) => ({
                    min: String(gap.min),
                    max: String(gap.max),
                    points: '0',
                  })),
                ].sort((left, right) => Number(left.min) - Number(right.min)),
              )
            }
          >
            空档补为 0 分
          </button>
        )}
      </div>
      <div className="strategy-dist-editor-feedback" aria-live="polite">
        {errors[-1] && (
          <p className="strategy-dist-editor-error">{errors[-1]}</p>
        )}
        {valid && validWindow && (
          <>
            <p>
              窗口 0～{window - 1} 日 ·
              {gaps.length === 0
                ? '已完整覆盖'
                : `空档 ${gaps.map((gap) => (gap.min === gap.max ? gap.min : `${gap.min}～${gap.max}`)).join('、')} 日计 0 分。`}
            </p>
            {points.some((point) => point.max >= window) && (
              <p className="strategy-dist-editor-warning">
                超出窗口的部分不计分，配置保留。
              </p>
            )}
          </>
        )}
        {!validWindow && (
          <p className="strategy-dist-editor-warning">
            请先填写有效的回看窗口。
          </p>
        )}
      </div>
      {showHelp ? <details className="strategy-rule-editor-help">
        <summary>计分说明</summary>
        <p>
          {scopeWay.toUpperCase() === 'EACH'
            ? 'EACH：每次命中分别查表，再累计得分。'
            : 'RECENT：只按最近一次命中的距今报价日查表。'}
          未命中或未配置的报价日计 0 分。分值支持负数和小数。
        </p>
      </details> : null}
    </div>
  )
}
