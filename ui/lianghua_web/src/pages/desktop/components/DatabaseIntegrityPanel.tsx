import { useState } from 'react'
import type { DatabaseCheckReport, DatabaseIssue } from '../../../apis/dataDownload'

export default function DatabaseIntegrityPanel({ report, busy, checking, error, onCheck, onRepair }: {
  report: DatabaseCheckReport | null
  busy: boolean
  checking: boolean
  error: string
  onCheck: () => void
  onRepair: (issues: DatabaseIssue[]) => void
}) {
  const [visibleCounts, setVisibleCounts] = useState<Record<string, number>>({})
  const issues = report?.issues ?? []
  const statusLabels = {
    passed: '通过', warning: '差异', error: '异常', incomplete: '未完成', skipped: '未检查',
  }
  return (
    <section className="data-download-card database-integrity-panel" aria-labelledby="database-integrity-title">
      <div className="data-download-head">
        <h2 id="database-integrity-title">数据库检测报告</h2>
        <button className="data-download-secondary-btn" type="button" disabled={busy} onClick={() => { setVisibleCounts({}); onCheck() }}>
          {checking ? '检查中…' : report ? '重新检查' : '检查数据库'}
        </button>
      </div>
      {error ? <div className="data-download-error" role="alert">{error}</div> : null}
      {checking ? <p role="status">正在检查…</p> : null}
      {!report && !checking ? <p className="database-integrity-hint">尚未检查。</p> : null}
      {report ? <>
        <div className={`database-integrity-summary ${report.sourceReady ? 'is-ready' : 'is-blocked'}`} role="status">
          <strong>{report.sourceReady ? '原始行情检查通过' : '原始行情检查未通过，暂停后续重算'}</strong>
          <span>{report.checks.length} 个检测项目 · {report.stockCount.toLocaleString()} 只股票 · {report.rowCount.toLocaleString()} 行行情</span>
          <small>检查时间：{new Date(report.checkedAt).toLocaleString()} · 有效交易日：{report.targetTradeDate ?? '未知'}</small>
        </div>
        <div className="database-integrity-checks">
          {report.checks.map((check) => {
            const checkIssues = issues.filter((issue) => issue.checkId === check.id)
            const repairIssues = checkIssues.filter((issue) => issue.repairAction !== null)
            const requiresSource = repairIssues.some((issue) => issue.repairAction !== null && !['stock', 'market', 'index', 'update'].includes(issue.repairAction))
            const requiresRanking = repairIssues.some((issue) => issue.repairAction === 'similarity') && issues.some((issue) => issue.repairAction === 'ranking')
            const disabled = busy || !repairIssues.length || (requiresSource && !report.sourceReady) || requiresRanking
            const repairButton = <button
              type="button"
              className="data-download-secondary-btn database-integrity-repair"
              aria-label={`修复${check.title}`}
              disabled={disabled}
              onClick={() => onRepair(repairIssues)}
            >修复</button>
            const visibleCount = visibleCounts[check.id] ?? 50
            const heading = <>
              <span className="database-integrity-check-title">{check.title}</span>
              <span className="database-integrity-check-meta">{check.summary}{checkIssues.length ? ` · ${checkIssues.length} 项问题` : ''}</span>
              <span className={`database-integrity-check-status is-${check.status}`}>{statusLabels[check.status]}</span>
            </>
            if (!checkIssues.length) {
              return <div key={`${report.checkedAt}:${check.id}`} className="database-integrity-check-row">
                <div className="database-integrity-check-heading">{heading}</div>
                {repairButton}
              </div>
            }
            return <div key={`${report.checkedAt}:${check.id}`} className="database-integrity-check-row">
              <details className="database-integrity-check">
                <summary className="database-integrity-check-heading">{heading}</summary>
                <div className="database-integrity-issues">
                  {checkIssues.slice(0, visibleCount).map((issue) => <article key={issue.id} className="database-integrity-issue">
                    <div className="database-integrity-issue-content">
                      {issue.tsCode ? <div className="database-integrity-meta">{issue.stockName} <code>{issue.tsCode}</code></div> : null}
                      {issue.comparisons.map((comparison, index) => <div key={index} className="database-integrity-comparison">
                        <div className="database-integrity-comparison-label">{comparison.label}</div>
                        <dl>
                          <div><dt>预期</dt><dd>{comparison.expected}</dd></div>
                          <div><dt>实际</dt><dd>{comparison.actual}</dd></div>
                        </dl>
                      </div>)}
                    </div>
                  </article>)}
                  {checkIssues.length > visibleCount ? <button type="button" className="data-download-secondary-btn" onClick={() => setVisibleCounts((counts) => ({ ...counts, [check.id]: visibleCount + 50 }))}>
                    显示更多（剩余 {checkIssues.length - visibleCount} 项）
                  </button> : null}
                </div>
              </details>
              {repairButton}
            </div>
          })}
        </div>
      </> : null}
    </section>
  )
}
