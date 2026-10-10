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
  const [filter, setFilter] = useState('')
  const [visibleCount, setVisibleCount] = useState(50)
  const issues = report?.issues ?? []
  const filtered = issues.filter((issue) => `${issue.tsCode ?? ''} ${issue.stockName ?? ''} ${issue.database} ${issue.title}`.toLowerCase().includes(filter.trim().toLowerCase()))
  const stockIssues = issues.filter((issue) => issue.repairAction === 'stock')
  const labels: Partial<Record<NonNullable<DatabaseIssue['repairAction']>, string>> = {
    stock: '重拉该股票完整历史', market: '重拉全市场完整历史', index: '修复该指数',
    concept: '重建概念／行业表现', chip: '修复／更新新筹码', 'legacy-chip': '修复／更新旧筹码',
    ranking: '重算评分／排名', similarity: '重算最新相似度', update: '执行增量更新',
  }
  return (
    <section className="data-download-card database-integrity-panel" aria-labelledby="database-integrity-title">
      <div className="data-download-head">
        <div>
          <h2 id="database-integrity-title">数据库完整性检查与修复</h2>
          <p>先检查当前仓库，再按股票或数据库修复。原始行情问题解决后，才能重算后续结果。</p>
        </div>
        <button className="data-download-secondary-btn" type="button" disabled={busy} onClick={onCheck}>
          {checking ? '只读检查中…' : report ? '重新检查' : '检查数据库'}
        </button>
      </div>
      {error ? <div className="data-download-error" role="alert">{error}</div> : null}
      {checking ? <p role="status">正在扫描行情与后续库，请等待；检查不会下载或改写行情。</p> : null}
      {!report && !checking ? <p className="database-integrity-hint">尚未检查。日期到最新并不能证明历史完整或复权正确。</p> : null}
      {report ? <>
        <div className={`database-integrity-summary ${report.sourceReady ? 'is-ready' : 'is-blocked'}`} role="status">
          <strong>{report.sourceReady ? '已保存行情通过本次本地检查' : '原始行情检查未通过，暂停后续重算'}</strong>
          <span>{report.stockCount.toLocaleString()} 只股票 · {report.rowCount.toLocaleString()} 行 · {issues.length} 项需处理或更新</span>
          <small>检查时间：{new Date(report.checkedAt).toLocaleString()}；当前有效交易日：{report.targetTradeDate ?? '无法判断'}</small>
        </div>
        {stockIssues.length ? <div className="database-integrity-actions">
          <button type="button" className="data-download-primary-btn" disabled={busy} onClick={() => onRepair(stockIssues)}>
            修复问题股票（{new Set(stockIssues.map((issue) => issue.tsCode)).size} 只）
          </button>
          <span>按股票整段重拉并重新计算指标，旧结果保留待重算记录；修复后自动复查。</span>
        </div> : null}
        {issues.length ? <>
          <label className="data-download-field database-integrity-filter">
            <span>筛选问题</span>
            <input value={filter} placeholder="股票代码、名称、数据库或问题类型" onChange={(event) => { setFilter(event.target.value); setVisibleCount(50) }} />
          </label>
          <div className="database-integrity-issues">
            {filtered.slice(0, visibleCount).map((issue) => {
              const requiresSource = issue.repairAction !== null && !['stock', 'market', 'index', 'update'].includes(issue.repairAction)
              const requiresRanking = issue.repairAction === 'similarity' && issues.some((item) => item.repairAction === 'ranking')
              const disabled = busy || (requiresSource && !report.sourceReady) || requiresRanking
              return <article key={issue.id} className={`database-integrity-issue is-${issue.severity}`}>
                <div>
                  <strong>{issue.title}{issue.tsCode ? ` · ${issue.stockName ?? ''} ${issue.tsCode}` : ''}</strong>
                  <small>{issue.database}{issue.startDate && issue.endDate ? ` · ${issue.startDate} 至 ${issue.endDate}` : ''}</small>
                  <p>{issue.detail}</p>
                </div>
                {issue.repairAction ? <button type="button" className="data-download-secondary-btn" disabled={disabled} title={requiresSource && !report.sourceReady ? '先修复原始行情并重新检查' : requiresRanking ? '先重算评分／排名' : undefined} onClick={() => onRepair([issue])}>
                  {labels[issue.repairAction] ?? '修复'}
                </button> : <span className="database-integrity-hint">按说明处理后重查</span>}
              </article>
            })}
          </div>
          {filtered.length > visibleCount ? <button type="button" className="data-download-secondary-btn" onClick={() => setVisibleCount((count) => count + 50)}>显示更多（剩余 {filtered.length - visibleCount} 项）</button> : null}
          {filtered.length === 0 ? <p>没有匹配的问题。</p> : null}
        </> : <p>本次检查范围内未发现待处理问题。下方列出了检查范围及未核验项。</p>}
        <details className="database-integrity-scope">
          <summary>检查范围与未核验项</summary>
          <ul>{report.checks.map((item, index) => <li key={index}>{item}</li>)}</ul>
          {report.limitations.map((item) => <p key={item}>{item}</p>)}
        </details>
      </> : null}
    </section>
  )
}
