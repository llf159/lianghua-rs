import { useEffect, useRef, useState } from 'react'
import {
  getStrategyTrackingPage,
  runStrategyTracking,
  type TrackingPage,
} from '../../apis/strategyTracking'
import type { StrategyManageRuleItem } from '../../apis/strategyManage'
import './css/StrategyTrackingPanel.css'

export default function StrategyTrackingPanel({
  sourcePath,
  rules,
}: {
  sourcePath: string
  rules: StrategyManageRuleItem[]
}) {
  const [page, setPage] = useState<TrackingPage | null>(null)
  const [busy, setBusy] = useState('加载中')
  const request = useRef(0)
  const [error, setError] = useState('')
  const [reference, setReference] = useState('')
  const [filter, setFilter] = useState('all')
  const [status, setStatus] = useState('all')
  const [limit, setLimit] = useState(50)
  const [search, setSearch] = useState('')

  useEffect(() => {
    const current = ++request.current
    getStrategyTrackingPage(sourcePath)
      .then((value) => {
        if (current === request.current) {
          setPage(value)
          setError('')
        }
      })
      .catch((cause) => {
        if (current === request.current) setError(String(cause))
      })
      .finally(() => {
        if (current === request.current) setBusy('')
      })
    return () => {
      ++request.current
    }
  }, [sourcePath, rules])

  async function perform(
    action: string,
    operation: () => Promise<TrackingPage>,
  ) {
    const current = ++request.current
    setBusy(action)
    setError('')
    try {
      const value = await operation()
      if (current === request.current) setPage(value)
    } catch (cause) {
      if (current === request.current) setError(String(cause))
    } finally {
      if (current === request.current) setBusy('')
    }
  }

  const definitions = new Map(page?.definitions.map((item) => [item.id, item]))
  const ruleNames = new Map(rules.flatMap((rule) => (rule.tracking ?? []).map((track) => [track.id, rule.name])))
  const currentFilter = filter === 'all' || definitions.has(filter) ? filter : 'all'
  const query = search.trim().toLocaleLowerCase()
  const rows = (page?.rows ?? []).filter(
    (row) =>
      (currentFilter === 'all' || row.track_id === currentFilter) &&
      (!query || `${row.ts_code} ${row.name}`.toLocaleLowerCase().includes(query)) &&
      (status === 'all' ||
        (status === 'signal' ? row.signal_today
          : status === 'stopped' ? !!row.stopped
            : status === 'holding' ? !row.stopped && !row.observing
              : !row.stopped && row.observing)),
  )

  return (
    <details className="strategy-manage-card strategy-tracking" aria-busy={!!busy}>
      <summary>
        后续跟踪结果{' '}
        <span>先触发，再逐日观察 · {page?.definitions.length ?? 0} 条</span>
      </summary>
      <p className="strategy-manage-note">
        在普通策略的编辑框中开启「后续跟踪」，随策略一起保存；排名计算会同时生成跟踪分和事件状态；此处也可单独回放，回放不会改写已保存的正式排名。
      </p>
      <div className="strategy-tracking-toolbar">
        <button
          type="button"
          disabled={!!busy}
          onClick={() =>
            void perform('刷新中', () => getStrategyTrackingPage(sourcePath))
          }
        >
          刷新
        </button>
        <label>
          参考日{' '}
          <input
            type="date"
            value={reference}
            disabled={!!busy}
            onChange={(event) => setReference(event.target.value)}
          />
        </label>
        <button
          type="button"
          disabled={!!busy || !page?.definitions.some((item) => item.enabled && (!item.source || page.sources.some((source) => source.id === item.source && source.enabled)))}
          onClick={() =>
            void perform('计算中，首次会回放历史', () =>
              runStrategyTracking(sourcePath, reference),
            )
          }
        >
          计算跟踪
        </button>
      </div>
      <p className="strategy-manage-note" role="status">
        {busy ||
          (page?.as_of
            ? `结果截至 ${page.as_of} · ${page.computed_symbols} 只股票 · ${page.resumed_symbols} 只续算${page.stale ? ' · 输入已变化，需重新计算' : ''}`
            : '尚未计算。参考日留空使用行情末日；保存配置后需重新计算。')}
      </p>
      {error && (
        <p className="strategy-tracking-error" role="alert">
          {error}
        </p>
      )}
      {!!page?.warnings.length && (
        <details>
          <summary>数据提示（{page.warnings.length}）</summary>
          {page.warnings.map((warning) => (
            <p key={warning}>{warning}</p>
          ))}
        </details>
      )}

      <div className="strategy-tracking-toolbar">
        <label>
          策略
          <select
            value={currentFilter}
            onChange={(event) => {
              setFilter(event.target.value)
              setLimit(50)
            }}
          >
            <option value="all">全部</option>
            {page?.definitions.map((item) => (
              <option key={item.id} value={item.id}>
                {ruleNames.get(item.id) ?? item.name}{item.enabled ? '' : '（已停用）'}
              </option>
            ))}
          </select>
        </label>
        <label>
          状态
          <select
            value={status}
            onChange={(event) => {
              setStatus(event.target.value)
              setLimit(50)
            }}
          >
            <option value="all">全部</option>
            <option value="signal">当日信号</option>
            <option value="active">观察期内</option>
            <option value="holding">期满保分中</option>
            <option value="stopped">已停止</option>
          </select>
        </label>
        <label className="strategy-tracking-search">
          股票
          <input
            type="search"
            placeholder="代码或名称"
            value={search}
            onChange={(event) => {
              setSearch(event.target.value)
              setLimit(50)
            }}
          />
        </label>
        <span>
          {rows.length} 个起点
          {page?.stale && page.as_of ? '（旧结果，仅供核对）' : ''}
        </span>
      </div>
      <div className="strategy-tracking-results">
        {rows.slice(0, limit).map((row) => {
          const definition = definitions.get(row.track_id)
          const label = row.reason === 'invalidated' ? '已失效'
            : row.reason === 'score_ended' ? '计分已结束'
              : row.stopped ? '数据不足，已停止'
                : row.signal_today ? '当日信号'
                  : !row.observing ? '期满保分中'
                    : row.age === 0 ? '新起点'
                      : !row.eligible_today ? '当日不满足资格'
                        : row.completed.length === (definition?.steps?.length ?? 0) ? '观察完成，等待信号'
                          : '观察中'
          return (
          <article key={`${row.track_id}/${row.ts_code}`}>
            <div>
              <strong>
                {row.ts_code} {row.name}
              </strong>
              <span className={`strategy-tracking-status${row.stopped ? ' is-stopped' : row.signal_today ? ' is-signal' : ''}`}>
                {label}
              </span>
            </div>
            <p>
              {ruleNames.get(row.track_id) ?? definition?.name}{' '}
              · 起点 {row.started} · 第 {row.age} 日 · 跟踪分 {row.score}
              {row.source_score !== null ? ` · 原分值 ${row.source_score}` : ''}
            </p>
            {!!row.completed.length && (
              <p>
                已完成：
                {row.completed
                  .map(
                    (date, i) =>
                      `${(!page?.stale && definition?.steps?.[i]?.name) || `步骤${i + 1}`} ${date}`,
                  )
                  .join(' → ')}
              </p>
            )}
            {row.stopped && (
              <p>
                {row.stopped}{' '}
                {row.reason === 'missing_quote'
                  ? '缺少行情'
                  : row.reason === 'missing_evidence'
                    ? '缺少必要指标'
                    : row.reason === 'score_ended'
                      ? '命中计分结束条件'
                      : '命中失效条件'}
              </p>
            )}
            {row.first_signal && <p>首次信号：{row.first_signal}</p>}
            {row.stopped && row.observing && <p>原观察期内仍保留起点，期满后才可重新发现。</p>}
            {!row.stopped && !row.observing && <p>不再推进步骤或产生新信号；保留已得分，等待结束或失效。</p>}
            {!!Object.keys(row.captured).length && (
              <details className="strategy-tracking-capture-details">
                <summary>起点冻结值（{Object.keys(row.captured).length}）</summary>
                <p className="strategy-tracking-captures">
                {Object.entries(row.captured)
                  .map(
                    ([name, value]) =>
                      `${name} = ${Number(value.toPrecision(7))}`,
                  )
                  .join(' · ')}
                </p>
              </details>
            )}
          </article>
          )
        })}
      </div>
      {rows.length > limit && (
        <button type="button" onClick={() => setLimit(limit + 50)}>
          再显示 50 个
        </button>
      )}
      {!rows.length && page?.as_of && (
        <p className="strategy-manage-note">当前筛选没有跟踪起点。</p>
      )}
    </details>
  )
}
