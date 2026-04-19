import { useDeferredValue, useEffect, useMemo, useState, type WheelEvent as ReactWheelEvent } from 'react'
import {
  DEFAULT_MANAGED_SOURCE_DIR,
  inspectManagedSourceStatus,
  previewManagedSourceDataset,
  type ManagedSourceDatasetId,
  type ManagedSourceDatasetPreviewResult,
  type ManagedSourceFileId,
  type ManagedSourceStatus,
} from '../../apis/managedSource'
import { listStockLookupRows, type StockLookupRow } from '../../apis/reader'
import { buildStockLookupCandidates, findExactStockLookupMatch, getLookupDigits } from '../../shared/stockLookup'
import { sanitizeCodeInput, stdTsCode } from '../../shared/stockCode'
import './css/DataImportPage.css'
import './css/DetailsPage.css'
import './css/DataViewerPage.css'

const MAX_STOCK_NAME_CANDIDATES = 12

type DatasetOption = {
  id: ManagedSourceDatasetId
  label: string
  description: string
  requiredFileId: ManagedSourceFileId
  supportsTradeDate: boolean
  supportsTsCode: boolean
}

const DATASET_OPTIONS: DatasetOption[] = [
  {
    id: 'stock-data-base',
    label: '原始行情库',
    description: '查看 stock_data，基础列在前，指标列紧随其后。',
    requiredFileId: 'source-db',
    supportsTradeDate: true,
    supportsTsCode: true,
  },
  {
    id: 'score-summary',
    label: '结果库汇总',
    description: '查看 scoring_result.db 的 score_summary。',
    requiredFileId: 'result-db',
    supportsTradeDate: true,
    supportsTsCode: true,
  },
  {
    id: 'rule-details',
    label: '规则明细',
    description: '查看 scoring_result.db 的 rule_details。',
    requiredFileId: 'result-db',
    supportsTradeDate: true,
    supportsTsCode: true,
  },
  {
    id: 'scene-details',
    label: '场景明细',
    description: '查看 scoring_result.db 的 scene_details。',
    requiredFileId: 'result-db',
    supportsTradeDate: true,
    supportsTsCode: true,
  },
  {
    id: 'concept-performance',
    label: '概念表现库',
    description: '查看 concept_performance.db 的 concept_performance。',
    requiredFileId: 'concept-performance-db',
    supportsTradeDate: true,
    supportsTsCode: false,
  },
  {
    id: 'stock-list-csv',
    label: '股票列表 CSV',
    description: '查看 stock_list.csv，包含名称、市值、拼音首字母等基础信息。',
    requiredFileId: 'stock-list',
    supportsTradeDate: true,
    supportsTsCode: true,
  },
  {
    id: 'trade-calendar-csv',
    label: '交易日历 CSV',
    description: '查看 trade_calendar.csv。',
    requiredFileId: 'trade-calendar',
    supportsTradeDate: true,
    supportsTsCode: false,
  },
  {
    id: 'stock-concepts-csv',
    label: '同花顺概念 CSV',
    description: '查看 stock_concepts.csv。',
    requiredFileId: 'ths-concepts',
    supportsTradeDate: false,
    supportsTsCode: true,
  },
]

function getDatasetOption(datasetId: ManagedSourceDatasetId) {
  return DATASET_OPTIONS.find((item) => item.id === datasetId) ?? DATASET_OPTIONS[0]
}

export default function DataViewerPage() {
  const [status, setStatus] = useState<ManagedSourceStatus | null>(null)
  const [statusLoading, setStatusLoading] = useState(true)
  const [statusError, setStatusError] = useState('')
  const [preview, setPreview] = useState<ManagedSourceDatasetPreviewResult | null>(null)
  const [previewLoading, setPreviewLoading] = useState(false)
  const [previewError, setPreviewError] = useState('')
  const [datasetId, setDatasetId] = useState<ManagedSourceDatasetId>('stock-data-base')
  const [tradeDateInput, setTradeDateInput] = useState('')
  const [lookupInput, setLookupInput] = useState('')
  const [lookupFocused, setLookupFocused] = useState(false)
  const [limitInput, setLimitInput] = useState('100')
  const [stockLookupRows, setStockLookupRows] = useState<StockLookupRow[]>([])

  const sourceDir = status?.sourceDir ?? DEFAULT_MANAGED_SOURCE_DIR
  const sourcePath = status?.sourcePath ?? ''
  const selectedDataset = getDatasetOption(datasetId)
  const deferredLookupInput = useDeferredValue(lookupInput)
  const stockNameCandidates = useMemo(
    () => buildStockLookupCandidates(stockLookupRows, deferredLookupInput, MAX_STOCK_NAME_CANDIDATES),
    [deferredLookupInput, stockLookupRows],
  )
  const exactStockLookupMatch = useMemo(
    () => findExactStockLookupMatch(stockLookupRows, lookupInput),
    [lookupInput, stockLookupRows],
  )
  const inputCodeDigits = sanitizeCodeInput(lookupInput)
  const readTargetCode =
    inputCodeDigits.length === 6
      ? stdTsCode(inputCodeDigits)
      : exactStockLookupMatch
        ? stdTsCode(getLookupDigits(exactStockLookupMatch.ts_code))
        : ''
  const showStockNameCandidates =
    selectedDataset.supportsTsCode &&
    lookupFocused &&
    lookupInput.trim() !== '' &&
    stockNameCandidates.length > 0
  const requiredFileImported =
    status?.items.some((item) => item.id === selectedDataset.requiredFileId && item.isImported) ?? false

  useEffect(() => {
    let cancelled = false

    const loadStatus = async () => {
      setStatusLoading(true)
      setStatusError('')
      try {
        const nextStatus = await inspectManagedSourceStatus()
        if (!cancelled) {
          setStatus(nextStatus)
        }
      } catch (error) {
        if (!cancelled) {
          setStatusError(`读取数据目录状态失败: ${String(error)}`)
        }
      } finally {
        if (!cancelled) {
          setStatusLoading(false)
        }
      }
    }

    void loadStatus()
    return () => {
      cancelled = true
    }
  }, [])

  useEffect(() => {
    if (!sourcePath.trim()) {
      setStockLookupRows([])
      return
    }

    let cancelled = false
    const loadStockLookupRows = async () => {
      try {
        const rows = await listStockLookupRows(sourcePath)
        if (!cancelled) {
          setStockLookupRows(rows)
        }
      } catch {
        if (!cancelled) {
          setStockLookupRows([])
        }
      }
    }

    void loadStockLookupRows()
    return () => {
      cancelled = true
    }
  }, [sourcePath])

  useEffect(() => {
    setPreview(null)
    setPreviewError('')
  }, [datasetId])

  async function onRefreshStatus() {
    setStatusLoading(true)
    setStatusError('')
    try {
      const nextStatus = await inspectManagedSourceStatus(sourceDir)
      setStatus(nextStatus)
    } catch (error) {
      setStatusError(`读取数据目录状态失败: ${String(error)}`)
    } finally {
      setStatusLoading(false)
    }
  }

  function onLookupInputChange(rawValue: string) {
    setLookupFocused(true)
    setLookupInput(rawValue)
  }

  function onSelectStockCandidate(row: StockLookupRow) {
    setLookupInput(row.name)
    setLookupFocused(false)
  }

  function onCandidateWheel(event: ReactWheelEvent<HTMLDivElement>) {
    const element = event.currentTarget
    const scrollTop = element.scrollTop
    const maxScrollTop = Math.max(element.scrollHeight - element.clientHeight, 0)
    const isAtTop = scrollTop <= 0
    const isAtBottom = scrollTop >= maxScrollTop - 1

    event.stopPropagation()
    if ((event.deltaY < 0 && isAtTop) || (event.deltaY > 0 && isAtBottom)) {
      event.preventDefault()
    }
  }

  async function onQueryPreview() {
    if (!requiredFileImported) {
      setPreview(null)
      setPreviewError(`当前数据目录缺少 ${selectedDataset.label} 所需文件，请先到数据管理页导入。`)
      return
    }

    if (selectedDataset.supportsTsCode && lookupInput.trim() !== '' && readTargetCode === '') {
      setPreview(null)
      setPreviewError('股票筛选请输入完整代码，或从候选列表中选择。')
      return
    }

    const parsedLimit = Number(limitInput.trim())
    if (!Number.isInteger(parsedLimit) || parsedLimit <= 0) {
      setPreview(null)
      setPreviewError('显示行数必须是正整数。')
      return
    }

    setPreviewLoading(true)
    setPreviewError('')
    try {
      const result = await previewManagedSourceDataset(datasetId, sourceDir, {
        tradeDate: selectedDataset.supportsTradeDate ? tradeDateInput : '',
        tsCode: selectedDataset.supportsTsCode ? readTargetCode : '',
        limit: parsedLimit,
      })
      setPreview(result)
    } catch (error) {
      setPreview(null)
      setPreviewError(`读取数据失败: ${String(error)}`)
    } finally {
      setPreviewLoading(false)
    }
  }

  return (
    <div className="settings-page">
      <section className="settings-card">
        <div className="settings-head">
          <div>
            <h2 className="settings-title">数据查看</h2>
            <p className="settings-subtitle">
              单独查看当前应用数据目录里的原始库、结果库和 CSV。
            </p>
          </div>

          <div className="settings-actions">
            <button className="settings-secondary-btn" type="button" onClick={() => void onRefreshStatus()} disabled={statusLoading}>
              {statusLoading ? '刷新中...' : '刷新状态'}
            </button>
          </div>
        </div>

        <div className="settings-summary-grid">
          <div className="settings-summary-item">
            <span>当前目录状态</span>
            <strong>{status?.isReady ? '数据已齐备' : statusLoading ? '读取中...' : '仍有缺失文件'}</strong>
          </div>
          <div className="settings-summary-item">
            <span>当前数据集</span>
            <strong>{selectedDataset.label}</strong>
          </div>
          <div className="settings-summary-item">
            <span>当前子目录</span>
            <strong>{status?.sourceDir ?? DEFAULT_MANAGED_SOURCE_DIR}</strong>
          </div>
        </div>

        {statusError ? <div className="settings-error">{statusError}</div> : null}
      </section>

      <section className="settings-card">
        <div className="settings-section-head">
          <div>
            <h3 className="settings-subtitle-head">查询条件</h3>
            <p className="settings-section-note">{selectedDataset.description}</p>
          </div>
        </div>

        <div className="settings-db-toolbar data-viewer-toolbar">
          <label className="settings-field data-viewer-field data-viewer-field-dataset">
            <span>数据源</span>
            <select
              value={datasetId}
              onChange={(event) => setDatasetId(event.target.value as ManagedSourceDatasetId)}
            >
              {DATASET_OPTIONS.map((item) => (
                <option key={item.id} value={item.id}>
                  {item.label}
                </option>
              ))}
            </select>
          </label>

          <label className="settings-field data-viewer-field data-viewer-field-trade-date">
            <span>交易日</span>
            <input
              type="text"
              value={tradeDateInput}
              onChange={(event) => setTradeDateInput(event.target.value.trim())}
              placeholder={selectedDataset.supportsTradeDate ? '例如 20260324' : '当前数据源不支持'}
              disabled={!selectedDataset.supportsTradeDate}
            />
          </label>

          <label className="settings-field data-viewer-field data-viewer-field-stock">
            <div className="data-viewer-field-head">
              <span>股票筛选</span>
              <span className="data-viewer-code-preview">代码：{readTargetCode || '--'}</span>
            </div>
            <div className="details-autocomplete">
              <input
                type="text"
                value={lookupInput}
                onChange={(event) => onLookupInputChange(event.target.value)}
                onFocus={() => setLookupFocused(true)}
                onBlur={() => setLookupFocused(false)}
                onKeyDown={(event) => {
                  if (event.key === 'Enter' && stockNameCandidates.length > 0) {
                    event.preventDefault()
                    onSelectStockCandidate(stockNameCandidates[0])
                  }
                }}
                placeholder={selectedDataset.supportsTsCode ? '输入股票名称、代码或拼音首字母' : '当前数据源不支持'}
                disabled={!selectedDataset.supportsTsCode}
              />
              {showStockNameCandidates ? (
                <div className="details-autocomplete-menu" onWheel={onCandidateWheel}>
                  {stockNameCandidates.map((row) => {
                    const code = getLookupDigits(row.ts_code)
                    return (
                      <button
                        className="details-autocomplete-option"
                        key={row.ts_code}
                        type="button"
                        onMouseDown={(event) => {
                          event.preventDefault()
                          onSelectStockCandidate(row)
                        }}
                      >
                        <strong>{row.name}</strong>
                        <span>{code || row.ts_code}</span>
                      </button>
                    )
                  })}
                </div>
              ) : null}
            </div>
          </label>

          <label className="settings-field data-viewer-field data-viewer-field-limit">
            <span>显示行数</span>
            <input
              type="number"
              min={1}
              step={1}
              value={limitInput}
              onChange={(event) => setLimitInput(event.target.value)}
              placeholder="例如 100"
            />
          </label>

          <div className="data-viewer-query-cell">
            <button
              className="settings-primary-btn data-viewer-query-btn"
              type="button"
              onClick={() => void onQueryPreview()}
              disabled={previewLoading || statusLoading}
            >
              {previewLoading ? '查询中...' : '查询数据'}
            </button>
          </div>
        </div>

        {!requiredFileImported ? (
          <div className="settings-empty-soft">当前目录缺少 {selectedDataset.label} 所需文件，查询前请先到“数据管理”页补齐。</div>
        ) : null}

        {previewError ? <div className="settings-error">{previewError}</div> : null}
      </section>

      <section className="settings-card">
        <div className="settings-section-head">
          <div>
            <h3 className="settings-subtitle-head">查询结果</h3>
            <p className="settings-section-note">
              {preview ? `当前展示 ${preview.datasetLabel}` : '尚未查询。'}
            </p>
          </div>
        </div>

        {preview ? (
          <>
            <div className="settings-summary-grid settings-db-summary-grid">
              <div className="settings-summary-item">
                <span>数据集总行数</span>
                <strong>{preview.rowCount.toLocaleString()}</strong>
              </div>
              <div className="settings-summary-item">
                <span>筛选命中</span>
                <strong>{preview.matchedRows.toLocaleString()}</strong>
              </div>
              <div className="settings-summary-item">
                <span>当前数据集</span>
                <strong>{preview.datasetLabel}</strong>
              </div>
              <div className="settings-summary-item">
                <span>当前列数</span>
                <strong>{preview.columns.length}</strong>
              </div>
              <div className="settings-summary-item settings-summary-item-wide">
                <span>源路径</span>
                <strong title={preview.sourcePath}>{preview.sourcePath}</strong>
              </div>
              <div className="settings-summary-item settings-summary-item-wide">
                <span>目标文件</span>
                <strong title={preview.targetPath}>{preview.targetPath}</strong>
              </div>
            </div>

            {preview.rows.length === 0 ? (
              <div className="settings-empty-soft">当前筛选没有查到数据。</div>
            ) : (
              <div className="settings-db-table-wrap">
                <table
                  className="settings-db-table"
                  style={{ minWidth: `${Math.max(preview.columns.length * 120, 720)}px` }}
                >
                  <thead>
                    <tr>
                      {preview.columns.map((column) => (
                        <th key={column}>{column}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {preview.rows.map((row, rowIndex) => (
                      <tr key={`${preview.datasetId}:${rowIndex}`}>
                        {row.map((value, columnIndex) => (
                          <td key={`${preview.columns[columnIndex]}:${rowIndex}`}>
                            {value === '' ? '--' : value}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </>
        ) : (
          <div className="settings-empty-soft">设置好筛选条件后，点击“查询数据”加载结果。</div>
        )}
      </section>
    </div>
  )
}
