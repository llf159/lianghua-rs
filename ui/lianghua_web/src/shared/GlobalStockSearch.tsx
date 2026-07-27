import {
  useDeferredValue,
  useEffect,
  useMemo,
  useRef,
  useState,
  type MouseEvent as ReactMouseEvent,
} from 'react'
import { useLocation, useNavigate } from 'react-router-dom'
import { ensureManagedSourcePath } from '../apis/managedSource'
import { listStockLookupRows, type StockLookupRow } from '../apis/reader'
import { buildLinkedDetailsPath } from './detailsRoute'
import { buildStockLookupCandidates, getLookupDigits } from './stockLookup'
import './globalStockSearch.css'

const MAX_GLOBAL_STOCK_CANDIDATES = 10

export default function GlobalStockSearch() {
  const location = useLocation()
  const navigate = useNavigate()
  const rootRef = useRef<HTMLDivElement | null>(null)
  const inputRef = useRef<HTMLInputElement | null>(null)
  const [isOpen, setIsOpen] = useState(false)
  const [lookupInput, setLookupInput] = useState('')
  const [sourcePath, setSourcePath] = useState('')
  const [stockLookupRows, setStockLookupRows] = useState<StockLookupRow[]>([])
  const [isLoading, setIsLoading] = useState(false)
  const [error, setError] = useState('')
  const deferredLookupInput = useDeferredValue(lookupInput)
  const stockNameCandidates = useMemo(
    () =>
      buildStockLookupCandidates(
        stockLookupRows,
        deferredLookupInput,
        MAX_GLOBAL_STOCK_CANDIDATES,
      ),
    [deferredLookupInput, stockLookupRows],
  )
  const showStockNameCandidates = lookupInput.trim() !== '' && stockNameCandidates.length > 0

  useEffect(() => {
    if (!isOpen) {
      return
    }

    let cancelled = false
    setIsLoading(true)
    setError('')
    void (async () => {
      try {
        const resolvedSourcePath = await ensureManagedSourcePath()
        const rows = await listStockLookupRows(resolvedSourcePath)
        if (!cancelled) {
          setSourcePath(resolvedSourcePath)
          setStockLookupRows(rows)
        }
      } catch (loadError) {
        if (!cancelled) {
          setStockLookupRows([])
          setError(`股票候选读取失败: ${String(loadError)}`)
        }
      } finally {
        if (!cancelled) {
          setIsLoading(false)
        }
      }
    })()

    return () => {
      cancelled = true
    }
  }, [isOpen])

  useEffect(() => {
    if (!isOpen) {
      return
    }

    const frameId = window.requestAnimationFrame(() => {
      inputRef.current?.focus()
    })

    const onPointerDown = (event: PointerEvent) => {
      const target = event.target
      if (target instanceof Node && !rootRef.current?.contains(target)) {
        setIsOpen(false)
      }
    }

    window.addEventListener('pointerdown', onPointerDown)
    return () => {
      window.cancelAnimationFrame(frameId)
      window.removeEventListener('pointerdown', onPointerDown)
    }
  }, [isOpen])

  function openPanel() {
    setIsOpen((current) => {
      const next = !current
      if (next) {
        setLookupInput('')
        setError('')
      }
      return next
    })
  }

  function openStockDetail(row: StockLookupRow) {
    const backgroundLocation =
      location.state &&
      typeof location.state === 'object' &&
      'backgroundLocation' in location.state &&
      location.state.backgroundLocation
        ? location.state.backgroundLocation
        : location

    setIsOpen(false)
    setLookupInput('')
    navigate(
      buildLinkedDetailsPath({
        tsCode: row.ts_code,
        sourcePath,
      }),
      {
        state: {
          backgroundLocation,
          navigationItems: [
            {
              tsCode: row.ts_code,
              name: row.name,
              sourcePath,
            },
          ],
        },
      },
    )
  }

  function onPanelMouseDown(event: ReactMouseEvent<HTMLDivElement>) {
    event.stopPropagation()
  }

  return (
    <div className="global-stock-search" ref={rootRef}>
      <button
        className={isOpen ? 'global-stock-search-toggle is-active' : 'global-stock-search-toggle'}
        type="button"
        aria-label="全局股票搜索"
        title="全局股票搜索"
        onClick={openPanel}
      >
        <span aria-hidden="true" />
      </button>

      {isOpen ? (
        <div className="global-stock-search-panel" onMouseDown={onPanelMouseDown}>
          <label className="global-stock-search-field">
            <span>股票搜索</span>
            <input
              ref={inputRef}
              type="text"
              value={lookupInput}
              onChange={(event) => setLookupInput(event.target.value)}
              onKeyDown={(event) => {
                if (event.key === 'Escape') {
                  event.preventDefault()
                  setIsOpen(false)
                }
                if (event.key === 'Enter' && stockNameCandidates.length > 0) {
                  event.preventDefault()
                  openStockDetail(stockNameCandidates[0])
                }
              }}
              placeholder="输入代码 / 名称 / 拼音首字母"
            />
          </label>

          {isLoading ? <div className="global-stock-search-hint">读取候选中...</div> : null}
          {error ? <div className="global-stock-search-error">{error}</div> : null}

          {showStockNameCandidates ? (
            <div className="global-stock-search-menu">
              {stockNameCandidates.map((row) => {
                const code = getLookupDigits(row.ts_code)
                return (
                  <button
                    className="global-stock-search-option"
                    key={row.ts_code}
                    type="button"
                    onMouseDown={(event) => {
                      event.preventDefault()
                      openStockDetail(row)
                    }}
                  >
                    <strong>{row.name}</strong>
                    <span>{code || row.ts_code}</span>
                  </button>
                )
              })}
            </div>
          ) : !isLoading && lookupInput.trim() !== '' && !error ? (
            <div className="global-stock-search-hint">没有匹配候选</div>
          ) : null}
        </div>
      ) : null}
    </div>
  )
}
