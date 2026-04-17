import { useDeferredValue, useEffect, useMemo, useState } from 'react'
import { ensureManagedSourcePath } from '../../apis/managedSource'
import { getStockPickOptions } from '../../apis/stockPick'
import { filterConceptItems, useConceptExclusions } from '../../shared/conceptExclusions'
import './css/DataImportPage.css'
import './css/StockPickPage.css'
import './css/DetailsPage.css'

const AUTOCOMPLETE_LIMIT = 12

export default function SettingsPage() {
  const {
    excludedConcepts,
    setExcludedConcepts,
    excludeStBoard,
    setExcludeStBoard,
  } = useConceptExclusions()
  const [conceptOptions, setConceptOptions] = useState<string[]>([])
  const [conceptKeyword, setConceptKeyword] = useState('')
  const [lookupFocused, setLookupFocused] = useState(false)
  const [isConceptEditorOpen, setIsConceptEditorOpen] = useState(false)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const deferredConceptKeyword = useDeferredValue(conceptKeyword)

  useEffect(() => {
    let cancelled = false

    const loadOptions = async () => {
      setLoading(true)
      setError('')
      try {
        const resolvedSourcePath = await ensureManagedSourcePath()
        const options = await getStockPickOptions(resolvedSourcePath)
        if (cancelled) {
          return
        }

        setConceptOptions(filterConceptItems(options.concept_options ?? [], []))
      } catch (loadError) {
        if (cancelled) {
          return
        }

        setConceptOptions([])
        setError(`读取概念列表失败: ${String(loadError)}`)
      } finally {
        if (!cancelled) {
          setLoading(false)
        }
      }
    }

    void loadOptions()
    return () => {
      cancelled = true
    }
  }, [])

  async function onRefreshOptions() {
    setLoading(true)
    setError('')

    try {
      const resolvedSourcePath = await ensureManagedSourcePath()
      const options = await getStockPickOptions(resolvedSourcePath)
      setConceptOptions(filterConceptItems(options.concept_options ?? [], []))
    } catch (loadError) {
      setConceptOptions([])
      setError(`读取概念列表失败: ${String(loadError)}`)
    } finally {
      setLoading(false)
    }
  }

  const filteredConceptOptions = useMemo(() => {
    const keyword = deferredConceptKeyword.trim().toLowerCase()
    if (!keyword) {
      return conceptOptions
    }

    return conceptOptions.filter((item) => item.toLowerCase().includes(keyword))
  }, [conceptOptions, deferredConceptKeyword])

  const autocompleteOptions = useMemo(() => {
    if (!conceptKeyword.trim()) {
      return []
    }

    return filteredConceptOptions.slice(0, AUTOCOMPLETE_LIMIT)
  }, [conceptKeyword, filteredConceptOptions])

  const showAutocomplete = lookupFocused && autocompleteOptions.length > 0

  useEffect(() => {
    if (!isConceptEditorOpen) {
      return
    }

    function onKeyDown(event: KeyboardEvent) {
      if (event.key === 'Escape') {
        setIsConceptEditorOpen(false)
        setConceptKeyword('')
        setLookupFocused(false)
      }
    }

    window.addEventListener('keydown', onKeyDown)
    return () => window.removeEventListener('keydown', onKeyDown)
  }, [isConceptEditorOpen])

  function openConceptEditor() {
    setIsConceptEditorOpen(true)
    setConceptKeyword('')
    setLookupFocused(false)
  }

  function closeConceptEditor() {
    setIsConceptEditorOpen(false)
    setConceptKeyword('')
    setLookupFocused(false)
  }

  function toggleConcept(value: string) {
    setExcludedConcepts(
      excludedConcepts.includes(value)
        ? excludedConcepts.filter((item) => item !== value)
        : [...excludedConcepts, value],
    )
  }

  function onSelectAutocomplete(value: string) {
    if (!excludedConcepts.includes(value)) {
      setExcludedConcepts([...excludedConcepts, value])
    }

    setConceptKeyword('')
    setLookupFocused(true)
  }

  return (
    <div className="settings-page">
      <section className="settings-card">
        <div className="settings-head">
          <div>
            <h2 className="settings-title">设置</h2>
            <p className="settings-section-note">概念排除名单会同步影响选股页的概念过滤结果。</p>
          </div>
          <div className="settings-actions">
            <button
              className={excludeStBoard ? 'settings-secondary-btn is-active' : 'settings-secondary-btn'}
              type="button"
              onClick={() => setExcludeStBoard(!excludeStBoard)}
            >
              {excludeStBoard ? '已排除 ST 板块' : '排除 ST 板块'}
            </button>
            <button
              className="settings-primary-btn"
              type="button"
              onClick={openConceptEditor}
            >
              概念筛选
            </button>
          </div>
        </div>

        <div className="settings-summary-grid">
          <div className="settings-summary-item">
            <span>已排除条数</span>
            <strong>{excludedConcepts.length}</strong>
          </div>
          <div className="settings-summary-item">
            <span>可选概念总数</span>
            <strong>{conceptOptions.length}</strong>
          </div>
          <div className="settings-summary-item">
            <span>ST 板块过滤</span>
            <strong>{excludeStBoard ? '已开启' : '未开启'}</strong>
          </div>
        </div>

        {error && !isConceptEditorOpen ? <div className="settings-error">{error}</div> : null}
      </section>

      {isConceptEditorOpen ? (
        <div
          className="settings-modal-backdrop"
          role="presentation"
          onClick={(event) => {
            if (event.target === event.currentTarget) {
              closeConceptEditor()
            }
          }}
        >
          <section className="settings-modal" role="dialog" aria-modal="true" aria-label="概念筛选">
            <div className="settings-modal-head">
              <div>
                <h3 className="settings-subtitle-head">概念筛选</h3>
                <p className="settings-section-note">点击概念可加入或移出排除名单，结果会即时保存。</p>
              </div>
              <div className="settings-actions">
                <button
                  className={excludeStBoard ? 'settings-secondary-btn is-active' : 'settings-secondary-btn'}
                  type="button"
                  onClick={() => setExcludeStBoard(!excludeStBoard)}
                >
                  {excludeStBoard ? '已排除 ST 板块' : '排除 ST 板块'}
                </button>
                <button
                  className="settings-secondary-btn"
                  type="button"
                  onClick={() => {
                    setConceptKeyword('')
                    setLookupFocused(false)
                    void onRefreshOptions()
                  }}
                  disabled={loading}
                >
                  {loading ? '读取中...' : '刷新概念'}
                </button>
                <button
                  className="settings-danger-btn"
                  type="button"
                  onClick={() => setExcludedConcepts([])}
                  disabled={excludedConcepts.length === 0}
                >
                  清空排除
                </button>
                <button className="settings-primary-btn" type="button" onClick={closeConceptEditor}>
                  完成
                </button>
              </div>
            </div>

            <div className="settings-summary-grid">
              <div className="settings-summary-item">
                <span>已排除条数</span>
                <strong>{excludedConcepts.length}</strong>
              </div>
              <div className="settings-summary-item">
                <span>当前匹配条数</span>
                <strong>{filteredConceptOptions.length}</strong>
              </div>
              <div className="settings-summary-item">
                <span>可选概念总数</span>
                <strong>{conceptOptions.length}</strong>
              </div>
              <div className="settings-summary-item">
                <span>ST 板块过滤</span>
                <strong>{excludeStBoard ? '已开启' : '未开启'}</strong>
              </div>
            </div>

            <div className="stock-pick-concept-panel">
              <div className="stock-pick-concept-head">
                <strong>概念排除名单编辑</strong>
              </div>

              <div className="stock-pick-concept-toolbar">
                <div className="details-autocomplete">
                  <input
                    type="text"
                    value={conceptKeyword}
                    onChange={(event) => setConceptKeyword(event.target.value)}
                    onFocus={() => setLookupFocused(true)}
                    onBlur={() => setLookupFocused(false)}
                    onKeyDown={(event) => {
                      if (event.key === 'Enter' && autocompleteOptions.length > 0) {
                        event.preventDefault()
                        onSelectAutocomplete(autocompleteOptions[0])
                      }
                    }}
                    placeholder="搜索概念"
                    className="stock-pick-concept-search"
                  />
                  {showAutocomplete ? (
                    <div className="details-autocomplete-menu">
                      {autocompleteOptions.map((item) => (
                        <button
                          className="details-autocomplete-option"
                          key={item}
                          type="button"
                          onMouseDown={(event) => {
                            event.preventDefault()
                            onSelectAutocomplete(item)
                          }}
                        >
                          <strong>{item}</strong>
                          <span>{excludedConcepts.includes(item) ? '已在排除名单' : '加入排除名单'}</span>
                        </button>
                      ))}
                    </div>
                  ) : null}
                </div>
              </div>

              {error ? <div className="settings-error">{error}</div> : null}

              <div className="settings-section-head">
                <div>
                  <h3 className="settings-subtitle-head">已排除概念</h3>
                </div>
              </div>
              {excludedConcepts.length === 0 ? (
                <div className="settings-empty-soft">当前还没有排除任何概念。</div>
              ) : (
                <div className="settings-chip-list">
                  {excludedConcepts.map((item) => (
                    <button
                      key={item}
                      type="button"
                      className="stock-pick-chip-btn is-active"
                      onClick={() => toggleConcept(item)}
                    >
                      {item}
                    </button>
                  ))}
                </div>
              )}

              <div className="settings-section-head">
                <div>
                  <h3 className="settings-subtitle-head">概念选择</h3>
                </div>
              </div>
              {loading ? (
                <div className="stock-pick-empty">读取概念列表中...</div>
              ) : filteredConceptOptions.length === 0 ? (
                <div className="stock-pick-empty">没有匹配的概念。</div>
              ) : (
                <div className="stock-pick-concept-list">
                  {filteredConceptOptions.map((item) => {
                    const active = excludedConcepts.includes(item)
                    return (
                      <button
                        key={item}
                        type="button"
                        className={active ? 'stock-pick-chip-btn is-active' : 'stock-pick-chip-btn'}
                        onClick={() => toggleConcept(item)}
                      >
                        {item}
                      </button>
                    )
                  })}
                </div>
              )}
            </div>
          </section>
        </div>
      ) : null}
    </div>
  )
}
