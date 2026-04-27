import { useEffect, useMemo, useState } from 'react'
import {
  getChartIndicatorSettings,
  resetChartIndicatorSettings,
  saveChartIndicatorSettings,
  validateChartIndicatorSettings,
  type ChartColorRuleDraft,
  type ChartIndicatorConfigDraft,
  type ChartIndicatorSettingsPayload,
  type ChartMarkerDraft,
  type ChartMarkerPosition,
  type ChartMarkerShape,
  type ChartPanelDraft,
  type ChartPanelKind,
  type ChartPanelRole,
  type ChartSeriesDraft,
  type ChartSeriesKind,
} from '../../../apis/chartIndicatorSettings'
import { ensureManagedSourcePath } from '../../../apis/managedSource'
import '../css/ChartIndicatorSettingsModal.css'

const KEY_PATTERN = /^[A-Za-z_][A-Za-z0-9_]*$/
const COLOR_PATTERN = /^#(?:[0-9a-fA-F]{3}|[0-9a-fA-F]{6})$/

const BASE_FIELDS = [
  'O',
  'H',
  'L',
  'C',
  'V',
  'OPEN',
  'HIGH',
  'LOW',
  'CLOSE',
  'VOL',
  'AMOUNT',
  'PRE_CLOSE',
  'CHANGE',
  'PCT_CHG',
  'TOR',
  'TURNOVER_RATE',
]

const FUNCTIONS = [
  'ABS',
  'MAX',
  'MIN',
  'DIV',
  'HHV',
  'LLV',
  'COUNT',
  'MA',
  'REF',
  'LAST',
  'SUM',
  'STD',
  'IF',
  'CROSS',
  'EMA',
  'SMA',
  'BARSLAST',
  'RSV',
  'GRANK',
  'GTOPCOUNT',
  'LTOPCOUNT',
  'LRANK',
  'GET',
]

type EditorMode = 'form' | 'source'
type DetailSelection = { kind: 'series' | 'marker'; index: number } | null

type Props = {
  open: boolean
  onClose: () => void
  onLoaded?: (payload: ChartIndicatorSettingsPayload) => void
}

export default function ChartIndicatorSettingsModal({ open, onClose, onLoaded }: Props) {
  const [sourcePath, setSourcePath] = useState('')
  const [payload, setPayload] = useState<ChartIndicatorSettingsPayload | null>(null)
  const [draft, setDraft] = useState<ChartIndicatorConfigDraft>(() => normalizeConfig(null))
  const [sourceText, setSourceText] = useState('')
  const [mode, setMode] = useState<EditorMode>('form')
  const [selectedPanelIndex, setSelectedPanelIndex] = useState(0)
  const [detailSelection, setDetailSelection] = useState<DetailSelection>(null)
  const [loading, setLoading] = useState(false)
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState('')
  const [notice, setNotice] = useState('')

  useEffect(() => {
    if (!open) {
      return
    }

    let cancelled = false
    async function loadSettings() {
      setLoading(true)
      setSaving(false)
      setError('')
      setNotice('')
      try {
        const resolvedSourcePath = await ensureManagedSourcePath()
        const nextPayload = await getChartIndicatorSettings(resolvedSourcePath)
        if (cancelled) {
          return
        }
        const nextDraft = normalizeConfig(nextPayload.config)
        setSourcePath(resolvedSourcePath)
        setPayload(nextPayload)
        setDraft(nextDraft)
        setSourceText(nextPayload.text)
        setSelectedPanelIndex(0)
        setDetailSelection(null)
        setMode(nextPayload.error ? 'source' : 'form')
        setError(nextPayload.error ?? '')
        onLoaded?.(nextPayload)
      } catch (loadError) {
        if (!cancelled) {
          setError(`读取图表指标配置失败: ${String(loadError)}`)
        }
      } finally {
        if (!cancelled) {
          setLoading(false)
        }
      }
    }

    void loadSettings()
    return () => {
      cancelled = true
    }
  }, [onLoaded, open])

  const panels = draft.panel
  const selectedPanel = panels[selectedPanelIndex] ?? panels[0]
  const localIssues = useMemo(() => validateDraft(draft), [draft])
  const localSummary = useMemo(() => summarizeDraft(draft), [draft])
  const previousSeriesKeys = useMemo(() => {
    if (!selectedPanel) {
      return []
    }
    const keys: string[] = []
    for (let panelIndex = 0; panelIndex < panels.length; panelIndex += 1) {
      const panel = panels[panelIndex]
      const limit = panelIndex === selectedPanelIndex ? Math.max(detailSelection?.index ?? 0, 0) : panel.series?.length ?? 0
      for (let seriesIndex = 0; seriesIndex < limit; seriesIndex += 1) {
        const key = panel.series?.[seriesIndex]?.key?.trim()
        if (key) {
          keys.push(key)
        }
      }
      if (panelIndex === selectedPanelIndex) {
        break
      }
    }
    return keys
  }, [detailSelection?.index, panels, selectedPanel, selectedPanelIndex])

  if (!open) {
    return null
  }

  function updateDraft(nextDraft: ChartIndicatorConfigDraft) {
    setDraft(normalizeConfig(nextDraft))
    setNotice('')
  }

  function updatePanel(index: number, patch: Partial<ChartPanelDraft>) {
    const nextPanels = panels.map((panel, panelIndex) => {
      if (panelIndex !== index) {
        return panel
      }
      const nextPanel = { ...panel, ...patch }
      if (panel.role === 'main') {
        nextPanel.role = 'main'
        nextPanel.kind = 'candles'
      } else if (nextPanel.kind === 'candles') {
        nextPanel.kind = 'line'
      }
      return nextPanel
    })
    updateDraft({ ...draft, panel: nextPanels })
  }

  function updateSeries(index: number, patch: Partial<ChartSeriesDraft>) {
    if (!selectedPanel) {
      return
    }
    const nextSeries = (selectedPanel.series ?? []).map((series, seriesIndex) =>
      seriesIndex === index ? normalizeSeriesByKind({ ...series, ...patch }) : series,
    )
    updatePanel(selectedPanelIndex, { series: nextSeries })
  }

  function updateMarker(index: number, patch: Partial<ChartMarkerDraft>) {
    if (!selectedPanel) {
      return
    }
    const nextMarkers = (selectedPanel.marker ?? []).map((marker, markerIndex) =>
      markerIndex === index ? { ...marker, ...patch } : marker,
    )
    updatePanel(selectedPanelIndex, { marker: nextMarkers })
  }

  function addPanel() {
    const usedPanelKeys = new Set(panels.map((panel) => panel.key))
    const key = uniqueKey('panel', usedPanelKeys)
    const nextPanel: ChartPanelDraft = {
      key,
      label: '指标面板',
      role: 'sub',
      kind: 'line',
      series: [],
      marker: [],
    }
    const nextPanels = panels.concat(nextPanel)
    updateDraft({ ...draft, panel: nextPanels })
    setSelectedPanelIndex(nextPanels.length - 1)
    setDetailSelection(null)
  }

  function copyPanel(index: number) {
    const panel = panels[index]
    if (!panel) {
      return
    }
    if (panel.role === 'main') {
      setError('主图 K 线面板固定存在；可复制的是它上面的序列或标记。')
      return
    }
    const usedPanelKeys = new Set(panels.map((item) => item.key))
    const usedSeriesKeys = collectSeriesKeys(draft)
    const nextPanel = clonePanelWithFreshKeys(
      { ...panel, role: 'sub', kind: panel.kind === 'candles' ? 'line' : panel.kind },
      usedPanelKeys,
      usedSeriesKeys,
    )
    nextPanel.label = `${panel.label}副本`
    const nextPanels = insertAt(panels, index + 1, nextPanel)
    updateDraft({ ...draft, panel: nextPanels })
    setSelectedPanelIndex(index + 1)
    setDetailSelection(null)
  }

  function deletePanel(index: number) {
    const panel = panels[index]
    if (!panel) {
      return
    }
    if (panels.length <= 1) {
      setError('至少需要保留一个面板。')
      return
    }
    if (panel.role === 'main') {
      setError('主图 K 线面板固定存在，只能删除它上面的叠加序列和标记。')
      return
    }
    const nextPanels = panels.filter((_, panelIndex) => panelIndex !== index)
    updateDraft({ ...draft, panel: nextPanels })
    setSelectedPanelIndex(Math.max(0, Math.min(index, nextPanels.length - 1)))
    setDetailSelection(null)
  }

  function movePanel(index: number, direction: -1 | 1) {
    const nextIndex = index + direction
    if (panels[index]?.role === 'main' || panels[nextIndex]?.role === 'main') {
      return
    }
    const nextPanels = moveItem(panels, index, direction)
    updateDraft({ ...draft, panel: nextPanels })
    setSelectedPanelIndex(index + direction)
    setDetailSelection(null)
  }

  function addSeries() {
    if (!selectedPanel) {
      return
    }
    const existing = selectedPanel.series ?? []
    const usedKeys = collectSeriesKeys(draft)
    const nextSeries = makeBlankSeries(usedKeys)
    updatePanel(selectedPanelIndex, { series: [...existing, nextSeries] })
    setDetailSelection({ kind: 'series', index: existing.length })
  }

  function copySeries(index: number) {
    if (!selectedPanel) {
      return
    }
    const series = selectedPanel.series?.[index]
    if (!series) {
      return
    }
    const usedKeys = collectSeriesKeys(draft)
    usedKeys.delete(series.key)
    const nextSeries = {
      ...series,
      key: uniqueKey(`${series.key}_copy`, usedKeys),
      label: `${series.label || series.key}副本`,
      color_when: (series.color_when ?? []).map((rule) => ({ ...rule })),
    }
    const nextList = insertAt(selectedPanel.series ?? [], index + 1, nextSeries)
    updatePanel(selectedPanelIndex, { series: nextList })
    setDetailSelection({ kind: 'series', index: index + 1 })
  }

  function deleteSeries(index: number) {
    if (!selectedPanel) {
      return
    }
    const nextList = (selectedPanel.series ?? []).filter((_, seriesIndex) => seriesIndex !== index)
    updatePanel(selectedPanelIndex, { series: nextList })
    setDetailSelection(null)
  }

  function moveSeries(index: number, direction: -1 | 1) {
    if (!selectedPanel) {
      return
    }
    const nextList = moveItem(selectedPanel.series ?? [], index, direction)
    updatePanel(selectedPanelIndex, { series: nextList })
    setDetailSelection({ kind: 'series', index: index + direction })
  }

  function addMarker() {
    if (!selectedPanel) {
      return
    }
    const existing = selectedPanel.marker ?? []
    const usedKeys = new Set(existing.map((marker) => marker.key))
    const marker: ChartMarkerDraft = {
      key: uniqueKey('marker', usedKeys),
      label: null,
      when: '',
      y: null,
      position: 'value',
      shape: 'dot',
      color: null,
      text: null,
    }
    updatePanel(selectedPanelIndex, { marker: [...existing, marker] })
    setDetailSelection({ kind: 'marker', index: existing.length })
  }

  function copyMarker(index: number) {
    if (!selectedPanel) {
      return
    }
    const marker = selectedPanel.marker?.[index]
    if (!marker) {
      return
    }
    const usedKeys = new Set((selectedPanel.marker ?? []).map((item) => item.key))
    usedKeys.delete(marker.key)
    const nextMarker = {
      ...marker,
      key: uniqueKey(`${marker.key}_copy`, usedKeys),
      label: `${marker.label || marker.key}副本`,
    }
    const nextList = insertAt(selectedPanel.marker ?? [], index + 1, nextMarker)
    updatePanel(selectedPanelIndex, { marker: nextList })
    setDetailSelection({ kind: 'marker', index: index + 1 })
  }

  function deleteMarker(index: number) {
    if (!selectedPanel) {
      return
    }
    const nextList = (selectedPanel.marker ?? []).filter((_, markerIndex) => markerIndex !== index)
    updatePanel(selectedPanelIndex, { marker: nextList })
    setDetailSelection(null)
  }

  function moveMarker(index: number, direction: -1 | 1) {
    if (!selectedPanel) {
      return
    }
    const nextList = moveItem(selectedPanel.marker ?? [], index, direction)
    updatePanel(selectedPanelIndex, { marker: nextList })
    setDetailSelection({ kind: 'marker', index: index + direction })
  }

  function updateColorRule(seriesIndex: number, ruleIndex: number, patch: Partial<ChartColorRuleDraft>) {
    const series = selectedPanel?.series?.[seriesIndex]
    if (!series) {
      return
    }
    const nextRules = (series.color_when ?? []).map((rule, index) =>
      index === ruleIndex ? { ...rule, ...patch } : rule,
    )
    updateSeries(seriesIndex, { color_when: nextRules })
  }

  function addColorRule(seriesIndex: number) {
    const series = selectedPanel?.series?.[seriesIndex]
    if (!series) {
      return
    }
    updateSeries(seriesIndex, {
      color_when: [...(series.color_when ?? []), { when: '', color: '' }],
    })
  }

  function deleteColorRule(seriesIndex: number, ruleIndex: number) {
    const series = selectedPanel?.series?.[seriesIndex]
    if (!series) {
      return
    }
    updateSeries(seriesIndex, {
      color_when: (series.color_when ?? []).filter((_, index) => index !== ruleIndex),
    })
  }

  async function switchMode(nextMode: EditorMode) {
    if (nextMode === mode) {
      return
    }
    setError('')
    setNotice('')
    if (nextMode === 'source') {
      setSourceText(serializeConfigToToml(draft))
      setMode('source')
      return
    }

    if (!sourcePath) {
      setError('')
      setNotice('当前为 Vite 预览模式，未连接 Tauri 数据源；已切回结构化视图（不会执行后端 TOML 校验）。')
      setMode('form')
      return
    }
    const result = await validateChartIndicatorSettings(sourcePath, sourceText)
    if (!result.ok || !result.config) {
      setError(result.error || 'TOML 校验未通过。')
      return
    }
    setDraft(normalizeConfig(result.config))
    setPayload((current) =>
      current && result.summary
        ? { ...current, config: result.config as ChartIndicatorConfigDraft, summary: result.summary, text: sourceText, error: null }
        : current,
    )
    setMode('form')
  }

  async function onValidate() {
    if (!sourcePath) {
      setError('数据源路径尚未准备好。')
      return
    }
    if (mode === 'form' && localIssues.length > 0) {
      setError(localIssues[0])
      setNotice('')
      return
    }
    setSaving(true)
    setError('')
    setNotice('')
    try {
      const text = mode === 'form' ? serializeConfigToToml(draft) : sourceText
      const result = await validateChartIndicatorSettings(sourcePath, text)
      if (!result.ok || !result.config || !result.summary) {
        setError(result.error || '校验未通过。')
        return
      }
      setDraft(normalizeConfig(result.config))
      setSourceText(text)
      setPayload((current) =>
        current ? { ...current, config: result.config as ChartIndicatorConfigDraft, summary: result.summary, text, error: null } : current,
      )
      setNotice('校验通过。')
    } catch (validateError) {
      setError(`校验失败: ${String(validateError)}`)
    } finally {
      setSaving(false)
    }
  }

  async function onSave() {
    if (!sourcePath) {
      setError('数据源路径尚未准备好。')
      return
    }
    if (mode === 'form' && localIssues.length > 0) {
      setError(localIssues[0])
      setNotice('')
      return
    }
    setSaving(true)
    setError('')
    setNotice('')
    try {
      const text = mode === 'form' ? serializeConfigToToml(draft) : sourceText
      const nextPayload = await saveChartIndicatorSettings(sourcePath, text)
      setPayload(nextPayload)
      setDraft(normalizeConfig(nextPayload.config))
      setSourceText(nextPayload.text)
      setMode('form')
      setNotice('已保存。重新进入详情页或刷新当前详情页后生效。')
      onLoaded?.(nextPayload)
    } catch (saveError) {
      setError(`保存失败: ${String(saveError)}`)
    } finally {
      setSaving(false)
    }
  }

  async function onReset() {
    if (!sourcePath) {
      setError('数据源路径尚未准备好。')
      return
    }
    if (!window.confirm('确认重置为默认图表指标配置吗？')) {
      return
    }
    setSaving(true)
    setError('')
    setNotice('')
    try {
      const nextPayload = await resetChartIndicatorSettings(sourcePath)
      setPayload(nextPayload)
      setDraft(normalizeConfig(nextPayload.config))
      setSourceText(nextPayload.text)
      setSelectedPanelIndex(0)
      setDetailSelection(null)
      setMode('form')
      setNotice('已重置为默认配置。重新进入详情页或刷新当前详情页后生效。')
      onLoaded?.(nextPayload)
    } catch (resetError) {
      setError(`重置失败: ${String(resetError)}`)
    } finally {
      setSaving(false)
    }
  }

  const activeSeries =
    detailSelection?.kind === 'series' ? selectedPanel?.series?.[detailSelection.index] : null
  const activeMarker =
    detailSelection?.kind === 'marker' ? selectedPanel?.marker?.[detailSelection.index] : null

  return (
    <div
      className="settings-modal-backdrop"
      role="presentation"
      onClick={(event) => {
        if (event.target === event.currentTarget) {
          onClose()
        }
      }}
    >
      <section className="settings-modal settings-modal-wide chart-indicator-modal" role="dialog" aria-modal="true" aria-label="自定义图表指标">
        <div className="settings-modal-head chart-indicator-modal-head">
          <div>
            <h3 className="settings-subtitle-head">自定义图表指标</h3>
            <p className="settings-section-note">
              {payload?.exists ? payload.filePath : '当前数据源未创建 chart_indicators.toml，保存后会写入新文件。'}
            </p>
          </div>
          <div className="settings-actions">
            <button className="settings-secondary-btn" type="button" onClick={onClose}>
              关闭
            </button>
            <button className="settings-secondary-btn" type="button" onClick={onReset} disabled={loading || saving}>
              重置为默认
            </button>
            <button
              className={mode === 'source' ? 'settings-secondary-btn is-active' : 'settings-secondary-btn'}
              type="button"
              onClick={() => void switchMode(mode === 'source' ? 'form' : 'source')}
              disabled={loading || saving}
            >
              {mode === 'source' ? '结构化' : '源码'}
            </button>
            <button className="settings-secondary-btn" type="button" onClick={() => void onValidate()} disabled={loading || saving}>
              校验
            </button>
            <button className="settings-primary-btn" type="button" onClick={() => void onSave()} disabled={loading || saving}>
              {saving ? '处理中...' : '保存'}
            </button>
          </div>
        </div>

        {loading ? <div className="settings-empty-soft">读取配置中...</div> : null}
        {error ? <div className="settings-error">{error}</div> : null}
        {notice ? <div className="settings-notice">{notice}</div> : null}

        <div className="chart-indicator-mode-tabs">
          <button className={mode === 'form' ? 'is-active' : ''} type="button" onClick={() => void switchMode('form')}>
            结构化编辑
          </button>
          <button className={mode === 'source' ? 'is-active' : ''} type="button" onClick={() => void switchMode('source')}>
            TOML 源码
          </button>
        </div>

        {mode === 'source' ? (
          <div className="chart-indicator-source-pane">
            <textarea
              className="settings-textarea chart-indicator-source-textarea"
              value={sourceText}
              onChange={(event) => {
                setSourceText(event.target.value)
                setNotice('')
              }}
              spellCheck={false}
            />
          </div>
        ) : (
          <div className="chart-indicator-editor-grid">
            <aside className="chart-indicator-panel-list">
              <div className="chart-indicator-section-title">面板</div>
              <div className="chart-indicator-panel-toolbar">
                <button className="settings-secondary-btn chart-indicator-mini-btn" type="button" onClick={addPanel}>
                  新增副图
                </button>
              </div>
              <div className="chart-indicator-list">
                {panels.map((panel, index) => (
                  <button
                    key={`${panel.key}-${index}`}
                    className={index === selectedPanelIndex ? 'chart-indicator-list-item is-active' : 'chart-indicator-list-item'}
                    type="button"
                    onClick={() => {
                      setSelectedPanelIndex(index)
                      setDetailSelection(null)
                    }}
                  >
                    <strong>{panel.label || panel.key || '未命名面板'}</strong>
                    <span>
                      {panel.key || '无 key'} · {panel.role === 'main' ? '固定K线' : panelKindLabel(panel.kind)}
                    </span>
                  </button>
                ))}
              </div>
              <div className="chart-indicator-inline-actions">
                <button className="settings-secondary-btn" type="button" onClick={() => movePanel(selectedPanelIndex, -1)} disabled={selectedPanelIndex <= 0 || selectedPanel?.role === 'main' || panels[selectedPanelIndex - 1]?.role === 'main'}>
                  上移
                </button>
                <button className="settings-secondary-btn" type="button" onClick={() => movePanel(selectedPanelIndex, 1)} disabled={selectedPanelIndex >= panels.length - 1 || selectedPanel?.role === 'main' || panels[selectedPanelIndex + 1]?.role === 'main'}>
                  下移
                </button>
                <button className="settings-secondary-btn" type="button" onClick={() => copyPanel(selectedPanelIndex)} disabled={!selectedPanel || selectedPanel.role === 'main'}>
                  复制
                </button>
                <button className="settings-danger-btn" type="button" onClick={() => deletePanel(selectedPanelIndex)} disabled={!selectedPanel || selectedPanel.role === 'main'}>
                  删除
                </button>
              </div>
            </aside>

            <main className="chart-indicator-panel-editor">
              {selectedPanel ? (
                <>
                  <div className="chart-indicator-section-title">当前面板</div>
                  {selectedPanel.role === 'main' ? (
                    <div className="settings-notice">
                      主图 K 线由行情 O/H/L/C 固定渲染；这里编辑的是叠加在主图上的指标线和信号标记。
                    </div>
                  ) : null}
                  <div className="chart-indicator-field-grid">
                    <label className="settings-field">
                      <span>面板标识</span>
                      <input
                        value={selectedPanel.key}
                        onChange={(event) => updatePanel(selectedPanelIndex, { key: event.target.value })}
                        disabled={selectedPanel.role === 'main'}
                      />
                    </label>
                    <label className="settings-field">
                      <span>显示名称</span>
                      <input
                        value={selectedPanel.label}
                        onChange={(event) => updatePanel(selectedPanelIndex, { label: event.target.value })}
                        disabled={selectedPanel.role === 'main'}
                      />
                    </label>
                  </div>

                  <div className="chart-indicator-subsection-head">
                    <strong>序列</strong>
                    <button className="settings-secondary-btn" type="button" onClick={addSeries}>
                      新增序列
                    </button>
                  </div>
                  <div className="chart-indicator-card-list">
                    {(selectedPanel.series ?? []).map((series, index) => (
                      <button
                        key={`${series.key}-${index}`}
                        className={detailSelection?.kind === 'series' && detailSelection.index === index ? 'chart-indicator-card-row is-active' : 'chart-indicator-card-row'}
                        type="button"
                        onClick={() => setDetailSelection({ kind: 'series', index })}
                      >
                        <strong>{series.label || series.key || '未命名序列'}</strong>
                        <span>{series.key || '无 key'} · {seriesKindLabel(series.kind)} · {series.expr || '无表达式'}</span>
                      </button>
                    ))}
                    {(selectedPanel.series ?? []).length === 0 ? <div className="settings-empty-soft">当前面板还没有序列。</div> : null}
                  </div>

                  <div className="chart-indicator-subsection-head">
                    <strong>标记</strong>
                    <button className="settings-secondary-btn" type="button" onClick={addMarker}>
                      新增标记
                    </button>
                  </div>
                  <div className="chart-indicator-card-list">
                    {(selectedPanel.marker ?? []).map((marker, index) => (
                      <button
                        key={`${marker.key}-${index}`}
                        className={detailSelection?.kind === 'marker' && detailSelection.index === index ? 'chart-indicator-card-row is-active' : 'chart-indicator-card-row'}
                        type="button"
                        onClick={() => setDetailSelection({ kind: 'marker', index })}
                      >
                        <strong>{marker.label || marker.key || '未命名标记'}</strong>
                        <span>{marker.key || '无 key'} · {marker.when || '无条件'}</span>
                      </button>
                    ))}
                    {(selectedPanel.marker ?? []).length === 0 ? <div className="settings-empty-soft">当前面板还没有标记。</div> : null}
                  </div>
                </>
              ) : (
                <div className="settings-empty-soft">请选择一个面板。</div>
              )}
            </main>

            <aside className="chart-indicator-detail-editor">
              <div className="chart-indicator-section-title">细节与预览</div>
              {activeSeries ? (
                <SeriesEditor
                  panel={selectedPanel}
                  series={activeSeries}
                  index={detailSelection?.index ?? 0}
                  onUpdate={updateSeries}
                  onMove={moveSeries}
                  onCopy={copySeries}
                  onDelete={deleteSeries}
                  onAddColorRule={addColorRule}
                  onUpdateColorRule={updateColorRule}
                  onDeleteColorRule={deleteColorRule}
                />
              ) : activeMarker ? (
                <MarkerEditor
                  marker={activeMarker}
                  index={detailSelection?.index ?? 0}
                  markerCount={selectedPanel?.marker?.length ?? 0}
                  previousSeriesKeys={previousSeriesKeys}
                  databaseIndicatorColumns={payload?.summary.databaseIndicatorColumns ?? []}
                  onUpdate={updateMarker}
                  onMove={moveMarker}
                  onCopy={copyMarker}
                  onDelete={deleteMarker}
                />
              ) : (
                <div className="settings-empty-soft">选择一个序列或标记后，在这里编辑详细参数。</div>
              )}

              <div className="chart-indicator-preview">
                <div className="chart-indicator-section-title">摘要</div>
                <div className="settings-summary-grid chart-indicator-summary-grid">
                  <div className="settings-summary-item">
                    <span>面板</span>
                    <strong>{localSummary.panelCount}</strong>
                  </div>
                  <div className="settings-summary-item">
                    <span>序列</span>
                    <strong>{localSummary.seriesCount}</strong>
                  </div>
                  <div className="settings-summary-item">
                    <span>标记</span>
                    <strong>{localSummary.markerCount}</strong>
                  </div>
                  <div className="settings-summary-item">
                    <span>可引用指标列</span>
                    <strong>{payload?.summary.databaseIndicatorColumns.length ?? 0}</strong>
                  </div>
                </div>
                {localIssues.length > 0 ? (
                  <div className="chart-indicator-issue-list">
                    {localIssues.slice(0, 5).map((issue) => (
                      <span key={issue}>{issue}</span>
                    ))}
                  </div>
                ) : (
                  <div className="settings-notice">结构化字段检查通过，保存时仍会执行后端表达式校验。</div>
                )}
              </div>
            </aside>
          </div>
        )}
      </section>
    </div>
  )
}

type SeriesEditorProps = {
  panel: ChartPanelDraft | undefined
  series: ChartSeriesDraft
  index: number
  onUpdate: (index: number, patch: Partial<ChartSeriesDraft>) => void
  onMove: (index: number, direction: -1 | 1) => void
  onCopy: (index: number) => void
  onDelete: (index: number) => void
  onAddColorRule: (index: number) => void
  onUpdateColorRule: (seriesIndex: number, ruleIndex: number, patch: Partial<ChartColorRuleDraft>) => void
  onDeleteColorRule: (seriesIndex: number, ruleIndex: number) => void
}

function SeriesEditor({
  panel,
  series,
  index,
  onUpdate,
  onMove,
  onCopy,
  onDelete,
  onAddColorRule,
  onUpdateColorRule,
  onDeleteColorRule,
}: SeriesEditorProps) {
  const allowedKinds = getAllowedSeriesKinds(panel)
  const opacityValue = series.opacity ?? 1
  const isLineSeries = series.kind === 'line'
  const isBarSeries = series.kind === 'bar'
  const isBrickSeries = series.kind === 'brick'
  return (
    <div className="chart-indicator-detail-form">
      <div className="chart-indicator-subsection-head">
        <strong>序列参数</strong>
        <div className="chart-indicator-inline-actions">
          <button className="settings-secondary-btn" type="button" onClick={() => onMove(index, -1)} disabled={index <= 0}>
            上移
          </button>
          <button className="settings-secondary-btn" type="button" onClick={() => onMove(index, 1)} disabled={index >= (panel?.series?.length ?? 0) - 1}>
            下移
          </button>
          <button className="settings-secondary-btn" type="button" onClick={() => onCopy(index)}>
            复制
          </button>
          <button className="settings-danger-btn" type="button" onClick={() => onDelete(index)}>
            删除
          </button>
        </div>
      </div>
      <label className="settings-field">
        <span>序列标识</span>
        <input value={series.key} onChange={(event) => onUpdate(index, { key: event.target.value })} />
      </label>
      <label className="settings-field">
        <span>显示名称</span>
        <input value={series.label ?? ''} onChange={(event) => onUpdate(index, { label: optionalString(event.target.value) })} />
      </label>
      <label className="settings-field settings-field-textarea">
        <span>计算表达式</span>
        <textarea className="settings-textarea chart-indicator-expr-textarea" value={series.expr} onChange={(event) => onUpdate(index, { expr: event.target.value })} />
      </label>
      <div className="chart-indicator-field-grid">
        <label className="settings-field">
          <span>图法</span>
          <select value={series.kind} onChange={(event) => onUpdate(index, { kind: event.target.value as ChartSeriesKind })}>
            {allowedKinds.includes(series.kind) ? null : <option value={series.kind}>{series.kind}（保留）</option>}
            {allowedKinds.map((kind) => (
              <option key={kind} value={kind}>{seriesKindLabel(kind)}</option>
            ))}
          </select>
        </label>
        <label className="settings-field">
          <span>默认颜色</span>
          <input type="text" value={series.color ?? ''} onChange={(event) => onUpdate(index, { color: optionalString(event.target.value) })} placeholder="#2563eb" />
        </label>
        {isLineSeries ? (
          <>
            <label className="settings-field">
              <span>线宽</span>
              <input type="number" min={0.5} max={6} step={0.1} value={series.line_width ?? ''} onChange={(event) => onUpdate(index, { line_width: optionalNumber(event.target.value) })} />
            </label>
            <label className="settings-field">
              <span>透明度</span>
              <div className="chart-indicator-range-row">
                <input
                  type="range"
                  min={0}
                  max={1}
                  step={0.05}
                  value={opacityValue}
                  onChange={(event) => onUpdate(index, { opacity: Number(event.target.value) })}
                />
                <span>{series.opacity === null || series.opacity === undefined ? '默认' : opacityValue.toFixed(2)}</span>
              </div>
            </label>
          </>
        ) : null}
        {isBarSeries ? (
          <label className="settings-field">
            <span>基线值</span>
            <input type="number" step="any" value={series.base_value ?? ''} onChange={(event) => onUpdate(index, { base_value: optionalNumber(event.target.value) })} />
            <small>柱体从这条数值线向上或向下绘制，常见为 0。</small>
          </label>
        ) : null}
        {isBrickSeries ? (
          <label className="settings-field settings-field-span-2">
            <span>图法说明</span>
            <small>砖体序列只使用表达式结果和颜色参数；线宽、透明度、基线值不生效。</small>
          </label>
        ) : null}
      </div>

      <div className="chart-indicator-subsection-head">
        <strong>条件颜色</strong>
        <button className="settings-secondary-btn" type="button" onClick={() => onAddColorRule(index)}>
          新增规则
        </button>
      </div>
      <div className="chart-indicator-rule-list">
        {(series.color_when ?? []).map((rule, ruleIndex) => (
          <div className="chart-indicator-rule-row" key={`${rule.when}-${ruleIndex}`}>
            <input value={rule.when} onChange={(event) => onUpdateColorRule(index, ruleIndex, { when: event.target.value })} placeholder="条件表达式" />
            <input value={rule.color} onChange={(event) => onUpdateColorRule(index, ruleIndex, { color: event.target.value })} placeholder="#d9485f" />
            <button className="settings-danger-btn" type="button" onClick={() => onDeleteColorRule(index, ruleIndex)}>
              删除
            </button>
          </div>
        ))}
        {(series.color_when ?? []).length === 0 ? <div className="settings-empty-soft">未设置条件颜色。</div> : null}
      </div>
    </div>
  )
}

type MarkerEditorProps = {
  marker: ChartMarkerDraft
  index: number
  markerCount: number
  previousSeriesKeys: string[]
  databaseIndicatorColumns: string[]
  onUpdate: (index: number, patch: Partial<ChartMarkerDraft>) => void
  onMove: (index: number, direction: -1 | 1) => void
  onCopy: (index: number) => void
  onDelete: (index: number) => void
}

function MarkerEditor({
  marker,
  index,
  markerCount,
  previousSeriesKeys,
  databaseIndicatorColumns,
  onUpdate,
  onMove,
  onCopy,
  onDelete,
}: MarkerEditorProps) {
  return (
    <div className="chart-indicator-detail-form">
      <div className="chart-indicator-subsection-head">
        <strong>标记参数</strong>
        <div className="chart-indicator-inline-actions">
          <button className="settings-secondary-btn" type="button" onClick={() => onMove(index, -1)} disabled={index <= 0}>
            上移
          </button>
          <button className="settings-secondary-btn" type="button" onClick={() => onMove(index, 1)} disabled={index >= markerCount - 1}>
            下移
          </button>
          <button className="settings-secondary-btn" type="button" onClick={() => onCopy(index)}>
            复制
          </button>
          <button className="settings-danger-btn" type="button" onClick={() => onDelete(index)}>
            删除
          </button>
        </div>
      </div>
      <label className="settings-field">
        <span>标记标识</span>
        <input value={marker.key} onChange={(event) => onUpdate(index, { key: event.target.value })} />
      </label>
      <label className="settings-field">
        <span>显示名称</span>
        <input value={marker.label ?? ''} onChange={(event) => onUpdate(index, { label: optionalString(event.target.value) })} />
      </label>
      <label className="settings-field settings-field-textarea">
        <span>出现条件</span>
        <textarea className="settings-textarea chart-indicator-expr-textarea" value={marker.when} onChange={(event) => onUpdate(index, { when: event.target.value })} />
      </label>
      <ReferenceStrip previousSeriesKeys={previousSeriesKeys} databaseIndicatorColumns={databaseIndicatorColumns} />
      <div className="chart-indicator-field-grid">
        <label className="settings-field">
          <span>定位字段</span>
          <input value={marker.y ?? ''} onChange={(event) => onUpdate(index, { y: optionalString(event.target.value) })} placeholder="C / L / ma20" />
        </label>
        <label className="settings-field">
          <span>位置</span>
          <select value={marker.position ?? 'value'} onChange={(event) => onUpdate(index, { position: event.target.value as ChartMarkerPosition })}>
            <option value="above">上方</option>
            <option value="below">下方</option>
            <option value="value">数值处</option>
          </select>
        </label>
        <label className="settings-field">
          <span>形状</span>
          <select value={marker.shape ?? 'dot'} onChange={(event) => onUpdate(index, { shape: event.target.value as ChartMarkerShape })}>
            <option value="dot">圆点</option>
            <option value="triangle_up">上三角</option>
            <option value="triangle_down">下三角</option>
            <option value="flag">旗标</option>
          </select>
        </label>
        <label className="settings-field">
          <span>颜色</span>
          <input value={marker.color ?? ''} onChange={(event) => onUpdate(index, { color: optionalString(event.target.value) })} placeholder="#d9485f" />
        </label>
        <label className="settings-field">
          <span>文本</span>
          <input value={marker.text ?? ''} onChange={(event) => onUpdate(index, { text: optionalString(event.target.value) })} placeholder="B / S" />
        </label>
      </div>
    </div>
  )
}

function ReferenceStrip({
  previousSeriesKeys,
  databaseIndicatorColumns,
}: {
  previousSeriesKeys: string[]
  databaseIndicatorColumns: string[]
}) {
  return (
    <div className="chart-indicator-reference-strip">
      <span>基础字段：{BASE_FIELDS.join(' / ')}</span>
      <span>前序序列：{previousSeriesKeys.length > 0 ? previousSeriesKeys.join(' / ') : '无'}</span>
      <span>已落库指标：{databaseIndicatorColumns.length > 0 ? databaseIndicatorColumns.slice(0, 18).join(' / ') : '未读取到'}</span>
      <span>函数：{FUNCTIONS.join(' / ')}</span>
    </div>
  )
}

function normalizeConfig(config: ChartIndicatorConfigDraft | null | undefined): ChartIndicatorConfigDraft {
  const panels = (config?.panel && config.panel.length > 0 ? config.panel : [{
    key: 'price',
    label: '主K',
    role: 'main' as ChartPanelRole,
    kind: 'candles' as ChartPanelKind,
    series: [],
    marker: [],
  }]).map((panel) => ({
    ...panel,
    series: (panel.series ?? []).map((series) => normalizeSeriesByKind({ ...series, color_when: series.color_when ?? [] })),
    marker: panel.marker ?? [],
  }))
  return {
    version: config?.version ?? 1,
    panel: panels,
  }
}

function summarizeDraft(config: ChartIndicatorConfigDraft) {
  return {
    panelCount: config.panel.length,
    seriesCount: config.panel.reduce((sum, panel) => sum + (panel.series?.length ?? 0), 0),
    markerCount: config.panel.reduce((sum, panel) => sum + (panel.marker?.length ?? 0), 0),
  }
}

function validateDraft(config: ChartIndicatorConfigDraft) {
  const issues: string[] = []
  if (config.version !== 1) {
    issues.push('配置协议版本必须为 1。')
  }
  if (config.panel.length === 0) {
    issues.push('至少需要一个面板。')
  }
  const mainPanels = config.panel.filter((panel) => panel.role === 'main')
  if (mainPanels.length !== 1) {
    issues.push('必须有且只有一个主图面板。')
  }

  const panelKeys = new Set<string>()
  const seriesKeys = new Set<string>()
  config.panel.forEach((panel) => {
    if (!KEY_PATTERN.test(panel.key.trim())) {
      issues.push(`面板 ${panel.label || panel.key || '(未命名)'} 的 key 不合法。`)
    }
    if (panelKeys.has(panel.key.trim())) {
      issues.push(`面板 key 重复: ${panel.key}`)
    }
    panelKeys.add(panel.key.trim())
    if (!panel.label.trim()) {
      issues.push(`面板 ${panel.key || '(未命名)'} 缺少显示名称。`)
    }
    if (panel.role === 'main' && panel.kind !== 'candles') {
      issues.push(`主图面板 ${panel.key} 必须使用 K线图法。`)
    }
    validatePanelSeriesCombination(panel, issues)

    const markerKeys = new Set<string>()
    ;(panel.series ?? []).forEach((series) => {
      if (!KEY_PATTERN.test(series.key.trim())) {
        issues.push(`序列 ${series.label || series.key || '(未命名)'} 的 key 不合法。`)
      }
      if (seriesKeys.has(series.key.trim())) {
        issues.push(`序列 key 全局重复: ${series.key}`)
      }
      seriesKeys.add(series.key.trim())
      if (!series.expr.trim()) {
        issues.push(`序列 ${series.key || '(未命名)'} 缺少计算表达式。`)
      }
      if (series.color && !COLOR_PATTERN.test(series.color)) {
        issues.push(`序列 ${series.key} 的颜色格式不合法。`)
      }
      if (series.opacity !== null && series.opacity !== undefined && (series.opacity < 0 || series.opacity > 1)) {
        issues.push(`序列 ${series.key} 的透明度必须在 0 到 1 之间。`)
      }
      if (series.line_width !== null && series.line_width !== undefined && (series.line_width < 0.5 || series.line_width > 6)) {
        issues.push(`序列 ${series.key} 的线宽建议在 0.5 到 6 之间。`)
      }
      if (series.kind !== 'line' && series.line_width !== null && series.line_width !== undefined) {
        issues.push(`序列 ${series.key} 当前图法不会使用线宽参数。`)
      }
      if (series.kind !== 'line' && series.opacity !== null && series.opacity !== undefined) {
        issues.push(`序列 ${series.key} 当前图法不会使用透明度参数。`)
      }
      if (series.kind !== 'bar' && series.base_value !== null && series.base_value !== undefined) {
        issues.push(`序列 ${series.key} 当前图法不会使用基线值参数。`)
      }
      ;(series.color_when ?? []).forEach((rule, index) => {
        if (!rule.when.trim()) {
          issues.push(`序列 ${series.key} 的第 ${index + 1} 条条件颜色缺少条件。`)
        }
        if (!COLOR_PATTERN.test(rule.color)) {
          issues.push(`序列 ${series.key} 的第 ${index + 1} 条条件颜色格式不合法。`)
        }
      })
    })
    ;(panel.marker ?? []).forEach((marker) => {
      if (!KEY_PATTERN.test(marker.key.trim())) {
        issues.push(`标记 ${marker.label || marker.key || '(未命名)'} 的 key 不合法。`)
      }
      if (markerKeys.has(marker.key.trim())) {
        issues.push(`面板 ${panel.key} 内标记 key 重复: ${marker.key}`)
      }
      markerKeys.add(marker.key.trim())
      if (!marker.when.trim()) {
        issues.push(`标记 ${marker.key || '(未命名)'} 缺少出现条件。`)
      }
      if (marker.y && !KEY_PATTERN.test(marker.y.trim())) {
        issues.push(`标记 ${marker.key} 的定位字段不合法。`)
      }
      if (marker.color && !COLOR_PATTERN.test(marker.color)) {
        issues.push(`标记 ${marker.key} 的颜色格式不合法。`)
      }
    })
  })
  return issues
}

function validatePanelSeriesCombination(panel: ChartPanelDraft, issues: string[]) {
  const series = panel.series ?? []
  if (panel.role === 'main' && series.some((item) => item.kind !== 'line')) {
    issues.push(`主图 ${panel.key} 只能叠加折线序列。`)
    return
  }
  const brickCount = series.filter((item) => item.kind === 'brick').length
  if (brickCount > 0 && (series.length !== 1 || brickCount !== 1)) {
    issues.push(`砖型序列在面板 ${panel.key} 中必须单独使用。`)
  }
}

function serializeConfigToToml(config: ChartIndicatorConfigDraft) {
  const lines: string[] = ['version = 1', '']
  config.panel.forEach((panel) => {
    lines.push('[[panel]]')
    lines.push(`key = ${tomlString(panel.key.trim())}`)
    lines.push(`label = ${tomlString(panel.label.trim())}`)
    lines.push(`role = ${tomlString(panel.role)}`)
    lines.push(`kind = ${tomlString(inferPanelKind(panel))}`)
    lines.push('')
    ;(panel.series ?? []).forEach((series) => {
      lines.push('[[panel.series]]')
      lines.push(`key = ${tomlString(series.key.trim())}`)
      if (series.label?.trim()) {
        lines.push(`label = ${tomlString(series.label.trim())}`)
      }
      lines.push(`expr = ${tomlString(series.expr)}`)
      lines.push(`kind = ${tomlString(series.kind)}`)
      if (series.color?.trim()) {
        lines.push(`color = ${tomlString(series.color.trim())}`)
      }
      if (series.line_width !== null && series.line_width !== undefined) {
        lines.push(`line_width = ${formatNumber(series.line_width)}`)
      }
      if (series.opacity !== null && series.opacity !== undefined) {
        lines.push(`opacity = ${formatNumber(series.opacity)}`)
      }
      if (series.base_value !== null && series.base_value !== undefined) {
        lines.push(`base_value = ${formatNumber(series.base_value)}`)
      }
      if ((series.color_when ?? []).length > 0) {
        lines.push('color_when = [')
        ;(series.color_when ?? []).forEach((rule) => {
          lines.push(`  { when = ${tomlString(rule.when)}, color = ${tomlString(rule.color.trim())} },`)
        })
        lines.push(']')
      }
      lines.push('')
    })
    ;(panel.marker ?? []).forEach((marker) => {
      lines.push('[[panel.marker]]')
      lines.push(`key = ${tomlString(marker.key.trim())}`)
      if (marker.label?.trim()) {
        lines.push(`label = ${tomlString(marker.label.trim())}`)
      }
      lines.push(`when = ${tomlString(marker.when)}`)
      if (marker.y?.trim()) {
        lines.push(`y = ${tomlString(marker.y.trim())}`)
      }
      if (marker.position) {
        lines.push(`position = ${tomlString(marker.position)}`)
      }
      if (marker.shape) {
        lines.push(`shape = ${tomlString(marker.shape)}`)
      }
      if (marker.color?.trim()) {
        lines.push(`color = ${tomlString(marker.color.trim())}`)
      }
      if (marker.text?.trim()) {
        lines.push(`text = ${tomlString(marker.text.trim())}`)
      }
      lines.push('')
    })
  })
  return lines.join('\n').trimEnd() + '\n'
}

function tomlString(value: string) {
  return JSON.stringify(value)
}

function formatNumber(value: number) {
  return Number.isInteger(value) ? String(value) : String(Number(value.toFixed(6)))
}

function optionalString(value: string) {
  const trimmed = value.trim()
  return trimmed ? value : null
}

function optionalNumber(value: string) {
  if (!value.trim()) {
    return null
  }
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : null
}

function collectSeriesKeys(config: ChartIndicatorConfigDraft) {
  return new Set(config.panel.flatMap((panel) => (panel.series ?? []).map((series) => series.key)))
}

function uniqueKey(base: string, usedKeys: Set<string>) {
  const normalizedBase = KEY_PATTERN.test(base) ? base : 'item'
  if (!usedKeys.has(normalizedBase)) {
    usedKeys.add(normalizedBase)
    return normalizedBase
  }
  let index = 2
  while (usedKeys.has(`${normalizedBase}${index}`)) {
    index += 1
  }
  const key = `${normalizedBase}${index}`
  usedKeys.add(key)
  return key
}

function clonePanelWithFreshKeys(panel: ChartPanelDraft, usedPanelKeys: Set<string>, usedSeriesKeys: Set<string>) {
  const key = uniqueKey(panel.key, usedPanelKeys)
  const clonedPanel: ChartPanelDraft = {
    ...panel,
    key,
    label: panel.label,
    series: (panel.series ?? []).map((series) => ({
      ...series,
      key: uniqueKey(series.key, usedSeriesKeys),
      color_when: (series.color_when ?? []).map((rule) => ({ ...rule })),
    })),
    marker: (panel.marker ?? []).map((marker) => ({ ...marker })),
  }
  return clonedPanel
}

function makeBlankSeries(usedKeys: Set<string>): ChartSeriesDraft {
  return {
    key: uniqueKey('series', usedKeys),
    label: null,
    expr: '',
    kind: 'line',
  }
}

function normalizeSeriesByKind(series: ChartSeriesDraft): ChartSeriesDraft {
  if (series.kind === 'line') {
    return {
      ...series,
      base_value: null,
    }
  }
  if (series.kind === 'bar') {
    return {
      ...series,
      line_width: null,
      opacity: null,
    }
  }
  if (series.kind === 'brick') {
    return {
      ...series,
      line_width: null,
      opacity: null,
      base_value: null,
    }
  }
  return series
}

function getAllowedSeriesKinds(panel: ChartPanelDraft | undefined): ChartSeriesKind[] {
  if (!panel) {
    return ['line']
  }
  if (panel.role === 'main') {
    return ['line']
  }
  return ['line', 'bar', 'brick']
}

function seriesKindLabel(kind: ChartSeriesKind) {
  if (kind === 'line') {
    return '折线'
  }
  if (kind === 'bar') {
    return '柱体'
  }
  if (kind === 'brick') {
    return '砖体'
  }
  return kind
}

function panelKindLabel(kind: ChartPanelKind) {
  void kind
  return '副图'
}

function inferPanelKind(panel: ChartPanelDraft): ChartPanelKind {
  if (panel.role === 'main') {
    return 'candles'
  }
  if ((panel.series ?? []).some((series) => series.kind === 'brick')) {
    return 'brick'
  }
  if ((panel.series ?? []).some((series) => series.kind === 'bar')) {
    return 'bar'
  }
  return 'line'
}

function insertAt<T>(items: T[], index: number, item: T) {
  return [...items.slice(0, index), item, ...items.slice(index)]
}

function moveItem<T>(items: T[], index: number, direction: -1 | 1) {
  const nextIndex = index + direction
  if (nextIndex < 0 || nextIndex >= items.length) {
    return items
  }
  const nextItems = [...items]
  const [item] = nextItems.splice(index, 1)
  nextItems.splice(nextIndex, 0, item)
  return nextItems
}
