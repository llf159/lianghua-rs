import { useDeferredValue, useEffect, useMemo, useState } from 'react'
import { ensureManagedSourcePath } from '../../apis/managedSource'
import { getStockPickOptions } from '../../apis/stockPick'
import { filterConceptItems, useConceptExclusions } from '../../shared/conceptExclusions'
import {
  CHART_RANK_MARKER_THRESHOLD_MAX,
  CHART_RANK_MARKER_THRESHOLD_MIN,
  DETAILS_NAV_LONG_PRESS_INTERVAL_SECONDS_MAX,
  DETAILS_NAV_LONG_PRESS_INTERVAL_SECONDS_MIN,
  CHART_INDICATOR_WIDTH_RATIO_MAX,
  CHART_INDICATOR_WIDTH_RATIO_MIN,
  CHART_MAIN_WIDTH_RATIO_MAX,
  CHART_MAIN_WIDTH_RATIO_MIN,
  clampChartRankMarkerThreshold,
  clampChartIndicatorWidthRatio,
  clampChartMainWidthRatio,
  clampDetailsNavLongPressIntervalSeconds,
  readStoredChartRankMarkerThreshold,
  readStoredChartIndicatorWidthRatio,
  readStoredChartMainWidthRatio,
  readStoredDetailsNavLongPressIntervalSeconds,
  writeStoredChartRankMarkerThreshold,
  writeStoredChartIndicatorWidthRatio,
  writeStoredChartMainWidthRatio,
  writeStoredDetailsNavLongPressIntervalSeconds,
} from '../../shared/chartSettings'
import {
  BACKTEST_IC_THRESHOLD_DEFAULT,
  BACKTEST_IR_THRESHOLD_DEFAULT,
  BACKTEST_T_THRESHOLD_DEFAULT,
  readStoredBacktestHighlightSettings,
  type BacktestHighlightSettings,
  writeStoredBacktestHighlightSettings,
} from '../../shared/backtestHighlightSettings'
import './css/DataImportPage.css'
import './css/StockPickPage.css'
import './css/DetailsPage.css'

const AUTOCOMPLETE_LIMIT = 12
const RATIO_INPUT_STEP = 0.01

type SettingsModalType =
  | 'concept'
  | 'st'
  | 'chart-layout'
  | 'rank-marker'
  | 'details-nav-long-press'
  | 'backtest-highlight'
  | null

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
  const [activeModal, setActiveModal] = useState<SettingsModalType>(null)
  const [chartMainRatioInput, setChartMainRatioInput] = useState(() =>
    readStoredChartMainWidthRatio().toFixed(2),
  )
  const [chartIndicatorRatioInput, setChartIndicatorRatioInput] = useState(() =>
    readStoredChartIndicatorWidthRatio().toFixed(2),
  )
  const [chartLayoutSettingError, setChartLayoutSettingError] = useState('')
  const [chartLayoutSettingNotice, setChartLayoutSettingNotice] = useState('')
  const [chartRankMarkerThresholdInput, setChartRankMarkerThresholdInput] = useState(() =>
    String(readStoredChartRankMarkerThreshold()),
  )
  const [chartRankMarkerSettingError, setChartRankMarkerSettingError] = useState('')
  const [chartRankMarkerSettingNotice, setChartRankMarkerSettingNotice] = useState('')
  const [detailsNavLongPressIntervalInput, setDetailsNavLongPressIntervalInput] = useState(
    () => String(readStoredDetailsNavLongPressIntervalSeconds()),
  )
  const [detailsNavLongPressSettingError, setDetailsNavLongPressSettingError] = useState('')
  const [detailsNavLongPressSettingNotice, setDetailsNavLongPressSettingNotice] = useState('')
  const [backtestHighlightIcThresholdInput, setBacktestHighlightIcThresholdInput] = useState(
    () => String(readStoredBacktestHighlightSettings().icThreshold),
  )
  const [backtestHighlightIrThresholdInput, setBacktestHighlightIrThresholdInput] = useState(
    () => String(readStoredBacktestHighlightSettings().irThreshold),
  )
  const [backtestHighlightTThresholdInput, setBacktestHighlightTThresholdInput] = useState(
    () => String(readStoredBacktestHighlightSettings().tThreshold),
  )
  const [backtestHighlightIcUseAbs, setBacktestHighlightIcUseAbs] = useState(
    () => readStoredBacktestHighlightSettings().icUseAbs,
  )
  const [backtestHighlightIrUseAbs, setBacktestHighlightIrUseAbs] = useState(
    () => readStoredBacktestHighlightSettings().irUseAbs,
  )
  const [backtestHighlightTUseAbs, setBacktestHighlightTUseAbs] = useState(
    () => readStoredBacktestHighlightSettings().tUseAbs,
  )
  const [backtestHighlightSettingError, setBacktestHighlightSettingError] = useState('')
  const [backtestHighlightSettingNotice, setBacktestHighlightSettingNotice] = useState('')
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const deferredConceptKeyword = useDeferredValue(conceptKeyword)
  const isConceptEditorOpen = activeModal === 'concept'
  const isStSettingOpen = activeModal === 'st'
  const isChartLayoutSettingOpen = activeModal === 'chart-layout'
  const isRankMarkerSettingOpen = activeModal === 'rank-marker'
  const isDetailsNavLongPressSettingOpen = activeModal === 'details-nav-long-press'
  const isBacktestHighlightSettingOpen = activeModal === 'backtest-highlight'

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
    if (!activeModal) {
      return
    }

    function onKeyDown(event: KeyboardEvent) {
      if (event.key === 'Escape') {
        setActiveModal(null)
        setConceptKeyword('')
        setLookupFocused(false)
      }
    }

    window.addEventListener('keydown', onKeyDown)
    return () => window.removeEventListener('keydown', onKeyDown)
  }, [activeModal])

  const chartMainWidthRatioPreview = useMemo(() => {
    const parsedValue = Number(chartMainRatioInput.trim())
    if (!Number.isFinite(parsedValue)) {
      return null
    }

    return clampChartMainWidthRatio(parsedValue)
  }, [chartMainRatioInput])

  const chartIndicatorWidthRatioPreview = useMemo(() => {
    const parsedValue = Number(chartIndicatorRatioInput.trim())
    if (!Number.isFinite(parsedValue)) {
      return null
    }

    return clampChartIndicatorWidthRatio(parsedValue)
  }, [chartIndicatorRatioInput])

  const currentChartMainWidthRatio = readStoredChartMainWidthRatio()
  const currentChartIndicatorWidthRatio = readStoredChartIndicatorWidthRatio()
  const currentChartRankMarkerThreshold = readStoredChartRankMarkerThreshold()
  const currentDetailsNavLongPressInterval = readStoredDetailsNavLongPressIntervalSeconds()
  const currentBacktestHighlightSettings = readStoredBacktestHighlightSettings()
  const chartRankMarkerThresholdPreview = useMemo(() => {
    const parsedValue = Number(chartRankMarkerThresholdInput.trim())
    if (!Number.isFinite(parsedValue)) {
      return null
    }

    return clampChartRankMarkerThreshold(parsedValue)
  }, [chartRankMarkerThresholdInput])
  const detailsNavLongPressIntervalPreview = useMemo(() => {
    const parsedValue = Number(detailsNavLongPressIntervalInput.trim())
    if (!Number.isFinite(parsedValue)) {
      return null
    }

    return clampDetailsNavLongPressIntervalSeconds(parsedValue)
  }, [detailsNavLongPressIntervalInput])

  function openConceptEditor() {
    setActiveModal('concept')
    setConceptKeyword('')
    setLookupFocused(false)
  }

  function openStSetting() {
    setActiveModal('st')
  }

  function openChartLayoutSetting() {
    setActiveModal('chart-layout')
    setChartMainRatioInput(readStoredChartMainWidthRatio().toFixed(2))
    setChartIndicatorRatioInput(readStoredChartIndicatorWidthRatio().toFixed(2))
    setChartLayoutSettingError('')
    setChartLayoutSettingNotice('')
  }

  function openRankMarkerSetting() {
    setActiveModal('rank-marker')
    setChartRankMarkerThresholdInput(String(readStoredChartRankMarkerThreshold()))
    setChartRankMarkerSettingError('')
    setChartRankMarkerSettingNotice('')
  }

  function openDetailsNavLongPressSetting() {
    setActiveModal('details-nav-long-press')
    setDetailsNavLongPressIntervalInput(String(readStoredDetailsNavLongPressIntervalSeconds()))
    setDetailsNavLongPressSettingError('')
    setDetailsNavLongPressSettingNotice('')
  }

  function openBacktestHighlightSetting() {
    const currentSettings = readStoredBacktestHighlightSettings()
    setActiveModal('backtest-highlight')
    setBacktestHighlightIcThresholdInput(String(currentSettings.icThreshold))
    setBacktestHighlightIrThresholdInput(String(currentSettings.irThreshold))
    setBacktestHighlightTThresholdInput(String(currentSettings.tThreshold))
    setBacktestHighlightIcUseAbs(currentSettings.icUseAbs)
    setBacktestHighlightIrUseAbs(currentSettings.irUseAbs)
    setBacktestHighlightTUseAbs(currentSettings.tUseAbs)
    setBacktestHighlightSettingError('')
    setBacktestHighlightSettingNotice('')
  }

  function closeActiveModal() {
    setActiveModal(null)
    setConceptKeyword('')
    setLookupFocused(false)
  }

  function onSaveChartLayoutRatios() {
    const parsedMainValue = Number(chartMainRatioInput.trim())
    const parsedIndicatorValue = Number(chartIndicatorRatioInput.trim())
    if (!Number.isFinite(parsedMainValue) || !Number.isFinite(parsedIndicatorValue)) {
      setChartLayoutSettingError('请输入有效数字。')
      setChartLayoutSettingNotice('')
      return
    }

    const normalizedMainValue = clampChartMainWidthRatio(parsedMainValue)
    const normalizedIndicatorValue = clampChartIndicatorWidthRatio(parsedIndicatorValue)
    writeStoredChartMainWidthRatio(normalizedMainValue)
    writeStoredChartIndicatorWidthRatio(normalizedIndicatorValue)
    setChartMainRatioInput(normalizedMainValue.toFixed(2))
    setChartIndicatorRatioInput(normalizedIndicatorValue.toFixed(2))
    setChartLayoutSettingError('')
    setChartLayoutSettingNotice('已保存。切回详情页后会使用新比例。')
  }

  function onSaveChartRankMarkerThreshold() {
    const parsedValue = Number(chartRankMarkerThresholdInput.trim())
    if (!Number.isFinite(parsedValue)) {
      setChartRankMarkerSettingError('请输入有效整数。')
      setChartRankMarkerSettingNotice('')
      return
    }

    const normalizedValue = clampChartRankMarkerThreshold(parsedValue)
    writeStoredChartRankMarkerThreshold(normalizedValue)
    setChartRankMarkerThresholdInput(String(normalizedValue))
    setChartRankMarkerSettingError('')
    setChartRankMarkerSettingNotice('已保存。详情页主图会按该排名阈值标记。')
  }

  function onSaveDetailsNavLongPressInterval() {
    const parsedValue = Number(detailsNavLongPressIntervalInput.trim())
    if (!Number.isFinite(parsedValue)) {
      setDetailsNavLongPressSettingError('请输入有效秒数。')
      setDetailsNavLongPressSettingNotice('')
      return
    }

    const normalizedValue = clampDetailsNavLongPressIntervalSeconds(parsedValue)
    writeStoredDetailsNavLongPressIntervalSeconds(normalizedValue)
    setDetailsNavLongPressIntervalInput(String(normalizedValue))
    setDetailsNavLongPressSettingError('')
    setDetailsNavLongPressSettingNotice('已保存。详情页长按上一条/下一条会按该间隔自动切换。')
  }

  function onSaveBacktestHighlightSettings() {
    const parsedIcThreshold = Number(backtestHighlightIcThresholdInput.trim())
    const parsedIrThreshold = Number(backtestHighlightIrThresholdInput.trim())
    const parsedTThreshold = Number(backtestHighlightTThresholdInput.trim())
    if (
      !Number.isFinite(parsedIcThreshold) ||
      !Number.isFinite(parsedIrThreshold) ||
      !Number.isFinite(parsedTThreshold)
    ) {
      setBacktestHighlightSettingError('阈值请输入有效数字。')
      setBacktestHighlightSettingNotice('')
      return
    }
    if (parsedIcThreshold < 0 || parsedIrThreshold < 0 || parsedTThreshold < 0) {
      setBacktestHighlightSettingError('阈值必须 >= 0。')
      setBacktestHighlightSettingNotice('')
      return
    }

    const nextSettings: BacktestHighlightSettings = {
      icThreshold: parsedIcThreshold,
      icUseAbs: backtestHighlightIcUseAbs,
      irThreshold: parsedIrThreshold,
      irUseAbs: backtestHighlightIrUseAbs,
      tThreshold: parsedTThreshold,
      tUseAbs: backtestHighlightTUseAbs,
    }
    writeStoredBacktestHighlightSettings(nextSettings)
    setBacktestHighlightIcThresholdInput(String(nextSettings.icThreshold))
    setBacktestHighlightIrThresholdInput(String(nextSettings.irThreshold))
    setBacktestHighlightTThresholdInput(String(nextSettings.tThreshold))
    setBacktestHighlightSettingError('')
    setBacktestHighlightSettingNotice('已保存。场景/策略回测页面会按新阈值高亮。')
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
            <p className="settings-section-note">每项设置单独编辑，点击条目打开对应设置弹窗。</p>
          </div>
        </div>

        <div className="settings-list">
          <button className="settings-list-item" type="button" onClick={openConceptEditor}>
            <div className="settings-list-item-main">
              <strong>概念排除</strong>
              <span>已排除 {excludedConcepts.length} 项，影响选股页概念过滤。</span>
            </div>
            <span className="settings-list-item-value">编辑</span>
          </button>

          <button className="settings-list-item" type="button" onClick={openStSetting}>
            <div className="settings-list-item-main">
              <strong>ST 排除</strong>
              <span>控制主板筛选是否自动排除 ST 板块。</span>
            </div>
            <span className="settings-list-item-value">{excludeStBoard ? '已开启' : '未开启'}</span>
          </button>

          <button className="settings-list-item" type="button" onClick={openChartLayoutSetting}>
            <div className="settings-list-item-main">
              <strong>图表高度比例</strong>
              <span>统一设置主图区与指标区高度比例。</span>
            </div>
            <span className="settings-list-item-value">
              主 {currentChartMainWidthRatio.toFixed(2)} / 指标 {currentChartIndicatorWidthRatio.toFixed(2)}
            </span>
          </button>

          <button className="settings-list-item" type="button" onClick={openRankMarkerSetting}>
            <div className="settings-list-item-main">
              <strong>标记阈值排名</strong>
              <span>详情页主图中，排名进入阈值时在当日K线上方做标记。</span>
            </div>
            <span className="settings-list-item-value">TOP {currentChartRankMarkerThreshold}</span>
          </button>

          <button className="settings-list-item" type="button" onClick={openDetailsNavLongPressSetting}>
            <div className="settings-list-item-main">
              <strong>详情长按切换间隔</strong>
              <span>长按详情页上一条/下一条时，按该秒数自动切换。</span>
            </div>
            <span className="settings-list-item-value">{currentDetailsNavLongPressInterval} 秒</span>
          </button>

          <button className="settings-list-item" type="button" onClick={openBacktestHighlightSetting}>
            <div className="settings-list-item-main">
              <strong>回测指标高亮阈值</strong>
              <span>配置 IC / IR / t 的阈值，以及是否按绝对值比较。</span>
            </div>
            <span className="settings-list-item-value">
              IC {currentBacktestHighlightSettings.icThreshold} · IR {currentBacktestHighlightSettings.irThreshold} · t {currentBacktestHighlightSettings.tThreshold}
            </span>
          </button>
        </div>

        {error && !activeModal ? <div className="settings-error">{error}</div> : null}
      </section>

      {isStSettingOpen ? (
        <div
          className="settings-modal-backdrop"
          role="presentation"
          onClick={(event) => {
            if (event.target === event.currentTarget) {
              closeActiveModal()
            }
          }}
        >
          <section className="settings-modal settings-modal-narrow" role="dialog" aria-modal="true" aria-label="ST 排除设置">
            <div className="settings-modal-head">
              <div>
                <h3 className="settings-subtitle-head">ST 排除</h3>
                <p className="settings-section-note">开启后，选股与榜单中的板块筛选会自动排除 ST。</p>
              </div>
              <div className="settings-actions">
                <button
                  className={excludeStBoard ? 'settings-secondary-btn is-active' : 'settings-secondary-btn'}
                  type="button"
                  onClick={() => setExcludeStBoard(!excludeStBoard)}
                >
                  {excludeStBoard ? '已开启' : '未开启'}
                </button>
                <button className="settings-primary-btn" type="button" onClick={closeActiveModal}>
                  完成
                </button>
              </div>
            </div>
          </section>
        </div>
      ) : null}

      {isChartLayoutSettingOpen ? (
        <div
          className="settings-modal-backdrop"
          role="presentation"
          onClick={(event) => {
            if (event.target === event.currentTarget) {
              closeActiveModal()
            }
          }}
        >
          <section className="settings-modal settings-modal-narrow" role="dialog" aria-modal="true" aria-label="图表高度比例设置">
            <div className="settings-modal-head">
              <div>
                <h3 className="settings-subtitle-head">图表高度比例</h3>
                <p className="settings-section-note">
                  主图区与指标区分开设置，保存时会同时写入两项配置。
                </p>
              </div>
              <div className="settings-actions">
                <button className="settings-secondary-btn" type="button" onClick={closeActiveModal}>
                  关闭
                </button>
              </div>
            </div>

            <div className="settings-field settings-field-textarea">
              <span>CHART_MAIN_WIDTH_RATIO</span>
              <input
                type="number"
                min={CHART_MAIN_WIDTH_RATIO_MIN}
                max={CHART_MAIN_WIDTH_RATIO_MAX}
                step={RATIO_INPUT_STEP}
                value={chartMainRatioInput}
                onChange={(event) => setChartMainRatioInput(event.target.value)}
              />
              <small>
                主图区预览：{chartMainWidthRatioPreview === null ? '--' : chartMainWidthRatioPreview.toFixed(2)}
              </small>
            </div>

            <div className="settings-field settings-field-textarea">
              <span>CHART_INDICATOR_WIDTH_RATIO</span>
              <input
                type="number"
                min={CHART_INDICATOR_WIDTH_RATIO_MIN}
                max={CHART_INDICATOR_WIDTH_RATIO_MAX}
                step={RATIO_INPUT_STEP}
                value={chartIndicatorRatioInput}
                onChange={(event) => setChartIndicatorRatioInput(event.target.value)}
              />
              <small>
                指标区预览：{chartIndicatorWidthRatioPreview === null ? '--' : chartIndicatorWidthRatioPreview.toFixed(2)}
              </small>
            </div>

            {chartLayoutSettingError ? <div className="settings-error">{chartLayoutSettingError}</div> : null}
            {chartLayoutSettingNotice ? <div className="settings-notice">{chartLayoutSettingNotice}</div> : null}

            <div className="settings-actions">
              <button className="settings-primary-btn" type="button" onClick={onSaveChartLayoutRatios}>
                保存
              </button>
            </div>
          </section>
        </div>
      ) : null}

      {isRankMarkerSettingOpen ? (
        <div
          className="settings-modal-backdrop"
          role="presentation"
          onClick={(event) => {
            if (event.target === event.currentTarget) {
              closeActiveModal()
            }
          }}
        >
          <section className="settings-modal settings-modal-narrow" role="dialog" aria-modal="true" aria-label="标记阈值排名设置">
            <div className="settings-modal-head">
              <div>
                <h3 className="settings-subtitle-head">标记阈值排名</h3>
                <p className="settings-section-note">
                  当日排名小于等于该阈值时，会在详情页主图K线顶部显示标记。
                </p>
              </div>
              <div className="settings-actions">
                <button className="settings-secondary-btn" type="button" onClick={closeActiveModal}>
                  关闭
                </button>
              </div>
            </div>

            <div className="settings-field settings-field-textarea">
              <span>RANK_MARKER_THRESHOLD</span>
              <input
                type="number"
                min={CHART_RANK_MARKER_THRESHOLD_MIN}
                max={CHART_RANK_MARKER_THRESHOLD_MAX}
                step={1}
                value={chartRankMarkerThresholdInput}
                onChange={(event) => setChartRankMarkerThresholdInput(event.target.value)}
              />
              <small>
                预览：{chartRankMarkerThresholdPreview === null ? '--' : `TOP ${chartRankMarkerThresholdPreview}`}
              </small>
            </div>

            {chartRankMarkerSettingError ? <div className="settings-error">{chartRankMarkerSettingError}</div> : null}
            {chartRankMarkerSettingNotice ? <div className="settings-notice">{chartRankMarkerSettingNotice}</div> : null}

            <div className="settings-actions">
              <button className="settings-primary-btn" type="button" onClick={onSaveChartRankMarkerThreshold}>
                保存
              </button>
            </div>
          </section>
        </div>
      ) : null}

      {isDetailsNavLongPressSettingOpen ? (
        <div
          className="settings-modal-backdrop"
          role="presentation"
          onClick={(event) => {
            if (event.target === event.currentTarget) {
              closeActiveModal()
            }
          }}
        >
          <section className="settings-modal settings-modal-narrow" role="dialog" aria-modal="true" aria-label="详情长按切换间隔设置">
            <div className="settings-modal-head">
              <div>
                <h3 className="settings-subtitle-head">详情长按切换间隔</h3>
                <p className="settings-section-note">
                  单位秒。详情页长按“上一条 / 下一条”进入锁定后，会按该间隔自动切换。
                </p>
              </div>
              <div className="settings-actions">
                <button className="settings-secondary-btn" type="button" onClick={closeActiveModal}>
                  关闭
                </button>
              </div>
            </div>

            <div className="settings-field settings-field-textarea">
              <span>DETAILS_NAV_LONG_PRESS_INTERVAL_SECONDS</span>
              <input
                type="number"
                min={DETAILS_NAV_LONG_PRESS_INTERVAL_SECONDS_MIN}
                max={DETAILS_NAV_LONG_PRESS_INTERVAL_SECONDS_MAX}
                step={0.1}
                value={detailsNavLongPressIntervalInput}
                onChange={(event) => setDetailsNavLongPressIntervalInput(event.target.value)}
              />
              <small>
                预览：{detailsNavLongPressIntervalPreview === null ? '--' : `${detailsNavLongPressIntervalPreview} 秒`}
              </small>
            </div>

            {detailsNavLongPressSettingError ? <div className="settings-error">{detailsNavLongPressSettingError}</div> : null}
            {detailsNavLongPressSettingNotice ? <div className="settings-notice">{detailsNavLongPressSettingNotice}</div> : null}

            <div className="settings-actions">
              <button className="settings-primary-btn" type="button" onClick={onSaveDetailsNavLongPressInterval}>
                保存
              </button>
            </div>
          </section>
        </div>
      ) : null}

      {isBacktestHighlightSettingOpen ? (
        <div
          className="settings-modal-backdrop"
          role="presentation"
          onClick={(event) => {
            if (event.target === event.currentTarget) {
              closeActiveModal()
            }
          }}
        >
          <section className="settings-modal settings-modal-narrow" role="dialog" aria-modal="true" aria-label="回测高亮阈值设置">
            <div className="settings-modal-head">
              <div>
                <h3 className="settings-subtitle-head">回测高亮阈值</h3>
                <p className="settings-section-note">
                  分别设置 IC / IR / t 的高亮阈值，并可单独控制是否按绝对值判断。
                </p>
              </div>
              <div className="settings-actions">
                <button className="settings-secondary-btn" type="button" onClick={closeActiveModal}>
                  关闭
                </button>
              </div>
            </div>

            <div className="settings-backtest-highlight-grid">
              <div className="settings-backtest-highlight-item">
                <label className="settings-field">
                  <span>IC 阈值（默认 {BACKTEST_IC_THRESHOLD_DEFAULT}）</span>
                  <input
                    type="number"
                    min={0}
                    step="any"
                    value={backtestHighlightIcThresholdInput}
                    onChange={(event) => setBacktestHighlightIcThresholdInput(event.target.value)}
                  />
                </label>
                <label className="settings-checkbox-inline">
                  <input
                    type="checkbox"
                    checked={backtestHighlightIcUseAbs}
                    onChange={(event) => setBacktestHighlightIcUseAbs(event.target.checked)}
                  />
                  <span>按绝对值比较</span>
                </label>
              </div>

              <div className="settings-backtest-highlight-item">
                <label className="settings-field">
                  <span>IR 阈值（默认 {BACKTEST_IR_THRESHOLD_DEFAULT}）</span>
                  <input
                    type="number"
                    min={0}
                    step="any"
                    value={backtestHighlightIrThresholdInput}
                    onChange={(event) => setBacktestHighlightIrThresholdInput(event.target.value)}
                  />
                </label>
                <label className="settings-checkbox-inline">
                  <input
                    type="checkbox"
                    checked={backtestHighlightIrUseAbs}
                    onChange={(event) => setBacktestHighlightIrUseAbs(event.target.checked)}
                  />
                  <span>按绝对值比较</span>
                </label>
              </div>

              <div className="settings-backtest-highlight-item">
                <label className="settings-field">
                  <span>t 阈值（默认 {BACKTEST_T_THRESHOLD_DEFAULT}）</span>
                  <input
                    type="number"
                    min={0}
                    step="any"
                    value={backtestHighlightTThresholdInput}
                    onChange={(event) => setBacktestHighlightTThresholdInput(event.target.value)}
                  />
                </label>
                <label className="settings-checkbox-inline">
                  <input
                    type="checkbox"
                    checked={backtestHighlightTUseAbs}
                    onChange={(event) => setBacktestHighlightTUseAbs(event.target.checked)}
                  />
                  <span>按绝对值比较</span>
                </label>
              </div>
            </div>

            {backtestHighlightSettingError ? <div className="settings-error">{backtestHighlightSettingError}</div> : null}
            {backtestHighlightSettingNotice ? <div className="settings-notice">{backtestHighlightSettingNotice}</div> : null}

            <div className="settings-actions">
              <button className="settings-primary-btn" type="button" onClick={onSaveBacktestHighlightSettings}>
                保存
              </button>
            </div>
          </section>
        </div>
      ) : null}

      {isConceptEditorOpen ? (
        <div
          className="settings-modal-backdrop"
          role="presentation"
          onClick={(event) => {
            if (event.target === event.currentTarget) {
              closeActiveModal()
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
                <button className="settings-primary-btn" type="button" onClick={closeActiveModal}>
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

              <div className="settings-section-head settings-section-head-loose">
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
