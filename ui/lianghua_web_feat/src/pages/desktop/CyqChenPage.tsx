import { useEffect, useMemo, useState, type PointerEvent as ReactPointerEvent } from 'react'
import {
  getCyqChenStrategyPage,
  runCyqChenSingleStockTest,
  type CyqChenBin,
  type CyqChenKlineRow,
  type CyqChenSingleStockData,
  type CyqChenSnapshot,
  type CyqChenStrategyPageData,
  type CyqChenStrategyDraft,
} from '../../apis/cyqChen'
import { ensureManagedSourcePath } from '../../apis/managedSource'
import './css/DetailsPage.css'
import './css/CyqChenPage.css'

const DEFAULT_STRATEGIES: CyqChenStrategyDraft[] = [
  {
    name: '主力低位承接',
    holder: 'main',
    direction: 'buy',
    when: 'RATEL < -0.08 AND C > O',
    bias: 1.5,
  },
  {
    name: '散户追高买入',
    holder: 'retail',
    direction: 'buy',
    when: 'RATEC > 0.05 AND C >= H * 0.98',
    bias: 1.2,
  },
  {
    name: '散户获利卖出',
    holder: 'retail',
    direction: 'sell',
    when: 'RATEH > 0.12',
    bias: 1.0,
  },
  {
    name: '主力高位派发',
    holder: 'main',
    direction: 'sell',
    when: 'RATEC > 0.2 AND C < O',
    bias: -0.6,
  },
]

type SavedRun = {
  id: string
  label: string
  data: CyqChenSingleStockData
}

type CyqChenChartFocus = {
  visibleIndex: number
  cursorYPercent: number
  pinned: boolean
}

type ChipPeakMode = 'total' | 'main' | 'retail'

const DEFAULT_VISIBLE_BARS = 90
const MIN_VISIBLE_BARS = 20
const CHART_MIN_RIGHT_ALIGNED_SLOTS = 60
const CHART_VIEWBOX_WIDTH = 1120
const CHART_VIEWBOX_HEIGHT = 240
const CHART_MARGIN = { top: 12, right: 8, bottom: 28, left: 52 }
const CHART_DATE_TICK_COUNT = 6
const CHART_CYQ_PANEL_WIDTH_RATIO = 0.22
const CHART_CYQ_PANEL_GAP = 12
const CANDLE_UP_COLOR = '#d9485f'
const CANDLE_DOWN_COLOR = '#178f68'
const CANDLE_FLAT_COLOR = '#536273'
const CHIP_COLOR_MAIN_PROFIT = '#2563eb'
const CHIP_COLOR_MAIN_TRAPPED = '#7c3aed'
const CHIP_COLOR_RETAIL_PROFIT = '#f59e0b'
const CHIP_COLOR_RETAIL_TRAPPED = '#dc2626'

function cloneDefaultStrategies() {
  return DEFAULT_STRATEGIES.map((strategy) => ({ ...strategy }))
}

function formatNumber(value: number | null | undefined, digits = 2) {
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    return '--'
  }
  return value.toFixed(digits)
}

function normalizeDateInput(value: string) {
  return value.trim().replaceAll('-', '')
}

function isFiniteNumber(value: unknown): value is number {
  return typeof value === 'number' && Number.isFinite(value)
}

function clampNumber(value: number, min: number, max: number) {
  if (max < min) {
    return min
  }
  return Math.min(Math.max(value, min), max)
}

function buildDomain(values: number[]) {
  if (values.length === 0) {
    return null
  }

  const min = Math.min(...values)
  const max = Math.max(...values)
  if (min === max) {
    const padding = Math.max(Math.abs(min) * 0.08, 1)
    return { min: min - padding, max: max + padding }
  }

  const span = max - min
  return {
    min: min - span * 0.08,
    max: max + span * 0.08,
  }
}

function formatAxisValue(value: number) {
  const abs = Math.abs(value)
  if (Math.abs(value - Math.round(value)) < 1e-6) {
    return Math.round(value).toString()
  }
  if (abs >= 100) {
    return value.toFixed(0)
  }
  if (abs >= 1) {
    return value.toFixed(2)
  }
  return value.toFixed(3)
}

function formatTradeDateLabel(value: string) {
  if (/^\d{8}$/.test(value)) {
    const month = value.slice(4, 6)
    const day = value.slice(6, 8)
    return month === '01' ? `${value.slice(2, 4)}-${month}-${day}` : `${month}-${day}`
  }
  return value
}

function buildCenteredPercentGrid(min: number, max: number) {
  const midpoint = (min + max) / 2
  const step = Math.max(Math.abs(midpoint), (max - min) / 2, 1) * 0.1
  const values = new Set<number>()
  const epsilon = step * 0.1

  values.add(Number(midpoint.toFixed(6)))

  for (let value = midpoint + step; value <= max + epsilon; value += step) {
    values.add(Number(value.toFixed(6)))
  }
  for (let value = midpoint - step; value >= min - epsilon; value -= step) {
    values.add(Number(value.toFixed(6)))
  }

  return [...values]
    .filter((value) => value >= min - epsilon && value <= max + epsilon)
    .sort((left, right) => right - left)
}

function buildDateTickIndices(count: number, maxTicks = CHART_DATE_TICK_COUNT) {
  if (count <= 0) {
    return []
  }

  const ticks = new Set<number>([0, count - 1])
  const visibleCount = Math.min(maxTicks, count)
  if (visibleCount > 2) {
    const step = (count - 1) / (visibleCount - 1)
    for (let index = 1; index < visibleCount - 1; index += 1) {
      ticks.add(Math.round(index * step))
    }
  }

  return [...ticks].sort((left, right) => left - right)
}

function getChartKlinePlotWidth(reserveCyqPanelWidth: boolean) {
  const plotWidth = CHART_VIEWBOX_WIDTH - CHART_MARGIN.left - CHART_MARGIN.right
  const chipPanelWidth = reserveCyqPanelWidth ? plotWidth * CHART_CYQ_PANEL_WIDTH_RATIO : 0

  return Math.max(plotWidth - chipPanelWidth - (reserveCyqPanelWidth ? CHART_CYQ_PANEL_GAP : 0), 1)
}

function getChartKlinePlotRight(reserveCyqPanelWidth: boolean) {
  return CHART_MARGIN.left + getChartKlinePlotWidth(reserveCyqPanelWidth)
}

function getChartLayoutSlotCount(itemCount: number, totalItemCount: number) {
  if (itemCount <= 0) {
    return 0
  }

  return totalItemCount === itemCount && itemCount < CHART_MIN_RIGHT_ALIGNED_SLOTS
    ? CHART_MIN_RIGHT_ALIGNED_SLOTS
    : itemCount
}

function getChartItemX(
  itemIndex: number,
  itemCount: number,
  layoutSlotCount: number,
  reserveCyqPanelWidth: boolean,
) {
  const resolvedLayoutSlotCount = Math.max(layoutSlotCount, itemCount)
  const klinePlotWidth = getChartKlinePlotWidth(reserveCyqPanelWidth)
  const step = resolvedLayoutSlotCount > 0 ? klinePlotWidth / resolvedLayoutSlotCount : klinePlotWidth
  const leadingSlotCount = Math.max(resolvedLayoutSlotCount - itemCount, 0)

  return CHART_MARGIN.left + step * (leadingSlotCount + itemIndex) + step / 2
}

function resolveVisibleIndexFromChartX(
  chartXPercent: number,
  itemCount: number,
  layoutSlotCount: number,
  reserveCyqPanelWidth: boolean,
) {
  if (itemCount <= 0 || layoutSlotCount <= 0) {
    return null
  }

  const plotStartPercent = CHART_MARGIN.left / CHART_VIEWBOX_WIDTH * 100
  const plotEndPercent = getChartKlinePlotRight(reserveCyqPanelWidth) / CHART_VIEWBOX_WIDTH * 100
  const plotXPercent = clampNumber((chartXPercent - plotStartPercent) / (plotEndPercent - plotStartPercent), 0, 0.999999)
  const leadingSlotCount = Math.max(layoutSlotCount - itemCount, 0)
  const slotIndex = clampNumber(Math.floor(plotXPercent * layoutSlotCount), 0, layoutSlotCount - 1)
  const visibleIndex = slotIndex - leadingSlotCount

  if (visibleIndex < 0 || visibleIndex >= itemCount) {
    return null
  }
  return visibleIndex
}

function formatTooltipPercent(value: number | null | undefined) {
  if (!isFiniteNumber(value)) {
    return '--'
  }
  return `${value.toFixed(2)}%`
}

function chipValueByMode(bin: CyqChenBin, mode: ChipPeakMode) {
  if (mode === 'main') {
    return bin.mainChip
  }
  if (mode === 'retail') {
    return bin.retailChip
  }
  return bin.totalChip
}

function chipModeLabel(mode: ChipPeakMode) {
  if (mode === 'main') {
    return '主力'
  }
  if (mode === 'retail') {
    return '散户'
  }
  return '合计'
}

function chipProfitState(price: number, close: number | null) {
  if (close === null) {
    return 'profit'
  }
  return price <= close ? 'profit' : 'trapped'
}

function chipColor(holder: 'main' | 'retail', state: 'profit' | 'trapped') {
  if (holder === 'main') {
    return state === 'profit' ? CHIP_COLOR_MAIN_PROFIT : CHIP_COLOR_MAIN_TRAPPED
  }
  return state === 'profit' ? CHIP_COLOR_RETAIL_PROFIT : CHIP_COLOR_RETAIL_TRAPPED
}

function findChipPeak(snapshot: CyqChenSnapshot | null, mode: ChipPeakMode) {
  if (!snapshot || snapshot.bins.length === 0) {
    return null
  }

  return snapshot.bins.reduce<CyqChenBin | null>((peak, bin) => {
    const value = chipValueByMode(bin, mode)
    if (!isFiniteNumber(value)) {
      return peak
    }
    if (!peak || value > chipValueByMode(peak, mode)) {
      return bin
    }
    return peak
  }, null)
}

function CyqChenProjectChart({
  kline,
  snapshot,
  selectedTradeDate,
  chipPeakMode,
  onSelectTradeDate,
}: {
  kline: CyqChenKlineRow[]
  snapshot: CyqChenSnapshot | null
  selectedTradeDate: string
  chipPeakMode: ChipPeakMode
  onSelectTradeDate: (tradeDate: string) => void
}) {
  const [visibleBarCount, setVisibleBarCount] = useState(DEFAULT_VISIBLE_BARS)
  const [visibleStartIndex, setVisibleStartIndex] = useState(0)
  const [focus, setFocus] = useState<CyqChenChartFocus | null>(null)

  const totalItems = kline.length
  const minVisibleBars = totalItems === 0 ? 1 : Math.min(MIN_VISIBLE_BARS, totalItems)
  const effectiveVisibleBarCount = totalItems === 0
    ? 0
    : clampNumber(Math.round(visibleBarCount), minVisibleBars, totalItems)
  const maxVisibleStart = Math.max(totalItems - effectiveVisibleBarCount, 0)
  const effectiveVisibleStart = clampNumber(Math.round(visibleStartIndex), 0, maxVisibleStart)
  const visibleItems = kline.slice(effectiveVisibleStart, effectiveVisibleStart + effectiveVisibleBarCount)
  const reserveCyqPanelWidth = Boolean(snapshot && snapshot.bins.length > 0)
  const klinePlotWidth = getChartKlinePlotWidth(reserveCyqPanelWidth)
  const klinePlotRight = getChartKlinePlotRight(reserveCyqPanelWidth)
  const chipPanelLeft = klinePlotRight + (reserveCyqPanelWidth ? CHART_CYQ_PANEL_GAP : 0)
  const chipPanelRight = CHART_VIEWBOX_WIDTH - CHART_MARGIN.right
  const plotHeight = CHART_VIEWBOX_HEIGHT - CHART_MARGIN.top - CHART_MARGIN.bottom
  const layoutSlotCount = getChartLayoutSlotCount(visibleItems.length, kline.length)
  const step = layoutSlotCount > 0 ? klinePlotWidth / layoutSlotCount : klinePlotWidth
  const xAt = (itemIndex: number) => getChartItemX(itemIndex, visibleItems.length, layoutSlotCount, reserveCyqPanelWidth)
  const priceValues = visibleItems.flatMap((item) =>
    [item.open, item.high, item.low, item.close].filter(isFiniteNumber),
  )
  const domain = buildDomain(priceValues)
  const selectedVisibleIndex = selectedTradeDate
    ? visibleItems.findIndex((item) => item.tradeDate === selectedTradeDate)
    : -1
  const focusedRow = focus ? visibleItems[focus.visibleIndex] : null
  const focusXPercent = focus ? xAt(focus.visibleIndex) / CHART_VIEWBOX_WIDTH * 100 : null
  const tooltipHorizontalClass = (focusXPercent ?? 0) > 62 ? 'details-chart-tooltip-left' : 'details-chart-tooltip-right'
  const chartRangeText = visibleItems.length > 0
    ? `${visibleItems[0]?.tradeDate ?? '--'} ~ ${visibleItems[visibleItems.length - 1]?.tradeDate ?? '--'}`
    : '--'

  function updateFocusFromPointer(event: ReactPointerEvent<HTMLDivElement>) {
    if (visibleItems.length === 0) {
      return
    }
    const rect = event.currentTarget.getBoundingClientRect()
    if (rect.width <= 0 || rect.height <= 0) {
      return
    }

    const chartXPercent = clampNumber((event.clientX - rect.left) / rect.width * 100, 0, 99.9999)
    const chartYPercent = clampNumber((event.clientY - rect.top) / rect.height * 100, 6, 94)
    const visibleIndex = resolveVisibleIndexFromChartX(
      chartXPercent,
      visibleItems.length,
      layoutSlotCount,
      reserveCyqPanelWidth,
    )
    if (visibleIndex === null) {
      return
    }

    setFocus({ visibleIndex, cursorYPercent: chartYPercent, pinned: true })
    onSelectTradeDate(visibleItems[visibleIndex]?.tradeDate ?? '')
  }

  return (
    <>
      <div className="details-chart-toolbar cyq-chen-project-chart-toolbar">
        <label className="details-chart-slider-field">
          <span>缩放</span>
          <input
            type="range"
            min={minVisibleBars}
            max={Math.max(totalItems, 1)}
            step={1}
            value={Math.max(effectiveVisibleBarCount, 1)}
            onChange={(event) => setVisibleBarCount(Number(event.target.value))}
            disabled={totalItems === 0}
          />
          <strong>{effectiveVisibleBarCount || 0} 根</strong>
        </label>
        <label className="details-chart-slider-field details-chart-slider-field-wide">
          <span>平移</span>
          <input
            type="range"
            min={0}
            max={Math.max(maxVisibleStart, 1)}
            step={1}
            value={effectiveVisibleStart}
            onChange={(event) => setVisibleStartIndex(Number(event.target.value))}
            disabled={totalItems === 0 || maxVisibleStart === 0}
          />
          <strong>{chartRangeText}</strong>
        </label>
      </div>

      <div className="details-chart-shell cyq-chen-project-chart-shell">
        <section className="details-chart-panel">
          <header className="details-chart-panel-head">
            <div className="details-chart-panel-head-main">
              <strong>主K</strong>
              <small>{snapshotLabel(snapshot)}</small>
            </div>
            <span>candles</span>
          </header>

          <div
            className="details-chart-viewport"
            onPointerDown={(event) => {
              event.currentTarget.setPointerCapture(event.pointerId)
              updateFocusFromPointer(event)
            }}
            onPointerMove={(event) => {
              if (event.buttons !== 1) {
                return
              }
              updateFocusFromPointer(event)
            }}
            onPointerUp={(event) => {
              if (event.currentTarget.hasPointerCapture(event.pointerId)) {
                event.currentTarget.releasePointerCapture(event.pointerId)
              }
            }}
          >
            {domain && visibleItems.length > 0 ? (() => {
              const yAt = (value: number) =>
                CHART_MARGIN.top + (domain.max - value) / (domain.max - domain.min) * plotHeight
              const bodyWidth = Math.max(Math.min(step * 0.58, 18), 3)
              const dateTickIndices = buildDateTickIndices(visibleItems.length)
              const gridValues = buildCenteredPercentGrid(domain.min, domain.max)
              const yAxisLabels = gridValues.map((value) => ({
                key: `price-y-${value}`,
                value: formatAxisValue(value),
                topPercent: (CHART_MARGIN.top + (domain.max - value) / (domain.max - domain.min) * plotHeight) / CHART_VIEWBOX_HEIGHT * 100,
              }))
              const xAxisLabels = dateTickIndices.map((itemIndex) => ({
                key: `price-x-${visibleItems[itemIndex]?.tradeDate ?? itemIndex}`,
                value: formatTradeDateLabel(visibleItems[itemIndex]?.tradeDate ?? ''),
                leftPercent: xAt(itemIndex) / CHART_VIEWBOX_WIDTH * 100,
              }))
              const selectedClose = isFiniteNumber(focusedRow?.close)
                ? focusedRow.close
                : isFiniteNumber(snapshot?.close)
                  ? snapshot.close
                  : null
              const visibleCyqBins = (snapshot?.bins ?? []).filter((bin) => {
                const binLow = Math.min(bin.priceLow, bin.priceHigh)
                const binHigh = Math.max(bin.priceLow, bin.priceHigh)
                return !(binHigh < domain.min || binLow > domain.max)
              })
              const maxChip = visibleCyqBins.reduce((acc, bin) => {
                const value = chipValueByMode(bin, chipPeakMode)
                return isFiniteNumber(value) ? Math.max(acc, value) : acc
              }, 0)
              const peakBin = findChipPeak(snapshot, chipPeakMode)

              return (
                <>
                  <svg
                    className="details-chart-svg"
                    viewBox={`0 0 ${CHART_VIEWBOX_WIDTH} ${CHART_VIEWBOX_HEIGHT}`}
                    preserveAspectRatio="none"
                  >
                    {gridValues.map((value) => {
                      const y = yAt(value)
                      return (
                        <line
                          className="details-chart-grid-line"
                          key={`grid-${value}`}
                          x1={CHART_MARGIN.left}
                          y1={y}
                          x2={klinePlotRight}
                          y2={y}
                        />
                      )
                    })}

                    {dateTickIndices.map((itemIndex) => (
                      <line
                        className="details-chart-vertical-line"
                        key={`guide-${visibleItems[itemIndex]?.tradeDate ?? itemIndex}`}
                        x1={xAt(itemIndex)}
                        y1={CHART_MARGIN.top}
                        x2={xAt(itemIndex)}
                        y2={CHART_VIEWBOX_HEIGHT - CHART_MARGIN.bottom}
                      />
                    ))}

                    {selectedVisibleIndex >= 0 ? (
                      <line
                        className="details-chart-reference-line"
                        x1={xAt(selectedVisibleIndex)}
                        y1={CHART_MARGIN.top}
                        x2={xAt(selectedVisibleIndex)}
                        y2={CHART_VIEWBOX_HEIGHT - CHART_MARGIN.bottom}
                      />
                    ) : null}

                    {visibleItems.map((item, itemIndex) => {
                      const { open, high, low, close } = item
                      if (!isFiniteNumber(open) || !isFiniteNumber(high) || !isFiniteNumber(low) || !isFiniteNumber(close)) {
                        return null
                      }

                      const x = xAt(itemIndex)
                      const bodyTop = Math.min(yAt(open), yAt(close))
                      const bodyHeight = Math.max(Math.abs(yAt(open) - yAt(close)), 1.2)
                      const color = close > open ? CANDLE_UP_COLOR : close < open ? CANDLE_DOWN_COLOR : CANDLE_FLAT_COLOR

                      return (
                        <g key={`price-${item.tradeDate}`}>
                          <line
                            className="details-chart-candle-wick"
                            x1={x}
                            y1={yAt(high)}
                            x2={x}
                            y2={yAt(low)}
                            stroke={color}
                          />
                          <rect
                            className="details-chart-candle-body"
                            x={x - bodyWidth / 2}
                            y={bodyTop}
                            width={bodyWidth}
                            height={bodyHeight}
                            fill={color}
                            stroke={color}
                            rx={1.2}
                          />
                        </g>
                      )
                    })}

                    {reserveCyqPanelWidth && maxChip > 0 ? (
                      <g key={`cyq-${snapshot?.tradeDate ?? 'none'}`}>
                        <line
                          className="details-chart-cyq-divider"
                          x1={chipPanelLeft}
                          y1={CHART_MARGIN.top}
                          x2={chipPanelLeft}
                          y2={CHART_VIEWBOX_HEIGHT - CHART_MARGIN.bottom}
                        />
                        {visibleCyqBins.map((bin) => {
                          const clampedLow = Math.max(Math.min(bin.priceLow, bin.priceHigh), domain.min)
                          const clampedHigh = Math.min(Math.max(bin.priceLow, bin.priceHigh), domain.max)
                          if (clampedLow > clampedHigh) {
                            return null
                          }

                          const yTop = yAt(clampedHigh)
                          const yBottom = yAt(clampedLow)
                          const barHeight = Math.max(yBottom - yTop, 1)
                          const maxBarWidth = Math.max(chipPanelRight - chipPanelLeft - 4, 0)
                          const chipValue = chipValueByMode(bin, chipPeakMode)
                          const barWidth = isFiniteNumber(chipValue) ? chipValue / maxChip * maxBarWidth : 0
                          const resolvedBarWidth = Math.max(barWidth, 1)
                          const representativePrice = (bin.priceLow + bin.priceHigh) / 2
                          const state = chipProfitState(representativePrice, selectedClose)
                          const fill = chipPeakMode === 'main'
                            ? chipColor('main', state)
                            : chipColor('retail', state)
                          const isPeak = peakBin?.index === bin.index
                          const totalChip = isFiniteNumber(bin.totalChip) && bin.totalChip > 0 ? bin.totalChip : 0
                          const mainRatio = totalChip > 0 && isFiniteNumber(bin.mainChip)
                            ? clampNumber(bin.mainChip / totalChip, 0, 1)
                            : 0
                          const mainWidth = chipPeakMode === 'total' ? resolvedBarWidth * mainRatio : 0
                          const retailWidth = chipPeakMode === 'total' ? resolvedBarWidth - mainWidth : 0
                          const barX = chipPanelRight - resolvedBarWidth

                          return chipPeakMode === 'total' ? (
                            <g key={`${bin.index}-${bin.priceLow}`}>
                              {retailWidth > 0 ? (
                                <rect
                                  className="details-chart-cyq-bar"
                                  x={barX}
                                  y={yTop}
                                  width={Math.max(retailWidth, 0.8)}
                                  height={barHeight}
                                  fill={chipColor('retail', state)}
                                  opacity={0.78}
                                  rx={1}
                                />
                              ) : null}
                              {mainWidth > 0 ? (
                                <rect
                                  className="details-chart-cyq-bar"
                                  x={chipPanelRight - Math.max(mainWidth, 0.8)}
                                  y={yTop}
                                  width={Math.max(mainWidth, 0.8)}
                                  height={barHeight}
                                  fill={chipColor('main', state)}
                                  opacity={0.84}
                                  rx={1}
                                />
                              ) : null}
                              {isPeak ? (
                                <rect
                                  className="cyq-chen-chip-peak-bar"
                                  x={barX}
                                  y={yTop}
                                  width={resolvedBarWidth}
                                  height={barHeight}
                                  fill="none"
                                  rx={1}
                                />
                              ) : null}
                            </g>
                          ) : (
                            <rect
                              className={isPeak ? 'details-chart-cyq-bar cyq-chen-chip-peak-bar' : 'details-chart-cyq-bar'}
                              key={`${bin.index}-${bin.priceLow}`}
                              x={barX}
                              y={yTop}
                              width={resolvedBarWidth}
                              height={barHeight}
                              fill={fill}
                              opacity={isPeak ? 0.96 : 0.82}
                              rx={1}
                            />
                          )
                        })}
                      </g>
                    ) : null}
                  </svg>

                  <div className="details-chart-overlay-layer" aria-hidden="true">
                    <div className="details-chart-axis-layer details-chart-axis-layer-y">
                      {yAxisLabels.map((label) => (
                        <span className="details-chart-y-label" key={label.key} style={{ top: `${label.topPercent}%` }}>
                          {label.value}
                        </span>
                      ))}
                    </div>
                    <div className="details-chart-axis-layer details-chart-axis-layer-x">
                      {xAxisLabels.map((label) => (
                        <span className="details-chart-x-label" key={label.key} style={{ left: `${label.leftPercent}%` }}>
                          {label.value}
                        </span>
                      ))}
                    </div>
                    {focusXPercent !== null ? (
                      <div className="details-chart-crosshair-vertical" style={{ left: `${focusXPercent}%` }} />
                    ) : null}
                    {focus ? (
                      <>
                        <div className="details-chart-crosshair-horizontal" style={{ top: `${focus.cursorYPercent}%` }} />
                        {focusedRow ? (
                          <div
                            className={[
                              'details-chart-tooltip',
                              tooltipHorizontalClass,
                              focus.pinned ? 'details-chart-tooltip-pinned' : '',
                            ].filter(Boolean).join(' ')}
                            style={{
                              left: `${focusXPercent ?? 0}%`,
                              top: `${focus.cursorYPercent}%`,
                            }}
                          >
                            <div className="details-chart-tooltip-head">
                              <strong>{focusedRow.tradeDate}</strong>
                            </div>
                            <div className="details-chart-tooltip-body">
                              <div className="details-chart-tooltip-grid details-chart-tooltip-grid-ohlc">
                                <div className="details-chart-tooltip-row">
                                  <span>C</span>
                                  <strong>{formatNumber(focusedRow.close)}</strong>
                                </div>
                                <div className="details-chart-tooltip-row">
                                  <span>O</span>
                                  <strong>{formatNumber(focusedRow.open)}</strong>
                                </div>
                                <div className="details-chart-tooltip-row">
                                  <span>H</span>
                                  <strong>{formatNumber(focusedRow.high)}</strong>
                                </div>
                                <div className="details-chart-tooltip-row">
                                  <span>L</span>
                                  <strong>{formatNumber(focusedRow.low)}</strong>
                                </div>
                                <div className="details-chart-tooltip-row">
                                  <span>换手</span>
                                  <strong>{formatTooltipPercent(focusedRow.turnoverRate)}</strong>
                                </div>
                              </div>
                            </div>
                          </div>
                        ) : null}
                      </>
                    ) : null}
                  </div>
                </>
              )
            })() : (
              <div className="details-chart-empty">暂无有效图表数据</div>
            )}
          </div>
        </section>
      </div>
    </>
  )
}

function selectedSnapshotByDate(snapshots: CyqChenSnapshot[], tradeDate: string) {
  if (!tradeDate || snapshots.length === 0) {
    return snapshots[snapshots.length - 1] ?? null
  }

  let latestBeforeOrEqual: CyqChenSnapshot | null = null
  for (const snapshot of snapshots) {
    const snapshotTradeDate = snapshot.tradeDate ?? ''
    if (snapshotTradeDate === tradeDate) {
      return snapshot
    }
    if (snapshotTradeDate <= tradeDate) {
      latestBeforeOrEqual = snapshot
      continue
    }
    break
  }

  return latestBeforeOrEqual ?? snapshots[snapshots.length - 1] ?? null
}

function snapshotLabel(snapshot: CyqChenSnapshot | null) {
  if (!snapshot) {
    return '无快照'
  }
  return `${snapshot.tradeDate ?? '--'} · 主 ${formatNumber(snapshot.mainTotal)} / 散 ${formatNumber(snapshot.retailTotal)}`
}

export default function CyqChenPage() {
  const [tsCodeInput, setTsCodeInput] = useState('000001')
  const [startDateInput, setStartDateInput] = useState('')
  const [endDateInput, setEndDateInput] = useState('')
  const [warmupDaysInput, setWarmupDaysInput] = useState('120')
  const [bucketPctInput, setBucketPctInput] = useState('1')
  const [strategies, setStrategies] = useState<CyqChenStrategyDraft[]>(() => cloneDefaultStrategies())
  const [sourcePath, setSourcePath] = useState('')
  const [strategyFilePath, setStrategyFilePath] = useState('')
  const [strategyFileExists, setStrategyFileExists] = useState(false)
  const [strategyLoading, setStrategyLoading] = useState(false)
  const [result, setResult] = useState<CyqChenSingleStockData | null>(null)
  const [savedRuns, setSavedRuns] = useState<SavedRun[]>([])
  const [selectedTradeDate, setSelectedTradeDate] = useState('')
  const [chipPeakMode, setChipPeakMode] = useState<ChipPeakMode>('total')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [notice, setNotice] = useState('')

  const selectedSnapshot = useMemo(() => {
    if (!result) {
      return null
    }
    const tradeDate = selectedTradeDate || result.snapshots[result.snapshots.length - 1]?.tradeDate || ''
    return selectedSnapshotByDate(result.snapshots, tradeDate)
  }, [result, selectedTradeDate])

  const snapshotOptions = result?.snapshots ?? []
  const mainPeakBin = useMemo(() => findChipPeak(selectedSnapshot, 'main'), [selectedSnapshot])
  const retailPeakBin = useMemo(() => findChipPeak(selectedSnapshot, 'retail'), [selectedSnapshot])
  const selectedPeakBin = useMemo(
    () => findChipPeak(selectedSnapshot, chipPeakMode),
    [selectedSnapshot, chipPeakMode],
  )

  function applyStrategyPage(page: CyqChenStrategyPageData) {
    setStrategies(page.strategies.map((strategy) => ({ ...strategy })))
    setStrategyFilePath(page.filePath)
    setStrategyFileExists(page.exists)
  }

  function normalizeStrategiesForSave() {
    const normalizedStrategies = strategies.map((strategy) => ({
      ...strategy,
      name: strategy.name.trim(),
      when: strategy.when.trim(),
      bias: Number(strategy.bias),
    }))
    if (
      normalizedStrategies.length === 0 ||
      normalizedStrategies.some((strategy) => !strategy.name || !strategy.when || !Number.isFinite(strategy.bias))
    ) {
      throw new Error('策略名称、表达式和 bias 都必须有效。')
    }
    return normalizedStrategies
  }

  async function loadStrategyFile() {
    setStrategyLoading(true)
    setError('')
    try {
      const nextSourcePath = sourcePath || await ensureManagedSourcePath()
      setSourcePath(nextSourcePath)
      const page = await getCyqChenStrategyPage(nextSourcePath)
      applyStrategyPage(page)
      setNotice(page.exists ? '已读取 chip_change_rule.toml。' : '策略文件不存在，已载入默认测试配置。')
    } catch (loadError) {
      setError(`读取筹码策略失败: ${String(loadError)}`)
      setNotice('')
    } finally {
      setStrategyLoading(false)
    }
  }

  useEffect(() => {
    let canceled = false

    async function initStrategyFile() {
      setStrategyLoading(true)
      setError('')
      try {
        const nextSourcePath = await ensureManagedSourcePath()
        const page = await getCyqChenStrategyPage(nextSourcePath)
        if (canceled) {
          return
        }
        setSourcePath(nextSourcePath)
        setStrategies(page.strategies.map((strategy) => ({ ...strategy })))
        setStrategyFilePath(page.filePath)
        setStrategyFileExists(page.exists)
        setNotice(page.exists ? '已读取 chip_change_rule.toml。' : '策略文件不存在，已载入默认测试配置。')
      } catch (loadError) {
        if (!canceled) {
          setError(`读取筹码策略失败: ${String(loadError)}`)
          setNotice('')
        }
      } finally {
        if (!canceled) {
          setStrategyLoading(false)
        }
      }
    }

    void initStrategyFile()
    return () => {
      canceled = true
    }
  }, [])

  async function runTest() {
    const warmupDays = Number(warmupDaysInput.trim())
    const bucketPct = Number(bucketPctInput.trim())
    if (!Number.isInteger(warmupDays) || warmupDays < 0) {
      setError('预热天数必须是 >= 0 的整数。')
      setNotice('')
      return
    }
    if (!Number.isFinite(bucketPct) || bucketPct <= 0) {
      setError('分桶百分比必须是正数。')
      setNotice('')
      return
    }
    let normalizedStrategies: CyqChenStrategyDraft[]
    try {
      normalizedStrategies = normalizeStrategiesForSave()
    } catch (validationError) {
      setError(String(validationError))
      setNotice('')
      return
    }

    setLoading(true)
    setError('')
    setNotice('')
    try {
      const nextSourcePath = sourcePath || await ensureManagedSourcePath()
      setSourcePath(nextSourcePath)
      const nextResult = await runCyqChenSingleStockTest({
        sourcePath: nextSourcePath,
        tsCode: tsCodeInput,
        startDate: normalizeDateInput(startDateInput) || null,
        endDate: normalizeDateInput(endDateInput) || null,
        warmupDays,
        bucketPct,
        strategies: normalizedStrategies,
      })
      const lastTradeDate = nextResult.snapshots[nextResult.snapshots.length - 1]?.tradeDate ?? ''
      setResult(nextResult)
      setSelectedTradeDate(lastTradeDate)
      setSavedRuns((items) => [
        {
          id: `${Date.now()}`,
          label: `${nextResult.resolvedTsCode} ${nextResult.startDate}-${nextResult.endDate}`,
          data: nextResult,
        },
        ...items,
      ].slice(0, 8))
      setNotice(`已完成临时计算：${nextResult.snapshots.length} 个快照。`)
    } catch (runError) {
      setError(String(runError))
      setResult(null)
      setSelectedTradeDate('')
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="cyq-chen-page">
      <section className="cyq-chen-panel cyq-chen-control-panel">
        <div className="cyq-chen-head">
          <div>
            <h2>筹码测试</h2>
          </div>
        </div>

        <div className="cyq-chen-form-grid">
          <label>
            <span>股票代码</span>
            <input value={tsCodeInput} onChange={(event) => setTsCodeInput(event.target.value)} placeholder="000001 或 000001.SZ" />
          </label>
          <label>
            <span>开始日期</span>
            <input value={startDateInput} onChange={(event) => setStartDateInput(event.target.value)} placeholder="YYYYMMDD，可空" />
          </label>
          <label>
            <span>结束日期</span>
            <input value={endDateInput} onChange={(event) => setEndDateInput(event.target.value)} placeholder="YYYYMMDD，可空" />
          </label>
          <label>
            <span>预热天数</span>
            <input type="number" min={0} step={1} value={warmupDaysInput} onChange={(event) => setWarmupDaysInput(event.target.value)} />
          </label>
          <label>
            <span>分桶百分比</span>
            <input type="number" min={0.01} step={0.1} value={bucketPctInput} onChange={(event) => setBucketPctInput(event.target.value)} />
          </label>
        </div>
        <div className="cyq-chen-run-row">
          <button className="cyq-chen-primary-btn cyq-chen-run-btn" type="button" onClick={runTest} disabled={loading}>
            {loading ? '计算中...' : '计算筹码测试'}
          </button>
        </div>

        <div className="cyq-chen-section-head">
          <strong>策略来源</strong>
          <div className="cyq-chen-actions">
            <button type="button" className="cyq-chen-secondary-btn" onClick={() => void loadStrategyFile()} disabled={strategyLoading}>
              {strategyLoading ? '读取中...' : '重新读取'}
            </button>
          </div>
        </div>

        <div className="cyq-chen-strategy-source">
          <div>
            <span>当前文件</span>
            <strong>{strategyFileExists ? strategyFilePath : '未落盘，使用默认测试策略'}</strong>
          </div>
          <div>
            <span>策略数量</span>
            <strong>{strategies.length}</strong>
          </div>
          <p>筹码变动策略请到“策略管理”页面底部编辑。</p>
        </div>

        {error ? <div className="cyq-chen-error">{error}</div> : null}
        {notice ? <div className="cyq-chen-notice">{notice}</div> : null}

        {savedRuns.length > 0 ? (
          <div className="cyq-chen-history">
            <strong>临时结果</strong>
            <div>
              {savedRuns.map((item) => (
                <button
                  key={item.id}
                  type="button"
                  className="cyq-chen-history-btn"
                  onClick={() => {
                    setResult(item.data)
                    setSelectedTradeDate(item.data.snapshots[item.data.snapshots.length - 1]?.tradeDate ?? '')
                  }}
                >
                  {item.label}
                </button>
              ))}
            </div>
          </div>
        ) : null}
      </section>

      <section className="cyq-chen-panel cyq-chen-chart-panel">
        <div className="cyq-chen-result-head">
          <div>
            <h3>{result ? result.resolvedTsCode : '未计算'}</h3>
            <p>
              {result
                ? `${result.startDate} 至 ${result.endDate} · 输出起点 ${result.outputStartDate ?? '--'}`
                : '设置单票参数后运行计算'}
            </p>
          </div>
          <label>
            <span>快照日期</span>
            <select
              value={selectedSnapshot?.tradeDate ?? ''}
              onChange={(event) => setSelectedTradeDate(event.target.value)}
              disabled={snapshotOptions.length === 0}
            >
              {snapshotOptions.length === 0 ? <option value="">无快照</option> : null}
              {snapshotOptions.map((snapshot) => (
                <option key={snapshot.tradeDate ?? ''} value={snapshot.tradeDate ?? ''}>
                  {snapshot.tradeDate}
                </option>
              ))}
            </select>
          </label>
        </div>

        <div className="cyq-chen-summary-grid">
          <div>
            <span>主力筹码</span>
            <strong>{formatNumber(selectedSnapshot?.mainTotal)}</strong>
          </div>
          <div>
            <span>散户筹码</span>
            <strong>{formatNumber(selectedSnapshot?.retailTotal)}</strong>
          </div>
          <div>
            <span>总筹码</span>
            <strong>{formatNumber(selectedSnapshot?.totalChips)}</strong>
          </div>
          <div>
            <span>收盘价</span>
            <strong>{formatNumber(selectedSnapshot?.close)}</strong>
          </div>
          <div>
            <span>主力筹码峰</span>
            <strong>{mainPeakBin ? formatNumber(mainPeakBin.price) : '--'}</strong>
            <small>{mainPeakBin ? formatNumber(mainPeakBin.mainChip, 4) : '--'}</small>
          </div>
          <div>
            <span>散户筹码峰</span>
            <strong>{retailPeakBin ? formatNumber(retailPeakBin.price) : '--'}</strong>
            <small>{retailPeakBin ? formatNumber(retailPeakBin.retailChip, 4) : '--'}</small>
          </div>
          <div>
            <span>{chipModeLabel(chipPeakMode)}显示峰</span>
            <strong>{selectedPeakBin ? formatNumber(selectedPeakBin.price) : '--'}</strong>
            <small>{selectedPeakBin ? formatNumber(chipValueByMode(selectedPeakBin, chipPeakMode), 4) : '--'}</small>
          </div>
        </div>

        <div className="cyq-chen-project-chart">
          <div className="cyq-chen-chart-title">
            <div>
              <strong>K 线与筹码分布</strong>
              <span>{result?.kline.length ?? 0} 根</span>
            </div>
            <div className="cyq-chen-chip-mode-switch" role="tablist" aria-label="筹码峰显示">
              {(['total', 'main', 'retail'] as const).map((mode) => (
                <button
                  key={mode}
                  type="button"
                  className={chipPeakMode === mode ? 'is-active' : ''}
                  onClick={() => setChipPeakMode(mode)}
                >
                  {chipModeLabel(mode)}
                </button>
              ))}
            </div>
            <div className="cyq-chen-chip-legend" aria-label="筹码颜色">
              <span><i style={{ background: CHIP_COLOR_MAIN_PROFIT }} />主力盈利</span>
              <span><i style={{ background: CHIP_COLOR_MAIN_TRAPPED }} />主力套牢</span>
              <span><i style={{ background: CHIP_COLOR_RETAIL_PROFIT }} />散户盈利</span>
              <span><i style={{ background: CHIP_COLOR_RETAIL_TRAPPED }} />散户套牢</span>
            </div>
          </div>
          <CyqChenProjectChart
            kline={result?.kline ?? []}
            snapshot={selectedSnapshot}
            selectedTradeDate={selectedSnapshot?.tradeDate ?? selectedTradeDate}
            chipPeakMode={chipPeakMode}
            onSelectTradeDate={setSelectedTradeDate}
          />
        </div>

        <div className="cyq-chen-bin-table-wrap">
          <table className="cyq-chen-bin-table">
            <thead>
              <tr>
                <th>价位</th>
                <th>区间</th>
                <th>主力</th>
                <th>散户</th>
                <th>合计</th>
              </tr>
            </thead>
            <tbody>
              {(selectedSnapshot?.bins ?? []).slice(0, 80).map((bin: CyqChenBin) => (
                <tr key={bin.index}>
                  <td>{formatNumber(bin.price)}</td>
                  <td>
                    {formatNumber(bin.priceLow)} - {formatNumber(bin.priceHigh)}
                  </td>
                  <td>{formatNumber(bin.mainChip, 4)}</td>
                  <td>{formatNumber(bin.retailChip, 4)}</td>
                  <td>{formatNumber(bin.totalChip, 4)}</td>
                </tr>
              ))}
              {!selectedSnapshot ? (
                <tr>
                  <td colSpan={5}>没有可展示的筹码分桶。</td>
                </tr>
              ) : null}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  )
}
