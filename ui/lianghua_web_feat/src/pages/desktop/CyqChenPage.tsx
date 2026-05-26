import {
  useDeferredValue,
  useEffect,
  useMemo,
  useRef,
  useState,
  type CSSProperties,
  type PointerEvent as ReactPointerEvent,
  type WheelEvent as ReactWheelEvent,
} from 'react'
import {
  getCyqChenStrategyPage,
  runCyqChenSingleStockTest,
  type CyqChenBin,
  type CyqChenKlineRow,
  type CyqChenSingleStockData,
  type CyqChenSnapshot,
  type CyqChenStrategyDraft,
} from '../../apis/cyqChen'
import type { DetailChartMarker, DetailChartSeries, DetailChartTooltip, DetailKlinePanel, DetailKlineRow } from '../../apis/details'
import { ensureManagedSourcePath } from '../../apis/managedSource'
import { listStockLookupRows, type StockLookupRow } from '../../apis/reader'
import { readStoredChartMainWidthRatio } from '../../shared/chartSettings'
import { sanitizeCodeInput, stdTsCode } from '../../shared/stockCode'
import { buildStockLookupCandidates, findExactStockLookupMatch, getLookupDigits } from '../../shared/stockLookup'
import { readJsonStorage, writeJsonStorage } from '../../shared/storage'
import './css/DetailsPage.css'
import './css/CyqChenPage.css'

type SavedRun = {
  id: string
  label: string
  data: CyqChenSingleStockData
}

type CyqChenChartFocus = {
  absoluteIndex: number
  panelKey: string
  cursorXPercent: number
  cursorYPercent: number
  pinned: boolean
}

type CyqChenChartPointerSnapshot = {
  cursorXPercent: number
  cursorYPercent: number
  visibleIndex: number
}

type CyqChenChartAnchorSnapshot = {
  visiblePosition: number
  visibleRatio: number
}

type CyqChenChartDragState = {
  pointerId: number
  panelKey: string
  mode: 'pan' | 'focus' | 'tap' | 'dismiss'
  startClientX: number
  startClientY: number
  startVisibleStart: number
  barsPerPixel: number
  maxVisibleStart: number
  moved: boolean
}

type CyqChenChartTouchPointer = {
  pointerId: number
  panelKey: string
  clientX: number
  clientY: number
}

type CyqChenChartPinchState = {
  panelKey: string
  pointerIds: [number, number]
  startDistance: number
  startVisibleBarCount: number
  startAnchorAbsoluteIndex: number
  startAnchorRatio: number
}

type ChipPeakMode = 'total' | 'main' | 'retail'

type CyqChenChartRow = CyqChenKlineRow & {
  tradeDate: string
}

type FieldRow = {
  label: string
  value: string
}

type TooltipSection = {
  key: string
  rows: FieldRow[]
  variant?: 'default' | 'ohlc'
}

type ChartMarkerOverlayPoint = {
  key: string
  leftPercent: number
  topPercent: number
  shape: DetailChartMarker['shape']
  color: string
  text?: string | null
}

const CYQ_CHEN_DRAFT_STORAGE_KEY = 'lh_cyq_chen_test_draft_v1'
const MAX_STOCK_NAME_CANDIDATES = 12

type CyqChenTestDraft = {
  tsCodeInput: string
  startDateInput: string
  endDateInput: string
  warmupDaysInput: string
  bucketPctInput: string
}

const DEFAULT_VISIBLE_BARS = 90
const MIN_VISIBLE_BARS = 20
const CHART_MIN_RIGHT_ALIGNED_SLOTS = 60
const CHART_VIEWBOX_WIDTH = 1120
const CHART_VIEWBOX_HEIGHT = 240
const CHART_MARGIN = { top: 12, right: 8, bottom: 28, left: 52 }
const CHART_DATE_TICK_COUNT = 6
const CHART_CURSOR_Y_MIN = 6
const CHART_CURSOR_Y_MAX = 94
const CHART_TOOLTIP_LEFT_THRESHOLD = 62
const CHART_POINTER_DRAG_THRESHOLD = 6
const CHART_PINCH_MIN_DISTANCE = 12
const CHART_TOUCH_VERTICAL_SCROLL_RATIO = 1.2
const CHART_WHEEL_ZOOM_FACTOR = 0.0025
const CHART_TOUCH_FOCUS_HIT_SLOP = 24
const CHART_CYQ_PANEL_WIDTH_RATIO = 0.22
const CHART_CYQ_PANEL_GAP = 12
const CANDLE_UP_COLOR = '#d9485f'
const CANDLE_DOWN_COLOR = '#178f68'
const CANDLE_FLAT_COLOR = '#536273'
const CHIP_COLOR_MAIN_PROFIT = '#d9485f'
const CHIP_COLOR_MAIN_TRAPPED = '#4d95c9'
const CHIP_COLOR_RETAIL_PROFIT = '#f18a9b'
const CHIP_COLOR_RETAIL_TRAPPED = '#9cc8e6'

function readInitialChartLayoutWidth() {
  return typeof window === 'undefined' ? CHART_VIEWBOX_WIDTH : window.innerWidth
}

function readCyqChenDraft(): CyqChenTestDraft {
  const fallback: CyqChenTestDraft = {
    tsCodeInput: '000001',
    startDateInput: '',
    endDateInput: '',
    warmupDaysInput: '120',
    bucketPctInput: '1',
  }
  const parsed = readJsonStorage<Partial<CyqChenTestDraft>>(
    typeof window === 'undefined' ? null : window.localStorage,
    CYQ_CHEN_DRAFT_STORAGE_KEY,
  )
  if (!parsed) {
    return fallback
  }

  return {
    tsCodeInput: typeof parsed.tsCodeInput === 'string' ? parsed.tsCodeInput : fallback.tsCodeInput,
    startDateInput: typeof parsed.startDateInput === 'string' ? parsed.startDateInput : fallback.startDateInput,
    endDateInput: typeof parsed.endDateInput === 'string' ? parsed.endDateInput : fallback.endDateInput,
    warmupDaysInput: typeof parsed.warmupDaysInput === 'string' ? parsed.warmupDaysInput : fallback.warmupDaysInput,
    bucketPctInput: typeof parsed.bucketPctInput === 'string' ? parsed.bucketPctInput : fallback.bucketPctInput,
  }
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

function normalizeDetailKlineRow(item: DetailKlineRow): CyqChenChartRow {
  return {
    ...item,
    tradeDate: item.trade_date,
    turnoverRate: item.tor,
  }
}

function buildChartRows(data: CyqChenSingleStockData | null): CyqChenChartRow[] {
  const detailItems = data?.klinePayload?.items
  if (detailItems?.length) {
    return detailItems.map(normalizeDetailKlineRow)
  }
  return (data?.kline ?? []) as CyqChenChartRow[]
}

function getChartRowNumber(row: CyqChenChartRow | null | undefined, key: string) {
  if (!row) {
    return null
  }
  const value = row[key]
  return isFiniteNumber(value) ? value : null
}

function getSeriesColor(seriesIndex: number, series?: DetailChartSeries | null) {
  if (series?.color) {
    return series.color
  }
  const palette = ['#2563eb', '#f59e0b', '#7c3aed', '#0891b2', '#db2777', '#64748b']
  return palette[seriesIndex % palette.length]
}

function getSeriesColorForRow(
  row: CyqChenChartRow,
  seriesIndex: number,
  series?: DetailChartSeries | null,
  fallbackColor?: string,
) {
  const matchedRule = series?.color_when?.find((rule) => row[rule.when_key] === true)
  return matchedRule?.color ?? fallbackColor ?? getSeriesColor(seriesIndex, series)
}

function getPanelSeries(panel: DetailKlinePanel) {
  return panel.series ?? []
}

function getPanelTooltips(panel: DetailKlinePanel): DetailChartTooltip[] {
  return panel.tooltips ?? []
}

function getFallbackSeriesLabel(key: string) {
  if (key === 'vol') {
    return '量'
  }
  if (key === 'brick') {
    return '砖'
  }
  return key
}

function formatSeriesLabel(key: string, series?: DetailChartSeries | null) {
  const label = series?.label?.trim()
  return label ? label : getFallbackSeriesLabel(key)
}

function formatTooltipLabel(tooltip: DetailChartTooltip) {
  const label = tooltip.label?.trim()
  return label ? label : getFallbackSeriesLabel(tooltip.key)
}

function formatFieldValue(value: unknown) {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return formatNumber(value)
  }
  if (typeof value === 'string' && value.trim() !== '') {
    return value
  }
  if (typeof value === 'boolean') {
    return value ? '是' : '否'
  }
  return '--'
}

function formatTooltipValue(row: CyqChenChartRow, tooltip: DetailChartTooltip) {
  const value = row[tooltip.value_key]
  if (tooltip.format === 'percent') {
    return typeof value === 'number' && Number.isFinite(value) ? `${value.toFixed(2)}%` : '--'
  }
  if (tooltip.format === 'ratio') {
    return typeof value === 'number' && Number.isFinite(value) ? value.toFixed(2) : '--'
  }
  return formatFieldValue(value)
}

function buildSeriesRuntimeValue(row: CyqChenChartRow | null, series: DetailChartSeries) {
  if (!row) {
    return '--'
  }
  return formatFieldValue(row[series.key])
}

function isMainChartPanel(panel: DetailKlinePanel) {
  return panel.role === 'main' || panel.key === 'price' || panel.kind === 'candles'
}

function resolveChartPanelRenderKind(panel: DetailKlinePanel) {
  if (isMainChartPanel(panel)) {
    return 'candles'
  }
  const series = getPanelSeries(panel)
  if (panel.kind === 'brick' || series.some((item) => item.kind === 'brick')) {
    return 'brick'
  }
  if (panel.kind === 'bar' || series.some((item) => item.kind === 'bar')) {
    return 'bar'
  }
  return 'line'
}

function buildLineSegments(
  items: CyqChenChartRow[],
  key: string,
  xAt: (index: number) => number,
  yAt: (value: number) => number,
) {
  const segments: Array<Array<{ x: number; y: number }>> = []
  let current: Array<{ x: number; y: number }> = []

  items.forEach((row, index) => {
    const value = getChartRowNumber(row, key)
    if (value === null) {
      if (current.length > 0) {
        segments.push(current)
        current = []
      }
      return
    }
    current.push({ x: xAt(index), y: yAt(value) })
  })

  if (current.length > 0) {
    segments.push(current)
  }

  return segments
}

function buildLinePath(points: Array<{ x: number; y: number }>) {
  return points
    .map((point, index) => `${index === 0 ? 'M' : 'L'} ${point.x.toFixed(2)} ${point.y.toFixed(2)}`)
    .join(' ')
}

function buildBrickBodies(
  items: CyqChenChartRow[],
  key: string,
  initialPrevious: number | null = null,
) {
  const bodies: Array<{
    tradeDate: string
    itemIndex: number
    open: number
    close: number
    high: number
    low: number
  }> = []
  let previous: number | null = initialPrevious

  items.forEach((row, itemIndex) => {
    const current = getChartRowNumber(row, key)
    if (current === null) {
      previous = null
      return
    }
    if (previous === null) {
      previous = current
      return
    }

    const open = previous
    const close = current
    bodies.push({
      tradeDate: row.tradeDate,
      itemIndex,
      open,
      close,
      high: Math.max(open, close),
      low: Math.min(open, close),
    })
    previous = current
  })

  return bodies
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

function buildNiceAxisGrid(min: number, max: number, targetTickCount = 7) {
  if (!Number.isFinite(min) || !Number.isFinite(max) || max <= min) {
    return []
  }

  const span = max - min
  const rawStep = span / Math.max(targetTickCount - 1, 1)
  const magnitude = 10 ** Math.floor(Math.log10(Math.max(rawStep, 1e-6)))
  const candidateSteps = Array.from(
    new Set(
      [1, 2, 2.5, 5, 10]
        .flatMap((factor) => [factor * magnitude, factor * magnitude * 0.1])
        .filter((step) => Number.isFinite(step) && step > 0),
    ),
  ).sort((left, right) => left - right)

  let bestValues: number[] = []
  let bestScore = Number.POSITIVE_INFINITY
  for (const step of candidateSteps) {
    const epsilon = step * 1e-6
    const start = Math.ceil((min - epsilon) / step) * step
    const end = Math.floor((max + epsilon) / step) * step
    if (end < start) {
      continue
    }

    const values: number[] = []
    for (let value = end; value >= start - epsilon; value -= step) {
      values.push(Number(value.toFixed(8)))
    }
    const score = Math.abs(values.length - targetTickCount) + (values.length < 4 ? 2 : 0) + step / Math.max(span, 1)
    if (values.length > 0 && score < bestScore) {
      bestScore = score
      bestValues = values
    }
  }

  return bestValues
}

function buildAxisLabelValues(values: number[], kind: string) {
  void kind
  if (values.length <= 5) {
    return values
  }

  const keep = new Set<number>()
  const midpointIndex = Math.floor(values.length / 2)
  const step = Math.max(Math.ceil(values.length / 5), 2)
  values.forEach((value, index) => {
    if (index === 0 || index === values.length - 1 || index === midpointIndex || index % step === 0) {
      keep.add(value)
    }
  })
  return values.filter((value) => keep.has(value))
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

function getChartViewportContentRect(viewport: HTMLDivElement) {
  const svg = viewport.querySelector<SVGSVGElement>('.details-chart-svg')
  return svg && svg.clientWidth > 0 && svg.clientHeight > 0
    ? svg.getBoundingClientRect()
    : viewport.getBoundingClientRect()
}

function resolveChartAnchorFromClientX(
  viewport: HTMLDivElement,
  clientX: number,
  itemCount: number,
  layoutSlotCount: number,
  reserveCyqPanelWidth: boolean,
): CyqChenChartAnchorSnapshot | null {
  if (itemCount <= 0 || layoutSlotCount <= 0) {
    return null
  }

  const svgRect = getChartViewportContentRect(viewport)
  if (svgRect.width <= 0) {
    return null
  }

  const chartXPercent = clampNumber((clientX - svgRect.left) / svgRect.width * 100, 0, 99.9999)
  const plotStartPercent = CHART_MARGIN.left / CHART_VIEWBOX_WIDTH * 100
  const plotEndPercent = getChartKlinePlotRight(reserveCyqPanelWidth) / CHART_VIEWBOX_WIDTH * 100
  const plotXPercent = clampNumber((chartXPercent - plotStartPercent) / (plotEndPercent - plotStartPercent), 0, 1)
  const leadingSlotCount = Math.max(layoutSlotCount - itemCount, 0)
  const visiblePosition = clampNumber(
    plotXPercent * layoutSlotCount - leadingSlotCount,
    0,
    Math.max(itemCount - 1, 0),
  )

  return {
    visiblePosition,
    visibleRatio: itemCount <= 1 ? 1 : visiblePosition / (itemCount - 1),
  }
}

function buildChartPointerSnapshot(
  viewport: HTMLDivElement,
  clientX: number,
  clientY: number,
  itemCount: number,
  layoutSlotCount: number,
  reserveCyqPanelWidth: boolean,
): CyqChenChartPointerSnapshot | null {
  if (itemCount <= 0 || layoutSlotCount <= 0) {
    return null
  }

  const viewportRect = viewport.getBoundingClientRect()
  if (viewportRect.width <= 0 || viewportRect.height <= 0) {
    return null
  }

  const svgRect = getChartViewportContentRect(viewport)
  const chartXPercent = clampNumber((clientX - svgRect.left) / svgRect.width * 100, 0, 99.9999)
  const chartYPercent = clampNumber(
    (clientY - svgRect.top) / svgRect.height * 100,
    CHART_CURSOR_Y_MIN,
    CHART_CURSOR_Y_MAX,
  )
  const visibleIndex = resolveVisibleIndexFromChartX(
    chartXPercent,
    itemCount,
    layoutSlotCount,
    reserveCyqPanelWidth,
  )
  if (visibleIndex === null) {
    return null
  }

  return {
    cursorXPercent: getChartItemX(visibleIndex, itemCount, layoutSlotCount, reserveCyqPanelWidth) / CHART_VIEWBOX_WIDTH * 100,
    cursorYPercent: chartYPercent,
    visibleIndex,
  }
}

function isPointerNearChartFocus(
  _panelKey: string,
  viewport: HTMLDivElement,
  clientX: number,
  _clientY: number,
  focus: CyqChenChartFocus | null,
) {
  if (!focus) {
    return false
  }

  const rect = getChartViewportContentRect(viewport)
  if (rect.width <= 0 || rect.height <= 0) {
    return false
  }

  const focusClientX = rect.left + rect.width * focus.cursorXPercent / 100
  return Math.abs(clientX - focusClientX) <= CHART_TOUCH_FOCUS_HIT_SLOP
}

function formatTooltipPercent(value: number | null | undefined) {
  if (!isFiniteNumber(value)) {
    return '--'
  }
  return `${value.toFixed(2)}%`
}

function formatRatioPercent(value: number | null | undefined) {
  if (!isFiniteNumber(value)) {
    return '--'
  }
  return `${(value * 100).toFixed(2)}%`
}

function buildPanelDrawingTooltipRows(panel: DetailKlinePanel, item: CyqChenChartRow): FieldRow[] {
  const rows: FieldRow[] = []
  if (isMainChartPanel(panel)) {
    rows.push(
      { label: 'C', value: formatFieldValue(item.close) },
      { label: 'O', value: formatFieldValue(item.open) },
      { label: 'H', value: formatFieldValue(item.high) },
      { label: 'L', value: formatFieldValue(item.low) },
      { label: '换手', value: formatTooltipPercent(item.turnoverRate) },
    )
  }

  for (const series of getPanelSeries(panel)) {
    rows.push({
      label: formatSeriesLabel(series.key, series),
      value: formatFieldValue(item[series.key]),
    })
  }

  return rows.filter((row) => row.value !== '--')
}

function buildDetailTooltipRows(panel: DetailKlinePanel, item: CyqChenChartRow | null): TooltipSection[] {
  if (!item) {
    return []
  }

  const tooltipRows = getPanelTooltips(panel)
    .map((tooltip) => ({
      label: formatTooltipLabel(tooltip),
      value: formatTooltipValue(item, tooltip),
    }))
    .filter((row) => row.value !== '--')
  const shownLabels = new Set(tooltipRows.map((row) => row.label))
  const drawingRows = buildPanelDrawingTooltipRows(panel, item).filter((row) => !shownLabels.has(row.label))
  const sections: TooltipSection[] = []
  if (tooltipRows.length > 0) {
    sections.push({ key: `${panel.key}-tooltip`, rows: tooltipRows })
  }
  if (drawingRows.length > 0) {
    sections.push({
      key: `${panel.key}-drawing`,
      variant: isMainChartPanel(panel) ? 'ohlc' : 'default',
      rows: drawingRows,
    })
  }
  return sections
}

function getMarkerYValue(row: CyqChenChartRow, marker: DetailChartMarker) {
  const key = marker.y_key?.trim()
  if (!key) {
    return getChartRowNumber(row, 'close')
  }

  const normalizedKey = key.toLowerCase()
  const mappedKey =
    normalizedKey === 'o' || normalizedKey === 'open'
      ? 'open'
      : normalizedKey === 'h' || normalizedKey === 'high'
        ? 'high'
        : normalizedKey === 'l' || normalizedKey === 'low'
          ? 'low'
          : normalizedKey === 'c' || normalizedKey === 'close'
            ? 'close'
            : normalizedKey === 'v' || normalizedKey === 'vol'
              ? 'vol'
              : key
  return getChartRowNumber(row, mappedKey)
}

function buildChartMarkerOverlayPoints(
  panel: DetailKlinePanel,
  items: CyqChenChartRow[],
  xAt: (itemIndex: number) => number,
  yAt: (value: number) => number,
): ChartMarkerOverlayPoint[] {
  const markers = panel.markers ?? []
  if (markers.length === 0) {
    return []
  }

  return markers.flatMap((marker) =>
    items.flatMap((row, itemIndex) => {
      if (row[marker.when_key] !== true) {
        return []
      }
      const value = getMarkerYValue(row, marker)
      if (value === null) {
        return []
      }

      const x = xAt(itemIndex)
      const baseY = yAt(value)
      const position = marker.position ?? 'value'
      const y = position === 'above'
        ? CHART_MARGIN.top + 10
        : position === 'below'
          ? CHART_VIEWBOX_HEIGHT - CHART_MARGIN.bottom - 10
          : baseY

      return [{
        key: `${panel.key}-${marker.key}-${row.tradeDate}`,
        leftPercent: (x / CHART_VIEWBOX_WIDTH) * 100,
        topPercent: (y / CHART_VIEWBOX_HEIGHT) * 100,
        shape: marker.shape,
        color: marker.color ?? CANDLE_UP_COLOR,
        text: marker.text,
      }]
    }),
  )
}

function renderChartMarkerOverlayPoint(point: ChartMarkerOverlayPoint) {
  const shape = point.shape ?? 'dot'
  return (
    <span
      className={[
        'details-chart-marker',
        `details-chart-marker-${shape}`,
        point.text ? 'details-chart-marker-with-text' : '',
      ].filter(Boolean).join(' ')}
      key={point.key}
      style={{
        left: `${point.leftPercent}%`,
        top: `${point.topPercent}%`,
        '--details-chart-marker-color': point.color,
      } as CSSProperties}
    >
      {point.text ? <span>{point.text}</span> : null}
    </span>
  )
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
  return '混合'
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

function buildWatermarkCode(tsCode: string | null | undefined) {
  const code = tsCode?.trim() ?? ''
  if (!code) {
    return '--'
  }
  return code.split('.')[0] || code
}

function CyqChenProjectChart({
  kline,
  panels,
  snapshot,
  selectedTradeDate,
  chipPeakMode,
  watermarkName,
  watermarkCode,
  onChipPeakModeChange,
  onSelectTradeDate,
}: {
  kline: CyqChenChartRow[]
  panels: DetailKlinePanel[]
  snapshot: CyqChenSnapshot | null
  selectedTradeDate: string
  chipPeakMode: ChipPeakMode
  watermarkName: string
  watermarkCode: string
  onChipPeakModeChange: (mode: ChipPeakMode) => void
  onSelectTradeDate: (tradeDate: string) => void
}) {
  const chartShellRef = useRef<HTMLDivElement | null>(null)
  const chartDragRef = useRef<CyqChenChartDragState | null>(null)
  const chartTouchPointersRef = useRef<Map<number, CyqChenChartTouchPointer>>(new Map())
  const chartPinchRef = useRef<CyqChenChartPinchState | null>(null)
  const [visibleBarCount, setVisibleBarCount] = useState(DEFAULT_VISIBLE_BARS)
  const [visibleStartIndex, setVisibleStartIndex] = useState(0)
  const [focus, setFocus] = useState<CyqChenChartFocus | null>(null)
  const [chartLayoutWidth, setChartLayoutWidth] = useState(readInitialChartLayoutWidth)
  const [chartMainWidthRatio, setChartMainWidthRatio] = useState(() => readStoredChartMainWidthRatio())
  const indicatorPanels = panels.filter((panel) => !isMainChartPanel(panel))
  const pricePanel = panels.find(isMainChartPanel) ?? {
    key: 'price',
    label: '主K',
    role: 'main',
    kind: 'candles',
    series: [],
    tooltips: [],
  } satisfies DetailKlinePanel

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
    [
      item.open,
      item.high,
      item.low,
      item.close,
      ...getPanelSeries(pricePanel).map((series) => getChartRowNumber(item, series.key)),
    ].filter(isFiniteNumber),
  )
  const domain = buildDomain(priceValues)
  const selectedVisibleIndex = selectedTradeDate
    ? visibleItems.findIndex((item) => item.tradeDate === selectedTradeDate)
    : -1
  const focusVisibleIndex = focus ? focus.absoluteIndex - effectiveVisibleStart : null
  const focusedRow = focusVisibleIndex !== null && focusVisibleIndex >= 0 && focusVisibleIndex < visibleItems.length
    ? visibleItems[focusVisibleIndex]
    : null
  const focusXPercent = focusedRow && focus ? focus.cursorXPercent : null
  const tooltipHorizontalClass = (focusXPercent ?? 0) > CHART_TOOLTIP_LEFT_THRESHOLD
    ? 'details-chart-tooltip-left'
    : 'details-chart-tooltip-right'
  const chartRangeText = visibleItems.length > 0
    ? `${visibleItems[0]?.tradeDate ?? '--'} ~ ${visibleItems[visibleItems.length - 1]?.tradeDate ?? '--'}`
    : '--'
  const chartMainPanelHeight = chartLayoutWidth * chartMainWidthRatio
  const chartIndicatorPanelHeight = Math.max(Math.min(chartLayoutWidth * 0.22, 180), 118)
  const chartShellHeight = chartMainPanelHeight + indicatorPanels.length * chartIndicatorPanelHeight
  const priceHeaderRuntimeRow = focusedRow ?? visibleItems[visibleItems.length - 1] ?? null

  function setChartZoomAnchored(nextCount: number, anchorAbsoluteIndex: number, anchorRatio: number) {
    if (totalItems === 0) {
      return
    }

    const resolvedCount = clampNumber(Math.round(nextCount), minVisibleBars, totalItems)
    const nextMaxVisibleStart = Math.max(totalItems - resolvedCount, 0)
    const nextVisibleStart = clampNumber(
      Math.round(anchorAbsoluteIndex - clampNumber(anchorRatio, 0, 1) * Math.max(resolvedCount - 1, 0)),
      0,
      nextMaxVisibleStart,
    )

    setVisibleBarCount(resolvedCount)
    setVisibleStartIndex(nextVisibleStart)
  }

  function getChartTouchPointers(panelKey: string) {
    return [...chartTouchPointersRef.current.values()].filter(
      (pointer) => pointer.panelKey === panelKey,
    )
  }

  function getChartPointerDistance(
    firstPointer: CyqChenChartTouchPointer,
    secondPointer: CyqChenChartTouchPointer,
  ) {
    return Math.hypot(
      secondPointer.clientX - firstPointer.clientX,
      secondPointer.clientY - firstPointer.clientY,
    )
  }

  function startChartPinch(
    panelKey: string,
    viewport: HTMLDivElement,
    pointers: CyqChenChartTouchPointer[],
  ) {
    if (totalItems === 0 || pointers.length < 2) {
      return false
    }

    const [firstPointer, secondPointer] = pointers
    const midpointX = (firstPointer.clientX + secondPointer.clientX) / 2
    const anchor = resolveChartAnchorFromClientX(
      viewport,
      midpointX,
      visibleItems.length,
      layoutSlotCount,
      reserveCyqPanelWidth,
    )
    if (!anchor) {
      return false
    }

    chartPinchRef.current = {
      panelKey,
      pointerIds: [firstPointer.pointerId, secondPointer.pointerId],
      startDistance: Math.max(getChartPointerDistance(firstPointer, secondPointer), CHART_PINCH_MIN_DISTANCE),
      startVisibleBarCount: effectiveVisibleBarCount,
      startAnchorAbsoluteIndex: effectiveVisibleStart + anchor.visiblePosition,
      startAnchorRatio: anchor.visibleRatio,
    }
    chartDragRef.current = null
    setFocus(null)
    return true
  }

  function updateChartPinchZoom(event: ReactPointerEvent<HTMLDivElement>) {
    const pinchState = chartPinchRef.current
    if (!pinchState || !pinchState.pointerIds.includes(event.pointerId)) {
      return false
    }

    const [firstPointerId, secondPointerId] = pinchState.pointerIds
    const firstPointer = chartTouchPointersRef.current.get(firstPointerId)
    const secondPointer = chartTouchPointersRef.current.get(secondPointerId)
    if (!firstPointer || !secondPointer) {
      chartPinchRef.current = null
      return true
    }

    const distance = Math.max(getChartPointerDistance(firstPointer, secondPointer), CHART_PINCH_MIN_DISTANCE)
    const scale = distance / pinchState.startDistance
    if (!Number.isFinite(scale) || scale <= 0) {
      return true
    }

    setChartZoomAnchored(
      pinchState.startVisibleBarCount / scale,
      pinchState.startAnchorAbsoluteIndex,
      pinchState.startAnchorRatio,
    )
    return true
  }

  function zoomChartFromWheel(viewport: HTMLDivElement, clientX: number, deltaY: number) {
    const anchor = resolveChartAnchorFromClientX(
      viewport,
      clientX,
      visibleItems.length,
      layoutSlotCount,
      reserveCyqPanelWidth,
    )
    if (!anchor) {
      return false
    }

    setChartZoomAnchored(
      effectiveVisibleBarCount * Math.exp(deltaY * CHART_WHEEL_ZOOM_FACTOR),
      effectiveVisibleStart + anchor.visiblePosition,
      anchor.visibleRatio,
    )
    return true
  }

  useEffect(() => {
    if (typeof window === 'undefined') {
      return
    }

    const updateChartLayoutWidth = () => {
      const shellWidth = chartShellRef.current?.getBoundingClientRect().width
      const nextWidth = typeof shellWidth === 'number' && shellWidth > 0
        ? shellWidth
        : window.innerWidth
      setChartLayoutWidth(nextWidth)
      setChartMainWidthRatio(readStoredChartMainWidthRatio())
    }

    updateChartLayoutWidth()
    window.addEventListener('resize', updateChartLayoutWidth)

    const shell = chartShellRef.current
    const observer = typeof ResizeObserver === 'undefined' || !shell
      ? null
      : new ResizeObserver(() => {
        updateChartLayoutWidth()
      })
    if (observer && shell) {
      observer.observe(shell)
    }

    return () => {
      window.removeEventListener('resize', updateChartLayoutWidth)
      observer?.disconnect()
    }
  }, [])

  useEffect(() => {
    const chartShell = chartShellRef.current
    if (!chartShell) {
      return
    }

    function handleNativeChartWheel(event: WheelEvent) {
      if (!event.ctrlKey && !event.metaKey) {
        return
      }

      const targetElement = event.target instanceof Element ? event.target : null
      const viewport = targetElement?.closest<HTMLDivElement>('.details-chart-viewport') ?? null
      if (!viewport || !chartShell?.contains(viewport)) {
        return
      }

      event.preventDefault()
      if (totalItems === 0) {
        return
      }

      zoomChartFromWheel(viewport, event.clientX, event.deltaY)
    }

    chartShell.addEventListener('wheel', handleNativeChartWheel, { passive: false })
    return () => {
      chartShell.removeEventListener('wheel', handleNativeChartWheel)
    }
  })

  function buildChartFocus(
    panelKey: string,
    viewport: HTMLDivElement,
    clientX: number,
    clientY: number,
    pinned: boolean,
  ) {
    const pointer = buildChartPointerSnapshot(
      viewport,
      clientX,
      clientY,
      visibleItems.length,
      layoutSlotCount,
      reserveCyqPanelWidth,
    )
    if (!pointer) {
      return null
    }

    return {
      absoluteIndex: effectiveVisibleStart + pointer.visibleIndex,
      panelKey,
      cursorXPercent: pointer.cursorXPercent,
      cursorYPercent: pointer.cursorYPercent,
      pinned,
    }
  }

  function applyChartFocus(nextFocus: CyqChenChartFocus | null) {
    setFocus(nextFocus)
    if (nextFocus) {
      onSelectTradeDate(kline[nextFocus.absoluteIndex]?.tradeDate ?? '')
    }
  }

  function clearChartPointerState(event: ReactPointerEvent<HTMLDivElement>) {
    try {
      if (event.currentTarget.hasPointerCapture(event.pointerId)) {
        event.currentTarget.releasePointerCapture(event.pointerId)
      }
    } catch {
      // Ignore stale pointer capture state during cleanup.
    }
    chartTouchPointersRef.current.delete(event.pointerId)
    chartDragRef.current = null
  }

  function onChartPointerDown(panelKey: string, event: ReactPointerEvent<HTMLDivElement>) {
    if (event.pointerType === 'mouse' && event.button !== 0) {
      return
    }

    const rect = event.currentTarget.getBoundingClientRect()
    if (rect.width <= 0) {
      return
    }

    if (event.pointerType === 'touch') {
      chartTouchPointersRef.current.set(event.pointerId, {
        pointerId: event.pointerId,
        panelKey,
        clientX: event.clientX,
        clientY: event.clientY,
      })
      try {
        event.currentTarget.setPointerCapture(event.pointerId)
      } catch {
        // Ignore browsers that do not support pointer capture for this target.
      }

      const touchPointers = getChartTouchPointers(panelKey)
      if (touchPointers.length >= 2) {
        event.preventDefault()
        startChartPinch(panelKey, event.currentTarget, touchPointers.slice(0, 2))
        return
      }
    }

    const isTouchPointer = event.pointerType !== 'mouse'
    const mode = focus?.pinned
      ? isTouchPointer && !isPointerNearChartFocus(panelKey, event.currentTarget, event.clientX, event.clientY, focus)
        ? 'dismiss'
        : 'focus'
      : maxVisibleStart > 0
        ? 'pan'
        : 'tap'

    try {
      event.currentTarget.setPointerCapture(event.pointerId)
    } catch {
      // Ignore browsers that do not support pointer capture for this target.
    }

    chartDragRef.current = {
      pointerId: event.pointerId,
      panelKey,
      mode,
      startClientX: event.clientX,
      startClientY: event.clientY,
      startVisibleStart: effectiveVisibleStart,
      barsPerPixel: effectiveVisibleBarCount / rect.width,
      maxVisibleStart,
      moved: false,
    }
  }

  function onChartPointerMove(panelKey: string, event: ReactPointerEvent<HTMLDivElement>) {
    if (event.pointerType === 'touch') {
      const trackedPointer = chartTouchPointersRef.current.get(event.pointerId)
      if (trackedPointer) {
        chartTouchPointersRef.current.set(event.pointerId, {
          ...trackedPointer,
          clientX: event.clientX,
          clientY: event.clientY,
        })
      }

      if (updateChartPinchZoom(event)) {
        event.preventDefault()
        return
      }
    }

    const dragState = chartDragRef.current
    if (!dragState) {
      if (event.pointerType !== 'mouse' || !focus?.pinned) {
        return
      }

      const nextFocus = buildChartFocus(panelKey, event.currentTarget, event.clientX, event.clientY, true)
      if (nextFocus) {
        applyChartFocus(nextFocus)
      }
      return
    }

    if (dragState.pointerId !== event.pointerId || dragState.panelKey !== panelKey) {
      return
    }

    const moveX = event.clientX - dragState.startClientX
    const moveY = event.clientY - dragState.startClientY
    const absMoveX = Math.abs(moveX)
    const absMoveY = Math.abs(moveY)
    if (
      event.pointerType === 'touch' &&
      !dragState.moved &&
      absMoveY >= CHART_POINTER_DRAG_THRESHOLD &&
      absMoveY > absMoveX * CHART_TOUCH_VERTICAL_SCROLL_RATIO
    ) {
      clearChartPointerState(event)
      return
    }

    const moveDistance = Math.hypot(moveX, moveY)
    if (!dragState.moved && moveDistance >= CHART_POINTER_DRAG_THRESHOLD) {
      dragState.moved = true
    }

    if (dragState.mode === 'pan') {
      if (!dragState.moved) {
        return
      }

      const deltaBars = Math.round(moveX * dragState.barsPerPixel)
      setVisibleStartIndex(clampNumber(dragState.startVisibleStart - deltaBars, 0, dragState.maxVisibleStart))
      return
    }

    if (dragState.mode === 'dismiss') {
      return
    }

    if (dragState.mode !== 'focus' || !dragState.moved) {
      return
    }

    const nextFocus = buildChartFocus(panelKey, event.currentTarget, event.clientX, event.clientY, true)
    if (nextFocus) {
      applyChartFocus(nextFocus)
    }
  }

  function onChartPointerUp(panelKey: string, event: ReactPointerEvent<HTMLDivElement>) {
    const pinchState = chartPinchRef.current
    if (pinchState?.pointerIds.includes(event.pointerId)) {
      event.preventDefault()
      chartPinchRef.current = null
      clearChartPointerState(event)
      return
    }

    const dragState = chartDragRef.current
    clearChartPointerState(event)
    if (!dragState || dragState.pointerId !== event.pointerId || dragState.panelKey !== panelKey) {
      return
    }

    if (dragState.mode === 'dismiss') {
      if (!dragState.moved) {
        applyChartFocus(null)
      }
      return
    }

    if (dragState.moved) {
      return
    }

    const nextFocus = buildChartFocus(panelKey, event.currentTarget, event.clientX, event.clientY, true)
    if (!nextFocus) {
      if (focus?.pinned) {
        applyChartFocus(null)
      }
      return
    }

    if (
      focus?.pinned &&
      focus.panelKey === nextFocus.panelKey &&
      focus.absoluteIndex === nextFocus.absoluteIndex
    ) {
      applyChartFocus(null)
      return
    }

    applyChartFocus(nextFocus)
  }

  function onChartPointerLeave(panelKey: string, event: ReactPointerEvent<HTMLDivElement>) {
    const dragState = chartDragRef.current
    if (dragState?.pointerId === event.pointerId && dragState.panelKey === panelKey) {
      return
    }

    if (!focus?.pinned) {
      applyChartFocus(null)
    }
  }

  function onChartPointerCancel(_panelKey: string, event: ReactPointerEvent<HTMLDivElement>) {
    if (chartPinchRef.current?.pointerIds.includes(event.pointerId)) {
      chartPinchRef.current = null
    }
    clearChartPointerState(event)
  }

  function renderIndicatorPanel(panel: DetailKlinePanel) {
    const seriesList = getPanelSeries(panel)
    const renderKind = resolveChartPanelRenderKind(panel)
    const values = visibleItems.flatMap((item) =>
      seriesList
        .map((series) => getChartRowNumber(item, series.key))
        .filter(isFiniteNumber),
    )
    const brickSeries = seriesList.find((series) => series.kind === 'brick') ?? null
    const brickKey = brickSeries?.key ?? seriesList[0]?.key ?? 'brick'
    const previousRow = effectiveVisibleStart > 0 ? kline[effectiveVisibleStart - 1] ?? null : null
    const previousBrick = previousRow ? getChartRowNumber(previousRow, brickKey) : null
    const brickBodies = renderKind === 'brick' ? buildBrickBodies(visibleItems, brickKey, previousBrick) : []
    const panelDomain = buildDomain(
      renderKind === 'brick'
        ? [
            ...values,
            ...(previousBrick === null ? [] : [previousBrick]),
            ...brickBodies.flatMap((body) => [body.low, body.high]),
          ]
        : values,
    )
    const dateTickIndices = buildDateTickIndices(visibleItems.length)
    const activeVisibleIndex = focus?.panelKey === panel.key &&
      focus.absoluteIndex >= effectiveVisibleStart &&
      focus.absoluteIndex < effectiveVisibleStart + visibleItems.length
      ? focus.absoluteIndex - effectiveVisibleStart
      : null
    const activeRow = activeVisibleIndex !== null ? visibleItems[activeVisibleIndex] ?? null : null
    const activeFocusXPercent = activeVisibleIndex !== null
      ? xAt(activeVisibleIndex) / CHART_VIEWBOX_WIDTH * 100
      : null
    const activeTooltipSections = buildDetailTooltipRows(panel, activeRow)
    const activeTooltipHorizontalClass = (activeFocusXPercent ?? 0) > CHART_TOOLTIP_LEFT_THRESHOLD
      ? 'details-chart-tooltip-left'
      : 'details-chart-tooltip-right'
    const headerRuntimeRow = activeRow ?? visibleItems[visibleItems.length - 1] ?? null

    return (
      <section className="details-chart-panel" key={panel.key}>
        <header className="details-chart-panel-head">
          <div className="details-chart-panel-head-main">
            <strong>{panel.label || panel.key}</strong>
            {seriesList.length > 0 ? (
              <div className="details-chart-panel-head-series">
                {seriesList.map((series, seriesIndex) => (
                  <span
                    className="details-chart-panel-head-series-tag"
                    key={`${panel.key}-${series.key}`}
                    style={{ color: getSeriesColor(seriesIndex, series) }}
                  >
                    <span className="details-chart-panel-head-series-label">
                      {formatSeriesLabel(series.key, series)}
                    </span>
                    <strong className="details-chart-panel-head-series-value">
                      {buildSeriesRuntimeValue(headerRuntimeRow, series)}
                    </strong>
                  </span>
                ))}
              </div>
            ) : (
              <small>{snapshotLabel(snapshot)}</small>
            )}
          </div>
          <span>{renderKind}</span>
        </header>
        <div
          className="details-chart-viewport"
          onPointerDown={(event) => onChartPointerDown(panel.key, event)}
          onPointerMove={(event) => onChartPointerMove(panel.key, event)}
          onPointerUp={(event) => onChartPointerUp(panel.key, event)}
          onPointerLeave={(event) => onChartPointerLeave(panel.key, event)}
          onPointerCancel={(event) => onChartPointerCancel(panel.key, event)}
        >
          {panelDomain && visibleItems.length > 0 ? (() => {
            const yAt = (value: number) =>
              CHART_MARGIN.top + (panelDomain.max - value) / (panelDomain.max - panelDomain.min) * plotHeight
            const tickValues = renderKind === 'brick'
              ? buildNiceAxisGrid(panelDomain.min, panelDomain.max)
              : buildCenteredPercentGrid(panelDomain.min, panelDomain.max)
            const labelValues = buildAxisLabelValues(tickValues, renderKind)
            const gridValues = renderKind === 'candles' ? tickValues : labelValues
            const yAxisLabels = labelValues.map((value) => ({
              key: `${panel.key}-y-${value}`,
              value: formatAxisValue(value),
              topPercent: (CHART_MARGIN.top + (panelDomain.max - value) / (panelDomain.max - panelDomain.min) * plotHeight) / CHART_VIEWBOX_HEIGHT * 100,
            }))
            const xAxisLabels = dateTickIndices.map((itemIndex) => ({
              key: `${panel.key}-x-${visibleItems[itemIndex]?.tradeDate ?? itemIndex}`,
              value: formatTradeDateLabel(visibleItems[itemIndex]?.tradeDate ?? ''),
              leftPercent: xAt(itemIndex) / CHART_VIEWBOX_WIDTH * 100,
            }))
            const barWidth = Math.max(Math.min(step * 0.58, 18), 3)
            const markerOverlayPoints = buildChartMarkerOverlayPoints(panel, visibleItems, xAt, yAt)

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
                        key={`grid-${panel.key}-${value}`}
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
                      key={`guide-${panel.key}-${visibleItems[itemIndex]?.tradeDate ?? itemIndex}`}
                      x1={xAt(itemIndex)}
                      y1={CHART_MARGIN.top}
                      x2={xAt(itemIndex)}
                      y2={CHART_VIEWBOX_HEIGHT - CHART_MARGIN.bottom}
                    />
                  ))}
                  {renderKind === 'brick' ? brickBodies.map((body) => {
                    const x = xAt(body.itemIndex)
                    const openY = yAt(body.open)
                    const closeY = yAt(body.close)
                    const highY = yAt(body.high)
                    const lowY = yAt(body.low)
                    const bodyTop = Math.min(openY, closeY)
                    const bodyHeight = Math.max(Math.abs(openY - closeY), 1.6)
                    const direction = body.close > body.open ? 'up' : body.close < body.open ? 'down' : 'flat'
                    const fallbackColor = direction === 'up'
                      ? CANDLE_UP_COLOR
                      : direction === 'down'
                        ? CANDLE_DOWN_COLOR
                        : CANDLE_FLAT_COLOR
                    const sourceRow = visibleItems[body.itemIndex]
                    const color = sourceRow
                      ? getSeriesColorForRow(sourceRow, 0, brickSeries, fallbackColor)
                      : fallbackColor
                    const bodyWidth = Math.max(Math.min(step * 0.72, 22), 4)

                    return (
                      <g key={`${panel.key}-${body.tradeDate}`}>
                        <line
                          className="details-chart-candle-wick"
                          x1={x}
                          y1={highY}
                          x2={x}
                          y2={lowY}
                          stroke={color}
                        />
                        <rect
                          className="details-chart-brick-body"
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
                  }) : seriesList.map((series, seriesIndex) => {
                    const color = getSeriesColor(seriesIndex, series)
                    const kind = renderKind === 'bar' ? series.kind ?? 'bar' : series.kind ?? 'line'
                    if (kind === 'bar' || kind === 'histogram') {
                      const baseValue = isFiniteNumber(series.base_value) ? series.base_value : 0
                      const baseY = yAt(clampNumber(baseValue, panelDomain.min, panelDomain.max))
                      return (
                        <g key={series.key}>
                          {visibleItems.map((item, itemIndex) => {
                            const value = getChartRowNumber(item, series.key)
                            if (!isFiniteNumber(value)) {
                              return null
                            }
                            const y = yAt(value)
                            const close = getChartRowNumber(item, 'close')
                            const prevClose = effectiveVisibleStart + itemIndex > 0
                              ? getChartRowNumber(kline[effectiveVisibleStart + itemIndex - 1], 'close')
                              : null
                            const fallbackColor = close !== null && prevClose !== null
                              ? close > prevClose
                                ? CANDLE_UP_COLOR
                                : close < prevClose
                                  ? CANDLE_DOWN_COLOR
                                  : CANDLE_FLAT_COLOR
                              : color
                            const fill = getSeriesColorForRow(item, seriesIndex, series, fallbackColor)
                            return (
                              <rect
                                className="details-chart-volume-bar"
                                key={`${series.key}-${item.tradeDate}`}
                                x={xAt(itemIndex) - barWidth / 2}
                                y={Math.min(y, baseY)}
                                width={barWidth}
                                height={Math.max(Math.abs(baseY - y), 1)}
                                fill={fill}
                                opacity={series.opacity ?? 1}
                                rx={1}
                              />
                            )
                          })}
                        </g>
                      )
                    }

                    const segments = buildLineSegments(visibleItems, series.key, xAt, yAt)
                    if (segments.length === 0) {
                      return null
                    }
                    return (
                      <g key={series.key}>
                        {segments.map((segment, segmentIndex) => (
                          <path
                            className="details-chart-line-path details-chart-line-path-indicator"
                            key={`${series.key}-${segmentIndex}`}
                            d={buildLinePath(segment)}
                            fill="none"
                            stroke={color}
                            strokeWidth={series.line_width ?? 1.6}
                            opacity={series.opacity ?? 0.95}
                          />
                        ))}
                      </g>
                    )
                  })}
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
                  {markerOverlayPoints.length > 0 ? (
                    <div className="details-chart-marker-layer">
                      {markerOverlayPoints.map(renderChartMarkerOverlayPoint)}
                    </div>
                  ) : null}
                  {focusXPercent !== null ? (
                    <div className="details-chart-crosshair-vertical" style={{ left: `${focusXPercent}%` }} />
                  ) : null}
                  {focus?.panelKey === panel.key && activeRow && activeFocusXPercent !== null ? (
                    <>
                      <div className="details-chart-crosshair-horizontal" style={{ top: `${focus.cursorYPercent}%` }} />
                      {activeTooltipSections.length > 0 ? (
                        <div
                          className={[
                            'details-chart-tooltip',
                            activeTooltipHorizontalClass,
                            focus.pinned ? 'details-chart-tooltip-pinned' : '',
                          ].filter(Boolean).join(' ')}
                          style={{
                            left: `${activeFocusXPercent}%`,
                            top: `${focus.cursorYPercent}%`,
                          }}
                        >
                          <div className="details-chart-tooltip-head">
                            <strong>{activeRow.tradeDate}</strong>
                          </div>
                          <div className="details-chart-tooltip-body">
                            {activeTooltipSections.map((section) => (
                              <div
                                className={[
                                  'details-chart-tooltip-grid',
                                  section.variant === 'ohlc' ? 'details-chart-tooltip-grid-ohlc' : '',
                                ].filter(Boolean).join(' ')}
                                key={section.key}
                              >
                                {section.rows.map((row) => (
                                  <div className="details-chart-tooltip-row" key={`${section.key}-${row.label}`}>
                                    <span>{row.label}</span>
                                    <strong>{row.value}</strong>
                                  </div>
                                ))}
                              </div>
                            ))}
                          </div>
                        </div>
                      ) : null}
                    </>
                  ) : null}
                </div>
              </>
            )
          })() : (
            <div className="details-chart-empty">暂无指标数据</div>
          )}
        </div>
      </section>
    )
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

      <div
        className="details-chart-shell cyq-chen-project-chart-shell"
        ref={chartShellRef}
        style={{
          height: `${chartShellHeight}px`,
          gridTemplateRows: [
            `${chartMainPanelHeight.toFixed(2)}px`,
            ...indicatorPanels.map(() => `${chartIndicatorPanelHeight.toFixed(2)}px`),
          ].join(' '),
        }}
      >
        <section className="details-chart-panel">
          <header className="details-chart-panel-head">
            <div className="details-chart-panel-head-main">
              <strong>{pricePanel.label || '主K'}</strong>
              {getPanelSeries(pricePanel).length > 0 ? (
                <div className="details-chart-panel-head-series">
                  {getPanelSeries(pricePanel).map((series, seriesIndex) => (
                    <span
                      className="details-chart-panel-head-series-tag"
                      key={`price-${series.key}`}
                      style={{ color: getSeriesColor(seriesIndex, series) }}
                    >
                      <span className="details-chart-panel-head-series-label">
                        {formatSeriesLabel(series.key, series)}
                      </span>
                      <strong className="details-chart-panel-head-series-value">
                        {buildSeriesRuntimeValue(priceHeaderRuntimeRow, series)}
                      </strong>
                    </span>
                  ))}
                </div>
              ) : (
                <small>{snapshotLabel(snapshot)}</small>
              )}
            </div>
            <span>candles</span>
          </header>

        <div
          className="details-chart-viewport"
          onPointerDown={(event) => onChartPointerDown(pricePanel.key, event)}
          onPointerMove={(event) => onChartPointerMove(pricePanel.key, event)}
          onPointerUp={(event) => onChartPointerUp(pricePanel.key, event)}
          onPointerLeave={(event) => onChartPointerLeave(pricePanel.key, event)}
          onPointerCancel={(event) => onChartPointerCancel(pricePanel.key, event)}
        >
          <div
            className="details-chart-watermark"
            style={{
              left: `${(CHART_MARGIN.left + klinePlotWidth / 2) / CHART_VIEWBOX_WIDTH * 100}%`,
            }}
          >
            <strong>{watermarkName}</strong>
            <span>{watermarkCode}</span>
          </div>
          {reserveCyqPanelWidth ? (
            <div
              className="details-chart-cyq-holder-switch"
              role="tablist"
              aria-label="筹码分布显示"
              onPointerDown={(event) => {
                event.stopPropagation()
              }}
            >
              {(['total', 'main', 'retail'] as const).map((mode) => (
                <button
                  key={mode}
                  type="button"
                  className={chipPeakMode === mode ? 'is-active' : ''}
                  onClick={(event) => {
                    event.stopPropagation()
                    onChipPeakModeChange(mode)
                  }}
                >
                  {chipModeLabel(mode)}
                </button>
              ))}
            </div>
          ) : null}
          {domain && visibleItems.length > 0 ? (() => {
              const yAt = (value: number) =>
                CHART_MARGIN.top + (domain.max - value) / (domain.max - domain.min) * plotHeight
              const bodyWidth = Math.max(Math.min(step * 0.58, 18), 3)
              const dateTickIndices = buildDateTickIndices(visibleItems.length)
              const tickValues = buildCenteredPercentGrid(domain.min, domain.max)
              const labelValues = buildAxisLabelValues(tickValues, 'candles')
              const gridValues = tickValues
              const yAxisLabels = labelValues.map((value) => ({
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
              const totalScaleMaxChip = visibleCyqBins.reduce((acc, bin) => {
                const value = bin.totalChip
                return isFiniteNumber(value) ? Math.max(acc, value) : acc
              }, 0)
              const selectedScaleMaxChip = visibleCyqBins.reduce((acc, bin) => {
                const value = chipValueByMode(bin, chipPeakMode)
                return isFiniteNumber(value) ? Math.max(acc, value) : acc
              }, 0)
              const maxChip = chipPeakMode === 'total'
                ? totalScaleMaxChip
                : totalScaleMaxChip || selectedScaleMaxChip
              const peakBin = findChipPeak(snapshot, chipPeakMode)
              const priceOverlaySeries = getPanelSeries(pricePanel)
              const markerOverlayPoints = buildChartMarkerOverlayPoints(pricePanel, visibleItems, xAt, yAt)
              const priceTooltipSections = buildDetailTooltipRows(pricePanel, focusedRow)

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

                    {priceOverlaySeries.map((series, seriesIndex) => {
                      const segments = buildLineSegments(visibleItems, series.key, xAt, yAt)
                      if (segments.length === 0) {
                        return null
                      }
                      return (
                        <g key={`price-overlay-${series.key}`}>
                          {segments.map((segment, segmentIndex) => (
                            <path
                              className="details-chart-line-path details-chart-line-path-main"
                              key={`${series.key}-${segmentIndex}`}
                              d={buildLinePath(segment)}
                              fill="none"
                              stroke={getSeriesColor(seriesIndex, series)}
                              strokeWidth={series.line_width ?? 1.6}
                              opacity={series.opacity ?? 0.95}
                            />
                          ))}
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
                          const retailRatio = totalChip > 0 && isFiniteNumber(bin.retailChip)
                            ? clampNumber(bin.retailChip / totalChip, 0, 1)
                            : 0
                          const selectedHolderRatio = totalChip > 0 && isFiniteNumber(chipValue)
                            ? clampNumber(chipValue / totalChip, 0, 1)
                            : 0
                          const mainWidth = chipPeakMode === 'total' ? resolvedBarWidth * mainRatio : 0
                          const retailWidth = chipPeakMode === 'total' ? resolvedBarWidth - mainWidth : 0
                          const barX = chipPanelRight - resolvedBarWidth

                          return chipPeakMode === 'total' ? (
                            <g key={`${bin.index}-${bin.priceLow}`}>
                              <title>
                                {`混合 ${formatNumber(bin.totalChip, 4)}；主力 ${formatRatioPercent(mainRatio)}，散户 ${formatRatioPercent(retailRatio)}`}
                              </title>
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
                            >
                              <title>
                                {`${chipModeLabel(chipPeakMode)} ${formatNumber(chipValue, 4)}；占该价位 ${formatRatioPercent(selectedHolderRatio)}`}
                              </title>
                            </rect>
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
                    {markerOverlayPoints.length > 0 ? (
                      <div className="details-chart-marker-layer">
                        {markerOverlayPoints.map(renderChartMarkerOverlayPoint)}
                      </div>
                    ) : null}
                    {focusXPercent !== null ? (
                      <div className="details-chart-crosshair-vertical" style={{ left: `${focusXPercent}%` }} />
                    ) : null}
                    {focus?.panelKey === pricePanel.key && focusedRow ? (
                      <>
                        <div className="details-chart-crosshair-horizontal" style={{ top: `${focus.cursorYPercent}%` }} />
                        {priceTooltipSections.length > 0 ? (
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
                              {priceTooltipSections.map((section) => (
                                <div
                                  className={[
                                    'details-chart-tooltip-grid',
                                    section.variant === 'ohlc' ? 'details-chart-tooltip-grid-ohlc' : '',
                                  ].filter(Boolean).join(' ')}
                                  key={section.key}
                                >
                                  {section.rows.map((row) => (
                                    <div className="details-chart-tooltip-row" key={`${section.key}-${row.label}`}>
                                      <span>{row.label}</span>
                                      <strong>{row.value}</strong>
                                    </div>
                                  ))}
                                </div>
                              ))}
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
        {indicatorPanels.map(renderIndicatorPanel)}
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
  return `${snapshot.tradeDate ?? '--'} · 获利 ${formatRatioPercent(snapshot.totalProfitRatio)} · 主 ${formatNumber(snapshot.mainTotal)} / 散 ${formatNumber(snapshot.retailTotal)}`
}

export default function CyqChenPage() {
  const persistedDraft = useMemo(() => readCyqChenDraft(), [])
  const [tsCodeInput, setTsCodeInput] = useState(persistedDraft.tsCodeInput)
  const [startDateInput, setStartDateInput] = useState(persistedDraft.startDateInput)
  const [endDateInput, setEndDateInput] = useState(persistedDraft.endDateInput)
  const [warmupDaysInput, setWarmupDaysInput] = useState(persistedDraft.warmupDaysInput)
  const [bucketPctInput, setBucketPctInput] = useState(persistedDraft.bucketPctInput)
  const [strategies, setStrategies] = useState<CyqChenStrategyDraft[]>([])
  const [sourcePath, setSourcePath] = useState('')
  const [stockLookupRows, setStockLookupRows] = useState<StockLookupRow[]>([])
  const [stockLookupFocused, setStockLookupFocused] = useState(false)
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

  const chartRows = useMemo(() => buildChartRows(result), [result])
  const chartPanels = result?.klinePayload?.panels ?? []
  const snapshotOptions = result?.snapshots ?? []
  const inputCodeDigits = sanitizeCodeInput(tsCodeInput)
  const normalizedInputTsCode = inputCodeDigits.length === 6 ? stdTsCode(inputCodeDigits) : ''
  const deferredTsCodeInput = useDeferredValue(tsCodeInput)
  const stockNameCandidates = useMemo(
    () => buildStockLookupCandidates(stockLookupRows, deferredTsCodeInput, MAX_STOCK_NAME_CANDIDATES),
    [deferredTsCodeInput, stockLookupRows],
  )
  const exactStockLookupMatch = useMemo(
    () => findExactStockLookupMatch(stockLookupRows, tsCodeInput),
    [stockLookupRows, tsCodeInput],
  )
  const resolvedTestTsCode = normalizedInputTsCode ||
    (exactStockLookupMatch ? stdTsCode(getLookupDigits(exactStockLookupMatch.ts_code)) : '')
  const showStockNameCandidates =
    stockLookupFocused &&
    tsCodeInput.trim() !== '' &&
    stockNameCandidates.length > 0
  const mainPeakBin = useMemo(() => findChipPeak(selectedSnapshot, 'main'), [selectedSnapshot])
  const retailPeakBin = useMemo(() => findChipPeak(selectedSnapshot, 'retail'), [selectedSnapshot])
  const selectedPeakBin = useMemo(
    () => findChipPeak(selectedSnapshot, chipPeakMode),
    [selectedSnapshot, chipPeakMode],
  )
  const resultStockLookupMatch = useMemo(() => {
    if (!result) {
      return null
    }
    return stockLookupRows.find((row) => row.ts_code === result.resolvedTsCode) ?? null
  }, [result, stockLookupRows])
  const watermarkName = result
    ? result.klinePayload?.watermark_name?.trim() || resultStockLookupMatch?.name || '筹码测试'
    : '筹码测试'
  const watermarkCode = result
    ? result.klinePayload?.watermark_code?.trim() || buildWatermarkCode(result.resolvedTsCode)
    : '--'

  useEffect(() => {
    writeJsonStorage(typeof window === 'undefined' ? null : window.localStorage, CYQ_CHEN_DRAFT_STORAGE_KEY, {
      tsCodeInput,
      startDateInput,
      endDateInput,
      warmupDaysInput,
      bucketPctInput,
    })
  }, [bucketPctInput, endDateInput, startDateInput, tsCodeInput, warmupDaysInput])

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

  useEffect(() => {
    let canceled = false

    async function initStrategyFile() {
      setError('')
      try {
        const nextSourcePath = await ensureManagedSourcePath()
        const page = await getCyqChenStrategyPage(nextSourcePath)
        if (canceled) {
          return
        }
        setSourcePath(nextSourcePath)
        setStrategies(page.strategies.map((strategy) => ({ ...strategy })))
        setNotice(page.exists ? '已读取 chip_change_rule.toml。' : '策略文件不存在，已载入默认测试配置。')
      } catch (loadError) {
        if (!canceled) {
          setError(`读取筹码策略失败: ${String(loadError)}`)
          setNotice('')
        }
      }
    }

    void initStrategyFile()
    return () => {
      canceled = true
    }
  }, [])

  useEffect(() => {
    if (!sourcePath.trim()) {
      setStockLookupRows([])
      return
    }

    let canceled = false
    async function loadStockLookupRows() {
      try {
        const rows = await listStockLookupRows(sourcePath.trim())
        if (!canceled) {
          setStockLookupRows(rows)
        }
      } catch {
        if (!canceled) {
          setStockLookupRows([])
        }
      }
    }

    void loadStockLookupRows()
    return () => {
      canceled = true
    }
  }, [sourcePath])

  function onSelectStockCandidate(row: StockLookupRow) {
    const nextCode = getLookupDigits(row.ts_code)
    if (nextCode === '') {
      return
    }

    setStockLookupFocused(false)
    setTsCodeInput(row.name || stdTsCode(nextCode))
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

  async function runTest() {
    if (resolvedTestTsCode === '') {
      setError(
        tsCodeInput.trim() !== ''
          ? '请从候选中选择股票名称，或输入 6 位代码。'
          : '股票代码不能为空。',
      )
      setNotice('')
      return
    }

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
        tsCode: resolvedTestTsCode,
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
          <label className="details-field cyq-chen-stock-lookup">
            <span>代码/名称输入，预览代码：{resolvedTestTsCode || '--'}</span>
            <div className="details-autocomplete">
              <input
                type="text"
                value={tsCodeInput}
                onChange={(event) => {
                  setStockLookupFocused(true)
                  setTsCodeInput(event.target.value)
                }}
                onFocus={() => setStockLookupFocused(true)}
                onBlur={() => setStockLookupFocused(false)}
                onKeyDown={(event) => {
                  if (event.key === 'Enter' && stockNameCandidates.length > 0) {
                    event.preventDefault()
                    onSelectStockCandidate(stockNameCandidates[0])
                  }
                }}
                placeholder="输入股票名称、代码或拼音首字母，支持候选补全"
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
            {loading ? '计算中...' : '计算'}
          </button>
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
            <span>获利比例</span>
            <strong>{formatRatioPercent(selectedSnapshot?.totalProfitRatio)}</strong>
          </div>
          <div>
            <span>套牢比例</span>
            <strong>{formatRatioPercent(selectedSnapshot?.totalTrappedRatio)}</strong>
          </div>
          <div>
            <span>总筹码峰</span>
            <strong>{formatNumber(selectedSnapshot?.chipPeakPrice)}</strong>
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
          <div>
            <span>70%区间</span>
            <strong>
              {selectedSnapshot
                ? `${formatNumber(selectedSnapshot.percent70.priceLow)} - ${formatNumber(selectedSnapshot.percent70.priceHigh)}`
                : '--'}
            </strong>
            <small>集中度 {formatRatioPercent(selectedSnapshot?.percent70.concentration)}</small>
          </div>
          <div>
            <span>90%区间</span>
            <strong>
              {selectedSnapshot
                ? `${formatNumber(selectedSnapshot.percent90.priceLow)} - ${formatNumber(selectedSnapshot.percent90.priceHigh)}`
                : '--'}
            </strong>
            <small>集中度 {formatRatioPercent(selectedSnapshot?.percent90.concentration)}</small>
          </div>
        </div>

        <div className="cyq-chen-project-chart">
          <div className="cyq-chen-chart-title">
            <div>
              <strong>K 线与筹码分布</strong>
              <span>{chartRows.length} 根</span>
            </div>
            <div className="cyq-chen-chip-legend" aria-label="筹码颜色">
              <span><i style={{ background: CHIP_COLOR_MAIN_PROFIT }} />主力盈利</span>
              <span><i style={{ background: CHIP_COLOR_MAIN_TRAPPED }} />主力套牢</span>
              <span><i style={{ background: CHIP_COLOR_RETAIL_PROFIT }} />散户盈利</span>
              <span><i style={{ background: CHIP_COLOR_RETAIL_TRAPPED }} />散户套牢</span>
            </div>
          </div>
          <CyqChenProjectChart
            kline={chartRows}
            panels={chartPanels}
            snapshot={selectedSnapshot}
            selectedTradeDate={selectedSnapshot?.tradeDate ?? selectedTradeDate}
            chipPeakMode={chipPeakMode}
            watermarkName={watermarkName}
            watermarkCode={watermarkCode}
            onChipPeakModeChange={setChipPeakMode}
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
              {(selectedSnapshot?.bins ?? []).map((bin: CyqChenBin) => (
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
