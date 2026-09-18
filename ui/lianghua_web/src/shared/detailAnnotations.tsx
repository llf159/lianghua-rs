import {
  useEffect,
  useMemo,
  useState,
  type CSSProperties,
  type ReactNode,
} from 'react'
import type {
  DetailChartMarkerLineStyle,
  DetailChartMarkerPosition,
  DetailChartMarkerShape,
  DetailKlinePanel,
} from '../apis/details'

export type DetailAnnotationKind = 'symbol' | 'vertical_line'

export type DetailAnnotationStyle = {
  kind: DetailAnnotationKind
  color: string
  opacity?: number
  position?: DetailChartMarkerPosition
  shape?: DetailChartMarkerShape
  lineStyle?: DetailChartMarkerLineStyle
  lineWidth?: number
}

export type DetailAnnotationEvent = {
  id: string
  tradeDate: string
  label?: string | null
  text?: string | null
  yKey?: string | null
  value?: number | null
  style?: Partial<DetailAnnotationStyle>
  detail?: Record<string, string | number | boolean | null>
}

export type DetailAnnotationEventSource =
  | {
      kind: 'events'
      events: readonly DetailAnnotationEvent[]
    }
  | {
      kind: 'indicator'
      whenKey: string
      label?: string | null
      text?: string | null
      yKey?: string | null
    }

export type DetailAnnotationLayer = {
  id: string
  providerId: string
  label: string
  panelKey: string
  enabled?: boolean
  defaultStyle: DetailAnnotationStyle
  eventSource: DetailAnnotationEventSource
}

export type DetailAnnotationIndicatorRow = {
  trade_date?: unknown
  tradeDate?: unknown
  [key: string]: unknown
}

export type DetailAnnotationProviderContext = {
  sourcePath?: string
  tsCode?: string
  startTradeDate?: string
  endTradeDate?: string
  indicator?: {
    panels: readonly DetailKlinePanel[]
    rows: readonly DetailAnnotationIndicatorRow[]
  }
}

type DetailAnnotationProviderBase = {
  id: string
  label: string
}

export type DetailAnnotationProvider =
  | DetailAnnotationProviderBase & {
      mode: 'sync'
      load(context: DetailAnnotationProviderContext): DetailAnnotationLayer[]
    }
  | DetailAnnotationProviderBase & {
      mode: 'async'
      load(
        context: DetailAnnotationProviderContext,
        signal: AbortSignal,
      ): Promise<DetailAnnotationLayer[]>
    }

type DetailAnnotationProviderResult = {
  layers: DetailAnnotationLayer[]
  errors: DetailAnnotationProviderError[]
}

type DetailAnnotationAsyncProviderState = DetailAnnotationProviderResult & {
  context: DetailAnnotationProviderContext | null
  loading: boolean
}

export type DetailAnnotationProviderError = {
  providerId: string
  message: string
}

function loadSyncAnnotationProviders(
  providers: readonly DetailAnnotationProvider[],
  context: DetailAnnotationProviderContext,
): DetailAnnotationProviderResult {
  const layers: DetailAnnotationLayer[] = []
  const errors: DetailAnnotationProviderError[] = []

  providers.forEach((provider) => {
    if (provider.mode !== 'sync') {
      return
    }
    try {
      layers.push(...provider.load(context))
    } catch (error) {
      errors.push({
        providerId: provider.id,
        message: String(error),
      })
    }
  })

  return { layers, errors }
}

function getAsyncAnnotationProviders(
  providers: readonly DetailAnnotationProvider[],
) {
  return providers.filter(
    (provider): provider is Extract<DetailAnnotationProvider, { mode: 'async' }> =>
      provider.mode === 'async',
  )
}

export const indicatorProvider: DetailAnnotationProvider = {
  id: 'indicator',
  label: '可编程指标',
  mode: 'sync',
  load(context) {
    const indicator = context.indicator
    if (!indicator) {
      return []
    }

    return indicator.panels.flatMap((panel) =>
      (panel.markers ?? []).map((marker) => {
        const kind = marker.kind ?? 'symbol'

        return {
          id: `${panel.key}:${marker.key}`,
          providerId: 'indicator',
          label: marker.label?.trim() || marker.key,
          panelKey: panel.key,
          enabled: true,
          defaultStyle: {
            kind,
            color: marker.color ?? '#d9485f',
            opacity: marker.opacity ?? 1,
            position: marker.position ?? 'value',
            shape: marker.shape ?? 'dot',
            lineStyle: marker.line_style ?? 'dashed',
            lineWidth: marker.line_width ?? 1.5,
          },
          eventSource: {
            kind: 'indicator',
            whenKey: marker.when_key,
            label: marker.label,
            text: marker.text,
            yKey: marker.y_key,
          },
        } satisfies DetailAnnotationLayer
      }),
    )
  },
}

export const DETAIL_ANNOTATION_PROVIDERS: readonly DetailAnnotationProvider[] = [
  indicatorProvider,
]

export function useDetailAnnotationProviders(
  providers: readonly DetailAnnotationProvider[],
  context: DetailAnnotationProviderContext,
) {
  const syncResult = useMemo(
    () => loadSyncAnnotationProviders(providers, context),
    [context, providers],
  )
  const asyncProviders = useMemo(
    () => getAsyncAnnotationProviders(providers),
    [providers],
  )
  const [asyncState, setAsyncState] = useState<DetailAnnotationAsyncProviderState>({
    context: null,
    layers: [],
    errors: [],
    loading: asyncProviders.length > 0,
  })

  useEffect(() => {
    if (asyncProviders.length === 0) {
      return
    }

    const controller = new AbortController()
    let cancelled = false

    void Promise.allSettled(
      asyncProviders.map((provider) =>
        provider.load(context, controller.signal).then((layers) => ({
          provider,
          layers,
        })),
      ),
    ).then((results) => {
      if (cancelled || controller.signal.aborted) {
        return
      }

      const layers: DetailAnnotationLayer[] = []
      const errors: DetailAnnotationProviderError[] = []
      results.forEach((result, index) => {
        const provider = asyncProviders[index]
        if (result.status === 'fulfilled') {
          layers.push(...result.value.layers)
          return
        }
        errors.push({
          providerId: provider?.id ?? `provider-${index}`,
          message: String(result.reason),
        })
      })
      setAsyncState({ context, layers, errors, loading: false })
    })

    return () => {
      cancelled = true
      controller.abort()
    }
  }, [asyncProviders, context])

  const hasCurrentAsyncResult = asyncState.context === context
  return {
    layers: hasCurrentAsyncResult
      ? [...syncResult.layers, ...asyncState.layers]
      : syncResult.layers,
    errors: hasCurrentAsyncResult
      ? [...syncResult.errors, ...asyncState.errors]
      : syncResult.errors,
    loading: asyncProviders.length > 0 && (
      !hasCurrentAsyncResult || asyncState.loading
    ),
  }
}

export type DetailAnnotationSvgLine = {
  key: string
  x: number
  color: string
  opacity: number
  lineStyle: DetailChartMarkerLineStyle
  lineWidth: number
}

export type DetailAnnotationOverlayPoint = {
  key: string
  kind: 'symbol' | 'line_badge'
  leftPercent: number
  topPercent: number
  color: string
  opacity: number
  shape?: DetailChartMarkerShape
  text?: string | null
}

export type DetailAnnotationRenderData = {
  lines: DetailAnnotationSvgLine[]
  overlayPoints: DetailAnnotationOverlayPoint[]
}

function normalizeYKey(key?: string | null) {
  const trimmed = key?.trim()
  if (!trimmed) {
    return 'close'
  }
  switch (trimmed.toLowerCase()) {
    case 'o':
      return 'open'
    case 'h':
      return 'high'
    case 'l':
      return 'low'
    case 'c':
      return 'close'
    case 'v':
      return 'vol'
    default:
      return trimmed
  }
}

function mergeAnnotationStyle(
  layerStyle: DetailAnnotationStyle,
  eventStyle?: Partial<DetailAnnotationStyle>,
): DetailAnnotationStyle {
  if (!eventStyle) {
    return layerStyle
  }
  return {
    ...layerStyle,
    ...eventStyle,
    kind: eventStyle?.kind ?? layerStyle.kind,
    color: eventStyle?.color ?? layerStyle.color,
  }
}

const annotationEventIndexCache = new WeakMap<
  DetailAnnotationLayer,
  ReadonlyMap<string, readonly DetailAnnotationEvent[]>
>()

function getAnnotationEventIndex(layer: DetailAnnotationLayer) {
  const cached = annotationEventIndexCache.get(layer)
  if (cached) {
    return cached
  }

  const index = new Map<string, DetailAnnotationEvent[]>()
  if (layer.eventSource.kind === 'events') {
    layer.eventSource.events.forEach((event) => {
      const tradeDate = event.tradeDate.trim()
      if (tradeDate === '') {
        return
      }
      const dateEvents = index.get(tradeDate)
      if (dateEvents) {
        dateEvents.push(event)
      } else {
        index.set(tradeDate, [event])
      }
    })
  }
  annotationEventIndexCache.set(layer, index)
  return index
}

export function buildDetailAnnotationRenderData<Row>({
  layers,
  panelKey,
  items,
  xAt,
  yAt,
  getTradeDate,
  getNumericValue,
  isConditionTrue,
  viewBoxWidth,
  viewBoxHeight,
  marginTop,
  marginBottom,
}: {
  layers: readonly DetailAnnotationLayer[]
  panelKey: string
  items: readonly Row[]
  xAt: (itemIndex: number) => number
  yAt: (value: number) => number
  getTradeDate: (row: Row) => string
  getNumericValue: (row: Row, key: string) => number | null
  isConditionTrue: (row: Row, key: string) => boolean
  viewBoxWidth: number
  viewBoxHeight: number
  marginTop: number
  marginBottom: number
}): DetailAnnotationRenderData {
  const panelLayers = layers.filter(
    (layer) => layer.enabled !== false && layer.panelKey === panelKey,
  )
  if (panelLayers.length === 0) {
    return { lines: [], overlayPoints: [] }
  }

  const tradeDates = items.map((row) => getTradeDate(row).trim())
  const lines: DetailAnnotationSvgLine[] = []
  const overlayPoints: DetailAnnotationOverlayPoint[] = []
  const laneByKey = new Map<string, number>()

  function appendEvent(
    layer: DetailAnnotationLayer,
    event: DetailAnnotationEvent,
    itemIndex: number,
  ) {
    const style = mergeAnnotationStyle(layer.defaultStyle, event.style)
    const x = xAt(itemIndex)
    const opacity = Math.min(Math.max(style.opacity ?? 1, 0), 1)
    if (style.kind === 'vertical_line') {
      lines.push({
        key: `${layer.providerId}:${layer.id}:${event.id}:line`,
        x,
        color: style.color,
        opacity,
        lineStyle: style.lineStyle ?? 'dashed',
        lineWidth: style.lineWidth ?? 1.5,
      })
      if (event.text?.trim()) {
        const laneKey = `${event.tradeDate}:line_badge`
        const lane = laneByKey.get(laneKey) ?? 0
        laneByKey.set(laneKey, lane + 1)
        overlayPoints.push({
          key: `${layer.providerId}:${layer.id}:${event.id}:badge`,
          kind: 'line_badge',
          leftPercent: (x / viewBoxWidth) * 100,
          topPercent: ((marginTop + 8 + lane * 16) / viewBoxHeight) * 100,
          color: style.color,
          opacity,
          text: event.text,
        })
      }
      return
    }

    const position = style.position ?? 'value'
    const value =
      typeof event.value === 'number' && Number.isFinite(event.value)
        ? event.value
        : getNumericValue(items[itemIndex], normalizeYKey(event.yKey))
    if (value === null) {
      return
    }

    const laneKey = `${event.tradeDate}:${position}`
    const lane = laneByKey.get(laneKey) ?? 0
    laneByKey.set(laneKey, lane + 1)
    const baseY = yAt(value)
    const y =
      position === 'above'
        ? marginTop + 10 + lane * 13
        : position === 'below'
          ? viewBoxHeight - marginBottom - 10 - lane * 13
          : baseY

    overlayPoints.push({
      key: `${layer.providerId}:${layer.id}:${event.id}:symbol`,
      kind: 'symbol',
      leftPercent: (x / viewBoxWidth) * 100,
      topPercent: (y / viewBoxHeight) * 100,
      color: style.color,
      opacity,
      shape: style.shape ?? 'dot',
      text: event.text,
    })
  }

  panelLayers.forEach((layer) => {
    if (layer.eventSource.kind === 'indicator') {
      const source = layer.eventSource
      items.forEach((row, itemIndex) => {
        const tradeDate = tradeDates[itemIndex]
        if (tradeDate === '' || !isConditionTrue(row, source.whenKey)) {
          return
        }
        appendEvent(layer, {
          id: `${layer.id}:${tradeDate}`,
          tradeDate,
          label: source.label,
          text: source.text,
          yKey: source.yKey,
        }, itemIndex)
      })
      return
    }

    const eventIndex = getAnnotationEventIndex(layer)
    tradeDates.forEach((tradeDate, itemIndex) => {
      if (tradeDate === '') {
        return
      }
      eventIndex.get(tradeDate)?.forEach((event) => {
        appendEvent(layer, event, itemIndex)
      })
    })
  })

  return { lines, overlayPoints }
}

function lineDashArray(style: DetailChartMarkerLineStyle) {
  switch (style) {
    case 'solid':
      return undefined
    case 'dotted':
      return '2 4'
    default:
      return '7 5'
  }
}

export function renderDetailAnnotationSvgLines(
  lines: readonly DetailAnnotationSvgLine[],
  y1: number,
  y2: number,
): ReactNode {
  return lines.map((line) => (
    <line
      className="details-chart-annotation-line"
      key={line.key}
      x1={line.x}
      y1={y1}
      x2={line.x}
      y2={y2}
      stroke={line.color}
      strokeDasharray={lineDashArray(line.lineStyle)}
      strokeOpacity={line.opacity}
      strokeWidth={line.lineWidth}
    />
  ))
}

export function renderDetailAnnotationOverlayPoint(
  point: DetailAnnotationOverlayPoint,
) {
  if (point.kind === 'line_badge') {
    return (
      <span
        className="details-chart-annotation-line-badge"
        key={point.key}
        style={{
          left: `${point.leftPercent}%`,
          top: `${point.topPercent}%`,
          color: point.color,
          borderColor: point.color,
          opacity: point.opacity,
        }}
      >
        {point.text}
      </span>
    )
  }

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
        opacity: point.opacity,
        '--details-chart-marker-color': point.color,
      } as CSSProperties}
    >
      {point.text ? <span>{point.text}</span> : null}
    </span>
  )
}
