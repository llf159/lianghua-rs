import { normalizeTsCode } from './stockCode'
import { normalizeDateValue } from './tradeDate'

const WATCH_CACHE_WRITE_KEY = 'lh_watch_observe_list'

export type WatchObserveRow = {
  tsCode: string
  name: string
  latestClose: number | null
  latestChangePct: number | null
  volumeRatio: number | null
  watchDate: string
  postWatchReturnPct: number | null
  todayRank: number | null
  tag: string
  concept: string
  markedDate: string | null
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
}

function mergeWatchObserveRow(primary: WatchObserveRow, secondary: WatchObserveRow): WatchObserveRow {
  return {
    tsCode: primary.tsCode,
    name: primary.name || secondary.name,
    latestClose: primary.latestClose ?? secondary.latestClose,
    latestChangePct: primary.latestChangePct ?? secondary.latestChangePct,
    volumeRatio: primary.volumeRatio ?? secondary.volumeRatio,
    watchDate: primary.watchDate || secondary.watchDate,
    postWatchReturnPct: primary.postWatchReturnPct ?? secondary.postWatchReturnPct,
    todayRank: primary.todayRank ?? secondary.todayRank,
    tag: primary.tag || secondary.tag,
    concept: primary.concept || secondary.concept,
    markedDate: primary.markedDate ?? secondary.markedDate,
  }
}

function normalizeRowInput(
  input: Partial<WatchObserveRow> & {
    tsCode: string
  },
): WatchObserveRow | null {
  const tsCode = normalizeTsCode(input.tsCode)
  if (!tsCode) {
    return null
  }

  return {
    tsCode,
    name: input.name?.trim() ?? '',
    latestClose:
      typeof input.latestClose === 'number' && Number.isFinite(input.latestClose)
        ? input.latestClose
        : null,
    latestChangePct:
      typeof input.latestChangePct === 'number' && Number.isFinite(input.latestChangePct)
        ? input.latestChangePct
        : null,
    volumeRatio:
      typeof input.volumeRatio === 'number' && Number.isFinite(input.volumeRatio)
        ? input.volumeRatio
        : null,
    watchDate: normalizeDateValue(input.watchDate ?? ''),
    postWatchReturnPct:
      typeof input.postWatchReturnPct === 'number' && Number.isFinite(input.postWatchReturnPct)
        ? input.postWatchReturnPct
        : null,
    todayRank:
      typeof input.todayRank === 'number' && Number.isFinite(input.todayRank) ? input.todayRank : null,
    tag: input.tag?.trim() ?? '',
    concept: input.concept?.trim() ?? '',
    markedDate: (() => {
      const value = normalizeDateValue(input.markedDate ?? '')
      return value === '' ? null : value
    })(),
  }
}

function normalizeNumberValue(value: unknown) {
  return typeof value === 'number' && Number.isFinite(value) ? value : null
}

function normalizeTextValue(value: unknown) {
  return typeof value === 'string' ? value.trim() : ''
}

function buildWatchRowFromCacheRecord(record: Record<string, unknown>) {
  const tsCode = normalizeTsCode(record.ts_code)
  if (!tsCode) {
    return null
  }

  const markedDate = normalizeDateValue(
    normalizeTextValue(record.marked_date ?? record.trade_date),
  )

  return {
    tsCode,
    name: normalizeTextValue(record.name),
    latestClose: normalizeNumberValue(record.latest_close),
    latestChangePct: normalizeNumberValue(record.latest_change_pct),
    volumeRatio: normalizeNumberValue(record.volume_ratio),
    watchDate: normalizeDateValue(
      normalizeTextValue(record.watch_date ?? record.added_date),
    ),
    postWatchReturnPct: normalizeNumberValue(record.post_watch_return_pct),
    todayRank: normalizeNumberValue(record.today_rank),
    tag: normalizeTextValue(record.tag),
    concept: normalizeTextValue(record.concept),
    markedDate: markedDate === '' ? null : markedDate,
  } satisfies WatchObserveRow
}

function parseWatchRows(raw: string) {
  const trimmed = raw.trim()
  if (trimmed === '') {
    return []
  }

  try {
    const parsed = JSON.parse(trimmed)
    if (!Array.isArray(parsed)) {
      return []
    }
    return parsed
      .map((item) => (isRecord(item) ? buildWatchRowFromCacheRecord(item) : null))
      .filter((item): item is WatchObserveRow => item !== null)
  } catch {
    return []
  }
}

export function readWatchObserveRowsFromCache() {
  if (typeof window === 'undefined') {
    return []
  }

  const raw = window.localStorage.getItem(WATCH_CACHE_WRITE_KEY)
  if (!raw) {
    return []
  }

  return parseWatchRows(raw)
}

export function writeWatchObserveRowsToCache(rows: WatchObserveRow[]) {
  if (typeof window === 'undefined') {
    return
  }

  const payload = rows.map((row) => ({
    ts_code: row.tsCode,
    name: row.name || undefined,
    latest_close: row.latestClose,
    latest_change_pct: row.latestChangePct,
    volume_ratio: row.volumeRatio,
    watch_date: row.watchDate || undefined,
    post_watch_return_pct: row.postWatchReturnPct,
    today_rank: row.todayRank,
    tag: row.tag || undefined,
    concept: row.concept || undefined,
    marked_date: row.markedDate || undefined,
  }))

  window.localStorage.setItem(WATCH_CACHE_WRITE_KEY, JSON.stringify(payload))
}

export function findWatchObserveRow(rows: WatchObserveRow[], tsCode: string) {
  const normalizedTsCode = normalizeTsCode(tsCode)
  if (!normalizedTsCode) {
    return null
  }

  return rows.find((row) => row.tsCode === normalizedTsCode) ?? null
}

export function upsertWatchObserveRow(
  input: Partial<WatchObserveRow> & {
    tsCode: string
  },
) {
  const normalizedInput = normalizeRowInput(input)
  if (!normalizedInput) {
    return readWatchObserveRowsFromCache()
  }

  const rows = readWatchObserveRowsFromCache()
  const existing = findWatchObserveRow(rows, normalizedInput.tsCode)
  const nextRow = existing ? mergeWatchObserveRow(normalizedInput, existing) : normalizedInput
  const nextRows = rows.filter((row) => row.tsCode !== normalizedInput.tsCode)

  nextRows.unshift(nextRow)
  writeWatchObserveRowsToCache(nextRows)
  return nextRows
}

export function removeWatchObserveRow(tsCode: string) {
  const normalizedTsCode = normalizeTsCode(tsCode)
  if (!normalizedTsCode) {
    return readWatchObserveRowsFromCache()
  }

  const nextRows = readWatchObserveRowsFromCache().filter((row) => row.tsCode !== normalizedTsCode)
  writeWatchObserveRowsToCache(nextRows)
  return nextRows
}
