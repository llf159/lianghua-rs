import { normalizeTsCode } from './stockCode'
import { normalizeDateValue } from './tradeDate'

const WATCH_CACHE_WRITE_KEY = 'lh_watch_observe_list'
const WATCH_CACHE_PRIMARY_KEYS = [
  'lh_watch_observe_list',
  'lh_watch_observe_rows',
  'lh_watch_observe',
  'lh_watch_list',
  'lh_watchlist',
  'watch_observe_list',
  'watch_list',
  'watchlist',
] as const
const WATCH_CACHE_DISCOVERY_TOKENS = ['watch', 'observe', 'favorite', '自选'] as const
const WATCH_CACHE_NESTED_KEYS = ['items', 'list', 'rows', 'data', 'values', 'watchlist'] as const

export type WatchObserveRow = {
  tsCode: string
  name: string
  latestClose: number | null
  latestChangePct: number | null
  addedDate: string
  postWatchReturnPct: number | null
  todayRank: number | null
  tag: string
  concept: string
  tradeDate: string | null
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
}

function extractTextValue(record: Record<string, unknown>, keys: readonly string[]) {
  for (const key of keys) {
    const value = record[key]
    if (typeof value === 'string' || typeof value === 'number') {
      const text = String(value).trim()
      if (text !== '') {
        return text
      }
    }
  }

  return ''
}

function extractNumberValue(record: Record<string, unknown>, keys: readonly string[]) {
  for (const key of keys) {
    const value = record[key]
    if (typeof value === 'number' && Number.isFinite(value)) {
      return value
    }
    if (typeof value === 'string') {
      const normalized = value.replace(/,/g, '').trim()
      if (normalized === '') {
        continue
      }
      const parsed = Number(normalized)
      if (Number.isFinite(parsed)) {
        return parsed
      }
    }
  }

  return null
}

function mergeWatchObserveRow(primary: WatchObserveRow, secondary: WatchObserveRow): WatchObserveRow {
  return {
    tsCode: primary.tsCode,
    name: primary.name || secondary.name,
    latestClose: primary.latestClose ?? secondary.latestClose,
    latestChangePct: primary.latestChangePct ?? secondary.latestChangePct,
    addedDate: primary.addedDate || secondary.addedDate,
    postWatchReturnPct: primary.postWatchReturnPct ?? secondary.postWatchReturnPct,
    todayRank: primary.todayRank ?? secondary.todayRank,
    tag: primary.tag || secondary.tag,
    concept: primary.concept || secondary.concept,
    tradeDate: primary.tradeDate ?? secondary.tradeDate,
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
    addedDate: normalizeDateValue(input.addedDate ?? ''),
    postWatchReturnPct:
      typeof input.postWatchReturnPct === 'number' && Number.isFinite(input.postWatchReturnPct)
        ? input.postWatchReturnPct
        : null,
    todayRank:
      typeof input.todayRank === 'number' && Number.isFinite(input.todayRank) ? input.todayRank : null,
    tag: input.tag?.trim() ?? '',
    concept: input.concept?.trim() ?? '',
    tradeDate: (() => {
      const value = normalizeDateValue(input.tradeDate ?? '')
      return value === '' ? null : value
    })(),
  }
}

function buildWatchRowFromRecord(record: Record<string, unknown>) {
  const tsCode = normalizeTsCode(
    extractTextValue(record, ['ts_code', 'tsCode', 'code', 'stock_code', 'stockCode']),
  )
  if (!tsCode) {
    return null
  }

  const tradeDateRaw = extractTextValue(record, [
    'trade_date',
    'tradeDate',
    'rank_date',
    'rankDate',
    'latest_trade_date',
    'latestTradeDate',
  ])

  return {
    tsCode,
    name: extractTextValue(record, ['name', 'stock_name', 'stockName']),
    latestClose: extractNumberValue(record, [
      'latest_close',
      'latestClose',
      'latest_close_price',
      'latestClosePrice',
      'close',
    ]),
    latestChangePct: extractNumberValue(record, [
      'latest_change_pct',
      'latestChangePct',
      'pct_chg',
      'pctChg',
      'change_pct',
      'changePct',
      'chgPct',
    ]),
    addedDate: normalizeDateValue(
      extractTextValue(record, [
        'watch_date',
        'watchDate',
        'observe_date',
        'observeDate',
        'added_date',
        'addedDate',
        'join_date',
        'joinDate',
        'created_at',
        'createdAt',
        'trade_date',
        'tradeDate',
        'date',
      ]),
    ),
    postWatchReturnPct: extractNumberValue(record, [
      'post_watch_return_pct',
      'postWatchReturnPct',
      'observe_return_pct',
      'observeReturnPct',
      'watch_return_pct',
      'watchReturnPct',
      'return_pct',
      'returnPct',
      'post_rank_return_pct',
    ]),
    todayRank: extractNumberValue(record, ['today_rank', 'todayRank', 'rank']),
    tag: extractTextValue(record, ['tag', 'label', 'memo', 'note']),
    concept: extractTextValue(record, ['concept', 'concepts']),
    tradeDate: tradeDateRaw ? normalizeDateValue(tradeDateRaw) : null,
  } satisfies WatchObserveRow
}

function buildWatchRowFromUnknown(value: unknown): WatchObserveRow | null {
  if (typeof value === 'string' || typeof value === 'number') {
    const tsCode = normalizeTsCode(value)
    if (!tsCode) {
      return null
    }
    return {
      tsCode,
      name: '',
      latestClose: null,
      latestChangePct: null,
      addedDate: '',
      postWatchReturnPct: null,
      todayRank: null,
      tag: '',
      concept: '',
      tradeDate: null,
    }
  }

  if (Array.isArray(value)) {
    const tsCode = normalizeTsCode(value[0])
    if (!tsCode) {
      return null
    }
    return {
      tsCode,
      name: typeof value[1] === 'string' ? value[1].trim() : '',
      latestClose: typeof value[2] === 'number' && Number.isFinite(value[2]) ? value[2] : null,
      latestChangePct: typeof value[3] === 'number' && Number.isFinite(value[3]) ? value[3] : null,
      addedDate: normalizeDateValue(value[4]),
      postWatchReturnPct: typeof value[5] === 'number' && Number.isFinite(value[5]) ? value[5] : null,
      todayRank: typeof value[6] === 'number' && Number.isFinite(value[6]) ? value[6] : null,
      tag: typeof value[7] === 'string' ? value[7].trim() : '',
      concept: typeof value[8] === 'string' ? value[8].trim() : '',
      tradeDate: value[9] ? normalizeDateValue(value[9]) : null,
    }
  }

  if (isRecord(value)) {
    return buildWatchRowFromRecord(value)
  }

  return null
}

function extractWatchRows(payload: unknown): WatchObserveRow[] {
  if (Array.isArray(payload)) {
    return payload.flatMap((item) => {
      const direct = buildWatchRowFromUnknown(item)
      if (direct) {
        return [direct]
      }
      return extractWatchRows(item)
    })
  }

  if (!isRecord(payload)) {
    return []
  }

  for (const key of WATCH_CACHE_NESTED_KEYS) {
    const nested = payload[key]
    const rows = extractWatchRows(nested)
    if (rows.length > 0) {
      return rows
    }
  }

  const direct = buildWatchRowFromRecord(payload)
  if (direct) {
    return [direct]
  }

  return Object.entries(payload).flatMap(([tsCodeKey, item]) => {
    if (!isRecord(item)) {
      return []
    }
    const row = buildWatchRowFromRecord({ ...item, ts_code: item.ts_code ?? item.tsCode ?? tsCodeKey })
    return row ? [row] : []
  })
}

function parseWatchRows(raw: string) {
  const trimmed = raw.trim()
  if (trimmed === '') {
    return []
  }

  try {
    return extractWatchRows(JSON.parse(trimmed))
  } catch {
    return trimmed
      .split(/[\n,]+/)
      .map((item) => buildWatchRowFromUnknown(item.trim()))
      .filter((item): item is WatchObserveRow => item !== null)
  }
}

function collectCandidateKeys(storage: Storage) {
  const keys: string[] = []
  const existing = new Set<string>()

  for (let index = 0; index < storage.length; index += 1) {
    const key = storage.key(index)
    if (typeof key === 'string' && key.trim() !== '') {
      existing.add(key)
    }
  }

  WATCH_CACHE_PRIMARY_KEYS.forEach((key) => {
    if (existing.has(key)) {
      keys.push(key)
    }
  })

  existing.forEach((key) => {
    const lowerKey = key.toLowerCase()
    if (WATCH_CACHE_DISCOVERY_TOKENS.some((token) => lowerKey.includes(token)) && !keys.includes(key)) {
      keys.push(key)
    }
  })

  return keys
}

function clearWatchObserveCacheEntries(storage: Storage) {
  const keys = collectCandidateKeys(storage)
  keys.forEach((key) => {
    storage.removeItem(key)
  })
}

export function readWatchObserveRowsFromCache() {
  if (typeof window === 'undefined') {
    return []
  }

  const merged = new Map<string, WatchObserveRow>()
  const storages = [window.localStorage, window.sessionStorage]

  for (const storage of storages) {
    const keys = collectCandidateKeys(storage)
    for (const key of keys) {
      const raw = storage.getItem(key)
      if (!raw) {
        continue
      }

      const rows = parseWatchRows(raw)
      rows.forEach((row) => {
        const existing = merged.get(row.tsCode)
        if (!existing) {
          merged.set(row.tsCode, row)
          return
        }
        merged.set(row.tsCode, mergeWatchObserveRow(existing, row))
      })
    }
  }

  return [...merged.values()]
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
    watch_date: row.addedDate || undefined,
    post_watch_return_pct: row.postWatchReturnPct,
    today_rank: row.todayRank,
    tag: row.tag || undefined,
    concept: row.concept || undefined,
    trade_date: row.tradeDate || undefined,
  }))

  clearWatchObserveCacheEntries(window.localStorage)
  clearWatchObserveCacheEntries(window.sessionStorage)
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
