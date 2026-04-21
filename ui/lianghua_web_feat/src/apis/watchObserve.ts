import { invoke } from '@tauri-apps/api/core'
import { readStoredSourcePath } from '../shared/storage'
import { normalizeTsCode } from '../shared/stockCode'
import {
  readWatchObserveRowsFromCache,
  upsertWatchObserveRow as upsertCachedWatchObserveRow,
  writeWatchObserveRowsToCache,
} from '../shared/watchObserve'

const WATCH_OBSERVE_MIGRATION_KEY = 'lh_watch_observe_browser_cache_migrated_v2'

export type WatchObserveRow = {
  tsCode: string
  name: string
  latestClose: number | null
  latestChangePct: number | null
  volumeRatio: number | null
  addedDate: string
  postWatchReturnPct: number | null
  todayRank: number | null
  tag: string
  concept: string
  tradeDate: string | null
}

export type WatchObserveSnapshotData = {
  mode: 'realtime'
  rows: WatchObserveRow[]
  refreshedAt: string | null
  referenceTradeDate: string | null
  requestedCount: number
  effectiveCount: number
  fetchedCount: number
  truncated: boolean
}

export type WatchObserveInput = {
  tsCode: string
  name?: string
  addedDate?: string
  tag?: string
  concept?: string
  tradeDate?: string | null
}

let watchObserveMigrationPromise: Promise<void> | null = null
let watchObservePreloadPromise: Promise<WatchObserveRow[]> | null = null
let watchObservePreloadKey = ''

function resolveSourcePath(sourcePath?: string | null) {
  const trimmed = sourcePath?.trim() ?? ''
  if (trimmed !== '') {
    return trimmed
  }

  const stored = readStoredSourcePath().trim()
  return stored !== '' ? stored : null
}

function buildStoredRowPayload(input: WatchObserveInput) {
  return {
    tsCode: input.tsCode,
    name: input.name?.trim() || undefined,
    addedDate: input.addedDate?.trim() || undefined,
    tag: input.tag?.trim() || undefined,
    concept: input.concept?.trim() || undefined,
    tradeDate: input.tradeDate?.trim() || undefined,
  }
}

function buildStoredRowPayloadFromRow(row: WatchObserveRow) {
  return buildStoredRowPayload({
    tsCode: row.tsCode,
    name: row.name,
    addedDate: row.addedDate,
    tag: row.tag,
    concept: row.concept,
    tradeDate: row.tradeDate,
  })
}

async function listHydratedWatchObserveRows(
  sourcePath?: string | null,
  referenceTradeDate?: string | null,
) {
  const rows = readWatchObserveRowsFromCache()
  return invoke<WatchObserveRow[]>('list_watch_observe_rows', {
    sourcePath: resolveSourcePath(sourcePath),
    referenceTradeDate: referenceTradeDate?.trim() || undefined,
    rows: rows.map(buildStoredRowPayloadFromRow),
  })
}

async function refreshHydratedWatchObserveRows(
  sourcePath?: string | null,
  referenceTradeDate?: string | null,
) {
  const rows = readWatchObserveRowsFromCache()
  return invoke<WatchObserveSnapshotData>('refresh_watch_observe_rows', {
    sourcePath: resolveSourcePath(sourcePath),
    referenceTradeDate: referenceTradeDate?.trim() || undefined,
    rows: rows.map(buildStoredRowPayloadFromRow),
  })
}

function updateCachedWatchObserveTag(tsCode: string, tag: string) {
  const normalizedTsCode = normalizeTsCode(tsCode)
  if (!normalizedTsCode) {
    throw new Error('自选代码无效')
  }

  const rows = readWatchObserveRowsFromCache()
  const existing = rows.find((row) => row.tsCode === normalizedTsCode)
  if (!existing) {
    throw new Error(`未找到自选记录: ${normalizedTsCode}`)
  }

  const nextRows = rows.map((row) =>
    row.tsCode === normalizedTsCode ? { ...row, tag: tag.trim() } : row,
  )
  writeWatchObserveRowsToCache(nextRows)
  return nextRows
}

function removeCachedWatchObserveRows(tsCodes: string[]) {
  const normalizedCodes = tsCodes
    .map((value) => normalizeTsCode(value))
    .filter((value): value is string => Boolean(value))

  if (normalizedCodes.length === 0) {
    return readWatchObserveRowsFromCache()
  }

  const normalizedCodeSet = new Set(normalizedCodes)
  const nextRows = readWatchObserveRowsFromCache().filter(
    (row) => !normalizedCodeSet.has(row.tsCode),
  )
  writeWatchObserveRowsToCache(nextRows)
  return nextRows
}

async function ensureWatchObserveCacheMigration(sourcePath?: string | null) {
  if (typeof window === 'undefined') {
    return
  }

  if (window.localStorage.getItem(WATCH_OBSERVE_MIGRATION_KEY) === '1') {
    return
  }

  if (!watchObserveMigrationPromise) {
    watchObserveMigrationPromise = (async () => {
      const cachedRows = readWatchObserveRowsFromCache()
      if (cachedRows.length > 0) {
        writeWatchObserveRowsToCache(cachedRows)
      } else {
        const backendRows = await invoke<WatchObserveRow[]>('list_watch_observe_rows', {
          sourcePath: resolveSourcePath(sourcePath),
        })
        writeWatchObserveRowsToCache(backendRows)
      }

      window.localStorage.setItem(WATCH_OBSERVE_MIGRATION_KEY, '1')
    })().finally(() => {
      watchObserveMigrationPromise = null
    })
  }

  await watchObserveMigrationPromise
}

export function findWatchObserveRow(rows: WatchObserveRow[], tsCode: string) {
  const normalizedTsCode = normalizeTsCode(tsCode)
  if (!normalizedTsCode) {
    return null
  }

  return rows.find((row) => row.tsCode === normalizedTsCode) ?? null
}

export async function listWatchObserveRows(
  sourcePath?: string | null,
  referenceTradeDate?: string | null,
) {
  await ensureWatchObserveCacheMigration(sourcePath)
  return listHydratedWatchObserveRows(sourcePath, referenceTradeDate)
}

export async function refreshWatchObserveRows(
  referenceTradeDate?: string | null,
  sourcePath?: string | null,
) {
  await ensureWatchObserveCacheMigration(sourcePath)
  return refreshHydratedWatchObserveRows(sourcePath, referenceTradeDate)
}

export async function upsertWatchObserveRow(input: WatchObserveInput, sourcePath?: string | null) {
  await ensureWatchObserveCacheMigration(sourcePath)
  upsertCachedWatchObserveRow({
    tsCode: input.tsCode,
    name: input.name,
    addedDate: input.addedDate,
    tag: input.tag,
    concept: input.concept,
    tradeDate: input.tradeDate,
  })
  return listHydratedWatchObserveRows(sourcePath)
}

export async function updateWatchObserveTag(tsCode: string, tag: string, sourcePath?: string | null) {
  await ensureWatchObserveCacheMigration(sourcePath)
  updateCachedWatchObserveTag(tsCode, tag)
  return listHydratedWatchObserveRows(sourcePath)
}

export async function removeWatchObserveRows(tsCodes: string[], sourcePath?: string | null) {
  await ensureWatchObserveCacheMigration(sourcePath)
  removeCachedWatchObserveRows(tsCodes)
  return listHydratedWatchObserveRows(sourcePath)
}

export function preloadWatchObserveRows(
  sourcePath?: string | null,
  referenceTradeDate?: string | null,
) {
  const preloadKey = `${resolveSourcePath(sourcePath) ?? ''}::${referenceTradeDate?.trim() ?? ''}`
  if (watchObservePreloadPromise && watchObservePreloadKey === preloadKey) {
    return watchObservePreloadPromise
  }

  const preloadPromise = (async () => {
    await ensureWatchObserveCacheMigration(sourcePath)
    return listHydratedWatchObserveRows(sourcePath, referenceTradeDate)
  })()

  watchObservePreloadPromise = preloadPromise
  watchObservePreloadKey = preloadKey

  void preloadPromise.finally(() => {
    if (watchObservePreloadPromise === preloadPromise) {
      watchObservePreloadPromise = null
      watchObservePreloadKey = ''
    }
  })

  return preloadPromise
}
