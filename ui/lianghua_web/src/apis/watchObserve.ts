import { invoke } from '@tauri-apps/api/core'
import { readStoredSourcePath } from '../shared/storage'
import { normalizeTsCode } from '../shared/stockCode'
import { normalizeDateValue } from '../shared/tradeDate'
import {
  readWatchObserveRowsFromCache,
  upsertWatchObserveRow as upsertCachedWatchObserveRow,
  writeWatchObserveRowsToCache,
} from '../shared/watchObserve'

export type WatchObserveRow = {
  tsCode: string
  name: string
  latestClose: number | null
  latestChangePct: number | null
  volumeRatio: number | null
  return3dPct: number | null
  watchDate: string
  postWatchReturnPct: number | null
  todayRank: number | null
  sceneMarker: string | null
  tag: string
  concept: string
  markedDate: string | null
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
  tag?: string
  concept?: string
  markedDate?: string | null
}

let watchObservePreloadPromise: Promise<WatchObserveRow[]> | null = null
let watchObservePreloadKey = ''
const ALL_MARKET_SCENE_STAGE_THRESHOLD_KEY = 'am_scene_stage_threshold'

function readSceneStageThreshold() {
  try {
    const value = localStorage.getItem(ALL_MARKET_SCENE_STAGE_THRESHOLD_KEY)
    if (value === 'observe' || value === 'trigger' || value === 'confirm') {
      return value
    }
  } catch {
    // localStorage unavailable
  }
  return 'trigger'
}

function resolveSourcePath(sourcePath?: string | null) {
  const trimmed = sourcePath?.trim() ?? ''
  if (trimmed !== '') {
    return trimmed
  }

  const stored = readStoredSourcePath().trim()
  return stored !== '' ? stored : null
}

function buildStoredRowPayload(
  input: WatchObserveInput & { watchDate?: string },
) {
  return {
    tsCode: input.tsCode,
    name: input.name?.trim() || undefined,
    watchDate: input.watchDate?.trim() || undefined,
    tag: input.tag?.trim() || undefined,
    concept: input.concept?.trim() || undefined,
    markedDate: input.markedDate?.trim() || undefined,
  }
}

function buildStoredRowPayloadFromRow(row: WatchObserveRow) {
  return buildStoredRowPayload({
    tsCode: row.tsCode,
    name: row.name,
    watchDate: row.watchDate,
    tag: row.tag,
    concept: row.concept,
    markedDate: row.markedDate,
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
    sceneStageThreshold: readSceneStageThreshold(),
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
    sceneStageThreshold: readSceneStageThreshold(),
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

function updateCachedWatchObserveMarkedDate(tsCode: string, markedDate: string) {
  const normalizedTsCode = normalizeTsCode(tsCode)
  if (!normalizedTsCode) {
    throw new Error('自选代码无效')
  }
  const normalizedMarkedDate = normalizeDateValue(markedDate)
  if (!/^\d{8}$/.test(normalizedMarkedDate)) {
    throw new Error('标记日期无效')
  }

  const rows = readWatchObserveRowsFromCache()
  const existing = rows.find((row) => row.tsCode === normalizedTsCode)
  if (!existing) {
    throw new Error(`未找到自选记录: ${normalizedTsCode}`)
  }

  const nextRows = rows.map((row) =>
    row.tsCode === normalizedTsCode
      ? { ...row, markedDate: normalizedMarkedDate }
      : row,
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
  return listHydratedWatchObserveRows(sourcePath, referenceTradeDate)
}

export async function refreshWatchObserveRows(
  referenceTradeDate?: string | null,
  sourcePath?: string | null,
) {
  return refreshHydratedWatchObserveRows(sourcePath, referenceTradeDate)
}

export async function upsertWatchObserveRow(input: WatchObserveInput, sourcePath?: string | null) {
  const resolvedSourcePath = resolveSourcePath(sourcePath)
  if (!resolvedSourcePath) {
    throw new Error('请先配置数据源目录')
  }
  const watchDate = await invoke<string>('resolve_watch_observe_watch_date', {
    sourcePath: resolvedSourcePath,
  })
  upsertCachedWatchObserveRow({
    tsCode: input.tsCode,
    name: input.name,
    watchDate,
    tag: input.tag,
    concept: input.concept,
    markedDate: input.markedDate,
  })
  return listHydratedWatchObserveRows(resolvedSourcePath)
}

export async function updateWatchObserveTag(tsCode: string, tag: string, sourcePath?: string | null) {
  updateCachedWatchObserveTag(tsCode, tag)
  return listHydratedWatchObserveRows(sourcePath)
}

export async function updateWatchObserveMarkedDate(
  tsCode: string,
  markedDate: string,
  sourcePath?: string | null,
) {
  updateCachedWatchObserveMarkedDate(tsCode, markedDate)
  return listHydratedWatchObserveRows(sourcePath)
}

export async function removeWatchObserveRows(tsCodes: string[], sourcePath?: string | null) {
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

  const preloadPromise = listHydratedWatchObserveRows(sourcePath, referenceTradeDate)

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
