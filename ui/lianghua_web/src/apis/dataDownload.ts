import { invoke } from '@tauri-apps/api/core'
import { listen, type UnlistenFn } from '@tauri-apps/api/event'

const DATA_DOWNLOAD_EVENT = 'data-download-status'
export const DATA_DOWNLOAD_DRAFT_STORAGE_KEY = 'lh_data_download_draft_v1'

export type StoredDragonTigerDownloadSettings = {
  token: string
  startDate: string
  retryTimes: number
  limitCallsPerMin: number
}

export function readStoredDragonTigerDownloadSettings(): StoredDragonTigerDownloadSettings {
  const fallback: StoredDragonTigerDownloadSettings = {
    token: '',
    startDate: '2005-01-01',
    retryTimes: 3,
    limitCallsPerMin: 190,
  }
  if (typeof window === 'undefined') {
    return fallback
  }

  try {
    const raw = window.localStorage.getItem(DATA_DOWNLOAD_DRAFT_STORAGE_KEY)
    if (!raw) {
      return fallback
    }
    const parsed = JSON.parse(raw) as Record<string, unknown>
    return {
      token: typeof parsed.token === 'string' ? parsed.token.trim() : fallback.token,
      startDate:
        typeof parsed.dragonTigerStartDate === 'string'
          ? parsed.dragonTigerStartDate
          : fallback.startDate,
      retryTimes:
        typeof parsed.retryTimes === 'number' && Number.isFinite(parsed.retryTimes)
          ? Math.max(0, parsed.retryTimes)
          : fallback.retryTimes,
      limitCallsPerMin:
        typeof parsed.limitCallsPerMin === 'number' && Number.isFinite(parsed.limitCallsPerMin)
          ? Math.max(1, parsed.limitCallsPerMin)
          : fallback.limitCallsPerMin,
    }
  } catch {
    return fallback
  }
}

export type DataDownloadFileStatus = {
  fileName: string
  exists: boolean
  rowCount: number
  minTradeDate: string | null
  maxTradeDate: string | null
}

export type RankComputeDbRange = {
  fileName: string
  tableName: string
  exists: boolean
  minTradeDate: string | null
  maxTradeDate: string | null
  distinctTradeDates: number
  rowCount: number
}

export type DragonTigerDbStatus = {
  fileName: string
  exists: boolean
  minTradeDate: string | null
  maxTradeDate: string | null
  syncedTradeDates: number
  topListRows: number
  topInstRows: number
}

export type DataDownloadStatus = {
  sourcePath: string
  sourceDb: RankComputeDbRange
  conceptPerformanceDb: RankComputeDbRange
  dragonTigerDb: DragonTigerDbStatus
  stockList: DataDownloadFileStatus
  tradeCalendar: DataDownloadFileStatus
  thsConcepts: DataDownloadFileStatus
  missingStockRepair: DataDownloadMissingStockRepairStatus
  pendingDerivedRebuilds: Array<{ component: string; startDate: string; endDate: string }>
  rankingResultsInvalidated: boolean
  similarityResultsInvalidated: boolean
  cyqChenMaintenance: DataDownloadCyqChenMaintenanceStatus
  dailyTargetTradeDate: string | null
  plannedAction: string
  plannedActionLabel: string
  plannedActionDetail: string
}

export type DataDownloadMissingStockRepairStatus = {
  ready: boolean
  missingCount: number
  missingSamples: string[]
  suggestedStartDate: string | null
  suggestedEndDate: string | null
  detail: string
}

export type DataDownloadCyqChenMaintenanceStatus = {
  dbExists: boolean
  hasData: boolean
  strategyChanged: boolean
  detail: string
}

export type DataDownloadRequest = {
  downloadId: string
  sourcePath: string
  token: string
  startDate: string
  endDate: string
  threads: number
  retryTimes: number
  limitCallsPerMin: number
  includeTurnover: boolean
  allowStaleStockList: boolean
  allowCyqChenStrategyRebuild: boolean
  chipModel?: 'legacy' | 'chen'
}

export type MissingStockRepairRequest = {
  downloadId: string
  sourcePath: string
  token: string
  threads: number
  retryTimes: number
  limitCallsPerMin: number
  includeTurnover: boolean
}

export type DragonTigerDownloadRequest = {
  downloadId: string
  sourcePath: string
  token: string
  startDate: string
  endDate: string
  retryTimes: number
  limitCallsPerMin: number
}

export type ThsConceptDownloadRequest = {
  downloadId: string
  sourcePath: string
  retryEnabled: boolean
  retryTimes: number
  retryIntervalSecs: number
  concurrentEnabled: boolean
  workerThreads: number
}

export type ConceptPerformanceRepairRequest = {
  downloadId: string
  sourcePath: string
}

export type ConceptMostRelatedRepairRequest = {
  downloadId: string
  sourcePath: string
}

export type StockDataIndicatorColumnsDeleteRequest = {
  downloadId: string
  sourcePath: string
}

export type StockDataIndicatorColumnsRebuildRequest = {
  downloadId: string
  sourcePath: string
}

export type DataDownloadSummary = {
  successCount: number
  failedCount: number
  savedRows: number
  conceptPerformanceRows: number
  failedItems: string[]
}

export type DataDownloadRunResult = {
  action: string
  actionLabel: string
  elapsedMs: number
  summary: DataDownloadSummary
  completionDetails: string[]
  status: DataDownloadStatus
}

export type DataDownloadProgress = {
  downloadId: string
  phase: 'started' | 'running' | 'completed' | 'failed' | string
  action: string
  actionLabel: string
  elapsedMs: number
  finished: number
  total: number
  currentLabel: string | null
  message: string
}

export type IndicatorManageItem = {
  index: number
  name: string
  expr: string
  prec: number
}

export type IndicatorManageDraft = {
  name: string
  expr: string
  prec: number
}

export type IndicatorManagePageData = {
  exists: boolean
  filePath: string
  items: IndicatorManageItem[]
}

export async function getDataDownloadStatus(sourcePath: string) {
  return invoke<DataDownloadStatus>('get_data_download_status', { sourcePath })
}

export async function runDataDownload(request: DataDownloadRequest) {
  return invoke<DataDownloadRunResult>('run_data_download', { request })
}

export async function runMissingStockRepair(request: MissingStockRepairRequest) {
  return invoke<DataDownloadRunResult>('run_missing_stock_repair', { request })
}

export async function runDragonTigerDownload(request: DragonTigerDownloadRequest) {
  return invoke<DataDownloadRunResult>('run_dragon_tiger_download', { request })
}

export async function runThsConceptDownload(request: ThsConceptDownloadRequest) {
  return invoke<DataDownloadRunResult>('run_ths_concept_download', { request })
}

export async function runConceptPerformanceRepair(request: ConceptPerformanceRepairRequest) {
  return invoke<DataDownloadRunResult>('run_concept_performance_repair', { request })
}

export async function runConceptMostRelatedRepair(request: ConceptMostRelatedRepairRequest) {
  return invoke<DataDownloadRunResult>('run_concept_most_related_repair', { request })
}

export async function runStockDataIndicatorColumnsDelete(
  request: StockDataIndicatorColumnsDeleteRequest,
) {
  return invoke<DataDownloadRunResult>('run_stock_data_indicator_columns_delete', { request })
}

export async function runStockDataIndicatorColumnsRebuild(
  request: StockDataIndicatorColumnsRebuildRequest,
) {
  return invoke<DataDownloadRunResult>('run_stock_data_indicator_columns_rebuild', { request })
}

export async function getIndicatorManagePage(sourcePath: string) {
  return invoke<IndicatorManagePageData>('get_indicator_manage_page', { sourcePath })
}

export async function saveIndicatorManagePage(sourcePath: string, items: IndicatorManageDraft[]) {
  return invoke<IndicatorManagePageData>('save_indicator_manage_page', { sourcePath, items })
}

export async function listenDataDownloadProgress(
  downloadId: string,
  onProgress: (progress: DataDownloadProgress) => void,
) {
  return listen<DataDownloadProgress>(DATA_DOWNLOAD_EVENT, (event) => {
    if (event.payload.downloadId !== downloadId) {
      return
    }

    onProgress(event.payload)
  }) as Promise<UnlistenFn>
}

export type DatabaseIssue = {
  id: string
  checkId: string
  database: string
  severity: 'error' | 'warning' | 'info'
  title: string
  comparisons: Array<{ label: string; expected: string; actual: string }>
  tsCode: string | null
  stockName: string | null
  startDate: string | null
  endDate: string | null
  repairAction: 'stock' | 'market' | 'index' | 'concept' | 'chip' | 'legacy-chip' | 'ranking' | 'similarity' | 'update' | null
  blocksDerived: boolean
}

export type DatabaseCheckReport = {
  sourcePath: string
  checkedAt: string
  sourceReady: boolean
  targetTradeDate: string | null
  stockCount: number
  rowCount: number
  checks: Array<{
    id: string
    title: string
    status: 'passed' | 'warning' | 'error' | 'incomplete' | 'skipped'
    summary: string
  }>
  issues: DatabaseIssue[]
  limitations: string[]
}

export async function checkDatabases(sourcePath: string) {
  return invoke<DatabaseCheckReport>('check_databases', { sourcePath })
}

export async function repairDatabaseIssues(downloadId: string, request: {
  sourcePath: string
  issueIds: string[]
  token: string
  threads: number
  retryTimes: number
  limitCallsPerMin: number
  includeTurnover: boolean
}) {
  return invoke<DataDownloadRunResult>('repair_database_issues', { downloadId, request })
}
