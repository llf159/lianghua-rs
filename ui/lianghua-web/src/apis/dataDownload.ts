import { invoke } from '@tauri-apps/api/core'
import { listen, type UnlistenFn } from '@tauri-apps/api/event'

const DATA_DOWNLOAD_EVENT = 'data-download-status'

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

export type DataDownloadStatus = {
  sourcePath: string
  sourceDb: RankComputeDbRange
  stockList: DataDownloadFileStatus
  tradeCalendar: DataDownloadFileStatus
  thsConcepts: DataDownloadFileStatus
  missingStockRepair: DataDownloadMissingStockRepairStatus
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

export type ThsConceptDownloadRequest = {
  downloadId: string
  sourcePath: string
  retryEnabled: boolean
  retryTimes: number
  retryIntervalSecs: number
  concurrentEnabled: boolean
  workerThreads: number
}

export type DataDownloadSummary = {
  successCount: number
  failedCount: number
  savedRows: number
  failedItems: string[]
}

export type DataDownloadRunResult = {
  action: string
  actionLabel: string
  elapsedMs: number
  summary: DataDownloadSummary
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

export async function runThsConceptDownload(request: ThsConceptDownloadRequest) {
  return invoke<DataDownloadRunResult>('run_ths_concept_download', { request })
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
