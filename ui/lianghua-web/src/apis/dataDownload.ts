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
  plannedAction: string
  plannedActionLabel: string
  plannedActionDetail: string
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

export async function getDataDownloadStatus(sourcePath: string) {
  return invoke<DataDownloadStatus>('get_data_download_status', { sourcePath })
}

export async function runDataDownload(request: DataDownloadRequest) {
  return invoke<DataDownloadRunResult>('run_data_download', { request })
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
