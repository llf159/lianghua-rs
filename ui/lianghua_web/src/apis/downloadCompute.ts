import { invoke } from '@tauri-apps/api/core'
import type { DataDownloadStatus } from './dataDownload'
import type { RankingComputeStatus } from './rankingCompute'

export type DownloadComputeOverview = {
  sourcePath: string
  downloadStatus: DataDownloadStatus
  rankingStatus: RankingComputeStatus
  rankingStrategyChanged: boolean
  similarityBenchmarkCodes: string[]
}

let pendingOverview: Promise<DownloadComputeOverview> | null = null

export function getDownloadComputeOverview() {
  if (!pendingOverview) {
    pendingOverview = invoke<DownloadComputeOverview>('get_download_compute_overview').finally(() => {
      pendingOverview = null
    })
  }
  return pendingOverview
}
