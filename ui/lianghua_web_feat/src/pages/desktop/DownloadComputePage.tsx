import { useState } from 'react'
import DataDownloadPage from './DataDownloadPage'
import RankingComputePage from './RankingComputePage'
import './css/DownloadComputePage.css'

export default function DownloadComputePage() {
  const [rankingStatusRefreshSignal, setRankingStatusRefreshSignal] = useState(0)

  return (
    <div className="download-compute-page">
      <RankingComputePage mergedMode statusRefreshSignal={rankingStatusRefreshSignal} />
      <DataDownloadPage
        mergedMode
        onMainTaskComplete={() => setRankingStatusRefreshSignal((current) => current + 1)}
      />
    </div>
  )
}
