import DataDownloadPage from './DataDownloadPage'
import RankingComputePage from './RankingComputePage'
import './css/DownloadComputePage.css'

export default function DownloadComputePage() {
  return (
    <div className="download-compute-page">
      <RankingComputePage mergedMode />
      <DataDownloadPage mergedMode />
    </div>
  )
}
