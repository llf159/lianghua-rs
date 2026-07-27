import { useState } from 'react'
import DataDownloadPage from './DataDownloadPage'
import RankingComputePage from './RankingComputePage'
import './css/DownloadComputePage.css'

export default function DownloadComputePage() {
  const [rankingStatusRefreshSignal, setRankingStatusRefreshSignal] = useState(0)

  return (
    <div className="download-compute-page">
      <section className="download-compute-section download-compute-section-download" aria-labelledby="download-task-title">
        <header className="download-compute-section-head">
          <span className="download-compute-section-index">01 · 数据获取</span>
          <div>
            <h1 id="download-task-title">下载任务</h1>
            <p>从外部数据源拉取行情、龙虎榜和概念数据。需要鉴权的任务统一使用下方公共 Token。</p>
          </div>
        </header>

        <DataDownloadPage
          mergedMode
          onMainTaskComplete={() => setRankingStatusRefreshSignal((current) => current + 1)}
        />
      </section>

      <section className="download-compute-section download-compute-section-compute" aria-labelledby="compute-task-title">
        <header className="download-compute-section-head">
          <span className="download-compute-section-index">02 · 本地处理</span>
          <div>
            <h1 id="compute-task-title">计算任务</h1>
            <p>基于已下载的本地数据执行排名、概念表现、筹码和指标列计算。</p>
          </div>
        </header>

        <RankingComputePage mergedMode statusRefreshSignal={rankingStatusRefreshSignal} />
      </section>
    </div>
  )
}
