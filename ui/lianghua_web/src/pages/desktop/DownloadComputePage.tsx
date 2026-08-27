import { useRef, useState } from 'react'
import { readStoredDragonTigerDownloadSettings } from '../../apis/dataDownload'
import DataDownloadPage, { type DataDownloadPageHandle } from './DataDownloadPage'
import RankingComputePage, { type RankingComputePageHandle } from './RankingComputePage'
import './css/DownloadComputePage.css'

type DailyWorkflowStage = 'idle' | 'download' | 'ranking' | 'similarity' | 'completed' | 'skipped' | 'failed'

export default function DownloadComputePage() {
  const [rankingStatusRefreshSignal, setRankingStatusRefreshSignal] = useState(0)
  const [sharedToken, setSharedToken] = useState(
    () => readStoredDragonTigerDownloadSettings().token,
  )
  const [dailyWorkflowStage, setDailyWorkflowStage] = useState<DailyWorkflowStage>('idle')
  const [dailyWorkflowMessage, setDailyWorkflowMessage] = useState('依次执行增量更新、当日排名和走势相似度排名。')
  const downloadPageRef = useRef<DataDownloadPageHandle>(null)
  const rankingPageRef = useRef<RankingComputePageHandle>(null)
  const dailyWorkflowRunningRef = useRef(false)
  const dailyWorkflowRunning = ['download', 'ranking', 'similarity'].includes(dailyWorkflowStage)

  async function runDailyWorkflow() {
    if (!downloadPageRef.current || !rankingPageRef.current) {
      return
    }

    dailyWorkflowRunningRef.current = true
    setDailyWorkflowStage('download')
    setDailyWorkflowMessage('正在执行行情增量更新；如新筹码策略有变化，将自动跳过新筹码维护。')
    try {
      const downloadResult = await downloadPageRef.current.runDailyIncrementalUpdate()
      if (!downloadResult.completed || !downloadResult.latestTradeDate) {
        setDailyWorkflowStage('failed')
        setDailyWorkflowMessage('增量更新未完成，工作流已停止；请查看下载任务提示。')
        return
      }

      setDailyWorkflowStage('ranking')
      setDailyWorkflowMessage(
        `${downloadResult.chipStrategySkipped ? '检测到新筹码策略变化，已走跳过路径；' : ''}正在检查排名策略并把排名补算到 ${downloadResult.latestTradeDate}。`,
      )
      const rankingResult = await rankingPageRef.current.runDailyRankingWorkflow(
        downloadResult.latestTradeDate,
        (stage) => {
          setDailyWorkflowStage(stage)
          if (stage === 'similarity') {
            setDailyWorkflowMessage(
              `排名计算已完成，正在计算 ${downloadResult.latestTradeDate} 的走势相似度排名。`,
            )
          }
        },
      )
      if (rankingResult.strategyChanged) {
        setDailyWorkflowStage('skipped')
        setDailyWorkflowMessage('检测到排名策略变化，已按规则跳过排名和后续走势相似度计算。')
        return
      }

      setDailyWorkflowStage('completed')
      setDailyWorkflowMessage(
        `每日工作流完成：增量更新、${downloadResult.latestTradeDate} 排名和走势相似度排名均已完成${downloadResult.chipStrategySkipped ? '；新筹码维护因策略变化已跳过' : ''}。`,
      )
    } catch (workflowError) {
      setDailyWorkflowStage('failed')
      setDailyWorkflowMessage(`每日工作流失败：${String(workflowError)}`)
    } finally {
      dailyWorkflowRunningRef.current = false
    }
  }

  return (
    <div className="download-compute-page">
      <section className="download-compute-token-card" aria-labelledby="shared-token-title">
        <div>
          <span>公共参数</span>
          <h1 id="shared-token-title">Tushare Token</h1>
          <p>行情下载、缺失股票补全和龙虎榜下载共用，并自动保存在当前浏览器。</p>
        </div>
        <label>
          <span>Token</span>
          <input
            type="password"
            value={sharedToken}
            onChange={(event) => setSharedToken(event.target.value)}
            placeholder="请输入 Tushare Token"
          />
        </label>
      </section>

      <section className="download-compute-section download-compute-section-daily" aria-labelledby="daily-workflow-title">
        <header className="download-compute-section-head">
          <span className="download-compute-section-index">00 · 每日工作流</span>
          <div>
            <h1 id="daily-workflow-title">一键完成每日更新</h1>
            <p>顺序执行增量更新、最新交易日排名和走势相似度排名，并按策略变化自动短路。</p>
          </div>
          <button
            className="download-compute-daily-btn"
            type="button"
            onClick={() => void runDailyWorkflow()}
            disabled={dailyWorkflowRunning}
          >
            {dailyWorkflowRunning ? '每日工作流执行中...' : '开始每日工作流'}
          </button>
        </header>
        <div className={`download-compute-daily-status is-${dailyWorkflowStage}`}>
          {dailyWorkflowMessage}
        </div>
      </section>

      <section className="download-compute-section download-compute-section-download" aria-labelledby="download-task-title">
        <header className="download-compute-section-head">
          <span className="download-compute-section-index">01 · 数据获取</span>
          <div>
            <h1 id="download-task-title">下载任务</h1>
            <p>从外部数据源拉取行情、龙虎榜和概念数据。需要鉴权的任务统一使用下方公共 Token。</p>
          </div>
        </header>

        <DataDownloadPage
          ref={downloadPageRef}
          mergedMode
          sharedToken={sharedToken}
          onSharedTokenChange={setSharedToken}
          hideSharedTokenSection
          onMainTaskComplete={() => {
            if (!dailyWorkflowRunningRef.current) {
              setRankingStatusRefreshSignal((current) => current + 1)
            }
          }}
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

        <RankingComputePage ref={rankingPageRef} mergedMode statusRefreshSignal={rankingStatusRefreshSignal} />
      </section>
    </div>
  )
}
