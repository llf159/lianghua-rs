import { useCallback, useEffect, useRef, useState } from 'react'
import {
  getDataDownloadStatus,
  readStoredDragonTigerDownloadSettings,
} from '../../apis/dataDownload'
import { inspectManagedSourceStatus } from '../../apis/managedSource'
import { getRankingComputeStatus } from '../../apis/rankingCompute'
import {
  getManagedStrategyAssetsStatus,
  getManagedStrategyBackupDiff,
} from '../../apis/strategyAssets'
import DataTaskProgress from '../../shared/DataTaskProgress'
import DataDownloadPage, { type DataDownloadPageHandle } from './DataDownloadPage'
import RankingComputePage, { type RankingComputePageHandle } from './RankingComputePage'
import './css/DownloadComputePage.css'

type DailyWorkflowStage = 'idle' | 'download' | 'ranking' | 'similarity' | 'completed' | 'skipped' | 'failed'

type DailyCompletion = {
  loading: boolean
  targetTradeDate: string | null
  download: boolean
  ranking: boolean
  similarity: boolean
  rankingStrategyChanged: boolean
  error: string
}

const EMPTY_DAILY_COMPLETION: DailyCompletion = {
  loading: true,
  targetTradeDate: null,
  download: false,
  ranking: false,
  similarity: false,
  rankingStrategyChanged: false,
  error: '',
}

function formatCompactTradeDate(value: string | null) {
  if (!value || !/^\d{8}$/.test(value)) {
    return value ?? '--'
  }
  return `${value.slice(0, 4)}-${value.slice(4, 6)}-${value.slice(6, 8)}`
}

function formatElapsedMs(value: number) {
  if (value < 1000) {
    return `${Math.max(0, Math.round(value))} ms`
  }
  return `${(value / 1000).toFixed(value >= 10_000 ? 1 : 2)} s`
}

export default function DownloadComputePage() {
  const [rankingStatusRefreshSignal, setRankingStatusRefreshSignal] = useState(0)
  const [sharedToken, setSharedToken] = useState(
    () => readStoredDragonTigerDownloadSettings().token,
  )
  const [dailyWorkflowStage, setDailyWorkflowStage] = useState<DailyWorkflowStage>('idle')
  const [dailyWorkflowMessage, setDailyWorkflowMessage] = useState('依次执行增量更新、当日排名和走势相似度排名。')
  const [dailyWorkflowStartedAt, setDailyWorkflowStartedAt] = useState<number | null>(null)
  const [dailyWorkflowElapsedMs, setDailyWorkflowElapsedMs] = useState(0)
  const [dailyCompletion, setDailyCompletion] = useState<DailyCompletion>(EMPTY_DAILY_COMPLETION)
  const downloadPageRef = useRef<DataDownloadPageHandle>(null)
  const rankingPageRef = useRef<RankingComputePageHandle>(null)
  const dailyWorkflowRunningRef = useRef(false)
  const dailyWorkflowRunning = ['download', 'ranking', 'similarity'].includes(dailyWorkflowStage)
  const dailyAllCompleted =
    Boolean(dailyCompletion.targetTradeDate) &&
    dailyCompletion.download &&
    dailyCompletion.ranking &&
    dailyCompletion.similarity

  const refreshDailyCompletion = useCallback(async () => {
    setDailyCompletion((current) => ({ ...current, loading: true, error: '' }))
    try {
      const managedStatus = await inspectManagedSourceStatus()
      const [downloadStatus, rankingStatus, strategyAssetsStatus] = await Promise.all([
        getDataDownloadStatus(managedStatus.sourcePath),
        getRankingComputeStatus(managedStatus.sourcePath),
        getManagedStrategyAssetsStatus(),
      ])
      const latestComputeSnapshot = strategyAssetsStatus.backups.find(
        (item) => item.sourceKind === 'rank_compute',
      )
      const rankingStrategyChanged = latestComputeSnapshot
        ? (await getManagedStrategyBackupDiff(latestComputeSnapshot.backupId)).changedLineCount > 0
        : false
      const targetTradeDate = downloadStatus.dailyTargetTradeDate
      const reachesTarget = (latestTradeDate: string | null | undefined) =>
        Boolean(targetTradeDate && latestTradeDate && latestTradeDate >= targetTradeDate)
      setDailyCompletion({
        loading: false,
        targetTradeDate,
        download: reachesTarget(downloadStatus.sourceDb.maxTradeDate),
        ranking: !rankingStrategyChanged && reachesTarget(rankingStatus.resultDb.maxTradeDate),
        similarity:
          !rankingStrategyChanged && reachesTarget(rankingStatus.similarityRankDb.maxTradeDate),
        rankingStrategyChanged,
        error: '',
      })
    } catch (completionError) {
      setDailyCompletion((current) => ({
        ...current,
        loading: false,
        error: `检测失败：${String(completionError)}`,
      }))
    }
  }, [])

  useEffect(() => {
    void refreshDailyCompletion()
  }, [refreshDailyCompletion])

  useEffect(() => {
    if (!dailyWorkflowRunning || dailyWorkflowStartedAt === null) {
      return
    }

    const updateElapsed = () => setDailyWorkflowElapsedMs(Date.now() - dailyWorkflowStartedAt)
    updateElapsed()
    const timer = window.setInterval(updateElapsed, 250)
    return () => window.clearInterval(timer)
  }, [dailyWorkflowRunning, dailyWorkflowStartedAt])

  const dailyStageIndex =
    dailyWorkflowStage === 'download' ? 0 : dailyWorkflowStage === 'ranking' ? 1 : 2
  const dailyStageLabels = ['增量更新', '排名计算', '相似度计算']
  const dailyProgressPercent = dailyWorkflowRunning
    ? Math.round((dailyStageIndex / dailyStageLabels.length) * 100)
    : null
  const dailyProgressSegments = dailyStageLabels.map((label, index) => ({
    key: label,
    label,
    state: index < dailyStageIndex ? 'done' as const : index === dailyStageIndex ? 'active' as const : 'pending' as const,
  }))

  async function runDailyWorkflow() {
    if (!downloadPageRef.current || !rankingPageRef.current) {
      return
    }

    dailyWorkflowRunningRef.current = true
    setDailyWorkflowStartedAt(Date.now())
    setDailyWorkflowElapsedMs(0)
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
      await refreshDailyCompletion()
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
        {!dailyWorkflowRunning ? (
          <div className={`download-compute-daily-status is-${dailyWorkflowStage}`}>
            {dailyWorkflowMessage}
          </div>
        ) : null}
        {dailyWorkflowRunning ? (
          <DataTaskProgress
            phaseLabel={dailyStageLabels[dailyStageIndex]}
            phaseStepPillText={` · ${dailyStageIndex + 1}/3`}
            phaseStepStatText={` ${dailyStageIndex + 1}/3`}
            actionLabel="每日工作流"
            progressPercent={dailyProgressPercent}
            progressSegments={dailyProgressSegments}
            elapsedText={formatElapsedMs(dailyWorkflowElapsedMs)}
            shownProgressPercent={dailyProgressPercent ?? 0}
            progressCounterText={`${dailyStageIndex + 1} / 3`}
            currentObjectText={formatCompactTradeDate(dailyCompletion.targetTradeDate)}
            message={dailyWorkflowMessage}
            fallbackMessage="每日工作流正在执行。"
          />
        ) : null}
        <div className={`download-compute-daily-check${dailyAllCompleted ? ' is-completed' : ''}`}>
          <div>
            <strong>
              {dailyCompletion.loading
                ? '正在检测当日完成状态...'
                : dailyCompletion.error
                  ? dailyCompletion.error
                  : dailyAllCompleted
                    ? '当日工作已全部完成'
                    : '当日工作尚未全部完成'}
            </strong>
            <span>
              目标交易日：{formatCompactTradeDate(dailyCompletion.targetTradeDate)}
              {dailyCompletion.rankingStrategyChanged ? '；排名策略有变化' : ''}
            </span>
          </div>
          <button
            type="button"
            onClick={() => void refreshDailyCompletion()}
            disabled={dailyCompletion.loading || dailyWorkflowRunning}
          >
            {dailyCompletion.loading ? '检测中...' : '重新检测'}
          </button>
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
