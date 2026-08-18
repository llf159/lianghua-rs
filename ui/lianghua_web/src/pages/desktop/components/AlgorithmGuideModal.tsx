import { useEffect } from 'react'
import '../css/AlgorithmGuideModal.css'

type AlgorithmSection = {
  title: string
  items: {
    heading: string
    description: string
    formula?: string
    interpretation?: string
  }[]
}

const ALGORITHM_SECTIONS: AlgorithmSection[] = [
  {
    title: '1. 评分 / 排名算法',
    items: [
      {
        heading: '总分计算',
        description: '每只股票在每个交易日的总分从 50 分开始，逐条累加命中规则的得分。规则得分取决于命中距离、命中次数和组合 bonus。',
        formula: 'total_score = 50 + Σ(各规则当日得分)',
        interpretation: '分数越高表示越符合策略选股条件。同一天不同股票可以比较总分大小。',
      },
      {
        heading: 'scope_way 命中判定',
        description: '每条规则的 scope_way 决定"窗口内满足条件"的统计方式：',
        formula: 'LAST：当天表达式为真即命中\nANY：窗口内任意一天为真即命中\nEACH：窗口内每天单独计分，命中次数 × 单次得分\nRECENT：取最近一次命中距当前的天数\nCONSEC：窗口内连续命中次数 ≥ 阈值才命中',
        interpretation: 'LAST 适合捕捉当天状态；EACH 适合"多次出现加分"；CONSEC 适合趋势确认。',
      },
      {
        heading: '场景状态机',
        description: '每个场景由 trigger / confirm / observe / fail 四条规则线组成，从下到上逐级判定：',
        formula: 'fail：有 fail 规则命中且 risk_score 超过阈值\nconfirm：有 confirm 规则命中且 stage_score 超过阈值\ntrigger / observe：有 trigger 规则命中且超过对应阈值',
        interpretation: 'confirm 是最强信号，fail 代表风险信号。场景状态越靠上，信号越强。',
      },
      {
        heading: '强度指标',
        description: 'confirm_strength 和 risk_intensity 分别衡量确认强度和风险强度：',
        formula: 'confirm_strength = |stage_score| / confirm_threshold\nrisk_intensity = |risk_score| / fail_threshold',
        interpretation: '数值 > 1 表示超过阈值，越大表示信号越强。',
      },
      {
        heading: '总榜排名',
        description: '所有股票按 total_score 降序排列，同分时按 ts_code 字典序打破平局。',
        interpretation: 'rank = 1 表示总分最高。排名每天重新计算，会随分数变化而变动。',
      },
    ],
  },
  {
    title: '2. 回测指标算法',
    items: [
      {
        heading: '残差收益率',
        description: '先用指数 / 概念 / 行业 beta 估算股票"应有收益"，再用实际收益减去它，得到剔除市场影响后的纯选股能力。',
        formula: 'expected_pct = index_beta × index_pct + concept_beta × concept_pct + industry_beta × industry_pct\nresidual_pct = stock_pct - expected_pct',
        interpretation: '正值表示跑赢基准，负值表示跑输基准。回测窗口内多日累加称为 forward residual。',
      },
      {
        heading: 'IC（信息系数）',
        description: '每天把股票的评分排名和残差收益率排名做 Pearson 相关系数，衡量"评分高低"与"未来收益"是否同向。',
        formula: 'IC = PearsonCorr(平均排名(评分), 平均排名(残差))',
        interpretation: 'IC ∈ [-1, 1]。正值越大说明评分越准；0.03 以上可视为有一定预测力，0.05 以上较好。',
      },
      {
        heading: 'ICIR（信息比率）',
        description: 'IC 的均值除以标准差，反映预测能力的稳定性。',
        formula: 'ICIR = mean(IC) / std(IC)',
        interpretation: 'ICIR > 1 表示稳定盈利；0.5~1 是可接受区间；< 0.5 说明 IC 波动大、不稳定。',
      },
      {
        heading: 't 统计量',
        description: '检验 IC 均值是否显著不为零。',
        formula: 't = IC_mean × √sample_count / IC_std',
        interpretation: '|t| > 2 通常认为统计显著（约 95% 置信度）。t 越大，IC 不是随机波动的证据越强。',
      },
      {
        heading: '残差均值',
        description: '所有触发样本在回测窗口内的日均残差收益率。',
        formula: 'avg_residual_mean = mean(每日触发样本残差收益率)',
        interpretation: '正值表示策略整体跑赢基准；结合 IC 一起看，IC 高 + 残差为正是最优组合。',
      },
      {
        heading: '超额残差均值',
        description: '触发样本残差减去全市场残差后的日均值。',
        formula: '每日超额 = mean(触发样本残差) - mean(全市场残差)\navg_excess = mean(每日超额)',
        interpretation: '剔除了市场整体涨跌后的相对优势，更能反映策略本身能力。',
      },
      {
        heading: '盈亏比',
        description: '盈利日残差总和与亏损日残差总和的比值。',
        formula: 'profit_loss_ratio = Σ(正残差) / Σ(|负残差|)',
        interpretation: '> 1 表示赢多亏少；1.5 以上较好；< 1 说明亏损幅度大于盈利幅度。',
      },
      {
        heading: '效率比率 (ER) 变化',
        description: '衡量价格走势的"方向效率"，即净位移占总波动的比例。',
        formula: 'ER = (close[t] - close[t-period]) / Σ|close[i] - close[i-1]|   (i 从 t-period+1 到 t)\nER_change = ER(窗口结束) - ER(窗口开始)',
        interpretation: 'ER_change > 0 表示走势变得更有方向性；< 0 表示震荡加剧。',
      },
      {
        heading: '分层回测',
        description: '按评分把股票分成若干层（组），观察每层平均残差收益率是否随分数单调递增。',
        formula: '方法一（Score 分层）：分数范围等距分组\n方法二（SampleCount 分层）：每组样本数相等（等分位数）\n方法三（Rank 分层）：按数据库排名分组',
        interpretation: '高分组残差 > 低分组残差 = 策略有效。top-bottom spread 越大，分层效果越好。',
      },
    ],
  },
  {
    title: '3. 股票遴选算法',
    items: [
      {
        heading: '表达式选股',
        description: '对全市场股票逐只求值用户表达式，按 scope_way 判定是否命中，再经过版块、概念、市值等过滤后输出候选列表。',
        formula: '命中判定 → 过滤条件 → 按 rank 升序排列',
        interpretation: '结果列表按排名从优到劣排列；排名靠前 = 综合评分更高。',
      },
      {
        heading: '概念选股',
        description: '不写表达式，直接按概念 / 行业 / 地区 / 市值范围筛选股票。支持 AND（同时满足）和 OR（满足任一）两种概念匹配模式。',
        formula: '过滤链：ST 排除 → 版块 → 地区 → 行业 → 市值 → 概念匹配 → 概念排除',
        interpretation: '适合快速筛选特定主题的股票池。',
      },
    ],
  },
  {
    title: '4. 实时监控算法',
    items: [
      {
        heading: '模板表达式求值',
        description: '盘中实时获取最新价、今开、最高、最低、成交量，合并到历史 K 线序列后，逐只股票求值用户定义的监控模板。',
        formula: 'RT_OP = (当前价 - 今开) / 昨收 × 100%\nRT_FH = (最高 - 当前价) / 最高 × 100%\nRT_VR = 实时量比\nRT_AVG = 实时均价',
        interpretation: 'RT_OP > 0 表示股价在今日开盘价之上；RT_FH 越大表示距高点回落越多；RT_VR > 1 表示成交量高于近期平均。',
      },
      {
        heading: '5 日涨跌幅',
        description: '以当日实时价相对于 5 个交易日前收盘价的变动百分比。',
        formula: 'return_5d = (realtime_price - close[5天前]) / close[5天前] × 100%',
        interpretation: '正值表示近 5 日上涨，负值表示下跌。',
      },
    ],
  },
  {
    title: '5. 关键数据表解读',
    items: [
      {
        heading: 'score_summary 表',
        description: '每只股票每个交易日的评分汇总。',
        formula: '字段：ts_code, trade_date, total_score, rank',
        interpretation: 'total_score 越高越好；rank = 1 是当日第一。按 trade_date + rank 查询可得每日榜单。',
      },
      {
        heading: 'rule_details 表',
        description: '每条规则对每只股票每个交易日的单独得分。',
        formula: '字段：rule_name, ts_code, trade_date, rule_score',
        interpretation: '用于查看某只股票"为什么得分高"——哪些规则贡献了正分，哪些拖了后腿。',
      },
      {
        heading: 'scene_details 表',
        description: '每个场景对每只股票每个交易日的状态和指标。',
        formula: '字段：scene_name, direction, stage, stage_score, risk_score, confirm_strength, risk_intensity, scene_rank',
        interpretation: 'stage 排序 confirm > trigger > observe > fail；scene_rank 是该场景内排名；confirm_strength > 1 表示超过确认阈值。',
      },
      {
        heading: '回测结果表（rule_backtest / scene_backtest / rank_layer）',
        description: '规则层 / 场景层 / 排名分层的回测统计结果。',
        formula: '规则层：IC, ICIR, t_value, avg_residual_mean, avg_excess_residual_mean, profit_loss_ratio, avg_er_change\n场景层：spread_mean, ic_mean, icir, ic_t_value\n分层：每层 avg_residual, top_bottom_spread',
        interpretation: '看策略是否有效，先看 IC 是否 > 0 且 t 是否显著，再看残差均值是否为正，最后看分层是否单调。',
      },
    ],
  },
]

type AlgorithmGuideModalProps = {
  open: boolean
  onClose: () => void
}

export default function AlgorithmGuideModal({
  open,
  onClose,
}: AlgorithmGuideModalProps) {
  useEffect(() => {
    if (!open) {
      return
    }
    function onKeyDown(event: KeyboardEvent) {
      if (event.key === 'Escape') {
        onClose()
      }
    }
    window.addEventListener('keydown', onKeyDown)
    return () => window.removeEventListener('keydown', onKeyDown)
  }, [onClose, open])

  if (!open) {
    return null
  }

  return (
    <div
      className="algorithm-guide-backdrop"
      role="presentation"
      onClick={(event) => {
        if (event.target === event.currentTarget) {
          onClose()
        }
      }}
    >
      <div className="algorithm-guide-modal" role="dialog" aria-modal="true">
        <div className="algorithm-guide-head">
          <div>
            <h3>算法说明</h3>
            <p>
              以下解释程序中用户可接触到的核心算法及其数据解读方式。
            </p>
          </div>
          <button type="button" className="settings-secondary-btn" onClick={onClose}>
            关闭
          </button>
        </div>

        {ALGORITHM_SECTIONS.map((section) => (
          <section key={section.title} className="algorithm-guide-section">
            <h4>{section.title}</h4>
            <div className="algorithm-guide-items">
              {section.items.map((item) => (
                <div key={item.heading} className="algorithm-guide-item">
                  <h5>{item.heading}</h5>
                  <p>{item.description}</p>
                  {item.formula ? (
                    <pre className="algorithm-guide-code">{item.formula}</pre>
                  ) : null}
                  {item.interpretation ? (
                    <div className="algorithm-guide-interpretation">
                      <strong>数据解读：</strong>
                      <span>{item.interpretation}</span>
                    </div>
                  ) : null}
                </div>
              ))}
            </div>
          </section>
        ))}
      </div>
    </div>
  )
}
