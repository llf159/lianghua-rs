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
      {
        heading: 'H30-L50 卷积排名',
        description: '对一只股票最近 30 个完整评分交易日做时间加权：一半权重关注最近 3 日，一半权重取 30 日等权均值，再按平滑后的分数重新排名。',
        formula: 'fast_3d = normalize([1, 0.7, 0.49])\nconvolution_score = 50% × fast_3d_score + 50% × mean(score[t-29…t])\nrank_change = raw_rank - convolution_rank',
        interpretation: 'rank_change > 0 表示平滑后名次上升。该榜偏好近期仍强、且中期评分稳定的股票；它是固定核的时间平滑，不是神经网络预测，也不等于“走势相似排名”。',
      },
    ],
  },
  {
    title: '2. 定制相似算法',
    items: [
      {
        heading: '先区分三种“相似”',
        description: '程序中的相似分为三个独立用途：策略相似度检查比较规则是否经常共同触发；相似股票比较同一天的静态业务标签；走势相似比较当前窗口与历史事件窗口。三者的数据源、分母和分数范围不同，不能横向比较。',
        formula: '策略相似度检查：同股 + 同日的触发集合\n相似股票：概念 + 行业 + trigger/confirm 场景\n走势相似：策略触发 + 量价 + 指标 + 市场环境的历史指纹',
        interpretation: '这些分数表示“按本算法定义有多像”，不是上涨概率，也不是收益预测的置信概率。',
      },
      {
        heading: '策略相似度检查：重叠率与 Lift',
        description: '表达式验证时，把当前待验证组合和每条现有策略在验证区间内的触发样本按“股票代码 + 交易日”求交集。结果按同时触发样本数降序展示，并排除当前导入策略自身。',
        formula: 'A = 当前组合触发样本，B = 现有策略触发样本，N = 区间内总评分样本\n同时触发 = |A ∩ B|\n占当前组合 = |A ∩ B| / |A|\n占现有策略 = |A ∩ B| / |B|\nLift = |A ∩ B| × N / (|A| × |B|)',
        interpretation: 'Lift = 1 表示共同触发程度接近独立随机基线；> 1 表示正关联；< 1 表示共同触发偏少。重叠率高但 Lift 接近 1，可能只是现有策略本身触发很频繁。Lift 只描述共现，不证明两条策略逻辑等价或存在因果关系。',
      },
      {
        heading: '相似股票：同日标签加权匹配',
        description: '候选股只要与目标股共享至少一个行业、概念或有效场景即可进入候选池。概念和场景按“目标股被覆盖的比例”计分，行业完全相同则得固定分；场景仅使用 trigger / confirm 状态。',
        formula: '概念分 = 40 × 共同概念数 / 目标股概念数\n行业分 = 同行业 ? 40 : 0\n场景分 = 30 × 共同场景数 / 目标股 trigger/confirm 场景数\nsimilarity_score = 概念分 + 行业分 + 场景分',
        interpretation: '满信息理论上限为 110 分，不是百分制。目标股缺少某一类信息时，该类不参与可用满分，但结果不会把剩余维度重新归一化；应结合 available_score 和各分项解读。并列时依次比较场景分、概念分、行业分、原总榜排名和股票代码。',
      },
      {
        heading: '走势相似：历史候选与防前视',
        description: '以目标股票参考日之前的一段交易日作为目标窗口。历史候选必须至少命中过目标窗口中的一种策略规则，并且候选结束日不晚于“参考日减去未来评价天数”，确保候选的后验收益在参考日当时已经完整可见。单股查询还会排除目标股票自身的历史窗口。',
        formula: 'target_window = [t-window+1, t]\nhistorical_cutoff = t 向前 outcome_trade_days 个交易日\n候选锚点：结束日 ≤ historical_cutoff 且至少共享一种触发规则',
        interpretation: '候选全集过大时最多保留 50,000 个锚点：40,000 个近期样本 + 10,000 个确定性哈希分散样本。candidate_pool_truncated 为 true 表示发生截断，因此结果是受控候选池内的近邻，不保证遍历全部历史。',
      },
      {
        heading: '走势相似：策略触发指纹',
        description: '触发指纹同时比较“触发了哪些规则”“同一规则何时、以多大强度触发”和“整个窗口的触发数量/得分节奏”。历史上越少见的规则权重越大，额外规则会通过加权 Jaccard 的并集受到惩罚。',
        formula: '规则权重 idf = clamp(ln(1 + 全部触发数 / 该规则触发数), 1, 6)\n规则集合相似 = Σidf(交集) / Σidf(并集)\n单次匹配 = exp(-|日期位置差| / 3) × clamp(1 - |分数差| / (|分数A|+|分数B|), 0, 1)\n触发相似 = 100 × (45% × 规则集合 + 35% × 时序匹配 + 20% × 聚合节奏)',
        interpretation: '同名规则在相近窗口位置、以相近正负方向和强度触发时得分更高。时序匹配保持先后顺序且每次触发最多匹配一次；聚合节奏比较每日触发总数与总得分的形状。',
      },
      {
        heading: '走势相似：量价、指标与市场环境指纹',
        description: '每条数值序列先按窗口分段池化，再提取均匀、短期指数、中期指数、近期增强、前中/中后/前后趋势和拐点共 8 类响应。量价还包含收益、振幅、收盘位置、量额、换手、资金流、跳空、上下影线、市值与板块类别；行情库中的其他数值列作为指标通道；全市场横截面汇总形成市场环境通道。',
        formula: '通道相似 = mean(50 × (1 + cosine(目标通道, 候选通道)))\n最终相似 = 35% × 触发 + 30% × 量价 + 15% × 指标 + 20% × 市场环境',
        interpretation: '余弦值会从 [-1, 1] 映射到 [0, 100]，所以 50 分表示近似正交/无明显同向，不能简单理解为“一半相似”。某通道无有效数据时会跳过，并按仍可用通道的权重和重新归一化；缺失不会自动记 0 分。',
      },
      {
        heading: '历史后验汇总与有效样本数',
        description: '相似历史按 similarity² 加权，最多选择 30 个评级样本；同一股票的邻近窗口会去重，同一后验区间附近最多保留 3 个样本，降低重叠行情造成的伪样本量。未来收益从候选结束后的第一个交易日开盘买入，到评价窗口末日收盘卖出。',
        formula: 'wᵢ = (similarityᵢ / 100)²\n加权均值 = Σ(wᵢ × outcomeᵢ) / Σwᵢ\n有效样本数 N_eff = (Σwᵢ)² / Σ(wᵢ²)\n收缩超额 = 加权超额 × N_eff / (N_eff + 8)',
        interpretation: 'N_eff 越接近样本数，说明权重越均匀；明显更小说明结论被少数高相似样本主导。收缩超额会在样本少时主动靠近 0。MFE/MAE 分别是持有期最大有利/不利变动；历史后验仍是条件统计，不保证未来复现。',
      },
      {
        heading: '走势相似排行榜：模板质量、预测信号与排行分',
        description: '全市场榜先在每个历史截面把未来超额、MFE、MAE 和上涨持续性转换为横截面百分位，合成模板质量；只保留首次跨入成功区（≥ 0.80）或失败区（≤ 0.20）且窗口内有策略触发的模板。当前股票分别检索最相似的成功/失败模板并加权预测质量。',
        formula: '模板质量 = 45% × 超额百分位 + 25% × MFE百分位 + 20% × MAE百分位 + 10% × 上涨持续性百分位\nconfidence = √(N_eff / (N_eff + 8))\nprediction_signal = 2 × (预测质量 - 0.5) × confidence\nranking_score = 按 prediction_signal 排名后的 0~100 线性名次分',
        interpretation: '至少需要 5 个样本且 N_eff ≥ 3 才产生 prediction_signal。信号范围约为 [-1, 1]：正值偏向成功模板，负值偏向失败模板，绝对值同时受样本置信度压缩。ranking_score 只是榜内相对名次刻度，不是相似度、收益率或成功概率。MAE 百分位按数值从小到大排列，因此“跌得没那么深”的样本排名更高。',
      },
    ],
  },
  {
    title: '3. 回测指标算法',
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
        interpretation: '绝对值越大表示 IC 相对自身波动越稳定，正负号表示预测方向。常见经验阈值只能作筛选参考；ICIR > 1 不等同于已经证明可稳定盈利，还需结合样本量、交易成本和样本外检验。',
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
    title: '4. 股票遴选算法',
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
    title: '5. 实时监控算法',
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
    title: '6. 关键数据表解读',
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
              以下口径与当前生产实现保持一致，重点说明分数如何得到、适合比较什么，以及不应如何解读。
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
