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
        formula: '通道相似 = mean(50 × (1 + cosine(目标通道, 候选通道)))\n最终相似 = 40% × 触发 + 25% × 市场环境 + 20% × 量价 + 15% × 指标',
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
        interpretation: 'N_eff ≥ 8（达到收缩先验强度）才产生 prediction_signal；由于 N_eff 不会超过原始样本数，这也同时要求至少 8 个评级样本。信号范围约为 [-1, 1]：正值偏向成功模板，负值偏向失败模板，绝对值同时受样本置信度压缩。ranking_score 只是榜内相对名次刻度，不是相似度、收益率或成功概率。MAE 百分位按数值从小到大排列，因此“跌得没那么深”的样本排名更高。',
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
        interpretation: '正值表示跑赢基准，负值表示跑输基准。形态在评分日收盘后确认，收益从下一交易日开盘开始，到持有期末收盘结束；各收益腿先复合，再按配置 Beta 扣减。',
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
        description: '检验 IC 均值是否显著不为零，并修正多日持有造成的相邻前瞻收益重叠。',
        formula: 't = IC_mean / Newey-West_HAC_SE，滞后阶数 = 持有期 - 1',
        interpretation: '|t| > 2 可作约 95% 显著性的初步参考。HAC 会计入异方差和相邻日期相关性，但仍需结合样本外与多重检验。',
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
        heading: '样本利润因子（Profit Factor）',
        description: '所有触发股票样本中，正残差总和与负残差绝对值总和的比值；它不是按盈利日统计的盈亏比。',
        formula: 'profit_loss_ratio = Σ(正残差) / Σ(|负残差|)',
        interpretation: '> 1 表示样本盈利总额大于亏损总额。全部策略汇总则先形成等权策略日度组合，再从组合日收益重新计算，避免平均各规则比率。',
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
    title: '6. 相关性与正交研究',
    items: [
      {
        heading: '共同触发：Jaccard / Phi',
        description: 'Jaccard 只看至少一方触发的集合；Phi 把共同不触发也纳入二元关联。覆盖率悬殊时，两者出现差异是正常现象。',
        formula: 'Jaccard = |A ∩ B| / |A ∪ B|\nPhi = 二元触发变量的相关系数',
        interpretation: '它们回答的是规则是否共同选中，不回答分数方向，也不能单独证明策略逻辑等价。',
      },
      {
        heading: '线性关系：Pearson',
        description: '在完整股票日宇宙上计算规则分数的线性相关，未触发规则按 0 分进入统计。',
        formula: 'Pearson = Corr(score_A_with_zeros, score_B_with_zeros)',
        interpretation: '接近 0 只表示直线关系弱，不能推出两个策略独立。',
      },
      {
        heading: '日度单调关系：Spearman',
        description: '先按交易日聚合每条规则的横截面平均分，再比较交易日之间的排序关系。',
        formula: 'daily_mean_score = 当日规则总分 / 当日股票数\nSpearman = Corr(rank(daily_mean_A), rank(daily_mean_B))',
        interpretation: '用于观察两条策略是否随市场环境共同增强或减弱，不要求分数之间存在线性比例。',
      },
      {
        heading: '一般依赖：距离相关',
        description: '距离相关可以捕捉 U 形、阈值和分群关系。受 O(n²) 成本限制，页面使用确定性日期抽样。',
        formula: 'distance_correlation ∈ [0, 1]',
        interpretation: '它能判断一般依赖强弱，但不提供方向，也不是显著性概率。',
      },
      {
        heading: '正交残差：核心策略组是研究基准',
        description: '页面可选择一个或多个代表性核心策略，系统将核心组放在内部序列前面；第 N 条规则仍由前 N−1 条规则解释。',
        formula: 'score_N = 前序规则线性拟合值 + residual_N\nresidual_variance_ratio = Var(residual_N) / Var(score_N)',
        interpretation: '核心组用于对齐已有代表能力，非核心规则用于寻找新增信息。残差比例越高，当前规则在线性层面留下的增量越多；组内使用稳定顺序。',
      },
      {
        heading: '可交易收益路径与收益相关',
        description: '对每条规则按评分日触发股票形成日度等权组合，使用次日开盘到持有期末收盘的残差收益，再比较策略日收益的 Pearson 相关。',
        formula: 'residual_return = stock_return - 0.5 × index_return - 0.2 × concept_return\nreturn_correlation = Corr(规则 A 日度残差收益, 规则 B 日度残差收益)',
        interpretation: '收益相关回答实际赚亏是否同步。它可能与触发、分数相关明显不同；只有两条策略都有有效收益的共同日期才进入计算。',
      },
      {
        heading: '相对核心组的样本外收益增量',
        description: '按时间将有效日期前 70% 作为训练期，只在训练期拟合候选规则对核心组及此前规则的岭回归系数；后 30% 固定系数并检验候选的未解释收益。',
        formula: 'incremental_return_test = candidate_return - Σ(训练期系数ᵢ × prior_returnᵢ)\nHAC lag = holding_period - 1',
        interpretation: '核心策略选择决定主要基准集合。正的样本外未解释收益表示候选在这段检验期提供了已有规则未覆盖的收益，但仍需结合 HAC t 值、检验样本数和多次研究造成的数据窥探。',
      },
      {
        heading: '结论边界',
        description: '研究页同时检查信号结构、风格暴露和毛收益路径，但尚未扣除换手、冲击成本，也不生成容量结论。',
        interpretation: '线性残差高仍可能存在强距离相关；收益增量也只有在样本外稳定、计入交易成本后仍成立，才可称为可交易的独立策略维度。',
      },
    ],
  },
  {
    title: '7. 策略风格维度',
    items: [
      {
        heading: '方向反应',
        description: '用触发日相对 5 个交易日前的收益，判断策略更常在近期强势还是弱势股票中触发。',
        formula: 'direction = 当日横截面百分位( close[t] / close[t-5] - 1 ) × 2 - 1',
        interpretation: '接近 1 表示偏近期上涨，接近 -1 表示偏近期下跌；这是入场前状态，不等同于触发后的趋势或反转收益。',
      },
      {
        heading: '入场形态',
        description: '用收盘价相对前 20 个交易日最高价的位置，观察策略偏向突破追随还是高点下方的回撤入场。',
        formula: 'entry = 当日横截面百分位( close[t] / max(high[t-20..t-1]) - 1 ) × 2 - 1',
        interpretation: '接近 1 表示更靠近或越过前高，接近 -1 表示离前高较远。',
      },
      {
        heading: '时间尺度',
        description: '用同一股票的规则触发是否在下一评分交易日继续出现，衡量信号持续性。',
        formula: 'time_scale = 2 × 连续触发比例 - 1',
        interpretation: '接近 1 表示触发更连续、偏慢变量；接近 -1 表示触发离散、偏短脉冲。',
      },
      {
        heading: '价格位置',
        description: '计算收盘价在近 60 个交易日最高最低区间的位置，再做当日横截面百分位。',
        formula: 'position = percentile( (close - low_60) / (high_60 - low_60) ) × 2 - 1',
        interpretation: '接近 1 表示偏 60 日高位，接近 -1 表示偏低位。',
      },
      {
        heading: '波动与跳跃',
        description: '比较触发日绝对涨跌幅与此前 20 日平均绝对涨跌幅，并做横截面百分位。',
        formula: 'volatility = percentile( |return[t]| / mean(|return[t-20..t-1]|) ) × 2 - 1',
        interpretation: '接近 1 表示策略偏好波动突然放大的股票，接近 -1 表示偏平稳状态。',
      },
      {
        heading: '量能与流动性',
        description: '用触发股票成交额对数的当日横截面百分位描述可交易流动性。',
        formula: 'liquidity = percentile( ln(amount) ) × 2 - 1',
        interpretation: '接近 1 表示偏高成交额股票，接近 -1 表示偏低流动性；它是流动性代理，不是实际冲击成本。',
      },
      {
        heading: '市场状态依赖',
        description: '比较每日规则触发覆盖率与沪深 300 当日涨跌幅的相关性。',
        formula: 'market_regime = Corr(每日触发数 / 每日评分股票数, 沪深300日收益)',
        interpretation: '正值表示上涨日更活跃，负值表示下跌日更活跃；接近 0 只表示线性依赖较弱。',
      },
      {
        heading: '收益形态',
        description: '使用日度等权持仓残差收益的正负总额平衡，描述收益由正侧还是负侧主导。',
        formula: 'return_shape = (Σ正收益 - Σ|负收益|) / (Σ正收益 + Σ|负收益|)',
        interpretation: '接近 1 表示正残差总额占优，接近 -1 表示负残差总额占优；它不区分高胜率小盈和低胜率右尾。',
      },
    ],
  },
  {
    title: '8. 关键数据表解读',
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
