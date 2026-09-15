import "./css/StrategyDimensionResearchPage.css";

const STYLE_DIMENSIONS = [
  {
    title: "方向反应",
    poles: "趋势延续 ↔ 均值回归",
    description: "信号是在已有方向上继续下注，还是等待价格偏离后回归。",
    observable: "前序收益方向、信号后收益方向、反转窗口、趋势持续率",
    misunderstanding: "使用均线不必然等于趋势策略，关键是触发后实际承担的方向暴露。",
  },
  {
    title: "入场形态",
    poles: "突破追随 ↔ 回撤 / 反转入场",
    description: "信号发生在价格越过边界时，还是发生在趋势中的回撤或极值反转处。",
    observable: "滚动高低点距离、突破幅度、回撤深度、形态确认耗时",
    misunderstanding: "突破和趋势并非同义；突破也可能服务于短期反转或失败形态。",
  },
  {
    title: "时间尺度",
    poles: "短周期 ↔ 中长周期",
    description: "描述形态形成、信号衰减和计划持有收益兑现所处的时间尺度。",
    observable: "回看窗口、信号半衰期、持有天数、换手率、领先滞后峰值",
    misunderstanding: "指标参数短不代表收益周期短，应以样本外收益衰减和持仓行为为准。",
  },
  {
    title: "价格位置",
    poles: "低位 / 区间内 ↔ 高位 / 区间外",
    description: "描述触发时价格处于自身历史区间、趋势带和近期成本区域的相对位置。",
    observable: "滚动区间分位、距均线或中枢幅度、创新高低比例、累计涨跌幅",
    misunderstanding: "绝对价格高低没有可比性，位置必须在同股、同窗口内标准化。",
  },
  {
    title: "波动与跳跃",
    poles: "平稳低波 ↔ 高波动 / 跳跃事件",
    description: "描述策略偏好平滑演化，还是依赖缺口、急涨跌和波动扩张等离散事件。",
    observable: "实现波动率、ATR、隔夜缺口、极端收益占比、波动率变化",
    misunderstanding: "高收益波动不等于高信号波动，应分别看入场环境和持有期风险。",
  },
  {
    title: "量能与流动性",
    poles: "缩量低关注 ↔ 放量高参与",
    description: "描述信号对成交扩张、市场关注度、可交易容量和拥挤程度的偏好。",
    observable: "量比、换手率、成交额分位、冲击成本代理、放缩量持续时间",
    misunderstanding: "放量既可能确认趋势也可能代表派发，必须结合价格方向与后续结果解释。",
  },
  {
    title: "市场状态依赖",
    poles: "跨状态稳健 ↔ 集中依赖特定状态",
    description: "描述策略是否只在上涨、下跌、趋势或震荡等特定环境中有效。",
    observable: "分状态触发率、收益与 IC、状态间离散度、最弱状态表现",
    misunderstanding: "全样本稳定可能只是主导状态样本较多，不能替代分状态检验。",
  },
  {
    title: "收益形态",
    poles: "高胜率小盈亏 ↔ 低胜率右尾收益",
    description: "描述策略依靠频繁小收益，还是依靠少数大收益，以及对应的尾部风险。",
    observable: "胜率、盈亏比、偏度、尾部损失、最大回撤、回撤修复时间",
    misunderstanding: "相同期望收益可能拥有完全不同的资金曲线和组合分散价值。",
  },
] as const;

const CORRELATION_LAYERS = [
  {
    index: "01",
    title: "信号层",
    question: "是否在相同股票、相同日期做出相近判断？",
    methods: "二元触发看 Jaccard、重叠率与 Phi；连续分数看秩相关；另查领先滞后。",
    caution: "共同不触发的样本很多时，普通一致率会虚高。",
  },
  {
    index: "02",
    title: "风格层",
    question: "是否以不同规则承担了相似的市场行为暴露？",
    methods: "比较标准化八维画像，并逐维报告方向、强度、稳定性和置信范围。",
    caution: "不把八维压成一个总分，否则距离相同的策略可能来源完全不同。",
  },
  {
    index: "03",
    title: "收益层",
    question: "进入组合后，收益路径与下行风险是否仍然同步？",
    methods: "并列观察 Pearson、秩相关、距离相关或 HSIC，以及状态条件和下行相关。",
    caution: "低收益相关可能仅由频率错配、错位持有期或样本缺失造成。",
  },
] as const;

const METHOD_LADDER = [
  {
    name: "Pearson",
    scope: "线性联动",
    meaning: "衡量两个标准化序列能否由一条直线解释。适合收益与连续暴露的基础诊断。",
    boundary: "对异常值敏感，也无法识别 U 形、阈值形等非单调依赖。",
  },
  {
    name: "Spearman / Kendall",
    scope: "单调非线性",
    meaning: "比较排序或成对次序，只要求一方随另一方大体单调变化。",
    boundary: "仍可能漏掉非单调关系；大量并列值时需要报告有效样本和并列处理。",
  },
  {
    name: "距离相关 / HSIC",
    scope: "一般统计依赖",
    meaning: "用于发现线性与秩相关看不到的曲线、阈值、分群等依赖结构。",
    boundary: "数值不是经济暴露方向；显著性需用保持时间结构的置换或分块重采样。",
  },
  {
    name: "状态条件与尾部检查",
    scope: "结构性依赖",
    meaning: "分别在趋势、震荡、上涨、下跌与压力区间检查相关是否集中出现。",
    boundary: "切分过细会迅速耗尽样本；状态定义必须只使用当时可见信息。",
  },
] as const;

const ORTHOGONAL_STEPS = [
  {
    title: "统一比较口径",
    text: "对齐股票池、交易日期、信号时点、持有周期和毛净收益，显式保留未触发样本。",
  },
  {
    title: "剥离共同暴露",
    text: "先移除市场收益以及已经定义的量价风格暴露，避免把大盘同步误认为策略同质。",
  },
  {
    title: "线性残差化",
    text: "用已有策略解释候选策略，取不可解释的残差；共线严重时采用正则化回归。",
  },
  {
    title: "非线性残差化",
    text: "按时间切分做交叉拟合，在训练段学习非线性关系，仅在未参与拟合的时间段生成残差。",
  },
  {
    title: "检查剩余依赖",
    text: "对残差继续计算距离相关或 HSIC，并检查领先滞后、市场状态和下行尾部依赖。",
  },
  {
    title: "验证真实增量",
    text: "只有样本外增量收益、增量 IC、回撤改善和跨状态稳定性共同支持，才认为新增了策略维度。",
  },
] as const;

const RESULT_DATABASE_INPUTS = [
  ["策略总览", "score_summary：股票、日期、总分与当日排名"],
  ["规则信号", "rule_details：股票、日期、规则名称与规则分数"],
  ["场景状态", "scene_details：场景、方向、阶段、风险、确认强度与场景排名"],
  ["收益路径", "未来由回测产出并写入或物化到结果库：持仓、周期、换手、成本、毛净收益与基准收益"],
  ["风格画像", "未来由结果库信号和回测结果派生：八维暴露、状态内表现、样本量与估计版本"],
  ["复现信息", "策略配置版本、市场状态版本、回测参数与计算时间"],
] as const;

export default function StrategyDimensionResearchPage() {
  return (
    <main className="dimension-research-page">
      <section className="dimension-research-hero">
        <div className="dimension-research-hero-copy">
          <span className="dimension-research-status">理论阶段 · 暂无实证数据</span>
          <p className="dimension-research-eyebrow">STRATEGY DIMENSION RESEARCH</p>
          <h2>相关性与正交研究</h2>
          <p className="dimension-research-lead">
            K 线策略的原始信息只有价格与成交量，但规则如何组合这些信息，会形成不同的方向、周期、状态与收益暴露。
            因此“量价两类输入”不等于“策略只有两个维度”。
          </p>
        </div>
        <aside className="dimension-research-principle" aria-label="研究结论边界">
          <span>核心边界</span>
          <strong>低线性相关，不代表统计独立</strong>
          <p>风格坐标负责解释，非线性依赖负责排查，样本外增量才负责确认正交价值。</p>
        </aside>
      </section>

      <section className="dimension-research-pipeline" aria-labelledby="research-pipeline-title">
        <div className="dimension-research-section-heading">
          <div>
            <p className="dimension-research-eyebrow">RESEARCH MAP</p>
            <h3 id="research-pipeline-title">从原始信息到可验证的新维度</h3>
          </div>
          <p>先描述策略“在做什么”，再判断它是否真的提供组合增量。</p>
        </div>
        <ol>
          {["量价输入", "交易规则", "风格暴露", "信号相关", "收益相关", "正交增量"].map(
            (item, index) => (
              <li key={item}>
                <span>{String(index + 1).padStart(2, "0")}</span>
                <strong>{item}</strong>
              </li>
            ),
          )}
        </ol>
      </section>

      <section className="dimension-research-section" aria-labelledby="style-dictionary-title">
        <div className="dimension-research-section-heading">
          <div>
            <p className="dimension-research-eyebrow">STYLE DICTIONARY</p>
            <h3 id="style-dictionary-title">八维策略风格字典</h3>
          </div>
          <p>这些是可解释坐标，不预设彼此正交；每一维都需要未来数据重新估计。</p>
        </div>
        <div className="dimension-research-style-grid">
          {STYLE_DIMENSIONS.map((dimension, index) => (
            <article className="dimension-research-style-card" key={dimension.title}>
              <div className="dimension-research-style-card-head">
                <span>{String(index + 1).padStart(2, "0")}</span>
                <h4>{dimension.title}</h4>
              </div>
              <strong className="dimension-research-poles">{dimension.poles}</strong>
              <p>{dimension.description}</p>
              <dl>
                <div>
                  <dt>未来观测</dt>
                  <dd>{dimension.observable}</dd>
                </div>
                <div>
                  <dt>避免误读</dt>
                  <dd>{dimension.misunderstanding}</dd>
                </div>
              </dl>
            </article>
          ))}
        </div>
      </section>

      <section className="dimension-research-section" aria-labelledby="correlation-layers-title">
        <div className="dimension-research-section-heading">
          <div>
            <p className="dimension-research-eyebrow">THREE-LAYER VIEW</p>
            <h3 id="correlation-layers-title">相关性必须分三层回答</h3>
          </div>
          <p>三层结论可以不同，差异本身就是策略结构的重要信息。</p>
        </div>
        <div className="dimension-research-layer-grid">
          {CORRELATION_LAYERS.map((layer) => (
            <article key={layer.title} data-index={layer.index}>
              <span>{layer.index}</span>
              <h4>{layer.title}</h4>
              <strong>{layer.question}</strong>
              <p>{layer.methods}</p>
              <small>{layer.caution}</small>
            </article>
          ))}
        </div>
      </section>

      <section className="dimension-research-details" aria-label="展开研究细节">
        <details>
          <summary>
            <span>
              <strong>非线性相关：稳健指标阶梯</strong>
              <small>为什么不能只看一个相关系数</small>
            </span>
            <i aria-hidden="true" />
          </summary>
          <div className="dimension-research-detail-body">
            <div className="dimension-research-method-grid">
              {METHOD_LADDER.map((method, index) => (
                <article key={method.name}>
                  <div>
                    <span>{index + 1}</span>
                    <small>{method.scope}</small>
                  </div>
                  <h4>{method.name}</h4>
                  <p>{method.meaning}</p>
                  <p className="dimension-research-method-boundary">边界：{method.boundary}</p>
                </article>
              ))}
            </div>
            <div className="dimension-research-formula-grid">
              <div>
                <span>二元信号重叠</span>
                <code>J(A,B) = |A ∩ B| / |A ∪ B|</code>
                <p>只在至少一个策略触发的样本上衡量共同触发，避免大量共同空白抬高一致率。</p>
              </div>
              <div>
                <span>线性正交条件</span>
                <code>Cov(residual, existing) = 0</code>
                <p>它只保证所选样本和表示下的线性不相关，并不自动得到非线性独立。</p>
              </div>
            </div>
          </div>
        </details>

        <details>
          <summary>
            <span>
              <strong>正交研究：从残差到真实增量</strong>
              <small>固定六步流程与结果解释边界</small>
            </span>
            <i aria-hidden="true" />
          </summary>
          <div className="dimension-research-detail-body">
            <ol className="dimension-research-orthogonal-steps">
              {ORTHOGONAL_STEPS.map((step, index) => (
                <li key={step.title}>
                  <span>{String(index + 1).padStart(2, "0")}</span>
                  <div>
                    <strong>{step.title}</strong>
                    <p>{step.text}</p>
                  </div>
                </li>
              ))}
            </ol>
            <div className="dimension-research-callout">
              <strong>报告原则</strong>
              <p>
                原策略、共同暴露和正交残差必须分开呈现。数学残差可能换手很高、无法成交或缺乏稳定经济含义，不能直接命名为新策略。
              </p>
            </div>
          </div>
        </details>

        <details>
          <summary>
            <span>
              <strong>实证纪律：常见失真来源</strong>
              <small>时间序列、样本外与多重检验约束</small>
            </span>
            <i aria-hidden="true" />
          </summary>
          <div className="dimension-research-detail-body dimension-research-discipline-grid">
            <article>
              <h4>保持时间结构</h4>
              <p>收益和重叠持有期存在自相关。显著性使用按时间连续成块的重采样或置换，不能随机打散单日样本。</p>
            </article>
            <article>
              <h4>严格样本外</h4>
              <p>标准化、状态划分、模型拟合和阈值选择都只使用训练期信息；按时间滚动或扩展窗口验证。</p>
            </article>
            <article>
              <h4>避免频率错配</h4>
              <p>不同触发频率和持有期先投影到共同决策时点与收益周期，否则低相关可能只是时间轴没有对齐。</p>
            </article>
            <article>
              <h4>约束多重检验</h4>
              <p>维度、状态、滞后与指标组合越多，偶然显著越常见。预先声明主指标，并校正探索结果。</p>
            </article>
            <article>
              <h4>报告有效样本</h4>
              <p>同时报告共同日期、共同股票、共同触发次数和各状态样本量；小样本结果只作为待验证线索。</p>
            </article>
            <article>
              <h4>稳定优先于单点最优</h4>
              <p>关注滚动窗口、不同市场状态和轻微参数扰动下的结论方向，不用全样本最优值替代稳定性。</p>
            </article>
          </div>
        </details>

        <details>
          <summary>
            <span>
              <strong>未来接入：结果库读取边界</strong>
              <small>结果库是唯一策略数据源，本阶段不创建接口或数据库表</small>
            </span>
            <i aria-hidden="true" />
          </summary>
          <div className="dimension-research-detail-body">
            <div className="dimension-research-contract-table" role="table" aria-label="未来结果库读取边界">
              {RESULT_DATABASE_INPUTS.map(([category, fields]) => (
                <div role="row" key={category}>
                  <strong role="cell">{category}</strong>
                  <span role="cell">{fields}</span>
                </div>
              ))}
            </div>
            <p className="dimension-research-contract-note">
              接入时不再建立平行的策略数据源：已有触发与评分直接读取结果库；缺失的收益路径和风格画像由回测计算后进入结果库，再供本研究读取。页面当前不会连接结果库、调用后端或生成任何统计结论。
            </p>
          </div>
        </details>
      </section>
    </main>
  );
}
