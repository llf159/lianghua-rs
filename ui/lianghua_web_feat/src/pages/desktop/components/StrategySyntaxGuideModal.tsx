import { useEffect } from 'react'
import '../css/StrategySyntaxGuideModal.css'

type SyntaxGuideFunction = {
  name: string
  signature: string
  returns: string
  description: string
  example: string
}

type SyntaxGuideField = {
  name: string
  scope: string
  description: string
  example: string
}

type SyntaxGuideFieldSection = {
  title: string
  note: string
  fields: SyntaxGuideField[]
}

type SyntaxGuideDynamicFunction = {
  group: string
  functions: string
  signature: string
  meaning: string
}

const SYNTAX_GUIDE_FUNCTIONS: SyntaxGuideFunction[] = [
  { name: 'ABS', signature: 'ABS(x)', returns: '数值序列', description: '取绝对值。', example: '输入 [-2, 3] -> 输出 [2, 3]' },
  { name: 'MAX', signature: 'MAX(a, b)', returns: '数值序列', description: '逐项取较大值。', example: 'a=[1,5], b=[2,3] -> [2,5]' },
  { name: 'MIN', signature: 'MIN(a, b)', returns: '数值序列', description: '逐项取较小值。', example: 'a=[1,5], b=[2,3] -> [1,3]' },
  { name: 'DIV', signature: 'DIV(a, b)', returns: '数值序列', description: '安全除法，除数为 0 时返回 0。', example: 'a=[6,5], b=[2,0] -> [3,0]' },
  { name: 'COUNT', signature: 'COUNT(cond, n)', returns: '数值序列', description: '统计最近 n 根里条件成立的次数。', example: 'cond=[真,假,真,真], n=3 -> [1,1,2,2]' },
  { name: 'EXIST', signature: 'EXIST(cond, n)', returns: '布尔序列', description: '判断最近 n 根里条件是否至少成立过一次。', example: 'cond=[假,真,假,假], n=3 -> [假,真,真,真]' },
  { name: 'EXISTD', signature: 'EXISTD(cond, win, max_win)', returns: '布尔序列', description: '动态窗口版 EXIST，每根使用当根 win 判断最近 win 根是否命中过。', example: 'cond=[假,真,假,假], win=[1,1,2,3] -> [假,真,真,真]' },
  { name: 'MA', signature: 'MA(x, n)', returns: '数值序列', description: '简单移动平均。', example: 'x=[1,2,3,4], n=3 -> [空,空,2,3]' },
  { name: 'REF', signature: 'REF(x, n)', returns: '数值序列', description: '取 n 根之前的值。', example: 'x=[10,11,12,13], n=2 -> [空,空,10,11]' },
  { name: 'LAST', signature: 'LAST(x, n)', returns: '标量(数字或布尔)', description: '取倒数第 n+1 根的值；n=0 表示最新值。', example: 'x=[10,11,12,13], n=0 -> 13；x=[假,真,真], n=1 -> 真' },
  { name: 'HHV', signature: 'HHV(x, n)', returns: '数值序列', description: '最近 n 根最高值。', example: 'x=[1,3,2,5], n=3 -> [空,空,3,5]' },
  { name: 'LLV', signature: 'LLV(x, n)', returns: '数值序列', description: '最近 n 根最低值。', example: 'x=[1,3,2,0], n=3 -> [空,空,1,0]' },
  { name: 'SUM', signature: 'SUM(x, n)', returns: '数值序列', description: '最近 n 根求和。', example: 'x=[1,2,3,4], n=3 -> [空,空,6,9]' },
  { name: 'STD', signature: 'STD(x, n)', returns: '数值序列', description: '最近 n 根标准差。', example: 'x=[1,3,3], n=2 -> [空,1,0]' },
  { name: 'IF', signature: 'IF(cond, a, b)', returns: '数值序列', description: '条件成立取 a，否则取 b。', example: 'cond=[真,假,真], a=[1,1,1], b=[0,0,0] -> [1,0,1]' },
  { name: 'CROSS', signature: 'CROSS(a, b)', returns: '布尔序列', description: 'a 当根上穿 b。', example: 'a=[1,2,4], b=[3,2,3] -> [假,假,真]' },
  { name: 'EMA', signature: 'EMA(x, n)', returns: '数值序列', description: '指数移动平均。', example: 'x=[1,2,3], n=3 -> [1,1.5,2.25]' },
  { name: 'SMA', signature: 'SMA(x, n, m)', returns: '数值序列', description: '平滑移动平均，权重约为 m / n。', example: 'x=[1,2,3], n=3, m=1 -> [1,1.33,1.89]' },
  { name: 'BARSLAST', signature: 'BARSLAST(cond)', returns: '数值序列', description: '距离上一次 cond 成立已经过去几根；首次命中前为空。', example: 'cond=[假,真,假,假,真] -> [空,0,1,2,0]' },
  { name: 'RSV', signature: 'RSV(C, H, L, n)', returns: '数值序列', description: '最近 n 根内先取最高价 HHV(H,n) 和最低价 LLV(L,n)，再按 (C-LLV)/(HHV-LLV)*100 逐根计算；分母为 0 时返回 0。', example: 'C=[8,9,10], H=[10,11,12], L=[6,7,8], n=3 -> [空,空,66.67]' },
  { name: 'GRANK', signature: 'GRANK(x, n)', returns: '数值序列', description: '最近 n 根里，当前值按从大到小的排名；1 表示最大。', example: 'x=[1,4,2], n=3 -> [空,空,2]' },
  { name: 'LRANK', signature: 'LRANK(x, n)', returns: '数值序列', description: '最近 n 根里，当前值按从小到大的排名；1 表示最小。', example: 'x=[3,2,1], n=3 -> [空,空,1]' },
  { name: 'GTOPCOUNT', signature: 'GTOPCOUNT(x, cond, win, topn)', returns: '数值序列', description: '最近 win 根按 x 从大到小取前 topn，统计其中 cond 成立的个数。', example: 'x=[1,5,3], cond=[真,假,真], win=3, topn=2 -> [空,空,1]' },
  { name: 'LTOPCOUNT', signature: 'LTOPCOUNT(x, cond, win, topn)', returns: '数值序列', description: '最近 win 根按 x 从小到大取前 topn，统计其中 cond 成立的个数。', example: 'x=[1,5,3], cond=[真,假,真], win=3, topn=2 -> [空,空,2]' },
  { name: 'GET', signature: 'GET(cond, x, n)', returns: '数值序列', description: '向前回看最近 n 根，取最后一次 cond 成立时对应的 x；不包含当前这根。', example: '可写 GET(CROSS(C, MA(C, 5)), C, 20) 取最近一次上穿时的收盘价' },
]

const SYNTAX_GUIDE_DYNAMIC_FUNCTIONS: SyntaxGuideDynamicFunction[] = [
  { group: '动态计数 / 引用', functions: 'COUNTD / EXISTD / REFD', signature: 'COUNTD(cond, win, max_win)；EXISTD(cond, win, max_win)；REFD(x, offset, max_offset)', meaning: 'win/offset 可以是数值序列，例如 GAP；第三个参数是正整数上限，也用于 warmup。' },
  { group: '动态窗口统计', functions: 'HHVD / LLVD / MAD / SUMD / STDD', signature: '函数名(x, win, max_win)', meaning: '每根 K 线用当根 win 回看，分别计算最高、最低、均值、求和、标准差；max_win 必须是正整数。' },
  { group: '动态排名 / 取值', functions: 'GRANKD / LRANKD / GETD', signature: 'GRANKD(x, win, max_win)；GETD(cond, x, win, max_win)', meaning: '排名函数按动态 win 排序；GETD 在动态 win 内找最近一次条件成立对应的 x；max_win 必须是正整数。' },
  { group: '动态扩展指标', functions: 'RSVD / GTOPCOUNTD / LTOPCOUNTD', signature: 'RSVD(C,H,L,win,max_win)；GTOPCOUNTD(x,cond,win,topn,max_win)', meaning: 'RSVD 是动态窗口 RSV；TOPCOUNTD 在动态 win 内排序后取前 topn 统计条件次数；max_win 必须是正整数。' },
]

const SYNTAX_GUIDE_FIELD_SECTIONS: SyntaxGuideFieldSection[] = [
  {
    title: '5. 常用行情字段',
    note: '这些字段来自历史 K 线或实时拼接后的 K 线序列，大部分表达式都可以直接使用。',
    fields: [
      { name: 'C / O / H / L / V', scope: '通用', description: '收盘 / 开盘 / 最高 / 最低 / 成交量。', example: 'C > O AND V > MA(V, 5)' },
      { name: 'AMOUNT', scope: '通用', description: '成交额。', example: 'AMOUNT > MA(AMOUNT, 10)' },
      { name: 'PRE_CLOSE', scope: '通用', description: '昨收价。', example: 'C > PRE_CLOSE' },
      { name: 'CHANGE / PCT_CHG', scope: '通用', description: '涨跌额 / 涨跌幅；其中 PCT_CHG 的单位是百分比。', example: 'PCT_CHG >= 5' },
      { name: 'TOR / TURNOVER_RATE', scope: '通用', description: '换手率；TOR 是常用简写，两者会按数据列互通。', example: 'TOR > 8' },
    ],
  },
  {
    title: '6. 额外常数字段',
    note: '这些字段由后端运行时统一注入，后续新增常量字段也会沿用这套入口。',
    fields: [
      { name: 'ZHANG', scope: '通用', description: '涨停幅比例，例如普通股约 0.095、创业板/科创板约 0.195、北交所约 0.295、ST 约 0.045。', example: 'PCT_CHG >= ZHANG * 100' },
      { name: 'TOTAL_MV_YI', scope: '通用', description: '总市值，单位“亿”；按 stock_list.csv 的 total_share × 当日收盘价 C / 10000 逐日计算。', example: 'TOTAL_MV_YI <= 300' },
    ],
  },
  {
    title: '7. 新筹码库字段',
    note: '这些字段来自 cyq_chen.db 的 cyq_chen_snapshot，会按股票代码和交易日对齐注入；排名计算、表达式选股、统计验证、模拟盘和实时监控都可使用。库、表、列或日期缺失时按空值处理。',
    fields: [
      { name: 'CYQ_TPR / CYQ_TTR', scope: '通用', description: '新筹码整体获利 / 套牢筹码比例。', example: 'CYQ_TPR > 0.6 AND CYQ_TTR < 0.35' },
      { name: 'CYQ_MPR / CYQ_MTR', scope: '通用', description: '新筹码主力获利 / 套牢筹码比例。', example: 'CYQ_MPR > 0.6 AND CYQ_MTR < 0.35' },
      { name: 'CYQ_PEAK', scope: '通用', description: '新筹码总筹码峰值价格。', example: 'C > CYQ_PEAK' },
      { name: 'CYQ_MT / CYQ_RT', scope: '通用', description: '主力 / 散户归一化持仓量。', example: 'CYQ_MT > CYQ_RT' },
      { name: 'CYQ_MIN / CYQ_MAX', scope: '通用', description: '新筹码分布的最低 / 最高价格边界。', example: 'C >= CYQ_MIN AND C <= CYQ_MAX' },
      { name: 'CYQ_P70L / CYQ_P70H / CYQ_P70C', scope: '通用', description: '70% 筹码集中区下沿 / 上沿 / 集中度。', example: 'CYQ_P70C > 0.4' },
      { name: 'CYQ_P90L / CYQ_P90H / CYQ_P90C', scope: '通用', description: '90% 筹码集中区下沿 / 上沿 / 集中度。', example: 'C > CYQ_P90H' },
    ],
  },
  {
    title: '8. 筹码变动策略附加字段',
    note: '这些字段只在 chip_change_rule.toml 的筹码变动策略中可用。RATE 系列按每个价格分桶的成本价动态计算，单位是百分比，30 表示 30%。',
    fields: [
      { name: 'RATEO', scope: '筹码变动策略', description: '当日开盘价相对当前价格分桶成本价的涨跌幅，单位是百分比。', example: 'RATEO > 30' },
      { name: 'RATEH', scope: '筹码变动策略', description: '当日最高价相对当前价格分桶成本价的涨跌幅，单位是百分比。', example: 'RATEH > 30' },
      { name: 'RATEL', scope: '筹码变动策略', description: '当日最低价相对当前价格分桶成本价的涨跌幅，单位是百分比。', example: 'RATEL < -8' },
      { name: 'RATEC', scope: '筹码变动策略', description: '当日收盘价相对当前价格分桶成本价的涨跌幅，单位是百分比。', example: 'RATEC > 10' },
      { name: 'MAIN_CHIP_RATIO', scope: '筹码变动策略', description: '当前价格分桶里主力筹码占该分桶总筹码的比例，取值通常在 0 到 1。', example: 'MAIN_CHIP_RATIO > 0.6' },
      { name: 'MAIN_CHIP_TOTAL / RETAIL_CHIP_TOTAL', scope: '筹码变动策略', description: '当前全分布主力 / 散户筹码量，按比例归一化后两者相加约等于 100。', example: 'MAIN_CHIP_TOTAL > RETAIL_CHIP_TOTAL' },
    ],
  },
  {
    title: '9. 指标列自动注入',
    note: 'DataReader 会把 stock_data 里实际存在的数值列自动转成大写变量；这通常是行情基础列之外的指标列，不等于 stock_list.csv 里的市值字段都会天然出现在这里。',
    fields: [
      { name: '已落库指标列 / 自定义数值列', scope: '按数据源实际情况', description: '只有已经写进 stock_data 的数值列才可直接引用，变量名会自动转成大写。', example: 'MY_IND > MA(MY_IND, 5)' },
    ],
  },
  {
    title: '10. 实时监控模板附加字段',
    note: '下面这些字段只在“实时监控”页面的模板表达式中可用，策略打分、选股或统计表达式里不要直接写。',
    fields: [
      { name: 'RT_OP', scope: '实时监控', description: '当前价相对今开涨跌幅，单位是百分比。', example: 'RT_OP >= 2' },
      { name: 'RT_FH', scope: '实时监控', description: '当前价相对于今日高点的回落幅度，单位是百分比；返回值恒为非负数。', example: 'RT_FH <= 1.5' },
      { name: 'RT_VR', scope: '实时监控', description: '行情源返回的盘中量比；新浪源没有该字段时为空。', example: 'RT_VR >= 2' },
      { name: 'RT_AVG', scope: '实时监控', description: '行情源返回的均价；新浪源没有该字段时为空。', example: 'C > RT_AVG' },
      { name: 'RANK', scope: '实时监控', description: '按当前榜单模式注入的历史排名序列；总榜读取 score_summary.rank，场景榜读取 scene_details.scene_rank；runtime 最新一根固定留空。', example: 'RANK <= 100 AND RT_OP >= 2' },
    ],
  },
  {
    title: '11. 表达式选股 / 模拟盘附加字段',
    note: 'RANK 在表达式选股与模拟盘买点方程中按交易日对齐；模拟盘卖点方程额外注入持仓相关字段。',
    fields: [
      { name: 'RANK', scope: '表达式选股 / 模拟盘买点', description: '个股在 score_summary 中按交易日对齐后的排名序列；1 表示当日排名第一。', example: 'RANK <= 100 AND C > MA(C, 20)' },
      { name: 'TIME', scope: '模拟盘卖点', description: '买入后经过的交易日数，买入当日为 0。', example: 'TIME >= 5' },
      { name: 'RATEO', scope: '模拟盘卖点', description: '当日开盘价相对买入成本的收益率，单位是百分比。', example: 'RATEO <= -3' },
      { name: 'RATEH', scope: '模拟盘卖点', description: '当日最高价相对买入成本的收益率，单位是百分比。', example: 'RATEH >= 8' },
    ],
  },
]

type StrategySyntaxGuideModalProps = {
  open: boolean
  onClose: () => void
}

export default function StrategySyntaxGuideModal({
  open,
  onClose,
}: StrategySyntaxGuideModalProps) {
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
      className="strategy-syntax-guide-backdrop"
      role="presentation"
      onClick={(event) => {
        if (event.target === event.currentTarget) {
          onClose()
        }
      }}
    >
      <div className="strategy-syntax-guide-modal" role="dialog" aria-modal="true">
        <div className="strategy-syntax-guide-head">
          <div>
            <h3>策略语法说明书</h3>
            <p>
              表达式支持多句，最后一句作为最终结果；常见字段可直接写 <code>C / O / H / L / V</code>。
            </p>
          </div>
          <button type="button" className="settings-secondary-btn" onClick={onClose}>
            关闭
          </button>
        </div>

        <section className="strategy-syntax-guide-section">
          <h4>1. 赋值</h4>
          <p>用 <code>:=</code> 给中间变量命名，用 <code>;</code> 分隔多句。</p>
          <pre className="strategy-syntax-guide-code">{`N := 20;
BASE := MA(C, N);
VOL_OK := V > MA(V, 5);
C > BASE AND VOL_OK`}</pre>
        </section>

        <section className="strategy-syntax-guide-section">
          <h4>2. 表达式</h4>
          <div className="strategy-syntax-guide-chip-list">
            <span>算术：+ - * /</span>
            <span>比较：&gt; &gt;= &lt; &lt;= == != IN</span>
            <span>逻辑：AND OR NOT</span>
            <span>分组：(...)</span>
          </div>
          <pre className="strategy-syntax-guide-code">{`C > O AND V > MA(V, 5)
C IN [MA(C, 5), HHV(C, 20))
NOT(CROSS(C, MA(C, 10)))
IF(C > O, C - O, 0)`}</pre>
        </section>

        <section className="strategy-syntax-guide-section">
          <h4>3. 返回结果</h4>
          <p>最后一条语句建议返回布尔序列或数值序列。</p>
          <div className="strategy-syntax-guide-result-grid">
            <div>
              <span>布尔序列例子</span>
              <strong>C &gt; MA(C, 20)</strong>
            </div>
            <div>
              <span>数值序列例子</span>
              <strong>COUNT(C &gt; O, 5)</strong>
            </div>
          </div>
        </section>

        <section className="strategy-syntax-guide-section">
          <h4>4. 支持的函数</h4>
          <div className="strategy-syntax-guide-table-wrap">
            <table className="strategy-syntax-guide-table">
              <thead>
                <tr>
                  <th>函数</th>
                  <th>签名</th>
                  <th>返回</th>
                  <th>作用</th>
                  <th>输入输出例子</th>
                </tr>
              </thead>
              <tbody>
                {SYNTAX_GUIDE_FUNCTIONS.map((item) => (
                  <tr key={item.name}>
                    <td><code>{item.name}</code></td>
                    <td><code>{item.signature}</code></td>
                    <td>{item.returns}</td>
                    <td>{item.description}</td>
                    <td>{item.example}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div className="strategy-syntax-guide-subsection">
            <h5>动态窗口 D 函数</h5>
            <p>
              D 函数用于“窗口/偏移本身也是序列”的场景，例如 <code>GAP := REF(BARSLAST(RED0), 1)</code> 后按每根 K 线自己的 GAP 回看。
              写法会比普通函数多一个正整数上限参数，运行时会把动态窗口截到该上限，查询 warmup 也按这个上限准备历史数据。
            </p>
            <div className="strategy-syntax-guide-table-wrap">
              <table className="strategy-syntax-guide-table">
                <thead>
                  <tr>
                    <th>类型</th>
                    <th>函数</th>
                    <th>新写法</th>
                    <th>含义</th>
                  </tr>
                </thead>
                <tbody>
                  {SYNTAX_GUIDE_DYNAMIC_FUNCTIONS.map((item) => (
                    <tr key={item.group}>
                      <td>{item.group}</td>
                      <td><code>{item.functions}</code></td>
                      <td><code>{item.signature}</code></td>
                      <td>{item.meaning}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <pre className="strategy-syntax-guide-code">{`GAP := REF(BARSLAST(RED0), 1);
MID_OK := GAP > 4 AND COUNTD(REF(MID_STRONG, 1), GAP, 60) <= 1;
PRE_H := REFD(H, GAP + 1, 60);`}</pre>
          </div>
        </section>

        {SYNTAX_GUIDE_FIELD_SECTIONS.map((section) => (
          <section key={section.title} className="strategy-syntax-guide-section">
            <h4>{section.title}</h4>
            <p>{section.note}</p>
            <div className="strategy-syntax-guide-table-wrap">
              <table className="strategy-syntax-guide-table">
                <thead>
                  <tr>
                    <th>字段</th>
                    <th>范围</th>
                    <th>作用</th>
                    <th>例子</th>
                  </tr>
                </thead>
                <tbody>
                  {section.fields.map((item) => (
                    <tr key={`${section.title}-${item.name}`}>
                      <td><code>{item.name}</code></td>
                      <td>{item.scope}</td>
                      <td>{item.description}</td>
                      <td>{item.example}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>
        ))}
      </div>
    </div>
  )
}
