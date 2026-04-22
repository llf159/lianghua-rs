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

const SYNTAX_GUIDE_FUNCTIONS: SyntaxGuideFunction[] = [
  { name: 'ABS', signature: 'ABS(x)', returns: '数值序列', description: '取绝对值。', example: '输入 [-2, 3] -> 输出 [2, 3]' },
  { name: 'MAX', signature: 'MAX(a, b)', returns: '数值序列', description: '逐项取较大值。', example: 'a=[1,5], b=[2,3] -> [2,5]' },
  { name: 'MIN', signature: 'MIN(a, b)', returns: '数值序列', description: '逐项取较小值。', example: 'a=[1,5], b=[2,3] -> [1,3]' },
  { name: 'DIV', signature: 'DIV(a, b)', returns: '数值序列', description: '安全除法，除数为 0 时返回 0。', example: 'a=[6,5], b=[2,0] -> [3,0]' },
  { name: 'COUNT', signature: 'COUNT(cond, n)', returns: '数值序列', description: '统计最近 n 根里条件成立的次数。', example: 'cond=[真,假,真,真], n=3 -> [1,1,2,2]' },
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
  { name: 'RSV', signature: 'RSV(C, H, L, n)', returns: '数值序列', description: '最近 n 根 RSV，常用于 KDJ。', example: 'C=[8,9,10], H=[10,11,12], L=[6,7,8], n=3 -> [空,空,66.67]' },
  { name: 'GRANK', signature: 'GRANK(x, n)', returns: '数值序列', description: '最近 n 根里，当前值按从大到小的排名；1 表示最大。', example: 'x=[1,4,2], n=3 -> [空,空,2]' },
  { name: 'LRANK', signature: 'LRANK(x, n)', returns: '数值序列', description: '最近 n 根里，当前值按从小到大的排名；1 表示最小。', example: 'x=[3,2,1], n=3 -> [空,空,1]' },
  { name: 'GTOPCOUNT', signature: 'GTOPCOUNT(x, cond, win, topn)', returns: '数值序列', description: '最近 win 根按 x 从大到小取前 topn，统计其中 cond 成立的个数。', example: 'x=[1,5,3], cond=[真,假,真], win=3, topn=2 -> [空,空,1]' },
  { name: 'LTOPCOUNT', signature: 'LTOPCOUNT(x, cond, win, topn)', returns: '数值序列', description: '最近 win 根按 x 从小到大取前 topn，统计其中 cond 成立的个数。', example: 'x=[1,5,3], cond=[真,假,真], win=3, topn=2 -> [空,空,2]' },
  { name: 'GET', signature: 'GET(cond, x, n)', returns: '数值序列', description: '向前回看最近 n 根，取最后一次 cond 成立时对应的 x；不包含当前这根。', example: '可写 GET(CROSS(C, MA(C, 5)), C, 20) 取最近一次上穿时的收盘价' },
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
      { name: 'TURNOVER_RATE', scope: '通用', description: '换手率。', example: 'TURNOVER_RATE > 8' },
    ],
  },
  {
    title: '6. 额外常数字段',
    note: '这些字段由后端运行时统一注入，后续新增常量字段也会沿用这套入口。',
    fields: [
      { name: 'ZHANG', scope: '通用', description: '涨停幅比例，例如普通股约 0.095、创业板/科创板约 0.195、北交所约 0.295、ST 约 0.045。', example: 'PCT_CHG >= ZHANG * 100' },
      { name: 'TOTAL_MV_YI', scope: '通用', description: '总市值，单位“亿”；优先由历史 TOTAL_MV 列换算得到。', example: 'TOTAL_MV_YI <= 300' },
    ],
  },
  {
    title: '7. 指标列自动注入',
    note: 'DataReader 会把 stock_data 里实际存在的数值列自动转成大写变量；这通常是行情基础列之外的指标列，不等于 stock_list.csv 里的市值字段都会天然出现在这里。',
    fields: [
      { name: '已落库指标列 / 自定义数值列', scope: '按数据源实际情况', description: '只有已经写进 stock_data 的数值列才可直接引用，变量名会自动转成大写。', example: 'MY_IND > MA(MY_IND, 5)' },
    ],
  },
  {
    title: '8. 实时监控模板附加字段',
    note: '下面这些字段只在“实时监控”页面的模板表达式中可用，策略打分、选股或统计表达式里不要直接写。',
    fields: [
      { name: 'REALTIME_CHANGE_OPEN_PCT', scope: '实时监控', description: '当前价相对今开涨跌幅，单位是百分比。', example: 'REALTIME_CHANGE_OPEN_PCT >= 2' },
      { name: 'REALTIME_FALL_FROM_HIGH_PCT', scope: '实时监控', description: '当前价相对于今日高点的回落幅度，单位是百分比；返回值恒为非负数，0 表示当前价等于今日高点，不会返回负数；计算口径为 max((今日高点 - 当前价) / 今日高点, 0) × 100%。', example: 'REALTIME_FALL_FROM_HIGH_PCT <= 1.5' },
      { name: 'REALTIME_VOL_RATIO', scope: '实时监控', description: '当前实时累计成交量 ÷ stock_data 中最新历史日的 vol，通常可理解为“相对上一交易日日成交量”的倍数。', example: 'REALTIME_VOL_RATIO >= 2' },
      { name: 'VOL_RATIO', scope: '实时监控', description: 'REALTIME_VOL_RATIO 的别名，基准相同。', example: 'VOL_RATIO >= 2' },
      { name: 'rank / RANK', scope: '实时监控', description: '按当前榜单模式注入的历史排名序列；总榜读取 score_summary.rank，场景榜读取 scene_details.scene_rank；runtime 最新一根固定留空。', example: 'rank <= 100 AND REALTIME_CHANGE_OPEN_PCT >= 2' },
    ],
  },
  {
    title: '9. 表达式选股 / 模拟盘附加字段',
    note: 'rank / RANK 在表达式选股与模拟盘买点方程中按交易日对齐；模拟盘卖点方程额外注入持仓相关字段。',
    fields: [
      { name: 'rank / RANK', scope: '表达式选股 / 模拟盘买点', description: '个股在 score_summary 中按交易日对齐后的排名序列；1 表示当日排名第一。', example: 'RANK <= 100 AND C > MA(C, 20)' },
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
            <span>比较：&gt; &gt;= &lt; &lt;= == !=</span>
            <span>逻辑：AND OR NOT</span>
            <span>分组：(...)</span>
          </div>
          <pre className="strategy-syntax-guide-code">{`C > O AND V > MA(V, 5)
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
