import { useEffect, useRef, useState } from "react";
import "../css/AlgorithmGuideModal.css";

type AlgorithmSectionId = "score" | "similarity" | "chip" | "backtest";

type AlgorithmSection = {
  id: AlgorithmSectionId;
  number: string;
  title: string;
  navHint: string;
  summary: string;
  inputs: string[];
  outputs: string[];
  steps: {
    heading: string;
    description: string;
  }[];
  designs: {
    heading: string;
    description: string;
    formula?: string;
  }[];
  reading: string[];
  boundary: string;
};

const ALGORITHM_SECTIONS: AlgorithmSection[] = [
  {
    id: "score",
    number: "01",
    title: "打分计算",
    navHint: "当日评分、场景与后续跟踪",
    summary:
      "打分计算回答：在某个交易日，一只股票符合当前策略编排的程度有多高；对于已经捕捉到的形态，后续跟踪还会继续观察它是否按预期推进。",
    inputs: [
      "个股行情与日期",
      "策略规则和分值",
      "指标、资金流、筹码等表达式字段",
      "场景方向与阈值",
      "后续跟踪配置",
    ],
    outputs: [
      "total_score",
      "每条规则及跟踪得分",
      "跟踪事件状态",
      "场景阶段与强度",
      "当日排名",
    ],
    steps: [
      {
        heading: "对齐日期",
        description:
          "按“股票 × 交易日”准备数据，所有表达式只使用参考日及以前可见的信息，保证同一天可以公平比较。",
      },
      {
        heading: "判断规则",
        description:
          "逐条计算规则表达式。比较所需数据缺失时结果为“未知”；只要统计窗口含未知值，本次规则就不计分、不触发。",
      },
      {
        heading: "汇总窗口",
        description:
          "LAST 看当天，ANY 看窗口内是否发生过，EACH 按窗口内命中次数累加，RECENT 取最近一次命中距离，CONSEC 要求连续命中达到阈值。",
      },
      {
        heading: "形成总分",
        description:
          "单条规则按命中方式得到正分或负分。组合规则先按命中条件数取基础分，再叠加条件 bonus，并可受单条规则上限约束。",
      },
      {
        heading: "推进跟踪与计分",
        description:
          "默认在关联规则取得正贡献后建立事件，也可使用自定义起点；起点参考值只冻结一次。之后按市场交易日逐步检查，每日最多完成一步。步骤分和确认分首次取得后每天保留，不重复累加，并以“跟踪·名称”单独记入原规则所属场景和阶段。",
      },
      {
        heading: "解析场景",
        description:
          "场景先判断风险，再依次判断确认、触发和观察。最终阶段表示该场景在当日走到哪一步，强度表示分数相对阈值有多高。",
      },
      {
        heading: "生成名次",
        description:
          "当日股票按总分从高到低排列，同分时使用股票代码稳定排序，形成可以直接横向比较的当日榜。",
      },
    ],
    designs: [
      {
        heading: "总分从 50 起步",
        description:
          "50 是中性基准，原规则贡献可正可负，已取得的跟踪分再单独累加。这样既保留当日策略判断，也让后续确认在有效期内持续参与排名。",
        formula: "total_score = 50 + Σ(原规则得分) + Σ(跟踪得分)",
      },
      {
        heading: "场景有明确优先级",
        description:
          "同一场景中，风险信号优先；没有风险时再确认、触发、观察。阶段和高强度只表示策略状态，不直接等于上涨概率。",
        formula: "fail → confirm → trigger → observe",
      },
      {
        heading: "后续跟踪是持续事件",
        description:
          "跟踪不是每天重新判断一次原规则。事件建立后，原规则当天是否继续命中、已有步骤条件是否回落，都不会撤回已取得的跟踪分；只有失效、额外结束条件或期限到达才清零。",
        formula:
          "规则正贡献 → 建立事件并冻结起点值\n→ 每日最多完成一个步骤 → 最终确认\n→ 保留已得分至事件结束",
      },
      {
        heading: "观察期和保分期分开",
        description:
          "ttl 只限制还能完成新步骤和最终确认的时间；max_age 是事件从起点开始可以保留的总期限。invalidate 或 score_until 一旦成立立即停止并清零，不必等到总期限。",
        formula:
          "观察步骤：在 ttl 内推进\n保分：不超过 max_age\n提前结束：invalidate / score_until 命中",
      },
      {
        heading: "名次只在同一交易日内比较",
        description:
          "不同交易日的市场环境和可用股票可能变化，因此排名用于排出当天的相对顺序；跨日期观察应比较同一股票的评分变化或使用回测。",
        formula: "当日排名 = total_score 降序；同分时按 ts_code",
      },
    ],
    reading: [
      "total_score 越高，表示越符合当前策略；同一天、同一批规则和场景下的股票可以直接比较。",
      "rank = 1 是当日最高分，名次只表示当天在股票池中的相对位置。",
      "规则明细中，原规则分与“跟踪·名称”分分别列出；跟踪分不会把场景阶段自动升级为 confirm。",
      "跟踪事件可分观察、保分和停止三种状态。已得分在有效期内保持，但失效、结束条件或总期限到达后会清零。",
    ],
    boundary:
      "分数、跟踪分和名次都不包含未来收益，也不是概率。规则与跟踪分值由策略配置决定；“未知”不能当作 0 分或利空，关键当前证据缺失时跟踪会停止。",
  },
  {
    id: "similarity",
    number: "02",
    title: "相似榜计算",
    navHint: "历史形态与条件表现",
    summary:
      "相似榜计算回答：当前股票近期的策略触发、量价、指标和市场环境，在历史上有没有出现过得像的窗口；那些窗口后来表现如何。",
    inputs: [
      "参考日与目标窗口",
      "策略触发记录",
      "量价和指标序列",
      "全市场环境",
      "历史后续行情",
    ],
    outputs: [
      "综合相似度",
      "相似历史窗口",
      "加权收益与超额",
      "有效样本数",
      "相似榜名次与预测信号",
    ],
    steps: [
      {
        heading: "截取目标窗口",
        description:
          "以参考日为终点，取最近一段交易日作为目标形态。默认窗口 20 日、评价期 5 日，页面参数可以调整。",
      },
      {
        heading: "隔离未来信息",
        description:
          "候选结束日必须早于历史截止日，给后续评价和样本间隔留出时间。候选只有恰好也触发过目标窗口中的至少一种规则，才进入比较。",
      },
      {
        heading: "建立多通道指纹",
        description:
          "分别描述策略触发、量价、指标和市场环境。数值序列会保留窗口分段、短中期变化、趋势和拐点信息，而不是只比较最后一个值。",
      },
      {
        heading: "挑选近邻",
        description:
          "先用低成本通道缩小范围，再计算精确相似度，保留最接近的候选。评级样本最多 30 个，并控制同股和评价区间重叠。",
      },
      {
        heading: "统计后续表现",
        description:
          "每个历史窗口从后一交易日开盘持有到评价期结束，计算收益、超额、最大有利波动 MFE 和最大不利波动 MAE。",
      },
      {
        heading: "形成相似榜",
        description:
          "以相似度的平方加权历史超额收益，再按有效样本数收缩。只有有效样本足够时产生预测信号，并按信号形成榜内名次分。",
      },
    ],
    designs: [
      {
        heading: "四个通道各有侧重",
        description:
          "策略触发最重要，其次看当时市场环境，再看个股量价和指标。某通道双方都没有有效数据时，会跳过并重新归一化其余权重。",
        formula: "综合相似度 = 40% 触发 + 25% 市场 + 20% 量价 + 15% 指标",
      },
      {
        heading: "触发不仅比“像不像规则”",
        description:
          "还要比较同一规则在窗口中的位置、强度和整体触发节奏。少见规则权重更高，额外规则会降低规则集合的相似度。",
        formula: "触发相似度 = 45% 规则集合 + 35% 时序 + 20% 节奏",
      },
      {
        heading: "减少重叠样本造成的虚假证据",
        description:
          "一个历史窗口只算一次；同一股票相邻窗口、后验区间高度重叠的样本会受限。最多 3 个样本共享同一后验区间。",
        formula: "样本上限 30；同股同时段与高重叠窗口去重",
      },
      {
        heading: "小样本主动向中性收缩",
        description:
          "相似度越高权重越大，但样本少或权重过度集中时，收缩后的超额不会给出过度自信的结论。",
        formula:
          "wᵢ = (相似度ᵢ / 100)²\nN_eff = (Σwᵢ)² / Σwᵢ²\n收缩超额 = 加权超额 × N_eff / (N_eff + 8)",
      },
    ],
    reading: [
      "similarity_score 表示在本算法定义下“有多像”，不是未来上涨概率，也不是置信度百分比。",
      "N_eff 是有效样本数；它明显低于样本数时，说明结论主要由少数高相似窗口贡献。N_eff 至少达到 8，才生成预测排名。",
      "prediction_signal 是相似历史的条件超额估计。ranking_score 是本次榜单内部的 0—100 名次分，不是预期收益。",
      "MFE / MAE 只描述历史路径；匹配提升是近邻相对背景候选的诊断值，不参与排名门槛。",
    ],
    boundary:
      "历史后验仍是条件统计，不保证未来复现。默认收益从次日开盘算起，但未扣交易成本；相似度也不能跨不同窗口、参数或算法版本直接比较。",
  },
  {
    id: "chip",
    number: "03",
    title: "筹码计算",
    navHint: "成本分布与主散库存",
    summary:
      "筹码计算回答：按历史成交和换手，股票的成本分布在哪里，哪些库存仍被套牢，以及主力与散户的身份库存如何逐日变化。",
    inputs: ["前复权开高低收", "换手率", "筹码变化策略", "价格分桶与预热参数"],
    outputs: [
      "成本分桶",
      "主力/散户库存",
      "获利盘与套牢盘",
      "平均成本和 70/90 成本区间",
      "库存年龄与留存",
    ],
    steps: [
      {
        heading: "向前预热",
        description:
          "从可重放的历史起点连续计算，默认先预热 120 个报价日；需要更长窗口的表达式还会自动增加预热。不能从最新检查点倒推历史。",
      },
      {
        heading: "建立成本网格",
        description:
          "使用前复权价格和几何价格桶，默认相邻边界相差 1%。新价格区间出现时向外扩展，已有桶的库存不会凭空搬家。",
      },
      {
        heading: "先处理卖出",
        description:
          "换手率决定当日移动的库存比例。卖出策略给成本桶和持有人加权，先出卖出倾向高的库存；不足时逐级扩大到其他库存。",
      },
      {
        heading: "再分配新买方",
        description:
          "当日成交按开高低收形成的价格密度放入成本桶。当前筹码策略先用“主力卖出量 / 总换手”保持中性买卖平衡，再叠加方向证据。",
      },
      {
        heading: "约束身份变化",
        description:
          "方向证据只改变当天新增成交的主散分配，不直接改总持仓。正证据把新成交中的主力比例往 85% 推，负证据往 10% 推；最终仍受中性卖出平衡和当天换手规模约束。",
      },
      {
        heading: "生成快照",
        description:
          "每天归一为 100 份模型库存，计算收益状态、成本区间、年龄和套牢指标，再写入连续检查点供下一次计算续算。",
      },
    ],
    designs: [
      {
        heading: "库存守恒",
        description:
          "模型库存始终按 100 份归一化：卖出减少库存，新增买方补回同等换手量。主散标签可以变化，但不能在没有成交时直接重分类。",
        formula: "主力库存 + 散户库存 = 100（模型份额）",
      },
      {
        heading: "方向证据先累加",
        description:
          "当天命中的主力买入规则会把 bias 相加，得到总方向证据 E。E 的正负表示方向，数值大小决定迁移强度；它不是概率，也不是某笔成交直接归主力的百分比。",
        formula:
          "E = Σ(当日命中的主力买入规则 bias)\nE > 0：向主力方向偏移\nE < 0：向散户方向偏移\nE = 0：保持中性",
      },
      {
        heading: "85% 与 10% 只影响新增成交",
        description:
          "正证据让当天新成交中的主力比例从卖出平衡值向 85% 靠近，负证据向 10% 靠近。它们是软目标，不是总持仓硬边界，也不会在没有证据时自动漂移。以主力占 50%、换手 8% 为例：中性卖出 4 份且 E=+0.12 时，新成交中主力约占 66.8%，当天总主力仅约升至 51.3%，不会直接变成 66.8% 或 85%。",
        formula:
          "E > 0：距离 = max(85% − 卖出前主力占比, 0)\nE < 0：距离 = min(10% − 卖出前主力占比, 0)\n强度 = min(1, |E| × 4 × 负向倍率)；负向倍率：正证据 1，负证据 3\n主力新增占比 = clamp(主力卖出量 / 总换手 + 强度 × 距离, 0, 1)",
      },
      {
        heading: "价格成本与库存年龄分开",
        description:
          "成本桶记录“买在哪里”，年龄账本记录“这部分库存形成多久”。初始库存年龄未知，不能被伪装成新成交。",
        formula: "库存年龄：0—4 / 5—19 / 20—59 / 60—119 / 120+ / 初始未知",
      },
      {
        heading: "留存查询冻结区间",
        description:
          "固定成本区留存查询会冻结锚点价格桶，再在评价日区分原批次剩余与后续补入，避免价格桶扩张后把不同库存混在一起。",
        formula:
          "留存率 = 锚点旧库存剩余 / 锚点原库存\n补入占比 = 锚点后新增库存 / 评价日区间总量",
      },
    ],
    reading: [
      "分桶图和 70/90 成本区间描述模型认为的持仓成本位置；收盘价以下为获利盘，以上为套牢盘。",
      "主力占比是行为推断，不是交易所披露的真实持仓。低换手股票可能长期保留初始 50/50 假设的影响。",
      "85% 与 10% 是新增成交的迁移参照，不是总持仓上下限；系数再大，也只能在当天换手和可卖库存范围内改变身份。",
      "年龄分布需要足够观察日才显示对应字段；留存率区分“原库存还在”和“卖出后又买回”，两者总量可能相同，但库存年龄不同。",
    ],
    boundary:
      "结果是日线成交下的条件估计，不是账户真实成本、主力身份真相或未来涨跌预测。改变复权坐标、模拟起点、分桶、预热或策略后，必须从历史重算，不能拼接旧结果。",
  },
  {
    id: "backtest",
    number: "04",
    title: "回测计算",
    navHint: "预测力、收益与稳定性",
    summary:
      "回测计算回答：评分高低与未来表现是否同向，高分组的收益是否更高，以及这种关系有没有足够的稳定性和统计支持。",
    inputs: [
      "评分结果",
      "后续行情和基准",
      "持有期与 Beta",
      "样本过滤条件",
      "收益口径",
    ],
    outputs: [
      "IC / ICIR / HAC t",
      "平均残差与超额",
      "高低分组差",
      "分层单调性",
      "样本利润因子",
    ],
    steps: [
      {
        heading: "确定样本",
        description:
          "只保留回测区间内有评分、有后续行情并满足上市天数、股票池和市值等过滤条件的“股票 × 日期”样本。",
      },
      {
        heading: "按真实时点进入",
        description:
          "评分在当日完成后确认，收益从下一交易日开盘开始，持有到评价期最后一个完整交易日收盘，避免把收盘信息当成开盘可成交。",
      },
      {
        heading: "计算残差收益",
        description:
          "先用指数、概念和行业收益估计股票应有表现，再与实际收益相减，得到尽量剥离市场和风格后的选股结果。",
      },
      {
        heading: "逐日比较",
        description:
          "每天比较评分排名与未来收益排名，计算 IC；同时比较高分组与低分组、不同分数层以及触发样本的收益分布。",
      },
      {
        heading: "汇总稳定性",
        description:
          "把每日 IC 汇总为均值、ICIR 和 HAC t。相邻持有期会共享未来行情，因此 t 值使用持有期减 1 的滞后修正。",
      },
      {
        heading: "输出结果",
        description:
          "同时展示预测方向、收益幅度、分层形态和样本质量，避免仅凭一个指标判断策略有效。",
      },
    ],
    designs: [
      {
        heading: "残差收益剔除共同风格",
        description:
          "Beta 越大，扣除的对应市场因素越多。残差为正表示股票跑赢它按配置应承担的那部分市场、概念和行业表现。",
        formula:
          "应有收益 = β指数 × 指数收益 + β概念 × 概念收益 + β行业 × 行业收益\n残差收益 = 股票收益 − 应有收益",
      },
      {
        heading: "IC 看排序，不看绝对分数",
        description:
          "每日先转成排名再求相关，所以分数整体放大或平移不会改变 IC。正值表示评分越高，未来表现通常越好。",
        formula: "IC = 评分排名与未来收益排名的相关系数",
      },
      {
        heading: "ICIR 与 HAC t 看稳定性",
        description:
          "ICIR 衡量 IC 相对自身波动有多大；HAC t 进一步处理相邻日期收益重叠，避免把同一次行情重复当成大量独立证据。",
        formula: "ICIR = mean(IC) / std(IC)\nHAC 滞后阶数 = max(0, 持有期 − 1)",
      },
      {
        heading: "分层和分组检查单调性",
        description:
          "按分数范围、样本分位数或数据库排名分组，比较高分组与低分组收益。有效策略通常应表现为中间层较平滑、首尾差方向稳定。",
        formula: "top_bottom_spread = 高分组平均收益 − 低分组平均收益",
      },
    ],
    reading: [
      "先看 IC 是否长期为正、t 是否足够大，再看分层是否单调，最后检查平均残差和超额是否支持实际收益方向。",
      "ICIR 绝对值越大，说明 IC 相对波动越稳定；但单一阈值不是盈利保证，要结合样本长度和样本外结果。",
      "收益金额模式用评分日推测流通市值把残差收益率换成金额；它按同一口径参与 IC、均值和分层，不改变时间顺序。",
      "样本利润因子是正残差总额除以负残差绝对值总额。它衡量样本盈亏总额，不是按天计算的盈亏比。",
    ],
    boundary:
      "这是因子与评分效果回测，不是完整账户模拟，默认不建模手续费、滑点和冲击成本。显著结果仍需样本外验证，频繁调参也会放大偶然发现。",
  },
];

type AlgorithmGuideModalProps = {
  open: boolean;
  onClose: () => void;
};

export default function AlgorithmGuideModal({
  open,
  onClose,
}: AlgorithmGuideModalProps) {
  const [activeId, setActiveId] = useState<AlgorithmSectionId>("score");
  const bodyRef = useRef<HTMLDivElement>(null);
  const activeSection =
    ALGORITHM_SECTIONS.find((section) => section.id === activeId) ??
    ALGORITHM_SECTIONS[0];

  useEffect(() => {
    if (!open) {
      return;
    }
    function onKeyDown(event: KeyboardEvent) {
      if (event.key === "Escape") {
        onClose();
      }
    }
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [onClose, open]);

  if (!open) {
    return null;
  }

  return (
    <div
      className="algorithm-guide-backdrop"
      role="presentation"
      onClick={(event) => {
        if (event.target === event.currentTarget) {
          onClose();
        }
      }}
    >
      <div className="algorithm-guide-modal" role="dialog" aria-modal="true">
        <header className="algorithm-guide-head">
          <div>
            <span className="algorithm-guide-kicker">
              计算口径 · 当前生产实现
            </span>
            <h3>算法说明</h3>
            <p>
              四套算法回答不同问题：当天强不强、历史像不像、成本在哪里、这种信号是否真的有效。每块只解释自己的输入、计算过程和结果边界。
            </p>
          </div>
          <button
            type="button"
            className="settings-secondary-btn"
            onClick={onClose}
          >
            关闭
          </button>
        </header>

        <nav
          className="algorithm-guide-tabs"
          role="tablist"
          aria-label="算法说明分类"
        >
          {ALGORITHM_SECTIONS.map((section) => (
            <button
              key={section.id}
              type="button"
              role="tab"
              id={`algorithm-tab-${section.id}`}
              aria-selected={section.id === activeSection.id}
              aria-controls={`algorithm-panel-${section.id}`}
              className={`algorithm-guide-tab algorithm-guide-tab-${section.id}`}
              onClick={() => {
                setActiveId(section.id);
                bodyRef.current?.scrollTo({ top: 0 });
              }}
            >
              <span className="algorithm-guide-tab-number">
                {section.number}
              </span>
              <span>
                <strong>{section.title}</strong>
                <small>{section.navHint}</small>
              </span>
            </button>
          ))}
        </nav>

        <div
          ref={bodyRef}
          className={`algorithm-guide-body algorithm-guide-theme-${activeSection.id}`}
          role="tabpanel"
          id={`algorithm-panel-${activeSection.id}`}
          aria-labelledby={`algorithm-tab-${activeSection.id}`}
        >
          <section className="algorithm-guide-summary">
            <div className="algorithm-guide-title-row">
              <span>{activeSection.number}</span>
              <div>
                <h4>{activeSection.title}</h4>
                <p>{activeSection.summary}</p>
              </div>
            </div>
            <div className="algorithm-guide-io">
              <div>
                <strong>它读取什么</strong>
                <div className="algorithm-guide-tags">
                  {activeSection.inputs.map((input) => (
                    <span key={input}>{input}</span>
                  ))}
                </div>
              </div>
              <div>
                <strong>它产出什么</strong>
                <div className="algorithm-guide-tags">
                  {activeSection.outputs.map((output) => (
                    <span key={output}>{output}</span>
                  ))}
                </div>
              </div>
            </div>
          </section>

          <section className="algorithm-guide-block">
            <div className="algorithm-guide-block-head">
              <span>计算路径</span>
              <p>从原始数据到页面结果，按这个顺序执行。</p>
            </div>
            <ol className="algorithm-guide-steps">
              {activeSection.steps.map((step, index) => (
                <li key={step.heading}>
                  <span>{String(index + 1).padStart(2, "0")}</span>
                  <div>
                    <strong>{step.heading}</strong>
                    <p>{step.description}</p>
                  </div>
                </li>
              ))}
            </ol>
          </section>

          <section className="algorithm-guide-block">
            <div className="algorithm-guide-block-head">
              <span>设计重点</span>
              <p>这些选择决定了算法为什么这样算，以及数值应该怎样理解。</p>
            </div>
            <div className="algorithm-guide-designs">
              {activeSection.designs.map((design) => (
                <article
                  key={design.heading}
                  className="algorithm-guide-design"
                >
                  <h5>{design.heading}</h5>
                  <p>{design.description}</p>
                  {design.formula ? (
                    <pre className="algorithm-guide-code">{design.formula}</pre>
                  ) : null}
                </article>
              ))}
            </div>
          </section>

          <section className="algorithm-guide-reading">
            <div>
              <strong>怎么读结果</strong>
              <ul>
                {activeSection.reading.map((item) => (
                  <li key={item}>{item}</li>
                ))}
              </ul>
            </div>
            <div className="algorithm-guide-boundary">
              <strong>算法边界</strong>
              <p>{activeSection.boundary}</p>
            </div>
          </section>
        </div>
      </div>
    </div>
  );
}
