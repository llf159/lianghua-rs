import {
  memo,
  useEffect,
  useMemo,
  useRef,
  useState,
  type PointerEvent as ReactPointerEvent,
} from "react";
import {
  getStockDetailCompany,
  type DetailCompanyDataset,
} from "../../../apis/details";
import "../css/DetailCompanySection.css";

type CompanyColumn = {
  key: string;
  label: string;
  unit?: "money" | "shares" | "percent" | "date" | "people" | "text";
};

const COMPANY_TABS: {
  key: string;
  label: string;
  group: string;
  columns: CompanyColumn[];
}[] = [
  {
    key: "fina_indicator",
    label: "财务指标",
    group: "基本面",
    columns: [
      { key: "eps", label: "每股收益（元）" },
      { key: "bps", label: "每股净资产（元）" },
      { key: "roe", label: "净资产收益率", unit: "percent" },
      { key: "grossprofit_margin", label: "销售毛利率", unit: "percent" },
      { key: "netprofit_margin", label: "销售净利率", unit: "percent" },
      { key: "or_yoy", label: "营收同比", unit: "percent" },
      { key: "netprofit_yoy", label: "归母净利同比", unit: "percent" },
      { key: "debt_to_assets", label: "资产负债率", unit: "percent" },
    ],
  },
  {
    key: "income",
    label: "利润表",
    group: "基本面",
    columns: [
      { key: "revenue", label: "营业收入", unit: "money" },
      { key: "total_revenue", label: "营业总收入", unit: "money" },
      { key: "operate_profit", label: "营业利润", unit: "money" },
      { key: "n_income_attr_p", label: "归母净利润", unit: "money" },
      { key: "basic_eps", label: "基本每股收益（元）" },
    ],
  },
  {
    key: "balancesheet",
    label: "资产负债",
    group: "基本面",
    columns: [
      { key: "total_assets", label: "资产总计", unit: "money" },
      { key: "total_liab", label: "负债合计", unit: "money" },
      {
        key: "total_hldr_eqy_exc_min_int",
        label: "归母股东权益",
        unit: "money",
      },
      { key: "money_cap", label: "货币资金", unit: "money" },
      { key: "accounts_receiv", label: "应收账款", unit: "money" },
      { key: "inventories", label: "存货", unit: "money" },
    ],
  },
  {
    key: "cashflow",
    label: "现金流量",
    group: "基本面",
    columns: [
      { key: "n_cashflow_act", label: "经营现金流净额", unit: "money" },
      { key: "n_cashflow_inv_act", label: "投资现金流净额", unit: "money" },
      { key: "n_cash_flows_fnc_act", label: "筹资现金流净额", unit: "money" },
      {
        key: "c_cash_equ_end_period",
        label: "期末现金及等价物",
        unit: "money",
      },
    ],
  },
  {
    key: "stk_holdernumber",
    label: "股东人数",
    group: "股东",
    columns: [{ key: "holder_num", label: "股东户数", unit: "people" }],
  },
  {
    key: "top10_holders",
    label: "十大股东",
    group: "股东",
    columns: [
      { key: "holder_name", label: "股东名称", unit: "text" },
      { key: "hold_amount", label: "持股数量", unit: "shares" },
      { key: "hold_ratio", label: "占总股本", unit: "percent" },
      { key: "hold_float_ratio", label: "占流通股本", unit: "percent" },
      { key: "holder_type", label: "股东类型", unit: "text" },
    ],
  },
  {
    key: "top10_floatholders",
    label: "十大流通股东",
    group: "股东",
    columns: [
      { key: "holder_name", label: "股东名称", unit: "text" },
      { key: "hold_amount", label: "持股数量", unit: "shares" },
      { key: "hold_ratio", label: "持股比例", unit: "percent" },
      { key: "share_type", label: "股份类型", unit: "text" },
    ],
  },
];

function formatCompanyValue(
  value: string | number | null | undefined,
  unit?: CompanyColumn["unit"],
): string {
  if (value == null || value === "") return "—";
  if (unit === "date")
    return String(value).replace(/^(\d{4})(\d{2})(\d{2})$/, "$1-$2-$3");
  if (unit === "text") return String(value);
  const number = Number(value);
  if (!Number.isFinite(number)) return "—";
  if (unit === "money" || unit === "shares") {
    return `${(number / (Math.abs(number) >= 1e8 ? 1e8 : 1e4)).toLocaleString("zh-CN", { maximumFractionDigits: 2 })}${Math.abs(number) >= 1e8 ? "亿" : "万"}${unit === "shares" ? "股" : "元"}`;
  }
  return `${number.toLocaleString("zh-CN", { maximumFractionDigits: unit === "people" ? 0 : 2 })}${unit === "percent" ? "%" : unit === "people" ? "户" : ""}`;
}

function companyNumber(
  value: string | number | null | undefined,
): number | null {
  if (value == null || value === "") return null;
  return Number.isFinite(Number(value)) ? Number(value) : null;
}

function companyScale(values: (number | null)[]) {
  let min = Math.min(
    0,
    ...values.filter((value): value is number => value !== null),
  );
  let max = Math.max(
    0,
    ...values.filter((value): value is number => value !== null),
  );
  if (min === max) max = min + 1;
  if (min < 0) min *= 1.12;
  if (max > 0) max *= 1.12;
  return { min, max };
}

const CompanyDataChart = memo(function CompanyDataChart({
  rows,
  columns,
  horizontal,
  comparePrevious,
}: {
  rows: DetailCompanyDataset["rows"];
  columns: CompanyColumn[];
  horizontal: boolean;
  comparePrevious: boolean;
}) {
  const containerRef = useRef<HTMLDivElement>(null);
  const [width, setWidth] = useState(900);
  const [primaryKey, setPrimaryKey] = useState("");
  const [focus, setFocus] = useState<{
    index: number;
    position: number;
  } | null>(null);
  useEffect(() => {
    if (!containerRef.current) return;
    const observer = new ResizeObserver(([entry]) => {
      setWidth(Math.max(1, entry.contentRect.width));
      setFocus(null);
    });
    observer.observe(containerRef.current);
    return () => observer.disconnect();
  }, []);

  const chartRows = useMemo(
    () => (horizontal ? rows : [...rows].reverse()),
    [rows, horizontal],
  );
  const numericColumns = columns.filter(
    (column) =>
      column.unit !== "text" &&
      chartRows.some((row) => companyNumber(row[column.key]) !== null),
  );
  const primary =
    (horizontal
      ? columns.find((column) => column.key === "hold_amount")
      : numericColumns.find((column) => column.key === primaryKey)) ??
    numericColumns[0];
  const primaryValues = chartRows.map((row) =>
    primary ? companyNumber(row[primary.key]) : null,
  );
  const changeValues = chartRows.map((row, index) => {
    if (horizontal) return null;
    const baseIndex = comparePrevious
      ? index - 1
      : chartRows.findIndex(
          (candidate) =>
            String(candidate.end_date) ===
            `${Number(String(row.end_date).slice(0, 4)) - 1}${String(row.end_date).slice(4)}`,
        );
    const current = primaryValues[index];
    const base = primaryValues[baseIndex];
    if (current == null || base == null || base === 0) return null;
    const change = ((current - base) / Math.abs(base)) * 100;
    return Number.isFinite(change) ? change : null;
  });
  const ratioColumns = columns.filter((column) => column.unit === "percent");
  const primaryScale =
    primary?.unit === "percent"
      ? { min: 0, max: 100 }
      : companyScale(primaryValues);
  const changeScale = {
    min: Math.min(
      -100,
      Math.floor(
        Math.min(
          0,
          ...changeValues.filter((value): value is number => value !== null),
        ) / 100,
      ) * 100,
    ),
    max: Math.max(
      100,
      Math.ceil(
        Math.max(
          0,
          ...changeValues.filter((value): value is number => value !== null),
        ) / 100,
      ) * 100,
    ),
  };
  const changeLabel = comparePrevious ? "较上期变化率" : "同比变化率";
  const left = horizontal ? (width < 600 ? 112 : 190) : width < 420 ? 68 : 88;
  const right = width - (!horizontal ? (width < 420 ? 68 : 88) : 140);
  const height = horizontal
    ? Math.max(300, chartRows.length * (ratioColumns.length > 1 ? 68 : 54) + 70)
    : 310;
  const top = 24;
  const bottom = height - 48;
  const y = (value: number, scale: { min: number; max: number }) =>
    bottom -
    ((Math.max(scale.min, Math.min(scale.max, value)) - scale.min) /
      (scale.max - scale.min)) *
      (bottom - top);
  const x = (index: number) =>
    left + ((index + 0.5) * (right - left)) / chartRows.length;
  const amountX = (value: number) =>
    left +
    ((value - primaryScale.min) / (primaryScale.max - primaryScale.min)) *
      (right - left);
  const rowY = (index: number) =>
    top + ((index + 0.5) * (bottom - top)) / chartRows.length;
  const focusedIndex = Math.min(
    focus?.index ?? (horizontal ? 0 : chartRows.length - 1),
    chartRows.length - 1,
  );
  const focusedRow = chartRows[focusedIndex];
  const cursorValue = focus
    ? horizontal
      ? primaryScale.min +
        ((focus.position - left) / (right - left)) *
          (primaryScale.max - primaryScale.min)
      : primaryScale.min +
        ((bottom - focus.position) / (bottom - top)) *
          (primaryScale.max - primaryScale.min)
    : null;
  let connected = false;
  const changePath = changeValues
    .map((value, index) => {
      if (value === null) {
        connected = false;
        return "";
      }
      const command = connected ? "L" : "M";
      connected = true;
      return `${command}${x(index)},${y(value, changeScale)}`;
    })
    .join(" ");

  const moveCursor = (event: ReactPointerEvent<SVGSVGElement>) => {
    const rect = event.currentTarget.getBoundingClientRect();
    const pointerX = Math.max(
      left,
      Math.min(right, ((event.clientX - rect.left) / rect.width) * width),
    );
    const pointerY = Math.max(
      top,
      Math.min(bottom, ((event.clientY - rect.top) / rect.height) * height),
    );
    setFocus({
      index: Math.min(
        chartRows.length - 1,
        Math.max(
          0,
          Math.floor(
            (horizontal
              ? (pointerY - top) / (bottom - top)
              : (pointerX - left) / (right - left)) * chartRows.length,
          ),
        ),
      ),
      position: horizontal ? pointerX : pointerY,
    });
  };
  return (
    <div className="details-company-chart" ref={containerRef}>
      {!horizontal && (
        <div
          className="details-company-chart-controls"
          role="group"
          aria-label="图表指标"
        >
          {numericColumns.map((column) => (
            <button
              key={column.key}
              type="button"
              aria-pressed={primary?.key === column.key}
              className={primary?.key === column.key ? "is-active" : ""}
              onClick={() => {
                setPrimaryKey(column.key);
                setFocus(null);
              }}
            >
              {column.label}
            </button>
          ))}
        </div>
      )}
      {!primary ? (
        <div className="details-empty">当前分类暂无可绘制的数值</div>
      ) : (
        <>
          <div className="details-company-chart-readout" aria-live="polite">
            <div>
              <strong>
                {horizontal
                  ? focusedRow?.holder_name
                  : formatCompanyValue(focusedRow?.end_date, "date")}
              </strong>
              <span>
                公告 {formatCompanyValue(focusedRow?.ann_date, "date")}
              </span>
            </div>
            <div>
              <span
                className="details-company-legend-dot"
                style={{ background: "#2563eb" }}
              />
              {primary.label}
              <strong>
                {formatCompanyValue(focusedRow?.[primary.key], primary.unit)}
              </strong>
            </div>
            {!horizontal && (
              <div>
                <span
                  className="details-company-legend-dot"
                  style={{ background: "#ea8a23" }}
                />
                {changeLabel}
                <strong>
                  {formatCompanyValue(changeValues[focusedIndex], "percent")}
                </strong>
              </div>
            )}
            {horizontal &&
              columns
                .filter(
                  (column) =>
                    column.unit === "percent" && column.key !== primary.key,
                )
                .map((column) => (
                  <div key={column.key}>
                    {column.label}
                    <strong>
                      {formatCompanyValue(
                        focusedRow?.[column.key],
                        column.unit,
                      )}
                    </strong>
                  </div>
                ))}
          </div>
          <svg
            className="details-company-chart-svg"
            viewBox={`0 0 ${width} ${height}`}
            preserveAspectRatio="xMidYMid meet"
            style={{ height }}
            role="img"
            aria-label={
              horizontal
                ? `${primary.label}股东对比，触摸查看详情`
                : `${primary.label}历史趋势，左右方向键或触摸移动十字准星`
            }
            tabIndex={0}
            onPointerMove={moveCursor}
            onPointerDown={(event) => {
              event.currentTarget.setPointerCapture(event.pointerId);
              moveCursor(event);
            }}
            onPointerLeave={(event) => {
              if (
                event.pointerType === "mouse" &&
                !event.currentTarget.hasPointerCapture(event.pointerId)
              )
                setFocus(null);
            }}
            onPointerUp={(event) => {
              if (event.currentTarget.hasPointerCapture(event.pointerId))
                event.currentTarget.releasePointerCapture(event.pointerId);
            }}
            onKeyDown={(event) => {
              if (
                [
                  "ArrowLeft",
                  "ArrowRight",
                  "ArrowUp",
                  "ArrowDown",
                  "Home",
                  "End",
                  "Escape",
                ].includes(event.key)
              ) {
                event.preventDefault();
                if (event.key === "Escape") {
                  setFocus(null);
                  return;
                }
                const index = Math.max(
                  0,
                  Math.min(
                    chartRows.length - 1,
                    event.key === "Home"
                      ? 0
                      : event.key === "End"
                        ? chartRows.length - 1
                        : focusedIndex +
                          (["ArrowLeft", "ArrowUp"].includes(event.key)
                            ? -1
                            : 1),
                  ),
                );
                setFocus({
                  index,
                  position: horizontal
                    ? amountX(primaryValues[index] ?? 0)
                    : y(primaryValues[index] ?? 0, primaryScale),
                });
              }
            }}
          >
            <title>
              {primary.label}
              {horizontal ? "股东持仓对比" : "最近八期变化"}
            </title>
            {Array.from({ length: 5 }, (_, index) => {
              const value =
                primaryScale.min +
                ((primaryScale.max - primaryScale.min) * index) / 4;
              return horizontal ? (
                <g key={index}>
                  <line
                    className="details-company-chart-grid"
                    x1={amountX(value)}
                    x2={amountX(value)}
                    y1={top}
                    y2={bottom}
                  />
                  <text
                    x={amountX(value)}
                    y={bottom + 24}
                    textAnchor={
                      index === 0 ? "start" : index === 4 ? "end" : "middle"
                    }
                  >
                    {formatCompanyValue(value, primary.unit)}
                  </text>
                </g>
              ) : (
                <g key={index}>
                  <line
                    className="details-company-chart-grid"
                    x1={left}
                    x2={right}
                    y1={y(value, primaryScale)}
                    y2={y(value, primaryScale)}
                  />
                  <text
                    x={left - 10}
                    y={y(value, primaryScale) + 4}
                    textAnchor="end"
                  >
                    {formatCompanyValue(value, primary.unit)}
                  </text>
                  {!horizontal && (
                    <text x={right + 10} y={y(value, primaryScale) + 4}>
                      {formatCompanyValue(
                        changeScale.min +
                          ((changeScale.max - changeScale.min) * index) / 4,
                        "percent",
                      )}
                    </text>
                  )}
                </g>
              );
            })}
            {!horizontal && (
              <line
                className="details-company-chart-zero"
                x1={left}
                x2={right}
                y1={y(0, primaryScale)}
                y2={y(0, primaryScale)}
              />
            )}
            {horizontal ? (
              chartRows.map((row, index) => (
                <g key={index}>
                  <text x={left - 10} y={rowY(index) - 5} textAnchor="end">
                    <title>{row.holder_name}</title>
                    {String(row.holder_name ?? "—").length >
                    (width < 600 ? 7 : 12)
                      ? `${String(row.holder_name).slice(0, width < 600 ? 7 : 12)}…`
                      : row.holder_name}
                  </text>
                  {primaryValues[index] !== null && (
                    <rect
                      x={Math.min(amountX(0), amountX(primaryValues[index]))}
                      y={rowY(index) - 18}
                      width={Math.max(
                        1,
                        Math.abs(amountX(primaryValues[index]) - amountX(0)),
                      )}
                      height={14}
                      rx={3}
                      fill={focusedIndex === index ? "#2563eb" : "#8bb8fb"}
                    />
                  )}
                  <text x={right + 8} y={rowY(index) - 7}>
                    {formatCompanyValue(primaryValues[index], "shares")}
                  </text>
                  {ratioColumns.map((column, ratioIndex) => (
                    <text
                      key={column.key}
                      x={right + 8}
                      y={rowY(index) + ratioIndex * 14 + 6}
                    >
                      {column.label}{" "}
                      {formatCompanyValue(row[column.key], "percent")}
                    </text>
                  ))}
                </g>
              ))
            ) : (
              <>
                {chartRows.map((row, index) => (
                  <g key={index}>
                    {primaryValues[index] !== null && (
                      <rect
                        x={
                          x(index) -
                          Math.min(
                            22,
                            ((right - left) / chartRows.length) * 0.27,
                          )
                        }
                        y={Math.min(
                          y(0, primaryScale),
                          y(primaryValues[index], primaryScale),
                        )}
                        width={Math.min(
                          44,
                          ((right - left) / chartRows.length) * 0.54,
                        )}
                        height={Math.max(
                          1,
                          Math.abs(
                            y(primaryValues[index], primaryScale) -
                              y(0, primaryScale),
                          ),
                        )}
                        rx={3}
                        fill={
                          primaryValues[index] < 0
                            ? "#e89898"
                            : focusedIndex === index
                              ? "#2563eb"
                              : "#8bb8fb"
                        }
                      />
                    )}
                    {primaryValues[index] === null && (
                      <text x={x(index)} y={bottom - 6} textAnchor="middle">
                        —
                      </text>
                    )}
                    <text x={x(index)} y={bottom + 20} textAnchor="middle">
                      {String(row.end_date ?? "").slice(0, 4)}
                    </text>
                    <text x={x(index)} y={bottom + 36} textAnchor="middle">
                      {String(row.end_date ?? "")
                        .slice(4)
                        .replace(/^(\d{2})(\d{2})$/, "$1-$2")}
                    </text>
                  </g>
                ))}
                <path
                  d={changePath}
                  fill="none"
                  stroke="#ea8a23"
                  strokeWidth={2.5}
                  strokeLinejoin="round"
                />
                {changeValues.map((value, index) =>
                  value === null ? null : (
                    <circle
                      key={index}
                      cx={x(index)}
                      cy={y(value, changeScale)}
                      r={focusedIndex === index ? 5 : 3.5}
                      fill="#fff"
                      stroke="#ea8a23"
                      strokeWidth={2}
                    />
                  ),
                )}
                {primary?.unit === "percent" &&
                  primaryValues.map((value, index) =>
                    value !== null && (value < 0 || value > 100) ? (
                      <text
                        key={`value-${index}`}
                        x={x(index)}
                        y={value > 100 ? top + 26 : bottom - 5}
                        textAnchor="middle"
                        style={{ fill: "#2563eb", fontSize: 15 }}
                      >
                        <title>
                          {primary.label} {formatCompanyValue(value, "percent")}
                          ，超出固定轴范围
                        </title>
                        {value > 100 ? "↑" : "↓"}
                      </text>
                    ) : null,
                  )}
              </>
            )}
            {focus && (
              <g
                className="details-company-chart-crosshair"
                pointerEvents="none"
              >
                <line
                  x1={horizontal ? focus.position : x(focusedIndex)}
                  x2={horizontal ? focus.position : x(focusedIndex)}
                  y1={top}
                  y2={bottom}
                />
                <line
                  x1={left}
                  x2={right}
                  y1={horizontal ? rowY(focusedIndex) : focus.position}
                  y2={horizontal ? rowY(focusedIndex) : focus.position}
                />
                {!horizontal && (
                  <>
                    <rect
                      x={0}
                      y={focus.position - 11}
                      width={left - 4}
                      height={22}
                      rx={4}
                    />
                    <text x={left - 10} y={focus.position + 4} textAnchor="end">
                      {formatCompanyValue(cursorValue, primary.unit)}
                    </text>
                    <rect
                      x={Math.max(
                        left,
                        Math.min(right - 90, x(focusedIndex) - 45),
                      )}
                      y={bottom + 4}
                      width={90}
                      height={22}
                      rx={4}
                    />
                    <text
                      x={
                        Math.max(
                          left,
                          Math.min(right - 90, x(focusedIndex) - 45),
                        ) + 45
                      }
                      y={bottom + 19}
                      textAnchor="middle"
                    >
                      {formatCompanyValue(focusedRow?.end_date, "date")}
                    </text>
                  </>
                )}
                {horizontal && (
                  <>
                    <rect
                      x={Math.max(
                        left,
                        Math.min(right - 110, focus.position - 55),
                      )}
                      y={bottom + 6}
                      width={110}
                      height={24}
                      rx={4}
                    />
                    <text
                      x={
                        Math.max(
                          left,
                          Math.min(right - 110, focus.position - 55),
                        ) + 55
                      }
                      y={bottom + 22}
                      textAnchor="middle"
                    >
                      {formatCompanyValue(cursorValue, primary.unit)}
                    </text>
                  </>
                )}
              </g>
            )}
          </svg>
        </>
      )}
    </div>
  );
});

export default memo(function DetailCompanySection({
  sourcePath,
  tsCode,
}: {
  sourcePath: string;
  tsCode: string;
}) {
  const [data, setData] = useState<DetailCompanyDataset[] | null>(null);
  const [error, setError] = useState("");
  const [activeKey, setActiveKey] = useState("");
  useEffect(() => {
    let cancelled = false;
    getStockDetailCompany({ sourcePath, tsCode })
      .then((result) => {
        if (!cancelled) {
          setData(result);
          setError("");
        }
      })
      .catch((reason: unknown) => {
        if (!cancelled) setError(String(reason));
      });
    return () => {
      cancelled = true;
    };
  }, [sourcePath, tsCode]);

  const availableTabs = COMPANY_TABS.filter((tab) =>
    data?.some(
      (item) => item.key === tab.key && (item.rows.length > 0 || item.error),
    ),
  );
  const selected =
    availableTabs.find((tab) => tab.key === activeKey) ?? availableTabs[0];
  const dataset = data?.find((item) => item.key === selected?.key);
  const latest = dataset?.rows[0];
  if (!error && data && availableTabs.length === 0) return null;
  return (
    <section
      className="details-card details-company-section"
      aria-label="基本面与股东"
    >
      <div className="details-company-heading">
        <div>
          <h3 className="details-subtitle">基本面与股东</h3>
          <p className="details-note">
            本地已下载 · 最新披露数据，独立于上方行情参考日
          </p>
        </div>
      </div>
      {error ? (
        <div className="details-empty" role="alert">
          {error}
        </div>
      ) : !data ? (
        <div className="details-empty" role="status">
          正在读取基本面与股东数据…
        </div>
      ) : selected ? (
        <>
          <div
            className="details-company-tabs"
            role="tablist"
            aria-label="公司数据分类"
          >
            {availableTabs.map((tab, index) => (
              <button
                key={tab.key}
                type="button"
                role="tab"
                id={`company-tab-${tab.key}`}
                aria-selected={selected.key === tab.key}
                aria-controls="company-data-panel"
                className={`${selected.key === tab.key ? "is-active" : ""} ${index > 0 && availableTabs[index - 1].group !== tab.group ? "is-group-start" : ""}`}
                onClick={() => setActiveKey(tab.key)}
              >
                {tab.label}
              </button>
            ))}
          </div>
          <div
            id="company-data-panel"
            role="tabpanel"
            aria-labelledby={`company-tab-${selected.key}`}
          >
            {dataset?.error ? (
              <div className="details-empty" role="alert">
                {dataset.error}
              </div>
            ) : (
              <>
                <div className="details-company-period">
                  <strong>{selected.label}</strong>
                  <span>
                    报告期 {formatCompanyValue(latest?.end_date, "date")}
                  </span>
                  <span>
                    公告日 {formatCompanyValue(latest?.ann_date, "date")}
                  </span>
                </div>
                <div className="details-company-metrics">
                  {selected.columns
                    .filter((column) => column.unit !== "text")
                    .slice(0, 4)
                    .map((column) => (
                      <div key={column.key}>
                        <span>
                          {column.label}
                          {selected.key.startsWith("top10_")
                            ? "（第一大股东）"
                            : ""}
                        </span>
                        <strong>
                          {formatCompanyValue(
                            latest?.[column.key],
                            column.unit,
                          )}
                        </strong>
                      </div>
                    ))}
                </div>
                {dataset && (
                  <CompanyDataChart
                    key={selected.key}
                    rows={dataset.rows}
                    columns={selected.columns}
                    horizontal={selected.key.startsWith("top10_")}
                    comparePrevious={selected.key === "stk_holdernumber"}
                  />
                )}
                <details className="details-company-table-details">
                  <summary>
                    查看数据明细（{dataset?.rows.length ?? 0} 条）
                  </summary>
                  <div className="details-company-table-wrap">
                    <table className="details-company-table">
                      <thead>
                        <tr>
                          <th>
                            {selected.key.startsWith("top10_")
                              ? "序号"
                              : "报告期"}
                          </th>
                          {selected.columns.map((column) => (
                            <th key={column.key}>{column.label}</th>
                          ))}
                          <th>公告日</th>
                        </tr>
                      </thead>
                      <tbody>
                        {dataset?.rows.map((row, index) => (
                          <tr
                            key={`${row.end_date}-${row.holder_name ?? index}`}
                          >
                            <th scope="row">
                              {selected.key.startsWith("top10_")
                                ? index + 1
                                : formatCompanyValue(row.end_date, "date")}
                            </th>
                            {selected.columns.map((column) => (
                              <td
                                key={column.key}
                                className={
                                  column.unit === "text" ? "is-text" : undefined
                                }
                              >
                                {formatCompanyValue(
                                  row[column.key],
                                  column.unit,
                                )}
                              </td>
                            ))}
                            <td>{formatCompanyValue(row.ann_date, "date")}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </details>
              </>
            )}
          </div>
        </>
      ) : null}
    </section>
  );
});
