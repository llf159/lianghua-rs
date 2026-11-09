import { memo, useEffect, useState } from "react";
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

export default memo(function DetailCompanySection({
  sourcePath,
  tsCode,
}: {
  sourcePath: string;
  tsCode: string;
}) {
  const [data, setData] = useState<DetailCompanyDataset[] | null>(null);
  const [error, setError] = useState("");
  const [revision, setRevision] = useState(0);
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
  }, [sourcePath, tsCode, revision]);

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
        <button
          type="button"
          className="details-primary-btn details-primary-btn-alt"
          onClick={() => {
            setData(null);
            setError("");
            setRevision((value) => value + 1);
          }}
        >
          刷新
        </button>
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
                        <tr key={`${row.end_date}-${row.holder_name ?? index}`}>
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
                              {formatCompanyValue(row[column.key], column.unit)}
                            </td>
                          ))}
                          <td>{formatCompanyValue(row.ann_date, "date")}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
                <p className="details-note details-company-footnote">
                  {selected.key.startsWith("top10_")
                    ? "展示最新一期披露，按持股数量排列；持股比例沿用数据源口径。"
                    : selected.key === "stk_holdernumber"
                      ? "展示最近 8 个报告期；股东人数为披露日对应的报告期户数。"
                      : "展示最近 8 个报告期；财务报表采用合并报表，利润及现金流为年初至报告期末累计值。"}{" "}
                  缺失字段以“—”表示。
                </p>
              </>
            )}
          </div>
        </>
      ) : null}
    </section>
  );
});
