import { useEffect, useMemo, useState } from "react";
import {
  readStoredDragonTigerDownloadSettings,
  runDragonTigerDownload,
} from "../../../apis/dataDownload";
import {
  getDragonTigerMarketData,
  type DragonTigerMarketData,
} from "../../../apis/strategyTrigger";
import DragonTigerStockDetailModal from "./DragonTigerStockDetailModal";

type DragonTigerMarketPanelProps = {
  sourcePath: string;
  referenceTradeDate?: string | null;
};

function compactDate(value: string) {
  return value.replaceAll("-", "").trim();
}

function inputDate(value?: string | null) {
  if (!value || !/^\d{8}$/.test(value)) {
    return "";
  }
  return `${value.slice(0, 4)}-${value.slice(4, 6)}-${value.slice(6, 8)}`;
}

function formatDate(value?: string | null) {
  return inputDate(value) || "--";
}

function formatNumber(value?: number | null, digits = 2) {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return "--";
  }
  return value.toFixed(digits);
}

function formatPercent(value?: number | null) {
  const formatted = formatNumber(value);
  return formatted === "--" ? formatted : `${formatted}%`;
}

function formatMoney(value?: number | null) {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return "--";
  }
  const absolute = Math.abs(value);
  if (absolute >= 100_000_000) {
    return `${(value / 100_000_000).toFixed(2)}亿`;
  }
  if (absolute >= 10_000) {
    return `${(value / 10_000).toFixed(2)}万`;
  }
  return value.toFixed(2);
}

function amountTone(value?: number | null) {
  if (value === null || value === undefined || !Number.isFinite(value) || value === 0) {
    return "scene-layer-value-flat";
  }
  return value > 0 ? "scene-layer-value-up" : "scene-layer-value-down";
}

export default function DragonTigerMarketPanel({
  sourcePath,
  referenceTradeDate,
}: DragonTigerMarketPanelProps) {
  const [viewDate, setViewDate] = useState(() => inputDate(referenceTradeDate));
  const [data, setData] = useState<DragonTigerMarketData | null>(null);
  const [loading, setLoading] = useState(false);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState("");
  const [notice, setNotice] = useState("");
  const [selectedStock, setSelectedStock] = useState<{
    tsCode: string;
    name: string;
    tradeDate: string;
  } | null>(null);

  async function loadDragonTiger(requestedDate = viewDate) {
    if (!sourcePath.trim()) {
      return;
    }
    setLoading(true);
    setError("");
    setNotice("");
    try {
      const next = await getDragonTigerMarketData({
        sourcePath,
        referenceTradeDate: compactDate(requestedDate) || undefined,
      });
      setData(next);
      if (next.resolved_trade_date) {
        setViewDate(inputDate(next.resolved_trade_date));
      }
    } catch (loadError) {
      setData(null);
      setError(`读取龙虎榜失败: ${String(loadError)}`);
    } finally {
      setLoading(false);
    }
  }

  async function refreshLatestDragonTiger() {
    if (!sourcePath.trim()) {
      return;
    }
    const settings = readStoredDragonTigerDownloadSettings();
    if (!settings.token) {
      setError("快捷刷新需要 Tushare Token，请先到“数据下载”页面填写并保存。");
      setNotice("");
      return;
    }

    setRefreshing(true);
    setError("");
    setNotice("");
    try {
      const startDate =
        data?.latest_sync_trade_date || compactDate(settings.startDate) || "20050101";
      const result = await runDragonTigerDownload({
        downloadId: `dragon-tiger-quick-${Date.now()}`,
        sourcePath: sourcePath.trim(),
        token: settings.token,
        startDate,
        endDate: "today",
        retryTimes: settings.retryTimes,
        limitCallsPerMin: settings.limitCallsPerMin,
      });
      await loadDragonTiger("");
      setNotice(
        result.summary.successCount > 0
          ? `已同步 ${result.summary.successCount} 个交易日，并切换到最新龙虎榜。`
          : "已检查最新数据，当前没有需要补充的交易日。",
      );
    } catch (refreshError) {
      setError(`刷新龙虎榜最新数据失败: ${String(refreshError)}`);
    } finally {
      setRefreshing(false);
    }
  }

  useEffect(() => {
    if (!sourcePath.trim()) {
      return;
    }
    const requestedDate = inputDate(referenceTradeDate);
    if (requestedDate) {
      setViewDate(requestedDate);
    }
    void loadDragonTiger(requestedDate);
    // referenceTradeDate changes only after the main analysis resolves.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [sourcePath, referenceTradeDate]);

  const navigationItems = useMemo(() => {
    const seen = new Set<string>();
    return (data?.top_list ?? [])
      .filter((item) => {
        if (seen.has(item.ts_code)) {
          return false;
        }
        seen.add(item.ts_code);
        return true;
      })
      .map((item) => ({
        tsCode: item.ts_code,
        tradeDate: data?.resolved_trade_date ?? undefined,
        sourcePath: sourcePath.trim() || undefined,
        name: item.name,
      }));
  }, [data, sourcePath]);

  return (
    <section className="scene-layer-card market-analysis-main-card dragon-tiger-card">
      <div className="dragon-tiger-heading">
        <div>
          <h2 className="scene-layer-title">龙虎榜</h2>
          <p className="scene-layer-caption">
            点击股票查看当日买卖席位与历史上榜记录；行情入口保留在个股龙虎榜详情中。
          </p>
        </div>
        <div className="dragon-tiger-view-controls">
          <label className="scene-layer-field">
            <span>查看日期</span>
            <input
              type="date"
              value={viewDate}
              list="dragon-tiger-trade-dates"
              onChange={(event) => setViewDate(event.target.value)}
            />
          </label>
          <datalist id="dragon-tiger-trade-dates">
            {(data?.available_trade_dates ?? []).map((date) => (
              <option key={date} value={inputDate(date)} />
            ))}
          </datalist>
          <button
            type="button"
            className="scene-layer-primary-btn"
            disabled={loading || refreshing || !sourcePath.trim()}
            onClick={() => void loadDragonTiger()}
          >
            {loading ? "读取中..." : "查看龙虎榜"}
          </button>
          <button
            type="button"
            className="scene-layer-secondary-btn dragon-tiger-refresh-btn"
            disabled={loading || refreshing || !sourcePath.trim()}
            onClick={() => void refreshLatestDragonTiger()}
            title="使用数据下载页已保存的 Token 增量同步至最新交易日"
          >
            {refreshing ? "更新中..." : "刷新最新数据"}
          </button>
        </div>
      </div>

      {error ? <div className="scene-layer-error">{error}</div> : null}
      {notice ? <div className="dragon-tiger-notice">{notice}</div> : null}

      {!data?.db_exists ? (
        <div className="dragon-tiger-empty">
          尚未发现 dragon_tiger.db，请先到数据下载页面同步龙虎榜数据。
        </div>
      ) : (
        <>
          <div className="scene-layer-summary-grid scene-layer-summary-grid-market">
            <div className="scene-layer-summary-item scene-layer-summary-item-kpi">
              <span>数据日期</span>
              <strong>{formatDate(data.resolved_trade_date)}</strong>
            </div>
            <div className="scene-layer-summary-item scene-layer-summary-item-kpi">
              <span>上榜股票</span>
              <strong>{data.summary.stock_count} 只</strong>
            </div>
            <div className="scene-layer-summary-item scene-layer-summary-item-kpi">
              <span>榜单买入</span>
              <strong>{formatMoney(data.summary.total_l_buy)}</strong>
            </div>
            <div className="scene-layer-summary-item scene-layer-summary-item-kpi">
              <span>榜单卖出</span>
              <strong>{formatMoney(data.summary.total_l_sell)}</strong>
            </div>
            <div className="scene-layer-summary-item scene-layer-summary-item-kpi">
              <span>净买入</span>
              <strong className={amountTone(data.summary.total_net_amount)}>
                {formatMoney(data.summary.total_net_amount)}
              </strong>
            </div>
          </div>

          <details className="dragon-tiger-table-section" open>
            <summary>龙虎榜每日明细（{data.top_list.length} 行）</summary>
            <div className="scene-layer-contrib-table-wrap dragon-tiger-table-wrap">
              <table className="scene-layer-contrib-table dragon-tiger-list-table">
                <thead>
                  <tr>
                    <th>#</th>
                    <th>代码</th>
                    <th>名称</th>
                    <th>涨跌幅</th>
                    <th>换手率</th>
                    <th>榜单买入</th>
                    <th>榜单卖出</th>
                    <th>净买入</th>
                    <th>净买占比</th>
                    <th>上榜理由</th>
                  </tr>
                </thead>
                <tbody>
                  {data.top_list.map((item, index) => (
                    <tr key={`${item.ts_code}-${item.reason}-${index}`}>
                      <td>{index + 1}</td>
                      <td>{item.ts_code}</td>
                      <td>
                        <button
                          type="button"
                          className="scene-layer-market-stock-link"
                          title={`查看 ${item.name} 详情`}
                          onClick={() =>
                            setSelectedStock({
                              tsCode: item.ts_code,
                              name: item.name,
                              tradeDate: data.resolved_trade_date || item.trade_date,
                            })
                          }
                        >
                          {item.name}
                        </button>
                      </td>
                      <td className={amountTone(item.pct_change)}>{formatPercent(item.pct_change)}</td>
                      <td>{formatPercent(item.turnover_rate)}</td>
                      <td>{formatMoney(item.l_buy)}</td>
                      <td>{formatMoney(item.l_sell)}</td>
                      <td className={amountTone(item.net_amount)}>{formatMoney(item.net_amount)}</td>
                      <td className={amountTone(item.net_rate)}>{formatPercent(item.net_rate)}</td>
                      <td title={item.reason}>{item.reason}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </details>
        </>
      )}

      {selectedStock ? (
        <DragonTigerStockDetailModal
          sourcePath={sourcePath.trim()}
          tsCode={selectedStock.tsCode}
          name={selectedStock.name}
          tradeDate={selectedStock.tradeDate}
          navigationItems={navigationItems}
          onClose={() => setSelectedStock(null)}
        />
      ) : null}
    </section>
  );
}
