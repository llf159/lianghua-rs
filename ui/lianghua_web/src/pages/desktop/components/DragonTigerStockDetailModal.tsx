import { useEffect, useMemo, useRef, useState } from "react";
import {
  getDragonTigerStockDetail,
  type DragonTigerStockDetailData,
  type DragonTigerTopInstItem,
} from "../../../apis/strategyTrigger";
import DetailsLink from "../../../shared/DetailsLink";
import type { DetailsNavigationItem } from "../../../shared/detailsLinkState";
import { splitTsCode } from "../../../shared/stockCode";
import DragonTigerSeatStatisticsModal from "./DragonTigerSeatStatisticsModal";

type DragonTigerStockDetailModalProps = {
  sourcePath: string;
  tsCode: string;
  name: string;
  tradeDate: string;
  navigationItems: DetailsNavigationItem[];
  onClose: () => void;
};

const HISTORY_PAGE_SIZE = 12;

function formatDate(value?: string | null) {
  if (!value || !/^\d{8}$/.test(value)) {
    return value || "--";
  }
  return `${value.slice(0, 4)}-${value.slice(4, 6)}-${value.slice(6, 8)}`;
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

function SeatTable({
  title,
  tone,
  rows,
  onSelectSeat,
}: {
  title: string;
  tone: "buy" | "sell";
  rows: DragonTigerTopInstItem[];
  onSelectSeat: (exalter: string) => void;
}) {
  return (
    <section className={`dragon-tiger-seat-card dragon-tiger-seat-card-${tone}`}>
      <div className="dragon-tiger-seat-title">
        <strong>{title}</strong>
        <span>{rows.length} 个席位</span>
      </div>
      {rows.length === 0 ? (
        <div className="dragon-tiger-seat-empty">暂无席位数据</div>
      ) : (
        <div className="dragon-tiger-detail-table-wrap">
          <table className="dragon-tiger-detail-table dragon-tiger-seat-table">
            <thead>
              <tr>
                <th>营业部 / 机构</th>
                <th>买入额</th>
                <th>卖出额</th>
                <th>净额</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((item, index) => (
                <tr key={`${item.exalter}-${item.side}-${index}`}>
                  <td title={`查看 ${item.exalter} 席位统计`}>
                    <button
                      type="button"
                      className="dragon-tiger-seat-stat-trigger"
                      onClick={() => onSelectSeat(item.exalter)}
                    >
                      <span>{item.exalter}</span>
                    </button>
                  </td>
                  <td>{formatMoney(item.buy)}</td>
                  <td>{formatMoney(item.sell)}</td>
                  <td className={amountTone(item.net_buy)}>{formatMoney(item.net_buy)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}

export default function DragonTigerStockDetailModal({
  sourcePath,
  tsCode,
  name,
  tradeDate,
  navigationItems,
  onClose,
}: DragonTigerStockDetailModalProps) {
  const [data, setData] = useState<DragonTigerStockDetailData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [activeTradeDate, setActiveTradeDate] = useState(tradeDate);
  const [selectedSeat, setSelectedSeat] = useState("");
  const [historyPage, setHistoryPage] = useState(1);
  const detailBodyRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    let cancelled = false;

    void getDragonTigerStockDetail({ sourcePath, tsCode, tradeDate: activeTradeDate })
      .then((next) => {
        if (!cancelled) {
          setData(next);
        }
      })
      .catch((loadError) => {
        if (!cancelled) {
          setError(`读取个股龙虎榜详情失败: ${String(loadError)}`);
        }
      })
      .finally(() => {
        if (!cancelled) {
          setLoading(false);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [activeTradeDate, sourcePath, tsCode]);

  function openTradeDate(nextTradeDate: string) {
    if (nextTradeDate === activeTradeDate) {
      return;
    }
    setLoading(true);
    setError("");
    setData(null);
    setActiveTradeDate(nextTradeDate);
    setHistoryPage(1);
    detailBodyRef.current?.scrollTo({ top: 0, behavior: "smooth" });
  }

  const reasonSections = useMemo(
    () =>
      (data?.current_list ?? []).map((item, index) => {
        let seats = (data?.seats ?? []).filter((seat) => seat.reason === item.reason);
        if (seats.length === 0 && data?.current_list.length === 1) {
          seats = data.seats;
        }
        return {
          key: `${item.reason}-${index}`,
          item,
          buySeats: seats.filter((seat) => seat.side === "0"),
          sellSeats: seats.filter((seat) => seat.side === "1"),
        };
      }),
    [data],
  );

  const headline = data?.current_list[0];
  const resolvedName = data?.name || name;
  const resolvedDate = data?.resolved_trade_date || activeTradeDate;
  const historyPageCount = Math.max(
    1,
    Math.ceil((data?.history.length ?? 0) / HISTORY_PAGE_SIZE),
  );
  const visibleHistory = (data?.history ?? []).slice(
    (historyPage - 1) * HISTORY_PAGE_SIZE,
    historyPage * HISTORY_PAGE_SIZE,
  );

  return (
    <div className="dragon-tiger-detail-backdrop" onClick={onClose} role="presentation">
      <section
        className="dragon-tiger-detail-modal"
        role="dialog"
        aria-modal="true"
        aria-label={`${resolvedName} 龙虎榜详情`}
        onClick={(event) => event.stopPropagation()}
      >
        <header className="dragon-tiger-detail-header">
          <div className="dragon-tiger-detail-header-main">
            <div className="dragon-tiger-detail-kicker">个股龙虎榜详情</div>
            <div className="dragon-tiger-detail-headline">
              <div className="dragon-tiger-detail-stock">
                <h3>{resolvedName}</h3>
                <span>{tsCode}</span>
              </div>
              <div className="dragon-tiger-detail-date">
                <span>上榜日期</span>
                <strong>{formatDate(resolvedDate)}</strong>
              </div>
            </div>
            <div className="dragon-tiger-detail-header-stats">
              <span>
                涨跌幅
                <strong className={amountTone(headline?.pct_change)}>
                  {formatPercent(headline?.pct_change)}
                </strong>
              </span>
              <span>
                榜单买入<strong>{formatMoney(headline?.l_buy)}</strong>
              </span>
              <span>
                榜单卖出<strong>{formatMoney(headline?.l_sell)}</strong>
              </span>
              <span>
                净买入
                <strong className={amountTone(headline?.net_amount)}>
                  {formatMoney(headline?.net_amount)}
                </strong>
              </span>
              <span>
                换手率<strong>{formatPercent(headline?.turnover_rate)}</strong>
              </span>
              <span>
                收盘<strong>{formatNumber(headline?.close)}</strong>
              </span>
              <span>此前上榜 {data?.history_trade_count ?? 0} 个交易日</span>
            </div>
          </div>
          <div className="dragon-tiger-detail-header-actions">
            {activeTradeDate !== tradeDate ? (
              <button
                type="button"
                className="dragon-tiger-detail-text-btn"
                onClick={() => openTradeDate(tradeDate)}
              >
                返回最新明细
              </button>
            ) : null}
            <button type="button" className="dragon-tiger-detail-text-btn" onClick={onClose}>
              关闭
            </button>
          </div>
        </header>

        <div className="dragon-tiger-detail-body" ref={detailBodyRef}>
          {loading ? <div className="dragon-tiger-detail-state">详情加载中...</div> : null}
          {error ? <div className="scene-layer-error">{error}</div> : null}

          {!loading && data ? (
            <>
              <div className="dragon-tiger-reason-list">
                {reasonSections.map(({ key, item, buySeats, sellSeats }) => (
                  <section className="dragon-tiger-reason-section" key={key}>
                    <div className="dragon-tiger-reason-heading">
                      <div>
                        <span>上榜原因</span>
                        <strong>{item.reason}</strong>
                      </div>
                      <div className="dragon-tiger-reason-net">
                        净买入
                        <strong className={amountTone(item.net_amount)}>
                          {formatMoney(item.net_amount)}
                        </strong>
                      </div>
                    </div>
                    <div className="dragon-tiger-seat-grid">
                      <SeatTable
                        title="买入金额最大的前五席位"
                        tone="buy"
                        rows={buySeats}
                        onSelectSeat={setSelectedSeat}
                      />
                      <SeatTable
                        title="卖出金额最大的前五席位"
                        tone="sell"
                        rows={sellSeats}
                        onSelectSeat={setSelectedSeat}
                      />
                    </div>
                  </section>
                ))}
              </div>

              <section className="dragon-tiger-history-section">
                <div className="dragon-tiger-history-heading">
                  <div>
                    <span>历史上榜</span>
                    <strong>
                      共 {data.history_record_count} 条 · {data.history_trade_count} 个交易日
                    </strong>
                  </div>
                  <small>
                    {data.history_record_count > data.history.length
                      ? `仅载入最近 ${data.history.length} 条，`
                      : ""}
                    每页 {HISTORY_PAGE_SIZE} 条
                  </small>
                </div>
                {data.history.length === 0 ? (
                  <div className="dragon-tiger-detail-state">暂无更早的上榜记录</div>
                ) : (
                  <div className="dragon-tiger-detail-table-wrap dragon-tiger-history-table-wrap">
                    <table className="dragon-tiger-detail-table dragon-tiger-history-table">
                      <thead>
                        <tr>
                          <th>上榜日期</th>
                          <th>涨跌幅</th>
                          <th>换手率</th>
                          <th>买入额</th>
                          <th>卖出额</th>
                          <th>净买入</th>
                          <th>上榜原因</th>
                        </tr>
                      </thead>
                      <tbody>
                        {visibleHistory.map((item, index) => (
                          <tr key={`${item.trade_date}-${item.reason}-${index}`}>
                            <td>
                              <button
                                type="button"
                                className="dragon-tiger-history-date-link"
                                title={`查看 ${formatDate(item.trade_date)} 当日明细`}
                                onClick={() => openTradeDate(item.trade_date)}
                              >
                                {formatDate(item.trade_date)}
                              </button>
                            </td>
                            <td className={amountTone(item.pct_change)}>
                              {formatPercent(item.pct_change)}
                            </td>
                            <td>{formatPercent(item.turnover_rate)}</td>
                            <td>{formatMoney(item.l_buy)}</td>
                            <td>{formatMoney(item.l_sell)}</td>
                            <td className={amountTone(item.net_amount)}>
                              {formatMoney(item.net_amount)}
                            </td>
                            <td title={item.reason}>{item.reason}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                    {historyPageCount > 1 ? (
                      <div className="dragon-tiger-pagination">
                        <button
                          type="button"
                          disabled={historyPage <= 1}
                          onClick={() => setHistoryPage((page) => Math.max(1, page - 1))}
                        >
                          上一页
                        </button>
                        <span>
                          {historyPage} / {historyPageCount}
                        </span>
                        <button
                          type="button"
                          disabled={historyPage >= historyPageCount}
                          onClick={() =>
                            setHistoryPage((page) => Math.min(historyPageCount, page + 1))
                          }
                        >
                          下一页
                        </button>
                      </div>
                    ) : null}
                  </div>
                )}
              </section>
            </>
          ) : null}
        </div>

        <DetailsLink
          className="dragon-tiger-quote-fab"
          tsCode={splitTsCode(tsCode)}
          tradeDate={resolvedDate}
          sourcePath={sourcePath}
          navigationItems={navigationItems}
          title={`查看 ${resolvedName} 行情详情`}
        >
          行情
        </DetailsLink>

        {selectedSeat ? (
          <DragonTigerSeatStatisticsModal
            sourcePath={sourcePath}
            exalter={selectedSeat}
            onClose={() => setSelectedSeat("")}
          />
        ) : null}
      </section>
    </div>
  );
}
