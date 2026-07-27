import { useEffect, useState } from "react";
import {
  getDragonTigerSeatStatistics,
  type DragonTigerSeatStatisticsData,
} from "../../../apis/strategyTrigger";
import DetailsLink from "../../../shared/DetailsLink";
import { splitTsCode } from "../../../shared/stockCode";

type DragonTigerSeatStatisticsModalProps = {
  sourcePath: string;
  exalter: string;
  onClose: () => void;
};

const RECENT_RECORD_PAGE_SIZE = 20;

function formatDate(value?: string | null) {
  if (!value || !/^\d{8}$/.test(value)) {
    return value || "--";
  }
  return `${value.slice(0, 4)}-${value.slice(4, 6)}-${value.slice(6, 8)}`;
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

export default function DragonTigerSeatStatisticsModal({
  sourcePath,
  exalter,
  onClose,
}: DragonTigerSeatStatisticsModalProps) {
  const [data, setData] = useState<DragonTigerSeatStatisticsData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [recentPage, setRecentPage] = useState(1);

  useEffect(() => {
    let cancelled = false;
    void getDragonTigerSeatStatistics({ sourcePath, exalter })
      .then((next) => {
        if (!cancelled) {
          setData(next);
        }
      })
      .catch((loadError) => {
        if (!cancelled) {
          setError(`读取席位统计失败: ${String(loadError)}`);
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
  }, [exalter, sourcePath]);

  const recentPageCount = Math.max(
    1,
    Math.ceil((data?.recent_records.length ?? 0) / RECENT_RECORD_PAGE_SIZE),
  );
  const visibleRecentRecords = (data?.recent_records ?? []).slice(
    (recentPage - 1) * RECENT_RECORD_PAGE_SIZE,
    recentPage * RECENT_RECORD_PAGE_SIZE,
  );

  return (
    <div className="dragon-tiger-seat-stat-backdrop" onClick={onClose} role="presentation">
      <section
        className="dragon-tiger-seat-stat-modal"
        role="dialog"
        aria-modal="true"
        aria-label={`${exalter} 席位统计`}
        onClick={(event) => event.stopPropagation()}
      >
        <header className="dragon-tiger-seat-stat-header">
          <div>
            <span>营业部 / 机构席位统计</span>
            <h3>{data?.exalter || exalter}</h3>
          </div>
          <button type="button" className="dragon-tiger-detail-text-btn" onClick={onClose}>
            返回
          </button>
        </header>

        <div className="dragon-tiger-seat-stat-body">
          {loading ? <div className="dragon-tiger-detail-state">席位统计加载中...</div> : null}
          {error ? <div className="scene-layer-error">{error}</div> : null}

          {!loading && data ? (
            <>
              <div className="dragon-tiger-seat-stat-summary">
                <div>
                  <span>上榜记录</span>
                  <strong>{data.summary.appearance_count} 条</strong>
                  <small>{data.summary.trade_date_count} 个交易日</small>
                </div>
                <div>
                  <span>参与股票</span>
                  <strong>{data.summary.stock_count} 只</strong>
                  <small>
                    买方 {data.summary.buy_count} 次 · 卖方 {data.summary.sell_count} 次
                  </small>
                </div>
                <div>
                  <span>买入合计</span>
                  <strong>{formatMoney(data.summary.total_buy)}</strong>
                </div>
                <div>
                  <span>卖出合计</span>
                  <strong>{formatMoney(data.summary.total_sell)}</strong>
                </div>
                <div>
                  <span>累计净买入</span>
                  <strong className={amountTone(data.summary.total_net_buy)}>
                    {formatMoney(data.summary.total_net_buy)}
                  </strong>
                </div>
              </div>

              <section className="dragon-tiger-seat-stat-section">
                <div className="dragon-tiger-history-heading">
                  <div>
                    <span>偏好股票</span>
                    <strong>按上榜次数排序</strong>
                  </div>
                  <small>点击股票名称查看行情</small>
                </div>
                <div className="dragon-tiger-detail-table-wrap">
                  <table className="dragon-tiger-detail-table dragon-tiger-seat-favorite-table">
                    <thead>
                      <tr>
                        <th>股票</th>
                        <th>上榜次数</th>
                        <th>买入额</th>
                        <th>卖出额</th>
                        <th>净买入</th>
                      </tr>
                    </thead>
                    <tbody>
                      {data.favorite_stocks.map((item) => (
                        <tr key={item.ts_code}>
                          <td>
                            <DetailsLink
                              className="dragon-tiger-stat-name-link"
                              tsCode={splitTsCode(item.ts_code)}
                              sourcePath={sourcePath}
                              title={`查看 ${item.name} 行情`}
                            >
                              {item.name}
                            </DetailsLink>
                            <small>{item.ts_code}</small>
                          </td>
                          <td>{item.appearance_count}</td>
                          <td>{formatMoney(item.total_buy)}</td>
                          <td>{formatMoney(item.total_sell)}</td>
                          <td className={amountTone(item.total_net_buy)}>
                            {formatMoney(item.total_net_buy)}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </section>

              <section className="dragon-tiger-seat-stat-section">
                <div className="dragon-tiger-history-heading">
                  <div>
                    <span>近期上榜</span>
                    <strong>共 {data.summary.appearance_count} 条记录</strong>
                  </div>
                  <small>
                    {data.summary.appearance_count > data.recent_records.length
                      ? `仅载入最近 ${data.recent_records.length} 条，`
                      : ""}
                    每页 {RECENT_RECORD_PAGE_SIZE} 条
                  </small>
                </div>
                <div className="dragon-tiger-detail-table-wrap">
                  <table className="dragon-tiger-detail-table dragon-tiger-seat-record-table">
                    <thead>
                      <tr>
                        <th>日期</th>
                        <th>股票</th>
                        <th>榜单</th>
                        <th>买入额</th>
                        <th>卖出额</th>
                        <th>净额</th>
                        <th>上榜原因</th>
                      </tr>
                    </thead>
                    <tbody>
                      {visibleRecentRecords.map((item, index) => (
                        <tr
                          key={`${item.trade_date}-${item.ts_code}-${item.side}-${item.reason}-${index}`}
                        >
                          <td>{formatDate(item.trade_date)}</td>
                          <td>
                            <DetailsLink
                              className="dragon-tiger-stat-name-link"
                              tsCode={splitTsCode(item.ts_code)}
                              tradeDate={item.trade_date}
                              sourcePath={sourcePath}
                              title={`查看 ${item.name} 行情`}
                            >
                              {item.name}
                            </DetailsLink>
                            <small>{item.ts_code}</small>
                          </td>
                          <td>{item.side === "0" ? "买入榜" : "卖出榜"}</td>
                          <td>{formatMoney(item.buy)}</td>
                          <td>{formatMoney(item.sell)}</td>
                          <td className={amountTone(item.net_buy)}>
                            {formatMoney(item.net_buy)}
                          </td>
                          <td title={item.reason}>{item.reason}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                  {recentPageCount > 1 ? (
                    <div className="dragon-tiger-pagination">
                      <button
                        type="button"
                        disabled={recentPage <= 1}
                        onClick={() => setRecentPage((page) => Math.max(1, page - 1))}
                      >
                        上一页
                      </button>
                      <span>
                        {recentPage} / {recentPageCount}
                      </span>
                      <button
                        type="button"
                        disabled={recentPage >= recentPageCount}
                        onClick={() =>
                          setRecentPage((page) => Math.min(recentPageCount, page + 1))
                        }
                      >
                        下一页
                      </button>
                    </div>
                  ) : null}
                </div>
              </section>
            </>
          ) : null}
        </div>
      </section>
    </div>
  );
}
