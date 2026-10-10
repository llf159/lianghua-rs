import { useState } from "react";
import {
  amount,
  percent,
  tone,
  type MarketGroup,
} from "../../../shared/intradayMarketGroups";
import "../css/IntradayMarketGroupTable.css";

export default function IntradayMarketGroupTable({
  groups,
  selection,
  onSelect,
}: {
  groups: MarketGroup[];
  selection: { name: string; kind: MarketGroup["kind"] } | null;
  onSelect: (group: MarketGroup) => void;
}) {
  const [visibleCount, setVisibleCount] = useState(20);
  return (
    <div className="intraday-group-scroll">
      <table className="intraday-group-table">
        <thead>
          <tr>
            <th>组合</th>
            <th>等权涨幅</th>
            <th>上涨 / 成分</th>
            <th>领涨股</th>
          </tr>
        </thead>
        <tbody>
          {groups.slice(0, visibleCount).map((group, index) => (
            <tr
              key={group.name}
              className={
                selection?.name === group.name && selection.kind === group.kind
                  ? "is-selected"
                  : ""
              }
            >
              <td>
                <button
                  className="intraday-group-select"
                  onClick={() => onSelect(group)}
                  aria-pressed={
                    selection?.name === group.name &&
                    selection.kind === group.kind
                  }
                >
                  <span className="intraday-group-position">{index + 1}</span>
                  {group.name}
                </button>
              </td>
              <td
                className={tone(group.change)}
                title={`中位数 ${percent(group.median)} · 成交额 ${amount(group.amount)}`}
              >
                {percent(group.change)}
              </td>
              <td>
                <span className="intraday-group-breadth">
                  <span
                    style={{
                      width: `${(group.rising / group.rows.length) * 100}%`,
                    }}
                  />
                </span>
                <span>
                  {group.rising} / {group.rows.length}
                </span>
              </td>
              <td>
                <span>{group.leader.name}</span>
                <small className={tone(group.leader.realtime_change_pct)}>
                  {percent(group.leader.realtime_change_pct)}
                </small>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      {groups.length === 0 ? (
        <div className="intraday-group-empty">等待有效行情与分类信息</div>
      ) : null}
      {groups.length > visibleCount ? (
        <button
          className="intraday-group-more"
          onClick={() => setVisibleCount((count) => count + 20)}
        >
          已显示 {visibleCount} / {groups.length}个组合 · 查看更多
        </button>
      ) : null}
    </div>
  );
}
