import { useEffect, useState } from "react";
import MarketMonitorRealtimeTab from "./MarketMonitorRealtimeTab";
import MarketSimulationTab from "./MarketSimulationTab";
import "./css/MarketMonitorPage.css";

type MarketMonitorTabKey = "realtime" | "simulation";

const MARKET_MONITOR_TAB_STATE_KEY = "lh_market_monitor_route_tab_v1";

function readStoredTab() {
  if (typeof window === "undefined") {
    return "realtime" as MarketMonitorTabKey;
  }
  const raw = window.sessionStorage.getItem(MARKET_MONITOR_TAB_STATE_KEY);
  return raw === "simulation" ? "simulation" : "realtime";
}

export default function MarketMonitorPage() {
  const [activeTab, setActiveTab] = useState<MarketMonitorTabKey>(readStoredTab);

  useEffect(() => {
    if (typeof window !== "undefined") {
      window.sessionStorage.setItem(MARKET_MONITOR_TAB_STATE_KEY, activeTab);
    }
  }, [activeTab]);

  return (
    <div className="market-monitor-page">
      <section className="market-monitor-tab-shell">
        <div className="market-monitor-route-tabs" role="tablist" aria-label="盘中监控">
          <button
            className={
              activeTab === "realtime"
                ? "market-monitor-route-tab is-active"
                : "market-monitor-route-tab"
            }
            type="button"
            role="tab"
            aria-selected={activeTab === "realtime"}
            onClick={() => setActiveTab("realtime")}
          >
            实时监控
          </button>
          <button
            className={
              activeTab === "simulation"
                ? "market-monitor-route-tab is-active"
                : "market-monitor-route-tab"
            }
            type="button"
            role="tab"
            aria-selected={activeTab === "simulation"}
            onClick={() => setActiveTab("simulation")}
          >
            预演买点
          </button>
        </div>

        {activeTab === "realtime" ? <MarketMonitorRealtimeTab /> : null}
        {activeTab === "simulation" ? <MarketSimulationTab /> : null}
      </section>
    </div>
  );
}
