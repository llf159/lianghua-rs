import { useEffect, useState } from "react";
import {
  Outlet,
  useOutletContext,
} from "react-router-dom";
import { ensureManagedSourcePath } from "../../apis/managedSource";
import { getStockPickOptions } from "../../apis/stockPick";
import { readStoredSourcePath } from "../../shared/storage";

export type StockPickOutletContext = {
  sourcePath: string;
  tradeDateOptions: string[];
  latestTradeDate: string;
  scoreTradeDateOptions: string[];
  latestScoreTradeDate: string;
  conceptOptions: string[];
  areaOptions: string[];
  industryOptions: string[];
  strategyOptions: string[];
  optionsLoading: boolean;
};

export function useStockPickOutletContext() {
  return useOutletContext<StockPickOutletContext>();
}

export default function StockPickPage() {
  const [sourcePath, setSourcePath] = useState(() => readStoredSourcePath());
  const [tradeDateOptions, setTradeDateOptions] = useState<string[]>([]);
  const [latestTradeDate, setLatestTradeDate] = useState("");
  const [scoreTradeDateOptions, setScoreTradeDateOptions] = useState<string[]>(
    [],
  );
  const [latestScoreTradeDate, setLatestScoreTradeDate] = useState("");
  const [conceptOptions, setConceptOptions] = useState<string[]>([]);
  const [areaOptions, setAreaOptions] = useState<string[]>([]);
  const [industryOptions, setIndustryOptions] = useState<string[]>([]);
  const [strategyOptions, setStrategyOptions] = useState<string[]>([]);
  const [optionsLoading, setOptionsLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    let cancelled = false;

    const loadOptions = async () => {
      setOptionsLoading(true);
      setError("");
      try {
        const resolvedSourcePath = await ensureManagedSourcePath();
        const options = await getStockPickOptions(resolvedSourcePath);
        if (cancelled) {
          return;
        }
        setSourcePath(resolvedSourcePath);
        setTradeDateOptions(options.trade_date_options ?? []);
        setLatestTradeDate(
          options.latest_trade_date ?? options.trade_date_options?.at(-1) ?? "",
        );
        setScoreTradeDateOptions(options.score_trade_date_options ?? []);
        setLatestScoreTradeDate(
          options.latest_score_trade_date ??
            options.score_trade_date_options?.at(-1) ??
            "",
        );
        setConceptOptions(options.concept_options ?? []);
        setAreaOptions(options.area_options ?? []);
        setIndustryOptions(options.industry_options ?? []);
        setStrategyOptions(options.strategy_options ?? []);
      } catch (loadError) {
        if (!cancelled) {
          setTradeDateOptions([]);
          setLatestTradeDate("");
          setScoreTradeDateOptions([]);
          setLatestScoreTradeDate("");
          setConceptOptions([]);
          setAreaOptions([]);
          setIndustryOptions([]);
          setStrategyOptions([]);
          setError(`读取选股配置失败: ${String(loadError)}`);
        }
      } finally {
        if (!cancelled) {
          setOptionsLoading(false);
        }
      }
    };

    void loadOptions();
    return () => {
      cancelled = true;
    };
  }, []);

  return (
    <div className="stock-pick-page">
      {error ? (
        <section className="stock-pick-card">
          <div className="stock-pick-message stock-pick-message-error">{error}</div>
        </section>
      ) : null}

      <Outlet
        context={{
          sourcePath,
          tradeDateOptions,
          latestTradeDate,
          scoreTradeDateOptions,
          latestScoreTradeDate,
          conceptOptions,
          areaOptions,
          industryOptions,
          strategyOptions,
          optionsLoading,
        }}
      />
    </div>
  );
}
