import {
  HashRouter,
  Navigate,
  Route,
  Routes,
  useLocation,
} from "react-router-dom";
import type { Location } from "react-router-dom";
import { lazy, Suspense } from "react";
import PageDesktop from "./PageDesktop.tsx";
import "./App.css";

const pages = {
  AllMarketMonitor: lazy(() => import("./pages/desktop/AllMarketMonitorPage")),
  Backtest: lazy(() => import("./pages/desktop/BacktestPage")),
  ConceptStockPick: lazy(() => import("./pages/desktop/ConceptStockPickPage")),
  CyqChen: lazy(() => import("./pages/desktop/CyqChenPage")),
  DataImport: lazy(() => import("./pages/desktop/DataImportPage")),
  DataViewer: lazy(() => import("./pages/desktop/DataViewerPage")),
  DetailsLinked: lazy(() => import("./pages/desktop/DetailsLinkedPage")),
  DetailsLinkedOverlay: lazy(
    () => import("./shared/DetailsLinkedOverlayRoute"),
  ),
  DownloadCompute: lazy(() => import("./pages/desktop/DownloadComputePage")),
  ExpressionStockPick: lazy(
    () => import("./pages/desktop/ExpressionStockPickPage"),
  ),
  ExpressionValidationSamples: lazy(
    () => import("./pages/desktop/ExpressionValidationSamplesPage"),
  ),
  MarketAnalysis: lazy(() => import("./pages/desktop/MarketAnalysisPage")),
  OverviewRaw: lazy(() => import("./pages/desktop/OverviewRawPage")),
  OverviewScene: lazy(() => import("./pages/desktop/OverviewScenePage")),
  OverviewSimilarityRanking: lazy(
    () => import("./pages/desktop/OverviewSimilarityRankingPage"),
  ),
  RankingOverview: lazy(() => import("./pages/desktop/RankingOverviewPage")),
  SceneLayerBacktest: lazy(
    () => import("./pages/desktop/SceneLayerBacktestPage"),
  ),
  Settings: lazy(() => import("./pages/desktop/SettingsPage")),
  StockPick: lazy(() => import("./pages/desktop/StockPickPage")),
  StrategyDimensionResearch: lazy(
    () => import("./pages/desktop/StrategyDimensionResearchPage"),
  ),
  StrategyManage: lazy(() => import("./pages/desktop/StrategyManagePage")),
  StrategyPaperValidation: lazy(
    () => import("./pages/desktop/StrategyPaperValidationPage"),
  ),
  StrategyTrigger: lazy(() => import("./pages/desktop/StrategyTriggerPage")),
  StrategyTriggerSimilarity: lazy(
    () => import("./pages/desktop/StrategyTriggerSimilarityPage"),
  ),
  WatchObserve: lazy(() => import("./pages/desktop/WatchObservePage")),
};

type BackgroundLocationState = {
  backgroundLocation?: Location;
};

function LegacyDetailsRedirect() {
  const location = useLocation();
  return (
    <Navigate
      to={`/details-linked${location.search}`}
      replace
      state={location.state}
    />
  );
}

function AppRoutes() {
  const location = useLocation();
  const locationState =
    location.state && typeof location.state === "object"
      ? (location.state as BackgroundLocationState)
      : null;
  const backgroundLocation = locationState?.backgroundLocation;

  return (
    <>
      <Suspense fallback={<div>正在加载页面…</div>}>
        <Routes location={backgroundLocation ?? location}>
          <Route path="/" element={<PageDesktop />}>
            <Route index element={<Navigate to="/watch-observe" replace />} />
            <Route path="watch-observe" element={<pages.WatchObserve />} />
            <Route path="overview" element={<pages.RankingOverview />}>
              <Route index element={<Navigate to="/overview/raw" replace />} />
              <Route path="raw" element={<pages.OverviewRaw />} />
              <Route
                path="convolution"
                element={<pages.OverviewSimilarityRanking />}
              />
              <Route path="scene" element={<pages.OverviewScene />} />
            </Route>
            <Route path="details" element={<LegacyDetailsRedirect />} />
            <Route path="details-linked" element={<pages.DetailsLinked />} />
            <Route
              path="data-import"
              element={<Navigate to="/raw-data/data-import" replace />}
            />
            <Route
              path="data-viewer"
              element={<Navigate to="/raw-data/data-viewer" replace />}
            />
            <Route
              path="data-download"
              element={<Navigate to="/raw-data/download-compute" replace />}
            />
            <Route path="stock-pick" element={<pages.StockPick />}>
              <Route
                index
                element={<Navigate to="/stock-pick/expression" replace />}
              />
              <Route
                path="expression"
                element={<pages.ExpressionStockPick />}
              />
              <Route path="concept" element={<pages.ConceptStockPick />} />
            </Route>
            <Route
              path="cyq-chen"
              element={<Navigate to="/strategy/cyq-chen" replace />}
            />
            <Route path="strategy">
              <Route
                index
                element={<Navigate to="/strategy/rules" replace />}
              />
              <Route
                path="manage"
                element={<Navigate to="/strategy/rules" replace />}
              />
              <Route
                path="rules"
                element={<pages.StrategyManage view="rules" />}
              />
              <Route
                path="chip-change"
                element={<pages.StrategyManage view="chip" />}
              />
              <Route path="cyq-chen" element={<pages.CyqChen />} />
            </Route>
            <Route
              path="strategy-trigger-similarity"
              element={<pages.StrategyTriggerSimilarity />}
            />
            <Route path="settings" element={<pages.Settings />} />
            <Route path="raw-data">
              <Route
                index
                element={<Navigate to="/raw-data/data-import" replace />}
              />
              <Route path="data-import" element={<pages.DataImport />} />
              <Route path="data-viewer" element={<pages.DataViewer />} />
              <Route
                path="download-compute"
                element={<pages.DownloadCompute />}
              />
              <Route
                path="data-download"
                element={<Navigate to="/raw-data/download-compute" replace />}
              />
              <Route
                path="ranking-compute"
                element={<Navigate to="/raw-data/download-compute" replace />}
              />
              <Route
                path="strategy-manage"
                element={<Navigate to="/strategy/rules" replace />}
              />
            </Route>
            <Route
              path="intraday-monitor"
              element={<pages.AllMarketMonitor />}
            />
            <Route path="market-analysis" element={<pages.MarketAnalysis />} />
            <Route path="backtest" element={<pages.Backtest />}>
              <Route
                index
                element={<Navigate to="/backtest/strategy-trigger" replace />}
              />
              <Route
                path="strategy-trigger"
                element={<pages.StrategyTrigger />}
              />
              <Route
                path="strategy-paper-validation"
                element={<pages.StrategyPaperValidation />}
              />
              <Route
                path="scene-layer"
                element={<pages.SceneLayerBacktest />}
              />
              <Route
                path="correlation-orthogonality"
                element={<pages.StrategyDimensionResearch />}
              />
              <Route
                path="scene-layer/expression-validation-samples"
                element={<pages.ExpressionValidationSamples />}
              />
              <Route
                path="market-analysis"
                element={<Navigate to="/market-analysis" replace />}
              />
            </Route>
          </Route>
        </Routes>
      </Suspense>

      {backgroundLocation ? (
        <Suspense fallback={<div>正在加载详情页…</div>}>
          <Routes>
            <Route path="/details" element={<LegacyDetailsRedirect />} />
            <Route
              path="/details-linked"
              element={<pages.DetailsLinkedOverlay />}
            />
          </Routes>
        </Suspense>
      ) : null}
    </>
  );
}

export default function App() {
  return (
    <HashRouter>
      <AppRoutes />
    </HashRouter>
  );
}
