import {
  HashRouter,
  Navigate,
  Route,
  Routes,
  useLocation,
} from 'react-router-dom'
import type { Location } from 'react-router-dom'
import PageDesktop from './PageDesktop.tsx'
import DataDownloadPage from './pages/desktop/DataDownloadPage'
import DataImportPage from './pages/desktop/DataImportPage'
import DataViewerPage from './pages/desktop/DataViewerPage'
import DetailsLinkedPage from './pages/desktop/DetailsLinkedPage'
import DetailsPage from './pages/desktop/DetailsPage'
import OverviewScenePage from './pages/desktop/OverviewScenePage'
import OverviewRawPage from './pages/desktop/OverviewRawPage'
import RankingOverviewPage from './pages/desktop/RankingOverviewPage'
import RankingComputePage from './pages/desktop/RankingComputePage'
import SettingsPage from './pages/desktop/SettingsPage'
import StockPickPage from './pages/desktop/StockPickPage'
import ExpressionStockPickPage from './pages/desktop/ExpressionStockPickPage'
import ConceptStockPickPage from './pages/desktop/ConceptStockPickPage'
import StrategyManagePage from './pages/desktop/StrategyManagePage'
import WatchObservePage from './pages/desktop/WatchObservePage'
import IntradayMonitorRealtimePage from './pages/desktop/IntradayMonitorRealtimePage'
import BacktestPage from './pages/desktop/BacktestPage'
import StrategyTriggerPage from './pages/desktop/StrategyTriggerPage'
import SceneLayerBacktestPage from './pages/desktop/SceneLayerBacktestPage'
import MarketAnalysisPage from './pages/desktop/MarketAnalysisPage'
import DetailsLinkedOverlayRoute from './shared/DetailsLinkedOverlayRoute'
import DetailsOverlayRoute from './shared/DetailsOverlayRoute'
import './App.css'

type BackgroundLocationState = {
  backgroundLocation?: Location
}

function AppRoutes() {
  const location = useLocation()
  const locationState =
    location.state && typeof location.state === 'object'
      ? (location.state as BackgroundLocationState)
      : null
  const backgroundLocation = locationState?.backgroundLocation

  return (
    <>
      <Routes location={backgroundLocation ?? location}>
        <Route path="/" element={<PageDesktop />}>
          <Route index element={<Navigate to="/watch-observe" replace />} />
          <Route path="watch-observe" element={<WatchObservePage />} />
          <Route path="overview" element={<RankingOverviewPage />}>
            <Route index element={<Navigate to="/overview/raw" replace />} />
            <Route path="raw" element={<OverviewRawPage />} />
            <Route path="scene" element={<OverviewScenePage />} />
          </Route>
          <Route path="details" element={<DetailsPage />} />
          <Route path="details-linked" element={<DetailsLinkedPage />} />
          <Route path="data-import" element={<Navigate to="/raw-data/data-import" replace />} />
          <Route path="data-viewer" element={<Navigate to="/raw-data/data-viewer" replace />} />
          <Route path="data-download" element={<Navigate to="/raw-data/data-download" replace />} />
          <Route path="stock-pick" element={<StockPickPage />}>
            <Route index element={<Navigate to="/stock-pick/expression" replace />} />
            <Route path="expression" element={<ExpressionStockPickPage />} />
            <Route path="concept" element={<ConceptStockPickPage />} />
          </Route>
          <Route path="settings" element={<SettingsPage />} />
          <Route path="raw-data">
            <Route index element={<Navigate to="/raw-data/data-import" replace />} />
            <Route path="data-import" element={<DataImportPage />} />
            <Route path="data-viewer" element={<DataViewerPage />} />
            <Route path="data-download" element={<DataDownloadPage />} />
            <Route path="ranking-compute" element={<RankingComputePage />} />
            <Route path="strategy-manage" element={<StrategyManagePage />} />
          </Route>
          <Route path="intraday-monitor">
            <Route index element={<Navigate to="/intraday-monitor/realtime-ranking" replace />} />
            <Route path="realtime-ranking" element={<IntradayMonitorRealtimePage />} />
          </Route>
          <Route path="backtest" element={<BacktestPage />}>
            <Route index element={<Navigate to="/backtest/strategy-trigger" replace />} />
            <Route path="strategy-trigger" element={<StrategyTriggerPage />} />
            <Route path="scene-layer" element={<SceneLayerBacktestPage />} />
            <Route path="market-analysis" element={<MarketAnalysisPage />} />
          </Route>
        </Route>
      </Routes>

      {backgroundLocation ? (
        <Routes>
          <Route path="/details" element={<DetailsOverlayRoute />} />
          <Route path="/details-linked" element={<DetailsLinkedOverlayRoute />} />
        </Routes>
      ) : null}
    </>
  )
}

export default function App() {
  return (
    <HashRouter>
      <AppRoutes />
    </HashRouter>
  )
}
