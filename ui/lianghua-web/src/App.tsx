import {
  HashRouter,
  Navigate,
  Route,
  Routes,
  useLocation,
  type Location,
} from 'react-router-dom'
import PageDesktop from './PageDesktop.tsx'
import WatchObservePage from './pages/desktop/WatchObservePage.tsx'
import OverviewPage from './pages/desktop/OverviewPage.tsx'
import DetailsPage from './pages/desktop/DetailsPage.tsx'
import DataImportPage from './pages/desktop/DataImportPage.tsx'
import DataViewerPage from './pages/desktop/DataViewerPage.tsx'
import DataDownloadPage from './pages/desktop/DataDownloadPage.tsx'
import RankingComputePage from './pages/desktop/RankingComputePage.tsx'
import StockPickPage from './pages/desktop/StockPickPage.tsx'
import ExpressionStockPickPage from './pages/desktop/ExpressionStockPickPage.tsx'
import ConceptStockPickPage from './pages/desktop/ConceptStockPickPage.tsx'
import AdvancedStockPickPage from './pages/desktop/AdvancedStockPickPage.tsx'
import StrategyTriggerPage from './pages/desktop/StrategyTriggerPage.tsx'
import StrategyManagePage from './pages/desktop/StrategyManagePage.tsx'
import SettingsPage from './pages/desktop/SettingsPage.tsx'
import MarketMonitorPage from './pages/desktop/MarketMonitorPage.tsx'
import BacktestPage from './pages/desktop/BacktestPage.tsx'
import ReturnBacktestPage from './pages/desktop/ReturnBacktestPage.tsx'
import BoardAnalysisPage from './pages/desktop/BoardAnalysisPage.tsx'
import StrategyPerformanceBacktestPage from './pages/desktop/StrategyPerformanceBacktestPage.tsx'
import StrategyValidationBacktestPage from './pages/desktop/StrategyValidationBacktestPage.tsx'
import DetailsOverlayRoute from './shared/DetailsOverlayRoute.tsx'
import DetailsLinkedOverlayRoute from './shared/DetailsLinkedOverlayRoute.tsx'
import DetailsLinkedPage from './pages/desktop/DetailsLinkedPage.tsx'

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
          <Route path="overview" element={<OverviewPage />} />
          <Route path="details" element={<DetailsPage />} />
          <Route path="details-linked" element={<DetailsLinkedPage />} />
          <Route path="stock-pick" element={<StockPickPage />}>
            <Route index element={<Navigate to="/stock-pick/expression" replace />} />
            <Route path="expression" element={<ExpressionStockPickPage />} />
            <Route path="concept" element={<ConceptStockPickPage />} />
            <Route path="advanced" element={<AdvancedStockPickPage />} />
          </Route>
          <Route path="market-monitor" element={<MarketMonitorPage />} />
          <Route path="data-import" element={<Navigate to="/raw-data/data-import" replace />} />
          <Route path="raw-data">
            <Route index element={<Navigate to="/raw-data/data-import" replace />} />
            <Route path="data-import" element={<DataImportPage />} />
            <Route path="data-viewer" element={<DataViewerPage />} />
            <Route path="data-download" element={<DataDownloadPage />} />
            <Route path="ranking-compute" element={<RankingComputePage />} />
            <Route path="strategy-manage" element={<StrategyManagePage />} />
          </Route>
          <Route path="backtest" element={<BacktestPage />}>
            <Route index element={<Navigate to="/backtest/strategy-trigger" replace />} />
            <Route path="strategy-trigger" element={<StrategyTriggerPage />} />
            <Route path="return-analysis" element={<ReturnBacktestPage />} />
            <Route path="board-analysis" element={<BoardAnalysisPage />} />
            <Route path="strategy-performance" element={<StrategyPerformanceBacktestPage />} />
            <Route path="strategy-validation" element={<StrategyValidationBacktestPage />} />
            <Route path="score-segmentation" element={<Navigate to="/backtest/strategy-performance" replace />} />
          </Route>
          <Route path="settings" element={<SettingsPage />} />
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
