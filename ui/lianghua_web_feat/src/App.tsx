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
import RankingComputePage from './pages/desktop/RankingComputePage'
import SettingsPage from './pages/desktop/SettingsPage'
import StockPickPage from './pages/desktop/StockPickPage'
import ExpressionStockPickPage from './pages/desktop/ExpressionStockPickPage'
import ConceptStockPickPage from './pages/desktop/ConceptStockPickPage'
import StrategyManagePage from './pages/desktop/StrategyManagePage'
import WatchObservePage from './pages/desktop/WatchObservePage'
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
          <Route index element={<Navigate to="/raw-data/data-import" replace />} />
          <Route path="watch-observe" element={<WatchObservePage />} />
          <Route path="overview" element={<OverviewScenePage />} />
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
