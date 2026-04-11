import {
  HashRouter,
  Navigate,
  Route,
  Routes,
} from 'react-router-dom'
import PageDesktop from './PageDesktop.tsx'
import DataDownloadPage from './pages/desktop/DataDownloadPage'
import DataImportPage from './pages/desktop/DataImportPage'
import DataViewerPage from './pages/desktop/DataViewerPage'
import RankingComputePage from './pages/desktop/RankingComputePage'
import SettingsPage from './pages/desktop/SettingsPage'
import StockPickPage from './pages/desktop/StockPickPage'
import ExpressionStockPickPage from './pages/desktop/ExpressionStockPickPage'
import ConceptStockPickPage from './pages/desktop/ConceptStockPickPage'
import StrategyManagePage from './pages/desktop/StrategyManagePage'
import './App.css'

function AppRoutes() {
  return (
    <Routes>
      <Route path="/" element={<PageDesktop />}>
        <Route index element={<Navigate to="/raw-data/data-import" replace />} />
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
  )
}

export default function App() {
  return (
    <HashRouter>
      <AppRoutes />
    </HashRouter>
  )
}
