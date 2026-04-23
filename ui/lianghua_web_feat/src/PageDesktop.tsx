import { NavLink, Outlet, useLocation } from 'react-router-dom'
import { useEffect, useRef, useState } from 'react'
import { preloadWatchObserveRows } from './apis/watchObserve'
import './Desktop.css'

const menuList = [
  { path: '/watch-observe', label: '自选观察' },
  { path: '/details', label: '个股详情' },
]

const overviewSubRoutes = [
  { path: '/overview/raw', label: '原始排名' },
  { path: '/overview/scene', label: '场景排名' },
]

const intradayMonitorSubRoutes = [
  { path: '/intraday-monitor/realtime-ranking', label: '排名实时' },
  { path: '/intraday-monitor/custom-monitor', label: '自定义监控' },
]

const settingsMenuItem = { path: '/settings', label: '设置' }

const backtestSubRoutes = [
  { path: '/backtest/strategy-trigger', label: '策略触发统计' },
  { path: '/backtest/strategy-paper-validation', label: '策略模拟盘验证' },
  { path: '/backtest/scene-layer', label: '策略回测' },
  { path: '/backtest/market-analysis', label: '市场分析' },
]

const stockPickSubRoutes = [
  { path: '/stock-pick/expression', label: '表达式选股' },
  { path: '/stock-pick/concept', label: '基础信息选股' },
]

const rawDataSubRoutes = [
  { path: '/raw-data/data-import', label: '管理' },
  { path: '/raw-data/data-viewer', label: '查看' },
  { path: '/raw-data/data-download', label: '下载' },
  { path: '/raw-data/ranking-compute', label: '计算' },
  { path: '/raw-data/strategy-manage', label: '策略管理' },
]

export default function PageDesktop() {
  const location = useLocation()
  const [isCollapsed, setIsCollapsed] = useState(false)
  const [isOverviewOpen, setIsOverviewOpen] = useState(true)
  const [isStockPickOpen, setIsStockPickOpen] = useState(true)
  const [isRawDataOpen, setIsRawDataOpen] = useState(true)
  const [isIntradayMonitorOpen, setIsIntradayMonitorOpen] = useState(true)
  const [isBacktestOpen, setIsBacktestOpen] = useState(true)
  const contentRef = useRef<HTMLElement | null>(null)
  const isOverviewActive = location.pathname.startsWith('/overview')
  const isStockPickActive = location.pathname.startsWith('/stock-pick')
  const isRawDataActive = location.pathname.startsWith('/raw-data')
  const isIntradayMonitorActive = location.pathname.startsWith('/intraday-monitor')
  const isBacktestActive = location.pathname.startsWith('/backtest')

  useEffect(() => {
    void preloadWatchObserveRows().catch(() => {})
  }, [])

  return (
    <div className={isCollapsed ? 'desktop-shell collapsed' : 'desktop-shell'}>
      <button className="sidebar-toggle" type="button" onClick={() => setIsCollapsed((v) => !v)}>
        {isCollapsed ? '☰' : '✕'}
      </button>
      <aside className="sidebar">
        <div className="brand">明元量化</div>

        <nav className="menu-wrap">
          {menuList.map((menuItem) => (
            <NavLink
              key={menuItem.path}
              to={menuItem.path}
              className={({ isActive }) => (isActive ? 'menu-item active' : 'menu-item')}
            >
              {menuItem.label}
            </NavLink>
          ))}

          <div className="menu-group">
            <button
              className={isOverviewActive ? 'menu-item menu-group-toggle active' : 'menu-item menu-group-toggle'}
              type="button"
              onClick={() => setIsOverviewOpen((value) => !value)}
            >
              <span>排名总览</span>
              <span>{isOverviewOpen ? '▾' : '▸'}</span>
            </button>

            {isOverviewOpen ? (
              <div className="submenu-wrap">
                {overviewSubRoutes.map((menuItem) => (
                  <NavLink
                    key={menuItem.path}
                    to={menuItem.path}
                    className={({ isActive }) => (isActive ? 'submenu-item active' : 'submenu-item')}
                  >
                    {menuItem.label}
                  </NavLink>
                ))}
              </div>
            ) : null}
          </div>

          <div className="menu-group">
            <button
              className={
                isIntradayMonitorActive ? 'menu-item menu-group-toggle active' : 'menu-item menu-group-toggle'
              }
              type="button"
              onClick={() => setIsIntradayMonitorOpen((value) => !value)}
            >
              <span>实时监控</span>
              <span>{isIntradayMonitorOpen ? '▾' : '▸'}</span>
            </button>

            {isIntradayMonitorOpen ? (
              <div className="submenu-wrap">
                {intradayMonitorSubRoutes.map((menuItem) => (
                  <NavLink
                    key={menuItem.path}
                    to={menuItem.path}
                    className={({ isActive }) => (isActive ? 'submenu-item active' : 'submenu-item')}
                  >
                    {menuItem.label}
                  </NavLink>
                ))}
              </div>
            ) : null}
          </div>

          <div className="menu-group">
            <button
              className={isStockPickActive ? 'menu-item menu-group-toggle active' : 'menu-item menu-group-toggle'}
              type="button"
              onClick={() => setIsStockPickOpen((value) => !value)}
            >
              <span>选股</span>
              <span>{isStockPickOpen ? '▾' : '▸'}</span>
            </button>

            {isStockPickOpen ? (
              <div className="submenu-wrap">
                {stockPickSubRoutes.map((menuItem) => (
                  <NavLink
                    key={menuItem.path}
                    to={menuItem.path}
                    className={({ isActive }) => (isActive ? 'submenu-item active' : 'submenu-item')}
                  >
                    {menuItem.label}
                  </NavLink>
                ))}
              </div>
            ) : null}
          </div>

          <div className="menu-group">
            <button
              className={isRawDataActive ? 'menu-item menu-group-toggle active' : 'menu-item menu-group-toggle'}
              type="button"
              onClick={() => setIsRawDataOpen((value) => !value)}
            >
              <span>原数据管理</span>
              <span>{isRawDataOpen ? '▾' : '▸'}</span>
            </button>

            {isRawDataOpen ? (
              <div className="submenu-wrap">
                {rawDataSubRoutes.map((menuItem) => (
                  <NavLink
                    key={menuItem.path}
                    to={menuItem.path}
                    className={({ isActive }) => (isActive ? 'submenu-item active' : 'submenu-item')}
                  >
                    {menuItem.label}
                  </NavLink>
                ))}
              </div>
            ) : null}
          </div>

          <div className="menu-group">
            <button
              className={isBacktestActive ? 'menu-item menu-group-toggle active' : 'menu-item menu-group-toggle'}
              type="button"
              onClick={() => setIsBacktestOpen((value) => !value)}
            >
              <span>统计回测</span>
              <span>{isBacktestOpen ? '▾' : '▸'}</span>
            </button>

            {isBacktestOpen ? (
              <div className="submenu-wrap">
                {backtestSubRoutes.map((menuItem) => (
                  <NavLink
                    key={menuItem.path}
                    to={menuItem.path}
                    className={({ isActive }) => (isActive ? 'submenu-item active' : 'submenu-item')}
                  >
                    {menuItem.label}
                  </NavLink>
                ))}
              </div>
            ) : null}
          </div>

          <NavLink
            to={settingsMenuItem.path}
            className={({ isActive }) => (isActive ? 'menu-item active' : 'menu-item')}
          >
            {settingsMenuItem.label}
          </NavLink>
        </nav>
      </aside>

      <main className="content" ref={contentRef}>
        <Outlet />
      </main>
    </div>
  )
}
