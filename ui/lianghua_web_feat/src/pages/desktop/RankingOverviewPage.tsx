import { NavLink, Outlet } from "react-router-dom";

const rankingOverviewSubRoutes = [
  { path: "/overview/raw", label: "原始排名" },
  { path: "/overview/scene", label: "场景排名" },
] as const;

export default function RankingOverviewPage() {
  return (
    <div className="overview-page">
      <section className="overview-card">
        <h2 className="overview-title">排名总览</h2>
        <div className="overview-actions">
          {rankingOverviewSubRoutes.map((menuItem) => (
            <NavLink
              key={menuItem.path}
              to={menuItem.path}
              className={({ isActive }) =>
                isActive
                  ? "overview-read-btn overview-overview-tab-active"
                  : "overview-read-btn overview-overview-tab"
              }
            >
              {menuItem.label}
            </NavLink>
          ))}
        </div>
      </section>

      <Outlet />
    </div>
  );
}
