import { Outlet } from "react-router-dom";

export default function RankingOverviewPage() {
  return (
    <div className="overview-page">
      <section className="overview-card">
        <h2 className="overview-title">排名总览</h2>
      </section>

      <Outlet />
    </div>
  );
}
