import { useEffect, useState } from "react";
import { createPortal } from "react-dom";
import { useNavigate } from "react-router-dom";
import DetailsLinkedPage from "../pages/desktop/DetailsLinkedPage";
import "./detailsOverlay.css";

export default function DetailsLinkedOverlayRoute() {
  const navigate = useNavigate();
  const [contentReady, setContentReady] = useState(false);

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        navigate(-1);
      }
    };

    window.addEventListener("keydown", onKeyDown);
    return () => {
      window.removeEventListener("keydown", onKeyDown);
    };
  }, [navigate]);

  useEffect(() => {
    let cancelled = false;
    const frameId = window.requestAnimationFrame(() => {
      if (!cancelled) {
        setContentReady(true);
      }
    });

    return () => {
      cancelled = true;
      window.cancelAnimationFrame(frameId);
    };
  }, []);

  if (typeof document === "undefined") {
    return null;
  }

  return createPortal(
    <div
      className="details-overlay-backdrop"
      onClick={() => navigate(-1)}
      role="presentation"
    >
      <div
        className="details-overlay-shell"
        onClick={(event) => event.stopPropagation()}
        role="dialog"
        aria-modal="true"
        aria-label="个股详情"
      >
        <div className="details-overlay-toolbar">
          <button
            className="details-overlay-close"
            type="button"
            onClick={() => navigate(-1)}
          >
            返回原页
          </button>
        </div>
        <div className="details-overlay-body" data-details-scroll-root="true">
          {contentReady ? (
            <DetailsLinkedPage />
          ) : (
            <div className="details-overlay-loading">详情加载中...</div>
          )}
        </div>
      </div>
    </div>,
    document.body,
  );
}
