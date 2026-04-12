import { useEffect } from "react";
import { createPortal } from "react-dom";
import { useNavigate } from "react-router-dom";
import DetailsPage from "../pages/desktop/DetailsPage";
import "./detailsOverlay.css";

export default function DetailsOverlayRoute() {
  const navigate = useNavigate();

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
          <DetailsPage />
        </div>
      </div>
    </div>,
    document.body,
  );
}
