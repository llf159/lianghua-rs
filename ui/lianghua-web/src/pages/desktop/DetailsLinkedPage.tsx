import { useLocation } from "react-router-dom";
import DetailsPage from "./DetailsPage";
import type { DetailsLinkLocationState } from "../../shared/detailsLinkState";

export default function DetailsLinkedPage() {
  const location = useLocation();
  const locationState =
    location.state && typeof location.state === "object"
      ? (location.state as DetailsLinkLocationState)
      : null;

  return (
    <DetailsPage
      variant="linked-overlay"
      navigationItems={locationState?.navigationItems}
    />
  );
}
