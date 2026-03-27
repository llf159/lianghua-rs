import { type ReactNode } from "react";
import { Link, useLocation } from "react-router-dom";
import { buildLinkedDetailsPath, type DetailsRouteInput } from "./detailsRoute";
import type { DetailsNavigationItem, DetailsLinkLocationState } from "./detailsLinkState";

type DetailsLinkProps = DetailsRouteInput & {
  children: ReactNode;
  className?: string;
  title?: string;
  navigationItems?: DetailsNavigationItem[];
};

export default function DetailsLink({
  tsCode,
  tradeDate,
  sourcePath,
  children,
  className,
  title,
  navigationItems,
}: DetailsLinkProps) {
  const location = useLocation();
  const backgroundLocation =
    location.state &&
    typeof location.state === "object" &&
    "backgroundLocation" in location.state &&
    location.state.backgroundLocation
      ? location.state.backgroundLocation
      : location;

  return (
    <Link
      className={className}
      preventScrollReset
      title={title}
      to={buildLinkedDetailsPath({ tsCode, tradeDate, sourcePath })}
      state={
        {
          backgroundLocation,
          navigationItems,
        } satisfies DetailsLinkLocationState
      }
    >
      {children}
    </Link>
  );
}
