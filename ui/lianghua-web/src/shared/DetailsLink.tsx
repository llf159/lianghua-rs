import { type ReactNode } from "react";
import { Link, useLocation } from "react-router-dom";
import { buildDetailsPath, type DetailsRouteInput } from "./detailsRoute";

type DetailsLinkProps = DetailsRouteInput & {
  children: ReactNode;
  className?: string;
  title?: string;
};

export default function DetailsLink({
  tsCode,
  tradeDate,
  sourcePath,
  children,
  className,
  title,
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
      to={buildDetailsPath({ tsCode, tradeDate, sourcePath })}
      state={{ backgroundLocation }}
    >
      {children}
    </Link>
  );
}
