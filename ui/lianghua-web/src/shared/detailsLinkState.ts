import type { Location } from "react-router-dom";

export type DetailsNavigationItem = {
  tsCode: string;
  tradeDate?: string | null;
  sourcePath?: string | null;
  name?: string | null;
};

export type DetailsLinkLocationState = {
  backgroundLocation?: Location;
  navigationItems?: DetailsNavigationItem[];
};
