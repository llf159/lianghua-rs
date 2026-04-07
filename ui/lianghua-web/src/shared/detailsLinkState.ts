import type { Location } from "react-router-dom";
import type { DetailStrategyTriggerRow } from "../apis/details";

export type DetailsNavigationItem = {
  tsCode: string;
  tradeDate?: string | null;
  sourcePath?: string | null;
  name?: string | null;
};

export type DetailsStrategyCompareSnapshot = {
  tsCode: string;
  relativeTradeDate: string;
  rows: DetailStrategyTriggerRow[];
};

export type DetailsLinkLocationState = {
  backgroundLocation?: Location;
  navigationItems?: DetailsNavigationItem[];
  strategyCompareSnapshot?: DetailsStrategyCompareSnapshot | null;
};
