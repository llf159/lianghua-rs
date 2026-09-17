import type { Location } from "react-router-dom";
import type { DetailStrategyTriggerRow } from "../apis/details";

export type DetailsNavigationRole = "self" | "success" | "failure";

export type DetailsNavigationItem = {
  tsCode: string;
  tradeDate?: string | null;
  intervalStartTradeDate?: string | null;
  intervalEndTradeDate?: string | null;
  sourcePath?: string | null;
  name?: string | null;
  role?: DetailsNavigationRole | null;
  groupId?: string | null;
};

export type DetailsSimilarityNavMode = "evidence" | "list";

export type DetailsStrategyCompareSnapshot = {
  tsCode: string;
  relativeTradeDate: string;
  rows: DetailStrategyTriggerRow[];
};

export type DetailsLinkLocationState = {
  backgroundLocation?: Location;
  navigationItems?: DetailsNavigationItem[];
  evidenceNavigationItems?: DetailsNavigationItem[];
  strategyCompareSnapshot?: DetailsStrategyCompareSnapshot | null;
};
