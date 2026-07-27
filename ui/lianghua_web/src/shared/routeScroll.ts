import { useRef } from "react";

export function useRouteScrollRegion<T extends HTMLElement>(
  _regionKey: string,
  _restoreDependencies: readonly unknown[] = [],
) {
  const elementRef = useRef<T | null>(null);
  return elementRef;
}
