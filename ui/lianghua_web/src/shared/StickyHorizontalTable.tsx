import { useRef, type ReactNode, type RefObject } from "react";
import "./stickyHorizontalTable.css";

type StickyHorizontalTableProps = {
  children: ReactNode;
  scrollClassName: string;
  scrollRef?: RefObject<HTMLDivElement | null>;
  stickyHeader: ReactNode;
};

export default function StickyHorizontalTable({
  children,
  scrollClassName,
  scrollRef,
  stickyHeader,
}: StickyHorizontalTableProps) {
  const stickyHeaderRef = useRef<HTMLDivElement | null>(null);

  return (
    <div className="sticky-horizontal-table-shell">
      <div className="sticky-horizontal-table-head" ref={stickyHeaderRef}>
        {stickyHeader}
      </div>
      <div
        className={scrollClassName}
        ref={(element) => {
          if (scrollRef) {
            scrollRef.current = element;
          }
        }}
        onScroll={(event) => {
          if (stickyHeaderRef.current) {
            stickyHeaderRef.current.scrollLeft = event.currentTarget.scrollLeft;
          }
        }}
      >
        {children}
      </div>
    </div>
  );
}
