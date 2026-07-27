import { Component, type ErrorInfo, type ReactNode } from "react";

type DetailsErrorBoundaryProps = {
  children: ReactNode;
};

type DetailsErrorBoundaryState = {
  errorText: string | null;
};

export default class DetailsErrorBoundary extends Component<
  DetailsErrorBoundaryProps,
  DetailsErrorBoundaryState
> {
  state: DetailsErrorBoundaryState = {
    errorText: null,
  };

  static getDerivedStateFromError(error: unknown): DetailsErrorBoundaryState {
    return {
      errorText: error instanceof Error ? error.message : String(error),
    };
  }

  componentDidCatch(error: unknown, errorInfo: ErrorInfo) {
    console.error("Details render failed", error, errorInfo);
  }

  render() {
    if (this.state.errorText !== null) {
      return (
        <div
          style={{
            padding: 16,
            color: "#991b1b",
            background: "#fef2f2",
            border: "1px solid #fecaca",
            borderRadius: 12,
            whiteSpace: "pre-wrap",
            wordBreak: "break-word",
          }}
        >
          {`详情页渲染失败\n${this.state.errorText}`}
        </div>
      );
    }

    return this.props.children;
  }
}
