import { invoke } from "@tauri-apps/api/core";
import { useCallback, useEffect, useRef, useState } from "react";

type LoginSource = "eastmoney" | "xueqiu" | "tonghuashun";
type SessionStatus = {
  source: LoginSource;
  window_open: boolean;
  page_ready: boolean;
  enabled: boolean;
  transport: "rust" | "webview" | null;
  transport_reason: string | null;
};

export default function DataSourceSessionMenu() {
  const [open, setOpen] = useState(false);
  const menuRef = useRef<HTMLDivElement>(null);
  const [sessions, setSessions] = useState<SessionStatus[]>([]);
  const [busy, setBusy] = useState<Partial<Record<LoginSource, boolean>>>({});
  const [messages, setMessages] = useState<Partial<Record<LoginSource, string>>>({});
  const [error, setError] = useState("");

  const refreshSessions = useCallback(async () => {
    try {
      setSessions(await invoke<SessionStatus[]>("data_source_sessions"));
      setError("");
    } catch (error) {
      setError(`读取会话状态失败：${String(error)}`);
    }
  }, []);

  useEffect(() => {
    if (!open) return;
    void refreshSessions();
    function onPointerDown(event: PointerEvent) {
      if (!menuRef.current?.contains(event.target as Node)) setOpen(false);
    }
    function onKeyDown(event: KeyboardEvent) {
      if (event.key === "Escape") {
        setOpen(false);
        menuRef.current?.querySelector<HTMLButtonElement>(".data-source-session-trigger")?.focus();
      }
    }
    function onVisibilityChange() {
      if (document.visibilityState === "visible") void refreshSessions();
    }
    window.addEventListener("focus", refreshSessions);
    document.addEventListener("visibilitychange", onVisibilityChange);
    document.addEventListener("pointerdown", onPointerDown);
    document.addEventListener("keydown", onKeyDown);
    return () => {
      window.removeEventListener("focus", refreshSessions);
      document.removeEventListener("visibilitychange", onVisibilityChange);
      document.removeEventListener("pointerdown", onPointerDown);
      document.removeEventListener("keydown", onKeyDown);
    };
  }, [open, refreshSessions]);

  async function runAction(source: LoginSource, action: "open" | "connect" | "stop") {
    setBusy((value) => ({ ...value, [source]: true }));
    setMessages((value) => ({ ...value, [source]: action === "connect" ? "正在试拉并检查连接…" : "处理中…" }));
    try {
      await invoke(
        action === "open" ? "open_data_source_login" : action === "connect" ? "use_data_source_session" : "stop_data_source_session",
        { source },
      );
      setMessages((value) => ({
        ...value,
        [source]: action === "open"
          ? "完成登录或验证后返回，点击“检查并启用”。"
          : action === "connect"
            ? "试拉通过，后续拉取使用此会话。"
            : "已停用会话，恢复默认拉取；保留网页登录状态。",
      }));
    } catch (error) {
      setMessages((value) => ({ ...value, [source]: `操作失败：${String(error)}` }));
    } finally {
      await refreshSessions();
      setBusy((value) => ({ ...value, [source]: false }));
    }
  }

  return (
    <div className="data-source-session-menu" ref={menuRef}>
      <button
        type="button"
        className="data-source-session-trigger"
        aria-expanded={open}
        aria-controls="data-source-session-popover"
        onClick={() => setOpen((value) => !value)}
      >
        数据源登录
      </button>
      {open ? (
        <div className="data-source-session-popover" id="data-source-session-popover">
          <div className="data-source-session-popover-head">
            <strong>数据源登录</strong>
            <button type="button" aria-label="关闭数据源登录" onClick={() => setOpen(false)}>关闭</button>
          </div>
          <p>七源自动并发补齐，无需逐一登录。遇到访问限制时，可为以下来源单独登录并启用会话。</p>
          {error ? <p role="alert">{error}</p> : null}
          {([
            ["eastmoney", "东方财富"],
            ["xueqiu", "雪球"],
            ["tonghuashun", "同花顺"],
          ] as const).map(([source, label]) => {
            const session = sessions.find((item) => item.source === source);
            return (
              <section className="data-source-session-source" key={source} aria-label={`${label}登录会话`}>
                <div className="data-source-session-source-head">
                  <strong>{label}</strong>
                  <span className={session?.enabled ? "is-connected" : undefined} title={session?.transport_reason ?? undefined}>
                    {session?.enabled
                      ? session.transport === "rust" ? "已启用 · 直连" : "已启用 · 网页"
                      : session?.page_ready ? "页面就绪 · 待检查" : session?.window_open ? "页面已打开" : session ? "默认拉取" : "状态未读取"}
                  </span>
                </div>
                <div className="data-source-session-actions" role="group" aria-label={`${label}会话操作`} aria-busy={!!busy[source]}>
                  <button className="all-market-toggle" type="button" disabled={busy[source]} onClick={() => void runAction(source, "open")}>打开登录页</button>
                  <button className="all-market-toggle" type="button" disabled={busy[source]} onClick={() => void runAction(source, "connect")}>{busy[source] ? "处理中…" : "检查并启用"}</button>
                  <button className="all-market-toggle" type="button" disabled={busy[source] || !session?.enabled} onClick={() => void runAction(source, "stop")}>停用</button>
                </div>
                {session?.enabled && session.transport === "webview" ? <p>拉取期间请保留登录窗口。</p> : null}
                {messages[source] ? <p role="status">{messages[source]}</p> : null}
              </section>
            );
          })}
        </div>
      ) : null}
    </div>
  );
}
