import { useEffect, useRef, useState } from "react";
import { listen } from "@tauri-apps/api/event";
import { BarChart3, PanelLeftOpen, Pin, Plus, Sparkles, Table2, TerminalSquare, X } from "lucide-react";
import { useStore } from "./store";
import type { TableRef, VizSpec } from "./types";
import { Sidebar } from "./components/Sidebar";
import { Welcome } from "./components/Welcome";
import { TableView } from "./components/TableView";
import { QueryView } from "./components/QueryView";
import { ChartTab } from "./components/ChartView";
import { ChatPanel } from "./components/ChatPanel";
import { FeedbackDialog } from "./components/Feedback";
import { QuickOpen } from "./components/QuickOpen";
import { ConnectionDialog, SettingsDialog, ShortcutsDialog } from "./components/Dialogs";
import "./styles.css";
import "./nav.css";

async function cycleTab(dir: 1 | -1) {
  const st = useStore.getState();
  const visible = st.tabs.filter((t) => t.connectionId === st.activeConnectionId);
  if (visible.length < 2) return;
  const i = visible.findIndex((t) => t.id === st.activeTabId);
  st.setActiveTab(visible[(i + dir + visible.length) % visible.length].id);
}

async function focusConnection(id: string) {
  const s = useStore.getState();
  if (s.activeConnectionId !== id) await s.openConnection(id);
}

export default function App() {
  const s = useStore();
  const tabs = s.tabs.filter((t) => t.connectionId === s.activeConnectionId);

  const tabsEl = useRef<HTMLDivElement>(null);
  const [tabMenu, setTabMenu] = useState<{ id: string; x: number; y: number } | null>(null);
  const [renaming, setRenaming] = useState<string | null>(null);
  useEffect(() => {
    if (!tabMenu) return;
    const close = () => setTabMenu(null);
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && close();
    window.addEventListener("mousedown", close);
    window.addEventListener("keydown", onKey);
    window.addEventListener("blur", close);
    return () => {
      window.removeEventListener("mousedown", close);
      window.removeEventListener("keydown", onKey);
      window.removeEventListener("blur", close);
    };
  }, [tabMenu]);
  const menuTab = tabMenu ? s.tabs.find((t) => t.id === tabMenu.id) : undefined;
  useEffect(() => {
    tabsEl.current?.querySelector(".tab.active")?.scrollIntoView({ block: "nearest", inline: "nearest" });
  }, [s.activeTabId, tabs.length]);

  useEffect(() => {
    s.loadConnections().then(() => useStore.getState().restoreSession());
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    const subs = [
      listen("update-ready", () => {
        const st = useStore.getState();
        st.set({ updateReady: true });
        st.notify("A new TableView version is ready. Open Feedback (⇧⌘F) to restart into it.");
      }),
      listen<{ connection_id: string; result_id: string; sql: string; title: string }>("claude-show-result", async ({ payload }) => {
        await focusConnection(payload.connection_id);
        useStore.getState().openQuery({ sql: payload.sql, title: payload.title, resultId: payload.result_id, connectionId: payload.connection_id, fromClaude: true });
      }),
      listen<{ connection_id: string; table: TableRef }>("claude-open-table", async ({ payload }) => {
        await focusConnection(payload.connection_id);
        useStore.getState().openTable(payload.table, payload.connection_id);
      }),
      listen<{ connection_id: string; sql: string; title?: string }>("claude-open-query", async ({ payload }) => {
        await focusConnection(payload.connection_id);
        useStore.getState().openQuery({ sql: payload.sql, title: payload.title ?? "Claude SQL", connectionId: payload.connection_id, fromClaude: true });
      }),
      listen<{ connection_id: string; result_id: string; spec: VizSpec; title: string }>("claude-show-chart", async ({ payload }) => {
        await focusConnection(payload.connection_id);
        useStore.getState().openChart(payload.connection_id, payload.result_id, payload.spec, payload.title);
      }),
    ];
    return () => subs.forEach((p) => p.then((f) => f()));
  }, []);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      const st = useStore.getState();
      if (e.ctrlKey && !e.metaKey && !e.altKey && e.key === "Tab") { e.preventDefault(); cycleTab(e.shiftKey ? -1 : 1); return; }
      if (!e.metaKey || e.ctrlKey || e.altKey) return;
      const k = e.key.toLowerCase();
      if (e.shiftKey && (e.code === "BracketLeft" || e.code === "BracketRight")) { e.preventDefault(); cycleTab(e.code === "BracketLeft" ? -1 : 1); }
      else if (e.shiftKey && (e.code === "Slash" || e.key === "?")) { e.preventDefault(); st.set({ showShortcuts: !st.showShortcuts }); }
      else if (e.shiftKey && k === "t") { e.preventDefault(); if (st.activeConnectionId) st.reopenTab(); }
      else if (e.shiftKey && k === "f") { e.preventDefault(); st.set({ showFeedback: !st.showFeedback }); }
      else if (e.shiftKey) return;
      else if (k === "n") { e.preventDefault(); st.set({ editingConnection: "new" }); }
      else if (k === "k") { e.preventDefault(); st.set({ chatOpen: !st.chatOpen }); }
      else if (k === "p") { e.preventDefault(); if (st.activeConnectionId) st.set({ quickOpen: true }); }
      else if (k === "t" && st.activeConnectionId) { e.preventDefault(); st.openQuery(); }
      else if (k === "w" && st.activeTabId) {
        e.preventDefault();
        if (!st.tabs.find((t) => t.id === st.activeTabId)?.pinned) st.closeTab(st.activeTabId);
        else st.notify("Pinned tab. Right-click it to unpin or close.");
      }
      else if (k === ",") { e.preventDefault(); st.set({ showSettings: true }); }
      else if (/^[1-9]$/.test(k)) {
        const visible = st.tabs.filter((t) => t.connectionId === st.activeConnectionId);
        const t = visible[Number(k) - 1];
        if (t) { e.preventDefault(); st.setActiveTab(t.id); }
      }
    };
    window.addEventListener("keydown", onKey, true);
    return () => window.removeEventListener("keydown", onKey, true);
  }, []);

  // Kept mounted while hidden so ⌘K toggles the panel without losing the conversation.
  const chat = <ChatPanel hidden={!s.chatOpen} />;

  return (
    <div className="app">
      {s.chatSettings.side === "left" && chat}
      {s.activeConnectionId ? (
        <>
          <div className={`sidebar-wrap ${s.sidebarCollapsed ? "collapsed" : ""}`} inert={s.sidebarCollapsed}>
            <Sidebar />
          </div>
          <div className="main">
            <div className={`tabs ${s.sidebarCollapsed && !(s.chatOpen && s.chatSettings.side === "left") ? "clear-lights" : ""}`} ref={tabsEl}>
              {s.sidebarCollapsed && <button className="icon-btn tabs-sidebar" title="Show sidebar" onClick={s.toggleSidebar}><PanelLeftOpen size={14} /></button>}
              {tabs.map((t) => (
                <div key={t.id} className={`tab ${t.id === s.activeTabId ? "active" : ""} ${t.pinned ? "pinned" : ""}`} title={t.kind === "table" ? `${t.table.schema}.${t.table.name}` : t.title}
                  onMouseDown={(e) => { if (e.button === 1) { e.preventDefault(); if (!t.pinned) s.closeTab(t.id); } else if (e.button === 0) s.setActiveTab(t.id); }}
                  onContextMenu={(e) => { e.preventDefault(); setTabMenu({ id: t.id, x: e.clientX, y: e.clientY }); }}>
                  {t.kind === "table" ? <Table2 size={12} /> : t.kind === "chart" ? <BarChart3 size={12} /> : t.fromClaude ? <Sparkles size={12} /> : <TerminalSquare size={12} />}
                  {renaming === t.id && t.kind === "query" ? (
                    <input className="tab-rename" autoFocus defaultValue={t.title} autoCapitalize="off" autoCorrect="off" spellCheck={false}
                      onMouseDown={(e) => e.stopPropagation()} onFocus={(e) => e.currentTarget.select()}
                      onBlur={(e) => { if (e.currentTarget.value.trim()) s.updateTab(t.id, { title: e.currentTarget.value.trim() }); setRenaming(null); }}
                      onKeyDown={(e) => { if (e.key === "Enter") e.currentTarget.blur(); if (e.key === "Escape") { e.currentTarget.value = t.title; e.currentTarget.blur(); } }} />
                  ) : (
                    <span className="label" onDoubleClick={() => t.kind === "query" && setRenaming(t.id)}>{t.kind === "table" ? t.table.name : t.title}</span>
                  )}
                  {t.pinned
                    ? <span className="pin" title="Pinned"><Pin size={10} /></span>
                    : <button className="close" tabIndex={-1} title="Close (⌘W)" onMouseDown={(e) => { e.stopPropagation(); s.closeTab(t.id); }}><X size={11} /></button>}
                </div>
              ))}
              <button className="icon-btn tabs-new" title="New query (⌘T)" onClick={() => s.openQuery()}><Plus size={14} /></button>
              <div className="tabs-fill" data-tauri-drag-region />
            </div>
            {tabs.length === 0 && (
              <div className="empty">
                <div style={{ fontSize: 15, color: "var(--text)" }}>Open a table from the sidebar</div>
                <div>⌘P find a table · ⌘T new query · ⌘K ask Claude</div>
                <button className="link-btn" onClick={() => s.set({ showShortcuts: true })}>All keyboard shortcuts ⇧⌘/</button>
              </div>
            )}
            {tabs.map((t) => (
              <div key={t.id} className="pane" style={{ display: t.id === s.activeTabId ? "flex" : "none" }}>
                {t.kind === "table" && <TableView connectionId={t.connectionId} table={t.table} active={t.id === s.activeTabId} />}
                {t.kind === "query" && <QueryView tabId={t.id} connectionId={t.connectionId} initialSql={t.sql} resultId={t.resultId} active={t.id === s.activeTabId} fromClaude={t.fromClaude} />}
                {t.kind === "chart" && <ChartTab resultId={t.resultId} spec={t.spec} />}
              </div>
            ))}
          </div>
        </>
      ) : (
        <Welcome />
      )}
      {s.chatSettings.side === "right" && chat}
      {tabMenu && menuTab && (
        <div className="tv-menu" style={{ position: "fixed", left: tabMenu.x, top: tabMenu.y }} onMouseDown={(e) => e.stopPropagation()}>
          <button onClick={() => { s.pinTab(menuTab.id, !menuTab.pinned); setTabMenu(null); }}>{menuTab.pinned ? "Unpin tab" : "Pin tab"}</button>
          {menuTab.kind === "query" && (
            <button onClick={() => { setRenaming(menuTab.id); setTabMenu(null); }}>Rename</button>
          )}
          <hr />
          <button onClick={() => { s.closeTab(menuTab.id); setTabMenu(null); }}>Close tab</button>
          <button onClick={() => { s.closeOtherTabs(menuTab.id); setTabMenu(null); }}>Close other tabs</button>
        </div>
      )}
      {s.quickOpen && <QuickOpen />}
      {s.editingConnection && <ConnectionDialog initial={s.editingConnection} />}
      {s.showSettings && <SettingsDialog />}
      {s.showShortcuts && <ShortcutsDialog />}
      {s.showFeedback && <FeedbackDialog />}
      {s.toast && <div className={`toast ${s.toast.error ? "error" : ""}`}>{s.toast.text}</div>}
    </div>
  );
}
