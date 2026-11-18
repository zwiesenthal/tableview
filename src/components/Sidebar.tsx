import { useEffect, useMemo, useRef, useState } from "react";
import { ChevronDown, ChevronRight, Eye, PanelLeftClose, Plus, RefreshCw, Search, Settings, Sparkles, Table2, TerminalSquare, MessageSquarePlus } from "lucide-react";
import { activeConnection, ENV_COLORS, useStore } from "../store";
import "../nav.css";

export function Sidebar() {
  const s = useStore();
  const conn = activeConnection(s);
  const tree = conn ? s.schemas[conn.id] : undefined;
  const [q, setQ] = useState("");
  const [collapsed, setCollapsed] = useState<Set<string>>(new Set());
  const [cursor, setCursor] = useState<number | null>(null);
  const [searching, setSearching] = useState(false);
  const treeEl = useRef<HTMLDivElement>(null);
  const activeTab = s.tabs.find((t) => t.id === s.activeTabId);
  const activeTable = activeTab?.kind === "table" ? activeTab.table : null;

  const groups = useMemo(() => {
    const needle = q.toLowerCase();
    return (tree?.schemas ?? [])
      .map((g) => ({ ...g, tables: g.tables.filter((t) => !needle || t.name.toLowerCase().includes(needle)) }))
      .filter((g) => g.tables.length);
  }, [tree, q]);

  const flat = useMemo(() => groups.flatMap((g) => (collapsed.has(g.name) && !q ? [] : g.tables)), [groups, collapsed, q]);

  useEffect(() => setCursor(null), [q]);

  useEffect(() => {
    if (!activeTable) return;
    setCollapsed((c) => {
      if (!c.has(activeTable.schema)) return c;
      const n = new Set(c);
      n.delete(activeTable.schema);
      return n;
    });
  }, [activeTable?.schema, activeTable?.name, s.activeTabId]);

  useEffect(() => {
    (treeEl.current?.querySelector(".tree-item.kbd") ?? treeEl.current?.querySelector(".tree-item.active"))?.scrollIntoView({ block: "nearest" });
  }, [s.activeTabId, cursor, collapsed, groups]);

  if (!conn) return null;
  const highlighted = searching && (cursor !== null || q) ? Math.min(cursor ?? 0, flat.length - 1) : -1;
  const onSearchKey = (e: React.KeyboardEvent) => {
    if (e.key === "Escape") { e.preventDefault(); if (q) setQ(""); else (e.target as HTMLElement).blur(); }
    else if (e.key === "ArrowDown") { e.preventDefault(); setCursor(Math.min(flat.length - 1, (cursor ?? (q ? 0 : -1)) + 1)); }
    else if (e.key === "ArrowUp") { e.preventDefault(); setCursor(Math.max(0, (cursor ?? 0) - 1)); }
    else if (e.key === "Enter" && !e.nativeEvent.isComposing && highlighted >= 0) s.openTable(flat[highlighted]);
  };
  return (
    <div className="sidebar">
      <div className="titlebar-pad" data-tauri-drag-region>
        <button className="icon-btn" title="Hide sidebar" onClick={s.toggleSidebar}><PanelLeftClose size={14} /></button>
      </div>
      <button className="sidebar-conn" title="Switch connection" onClick={() => s.closeConnection()}>
        <span className="env-dot" style={{ background: ENV_COLORS[conn.env] ?? ENV_COLORS.local }} />
        <span className="name">{conn.name}</span>
        <span className="ver">{(s.serverVersion[conn.id] ?? "").split(" ").slice(0, 2).join(" ").replace(/\.\d+$/, "")}</span>
      </button>
      <div className="search">
        <Search size={13} />
        <input autoCapitalize="off" autoCorrect="off" spellCheck={false} placeholder="Search tables  ⌘P" value={q} onChange={(e) => setQ(e.target.value)}
          onKeyDown={onSearchKey} onFocus={() => setSearching(true)} onBlur={() => setSearching(false)} />
      </div>
      <div className="tree" ref={treeEl}>
        {groups.map((g) => {
          const isCollapsed = collapsed.has(g.name) && !q;
          return (
            <div key={g.name}>
              {(groups.length > 1 || g.name !== "main") && (
                <div className="tree-group" onClick={() => setCollapsed((c) => {
                  const n = new Set(c);
                  if (n.has(g.name)) n.delete(g.name); else n.add(g.name);
                  return n;
                })}>
                  {isCollapsed ? <ChevronRight size={11} /> : <ChevronDown size={11} />}{g.name}
                  <span style={{ marginLeft: "auto", fontWeight: 400 }}>{g.tables.length}</span>
                </div>
              )}
              {!isCollapsed && g.tables.map((t) => {
                const active = activeTab?.kind === "table" && activeTab.connectionId === conn.id && activeTab.table.schema === t.schema && activeTab.table.name === t.name;
                const kbd = flat[highlighted] === t;
                return (
                  <div key={`${t.schema}.${t.name}`} className={`tree-item ${active ? "active" : ""} ${kbd ? "kbd" : ""}`} onClick={() => s.openTable(t)} title={`${t.schema}.${t.name}`}>
                    {t.kind === "view" ? <Eye size={13} /> : <Table2 size={13} />}
                    <span>{t.name}</span>
                  </div>
                );
              })}
            </div>
          );
        })}
        {tree && groups.length === 0 && <div style={{ padding: 10, color: "var(--text-2)" }}>{q ? "No matches" : "No tables"}</div>}
      </div>
      <div className="sidebar-footer">
        <button className="icon-btn" title="New query (⌘T)" onClick={() => s.openQuery()}><TerminalSquare size={14} /></button>
        <button className="icon-btn" title="Refresh schema" onClick={() => s.refreshSchema()}><RefreshCw size={13} /></button>
        <button className="icon-btn" title="Claude (⌘K)" onClick={() => s.set({ chatOpen: !s.chatOpen })}><Sparkles size={14} /></button>
        <div style={{ flex: 1 }} />
        <button className="icon-btn" title="New connection" onClick={() => s.set({ editingConnection: "new" })}><Plus size={14} /></button>
        <button className={`icon-btn ${s.updateReady ? "fb-badge" : ""}`} title={s.updateReady ? "Update ready (⇧⌘F)" : "Feedback (⇧⌘F)"} onClick={() => s.set({ showFeedback: true })}><MessageSquarePlus size={14} /></button>
        <button className="icon-btn" title="Settings (⌘,)" onClick={() => s.set({ showSettings: true })}><Settings size={13} /></button>
      </div>
    </div>
  );
}
