import { create } from "zustand";
import { api, connectErrorText, errorText } from "./api";
import { DEFAULT_PREFS, Prefs, withDefaults } from "./prefs";
import type { ChatSettings, ConnectionConfig, SchemaTree, Tab, TableRef, VizSpec } from "./types";

type SavedTab = { kind: "table"; table: TableRef } | { kind: "query"; title: string; sql: string };

/** Pinned tabs are saved per connection and reopened on connect. */
function savePinned(connectionId: string, tabs: Tab[]) {
  const saved: SavedTab[] = tabs
    .filter((t) => t.connectionId === connectionId && t.pinned && t.kind !== "chart")
    .map((t) => (t.kind === "table" ? { kind: "table", table: t.table } : { kind: "query", title: t.title, sql: (t as { sql: string }).sql }));
  try {
    if (saved.length) localStorage.setItem(`tv.pinned.${connectionId}`, JSON.stringify(saved));
    else localStorage.removeItem(`tv.pinned.${connectionId}`);
  } catch {
    /* storage unavailable: pins last for this session */
  }
}

function loadPinned(connectionId: string): SavedTab[] {
  try {
    return JSON.parse(localStorage.getItem(`tv.pinned.${connectionId}`) ?? "[]") as SavedTab[];
  } catch {
    return [];
  }
}

/** The last open connection and its tabs, reopened when the app starts. */
type Session = { connectionId: string; tabs: (SavedTab & { pinned?: boolean })[]; active: number };

function loadSession(): Session | null {
  try {
    return JSON.parse(localStorage.getItem("tv.session") ?? "null") as Session | null;
  } catch {
    return null;
  }
}

let seq = 0;
export const newId = (p: string) => `${p}-${Date.now().toString(36)}-${(seq++).toString(36)}`;

export const ENV_COLORS: Record<string, string> = {
  local: "var(--env-local)", dev: "var(--env-dev)", staging: "var(--env-staging)", prod: "var(--env-prod)",
};

interface State {
  connections: ConnectionConfig[];
  activeConnectionId: string | null;
  serverVersion: Record<string, string>;
  schemas: Record<string, SchemaTree>;
  tabs: Tab[];
  closedTabs: Tab[];
  activeTabId: string | null;
  toast: { text: string; error: boolean } | null;
  editingConnection: ConnectionConfig | "new" | null;
  showSettings: boolean;
  showShortcuts: boolean;
  showFeedback: boolean;
  updateReady: boolean;
  quickOpen: boolean;
  chatOpen: boolean;
  sidebarCollapsed: boolean;
  toggleSidebar(): void;
  chatSettings: ChatSettings;
  selectedTable: TableRef | null;
  prefs: Prefs;
  setPref<K extends keyof Prefs>(key: K, value: Prefs[K]): void;

  loadConnections(): Promise<void>;
  restoreSession(): Promise<void>;
  openConnection(id: string): Promise<boolean>;
  closeConnection(): void;
  deleteConnection(id: string): Promise<boolean>;
  refreshSchema(): Promise<void>;
  openTable(t: TableRef, connectionId?: string): void;
  openQuery(opts?: { sql?: string; title?: string; resultId?: string; connectionId?: string; fromClaude?: boolean }): string;
  openChart(connectionId: string, resultId: string, spec: VizSpec, title: string): void;
  updateTab(id: string, patch: Partial<Tab>): void;
  pinTab(id: string, pinned: boolean): void;
  closeOtherTabs(id: string): void;
  closeTab(id: string): void;
  reopenTab(): void;
  setActiveTab(id: string): void;
  notify(text: string, error?: boolean): void;
  set(patch: Partial<State>): void;
  saveChatSettings(c: ChatSettings): Promise<void>;
}

export const useStore = create<State>((set, get) => ({
  connections: [],
  activeConnectionId: null,
  serverVersion: {},
  schemas: {},
  tabs: [],
  closedTabs: [],
  activeTabId: null,
  toast: null,
  editingConnection: null,
  showSettings: false,
  showShortcuts: false,
  showFeedback: false,
  updateReady: false,
  quickOpen: false,
  chatOpen: false,
  sidebarCollapsed: localStorage.getItem("tv.sidebarCollapsed") === "1",
  toggleSidebar() {
    const sidebarCollapsed = !get().sidebarCollapsed;
    localStorage.setItem("tv.sidebarCollapsed", sidebarCollapsed ? "1" : "0");
    set({ sidebarCollapsed });
  },
  chatSettings: { side: "right", width: 380, model: "", extra_instructions: "", claude_path: "" },
  selectedTable: null,
  prefs: DEFAULT_PREFS,

  async loadConnections() {
    const [connections, chatSettings, prefs] = await Promise.all([api.listConnections(), api.getChatSettings(), api.getPrefs()]);
    set({ connections, chatSettings, prefs: withDefaults(prefs) });
  },

  async restoreSession() {
    const session = loadSession();
    if (!session || !get().connections.some((c) => c.id === session.connectionId)) return;
    const id = session.connectionId;
    if (!(await get().openConnection(id))) return;
    // Pinned tabs came back with the connection; add the unpinned ones in their saved order.
    set((s) => {
      const restored: Tab[] = session.tabs
        .filter((p) => !p.pinned)
        .map((p) => (p.kind === "table"
          ? { id: newId("tab"), kind: "table", connectionId: id, table: p.table }
          : { id: newId("tab"), kind: "query", connectionId: id, title: p.title, sql: p.sql }));
      const tabs = [...s.tabs, ...restored];
      const visible = tabs.filter((t) => t.connectionId === id);
      return { tabs, activeTabId: visible[session.active]?.id ?? s.activeTabId };
    });
  },

  async openConnection(id) {
    try {
      const version = await api.connect(id);
      const schema = await api.getSchema(id);
      set((s) => {
        const open = s.tabs.filter((t) => t.connectionId === id);
        const restored: Tab[] = loadPinned(id)
          .filter((p) => !open.some((t) => (p.kind === "table" && t.kind === "table" && t.table.schema === p.table.schema && t.table.name === p.table.name)
            || (p.kind === "query" && t.kind === "query" && t.pinned && t.title === p.title)))
          .map((p) => (p.kind === "table"
            ? { id: newId("tab"), kind: "table", connectionId: id, table: p.table, pinned: true }
            : { id: newId("tab"), kind: "query", connectionId: id, title: p.title, sql: p.sql, pinned: true }));
        const tabs = [...s.tabs, ...restored];
        return {
          activeConnectionId: id,
          serverVersion: { ...s.serverVersion, [id]: version },
          schemas: { ...s.schemas, [id]: schema },
          tabs,
          activeTabId: tabs.find((t) => t.connectionId === id)?.id ?? null,
        };
      });
      return true;
    } catch (e) {
      const conn = get().connections.find((c) => c.id === id);
      get().notify(`Could not connect to ${conn?.name ?? "the database"}. ${connectErrorText(e, conn)}`, true);
      return false;
    }
  },

  closeConnection() {
    set({ activeConnectionId: null, activeTabId: null, selectedTable: null });
  },

  /** Forgets a saved connection: its pool, Keychain password, tabs and pinned tabs. */
  async deleteConnection(id) {
    try {
      await api.deleteConnection(id);
    } catch (e) {
      get().notify(`Could not delete the connection: ${errorText(e)}`, true);
      return false;
    }
    for (const t of get().tabs) {
      if (t.connectionId === id && t.kind !== "table" && t.resultId) api.releaseResult(t.resultId).catch(() => {});
    }
    try {
      localStorage.removeItem(`tv.pinned.${id}`);
    } catch {
      /* storage unavailable */
    }
    set((s) => {
      const { [id]: _schema, ...schemas } = s.schemas;
      const active = s.activeConnectionId === id;
      return {
        connections: s.connections.filter((c) => c.id !== id),
        tabs: s.tabs.filter((t) => t.connectionId !== id),
        closedTabs: s.closedTabs.filter((t) => t.connectionId !== id),
        schemas,
        ...(active ? { activeConnectionId: null, activeTabId: null, selectedTable: null } : {}),
      };
    });
    return true;
  },

  async refreshSchema() {
    const id = get().activeConnectionId;
    if (!id) return;
    try {
      const schema = await api.getSchema(id);
      set((s) => ({ schemas: { ...s.schemas, [id]: schema } }));
    } catch (e) {
      get().notify(errorText(e), true);
    }
  },

  openTable(table, connectionId) {
    const conn = connectionId ?? get().activeConnectionId;
    if (!conn) return;
    const existing = get().tabs.find(
      (t) => t.kind === "table" && t.connectionId === conn && t.table.schema === table.schema && t.table.name === table.name,
    );
    if (existing) return set({ activeTabId: existing.id, selectedTable: table });
    const tab: Tab = { id: newId("tab"), kind: "table", connectionId: conn, table };
    set((s) => ({ tabs: [...s.tabs, tab], activeTabId: tab.id, selectedTable: table }));
  },

  openQuery({ sql = "", title, resultId, connectionId, fromClaude } = {}) {
    const conn = connectionId ?? get().activeConnectionId!;
    const n = get().tabs.filter((t) => t.kind === "query").length + 1;
    const tab: Tab = { id: newId("tab"), kind: "query", connectionId: conn, title: title ?? `Query ${n}`, sql, resultId, fromClaude };
    set((s) => ({ tabs: [...s.tabs, tab], activeTabId: tab.id }));
    return tab.id;
  },

  openChart(connectionId, resultId, spec, title) {
    const tab: Tab = { id: newId("tab"), kind: "chart", connectionId, title, resultId, spec };
    set((s) => ({ tabs: [...s.tabs, tab], activeTabId: tab.id }));
  },

  updateTab(id, patch) {
    set((s) => ({ tabs: s.tabs.map((t) => (t.id === id ? ({ ...t, ...patch } as Tab) : t)) }));
    const tab = get().tabs.find((t) => t.id === id);
    if (tab?.pinned && ("sql" in patch || "title" in patch)) savePinned(tab.connectionId, get().tabs);
  },

  pinTab(id, pinned) {
    set((s) => {
      const tabs = s.tabs.map((t) => (t.id === id ? ({ ...t, pinned } as Tab) : t));
      // Pinned tabs sit first, keeping their relative order.
      return { tabs: [...tabs.filter((t) => t.pinned), ...tabs.filter((t) => !t.pinned)] };
    });
    const tab = get().tabs.find((t) => t.id === id);
    if (tab) savePinned(tab.connectionId, get().tabs);
  },

  closeOtherTabs(id) {
    const keep = get().tabs.find((t) => t.id === id);
    if (!keep) return;
    get().tabs
      .filter((t) => t.connectionId === keep.connectionId && t.id !== id && !t.pinned)
      .forEach((t) => get().closeTab(t.id));
    set({ activeTabId: id });
  },

  closeTab(id) {
    const tab = get().tabs.find((t) => t.id === id);
    if (!tab) return;
    api.closeTab(tab.connectionId, tab.id).catch(() => {});
    if (tab.kind !== "table" && tab.resultId) {
      const shared = get().tabs.some((t) => t.id !== id && t.kind !== "table" && t.resultId === tab.resultId);
      if (!shared) api.releaseResult(tab.resultId).catch(() => {});
    }
    set((s) => {
      const visible = s.tabs.filter((t) => t.connectionId === s.activeConnectionId);
      const i = visible.findIndex((t) => t.id === id);
      const tabs = s.tabs.filter((t) => t.id !== id);
      const rest = visible.filter((t) => t.id !== id);
      const activeTabId = s.activeTabId === id ? (rest[Math.max(0, i - 1)]?.id ?? null) : s.activeTabId;
      const closedTabs = tab.kind === "chart" ? s.closedTabs : [...s.closedTabs, { ...tab, pinned: false } as Tab].slice(-20);
      return { tabs, activeTabId, closedTabs };
    });
    if (tab.pinned) savePinned(tab.connectionId, get().tabs);
  },

  reopenTab() {
    const tab = get().closedTabs[get().closedTabs.length - 1];
    if (!tab) return;
    set((s) => ({ closedTabs: s.closedTabs.slice(0, -1) }));
    if (tab.kind === "table") get().openTable(tab.table, tab.connectionId);
    else if (tab.kind === "query") get().openQuery({ sql: tab.sql, title: tab.title, connectionId: tab.connectionId, fromClaude: tab.fromClaude });
  },

  setActiveTab(id) {
    set({ activeTabId: id });
  },

  notify(text, error = false) {
    set({ toast: { text, error } });
    setTimeout(() => {
      if (get().toast?.text === text) set({ toast: null });
    }, error ? 7000 : 2500);
  },

  set(patch) {
    set(patch);
  },

  setPref(key, value) {
    const prefs = { ...get().prefs, [key]: value };
    set({ prefs });
    api.savePrefs(prefs).catch((e) => get().notify(errorText(e), true));
  },

  async saveChatSettings(c) {
    set({ chatSettings: c });
    try {
      await api.saveChatSettings(c);
    } catch (e) {
      get().notify(errorText(e), true);
    }
  },
}));

useStore.subscribe((s, prev) => {
  if (s.activeConnectionId === prev.activeConnectionId && s.tabs === prev.tabs && s.activeTabId === prev.activeTabId) return;
  try {
    const id = s.activeConnectionId;
    if (!id) return localStorage.removeItem("tv.session");
    const visible = s.tabs.filter((t) => t.connectionId === id);
    const session: Session = {
      connectionId: id,
      tabs: visible
        .filter((t) => t.kind !== "chart")
        .map((t) => (t.kind === "table" ? { kind: "table", table: t.table, pinned: t.pinned } : { kind: "query", title: t.title, sql: (t as { sql: string }).sql, pinned: t.pinned })),
      active: visible.filter((t) => t.kind !== "chart").findIndex((t) => t.id === s.activeTabId),
    };
    localStorage.setItem("tv.session", JSON.stringify(session));
  } catch {
    /* storage unavailable: the next launch starts at the welcome screen */
  }
});

export const activeConnection =(s: State) => s.connections.find((c) => c.id === s.activeConnectionId) ?? null;
