use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use serde::Serialize;
use tauri::{AppHandle, Emitter};

use crate::config::{self, ConnectionConfig, DbKind, Settings, Store};
use crate::db::{self, Conn, Pool, Session, SharedPool, TableStructure, guard};
use crate::results::{QueryResult, ResultStore, Stored};

pub const CLAUDE_TIMEOUT: Duration = Duration::from_secs(30);
/// Shorter under test so the runaway-query test stays quick.
const SQLITE_DEADLINE: Duration = if cfg!(test) {
    Duration::from_secs(2)
} else {
    CLAUDE_TIMEOUT
};
/// Memory one Claude query may hold. Claude sees at most 500 rows of it.
pub const CLAUDE_MAX_BYTES: usize = 64 * 1024 * 1024;

/// Waits for any running query on the session, then closes its connection.
fn discard_session(s: Arc<tokio::sync::Mutex<Session>>) {
    tauri::async_runtime::spawn(async move {
        drop(s.lock().await);
        if let Ok(m) = Arc::try_unwrap(s) {
            m.into_inner().conn.discard();
        }
    });
}

pub struct AppState {
    pub store: Store,
    pub settings: Mutex<Settings>,
    pub connections: Mutex<Vec<ConnectionConfig>>,
    pools: Mutex<HashMap<String, SharedPool>>,
    connecting: tokio::sync::Mutex<()>,
    pub versions: Mutex<HashMap<String, String>>,
    sessions: Mutex<HashMap<String, Arc<tokio::sync::Mutex<Session>>>>,
    running: Mutex<HashMap<String, (SharedPool, i64)>>,
    /// The same running queries keyed by "connection|tab", so closing a tab can stop its query.
    running_tabs: Mutex<HashMap<String, (SharedPool, i64)>>,
    /// Filtered table pages in flight, by `BrowseOptions::supersede`, with a token naming the run.
    browsing: Mutex<HashMap<String, (String, Arc<tokio::sync::Notify>)>>,
    pub results: ResultStore,
    /// Row and byte caps for editor results; tests lower them.
    pub editor_limits: Mutex<(usize, usize)>,
    pub mcp_status: Mutex<String>,
    pub app: OnceLock<AppHandle>,
    /// Passwords set without the Keychain, for tests.
    pub password_override: Mutex<HashMap<String, String>>,
    /// Events emitted while no window exists, for tests.
    pub emitted: Mutex<Vec<(String, serde_json::Value)>>,
}

#[derive(Serialize, Clone)]
pub struct ShowResultEvent {
    pub connection_id: String,
    pub result_id: String,
    pub sql: String,
    pub title: String,
}

fn new_id(prefix: &str) -> String {
    format!("{prefix}-{}", uuid::Uuid::new_v4().simple())
}

impl AppState {
    pub fn new(store: Store) -> anyhow::Result<Self> {
        let settings = store.settings()?;
        let connections = store.connections()?;
        Ok(AppState {
            store,
            settings: Mutex::new(settings),
            connections: Mutex::new(connections),
            pools: Mutex::new(HashMap::new()),
            connecting: tokio::sync::Mutex::new(()),
            versions: Mutex::new(HashMap::new()),
            sessions: Mutex::new(HashMap::new()),
            running: Mutex::new(HashMap::new()),
            running_tabs: Mutex::new(HashMap::new()),
            browsing: Mutex::new(HashMap::new()),
            // Room for two full-size editor results plus pages and Claude's results.
            results: ResultStore::new(2 * db::UI_MAX_BYTES + 512 * 1024 * 1024),
            editor_limits: Mutex::new((db::UI_MAX_ROWS, db::UI_MAX_BYTES)),
            mcp_status: Mutex::new("starting".into()),
            app: OnceLock::new(),
            password_override: Mutex::new(HashMap::new()),
            emitted: Mutex::new(vec![]),
        })
    }

    pub fn emit<S: Serialize + Clone>(&self, event: &str, payload: S) {
        match self.app.get() {
            Some(app) => {
                let _ = app.emit(event, payload);
            }
            None => {
                let v = serde_json::to_value(payload).unwrap_or_default();
                self.emitted.lock().unwrap().push((event.to_string(), v));
            }
        }
    }

    pub fn config(&self, id: &str) -> anyhow::Result<ConnectionConfig> {
        self.connections
            .lock()
            .unwrap()
            .iter()
            .find(|c| c.id == id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("no connection with id {id}"))
    }

    /// Resolves a connection by id or, case-insensitively, by name.
    pub fn find_config(&self, key: &str) -> anyhow::Result<ConnectionConfig> {
        let list = self.connections.lock().unwrap();
        list.iter()
            .find(|c| c.id == key)
            .or_else(|| list.iter().find(|c| c.name.eq_ignore_ascii_case(key)))
            .cloned()
            .ok_or_else(|| {
                let names: Vec<_> = list.iter().map(|c| c.name.clone()).collect();
                anyhow::anyhow!(
                    "no connection named {key}. Known connections: {}",
                    names.join(", ")
                )
            })
    }

    pub async fn pool(&self, id: &str) -> anyhow::Result<SharedPool> {
        if let Some(p) = self.pools.lock().unwrap().get(id) {
            return Ok(p.clone());
        }
        let _guard = self.connecting.lock().await;
        if let Some(p) = self.pools.lock().unwrap().get(id) {
            return Ok(p.clone());
        }
        let cfg = self.config(id)?;
        let overridden = self.password_override.lock().unwrap().get(id).cloned();
        let pw = match overridden {
            Some(p) => Some(p),
            None => {
                // The Keychain can block on a system prompt, so keep it off the async workers.
                let key = id.to_string();
                tokio::task::spawn_blocking(move || config::get_password(&key)).await?
            }
        };
        let (pool, version) = db::connect(&cfg, pw.as_deref()).await?;
        let pool = Arc::new(pool);
        self.pools
            .lock()
            .unwrap()
            .insert(id.to_string(), pool.clone());
        self.versions
            .lock()
            .unwrap()
            .insert(id.to_string(), version);
        Ok(pool)
    }

    pub async fn disconnect(&self, id: &str) {
        let pool = self.pools.lock().unwrap().remove(id);
        let sessions: Vec<String> = self
            .sessions
            .lock()
            .unwrap()
            .keys()
            .filter(|k| k.starts_with(&format!("{id}|")))
            .cloned()
            .collect();
        for k in sessions {
            let removed = self.sessions.lock().unwrap().remove(&k);
            if let Some(s) = removed {
                discard_session(s);
            }
        }
        if let Some(p) = pool {
            p.close().await;
        }
    }

    pub async fn structure(
        &self,
        id: &str,
        schema: &str,
        table: &str,
    ) -> anyhow::Result<TableStructure> {
        self.pool(id).await?.describe(schema, table).await
    }

    fn store_result(
        &self,
        connection_id: &str,
        sql: &str,
        out: db::Output,
        table: Option<(String, String)>,
    ) -> Arc<Stored> {
        let meta = QueryResult {
            id: new_id("res"),
            connection_id: connection_id.to_string(),
            sql: sql.to_string(),
            columns: out.columns,
            row_count: out.row_count,
            elapsed_ms: out.elapsed_ms,
            truncated: out.truncated,
            rows_affected: out.rows_affected,
            estimated_rows: None,
            table,
        };
        self.results.put(meta, out.ipc)
    }

    async fn tab_session(
        &self,
        connection_id: &str,
        tab_id: &str,
    ) -> anyhow::Result<Arc<tokio::sync::Mutex<Session>>> {
        let key = format!("{connection_id}|{tab_id}");
        if let Some(s) = self.sessions.lock().unwrap().get(&key) {
            return Ok(s.clone());
        }
        let session = Arc::new(tokio::sync::Mutex::new(
            self.new_session(connection_id).await?,
        ));
        self.sessions.lock().unwrap().insert(key, session.clone());
        Ok(session)
    }

    async fn new_session(&self, connection_id: &str) -> anyhow::Result<Session> {
        let mut fresh = self.pool(connection_id).await?.session().await?;
        if self.config(connection_id)?.read_only {
            // Enforced by the server, not just the SQL parser.
            let stmt = match fresh.conn {
                Conn::Pg(_) => Some("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY"),
                Conn::My(_) => Some("SET SESSION TRANSACTION READ ONLY"),
                Conn::Lite(_) => None,
            };
            if let Some(stmt) = stmt {
                db::exec_raw(&mut fresh.conn, stmt).await?;
            }
        }
        Ok(fresh)
    }

    /// Closes the tab's pinned connection rather than returning it to the pool:
    /// it may hold an open transaction, SET ROLE or search_path changes.
    pub fn close_tab_session(&self, connection_id: &str, tab_id: &str) {
        let key = format!("{connection_id}|{tab_id}");
        let session = self.sessions.lock().unwrap().remove(&key);
        // Stop the tab's running query; otherwise the connection stays busy until it ends.
        if let Some((pool, bid)) = self.running_tabs.lock().unwrap().remove(&key) {
            tauri::async_runtime::spawn(async move {
                let _ = pool.cancel(bid).await;
            });
        }
        if let Some(s) = session {
            discard_session(s);
        }
    }

    /// SQLite's errors for a database file that was deleted or moved while open
    /// ("unable to open database file", "attempt to write a readonly database")
    /// do not say what happened; this does.
    fn explain_missing_file(&self, connection_id: &str, err: anyhow::Error) -> anyhow::Error {
        match self.config(connection_id) {
            Ok(cfg) if cfg.kind == DbKind::Sqlite => {
                let path = db::shellexpand_home(&cfg.file_path);
                if std::path::Path::new(&path).exists() {
                    err
                } else {
                    anyhow::anyhow!(
                        "The SQLite file {path} was moved or deleted while open, so nothing was read or written. Restore it and reconnect."
                    )
                }
            }
            _ => err,
        }
    }

    /// Runs editor SQL on the tab's pinned connection.
    pub async fn run_editor_query(
        &self,
        connection_id: &str,
        tab_id: &str,
        sql: &str,
        query_id: &str,
    ) -> anyhow::Result<QueryResult> {
        self.editor_query(connection_id, tab_id, sql, query_id)
            .await
            .map_err(|e| self.explain_missing_file(connection_id, e))
    }

    async fn editor_query(
        &self,
        connection_id: &str,
        tab_id: &str,
        sql: &str,
        query_id: &str,
    ) -> anyhow::Result<QueryResult> {
        let cfg = self.config(connection_id)?;
        if cfg.read_only && cfg.kind == DbKind::Sqlite {
            guard::check_read_only(cfg.kind, sql)
                .map_err(|e| anyhow::anyhow!("this connection is read-only: {e}"))?;
        }
        let pool = self.pool(connection_id).await?;
        let session = self.tab_session(connection_id, tab_id).await?;
        let mut s = session.lock().await;
        let key = format!("{connection_id}|{tab_id}");
        if let Some(bid) = s.backend_id {
            self.running
                .lock()
                .unwrap()
                .insert(query_id.to_string(), (pool.clone(), bid));
            self.running_tabs
                .lock()
                .unwrap()
                .insert(key.clone(), (pool.clone(), bid));
        }
        let cancel_pool = pool.clone();
        let bid = s.backend_id;
        let on_full = move || {
            if let Some(b) = bid {
                tokio::spawn(async move {
                    let _ = cancel_pool.cancel(b).await;
                });
            }
        };
        let limits = *self.editor_limits.lock().unwrap();
        let res = db::run_script(&mut s, sql, limits, on_full).await;
        self.running.lock().unwrap().remove(query_id);
        self.running_tabs.lock().unwrap().remove(&key);
        let tab_open = self
            .sessions
            .lock()
            .unwrap()
            .get(&key)
            .is_some_and(|cur| Arc::ptr_eq(cur, &session));
        if !tab_open {
            // Closed while running: nobody would ever release a stored result.
            anyhow::bail!("the tab was closed");
        }
        let out = match res {
            Err(err) if db::connection_lost(&err) => {
                // Drop the dead connection so the next run in this tab gets a fresh one.
                drop(s);
                let removed = {
                    let mut sessions = self.sessions.lock().unwrap();
                    match sessions.get(&key) {
                        Some(cur) if Arc::ptr_eq(cur, &session) => sessions.remove(&key),
                        _ => None,
                    }
                };
                if let Some(old) = removed {
                    discard_session(old);
                }
                let msg = db::error_message(&err).unwrap_or_else(|| err.to_string());
                anyhow::bail!(
                    "Lost the connection to the database: {msg}\nThe next run reconnects. Anything that lived on the old connection (an open transaction, SET, temp tables) is gone."
                );
            }
            other => other?,
        };
        if out.abandoned {
            let fresh = self.new_session(connection_id).await?;
            std::mem::replace(&mut *s, fresh).conn.discard();
        }
        Ok(self
            .store_result(connection_id, sql, out, None)
            .meta
            .clone())
    }

    pub async fn cancel(&self, query_id: &str) -> anyhow::Result<bool> {
        let entry = self.running.lock().unwrap().get(query_id).cloned();
        match entry {
            Some((pool, bid)) => pool.cancel(bid).await,
            None => Ok(false),
        }
    }

    pub async fn browse(
        &self,
        connection_id: &str,
        schema: &str,
        table: &str,
        opts: &db::BrowseOptions,
    ) -> anyhow::Result<QueryResult> {
        self.browse_page(connection_id, schema, table, opts)
            .await
            .map_err(|e| self.explain_missing_file(connection_id, e))
    }

    async fn browse_page(
        &self,
        connection_id: &str,
        schema: &str,
        table: &str,
        opts: &db::BrowseOptions,
    ) -> anyhow::Result<QueryResult> {
        let pool = self.pool(connection_id).await?;
        let st = pool.describe(schema, table).await?;
        let (sql, binds) = db::browse_sql(pool.kind(), schema, table, &st, opts)?;
        let supersede = opts.supersede.as_ref().filter(|_| !opts.filters.is_empty());
        let res = match supersede {
            None => {
                let mut conn = pool.conn().await?;
                let res = db::run_prepared(
                    &mut conn,
                    &sql,
                    &binds,
                    (opts.limit.max(1), db::UI_MAX_BYTES),
                    || {},
                )
                .await;
                if res.as_ref().is_ok_and(|o| o.abandoned) {
                    conn.discard();
                }
                res
            }
            Some(key) => {
                // A filter can scan the whole table. The next page with the same key
                // signals this one, which cancels its own query while it still holds
                // the connection, so the cancel cannot hit whoever uses it next.
                let mut s = pool.session().await?;
                let token = new_id("browse");
                let stop = Arc::new(tokio::sync::Notify::new());
                let prev = self
                    .browsing
                    .lock()
                    .unwrap()
                    .insert(key.clone(), (token.clone(), stop.clone()));
                if let Some((_, prev_stop)) = prev {
                    prev_stop.notify_one();
                }
                let bid = s.backend_id;
                let mut cancelled = false;
                let res = {
                    let run = db::run_prepared(
                        &mut s.conn,
                        &sql,
                        &binds,
                        (opts.limit.max(1), db::UI_MAX_BYTES),
                        || {},
                    );
                    tokio::pin!(run);
                    tokio::select! {
                        r = &mut run => r,
                        _ = stop.notified() => {
                            if let Some(bid) = bid {
                                let _ = pool.cancel(bid).await;
                                cancelled = true;
                            }
                            run.await
                        }
                    }
                };
                {
                    let mut b = self.browsing.lock().unwrap();
                    if b.get(key).is_some_and(|(t, _)| t == &token) {
                        b.remove(key);
                    }
                }
                // A cancel that lands after the query finished would hit the next
                // statement on this connection, so it is closed rather than reused.
                if cancelled || res.as_ref().is_ok_and(|o| o.abandoned) {
                    s.conn.discard();
                }
                res
            }
        };
        let out = res?;
        let mut meta = self
            .store_result(
                connection_id,
                &sql,
                out,
                Some((schema.to_string(), table.to_string())),
            )
            .meta
            .clone();
        meta.estimated_rows = pool.estimate_rows(schema, table).await;
        Ok(meta)
    }

    pub async fn count(
        &self,
        connection_id: &str,
        schema: &str,
        table: &str,
        filters: &[db::Filter],
    ) -> anyhow::Result<i64> {
        let pool = self.pool(connection_id).await?;
        let st = pool.describe(schema, table).await?;
        let (sql, binds) = db::count_sql(pool.kind(), schema, table, &st, filters)?;
        let mut conn = pool.conn().await?;
        let out = db::run_prepared(&mut conn, &sql, &binds, (1, 1 << 20), || {}).await?;
        let s = self.results.put(
            QueryResult {
                id: new_id("cnt"),
                connection_id: connection_id.into(),
                sql,
                columns: out.columns,
                row_count: out.row_count,
                elapsed_ms: out.elapsed_ms,
                truncated: false,
                rows_affected: None,
                estimated_rows: None,
                table: None,
            },
            out.ipc,
        );
        let batch = s.batch()?;
        self.results.release(&s.meta.id);
        let v = crate::edits::arrow_text(&batch, 0, 0, pool.kind()).unwrap_or_default();
        Ok(v.parse().unwrap_or(0))
    }

    /// Runs SQL for Claude: gated to one read-only statement, inside a
    /// read-only transaction that always rolls back, with a timeout.
    pub async fn run_claude_query(
        &self,
        key: &str,
        sql: &str,
        max_rows: usize,
    ) -> anyhow::Result<Arc<Stored>> {
        let cfg = self.find_config(key)?;
        if !cfg.allow_claude {
            anyhow::bail!(
                "Claude access is turned off for connection {}. Enable it in the connection settings.",
                cfg.name
            );
        }
        guard::check_read_only(cfg.kind, sql).map_err(|e| anyhow::anyhow!(e))?;
        let pool = self.pool(&cfg.id).await?;
        let limits = (max_rows, CLAUDE_MAX_BYTES);
        let out = match pool.as_ref() {
            Pool::Lite { ro, .. } => {
                let mut c = ro.acquire().await?;
                // SQLite cannot be cancelled from outside, so the statement checks
                // its own deadline; otherwise a runaway query holds one of the two
                // read-only connections until the app quits.
                let deadline = std::time::Instant::now() + SQLITE_DEADLINE;
                c.lock_handle()
                    .await?
                    .set_progress_handler(10_000, move || std::time::Instant::now() < deadline);
                let mut conn = Conn::Lite(c);
                let res = tokio::time::timeout(
                    CLAUDE_TIMEOUT + Duration::from_secs(5),
                    db::run_prepared(&mut conn, sql, &[], limits, || {}),
                )
                .await;
                if let Conn::Lite(c) = &mut conn
                    && let Ok(mut h) = c.lock_handle().await
                {
                    h.remove_progress_handler();
                }
                let timed_out =
                    || anyhow::anyhow!("query timed out after {}s", CLAUDE_TIMEOUT.as_secs());
                match res {
                    Err(_) => return Err(timed_out()),
                    Ok(Err(e)) if e.to_string().contains("interrupt") => return Err(timed_out()),
                    Ok(r) => r?,
                }
            }
            _ => {
                let mut s = pool.session().await?;
                let (begin, end): (&[&'static str], &[&'static str]) = match cfg.kind {
                    // Session-level locks survive ROLLBACK and would ride along on the
                    // pooled connection, so they are released even if the gate missed one.
                    DbKind::Postgres => (
                        &["BEGIN READ ONLY", "SET LOCAL statement_timeout = '30s'"],
                        &["ROLLBACK", "SELECT pg_advisory_unlock_all()"],
                    ),
                    _ => (
                        &[
                            "SET SESSION max_execution_time = 30000",
                            "START TRANSACTION READ ONLY",
                        ],
                        &[
                            "ROLLBACK",
                            "SELECT RELEASE_ALL_LOCKS()",
                            "SET SESSION max_execution_time = 0",
                        ],
                    ),
                };
                for stmt in begin {
                    if let Err(err) = db::exec_raw(&mut s.conn, stmt).await {
                        s.conn.discard();
                        return Err(err);
                    }
                }
                let bid = s.backend_id;
                let cancel_pool = pool.clone();
                let on_full = move || {
                    if let Some(b) = bid {
                        tokio::spawn(async move {
                            let _ = cancel_pool.cancel(b).await;
                        });
                    }
                };
                let res = tokio::time::timeout(
                    CLAUDE_TIMEOUT + Duration::from_secs(5),
                    db::run_prepared_in_tx(&mut s.conn, sql, &[], limits, on_full),
                )
                .await;
                if res.is_err()
                    && let Some(b) = bid
                {
                    let _ = pool.cancel(b).await;
                }
                let mut clean = true;
                for stmt in end {
                    if db::exec_raw(&mut s.conn, stmt).await.is_err() {
                        clean = false;
                    }
                }
                let out = res.map_err(|_| {
                    anyhow::anyhow!("query timed out after {}s", CLAUDE_TIMEOUT.as_secs())
                });
                if !clean
                    || out
                        .as_ref()
                        .map_or(true, |o| o.as_ref().is_ok_and(|o| o.abandoned))
                {
                    s.conn.discard();
                }
                out??
            }
        };
        Ok(self.store_result(&cfg.id, sql, out, None))
    }
}
