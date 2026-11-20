pub mod columns;
pub mod decode;
pub mod guard;

use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::StreamExt;
use serde::{Deserialize, Serialize};
use sqlx::mysql::{MySqlConnectOptions, MySqlPool, MySqlPoolOptions, MySqlSslMode};
use sqlx::pool::PoolConnection;
use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions, PgSslMode};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};
use sqlx::{
    AssertSqlSafe, Column, Either, Executor, MySql, Postgres, Row, SqlSafeStr, Sqlite,
    Statement,
};

use crate::config::{ConnectionConfig, DbKind};
use columns::{Cell, Collector};

/// Editor results stop at whichever cap comes first. The byte cap is the real
/// guard: it counts the Arrow data being built, which is what the result costs
/// in memory here and again in the grid.
pub const UI_MAX_ROWS: usize = 10_000_000;
pub const UI_MAX_BYTES: usize = 1024 * 1024 * 1024;

#[derive(Clone)]
pub enum Pool {
    Pg(PgPool),
    My(MySqlPool),
    Lite { rw: SqlitePool, ro: SqlitePool },
}

pub enum Conn {
    Pg(PoolConnection<Postgres>),
    My(PoolConnection<MySql>),
    Lite(PoolConnection<Sqlite>),
}

/// A connection pinned to one query tab, so session state (SET, temp tables,
/// open transactions) survives between runs in that tab.
pub struct Session {
    pub conn: Conn,
    /// Server-side id of this connection, used to cancel its running query.
    pub backend_id: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableRef {
    pub schema: String,
    pub name: String,
    pub kind: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SchemaTree {
    pub schemas: Vec<SchemaGroup>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaGroup {
    pub name: String,
    pub tables: Vec<TableRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub default: Option<String>,
    pub is_pk: bool,
    /// Part of a foreign key.
    #[serde(default)]
    pub is_fk: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexInfo {
    pub name: String,
    pub columns: Vec<String>,
    pub unique: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TableStructure {
    pub columns: Vec<ColumnInfo>,
    pub indexes: Vec<IndexInfo>,
}

impl TableStructure {
    /// Primary key columns in key order, not table order: `ORDER BY` the key's own
    /// order lets the database walk the index instead of sorting the whole table.
    pub fn pk(&self) -> Vec<&ColumnInfo> {
        let mut pk: Vec<&ColumnInfo> = self.columns.iter().filter(|c| c.is_pk).collect();
        let same_set = |idx: &IndexInfo| {
            idx.unique
                && idx.columns.len() == pk.len()
                && pk.iter().all(|c| idx.columns.contains(&c.name))
        };
        if let Some(idx) = self.indexes.iter().find(|i| same_set(i)) {
            let order = idx.columns.clone();
            pk.sort_by_key(|c| order.iter().position(|n| n == &c.name));
        }
        pk
    }
    pub fn column(&self, name: &str) -> Option<&ColumnInfo> {
        self.columns.iter().find(|c| c.name == name)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ResultColumn {
    pub name: String,
    pub db_type: String,
}

/// The decoded output of one statement, before it is stored.
pub struct Output {
    pub columns: Vec<ResultColumn>,
    pub ipc: Vec<u8>,
    pub row_count: usize,
    pub truncated: bool,
    pub rows_affected: Option<u64>,
    pub elapsed_ms: u64,
    /// The connection still has unread rows from a query the server would not
    /// cancel; it must be closed instead of reused.
    pub abandoned: bool,
}

fn ssl_pg(mode: &str) -> PgSslMode {
    match mode {
        "disable" => PgSslMode::Disable,
        "require" => PgSslMode::Require,
        _ => PgSslMode::Prefer,
    }
}

fn ssl_my(mode: &str) -> MySqlSslMode {
    match mode {
        "disable" => MySqlSslMode::Disabled,
        "require" => MySqlSslMode::Required,
        _ => MySqlSslMode::Preferred,
    }
}

pub async fn connect(
    cfg: &ConnectionConfig,
    password: Option<&str>,
) -> anyhow::Result<(Pool, String)> {
    let timeout = Duration::from_secs(10);
    match cfg.kind {
        DbKind::Postgres => {
            let mut o = PgConnectOptions::new()
                .host(&cfg.host)
                .port(cfg.port)
                .username(&cfg.user)
                .database(&cfg.database)
                .ssl_mode(ssl_pg(&cfg.ssl_mode))
                .application_name("TableView")
                // Pooler-safe (PgBouncer, Supabase, Neon): a transaction-mode pooler
                // shares server sessions between clients, so sqlx's numbered
                // statements (sqlx_s_1, …) collide with ones another client left
                // there. TableView only uses unnamed statements; see `pg_query`.
                .statement_cache_capacity(0);
            if let Some(p) = password {
                o = o.password(p);
            }
            let pool = PgPoolOptions::new()
                .max_connections(8)
                .acquire_timeout(timeout)
                .connect_with(o)
                .await?;
            let v: String = sqlx::raw_sql("SELECT current_setting('server_version')")
                .fetch_one(&pool)
                .await?
                .try_get(0)?;
            Ok((Pool::Pg(pool), format!("PostgreSQL {v}")))
        }
        DbKind::Mysql => {
            let mut o = MySqlConnectOptions::new()
                .host(&cfg.host)
                .port(cfg.port)
                .username(&cfg.user)
                .database(&cfg.database)
                .ssl_mode(ssl_my(&cfg.ssl_mode))
                .timezone(Some(String::from("+00:00")));
            if let Some(p) = password {
                o = o.password(p);
            }
            let pool = MySqlPoolOptions::new()
                .max_connections(8)
                .acquire_timeout(timeout)
                .connect_with(o)
                .await?;
            let v: String = sqlx::query_scalar("SELECT CAST(VERSION() AS CHAR)")
                .fetch_one(&pool)
                .await?;
            Ok((Pool::My(pool), format!("MySQL {v}")))
        }
        DbKind::Sqlite => {
            let path = shellexpand_home(&cfg.file_path);
            if !std::path::Path::new(&path).exists() {
                anyhow::bail!("SQLite file not found: {path}");
            }
            let rw = SqlitePoolOptions::new()
                .max_connections(4)
                .connect_with(
                    SqliteConnectOptions::new()
                        .filename(&path)
                        .read_only(cfg.read_only),
                )
                .await?;
            let ro = SqlitePoolOptions::new()
                .max_connections(2)
                .after_connect(|c, _| {
                    Box::pin(async move {
                        c.execute("PRAGMA query_only = ON").await?;
                        Ok(())
                    })
                })
                .connect_with(SqliteConnectOptions::new().filename(&path).read_only(true))
                .await?;
            let v: String = sqlx::query_scalar("SELECT sqlite_version()")
                .fetch_one(&rw)
                .await?;
            Ok((Pool::Lite { rw, ro }, format!("SQLite {v}")))
        }
    }
}

pub fn shellexpand_home(p: &str) -> String {
    match (p.strip_prefix("~/"), dirs::home_dir()) {
        (Some(rest), Some(home)) => home.join(rest).to_string_lossy().into_owned(),
        _ => p.to_string(),
    }
}

impl Conn {
    /// Closes the underlying connection instead of returning it to the pool.
    pub fn discard(self) {
        match self {
            Conn::Pg(c) => drop(c.detach()),
            Conn::My(c) => drop(c.detach()),
            Conn::Lite(c) => drop(c.detach()),
        }
    }
}

impl Pool {
    pub fn kind(&self) -> DbKind {
        match self {
            Pool::Pg(_) => DbKind::Postgres,
            Pool::My(_) => DbKind::Mysql,
            Pool::Lite { .. } => DbKind::Sqlite,
        }
    }

    pub async fn close(&self) {
        match self {
            Pool::Pg(p) => p.close().await,
            Pool::My(p) => p.close().await,
            Pool::Lite { rw, ro } => {
                rw.close().await;
                ro.close().await;
            }
        }
    }

    pub async fn session(&self) -> anyhow::Result<Session> {
        Ok(match self {
            Pool::Pg(p) => {
                let mut c = p.acquire().await?;
                let id: i32 = sqlx::raw_sql("SELECT pg_backend_pid()")
                    .fetch_one(&mut *c)
                    .await?
                    .try_get(0)?;
                Session {
                    conn: Conn::Pg(c),
                    backend_id: Some(id as i64),
                }
            }
            Pool::My(p) => {
                let mut c = p.acquire().await?;
                let id: u64 = sqlx::query_scalar("SELECT CONNECTION_ID()")
                    .fetch_one(&mut *c)
                    .await?;
                Session {
                    conn: Conn::My(c),
                    backend_id: Some(id as i64),
                }
            }
            Pool::Lite { rw, .. } => Session {
                conn: Conn::Lite(rw.acquire().await?),
                backend_id: None,
            },
        })
    }

    /// Ask the server to stop whatever the connection `backend_id` is running.
    pub async fn cancel(&self, backend_id: i64) -> anyhow::Result<bool> {
        match self {
            Pool::Pg(p) => Ok(sqlx::raw_sql(AssertSqlSafe(format!(
                "SELECT pg_cancel_backend({})",
                backend_id as i32
            )))
            .fetch_one(p)
            .await?
            .try_get(0)?),
            Pool::My(p) => {
                sqlx::raw_sql(AssertSqlSafe(format!("KILL QUERY {backend_id}")))
                    .execute(p)
                    .await?;
                Ok(true)
            }
            Pool::Lite { .. } => Ok(false),
        }
    }
}

macro_rules! drain {
    ($stream:expr, $decoder:ident, $limits:expr, $on_full:expr) => {{
        let (max_rows, max_bytes) = $limits;
        let mut stream = $stream;
        let mut last: Option<Collector> = None;
        let mut cur = Collector::new(max_rows, max_bytes);
        let mut affected: Option<u64> = None;
        let mut truncated = false;
        let mut truncated_at: Option<Instant> = None;
        let mut abandoned = false;
        let mut on_full = Some($on_full);
        let mut err: Option<sqlx::Error> = None;
        let mut dec: Option<decode::$decoder> = None;
        while let Some(item) = stream.next().await {
            match item {
                Ok(Either::Right(row)) => {
                    if truncated {
                        // The cancel has not taken effect yet. Give up on a server that ignores it.
                        if truncated_at.is_some_and(|t| t.elapsed() > Duration::from_secs(3)) {
                            abandoned = true;
                            break;
                        }
                        continue;
                    }
                    if !cur.has_columns() {
                        let (n, t) = decode::column_meta(&row);
                        cur.set_columns(n, t);
                        dec = Some(decode::$decoder::new(&row));
                    }
                    if cur.full() {
                        // A row beyond the cap: the result really is truncated.
                        truncated = true;
                        truncated_at = Some(Instant::now());
                        #[allow(unused_mut)]
                        if let Some(mut f) = on_full.take() {
                            f();
                        }
                        continue;
                    }
                    if let Some(d) = &dec {
                        d.push(&row, &mut cur);
                    }
                }
                Ok(Either::Left(res)) => {
                    if cur.has_columns() {
                        last = Some(std::mem::replace(
                            &mut cur,
                            Collector::new(max_rows, max_bytes),
                        ));
                    } else {
                        *affected.get_or_insert(0) += res.rows_affected();
                    }
                    if truncated {
                        break;
                    }
                }
                Err(e) => {
                    if !truncated {
                        err = Some(e);
                    }
                    break;
                }
            }
        }
        drop(stream);
        if cur.has_columns() {
            last = Some(cur);
        }
        match err {
            Some(e) => Err(anyhow::Error::from(e)),
            None => Ok((last, affected, truncated, abandoned)),
        }
    }};
}

fn finish(
    collected: (Option<Collector>, Option<u64>, bool, bool),
    started: Instant,
    fallback_columns: Vec<(String, String)>,
) -> anyhow::Result<Output> {
    let (last, affected, truncated, abandoned) = collected;
    let elapsed_ms = started.elapsed().as_millis() as u64;
    let col = match last {
        Some(c) => c,
        None => {
            let mut c = Collector::new(0, 0);
            let (n, t): (Vec<_>, Vec<_>) = fallback_columns.into_iter().unzip();
            c.set_columns(n, t);
            c
        }
    };
    let columns = col
        .names
        .iter()
        .zip(&col.db_types)
        .map(|(n, t)| ResultColumn {
            name: n.clone(),
            db_type: t.clone(),
        })
        .collect();
    let row_count = col.rows;
    let ipc = col.to_ipc()?;
    let rows_affected = if row_count == 0 && affected.is_some() {
        affected
    } else {
        None
    };
    Ok(Output {
        columns,
        ipc,
        row_count,
        truncated,
        rows_affected,
        elapsed_ms,
        abandoned,
    })
}

fn returns_rows(sql: &str) -> bool {
    let s = sql.trim_start().to_ascii_lowercase();
    [
        "select", "with", "show", "explain", "values", "table", "pragma", "describe", "desc ",
    ]
    .iter()
    .any(|k| s.starts_with(k))
}

/// Column names for a statement that returned no rows, so an empty result still shows headers.
pub(crate) async fn empty_columns(conn: &mut Conn, sql: &str) -> Vec<(String, String)> {
    if !returns_rows(sql) {
        return vec![];
    }
    let cols = |names: Vec<(String, String)>| names;
    match conn {
        // Not a prepared statement: sqlx would name it, which breaks behind poolers.
        // One row of NULLs from a subquery the planner never runs (WHERE false)
        // carries the column names and types.
        Conn::Pg(c) => {
            let body = sql.trim().trim_end_matches(';');
            let probe = format!(
                "SELECT _tv.* FROM (SELECT 1) AS _tv_one LEFT JOIN \
                 (SELECT * FROM (\n{body}\n) AS _tv_q WHERE false) AS _tv ON true"
            );
            match sqlx::raw_sql(AssertSqlSafe(probe)).fetch_optional(&mut **c).await {
                Ok(Some(row)) => cols(
                    row.columns()
                        .iter()
                        .map(|c| {
                            (
                                c.name().to_string(),
                                sqlx::TypeInfo::name(c.type_info()).to_string(),
                            )
                        })
                        .collect(),
                ),
                _ => vec![],
            }
        }
        Conn::My(c) => match c
            .prepare(AssertSqlSafe(sql.to_string()).into_sql_str())
            .await
        {
            Ok(st) => cols(
                st.columns()
                    .iter()
                    .map(|c| {
                        (
                            c.name().to_string(),
                            sqlx::TypeInfo::name(c.type_info()).to_string(),
                        )
                    })
                    .collect(),
            ),
            Err(_) => vec![],
        },
        Conn::Lite(c) => match c
            .prepare(AssertSqlSafe(sql.to_string()).into_sql_str())
            .await
        {
            Ok(st) => cols(
                st.columns()
                    .iter()
                    .map(|c| {
                        (
                            c.name().to_string(),
                            sqlx::TypeInfo::name(c.type_info()).to_string(),
                        )
                    })
                    .collect(),
            ),
            Err(_) => vec![],
        },
    }
}

/// Runs user SQL from the editor. It may hold several statements. The result is
/// the last statement that returned rows, or the summed rows affected.
pub async fn run_script(
    session: &mut Session,
    sql: &str,
    limits: (usize, usize),
    on_full: impl FnOnce() + Send,
) -> anyhow::Result<Output> {
    let started = Instant::now();
    let collected = match &mut session.conn {
        Conn::Pg(c) => drain!(
            sqlx::raw_sql(AssertSqlSafe(sql.to_string())).fetch_many(&mut **c),
            PgDecoder,
            limits,
            on_full
        )?,
        Conn::My(c) => drain!(
            sqlx::raw_sql(AssertSqlSafe(sql.to_string())).fetch_many(&mut **c),
            MySqlDecoder,
            limits,
            on_full
        )?,
        Conn::Lite(c) => drain!(
            sqlx::raw_sql(AssertSqlSafe(sql.to_string())).fetch_many(&mut **c),
            SqliteDecoder,
            limits,
            on_full
        )?,
    };
    let fallback = if collected.0.is_none() && collected.1.is_none() {
        empty_columns(&mut session.conn, sql).await
    } else {
        vec![]
    };
    finish(collected, started, fallback)
}

/// Runs one prepared statement with text binds, outside a transaction.
/// Prepared statements reject multiple statements on Postgres and MySQL.
pub async fn run_prepared(
    conn: &mut Conn,
    sql: &str,
    binds: &[Option<String>],
    limits: (usize, usize),
    on_full: impl FnOnce() + Send,
) -> anyhow::Result<Output> {
    prepared(conn, sql, binds, limits, on_full, false).await
}

/// Like `run_prepared`, inside a transaction the caller opened with BEGIN.
pub async fn run_prepared_in_tx(
    conn: &mut Conn,
    sql: &str,
    binds: &[Option<String>],
    limits: (usize, usize),
    on_full: impl FnOnce() + Send,
) -> anyhow::Result<Output> {
    prepared(conn, sql, binds, limits, on_full, true).await
}

#[allow(deprecated)]
async fn prepared(
    conn: &mut Conn,
    sql: &str,
    binds: &[Option<String>],
    limits: (usize, usize),
    on_full: impl FnOnce() + Send,
    in_tx: bool,
) -> anyhow::Result<Output> {
    let started = Instant::now();
    let collected = match conn {
        Conn::Pg(c) => {
            // Inside a transaction a failed statement aborts it, so a retry needs a savepoint.
            let mut on_full = Some(on_full);
            let mut attempt = 0;
            loop {
                attempt += 1;
                if in_tx {
                    sqlx::raw_sql("SAVEPOINT tv_stmt").execute(&mut **c).await?;
                }
                let mut q = pg_query(sql);
                for b in binds {
                    q = q.bind(b.clone());
                }
                let res = drain!(q.fetch_many(&mut **c), PgDecoder, limits, || {
                    if let Some(f) = on_full.take() {
                        f()
                    }
                });
                match res {
                    Err(e) if attempt < 3 && pg_unnamed_lost(&e) => {
                        if in_tx {
                            sqlx::raw_sql("ROLLBACK TO SAVEPOINT tv_stmt").execute(&mut **c).await?;
                        }
                    }
                    Ok(r) => {
                        // An abandoned stream still has rows on the wire; the caller closes it.
                        if in_tx && !r.3 {
                            sqlx::raw_sql("RELEASE SAVEPOINT tv_stmt").execute(&mut **c).await?;
                        }
                        break r;
                    }
                    Err(e) => return Err(e),
                }
            }
        }
        Conn::My(c) => {
            let mut q = sqlx::query::<MySql>(AssertSqlSafe(sql.to_string()));
            for b in binds {
                q = q.bind(b.clone());
            }
            drain!(q.fetch_many(&mut **c), MySqlDecoder, limits, on_full)?
        }
        Conn::Lite(c) => {
            let mut q = sqlx::query::<Sqlite>(AssertSqlSafe(sql.to_string()));
            for b in binds {
                q = q.bind(b.clone());
            }
            drain!(q.fetch_many(&mut **c), SqliteDecoder, limits, on_full)?
        }
    };
    let fallback = if collected.0.is_none() && collected.1.is_none() {
        empty_columns(conn, sql).await
    } else {
        vec![]
    };
    finish(collected, started, fallback)
}

/// A Postgres query sent as an unnamed prepared statement. sqlx names
/// statements sqlx_s_1, sqlx_s_2, … per connection; behind a transaction-mode
/// pooler the server session is shared, and a name another client already used
/// fails with "prepared statement sqlx_s_1 already exists".
fn pg_query(sql: &str) -> sqlx::query::Query<'static, Postgres, sqlx::postgres::PgArguments> {
    sqlx::query::<Postgres>(AssertSqlSafe(sql.to_string())).persistent(false)
}

/// The unnamed statement was gone at Bind (SQLSTATE 26000). Nothing ran, so the
/// query can be sent again. Two causes: sqlx looks up a custom column type
/// (enum, domain) with a plain query between Parse and Bind, which discards the
/// unnamed statement (the type is cached afterwards); or a transaction-mode
/// pooler moved the client to another server session between the two.
fn pg_unnamed_lost(e: &anyhow::Error) -> bool {
    e.downcast_ref::<sqlx::Error>()
        .and_then(|e| e.as_database_error())
        .and_then(|d| d.code())
        .is_some_and(|c| c == "26000")
}

/// Runs a fixed control statement (BEGIN, SET, ROLLBACK) over the simple protocol.
pub async fn exec_raw(conn: &mut Conn, sql: &'static str) -> anyhow::Result<()> {
    match conn {
        Conn::Pg(c) => {
            sqlx::raw_sql(sql).execute(&mut **c).await?;
        }
        Conn::My(c) => {
            sqlx::raw_sql(sql).execute(&mut **c).await?;
        }
        Conn::Lite(c) => {
            sqlx::raw_sql(sql).execute(&mut **c).await?;
        }
    }
    Ok(())
}

impl Pool {
    pub async fn conn(&self) -> anyhow::Result<Conn> {
        Ok(match self {
            Pool::Pg(p) => Conn::Pg(p.acquire().await?),
            Pool::My(p) => Conn::My(p.acquire().await?),
            Pool::Lite { rw, .. } => Conn::Lite(rw.acquire().await?),
        })
    }

    async fn rows(&self, sql: &str, binds: &[&str]) -> anyhow::Result<Vec<Vec<Cell>>> {
        Ok(match self {
            Pool::Pg(p) => {
                let mut attempt = 0;
                loop {
                    attempt += 1;
                    let mut q = pg_query(sql);
                    for b in binds {
                        q = q.bind(b.to_string());
                    }
                    match q.fetch_all(p).await {
                        Ok(rows) => break rows.iter().map(decode::pg_row).collect(),
                        Err(e) => {
                            let e = anyhow::Error::from(e);
                            if attempt >= 3 || !pg_unnamed_lost(&e) {
                                return Err(e);
                            }
                        }
                    }
                }
            }
            Pool::My(p) => {
                let mut q = sqlx::query::<MySql>(AssertSqlSafe(sql.to_string()));
                for b in binds {
                    q = q.bind(b.to_string());
                }
                q.fetch_all(p)
                    .await?
                    .iter()
                    .map(decode::mysql_row)
                    .collect()
            }
            Pool::Lite { rw, .. } => {
                let mut q = sqlx::query::<Sqlite>(AssertSqlSafe(sql.to_string()));
                for b in binds {
                    q = q.bind(b.to_string());
                }
                q.fetch_all(rw)
                    .await?
                    .iter()
                    .map(decode::sqlite_row)
                    .collect()
            }
        })
    }

    pub async fn schema_tree(&self) -> anyhow::Result<SchemaTree> {
        let sql = match self {
            Pool::Pg(_) => {
                "SELECT table_schema::text, table_name::text, table_type::text FROM information_schema.tables \
                 WHERE table_schema NOT IN ('pg_catalog','information_schema') AND table_schema NOT LIKE 'pg_toast%' \
                 AND table_schema NOT LIKE 'pg_temp%' \
                 UNION ALL SELECT schemaname::text, matviewname::text, 'VIEW' FROM pg_matviews ORDER BY 1, 2"
            }
            Pool::My(_) => {
                "SELECT CAST(TABLE_SCHEMA AS CHAR), CAST(TABLE_NAME AS CHAR), CAST(TABLE_TYPE AS CHAR) \
                 FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() ORDER BY 2"
            }
            Pool::Lite { .. } => {
                "SELECT 'main', name, type FROM sqlite_master WHERE type IN ('table','view') \
                 AND name NOT LIKE 'sqlite_%' ORDER BY name"
            }
        };
        let mut tree = SchemaTree::default();
        for r in self.rows(sql, &[]).await? {
            let mut it = r.into_iter().map(|c| c.into_string().unwrap_or_default());
            let (schema, name, kind) = (
                it.next().unwrap_or_default(),
                it.next().unwrap_or_default(),
                it.next().unwrap_or_default(),
            );
            let kind = if kind.to_ascii_uppercase().contains("VIEW") {
                "view"
            } else {
                "table"
            }
            .to_string();
            match tree.schemas.iter_mut().find(|g| g.name == schema) {
                Some(g) => g.tables.push(TableRef {
                    schema: schema.clone(),
                    name,
                    kind,
                }),
                None => tree.schemas.push(SchemaGroup {
                    name: schema.clone(),
                    tables: vec![TableRef { schema, name, kind }],
                }),
            }
        }
        if let Some(i) = tree.schemas.iter().position(|g| g.name == "public") {
            let g = tree.schemas.remove(i);
            tree.schemas.insert(0, g);
        }
        Ok(tree)
    }

    pub async fn describe(&self, schema: &str, table: &str) -> anyhow::Result<TableStructure> {
        let s = |c: &Cell| c.clone().into_string();
        let b = |c: &Cell| match c {
            Cell::Bool(v) => *v,
            Cell::Int(v) => *v != 0,
            Cell::Str(v) => v == "t" || v == "true" || v == "1",
            _ => false,
        };
        let (cols_sql, idx_sql) = match self {
            Pool::Pg(_) => (
                "SELECT a.attname::text, format_type(a.atttypid, a.atttypmod), NOT a.attnotnull, \
                 pg_get_expr(d.adbin, d.adrelid), COALESCE(a.attnum = ANY(i.indkey), false), \
                 EXISTS (SELECT 1 FROM pg_constraint k WHERE k.conrelid = c.oid AND k.contype = 'f' AND a.attnum = ANY(k.conkey)) \
                 FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace \
                 LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum \
                 LEFT JOIN pg_index i ON i.indrelid = c.oid AND i.indisprimary \
                 WHERE n.nspname = $1 AND c.relname = $2 AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum",
                "SELECT ic.relname::text, string_agg(a.attname::text, ',' ORDER BY k.ord), ix.indisunique \
                 FROM pg_index ix JOIN pg_class c ON c.oid = ix.indrelid JOIN pg_namespace n ON n.oid = c.relnamespace \
                 JOIN pg_class ic ON ic.oid = ix.indexrelid \
                 CROSS JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY k(attnum, ord) \
                 JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = k.attnum \
                 WHERE n.nspname = $1 AND c.relname = $2 GROUP BY ic.relname, ix.indisunique ORDER BY 1",
            ),
            Pool::My(_) => (
                "SELECT CAST(COLUMN_NAME AS CHAR), CAST(COLUMN_TYPE AS CHAR), IS_NULLABLE = 'YES', \
                 CAST(COLUMN_DEFAULT AS CHAR), COLUMN_KEY = 'PRI', EXISTS (SELECT 1 FROM information_schema.KEY_COLUMN_USAGE k \
                 WHERE k.TABLE_SCHEMA = c.TABLE_SCHEMA AND k.TABLE_NAME = c.TABLE_NAME AND k.COLUMN_NAME = c.COLUMN_NAME \
                 AND k.REFERENCED_TABLE_NAME IS NOT NULL) FROM information_schema.COLUMNS c \
                 WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ? ORDER BY c.ORDINAL_POSITION",
                "SELECT CAST(INDEX_NAME AS CHAR), CAST(GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) AS CHAR), \
                 MIN(NON_UNIQUE) = 0 FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? \
                 GROUP BY INDEX_NAME ORDER BY 1",
            ),
            Pool::Lite { .. } => (
                "SELECT name, type, \"notnull\" = 0, dflt_value, pk > 0, \
                 EXISTS (SELECT 1 FROM pragma_foreign_key_list(?2) f WHERE f.\"from\" = name) \
                 FROM pragma_table_info(?2) WHERE ?1 = ?1",
                "SELECT il.name, group_concat(ii.name, ','), il.\"unique\" FROM pragma_index_list(?2) il, \
                 pragma_index_info(il.name) ii WHERE ?1 = ?1 GROUP BY il.name ORDER BY 1",
            ),
        };
        let columns = self
            .rows(cols_sql, &[schema, table])
            .await?
            .iter()
            .map(|r| ColumnInfo {
                name: s(&r[0]).unwrap_or_default(),
                data_type: s(&r[1]).unwrap_or_default(),
                nullable: b(&r[2]),
                default: s(&r[3]),
                is_pk: b(&r[4]),
                is_fk: b(&r[5]),
            })
            .collect::<Vec<_>>();
        if columns.is_empty() {
            anyhow::bail!("table {schema}.{table} not found");
        }
        let indexes = self
            .rows(idx_sql, &[schema, table])
            .await?
            .iter()
            .map(|r| IndexInfo {
                name: s(&r[0]).unwrap_or_default(),
                columns: s(&r[1])
                    .unwrap_or_default()
                    .split(',')
                    .map(str::to_string)
                    .collect(),
                unique: b(&r[2]),
            })
            .collect();
        Ok(TableStructure { columns, indexes })
    }
}

impl Pool {
    /// The planner's row estimate for a table, which costs nothing on any table
    /// size, where `COUNT(*)` reads the whole table. None for views, SQLite and
    /// tables never analyzed.
    pub async fn estimate_rows(&self, schema: &str, table: &str) -> Option<i64> {
        let sql = match self {
            // reltuples is from the last ANALYZE; scaled by the table's current size like the planner does.
            Pool::Pg(_) => {
                "SELECT CASE WHEN c.relkind = 'p' THEN (SELECT sum(GREATEST(ch.reltuples, 0))::bigint \
                 FROM pg_inherits i JOIN pg_class ch ON ch.oid = i.inhrelid WHERE i.inhparent = c.oid) \
                 WHEN c.reltuples < 0 THEN NULL \
                 WHEN c.relpages > 0 THEN (c.reltuples / c.relpages * (pg_relation_size(c.oid) / current_setting('block_size')::int))::bigint \
                 ELSE c.reltuples::bigint END \
                 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind IN ('r', 'p', 'm')"
            }
            Pool::My(_) => {
                "SELECT CAST(TABLE_ROWS AS SIGNED) FROM information_schema.TABLES \
                 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND TABLE_TYPE = 'BASE TABLE'"
            }
            Pool::Lite { .. } => return None,
        };
        let rows = self.rows(sql, &[schema, table]).await.ok()?;
        match rows.first()?.first()? {
            Cell::Int(n) => Some(*n),
            Cell::Str(s) => s.parse().ok(),
            _ => None,
        }
    }
}

pub fn quote_ident(kind: DbKind, id: &str) -> String {
    match kind {
        DbKind::Mysql => format!("`{}`", id.replace('`', "``")),
        _ => format!("\"{}\"", id.replace('"', "\"\"")),
    }
}

pub fn qualified(kind: DbKind, schema: &str, table: &str) -> String {
    match kind {
        DbKind::Sqlite => quote_ident(kind, table),
        _ => format!("{}.{}", quote_ident(kind, schema), quote_ident(kind, table)),
    }
}

/// Postgres column types the binary protocol decodes natively (see `decode::pg_val`),
/// as `format_type` names them once `strip_typmod` has removed lengths.
fn pg_native(data_type: &str) -> bool {
    // Arrays are left out: `integer[]` may hold a 2-D array, which sqlx cannot decode.
    // Money is left out so it reads `$1,234.50` the way the editor shows it.
    matches!(
        strip_typmod(data_type).as_str(),
        "boolean"
            | "smallint"
            | "integer"
            | "bigint"
            | "oid"
            | "real"
            | "double precision"
            | "numeric"
            | "uuid"
            | "json"
            | "jsonb"
            | "date"
            | "time without time zone"
            | "time with time zone"
            | "timestamp without time zone"
            | "timestamp with time zone"
            | "interval"
            | "bytea"
            | "inet"
            | "cidr"
            | "text"
            | "character varying"
            | "bpchar"
            | "name"
            | "\"char\""
            | "xml"
    )
}

/// Placeholder for bind number `n` (1-based), with a cast to `ty` on Postgres.
/// Binds always travel as text. For types sqlx cannot describe (multiranges,
/// some extension types) the bind is declared text and cast server-side, so
/// preparing the statement never asks sqlx to resolve the type.
pub fn param(kind: DbKind, n: usize, ty: Option<&str>) -> String {
    match kind {
        DbKind::Postgres => match ty {
            Some(t) if pg_native(t) => format!("CAST(${n} AS {})", strip_typmod(t)),
            Some(t) => format!("CAST(CAST(${n} AS text) AS {})", strip_typmod(t)),
            None => format!("${n}"),
        },
        DbKind::Mysql => "?".to_string(),
        DbKind::Sqlite => format!("?{n}"),
    }
}

/// Removes the length from a cast target so an explicit CAST cannot silently
/// truncate (`character varying(5)` becomes `character varying`); the column
/// assignment then enforces the limit. Bare `character` and `bit` mean length 1,
/// so `character(n)` becomes `bpchar` and `bit(n)` keeps its length.
pub fn strip_typmod(t: &str) -> String {
    let array = if t.ends_with("[]") { "[]" } else { "" };
    let lower = t.to_ascii_lowercase();
    if lower.starts_with("character(")
        || lower.starts_with("char(")
        || lower == "character"
        || lower.starts_with("character[")
    {
        return format!("bpchar{array}");
    }
    if lower.starts_with("bit(") {
        return t.to_string();
    }
    let mut out = String::with_capacity(t.len());
    let mut depth = 0;
    for c in t.chars() {
        match c {
            '(' => depth += 1,
            ')' if depth > 0 => depth -= 1,
            _ if depth == 0 => out.push(c),
            _ => {}
        }
    }
    out.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[derive(Debug, Clone, Deserialize)]
pub struct Filter {
    pub column: String,
    pub op: String,
    pub value: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OrderBy {
    pub column: String,
    pub desc: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BrowseOptions {
    pub limit: usize,
    pub offset: usize,
    pub filters: Vec<Filter>,
    pub order_by: Option<OrderBy>,
    /// A caller key (one per table tab). A new filtered page with the same key
    /// cancels the previous one still running, so search-as-you-type on a huge
    /// table runs one scan at a time instead of one per keystroke.
    #[serde(default)]
    pub supersede: Option<String>,
}

/// Builds a WHERE clause with binds. Columns are checked against the table so
/// only known identifiers reach the SQL.
/// Filter column meaning "any column" (⌘F quick search).
pub const ANY_COLUMN: &str = "*";

/// `%value%` with LIKE wildcards in the value matched literally.
fn like_pattern(value: &str) -> String {
    let escaped = value
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_");
    format!("%{escaped}%")
}

/// Case-insensitive "column text contains bind n", for any column type.
fn contains_expr(kind: DbKind, q: &str, n: usize) -> String {
    let p = param(kind, n, None);
    match kind {
        DbKind::Postgres => format!("CAST({q} AS TEXT) ILIKE {p} ESCAPE '\\'"),
        DbKind::Mysql => format!("LOWER(CAST({q} AS CHAR)) LIKE LOWER({p}) ESCAPE '\\\\'"),
        DbKind::Sqlite => format!("LOWER(CAST({q} AS TEXT)) LIKE LOWER({p}) ESCAPE '\\'"),
    }
}

pub fn where_clause(
    kind: DbKind,
    st: &TableStructure,
    filters: &[Filter],
    binds: &mut Vec<Option<String>>,
) -> anyhow::Result<String> {
    let mut parts = vec![];
    for f in filters {
        if f.column == ANY_COLUMN {
            if f.op != "contains" {
                anyhow::bail!("search across all columns only supports contains");
            }
            if f.value.is_empty() {
                continue;
            }
            binds.push(Some(like_pattern(&f.value)));
            let n = binds.len();
            let ors: Vec<String> = st
                .columns
                .iter()
                .map(|c| contains_expr(kind, &quote_ident(kind, &c.name), n))
                .collect();
            parts.push(format!("({})", ors.join(" OR ")));
            continue;
        }
        let col = st
            .column(&f.column)
            .ok_or_else(|| anyhow::anyhow!("unknown column {}", f.column))?;
        let q = quote_ident(kind, &col.name);
        let ty = Some(col.data_type.as_str());
        let part = match f.op.as_str() {
            "is null" => format!("{q} IS NULL"),
            "is not null" => format!("{q} IS NOT NULL"),
            "contains" => {
                binds.push(Some(like_pattern(&f.value)));
                contains_expr(kind, &q, binds.len())
            }
            op @ ("=" | "!=" | "<" | ">" | "<=" | ">=") => {
                binds.push(Some(f.value.clone()));
                let op = if op == "!=" { "<>" } else { op };
                format!("{q} {op} {}", param(kind, binds.len(), ty))
            }
            other => anyhow::bail!("unsupported filter operator {other}"),
        };
        parts.push(part);
    }
    Ok(if parts.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", parts.join(" AND "))
    })
}

/// `*`, unless a Postgres column has a type the binary protocol cannot decode
/// (ranges, geometry, composites, domains, extension types). Those come back as
/// the server's own text, exactly as the editor shows them; without the cast,
/// a multirange makes the whole page fail to load.
fn select_list(kind: DbKind, st: &TableStructure) -> String {
    if kind != DbKind::Postgres || st.columns.iter().all(|c| pg_native(&c.data_type)) {
        return "*".into();
    }
    st.columns
        .iter()
        .map(|c| {
            let q = quote_ident(kind, &c.name);
            if pg_native(&c.data_type) {
                q
            } else {
                format!("CAST({q} AS text) AS {q}")
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// A sort key that names the table's column, not a same-named text cast in the select list.
fn sort_key(kind: DbKind, schema: &str, table: &str, col: &ColumnInfo) -> String {
    let q = quote_ident(kind, &col.name);
    if kind == DbKind::Postgres && !pg_native(&col.data_type) {
        format!("{}.{q}", qualified(kind, schema, table))
    } else {
        q
    }
}

pub fn browse_sql(
    kind: DbKind,
    schema: &str,
    table: &str,
    st: &TableStructure,
    opts: &BrowseOptions,
) -> anyhow::Result<(String, Vec<Option<String>>)> {
    let mut binds = vec![];
    let wh = where_clause(kind, st, &opts.filters, &mut binds)?;
    let order = match &opts.order_by {
        Some(o) => {
            let col = st
                .column(&o.column)
                .ok_or_else(|| anyhow::anyhow!("unknown column {}", o.column))?;
            let mut keys = vec![format!(
                "{} {}",
                sort_key(kind, schema, table, col),
                if o.desc { "DESC" } else { "ASC" }
            )];
            // Rows with equal sort values need a stable order, or OFFSET paging skips and repeats them.
            keys.extend(
                st.pk()
                    .iter()
                    .filter(|c| c.name != col.name)
                    .map(|c| sort_key(kind, schema, table, c)),
            );
            format!(" ORDER BY {}", keys.join(", "))
        }
        None => {
            let pk = st.pk();
            if pk.is_empty() {
                String::new()
            } else {
                format!(
                    " ORDER BY {}",
                    pk.iter()
                        .map(|c| sort_key(kind, schema, table, c))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
        }
    };
    let limit = opts.limit.clamp(1, 50_000);
    Ok((
        format!(
            "SELECT {} FROM {}{wh}{order} LIMIT {limit} OFFSET {}",
            select_list(kind, st),
            qualified(kind, schema, table),
            opts.offset
        ),
        binds,
    ))
}

pub fn count_sql(
    kind: DbKind,
    schema: &str,
    table: &str,
    st: &TableStructure,
    filters: &[Filter],
) -> anyhow::Result<(String, Vec<Option<String>>)> {
    let mut binds = vec![];
    let wh = where_clause(kind, st, filters, &mut binds)?;
    Ok((
        format!(
            "SELECT COUNT(*) FROM {}{wh}",
            qualified(kind, schema, table)
        ),
        binds,
    ))
}

pub type SharedPool = Arc<Pool>;

/// The 1-based character offset Postgres reports for an error in the submitted SQL.
/// The server's own wording for a Postgres error, with DETAIL and HINT lines like psql.
/// sqlx's Display adds "at line N", which is a line in Postgres's C source.
pub fn error_message(err: &anyhow::Error) -> Option<String> {
    let sqlx::Error::Database(db) = err.downcast_ref::<sqlx::Error>()? else {
        return None;
    };
    let pg = db.try_downcast_ref::<sqlx::postgres::PgDatabaseError>()?;
    let mut out = pg.message().to_string();
    if let Some(d) = pg.detail() {
        out.push_str(&format!("\nDETAIL:  {d}"));
    }
    if let Some(h) = pg.hint() {
        out.push_str(&format!("\nHINT:  {h}"));
    }
    Some(out)
}

/// The connection itself is gone (server restart, KILL, network), as opposed to
/// a statement failing on a healthy connection.
pub fn connection_lost(err: &anyhow::Error) -> bool {
    match err.downcast_ref::<sqlx::Error>() {
        Some(
            sqlx::Error::Io(_)
            | sqlx::Error::Protocol(_)
            | sqlx::Error::Tls(_)
            | sqlx::Error::PoolClosed
            | sqlx::Error::WorkerCrashed,
        ) => true,
        // Postgres class 08 (connection exception) and 57P01-03 (shutdown, terminated).
        Some(sqlx::Error::Database(db)) => db
            .code()
            .is_some_and(|c| c.starts_with("08") || c.starts_with("57P0")),
        _ => false,
    }
}

pub fn error_position(err: &anyhow::Error) -> Option<usize> {
    let sqlx::Error::Database(db) = err.downcast_ref::<sqlx::Error>()? else {
        return None;
    };
    match db
        .try_downcast_ref::<sqlx::postgres::PgDatabaseError>()?
        .position()?
    {
        sqlx::postgres::PgErrorPosition::Original(p) => Some(p),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn st() -> TableStructure {
        TableStructure {
            columns: vec![
                ColumnInfo {
                    name: "id".into(),
                    data_type: "integer".into(),
                    nullable: false,
                    default: None,
                    is_pk: true,
                    is_fk: false,
                },
                ColumnInfo {
                    name: "na\"me".into(),
                    data_type: "text".into(),
                    nullable: true,
                    default: None,
                    is_pk: false,
                    is_fk: false,
                },
            ],
            indexes: vec![],
        }
    }

    #[test]
    fn browse_sql_quotes_and_casts() {
        let opts = BrowseOptions {
            supersede: None,
            limit: 100,
            offset: 200,
            filters: vec![Filter {
                column: "id".into(),
                op: ">".into(),
                value: "5".into(),
            }],
            order_by: None,
        };
        let (sql, binds) = browse_sql(DbKind::Postgres, "public", "t", &st(), &opts).unwrap();
        assert_eq!(
            sql,
            "SELECT * FROM \"public\".\"t\" WHERE \"id\" > CAST($1 AS integer) ORDER BY \"id\" LIMIT 100 OFFSET 200"
        );
        assert_eq!(binds, vec![Some("5".to_string())]);
        let (sql, _) = browse_sql(
            DbKind::Mysql,
            "shop",
            "t",
            &st(),
            &BrowseOptions {
                filters: vec![],
                ..opts.clone()
            },
        )
        .unwrap();
        assert_eq!(
            sql,
            "SELECT * FROM `shop`.`t` ORDER BY `id` LIMIT 100 OFFSET 200"
        );
    }

    #[test]
    fn user_sort_breaks_ties_by_primary_key() {
        let opts = BrowseOptions {
            supersede: None,
            limit: 10,
            offset: 0,
            filters: vec![],
            order_by: Some(OrderBy {
                column: "na\"me".into(),
                desc: true,
            }),
        };
        let (sql, _) = browse_sql(DbKind::Postgres, "public", "t", &st(), &opts).unwrap();
        assert_eq!(
            sql,
            "SELECT * FROM \"public\".\"t\" ORDER BY \"na\"\"me\" DESC, \"id\" LIMIT 10 OFFSET 0"
        );
        let by_pk = BrowseOptions {
            order_by: Some(OrderBy {
                column: "id".into(),
                desc: false,
            }),
            ..opts
        };
        let (sql, _) = browse_sql(DbKind::Mysql, "shop", "t", &st(), &by_pk).unwrap();
        assert_eq!(
            sql,
            "SELECT * FROM `shop`.`t` ORDER BY `id` ASC LIMIT 10 OFFSET 0"
        );
    }

    #[test]
    fn any_column_contains_is_case_insensitive_and_literal() {
        let opts = BrowseOptions {
            supersede: None,
            limit: 10,
            offset: 0,
            filters: vec![Filter {
                column: ANY_COLUMN.into(),
                op: "contains".into(),
                value: "50%_off".into(),
            }],
            order_by: None,
        };
        let (sql, binds) = browse_sql(DbKind::Postgres, "public", "t", &st(), &opts).unwrap();
        assert!(sql.contains("WHERE (CAST(\"id\" AS TEXT) ILIKE $1 ESCAPE '\\' OR CAST(\"na\"\"me\" AS TEXT) ILIKE $1 ESCAPE '\\')"), "{sql}");
        assert_eq!(binds, vec![Some("%50\\%\\_off%".to_string())]);
        let (sql, _) = browse_sql(DbKind::Mysql, "shop", "t", &st(), &opts).unwrap();
        assert!(
            sql.contains("LOWER(CAST(`id` AS CHAR)) LIKE LOWER(?)"),
            "{sql}"
        );
        let empty = BrowseOptions {
            filters: vec![Filter {
                column: ANY_COLUMN.into(),
                op: "contains".into(),
                value: String::new(),
            }],
            ..opts
        };
        assert!(
            !browse_sql(DbKind::Sqlite, "main", "t", &st(), &empty)
                .unwrap()
                .0
                .contains("WHERE")
        );
    }

    #[test]
    fn unknown_filter_column_is_rejected() {
        let opts = BrowseOptions {
            supersede: None,
            limit: 10,
            offset: 0,
            filters: vec![Filter {
                column: "id; DROP TABLE t".into(),
                op: "=".into(),
                value: "1".into(),
            }],
            order_by: None,
        };
        assert!(browse_sql(DbKind::Postgres, "public", "t", &st(), &opts).is_err());
    }

    #[test]
    fn typmods_are_stripped_from_casts() {
        assert_eq!(strip_typmod("character varying(5)"), "character varying");
        assert_eq!(
            strip_typmod("timestamp(3) without time zone"),
            "timestamp without time zone"
        );
        assert_eq!(strip_typmod("numeric(12,2)[]"), "numeric[]");
        assert_eq!(strip_typmod("character(10)"), "bpchar");
        assert_eq!(strip_typmod("character(3)[]"), "bpchar[]");
        assert_eq!(strip_typmod("bit(8)"), "bit(8)");
    }

    #[test]
    fn composite_key_orders_by_index_and_exotic_types_are_cast() {
        let col = |name: &str, ty: &str, pk: bool| ColumnInfo {
            name: name.into(),
            data_type: ty.into(),
            nullable: false,
            default: None,
            is_pk: pk,
            is_fk: false,
        };
        let st = TableStructure {
            columns: vec![
                col("a", "integer", true),
                col("b", "text", true),
                col("r", "int4multirange", false),
            ],
            indexes: vec![IndexInfo {
                name: "t_pkey".into(),
                columns: vec!["b".into(), "a".into()],
                unique: true,
            }],
        };
        let opts = BrowseOptions {
            supersede: None,
            limit: 10,
            offset: 0,
            filters: vec![],
            order_by: None,
        };
        let (sql, _) = browse_sql(DbKind::Postgres, "public", "t", &st, &opts).unwrap();
        assert_eq!(
            sql,
            "SELECT \"a\", \"b\", CAST(\"r\" AS text) AS \"r\" FROM \"public\".\"t\" ORDER BY \"b\", \"a\" LIMIT 10 OFFSET 0"
        );
        let by_r = BrowseOptions {
            order_by: Some(OrderBy {
                column: "r".into(),
                desc: false,
            }),
            filters: vec![Filter {
                column: "r".into(),
                op: "=".into(),
                value: "{[1,2)}".into(),
            }],
            ..opts.clone()
        };
        let (sql, _) = browse_sql(DbKind::Postgres, "public", "t", &st, &by_r).unwrap();
        assert!(sql.contains("WHERE \"r\" = CAST(CAST($1 AS text) AS int4multirange) ORDER BY \"public\".\"t\".\"r\" ASC, \"b\", \"a\""), "{sql}");
        assert_eq!(
            browse_sql(DbKind::Mysql, "s", "t", &st, &opts).unwrap().0,
            "SELECT * FROM `s`.`t` ORDER BY `b`, `a` LIMIT 10 OFFSET 0"
        );
    }

    #[test]
    fn identifiers_escape_quotes() {
        assert_eq!(quote_ident(DbKind::Postgres, "na\"me"), "\"na\"\"me\"");
        assert_eq!(quote_ident(DbKind::Mysql, "a`b"), "`a``b`");
    }
}
