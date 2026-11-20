//! Integration tests against real databases. They run when TV_ITEST=1, against
//! the containers from tests/seed (Postgres on 55432, MySQL on 53306).

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::RecordBatch;
use serde_json::json;

use crate::config::{ConnectionConfig, DbKind, Store};
use crate::db::{self, BrowseOptions, Filter, OrderBy};
use crate::edits::{self, CellEdit, Changes, arrow_text};
use crate::state::AppState;

fn enabled() -> bool {
    std::env::var("TV_ITEST").as_deref() == Ok("1")
}

fn cfg(id: &str, kind: DbKind) -> ConnectionConfig {
    let (host, port, user, database) = match kind {
        DbKind::Postgres => ("127.0.0.1", 55432, "postgres", "shop"),
        DbKind::Mysql => ("127.0.0.1", 53306, "root", "shop"),
        DbKind::Sqlite => ("", 0, "", ""),
    };
    ConnectionConfig {
        id: id.into(),
        name: id.into(),
        kind,
        host: host.into(),
        port,
        user: user.into(),
        database: database.into(),
        file_path: String::new(),
        ssl_mode: "disable".into(),
        env: "local".into(),
        read_only: false,
        allow_claude: true,
    }
}

fn state() -> Arc<AppState> {
    let dir = std::env::temp_dir().join(format!("tv-itest-{}", uuid::Uuid::new_v4().simple()));
    let st = Arc::new(AppState::new(Store::new(dir.clone()).unwrap()).unwrap());
    let sqlite = dir.join("lite.db");
    std::fs::File::create(&sqlite).unwrap();
    let mut lite = cfg("lite", DbKind::Sqlite);
    lite.file_path = sqlite.to_string_lossy().into_owned();
    *st.connections.lock().unwrap() =
        vec![cfg("pg", DbKind::Postgres), cfg("my", DbKind::Mysql), lite];
    for id in ["pg", "my"] {
        st.password_override
            .lock()
            .unwrap()
            .insert(id.into(), "tvpass".into());
    }
    st
}

fn batch(st: &AppState, id: &str) -> RecordBatch {
    st.results.get(id).unwrap().batch().unwrap()
}

fn row_strings(b: &RecordBatch, kind: DbKind) -> Vec<Vec<Option<String>>> {
    (0..b.num_rows())
        .map(|r| {
            (0..b.num_columns())
                .map(|c| arrow_text(b, c, r, kind))
                .collect()
        })
        .collect()
}

fn field_type(b: &RecordBatch, name: &str) -> String {
    b.schema()
        .field_with_name(name)
        .unwrap()
        .data_type()
        .to_string()
}

#[tokio::test]
async fn postgres_types_match_across_protocols() {
    if !enabled() {
        return;
    }
    let st = state();
    let sql = "SELECT * FROM customers ORDER BY id LIMIT 50";
    let text = st.run_editor_query("pg", "t1", sql, "q1").await.unwrap();
    let opts = BrowseOptions {
        supersede: None,
        limit: 50,
        offset: 0,
        filters: vec![],
        order_by: None,
    };
    let bin = st.browse("pg", "public", "customers", &opts).await.unwrap();
    let (tb, bb) = (batch(&st, &text.id), batch(&st, &bin.id));
    assert_eq!(
        row_strings(&tb, DbKind::Postgres),
        row_strings(&bb, DbKind::Postgres),
        "text and binary decode differ"
    );
    assert_eq!(field_type(&bb, "id"), "Int64");
    assert_eq!(field_type(&bb, "mrr"), "Utf8");
    assert!(
        field_type(&bb, "signup_at").starts_with("Timestamp(µs"),
        "{}",
        field_type(&bb, "signup_at")
    );
    assert_eq!(field_type(&bb, "birthday"), "Date32");
    let rows = row_strings(&bb, DbKind::Postgres);
    let col = |n: &str| bb.schema().index_of(n).unwrap();
    assert_eq!(rows[0][col("email")].as_deref(), Some("user1@example.com"));
    assert_eq!(rows[0][col("plan")].as_deref(), Some("pro"));
    assert_eq!(rows[0][col("mrr")].as_deref(), Some("1.25"));
    assert_eq!(
        rows[0][col("signup_at")].as_deref(),
        Some("2024-01-01 01:00:00.000000+00")
    );
    assert_eq!(rows[4][col("birthday")], None);
    assert_eq!(rows[0][col("tags")].as_deref(), Some("{a1,b}"));
    assert_eq!(rows[0][col("ip")].as_deref(), Some("10.0.1.1"));
    assert!(
        rows[0][col("prefs")]
            .as_deref()
            .unwrap()
            .contains("\"theme\"")
    );
    assert_eq!(rows[0][col("ext_id")].as_deref().map(str::len), Some(36));
}

#[tokio::test]
async fn mysql_types_match_across_protocols() {
    if !enabled() {
        return;
    }
    let st = state();
    let text = st
        .run_editor_query(
            "my",
            "t1",
            "SELECT * FROM customers ORDER BY id LIMIT 30",
            "q1",
        )
        .await
        .unwrap();
    let opts = BrowseOptions {
        supersede: None,
        limit: 30,
        offset: 0,
        filters: vec![],
        order_by: None,
    };
    let bin = st.browse("my", "shop", "customers", &opts).await.unwrap();
    let (tb, bb) = (batch(&st, &text.id), batch(&st, &bin.id));
    assert_eq!(
        row_strings(&tb, DbKind::Mysql),
        row_strings(&bb, DbKind::Mysql),
        "text and binary decode differ"
    );
    let rows = row_strings(&bb, DbKind::Mysql);
    let col = |n: &str| bb.schema().index_of(n).unwrap();
    assert_eq!(rows[0][col("big")].as_deref(), Some("18446744073709551614"));
    assert_eq!(rows[0][col("flags")].as_deref(), Some("b'10100101'"));
    assert_eq!(rows[0][col("avatar")].as_deref(), Some("\\xdeadbeef"));
    assert_eq!(rows[0][col("mrr")].as_deref(), Some("1.25"));
    assert_eq!(rows[0][col("active")].as_deref(), Some("1"));
    assert_eq!(
        rows[0][col("signup_at")].as_deref(),
        Some("2024-01-01 01:00:00.000000")
    );
    assert_eq!(rows[0][col("plan")].as_deref(), Some("pro"));
}

#[tokio::test]
async fn sqlite_per_value_typing_widens() {
    if !enabled() {
        return;
    }
    let st = state();
    st.run_editor_query("lite", "t", "CREATE TABLE m (id INTEGER PRIMARY KEY, v); INSERT INTO m (v) VALUES (1), (2.5), ('x'), (NULL), (x'00ff')", "q").await.unwrap();
    let r = st
        .run_editor_query("lite", "t", "SELECT v FROM m ORDER BY id", "q")
        .await
        .unwrap();
    let b = batch(&st, &r.id);
    assert_eq!(field_type(&b, "v"), "Utf8");
    assert_eq!(
        row_strings(&b, DbKind::Sqlite)
            .into_iter()
            .map(|r| r[0].clone())
            .collect::<Vec<_>>(),
        vec![
            Some("1".into()),
            Some("2.5".into()),
            Some("x".into()),
            None,
            Some("\\x00ff".into())
        ]
    );
}

#[tokio::test]
async fn introspection_browse_and_count() {
    if !enabled() {
        return;
    }
    let st = state();
    for (id, schema) in [("pg", "public"), ("my", "shop")] {
        let pool = st.pool(id).await.unwrap();
        let tree = pool.schema_tree().await.unwrap();
        let names: Vec<_> = tree
            .schemas
            .iter()
            .flat_map(|g| g.tables.iter().map(|t| (t.name.clone(), t.kind.clone())))
            .collect();
        assert!(
            names.contains(&("customers".into(), "table".into())),
            "{id}: {names:?}"
        );
        assert!(names.iter().any(|(_, k)| k == "view"), "{id} has a view");
        let s = pool.describe(schema, "orders").await.unwrap();
        assert!(
            s.columns.iter().any(|c| c.name == "id" && c.is_pk),
            "{id} pk"
        );
        if id == "pg" {
            assert!(
                s.columns.iter().any(|c| c.name == "customer_id" && c.is_fk),
                "pg fk"
            );
            assert!(
                !s.columns.iter().any(|c| c.name == "total" && c.is_fk),
                "pg non-fk"
            );
        }
        assert!(
            s.indexes
                .iter()
                .any(|i| i.columns == vec!["customer_id".to_string()]),
            "{id} index: {:?}",
            s.indexes
        );
        let opts = BrowseOptions {
            supersede: None,
            limit: 10,
            offset: 5,
            filters: vec![
                Filter {
                    column: "status".into(),
                    op: "=".into(),
                    value: "paid".into(),
                },
                Filter {
                    column: "total".into(),
                    op: ">".into(),
                    value: "100".into(),
                },
            ],
            order_by: Some(OrderBy {
                column: "total".into(),
                desc: true,
            }),
        };
        let r = st.browse(id, schema, "orders", &opts).await.unwrap();
        assert_eq!(r.row_count, 10);
        let n = st.count(id, schema, "orders", &opts.filters).await.unwrap();
        assert!(n > 1000, "{id} count {n}");
        // The planner's estimate comes with every page of a table, so huge tables need no COUNT(*).
        st.run_editor_query(
            id,
            "an",
            if id == "pg" {
                "ANALYZE orders"
            } else {
                "ANALYZE TABLE orders"
            },
            "an",
        )
        .await
        .unwrap();
        let page = st
            .browse(
                id,
                schema,
                "orders",
                &BrowseOptions {
                    supersede: None,
                    limit: 10,
                    offset: 0,
                    filters: vec![],
                    order_by: None,
                },
            )
            .await
            .unwrap();
        let exact = st.count(id, schema, "orders", &[]).await.unwrap();
        let est = page.estimated_rows.expect("estimate");
        assert!(
            (est - exact).abs() < exact / 2,
            "{id} estimate {est} vs {exact}"
        );
    }
}

async fn edit_roundtrip(st: &AppState, id: &str, schema: &str, setup: &str) {
    st.run_editor_query(id, "setup", setup, "s").await.unwrap();
    let opts = BrowseOptions {
        supersede: None,
        limit: 10,
        offset: 0,
        filters: vec![],
        order_by: None,
    };
    let page = st.browse(id, schema, "tv_edit", &opts).await.unwrap();
    let pool = st.pool(id).await.unwrap();
    let structure = pool.describe(schema, "tv_edit").await.unwrap();
    let b = batch(st, &page.id);
    let change = Changes {
        edits: vec![
            CellEdit {
                row: 0,
                column: "name".into(),
                value: Some("Edited".into()),
            },
            CellEdit {
                row: 1,
                column: "qty".into(),
                value: None,
            },
        ],
        ..Default::default()
    };
    let stmts = edits::plan(
        pool.kind(),
        schema,
        "tv_edit",
        &structure,
        Some(&b),
        &change,
    )
    .unwrap();
    assert_eq!(edits::execute(&pool, &stmts).await.unwrap(), 2, "{id}");
    let after = st
        .run_editor_query(
            id,
            "check",
            "SELECT name, qty FROM tv_edit ORDER BY id",
            "c",
        )
        .await
        .unwrap();
    let rows = row_strings(&batch(st, &after.id), pool.kind());
    assert_eq!(rows[0][0].as_deref(), Some("Edited"), "{id}");
    assert_eq!(rows[1][1], None, "{id}");

    // A stale edit: the row changed after the page loaded, so the commit must fail and change nothing.
    st.run_editor_query(
        id,
        "other",
        "UPDATE tv_edit SET name = 'Someone else' WHERE id = 3",
        "o",
    )
    .await
    .unwrap();
    let stale = Changes {
        edits: vec![
            CellEdit {
                row: 2,
                column: "name".into(),
                value: Some("Mine".into()),
            },
            CellEdit {
                row: 3,
                column: "name".into(),
                value: Some("Also mine".into()),
            },
        ],
        ..Default::default()
    };
    let stmts = edits::plan(pool.kind(), schema, "tv_edit", &structure, Some(&b), &stale).unwrap();
    let err = edits::execute(&pool, &stmts)
        .await
        .err()
        .unwrap()
        .to_string();
    assert!(err.contains("changed or deleted"), "{id}: {err}");
    let after = st
        .run_editor_query(id, "check", "SELECT name FROM tv_edit ORDER BY id", "c")
        .await
        .unwrap();
    let rows = row_strings(&batch(st, &after.id), pool.kind());
    assert_eq!(rows[2][0].as_deref(), Some("Someone else"), "{id}");
    assert_eq!(
        rows[3][0].as_deref(),
        Some("d"),
        "{id}: whole commit rolled back"
    );

    let ins = Changes {
        inserts: vec![
            [("name".to_string(), Some("new".to_string()))]
                .into_iter()
                .collect(),
        ],
        deletes: vec![0],
        ..Default::default()
    };
    let stmts = edits::plan(pool.kind(), schema, "tv_edit", &structure, Some(&b), &ins).unwrap();
    assert_eq!(edits::execute(&pool, &stmts).await.unwrap(), 2, "{id}");
    let after = st
        .run_editor_query(
            id,
            "check",
            "SELECT count(*) FROM tv_edit WHERE name = 'new'",
            "c",
        )
        .await
        .unwrap();
    assert_eq!(
        row_strings(&batch(st, &after.id), pool.kind())[0][0].as_deref(),
        Some("1")
    );
}

#[tokio::test]
async fn edits_commit_and_detect_conflicts() {
    if !enabled() {
        return;
    }
    let st = state();
    edit_roundtrip(&st, "pg", "public", "DROP TABLE IF EXISTS tv_edit; CREATE TABLE tv_edit (id serial PRIMARY KEY, name text, qty int, at timestamptz DEFAULT now()); INSERT INTO tv_edit (name, qty) VALUES ('a',1),('b',2),('c',3),('d',4)").await;
    edit_roundtrip(&st, "my", "shop", "DROP TABLE IF EXISTS tv_edit; CREATE TABLE tv_edit (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(50), qty INT, at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6)); INSERT INTO tv_edit (name, qty) VALUES ('a',1),('b',2),('c',3),('d',4)").await;
    edit_roundtrip(&st, "lite", "main", "DROP TABLE IF EXISTS tv_edit; CREATE TABLE tv_edit (id INTEGER PRIMARY KEY, name TEXT, qty INTEGER); INSERT INTO tv_edit (name, qty) VALUES ('a',1),('b',2),('c',3),('d',4)").await;
}

#[tokio::test]
async fn claude_queries_are_read_only() {
    if !enabled() {
        return;
    }
    let st = state();
    for id in ["pg", "my", "lite"] {
        for bad in [
            "DELETE FROM customers",
            "SELECT 1; DROP TABLE customers",
            "COMMIT; DROP TABLE customers",
            "CREATE TABLE x (a int)",
        ] {
            assert!(
                st.run_claude_query(id, bad, 10).await.is_err(),
                "{id} allowed {bad}"
            );
        }
    }
    let ok = st
        .run_claude_query(
            "pg",
            "SELECT plan, count(*) AS n FROM customers GROUP BY plan ORDER BY plan",
            10,
        )
        .await
        .unwrap();
    assert_eq!(ok.meta.row_count, 4);
    // The read-only transaction blocks writes even if a statement slipped past the parser.
    let pool = st.pool("pg").await.unwrap();
    let mut s = pool.session().await.unwrap();
    db::exec_raw(&mut s.conn, "BEGIN READ ONLY").await.unwrap();
    let w = db::run_prepared_in_tx(
        &mut s.conn,
        "WITH d AS (DELETE FROM orders WHERE id = 1 RETURNING id) SELECT * FROM d",
        &[],
        (10, 1 << 20),
        || {},
    )
    .await;
    assert!(w.err().unwrap().to_string().contains("read-only"));
    db::exec_raw(&mut s.conn, "ROLLBACK").await.unwrap();
    let mut off = cfg("pg2", DbKind::Postgres);
    off.allow_claude = false;
    st.connections.lock().unwrap().push(off);
    assert!(
        st.run_claude_query("pg2", "SELECT 1", 1)
            .await
            .err()
            .unwrap()
            .to_string()
            .contains("turned off")
    );
    // Running against the same connection afterwards still works, so no transaction leaked.
    assert!(
        st.run_claude_query("my", "SELECT COUNT(*) FROM orders", 1)
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn truncation_and_cancel() {
    if !enabled() {
        return;
    }
    let st = state();
    *st.editor_limits.lock().unwrap() = (500_000, db::UI_MAX_BYTES);
    let t = Instant::now();
    let r = st
        .run_editor_query(
            "pg",
            "big",
            "SELECT g, md5(g::text) FROM generate_series(1, 3000000) g",
            "q",
        )
        .await
        .unwrap();
    assert!(r.truncated);
    assert_eq!(r.row_count, 500_000);
    eprintln!("500k rows from postgres into arrow in {:?}", t.elapsed());
    let again = st
        .run_editor_query("pg", "big", "SELECT 1 AS one", "q2")
        .await
        .unwrap();
    assert_eq!(again.row_count, 1, "pinned session usable after truncation");

    let my = st
        .run_editor_query(
            "my",
            "big",
            "SELECT a.id, b.id FROM orders a CROSS JOIN orders b",
            "qm",
        )
        .await
        .unwrap();
    assert!(my.truncated && my.row_count == 500_000, "mysql truncation");
    let again = st
        .run_editor_query("my", "big", "SELECT 1 AS one", "q3")
        .await
        .unwrap();
    assert_eq!(again.row_count, 1, "mysql session usable after truncation");

    let st2 = st.clone();
    let slow = tokio::spawn(async move {
        st2.run_editor_query("pg", "slow", "SELECT pg_sleep(20)", "sleep")
            .await
    });
    tokio::time::sleep(Duration::from_millis(800)).await;
    let t = Instant::now();
    assert!(st.cancel("sleep").await.unwrap());
    let res = slow.await.unwrap();
    assert!(
        res.err().unwrap().to_string().contains("cancel"),
        "cancelled"
    );
    assert!(t.elapsed() < Duration::from_secs(3));
}

#[tokio::test]
async fn throughput_postgres_to_arrow() {
    if !enabled() {
        return;
    }
    let st = state();
    let t = Instant::now();
    let r = st
        .run_editor_query("pg", "tp", "SELECT * FROM orders", "q")
        .await
        .unwrap();
    let ms = t.elapsed().as_millis().max(1);
    eprintln!(
        "orders: {} rows, {} cols in {ms} ms ({} rows/s)",
        r.row_count,
        r.columns.len(),
        r.row_count as u128 * 1000 / ms
    );
    assert_eq!(r.row_count, 200_000);
}

async fn post(
    client: &reqwest::Client,
    url: &str,
    token: Option<&str>,
    origin: bool,
    session: Option<&str>,
    body: serde_json::Value,
) -> (u16, String, Option<String>) {
    let mut req = client
        .post(url)
        .header("content-type", "application/json")
        .header("accept", "application/json, text/event-stream");
    if let Some(t) = token {
        req = req.header("authorization", format!("Bearer {t}"));
    }
    if origin {
        req = req.header("origin", "http://evil.example");
    }
    if let Some(s) = session {
        req = req.header("mcp-session-id", s);
    }
    let resp = req.json(&body).send().await.unwrap();
    let sid = resp
        .headers()
        .get("mcp-session-id")
        .map(|v| v.to_str().unwrap().to_string());
    (resp.status().as_u16(), resp.text().await.unwrap(), sid)
}

#[tokio::test]
async fn mcp_auth_and_tools() {
    if !enabled() {
        return;
    }
    let st = state();
    let token = st.settings.lock().unwrap().mcp_token.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/mcp", listener.local_addr().unwrap());
    let app = crate::mcp::router(st.clone(), token.clone());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let c = reqwest::Client::new();
    let init = json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"t","version":"0"}}});
    assert_eq!(post(&c, &url, None, false, None, init.clone()).await.0, 401);
    assert_eq!(
        post(&c, &url, Some("wrong"), false, None, init.clone())
            .await
            .0,
        401
    );
    assert_eq!(
        post(&c, &url, Some(&token), true, None, init.clone())
            .await
            .0,
        403
    );
    let (code, _, sid) = post(&c, &url, Some(&token), false, None, init).await;
    assert_eq!(code, 200);
    let sid = sid.unwrap();
    post(
        &c,
        &url,
        Some(&token),
        false,
        Some(&sid),
        json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
    )
    .await;
    let call = |id: i64, name: &str, args: serde_json::Value| json!({"jsonrpc":"2.0","id":id,"method":"tools/call","params":{"name":name,"arguments":args}});
    let (_, body, _) = post(&c, &url, Some(&token), false, Some(&sid), call(2, "run_query", json!({"connection":"pg","sql":"SELECT plan, count(*) AS n FROM customers GROUP BY plan ORDER BY plan"}))).await;
    assert!(
        body.contains("row_count") && body.contains("enterprise"),
        "{body}"
    );
    assert!(
        st.emitted
            .lock()
            .unwrap()
            .iter()
            .any(|(e, _)| e == "claude-show-result")
    );
    let (_, body, _) = post(
        &c,
        &url,
        Some(&token),
        false,
        Some(&sid),
        call(
            3,
            "run_query",
            json!({"connection":"pg","sql":"DELETE FROM customers"}),
        ),
    )
    .await;
    assert!(
        body.contains("\"isError\":true") && body.contains("only SELECT"),
        "{body}"
    );
    let (_, body, _) = post(&c, &url, Some(&token), false, Some(&sid), call(4, "visualize", json!({"connection":"pg","sql":"SELECT status, sum(total) AS revenue FROM orders GROUP BY status","kind":"bar","x":"status","y":["revenue"]}))).await;
    assert!(
        body.contains("shown") && body.contains("\"isError\":false"),
        "{body}"
    );
    assert!(
        st.emitted
            .lock()
            .unwrap()
            .iter()
            .any(|(e, _)| e == "claude-show-chart")
    );
    let (_, body, _) = post(
        &c,
        &url,
        Some(&token),
        false,
        Some(&sid),
        call(
            5,
            "describe_table",
            json!({"connection":"my","table":"orders"}),
        ),
    )
    .await;
    assert!(body.contains("customer_id"), "{body}");
}

#[tokio::test]
async fn review_fixes_hold_on_real_databases() {
    if !enabled() {
        return;
    }
    let st = state();
    // An open transaction in a closed tab must not reach the next user of the pool.
    st.run_editor_query("pg", "t", "DROP TABLE IF EXISTS tv_leak; CREATE TABLE tv_leak (id int PRIMARY KEY, v varchar(5), code char(4), flags bit(8))", "a").await.unwrap();
    st.run_editor_query(
        "pg",
        "dirty",
        "BEGIN; INSERT INTO tv_leak VALUES (1, 'x')",
        "b",
    )
    .await
    .unwrap();
    st.close_tab_session("pg", "dirty");
    tokio::time::sleep(Duration::from_millis(300)).await;
    let pool = st.pool("pg").await.unwrap();
    for _ in 0..8 {
        let mut c = pool.conn().await.unwrap();
        db::exec_raw(&mut c, "COMMIT").await.unwrap();
    }
    let r = st
        .run_editor_query("pg", "check", "SELECT count(*) FROM tv_leak", "c")
        .await
        .unwrap();
    assert_eq!(
        row_strings(&batch(&st, &r.id), DbKind::Postgres)[0][0].as_deref(),
        Some("0"),
        "abandoned insert was committed"
    );

    // Over-long text errors instead of being cut by an explicit cast.
    let page = st
        .browse(
            "pg",
            "public",
            "tv_leak",
            &BrowseOptions {
                supersede: None,
                limit: 10,
                offset: 0,
                filters: vec![],
                order_by: None,
            },
        )
        .await
        .unwrap();
    let _ = page;
    let structure = pool.describe("public", "tv_leak").await.unwrap();
    let ins = Changes {
        inserts: vec![
            [
                ("id".to_string(), Some("2".to_string())),
                ("v".to_string(), Some("toolongvalue".to_string())),
            ]
            .into_iter()
            .collect(),
        ],
        ..Default::default()
    };
    let stmts = edits::plan(
        DbKind::Postgres,
        "public",
        "tv_leak",
        &structure,
        None,
        &ins,
    )
    .unwrap();
    let err = edits::execute(&pool, &stmts)
        .await
        .err()
        .expect("must fail")
        .to_string();
    assert!(err.contains("too long"), "{err}");

    // A read-only connection is enforced by the server, including writable CTEs.
    // Fixed-length types round-trip through the casts unchanged.
    let ok = Changes {
        inserts: vec![
            [
                ("id".to_string(), Some("3".to_string())),
                ("code".to_string(), Some("abcd".to_string())),
                ("flags".to_string(), Some("10100101".to_string())),
            ]
            .into_iter()
            .collect(),
        ],
        ..Default::default()
    };
    edits::execute(
        &pool,
        &edits::plan(DbKind::Postgres, "public", "tv_leak", &structure, None, &ok).unwrap(),
    )
    .await
    .unwrap();
    let r = st
        .run_editor_query(
            "pg",
            "check",
            "SELECT code, flags::text FROM tv_leak WHERE id = 3",
            "f",
        )
        .await
        .unwrap();
    let row = &row_strings(&batch(&st, &r.id), DbKind::Postgres)[0];
    assert_eq!(
        (row[0].as_deref(), row[1].as_deref()),
        (Some("abcd"), Some("10100101"))
    );

    let mut ro = cfg("pgro", DbKind::Postgres);
    ro.read_only = true;
    st.connections.lock().unwrap().push(ro);
    st.password_override
        .lock()
        .unwrap()
        .insert("pgro".into(), "tvpass".into());
    let w = st
        .run_editor_query(
            "pgro",
            "t",
            "WITH d AS (DELETE FROM tv_leak RETURNING *) SELECT * FROM d",
            "d",
        )
        .await;
    assert!(w.err().unwrap().to_string().contains("read-only"));
    assert!(
        st.run_editor_query("pgro", "t", "SELECT 1", "e")
            .await
            .is_ok()
    );

    // Claude's FROM-clause function calls and data-modifying CTEs are refused.
    for q in [
        "SELECT * FROM pg_terminate_backend(1)",
        "WITH d AS (DELETE FROM tv_leak RETURNING *) SELECT * FROM d",
    ] {
        assert!(st.run_claude_query("pg", q, 5).await.is_err(), "{q}");
    }
}

async fn text_and_binary_rows(
    st: &AppState,
    id: &str,
    schema: &str,
    table: &str,
) -> (
    Vec<Vec<Option<String>>>,
    Vec<Vec<Option<String>>>,
    RecordBatch,
) {
    let kind = st.pool(id).await.unwrap().kind();
    let text = st
        .run_editor_query(
            id,
            "nasty",
            &format!("SELECT * FROM {table} ORDER BY id"),
            "t",
        )
        .await
        .unwrap();
    let bin = st
        .browse(
            id,
            schema,
            table,
            &BrowseOptions {
                supersede: None,
                limit: 100,
                offset: 0,
                filters: vec![],
                order_by: None,
            },
        )
        .await
        .unwrap();
    let bb = batch(st, &bin.id);
    (
        row_strings(&batch(st, &text.id), kind),
        row_strings(&bb, kind),
        bb,
    )
}

/// Types the binary protocol cannot decode used to fail the whole page
/// (multirange) or show hex (2-D arrays), and 'infinity' dates panicked inside
/// chrono, which aborts the release build. Browse must now match the editor.
#[tokio::test]
async fn nasty_postgres_types_browse_like_the_editor() {
    if !enabled() {
        return;
    }
    let st = state();
    st.run_editor_query("pg", "setup", "CREATE EXTENSION IF NOT EXISTS hstore; DROP TABLE IF EXISTS tv_nasty; \
        DROP TYPE IF EXISTS tv_comp; DROP DOMAIN IF EXISTS tv_dom; DROP TYPE IF EXISTS tv_mood; \
        CREATE TYPE tv_comp AS (x int, y text); CREATE DOMAIN tv_dom AS int CHECK (VALUE > 0); CREATE TYPE tv_mood AS ENUM ('sad','happy'); \
        CREATE TABLE tv_nasty (id int PRIMARY KEY, arr2 int[][], tarr text[], range_i int4range, mr int4multirange, hs hstore, \
          dom tv_dom, comp tv_comp, mood tv_mood, mood_arr tv_mood[], money_v money, tsv tsvector, pt point, bx box, \
          bv bit varying, tt timetz, t24 time, d date, ts timestamp, tstz timestamptz, nnan numeric, nhuge numeric, \
          f8 double precision, j jsonb, mac macaddr, numarr numeric[], datearr date[], by bytea, x xml); \
        INSERT INTO tv_nasty VALUES (1, '{{1,2},{3,4}}', '{\"a b\",\"c,d\",NULL}', '[1,10)', '{[1,3),[5,7)}', 'a=>1,\"b c\"=>NULL', \
          5, ROW(1,'he said \"hi\"'), 'happy', '{sad,happy}', 12345.67, to_tsvector('simple','quick foxes'), '(1,2)', '((0,0),(1,1))', \
          B'10101', '12:34:56+05:30', '24:00:00', 'infinity', '-infinity', 'infinity', 'NaN', (repeat('9', 1000) || '.5')::numeric, \
          'NaN', '{\"k\": \"ünï 😀\"}', '08:00:2b:01:02:03', '{1.5,NaN}', '{2020-01-01,infinity}', '\\x00ff', '<a>x</a>'), \
          (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '0044-03-15 BC', \
          '294276-12-31 23:59:59', '2024-02-29 12:00:00.5+00', '0.000500', '-1', '-0', NULL, NULL, NULL, NULL, NULL, NULL)", "s").await.unwrap();
    let (text, bin, bb) = text_and_binary_rows(&st, "pg", "public", "tv_nasty").await;
    let names: Vec<String> = bb
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    for (r, (t, b)) in text.iter().zip(&bin).enumerate() {
        for (c, n) in names.iter().enumerate() {
            assert_eq!(t[c], b[c], "row {r} column {n}: editor vs browse");
        }
    }
    let col = |n: &str| names.iter().position(|x| x == n).unwrap();
    assert_eq!(bin[0][col("arr2")].as_deref(), Some("{{1,2},{3,4}}"));
    assert_eq!(bin[0][col("mr")].as_deref(), Some("{[1,3),[5,7)}"));
    assert_eq!(bin[0][col("d")].as_deref(), Some("infinity"));
    assert_eq!(bin[0][col("t24")].as_deref(), Some("24:00:00"));
    assert_eq!(bin[1][col("d")].as_deref(), Some("0044-03-15 BC"));
    assert_eq!(field_type(&bb, "dom"), "Utf8");

    // Edits work on those types too: filters, sorting and UPDATE all bind through text.
    let opts = BrowseOptions {
        supersede: None,
        limit: 10,
        offset: 0,
        filters: vec![Filter {
            column: "mr".into(),
            op: "=".into(),
            value: "{[1,3),[5,7)}".into(),
        }],
        order_by: Some(OrderBy {
            column: "dom".into(),
            desc: true,
        }),
    };
    assert_eq!(
        st.browse("pg", "public", "tv_nasty", &opts)
            .await
            .unwrap()
            .row_count,
        1
    );
    let pool = st.pool("pg").await.unwrap();
    let structure = pool.describe("public", "tv_nasty").await.unwrap();
    let ch = Changes {
        edits: vec![
            CellEdit {
                row: 0,
                column: "mr".into(),
                value: Some("{[2,4)}".into()),
            },
            CellEdit {
                row: 0,
                column: "dom".into(),
                value: Some("7".into()),
            },
        ],
        ..Default::default()
    };
    let stmts = edits::plan(
        DbKind::Postgres,
        "public",
        "tv_nasty",
        &structure,
        Some(&bb),
        &ch,
    )
    .unwrap();
    assert_eq!(edits::execute(&pool, &stmts).await.unwrap(), 1);
}

#[tokio::test]
async fn nasty_mysql_types_browse_like_the_editor() {
    if !enabled() {
        return;
    }
    let st = state();
    st.run_editor_query("my", "setup", "SET SESSION sql_mode = ''; DROP TABLE IF EXISTS tv_nasty; \
        CREATE TABLE tv_nasty (id INT PRIMARY KEY, zd DATE, zdt DATETIME, zts TIMESTAMP NULL, y YEAR, s SET('a','b','c'), e ENUM('x','y'), \
          j JSON, emo VARCHAR(50) CHARACTER SET utf8mb4, bl BLOB, t TIME(6), dec65 DECIMAL(65,30), bt BIT(64), u BIGINT UNSIGNED, lat1 VARCHAR(20) CHARACTER SET latin1); \
        INSERT INTO tv_nasty VALUES (1, '0000-00-00', '0000-00-00 00:00:00', '0000-00-00 00:00:00', 0, 'a,c', 'y', '{\"k\": \"ünï 😀\"}', '😀🎉', x'FFFE00C3', \
          '-838:59:59', '12345678901234567890123456789012345.123456789012345678901234567890', b'1111', 18446744073709551615, 'café'), \
          (2, '2020-02-00', '2020-00-01 10:00:00', NULL, 2155, '', NULL, '[]', NULL, NULL, NULL, NULL, NULL, NULL, NULL)", "s").await.unwrap();
    let (text, bin, bb) = text_and_binary_rows(&st, "my", "shop", "tv_nasty").await;
    let names: Vec<String> = bb
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    for (r, (t, b)) in text.iter().zip(&bin).enumerate() {
        for (c, n) in names.iter().enumerate() {
            assert_eq!(t[c], b[c], "row {r} column {n}: editor vs browse");
        }
    }
    let col = |n: &str| names.iter().position(|x| x == n).unwrap();
    assert_eq!(
        bin[0][col("zd")].as_deref(),
        Some("0000-00-00"),
        "zero date is not NULL"
    );
    assert_eq!(bin[0][col("zdt")].as_deref(), Some("0000-00-00 00:00:00"));
    assert_eq!(bin[1][col("zd")].as_deref(), Some("2020-02-00"));
    assert_eq!(bin[1][col("zts")], None, "a real NULL stays NULL");
    assert_eq!(bin[0][col("emo")].as_deref(), Some("😀🎉"));
}

/// A tab's pinned connection that the server dropped (restart, idle timeout,
/// pg_terminate_backend, KILL) must not leave the tab failing forever.
#[tokio::test]
async fn tab_recovers_after_its_connection_is_killed() {
    if !enabled() {
        return;
    }
    let st = state();
    for (id, pid_sql, kill) in [
        (
            "pg",
            "SELECT pg_backend_pid()",
            "SELECT pg_terminate_backend({})",
        ),
        ("my", "SELECT CONNECTION_ID()", "KILL {}"),
    ] {
        let r = st
            .run_editor_query(id, "victim", pid_sql, "a")
            .await
            .unwrap();
        let pid = row_strings(&batch(&st, &r.id), DbKind::Postgres)[0][0]
            .clone()
            .unwrap();
        st.run_editor_query(id, "killer", &kill.replace("{}", &pid), "b")
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        let first = st.run_editor_query(id, "victim", "SELECT 1", "c").await;
        eprintln!(
            "{id} after kill: {:?}",
            first
                .as_ref()
                .map(|r| r.row_count)
                .map_err(|e| e.to_string())
        );
        if let Err(e) = &first {
            assert!(e.to_string().contains("connection"), "{id}: {e}");
        }
        let second = st.run_editor_query(id, "victim", "SELECT 1", "d").await;
        assert!(
            second.is_ok(),
            "{id}: tab still broken: {:?}",
            second.err().map(|e| e.to_string())
        );
    }
}

/// Closing a tab stops its query instead of leaving the connection busy until it ends.
#[tokio::test]
async fn closing_a_tab_cancels_its_query() {
    if !enabled() {
        return;
    }
    let st = state();
    for (id, sleep, busy) in [
        (
            "pg",
            "SELECT pg_sleep(30)",
            "SELECT count(*) FROM pg_stat_activity WHERE query = 'SELECT pg_sleep(30)'",
        ),
        (
            "my",
            "SELECT SLEEP(30)",
            "SELECT count(*) FROM information_schema.PROCESSLIST WHERE INFO = 'SELECT SLEEP(30)'",
        ),
    ] {
        let st2 = st.clone();
        let sleep2 = sleep.to_string();
        let run =
            tokio::spawn(async move { st2.run_editor_query(id, "doomed", &sleep2, "z").await });
        tokio::time::sleep(Duration::from_millis(700)).await;
        let t = Instant::now();
        st.close_tab_session(id, "doomed");
        let res = run.await.unwrap();
        assert!(res.is_err(), "{id}: closed tab still stored a result");
        assert!(
            t.elapsed() < Duration::from_secs(5),
            "{id}: query kept running for {:?}",
            t.elapsed()
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
        let r = st.run_editor_query(id, "watch", busy, "w").await.unwrap();
        assert_eq!(
            row_strings(&batch(&st, &r.id), DbKind::Postgres)[0][0].as_deref(),
            Some("0"),
            "{id}: still running on the server"
        );
    }
}

#[tokio::test]
async fn sqlite_file_deleted_or_unreadable() {
    if !enabled() {
        return;
    }
    let st = state();
    st.run_editor_query(
        "lite",
        "t",
        "CREATE TABLE gone (id INTEGER PRIMARY KEY, v TEXT); INSERT INTO gone (v) VALUES ('a')",
        "a",
    )
    .await
    .unwrap();
    let path = st.config("lite").unwrap().file_path;
    std::fs::remove_file(&path).unwrap();
    let r = st
        .run_editor_query("lite", "t", "INSERT INTO gone (v) VALUES ('lost?')", "b")
        .await;
    assert!(r.err().unwrap().to_string().contains("moved or deleted"));
    let b = st
        .browse(
            "lite",
            "main",
            "gone",
            &BrowseOptions {
                supersede: None,
                limit: 10,
                offset: 0,
                filters: vec![],
                order_by: None,
            },
        )
        .await;
    assert!(b.err().unwrap().to_string().contains("moved or deleted"));
}

/// The byte cap, not the row cap, bounds editor results: it counts the Arrow
/// data, the session stays usable afterwards, and the result fits the cap.
#[tokio::test]
async fn byte_cap_truncates_editor_results() {
    if !enabled() {
        return;
    }
    let st = state();
    let cap = 8 << 20;
    *st.editor_limits.lock().unwrap() = (db::UI_MAX_ROWS, cap);
    for id in ["pg", "my"] {
        let sql = if id == "pg" {
            "SELECT g, md5(g::text) AS h FROM generate_series(1, 2000000) g"
        } else {
            "SELECT a.id, MD5(b.id) AS h FROM orders a CROSS JOIN orders b"
        };
        let r = st.run_editor_query(id, "cap", sql, "q").await.unwrap();
        let ipc = st.results.get(&r.id).unwrap().ipc.len();
        assert!(r.truncated, "{id}");
        assert!(
            r.row_count > 100_000 && r.row_count < 300_000,
            "{id}: {} rows",
            r.row_count
        );
        assert!(
            ipc > cap * 9 / 10 && ipc < cap + (1 << 20),
            "{id}: {ipc} bytes of IPC for a {cap} byte cap"
        );
        assert_eq!(
            st.run_editor_query(id, "cap", "SELECT 1", "q2")
                .await
                .unwrap()
                .row_count,
            1,
            "{id}"
        );
    }
}

/// Millions of rows load in full now that the row cap is 10M (TablePlus loads
/// them too). Gated: about 5 s and 1 GB.
#[tokio::test]
async fn large_editor_result_loads_fully() {
    if std::env::var("TV_ITEST_LARGE").as_deref() != Ok("1") {
        return;
    }
    let st = state();
    let t = Instant::now();
    let r = st.run_editor_query("pg", "large", "SELECT g AS id, md5(g::text) AS h, g * 0.5 AS f, now() AS at FROM generate_series(1, 5000000) g", "q").await.unwrap();
    let ms = t.elapsed().as_millis().max(1);
    let ipc = st.results.get(&r.id).unwrap().ipc.len();
    eprintln!(
        "5M rows in {ms} ms ({} rows/s), {} MB of Arrow",
        r.row_count as u128 * 1000 / ms,
        ipc >> 20
    );
    assert_eq!(r.row_count, 5_000_000);
    assert!(!r.truncated);
    let b = batch(&st, &r.id);
    assert_eq!(
        arrow_text(&b, 0, 4_999_999, DbKind::Postgres).as_deref(),
        Some("5000000")
    );
}

/// Search-as-you-type on a big table: each keystroke's page cancels the last
/// one, so one scan runs at a time instead of filling the connection pool
/// (before: ten keystrokes on a 5M-row table held all 8 connections for 16 s).
#[tokio::test]
async fn new_search_cancels_the_previous_scan() {
    if !enabled() {
        return;
    }
    let st = state();
    st.run_editor_query("pg", "s", "DROP TABLE IF EXISTS tv_scan; CREATE UNLOGGED TABLE tv_scan AS SELECT g AS id, md5(g::text) AS h, md5((g * 7)::text) AS h2 FROM generate_series(1, 1000000) g; ALTER TABLE tv_scan ADD PRIMARY KEY (id)", "s").await.unwrap();
    let mut hs = vec![];
    for q in ["zz", "zzz", "zzzz", "zzzzz", "zzzzzz"] {
        let st = st.clone();
        // No row matches, so every search scans the whole table.
        let o = BrowseOptions {
            limit: 100,
            offset: 0,
            order_by: None,
            supersede: Some("tab1".into()),
            filters: vec![Filter {
                column: "*".into(),
                op: "contains".into(),
                value: q.into(),
            }],
        };
        hs.push(tokio::spawn(async move {
            st.browse("pg", "public", "tv_scan", &o)
                .await
                .map(|r| r.row_count)
        }));
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
    let mut results = vec![];
    for h in hs {
        results.push(h.await.unwrap().map_err(|e| e.to_string()));
    }
    assert_eq!(
        results.last().unwrap(),
        &Ok(0),
        "the latest search completes"
    );
    let cancelled = results
        .iter()
        .filter(|r| r.as_ref().err().is_some_and(|e| e.contains("cancel")))
        .count();
    assert_eq!(cancelled, results.len() - 1, "{results:?}");
    st.run_editor_query("pg", "s", "DROP TABLE tv_scan", "d")
        .await
        .unwrap();
}

/// Behind a transaction-mode pooler (PgBouncer, Supabase, Neon) a server
/// session is shared with other clients, so it can already hold statements
/// named sqlx_s_1, sqlx_s_2, …; a client that names its own statements the same
/// way fails with "prepared statement sqlx_s_1 already exists". This poisons
/// every pooled connection with those names and then uses every Postgres path.
#[tokio::test]
async fn postgres_works_when_sessions_already_hold_sqlx_statements() {
    if !enabled() {
        return;
    }
    let st = state();
    let pool = st.pool("pg").await.unwrap();
    let db::Pool::Pg(pg) = &*pool else { panic!("pg pool") };
    let mut held = vec![];
    for _ in 0..8 {
        held.push(pg.acquire().await.unwrap());
    }
    for c in held.iter_mut() {
        // Connecting must not have left named statements behind either.
        let n: i64 = sqlx::Row::try_get(
            &sqlx::raw_sql("SELECT count(*) FROM pg_prepared_statements")
                .fetch_one(&mut **c)
                .await
                .unwrap(),
            0,
        )
        .unwrap();
        assert_eq!(n, 0, "connect left prepared statements");
        let poison: String = (1..=40)
            .map(|i| format!("PREPARE sqlx_s_{i} AS SELECT {i};"))
            .collect();
        sqlx::raw_sql(sqlx::AssertSqlSafe(poison))
            .execute(&mut **c)
            .await
            .unwrap();
    }
    drop(held);

    pool.schema_tree().await.unwrap();
    pool.describe("public", "orders").await.unwrap();
    let opts = BrowseOptions {
        supersede: None,
        limit: 20,
        offset: 0,
        filters: vec![Filter { column: "status".into(), op: "=".into(), value: "paid".into() }],
        order_by: None,
    };
    let page = st.browse("pg", "public", "orders", &opts).await.unwrap();
    assert!(page.row_count > 0);
    let searched = BrowseOptions { supersede: Some("tab".into()), ..opts.clone() };
    st.browse("pg", "public", "orders", &searched).await.unwrap();
    assert!(st.count("pg", "public", "orders", &opts.filters).await.unwrap() > 0);
    let claude = st.run_claude_query("pg", "SELECT count(*) AS n FROM customers", 10).await.unwrap();
    assert_eq!(claude.meta.row_count, 1);
    let ran = st.run_editor_query("pg", "t1", "SELECT * FROM customers LIMIT 3", "q1").await.unwrap();
    assert_eq!(ran.row_count, 3);
    // Column names for an empty result come from a plain query, not a prepared statement.
    let mut conn = pool.conn().await.unwrap();
    let cols = db::empty_columns(&mut conn, "SELECT id, plan FROM customers WHERE false;").await;
    let names: Vec<_> = cols.iter().map(|(n, _)| n.as_str()).collect();
    assert_eq!(names, ["id", "plan"]);
    drop(conn);
    pool.session().await.unwrap();
}
