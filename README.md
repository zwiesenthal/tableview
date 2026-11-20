<p align="center">
  <img src="docs/images/icon.png" width="112" alt="TableView icon">
</p>

<h1 align="center">TableView</h1>

<p align="center">
  A fast, native-feeling database client for macOS, with Claude built in.<br>
  Postgres · MySQL · SQLite
</p>

<p align="center">
  <img src="docs/images/demo.gif" width="800" alt="25-second demo: a million rows in 563 ms, benchmarks against TablePlus, Claude making a chart, and features Claude built from feedback"><br>
  <sub><a href="https://github.com/zwiesenthal/tableview/raw/main/docs/demo.mp4">Download the 1080p video with sound</a></sub>
</p>

<p align="center">
  <img src="docs/images/hero-claude-chart.png" alt="TableView with the Claude panel open, answering a revenue question with a table and a bar chart in a new tab">
</p>

TableView looks and works like the database clients you already know: a sidebar of tables, tabs, a grid you can edit, and a SQL editor. It adds two things. Results move from Rust to the grid as Apache Arrow and render on a canvas, so large results stay smooth. And **⌘K** opens a Claude panel that runs on your own Claude Code login. Ask a question in plain English and Claude explores the schema, runs read-only queries, and opens the results and charts as tabs next to your work.

## Highlights

- **Ask Claude about your data (⌘K).** Claude reads table structures, runs read-only queries, and opens results, tables, SQL and charts in new tabs. When you want data changed, it hands you the SQL in a tab instead of running it.
- **Fast with big results.** A million rows load in about half a second, using a quarter of the memory TablePlus needs for the same query, and a 5-million-row table opens in milliseconds. See [benchmarks](#benchmarks).
- **Edit safely.** Double-click a cell to edit, insert or delete rows, then **⌘S** to review the exact SQL before it commits. Each update checks the row still holds what you saw, so a concurrent change aborts the commit instead of overwriting it.
- **Find anything (⌘F).** Search every column of a table at once, ignoring case, as you type.
- **Made for daily use.** ⌘P table finder, pinned tabs that come back when you reconnect, columns you can drag into place and keep that way, environment colours (local, dev, staging, prod), and read-only connections.
- **Claude Code in your terminal, too.** TableView runs an MCP server, so `claude` in your terminal can query your databases and open tabs in the app.

## Claude, built in

<p align="center">
  <img src="docs/images/sql-editor.png" width="49%" alt="SQL editor with a revenue query and 500 result rows">
  <img src="docs/images/search.png" width="49%" alt="Any-column search finding 28,561 matching users">
</p>

The panel runs the `claude` CLI you already have, signed in with your account, so there's no API key to manage. If you're signed out, the panel shows a **Sign in** button.

Claude only gets TableView's tools:

| Tool | What it does |
|---|---|
| `list_connections`, `list_tables`, `describe_table` | Learn the schema before writing SQL |
| `run_query`, `get_result` | Run one read-only statement and page through the result |
| `open_table`, `open_query_tab` | Open a table, or hand you SQL to review and run |
| `visualize` | Open a bar, line, area, scatter or pie chart in a new tab |

To use the same tools from Claude Code in your terminal, open **Settings** and copy the `claude mcp add …` command.

### How Claude is kept read-only

Several independent layers, so that no single mistake lets a write through:

1. **SQL gate.** Only a single `SELECT`, `WITH`, `EXPLAIN` (without `ANALYZE`) or `SHOW` statement passes. Side-effect functions are refused wherever they appear, including quoted, Unicode-escaped and `FROM`-clause forms. So are data-modifying CTEs and optimizer hints.
2. **Read-only transaction.** Postgres runs `BEGIN READ ONLY` and MySQL `START TRANSACTION READ ONLY`. SQLite uses a read-only connection with `query_only`. Every query has a 30-second deadline and always rolls back.
3. **Prepared statements**, which the server won't split into several statements.
4. **Local-only MCP server.** It listens on `127.0.0.1`, needs a bearer token, and refuses browser requests and other host names (DNS rebinding).
5. **A confined chat CLI.** It gets no built-in tools, none of your Claude Code settings or hooks, and an empty working directory.

Turn Claude off per connection; `prod` connections start with it off. [SECURITY.md](SECURITY.md) covers the threat model and the known limits.

## Benchmarks

| | TablePlus 6.7.8 | TableView |
|---|---|---|
| Load 1M rows (`SELECT * FROM invoices`) | 1.13 s | **0.52–0.57 s (2× faster)** |
| Memory added by that result | +1.24 GB | **+0.29 GB (¼ the memory)** |

The same queries, run in TablePlus 6.7.8 and TableView on an M-series MacBook, against a local Postgres 17 with a [generated demo dataset](tests/demo/demo.sql) (20k companies, 400k users, 1M invoices, 5M events).

| Query | TablePlus 6.7.8 | TableView |
|---|---|---|
| `SELECT * FROM invoices`: 1M rows, 6 columns | 1.13 s, +1.24 GB memory | **0.52–0.57 s, +0.29 GB memory** |
| `SELECT * FROM events LIMIT 500000` (with `jsonb`) | 0.43 s | **0.25–0.36 s** |
| `SELECT * FROM users`: 400k rows, 8 columns | 0.49 s | **0.34–0.44 s** |
| Aggregate over 5M events | 0.36 s | 0.35 s (the database is the bottleneck) |
| Memory at idle | ~115 MB | ~60–120 MB |

On a million rows, TableView finished in about half the time and used about a quarter of the memory. On smaller results the two are closer, and when the database does the work they tie. In a release-build test of the same grid stack, scrolling 5 million rows held 60 fps (p99 frame 18 ms).

How these were measured:
- **Time** for TablePlus is its own query timer. For TableView it is the range across the editor's query timer and a headless release benchmark of the same code path (fetch plus decoding into Arrow). Both were cross-checked against the server's statement duration (`log_min_duration_statement`). A statement only finishes once the client has read every row, so both apps are measured the same way.
- **Memory** is the resident memory increase from loading the result in a freshly started app. For TableView that's the app plus its web view process.
- **Repeatability:** you can rerun TableView's side with `TV_BENCH=1 cargo test --release bench -- --nocapture` (see [`bench.rs`](src-tauri/src/bench.rs)) against `docker compose -f tests/demo/compose.yml up`.

## Improve it from inside the app

Press **⇧⌘F** and describe a change ("show row counts next to each table"). Claude Code makes it in its own git branch of TableView's source and opens a pull request on your GitHub repo. TableView then runs the type-check, build and tests itself. If they pass, it merges the PR, pulls `main`, rebuilds, and asks to restart into the new version. If a check fails, the PR stays open for you. To review every change before it ships, turn off **Merge automatically** in Settings; TableView then updates within a minute of you merging.

This needs a git checkout of TableView with a GitHub remote, the GitHub CLI signed in (`gh auth login`), and Claude Code. TableView uses the folder it was built from unless you set another in Settings.

The pull requests go to the checkout's `origin`, so you need push access there. If you cloned this repo, the Feedback window offers **Fork on GitHub**: it forks TableView to your account and makes the fork `origin` (this repo becomes `upstream`), so your changes open, merge and install from your fork. To suggest a change back, open a pull request from your fork here.

## Make it yours

Settings (**⌘,**) is driven by a small registry in [`src/prefs.ts`](src/prefs.ts): each setting is declared once, the window draws its controls from that list, and values are saved without backend changes. Built in:

- **Shorten IDs:** long ID, primary-key and foreign-key values show as `usv…jAw`. Copying, editing and the row inspector keep the full value.
- **Timestamp precision:** minutes, seconds or full, plus UTC or local time for zone-aware columns.
- **Claude panel:** left or right side, width, model, and standing instructions ("revenue is in cents", "always chart time series").
- **Per-table layout:** drag columns into the order you like; right-click the header to reset.

All shortcuts are in [docs/SHORTCUTS.md](docs/SHORTCUTS.md), or press **⇧⌘/** in the app.

## Install

TableView builds from source on macOS:

```bash
git clone https://github.com/zwiesenthal/tableview.git
cd tableview
npm install
npm run app
open src-tauri/target/release/bundle/macos/TableView.app
```

You need Node 20.19+, Rust stable, and the Xcode Command Line Tools. `npm run app` signs with a code-signing certificate from your login keychain when there is one (Developer ID, else Apple Development; a free Apple Development certificate comes with Xcode), and so do the updates TableView builds for itself. A stable signature means macOS asks for Keychain access once, not after every rebuild. Without a certificate the build is ad-hoc signed. Set `APPLE_SIGNING_IDENTITY`, or the signing identity in Settings, to pick one. Saved passwords live in the macOS Keychain, never in plain files.

## How it works

```
React + Glide Data Grid (canvas)  ◀── Arrow IPC ──  Rust (Tauri 2)
CodeMirror · ECharts · Claude panel                 sqlx: Postgres · MySQL · SQLite
                                                    MCP server (127.0.0.1) ◀── claude CLI
```

Queries decode straight into Arrow columns in Rust, and the webview receives raw Arrow bytes over Tauri's binary IPC. The grid draws only the visible cells, so scrolling a million rows costs the same as scrolling a hundred. [docs/architecture.md](docs/architecture.md) has the details.

## Contributing

Bug reports and pull requests are welcome. [CONTRIBUTING.md](CONTRIBUTING.md) explains how to run the app in development and how to run the unit and integration tests. Please report security issues as described in [SECURITY.md](SECURITY.md).

## License

[MIT](LICENSE)
