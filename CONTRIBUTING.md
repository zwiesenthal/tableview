# Contributing

Thanks for taking a look. TableView is a Tauri 2 app: a Rust backend in `src-tauri/` and a React + TypeScript frontend in `src/`. [docs/architecture.md](docs/architecture.md) explains how the pieces fit.

## Prerequisites

- macOS (the app uses the Keychain and macOS window vibrancy).
- Node.js 20.19 or newer, and npm.
- A recent Rust stable via [rustup](https://rustup.rs) (the crate uses edition 2024).
- Xcode Command Line Tools (`xcode-select --install`).
- Docker, only for the integration tests.
- Optional: the [Claude Code](https://docs.claude.com/en/docs/claude-code) CLI, signed in, for the ⌘K chat panel.

## Run it

```bash
npm install            # .npmrc sets legacy-peer-deps; Glide Data Grid declares React <= 18
npm run tauri dev      # Vite on :1420 plus the app, with hot reload for the frontend
```

To keep your everyday settings and connections apart from a development copy, point it at another folder. This also lets it run beside an installed TableView:

```bash
TABLEVIEW_DATA_DIR=/tmp/tableview-dev npm run tauri dev
```

## Build

```bash
npm run app
# src-tauri/target/release/bundle/macos/TableView.app
```

`npm run app` signs with a Developer ID or Apple Development certificate from your login keychain when it finds one, and otherwise ad-hoc. To choose the certificate, set `APPLE_SIGNING_IDENTITY` (for example `"Apple Development: you@example.com (TEAMID)"`) when you build. Do not commit it to `tauri.conf.json`.

## Checks

```bash
npm run typecheck      # tsc --noEmit
npx vite build         # also needed before cargo: Tauri embeds ../dist at compile time
cd src-tauri
cargo test             # unit tests
cargo clippy --all-targets
```

`npm test` runs the frontend build and the unit tests in one go. CI (`.github/workflows/ci.yml`) runs the same on macOS.

## Integration tests

They run against real Postgres 16 and MySQL 8.4 seeded from `tests/seed`. Bind the containers to 127.0.0.1 so the throwaway password is not exposed on your network:

```bash
docker compose -f tests/seed/compose.yml up -d --wait   # waits until both are seeded and healthy
cd src-tauri && TV_ITEST=1 cargo test
```

Without Compose, the equivalent `docker run` commands are:

```bash
cd tests/seed
docker run -d --name tv-pg -e POSTGRES_PASSWORD=tvpass -e POSTGRES_DB=shop \
  -p 127.0.0.1:55432:5432 -v "$PWD/pg.sql:/docker-entrypoint-initdb.d/seed.sql:ro" postgres:16
docker run -d --name tv-mysql -e MYSQL_ROOT_PASSWORD=tvpass -e MYSQL_DATABASE=shop \
  -p 127.0.0.1:53306:3306 -v "$PWD/mysql.sql:/docker-entrypoint-initdb.d/seed.sql:ro" mysql:8.4
```

Without `TV_ITEST=1` the integration tests return early and pass. Remove them with `docker compose -f tests/seed/compose.yml down` (or `docker rm -f tv-pg tv-mysql`).

The same containers make good manual test data: add a connection to `127.0.0.1:55432`, user `postgres`, password `tvpass`, database `shop`.

## Pull requests

- Keep changes focused, one topic per PR.
- Run the checks above. Add a unit test for backend logic, and an integration test when it depends on a real server.
- Anything that lets Claude touch a database goes through `db/guard.rs` and the read-only transaction. Changes there need tests that show a write still fails.
- Match the existing UI: native macOS look, works in light and dark mode, keyboard reachable. When in doubt, do what TablePlus does.
- Never commit credentials, signing identities or local paths.

## License

By contributing you agree that your contributions are licensed under the [MIT License](LICENSE).
