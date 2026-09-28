# CLAUDE.md — logstream

logstream is an OTLP-HTTP log ingest service written in Rust. It writes logs to
ClickHouse and has a read API that speaks a LogQL subset and a subset of the
Loki HTTP API, so Grafana can query it. Tenants are identified by API keys:
Postgres is the source of truth, cached in Redis and in-process. Start with
README.md (architecture, API, tunables, metrics), docs/operations.md (runbook)
and PROGRESS.md (status, benchmarks).

## Layout

- `crates/core` holds the shared code:
  - `types.rs`: `LogRow` and `TenantBatch`, plus the RowBinary serde helpers.
  - `otlp.rs`: OTLP to `LogRow` mapping.
  - `auth.rs`: `TenantAuth`, the `TenantLookup` trait and `api_key_from_headers`.
  - `batcher.rs`: the ClickHouse batcher.
  - `examples/hash_key.rs` and `benches/otlp.rs`.
- `crates/ingest` is the `POST /v1/logs` binary. `examples/loadgen.rs` is the
  load generator used by `scripts/bench.sh`.
- `crates/query` is the read API, with a lib target and a bin target:
  - `logql.rs`: parser.
  - `sql.rs`: translation to ClickHouse SQL.
  - `handlers.rs`: HTTP handlers.
  - `time_util.rs`: Loki-style time parsing.
  - `row.rs`: row type used for SELECTs.
- `clickhouse/init.sql` is the `logs` table and `migrations/0001_init.sql` is
  `api_keys` (idempotent).
- `scripts/` contains `e2e.sh`, `bench.sh` and `seed-key.sh`. `grafana/`
  contains the provisioning for datasources, the dashboard and Prometheus.

## Commands

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings   # CI uses RUSTFLAGS=-D warnings
cargo test --workspace                                   # hermetic; integration tests skip without env
cargo bench -p logstream-core --bench otlp -- --quick
```

The integration tests are gated on environment variables. When a variable is
unset, the test prints a message and passes.

- `LOGSTREAM_IT_CLICKHOUSE_URL` covers `crates/ingest/tests/clickhouse_it.rs`
  and `crates/query/tests/clickhouse_integration.rs`. The table from
  `clickhouse/init.sql` must already exist.
- `LOGSTREAM_IT_PG_URL` and `LOGSTREAM_IT_REDIS_URL` cover
  `crates/ingest/tests/auth_it.rs`, which applies the migration itself.

Run them against throwaway containers. ClickHouse needs
`-e CLICKHOUSE_SKIP_USER_SETUP=1`, otherwise the empty-password `default` user
cannot connect over the network. Postgres needs a readiness wait before the
tests run.

For the full-stack check, run `scripts/e2e.sh`. It brings the compose stack up
and does not tear it down unless `TEARDOWN=1` is set. After that,
`scripts/bench.sh` runs load against the same stack.

## Local environment gotchas

- The docker CLI context `desktop-linux` (Docker Desktop) has hung before.
  When that happens, every `docker` command blocks. Use the native engine:
  `export DOCKER_HOST=unix:///var/run/docker.sock`, and wrap docker commands in
  `timeout`.
- Host port 8123 is already in use. Use the compose port overrides
  (`CH_HTTP_PORT`, `PG_PORT`, `REDIS_PORT`, `GRAFANA_PORT`, `PROM_PORT`,
  `INGEST_PORT`, `QUERY_PORT`, `CH_NATIVE_PORT`) and a unique
  `COMPOSE_PROJECT_NAME`.
- The native engine also runs unrelated stacks (coolify, n8n, odoo). Only
  touch `logstream-*` projects.
- The disk is nearly full. After runs, clean up with
  `docker compose -p <name> down -v --rmi local`.

## Invariants — don't break these

- **Schema lockstep.** Column order and types in `clickhouse/init.sql` must
  match `LogRow`'s field order (RowBinary is positional). The clickhouse crate
  is 0.12, which does not validate the schema. Two consequences:
  - `FixedString(N)` needs `fixed_string::n32`/`n16`. A plain `String` is
    written length-prefixed and corrupts the insert.
  - `Map` needs `map_as_pairs`, because 0.12 panics on serde maps.
- **Tenant isolation.** Every SQL statement from `query::sql` begins with
  `tenant_id = ?`, and every predicate is joined with `AND`. Every user value
  is a `?` bind. The only identifiers written into the SQL text come from the
  whitelisted column names. Unknown labels are bound as map keys.
- **SELECT aliases must not reuse a column name.** Writing
  `toUnixTimestamp64Nano(ts) AS ts` makes ClickHouse substitute the alias into
  `WHERE ts >= ...`. Use `ts_ns`, `trace_id_hex` and similar.
- **Row accounting.** Every accepted row is either flushed or counted in
  `logstream_dropped_rows_total{reason=...}`. The `--max-buffered-rows`
  semaphore permit travels with rows until their flush finishes. Rows are
  counted, and the permit is taken, before `otlp_to_rows` builds them.
- **Auth failure modes.** Backend failure returns `Err` and maps to 503. An
  unknown key returns `Ok(None)` and maps to 401. The difference matters
  because OTLP exporters drop data on 401 but retry on 503. Redis is keyed only
  by the blake3 hash of the key, never the plaintext key. The negative-cache
  sentinel is `"\0"`.
- **Timestamps.** Client timestamps are kept only inside
  `[now - 30d, now + 1h]`. Outside that window the observed time is used, and
  failing that the current time. This keeps one insert under ClickHouse's
  100-partitions-per-insert limit and inside the 30-day TTL.
- **Ingest checks run before the body is read.** Content-type and auth are
  checked first. The body limit applies after gzip decompression.

## Documentation site

The site at https://bunty9.github.io/logstream/ is an mdBook.
`scripts/build-docs.py` assembles `book-src/` from README.md, PROGRESS.md and
`docs/`, rewriting links that leave the book to GitHub URLs. A new doc page
must be added to `PAGES` and `SUMMARY` in that script.
`.github/workflows/docs.yml` deploys the site on pushes to `main` that touch
the docs. To build locally, run `python3 scripts/build-docs.py && mdbook build`.

## Releasing

See docs/publishing.md. All three crates share one version, and
`rust-version` is 1.88 (the highest minimum any locked dependency declares).
Always run `cargo publish --workspace --dry-run` first. A real publish cannot
be undone, so only do it when explicitly asked.

## Conventions

- Keep code minimal and idiomatic, and match the existing doc-comment density.
- Every behaviour change gets a focused test. Handler tests use
  `tower::ServiceExt::oneshot` with fake `TenantLookup` implementations.
- Docs must match the code. Grep for env vars and metric names before
  documenting them.
- Commits: `Bunty9 <Bunty9@users.noreply.github.com>`, with no AI or tool
  attribution trailers (see the global CLAUDE.md).
