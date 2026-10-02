---
title: logstream — phases 2-4 summary
date: 2026-09-26
---

# Phases 2-4 summary (2026-09-26)

Phase 1 (scaffold + compile) is `docs/plans/2026-05-28-logstream-phase-1-scaffold.md`
and commit `b0d1fdf`. This note covers everything since: Phase 2 (real
OTLP mapping + observability), Phase 3 (query API), and the "ops
hardening + perf" sprint that followed it (`PROGRESS.md` doesn't number
that sprint "Phase 4", but it's the fourth batch of work after the
scaffold, hence this doc's title).

## Delivered, by commit

`git log --oneline` (oldest first):

```
b0d1fdf Initial commit: logstream L3 OTel to ClickHouse ingest       (Phase 1)
133f832 Implement OTLP mapping, harden ingest, add LogQL query API    (Phase 2 + 3)
4ccc604 Fix review findings: ts clamping, memory bounds, auth failure modes
d75326b Fix re-review findings in limits, error mapping and auth tests
e4b9ad3 Add end-to-end script, load generator and compose fixes
1d2c90a Concurrent flushes, local auth cache, Grafana Loki health, bench docs
```

**`133f832`** — the bulk of Phase 2 and Phase 3 in one commit:

- Real `otlp_to_rows` mapping (`crates/core/src/otlp.rs`):
  `resource_logs[].scope_logs[].log_records[]` walk, severity-number →
  fixed vocabulary, attribute/resource flattening with JSON coercion for
  non-string `AnyValue`s.
- Prometheus `/metrics` on both binaries.
- The LogQL-subset parser (`crates/query/src/logql.rs`) and translation
  to parameterized ClickHouse SQL (`crates/query/src/sql.rs`), plus the
  `/query`, `/trace/{trace_id}`, and `/loki/api/v1/*` handlers
  (`crates/query/src/handlers.rs`).
- Grafana provisioning (ClickHouse + Prometheus datasources, dashboard).

**`4ccc604` / `d75326b`** — review-driven hardening, not new features:
timestamp clamping (`row_ts_at`, the 30-day/1h window), `Arc`-sharing of
`tenant_id`/`service`/`resource` to bound per-request memory, the
`RowBinary` `FixedString`/`Map` serde helpers
(`crates/core/src/types.rs`), `AuthError`/`Result`-returning auth (503 on
backend failure vs. 401 on a definitive miss), negative caching, the
`ch_err` ClickHouse-exception-code sanitization, the
`GlobalConcurrencyLimitLayer` fix (a plain `ConcurrencyLimitLayer` would
have given each of 6 routes its own semaphore), and the row-count-before-
row-materialization fix in the ingest handler (413/permit checks run
against `record_count(&decoded)`, not `otlp_to_rows(..).len()`, so an
oversized request never pays for the full allocation first).

**`e4b9ad3`** — `scripts/e2e.sh`, `crates/ingest/examples/loadgen.rs`
(the OTLP load generator), and compose fixes (the ClickHouse
`127.0.0.1`-not-`localhost` healthcheck, `CLICKHOUSE_SKIP_USER_SETUP`).

**`1d2c90a`** — the ops-hardening/perf sprint: concurrent flushes in the
batcher (`--flush-concurrency`, default 4 — the dominant throughput
fix), `--max-rows` default raised 5,000 → 50,000, the in-process auth TTL
cache in front of Redis (`crates/core/src/auth.rs`'s `local_cache`), the
Grafana 11 Loki-datasource health-check simplification
(`is_vector_health_probe`), `scripts/bench.sh`, and the bench
tables/loss-test writeup in `PROGRESS.md`.

## Verification performed

- **Unit tests**: extensive `#[cfg(test)]` coverage in every module that
  has non-trivial logic — `crates/core/src/otlp.rs` (severity mapping,
  timestamp clamping edge cases, `Arc`-sharing, hex id validation),
  `crates/core/src/auth.rs` (local-cache hit/miss/expiry/eviction,
  header parsing), `crates/query/src/logql.rs` (parser grammar and error
  cases), `crates/query/src/sql.rs` (SQL/bind generation per matcher/
  filter type, limit clamping), `crates/query/src/time_util.rs` (Loki's
  timestamp-magnitude heuristic, overflow-safe range clamping),
  `crates/query/src/handlers.rs` and `crates/ingest/src/ingest.rs`
  (status-code mapping via `tower::ServiceExt::oneshot` against the real
  router, no real Postgres/Redis/ClickHouse needed).
- **Integration tests** (gated on env vars, skipped with a printed
  message otherwise — not run by default `cargo test`):
  `crates/ingest/tests/clickhouse_it.rs` (`LOGSTREAM_IT_CLICKHOUSE_URL`)
  round-trips a `LogRow` through a real ClickHouse server to catch
  `FixedString`/`Map` RowBinary wire-format bugs a pure in-process test
  can't; `crates/ingest/tests/auth_it.rs`
  (`LOGSTREAM_IT_PG_URL`+`LOGSTREAM_IT_REDIS_URL`) verifies the Redis
  cache key is the blake3 hash, never the plaintext key, and that
  unknown keys are negative-cached, against real Postgres + Redis;
  `crates/query/tests/clickhouse_integration.rs`
  (`LOGSTREAM_IT_CLICKHOUSE_URL`) exercises the generated SQL against a
  real ClickHouse server including tenant isolation;
  `crates/query/tests/concurrency_limit.rs` proves
  `GlobalConcurrencyLimitLayer` is shared across merged routes, not
  duplicated per-route. `.github/workflows/ci.yml`'s `integration` job
  runs all of these against service containers on every push/PR.
- **End-to-end**: `scripts/e2e.sh` — brings up the full compose stack,
  seeds two tenants, sends real OTLP-HTTP traffic via the `loadgen`
  example, and checks: health/metrics endpoints, ClickHouse row counts,
  `POST /query`, `GET /trace/{id}`, the `/loki/api/v1/*` subset, tenant
  isolation (one tenant's key must see zero of another's rows),
  missing-key → `401`, Grafana datasource provisioning + the Loki
  datasource's real health-check round trip, dashboard provisioning, and
  graceful shutdown (a known batch of rows must be durably flushed after
  `docker compose stop -t 10`). Last recorded run: "ALL CHECKS PASSED"
  (`PROGRESS.md`, 2026-09-26).
- **Bench**: `scripts/bench.sh` sweeps loadgen batch size × concurrency
  against a running stack, reconciling accepted-row count against
  ClickHouse's row-count delta. Measured on an 8-core shared dev box
  (see `PROGRESS.md`'s "Bench methodology" for caveats about the shared
  host and Docker engine choice): 267k-281k rec/s sustained on a
  `docker update --cpus 2`-capped ingest container, 296k-363k rec/s
  uncapped — both well over the 100k rec/s target. p99 ack latency meets
  the <20ms target at concurrency 1-2 (5.0ms/19.3ms) but grows past it at
  concurrency ≥8 (34-388ms), attributed to queueing behind the shared
  bounded channel and flush-concurrency slots, not a fixed per-request
  cost. `cargo bench -p logstream-core --bench otlp` confirms OTLP→row
  mapping alone sustains ~980k-1M records/s on a single core, i.e. it is
  not the bottleneck.
- **Loss test**: `docker pause`/`unpause` ClickHouse for 15s mid-load
  (batch=500, concurrency=16, 45s run) — PASS. Accepted rows
  (7,597,000), `logstream_rows_flushed_total` (7,597,000), and the final
  ClickHouse row count all matched exactly; 12,577,000 rows were
  rejected at the door (`buffer_full`) once the 1,000,000-row buffered
  budget filled during the pause, counted rather than silently dropped;
  `logstream_flush_errors_total` stayed at zero because `docker pause`
  freezes the container rather than closing sockets, so in-flight
  inserts simply resumed on `unpause`. Full writeup in `PROGRESS.md`
  under "Loss test result".

## Remaining open items

- **`sqlx::query!` macros**: currently `sqlx::query_as` (untyped,
  runtime-checked) is used for the one tenant-lookup query
  (`crates/core/src/auth.rs`); switching to the `query!`/`query_as!`
  compile-time-checked macros (and wiring `sqlx prepare`/`.sqlx/` into
  CI) is tracked as not-yet-done in `PROGRESS.md`'s Phase 2 checklist.
- **Streaming query results**: `crates/query/src/handlers.rs::run_select`
  uses `fetch_all` — the entire result set is buffered in memory before
  the JSON response is built. Fine at today's `MAX_LIMIT = 5000` row cap,
  but not truly streamed; `PROGRESS.md`'s Phase 3 checklist flags this as
  outstanding.
- **`/v1/traces`**: no traces table, no OTLP traces protobuf decoding,
  and no ingest route for it, despite both `README.md` and the design
  spec's architecture diagram listing `/v1/logs /v1/traces`. `GET
  /trace/{trace_id}` on the query side is a *log* lookup by trace id, not
  trace ingestion/storage — see the design spec's new §9 for the same
  deviation noted against the original spec.
- **Phase 5 (deploy)**: `README.md`'s roadmap lists a deploy phase
  (ClickHouse Cloud / Hetzner / Fly.io per the original spec's §5) as not
  yet started; today's only deployment target is the local
  `docker-compose.yml` stack.
- **CI on GitHub**: first runs on 2026-09-26 surfaced two CI-only bugs
  (missing `PGPASSWORD` in the migration step; the ingest ClickHouse
  integration test dropping `default.logs` while the query crate's test
  used it). Both fixed; run 36220041790 is green across test (stable,
  beta), integration, cargo-deny, docker build and criterion.
