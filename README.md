# logstream

> OTLP-HTTP log ingest with ClickHouse storage and a Grafana-compatible read
> API. Tenanted, batched, drop-newest under backpressure. Built in Rust to
> sit alongside Vector / OpenObserve / Uptrace as a power-couple stack of
> `axum + clickhouse-rs`.

[![ci](https://img.shields.io/badge/ci-pending-lightgrey.svg)](./.github/workflows/ci.yml)
[![license](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

## The problem

A typical self-hosted observability stack is Prometheus + Loki + Grafana +
cAdvisor. Loki is fine for logs but cluster-grade log ingestion is its own
discipline. **logstream** is a Rust ingest endpoint that speaks OTLP-HTTP,
validates tenants against Postgres (Redis- and in-process-cached), batches
into ClickHouse on a 50k-rows-or-200ms trigger with several flushes
in flight at once, and serves a Grafana-compatible read API.

## Architecture

```
+--------------+   OTLP/HTTP    +-----------------------+   batch insert    +---------------+
| OTel SDK     | -------------> | axum ingest endpoint  | ----------------> | ClickHouse    |
| (any lang)   |   protobuf     |  /v1/logs /v1/traces  |   async client    | logs, traces  |
+--------------+                +-----+-----------------+                   +-------+-------+
                                      |                                             |
                                      | tenant-key check (local TTL cache -> Redis -> PG) |
                                      v                                             |
                                +-----+-----------+                                  |
                                | bounded mpsc    |     drop-newest backpressure     |
                                +-----+-----------+     when ClickHouse slows        |
                                      |                                              |
                                      v                                              |
                                +-----+-----------+                                  |
                                | batcher actor   |  size: 50k rows OR time: 200ms   |
                                | (N flushes      |  whichever first; up to          |
                                |  in flight)     |  --flush-concurrency at once     |
                                +-----+-----------+                                  |
                                      |                                              |
                                      +----------------------------------------------+

Read side (separate binary):
  +----------------+   LogQL-ish     +-------------------+
  | Grafana plugin | --------------> | logstream-query   | -> ClickHouse SELECT
  +----------------+                 +-------------------+
```

## Stack

| Layer              | Crate / Tool                                              |
| ------------------ | --------------------------------------------------------- |
| Async runtime      | `tokio` 1.47 (full)                                       |
| HTTP server        | `axum` 0.8 + `tower` + `tower-http`                       |
| OTLP wire format   | `prost` 0.13 + `opentelemetry-proto` 0.27                 |
| Columnar store     | `clickhouse` 0.12 (clickhouse-rs)                         |
| Tenant metadata    | `sqlx` 0.8 (Postgres)                                     |
| API-key cache      | `redis` 0.27 (TTL 60s) + in-process `RwLock<HashMap>` (TTL 10s) |
| Backpressure       | `tokio::sync::mpsc` bounded + `try_send` drop-newest      |
| Hashing            | `blake3` 1 (API key fingerprints)                         |
| Observability      | `tracing` + `metrics-exporter-prometheus`                 |
| CLI                | `clap` 4                                                  |
| Container build    | `cargo-chef` multi-stage, distroless final                |
| Local orchestration| `docker-compose` (CH + PG + Redis + Grafana + 2 services) |
| CI                 | GitHub Actions (stable + beta) + `cargo-deny` + `cargo-nextest` |

Full pinned versions live in [`Cargo.toml`](./Cargo.toml). Schemas:
[`clickhouse/init.sql`](./clickhouse/init.sql),
[`migrations/0001_init.sql`](./migrations/0001_init.sql).

## Quick start (docker-compose)

```bash
git clone <your-fork-url> logstream
cd logstream
docker compose up --build -d
# ClickHouse :8123 (HTTP) / :9000 (native), Postgres :5432, Redis :6379,
# Prometheus :9090, Grafana :3000, ingest :4318, query :4319.
# Every port is a `${VAR:-default}` in docker-compose.yml (CH_HTTP_PORT,
# CH_NATIVE_PORT, PG_PORT, REDIS_PORT, GRAFANA_PORT, PROM_PORT,
# INGEST_PORT, QUERY_PORT) so a second stack can run alongside this one
# under a different `-p <project>` with different ports.

# Seed an API key (hashes with b3sum/python-blake3/cargo, whichever is
# available, then INSERTs with ON CONFLICT DO NOTHING):
./scripts/seed-key.sh TESTKEY demo

# Generate real OTLP-HTTP traffic with the bundled load generator
# (protobuf ExportLogsServiceRequest batches, connection-pooled client):
cargo run --release -p logstream-ingest --example loadgen -- \
  --url http://localhost:4318/v1/logs --key TESTKEY \
  --duration 10 --batch 500 --concurrency 8

# Query it back — plain JSON:
curl -sS -H 'x-api-key: TESTKEY' -H 'content-type: application/json' \
  -d '{"query":"{service=\"checkout\"}"}' http://localhost:4319/query | jq

# ...or by trace id:
curl -sS -H 'x-api-key: TESTKEY' \
  http://localhost:4319/trace/0123456789abcdef0123456789abcdef | jq

# Grafana (http://localhost:3000, admin/admin, anonymous viewing enabled)
# comes with three provisioned datasources (grafana/provisioning/):
# "logstream" (Loki-compatible, backed by logstream-query), "ClickHouse"
# (direct SQL), and "Prometheus" (scrapes both binaries' /metrics) — plus
# a pre-loaded dashboard. Explore against "logstream" with a LogQL query
# like `{service="checkout"}`.
```

`scripts/e2e.sh` brings the stack up and runs a full proof (health,
tenant isolation, ClickHouse/query-API/Grafana round trips, graceful
shutdown). `scripts/bench.sh` runs a throughput/latency sweep against an
already-running stack — see "Bench methodology" in
[`PROGRESS.md`](./PROGRESS.md).

## Bench targets

| Metric                                                       | Target          | Notes                                              |
| ------------------------------------------------------------ | --------------- | -------------------------------------------------- |
| Throughput (sustained, 2 vCPU host)                          | >= 100k rec/s   | OTLP-HTTP → ClickHouse, drop-newest under pressure |
| p99 ingest ack                                               | < 20 ms         | from request to channel hand-off                   |
| 1B-row trace_id point lookup                                 | < 1 s           | bloom_filter(0.01) index on `trace_id`             |
| Loss test (ClickHouse paused via `docker pause`)             | drop-newest     | row drops counted, never silently absorbed         |
| Cost vs Datadog log ingestion at same volume                 | 10-50x cheaper  | rows/$ comparison documented in benches            |

See [`PROGRESS.md`](./PROGRESS.md) for the measured bench table, loss-test
result, and methodology notes. `cargo bench --workspace` runs the
`otlp_to_rows` mapping-cost microbenchmark
(`crates/core/benches/otlp.rs`); end-to-end throughput/latency is measured
with `scripts/bench.sh` against a running compose stack, not `cargo
bench`.

## API

### Ingest — `POST /v1/logs` (logstream-ingest, default `:4318`)

- Body: a protobuf-encoded OTLP `ExportLogsServiceRequest`
  (`Content-Type: application/x-protobuf`). OTLP/JSON bodies aren't
  supported — no client in practice sends them by default.
- `Content-Encoding: gzip` is accepted; decompression is streamed and
  capped by `--max-body-bytes` on the *decompressed* size, so a gzip bomb
  can't over-allocate.
- Auth: `x-api-key: <key>` header, or `Authorization: Bearer <key>` as a
  fallback (checked in that order).
- On success: `200` with a protobuf-encoded `ExportLogsServiceResponse`
  body (`Content-Type: application/x-protobuf`) — an OTLP-HTTP exporter
  expects exactly this shape, not JSON.
- Status codes: `200` accepted onto the batcher (not yet durably
  written); `401` missing/invalid API key; `413` body over
  `--max-body-bytes`, or more log records than `--max-buffered-rows`
  could ever hold; `415` `Content-Type` isn't `application/x-protobuf`;
  `429` the `--max-buffered-rows` budget or the batcher's bounded channel
  is temporarily full (OTLP-retryable — back off and retry); `503` the
  Postgres tenant-auth backend is unreachable, or the process is
  mid-shutdown.
- `GET /health` → `200 ok`. `GET /metrics` → Prometheus text exposition.

### Query — logstream-query (default `:4319`)

- `POST /query` — JSON body `{"query": "<logql>", "start"?, "end"?,
  "limit"?, "direction"?}` → `{"rows": [...]}`. `query` is the LogQL
  subset in `crates/query/src/logql.rs`: a stream selector
  `{label op "value", ...}` (`=`, `!=`, `=~`, `!~`; at least one matcher
  required) plus zero or more line filters (`|=`, `!=`, `|~`, `!~`).
  `start`/`end` accept unix seconds, unix nanoseconds, fractional
  seconds, or RFC3339 (Loki's own parsing rule); default window is the
  trailing 1h, server-clamped to 31 days regardless of what's asked for.
- `GET /trace/{trace_id}` — 32 lowercase-hex-char trace id → every row
  with that `trace_id` for the caller's tenant, optionally bounded by
  `?start=&end=`.
- Loki-compatible subset (what Grafana's built-in Loki datasource and
  Explore use): `GET /loki/api/v1/query_range`, `GET /loki/api/v1/query`
  (instant — including Grafana's own `vector(1)+vector(1)` health-check
  probe, answered directly as a `vector` result rather than through the
  LogQL parser), `GET /loki/api/v1/labels`,
  `GET /loki/api/v1/label/{name}/values`.
- Auth: same `x-api-key`/`Bearer` as ingest, required on every route.
  `401` missing/invalid key; `503` auth backend unreachable; `400` bad
  LogQL syntax; `502`/`503` a ClickHouse-side error (the raw exception
  text never reaches the client — only a sanitized message; the full
  error is logged server-side).

## Tunables

`logstream-ingest` (env var / flag / default / what it trades off):

| Env var                       | Flag                    | Default                | Trade-off                                                                 |
| ------------------------------ | ------------------------ | ----------------------- | -------------------------------------------------------------------------- |
| `LOGSTREAM_BIND`               | `--bind`                | `0.0.0.0:4318`          | listen address                                                            |
| `DATABASE_URL`                 | `--database-url`        | *(required)*            | Postgres, tenant metadata source of truth                                 |
| `REDIS_URL`                    | `--redis-url`           | `redis://127.0.0.1:6379`| API-key cache                                                             |
| `CLICKHOUSE_URL`               | `--clickhouse-url`      | `http://127.0.0.1:8123` | |
| `CLICKHOUSE_DB`                | `--clickhouse-db`       | `default`               | |
| `LOGSTREAM_CHAN_CAPACITY`      | `--chan-capacity`       | `1024`                  | batcher channel depth, in batches (not rows)                              |
| `LOGSTREAM_MAX_ROWS`           | `--max-rows`            | `50000`                 | flush trigger by row count — bigger batches amortize ClickHouse's per-insert overhead better; see PROGRESS.md's bench methodology |
| `LOGSTREAM_FLUSH_MS`           | `--flush-ms`            | `200`                   | flush trigger by elapsed time — bounds ack-to-durable latency, not raw throughput |
| `LOGSTREAM_FLUSH_CONCURRENCY`  | `--flush-concurrency`   | `4`                     | ClickHouse inserts allowed in flight at once (see `crates/core/src/batcher.rs`) |
| `LOGSTREAM_MAX_BODY_BYTES`     | `--max-body-bytes`      | `16777216`              | request size cap, post-gzip-decompression                                 |
| `LOGSTREAM_MAX_BUFFERED_ROWS`  | `--max-buffered-rows`   | `1000000`               | total rows in flight (channel + batcher buffer + flushes in progress); the `429` backpressure budget |

`logstream-query`:

| Env var                            | Flag                          | Default        | Trade-off                                          |
| ------------------------------------ | ------------------------------ | ---------------- | ----------------------------------------------------- |
| `LOGSTREAM_QUERY_BIND`              | `--bind`                      | `0.0.0.0:4319` | listen address                                     |
| `DATABASE_URL` / `REDIS_URL` / `CLICKHOUSE_URL` / `CLICKHOUSE_DB` | same as ingest | | |
| `LOGSTREAM_MAX_CONCURRENT_QUERIES`  | `--max-concurrent-queries`    | `16`           | ClickHouse queries served concurrently, across every `/query`/`/trace`/`/loki` route combined |

`docker-compose.yml` port overrides: `CH_HTTP_PORT`, `CH_NATIVE_PORT`,
`PG_PORT`, `REDIS_PORT`, `GRAFANA_PORT`, `PROM_PORT`, `INGEST_PORT`,
`QUERY_PORT`, plus `LOGSTREAM_GRAFANA_API_KEY` (the key the provisioned
Grafana "logstream" datasource authenticates with) and
`LOGSTREAM_MAX_ROWS` / `LOGSTREAM_FLUSH_MS` / `LOGSTREAM_FLUSH_CONCURRENCY`,
which pass straight through to the `logstream-ingest` container.

Auth caching: a revoked key keeps working for up to ~70s per process
(60s Redis TTL + 10s in-process cache). Unknown keys are cached as misses
for 10s in Redis and 10s locally, so a key looked up just before it was
issued can be rejected for up to ~20s.

## Metrics

Both binaries expose Prometheus text exposition on `GET /metrics`.

`logstream-ingest`:

- `logstream_ingest_requests_total{status}` — counter, one per request
- `logstream_ingest_rows_total` — counter, rows accepted onto the batcher (not yet durably written)
- `logstream_dropped_rows_total{reason="channel_full"|"buffer_full"|"flush_error"|"flush_panic"}` — counter
- `logstream_rows_flushed_total` — counter, rows durably written to ClickHouse
- `logstream_flush_duration_seconds` — histogram, one ClickHouse insert's wall time
- `logstream_flush_errors_total` — counter, flush attempts that exhausted their retries (rows counted in `logstream_dropped_rows_total{reason="flush_error"}` are dropped, not silently absorbed)
- `logstream_ts_clamped_total` — counter, records whose `time_unix_nano` fell outside `[now - 30d, now + 1h]` and was clamped

`logstream-query`:

- `logstream_query_requests_total{status}` — counter
- `logstream_query_duration_seconds` — histogram

## Repository layout

```
logstream/
  Cargo.toml                # workspace
  crates/
    core/                   # LogRow, TenantBatch, auth, batcher, otlp mapping
    ingest/                 # axum binary — POST /v1/logs
    query/                  # axum binary — Grafana-compatible read API
  clickhouse/init.sql       # `logs` MergeTree + bloom_filter trace_id index
  migrations/0001_init.sql  # Postgres `api_keys` table
  Dockerfile                # cargo-chef multi-stage, distroless final
  docker-compose.yml        # CH + PG + Redis + Grafana + 2 services
  deny.toml                 # cargo-deny config
  rust-toolchain.toml       # stable channel
  .github/workflows/ci.yml  # nextest + clippy + fmt + deny + bench
  docs/
    specs/2026-05-28-logstream-design.md      # full design spec
    plans/2026-05-28-logstream-phase-1-scaffold.md
  PROGRESS.md               # per-sprint tracker
```

## Roadmap

Phase 1 (scaffold + compile) is the current sprint — see
[`docs/plans/2026-05-28-logstream-phase-1-scaffold.md`](./docs/plans/2026-05-28-logstream-phase-1-scaffold.md).
Subsequent phases (OTLP→row mapping, read API, Grafana datasource, benches
at 100k events/s, deploy) are tracked in [`PROGRESS.md`](./PROGRESS.md).

## License <a id="license"></a>

Dual-licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](./LICENSE-APACHE) or
  <https://www.apache.org/licenses/LICENSE-2.0>)
- MIT License ([LICENSE-MIT](./LICENSE-MIT) or
  <https://opensource.org/licenses/MIT>)

at your option.
