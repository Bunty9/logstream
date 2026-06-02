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
validates tenants against Postgres (Redis-cached), batches into ClickHouse
on a 5k-rows-or-200ms trigger, and serves a Grafana-compatible read API.

## Architecture

```
+--------------+   OTLP/HTTP    +-----------------------+   batch insert    +---------------+
| OTel SDK     | -------------> | axum ingest endpoint  | ----------------> | ClickHouse    |
| (any lang)   |   protobuf     |  /v1/logs /v1/traces  |   async client    | logs, traces  |
+--------------+                +-----+-----------------+                   +-------+-------+
                                      |                                             |
                                      | tenant-key check (Redis cache, PG truth)    |
                                      v                                             |
                                +-----+-----------+                                  |
                                | bounded mpsc    |     drop-newest backpressure     |
                                | (per-tenant)    |     when ClickHouse slows        |
                                +-----+-----------+                                  |
                                      |                                              |
                                      v                                              |
                                +-----+-----------+                                  |
                                | batcher actor   |  size: 5k rows OR time: 200ms    |
                                | per-tenant      |  whichever first                 |
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
| API-key cache      | `redis` 0.27 (TTL 60s)                                    |
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
docker compose up --build
# ClickHouse :8123/:9000, Postgres :5432, Redis :6379,
# Grafana :3000, ingest :4318, query :4319.

# Seed an API key (blake3 of "TESTKEY" → key_hash):
docker compose exec postgres psql -U logstream -d logstream -c \
  "INSERT INTO api_keys (key_hash, tenant_id) VALUES ('$(printf TESTKEY | b3sum | cut -d' ' -f1)', 'demo');"

# Send an empty OTLP logs request (no payload mapping yet — Phase 2):
curl -sS -X POST http://localhost:4318/v1/logs \
  -H 'Content-Type: application/x-protobuf' \
  -H 'x-api-key: TESTKEY' \
  --data-binary @/dev/null
# => 200 {"rejected": 0}
```

## Bench targets

| Metric                                                       | Target          | Notes                                              |
| ------------------------------------------------------------ | --------------- | -------------------------------------------------- |
| Throughput (sustained, 2 vCPU host)                          | >= 100k rec/s   | OTLP-HTTP → ClickHouse, drop-newest under pressure |
| p99 ingest ack                                               | < 20 ms         | from request to channel hand-off                   |
| 1B-row trace_id point lookup                                 | < 1 s           | bloom_filter(0.01) index on `trace_id`             |
| Loss test (ClickHouse paused via `docker pause`)             | drop-newest     | row drops counted, never silently absorbed         |
| Cost vs Datadog log ingestion at same volume                 | 10-50x cheaper  | rows/$ comparison documented in benches            |

Run benches (once `cargo bench` targets exist):

```bash
cargo bench --workspace
```

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
