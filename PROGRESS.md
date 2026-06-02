# PROGRESS — logstream

> Per-sprint tracker. Template adapted from `project-plan.md` § 7,
> customised for P3 (logstream) bench targets and the Phase B sequencing
> in `backend-cloud-roadmap.md` § 2 (weeks 17–22).

## Sprint — Phase 1 scaffold

- [x] Workspace `Cargo.toml` with three members + pinned stack deps
- [x] `crates/core` — `LogRow`, `TenantBatch`, `TenantAuth`, `run_batcher`,
      `otlp_to_rows` placeholder
- [x] `crates/ingest` — axum binary, `POST /v1/logs`, bounded channel,
      drop-newest backpressure
- [x] `crates/query` — axum binary, `/health` + `/query` placeholder
- [x] `clickhouse/init.sql` — `logs` MergeTree + bloom_filter trace index
      + 30-day TTL
- [x] `migrations/0001_init.sql` — Postgres `api_keys` table
- [x] `Dockerfile` — cargo-chef multi-stage + distroless
- [x] `docker-compose.yml` — clickhouse + postgres + redis + grafana +
      ingest + query
- [x] `.github/workflows/ci.yml` — fmt + clippy + nextest + deny + bench
- [x] `deny.toml`, `rust-toolchain.toml`, `.gitignore`
- [x] `README.md`, design spec, phase plan
- [ ] `cargo check --workspace` passes locally (verified at end of scaffold)
- [ ] `docker compose up` boots ClickHouse + Postgres + Redis + Grafana +
      both services
- [ ] `POST /v1/logs` (with seeded tenant) returns 200

## Next sprint — Phase 2: real OTLP mapping + observability

- [ ] `otlp_to_rows` walks `resource_logs[].scope_logs[].log_records[]`
      → `LogRow`
- [ ] Severity mapping (number → `LowCardinality(String)` text)
- [ ] Attribute flattening (string-coerce `AnyValue`)
- [ ] Prometheus `/metrics` endpoint on both binaries
- [ ] Pre-provisioned Grafana datasource (ClickHouse) + dashboard
- [ ] Switch back to `sqlx::query!` macros + `sqlx prepare` in CI

## Next next sprint — Phase 3: query API

- [ ] `/query` parses a minimal LogQL-ish surface
- [ ] Translation to ClickHouse SELECT (with tenant filter)
- [ ] Streaming rows back (chunked)

## Done

(none yet — scaffold landing is the first commit)

## Blocked

- (none)

## Bench numbers (targets per `projects-l3-l4.md` § P3; updated weekly)

| metric                                              | target          | current | as-of      |
|-----------------------------------------------------|-----------------|---------|------------|
| Throughput (sustained, 2 vCPU)                      | >= 100k rec/s   |         |            |
| p99 ingest ack                                      | < 20 ms         |         |            |
| 1B-row trace_id point lookup                        | < 1 s           |         |            |
| Loss test (ClickHouse paused via `docker pause`)    | drop-newest     |         |            |
| Cost vs Datadog log ingestion (same volume)         | 10-50x cheaper  |         |            |

## Blog topics surfacing

- (none yet)
