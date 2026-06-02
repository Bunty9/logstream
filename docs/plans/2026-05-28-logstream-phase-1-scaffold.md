---
title: logstream Phase 1 — Scaffold + Compile
status: draft
date: 2026-05-28
related:
    - ../specs/2026-05-28-logstream-design.md
    - ../../../projects-l3-l4.md
    - ../../../backend-cloud-roadmap.md
---

# logstream Phase 1 — Scaffold + Compile

> **Goal:** lay down every workspace member, the ClickHouse + Postgres
> schemas, the container story, and CI so that `cargo check --workspace`
> is green and `docker compose up` boots ClickHouse + Postgres + Redis +
> Grafana + the two `logstream-*` services end-to-end. No real OTLP →
> row mapping yet; `otlp_to_rows` returns an empty `Vec`.

**Spec source:** [`../specs/2026-05-28-logstream-design.md`](../specs/2026-05-28-logstream-design.md).

## File inventory (checklist)

- [x] `Cargo.toml` — workspace root, three members + pinned stack deps
      from `backend-cloud-roadmap.md` § 3.
- [x] `rust-toolchain.toml` — `channel = "stable"` + clippy + rustfmt.
- [x] `deny.toml` — minimal `cargo-deny` config (advisories deny, license
      allowlist for MIT/Apache/BSD/ISC/MPL/Unicode/CC0).
- [x] `.gitignore` — Rust + `.env` + `target/` + `*.cwasm` + `dist/` +
      `.venv/`.
- [x] `clickhouse/init.sql` — `logs` MergeTree table, bloom_filter index
      on `trace_id`, `PARTITION BY toYYYYMMDD(ts)`, 30-day TTL.
- [x] `migrations/0001_init.sql` — Postgres `api_keys` table.
- [x] `crates/core/Cargo.toml` + `crates/core/src/lib.rs` — exports
      `auth`, `batcher`, `otlp`, `types`.
- [x] `crates/core/src/types.rs` — `LogRow` (matches ClickHouse columns) +
      `TenantBatch`.
- [x] `crates/core/src/auth.rs` — `TenantAuth` (Redis cache + Postgres
      truth, blake3 key hash).
- [x] `crates/core/src/batcher.rs` — `run_batcher` + `flush`
      (size-or-time trigger).
- [x] `crates/core/src/otlp.rs` — `otlp_to_rows` placeholder with the
      Phase-2 TODO.
- [x] `crates/ingest/Cargo.toml` + `crates/ingest/src/main.rs` +
      `crates/ingest/src/ingest.rs` — axum server with `POST /v1/logs`.
- [x] `crates/query/Cargo.toml` + `crates/query/src/main.rs` — axum
      server with `/health` and a `/query` placeholder.
- [x] `Dockerfile` — cargo-chef multi-stage, distroless final, both bins.
- [x] `docker-compose.yml` — clickhouse + postgres + redis + grafana +
      logstream-ingest + logstream-query.
- [x] `.github/workflows/ci.yml` — matrix on stable + beta; runs `cargo
      fmt --check`, `cargo clippy -- -D warnings`, `cargo nextest run`,
      `cargo deny check`, and `cargo bench --no-run` (non-blocking).
- [x] `README.md` — problem, ASCII architecture, stack table,
      docker-compose quick-start, bench targets, license.
- [x] `docs/specs/2026-05-28-logstream-design.md` — full P3 design spec.
- [x] `docs/plans/2026-05-28-logstream-phase-1-scaffold.md` — this plan.
- [x] `PROGRESS.md` — per-sprint tracker, P3 bench targets recorded.

## Exit criteria

1. **`cargo check --workspace` passes** from the project root with no
   `DATABASE_URL`, `REDIS_URL`, or ClickHouse instance available
   (offline check; `sqlx::query!` macros are avoided in Phase 1 — switch
   to them once `sqlx prepare` runs in CI).
2. **`docker compose up`** boots `clickhouse` + `postgres` + `redis` +
   `grafana` (all healthy), `logstream-ingest` on `:4318`, and
   `logstream-query` on `:4319`.
3. **`curl -X POST -H 'x-api-key: TESTKEY' http://localhost:4318/v1/logs`**
   returns **HTTP 200** with `{"rejected": 0}` once the tenant has been
   seeded via:

   ```sql
   INSERT INTO api_keys (key_hash, tenant_id)
   VALUES (blake3_hex('TESTKEY'), 'demo');
   ```

## Out of scope (deferred to later phases)

- Real `otlp_to_rows` mapping (Phase 2).
- Prometheus `/metrics` endpoint on both binaries and Grafana
  pre-provisioned datasource (Phase 2).
- LogQL-ish query parser + ClickHouse SELECT translation (Phase 3).
- 100k events/s benchmark harness + criterion targets (Phase 4).
- ClickHouse Cloud + Neon + Upstash deploy pipeline (Phase 5).
- Switch back to `sqlx::query!` macros + `sqlx prepare` artifact in CI.

## Verification recipe

```bash
cd logstream
cargo check --workspace           # exit-criterion 1
docker compose up --build -d      # exit-criterion 2
sleep 10
# Seed a tenant key (blake3 of "TESTKEY"):
HASH=$(printf TESTKEY | b3sum | cut -d' ' -f1)
docker compose exec -T postgres psql -U logstream -d logstream -c \
  "INSERT INTO api_keys (key_hash, tenant_id) VALUES ('$HASH', 'demo');"
curl -sS -o /dev/null -w '%{http_code}\n' -X POST \
  -H 'x-api-key: TESTKEY' \
  -H 'Content-Type: application/x-protobuf' \
  --data-binary @/dev/null \
  http://localhost:4318/v1/logs
# exit-criterion 3: response is 200
docker compose down -v
```
