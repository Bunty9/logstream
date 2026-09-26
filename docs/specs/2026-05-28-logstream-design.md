---
title: logstream — OTel + ClickHouse log ingestion (P3)
status: implemented
date: 2026-05-28
related:
    - ../../../backend-cloud-roadmap.md
    - ../../../projects-l3-l4.md
---

# logstream — Design Spec

> Companion spec lifted from `projects-l3-l4.md` § "P3 — OTel + ClickHouse
> log ingestion (logstream)". Code blocks are the authoritative
> implementation reference for the scaffold; downstream phases extend, they
> do not contradict. Default stack pins live in `backend-cloud-roadmap.md`
> § 3 and are mirrored verbatim in [`Cargo.toml`](../../Cargo.toml).

## 1. Problem

A typical self-hosted observability stack is Prometheus + Loki + Grafana +
cAdvisor. Loki is fine for logs but cluster-grade log ingestion is its own
discipline. Build a Rust ingest endpoint that speaks OTLP-HTTP, validates
tenants, batches into ClickHouse, serves a Grafana-compatible read API.
**Interview pitch:** observability is a top-3 hiring vertical (Grafana,
Axiom, Datadog, Sentry, Honeycomb, Highlight, Baselime, OpenObserve,
Uptrace) and Rust + ClickHouse is the recognized power-couple.

## 2. Architecture

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

## 3. Stack

- `axum` + `prost` + `opentelemetry-proto` for OTLP parsing.
- `clickhouse` crate (clickhouse-rs) for inserts.
- `sqlx` (Postgres) for tenant metadata, `redis` for API-key cache (TTL 60s).
- `tokio::sync::mpsc::channel` (bounded) for backpressure.
- `tracing` + `metrics-exporter-prometheus` (eat dog food).

## 4. Key Rust code

### 4.1 ClickHouse schema

```sql
CREATE TABLE logs (
  tenant_id     LowCardinality(String),
  ts            DateTime64(9, 'UTC'),
  severity      LowCardinality(String),
  service       LowCardinality(String),
  trace_id      FixedString(32),
  span_id       FixedString(16),
  body          String,
  attrs         Map(LowCardinality(String), String),
  resource      Map(LowCardinality(String), String),
  INDEX idx_trace trace_id TYPE bloom_filter(0.01) GRANULARITY 4
) ENGINE = MergeTree
PARTITION BY toYYYYMMDD(ts)
ORDER BY (tenant_id, service, ts)
TTL toDateTime(ts) + INTERVAL 30 DAY;
```

### 4.2 Ingest endpoint (`crates/ingest/src/ingest.rs`)

```rust
use axum::{extract::State, body::Bytes, http::HeaderMap, response::IntoResponse};
use prost::Message;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;

#[derive(Clone)]
pub struct AppState {
    pub auth: Arc<TenantAuth>,
    pub sender: tokio::sync::mpsc::Sender<TenantBatch>,
}

pub async fn logs(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<impl IntoResponse, (axum::http::StatusCode, &'static str)> {
    let api_key = headers.get("x-api-key").and_then(|v| v.to_str().ok())
        .ok_or((axum::http::StatusCode::UNAUTHORIZED, "missing x-api-key"))?;
    let tenant_id = state.auth.lookup(api_key).await
        .ok_or((axum::http::StatusCode::UNAUTHORIZED, "invalid key"))?;

    let req = ExportLogsServiceRequest::decode(body)
        .map_err(|_| (axum::http::StatusCode::BAD_REQUEST, "bad proto"))?;

    let rows = otlp_to_rows(tenant_id, &req);
    let batch = TenantBatch { tenant_id, rows };

    // try_send => drop-newest backpressure (defensible default)
    match state.sender.try_send(batch) {
        Ok(()) => Ok(axum::Json(serde_json::json!({"rejected": 0}))),
        Err(tokio::sync::mpsc::error::TrySendError::Full(b)) => {
            metrics::counter!("logstream_dropped_rows",
                "tenant" => b.tenant_id.clone()).increment(b.rows.len() as u64);
            Err((axum::http::StatusCode::TOO_MANY_REQUESTS, "channel full"))
        }
        Err(_) => Err((axum::http::StatusCode::SERVICE_UNAVAILABLE, "shutdown")),
    }
}
```

### 4.3 Batcher actor (`crates/core/src/batcher.rs`)

```rust
use tokio::sync::mpsc::Receiver;
use tokio::time::{interval, Duration, MissedTickBehavior};

pub async fn run_batcher(
    mut rx: Receiver<TenantBatch>,
    ch: clickhouse::Client,
    max_rows: usize,
    flush_ms: u64,
) -> anyhow::Result<()> {
    let mut buf: Vec<LogRow> = Vec::with_capacity(max_rows * 2);
    let mut tick = interval(Duration::from_millis(flush_ms));
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            Some(batch) = rx.recv() => {
                buf.extend(batch.rows);
                if buf.len() >= max_rows {
                    flush(&ch, &mut buf).await?;
                }
            }
            _ = tick.tick() => {
                if !buf.is_empty() {
                    flush(&ch, &mut buf).await?;
                }
            }
            else => break,
        }
    }
    if !buf.is_empty() { flush(&ch, &mut buf).await?; }
    Ok(())
}

async fn flush(ch: &clickhouse::Client, buf: &mut Vec<LogRow>) -> anyhow::Result<()> {
    let started = std::time::Instant::now();
    let mut insert = ch.insert("logs")?;
    for row in buf.drain(..) {
        insert.write(&row).await?;
    }
    insert.end().await?;
    metrics::histogram!("logstream_flush_duration_seconds")
        .record(started.elapsed().as_secs_f64());
    Ok(())
}
```

### 4.4 Tenant auth with Redis cache (`crates/core/src/auth.rs`)

```rust
use redis::AsyncCommands;
use sqlx::PgPool;

pub struct TenantAuth { redis: redis::Client, pg: PgPool }

impl TenantAuth {
    pub async fn lookup(&self, api_key: &str) -> Option<String> {
        let mut r = self.redis.get_multiplexed_async_connection().await.ok()?;
        let cache_key = format!("tenant:{}", api_key);
        if let Ok(Some(tenant_id)) = r.get::<_, Option<String>>(&cache_key).await {
            return Some(tenant_id);
        }
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT tenant_id FROM api_keys WHERE key_hash = $1 AND revoked_at IS NULL"
        )
        .bind(blake3::hash(api_key.as_bytes()).to_hex().to_string())
        .fetch_optional(&self.pg).await.ok()?;
        if let Some((tenant_id,)) = row {
            let _: () = r.set_ex(&cache_key, &tenant_id, 60).await.ok()?;
            Some(tenant_id)
        } else { None }
    }
}
```

## 5. Deployment

- ClickHouse Cloud free tier (or self-hosted ClickHouse single-node for demo).
- Hetzner CX22 (~€4/mo) or Fly.io for the ingest service.
- Tenant config in Neon Postgres + Upstash Redis.

## 6. Eval / benchmarks

- Throughput: 100k log records/s sustained on a 2-vCPU host with p99
  ingest ack < 20 ms.
- Query: 1B-row trace_id lookup target < 1 s.
- Loss test: pause ClickHouse with `docker pause`, sustain ingest →
  measure dropped rows; resume → verify recovery; document the policy
  (drop-newest, not block).
- Cost: rows/$ vs Datadog log ingestion at the same volume — should be
  10–50× cheaper.

## 7. Stretch to L4

- Replace ClickHouse with a custom columnar disk format + memory-mapped
  index (push toward P5 storage territory).
- Build a PromQL/LogQL parser + query planner in Rust.
- Streaming queries via gRPC + tonic + flight-rs.

## 8. Source references

- Vector.dev architecture.
- ClickHouse docs on inserts + bloom filter indexes.
- OpenObserve, Uptrace (Rust + ClickHouse stacks for prior art).

## 9. As-built deviations (2026-09-26)

Concrete differences between what shipped (`crates/*/src`,
`clickhouse/init.sql`, `migrations/0001_init.sql`) and this spec's
original §2–§4 sketch, with the one-line reason for each. See
`docs/operations.md` for full operational detail on any of these.

- **Single shared bounded channel + a row-count semaphore, not a
  per-tenant channel/batcher.** §2's diagram shows a per-tenant `bounded
  mpsc` and per-tenant batcher actor; the shipped design
  (`crates/ingest/src/main.rs`, `crates/core/src/batcher.rs`) runs one
  process-wide `mpsc::channel<TenantBatch>` and one batcher task, with a
  `tokio::sync::Semaphore` (`--max-buffered-rows`) bounding total
  in-flight rows across every tenant combined. Simpler to reason about
  and bound memory for, at the cost of one noisy tenant being able to
  fill the shared channel/budget and 429 everyone else — acceptable
  given the drop-newest-and-count backpressure policy already implies no
  per-tenant fairness guarantee.
- **Concurrent flushes, not one flush at a time.** §4.3's `run_batcher`
  sketch `.await`s each flush serially. The shipped batcher spawns each
  due flush into its own task, bounded by `--flush-concurrency`
  (default 4), so new rows keep accumulating while earlier batches are
  still being written — measured as the dominant throughput fix (see
  `PROGRESS.md`'s bench table: ~50% higher accepted rec/s at moderate
  concurrency, `429`s eliminated at c=8).
- **`--max-rows` default raised from the spec's 5,000 to 50,000.**
  Bigger batches amortize ClickHouse's per-insert overhead; measured
  gain, smaller than concurrent flushes but still a real one (see
  `PROGRESS.md`).
- **Timestamp clamping**, absent from the spec entirely.
  `crates/core/src/otlp.rs::row_ts_at` rejects `time_unix_nano` outside
  `[now-30d, now+1h]` (falling back to `observed_time_unix_nano`, then
  `now`) because the `logs` table is `PARTITION BY toYYYYMMDD(ts)` and
  ClickHouse refuses an insert spanning more than 100 partitions — one
  client with a wrong clock could otherwise fail an entire multi-tenant
  flush.
- **`Arc`-shared row fields**, not per-row owned `String`/`BTreeMap`.
  `LogRow`'s `tenant_id`, `service`, and `resource` are `Arc<str>`/
  `Arc<BTreeMap<..>>`, built once per OTLP request/resource and cloned
  (refcount bump) per row, because a single 16 MiB request can decode
  into millions of tiny records and per-row deep-cloning those fields was
  the dominant memory cost.
- **RowBinary serde helpers for `FixedString`/`Map`**, not a plain
  derive. The spec's schema (§4.1) and `LogRow` sketch don't address the
  wire format at all; the installed `clickhouse` crate (0.12) panics on
  a plain `Map` derive (`serialize_map`/`deserialize_map` are
  `unimplemented!()`) and silently corrupts `FixedString(N)` columns
  given a length-prefixed `String`. `crates/core/src/types.rs` adds
  `map_as_pairs` (shuttles through `Vec<(K, V)>`, which the crate does
  support) and `fixed_string::{n16,n32}` (packs/unpacks a `[u8; N]`)
  serde helper modules to bridge this.
- **Auth returns `Result`, not `Option`; `503` on backend failure;
  negative caching; a local in-process cache; hashed Redis keys.** §4.4's
  `TenantAuth::lookup` sketch returns `Option<String>` (folding "invalid
  key" and "backend down" into the same `None`) and caches under
  `tenant:{api_key}` (plaintext). The shipped version
  (`crates/core/src/auth.rs`) returns
  `Result<Option<String>, AuthError>` so callers can map a genuine
  Postgres outage to `503` (OTLP/Loki-retryable) instead of `401`
  (terminal); negative-caches unknown keys (10s TTL, `"\0"` sentinel) so
  a flood of garbage keys doesn't hit Postgres on every request; adds a
  10s in-process `RwLock<HashMap>` cache in front of the Redis round
  trip; and caches under `tenant:{blake3(api_key)}` so a Redis
  `KEYS`/`SCAN`/dump can't leak live plaintext secrets.
- **Protobuf response body, gzip request support, bearer-token auth
  fallback** — none of which the spec's §4.2 sketch mentions. The
  shipped `/v1/logs` handler returns a protobuf-encoded
  `ExportLogsServiceResponse` (OTLP-HTTP exporters expect exactly this
  shape, not the sketch's JSON `{"rejected": 0}`), accepts
  `Content-Encoding: gzip` via `tower_http::decompression`, and accepts
  `Authorization: Bearer <key>` as a fallback when `x-api-key` is absent.
- **Query API is a LogQL subset plus a Loki HTTP subset**, not the bare
  ClickHouse-SELECT sketch implied by §2's read-side diagram. Shipped:
  a hand-rolled recursive-descent LogQL parser (`crates/query/src/logql.rs`,
  stream selectors + line filters, no metric queries), translation to
  parameterized ClickHouse SQL with every value bound rather than
  interpolated (`crates/query/src/sql.rs`), and Grafana/Loki-compatible
  routes (`/loki/api/v1/query_range`, `/query`, `/labels`,
  `/label/{name}/values`) so Grafana's built-in Loki datasource works
  against it directly — plus a hard-coded tenant filter on every
  translated query, which the spec's read-side diagram doesn't call out
  as a requirement at all.
- **No `/v1/traces` yet.** Both the spec's architecture diagram (§2) and
  `README.md`'s currently list `/v1/logs /v1/traces` on the ingest
  endpoint; only `/v1/logs` is implemented. There is no traces table, no
  OTLP traces protobuf decoding, and no trace-ingest route — `GET
  /trace/{trace_id}` on the *query* side looks up log rows carrying that
  `trace_id`, which is a different feature (trace-correlated log lookup,
  not trace ingestion/storage).
