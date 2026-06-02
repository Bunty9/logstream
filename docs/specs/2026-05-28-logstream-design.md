---
title: logstream — OTel + ClickHouse log ingestion (P3)
status: draft
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
