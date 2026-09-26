# logstream operations runbook

Operator-facing reference: deploying the stack, managing tenants/keys,
what clients should expect from the ingest and query APIs, how
backpressure and data loss actually work, query limits, metrics/alerts,
tuning, and troubleshooting. For architecture and the API summary, see
[`README.md`](../README.md) (the architecture diagram lives there — this
doc doesn't repeat it).

## 1. Components and data flow

Two binaries share one workspace (`crates/core`, `crates/ingest`,
`crates/query`) and three backing services:

- **logstream-ingest** (`POST /v1/logs`, default `:4318`) — decodes OTLP
  protobuf, resolves the API key to a tenant, hands rows to an in-process
  batcher, which flushes to ClickHouse.
- **logstream-query** (`/query`, `/trace/{id}`, `/loki/api/v1/*`, default
  `:4319`) — resolves the API key to a tenant, translates a LogQL-subset
  query or route into a ClickHouse `SELECT`, returns JSON.
- **ClickHouse** — the `logs` table (`clickhouse/init.sql`), the only
  place log rows are durably stored.
- **Postgres** — `api_keys` table (`migrations/0001_init.sql`), the
  source of truth for which API key belongs to which tenant.
- **Redis** — shared cache in front of Postgres for both binaries (see
  §3).

Both binaries link the same `logstream-core` crate for tenant auth, OTLP
mapping, and (ingest only) the batcher — there is exactly one
implementation of tenant lookup and one of the wire-format tricks
(`crates/core/src/types.rs`), not a copy per binary.

## 2. Deploying

### Required services

| Service    | Purpose                          | Schema / setup                                              |
| ---------- | --------------------------------- | ------------------------------------------------------------ |
| ClickHouse | log storage                       | `clickhouse/init.sql` (mounted at `/docker-entrypoint-initdb.d/`) |
| Postgres   | tenant/API-key metadata            | `migrations/0001_init.sql`                                    |
| Redis      | API-key cache (both binaries)      | none — plain cache, no persistence required                  |

`docker-compose.yml` wires all three plus Prometheus and Grafana. Both
application images build from the same [`Dockerfile`](../Dockerfile)
(`cargo-chef` multi-stage build, `gcr.io/distroless/cc-debian12` runtime,
non-root `nonroot:nonroot` user); compose overrides `entrypoint:` per
service to pick `logstream-ingest` or `logstream-query` out of the same
image.

### ClickHouse 24.8 empty-password gotcha

`clickhouse/clickhouse-server:24.8-alpine` blocks *network* access
(only the container's local unix socket / `127.0.0.1` inside the
container is allowed) for the `default` user when its password is empty,
unless `CLICKHOUSE_SKIP_USER_SETUP=1` is also set. `docker-compose.yml`
sets `CLICKHOUSE_PASSWORD: ""` and `CLICKHOUSE_SKIP_USER_SETUP: "1"`
together — dropping the latter while keeping an empty password will make
both `logstream-ingest` and `logstream-query` fail to authenticate against
ClickHouse from another container, even though `clickhouse-client` run
inside the ClickHouse container itself still works fine (see
Troubleshooting, §9).

### Env vars

`logstream-ingest`:

| Env var                       | Flag                    | Default                | What it controls                                                            |
| ------------------------------ | ------------------------ | ----------------------- | ------------------------------------------------------------------------- |
| `LOGSTREAM_BIND`               | `--bind`                | `0.0.0.0:4318`          | listen address                                                            |
| `DATABASE_URL`                 | `--database-url`        | *(required)*            | Postgres connection string, tenant metadata source of truth               |
| `REDIS_URL`                    | `--redis-url`           | `redis://127.0.0.1:6379`| API-key cache                                                             |
| `CLICKHOUSE_URL`               | `--clickhouse-url`      | `http://127.0.0.1:8123` | ClickHouse HTTP endpoint                                                  |
| `CLICKHOUSE_DB`                | `--clickhouse-db`       | `default`               | ClickHouse database                                                       |
| `LOGSTREAM_CHAN_CAPACITY`      | `--chan-capacity`       | `1024`                  | batcher channel depth, in **batches** (one per request), not rows          |
| `LOGSTREAM_MAX_ROWS`           | `--max-rows`            | `50000`                 | flush trigger by row count                                                 |
| `LOGSTREAM_FLUSH_MS`           | `--flush-ms`            | `200`                   | flush trigger by elapsed time                                              |
| `LOGSTREAM_FLUSH_CONCURRENCY`  | `--flush-concurrency`   | `4`                     | ClickHouse inserts allowed in flight at once                               |
| `LOGSTREAM_MAX_BODY_BYTES`     | `--max-body-bytes`      | `16777216` (16 MiB)     | request size cap, post-gzip-decompression                                  |
| `LOGSTREAM_MAX_BUFFERED_ROWS`  | `--max-buffered-rows`   | `1000000`                | total rows in flight (channel + batcher buffer + flushes in progress); the `429` budget |

`logstream-query`:

| Env var                             | Flag                        | Default        | What it controls                                    |
| ------------------------------------ | ----------------------------- | ---------------- | ---------------------------------------------------- |
| `LOGSTREAM_QUERY_BIND`              | `--bind`                      | `0.0.0.0:4319` | listen address                                       |
| `DATABASE_URL`                      | `--database-url`              | *(required)*      | Postgres, same tenant table as ingest                |
| `REDIS_URL`                         | `--redis-url`                 | `redis://127.0.0.1:6379` | API-key cache                             |
| `CLICKHOUSE_URL`                    | `--clickhouse-url`            | `http://127.0.0.1:8123` | ClickHouse HTTP endpoint                  |
| `CLICKHOUSE_DB`                     | `--clickhouse-db`             | `default`        | ClickHouse database                                   |
| `LOGSTREAM_MAX_CONCURRENT_QUERIES`  | `--max-concurrent-queries`    | `16`             | requests served concurrently across `/query`, `/trace/*`, `/loki/*` **combined** (not `/health`/`/metrics`) |

`docker-compose.yml` also exposes port overrides (`CH_HTTP_PORT`,
`CH_NATIVE_PORT`, `PG_PORT`, `REDIS_PORT`, `GRAFANA_PORT`, `PROM_PORT`,
`INGEST_PORT`, `QUERY_PORT`) and `LOGSTREAM_GRAFANA_API_KEY` (the key the
provisioned Grafana "logstream" Loki datasource authenticates with —
must be a key already seeded via `scripts/seed-key.sh`, default
`TESTKEY`).

### Health endpoints

Both binaries expose `GET /health` (plain `200 ok`, no dependency checks)
and `GET /metrics` (Prometheus text exposition). `logstream-query`'s
distroless image ships no shell/curl/wget, so it has no Docker
`healthcheck:` in compose — `condition: service_started` is used for it
instead, and Grafana's own Loki-datasource health probe (see §6) is what
actually proves it's serving real queries, not just accepting TCP
connections.

### Graceful shutdown

Both binaries treat SIGINT and SIGTERM the same way (`shutdown_signal()`
in each `main.rs`): `axum::serve(...).with_graceful_shutdown(...)` stops
accepting new connections and waits for in-flight requests to finish.

For `logstream-ingest` specifically, shutdown does **not** drop buffered
rows: the router (and the `mpsc::Sender<TenantBatch>` it owns) is only
dropped once graceful shutdown completes, which closes the batcher's
channel; `main` then `.await`s the batcher task, which — per
`crates/core/src/batcher.rs` — flushes whatever's left in its buffer and
drains every already-spawned flush task before returning. So a clean
shutdown flushes everything that was ever accepted (`200`'d) onto the
batcher, regardless of whether it had reached `--max-rows` or
`--flush-ms` yet.

This takes real wall-clock time — a full flush plus up to
`--flush-concurrency` in-flight ClickHouse inserts draining. **Give it
time**: Docker's default `stop` grace period is 10s before it sends
SIGKILL; `scripts/e2e.sh` explicitly uses `docker compose stop -t 10
logstream-ingest` and that's normally enough at moderate load, but a
container mid-burst with a large buffered-rows budget can need longer.
Size the compose/Kubernetes stop timeout (`stop_grace_period` in compose,
`terminationGracePeriodSeconds` in k8s) to comfortably exceed one flush's
worst-case latency, not just its `--flush-ms` trigger.

## 3. Tenant / API-key management

Tenant metadata is Postgres-only (`api_keys` table); ClickHouse never
sees plaintext keys or tenant-key mappings, only the `tenant_id` string
already resolved by the auth layer. **Only the blake3 hash of an API key
is ever persisted** — `key_hash TEXT UNIQUE`, computed as
`blake3(api_key).to_hex()` — and the same hash (not the plaintext key) is
also what's cached in Redis, under key `tenant:{key_hash}`, so a Redis
`KEYS`/`SCAN`/dump doesn't leak live secrets either.

### Seeding a key

`./scripts/seed-key.sh <api-key> <tenant>` hashes the key (tries `b3sum`,
then Python's `blake3` module, then falls back to
`cargo run -p logstream-core --example hash_key` — the workspace's own
blake3 dependency, so no extra install is required) and runs:

```sql
INSERT INTO api_keys (key_hash, tenant_id) VALUES (:'key_hash', :'tenant')
ON CONFLICT (key_hash) DO NOTHING;
```

via `docker compose exec -T postgres psql` (values passed as psql `:'var'`
bind-style substitutions, not string-interpolated into SQL). Re-running
it for an existing key is a no-op (`ON CONFLICT ... DO NOTHING`).

### Revoking a key

There's no script for this — revoke directly:

```sql
UPDATE api_keys SET revoked_at = now() WHERE key_hash = '<hash>';
-- or, to revoke by known plaintext key, compute the hash first
-- (scripts/seed-key.sh's hash_key() logic, or the hash_key example).
```

`TenantAuth::lookup` only returns a tenant for rows with
`revoked_at IS NULL`, so once the row is updated the *next* uncached
lookup will reject it.

### Cache lag (why revocation and new keys aren't instant)

Both binaries share the same two-layer cache in front of Postgres
(`crates/core/src/auth.rs`):

- Redis: 60s TTL on a positive (resolved-tenant) entry.
- In-process (per-process `RwLock<HashMap>`): 10s TTL, sitting in front
  of the Redis round trip.

**Revocation lag: up to ~70s per process** (60s Redis + 10s local) — a
revoked key can still authenticate for that long on a given process
after the Postgres row is updated, since a still-live cache entry is
never invalidated early.

**New-key lag: up to ~20s.** Unknown/not-yet-issued keys are
negative-cached too (10s in Redis, 10s locally) so that a flood of
garbage/typo'd keys doesn't hit Postgres on every request. A key looked
up moments before it was seeded can therefore be rejected (`401`) for up
to ~20s after the `INSERT`.

Both TTLs are compile-time constants, not tunable via env var/flag — if
tighter revocation is a hard requirement, shorten
`NEGATIVE_CACHE_TTL_SECS` / `POSITIVE_CACHE_TTL_SECS` /
`LOCAL_CACHE_TTL` in `crates/core/src/auth.rs` and rebuild.

## 4. Ingest semantics for clients

- **Protobuf only.** `Content-Type` must start with
  `application/x-protobuf` (an OTLP `ExportLogsServiceRequest`). Any
  other `Content-Type` (including OTLP/JSON) is rejected with `415`;
  OTLP/JSON isn't implemented at all (no collector default exporter sends
  it).
- **Gzip** (`Content-Encoding: gzip`) is accepted and decompressed lazily
  as the body is read, so a request with a bad key never pays for
  decompression. The decompressed-size cap (`--max-body-bytes`) is
  enforced by the streaming reader, so a gzip bomb can't over-allocate
  before being rejected.
- **Auth**: `x-api-key: <key>` header, checked first; falls back to
  `Authorization: Bearer <key>` if `x-api-key` is absent. Checked
  *before* the body is read at all.
- **Status codes** (and OTLP retry semantics):

  | Status | Meaning | OTLP-retryable? |
  | --- | --- | --- |
  | `200` | accepted onto the batcher (not yet durably written) | — |
  | `400` | bad protobuf, or a body-read failure that isn't a size-limit hit (e.g. corrupt gzip) | no |
  | `401` | missing or invalid API key | no |
  | `413` | body over `--max-body-bytes`, or the request has more log records than `--max-buffered-rows` could ever hold even against an empty buffer | no |
  | `415` | `Content-Type` isn't `application/x-protobuf` | no |
  | `429` | `--max-buffered-rows` budget or the batcher's bounded channel is transiently full | **yes** — back off and retry |
  | `503` | the Postgres tenant-auth backend is unreachable, or the process is mid-shutdown | **yes** |

- On success, the response body is a protobuf-encoded
  `ExportLogsServiceResponse` (`Content-Type: application/x-protobuf`),
  not JSON — OTLP-HTTP exporters expect exactly this shape.
- **Timestamp clamping**: `time_unix_nano` is used as-is only if it falls
  within `[now - 30d, now + 1h]` (30 days matches the `logs` table's TTL;
  1h tolerates clock skew). Outside that window, `observed_time_unix_nano`
  is tried under the same window; if that's also out of window (or
  unset), the row gets `now`. This exists because the table is
  `PARTITION BY toYYYYMMDD(ts)` and ClickHouse refuses an INSERT that
  touches more than 100 partitions — one client with a badly wrong clock
  (or backfilling months of history) mixed into a batch could otherwise
  fail the *entire* flush and drop every tenant's accepted rows in it.
  Every clamp of a genuinely out-of-window `time_unix_nano` (not just a
  record that never set it) increments `logstream_ts_clamped_total`.
- **Per-request limits**: request body capped at `--max-body-bytes`
  (16 MiB default) *after* gzip decompression; total records in one
  request capped indirectly by `--max-buffered-rows` (a request whose
  record count alone exceeds the entire buffered-rows budget is `413`,
  permanently — no amount of waiting fixes it, unlike the transient `429`
  case where the budget is just temporarily exhausted by other traffic).

## 5. Backpressure and data-loss model

logstream is deliberately **drop-newest**, never blocking, and every drop
is counted — nothing is silently absorbed. Three independent
rejection/drop points, each mapped to a metric:

| Point | When | Client sees | Metric |
| --- | --- | --- | --- |
| `--max-buffered-rows` semaphore, at the door | The request's row count can't get a permit right now (budget exhausted by other in-flight batches) | `429` — rejected before ever touching the channel | `logstream_dropped_rows_total{reason="buffer_full"}` |
| Batcher's bounded channel (`--chan-capacity`, in batches) | `try_send` on the channel returns `Full` | `429` | `logstream_dropped_rows_total{reason="channel_full"}` |
| ClickHouse flush, after acceptance | A flush exhausts `MAX_FLUSH_ATTEMPTS` (3 tries, 50ms fixed backoff) | none — the client already got `200` | `logstream_dropped_rows_total{reason="flush_error"}` + `logstream_flush_errors_total` |
| Flush task panic | The spawned flush task itself panics mid-write | none — client already got `200` | `logstream_dropped_rows_total{reason="flush_panic"}` |

The first two are **rejected-before-accepted**: the caller gets a `429`
and can retry (OTLP-retryable). The last two are
**accepted-then-dropped**: the caller already got a `200` (the row was
handed to the batcher) but the row never reached ClickHouse — this is the
only case where "accepted" doesn't eventually mean "durable," and it's
why `logstream_dropped_rows_total{reason="flush_error"}` (or
`flush_panic`) firing at all is worth alerting on immediately rather than
just watching a rate.

**Retry duplicate caveat**: `crates/core/src/batcher.rs::flush` has no
idempotency key. If `insert.end()` fails after ClickHouse already durably
wrote part of a batch (e.g. the connection drops mid-write), the bounded
retry (`flush_with_retry`) re-sends the *same* buffered rows — trading
"rows lost" for "rows possibly duplicated" rather than fixing either.
There is currently no dedup on the write or read path for this case.

**Shutdown drain**: see §2 — a graceful SIGTERM/SIGINT is not a drop
point; every accepted row gets a final flush attempt (still subject to
the same retry/drop-on-exhaustion behavior above) before the process
exits.

**Verified loss behavior** (`PROGRESS.md`'s loss test, ClickHouse paused
via `docker pause` mid-load): the accepted-row count, the ClickHouse row
count, and `logstream_rows_flushed_total` all matched exactly
(7,597,000) — every accepted row landed, and every rejected row was
counted as `buffer_full` before acceptance. `docker pause` freezes the
container via the cgroup freezer rather than closing sockets, so in-flight
inserts simply resumed on `unpause` rather than erroring — this test does
not exercise the `flush_error` path.

## 6. Query API operations

Server-side limits, enforced regardless of what the client asks for:

- **Time range**: `end` defaults to now, `start` defaults to `end - 1h`;
  the resulting span is silently clamped to 31 days
  (`time_util::MAX_RANGE_NS`) — a too-wide request still returns data for
  the clamped window rather than erroring (Loki-style silent clamp, not a
  `400`).
- **Row limit**: `limit` defaults to 100 (`sql::DEFAULT_LIMIT`), clamped
  to at most 5000 (`sql::MAX_LIMIT`) and at least 1, regardless of what's
  requested.
- **`max_execution_time`**: every ClickHouse query issued by
  `logstream-query` carries `max_execution_time=30` (set once on the
  shared `clickhouse::Client` in `main.rs`), bounding a single query's
  server-side cost even if it hasn't hit its row limit yet (a wide
  time-range scan can do a lot of I/O before that).
- **Global concurrency**: `--max-concurrent-queries` (default 16) caps
  requests in flight across `/query`, `/trace/*`, and every `/loki/*`
  route **combined** (via `tower::limit::GlobalConcurrencyLimitLayer`,
  which shares one semaphore across all of them — a plain
  `ConcurrencyLimitLayer` would give each of the 6 routes its own
  semaphore, an easy 6x-the-intended-limit bug this avoids). `/health`
  and `/metrics` are not limited.

Error mapping:

| Status | Meaning |
| --- | --- |
| `400` | bad LogQL syntax, or a ClickHouse error attributable to bad user input (invalid regex — CH code 427; invalid query syntax — CH code 62) |
| `401` | missing/invalid API key |
| `502` | any other ClickHouse `BadResponse` — sanitized generic "upstream error"; the real exception text is logged server-side only, never returned to the client |
| `503` | ClickHouse network error/timeout, or the Postgres tenant-auth backend is unreachable |

Loki compatibility subset (what Grafana's built-in Loki datasource and
Explore use): `GET /loki/api/v1/query_range`, `GET /loki/api/v1/query`
(instant), `GET /loki/api/v1/labels` (static `["service", "severity"]`),
`GET /loki/api/v1/label/{name}/values` (whitelisted to `service` and
`severity`; any other label name returns an empty list rather than an
error). The instant `/query` endpoint has no real "value at exactly this
instant" semantics for logs, so it returns the trailing-1h window ending
at `time` (default now) — same shape as `/query_range`.

**Instant-query health-check simplification**: Grafana 11's Loki
datasource health check always sends the literal query
`vector(1)+vector(1)` to `/loki/api/v1/query` and expects a `vector`
result equal to `2`. logstream's LogQL grammar has no metric-query
support to actually evaluate that expression, so
`handlers::is_vector_health_probe` recognizes this one literal
(whitespace-insensitive) and answers it directly with Loki's `vector`
shape — auth is still enforced on that path (a bad key still gets `401`,
not a free pass).

**Grafana datasource setup**: the provisioned "logstream" datasource
(`grafana/provisioning/datasources/datasources.yml`) is `type: loki`,
`url: http://logstream-query:4319`, authenticating via a custom header —
`jsonData.httpHeaderName1: x-api-key` /
`secureJsonData.httpHeaderValue1: $__env{LOGSTREAM_GRAFANA_API_KEY}`. To
add another Loki-compatible datasource by hand (Grafana UI or another
provisioning file), replicate that: type `loki`, the query API's base
URL, and a custom HTTP header named `x-api-key` set to a seeded key — not
Loki's own basic-auth fields, since logstream doesn't implement HTTP
basic auth.

## 7. Metrics reference and suggested alerts

Both binaries expose Prometheus text exposition on `GET /metrics`.

`logstream-ingest`:

| Metric | Type | Meaning |
| --- | --- | --- |
| `logstream_ingest_requests_total{status}` | counter | one per request, labeled by final HTTP status |
| `logstream_ingest_rows_total` | counter | rows accepted onto the batcher (not yet durable) |
| `logstream_dropped_rows_total{reason}` | counter | `reason` one of `channel_full`, `buffer_full`, `flush_error`, `flush_panic` — see §5 |
| `logstream_rows_flushed_total` | counter | rows durably written to ClickHouse |
| `logstream_flush_duration_seconds` | histogram | one ClickHouse insert's wall time |
| `logstream_flush_errors_total` | counter | flush attempts that exhausted all 3 retries |
| `logstream_ts_clamped_total` | counter | records whose nonzero `time_unix_nano` fell outside `[now-30d, now+1h]` and was clamped away from |

`logstream-query`:

| Metric | Type | Meaning |
| --- | --- | --- |
| `logstream_query_requests_total{status}` | counter | one per request across all routes |
| `logstream_query_duration_seconds` | histogram | end-to-end handler latency |

Suggested alerts:

- **`rate(logstream_dropped_rows_total{reason="flush_error"}[5m]) > 0`** —
  page immediately: this is silent (from the client's perspective)
  durable data loss, distinct from the countable-but-expected `429`
  backpressure paths.
- **`rate(logstream_dropped_rows_total{reason="flush_panic"}[5m]) > 0`** —
  same severity as above; also indicates a bug worth a stack trace from
  the logs.
- **Sustained `rate(logstream_dropped_rows_total{reason="buffer_full"}[5m]) > 0`
  / high `429` share of `logstream_ingest_requests_total`** — the
  ingest pipeline is backpressured; check whether ClickHouse is slow
  (flush duration, below) or just under-provisioned for the traffic
  (§8's tuning knobs).
- **`histogram_quantile(0.99, rate(logstream_flush_duration_seconds_bucket[5m]))`
  trending up** — ClickHouse-side slowness (merges, disk I/O, resource
  contention); the leading indicator for buffer-full backpressure before
  it starts rejecting requests.
- **Spikes in `rate(logstream_ts_clamped_total[5m])`** — a client's clock
  is wrong, or someone is backfilling old data through the live ingest
  path instead of a dedicated backfill path; the data still lands (as
  `now`), just not at its true timestamp.
- **`rate(logstream_query_requests_total{status=~"5.."}[5m]) > 0`** —
  query-side 5xx (ClickHouse or Postgres/Redis auth backend trouble);
  cross-reference with ClickHouse/Postgres health directly.

## 8. Tuning guide

All five ingest tunables trade throughput against latency/memory; the
measured effects below are from `PROGRESS.md`'s bench table (loadgen
co-located with every container on the same host — a real network hop
between loadgen and the ingest service would shift the absolute numbers,
though the relative before/after deltas should hold).

- **`--max-rows`** (default `50000`, raised from an original `5000`):
  bigger batches amortize ClickHouse's per-insert overhead. Measured:
  raising `max_rows` alone (holding `flush_concurrency=1`) took c=8
  throughput from 202,632 to 232,320 rec/s — a real but secondary gain.
  Pushing to `100000` gave a further bump at c=8 (373,251 rec/s) but
  *reintroduced* 27% `429`s at c=64 (larger, chunkier flushes make the
  buffered-rows semaphore burstier) — diminishing/negative returns past
  50000.
- **`--flush-concurrency`** (default `4`): the dominant throughput fix.
  Measured: `flush_concurrency=4` alone (holding `max_rows=5000`) took
  c=8 from 202,632 to 300,911 rec/s and eliminated `429`s entirely at
  c=8 (down from 71%). Combined with the `max_rows=50000` default,
  c=8 reached 362,986 rec/s at 0% `429`. Rationale: a single batcher
  awaiting each ClickHouse insert serially left the buffer idle
  (accepting nothing new) for the insert's entire network round trip;
  running flushes concurrently (bounded by this semaphore) keeps new
  rows accumulating while earlier batches are still being written.
  `flush_concurrency=8` gave a further marginal gain at c=64 (322,597 vs.
  295,679 rec/s) — worth considering if `flush_error`/`buffer_full`
  metrics show sustained pressure at high concurrency, but not a clear
  win over `4` in the measured sweep.
- **`--flush-ms`** (default `200`, unchanged from the original spec):
  this is the **ack-latency bound**, not a throughput knob — it caps how
  long a row can sit in the buffer before a time-triggered flush, not how
  fast the pipeline drains overall. Raising it trades latency for no
  measured throughput gain; lowering it trades smaller/more-frequent
  flushes (worse ClickHouse-side amortization) for tighter latency.
- **`--max-buffered-rows`** (default `1000000`): the total in-flight row
  budget (channel + batcher buffer + everything mid-flush). This is the
  memory/backpressure-latitude knob, not a throughput one — raising it
  lets the system absorb a longer ClickHouse outage before rejecting
  with `429` (see the loss test in §5: 12,577,000 rows were rejected at
  the door once this budget filled during a 15s ClickHouse pause), at
  the cost of proportionally more memory held by buffered `LogRow`s.
- **`--chan-capacity`** (default `1024` **batches**, not rows): the
  batcher's bounded mpsc channel depth. Not directly bench-swept in
  PROGRESS.md (the buffered-rows semaphore is the primary
  backpressure signal at today's defaults), but a channel this shallow
  relative to `--max-buffered-rows`'s row-level budget means the channel
  itself is unlikely to be the first thing to fill under sustained load
  — raise it only if `logstream_dropped_rows_total{reason="channel_full"}`
  specifically (as opposed to `buffer_full`) is what's firing.

Other levers confirmed in place but not independently toggleable via
flag: the `clickhouse` crate's `lz4` compression (already a crate
default; pinned explicitly in `Cargo.toml` for clarity) compresses both
insert bodies and query responses, and was active throughout every bench
run above — no separate before/after measurement exists for it. The
in-process auth TTL cache (§3) isn't a throughput knob at typical
single-key benchmark concurrency either (Redis wasn't the bottleneck),
but removed ~99.7% of Redis `GET` calls in a measured 11,143-request
run at c=32 (38 calls total vs. one per request) — it matters far more
under many distinct tenant keys or a slower/loaded Redis than the
single-key bench captures.

## 9. Troubleshooting

- **ClickHouse auth failures with the default user** — see §2's
  `CLICKHOUSE_SKIP_USER_SETUP` note. Symptom: `logstream-ingest`/
  `logstream-query` logs show a connection/auth error against ClickHouse
  even though `docker exec <ch> clickhouse-client` works fine locally —
  that's the network-vs-local-socket distinction, not a credentials
  typo.
- **ClickHouse healthcheck: `localhost` vs. `127.0.0.1`** — the compose
  healthcheck deliberately targets `http://127.0.0.1:8123/ping`, not
  `localhost`: inside the ClickHouse container, `localhost` resolves
  IPv6-first to `::1`, which nothing listens on (no IPv6 loopback
  configured), so `wget` gets "connection refused" forever even though
  the server is healthy on `127.0.0.1`. If you write a custom healthcheck
  or debug script against this container, use `127.0.0.1` explicitly.
- **Rows missing that you know were ingested** — check the 30-day TTL
  (`TTL toDateTime(ts) + INTERVAL 30 DAY` in `clickhouse/init.sql`)
  before assuming a bug: ClickHouse physically deletes rows older than
  30 days from `ts` in the background. Combined with the timestamp
  clamping in §4, a client backfilling data older than 30 days will have
  every such row's `ts` clamped to `now` on ingest anyway (it can never
  actually land with an old `ts`), so TTL expiry is a concern for rows
  that were genuinely recent *when ingested* and have since aged out —
  not for backfilled history.
- **Grafana "logstream" Loki datasource shows unhealthy** — confirm
  `LOGSTREAM_GRAFANA_API_KEY` matches a key actually seeded via
  `scripts/seed-key.sh` (the compose default is `TESTKEY`); the
  datasource health check exercises the real `/loki/api/v1/query`
  vector-probe path (§6), which requires valid auth like any other route.
  Also confirm `logstream-query` is up and reachable at
  `http://logstream-query:4319` from inside the Grafana container network.
- **`401` vs. `503` from either binary** — `401` means Postgres was
  reached and definitively has no matching, unrevoked key (a real
  auth failure, not a transient outage); `503` means the Postgres
  backend itself is unreachable/erroring, i.e. the service genuinely
  cannot tell whether the key is valid. Clients should treat `503` as
  retryable and `401` as terminal (drop the batch/request) — this
  distinction is why `TenantAuth::lookup` returns `Result<Option<..>, ..>`
  rather than folding both cases into `None`.
- **A dev-only alias gotcha** — not relevant to production deployment;
  skip.
- **Running the end-to-end proof**: `scripts/e2e.sh` brings up the full
  compose stack, seeds `TESTKEY`/`OTHERKEY`, sends real OTLP traffic,
  and checks ingest → ClickHouse → query API → Grafana, tenant isolation,
  and graceful shutdown. Every port is overridable
  (`CH_HTTP_PORT`, `CH_NATIVE_PORT`, `PG_PORT`, `REDIS_PORT`,
  `GRAFANA_PORT`, `PROM_PORT`, `INGEST_PORT`, `QUERY_PORT`) so it can run
  against a second stack under a different `COMPOSE_PROJECT_NAME`
  without colliding with a stack already up on the default ports; set
  `SKIP_UP=1` to reuse an already-running stack, or `TEARDOWN=1` to
  `docker compose down -v` at the end (default: left running, so
  `scripts/bench.sh` can reuse it).
- **Running the bench sweep**: `scripts/bench.sh` assumes `scripts/e2e.sh`
  already brought a stack up and seeded `TESTKEY`; it re-points at that
  stack via `INGEST_PORT`/`CH_HTTP_PORT` and runs `loadgen` across a
  batch-size × concurrency sweep (`BATCHES`, `CONCS`, `DURATION` env vars
  override the defaults `"500 1000"` / `"8 32 64"` / `30`), reconciling
  loadgen's accepted count against ClickHouse's row-count delta after
  each run (polls until the count stabilizes rather than a fixed sleep,
  so a slow drain isn't misreported as loss).
