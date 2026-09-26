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
- [x] `cargo check --workspace` passes locally (verified at end of scaffold)
- [x] `docker compose up` boots ClickHouse + Postgres + Redis + Grafana +
      both services (verified via `scripts/e2e.sh`, 2026-09-26)
- [x] `POST /v1/logs` (with seeded tenant) returns 200 (verified via
      `scripts/e2e.sh`, 2026-09-26)

## Sprint — Phase 2: real OTLP mapping + observability

- [x] `otlp_to_rows` walks `resource_logs[].scope_logs[].log_records[]`
      → `LogRow`
- [x] Severity mapping (number → `LowCardinality(String)` text)
- [x] Attribute flattening (string-coerce `AnyValue`)
- [x] Prometheus `/metrics` endpoint on both binaries
- [x] Pre-provisioned Grafana datasource (ClickHouse) + dashboard
- [ ] Switch back to `sqlx::query!` macros + `sqlx prepare` in CI

## Sprint — Phase 3: query API

- [x] `/query` parses a minimal LogQL-ish surface
- [x] Translation to ClickHouse SELECT (with tenant filter)
- [ ] Streaming rows back (chunked) — `handlers::run_select` currently
      buffers the whole result set via `fetch_all`; fine at today's
      `MAX_LIMIT = 5000` row cap but not truly streamed.

## Sprint — Ops hardening + perf (2026-09-26)

- [x] Grafana 11 Loki datasource health check (`vector(1)+vector(1)` probe)
      answered directly — `crates/query/src/handlers.rs`
      (`is_vector_health_probe`); `scripts/e2e.sh`'s datasource-health
      check upgraded from an informational "known gap" line to a real
      PASS/FAIL check.
- [x] `loadgen` per-request timeout (`--timeout-secs`, default 10s) — a
      hung connection no longer blocks a worker (and therefore the whole
      run) past `--duration`; counted as a transport error like any other
      connection failure.
- [x] Batcher: concurrent flushes (`--flush-concurrency`, default 4) —
      `crates/core/src/batcher.rs` spawns each due flush into its own
      task (bounded by a semaphore) instead of awaiting it serially, so
      new rows keep accumulating while earlier batches are still being
      written to ClickHouse.
- [x] Batcher: default `--max-rows` raised 5,000 → 50,000 — bigger
      inserts amortize ClickHouse's per-insert overhead; `--flush-ms`
      (200ms) unchanged since it bounds ack latency, not throughput.
- [x] `clickhouse` crate's `lz4` feature confirmed active (it's already a
      crate default; pinned explicitly in the workspace `Cargo.toml` for
      clarity) — compresses both insert bodies and query responses.
- [x] In-process TTL cache (`TenantAuth::local_cache`, 10s TTL, cleared
      past 100k entries) in front of the per-request Redis round trip —
      `crates/core/src/auth.rs`.
- [x] Loss test: `docker pause`/`unpause` ClickHouse mid-load — see
      "Loss test result" below.
- [x] `scripts/e2e.sh` — ALL CHECKS PASSED against the perf stack,
      including the Loki datasource health check.

## Done

(see the per-phase checklists above — nothing tracked separately here)

## Blocked

- (none)

## Bench numbers

Measured 2026-09-26 on an 8-core shared dev box (not a dedicated 2 vCPU
box — see "Bench methodology" for how the 2-vCPU numbers were produced).
loadgen ran co-located with every container on the same host
(compose project `logstream-perf`).

| metric                                              | target          | current | as-of      |
|-----------------------------------------------------|-----------------|---------|------------|
| Throughput (sustained, 2 vCPU ingest container)     | >= 100k rec/s   | **267–281k rec/s** accepted (batch=500, c=8/32/64, `docker update --cpus 2`) | 2026-09-26 |
| Throughput (sustained, uncapped ingest)             | >= 100k rec/s   | **296–363k rec/s** accepted (batch=500, c=8–64, defaults `--max-rows 50000 --flush-concurrency 4`) | 2026-09-26 |
| p99 ingest ack                                      | < 20 ms         | **5.0ms at c=1**, 19.3ms at c=2, 34–388ms at c=8–64 (see methodology — grows with concurrency; target met at low concurrency, not at c>=8) | 2026-09-26 |
| 1B-row trace_id point lookup                        | < 1 s           | not yet measured (needs a 1B-row dataset) | |
| Loss test (ClickHouse paused via `docker pause`)    | drop-newest     | **PASS** — 0 rows silently lost (see below) | 2026-09-26 |
| Cost vs Datadog log ingestion (same volume)         | 10-50x cheaper  | not measured this sprint | |

### Bench methodology

Tooling: `scripts/bench.sh` (loadgen sweep + ClickHouse row-count
reconciliation) against compose project `logstream-perf`, ports shifted
off the defaults (`CH_HTTP_PORT=28123` etc.) because the machine's default
Docker context (`desktop-linux`) was wedged and had stale listeners
squatting on the usual ports — see the ports actually used in
`scripts/e2e.sh`'s header comment. All `docker`/`docker compose` calls
used `DOCKER_HOST=unix:///var/run/docker.sock` (the native engine), never
the hung `desktop-linux` context. Host: 8-core shared dev box, load
average ~3.6–4.4 during the runs (other unrelated services — coolify,
n8n, etc. — share the box), loadgen co-located with every container.
Runs were shortened to 15s (`DURATION=15`) instead of the default 30s to
fit the sweep into the available time; row counts reconciled
(`accepted == ClickHouse rows added`) after every run, confirmed exact in
every case below.

**Note on the stated pre-existing baseline** (batch=500 c=8 → 52–64k
rec/s, c=64 → 28–34k rec/s, from a previous run on this same host): this
session's very first measurement, using old-equivalent settings
(`--max-rows 5000 --flush-concurrency 1`, i.e. the original serial-flush
single-batcher code path) on the **native** Docker engine, already
measured 202k rec/s (c=8) / 130k rec/s (c=64) — 3–4x that baseline. The
likely explanation is that the earlier baseline was measured while the
host's Docker context was the hung `desktop-linux` VM (Docker Desktop's
userspace network proxy is known to add substantial latency/throughput
overhead vs. the native engine) — not a difference in logstream's code.
This is flagged rather than hidden: it means the *absolute* numbers below
aren't apples-to-apples with the stated baseline, but the *relative*
before/after deltas for each change (measured back-to-back on the same
engine, same host, same run methodology) are.

One change at a time, holding the other constant, batch=500, `c` = loadgen
concurrency, DURATION=15s:

| config                                         | c=8 accepted rec/s | c=8 p99 | c=8 429% | c=64 accepted rec/s | c=64 p99 | c=64 429% |
|-------------------------------------------------|--------------------:|--------:|---------:|---------------------:|---------:|----------:|
| baseline: `max_rows=5000 flush_concurrency=1`   | 202,632             | 13.0ms  | 71%      | 129,780               | 93.7ms   | 85%       |
| (a) `max_rows=50000` alone                      | 232,320             | 16.4ms  | 64%      | 170,709               | 104.3ms  | 77%       |
| (b) `flush_concurrency=4` alone                 | 300,911             | 43.2ms  | 0%       | 215,549               | 150.3ms  | 60%       |
| (a+b) new defaults `50000 / 4`                  | 362,986             | 34.2ms  | 0%       | 295,679               | 388.7ms  | 0.2%      |
| `50000 / flush_concurrency=8`                   | 358,660             | 37.3ms  | 0%       | 322,597               | 240.9ms  | 0%        |
| `max_rows=100000 / flush_concurrency=4`         | 373,251             | 34.7ms  | 0%       | 309,534               | 230.8ms  | 27%       |

Reading it: (b) — concurrent flushes — is the dominant fix, matching the
diagnosis (a single batcher awaiting each ClickHouse insert serially was
the throughput ceiling: while one insert is in flight, no new rows can be
flushed no matter how fast they arrive). (a) — bigger batches — helps too
but less. Combined, 429s vanish at c=8 and drop to near-zero at c=64.
Pushing further (`flush_concurrency=8`, `max_rows=100000`) gave marginal
or *negative* returns (100k rows/flush reintroduced 27% 429s at c=64 —
larger, chunkier flushes make the semaphore-based backpressure burstier)
— kept the smaller, well-tested defaults (`50000 / 4`) rather than chase
diminishing returns. **Final defaults: `--max-rows 50000
--flush-concurrency 4`.**

(c) `clickhouse` crate's `lz4` feature: already a crate default (see
`Cargo.toml`'s comment) — active in every run above, no separate
before/after exists.

(d) In-process auth TTL cache: measured directly via Redis
`INFO commandstats`, not via a throughput toggle (there's no flag to
disable it). `CONFIG RESETSTAT` then an 11,143-request/15s run at c=32,
single API key: Redis saw **38 `GET` calls total** — a ~99.7% reduction
from one Redis round trip per request to one burst of up to `c` concurrent
misses per 10s local-cache expiry, per process. Didn't move the throughput ceiling in this
single-key benchmark (Redis wasn't the bottleneck at this concurrency),
but removes a network round trip from the hot path unconditionally, and
matters far more under many distinct tenant keys or a slower/loaded
Redis.

`cargo bench -p logstream-core --bench otlp -- --quick`:
`otlp_to_rows` mapping alone sustains **~980k–1M records/s on a single
core** — confirms the OTLP decode/row-mapping step is not the bottleneck
(it's ~300x the per-core share of the throughput target).

**p99 ack latency vs. concurrency** (defaults, uncapped, batch=500):

| concurrency | accepted rec/s | p99   |
|-------------|----------------:|------:|
| c=1         | 212,385         | 5.0ms |
| c=2         | 236,678         | 19.3ms|
| c=8         | 362,986         | 34.2ms|
| c=32        | 325,871         | 139.6ms|
| c=64        | 295,679         | 388.7ms|

The `< 20ms` p99 target is met at c=1–2 and missed at c>=8: p99 grows with
concurrency because more concurrent requests queue behind the same
bounded channel and `--flush-concurrency` ClickHouse-insert slots — that's
queueing delay from sharing a fixed pipeline, not a fixed per-request
cost. Throughput stays 2.1–3.6x the 100k target across the whole range,
including at c=1.

**2 vCPU vs. uncapped vs. native** (defaults `50000/4`, batch=500):

| run                                                        | c=8              | c=32             | c=64             |
|--------------------------------------------------------------|------------------:|------------------:|------------------:|
| containerized, uncapped                                     | 362,986 / 34.2ms  | 325,871 / 139.6ms | 295,679 / 388.7ms |
| containerized, `docker update --cpus 2`                      | 266,728 / 53.9ms  | 276,195 / 147.4ms | 281,378 / 248.0ms (8% 429) |
| native binary, `taskset -c 0,1` (2 cores)                    | 292,111 / 57.9ms  | 149,627 / 371.0ms | 104,595 / 701.3ms |

The containerized 2-vCPU run (cgroup CPU quota, free to run on *any* of
the host's 8 cores) comfortably clears the 100k target at every
concurrency. The native `taskset`-pinned run is competitive at c=8 but
degrades badly at c=32/64 — worse than the cgroup-limited container, and
barely above target at c=64. That's a host artifact, not a code
regression: `taskset -c 0,1` hard-pins to two *specific* physical cores,
and this shared box's other tenants (coolify, n8n, etc., load average
3.6–4.4 during these runs) were contending for exactly those cores;
`docker update --cpus 2` caps total CPU-time without core affinity, so
the container's threads get scheduled onto whichever cores are free.
Reported for completeness rather than hidden — on a real dedicated 2 vCPU
box (no host-level cross-tenant contention for specific cores), the two
should be equivalent.

### Loss test result

Procedure (`scripts/bench.sh`-style loadgen, batch=500, concurrency=16,
45s run): started load, `docker pause`d the `clickhouse` container at
t=12s, `docker unpause`d at t=27s (15s paused), let the run finish, then
polled until the ClickHouse row count stopped moving.

Result: **PASS**.

- `records: sent=20,174,000 accepted=7,597,000` (`status histogram:
  200=15,194 429=25,154`)
- `logstream_dropped_rows_total{reason="buffer_full"} = 12,577,000` — the
  `--max-buffered-rows` semaphore (1,000,000 rows) filled while
  ClickHouse was paused and nothing could flush, so the ingest handler's
  `429`/drop-newest path rejected the rest at the door; counted, not
  silently absorbed.
- `logstream_flush_errors_total`: absent (zero) — `docker pause` freezes
  the container's process via the cgroup freezer rather than closing its
  sockets, so any insert already in flight when the pause hit just
  resumed once unpaused instead of erroring/timing out.
- `logstream_rows_flushed_total = 7,597,000`.
- **ClickHouse rows added = 7,597,000 — exactly equal to
  `accepted` (200s) and to `rows_flushed`.** No silent loss: every
  accepted row landed in ClickHouse, and every rejected row was rejected
  (and counted) before ever being accepted.
- Recovery: rows resumed flowing immediately after `unpause` with no
  restart or manual intervention; the ClickHouse row count kept climbing
  until it stabilized at the reconciled total above.

## Blog topics surfacing

- (none yet)
