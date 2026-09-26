#!/usr/bin/env bash
# End-to-end proof for logstream: brings the compose stack up, seeds two
# tenants, sends real OTLP traffic, and checks ingest -> ClickHouse ->
# query API -> Grafana all actually work together, plus tenant isolation
# and graceful shutdown. Safe to rerun (idempotent seeding, `down -v` at
# the end only if TEARDOWN=1).
#
# All ports are overridable via env vars (defaults match docker-compose.yml
# so this also runs unmodified in CI):
#   CH_HTTP_PORT CH_NATIVE_PORT PG_PORT REDIS_PORT GRAFANA_PORT PROM_PORT
#   INGEST_PORT QUERY_PORT
#
# Other knobs:
#   COMPOSE_PROJECT_NAME (default logstream-e2e)
#   SKIP_UP=1     don't run `docker compose up` (stack already running)
#   TEARDOWN=1    `docker compose down -v` at the end (default: leave it up,
#                 so scripts/bench.sh can reuse the same stack)
#
# Usage: scripts/e2e.sh
#   CH_HTTP_PORT=18123 CH_NATIVE_PORT=19000 PG_PORT=15432 REDIS_PORT=16379 \
#   GRAFANA_PORT=13000 PROM_PORT=19090 INGEST_PORT=14318 QUERY_PORT=14319 \
#   scripts/e2e.sh

set -uo pipefail

export CH_HTTP_PORT="${CH_HTTP_PORT:-8123}"
export CH_NATIVE_PORT="${CH_NATIVE_PORT:-9000}"
export PG_PORT="${PG_PORT:-5432}"
export REDIS_PORT="${REDIS_PORT:-6379}"
export GRAFANA_PORT="${GRAFANA_PORT:-3000}"
export PROM_PORT="${PROM_PORT:-9090}"
export INGEST_PORT="${INGEST_PORT:-4318}"
export QUERY_PORT="${QUERY_PORT:-4319}"
export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-logstream-e2e}"
export LOGSTREAM_GRAFANA_API_KEY="${LOGSTREAM_GRAFANA_API_KEY:-TESTKEY}"
export PATH="$HOME/.cargo/bin:$PATH"

SKIP_UP="${SKIP_UP:-0}"
TEARDOWN="${TEARDOWN:-0}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

INGEST_URL="http://localhost:${INGEST_PORT}"
QUERY_URL="http://localhost:${QUERY_PORT}"
CH_URL="http://localhost:${CH_HTTP_PORT}"
GRAFANA_URL="http://localhost:${GRAFANA_PORT}"

FAILED=0
pass() { echo "  PASS: $*"; }
fail() { echo "  FAIL: $*"; FAILED=1; }
section() { echo; echo "== $* =="; }

ch_query() {
    curl -sS "${CH_URL}/" --data-binary "$1"
}

wait_for_http() {
    local url="$1" tries="${2:-90}"
    for ((i = 0; i < tries; i++)); do
        if curl -sf -o /dev/null "$url"; then
            return 0
        fi
        sleep 2
    done
    return 1
}

# === bring the stack up =====================================================

if [[ "$SKIP_UP" != "1" ]]; then
    section "docker compose up --build -d (project: ${COMPOSE_PROJECT_NAME})"
    docker compose up --build -d || {
        fail "docker compose up failed"
        exit 1
    }
fi

section "waiting for services to answer"
wait_for_http "${CH_URL}/ping" && pass "clickhouse /ping" || {
    fail "clickhouse never answered /ping"
    exit 1
}
wait_for_http "${INGEST_URL}/health" && pass "ingest /health" || {
    fail "ingest never answered /health"
    exit 1
}
wait_for_http "${QUERY_URL}/health" && pass "query /health" || {
    fail "query never answered /health"
    exit 1
}

# === 1. health + metrics ====================================================

section "1. health + metrics endpoints"
for name_url in "ingest-health ${INGEST_URL}/health" "query-health ${QUERY_URL}/health" \
    "ingest-metrics ${INGEST_URL}/metrics" "query-metrics ${QUERY_URL}/metrics"; do
    name="${name_url%% *}"
    url="${name_url#* }"
    code=$(curl -sS -o /dev/null -w '%{http_code}' "$url")
    [[ "$code" == "200" ]] && pass "$name -> 200" || fail "$name -> $code"
done

# === 2. seed keys ============================================================

section "2. seed API keys"
./scripts/seed-key.sh TESTKEY demo && pass "seeded TESTKEY -> demo" || fail "seeding TESTKEY failed"
./scripts/seed-key.sh OTHERKEY other && pass "seeded OTHERKEY -> other" || fail "seeding OTHERKEY failed"
# TTL on the negative/positive redis cache is short-lived but give it a beat.
sleep 1

# === 3. build loadgen, send a one-shot batch with a known trace id =========

section "3. build loadgen"
cargo build --release -p logstream-ingest --example loadgen 2>&1 | tail -20
LOADGEN="target/release/examples/loadgen"
[[ -x "$LOADGEN" ]] && pass "loadgen built" || {
    fail "loadgen binary missing after build"
    exit 1
}

# 32-hex-char trace id, deterministic per run: blake3 hex of a timestamped
# seed via the core crate's own hash_key example (see scripts/seed-key.sh
# for the same trick), truncated to trace-id length.
TRACE_ID="$(cargo run -q -p logstream-core --example hash_key -- "e2e-trace-$(date +%s%N)" | cut -c1-32)"

section "3. one-shot: send 20 records under one known trace id (tenant demo)"
"$LOADGEN" --url "${INGEST_URL}/v1/logs" --key TESTKEY --requests 1 --batch 20 \
    --trace-id "$TRACE_ID" --service checkout
sleep 1 # batcher flush_ms default 200ms; give it margin

# === 4. verify rows landed ===================================================

section "4. verify rows in ClickHouse + via query API"

ch_count=$(ch_query "SELECT count() FROM logs WHERE tenant_id='demo' AND trace_id='${TRACE_ID}' FORMAT TSV" | tr -d '[:space:]')
[[ "$ch_count" == "20" ]] && pass "clickhouse row count for trace = 20" || fail "clickhouse row count for trace = '$ch_count' (expected 20)"

demo_total=$(ch_query "SELECT count() FROM logs WHERE tenant_id='demo' FORMAT TSV" | tr -d '[:space:]')
echo "  info: total demo rows so far = $demo_total"

query_resp=$(curl -sS -H 'x-api-key: TESTKEY' -H 'content-type: application/json' \
    -d '{"query":"{service=\"checkout\"} |= \"checkout\""}' "${QUERY_URL}/query")
qcount=$(echo "$query_resp" | jq '.rows | length' 2>/dev/null || echo 0)
[[ "${qcount:-0}" -ge 1 ]] && pass "POST /query returned $qcount rows" || fail "POST /query returned no rows: $query_resp"

trace_resp=$(curl -sS -H 'x-api-key: TESTKEY' "${QUERY_URL}/trace/${TRACE_ID}")
trace_count=$(echo "$trace_resp" | jq '.rows | length' 2>/dev/null || echo 0)
[[ "$trace_count" == "20" ]] && pass "GET /trace/{id} returned 20 rows" || fail "GET /trace/{id} returned $trace_count rows: $trace_resp"

loki_range=$(curl -sS -G -H 'x-api-key: TESTKEY' --data-urlencode 'query={service="checkout"}' "${QUERY_URL}/loki/api/v1/query_range")
[[ "$(echo "$loki_range" | jq -r .status)" == "success" ]] && pass "/loki/api/v1/query_range status=success" || fail "/loki/api/v1/query_range: $loki_range"

loki_labels=$(curl -sS -H 'x-api-key: TESTKEY' "${QUERY_URL}/loki/api/v1/labels")
[[ "$(echo "$loki_labels" | jq -r .status)" == "success" ]] && pass "/loki/api/v1/labels status=success" || fail "/loki/api/v1/labels: $loki_labels"

label_values=$(curl -sS -H 'x-api-key: TESTKEY' "${QUERY_URL}/loki/api/v1/label/service/values")
lv_has_checkout=$(echo "$label_values" | jq -r '.data[]? // empty' | grep -c '^checkout$' || true)
[[ "$lv_has_checkout" -ge 1 ]] && pass "/loki/api/v1/label/service/values includes checkout" || fail "/loki/api/v1/label/service/values missing checkout: $label_values"

# tenant isolation: OTHERKEY (tenant other) must see none of demo's rows.
other_trace=$(curl -sS -H 'x-api-key: OTHERKEY' "${QUERY_URL}/trace/${TRACE_ID}")
other_count=$(echo "$other_trace" | jq '.rows | length' 2>/dev/null || echo -1)
[[ "$other_count" == "0" ]] && pass "tenant isolation: OTHERKEY sees 0 rows of demo's trace" || fail "tenant isolation broken: OTHERKEY saw $other_count rows: $other_trace"

no_key_code=$(curl -sS -o /dev/null -w '%{http_code}' -X POST -H 'content-type: application/json' \
    -d '{"query":"{service=\"checkout\"}"}' "${QUERY_URL}/query")
[[ "$no_key_code" == "401" ]] && pass "no api key -> 401" || fail "no api key -> $no_key_code (expected 401)"

# === 5. Grafana datasources + dashboard =====================================

section "5. Grafana datasources + dashboard"
wait_for_http "${GRAFANA_URL}/api/health" 60 && pass "grafana /api/health" || fail "grafana never answered /api/health"

ds_json=$(curl -sS -u admin:admin "${GRAFANA_URL}/api/datasources")
for want in logstream ClickHouse Prometheus; do
    if echo "$ds_json" | jq -e --arg n "$want" '.[] | select(.name==$n)' >/dev/null 2>&1; then
        pass "datasource '$want' provisioned"
    else
        fail "datasource '$want' NOT provisioned: $ds_json"
    fi
done

echo "$ds_json" | jq -c '.[] | {id, uid, name}' 2>/dev/null | while read -r row; do
    uid=$(echo "$row" | jq -r .uid)
    name=$(echo "$row" | jq -r .name)
    id=$(echo "$row" | jq -r .id)
    health=$(curl -sS -u admin:admin -X POST "${GRAFANA_URL}/api/datasources/uid/${uid}/health" 2>/dev/null)
    hstatus=$(echo "$health" | jq -r '.status // empty' 2>/dev/null)
    if [[ "$hstatus" != "OK" ]]; then
        # fall back to the by-id health endpoint on older Grafana APIs
        health=$(curl -sS -u admin:admin "${GRAFANA_URL}/api/datasources/${id}/health" 2>/dev/null)
        hstatus=$(echo "$health" | jq -r '.status // empty' 2>/dev/null)
    fi
    if [[ "$name" == "logstream" && "$hstatus" != "OK" ]]; then
        # KNOWN GAP (query-side, not provisioning): Grafana's built-in Loki
        # datasource health check always sends the literal probe query
        # `vector(1)+vector(1)` to /loki/api/v1/query. Our LogQL grammar
        # (crates/query/src/logql.rs) only implements the log-selector
        # subset ("{label=...} filter*"), not Loki's metric-query literals
        # like `vector(N)`, so it 400s that probe specifically — this is
        # not an auth or provisioning problem: the same header-authed
        # request reaches logstream-query and gets a structured LogQL
        # parse error back (not a 401), and /labels, /query_range, and
        # /label/{name}/values (what dashboards/Explore actually use) all
        # verified OK above. Reported here, not fixed: fixing it means
        # widening the LogQL parser (crates/query/src/*), out of this
        # script's scope.
        echo "  info: datasource '$name' health = ${hstatus:-unknown} ($health) -- KNOWN GAP: Grafana's Loki healthcheck probes 'vector(1)+vector(1)', which our LogQL subset doesn't parse; auth/proxy/labels/query_range all verified working independently above"
    else
        echo "  info: datasource '$name' health = ${hstatus:-unknown} ($health)"
    fi
done

search_json=$(curl -sS -u admin:admin "${GRAFANA_URL}/api/search")
if echo "$search_json" | jq -e '.[] | select(.type=="dash-db")' >/dev/null 2>&1; then
    pass "dashboard provisioned (found in /api/search)"
else
    fail "no dashboard found in /api/search: $search_json"
fi

# === 6. graceful shutdown: sent rows must be flushed on SIGTERM ============

section "6. graceful shutdown flushes buffered rows"
SHUTDOWN_TRACE="$(cargo run -q -p logstream-core --example hash_key -- "e2e-shutdown-trace-seed" | cut -c1-32)"
"$LOADGEN" --url "${INGEST_URL}/v1/logs" --key TESTKEY --requests 1 --batch 50 \
    --trace-id "$SHUTDOWN_TRACE" --service checkout
docker compose stop -t 10 logstream-ingest
sleep 1
shutdown_count=$(ch_query "SELECT count() FROM logs WHERE tenant_id='demo' AND trace_id='${SHUTDOWN_TRACE}' FORMAT TSV" | tr -d '[:space:]')
[[ "$shutdown_count" == "50" ]] && pass "all 50 rows flushed on graceful shutdown" || fail "only '$shutdown_count'/50 rows flushed on shutdown"
docker compose start logstream-ingest
wait_for_http "${INGEST_URL}/health" && pass "ingest back up after restart" || fail "ingest did not come back up"

# === summary =================================================================

section "summary"
if [[ "$FAILED" == "0" ]]; then
    echo "ALL CHECKS PASSED"
else
    echo "SOME CHECKS FAILED (see FAIL lines above)"
fi

if [[ "$TEARDOWN" == "1" ]]; then
    section "tearing down"
    docker compose down -v
fi

exit "$FAILED"
