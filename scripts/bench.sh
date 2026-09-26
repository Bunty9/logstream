#!/usr/bin/env bash
# Throughput/latency sweep against a running logstream-ingest container
# (scripts/e2e.sh must have brought the stack up and seeded TESTKEY first).
# Runs loadgen at several concurrencies for ~30s each at batch 500 and
# 1000, then reports the sustained accepted rec/s + p99 loadgen printed,
# plus the ClickHouse row count delta so silent loss is caught even if
# loadgen's own "accepted" counter and the DB disagree.
#
# Usage: INGEST_PORT=14318 CH_HTTP_PORT=18123 scripts/bench.sh

set -uo pipefail

export INGEST_PORT="${INGEST_PORT:-4318}"
export CH_HTTP_PORT="${CH_HTTP_PORT:-8123}"
export PATH="$HOME/.cargo/bin:$PATH"
DURATION="${DURATION:-30}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

INGEST_URL="http://localhost:${INGEST_PORT}"
CH_URL="http://localhost:${CH_HTTP_PORT}"
LOADGEN="target/release/examples/loadgen"

cargo build --release -p logstream-ingest --example loadgen 2>&1 | tail -20
[[ -x "$LOADGEN" ]] || {
    echo "loadgen binary missing"
    exit 1
}

ch_count() {
    curl -sS "${CH_URL}/" --data-binary "SELECT count() FROM logs WHERE tenant_id='demo' FORMAT TSV" | tr -d '[:space:]'
}

for batch in 500 1000; do
    for conc in 8 32 64; do
        echo
        echo "== batch=$batch concurrency=$conc duration=${DURATION}s =="
        before=$(ch_count)
        "$LOADGEN" --url "${INGEST_URL}/v1/logs" --key TESTKEY \
            --duration "$DURATION" --batch "$batch" --concurrency "$conc" \
            --service bench
        # Drain wait: "accepted" (try_send Ok) only means a row is queued,
        # not flushed — at high concurrency the bounded channel (default
        # capacity 1024 batches) can be holding hundreds of thousands of
        # rows when loadgen stops. Poll until the count stops moving
        # (stable across two consecutive 1s samples) instead of a fixed
        # sleep, so a real drop isn't masked as "still draining" and a
        # slow drain isn't misreported as loss.
        prev=-1
        for _ in $(seq 1 60); do
            cur=$(ch_count)
            [[ "$cur" == "$prev" ]] && break
            prev="$cur"
            sleep 1
        done
        after=$(ch_count)
        echo "clickhouse rows added: $((after - before))"
    done
done
