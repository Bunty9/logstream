//! Per-process batcher actor. Receives `TenantBatch` over a bounded mpsc
//! channel and writes to ClickHouse either when the in-memory buffer
//! reaches `max_rows` or every `flush_ms` milliseconds, whichever fires
//! first.
//!
//! Backpressure is intentionally drop-newest at the ingest boundary
//! (see `crates/ingest/src/ingest.rs`); the batcher itself never blocks
//! recv() because the channel does that for us. Total in-flight rows
//! (channel + buffer + any flush still in flight) are additionally
//! bounded by the `--max-buffered-rows` semaphore permit each
//! `TenantBatch` carries — see that permit's handling in the loop below.
//!
//! A flush failure is non-fatal: it's logged, counted, and the buffered
//! rows are dropped so the batcher keeps running — one bad flush must not
//! 429 every subsequent request for the rest of the process's life (the
//! old behavior: `flush(...).await?` inside `run_batcher`'s loop propagated
//! the error out of the loop entirely, ending the actor and leaving the
//! `mpsc::Sender` with no reader).
//!
//! Flushes run concurrently, up to `--flush-concurrency`: a full ClickHouse
//! insert (network round trip + server-side merge work) is slow enough
//! relative to row accumulation that awaiting each flush serially — the
//! original design — left the buffer sitting idle (accepting nothing new
//! into the *current* batch, since the recv loop was blocked on `.await`)
//! for most of the flush's duration, capping throughput at
//! `max_rows / flush_latency` regardless of how fast rows arrive. Each
//! flush that's due now takes the buffer (`mem::take`) and its permits,
//! and runs in its own task so the recv loop immediately starts filling a
//! fresh buffer instead of waiting; `flush_limit` bounds how many of those
//! run against ClickHouse at once (unbounded concurrency would just move
//! the bottleneck to ClickHouse-side connection/merge contention).

use crate::types::{LogRow, TenantBatch};
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;
use tokio::time::{interval, sleep, Duration, MissedTickBehavior};

/// Bounded retry for a transient flush failure. Kept small and simple:
/// retrying an insert that failed *after* ClickHouse durably wrote part of
/// the batch can duplicate rows (no idempotency key on this path), so more
/// attempts / longer backoff would just trade "rows lost" for "rows
/// duplicated" without fixing either — 3 tries with a short fixed backoff
/// is a reasonable default for "the server hiccuped", not for "the server
/// is down".
const MAX_FLUSH_ATTEMPTS: u32 = 3;
const RETRY_BACKOFF: Duration = Duration::from_millis(50);

/// Drain a bounded `TenantBatch` channel into ClickHouse.
///
/// `max_rows` is the size-based flush threshold; the spec calls for 5,000
/// (the default is higher — see `crates/ingest/src/main.rs` — ClickHouse
/// amortizes its per-insert overhead better over bigger batches).
/// `flush_ms` is the time-based threshold; 200ms keeps tail latency tight
/// while still amortizing the ClickHouse round-trip across thousands of
/// rows. `flush_concurrency` bounds how many flushes run against
/// ClickHouse at once — see the module doc for why flushes aren't awaited
/// serially.
///
/// Returns only when the channel closes (sender side dropped) *and* every
/// flush spawned along the way has completed — draining `inflight` at the
/// end means a graceful shutdown still waits for in-flight ClickHouse
/// writes before this returns, same guarantee the old serial-await version
/// had.
pub async fn run_batcher(
    mut rx: Receiver<TenantBatch>,
    ch: clickhouse::Client,
    max_rows: usize,
    flush_ms: u64,
    flush_concurrency: usize,
) -> anyhow::Result<()> {
    let mut buf: Vec<LogRow> = Vec::with_capacity(max_rows * 2);
    // Accumulated `--max-buffered-rows` permits covering every row
    // currently in `buf`. Each incoming `TenantBatch`'s permit is merged
    // into this one (`OwnedSemaphorePermit::merge`, cheap — just adds
    // permit counts), and it's handed off (via `spawn_flush`) to whichever
    // flush task drains `buf`, released only once that flush attempt
    // (success or dropped-after-retries) finishes — either way the rows
    // it covered are no longer buffered anywhere at that point.
    let mut permit: Option<OwnedSemaphorePermit> = None;
    let mut tick = interval(Duration::from_millis(flush_ms));
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let flush_limit = Arc::new(Semaphore::new(flush_concurrency.max(1)));
    let mut inflight: JoinSet<()> = JoinSet::new();

    loop {
        tokio::select! {
            maybe_batch = rx.recv() => {
                match maybe_batch {
                    Some(batch) => {
                        buf.extend(batch.rows);
                        match &mut permit {
                            Some(p) => p.merge(batch.permit),
                            None => permit = Some(batch.permit),
                        }
                        if buf.len() >= max_rows {
                            spawn_flush(&ch, &mut buf, max_rows, &mut permit, &flush_limit, &mut inflight);
                        }
                    }
                    None => break, // sender side dropped — shutdown
                }
            }
            _ = tick.tick() => {
                // With every flush slot busy, let the buffer keep growing
                // toward `max_rows` instead of queueing many tiny inserts.
                if !buf.is_empty() && flush_limit.available_permits() > 0 {
                    spawn_flush(&ch, &mut buf, max_rows, &mut permit, &flush_limit, &mut inflight);
                }
            }
            // Reap completed flush tasks as they finish so `inflight`
            // doesn't grow without bound and a panicking flush task is
            // at least logged instead of silently vanishing.
            Some(res) = inflight.join_next(), if !inflight.is_empty() => {
                log_panicked_flush(res, "flush task panicked");
            }
        }
    }
    if !buf.is_empty() {
        spawn_flush(
            &ch,
            &mut buf,
            max_rows,
            &mut permit,
            &flush_limit,
            &mut inflight,
        );
    }
    // Drain every in-flight flush before returning, so shutdown never
    // drops rows that were already handed to a flush task.
    while let Some(res) = inflight.join_next().await {
        log_panicked_flush(res, "flush task panicked during shutdown drain");
    }
    Ok(())
}

/// Log a flush task that panicked. Its rows are counted as dropped by
/// the task's own `PanicDropGuard` during unwinding (see `spawn_flush`).
fn log_panicked_flush(res: Result<(), tokio::task::JoinError>, msg: &'static str) {
    if let Err(err) = res {
        tracing::error!(%err, "{msg}");
    }
}

/// Take the current buffer + its accumulated permit and hand them to a
/// spawned flush task, bounded by `flush_limit`. Leaves `buf` empty (fresh
/// capacity) and `permit` `None` so the caller's loop immediately starts
/// accumulating the next batch instead of waiting on this flush.
fn spawn_flush(
    ch: &clickhouse::Client,
    buf: &mut Vec<LogRow>,
    max_rows: usize,
    permit: &mut Option<OwnedSemaphorePermit>,
    flush_limit: &Arc<Semaphore>,
    inflight: &mut JoinSet<()>,
) {
    let taken = std::mem::replace(buf, Vec::with_capacity(max_rows * 2));
    // Only ever `None` if `buf` was empty, and both call sites already
    // check that before calling `spawn_flush`.
    let Some(taken_permit) = permit.take() else {
        return;
    };
    let ch = ch.clone();
    let flush_limit = flush_limit.clone();
    inflight.spawn(async move {
        // Acquired inside the task, not before spawning: that way the
        // recv loop above never blocks waiting for a flush slot, it just
        // keeps accumulating the next buffer. Total buffered rows
        // (including every flush queued here waiting on this semaphore)
        // stay bounded regardless, via `taken_permit` below and the
        // `--max-buffered-rows` semaphore it comes from.
        let _flush_permit = flush_limit
            .acquire_owned()
            .await
            .expect("flush_limit semaphore is never closed");
        let mut taken = taken;
        let guard = PanicDropGuard(taken.len() as u64);
        flush_with_retry(&ch, &mut taken).await;
        std::mem::forget(guard);
        // Release the buffered-rows budget only now — after the flush
        // attempt (success or dropped-after-retries) has fully finished.
        drop(taken_permit);
    });
}

/// `flush` wrapped with a bounded retry and failure metrics; never
/// propagates an error — on exhausted retries the buffered rows are
/// dropped (and counted) so the actor loop keeps going.
async fn flush_with_retry(ch: &clickhouse::Client, buf: &mut Vec<LogRow>) {
    for attempt in 1..=MAX_FLUSH_ATTEMPTS {
        match flush(ch, buf).await {
            Ok(()) => return,
            Err(err) if attempt < MAX_FLUSH_ATTEMPTS => {
                tracing::warn!(%err, attempt, "clickhouse flush failed, retrying");
                sleep(RETRY_BACKOFF).await;
            }
            Err(err) => {
                let dropped = buf.len() as u64;
                tracing::error!(%err, attempts = MAX_FLUSH_ATTEMPTS, dropped, "clickhouse flush failed, dropping buffered rows");
                metrics::counter!("logstream_flush_errors_total").increment(1);
                metrics::counter!("logstream_dropped_rows_total", "reason" => "flush_error")
                    .increment(dropped);
                buf.clear();
            }
        }
    }
}

/// Drain `buf` into a single ClickHouse insert. Public only so integration
/// tests (`crates/ingest/tests/clickhouse_it.rs`) can exercise one flush
/// directly against a real ClickHouse server without driving the full
/// `run_batcher` actor loop — nothing in the ingest binary calls this
/// directly; `run_batcher` (via `flush_with_retry`) is the only production
/// caller.
///
/// Note: if `insert.end()` fails after some rows were already accepted by
/// ClickHouse (e.g. the connection dropped mid-write), a caller that
/// retries this same `buf` can duplicate those rows — there's no
/// idempotency key on this path, so retries trade "at most once" for "at
/// least once".
pub async fn flush(ch: &clickhouse::Client, buf: &mut Vec<LogRow>) -> anyhow::Result<()> {
    let started = std::time::Instant::now();
    let rows = buf.len() as u64;
    let mut insert = ch.insert("logs")?;
    for row in buf.iter() {
        insert.write(row).await?;
    }
    insert.end().await?;
    buf.clear();
    metrics::histogram!("logstream_flush_duration_seconds").record(started.elapsed().as_secs_f64());
    metrics::counter!("logstream_rows_flushed_total").increment(rows);
    Ok(())
}

/// Counts a flush task's rows as dropped if the task unwinds (panics)
/// before `flush_with_retry` returns; forgotten on the normal path, where
/// `flush_with_retry` has already accounted for every row.
struct PanicDropGuard(u64);

impl Drop for PanicDropGuard {
    fn drop(&mut self) {
        metrics::counter!("logstream_dropped_rows_total", "reason" => "flush_panic")
            .increment(self.0);
    }
}
