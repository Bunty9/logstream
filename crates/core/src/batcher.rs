//! Per-process batcher actor. Receives `TenantBatch` over a bounded mpsc
//! channel and writes to ClickHouse either when the in-memory buffer
//! reaches `max_rows` or every `flush_ms` milliseconds, whichever fires
//! first.
//!
//! Backpressure is intentionally drop-newest at the ingest boundary
//! (see `crates/ingest/src/ingest.rs`); the batcher itself never blocks
//! recv() because the channel does that for us.
//!
//! A flush failure is non-fatal: it's logged, counted, and the buffered
//! rows are dropped so the batcher keeps running — one bad flush must not
//! 429 every subsequent request for the rest of the process's life (the
//! old behavior: `flush(...).await?` inside `run_batcher`'s loop propagated
//! the error out of the loop entirely, ending the actor and leaving the
//! `mpsc::Sender` with no reader).

use crate::types::{LogRow, TenantBatch};
use tokio::sync::mpsc::Receiver;
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
/// `max_rows` is the size-based flush threshold; the spec calls for 5,000.
/// `flush_ms` is the time-based threshold; 200ms keeps tail latency tight
/// while still amortizing the ClickHouse round-trip across thousands of
/// rows.
///
/// Returns only when the channel closes (sender side dropped), i.e. on
/// intentional shutdown — never as a side effect of a flush error.
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
            maybe_batch = rx.recv() => {
                match maybe_batch {
                    Some(batch) => {
                        buf.extend(batch.rows);
                        if buf.len() >= max_rows {
                            flush_with_retry(&ch, &mut buf).await;
                        }
                    }
                    None => break, // sender side dropped — shutdown
                }
            }
            _ = tick.tick() => {
                if !buf.is_empty() {
                    flush_with_retry(&ch, &mut buf).await;
                }
            }
        }
    }
    if !buf.is_empty() {
        flush_with_retry(&ch, &mut buf).await;
    }
    Ok(())
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

/// Drain `buf` into a single ClickHouse insert. Public so the ingest binary
/// can call it on shutdown if the channel is closed mid-batch.
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
