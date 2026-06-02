//! Per-process batcher actor. Receives `TenantBatch` over a bounded mpsc
//! channel and writes to ClickHouse either when the in-memory buffer
//! reaches `max_rows` or every `flush_ms` milliseconds, whichever fires
//! first.
//!
//! Backpressure is intentionally drop-newest at the ingest boundary
//! (see `crates/ingest/src/ingest.rs`); the batcher itself never blocks
//! recv() because the channel does that for us.

use crate::types::{LogRow, TenantBatch};
use tokio::sync::mpsc::Receiver;
use tokio::time::{interval, Duration, MissedTickBehavior};

/// Drain a bounded `TenantBatch` channel into ClickHouse.
///
/// `max_rows` is the size-based flush threshold; the spec calls for 5,000.
/// `flush_ms` is the time-based threshold; 200ms keeps tail latency tight
/// while still amortizing the ClickHouse round-trip across thousands of
/// rows.
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
                            flush(&ch, &mut buf).await?;
                        }
                    }
                    None => break, // sender side dropped — shutdown
                }
            }
            _ = tick.tick() => {
                if !buf.is_empty() {
                    flush(&ch, &mut buf).await?;
                }
            }
        }
    }
    if !buf.is_empty() {
        flush(&ch, &mut buf).await?;
    }
    Ok(())
}

/// Drain `buf` into a single ClickHouse insert. Public so the ingest binary
/// can call it on shutdown if the channel is closed mid-batch.
pub async fn flush(ch: &clickhouse::Client, buf: &mut Vec<LogRow>) -> anyhow::Result<()> {
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
