//! logstream-ingest entrypoint — boots axum on `--bind`, wires Postgres +
//! Redis tenant auth, spawns the ClickHouse batcher actor, and serves
//! `POST /v1/logs`. Health endpoint is plain `GET /health` for compose
//! probes; `GET /metrics` renders Prometheus text exposition format.
//!
//! Shutdown is graceful: on SIGINT/SIGTERM, `axum::serve` stops accepting
//! new connections and waits for in-flight requests to finish, then this
//! function's router (and the `AppState`/`mpsc::Sender` it owns) is
//! dropped, which closes the batcher's channel; we then await the
//! batcher task so its buffered rows get one last flush before the
//! process exits, instead of being silently lost.

mod ingest;

use axum::routing::get;
use clap::Parser;
use ingest::AppState;
use logstream_core::{run_batcher, TenantAuth, TenantBatch};
use sqlx::postgres::PgPoolOptions;
use std::{net::SocketAddr, sync::Arc};
use tokio::sync::Semaphore;
use tracing_subscriber::EnvFilter;

/// Re-exported so `ingest.rs` and its tests can name it as `crate::TenantLookup`
/// without reaching into `logstream_core` directly everywhere; the real
/// trait + the `TenantAuth` impl of it live in `logstream_core::auth` (also
/// used by `logstream-query`, so there's exactly one copy of this seam).
pub use logstream_core::TenantLookup;

const DEFAULT_MAX_BODY_BYTES: usize = 16 * 1024 * 1024;
const DEFAULT_MAX_BUFFERED_ROWS: usize = 1_000_000;

#[derive(Parser, Debug)]
#[command(name = "logstream-ingest", about = "logstream OTLP-HTTP ingest")]
struct Args {
    /// Bind address for the OTLP-HTTP server.
    #[arg(long, env = "LOGSTREAM_BIND", default_value = "0.0.0.0:4318")]
    bind: SocketAddr,

    /// Postgres connection string for tenant metadata.
    #[arg(long, env = "DATABASE_URL")]
    database_url: String,

    /// Redis URL for the API-key cache.
    #[arg(long, env = "REDIS_URL", default_value = "redis://127.0.0.1:6379")]
    redis_url: String,

    /// ClickHouse HTTP endpoint.
    #[arg(long, env = "CLICKHOUSE_URL", default_value = "http://127.0.0.1:8123")]
    clickhouse_url: String,

    /// ClickHouse database.
    #[arg(long, env = "CLICKHOUSE_DB", default_value = "default")]
    clickhouse_db: String,

    /// Bounded channel capacity (batches, not rows).
    #[arg(long, env = "LOGSTREAM_CHAN_CAPACITY", default_value_t = 1024)]
    chan_capacity: usize,

    /// Flush threshold by row count.
    #[arg(long, env = "LOGSTREAM_MAX_ROWS", default_value_t = 5_000)]
    max_rows: usize,

    /// Flush threshold by elapsed milliseconds.
    #[arg(long, env = "LOGSTREAM_FLUSH_MS", default_value_t = 200)]
    flush_ms: u64,

    /// Max accepted request body size, in bytes (post gzip-decompression).
    #[arg(long, env = "LOGSTREAM_MAX_BODY_BYTES", default_value_t = DEFAULT_MAX_BODY_BYTES)]
    max_body_bytes: usize,

    /// Max total rows buffered in-flight at once (channel + batcher
    /// buffer combined). Bounds worst-case memory: a 16 MiB request can
    /// decode into millions of tiny `LogRow`s, and the channel is bounded
    /// by batch count, not row count, so without this a handful of huge
    /// requests can still exhaust memory before the channel looks full.
    #[arg(long, env = "LOGSTREAM_MAX_BUFFERED_ROWS", default_value_t = DEFAULT_MAX_BUFFERED_ROWS)]
    max_buffered_rows: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .json()
        .init();

    let args = Args::parse();

    let pg = PgPoolOptions::new()
        .max_connections(16)
        .connect(&args.database_url)
        .await?;
    // A managed connection auto-reconnects and is cheap to clone, unlike
    // opening a fresh multiplexed connection per lookup.
    let redis = redis::Client::open(args.redis_url.clone())?
        .get_connection_manager()
        .await?;
    let auth: Arc<dyn TenantLookup> = Arc::new(TenantAuth::new(redis, pg));

    let ch = clickhouse::Client::default()
        .with_url(&args.clickhouse_url)
        .with_database(&args.clickhouse_db);

    let (tx, rx) = tokio::sync::mpsc::channel::<TenantBatch>(args.chan_capacity);
    let max_rows = args.max_rows;
    let flush_ms = args.flush_ms;
    let batcher_handle = tokio::spawn(run_batcher(rx, ch, max_rows, flush_ms));

    let buffer_limit = Arc::new(Semaphore::new(args.max_buffered_rows));
    let state = AppState {
        auth,
        sender: tx,
        buffer_limit,
        max_buffered_rows: args.max_buffered_rows,
        max_body_bytes: args.max_body_bytes,
    };

    let prometheus_handle =
        metrics_exporter_prometheus::PrometheusBuilder::new().install_recorder()?;
    let app = ingest::app(state).route(
        "/metrics",
        get(move || {
            let handle = prometheus_handle.clone();
            async move { handle.render() }
        }),
    );

    let listener = tokio::net::TcpListener::bind(&args.bind).await?;
    tracing::info!(addr = %args.bind, "logstream-ingest listening");

    // `app` (and the `AppState`/`mpsc::Sender` it carries) is only dropped
    // once this statement ends, i.e. after graceful shutdown completes —
    // that's what closes the batcher's channel.
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    tracing::info!("shutdown signal handled, draining batcher");
    batcher_handle.await??;
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install SIGINT handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    tracing::info!("shutdown signal received");
}
