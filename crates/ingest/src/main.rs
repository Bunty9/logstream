//! logstream-ingest entrypoint — boots axum on `--bind`, wires Postgres +
//! Redis tenant auth, spawns the ClickHouse batcher actor, and serves
//! `POST /v1/logs`. Health endpoint is plain `GET /health` for compose
//! probes.

mod ingest;

use axum::{
    routing::{get, post},
    Router,
};
use clap::Parser;
use ingest::AppState;
use logstream_core::{run_batcher, TenantAuth, TenantBatch};
use sqlx::postgres::PgPoolOptions;
use std::{net::SocketAddr, sync::Arc};
use tracing_subscriber::EnvFilter;

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
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .json()
        .init();

    let args = Args::parse();

    let pg = PgPoolOptions::new()
        .max_connections(16)
        .connect(&args.database_url)
        .await?;
    let redis = redis::Client::open(args.redis_url.clone())?;
    let auth = Arc::new(TenantAuth::new(redis, pg));

    let ch = clickhouse::Client::default()
        .with_url(&args.clickhouse_url)
        .with_database(&args.clickhouse_db);

    let (tx, rx) = tokio::sync::mpsc::channel::<TenantBatch>(args.chan_capacity);
    let max_rows = args.max_rows;
    let flush_ms = args.flush_ms;
    let batcher_handle = tokio::spawn(async move {
        if let Err(e) = run_batcher(rx, ch, max_rows, flush_ms).await {
            tracing::error!(?e, "batcher exited with error");
        }
    });

    let state = AppState { auth, sender: tx };

    let app = Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/v1/logs", post(ingest::logs))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(&args.bind).await?;
    tracing::info!(addr = %args.bind, "logstream-ingest listening");
    axum::serve(listener, app).await?;

    drop(batcher_handle);
    Ok(())
}
