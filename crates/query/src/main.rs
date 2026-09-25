//! logstream-query entrypoint — axum binary serving the read API:
//! `POST /query` (plain JSON), `GET /trace/{trace_id}`, and the
//! Grafana-compatible `/loki/api/v1/*` subset. See `handlers.rs` for the
//! routes and `logql.rs`/`sql.rs` for the LogQL-ish parser and its
//! translation to ClickHouse SQL.

use axum::{routing::get, Router};
use clap::Parser;
use logstream_core::TenantAuth;
use logstream_query::auth;
use logstream_query::handlers::{self, AppState};
use metrics_exporter_prometheus::PrometheusBuilder;
use sqlx::postgres::PgPoolOptions;
use std::{net::SocketAddr, sync::Arc};
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(name = "logstream-query", about = "logstream read API")]
struct Args {
    /// Bind address for the read API.
    #[arg(long, env = "LOGSTREAM_QUERY_BIND", default_value = "0.0.0.0:4319")]
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
    let redis = redis::Client::open(args.redis_url.clone())?
        .get_connection_manager()
        .await?;
    let auth: Arc<dyn auth::TenantResolver> = Arc::new(TenantAuth::new(redis, pg));

    let ch = clickhouse::Client::default()
        .with_url(&args.clickhouse_url)
        .with_database(&args.clickhouse_db);

    let metrics_handle = PrometheusBuilder::new().install_recorder()?;

    let state = AppState { ch, auth };

    let app = Router::new()
        .route("/health", get(|| async { "ok" }))
        .route(
            "/metrics",
            get(move || {
                let h = metrics_handle.clone();
                async move { h.render() }
            }),
        )
        .merge(handlers::build_router(state))
        .layer(axum::middleware::from_fn(handlers::track_metrics));

    let listener = tokio::net::TcpListener::bind(&args.bind).await?;
    tracing::info!(addr = %args.bind, "logstream-query listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
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
