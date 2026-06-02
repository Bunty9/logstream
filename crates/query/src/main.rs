//! logstream-query entrypoint — axum binary serving the Grafana-compatible
//! read API. Phase 1 only wires `GET /health` and a placeholder `POST
//! /query`; the LogQL-ish planner + ClickHouse SELECT translation arrive
//! in Phase 3 (see PROGRESS.md).

use axum::{
    extract::State,
    routing::{get, post},
    Json, Router,
};
use clap::Parser;
use std::net::SocketAddr;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(name = "logstream-query", about = "logstream read API")]
struct Args {
    /// Bind address for the read API.
    #[arg(long, env = "LOGSTREAM_QUERY_BIND", default_value = "0.0.0.0:4319")]
    bind: SocketAddr,

    /// ClickHouse HTTP endpoint.
    #[arg(long, env = "CLICKHOUSE_URL", default_value = "http://127.0.0.1:8123")]
    clickhouse_url: String,

    /// ClickHouse database.
    #[arg(long, env = "CLICKHOUSE_DB", default_value = "default")]
    clickhouse_db: String,
}

#[derive(Clone)]
struct QueryState {
    #[allow(dead_code)] // wired in Phase 3
    ch: clickhouse::Client,
}

#[derive(serde::Deserialize)]
struct QueryReq {
    // Wired in Phase 3 — the LogQL-ish surface gets parsed off this field.
    #[allow(dead_code)]
    #[serde(default)]
    query: String,
}

async fn query(
    State(_state): State<QueryState>,
    Json(_req): Json<QueryReq>,
) -> Json<serde_json::Value> {
    // TODO(phase-3): parse the query, translate to ClickHouse SELECT, stream rows.
    Json(serde_json::json!({ "status": "not_implemented", "rows": [] }))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .json()
        .init();

    let args = Args::parse();
    let ch = clickhouse::Client::default()
        .with_url(&args.clickhouse_url)
        .with_database(&args.clickhouse_db);
    let state = QueryState { ch };

    let app = Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/query", post(query))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(&args.bind).await?;
    tracing::info!(addr = %args.bind, "logstream-query listening");
    axum::serve(listener, app).await?;
    Ok(())
}
