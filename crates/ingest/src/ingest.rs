//! OTLP-HTTP `/v1/logs` handler.
//!
//! 1. Pull the API key, resolve to a tenant via `TenantAuth`.
//! 2. Decode the body as `ExportLogsServiceRequest` (protobuf, not JSON).
//! 3. Project to `Vec<LogRow>` and hand off to the batcher actor via a
//!    bounded `mpsc::Sender`.
//! 4. `try_send` => drop-newest backpressure: if the channel is full we
//!    fail the request with `429` and record a `logstream_dropped_rows`
//!    counter — defensible default for log ingestion (never block the
//!    producer, never silently absorb).

use axum::{body::Bytes, extract::State, http::HeaderMap, response::IntoResponse};
use logstream_core::{otlp_to_rows, TenantAuth, TenantBatch};
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use prost::Message;
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub auth: Arc<TenantAuth>,
    pub sender: tokio::sync::mpsc::Sender<TenantBatch>,
}

pub async fn logs(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<impl IntoResponse, (axum::http::StatusCode, &'static str)> {
    let api_key = headers
        .get("x-api-key")
        .and_then(|v| v.to_str().ok())
        .ok_or((axum::http::StatusCode::UNAUTHORIZED, "missing x-api-key"))?;
    let tenant_id = state
        .auth
        .lookup(api_key)
        .await
        .ok_or((axum::http::StatusCode::UNAUTHORIZED, "invalid key"))?;

    let req = ExportLogsServiceRequest::decode(body)
        .map_err(|_| (axum::http::StatusCode::BAD_REQUEST, "bad proto"))?;

    let rows = otlp_to_rows(tenant_id.clone(), &req);
    let batch = TenantBatch { tenant_id, rows };

    // try_send => drop-newest backpressure (defensible default for logs)
    match state.sender.try_send(batch) {
        Ok(()) => Ok(axum::Json(serde_json::json!({ "rejected": 0 }))),
        Err(tokio::sync::mpsc::error::TrySendError::Full(b)) => {
            metrics::counter!("logstream_dropped_rows", "tenant" => b.tenant_id.clone())
                .increment(b.rows.len() as u64);
            Err((axum::http::StatusCode::TOO_MANY_REQUESTS, "channel full"))
        }
        Err(_) => Err((axum::http::StatusCode::SERVICE_UNAVAILABLE, "shutdown")),
    }
}
