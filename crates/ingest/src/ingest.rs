//! OTLP-HTTP `/v1/logs` handler + router assembly.
//!
//! 1. Reject non-protobuf `Content-Type` with `415` (OTLP/JSON is not
//!    implemented — see the module doc below for why).
//! 2. Pull the API key (`x-api-key`, or `Authorization: Bearer <key>` as a
//!    fallback — both are common OTel exporter configurations), resolve to
//!    a tenant via `TenantLookup`.
//! 3. Decode the body as `ExportLogsServiceRequest` (protobuf).
//! 4. Project to `Vec<LogRow>` and hand off to the batcher actor via a
//!    bounded `mpsc::Sender`.
//! 5. `try_send` => drop-newest backpressure: if the channel is full we
//!    fail the request with `429` (OTLP-retryable) and record
//!    `logstream_dropped_rows_total{reason="channel_full"}` — defensible
//!    default for log ingestion (never block the producer, never silently
//!    absorb). A closed channel (shutdown in progress) is `503`.
//!
//! On success the response body is a protobuf-encoded
//! `ExportLogsServiceResponse` with `Content-Type: application/x-protobuf`
//! — OTLP-HTTP clients (the collector's `otlphttp` exporter, language
//! SDKs) expect that exact shape, not a JSON `{"rejected": 0}` object.
//!
//! OTLP/JSON request bodies are not supported: `opentelemetry-proto`'s
//! `with-serde` feature would need enabling workspace-wide plus
//! content-sniffing branch logic in this handler, for a content type none
//! of the collector's default exporters send. Skipped — add it if a
//! client that only speaks OTLP/JSON shows up.

use crate::TenantLookup;
use axum::{
    body::Bytes,
    extract::{DefaultBodyLimit, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Router,
};
use logstream_core::{otlp_to_rows, TenantBatch};
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use prost::Message;
use std::sync::Arc;
use tokio::sync::mpsc;
use tower_http::decompression::RequestDecompressionLayer;

#[derive(Clone)]
pub struct AppState {
    pub auth: Arc<dyn TenantLookup>,
    pub sender: mpsc::Sender<TenantBatch>,
}

/// Build the router: `/health` plus `POST /v1/logs`, with gzip request
/// decompression (the collector's default `Content-Encoding`) and a
/// configurable body-size cap. Split out from `main` so handler tests can
/// drive it directly with `tower::ServiceExt::oneshot` instead of binding
/// a real socket; `main` adds `/metrics` afterwards since that route needs
/// the process-wide Prometheus handle, which is unrelated to request
/// routing/state.
pub fn app(state: AppState, max_body_bytes: usize) -> Router {
    Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/v1/logs", post(logs))
        .layer(DefaultBodyLimit::max(max_body_bytes))
        .layer(RequestDecompressionLayer::new())
        .with_state(state)
}

async fn logs(State(state): State<AppState>, headers: HeaderMap, body: Bytes) -> Response {
    if let Some(ct) = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
    {
        if !ct.starts_with("application/x-protobuf") {
            record_status("415");
            return (
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "unsupported content-type, expected application/x-protobuf",
            )
                .into_response();
        }
    }

    let Some(api_key) = extract_api_key(&headers) else {
        record_status("401");
        return (StatusCode::UNAUTHORIZED, "missing api key").into_response();
    };

    let Some(tenant_id) = state.auth.lookup(&api_key).await else {
        record_status("401");
        return (StatusCode::UNAUTHORIZED, "invalid key").into_response();
    };

    let req = match ExportLogsServiceRequest::decode(body) {
        Ok(req) => req,
        Err(_) => {
            record_status("400");
            return (StatusCode::BAD_REQUEST, "bad proto").into_response();
        }
    };

    let rows = otlp_to_rows(tenant_id.clone(), &req);
    let row_count = rows.len() as u64;
    let batch = TenantBatch { tenant_id, rows };

    match state.sender.try_send(batch) {
        Ok(()) => {
            record_status("200");
            metrics::counter!("logstream_ingest_rows_total").increment(row_count);
            protobuf_response(StatusCode::OK, &ExportLogsServiceResponse::default())
        }
        Err(mpsc::error::TrySendError::Full(b)) => {
            metrics::counter!("logstream_dropped_rows_total", "reason" => "channel_full")
                .increment(b.rows.len() as u64);
            record_status("429");
            (StatusCode::TOO_MANY_REQUESTS, "channel full").into_response()
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            record_status("503");
            (StatusCode::SERVICE_UNAVAILABLE, "shutting down").into_response()
        }
    }
}

fn record_status(status: &'static str) {
    metrics::counter!("logstream_ingest_requests_total", "status" => status).increment(1);
}

/// `x-api-key: <key>`, falling back to `Authorization: Bearer <key>` —
/// OTel exporters commonly configure either.
fn extract_api_key(headers: &HeaderMap) -> Option<String> {
    if let Some(key) = headers.get("x-api-key").and_then(|v| v.to_str().ok()) {
        return Some(key.to_string());
    }
    headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::to_string)
}

fn protobuf_response(status: StatusCode, msg: &impl prost::Message) -> Response {
    (
        status,
        [(header::CONTENT_TYPE, "application/x-protobuf")],
        msg.encode_to_vec(),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TenantLookup;
    use axum::http::Request;
    use flate2::{write::GzEncoder, Compression};
    use opentelemetry_proto::tonic::{
        collector::logs::v1::ExportLogsServiceRequest,
        common::v1::{any_value::Value, AnyValue, KeyValue},
        logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
    };
    use std::{collections::HashMap, future::Future, io::Write, pin::Pin};
    use tower::ServiceExt;

    /// Static in-memory `TenantLookup` — no Postgres/Redis needed for
    /// handler-level tests.
    struct StaticTenants(HashMap<&'static str, &'static str>);

    impl TenantLookup for StaticTenants {
        fn lookup<'a>(
            &'a self,
            api_key: &'a str,
        ) -> Pin<Box<dyn Future<Output = Option<String>> + Send + 'a>> {
            let found = self.0.get(api_key).map(|t| t.to_string());
            Box::pin(async move { found })
        }
    }

    fn state_with(capacity: usize) -> (AppState, mpsc::Receiver<TenantBatch>) {
        let (tx, rx) = mpsc::channel(capacity);
        let auth: Arc<dyn TenantLookup> =
            Arc::new(StaticTenants(HashMap::from([("TESTKEY", "demo")])));
        (AppState { auth, sender: tx }, rx)
    }

    fn sample_request() -> ExportLogsServiceRequest {
        ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: None,
                scope_logs: vec![ScopeLogs {
                    scope: None,
                    log_records: vec![LogRecord {
                        body: Some(AnyValue {
                            value: Some(Value::StringValue("hello".into())),
                        }),
                        attributes: vec![KeyValue {
                            key: "k".into(),
                            value: Some(AnyValue {
                                value: Some(Value::StringValue("v".into())),
                            }),
                        }],
                        ..Default::default()
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        }
    }

    #[tokio::test]
    async fn missing_key_is_401() {
        let (state, _rx) = state_with(4);
        let app = app(state, 1024 * 1024);
        let resp = app
            .oneshot(
                Request::post("/v1/logs")
                    .header(header::CONTENT_TYPE, "application/x-protobuf")
                    .body(axum::body::Body::from(Bytes::new()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn bad_proto_is_400() {
        let (state, _rx) = state_with(4);
        let app = app(state, 1024 * 1024);
        let resp = app
            .oneshot(
                Request::post("/v1/logs")
                    .header(header::CONTENT_TYPE, "application/x-protobuf")
                    .header("x-api-key", "TESTKEY")
                    .body(axum::body::Body::from(Bytes::from_static(
                        b"\xff\xff\xff not a proto",
                    )))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn channel_full_is_429() {
        // Capacity-1 channel, pre-filled, so the handler's `try_send` hits
        // `Full`.
        let (state, _rx) = state_with(1);
        state
            .sender
            .try_send(TenantBatch {
                tenant_id: "demo".into(),
                rows: vec![],
            })
            .unwrap();
        let app = app(state, 1024 * 1024);
        let body = sample_request().encode_to_vec();
        let resp = app
            .oneshot(
                Request::post("/v1/logs")
                    .header(header::CONTENT_TYPE, "application/x-protobuf")
                    .header("x-api-key", "TESTKEY")
                    .body(axum::body::Body::from(Bytes::from(body)))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
    }

    #[tokio::test]
    async fn happy_path_returns_protobuf_and_forwards_rows() {
        let (state, mut rx) = state_with(4);
        let app = app(state, 1024 * 1024);
        let body = sample_request().encode_to_vec();
        let resp = app
            .oneshot(
                Request::post("/v1/logs")
                    .header(header::CONTENT_TYPE, "application/x-protobuf")
                    .header("x-api-key", "TESTKEY")
                    .body(axum::body::Body::from(Bytes::from(body)))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers().get(header::CONTENT_TYPE).unwrap(),
            "application/x-protobuf"
        );
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        ExportLogsServiceResponse::decode(bytes).expect("valid protobuf response");

        let batch = rx.try_recv().expect("batch forwarded to batcher channel");
        assert_eq!(batch.tenant_id, "demo");
    }

    #[tokio::test]
    async fn gzip_body_is_decompressed() {
        let (state, mut rx) = state_with(4);
        let app = app(state, 1024 * 1024);
        let raw = sample_request().encode_to_vec();
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&raw).unwrap();
        let gzipped = encoder.finish().unwrap();

        let resp = app
            .oneshot(
                Request::post("/v1/logs")
                    .header(header::CONTENT_TYPE, "application/x-protobuf")
                    .header(header::CONTENT_ENCODING, "gzip")
                    .header("x-api-key", "TESTKEY")
                    .body(axum::body::Body::from(Bytes::from(gzipped)))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        rx.try_recv().expect("batch forwarded despite gzip body");
    }
}
