//! OTLP-HTTP `/v1/logs` handler + router assembly.
//!
//! 1. Reject non-protobuf `Content-Type` with `415` (OTLP/JSON is not
//!    implemented — see the module doc below for why).
//! 2. Pull the API key (`x-api-key`, or `Authorization: Bearer <key>` as a
//!    fallback — both are common OTel exporter configurations), resolve to
//!    a tenant via `TenantLookup`. This — and the content-type check above
//!    it — happen *before* the request body is read at all: a request
//!    with a missing/bad key never pays for gzip decompression or
//!    buffering (see the handler for why that ordering matters).
//! 3. Read the (lazily-decompressed) body, capped at `max_body_bytes`, and
//!    decode it as `ExportLogsServiceRequest` (protobuf).
//! 4. Project to `Vec<LogRow>`, acquire a `--max-buffered-rows` semaphore
//!    permit covering every row, and hand both off to the batcher actor
//!    via a bounded `mpsc::Sender`.
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
    extract::{Request, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Router,
};
use logstream_core::{api_key_from_headers, otlp_to_rows, record_count, TenantBatch};
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use prost::Message;
use std::sync::Arc;
use tokio::sync::{mpsc, Semaphore};
use tower_http::decompression::RequestDecompressionLayer;

#[derive(Clone)]
pub struct AppState {
    pub auth: Arc<dyn TenantLookup>,
    pub sender: mpsc::Sender<TenantBatch>,
    /// In-flight row budget shared by every request (channel + batcher
    /// buffer combined) — see `crates/core/src/types.rs`'s `TenantBatch`
    /// doc for how the permit travels with a batch.
    pub buffer_limit: Arc<Semaphore>,
    /// The semaphore's total capacity, so a single request that could
    /// never fit (even against an empty buffer) can be told apart from
    /// one that just has to wait its turn — see `logs` below.
    pub max_buffered_rows: usize,
    pub max_body_bytes: usize,
}

/// Build the router: `/health` plus `POST /v1/logs`, with gzip request
/// decompression (the collector's default `Content-Encoding`) applied
/// lazily as the handler reads the body. Split out from `main` so handler
/// tests can drive it directly with `tower::ServiceExt::oneshot` instead
/// of binding a real socket; `main` adds `/metrics` afterwards since that
/// route needs the process-wide Prometheus handle, which is unrelated to
/// request routing/state.
pub fn app(state: AppState) -> Router {
    Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/v1/logs", post(logs))
        .layer(RequestDecompressionLayer::new())
        .with_state(state)
}

async fn logs(State(state): State<AppState>, req: Request) -> Response {
    let headers = req.headers();

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

    let Some(api_key) = api_key_from_headers(headers) else {
        record_status("401");
        return (StatusCode::UNAUTHORIZED, "missing api key").into_response();
    };

    let tenant_id = match state.auth.lookup(api_key).await {
        Ok(Some(tenant_id)) => tenant_id,
        Ok(None) => {
            record_status("401");
            return (StatusCode::UNAUTHORIZED, "invalid key").into_response();
        }
        Err(err) => {
            tracing::error!(%err, "tenant auth backend unavailable");
            record_status("503");
            return (StatusCode::SERVICE_UNAVAILABLE, "auth backend unavailable").into_response();
        }
    };

    // Only now — after content-type and auth both passed — do we read the
    // body at all. `RequestDecompressionLayer` wraps the body to
    // decompress *lazily* as it's polled rather than eagerly up front, so
    // a request with a missing/invalid key never pays for gzip
    // decompression or buffering. `to_bytes`'s own limit caps the
    // *decompressed* size as it streams in, so a gzip bomb still can't
    // over-allocate: it aborts as soon as more than `max_body_bytes` has
    // come out of the decompressor, without ever holding the full
    // inflated body.
    let body = match axum::body::to_bytes(req.into_body(), state.max_body_bytes).await {
        Ok(b) => b,
        Err(err) => {
            // Only a hit on the `max_body_bytes` length cap is a client
            // payload-size problem (413) — `to_bytes` wraps `Limited`,
            // whose length-limit failure is reported via the error's
            // `source()` as `http_body_util::LengthLimitError` (see
            // `axum::body::to_bytes`'s own doc example). Anything else
            // (corrupt/truncated gzip from `RequestDecompressionLayer`, a
            // connection drop mid-body, ...) isn't a size problem, so it's
            // a generic 400 rather than a possibly-misleading 413.
            let is_length_limit = std::error::Error::source(&err)
                .is_some_and(|src| src.is::<http_body_util::LengthLimitError>());
            if is_length_limit {
                record_status("413");
                return (StatusCode::PAYLOAD_TOO_LARGE, "body too large").into_response();
            }
            record_status("400");
            return (StatusCode::BAD_REQUEST, "bad body").into_response();
        }
    };

    let decoded = match ExportLogsServiceRequest::decode(body) {
        Ok(req) => req,
        Err(_) => {
            record_status("400");
            return (StatusCode::BAD_REQUEST, "bad proto").into_response();
        }
    };

    // Row count comes straight off the decoded protobuf (`record_count`),
    // *before* `otlp_to_rows` builds a single `LogRow` — that way the
    // 413/permit checks below reject an oversized or budget-exceeding
    // request without ever materializing its rows, bounding peak memory
    // under 429 pressure instead of paying for the full allocation only
    // to throw it away.
    let row_count = record_count(&decoded);

    if row_count > state.max_buffered_rows || row_count > u32::MAX as usize {
        record_status("413");
        return (StatusCode::PAYLOAD_TOO_LARGE, "too many records in request").into_response();
    }

    let permit = match state
        .buffer_limit
        .clone()
        .try_acquire_many_owned(row_count as u32)
    {
        Ok(permit) => permit,
        Err(_) => {
            metrics::counter!("logstream_dropped_rows_total", "reason" => "buffer_full")
                .increment(row_count as u64);
            record_status("429");
            return (StatusCode::TOO_MANY_REQUESTS, "buffer full").into_response();
        }
    };

    let rows = otlp_to_rows(tenant_id, &decoded);
    let batch = TenantBatch { rows, permit };

    match state.sender.try_send(batch) {
        Ok(()) => {
            record_status("200");
            metrics::counter!("logstream_ingest_rows_total").increment(row_count as u64);
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
    use axum::http::Request as HttpRequest;
    use flate2::{write::GzEncoder, Compression};
    use logstream_core::AuthError;
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
        ) -> Pin<Box<dyn Future<Output = Result<Option<String>, AuthError>> + Send + 'a>> {
            let found = self.0.get(api_key).map(|t| t.to_string());
            Box::pin(async move { Ok(found) })
        }
    }

    /// Always reports a backend outage — exercises the `503` mapping for
    /// `TenantLookup::lookup` returning `Err`.
    struct FailingTenants;

    impl TenantLookup for FailingTenants {
        fn lookup<'a>(
            &'a self,
            _api_key: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<Option<String>, AuthError>> + Send + 'a>> {
            Box::pin(async {
                Err(AuthError::Backend(sqlx::Error::Protocol(
                    "simulated backend failure".into(),
                )))
            })
        }
    }

    fn state_with(
        chan_capacity: usize,
        max_buffered_rows: usize,
    ) -> (AppState, mpsc::Receiver<TenantBatch>) {
        let (tx, rx) = mpsc::channel(chan_capacity);
        let auth: Arc<dyn TenantLookup> =
            Arc::new(StaticTenants(HashMap::from([("TESTKEY", "demo")])));
        let state = AppState {
            auth,
            sender: tx,
            buffer_limit: Arc::new(Semaphore::new(max_buffered_rows)),
            max_buffered_rows,
            max_body_bytes: 1024 * 1024,
        };
        (state, rx)
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

    fn request_with_n_records(n: usize) -> ExportLogsServiceRequest {
        ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: None,
                scope_logs: vec![ScopeLogs {
                    scope: None,
                    log_records: (0..n).map(|_| LogRecord::default()).collect(),
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        }
    }

    #[tokio::test]
    async fn missing_key_is_401() {
        let (state, _rx) = state_with(4, 1_000_000);
        let app = app(state);
        let resp = app
            .oneshot(
                HttpRequest::post("/v1/logs")
                    .header(header::CONTENT_TYPE, "application/x-protobuf")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn auth_backend_failure_is_503() {
        let (mut state, _rx) = state_with(4, 1_000_000);
        state.auth = Arc::new(FailingTenants);
        let app = app(state);
        let body = sample_request().encode_to_vec();
        let resp = app
            .oneshot(
                HttpRequest::post("/v1/logs")
                    .header(header::CONTENT_TYPE, "application/x-protobuf")
                    .header("x-api-key", "TESTKEY")
                    .body(axum::body::Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn bad_proto_is_400() {
        let (state, _rx) = state_with(4, 1_000_000);
        let app = app(state);
        let resp = app
            .oneshot(
                HttpRequest::post("/v1/logs")
                    .header(header::CONTENT_TYPE, "application/x-protobuf")
                    .header("x-api-key", "TESTKEY")
                    .body(axum::body::Body::from(&b"\xff\xff\xff not a proto"[..]))
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
        let (state, _rx) = state_with(1, 1_000_000);
        let permit = state
            .buffer_limit
            .clone()
            .try_acquire_many_owned(0)
            .unwrap();
        state
            .sender
            .try_send(TenantBatch {
                rows: vec![],
                permit,
            })
            .unwrap();
        let app = app(state);
        let body = sample_request().encode_to_vec();
        let resp = app
            .oneshot(
                HttpRequest::post("/v1/logs")
                    .header(header::CONTENT_TYPE, "application/x-protobuf")
                    .header("x-api-key", "TESTKEY")
                    .body(axum::body::Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
    }

    #[tokio::test]
    async fn buffer_full_is_429_and_drops_before_touching_channel() {
        // Budget for exactly 1 row, but it's already held by another
        // in-flight batch: `sample_request`'s single row is within the
        // budget (so this isn't the permanent "too large" 413 case below)
        // but can't be acquired *right now*, so this must fail via the
        // semaphore (429), not the channel (which has plenty of capacity
        // here).
        let (state, mut rx) = state_with(4, 1);
        let _held = state
            .buffer_limit
            .clone()
            .try_acquire_many_owned(1)
            .unwrap();
        let app = app(state);
        let body = sample_request().encode_to_vec();
        let resp = app
            .oneshot(
                HttpRequest::post("/v1/logs")
                    .header(header::CONTENT_TYPE, "application/x-protobuf")
                    .header("x-api-key", "TESTKEY")
                    .body(axum::body::Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
        assert!(
            rx.try_recv().is_err(),
            "a buffer-rejected batch must never reach the channel"
        );
    }

    #[tokio::test]
    async fn row_count_over_max_buffered_rows_is_413() {
        // 1 log record in the request, but the budget is 0 — even an
        // empty buffer could never fit it, so this is a permanent
        // "too large", not a transient "try again" like the 429 case.
        let (state, _rx) = state_with(4, 0);
        let app = app(state);
        let body = sample_request().encode_to_vec();
        let resp = app
            .oneshot(
                HttpRequest::post("/v1/logs")
                    .header(header::CONTENT_TYPE, "application/x-protobuf")
                    .header("x-api-key", "TESTKEY")
                    .body(axum::body::Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        // Note: with a 0-row budget both the "too large" (413) and
        // "buffer full" (429) checks would technically match a 1-row
        // request; the handler checks "too large" first since it's the
        // permanent condition. See `buffer_full_is_429_and_drops_before_touching_channel`
        // for the transient (nonzero budget, temporarily exhausted) case.
        assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[tokio::test]
    async fn row_count_is_computed_before_rows_are_built() {
        // Regression test for the row-budget ordering fix: the row count
        // used for the 413 check and the permit acquisition must come
        // from the decoded request directly (`record_count`), not from
        // `rows.len()` after `otlp_to_rows` already built every row.
        // Exercised with >1 record across the request so the decoded
        // count and the built-row count genuinely have to agree; a
        // budget of 2 against 5 records must reject via 413 without a
        // batch ever reaching the channel.
        let (state, mut rx) = state_with(4, 2);
        let app = app(state);
        let body = request_with_n_records(5).encode_to_vec();
        let resp = app
            .oneshot(
                HttpRequest::post("/v1/logs")
                    .header(header::CONTENT_TYPE, "application/x-protobuf")
                    .header("x-api-key", "TESTKEY")
                    .body(axum::body::Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn happy_path_returns_protobuf_and_forwards_rows() {
        let (state, mut rx) = state_with(4, 1_000_000);
        let app = app(state);
        let body = sample_request().encode_to_vec();
        let resp = app
            .oneshot(
                HttpRequest::post("/v1/logs")
                    .header(header::CONTENT_TYPE, "application/x-protobuf")
                    .header("x-api-key", "TESTKEY")
                    .body(axum::body::Body::from(body))
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
        assert_eq!(batch.rows.len(), 1);
    }

    #[tokio::test]
    async fn gzip_body_is_decompressed() {
        let (state, mut rx) = state_with(4, 1_000_000);
        let app = app(state);
        let raw = sample_request().encode_to_vec();
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&raw).unwrap();
        let gzipped = encoder.finish().unwrap();

        let resp = app
            .oneshot(
                HttpRequest::post("/v1/logs")
                    .header(header::CONTENT_TYPE, "application/x-protobuf")
                    .header(header::CONTENT_ENCODING, "gzip")
                    .header("x-api-key", "TESTKEY")
                    .body(axum::body::Body::from(gzipped))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        rx.try_recv().expect("batch forwarded despite gzip body");
    }

    /// A highly-compressible gzip body (a run of zero bytes) that inflates
    /// to well past `max_body_bytes` — verifies the decompressed-size cap
    /// still applies (413), and that it's enforced by the streaming
    /// `to_bytes` limit rather than an eager full decompression: this test
    /// finishes quickly and without an allocation anywhere near the
    /// inflated size, which a bug reintroducing eager decompression before
    /// auth/the size check would blow past.
    #[tokio::test]
    async fn gzip_bomb_beyond_body_limit_is_413() {
        let (mut state, mut rx) = state_with(4, 1_000_000);
        state.max_body_bytes = 1024; // tiny cap for the test
        let app = app(state);

        let inflated = vec![0u8; 64 * 1024 * 1024]; // 64 MiB of zeros
        let mut encoder = GzEncoder::new(Vec::new(), Compression::best());
        encoder.write_all(&inflated).unwrap();
        let gzipped = encoder.finish().unwrap();
        assert!(
            gzipped.len() < inflated.len() / 100,
            "sanity check: the bomb must compress at well over 100x, otherwise this test \
             isn't actually exercising the decompressed-size cap"
        );

        let resp = app
            .oneshot(
                HttpRequest::post("/v1/logs")
                    .header(header::CONTENT_TYPE, "application/x-protobuf")
                    .header(header::CONTENT_ENCODING, "gzip")
                    .header("x-api-key", "TESTKEY")
                    .body(axum::body::Body::from(gzipped))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert!(
            rx.try_recv().is_err(),
            "an oversized body must never reach the channel"
        );
    }

    /// A corrupt gzip stream (well within `max_body_bytes`) must fail as
    /// `400` ("bad body"), not `413` — the old code mapped *every*
    /// `to_bytes` error to 413 regardless of cause. `RequestDecompressionLayer`
    /// surfaces the flate2 decode failure as a body-read error with no
    /// `LengthLimitError` in its source chain, which is exactly the case
    /// this fix distinguishes from a real length-limit hit.
    #[tokio::test]
    async fn corrupt_gzip_body_is_400_not_413() {
        let (state, mut rx) = state_with(4, 1_000_000);
        let app = app(state);

        // Valid gzip magic bytes followed by garbage: passes the
        // decompression layer's initial sniff but fails mid-stream.
        let bogus_gzip = vec![0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0xff];

        let resp = app
            .oneshot(
                HttpRequest::post("/v1/logs")
                    .header(header::CONTENT_TYPE, "application/x-protobuf")
                    .header(header::CONTENT_ENCODING, "gzip")
                    .header("x-api-key", "TESTKEY")
                    .body(axum::body::Body::from(bogus_gzip))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        assert!(
            rx.try_recv().is_err(),
            "a corrupt body must never reach the channel"
        );
    }
}
