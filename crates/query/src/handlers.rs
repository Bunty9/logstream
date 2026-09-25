//! HTTP handlers: the plain JSON `/query` + `/trace/{trace_id}` surface,
//! and the Loki-compatible subset Grafana's built-in Loki datasource
//! needs (`/loki/api/v1/query_range`, `/query`, `/labels`,
//! `/label/{name}/values`).
//!
//! Every handler resolves a tenant from the request before touching
//! ClickHouse (`require_tenant`), and every ClickHouse query built here
//! goes through `sql::translate*`, which always filters on the resolved
//! tenant — see that module's docs for how user input is bound rather
//! than interpolated.

use crate::auth::{self, TenantResolver};
use crate::logql;
use crate::row::SelectedRow;
use crate::sql::{self, Bind, Direction, Translated};
use crate::time_util;

use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    middleware::Next,
    response::Response,
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

#[derive(Clone)]
pub struct AppState {
    pub ch: clickhouse::Client,
    pub auth: Arc<dyn TenantResolver>,
}

/// The `/query` + `/trace/*` + `/loki/*` router. `/health` and `/metrics`
/// are wired separately in `main.rs` since they don't need `AppState`.
pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/query", post(query))
        .route("/trace/{trace_id}", get(trace_lookup))
        .route("/loki/api/v1/query_range", get(loki_query_range))
        .route("/loki/api/v1/query", get(loki_query))
        .route("/loki/api/v1/labels", get(loki_labels))
        .route("/loki/api/v1/label/{name}/values", get(loki_label_values))
        .with_state(state)
}

/// Tower middleware recording `logstream_query_requests_total{status}` and
/// `logstream_query_duration_seconds` for every request.
pub async fn track_metrics(req: axum::extract::Request, next: Next) -> Response {
    let start = Instant::now();
    let resp = next.run(req).await;
    let status = resp.status().as_u16().to_string();
    metrics::counter!("logstream_query_requests_total", "status" => status).increment(1);
    metrics::histogram!("logstream_query_duration_seconds").record(start.elapsed().as_secs_f64());
    resp
}

type JsonErr = (StatusCode, Json<Value>);

fn err_json(status: StatusCode, msg: impl Into<String>) -> JsonErr {
    (status, Json(json!({ "error": msg.into() })))
}

fn err_loki(status: StatusCode, msg: impl Into<String>) -> JsonErr {
    (
        status,
        Json(json!({ "status": "error", "error": msg.into() })),
    )
}

/// Resolve `x-api-key`/`Authorization: Bearer` to a tenant id, or a
/// `(status, message)` the caller wraps in its own error envelope.
async fn require_tenant(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<String, (StatusCode, String)> {
    let key = auth::extract_api_key(headers).ok_or((
        StatusCode::UNAUTHORIZED,
        "missing x-api-key or Authorization: Bearer".to_string(),
    ))?;
    state
        .auth
        .lookup(key)
        .await
        .ok_or((StatusCode::UNAUTHORIZED, "invalid API key".to_string()))
}

fn bind_one(q: clickhouse::query::Query, b: Bind) -> clickhouse::query::Query {
    match b {
        Bind::Str(s) => q.bind(s),
        Bind::I64(n) => q.bind(n),
    }
}

/// ClickHouse errors surfacing a bad query (e.g. an invalid re2 pattern
/// from `=~`) come back as `Error::BadResponse` with ClickHouse's own
/// message — that's a client mistake, so it's a 400, not a 500.
fn ch_err(e: clickhouse::error::Error) -> (StatusCode, String) {
    match e {
        clickhouse::error::Error::BadResponse(msg) => (StatusCode::BAD_REQUEST, msg),
        other => (StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
    }
}

async fn run_select(
    ch: &clickhouse::Client,
    t: Translated,
) -> Result<Vec<SelectedRow>, clickhouse::error::Error> {
    let mut q = ch.query(&t.sql);
    for b in t.binds {
        q = bind_one(q, b);
    }
    q.fetch_all::<SelectedRow>().await
}

/// Auth + parse + translate + run, shared by `/query` and the Loki
/// range/instant handlers.
async fn execute_logql(
    state: &AppState,
    headers: &HeaderMap,
    raw_query: &str,
    start_ns: i64,
    end_ns: i64,
    limit: u32,
    direction: Direction,
) -> Result<Vec<SelectedRow>, (StatusCode, String)> {
    let tenant = require_tenant(state, headers).await?;
    let ast = logql::parse(raw_query).map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    let translated = sql::translate(&ast, &tenant, start_ns, end_ns, limit, direction);
    run_select(&state.ch, translated).await.map_err(ch_err)
}

// === POST /query ===

#[derive(Debug, Deserialize)]
pub struct QueryReq {
    query: String,
    #[serde(default)]
    start: Option<Value>,
    #[serde(default)]
    end: Option<Value>,
    #[serde(default)]
    limit: Option<u32>,
    #[serde(default)]
    direction: Option<String>,
}

async fn query(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<QueryReq>,
) -> Result<Json<Value>, JsonErr> {
    let start = req
        .start
        .as_ref()
        .map(time_util::parse_json_to_ns)
        .transpose()
        .map_err(|m| err_json(StatusCode::BAD_REQUEST, m))?;
    let end = req
        .end
        .as_ref()
        .map(time_util::parse_json_to_ns)
        .transpose()
        .map_err(|m| err_json(StatusCode::BAD_REQUEST, m))?;
    let (start_ns, end_ns) = time_util::resolve_range(start, end);
    let limit = req.limit.unwrap_or(sql::DEFAULT_LIMIT);
    let direction = Direction::parse(req.direction.as_deref())
        .map_err(|m| err_json(StatusCode::BAD_REQUEST, m))?;

    let rows = execute_logql(
        &state, &headers, &req.query, start_ns, end_ns, limit, direction,
    )
    .await
    .map_err(|(s, m)| err_json(s, m))?;

    Ok(Json(
        json!({ "rows": rows.iter().map(SelectedRow::to_json).collect::<Vec<_>>() }),
    ))
}

// === GET /trace/{trace_id} ===

#[derive(Debug, Deserialize)]
struct TraceParams {
    start: Option<String>,
    end: Option<String>,
    limit: Option<u32>,
}

fn is_valid_trace_id(s: &str) -> bool {
    s.len() == 32
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

async fn trace_lookup(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(trace_id): Path<String>,
    Query(params): Query<TraceParams>,
) -> Result<Json<Value>, JsonErr> {
    let tenant = require_tenant(&state, &headers)
        .await
        .map_err(|(s, m)| err_json(s, m))?;

    if !is_valid_trace_id(&trace_id) {
        return Err(err_json(
            StatusCode::BAD_REQUEST,
            "trace_id must be 32 lowercase hex characters",
        ));
    }

    let start = params
        .start
        .as_deref()
        .map(time_util::parse_str_to_ns)
        .transpose()
        .map_err(|m| err_json(StatusCode::BAD_REQUEST, m))?;
    let end = params
        .end
        .as_deref()
        .map(time_util::parse_str_to_ns)
        .transpose()
        .map_err(|m| err_json(StatusCode::BAD_REQUEST, m))?;
    let time_range = match (start, end) {
        (Some(s), Some(e)) => Some((s, e)),
        (None, None) => None,
        _ => {
            return Err(err_json(
                StatusCode::BAD_REQUEST,
                "start and end must be supplied together",
            ))
        }
    };
    let limit = params.limit.unwrap_or(sql::MAX_LIMIT);

    let translated = sql::translate_trace(&tenant, &trace_id, time_range, limit);
    let rows = run_select(&state.ch, translated).await.map_err(|e| {
        let (s, m) = ch_err(e);
        err_json(s, m)
    })?;

    Ok(Json(
        json!({ "rows": rows.iter().map(SelectedRow::to_json).collect::<Vec<_>>() }),
    ))
}

// === Loki-compatible subset ===

fn rows_to_loki_streams(rows: &[SelectedRow]) -> Vec<Value> {
    let mut streams: BTreeMap<(String, String), Vec<[String; 2]>> = BTreeMap::new();
    for r in rows {
        streams
            .entry((r.service.clone(), r.severity.clone()))
            .or_default()
            .push([r.ts.to_string(), r.body.clone()]);
    }
    streams
        .into_iter()
        .map(|((service, severity), values)| {
            json!({ "stream": { "service": service, "severity": severity }, "values": values })
        })
        .collect()
}

#[derive(Debug, Deserialize)]
struct LokiRangeParams {
    query: String,
    start: Option<String>,
    end: Option<String>,
    limit: Option<u32>,
    direction: Option<String>,
}

async fn loki_query_range(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<LokiRangeParams>,
) -> Result<Json<Value>, JsonErr> {
    let start = params
        .start
        .as_deref()
        .map(time_util::parse_str_to_ns)
        .transpose()
        .map_err(|m| err_loki(StatusCode::BAD_REQUEST, m))?;
    let end = params
        .end
        .as_deref()
        .map(time_util::parse_str_to_ns)
        .transpose()
        .map_err(|m| err_loki(StatusCode::BAD_REQUEST, m))?;
    let (start_ns, end_ns) = time_util::resolve_range(start, end);
    let limit = params.limit.unwrap_or(sql::DEFAULT_LIMIT);
    let direction = Direction::parse(params.direction.as_deref())
        .map_err(|m| err_loki(StatusCode::BAD_REQUEST, m))?;

    let rows = execute_logql(
        &state,
        &headers,
        &params.query,
        start_ns,
        end_ns,
        limit,
        direction,
    )
    .await
    .map_err(|(s, m)| err_loki(s, m))?;

    Ok(Json(json!({
        "status": "success",
        "data": { "resultType": "streams", "result": rows_to_loki_streams(&rows) },
    })))
}

#[derive(Debug, Deserialize)]
struct LokiInstantParams {
    query: String,
    time: Option<String>,
    limit: Option<u32>,
    direction: Option<String>,
}

/// Loki's instant-query endpoint. We don't have a notion of "value at
/// exactly this instant" for log lines, so — like `query_range` — this
/// returns everything in the trailing 1h window up to `time` (default
/// now). Grafana mostly uses `query_range`; this exists so a datasource
/// that calls the instant endpoint still gets a sane answer.
async fn loki_query(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<LokiInstantParams>,
) -> Result<Json<Value>, JsonErr> {
    let end = match params.time.as_deref() {
        Some(t) => {
            time_util::parse_str_to_ns(t).map_err(|m| err_loki(StatusCode::BAD_REQUEST, m))?
        }
        None => time_util::now_ns(),
    };
    let start = end - time_util::DEFAULT_RANGE_NS;
    let limit = params.limit.unwrap_or(sql::DEFAULT_LIMIT);
    let direction = Direction::parse(params.direction.as_deref())
        .map_err(|m| err_loki(StatusCode::BAD_REQUEST, m))?;

    let rows = execute_logql(
        &state,
        &headers,
        &params.query,
        start,
        end,
        limit,
        direction,
    )
    .await
    .map_err(|(s, m)| err_loki(s, m))?;

    Ok(Json(json!({
        "status": "success",
        "data": { "resultType": "streams", "result": rows_to_loki_streams(&rows) },
    })))
}

/// Static label set — good enough for Grafana's Loki datasource "Save &
/// test" (which just needs this to return 200) and for populating the
/// label-name dropdown. Distinct `attrs`/`resource` keys would need a
/// full-table `mapKeys` scan; not worth it for a dropdown.
async fn loki_labels(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, JsonErr> {
    require_tenant(&state, &headers)
        .await
        .map_err(|(s, m)| err_loki(s, m))?;
    Ok(Json(
        json!({ "status": "success", "data": ["service", "severity"] }),
    ))
}

#[derive(Debug, Deserialize)]
struct LabelValuesParams {
    start: Option<String>,
    end: Option<String>,
}

async fn loki_label_values(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
    Query(params): Query<LabelValuesParams>,
) -> Result<Json<Value>, JsonErr> {
    let tenant = require_tenant(&state, &headers)
        .await
        .map_err(|(s, m)| err_loki(s, m))?;

    // Whitelisted match arm literals only — `column` never carries raw
    // user text into the SQL string (see `sql::translate_label_values`).
    let column: &'static str = match name.as_str() {
        "service" => "service",
        "severity" => "severity",
        _ => return Ok(Json(json!({ "status": "success", "data": [] }))),
    };

    let start = params
        .start
        .as_deref()
        .map(time_util::parse_str_to_ns)
        .transpose()
        .map_err(|m| err_loki(StatusCode::BAD_REQUEST, m))?;
    let end = params
        .end
        .as_deref()
        .map(time_util::parse_str_to_ns)
        .transpose()
        .map_err(|m| err_loki(StatusCode::BAD_REQUEST, m))?;
    let (start_ns, end_ns) = time_util::resolve_range(start, end);

    let translated = sql::translate_label_values(column, &tenant, start_ns, end_ns, 1000);
    let mut q = state.ch.query(&translated.sql);
    for b in translated.binds {
        q = bind_one(q, b);
    }
    let values: Vec<String> = q.fetch_all::<String>().await.map_err(|e| {
        let (s, m) = ch_err(e);
        err_loki(s, m)
    })?;

    Ok(Json(json!({ "status": "success", "data": values })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::test_support::FakeAuth;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    fn state(auth: Arc<dyn TenantResolver>) -> AppState {
        AppState {
            // Never dialed in these tests — every case fails (or, for
            // `/loki/api/v1/labels`, succeeds without touching the DB)
            // before a ClickHouse round trip happens.
            ch: clickhouse::Client::default().with_url("http://127.0.0.1:1"),
            auth,
        }
    }

    fn authed_state() -> AppState {
        state(Arc::new(FakeAuth(Some("tenant-a"))))
    }

    fn unauthed_state() -> AppState {
        state(Arc::new(FakeAuth(None)))
    }

    #[tokio::test]
    async fn query_without_api_key_is_401() {
        let app = build_router(authed_state());
        let req = Request::builder()
            .method("POST")
            .uri("/query")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"query":"{service=\"api\"}"}"#))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn query_with_invalid_api_key_is_401() {
        let app = build_router(unauthed_state());
        let req = Request::builder()
            .method("POST")
            .uri("/query")
            .header("content-type", "application/json")
            .header("x-api-key", "nope")
            .body(Body::from(r#"{"query":"{service=\"api\"}"}"#))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn query_with_bad_logql_is_400() {
        let app = build_router(authed_state());
        let req = Request::builder()
            .method("POST")
            .uri("/query")
            .header("content-type", "application/json")
            .header("x-api-key", "k")
            .body(Body::from(r#"{"query":"not logql"}"#))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert!(v["error"].as_str().unwrap().contains("logql parse error"));
    }

    #[tokio::test]
    async fn query_with_empty_selector_is_400() {
        let app = build_router(authed_state());
        let req = Request::builder()
            .method("POST")
            .uri("/query")
            .header("content-type", "application/json")
            .header("x-api-key", "k")
            .body(Body::from(r#"{"query":"{}"}"#))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn trace_lookup_rejects_bad_hex_without_touching_ch() {
        let app = build_router(authed_state());
        let req = Request::builder()
            .method("GET")
            .uri("/trace/not-hex")
            .header("x-api-key", "k")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn trace_lookup_requires_auth() {
        let app = build_router(unauthed_state());
        let req = Request::builder()
            .method("GET")
            .uri("/trace/0123456789abcdef0123456789abcdef")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn loki_labels_is_static_and_needs_no_clickhouse() {
        let app = build_router(authed_state());
        let req = Request::builder()
            .method("GET")
            .uri("/loki/api/v1/labels")
            .header("x-api-key", "k")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            v,
            json!({ "status": "success", "data": ["service", "severity"] })
        );
    }

    #[tokio::test]
    async fn loki_label_values_unknown_label_is_empty_without_clickhouse() {
        let app = build_router(authed_state());
        let req = Request::builder()
            .method("GET")
            .uri("/loki/api/v1/label/nonsense/values")
            .header("x-api-key", "k")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v, json!({ "status": "success", "data": [] }));
    }

    #[tokio::test]
    async fn loki_query_range_bad_logql_is_loki_shaped_error() {
        let app = build_router(authed_state());
        let req = Request::builder()
            .method("GET")
            .uri("/loki/api/v1/query_range?query=not-logql")
            .header("x-api-key", "k")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["status"], "error");
        assert!(v["error"].is_string());
    }

    #[test]
    fn valid_trace_id_check() {
        assert!(is_valid_trace_id("0123456789abcdef0123456789abcdef"));
        assert!(!is_valid_trace_id("0123456789ABCDEF0123456789abcdef")); // uppercase
        assert!(!is_valid_trace_id("short"));
        assert!(!is_valid_trace_id("g123456789abcdef0123456789abcdef")); // non-hex
    }
}
