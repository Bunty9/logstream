//! Regression test for the `GlobalConcurrencyLimitLayer` fix in
//! `crates/query/src/main.rs`.
//!
//! axum builds one `Service` per route, and `Router::layer` applies the
//! given `Layer` independently to each of them. A plain
//! `tower::limit::ConcurrencyLimitLayer` therefore hands *every* route its
//! own fresh semaphore — a `Router` with N routes wrapped in
//! `ConcurrencyLimitLayer::new(16)` actually allows `16 * N` requests in
//! flight, not 16. `GlobalConcurrencyLimitLayer` fixes this by owning a
//! single `Arc<Semaphore>` that every `.layer()` call (one per route)
//! shares, so cloning/reapplying it never creates a new semaphore.
//!
//! This test builds the same "merge two routes, then `.layer()` the
//! merged router" shape `main.rs` uses and proves the limit is enforced
//! globally across both routes, not per-route.

use axum::{body::Body, http::Request, routing::get, Router};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tower::limit::GlobalConcurrencyLimitLayer;
use tower::ServiceExt;

#[tokio::test]
async fn global_limit_is_shared_across_merged_routes() {
    let started_a = Arc::new(Notify::new());
    let hold_a = Arc::new(Notify::new());

    let route_a = {
        let started_a = started_a.clone();
        let hold_a = hold_a.clone();
        Router::new().route(
            "/a",
            get(move || {
                let started_a = started_a.clone();
                let hold_a = hold_a.clone();
                async move {
                    started_a.notify_one();
                    hold_a.notified().await;
                    "a"
                }
            }),
        )
    };
    let route_b = Router::new().route("/b", get(|| async { "b" }));

    // Same shape as main.rs: merge routes, then wrap the merged router in
    // one `GlobalConcurrencyLimitLayer` — capacity 1.
    let app = route_a
        .merge(route_b)
        .layer(GlobalConcurrencyLimitLayer::new(1));

    // Request 1 occupies the single global permit and parks inside the
    // handler until told to proceed.
    let app_a = app.clone();
    let task_a = tokio::spawn(async move {
        app_a
            .oneshot(Request::builder().uri("/a").body(Body::empty()).unwrap())
            .await
    });
    started_a.notified().await;

    // Request 2, to a *different* route, must block behind the same
    // permit if the limit is truly global. Race it against a short
    // timeout: with the old per-route-semaphore bug it would complete
    // immediately (a fresh, untouched semaphore for `/b`).
    let app_b = app.clone();
    let task_b = tokio::spawn(async move {
        app_b
            .oneshot(Request::builder().uri("/b").body(Body::empty()).unwrap())
            .await
    });
    let raced = tokio::time::timeout(Duration::from_millis(200), task_b).await;
    assert!(
        raced.is_err(),
        "/b must be blocked by /a holding the one global permit — it completed instead, \
         meaning the two routes have independent limiters"
    );

    // Release /a; both requests should now complete.
    hold_a.notify_one();
    let resp_a = task_a.await.unwrap().unwrap();
    assert_eq!(resp_a.status(), 200);
}
