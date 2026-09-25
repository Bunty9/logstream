//! End-to-end test against a real ClickHouse instance.
//!
//! Skipped unless `LOGSTREAM_IT_CLICKHOUSE_URL` is set — unit tests cover
//! the SQL string/bind generation (`sql.rs`) and the handler auth/parse
//! paths (`handlers.rs`) without a database; this test exists to catch
//! the things only a real ClickHouse can: the `FixedString`/`Map`
//! RowBinary decoding tricks in `row.rs`, whether the generated SQL is
//! actually valid ClickHouse, and that tenant isolation holds end to end.
//!
//! Run it with:
//!
//! ```text
//! docker run -d --name ls-q-ch -p 28123:8123 --ulimit nofile=262144:262144 \
//!   clickhouse/clickhouse-server:24.8-alpine
//! cat clickhouse/init.sql | curl -sS http://127.0.0.1:28123/ --data-binary @-
//! LOGSTREAM_IT_CLICKHOUSE_URL=http://127.0.0.1:28123 cargo test -p logstream-query --test clickhouse_integration
//! docker rm -f ls-q-ch
//! ```

use logstream_query::logql;
use logstream_query::sql::{self, Bind, Direction};

fn client(url: &str) -> clickhouse::Client {
    clickhouse::Client::default()
        .with_url(url)
        .with_database("default")
}

async fn bind_and_fetch(
    ch: &clickhouse::Client,
    t: sql::Translated,
) -> Vec<logstream_query::row::SelectedRow> {
    let mut q = ch.query(&t.sql);
    for b in t.binds {
        q = match b {
            Bind::Str(s) => q.bind(s),
            Bind::I64(n) => q.bind(n),
        };
    }
    q.fetch_all()
        .await
        .expect("query should succeed against a real ClickHouse")
}

/// One row to seed, matching the exact column list/order from
/// `clickhouse/init.sql`.
struct SeedRow<'a> {
    tenant_id: &'a str,
    ts_ns: i64,
    severity: &'a str,
    service: &'a str,
    trace_id: &'a str,
    span_id: &'a str,
    body: &'a str,
}

/// Insert one row via the HTTP interface's `INSERT ... VALUES`. `attrs`
/// deliberately carries one entry so the attrs/resource matcher fallback
/// has something to find.
async fn seed_row(base_url: &str, row: SeedRow<'_>) {
    let SeedRow {
        tenant_id,
        ts_ns,
        severity,
        service,
        trace_id,
        span_id,
        body,
    } = row;
    let sql = format!(
        "INSERT INTO logs (tenant_id, ts, severity, service, trace_id, span_id, body, attrs, resource) \
         VALUES ('{tenant_id}', fromUnixTimestamp64Nano({ts_ns}), '{severity}', '{service}', \
         '{trace_id}', '{span_id}', '{body}', {{'env':'prod'}}, {{'region':'us-east'}})"
    );
    let resp = reqwest_post(base_url, &sql).await;
    assert!(resp.0, "seed insert failed: {}", resp.1);
}

/// Tiny blocking-free HTTP POST helper so this test doesn't need to pull
/// in `reqwest` just to talk to ClickHouse's HTTP interface — `hyper` is
/// already in the dependency tree via `clickhouse`/`axum`, but even that's
/// overkill for "POST this SQL string"; std's blocking primitives aren't
/// available in async tests, so this shells out to `curl`, which every
/// dev/CI image already needs for the container healthcheck anyway.
async fn reqwest_post(base_url: &str, body: &str) -> (bool, String) {
    let out = tokio::process::Command::new("curl")
        .args(["-sS", "-w", "\n%{http_code}", base_url])
        .arg("--data-binary")
        .arg(body)
        .output()
        .await
        .expect("failed to invoke curl");
    let text = String::from_utf8_lossy(&out.stdout).to_string();
    let ok = text.trim_end().ends_with("200");
    (ok, text)
}

macro_rules! skip_without_clickhouse {
    () => {
        match std::env::var("LOGSTREAM_IT_CLICKHOUSE_URL") {
            Ok(url) => url,
            Err(_) => {
                eprintln!(
                    "skipping: set LOGSTREAM_IT_CLICKHOUSE_URL to a running ClickHouse \
                     (see this file's doc comment) to run this test"
                );
                return;
            }
        }
    };
}

#[tokio::test]
async fn selector_regex_filters_trace_and_tenant_isolation() {
    let base_url = skip_without_clickhouse!();
    let ch = client(&base_url);

    let tenant_a = format!("it-tenant-a-{}", std::process::id());
    let tenant_b = format!("it-tenant-b-{}", std::process::id());
    let now_ns = logstream_query::time_util::now_ns();

    seed_row(
        &base_url,
        SeedRow {
            tenant_id: &tenant_a,
            ts_ns: now_ns - 1_000_000_000,
            severity: "ERROR",
            service: "checkout",
            trace_id: "0123456789abcdef0123456789abcdef",
            span_id: "0123456789abcdef",
            body: "payment failed: card declined",
        },
    )
    .await;
    seed_row(
        &base_url,
        SeedRow {
            tenant_id: &tenant_a,
            ts_ns: now_ns,
            severity: "INFO",
            service: "checkout",
            trace_id: "fedcba9876543210fedcba9876543210",
            span_id: "fedcba9876543210",
            body: "order placed",
        },
    )
    .await;
    seed_row(
        &base_url,
        SeedRow {
            tenant_id: &tenant_b,
            ts_ns: now_ns,
            severity: "ERROR",
            service: "checkout",
            trace_id: "11111111111111111111111111111111",
            span_id: "1111111111111111",
            body: "payment failed: card declined",
        },
    )
    .await;

    let start = now_ns - 3_600_000_000_000;
    let end = now_ns + 1_000_000_000;

    // --- plain selector match ---
    let ast = logql::parse(r#"{service="checkout"}"#).unwrap();
    let t = sql::translate(&ast, &tenant_a, start, end, 100, Direction::Backward);
    let rows = bind_and_fetch(&ch, t).await;
    assert_eq!(
        rows.len(),
        2,
        "selector should only see tenant_a's two rows"
    );

    // --- regex matcher (=~), anchored ---
    let ast = logql::parse(r#"{severity=~"ERR.*"}"#).unwrap();
    let t = sql::translate(&ast, &tenant_a, start, end, 100, Direction::Backward);
    let rows = bind_and_fetch(&ch, t).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].severity, "ERROR");

    // --- attrs fallback matcher (env is not a real column) ---
    let ast = logql::parse(r#"{env="prod"}"#).unwrap();
    let t = sql::translate(&ast, &tenant_a, start, end, 100, Direction::Backward);
    let rows = bind_and_fetch(&ch, t).await;
    assert_eq!(rows.len(), 2, "both tenant_a rows carry attrs.env=prod");

    // --- line filter (|=) ---
    let ast = logql::parse(r#"{service="checkout"} |= "declined""#).unwrap();
    let t = sql::translate(&ast, &tenant_a, start, end, 100, Direction::Backward);
    let rows = bind_and_fetch(&ch, t).await;
    assert_eq!(rows.len(), 1);
    assert!(rows[0].body.contains("declined"));

    // --- line filter regex (|~), unanchored ---
    let ast = logql::parse(r#"{service="checkout"} |~ "plac""#).unwrap();
    let t = sql::translate(&ast, &tenant_a, start, end, 100, Direction::Backward);
    let rows = bind_and_fetch(&ch, t).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].body, "order placed");

    // --- FixedString round-trip sanity: trace_id/span_id come back as the
    // exact hex strings inserted, with no NUL padding leaking through. ---
    let t = sql::translate_trace(&tenant_a, "0123456789abcdef0123456789abcdef", None, 10);
    let rows = bind_and_fetch(&ch, t).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].trace_id, "0123456789abcdef0123456789abcdef");
    assert_eq!(rows[0].span_id, "0123456789abcdef");
    assert_eq!(rows[0].attrs, vec![("env".to_string(), "prod".to_string())]);
    assert_eq!(
        rows[0].resource,
        vec![("region".to_string(), "us-east".to_string())]
    );

    // --- tenant isolation: tenant_b must never see tenant_a's trace, even
    // by exact trace_id, and vice versa. ---
    let t = sql::translate_trace(&tenant_b, "0123456789abcdef0123456789abcdef", None, 10);
    let rows = bind_and_fetch(&ch, t).await;
    assert!(rows.is_empty(), "tenant_b must not see tenant_a's rows");

    let ast = logql::parse(r#"{service="checkout"}"#).unwrap();
    let t = sql::translate(&ast, &tenant_b, start, end, 100, Direction::Backward);
    let rows = bind_and_fetch(&ch, t).await;
    assert_eq!(rows.len(), 1, "tenant_b should only see its own row");
    assert_eq!(rows[0].trace_id, "11111111111111111111111111111111");
}

#[tokio::test]
async fn label_values_query() {
    let base_url = skip_without_clickhouse!();
    let ch = client(&base_url);
    let tenant = format!("it-tenant-lv-{}", std::process::id());
    let now_ns = logstream_query::time_util::now_ns();

    seed_row(
        &base_url,
        SeedRow {
            tenant_id: &tenant,
            ts_ns: now_ns,
            severity: "WARN",
            service: "billing",
            trace_id: "22222222222222222222222222222222",
            span_id: "2222222222222222",
            body: "retrying charge",
        },
    )
    .await;

    let start = now_ns - 3_600_000_000_000;
    let end = now_ns + 1_000_000_000;
    let t = sql::translate_label_values("service", &tenant, start, end, 1000);
    let mut q = ch.query(&t.sql);
    for b in t.binds {
        q = match b {
            Bind::Str(s) => q.bind(s),
            Bind::I64(n) => q.bind(n),
        };
    }
    let values: Vec<String> = q.fetch_all().await.unwrap();
    assert_eq!(values, vec!["billing".to_string()]);
}
