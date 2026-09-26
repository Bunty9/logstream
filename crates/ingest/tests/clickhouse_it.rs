//! Integration test against a real ClickHouse server, verifying the
//! `LogRow` RowBinary wire-format fixes in `crates/core/src/types.rs`
//! (`FixedString` trace/span ids, `Map` attrs/resource) actually
//! round-trip — the wrong shape either panics (`clickhouse` 0.12's
//! RowBinary `serialize_map`/`deserialize_map` are `unimplemented!()`) or
//! silently corrupts the column (a length-prefixed `String` written where
//! ClickHouse expects fixed-width bytes), neither of which a pure
//! in-process (de)serializer test would catch.
//!
//! Gated on `LOGSTREAM_IT_CLICKHOUSE_URL`; skipped with a printed message
//! otherwise. Throwaway container used to develop/verify this test:
//!
//! ```sh
//! docker run -d --name ls-it-ch -p 18123:8123 \
//!   --ulimit nofile=262144:262144 clickhouse/clickhouse-server:24.8-alpine
//! LOGSTREAM_IT_CLICKHOUSE_URL=http://127.0.0.1:18123 \
//!   cargo test -p logstream-ingest --test clickhouse_it
//! docker rm -f ls-it-ch
//! ```

use logstream_core::{flush, LogRow};
use std::collections::BTreeMap;

#[tokio::test]
async fn insert_and_read_back_roundtrips_map_and_fixedstring() {
    let Ok(url) = std::env::var("LOGSTREAM_IT_CLICKHOUSE_URL") else {
        println!(
            "skipping insert_and_read_back_roundtrips_map_and_fixedstring: \
             set LOGSTREAM_IT_CLICKHOUSE_URL to run against a real ClickHouse server"
        );
        return;
    };

    // Own database: this test drops and recreates `logs`, which must not
    // race the query crate's integration tests using `default.logs` when
    // nextest runs both test binaries in parallel against one server.
    const DB: &str = "logstream_it_ingest";
    clickhouse::Client::default()
        .with_url(&url)
        .query(&format!("CREATE DATABASE IF NOT EXISTS {DB}"))
        .execute()
        .await
        .unwrap();
    let ch = clickhouse::Client::default()
        .with_url(&url)
        .with_database(DB);

    // Apply the real schema (not a hand-simplified stand-in) so this test
    // exercises the actual DDL other agents/services rely on.
    let ddl_path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../clickhouse/init.sql");
    let ddl = std::fs::read_to_string(ddl_path).expect("read clickhouse/init.sql");
    ch.query("DROP TABLE IF EXISTS logs")
        .execute()
        .await
        .unwrap();
    for stmt in ddl.split(';').map(str::trim).filter(|s| !s.is_empty()) {
        ch.query(stmt).execute().await.unwrap();
    }

    let tenant = format!("it-{}", std::process::id());
    let mut attrs = BTreeMap::new();
    attrs.insert("http.method".to_string(), "GET".to_string());
    attrs.insert("http.status_code".to_string(), "200".to_string());
    let mut resource = BTreeMap::new();
    resource.insert("service.name".to_string(), "logstream-it".to_string());

    // Must be "now", not a stale constant: the schema TTLs rows out 30
    // days after `ts` (`clickhouse/init.sql`), and ClickHouse's
    // background TTL merge will silently delete an old-dated row out from
    // under this test before the SELECT below ever runs.
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64;

    let row = LogRow {
        tenant_id: tenant.as_str().into(),
        ts,
        severity: "INFO".to_string(),
        service: "ingest-it".into(),
        trace_id: "0123456789abcdef0123456789abcdef".to_string(), // 32 hex chars
        span_id: "0123456789abcdef".to_string(),                  // 16 hex chars
        body: "hello from the integration test".to_string(),
        attrs: attrs.clone(),
        resource: std::sync::Arc::new(resource.clone()),
    };

    let mut buf = vec![row.clone()];
    flush(&ch, &mut buf)
        .await
        .expect("flush to a real ClickHouse server");
    assert!(buf.is_empty(), "flush drains the buffer on success");

    let got: Vec<LogRow> = ch
        .query("SELECT ?fields FROM logs WHERE tenant_id = ?")
        .bind(&tenant)
        .fetch_all()
        .await
        .expect("read back inserted row");

    assert_eq!(got.len(), 1);
    let back = &got[0];
    assert_eq!(&*back.tenant_id, tenant.as_str());
    assert_eq!(back.ts, row.ts);
    assert_eq!(
        back.trace_id, row.trace_id,
        "FixedString(32) trace_id round-trips"
    );
    assert_eq!(
        back.span_id, row.span_id,
        "FixedString(16) span_id round-trips"
    );
    assert_eq!(back.attrs, attrs, "Map(...) attrs round-trips");
    assert_eq!(*back.resource, resource, "Map(...) resource round-trips");

    ch.query("DROP TABLE logs").execute().await.ok();
}
