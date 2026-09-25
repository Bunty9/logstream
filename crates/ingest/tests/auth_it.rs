//! Integration test against real Postgres + Redis, verifying the
//! `crates/core/src/auth.rs` fix: the Redis cache key is
//! `tenant:{blake3(api_key)}`, never `tenant:{api_key}` — caching under
//! the plaintext key would leak live secrets into Redis.
//!
//! Gated on `LOGSTREAM_IT_PG_URL` + `LOGSTREAM_IT_REDIS_URL`; skipped with
//! a printed message otherwise. Throwaway containers used to develop/
//! verify this test:
//!
//! ```sh
//! docker run -d --name ls-it-pg -p 15432:5432 -e POSTGRES_PASSWORD=pw postgres:16-alpine
//! docker run -d --name ls-it-redis -p 16379:6379 redis:7-alpine
//! LOGSTREAM_IT_PG_URL=postgres://postgres:pw@127.0.0.1:15432/postgres \
//! LOGSTREAM_IT_REDIS_URL=redis://127.0.0.1:16379 \
//!   cargo test -p logstream-ingest --test auth_it
//! docker rm -f ls-it-pg ls-it-redis
//! ```

use logstream_core::TenantAuth;
use redis::AsyncCommands;
use sqlx::postgres::PgPoolOptions;

#[tokio::test]
async fn cache_key_is_hashed_not_plaintext() {
    let (Ok(pg_url), Ok(redis_url)) = (
        std::env::var("LOGSTREAM_IT_PG_URL"),
        std::env::var("LOGSTREAM_IT_REDIS_URL"),
    ) else {
        println!(
            "skipping cache_key_is_hashed_not_plaintext: set LOGSTREAM_IT_PG_URL and \
             LOGSTREAM_IT_REDIS_URL to run against real Postgres + Redis"
        );
        return;
    };

    let pg = PgPoolOptions::new()
        .max_connections(4)
        .connect(&pg_url)
        .await
        .expect("connect to postgres");

    let migration_path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations/0001_init.sql"
    );
    let migration = std::fs::read_to_string(migration_path).expect("read migrations/0001_init.sql");
    for stmt in migration
        .split(';')
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        sqlx::query(stmt)
            .execute(&pg)
            .await
            .expect("apply migration statement");
    }

    let api_key = "TESTKEY-it";
    let key_hash = blake3::hash(api_key.as_bytes()).to_hex().to_string();
    sqlx::query(
        "INSERT INTO api_keys (key_hash, tenant_id) VALUES ($1, $2) \
         ON CONFLICT (key_hash) DO UPDATE SET revoked_at = NULL",
    )
    .bind(&key_hash)
    .bind("demo-it")
    .execute(&pg)
    .await
    .expect("seed api key");

    let redis_client = redis::Client::open(redis_url).expect("open redis client");
    let manager = redis_client
        .get_connection_manager()
        .await
        .expect("redis connection manager");
    let auth = TenantAuth::new(manager, pg);

    let tenant = auth.lookup(api_key).await;
    assert_eq!(
        tenant.as_deref(),
        Some("demo-it"),
        "lookup resolves the seeded tenant"
    );

    let mut raw = redis_client
        .get_multiplexed_async_connection()
        .await
        .expect("raw redis connection");

    let plaintext: Option<String> = raw
        .get(format!("tenant:{api_key}"))
        .await
        .expect("redis GET");
    assert!(
        plaintext.is_none(),
        "plaintext api key must never be cached as a redis key"
    );

    let hashed: Option<String> = raw
        .get(format!("tenant:{key_hash}"))
        .await
        .expect("redis GET");
    assert_eq!(
        hashed.as_deref(),
        Some("demo-it"),
        "cache is keyed by the blake3 hash"
    );
}
