//! Tenant authentication. API keys are blake3-hashed, looked up in
//! Postgres as the source of truth, and cached in Redis with a 60s TTL.
//! The cache is the hot path; Postgres is consulted on cache miss and
//! revocation lag is bounded by the TTL.
//!
//! The Redis connection is a `ConnectionManager` built once at startup
//! (auto-reconnecting, cheap to clone/share) rather than a fresh
//! multiplexed connection per lookup — opening a connection per request is
//! expensive and was the previous behavior. The cache key is
//! `tenant:{blake3(api_key)}`, not `tenant:{api_key}`: caching under the
//! plaintext key would leak live secrets into Redis (visible to anyone who
//! can `KEYS`/`SCAN`/dump the cache).
//!
//! Availability posture: Redis is a cache, not a dependency — if it errors
//! on read or write we fall back to / rely on Postgres rather than reject
//! a valid key. Postgres errors fail closed (`None`, logged at `warn`)
//! since it's the source of truth.

use redis::AsyncCommands;
use sqlx::PgPool;

/// Tenant-key resolver shared by the ingest endpoint.
pub struct TenantAuth {
    redis: redis::aio::ConnectionManager,
    pg: PgPool,
}

impl TenantAuth {
    pub fn new(redis: redis::aio::ConnectionManager, pg: PgPool) -> Self {
        Self { redis, pg }
    }

    /// Resolve an `x-api-key` header value to a tenant id.
    ///
    /// Returns `None` on missing key, revoked key, or a Postgres error —
    /// the ingest handler upgrades that to `401`. A Redis error (read or
    /// write) is logged and treated as a cache miss / no-op, never as a
    /// rejection: Postgres remains authoritative.
    pub async fn lookup(&self, api_key: &str) -> Option<String> {
        let key_hash = blake3::hash(api_key.as_bytes()).to_hex().to_string();
        let cache_key = format!("tenant:{key_hash}");

        let mut redis = self.redis.clone();
        match redis.get::<_, Option<String>>(&cache_key).await {
            Ok(Some(tenant_id)) => return Some(tenant_id),
            Ok(None) => {}
            Err(err) => tracing::warn!(%err, "redis lookup failed, falling back to postgres"),
        }

        let row: Option<(String,)> = sqlx::query_as(
            "SELECT tenant_id FROM api_keys WHERE key_hash = $1 AND revoked_at IS NULL",
        )
        .bind(&key_hash)
        .fetch_optional(&self.pg)
        .await
        .map_err(|err| tracing::warn!(%err, "postgres tenant lookup failed"))
        .ok()?;

        let (tenant_id,) = row?;

        if let Err(err) = redis.set_ex::<_, _, ()>(&cache_key, &tenant_id, 60).await {
            // A valid key must not be rejected just because the cache write
            // failed — log and serve the result anyway.
            tracing::warn!(%err, "redis cache write failed");
        }

        Some(tenant_id)
    }
}
