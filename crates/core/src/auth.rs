//! Tenant authentication. API keys are SHA-class-hashed with blake3, looked
//! up in Postgres as the source of truth, and cached in Redis with a 60s
//! TTL. The cache is the hot path; Postgres is consulted on cache miss
//! and revocation lag is bounded by the TTL.

use redis::AsyncCommands;
use sqlx::PgPool;

/// Tenant-key resolver shared by the ingest endpoint.
pub struct TenantAuth {
    redis: redis::Client,
    pg: PgPool,
}

impl TenantAuth {
    pub fn new(redis: redis::Client, pg: PgPool) -> Self {
        Self { redis, pg }
    }

    /// Resolve an `x-api-key` header value to a tenant id.
    ///
    /// Returns `None` on missing key, revoked key, or any transport error —
    /// the ingest handler upgrades that to `401`. Cache misses populate the
    /// Redis entry with a 60s TTL so a revocation propagates within one
    /// TTL window (acceptable for log ingestion).
    pub async fn lookup(&self, api_key: &str) -> Option<String> {
        let mut r = self.redis.get_multiplexed_async_connection().await.ok()?;
        let cache_key = format!("tenant:{}", api_key);
        if let Ok(Some(tenant_id)) = r.get::<_, Option<String>>(&cache_key).await {
            return Some(tenant_id);
        }
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT tenant_id FROM api_keys WHERE key_hash = $1 AND revoked_at IS NULL",
        )
        .bind(blake3::hash(api_key.as_bytes()).to_hex().to_string())
        .fetch_optional(&self.pg)
        .await
        .ok()?;
        if let Some((tenant_id,)) = row {
            let _: () = r.set_ex(&cache_key, &tenant_id, 60).await.ok()?;
            Some(tenant_id)
        } else {
            None
        }
    }
}
