//! Tenant authentication. API keys are blake3-hashed, looked up in
//! Postgres as the source of truth, and cached in Redis with a 60s TTL.
//! The cache is the hot path; Postgres is consulted on cache miss and
//! revocation lag is bounded by the TTL.
//!
//! Unknown keys are negative-cached too (`"\0"` sentinel, 10s TTL — not
//! `""`, which collides with a legitimately empty `tenant_id`; Postgres
//! `TEXT` can't store a NUL byte, so no real tenant id can ever equal it):
//! without it, a flood of random/garbage keys (typos, a leaked-then-rotated
//! key still configured somewhere, an attacker probing) would hit Postgres
//! on every single request forever, since a miss is never cached.
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
//! a valid key. Postgres is the source of truth, so a Postgres failure
//! (whether or not Redis had already failed) is reported as `Err` rather
//! than silently treated as "key not found": OTLP exporters retry a `503`
//! but drop the batch on a `401`, so collapsing "we can't tell" into "the
//! key is invalid" would mean an outage quietly drops every tenant's data
//! instead of applying backpressure.

use http::HeaderMap;
use redis::AsyncCommands;
use sqlx::PgPool;
use std::future::Future;
use std::pin::Pin;

/// How long an unknown/revoked key is negative-cached in Redis before the
/// next lookup is allowed to hit Postgres again.
const NEGATIVE_CACHE_TTL_SECS: u64 = 10;
/// Negative-cache marker for "no tenant found". Not `""`: Postgres `TEXT`
/// can hold an empty string, so a real (if oddly provisioned) tenant id
/// could be `""` and would then be misread as a cache miss turned
/// negative-cache hit. `"\0"` is safe — Postgres `TEXT` cannot contain a
/// NUL byte, so no real `tenant_id` can ever equal this sentinel.
const NEGATIVE_CACHE_SENTINEL: &str = "\0";
/// How long a resolved tenant id is cached in Redis.
const POSITIVE_CACHE_TTL_SECS: u64 = 60;

/// Tenant auth backend failure — Postgres unreachable/erroring, i.e. we
/// genuinely don't know whether the key is valid. Distinct from
/// `Ok(None)`, which means Postgres was reachable and definitively has no
/// matching, unrevoked key.
#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("tenant auth backend unavailable: {0}")]
    Backend(#[from] sqlx::Error),
}

/// Tenant-key resolver shared by the ingest and query services.
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
    /// `Ok(None)` means Postgres was reachable and definitively has no
    /// matching, unrevoked key (including a negative-cache hit) — callers
    /// map that to `401`. `Err` means the backend itself is unavailable —
    /// callers map that to `503`, since we can't tell whether the key is
    /// valid. A Redis error (read or write) is logged and treated as a
    /// cache miss / no-op, never as a rejection: Postgres remains
    /// authoritative.
    pub async fn lookup(&self, api_key: &str) -> Result<Option<String>, AuthError> {
        let key_hash = blake3::hash(api_key.as_bytes()).to_hex().to_string();
        let cache_key = format!("tenant:{key_hash}");

        let mut redis = self.redis.clone();
        match redis.get::<_, Option<String>>(&cache_key).await {
            Ok(Some(cached)) if cached == NEGATIVE_CACHE_SENTINEL => return Ok(None), // negative-cache hit
            Ok(Some(tenant_id)) => return Ok(Some(tenant_id)),
            Ok(None) => {}
            Err(err) => tracing::warn!(%err, "redis lookup failed, falling back to postgres"),
        }

        let row: Option<(String,)> = sqlx::query_as(
            "SELECT tenant_id FROM api_keys WHERE key_hash = $1 AND revoked_at IS NULL",
        )
        .bind(&key_hash)
        .fetch_optional(&self.pg)
        .await
        .inspect_err(|err| tracing::error!(%err, "postgres tenant lookup failed"))?;

        let Some((tenant_id,)) = row else {
            if let Err(err) = redis
                .set_ex::<_, _, ()>(&cache_key, NEGATIVE_CACHE_SENTINEL, NEGATIVE_CACHE_TTL_SECS)
                .await
            {
                tracing::warn!(%err, "redis negative-cache write failed");
            }
            return Ok(None);
        };

        if let Err(err) = redis
            .set_ex::<_, _, ()>(&cache_key, &tenant_id, POSITIVE_CACHE_TTL_SECS)
            .await
        {
            // A valid key must not be rejected just because the cache write
            // failed — log and serve the result anyway.
            tracing::warn!(%err, "redis cache write failed");
        }

        Ok(Some(tenant_id))
    }
}

/// Object-safe tenant lookup shared by the ingest and query services, so
/// handler tests in either crate can swap in a static/fake double instead
/// of standing up Postgres + Redis (`async fn` in traits isn't dyn-safe
/// yet, hence the manual boxed future). `TenantAuth::lookup` above is the
/// real implementation.
pub trait TenantLookup: Send + Sync {
    fn lookup<'a>(
        &'a self,
        api_key: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Option<String>, AuthError>> + Send + 'a>>;
}

impl TenantLookup for TenantAuth {
    fn lookup<'a>(
        &'a self,
        api_key: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Option<String>, AuthError>> + Send + 'a>> {
        // Calls the inherent `TenantAuth::lookup` (Rust resolves inherent
        // methods before trait methods, so this isn't self-recursive).
        Box::pin(TenantAuth::lookup(self, api_key))
    }
}

/// Pull the API key out of `x-api-key`, falling back to
/// `Authorization: Bearer <key>` — OTel exporters and most HTTP clients
/// commonly configure either. No allocation: borrows straight out of the
/// header map.
pub fn api_key_from_headers(headers: &HeaderMap) -> Option<&str> {
    if let Some(v) = headers.get("x-api-key").and_then(|v| v.to_str().ok()) {
        return Some(v);
    }
    headers
        .get(http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderValue;

    #[test]
    fn prefers_x_api_key() {
        let mut headers = HeaderMap::new();
        headers.insert("x-api-key", HeaderValue::from_static("k1"));
        headers.insert(
            http::header::AUTHORIZATION,
            HeaderValue::from_static("Bearer k2"),
        );
        assert_eq!(api_key_from_headers(&headers), Some("k1"));
    }

    #[test]
    fn falls_back_to_bearer() {
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::AUTHORIZATION,
            HeaderValue::from_static("Bearer k2"),
        );
        assert_eq!(api_key_from_headers(&headers), Some("k2"));
    }

    #[test]
    fn missing_both() {
        assert_eq!(api_key_from_headers(&HeaderMap::new()), None);
    }

    #[test]
    fn negative_cache_sentinel_cannot_equal_a_real_tenant_id() {
        // The whole point of the sentinel: unlike the old `""` marker, a
        // real (Postgres `TEXT`) tenant id can never equal it, so a
        // negative-cache hit can never be misread as "tenant id is empty"
        // or vice versa.
        assert_ne!(NEGATIVE_CACHE_SENTINEL, "");
        assert!(NEGATIVE_CACHE_SENTINEL.contains('\0'));
    }
}
