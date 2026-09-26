//! Tenant authentication. API keys are blake3-hashed, looked up in
//! Postgres as the source of truth, and cached in Redis with a 60s TTL.
//! A 10s in-process cache sits in front of Redis, so revocation lag is up
//! to ~70s per process (60s Redis + 10s local). Misses are cached too, so
//! a key looked up just before it was issued may be rejected for up to
//! ~20s (10s Redis negative entry + 10s local).
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
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::RwLock;
use std::time::{Duration, Instant};

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

/// How long a resolved lookup (positive or negative) is cached in the
/// in-process `TenantAuth::local_cache` before the next lookup is allowed
/// to hit Redis again. Deliberately shorter than Redis's own 60s positive
/// TTL: this layer only exists to absorb the per-request Redis round trip
/// under load, so a small extra bound on revocation lag is an acceptable
/// trade for that.
const LOCAL_CACHE_TTL: Duration = Duration::from_secs(10);
/// Once the in-process cache grows past this many entries, it's cleared
/// outright on the next write rather than evicting individually — see
/// `TenantAuth::local_cache`'s doc for why that's an acceptable trade
/// here.
const LOCAL_CACHE_MAX_ENTRIES: usize = 100_000;

/// Tenant-key resolver shared by the ingest and query services.
pub struct TenantAuth {
    redis: redis::aio::ConnectionManager,
    pg: PgPool,
    /// In-process cache in front of Redis, keyed by the same blake3
    /// `key_hash` used for the Redis cache key. Under sustained ingest
    /// load a Redis round trip *per request* is real, measurable
    /// per-request latency and connection pressure — this absorbs almost
    /// all of it for the common case of a small, steady set of tenant
    /// keys hammering the endpoint.
    ///
    /// `std::sync::RwLock`, not an async lock: the critical section is a
    /// plain `HashMap` lookup/insert with no `.await` inside, so a
    /// blocking lock held for a few nanoseconds is cheaper and simpler
    /// than routing it through an async mutex.
    ///
    /// ponytail: one global lock and a full `clear()` past
    /// `LOCAL_CACHE_MAX_ENTRIES`, instead of sharding or real LRU
    /// eviction — fine for a process's realistic tenant-key cardinality
    /// (this is a cache of *distinct API keys seen recently*, not of
    /// tenants or requests); revisit if a profile ever shows contention
    /// or eviction thrashing here.
    local_cache: LocalCache,
}

impl TenantAuth {
    pub fn new(redis: redis::aio::ConnectionManager, pg: PgPool) -> Self {
        Self {
            redis,
            pg,
            local_cache: RwLock::new(HashMap::new()),
        }
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

        if let Some(tenant_id) = self.local_cache_get(&key_hash) {
            return Ok(tenant_id);
        }

        let cache_key = format!("tenant:{key_hash}");
        let mut redis = self.redis.clone();
        match redis.get::<_, Option<String>>(&cache_key).await {
            Ok(Some(cached)) if cached == NEGATIVE_CACHE_SENTINEL => {
                self.local_cache_put(key_hash, None);
                return Ok(None); // negative-cache hit
            }
            Ok(Some(tenant_id)) => {
                self.local_cache_put(key_hash, Some(tenant_id.clone()));
                return Ok(Some(tenant_id));
            }
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
            self.local_cache_put(key_hash, None);
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

        self.local_cache_put(key_hash, Some(tenant_id.clone()));
        Ok(Some(tenant_id))
    }

    /// `None` means "no unexpired local cache entry" (caller must fall
    /// through to Redis/Postgres) — distinct from `Some(None)`, a cached
    /// negative result.
    fn local_cache_get(&self, key_hash: &str) -> Option<Option<String>> {
        local_cache_get(&self.local_cache, key_hash)
    }

    fn local_cache_put(&self, key_hash: String, tenant_id: Option<String>) {
        local_cache_put(&self.local_cache, key_hash, tenant_id);
    }
}

// Free functions (rather than methods) so the cache logic is unit-testable
// against a bare `RwLock<HashMap<...>>` without standing up the real
// `redis::aio::ConnectionManager`/`PgPool` a `TenantAuth` needs.
type LocalCache = RwLock<HashMap<String, (Option<String>, Instant)>>;

fn local_cache_get(cache: &LocalCache, key_hash: &str) -> Option<Option<String>> {
    let cache = cache.read().unwrap_or_else(|e| e.into_inner());
    let (tenant_id, cached_at) = cache.get(key_hash)?;
    (cached_at.elapsed() < LOCAL_CACHE_TTL).then(|| tenant_id.clone())
}

fn local_cache_put(cache: &LocalCache, key_hash: String, tenant_id: Option<String>) {
    let mut cache = cache.write().unwrap_or_else(|e| e.into_inner());
    if cache.len() >= LOCAL_CACHE_MAX_ENTRIES {
        cache.clear();
    }
    cache.insert(key_hash, (tenant_id, Instant::now()));
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
    fn local_cache_hit_and_miss() {
        let cache: LocalCache = RwLock::new(HashMap::new());
        assert_eq!(local_cache_get(&cache, "h1"), None, "empty cache misses");

        local_cache_put(&cache, "h1".into(), Some("tenant-a".into()));
        assert_eq!(
            local_cache_get(&cache, "h1"),
            Some(Some("tenant-a".to_string()))
        );
        assert_eq!(
            local_cache_get(&cache, "h2"),
            None,
            "distinct key still misses"
        );
    }

    #[test]
    fn local_cache_caches_negative_results_too() {
        let cache: LocalCache = RwLock::new(HashMap::new());
        local_cache_put(&cache, "h1".into(), None);
        // `Some(None)`: a cache hit whose cached value is "no tenant" —
        // must be distinguishable from `None` ("no cache entry at all").
        assert_eq!(local_cache_get(&cache, "h1"), Some(None));
    }

    #[test]
    fn local_cache_entry_expires_after_ttl() {
        let cache: LocalCache = RwLock::new(HashMap::new());
        let expired_at = Instant::now()
            .checked_sub(LOCAL_CACHE_TTL + Duration::from_secs(1))
            .unwrap();
        cache
            .write()
            .unwrap()
            .insert("h1".into(), (Some("tenant-a".into()), expired_at));
        assert_eq!(
            local_cache_get(&cache, "h1"),
            None,
            "an entry older than LOCAL_CACHE_TTL must be treated as a miss"
        );
    }

    #[test]
    fn local_cache_clears_at_capacity_instead_of_growing_unbounded() {
        let cache: LocalCache = RwLock::new(HashMap::new());
        cache
            .write()
            .unwrap()
            .extend((0..LOCAL_CACHE_MAX_ENTRIES).map(|i| {
                (
                    format!("existing-{i}"),
                    (Some("tenant-a".to_string()), Instant::now()),
                )
            }));
        local_cache_put(&cache, "new-key".into(), Some("tenant-b".into()));
        let guard = cache.read().unwrap();
        assert_eq!(
            guard.len(),
            1,
            "hitting the cap must clear the old entries, not grow past it"
        );
        assert!(guard.contains_key("new-key"));
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
