//! Tenant-resolution seam.
//!
//! Handlers depend on `dyn TenantResolver` rather than
//! `logstream_core::TenantAuth` directly, so handler tests can swap in a
//! fake and exercise the 401 paths (and everything downstream of a
//! resolved tenant) without standing up Redis or Postgres. It's a plain
//! hand-rolled trait object — no `async-trait` dependency — since a
//! boxed future is a one-liner here.

use axum::http::{header::AUTHORIZATION, HeaderMap};
use std::future::Future;
use std::pin::Pin;

pub trait TenantResolver: Send + Sync {
    /// Resolve an API key to a tenant id, or `None` if it's missing/revoked.
    fn lookup<'a>(
        &'a self,
        api_key: &'a str,
    ) -> Pin<Box<dyn Future<Output = Option<String>> + Send + 'a>>;
}

impl TenantResolver for logstream_core::TenantAuth {
    fn lookup<'a>(
        &'a self,
        api_key: &'a str,
    ) -> Pin<Box<dyn Future<Output = Option<String>> + Send + 'a>> {
        // Calls the inherent `TenantAuth::lookup` (Rust resolves inherent
        // methods before trait methods, so this isn't self-recursive).
        Box::pin(self.lookup(api_key))
    }
}

/// Pull the API key out of `x-api-key`, falling back to
/// `Authorization: Bearer <key>`.
pub fn extract_api_key(headers: &HeaderMap) -> Option<&str> {
    if let Some(v) = headers.get("x-api-key").and_then(|v| v.to_str().ok()) {
        return Some(v);
    }
    headers
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
}

#[cfg(test)]
pub mod test_support {
    use super::TenantResolver;
    use std::future::Future;
    use std::pin::Pin;

    /// A `TenantResolver` double: `Some(tenant)` accepts every key,
    /// `None` rejects every key. Used by handler tests to avoid a real
    /// Redis/Postgres round trip.
    pub struct FakeAuth(pub Option<&'static str>);

    impl TenantResolver for FakeAuth {
        fn lookup<'a>(
            &'a self,
            _api_key: &'a str,
        ) -> Pin<Box<dyn Future<Output = Option<String>> + Send + 'a>> {
            let tenant = self.0.map(str::to_string);
            Box::pin(async move { tenant })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    #[test]
    fn prefers_x_api_key() {
        let mut headers = HeaderMap::new();
        headers.insert("x-api-key", HeaderValue::from_static("k1"));
        headers.insert(AUTHORIZATION, HeaderValue::from_static("Bearer k2"));
        assert_eq!(extract_api_key(&headers), Some("k1"));
    }

    #[test]
    fn falls_back_to_bearer() {
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, HeaderValue::from_static("Bearer k2"));
        assert_eq!(extract_api_key(&headers), Some("k2"));
    }

    #[test]
    fn missing_both() {
        assert_eq!(extract_api_key(&HeaderMap::new()), None);
    }
}
