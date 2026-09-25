//! Tenant-resolution seam — thin re-export of the shared
//! `logstream_core::auth` trait/helper so ingest and query don't each keep
//! their own copy of the same trait + header-parsing function (see that
//! module for the real implementation, the `TenantAuth` impl, and its
//! tests).
//!
//! Handlers depend on `dyn TenantResolver` rather than
//! `logstream_core::TenantAuth` directly, so handler tests can swap in a
//! fake and exercise the 401/503 paths (and everything downstream of a
//! resolved tenant) without standing up Redis or Postgres.

pub use logstream_core::auth::{
    api_key_from_headers as extract_api_key, TenantLookup as TenantResolver,
};

#[cfg(test)]
pub mod test_support {
    use logstream_core::{AuthError, TenantLookup};
    use std::future::Future;
    use std::pin::Pin;

    /// A `TenantResolver` double: `Some(tenant)` accepts every key,
    /// `None` rejects every key. Used by handler tests to avoid a real
    /// Redis/Postgres round trip.
    pub struct FakeAuth(pub Option<&'static str>);

    impl TenantLookup for FakeAuth {
        fn lookup<'a>(
            &'a self,
            _api_key: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<Option<String>, AuthError>> + Send + 'a>> {
            let tenant = self.0.map(str::to_string);
            Box::pin(async move { Ok(tenant) })
        }
    }

    /// Always reports a backend outage — exercises the `503` mapping for
    /// `TenantLookup::lookup` returning `Err` (Postgres unreachable).
    pub struct FailingAuth;

    impl TenantLookup for FailingAuth {
        fn lookup<'a>(
            &'a self,
            _api_key: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<Option<String>, AuthError>> + Send + 'a>> {
            Box::pin(async {
                Err(AuthError::Backend(sqlx::Error::Protocol(
                    "simulated backend failure".into(),
                )))
            })
        }
    }
}
