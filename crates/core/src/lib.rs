//! logstream-core — shared types, tenant auth, OTLP→ClickHouse mapping, and
//! the bounded-channel batcher actor.
//!
//! Public surface intentionally narrow: ingest and query binaries import
//! these four modules and assemble the pipeline.

pub mod auth;
pub mod batcher;
pub mod otlp;
pub mod types;

pub use auth::{api_key_from_headers, AuthError, TenantAuth, TenantLookup};
pub use batcher::{flush, run_batcher};
pub use otlp::{otlp_to_rows, record_count};
pub use types::{LogRow, TenantBatch};
