//! Library half of `logstream-query`: the LogQL-ish parser, its
//! translation to ClickHouse SQL, the tenant-auth seam, and the HTTP
//! handlers. Split out from `main.rs` (which stays a thin CLI/wiring
//! shell) so `tests/clickhouse_integration.rs` can exercise the real
//! `sql::translate*` output against a live ClickHouse without needing
//! to go through HTTP.

pub mod auth;
pub mod handlers;
pub mod logql;
pub mod row;
pub mod sql;
pub mod time_util;
