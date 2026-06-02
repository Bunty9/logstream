//! Row + batch types mirrored against the ClickHouse `logs` table schema
//! in `clickhouse/init.sql`. Column ordering and naming here must stay in
//! lock-step with that DDL so the `clickhouse` crate's `Row` derive can
//! insert without manual column lists.

use clickhouse::Row;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// One log record as written to ClickHouse.
///
/// Field order matches the `CREATE TABLE logs (...)` definition in
/// `clickhouse/init.sql`. `attrs` and `resource` materialize the
/// `Map(LowCardinality(String), String)` columns; we use `BTreeMap` for
/// deterministic key order so identical records hash identically (helps
/// dedup tooling downstream).
///
/// `ts` is encoded as nanoseconds since Unix epoch — the wire shape that
/// the `clickhouse` crate maps to `DateTime64(9, 'UTC')`.
#[derive(Debug, Clone, Serialize, Deserialize, Row)]
pub struct LogRow {
    pub tenant_id: String,
    pub ts: i64,
    pub severity: String,
    pub service: String,
    pub trace_id: String,
    pub span_id: String,
    pub body: String,
    pub attrs: BTreeMap<String, String>,
    pub resource: BTreeMap<String, String>,
}

/// One ingest hand-off across the bounded mpsc channel: rows are
/// pre-grouped by tenant so the batcher can record per-tenant
/// drop / flush metrics without re-keying.
#[derive(Debug, Clone)]
pub struct TenantBatch {
    pub tenant_id: String,
    pub rows: Vec<LogRow>,
}
