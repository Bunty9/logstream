//! OTLP `ExportLogsServiceRequest` → `LogRow` mapping.
//!
//! Phase 1 keeps this a placeholder so the workspace compiles end-to-end
//! with the protobuf wire types resolved. The Phase 2 implementation will
//! walk `resource_logs[].scope_logs[].log_records[]`, project the OTLP
//! severity numbers onto our `LowCardinality(String)` column, and flatten
//! attributes into the `Map(String, String)` shape (string-coercing
//! non-string values so the schema stays uniform).

use crate::types::LogRow;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;

/// Project an OTLP logs request into the row representation persisted in
/// ClickHouse. Tenant id is supplied by the auth layer — OTLP itself has
/// no tenant concept.
pub fn otlp_to_rows(_tenant_id: String, _req: &ExportLogsServiceRequest) -> Vec<LogRow> {
    // TODO: implement OTLP → LogRow mapping (Phase 2)
    Vec::new()
}
