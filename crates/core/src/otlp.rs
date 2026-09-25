//! OTLP `ExportLogsServiceRequest` → `LogRow` mapping.
//!
//! Walks `resource_logs[].scope_logs[].log_records[]`, projects OTLP
//! severity numbers onto our `LowCardinality(String)` column, and flattens
//! attributes into the `Map(String, String)` shape (string-coercing
//! non-string values so the schema stays uniform).

use crate::types::LogRow;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{
    any_value::Value as AnyValueKind, AnyValue, KeyValue,
};
use opentelemetry_proto::tonic::logs::v1::LogRecord;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Accepted timestamp window, relative to `now`: the `logs` table is
/// `PARTITION BY toYYYYMMDD(ts)` with a 30-day TTL, and ClickHouse refuses
/// an INSERT that touches more than 100 partitions — so a single client
/// with a wrong clock (or backfilling months of history) mixed into a
/// multi-tenant batch can fail the whole flush and drop every tenant's
/// acked rows. Clamping every accepted `ts` to `[now - TTL, now + skew]`
/// keeps a batch's partition spread bounded no matter what a client sends.
const TS_WINDOW_PAST_NS: i64 = 30 * 24 * 60 * 60 * 1_000_000_000; // 30 days, matches the table TTL
const TS_WINDOW_FUTURE_NS: i64 = 60 * 60 * 1_000_000_000; // 1h of tolerated clock skew

/// Project an OTLP logs request into the row representation persisted in
/// ClickHouse. Tenant id is supplied by the auth layer — OTLP itself has
/// no tenant concept.
pub fn otlp_to_rows(tenant_id: String, req: &ExportLogsServiceRequest) -> Vec<LogRow> {
    let total: usize = req
        .resource_logs
        .iter()
        .flat_map(|rl| rl.scope_logs.iter())
        .map(|sl| sl.log_records.len())
        .sum();
    let mut rows = Vec::with_capacity(total);

    // Built once per request/resource and cloned (an `Arc` refcount bump)
    // per row rather than deep-cloned — see `LogRow`'s doc comment.
    let tenant_id: Arc<str> = Arc::from(tenant_id);
    let now = SystemTime::now();

    for resource_logs in &req.resource_logs {
        let resource = resource_logs
            .resource
            .as_ref()
            .map(|r| flatten_kvs(&r.attributes))
            .unwrap_or_default();
        let service: Arc<str> = resource
            .get("service.name")
            .map(|s| Arc::from(s.as_str()))
            .unwrap_or_else(|| Arc::from("unknown"));
        let resource: Arc<BTreeMap<String, String>> = Arc::new(resource);

        for scope_logs in &resource_logs.scope_logs {
            for record in &scope_logs.log_records {
                let (ts, clamped) = row_ts_at(record, now);
                if clamped {
                    metrics::counter!("logstream_ts_clamped_total").increment(1);
                }
                rows.push(LogRow {
                    tenant_id: tenant_id.clone(),
                    ts,
                    severity: row_severity(record),
                    service: service.clone(),
                    trace_id: hex_id(&record.trace_id, 16),
                    span_id: hex_id(&record.span_id, 8),
                    body: record
                        .body
                        .as_ref()
                        .map(any_value_to_string)
                        .unwrap_or_default(),
                    attrs: flatten_kvs(&record.attributes),
                    resource: resource.clone(),
                });
            }
        }
    }
    rows
}

/// `time_unix_nano` if set and within `[now - 30d, now + 1h]`, else
/// `observed_time_unix_nano` under the same window, else `now` — see the
/// module-level `TS_WINDOW_*` docs for why out-of-window timestamps are
/// rejected instead of trusted verbatim. `now` is a parameter (rather than
/// reading `SystemTime::now()` internally) so tests are deterministic;
/// `otlp_to_rows` is the only caller and always passes real wall-clock
/// time. Returns `(ts, was_clamped)`, where `was_clamped` is true whenever
/// the result isn't `record.time_unix_nano` taken as-is.
fn row_ts_at(record: &LogRecord, now: SystemTime) -> (i64, bool) {
    let now_ns = now
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos().min(i64::MAX as u128) as i64)
        .unwrap_or(0);

    let in_window = |nanos: u64| -> Option<i64> {
        if nanos == 0 {
            return None;
        }
        let ts = nanos.min(i64::MAX as u64) as i64;
        let lo = now_ns.saturating_sub(TS_WINDOW_PAST_NS);
        let hi = now_ns.saturating_add(TS_WINDOW_FUTURE_NS);
        (ts >= lo && ts <= hi).then_some(ts)
    };

    if let Some(ts) = in_window(record.time_unix_nano) {
        return (ts, false);
    }
    if let Some(ts) = in_window(record.observed_time_unix_nano) {
        return (ts, true);
    }
    (now_ns, true)
}

/// Map OTLP `severity_number` ranges onto our fixed severity vocabulary,
/// falling back to the free-form `severity_text` and finally "UNSPECIFIED".
fn row_severity(record: &LogRecord) -> String {
    let text = match record.severity_number {
        1..=4 => "TRACE",
        5..=8 => "DEBUG",
        9..=12 => "INFO",
        13..=16 => "WARN",
        17..=20 => "ERROR",
        21..=24 => "FATAL",
        _ => {
            return if !record.severity_text.is_empty() {
                record.severity_text.to_uppercase()
            } else {
                "UNSPECIFIED".to_string()
            };
        }
    };
    text.to_string()
}

/// Lowercase hex of `bytes` if it is exactly `expected_len` bytes and not
/// all zero, else empty string (OTLP's convention for "no id").
fn hex_id(bytes: &[u8], expected_len: usize) -> String {
    if bytes.len() == expected_len && bytes.iter().any(|&b| b != 0) {
        hex::encode(bytes)
    } else {
        String::new()
    }
}

/// Flatten a `KeyValue` list into a string map, string-coercing non-string
/// `AnyValue`s so the ClickHouse `Map(String, String)` column stays uniform.
fn flatten_kvs(kvs: &[KeyValue]) -> BTreeMap<String, String> {
    kvs.iter()
        .map(|kv| {
            let value = kv
                .value
                .as_ref()
                .map(any_value_to_string)
                .unwrap_or_default();
            (kv.key.clone(), value)
        })
        .collect()
}

/// Stringify an `AnyValue`: strings pass through, scalars use `to_string`,
/// bytes become lowercase hex, and arrays/kvlists become compact JSON.
fn any_value_to_string(value: &AnyValue) -> String {
    match &value.value {
        None => String::new(),
        Some(AnyValueKind::StringValue(s)) => s.clone(),
        Some(AnyValueKind::BoolValue(b)) => b.to_string(),
        Some(AnyValueKind::IntValue(i)) => i.to_string(),
        Some(AnyValueKind::DoubleValue(d)) => d.to_string(),
        Some(AnyValueKind::BytesValue(b)) => hex::encode(b),
        Some(AnyValueKind::ArrayValue(_)) | Some(AnyValueKind::KvlistValue(_)) => {
            serde_json::to_string(&any_value_to_json(value)).unwrap_or_default()
        }
    }
}

/// Recursively lower an `AnyValue` into `serde_json::Value`, used only for
/// the array/kvlist compact-JSON coercion above.
fn any_value_to_json(value: &AnyValue) -> serde_json::Value {
    match &value.value {
        None => serde_json::Value::Null,
        Some(AnyValueKind::StringValue(s)) => serde_json::Value::String(s.clone()),
        Some(AnyValueKind::BoolValue(b)) => serde_json::Value::Bool(*b),
        Some(AnyValueKind::IntValue(i)) => serde_json::Value::from(*i),
        Some(AnyValueKind::DoubleValue(d)) => serde_json::Number::from_f64(*d)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Some(AnyValueKind::BytesValue(b)) => serde_json::Value::String(hex::encode(b)),
        Some(AnyValueKind::ArrayValue(arr)) => {
            serde_json::Value::Array(arr.values.iter().map(any_value_to_json).collect())
        }
        Some(AnyValueKind::KvlistValue(kv)) => serde_json::Value::Object(
            kv.values
                .iter()
                .map(|kv| {
                    let v = kv
                        .value
                        .as_ref()
                        .map(any_value_to_json)
                        .unwrap_or(serde_json::Value::Null);
                    (kv.key.clone(), v)
                })
                .collect(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::common::v1::{ArrayValue, KeyValueList};
    use opentelemetry_proto::tonic::logs::v1::{ResourceLogs, ScopeLogs};
    use opentelemetry_proto::tonic::resource::v1::Resource;

    fn any(value: AnyValueKind) -> AnyValue {
        AnyValue { value: Some(value) }
    }

    fn kv(key: &str, value: AnyValueKind) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(any(value)),
        }
    }

    fn resource(attrs: Vec<KeyValue>) -> Resource {
        Resource {
            attributes: attrs,
            dropped_attributes_count: 0,
        }
    }

    fn req(resource: Resource, scopes: Vec<Vec<LogRecord>>) -> ExportLogsServiceRequest {
        ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(resource),
                scope_logs: scopes
                    .into_iter()
                    .map(|log_records| ScopeLogs {
                        scope: None,
                        log_records,
                        schema_url: String::new(),
                    })
                    .collect(),
                schema_url: String::new(),
            }],
        }
    }

    fn one_record_req(record: LogRecord) -> ExportLogsServiceRequest {
        req(resource(vec![]), vec![vec![record]])
    }

    fn now_ns(now: SystemTime) -> i64 {
        now.duration_since(UNIX_EPOCH).unwrap().as_nanos() as i64
    }

    #[test]
    fn severity_number_ranges() {
        let cases = [
            (1, "TRACE"),
            (4, "TRACE"),
            (5, "DEBUG"),
            (8, "DEBUG"),
            (9, "INFO"),
            (12, "INFO"),
            (13, "WARN"),
            (16, "WARN"),
            (17, "ERROR"),
            (20, "ERROR"),
            (21, "FATAL"),
            (24, "FATAL"),
        ];
        for (n, expected) in cases {
            let record = LogRecord {
                severity_number: n,
                ..Default::default()
            };
            let rows = otlp_to_rows("t".into(), &one_record_req(record));
            assert_eq!(rows[0].severity, expected, "severity_number={n}");
        }
    }

    #[test]
    fn severity_text_fallback_when_number_unset() {
        let record = LogRecord {
            severity_number: 0,
            severity_text: "warning".into(),
            ..Default::default()
        };
        let rows = otlp_to_rows("t".into(), &one_record_req(record));
        assert_eq!(rows[0].severity, "WARNING");
    }

    #[test]
    fn severity_unspecified_when_nothing_set() {
        let rows = otlp_to_rows("t".into(), &one_record_req(LogRecord::default()));
        assert_eq!(rows[0].severity, "UNSPECIFIED");
    }

    // --- ts clamping (`row_ts_at`), tested directly with a fixed `now` for
    // determinism --------------------------------------------------------

    #[test]
    fn ts_within_window_is_used_as_is() {
        let now = SystemTime::now();
        let ts = now_ns(now) - 60_000_000_000; // 1 minute ago
        let record = LogRecord {
            time_unix_nano: ts as u64,
            ..Default::default()
        };
        let (got, clamped) = row_ts_at(&record, now);
        assert_eq!(got, ts);
        assert!(!clamped);
    }

    #[test]
    fn ts_too_far_in_past_falls_back_to_observed_time() {
        let now = SystemTime::now();
        let stale = now_ns(now) - 40 * 24 * 60 * 60 * 1_000_000_000i64; // 40 days ago
        let observed = now_ns(now) - 5_000_000_000; // 5s ago, in window
        let record = LogRecord {
            time_unix_nano: stale as u64,
            observed_time_unix_nano: observed as u64,
            ..Default::default()
        };
        let (got, clamped) = row_ts_at(&record, now);
        assert_eq!(got, observed);
        assert!(clamped);
    }

    #[test]
    fn ts_too_far_in_future_falls_back_to_observed_time() {
        let now = SystemTime::now();
        let far_future = now_ns(now) + 2 * 60 * 60 * 1_000_000_000i64; // 2h ahead
        let observed = now_ns(now);
        let record = LogRecord {
            time_unix_nano: far_future as u64,
            observed_time_unix_nano: observed as u64,
            ..Default::default()
        };
        let (got, clamped) = row_ts_at(&record, now);
        assert_eq!(got, observed);
        assert!(clamped);
    }

    #[test]
    fn ts_and_observed_both_out_of_window_falls_back_to_now() {
        let now = SystemTime::now();
        let stale = now_ns(now) - 365 * 24 * 60 * 60 * 1_000_000_000i64; // 1y ago
        let record = LogRecord {
            time_unix_nano: stale as u64,
            observed_time_unix_nano: stale as u64,
            ..Default::default()
        };
        let (got, clamped) = row_ts_at(&record, now);
        assert_eq!(got, now_ns(now));
        assert!(clamped);
    }

    #[test]
    fn ts_falls_back_to_observed_time_when_missing() {
        let now = SystemTime::now();
        let observed = now_ns(now) - 1_000_000_000;
        let record = LogRecord {
            time_unix_nano: 0,
            observed_time_unix_nano: observed as u64,
            ..Default::default()
        };
        let (got, clamped) = row_ts_at(&record, now);
        assert_eq!(got, observed);
        assert!(clamped);
    }

    #[test]
    fn ts_falls_back_to_now_when_both_unset() {
        let now = SystemTime::now();
        let (got, clamped) = row_ts_at(&LogRecord::default(), now);
        assert_eq!(got, now_ns(now));
        assert!(clamped);
    }

    #[test]
    fn otlp_to_rows_uses_time_unix_nano_when_in_window() {
        let now = now_ns(SystemTime::now());
        let ts = now - 1_000_000_000;
        let record = LogRecord {
            time_unix_nano: ts as u64,
            ..Default::default()
        };
        let rows = otlp_to_rows("t".into(), &one_record_req(record));
        assert_eq!(rows[0].ts, ts);
    }

    #[test]
    fn service_defaults_to_unknown_without_service_name() {
        let rows = otlp_to_rows("t".into(), &one_record_req(LogRecord::default()));
        assert_eq!(&*rows[0].service, "unknown");
    }

    #[test]
    fn service_from_resource_attribute() {
        let res = resource(vec![kv(
            "service.name",
            AnyValueKind::StringValue("checkout".into()),
        )]);
        let rows = otlp_to_rows("t".into(), &req(res, vec![vec![LogRecord::default()]]));
        assert_eq!(&*rows[0].service, "checkout");
        // service.name stays in the resource map too.
        assert_eq!(
            rows[0].resource.get("service.name"),
            Some(&"checkout".to_string())
        );
    }

    #[test]
    fn trace_and_span_id_hex() {
        let record = LogRecord {
            trace_id: vec![0xab; 16],
            span_id: vec![0xcd; 8],
            ..Default::default()
        };
        let rows = otlp_to_rows("t".into(), &one_record_req(record));
        assert_eq!(rows[0].trace_id, "ab".repeat(16));
        assert_eq!(rows[0].span_id, "cd".repeat(8));
    }

    #[test]
    fn trace_and_span_id_invalid_length_is_empty() {
        let record = LogRecord {
            trace_id: vec![0xab; 15],
            span_id: vec![0xcd; 9],
            ..Default::default()
        };
        let rows = otlp_to_rows("t".into(), &one_record_req(record));
        assert_eq!(rows[0].trace_id, "");
        assert_eq!(rows[0].span_id, "");
    }

    #[test]
    fn trace_and_span_id_all_zero_is_empty() {
        let record = LogRecord {
            trace_id: vec![0; 16],
            span_id: vec![0; 8],
            ..Default::default()
        };
        let rows = otlp_to_rows("t".into(), &one_record_req(record));
        assert_eq!(rows[0].trace_id, "");
        assert_eq!(rows[0].span_id, "");
    }

    #[test]
    fn body_missing_is_empty_string() {
        let rows = otlp_to_rows("t".into(), &one_record_req(LogRecord::default()));
        assert_eq!(rows[0].body, "");
    }

    #[test]
    fn any_value_string() {
        assert_eq!(
            any_value_to_string(&any(AnyValueKind::StringValue("hi".into()))),
            "hi"
        );
    }

    #[test]
    fn any_value_bool() {
        assert_eq!(
            any_value_to_string(&any(AnyValueKind::BoolValue(true))),
            "true"
        );
    }

    #[test]
    fn any_value_int() {
        assert_eq!(any_value_to_string(&any(AnyValueKind::IntValue(-7))), "-7");
    }

    #[test]
    fn any_value_double() {
        assert_eq!(
            any_value_to_string(&any(AnyValueKind::DoubleValue(1.5))),
            "1.5"
        );
    }

    #[test]
    fn any_value_bytes() {
        assert_eq!(
            any_value_to_string(&any(AnyValueKind::BytesValue(vec![0xde, 0xad]))),
            "dead"
        );
    }

    #[test]
    fn any_value_empty() {
        assert_eq!(any_value_to_string(&AnyValue { value: None }), "");
    }

    #[test]
    fn any_value_array_is_compact_json() {
        let arr = any(AnyValueKind::ArrayValue(ArrayValue {
            values: vec![
                any(AnyValueKind::IntValue(1)),
                any(AnyValueKind::StringValue("x".into())),
            ],
        }));
        assert_eq!(any_value_to_string(&arr), r#"[1,"x"]"#);
    }

    #[test]
    fn any_value_nested_kvlist_is_compact_json() {
        let kvlist = any(AnyValueKind::KvlistValue(KeyValueList {
            values: vec![kv(
                "inner",
                AnyValueKind::ArrayValue(ArrayValue {
                    values: vec![any(AnyValueKind::BoolValue(false))],
                }),
            )],
        }));
        assert_eq!(any_value_to_string(&kvlist), r#"{"inner":[false]}"#);
    }

    #[test]
    fn multiple_resources_and_scopes_produce_correct_rows() {
        let res_a = resource(vec![kv(
            "service.name",
            AnyValueKind::StringValue("a".into()),
        )]);
        let res_b = resource(vec![kv(
            "service.name",
            AnyValueKind::StringValue("b".into()),
        )]);
        let mut request = req(
            res_a,
            vec![vec![LogRecord::default()], vec![LogRecord::default()]],
        );
        request.resource_logs.push(ResourceLogs {
            resource: Some(res_b),
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records: vec![LogRecord::default()],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        });

        let rows = otlp_to_rows("tenant".into(), &request);
        assert_eq!(rows.len(), 3);
        assert_eq!(&*rows[0].service, "a");
        assert_eq!(&*rows[1].service, "a");
        assert_eq!(&*rows[2].service, "b");
        assert!(rows.iter().all(|r| &*r.tenant_id == "tenant"));
    }

    #[test]
    fn resource_and_service_are_shared_via_arc_not_cloned_per_record() {
        let res = resource(vec![kv(
            "service.name",
            AnyValueKind::StringValue("svc".into()),
        )]);
        let rows = otlp_to_rows(
            "tenant".into(),
            &req(res, vec![vec![LogRecord::default(), LogRecord::default()]]),
        );
        assert_eq!(rows.len(), 2);
        assert!(
            Arc::ptr_eq(&rows[0].resource, &rows[1].resource),
            "resource map is shared across rows from the same resource_logs entry"
        );
        assert!(
            Arc::ptr_eq(&rows[0].service, &rows[1].service),
            "service is shared across rows from the same resource_logs entry"
        );
        assert!(
            Arc::ptr_eq(&rows[0].tenant_id, &rows[1].tenant_id),
            "tenant_id is shared across every row in the request"
        );
    }
}
