//! Throughput bench for `otlp_to_rows`: 1000 log records, ~5 attributes
//! each, across a handful of resources/scopes — roughly one ingest batch.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use logstream_core::otlp_to_rows;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{
    any_value::Value as AnyValueKind, AnyValue, KeyValue,
};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;

const RECORD_COUNT: usize = 1000;
const ATTRS_PER_RECORD: usize = 5;

fn kv(key: String, value: &str) -> KeyValue {
    KeyValue {
        key,
        value: Some(AnyValue {
            value: Some(AnyValueKind::StringValue(value.to_string())),
        }),
    }
}

fn build_request() -> ExportLogsServiceRequest {
    let resource = Resource {
        attributes: vec![kv("service.name".into(), "bench-service")],
        dropped_attributes_count: 0,
    };
    let log_records = (0..RECORD_COUNT)
        .map(|i| LogRecord {
            time_unix_nano: 1_700_000_000_000_000_000 + i as u64,
            severity_number: 9,
            severity_text: "info".into(),
            body: Some(AnyValue {
                value: Some(AnyValueKind::StringValue(format!("log line {i}"))),
            }),
            attributes: (0..ATTRS_PER_RECORD)
                .map(|a| kv(format!("attr.{a}"), "value"))
                .collect(),
            trace_id: vec![0xab; 16],
            span_id: vec![0xcd; 8],
            ..Default::default()
        })
        .collect();

    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(resource),
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

fn bench_otlp_to_rows(c: &mut Criterion) {
    let request = build_request();
    let mut group = c.benchmark_group("otlp_to_rows");
    group.throughput(Throughput::Elements(RECORD_COUNT as u64));
    group.bench_with_input(
        BenchmarkId::new("records", RECORD_COUNT),
        &request,
        |b, request| {
            b.iter(|| otlp_to_rows("bench-tenant".to_string(), std::hint::black_box(request)));
        },
    );
    group.finish();
}

criterion_group!(benches, bench_otlp_to_rows);
criterion_main!(benches);
