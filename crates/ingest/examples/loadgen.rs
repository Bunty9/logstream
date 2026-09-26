//! OTLP-HTTP load generator for `logstream-ingest`.
//!
//! Builds real `ExportLogsServiceRequest` protobuf batches (realistic
//! `service.name` resource, cycling severities, a few attributes, and
//! per-record trace/span ids) and posts them concurrently with a
//! connection-pooled `hyper-util` client — not `reqwest`: `hyper-util`
//! (client-legacy) and `http-body-util` are already pulled into
//! `Cargo.lock` transitively by the `clickhouse` crate, so this adds no
//! new crate to the dependency tree.
//!
//! Two ways to drive it:
//!   - throughput run: `--duration 30 --concurrency 32 --batch 500`
//!   - one-shot e2e check: `--requests 1 --batch 20 --trace-id <32 hex chars>`
//!     sends exactly one request of N records all sharing a known trace id,
//!     so a caller can immediately `GET /trace/<id>` and expect N rows.
//!
//! Prints total records sent/accepted, records/s, request latency
//! p50/p99/max, and a status-code histogram.

use bytes::Bytes;
use clap::Parser;
use http_body_util::{BodyExt, Full};
use hyper::{header, Method, Request, Uri};
use hyper_util::client::legacy::{connect::HttpConnector, Client};
use hyper_util::rt::TokioExecutor;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{
    any_value::Value as AnyValueKind, AnyValue, KeyValue,
};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;
use std::collections::{BTreeMap, HashMap};
use std::io::Write;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Parser, Debug)]
#[command(
    name = "loadgen",
    about = "OTLP-HTTP load generator for logstream-ingest"
)]
struct Args {
    /// Ingest endpoint to POST to.
    #[arg(long, default_value = "http://127.0.0.1:4318/v1/logs")]
    url: String,

    /// x-api-key header value.
    #[arg(long)]
    key: String,

    /// Total number of requests to send (mutually exclusive with `--duration`;
    /// if neither is set, runs for 10s).
    #[arg(long)]
    requests: Option<u64>,

    /// Run for this many seconds instead of a fixed request count.
    #[arg(long)]
    duration: Option<u64>,

    /// Log records per request.
    #[arg(long, default_value_t = 500)]
    batch: usize,

    /// Number of concurrent sender tasks.
    #[arg(long, default_value_t = 8)]
    concurrency: usize,

    /// Gzip-compress the request body (Content-Encoding: gzip).
    #[arg(long, default_value_t = false)]
    gzip: bool,

    /// Fixed 32-hex-char trace id applied to every record in every
    /// request, instead of a unique per-record id. Used for the e2e
    /// "send N records with a known trace id" check.
    #[arg(long)]
    trace_id: Option<String>,

    /// service.name resource attribute.
    #[arg(long, default_value = "checkout")]
    service: String,
}

#[derive(Default)]
struct WorkerStats {
    latencies_us: Vec<u64>,
    status_counts: HashMap<u16, u64>,
    transport_errors: u64,
    records_sent: u64,
    records_accepted: u64,
}

#[tokio::main]
async fn main() {
    let args = Arc::new(Args::parse());
    assert!(args.batch > 0, "--batch must be > 0");
    assert!(args.concurrency > 0, "--concurrency must be > 0");
    if let Some(t) = &args.trace_id {
        assert!(
            t.len() == 32 && t.bytes().all(|b| b.is_ascii_hexdigit()),
            "--trace-id must be 32 hex characters"
        );
    }

    let uri: Uri = args.url.parse().expect("--url must be a valid URL");
    let client: Client<HttpConnector, Full<Bytes>> =
        Client::builder(TokioExecutor::new()).build(HttpConnector::new());

    // Requests mode: a shared countdown all workers race to decrement.
    // Duration mode: countdown stays at i64::MAX (never hits zero) and
    // workers instead stop at a wall-clock deadline.
    let (remaining, deadline) = match (args.requests, args.duration) {
        (Some(n), _) => (Arc::new(AtomicI64::new(n as i64)), None),
        (None, Some(secs)) => (
            Arc::new(AtomicI64::new(i64::MAX)),
            Some(Instant::now() + Duration::from_secs(secs)),
        ),
        (None, None) => (
            Arc::new(AtomicI64::new(i64::MAX)),
            Some(Instant::now() + Duration::from_secs(10)),
        ),
    };

    let start = Instant::now();
    let handles: Vec<_> = (0..args.concurrency)
        .map(|w| {
            tokio::spawn(run_worker(
                args.clone(),
                client.clone(),
                uri.clone(),
                remaining.clone(),
                deadline,
                (w as u64).wrapping_mul(1_000_003),
            ))
        })
        .collect();

    let mut all_latencies = Vec::new();
    let mut status_counts: HashMap<u16, u64> = HashMap::new();
    let mut transport_errors = 0u64;
    let mut records_sent = 0u64;
    let mut records_accepted = 0u64;
    for h in handles {
        let s = h.await.expect("worker task panicked");
        all_latencies.extend(s.latencies_us);
        for (code, n) in s.status_counts {
            *status_counts.entry(code).or_insert(0) += n;
        }
        transport_errors += s.transport_errors;
        records_sent += s.records_sent;
        records_accepted += s.records_accepted;
    }
    let elapsed = start.elapsed();
    all_latencies.sort_unstable();

    let total_requests: u64 = status_counts.values().sum::<u64>() + transport_errors;
    let p50 = percentile_us(&all_latencies, 0.50);
    let p99 = percentile_us(&all_latencies, 0.99);
    let max = all_latencies.last().copied().unwrap_or(0);

    println!("== loadgen results ==");
    println!("elapsed:          {:.2}s", elapsed.as_secs_f64());
    println!("requests:         total={total_requests} transport_errors={transport_errors}");
    print!("status histogram: ");
    let sorted: BTreeMap<u16, u64> = status_counts.into_iter().collect();
    for (code, n) in &sorted {
        print!("{code}={n} ");
    }
    println!();
    println!("records:          sent={records_sent} accepted={records_accepted}");
    println!(
        "throughput:       {:.0} rec/s accepted, {:.0} rec/s attempted",
        records_accepted as f64 / elapsed.as_secs_f64(),
        records_sent as f64 / elapsed.as_secs_f64()
    );
    println!(
        "latency (ms):     p50={:.2} p99={:.2} max={:.2}",
        p50 as f64 / 1000.0,
        p99 as f64 / 1000.0,
        max as f64 / 1000.0
    );
}

async fn run_worker(
    args: Arc<Args>,
    client: Client<HttpConnector, Full<Bytes>>,
    uri: Uri,
    remaining: Arc<AtomicI64>,
    deadline: Option<Instant>,
    seq_base: u64,
) -> WorkerStats {
    let mut stats = WorkerStats::default();
    let mut i: u64 = 0;
    loop {
        if let Some(dl) = deadline {
            if Instant::now() >= dl {
                break;
            }
        }
        if remaining.fetch_sub(1, Ordering::Relaxed) <= 0 {
            break;
        }
        let seq = seq_base.wrapping_add(i);
        i += 1;

        let body = build_body(&args, seq);
        let req = build_http_request(&uri, &args.key, args.gzip, body);

        let sent_at = Instant::now();
        match client.request(req).await {
            Ok(resp) => {
                let status = resp.status().as_u16();
                // Drain the body so the connection can be reused for the
                // next request on this worker (hyper won't pool a
                // connection whose response body wasn't fully read).
                let _ = resp.into_body().collect().await;
                stats
                    .latencies_us
                    .push(sent_at.elapsed().as_micros() as u64);
                *stats.status_counts.entry(status).or_insert(0) += 1;
                stats.records_sent += args.batch as u64;
                if status == 200 {
                    stats.records_accepted += args.batch as u64;
                }
            }
            Err(_) => stats.transport_errors += 1,
        }
    }
    stats
}

fn build_http_request(uri: &Uri, key: &str, gzip: bool, body: Vec<u8>) -> Request<Full<Bytes>> {
    let body = if gzip { gzip_compress(&body) } else { body };
    let mut builder = Request::builder()
        .method(Method::POST)
        .uri(uri.clone())
        .header(header::CONTENT_TYPE, "application/x-protobuf")
        .header("x-api-key", key);
    if gzip {
        builder = builder.header(header::CONTENT_ENCODING, "gzip");
    }
    builder
        .body(Full::new(Bytes::from(body)))
        .expect("valid http request")
}

fn gzip_compress(raw: &[u8]) -> Vec<u8> {
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    enc.write_all(raw).expect("gzip write");
    enc.finish().expect("gzip finish")
}

/// Build one `ExportLogsServiceRequest` (one resource, one scope,
/// `args.batch` records) and protobuf-encode it.
fn build_body(args: &Args, seq: u64) -> Vec<u8> {
    let now_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;

    let log_records: Vec<LogRecord> = (0..args.batch)
        .map(|i| {
            let rec_seq = seq.wrapping_mul(args.batch as u64).wrapping_add(i as u64);
            let trace_id = match &args.trace_id {
                Some(t) => hex_to_bytes(t),
                None => gen_bytes(rec_seq, 0, 16),
            };
            let span_id = gen_bytes(rec_seq, 1, 8);
            let severity_number = 1 + (rec_seq % 24) as i32;
            LogRecord {
                time_unix_nano: now_ns + i as u64,
                observed_time_unix_nano: now_ns + i as u64,
                severity_number,
                severity_text: String::new(),
                body: Some(any_str(format!("checkout request {rec_seq} processed"))),
                attributes: vec![
                    kv("http.method", "POST"),
                    kv("http.status_code", "200"),
                    kv("user.id", &format!("u-{}", rec_seq % 10_000)),
                ],
                trace_id,
                span_id,
                ..Default::default()
            }
        })
        .collect();

    let req = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![kv("service.name", &args.service)],
                dropped_attributes_count: 0,
            }),
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };
    req.encode_to_vec()
}

fn kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(any_str(value.to_string())),
    }
}

fn any_str(s: String) -> AnyValue {
    AnyValue {
        value: Some(AnyValueKind::StringValue(s)),
    }
}

/// Deterministic pseudo-random id bytes, keyed by `(seed, salt)` — reuses
/// the workspace's own `blake3` dependency instead of adding `rand`.
fn gen_bytes(seed: u64, salt: u8, len: usize) -> Vec<u8> {
    let mut input = seed.to_le_bytes().to_vec();
    input.push(salt);
    blake3::hash(&input).as_bytes()[..len].to_vec()
}

fn hex_to_bytes(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("valid hex"))
        .collect()
}

fn percentile_us(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = (((sorted.len() - 1) as f64) * p).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}
