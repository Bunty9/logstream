//! Time-value parsing shared by `POST /query` and the `/loki/api/v1/*`
//! routes, plus the `end=now, start=end-1h` default window used
//! everywhere rows are looked up by time range.
//!
//! Accepted shapes (matching what the phase-3 spec asks for): a plain
//! integer is nanoseconds since the Unix epoch; a value with a decimal
//! point is Loki-style fractional unix *seconds*; anything else is tried
//! as RFC3339. That's an unambiguous, order-independent rule (no
//! magnitude heuristics) so the same parser serves both `POST /query`'s
//! own surface and Loki's more permissive one.

use serde_json::Value;
use time::{format_description::well_known::Rfc3339, OffsetDateTime};

pub const DEFAULT_RANGE_NS: i64 = 3_600_000_000_000; // 1h

pub fn now_ns() -> i64 {
    (OffsetDateTime::now_utc().unix_timestamp_nanos()) as i64
}

/// Parse a query-string time value (`?start=...&end=...`).
pub fn parse_str_to_ns(raw: &str) -> Result<i64, String> {
    if let Ok(ns) = raw.parse::<i64>() {
        return Ok(ns);
    }
    if raw.contains('.') {
        if let Ok(secs) = raw.parse::<f64>() {
            return Ok((secs * 1_000_000_000.0).round() as i64);
        }
    }
    OffsetDateTime::parse(raw, &Rfc3339)
        .map(|dt| dt.unix_timestamp_nanos() as i64)
        .map_err(|_| {
            format!(
                "invalid time value '{raw}': expected unix nanoseconds, \
                 fractional unix seconds, or RFC3339"
            )
        })
}

/// Parse a JSON body time value (`{"start": ..., "end": ...}`), which may
/// be a number (nanoseconds, or seconds if it has a fractional part) or a
/// string (same rules as `parse_str_to_ns`).
pub fn parse_json_to_ns(v: &Value) -> Result<i64, String> {
    match v {
        Value::String(s) => parse_str_to_ns(s),
        Value::Number(n) => {
            if n.is_i64() || n.is_u64() {
                n.as_i64()
                    .ok_or_else(|| "time value out of range".to_string())
            } else {
                n.as_f64()
                    .map(|secs| (secs * 1_000_000_000.0).round() as i64)
                    .ok_or_else(|| "invalid numeric time value".to_string())
            }
        }
        other => Err(format!("invalid time value: {other}")),
    }
}

/// Apply the shared `end=now, start=end-1h` default.
pub fn resolve_range(start: Option<i64>, end: Option<i64>) -> (i64, i64) {
    let end = end.unwrap_or_else(now_ns);
    let start = start.unwrap_or(end - DEFAULT_RANGE_NS);
    (start, end)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_ns_integer() {
        assert_eq!(parse_str_to_ns("1234567890").unwrap(), 1_234_567_890);
    }

    #[test]
    fn parses_fractional_seconds() {
        assert_eq!(parse_str_to_ns("1.5").unwrap(), 1_500_000_000);
    }

    #[test]
    fn parses_rfc3339() {
        let ns = parse_str_to_ns("2026-01-01T00:00:00Z").unwrap();
        assert!(ns > 0);
    }

    #[test]
    fn rejects_garbage() {
        assert!(parse_str_to_ns("not-a-time").is_err());
    }

    #[test]
    fn json_number_is_ns() {
        assert_eq!(parse_json_to_ns(&Value::from(42)).unwrap(), 42);
    }

    #[test]
    fn json_float_is_seconds() {
        assert_eq!(parse_json_to_ns(&Value::from(2.0)).unwrap(), 2_000_000_000);
    }

    #[test]
    fn json_string_delegates() {
        assert_eq!(parse_json_to_ns(&Value::from("100")).unwrap(), 100);
    }

    #[test]
    fn default_range_is_one_hour() {
        let (start, end) = resolve_range(None, Some(10 * DEFAULT_RANGE_NS));
        assert_eq!(start, 9 * DEFAULT_RANGE_NS);
        assert_eq!(end, 10 * DEFAULT_RANGE_NS);
    }
}
