//! Time-value parsing shared by `POST /query` and the `/loki/api/v1/*`
//! routes, plus the `end=now, start=end-1h` default window (clamped to a
//! max span) used everywhere rows are looked up by time range.
//!
//! Accepted shapes, matching Loki's own `parseTimestamp`: a value with a
//! decimal point is fractional unix *seconds*; a plain integer string of
//! at most 10 digits is unix *seconds* (10 digits covers every unix
//! second through the year 2286), otherwise it's nanoseconds; anything
//! else is tried as RFC3339. This is a deliberate magnitude heuristic —
//! Loki's own rule — kept so `/loki/api/v1/*` clients that assume Loki's
//! semantics get them; `POST /query`'s own numeric `start`/`end` follow
//! the same rule for consistency between the two surfaces rather than
//! silently disagreeing on what a bare number means.

use serde_json::Value;
use time::{format_description::well_known::Rfc3339, OffsetDateTime};

pub const DEFAULT_RANGE_NS: i64 = 3_600_000_000_000; // 1h
/// Server-side cap on `end - start`, applied by `resolve_range` regardless
/// of what the client asks for — bounds the partition/row scan cost of a
/// single query. Loki-style silent clamp (not a `400`): a dashboard panel
/// that's slightly too wide still returns data for the clamped window
/// instead of erroring.
pub const MAX_RANGE_NS: i64 = 31 * 24 * 3_600 * 1_000_000_000; // 31 days

pub fn now_ns() -> i64 {
    (OffsetDateTime::now_utc().unix_timestamp_nanos()) as i64
}

/// A bare integer, per Loki's `parseTimestamp`: <=10 digits is unix
/// seconds (converted to nanoseconds), otherwise it's already nanoseconds.
fn int_to_ns(n: i64) -> i64 {
    let digits = n.unsigned_abs().to_string().len();
    if digits <= 10 {
        n.saturating_mul(1_000_000_000)
    } else {
        n
    }
}

/// Parse a query-string time value (`?start=...&end=...`).
pub fn parse_str_to_ns(raw: &str) -> Result<i64, String> {
    if raw.contains('.') {
        if let Ok(secs) = raw.parse::<f64>() {
            return Ok((secs * 1_000_000_000.0).round() as i64);
        }
    } else if let Ok(n) = raw.parse::<i64>() {
        return Ok(int_to_ns(n));
    }
    OffsetDateTime::parse(raw, &Rfc3339)
        .map(|dt| dt.unix_timestamp_nanos() as i64)
        .map_err(|_| {
            format!(
                "invalid time value '{raw}': expected unix seconds (<=10 digits), unix \
                 nanoseconds, fractional unix seconds, or RFC3339"
            )
        })
}

/// Parse a JSON body time value (`{"start": ..., "end": ...}`), which may
/// be a number (seconds if <=10 digits else nanoseconds, or fractional
/// seconds — same magnitude rule as `parse_str_to_ns`, for consistency) or
/// a string (same rules as `parse_str_to_ns`).
pub fn parse_json_to_ns(v: &Value) -> Result<i64, String> {
    match v {
        Value::String(s) => parse_str_to_ns(s),
        Value::Number(n) => {
            if n.is_i64() || n.is_u64() {
                n.as_i64()
                    .map(int_to_ns)
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

/// Apply the shared `end=now, start=end-1h` default, then clamp the
/// resulting span to `MAX_RANGE_NS` (see its doc).
pub fn resolve_range(start: Option<i64>, end: Option<i64>) -> (i64, i64) {
    let end = end.unwrap_or_else(now_ns);
    // `saturating_sub`: `end` comes straight from client input (a parsed
    // query-time value), so `end` near `i64::MIN` must clamp rather than
    // panic on overflow.
    let start = start.unwrap_or(end.saturating_sub(DEFAULT_RANGE_NS));
    let start = start.max(end.saturating_sub(MAX_RANGE_NS));
    (start, end)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_integer_string_is_unix_seconds() {
        // 10 digits or fewer => seconds, per Loki's parseTimestamp.
        assert_eq!(
            parse_str_to_ns("1234567890").unwrap(),
            1_234_567_890_000_000_000
        );
    }

    #[test]
    fn long_integer_string_is_nanoseconds() {
        // More than 10 digits => already nanoseconds, used as-is.
        assert_eq!(parse_str_to_ns("12345678901").unwrap(), 12_345_678_901);
        assert_eq!(
            parse_str_to_ns("1700000000000000000").unwrap(),
            1_700_000_000_000_000_000
        );
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
    fn json_short_integer_is_seconds() {
        assert_eq!(parse_json_to_ns(&Value::from(42)).unwrap(), 42_000_000_000);
    }

    #[test]
    fn json_long_integer_is_nanoseconds() {
        assert_eq!(
            parse_json_to_ns(&Value::from(1_700_000_000_000_000_000i64)).unwrap(),
            1_700_000_000_000_000_000
        );
    }

    #[test]
    fn json_float_is_seconds() {
        assert_eq!(parse_json_to_ns(&Value::from(2.0)).unwrap(), 2_000_000_000);
    }

    #[test]
    fn json_string_delegates() {
        assert_eq!(
            parse_json_to_ns(&Value::from("100")).unwrap(),
            100_000_000_000
        );
    }

    #[test]
    fn default_range_is_one_hour() {
        let (start, end) = resolve_range(None, Some(10 * DEFAULT_RANGE_NS));
        assert_eq!(start, 9 * DEFAULT_RANGE_NS);
        assert_eq!(end, 10 * DEFAULT_RANGE_NS);
    }

    #[test]
    fn range_wider_than_max_is_clamped() {
        let end = 100 * MAX_RANGE_NS;
        let (start, clamped_end) = resolve_range(Some(0), Some(end));
        assert_eq!(clamped_end, end);
        assert_eq!(start, end - MAX_RANGE_NS);
    }

    #[test]
    fn range_within_max_is_untouched() {
        let (start, end) = resolve_range(Some(1_000), Some(1_000 + DEFAULT_RANGE_NS));
        assert_eq!(start, 1_000);
        assert_eq!(end, 1_000 + DEFAULT_RANGE_NS);
    }

    #[test]
    fn end_near_i64_min_does_not_panic() {
        // `end - DEFAULT_RANGE_NS` / `end - MAX_RANGE_NS` would overflow
        // (panic in debug) for `end` this close to `i64::MIN`; both
        // subtractions must saturate instead.
        let (start, end) = resolve_range(None, Some(i64::MIN));
        assert_eq!(end, i64::MIN);
        assert_eq!(start, i64::MIN);
    }

    #[test]
    fn extreme_negative_json_number_parses_and_resolves_without_panicking() {
        // JSON's exponent notation doesn't need a decimal point, so
        // `-1e300` is a valid JSON *number* (unlike as a bare query-string
        // value, which `parse_str_to_ns` would reject) and lands in
        // `parse_json_to_ns`'s float branch. `secs * 1e9` overflows to
        // `-inf`, and Rust's `as i64` cast on that saturates to
        // `i64::MIN` rather than panicking — feeding that straight into
        // `resolve_range` must likewise not panic.
        let end = parse_json_to_ns(&Value::from(-1e300)).unwrap();
        assert_eq!(end, i64::MIN);
        let (start, end) = resolve_range(None, Some(end));
        assert_eq!(end, i64::MIN);
        assert_eq!(start, i64::MIN);
    }
}
