//! Translate a parsed [`LogQlQuery`] plus tenant/time/limit/direction into
//! a ClickHouse `SELECT` against the `logs` table.
//!
//! Every user-supplied value — tenant id, time bounds, label names,
//! matcher/filter values, limit — travels as a `?` placeholder bound
//! through the `clickhouse` crate's [`clickhouse::query::Query::bind`],
//! never string-interpolated. `translate()` (and friends) instead return
//! a `(sql_template, binds)` pair; `bind_all` in `main.rs` applies the
//! binds to a `clickhouse::Query` in one place.
//!
//! Label-name handling: `service`, `severity`, `trace_id`, `span_id` are
//! real columns, matched against a Rust-side whitelist (never
//! interpolated from user text — only our own match-arm literals reach
//! the SQL string). Any other label `k` is treated as an attribute or
//! resource key and turned into
//! `if(mapContains(attrs, ?), attrs[?], resource[?])` with `k` bound
//! (three times, once per `?`) rather than spliced in — `attrs` wins on
//! collision, falling back to `resource`; a key present in neither just
//! makes the comparison false (`Map` `[]` access returns the empty
//! string default) rather than erroring.
//!
//! `=~`/`!~` compile to ClickHouse's `match()` (re2). LogQL regex
//! matchers are fully anchored, so the pattern is wrapped `^(?:re)$`
//! before binding; line filters (`|~`/`!~` on the body) stay unanchored,
//! matching Loki's own semantics.

use crate::logql::{LineFilterOp, LogQlQuery, MatchOp};

/// Server-side clamp regardless of what the client asks for.
pub const MAX_LIMIT: u32 = 5000;
pub const DEFAULT_LIMIT: u32 = 100;

/// A value bound to one `?` placeholder, in the order the placeholders
/// appear in `sql`.
#[derive(Debug, Clone, PartialEq)]
pub enum Bind {
    Str(String),
    I64(i64),
}

/// Read direction, i.e. `ORDER BY ts {ASC,DESC}`. Backward (newest first)
/// is the default, matching Loki.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Forward,
    Backward,
}

impl Direction {
    pub fn parse(raw: Option<&str>) -> Result<Self, String> {
        match raw.map(str::to_ascii_lowercase).as_deref() {
            None | Some("backward") | Some("") => Ok(Direction::Backward),
            Some("forward") => Ok(Direction::Forward),
            Some(other) => Err(format!(
                "invalid direction '{other}', expected 'forward' or 'backward'"
            )),
        }
    }

    fn order_sql(self) -> &'static str {
        match self {
            Direction::Backward => "DESC",
            Direction::Forward => "ASC",
        }
    }
}

/// A translated query: the SQL template (with `?` placeholders) and the
/// values to bind, in order.
#[derive(Debug, Clone, PartialEq)]
pub struct Translated {
    pub sql: String,
    pub binds: Vec<Bind>,
}

/// Column list shared by every row-returning query here. Order must match
/// `row::SelectedRow`'s field order field-for-field — RowBinary is
/// positional, not name-keyed, so the *aliases* below are cosmetic to
/// ClickHouse but their absence would not be: every computed column is
/// aliased to a name that is **not** the underlying column's own name
/// (`ts_ns`, not `ts`; `trace_id_hex`, not `trace_id`). Aliasing a
/// computed expression back onto its source column's name is a real
/// ClickHouse footgun — its analyzer can substitute the alias expression
/// back into an unrelated `WHERE`/`ORDER BY` reference to that column
/// name elsewhere in the same query, which for `ts` turned
/// `ts >= fromUnixTimestamp64Nano(?)` into an `Int64`-vs-`DateTime64`
/// comparison that fails with `DECIMAL_OVERFLOW` (confirmed against a
/// real 24.8 server — see `tests/clickhouse_integration.rs`).
///
/// `trace_id`/`span_id` are `FixedString` columns: RowBinary encodes those
/// as raw fixed-width bytes with no length prefix, which would desync a
/// `String`-typed struct field mid-stream. Converting with `toString` +
/// stripping the NUL padding turns them into ordinary length-prefixed
/// strings before they ever reach the wire deserializer.
const SELECT_COLUMNS: &str = "toUnixTimestamp64Nano(ts) AS ts_ns, severity, service, \
     replaceAll(toString(trace_id), '\\0', '') AS trace_id_hex, \
     replaceAll(toString(span_id), '\\0', '') AS span_id_hex, \
     body, attrs, resource";

fn anchor(re: &str) -> String {
    format!("^(?:{re})$")
}

/// Build the SQL value-expression for a stream-selector label, pushing
/// any binds it needs. See module docs for the attrs/resource fallback.
fn label_expr(label: &str, binds: &mut Vec<Bind>) -> String {
    match label {
        "service" | "severity" | "trace_id" | "span_id" => label.to_string(),
        other => {
            binds.push(Bind::Str(other.to_string()));
            binds.push(Bind::Str(other.to_string()));
            binds.push(Bind::Str(other.to_string()));
            "if(mapContains(attrs, ?), attrs[?], resource[?])".to_string()
        }
    }
}

/// Translate a parsed query into a tenant- and time-scoped `SELECT`.
///
/// `tenant_id = ?` is always the first predicate — every caller in
/// `handlers.rs` resolves the tenant from the API key before calling
/// this, so there is no code path that can build a query without it.
pub fn translate(
    q: &LogQlQuery,
    tenant_id: &str,
    start_ns: i64,
    end_ns: i64,
    limit: u32,
    direction: Direction,
) -> Translated {
    let mut sql = format!("SELECT {SELECT_COLUMNS} FROM logs WHERE tenant_id = ?");
    let mut binds = vec![Bind::Str(tenant_id.to_string())];

    sql.push_str(" AND ts >= fromUnixTimestamp64Nano(?) AND ts < fromUnixTimestamp64Nano(?)");
    binds.push(Bind::I64(start_ns));
    binds.push(Bind::I64(end_ns));

    for m in &q.matchers {
        let expr = label_expr(&m.label, &mut binds);
        sql.push_str(" AND ");
        match m.op {
            MatchOp::Eq => {
                sql.push_str(&expr);
                sql.push_str(" = ?");
                binds.push(Bind::Str(m.value.clone()));
            }
            MatchOp::Neq => {
                sql.push_str(&expr);
                sql.push_str(" != ?");
                binds.push(Bind::Str(m.value.clone()));
            }
            MatchOp::Match => {
                sql.push_str("match(");
                sql.push_str(&expr);
                sql.push_str(", ?)");
                binds.push(Bind::Str(anchor(&m.value)));
            }
            MatchOp::NotMatch => {
                sql.push_str("NOT match(");
                sql.push_str(&expr);
                sql.push_str(", ?)");
                binds.push(Bind::Str(anchor(&m.value)));
            }
        }
    }

    for f in &q.filters {
        sql.push_str(" AND ");
        match f.op {
            LineFilterOp::Contains => {
                sql.push_str("position(body, ?) > 0");
                binds.push(Bind::Str(f.value.clone()));
            }
            LineFilterOp::NotContains => {
                sql.push_str("position(body, ?) = 0");
                binds.push(Bind::Str(f.value.clone()));
            }
            LineFilterOp::Match => {
                sql.push_str("match(body, ?)");
                binds.push(Bind::Str(f.value.clone()));
            }
            LineFilterOp::NotMatch => {
                sql.push_str("NOT match(body, ?)");
                binds.push(Bind::Str(f.value.clone()));
            }
        }
    }

    sql.push_str(" ORDER BY ts ");
    sql.push_str(direction.order_sql());
    sql.push_str(" LIMIT ?");
    binds.push(Bind::I64(limit.clamp(1, MAX_LIMIT) as i64));

    Translated { sql, binds }
}

/// Translate a `GET /trace/{trace_id}` lookup. `time_range` is `None` when
/// the caller didn't supply `start`/`end` — trace lookups are meant to
/// find a trace regardless of when it happened, leaning on the
/// `bloom_filter` index on `trace_id` rather than a time bound.
pub fn translate_trace(
    tenant_id: &str,
    trace_id: &str,
    time_range: Option<(i64, i64)>,
    limit: u32,
) -> Translated {
    let mut sql = format!("SELECT {SELECT_COLUMNS} FROM logs WHERE tenant_id = ? AND trace_id = ?");
    let mut binds = vec![
        Bind::Str(tenant_id.to_string()),
        Bind::Str(trace_id.to_string()),
    ];

    if let Some((start_ns, end_ns)) = time_range {
        sql.push_str(" AND ts >= fromUnixTimestamp64Nano(?) AND ts < fromUnixTimestamp64Nano(?)");
        binds.push(Bind::I64(start_ns));
        binds.push(Bind::I64(end_ns));
    }

    sql.push_str(" ORDER BY ts ASC LIMIT ?");
    binds.push(Bind::I64(limit.clamp(1, MAX_LIMIT) as i64));

    Translated { sql, binds }
}

/// A label column allowed in `GET /loki/api/v1/label/{name}/values`.
/// `column` must come from a match-arm literal in the caller (never raw
/// user text) — it is interpolated directly since ClickHouse has no bind
/// syntax for identifiers.
pub fn translate_label_values(
    column: &'static str,
    tenant_id: &str,
    start_ns: i64,
    end_ns: i64,
    limit: u32,
) -> Translated {
    let sql = format!(
        "SELECT DISTINCT {column} FROM logs WHERE tenant_id = ? \
         AND ts >= fromUnixTimestamp64Nano(?) AND ts < fromUnixTimestamp64Nano(?) \
         ORDER BY {column} LIMIT ?"
    );
    let binds = vec![
        Bind::Str(tenant_id.to_string()),
        Bind::I64(start_ns),
        Bind::I64(end_ns),
        Bind::I64(limit.clamp(1, MAX_LIMIT) as i64),
    ];
    Translated { sql, binds }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logql::{LineFilter, Matcher};

    fn q(matchers: Vec<Matcher>, filters: Vec<LineFilter>) -> LogQlQuery {
        LogQlQuery { matchers, filters }
    }

    #[test]
    fn simple_selector() {
        let query = q(
            vec![Matcher {
                label: "service".into(),
                op: MatchOp::Eq,
                value: "api".into(),
            }],
            vec![],
        );
        let t = translate(&query, "tenant-a", 0, 100, 50, Direction::Backward);
        assert_eq!(
            t.sql,
            "SELECT toUnixTimestamp64Nano(ts) AS ts_ns, severity, service, \
             replaceAll(toString(trace_id), '\\0', '') AS trace_id_hex, \
             replaceAll(toString(span_id), '\\0', '') AS span_id_hex, \
             body, attrs, resource FROM logs WHERE tenant_id = ? \
             AND ts >= fromUnixTimestamp64Nano(?) AND ts < fromUnixTimestamp64Nano(?) \
             AND service = ? ORDER BY ts DESC LIMIT ?"
        );
        assert_eq!(
            t.binds,
            vec![
                Bind::Str("tenant-a".into()),
                Bind::I64(0),
                Bind::I64(100),
                Bind::Str("api".into()),
                Bind::I64(50),
            ]
        );
    }

    #[test]
    fn tenant_is_always_the_first_predicate() {
        let query = q(vec![], vec![]);
        let t = translate(&query, "victim-tenant", 1, 2, 10, Direction::Backward);
        assert!(t.sql.starts_with("SELECT "));
        assert!(t.sql.contains("WHERE tenant_id = ?"));
        assert_eq!(t.binds[0], Bind::Str("victim-tenant".into()));
    }

    #[test]
    fn regex_matcher_is_anchored() {
        let query = q(
            vec![Matcher {
                label: "service".into(),
                op: MatchOp::Match,
                value: "api-.*".into(),
            }],
            vec![],
        );
        let t = translate(&query, "t", 0, 1, 10, Direction::Backward);
        assert!(t.sql.contains("match(service, ?)"));
        // binds: [tenant, start, end, <matcher value>, limit] — the regex
        // value is second-to-last, not last (that's the LIMIT bind).
        let n = t.binds.len();
        assert_eq!(t.binds[n - 2], Bind::Str("^(?:api-.*)$".into()));
    }

    #[test]
    fn not_match_matcher() {
        let query = q(
            vec![Matcher {
                label: "service".into(),
                op: MatchOp::NotMatch,
                value: "api-.*".into(),
            }],
            vec![],
        );
        let t = translate(&query, "t", 0, 1, 10, Direction::Backward);
        assert!(t.sql.contains("NOT match(service, ?)"));
    }

    #[test]
    fn neq_matcher() {
        let query = q(
            vec![Matcher {
                label: "severity".into(),
                op: MatchOp::Neq,
                value: "debug".into(),
            }],
            vec![],
        );
        let t = translate(&query, "t", 0, 1, 10, Direction::Backward);
        assert!(t.sql.contains("AND severity != ?"));
    }

    #[test]
    fn unknown_label_falls_back_to_attrs_then_resource() {
        let query = q(
            vec![Matcher {
                label: "env".into(),
                op: MatchOp::Eq,
                value: "prod".into(),
            }],
            vec![],
        );
        let t = translate(&query, "t", 0, 1, 10, Direction::Backward);
        assert!(t
            .sql
            .contains("if(mapContains(attrs, ?), attrs[?], resource[?]) = ?"));
        assert_eq!(
            t.binds,
            vec![
                Bind::Str("t".into()),
                Bind::I64(0),
                Bind::I64(1),
                Bind::Str("env".into()),
                Bind::Str("env".into()),
                Bind::Str("env".into()),
                Bind::Str("prod".into()),
                Bind::I64(10),
            ]
        );
    }

    #[test]
    fn line_filters() {
        let query = q(
            vec![Matcher {
                label: "service".into(),
                op: MatchOp::Eq,
                value: "api".into(),
            }],
            vec![
                LineFilter {
                    op: LineFilterOp::Contains,
                    value: "err".into(),
                },
                LineFilter {
                    op: LineFilterOp::NotContains,
                    value: "dbg".into(),
                },
                LineFilter {
                    op: LineFilterOp::Match,
                    value: "^fatal".into(),
                },
                LineFilter {
                    op: LineFilterOp::NotMatch,
                    value: "ignore".into(),
                },
            ],
        );
        let t = translate(&query, "t", 0, 1, 10, Direction::Backward);
        assert!(t.sql.contains("position(body, ?) > 0"));
        assert!(t.sql.contains("position(body, ?) = 0"));
        assert!(t.sql.contains("match(body, ?)"));
        assert!(t.sql.contains("NOT match(body, ?)"));
        // line filter values are NOT anchored, unlike selector regex matchers.
        assert!(t.binds.contains(&Bind::Str("^fatal".into())));
        assert!(!t.binds.contains(&Bind::Str("^(?:^fatal)$".into())));
    }

    #[test]
    fn limit_is_clamped_to_max() {
        let query = q(
            vec![Matcher {
                label: "a".into(),
                op: MatchOp::Eq,
                value: "b".into(),
            }],
            vec![],
        );
        let t = translate(&query, "t", 0, 1, 999_999, Direction::Backward);
        assert_eq!(t.binds.last(), Some(&Bind::I64(MAX_LIMIT as i64)));
    }

    #[test]
    fn limit_zero_is_clamped_to_one() {
        let query = q(
            vec![Matcher {
                label: "a".into(),
                op: MatchOp::Eq,
                value: "b".into(),
            }],
            vec![],
        );
        let t = translate(&query, "t", 0, 1, 0, Direction::Backward);
        assert_eq!(t.binds.last(), Some(&Bind::I64(1)));
    }

    #[test]
    fn forward_direction_orders_ascending() {
        let query = q(
            vec![Matcher {
                label: "a".into(),
                op: MatchOp::Eq,
                value: "b".into(),
            }],
            vec![],
        );
        let t = translate(&query, "t", 0, 1, 10, Direction::Forward);
        assert!(t.sql.ends_with("ORDER BY ts ASC LIMIT ?"));
    }

    #[test]
    fn direction_parse() {
        assert_eq!(Direction::parse(None).unwrap(), Direction::Backward);
        assert_eq!(
            Direction::parse(Some("backward")).unwrap(),
            Direction::Backward
        );
        assert_eq!(
            Direction::parse(Some("FORWARD")).unwrap(),
            Direction::Forward
        );
        assert!(Direction::parse(Some("sideways")).is_err());
    }

    #[test]
    fn trace_lookup_without_time_range() {
        let t = translate_trace("t", "0123456789abcdef0123456789abcdef", None, 5000);
        assert!(!t.sql.contains("ts >="));
        assert_eq!(
            t.binds,
            vec![
                Bind::Str("t".into()),
                Bind::Str("0123456789abcdef0123456789abcdef".into()),
                Bind::I64(5000),
            ]
        );
        assert!(t.sql.ends_with("ORDER BY ts ASC LIMIT ?"));
    }

    #[test]
    fn trace_lookup_with_time_range() {
        let t = translate_trace("t", "trace123", Some((10, 20)), 100);
        assert!(t
            .sql
            .contains("ts >= fromUnixTimestamp64Nano(?) AND ts < fromUnixTimestamp64Nano(?)"));
        assert_eq!(
            t.binds,
            vec![
                Bind::Str("t".into()),
                Bind::Str("trace123".into()),
                Bind::I64(10),
                Bind::I64(20),
                Bind::I64(100),
            ]
        );
    }

    #[test]
    fn label_values_query_whitelists_column() {
        let t = translate_label_values("service", "t", 0, 1, 1000);
        assert!(t
            .sql
            .starts_with("SELECT DISTINCT service FROM logs WHERE tenant_id = ?"));
        assert_eq!(t.binds[0], Bind::Str("t".into()));
    }
}
