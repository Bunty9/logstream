//! Wire shape for rows read back from ClickHouse.
//!
//! Field order here must match the column list every query in `sql.rs`
//! selects (`SELECT_COLUMNS`) — `clickhouse`'s RowBinary format is
//! positional, not name-keyed, so a struct/column mismatch silently
//! desyncs the byte stream rather than erroring cleanly.
//!
//! Two gotchas baked into the column list on the SQL side rather than
//! here (see `sql.rs` for the actual `SELECT`):
//! - `trace_id`/`span_id` are `FixedString` columns; RowBinary encodes a
//!   `FixedString(N)` as exactly `N` raw bytes with **no** length prefix,
//!   which would desync a `String` field mid-row. The `SELECT` converts
//!   them with `toString` + strips NUL padding first, so by the time they
//!   land here they're ordinary length-prefixed strings.
//! - `attrs`/`resource` are `Map(LowCardinality(String), String)`
//!   columns. The installed `clickhouse` crate (0.12) doesn't implement
//!   `deserialize_map` (it panics — see its `rowbinary/de.rs`), but a
//!   `Map` and `Array(Tuple(K, V))` share the same wire encoding, so we
//!   read it as `Vec<(String, String)>` and reshape to a JSON object for
//!   the HTTP response.

use clickhouse::Row as ChRow;
use serde::Deserialize;
use serde_json::{Map, Value};

#[derive(Debug, Clone, Deserialize, ChRow)]
pub struct SelectedRow {
    pub ts: i64,
    pub severity: String,
    pub service: String,
    pub trace_id: String,
    pub span_id: String,
    pub body: String,
    pub attrs: Vec<(String, String)>,
    pub resource: Vec<(String, String)>,
}

impl SelectedRow {
    pub fn to_json(&self) -> Value {
        Value::Object(
            [
                ("ts".to_string(), Value::from(self.ts)),
                ("severity".to_string(), Value::from(self.severity.clone())),
                ("service".to_string(), Value::from(self.service.clone())),
                ("trace_id".to_string(), Value::from(self.trace_id.clone())),
                ("span_id".to_string(), Value::from(self.span_id.clone())),
                ("body".to_string(), Value::from(self.body.clone())),
                ("attrs".to_string(), pairs_to_object(&self.attrs)),
                ("resource".to_string(), pairs_to_object(&self.resource)),
            ]
            .into_iter()
            .collect::<Map<_, _>>(),
        )
    }
}

fn pairs_to_object(pairs: &[(String, String)]) -> Value {
    Value::Object(
        pairs
            .iter()
            .cloned()
            .map(|(k, v)| (k, Value::String(v)))
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn to_json_shape() {
        let row = SelectedRow {
            ts: 42,
            severity: "ERROR".into(),
            service: "api".into(),
            trace_id: "abc".into(),
            span_id: "def".into(),
            body: "boom".into(),
            attrs: vec![("k".into(), "v".into())],
            resource: vec![],
        };
        let json = row.to_json();
        assert_eq!(json["ts"], 42);
        assert_eq!(json["severity"], "ERROR");
        assert_eq!(json["attrs"]["k"], "v");
        assert_eq!(json["resource"], serde_json::json!({}));
    }
}
