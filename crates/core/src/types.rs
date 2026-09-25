//! Row + batch types mirrored against the ClickHouse `logs` table schema
//! in `clickhouse/init.sql`. Column ordering and naming here must stay in
//! lock-step with that DDL so the `clickhouse` crate's `Row` derive can
//! insert without manual column lists.
//!
//! The `clickhouse` crate's RowBinary (de)serializer is stricter than the
//! field types would suggest: `serialize_map`/`deserialize_map` are
//! unimplemented (`panic!("maps are unsupported, use Vec<(A, B)> instead")`)
//! and `FixedString(N)` wants an `[u8; N]`-shaped value, not a
//! length-prefixed `String`. Plain `#[derive(Serialize, Deserialize)]` on
//! `BTreeMap`/`String` fields therefore either panics (`attrs`/`resource`)
//! or silently corrupts the column (`trace_id`/`span_id` would write a
//! length prefix into what ClickHouse reads as fixed-width bytes). The
//! `serialize_with`/`deserialize_with` helpers below keep the public field
//! types ergonomic (`String`, `BTreeMap<String, String>` — what
//! `otlp_to_rows` naturally produces) while fixing the wire shape.

use clickhouse::Row;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::BTreeMap;

/// One log record as written to ClickHouse.
///
/// Field order matches the `CREATE TABLE logs (...)` definition in
/// `clickhouse/init.sql`. `attrs` and `resource` materialize the
/// `Map(LowCardinality(String), String)` columns; we use `BTreeMap` for
/// deterministic key order so identical records hash identically (helps
/// dedup tooling downstream) — see the `map_as_pairs` helper for why the
/// wire format isn't a plain serde map.
///
/// `ts` is encoded as nanoseconds since Unix epoch — the wire shape that
/// the `clickhouse` crate maps to `DateTime64(9, 'UTC')` (confirmed against
/// clickhouse-rs 0.12's own `README.md`, which uses a raw `i64` field for
/// `DateTime64(9)` — no newtype or `serde(with = ...)` needed).
///
/// `trace_id`/`span_id` are hex strings (32 / 16 chars, matching OTLP's
/// 16-byte/8-byte ids) that get packed into the `FixedString(32)` /
/// `FixedString(16)` columns byte-for-byte via `fixed_string`; an empty
/// string (no trace context) packs as all-zero bytes.
#[derive(Debug, Clone, Serialize, Deserialize, Row)]
pub struct LogRow {
    pub tenant_id: String,
    pub ts: i64,
    pub severity: String,
    pub service: String,
    #[serde(with = "fixed_string::n32")]
    pub trace_id: String,
    #[serde(with = "fixed_string::n16")]
    pub span_id: String,
    pub body: String,
    #[serde(with = "map_as_pairs")]
    pub attrs: BTreeMap<String, String>,
    #[serde(with = "map_as_pairs")]
    pub resource: BTreeMap<String, String>,
}

/// `Map(LowCardinality(String), String)` <-> `BTreeMap<String, String>`.
///
/// clickhouse-rs's RowBinary (de)serializer has no `Map` support — it wants
/// what `Map(K, V)` actually is on the wire, `Array((K, V))` — so a plain
/// `BTreeMap` derive panics both ways. This shuttles through
/// `Vec<(String, String)>` instead, which the crate does support.
mod map_as_pairs {
    use super::*;
    use serde::ser::SerializeSeq;

    pub fn serialize<S: Serializer>(
        map: &BTreeMap<String, String>,
        ser: S,
    ) -> Result<S::Ok, S::Error> {
        let mut seq = ser.serialize_seq(Some(map.len()))?;
        for kv in map {
            seq.serialize_element(&kv)?;
        }
        seq.end()
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        de: D,
    ) -> Result<BTreeMap<String, String>, D::Error> {
        Ok(Vec::<(String, String)>::deserialize(de)?
            .into_iter()
            .collect())
    }
}

/// `FixedString(N)` <-> `String`, by packing/unpacking a `[u8; N]`.
///
/// clickhouse-rs maps `FixedString(N)` to an `[u8; N]`-shaped value on the
/// wire (a fixed-size byte array, no length prefix) — serializing a plain
/// `String` there writes a LEB128 length prefix ClickHouse doesn't expect,
/// corrupting every column after it. Longer-than-`N` input is an error;
/// shorter input is zero-padded and trailing NUL bytes are trimmed back off
/// on read (safe here since hex trace/span ids never contain a NUL byte).
mod fixed_string {
    use super::*;

    // `const N: usize` can't be used here: serde only implements
    // `Serialize`/`Deserialize` for `[u8; N]` at the specific lengths its
    // own macro expands (0..=32), not generically for all `N` — a
    // function generic over `N` can't call `.serialize()`/`deserialize()`
    // on `[u8; N]` even though every length we actually use (16, 32) is
    // covered. A macro generating one concrete-array-length module per
    // field keeps the impl trivial instead of fighting that.
    macro_rules! fixed_width_mod {
        ($name:ident, $n:literal) => {
            pub mod $name {
                use super::*;

                pub fn serialize<S: Serializer>(s: &String, ser: S) -> Result<S::Ok, S::Error> {
                    let bytes = s.as_bytes();
                    if bytes.len() > $n {
                        return Err(serde::ser::Error::custom(format!(
                            "expected at most {} bytes, got {}",
                            $n,
                            bytes.len()
                        )));
                    }
                    let mut buf = [0u8; $n];
                    buf[..bytes.len()].copy_from_slice(bytes);
                    buf.serialize(ser)
                }

                pub fn deserialize<'de, D: Deserializer<'de>>(de: D) -> Result<String, D::Error> {
                    let buf = <[u8; $n]>::deserialize(de)?;
                    let end = buf.iter().position(|&b| b == 0).unwrap_or($n);
                    Ok(String::from_utf8_lossy(&buf[..end]).into_owned())
                }
            }
        };
    }

    fixed_width_mod!(n32, 32);
    fixed_width_mod!(n16, 16);
}

/// One ingest hand-off across the bounded mpsc channel: rows are
/// pre-grouped by tenant so the batcher can record per-tenant
/// drop / flush metrics without re-keying.
#[derive(Debug, Clone)]
pub struct TenantBatch {
    pub tenant_id: String,
    pub rows: Vec<LogRow>,
}
