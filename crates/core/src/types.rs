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
use std::sync::Arc;

/// One log record as written to ClickHouse.
///
/// Field order matches the `CREATE TABLE logs (...)` definition in
/// `clickhouse/init.sql`. `attrs` and `resource` materialize the
/// `Map(LowCardinality(String), String)` columns; we use `BTreeMap` for
/// deterministic key order so identical records hash identically (helps
/// dedup tooling downstream) — see the `map_as_pairs` helper for why the
/// wire format isn't a plain serde map.
///
/// `tenant_id`, `service`, and `resource` are `Arc`-wrapped: every log
/// record in one OTLP request shares the same tenant, and every record
/// under one `resource_logs` entry shares the same service/resource
/// attributes, so `otlp_to_rows` builds each of these once and clones the
/// `Arc` (a refcount bump) per row instead of deep-cloning a `String`/
/// `BTreeMap` per row — a 16 MiB request can hold millions of tiny
/// records, and that per-row deep clone was the dominant memory cost.
/// `serde`'s `rc` feature (enabled workspace-wide) gives `Arc<str>`
/// `Serialize`/`Deserialize` for free; `map_as_pairs` is generic over
/// anything that `Borrow`s/`From`s a `BTreeMap` so it covers both the
/// plain `BTreeMap` (`attrs`, unique per row already) and `Arc<BTreeMap>`
/// (`resource`) with one implementation.
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
    pub tenant_id: Arc<str>,
    pub ts: i64,
    pub severity: String,
    pub service: Arc<str>,
    #[serde(with = "fixed_string::n32")]
    pub trace_id: String,
    #[serde(with = "fixed_string::n16")]
    pub span_id: String,
    pub body: String,
    #[serde(with = "map_as_pairs")]
    pub attrs: BTreeMap<String, String>,
    #[serde(with = "map_as_pairs")]
    pub resource: Arc<BTreeMap<String, String>>,
}

/// `Map(LowCardinality(String), String)` <-> `BTreeMap<String, String>`,
/// generic over anything that borrows/produces a `BTreeMap` so it serves
/// both a plain `BTreeMap` field (`attrs`) and an `Arc<BTreeMap>` field
/// (`resource`) without duplicating the (de)serialization logic.
///
/// clickhouse-rs's RowBinary (de)serializer has no `Map` support — it wants
/// what `Map(K, V)` actually is on the wire, `Array((K, V))` — so a plain
/// `BTreeMap` derive panics both ways. This shuttles through
/// `Vec<(String, String)>` instead, which the crate does support.
mod map_as_pairs {
    use super::*;
    use serde::ser::SerializeSeq;
    use std::borrow::Borrow;

    pub fn serialize<T, S>(map: &T, ser: S) -> Result<S::Ok, S::Error>
    where
        T: Borrow<BTreeMap<String, String>>,
        S: Serializer,
    {
        let map = map.borrow();
        let mut seq = ser.serialize_seq(Some(map.len()))?;
        for kv in map {
            seq.serialize_element(&kv)?;
        }
        seq.end()
    }

    pub fn deserialize<'de, D, T>(de: D) -> Result<T, D::Error>
    where
        D: Deserializer<'de>,
        T: From<BTreeMap<String, String>>,
    {
        Ok(Vec::<(String, String)>::deserialize(de)?
            .into_iter()
            .collect::<BTreeMap<String, String>>()
            .into())
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

/// One ingest hand-off across the bounded mpsc channel: `rows` are the
/// `LogRow`s projected from one request, and `permit` is the
/// `--max-buffered-rows` semaphore permit covering all of them (see
/// `crates/ingest/src/ingest.rs`) — the batcher merges it into its
/// running permit and holds it until those rows are flushed (or dropped),
/// then releases it, so total in-flight rows across the channel *and* the
/// batcher's buffer stay bounded regardless of batch count.
#[derive(Debug)]
pub struct TenantBatch {
    pub rows: Vec<LogRow>,
    pub permit: tokio::sync::OwnedSemaphorePermit,
}
