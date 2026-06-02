-- ClickHouse schema for the `logs` table. Mounted into
-- `/docker-entrypoint-initdb.d/` so a fresh container provisions this on
-- first boot.
--
-- Column order, types, indexes and TTL all match the design spec in
-- `docs/specs/2026-05-28-logstream-design.md` § ClickHouse schema and the
-- `LogRow` struct in `crates/core/src/types.rs`. Changing any of those
-- three places without the other two will break inserts.

CREATE TABLE logs (
  tenant_id     LowCardinality(String),
  ts            DateTime64(9, 'UTC'),
  severity      LowCardinality(String),
  service       LowCardinality(String),
  trace_id      FixedString(32),
  span_id       FixedString(16),
  body          String,
  attrs         Map(LowCardinality(String), String),
  resource      Map(LowCardinality(String), String),
  INDEX idx_trace trace_id TYPE bloom_filter(0.01) GRANULARITY 4
) ENGINE = MergeTree
PARTITION BY toYYYYMMDD(ts)
ORDER BY (tenant_id, service, ts)
TTL toDateTime(ts) + INTERVAL 30 DAY;
