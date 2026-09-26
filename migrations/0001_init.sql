-- logstream Postgres schema. Tenant key metadata only — log payloads live
-- in ClickHouse. The `key_hash` column stores `blake3(api_key).to_hex()`
-- so the plaintext key never lands in Postgres.

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS api_keys (
  id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  key_hash     TEXT NOT NULL UNIQUE,
  tenant_id    TEXT NOT NULL,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  revoked_at   TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_api_keys_tenant ON api_keys (tenant_id);
