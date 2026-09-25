#!/usr/bin/env bash

set -euo pipefail

# Seed an API key into the logstream Postgres database.
# Usage: scripts/seed-key.sh <api-key> <tenant>
#
# Computes blake3(api-key).to_hex() and inserts into api_keys table
# with ON CONFLICT (key_hash) DO NOTHING.

if [[ $# -lt 2 ]]; then
    echo "Usage: $0 <api-key> <tenant>" >&2
    exit 1
fi

API_KEY="$1"
TENANT="$2"

# Compute blake3 hash of the API key (without plaintext persistence).
hash_key() {
    local key="$1"

    # Try b3sum first (fastest)
    if command -v b3sum &> /dev/null; then
        printf "%s" "$key" | b3sum | cut -d' ' -f1
        return 0
    fi

    # Try Python blake3 (common in dev environments)
    if command -v python3 &> /dev/null && python3 -c "import blake3" 2>/dev/null; then
        python3 -c "import blake3; import sys; print(blake3.blake3(sys.argv[1].encode()).hexdigest())" "$key"
        return 0
    fi

    # No blake3 available locally — tell user how to install
    cat >&2 <<EOF
Error: No blake3 hasher found. Please install one of:

  1. b3sum (Rust):      cargo install b3sum
  2. Python blake3:     pip install blake3

After installation, run: $0 "$API_KEY" "$TENANT"
EOF
    exit 1
}

KEY_HASH=$(hash_key "$API_KEY")

# Insert into Postgres via docker compose exec.
# -T: disable pseudo-TTY (important for non-interactive use).
# SQL is fed on stdin (psql does not interpolate variables in -c strings);
# :'var' makes psql quote the values, so the tenant cannot inject SQL.
docker compose exec -T postgres psql \
    -U logstream -d logstream -v ON_ERROR_STOP=1 \
    -v key_hash="$KEY_HASH" -v tenant="$TENANT" <<'SQL'
INSERT INTO api_keys (key_hash, tenant_id) VALUES (:'key_hash', :'tenant')
ON CONFLICT (key_hash) DO NOTHING;
SQL

echo "✓ Seeded API key for tenant '$TENANT' (hash: ${KEY_HASH:0:16}...)"
