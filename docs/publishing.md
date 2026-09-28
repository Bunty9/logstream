# Publishing to crates.io

This workspace publishes three crates. The two binary crates depend on
`logstream-core`, so cargo publishes that one first.

| Crate              | Kind       | What users get                                                |
| ------------------ | ---------- | ------------------------------------------------------------- |
| `logstream-core`   | library    | OTLP to row mapping, tenant auth, the ClickHouse batcher (docs.rs) |
| `logstream-ingest` | binary     | `cargo install logstream-ingest` installs the OTLP-HTTP ingest server |
| `logstream-query`  | lib + bin  | `cargo install logstream-query` installs the LogQL/Loki read API |

On 2026-09-28 crates.io had no crate named `logstream`, `logstream-core`,
`logstream-ingest` or `logstream-query`.

Released versions:

| Version | Date       | Tag      |
| ------- | ---------- | -------- |
| 0.1.0   | 2026-09-28 | `v0.1.0` |

The 0.1.0 release was checked after publishing: `cargo install` works for
both binaries and docs.rs built the `logstream-core` and `logstream-query`
docs.

## Metadata (already configured)

Most fields are shared through `[workspace.package]` in `Cargo.toml`:

- `license`, `repository`, `homepage` and `readme` (the root README.md)
- `rust-version = "1.88"`

The minimum Rust version is 1.88 because that is the highest minimum that any
locked dependency declares (the `time` crate, among others). It was checked
with `cargo +1.88 check --workspace --all-targets`. Raise it whenever a
dependency update requires a newer toolchain.

Each crate sets its own `description`, `keywords` and `categories`.

The license files `LICENSE-MIT` and `LICENSE-APACHE` live at the repository
root. Each crate directory has symlinks to them, and `cargo package` follows
those symlinks, so every published crate ships both texts.

Path dependencies carry `version = "0.1.0"`. That lets them resolve from
crates.io once published, and it keeps cargo-deny's wildcard ban passing.

## Release checklist

1. Start from a clean `main` with CI green.
2. Bump `version` in `[workspace.package]`. Also bump the `version` on the
   `logstream-core` path dependencies in `crates/ingest/Cargo.toml` and
   `crates/query/Cargo.toml`. All three crates share one version.
3. Run `cargo update --workspace`. Then run `cargo deny check` to catch new
   advisories and yanked crates.
4. Run the local checks:
   `cargo fmt --all -- --check`,
   `cargo clippy --workspace --all-targets -- -D warnings` and
   `cargo test --workspace`.
5. Run `cargo publish --workspace --dry-run`. This packages and verifies all
   three crates in dependency order without uploading anything.
6. Run `cargo package --list -p <crate>` for each crate and check that both
   licenses and README.md are included.
7. Commit the version bump, tag it with `git tag v0.1.0`, then push with
   `git push origin main --tags`.
8. Run `cargo publish --workspace`. It needs a crates.io token, set with
   `cargo login` or stored in `~/.cargo/credentials.toml`.
   **Publishing cannot be undone.** A version can only be yanked, never
   deleted or re-uploaded.
9. After publishing:
   - Check that the docs.rs build for `logstream-core` succeeded.
   - Check that `cargo install logstream-ingest` works from a clean
     environment.
   - Add crates.io and docs.rs badges to README.md.

## Known caveats

- The README's relative links (for example `./docs/operations.md`) are rewritten
  by crates.io to point at the GitHub repository. They only work after that
  commit has been pushed.
- The packaged `crates/*/tests` read `clickhouse/init.sql` and
  `migrations/0001_init.sql` from the workspace at runtime. Those files are not
  in the published crates. The tests are env-gated, so they skip cleanly when
  run from a crates.io download.
- The binaries still need the external services described in
  [operations.md](./operations.md): ClickHouse with the schema applied,
  Postgres with the migration applied, and Redis.
