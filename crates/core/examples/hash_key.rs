//! Print `blake3(arg).to_hex()` for a single CLI argument.
//!
//! Exists because `scripts/seed-key.sh` needs a blake3 hash of the
//! plaintext API key and this host has neither `b3sum` nor Python's
//! `blake3` package installed — but `blake3` is already a workspace
//! dependency (`crates/core/src/auth.rs` hashes keys the same way), so
//! `cargo run --example hash_key` needs no new dependency.

fn main() {
    let key = std::env::args().nth(1).unwrap_or_else(|| {
        eprintln!("usage: hash_key <api-key>");
        std::process::exit(1);
    });
    println!("{}", blake3::hash(key.as_bytes()).to_hex());
}
