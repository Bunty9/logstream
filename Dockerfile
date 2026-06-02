# syntax=docker/dockerfile:1.7

# ---- Stage 1: chef base (cargo-chef for layer-cacheable builds) -------------
FROM lukemathwalker/cargo-chef:latest-rust-1 AS chef
WORKDIR /app

# ---- Stage 2: planner (compute the recipe of dependencies) ------------------
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# ---- Stage 3: builder (cook deps, then build the actual workspace) ----------
FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
# Cook only the dependency graph — cached as long as Cargo.{toml,lock} are stable.
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
# Build only the bin crates we ship.
RUN cargo build --release \
    --bin logstream-ingest \
    --bin logstream-query

# ---- Stage 4: distroless runtime --------------------------------------------
FROM gcr.io/distroless/cc-debian12 AS runtime
WORKDIR /app
COPY --from=builder /app/target/release/logstream-ingest /usr/local/bin/logstream-ingest
COPY --from=builder /app/target/release/logstream-query  /usr/local/bin/logstream-query
USER nonroot:nonroot
EXPOSE 4318 4319
# Default to the ingest binary; compose / fly.toml override CMD for the
# query service.
ENTRYPOINT ["/usr/local/bin/logstream-ingest"]
