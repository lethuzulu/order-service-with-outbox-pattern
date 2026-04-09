# ── Stage 1: build ────────────────────────────────────────────────────────────
FROM rust:slim AS builder

WORKDIR /app

# Install build dependencies needed for linking
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Cache dependencies by copying manifests first
COPY Cargo.toml Cargo.lock ./

# Create a dummy main so cargo can fetch and compile dependencies
RUN mkdir -p src && echo "fn main() {}" > src/main.rs
RUN SQLX_OFFLINE=true cargo build --release --bin order-service-with-outbox-pattern
RUN rm -rf src

# Copy the real source, migrations, and sqlx offline query cache
COPY src ./src
COPY migrations ./migrations
COPY .sqlx ./.sqlx

# Touch main.rs so cargo knows to rebuild (the dummy was cached)
RUN touch src/main.rs
RUN SQLX_OFFLINE=true cargo build --release --bin order-service-with-outbox-pattern

# ── Stage 2: runtime ──────────────────────────────────────────────────────────
# distroless/cc contains only the C runtime (~20MB) — no shell, no package manager
FROM gcr.io/distroless/cc-debian12 AS runtime

WORKDIR /app

# Copy the compiled binary and migrations
COPY --from=builder /app/target/release/order-service-with-outbox-pattern ./order-service-with-outbox-pattern
COPY --from=builder /app/migrations ./migrations

EXPOSE 3000

# distroless has no shell so CMD must use exec form
CMD ["./order-service-with-outbox-pattern"]
