# Development Dockerfile - builds from source
# Uses cargo-chef for dependency caching (faster rebuilds)
# Use Dockerfile.release for production with pre-built binaries

# Stage 1: Plan dependencies
FROM rust:1.99.1-alpine3.24@sha256:b1d5dd5a22a09404d3e798c0cf051b689077360374eb505fa5bb93c479693d87 AS planner
RUN cargo install cargo-chef --locked
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo chef prepare --recipe-path recipe.json

# Stage 2: Cache dependencies
FROM rust:1.99.1-alpine3.24@sha256:b1d5dd5a22a09404d3e798c0cf051b689077360374eb505fa5bb93c479693d87 AS cacher
RUN apk add --no-cache musl-dev
RUN cargo install cargo-chef --locked
WORKDIR /build
COPY --from=planner /build/recipe.json recipe.json
# Build dependencies only - this layer is cached unless Cargo.toml/Cargo.lock change
RUN cargo chef cook --release --recipe-path recipe.json

# Stage 3: Build application
FROM rust:1.99.1-alpine3.24@sha256:b1d5dd5a22a09404d3e798c0cf051b689077360374eb505fa5bb93c479693d87 AS builder
RUN apk add --no-cache musl-dev
WORKDIR /build
# Copy cached dependencies
COPY --from=cacher /build/target target
COPY --from=cacher /usr/local/cargo /usr/local/cargo
# Copy source code
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
# Build only the application (dependencies are cached)
RUN cargo build --release --package sagitta

# Runtime stage
FROM alpine:3.24.3@sha256:be2656061b536c1883c4034790e21d0a09c5aaaf51968b89368a3c0371ea2c9a

# Install minimal runtime dependencies
RUN apk add --no-cache ca-certificates && apk upgrade --no-cache libcrypto3 libssl3

# Copy binary from builder
COPY --from=builder /build/target/release/sagitta /usr/local/bin/sagitta

# Create non-root user
RUN adduser -D -h /home/app app
USER app

# Default configuration directory
ENV SAGITTA_CONFIG=/etc/sagitta/config.toml

# Expose Flight gRPC port
EXPOSE 50051

# Healthcheck using nc (busybox)
HEALTHCHECK --interval=10s --timeout=5s --start-period=5s --retries=3 \
    CMD nc -z localhost 50051

ENTRYPOINT ["/usr/local/bin/sagitta"]
