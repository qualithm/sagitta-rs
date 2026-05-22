# Development Dockerfile - builds from source
# Uses cargo-chef for dependency caching (faster rebuilds)
# Use Dockerfile.release for production with pre-built binaries

# Stage 1: Plan dependencies
FROM rust:1.95-alpine3.23@sha256:606fd313a0f49743ee2a7bd49a0914bab7deedb12791f3a846a34a4711db7ed2 AS planner
RUN cargo install cargo-chef --locked
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo chef prepare --recipe-path recipe.json

# Stage 2: Cache dependencies
FROM rust:1.95-alpine3.23@sha256:606fd313a0f49743ee2a7bd49a0914bab7deedb12791f3a846a34a4711db7ed2 AS cacher
RUN apk add --no-cache musl-dev
RUN cargo install cargo-chef --locked
WORKDIR /build
COPY --from=planner /build/recipe.json recipe.json
# Build dependencies only - this layer is cached unless Cargo.toml/Cargo.lock change
RUN cargo chef cook --release --recipe-path recipe.json

# Stage 3: Build application
FROM rust:1.95-alpine3.23@sha256:606fd313a0f49743ee2a7bd49a0914bab7deedb12791f3a846a34a4711db7ed2 AS builder
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
FROM alpine:3.23@sha256:5b10f432ef3da1b8d4c7eb6c487f2f5a8f096bc91145e68878dd4a5019afde11

# Install minimal runtime dependencies
RUN apk add --no-cache ca-certificates

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
