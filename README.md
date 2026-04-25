# Sagitta

[![CI](https://github.com/qualithm/sagitta-rs/actions/workflows/ci.yaml/badge.svg)](https://github.com/qualithm/sagitta-rs/actions/workflows/ci.yaml)
[![codecov](https://codecov.io/gh/qualithm/sagitta-rs/graph/badge.svg)](https://codecov.io/gh/qualithm/sagitta-rs)
[![Crates.io](https://img.shields.io/crates/v/sagitta)](https://crates.io/crates/sagitta)
[![docs.rs](https://docs.rs/sagitta/badge.svg)](https://docs.rs/sagitta)

Rust framework for building analytical data services on Arrow Flight and DataFusion.

## Features

- **Arrow Flight & Flight SQL** — all RPC methods (Handshake, ListFlights, GetFlightInfo, GetSchema,
  DoGet, DoPut, DoExchange, DoAction, ListActions, PollFlightInfo) plus full Flight SQL command
  support
- **SQL via DataFusion** — DDL (CREATE/DROP/ALTER TABLE, CREATE/DROP VIEW, CREATE/DROP SCHEMA), DML
  (INSERT, UPDATE, DELETE, MERGE), queries (joins, subqueries, CTEs, window functions, aggregates),
  transactions, savepoints, and prepared statements
- **Pluggable storage** — implement the `Store` trait for custom backends; ships with an in-memory
  store
- **Pluggable authentication** — implement `UserStore` for custom identity providers; ships with
  basic auth, bearer tokens, and mTLS
- **Custom actions** — extend `DoAction`/`ListActions` with application-specific handlers via the
  `CustomAction` trait
- **TLS & mTLS** — optional transport security with configurable client certificate requirements
- **Configuration** — TOML files with environment variable override (`SAGITTA_CONFIG`)
- **Observability** — structured logging via `tracing`, health checks, graceful shutdown

## Installation

```toml
# Cargo.toml
[dependencies]
sagitta = "0.1"
```

## Quick Start

```bash
cargo build --release

# Copy and edit configuration (or skip to use defaults)
cp sagitta.example.toml sagitta.toml

cargo run -p sagitta
```

The server listens on `0.0.0.0:50051` by default.

## Usage

Add `sagitta` as a dependency and use `Sagitta` to build a custom server:

```rust
use std::sync::Arc;

use sagitta::{AccessLevel, Sagitta, Config, UserStore};
use sagitta_core::InMemoryUserStore;
use sagitta_store::MemoryStore;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let store = Arc::new(MemoryStore::new());

    let mut users = InMemoryUserStore::new();
    users.add_user("alice", "secret", AccessLevel::FullAccess);
    let user_store: Arc<dyn UserStore> = Arc::new(users);

    Sagitta::builder()
        .config(Config::default())
        .store(store)
        .user_store(user_store)
        .serve()
        .await
}
```

### Crates

| Crate           | Purpose                                         |
| --------------- | ----------------------------------------------- |
| `sagitta`       | Server framework library and default binary     |
| `sagitta-core`  | Core types, error handling, shared utilities    |
| `sagitta-store` | Storage backend abstraction and implementations |

### Configuration

See [sagitta.example.toml](sagitta.example.toml) for all options:

| Section     | Key                     | Default         | Description                                |
| ----------- | ----------------------- | --------------- | ------------------------------------------ |
| _(root)_    | `listen_addr`           | `0.0.0.0:50051` | Server listen address                      |
| _(root)_    | `catalog_name`          | `default`       | Catalog name for SQL queries               |
| _(root)_    | `default_schema`        | `public`        | Default schema name                        |
| _(root)_    | `enable_test_fixtures`  | `false`         | Load test datasets on startup              |
| `[logging]` | `level`                 | `info`          | Log level (trace/debug/info/warn/error)    |
| `[logging]` | `format`                | `pretty`        | Log format (`pretty` or `json`)            |
| `[server]`  | `shutdown_timeout_secs` | `30`            | Graceful shutdown timeout                  |
| `[server]`  | `tcp_keepalive_secs`    | `60`            | TCP keepalive interval (0 to disable)      |
| `[server]`  | `max_connections`       | `0`             | Max concurrent connections (0 = unlimited) |
| `[tls]`     | `cert_path`             | —               | Server certificate path                    |
| `[tls]`     | `key_path`              | —               | Server private key path                    |
| `[tls]`     | `ca_path`               | —               | CA certificate for mTLS                    |
| `[tls]`     | `client_auth_optional`  | `false`         | Allow missing client certs in mTLS         |

### Error Handling

Sagitta surfaces errors via `anyhow::Result` at the server level. Storage and auth errors implement
`std::error::Error` and are propagated through the Flight RPC status codes.

## API Reference

Full API documentation is generated with `cargo doc`:

```bash
cargo doc --open
```

Or browse the published docs at [docs.rs/sagitta](https://docs.rs/sagitta).

## Examples

See the [`examples/`](crates/sagitta/examples/) directory for runnable examples:

| Example                                                        | Description                       |
| -------------------------------------------------------------- | --------------------------------- |
| [`custom_server.rs`](crates/sagitta/examples/custom_server.rs) | Custom server with custom actions |

```bash
cargo run --example custom_server
```

## Development

### Prerequisites

- [Rust](https://rustup.rs/) (stable toolchain, 1.95+)

### Building

```bash
cargo build --workspace
```

### Running

```bash
cargo run -p sagitta
```

### Testing

```bash
cargo test --workspace
```

### Linting & Formatting

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
```

### Benchmarks

```bash
cargo bench --workspace
```

### Docker

```bash
docker compose -f docker/docker-compose.yaml up --build -d
docker compose -f docker/docker-compose.yaml logs -f
docker compose -f docker/docker-compose.yaml down
```

## Publishing

The crate is automatically published to [crates.io](https://crates.io) when CI passes on main.
Update the version in the root `Cargo.toml` before merging to trigger a new release.

## Minimum Supported Rust Version

Rust 1.95+ (edition 2024).

## Licence

Apache-2.0
