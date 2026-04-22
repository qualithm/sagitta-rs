# Rust Code Guidelines

## General

- Use the current stable Rust edition
- Lowercase error messages with no trailing punctuation
- Run `cargo fmt` before committing
- Run `cargo clippy` and address all warnings
- Use `anyhow::Result` in binaries, `thiserror` in libraries

## When Code Changes

Any code change should include review of:

- **Tests** - update existing tests, add new tests for new behaviour
- **Benchmarks** - update if performance characteristics change
- **Documentation** - update doc comments if public API changes
- **Error messages** - ensure they remain accurate and helpful
- **Configuration** - update defaults, env vars, or config files if affected
- **Dependencies** - check for unused deps after removing code

Run before committing:

```bash
cargo fmt
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

## Imports

**Order:** std → external crates → workspace crates → crate modules

```rust
use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;
use tracing::{info, instrument};

use my_other_crate::SomeType;

use crate::config::Config;
use crate::error::Error;
```

**Rules:**

- Group imports by origin with blank lines between groups
- Use `use crate::` for internal modules
- Use `use super::` sparingly, prefer absolute paths
- Prefer explicit imports over glob imports (`*`)
- Combine imports from same module: `use std::sync::{Arc, Mutex}`

## File Structure

| Path        | Purpose                          |
| ----------- | -------------------------------- |
| `lib.rs`    | Module exports and re-exports    |
| `main.rs`   | Binary entry point               |
| `config.rs` | Configuration structs + defaults |
| `error.rs`  | Error types using `thiserror`    |

## Naming Conventions

| Type                | Pattern             | Example                    |
| ------------------- | ------------------- | -------------------------- |
| Types/Structs/Enums | `PascalCase`        | `QueryEngine`, `AuthError` |
| Functions/Methods   | `snake_case`        | `get_user`, `flush_buffer` |
| Constants           | `SCREAMING_SNAKE`   | `DEFAULT_TIMEOUT_MS`       |
| Modules             | `snake_case`        | `auth`, `storage`          |
| Config structs      | `{Component}Config` | `ServerConfig`             |
| Error enums         | `{Component}Error`  | `StorageError`             |

## Comments and Documentation

**Avoid comments that age poorly.** Stale documentation is worse than none.

**Do not include:**

- Overview/summary docs describing architecture
- ASCII diagrams showing component relationships
- Feature lists or "this module provides" enumerations
- Example code blocks in module docs
- "How to use" guides or run instructions

**Do include:**

- Single-line module descriptions: `//! HTTP client utilities.`
- Implementation comments explaining non-obvious logic
- `///` docs on public APIs describing parameters and return values
- `# Errors` and `# Panics` sections where applicable

## Error Handling

Use `thiserror` for domain errors in libraries:

```rust
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("file not found: {0}")]
    NotFound(String),

    #[error("permission denied")]
    PermissionDenied,

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}
```

**Rules:**

- Error messages: lowercase, no trailing punctuation
- Use `#[from]` for automatic conversion from underlying errors
- Prefer `Result<T, E>` over panics
- Use `?` for error propagation
- Use `anyhow::Result` in binaries

## Configuration Structs

```rust
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Port to listen on.
    pub port: u16,

    /// Request timeout in milliseconds.
    pub timeout_ms: u64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            port: 8080,
            timeout_ms: 30_000,
        }
    }
}
```

**Rules:**

- Document each field with `///` comments
- Include units in field names or comments (`_ms`, `_bytes`, `_secs`)
- Implement `Default` with sensible production values

## Component Pattern

```rust
pub struct Engine {
    config: EngineConfig,
}

impl Engine {
    pub fn new(config: EngineConfig) -> Self {
        Self { config }
    }

    pub fn config(&self) -> &EngineConfig {
        &self.config
    }
}
```

## Logging

Use `tracing` for structured logging:

```rust
use tracing::{info, instrument};

#[instrument(skip(db))]
async fn fetch_user(db: &Database, id: u64) -> Result<User, Error> {
    info!(user_id = id, "fetching user");
    // ...
}
```

**Rules:**

- Use `tracing` macros: `trace!`, `debug!`, `info!`, `warn!`, `error!`
- Use structured fields: `info!(count = n, "processed items")`
- Use `#[instrument]` for automatic span creation
- Use `skip(field)` to avoid logging sensitive or large data
- Never use `println!` for logging in production code

## Async

Use `tokio` for async runtime:

```rust
use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    // ...

    tokio::signal::ctrl_c().await?;
    Ok(())
}
```

## Dependencies

Prefer workspace dependencies:

```toml
[dependencies]
tokio = { workspace = true }
tracing = { workspace = true }
thiserror = { workspace = true }
```

## Testing

Place unit tests in the same file:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_valid_input() {
        let result = parse("valid");
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_async_operation() {
        // ...
    }
}
```

**Rules:**

- Use `rstest` for parameterised tests
- Use `tempfile` for temporary directories
- Use `criterion` for benchmarks (in `benches/`)

## Public API Documentation

```rust
/// Fetch a user by ID.
///
/// # Errors
///
/// Returns `UserError::NotFound` if the user doesn't exist.
pub async fn get_user(&self, id: u64) -> Result<User, UserError> {
    // ...
}
```

**Rules:**

- Keep descriptions brief - one line if possible
- Use `# Errors` section when returning `Result`
- Use `# Panics` section if function can panic
- Avoid `# Examples` unless genuinely non-obvious
