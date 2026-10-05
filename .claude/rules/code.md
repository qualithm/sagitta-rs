# Rust Code Guidelines

`cargo fmt` and `cargo clippy` enforce formatting, naming and most idioms. Fix what they flag; this
file covers what they can't.

## Layout

- Group `use` statements std → external crates → workspace crates → `crate::`, with a blank line
  between groups. Prefer `crate::` paths over `super::` and explicit imports over globs.
- `lib.rs` holds module exports, `config.rs` config structs and defaults, `error.rs` error types.
  Depend on workspace dependencies (`tokio = { workspace = true }`).
- Unit tests sit in the same file under `#[cfg(test)] mod tests`; integration tests in the crate's
  `tests/`; benchmarks use `criterion` in `benches/`. Use `tempfile` for temporary directories.

## Errors

- `thiserror` enums (`{Component}Error`) in libraries, `anyhow::Result` in binaries. Messages are
  lowercase with no trailing punctuation. Use `#[from]` and `?`; prefer `Result` over panics.
- Public APIs carry `///` docs, brief, with `# Errors` when returning `Result` and `# Panics` when
  they can panic. Module docs are one line (`//! HTTP client utilities.`). No architecture
  overviews, ASCII diagrams, feature lists or how-to guides in comments.

## Runtime

- Async runs on `tokio`.
- Log with `tracing` macros and structured fields (`info!(count = n, "processed items")`); use
  `#[instrument]` with `skip(...)` for sensitive or large arguments. Never `println!` in production
  code.
- Config structs are `{Component}Config` with `///` on each field, units in names or comments
  (`timeout_ms`), and a `Default` impl with production values.

## When code changes

A behavior change carries a test change; update benchmarks if performance changes, doc comments if
the public API changes, and drop dependencies nothing uses.

## Environment variables

Adding or renaming an env var is a two-file change in one commit: declare it in `env-example` and
classify it in `env-manifest.json` (a per-environment static, a `generate` recipe, or an `obtain`
pointer to where the value comes from). Run `dx env local` before committing; it fails when a
declared key is missing from the local `.env`. `*-example` template repos carry an empty
`env-example` and no manifest.

## Generated files

`.github/workflows/ci.yaml` is generated from `dx/ci-templates/`; change the template and run
`dx ci sync` from dx, never edit it here. `CI Required` is the single required status check.
