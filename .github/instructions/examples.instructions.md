---
description: "Conventions for runnable example binaries"
applyTo: "crates/*/examples/**"
---

# Example Conventions

## File Structure

1. Module-level doc comment: single `//!` line — `//! Example: <title and brief description.>`
2. Imports from `std`, then external crates, then workspace crates (alphabetical within each group)
3. Named sections separated by a banner comment (see below)
4. Helper types/functions, each preceded by a `/// Doc comment.`
5. `main()` as the single entry point

## Section Banners

Use a dashed banner to visually separate top-level sections:

```rust
// ---------------------------------------------------------------------------
// Section name
// ---------------------------------------------------------------------------
```

Typical sections: custom types/actions, then `// Entrypoint`.

## main() Pattern

- Synchronous: `fn main()` called directly
- Asynchronous: `#[tokio::main] async fn main() -> anyhow::Result<()>`
- Server (long-running): wrap setup in `main()` — no "Done." epilogue

## Custom Types

When an example requires a custom type (e.g. a trait implementation), define it as a struct with an
`impl` block above `main()`, inside its own named section. Add a `/// Doc comment.` on the struct
describing what it does.

## Running Examples

```bash
cargo run --example EXAMPLE_NAME
```

## README

Each `examples/` directory has a `README.md` with:

1. `# Examples` heading
2. Brief intro: "Runnable examples demonstrating [crate] usage."
3. `## Prerequisites` (if external services required)
4. `## Environment Variables` table (if any)
5. `## Running Examples` with `cargo run --example` commands
6. `## Example Files` table: `| File | Description |`
