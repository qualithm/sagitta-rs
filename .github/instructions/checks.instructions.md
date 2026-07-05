---
applyTo: "**"
description: "Exact pre-commit commands for the rust-lib CI archetype, kept in sync with ci.yaml"
---

# Pre-commit Checks

This repo's `ci.yaml` is generated from `dx/ci-templates/rust-lib.yaml` via `dx ci sync` (check for
drift with `dx ci drift`). Run these before committing so CI passes on the first try:

```bash
cargo fmt --all -- --check
cargo check --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

On PRs targeting `main`, CI additionally blocks on:

```bash
RUSTDOCFLAGS="-Dwarnings" cargo doc --workspace --no-deps   # doc warnings fail this
cargo llvm-cov --workspace --lcov --output-path lcov.info   # line coverage must be >=80%
```

`cargo doc` warnings (broken intra-doc links, etc.) are easy to miss locally since
`cargo build`/`clippy` don't surface them — run it whenever public API docs change.
