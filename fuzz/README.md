# Fuzzing

Fuzz targets for the ingest path, run with [cargo-fuzz](https://github.com/rust-fuzz/cargo-fuzz).
The `do_put_ingest` target sends each input to `SagittaService::do_put` as a raw gRPC request body,
over a `MemoryStore`.

## Prerequisites

A nightly toolchain and cargo-fuzz:

```bash
rustup toolchain install nightly
cargo install cargo-fuzz
```

## Run a target

From the repo root, fuzz with the checked-in seeds. New inputs go to `fuzz/corpus/`, so the seeds
stay unchanged:

```bash
cargo +nightly fuzz run do_put_ingest fuzz/corpus/do_put_ingest fuzz/seeds/do_put_ingest
```

## Reproduce a crash

A failing input lands in `fuzz/artifacts/do_put_ingest/`. Run the target on that file alone:

```bash
cargo +nightly fuzz run do_put_ingest fuzz/artifacts/do_put_ingest/<crash-file>
```

Shrink it before writing a regression test:

```bash
cargo +nightly fuzz tmin do_put_ingest fuzz/artifacts/do_put_ingest/<crash-file>
```
