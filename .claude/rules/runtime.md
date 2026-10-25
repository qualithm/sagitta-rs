---
description: "Runtime conventions for errors, logs, settings and CLIs"
paths:
  - "**/*.rs"
---

# Runtime Conventions

One set of rules for errors, logs, settings and CLIs holds in every language, so logs and errors
from different repos read and search together. This is how they look here.

## Errors

- A library's errors are a `thiserror` enum, `{Component}Error`. Messages are lowercase, with no
  trailing punctuation, and name what failed (`claim code expired`).
- A binary handles errors once, in `main`.

## Logging

- Log with `tracing`: structured fields plus a `snake_case` event name as the message
  (`info!(item_count = n, "items_processed")`), never a sentence with values interpolated.
- Field keys are `snake_case` in every language, so one query reads every service. The shared names
  are `team_id`, `device_id`, `request_id`, `trace_id`, `duration_ms`, and `error` for the error,
  never `e` or `err`.
- Library crates never print and install no subscriber.
- Logs aren't redacted, so a credential in a log is a leak to fix, not to mask.

## Settings

- Config structs are `{Component}Config` with production defaults.
- Units go in names: `_ms` for durations and `_bytes` for sizes, in config fields and env vars
  (`POLL_INTERVAL_MS`). Keep an existing `_SECS` or `_SECONDS` name: renaming a deployed variable
  gains nothing.
- Env var prefix: software that runs on a builder's machine or device (a CLI, an SDK) uses
  `QUALITHM_`; a service uses its own name (`VARV_`) or none. Standard names (`PORT`, `LOG_LEVEL`)
  stay unprefixed, and credentials keep their own naming rule.

## CLIs

A binary follows this contract:

- Exit `0` on success, `1` on a failure or a problem found, `2` on invalid usage. Codes from `3` up
  are allowed, each mapped one-to-one from an error code in the `--json` envelope.
- `--json` prints one envelope line, `{ok, code?, message?, dryRun?, data?}`. Its `code` values are
  stable, because agents branch on them.
- Every command that changes something takes `--dry-run`, and a dry run exits `0`.
- Without `--json`, errors go to stderr as `<binary>: <message>`.
