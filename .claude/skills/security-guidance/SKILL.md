---
name: security-guidance
description:
  "Security baseline for this repository: OWASP basics as they apply here. Use for any
  security-sensitive change or security review."
---

# Security guidance

Baseline for any security-sensitive change or review in this repository.

## OWASP basics, as they apply here

- **Injection** — never interpolate untrusted input into shell commands, SQL, or HTML. Parameterise
  or escape at the boundary.
- **Secrets** — no secret in code, tests, fixtures, or example files. Secrets live in the secrets
  manager and arrive as env vars; `env-example` values stay empty. Where a repo carries
  `env-manifest.json`, recommended values and generation recipes live there instead — a
  `secret: true` var still never holds a static value (env-parity blocks it).
- **Auth** — validate input at every trust boundary, including data from the frontend (a server-side
  handler treats its caller as untrusted).
- **Dependencies** — no new direct dependency without justification; prefer the standard
  library/runtime.
