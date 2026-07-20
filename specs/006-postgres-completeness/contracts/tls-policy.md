# Contract: Postgres TLS Policy (source + destination)

**Feature**: 006-postgres-completeness | **Date**: 2026-07-20

One policy, both connectors. Supersedes the 005 "TLS not wired"
rejection (removed from code, contracts, READMEs by this feature).

## Modes (libpq vocabulary, libpq semantics)

| mode | encrypted | chain verified | hostname verified | plaintext fallback |
|---|---|---|---|---|
| `disable` | no | — | — | — |
| `prefer` (default) | if server offers | no | no | yes |
| `require` | yes | **no** | **no** | no |
| `verify_ca` | yes | yes | **no** | no |
| `verify_full` | yes | yes | yes | no |

`require`/`verify_ca` are deliberately weaker levels users explicitly
opt into (ecosystem-standard meaning); `verify_full` is the documented
production recommendation. `prefer` never validates (it exists for
opportunistic encryption only).

## Configuration

- Conn string `sslmode=disable|prefer|require` — honored (parsed,
  never string-matched).
- Config block (source YAML/JSON; destination builder/CLI TOML):
  `tls: { mode: <any of five>, root_cert: <path or inline PEM> }`.
  verify-* is expressible ONLY here.
- Both present and contradictory (e.g. conn `sslmode=disable` +
  `tls.mode: verify_full`) → typed CONFIG error; matching or
  block-only or conn-only compose fine.
- Trust roots: `root_cert` when given, else the platform trust store.
  verify-* with no resolvable root → typed CONFIG error at open.

## Error taxonomy (typed, phase-tagged)

- Open/config phase: contradictory mode, unreadable/unparseable root
  (names the path), verify-* without roots.
- Connect phase, distinguished: missing trust anchor ("unknown CA —
  supply tls.root_cert"), chain invalid (expired/wrong usage),
  hostname mismatch (verify_full only), server refuses TLS under
  require/verify-*.

## Conformance obligations

Matrix tested against a real TLS-enabled Postgres (generated CA):
all five modes × {hostname match, mismatch, unknown CA} × {source,
destination}, including `prefer` fallback against a plaintext server
and `require` succeeding against a self-signed cert. Client
certificates (mTLS), GSSAPI, and revocation tuning are OUT (recorded).
