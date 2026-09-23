# ADR 0001: Engine architecture

Status: accepted, 2026-09-23.

## Context

rdlt is a from-scratch rewrite of an earlier engine whose core idea held up (cursors committed in
the same destination transaction as the data, idempotent on `(load_id, commit_seq)`) but whose
implementation had silent correctness bugs, a serial data path and inconsistent code.

## Decision

| Topic | Decision |
|---|---|
| Deployment | Open-core library and CLI; the managed cloud runs the same engine as stateless workers |
| Trust | Connectors are trusted code; their output is untrusted data; isolation is the platform's job |
| Runtime | A purpose-built Arrow dataflow with engine-driven checkpoint barriers; DataFusion only as an optional transform stage |
| Exactly-once | Segments sealed by checkpoints, published atomically with state; epoch fencing in the destination |
| Determinism | All nondeterminism (time, randomness, scheduling) enters through `Env`, so the engine runs under deterministic simulation |
| Connectors | First-class Rust and Python SDKs over one gRPC + Arrow protocol; production connectors live in `rdlt-connectors` |
| Platforms | Linux and macOS |

## Consequences

- Every engine module is written against `Env`; clippy bans direct clock, randomness and spawn
  calls inside `rdlt-engine`.
- The simulation harness (`rdlt-sim`) is the primary correctness oracle, backed by property,
  differential, crash and mutation testing.
