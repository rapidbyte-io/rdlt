# ADR 0001: Engine architecture

Status: accepted, 2026-09-23.

## Context

rdlt is written from scratch around one idea of an earlier engine: cursors committed in the same
destination transaction as the data, each commit applied once. It takes none of that engine's
code, whose silent correctness bugs, serial data path and inconsistent code the decisions below
are made to avoid.

## Decision

| Topic | Decision |
|---|---|
| Deployment | Open-core library and CLI; the managed cloud runs the same engine as stateless workers |
| Trust | Connectors not compiled into the host's binary are untrusted code, and every connector's output untrusted data: the engine and the host survive any connector they do not link (ADR 0037) |
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
