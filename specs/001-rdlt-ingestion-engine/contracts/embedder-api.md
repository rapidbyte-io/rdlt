# Contract: Embedder API (`rdlt` facade)

**Status**: v1 target · **Stability**: public API of the facade crate; product-facing
types (`RunReport`, `PipelineEvent`, `RdltError`) are `#[non_exhaustive]` with stable
serde and live in `rdlt-core`.
**Audience**: applications embedding the engine (rapidbyte platform, user services, CLI).

## Shape

```rust
let mut pipeline = Pipeline::builder("github_issues")
    .source(RestSource::from_yaml(include_str!("github.yaml"))?)
    .destination(Postgres::connect(&url)?.dataset("raw"))
    .write_mode(WriteMode::Merge { key: &["id"] })     // default: Append
    .schema_policy(SchemaPolicy::evolve())             // default
    .workdir(".rdlt")                                  // default; holds WAL
    .build()?;                                         // typestate; see B1–B3

let mut events = pipeline.events();                    // typed event stream
let report: RunReport = pipeline.run().await?;         // resumable; cancel-safe
```

## Behavioral clauses

### Build time (fail-fast)

| # | Clause |
|---|---|
| B1 | Builder uses typestate: missing source or destination is a **compile** error, not a runtime one. |
| B2 | `build()` validates configuration against `DestCapabilities` pre-I/O: e.g. `Merge` requested but unsupported, ident-rule conflicts — die here, never mid-run. |
| B3 | `build()` performs no network or destination I/O; first I/O happens in `run()`. |

### Run time

| # | Clause |
|---|---|
| R1 | `run()` is resumable: on start it recovers state from the destination (and WAL if present) and continues per the crash matrix; callers do nothing special to resume. |
| R2 | Cancellation (token or drop) is safe at any instant and equivalent to a crash — the next `run()` recovers identically. |
| R3 | `events()` yields typed `PipelineEvent`s in causal order (a table's `SchemaEvolved` precedes the first `BatchLoaded` at the new version; `Committed` follows everything it covers). |
| R4 | `RunReport` totals match destination-visible reality; retries, widenings, and discards are all counted — no silent failures. |
| R5 | Concurrent `run()` on the same pipeline/workdir is refused (single-process-per-pipeline is an embedder responsibility; the engine locks the workdir defensively). |

### Errors

| # | Clause |
|---|---|
| X1 | `RdltError` variants map 1:1 to operator actions: `Config` → fix config; `Schema(ContractViolation)` → unfreeze/adjust contract; `Source{stream, ..}` → check upstream; `Destination{..}` → check warehouse; `Wal(..)` → check local disk. |
| X2 | Every error names its locus (stream/table/column where applicable). |
| X3 | `thiserror`-derived; `anyhow` never crosses this seam. |

### Observability

| # | Clause |
|---|---|
| O1 | Two channels, both always on: typed `events()` stream (for product UIs) and `tracing` spans `rdlt.extract` / `rdlt.shred` / `rdlt.load` (for ops). |
| O2 | `PipelineEvent` and `RunReport` are serde-stable: platforms may persist and re-render them across engine upgrades (format-versioned). |

## CLI (`rdlt-cli`, dev tool — thin wrapper, no independent behavior)

- `rdlt run <pipeline.toml>` → executes via the facade, streams events to stderr
  (human-readable), writes `RunReport` JSON to stdout or `--report <path>`.
- Exit codes mirror `RdltError` variants (documented, stable) so shell scripts can branch
  on failure class.
- Everything the CLI can do, the library can do — the CLI adds zero engine capability.
