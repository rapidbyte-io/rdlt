# Authoring an rdlt connector

How to build a connector on `rdlt-connector-sdk` — the shape every
first-party connector converges on and the shape an out-of-tree
connector starts with. The living in-tree example is
`rdlt-connector-reference` (both halves, deliberately small — the
contract's own connector, which this repo's gates spawn and certify);
the seven first-party connectors in the sibling
[rdlt-connectors](https://github.com/rapidbyte-io/rdlt-connectors)
repository are the production-shaped worked examples (`rest` as a
source, `postgres` as both halves). Read them alongside this.

## The one-dependency rule

A connector's `[dependencies]` carries **exactly one rdlt crate**:

```toml
[dependencies]
rdlt-connector-sdk = { workspace = true, features = ["schema"] }
```

The protocol (SPI) is reached through the sdk's re-export — `use
rdlt_connector_sdk::spi::{SourceError, StreamSpec, ...}`, vocabulary via
`spi::core` — never as a direct dependency. The sdk forwards every SPI
feature under the same spelling: `failpoints` (crash sweeps) and
`schema` (generated config schemas). Storage-, format-, and
driver-specific pieces (parquet options, part rolling, PEM material,
object-store retry rules) are the connector's own: the sdk carries only
what is true of every connector by virtue of the protocol, so a
connector that needs one of those owns its copy, sized to its use.
`rdlt-testkit` is a **dev**-dependency (the verification half),
tolerated in `[dependencies]` only as an *optional* dep behind a
`fixtures` feature.
One recorded exception exists: SQL destinations may depend on
`rdlt-connector-sqlcore` (the shared merge core, which lives with them
in rdlt-connectors), and nothing else. `rdlt-connector-sdk`'s
`test_dependency_rule` enforces the rule for in-tree connectors.

Hosts are the mirror image: the engine and embedders depend on
`rdlt-connector` alone and never on the sdk.

## The three seams you implement

### 1. `config::Document` — the validated config document

Your config is DATA (serde structs, no callbacks). Implement the one
gate; the sdk provides `from_yaml`/`from_json`/`from_value`, each of
which parses **then validates** — there is no path around the gate.

```rust
impl Document for Config {
    type Error = ConfigError;               // your own frozen wordings
    fn validate(&self) -> Result<(), ConfigError> { ... }
}
```

Your error type absorbs the two parser errors through its own `From`
impls, so every message spelling stays yours. The seam renders no text.
Generate the schema from the same structs (`config::schema_of`, or
schemars directly) so declaration and parser cannot drift.

### 2. Source: `SourceConnector` + `Feed`

```rust
#[async_trait]
impl SourceConnector for MySource {
    const NAME: &'static str = "io.example.mysource";
    const VERSION: &'static str = env!("CARGO_PKG_VERSION");
    type Config = Config;
    fn assemble(config: Config) -> Result<Self, ConfigError> { ... }
    fn config_schema() -> Option<serde_json::Value> { ... }
    async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError> { ... }
    async fn read_stream(&self, stream: &StreamSpec, since: Option<Cursor>,
                         feed: &mut Feed) -> Result<(), SourceError> { ... }
}
```

`NAME` is the connector's **id**, spelled reverse-DNS (in-tree:
`io.rapidbyte.<name>`, e.g. `io.rapidbyte.postgres`). One const carries
three derived facts: the wire handshake reports it and the client
verifies it by strict equality against the requirement's id; discovery
resolves the id's **last segment** to the binary name on PATH
(`io.rapidbyte.reference` → `rdlt-connector-reference`); and the `Spec`
RPC answers it as the connector's static identity. A dotless `NAME`
would still resolve — the last-segment convention degenerates to the
whole name — but the id is the connector's public identity, verified
by strict equality on every handshake, so a later rename is an
identity change: spell it reverse-DNS from the start. Both halves of a
dual-role connector report the SAME id.

A connector binary MUST refuse an unsupported `--role` by exiting with
code 2 before writing any stdout byte. The handshake line is written
only for a role the binary serves. The role-less schema probe relies on
exactly this signal to retry a destination probe after a source
refusal; no other exit or stdout-producing failure means "unsupported
role." First-party binaries obtain this refusal from clap, and their
spawn suites pin the exit code and empty stdout.

The sdk's shell provides the whole SPI, and a connector runs as a
PROCESS: `serve` builds the shell from the handshake's config document
and serves it over the wire, and `serve_main!` is the binary's whole
`main` — one role arm per half you implement:

```rust
rdlt_connector_sdk::serve_main! {
    about: "my connector — what goes in, what comes out",
    roles: {
        Source => rdlt_connector_sdk::serve::source::run::<MySource>(),
        Destination => rdlt_connector_sdk::serve::destination::run::<MyDestination>(),
    }
}
```

Export no in-process door: a connector's own tests may build the same
face directly — `rdlt_connector_sdk::source::Shell::<MySource>::from_value(..)`
(or `from_yaml`/`from_json`/`new`) — to drive it and the kits without
spawning, and that is the one in-process use.

`Feed` makes cancellation a property of the type: every push returns
`ControlFlow`, and `Break` means the host hung up — return `Ok(())`
promptly. Never invent an error for a closed channel.

Memory, when spawned: the serve loop that carries your frames to the
wire is BYTE-bounded, so a spawned connector's in-flight encoded
frames are capped by the sdk's own budget — your producer parks
behind a slow consumer rather than buffering ahead without limit. `BYTE_FRAME_BUDGET` in the
sdk's `serve/source.rs` owns the numbers and the worst-case
arithmetic. Two consequences: your own batch/page sizing decides
FRAME sizes (the budget prices frames by what they weigh, so smaller
frames pipeline more smoothly than a few enormous ones), and the
engine-side `batch_policy.every_bytes` is the ENGINE's accumulate
cadence — it never reaches, and never needs to reach, your process's
buffers.

What stays yours: stream resolution against your config (including the
unknown-stream refusal, worded where the config's shape is known),
cursor semantics, error **classification** (your keys decide
Fatal/Transient/RateLimited — the engine's whole retry story), and
every message spelling.

### 3. Destination: `DestinationConnector` + `Backend`

You write the system IO; the sdk session owns the protocol
choreography:

```rust
impl DestinationConnector for MyDestination {
    ...same constants/config as a source...
    type Backend = MyBackend;
    fn capabilities(&self) -> DestinationCapabilities { ... }  // truthful!
    async fn connect(&self, ctx: &OpenContext) -> Result<MyBackend, _> { ... }
}
```

`connect` MUST make a crashed predecessor's staging invisible and
reclaimable (the open contract). The `Backend` then serves:

- `ensure_table` — idempotent DDL/migration.
- `write` — stage a batch, invisibly (D1 is a storage property; no
  wrapper can add it for you).
- `existing_receipt(load_id, commit_seq)` — a LOOKUP ONLY: the receipt
  this key already published under, or `None`.
- `replay(meta, receipt)` — housekeeping for a replayed commit. Not
  always a no-op: clear redelivered staging (or a later commit
  publishes it twice) and re-mark once-per-load guards. The default
  no-op is correct only for direct-publish backends.
- `publish(meta)` — atomically publish everything staged AND persist
  `meta.state`, returning the new receipt.
- `read_state` — the last committed state.

The sdk session refuses a write to a never-ensured table and always
consults `existing_receipt` (then `replay`) before `publish` — you
never re-implement the idempotence dance, but your storage must still
make publish-with-state atomic. Keep a transactional receipt guard as
defense in depth. The postgres destination (in rdlt-connectors) is the
model for a transactional backend; `rdlt-connector-reference`'s jsonl
destination for a direct-publish one; the sdk's example connector
(`rdlt-connector-sdk/tests/cases/example.rs`) for the minimal one.

### Load identity

Load ids MUST be globally unique across every pipeline that shares a
destination — that uniqueness is the contract the whole idempotence
dance keys on. Every shipped destination's receipt and convergence
lookup keys on load identity ALONE, never the pipeline: the shared
sqlcore receipt table asks `WHERE load_id AND commit_seq` with no
pipeline column (postgres, duckdb ride it), and iceberg's snapshot
convergence matches `(load_id, commit_seq)` across pipeline scopes —
so a re-attempt that reaches the store under a different pipeline
scope (an orchestrator re-scope, the certify kill matrix's sibling
re-run) still converges instead of duplicating. The engine's
entropy-bearing load ids guarantee the uniqueness; an embedder that
mints its own load ids owns the same guarantee — a deterministic or
reused id (`load-1` from two orchestrators) would read one pipeline's
commit as another's replay, silently.

## Certification — "certified = passes conformance"

The kits are the contract:

```rust
use rdlt_testkit::conformance::{self, assert_conformant};

assert_conformant(conformance::source::verify(&shell).await.expecting_no_skips());
assert_conformant(conformance::destination::verify(&shell, &probe).await.expecting_no_skips()); // + your TableProbe
```

`shell` is the face your test builds in-process
(`Shell::<MySource>::from_value(..)`); the wire-side certifier
(the `rdlt-certify` binary) judges the BUILT binary
against the same clauses plus the protocol and kill clauses, which is
what this repo's gates run against the reference connector. The kits
run anywhere — no network, no containers. Container-backed suites
follow the gate posture: probe `rdlt_testkit::gate::runtime_available`,
print a visible `SKIP` and return early when absent (never panic), and
label every container `rdlt-test=1` so `make reclaim` can sweep it.
Crash points register with
`rdlt_testkit::scanner::assert_registry_matches_sources` and join the
sweep matrix.

## House rules that reviews enforce

- **Naming**: module paths are canonical (no crate-root re-export
  soup); types don't repeat their module's noun (`source::Postgres`,
  `tls::Policy`); no `name.rs` beside a `name/` directory — pure-TOC
  `mod.rs`, code under its noun; tests are `integration.rs` +
  `cases/test_<noun>.rs` with sentence-style names.
- **Frozen surfaces**: operator-facing message spellings, YAML/JSON
  vocabulary, persisted state formats, and wire behavior are frozen
  once shipped; rewrites prove parity against them (ported suites,
  byte-identical assertions).
- **Errors**: typed taxonomy, no citation IDs in user-facing strings,
  context wraps the INNER cause exactly once — never double-frame.
- **No unsafe** (workspace-denied), zero warnings (clippy/rustdoc `-D
  warnings`), comments explain constraints the code can't show.
- **Review**: adversarial lenses (contract parity, fresh-eyes
  correctness, test adequacy) until clean; every finding fixed gets a
  pin; the gate runs twice clean untouched at the end.
