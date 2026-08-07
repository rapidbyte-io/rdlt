# Authoring an rdlt connector

How to build a connector on `rdlt-connector-sdk` — the shape every
in-tree connector converges on and the shape an out-of-tree connector
starts with. The worked examples are `rdlt-connector-rest` (a source)
and `rdlt-connector-postgres` (both halves); read them alongside this.

## The one-dependency rule

A connector's `[dependencies]` carries **exactly one rdlt crate**:

```toml
[dependencies]
rdlt-connector-sdk = { workspace = true, features = ["schema"] }
```

The protocol (SPI) is reached through the sdk's re-export — `use
rdlt_connector_sdk::spi::{SourceError, StreamSpec, ...}`, vocabulary via
`spi::core` — never as a direct dependency. The sdk forwards every SPI
feature under the same spelling: `failpoints` (crash sweeps), `schema`
(generated config schemas), `object-store`. `rdlt-testkit` is a
**dev**-dependency (the verification half), tolerated in
`[dependencies]` only as an *optional* dep behind a `fixtures` feature.
Two recorded exceptions exist and are enforced as such by
`rdlt-connector-sdk`'s `test_dependency_rule`: SQL destinations may
depend on `rdlt-connector-sqlcore` (the shared merge core), and nothing
else.

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
(`io.rapidbyte.postgres` → `rdlt-connector-postgres`); and the `Spec`
RPC answers it as the connector's static identity. A dotless `NAME`
stays self-consistent for a connector that only ever runs in-process —
the last-segment convention degenerates to the whole name — but going
out-of-process later then makes the rename an identity change, so spell
it reverse-DNS from the start. Both halves of a dual-role connector
report the SAME id.

The sdk's shell provides the whole SPI. Export the canonical face as a
type alias and the SPI arrives in one call:

```rust
pub type Shell = rdlt_connector_sdk::source::Shell<MySource>;
// callers: Shell::from_yaml(text)?  /  Shell::new(config)?
```

`Feed` makes cancellation a property of the type: every push returns
`ControlFlow`, and `Break` means the host hung up — return `Ok(())`
promptly. Never invent an error for a closed channel.

What stays yours: stream resolution against your config (including the
unknown-stream refusal, worded where the config's shape is known),
cursor semantics, error **classification** (your keys decide
Fatal/Transient/RateLimited — the engine's whole retry story), and
every message spelling.

### 3. Destination: `DestinationConnector` + `Backend`

You write the system IO; the sdk session owns the protocol
choreography:

```rust
impl DestinationConnector for MyDest {
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
defense in depth. The postgres destination is the reference for a
transactional backend; the sdk's example connector
(`rdlt-connector-sdk/tests/cases/example.rs`) for the minimal one.

## Certification — "certified = passes conformance"

The kits are the contract:

```rust
assert_conformant(verify_source(&shell).await);
assert_conformant(verify_destination(&shell, &probe).await);   // + your TableProbe
```

They run anywhere — no network, no containers. Container-backed suites
follow the gate posture: probe `rdlt_testkit::gate::runtime_available`,
print a visible `SKIP` and return early when absent (never panic), and
label every container `rdlt-test=1` so `make reclaim` can sweep it.
Crash points register with `assert_registry_matches_sources` and join
the sweep matrix.

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
