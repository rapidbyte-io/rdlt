# rdlt-connector-sdk

Connector authoring scaffolding — the OPTIONAL layer of rdlt's connector
SDK. The protocol lives in `rdlt-connector`; the conformance suites live
in `rdlt-testkit`; this crate carries what two or more connectors were
**measured** to implement identically, extracted once. A connector may
use it or ignore it; hosts never depend on it.

The extraction bar (recorded in `specs/027-sdk-trio/plan.md`, Wave 2):
only seams whose adoption keeps every existing message byte-identical.
Two candidates failed that bar on evidence and were deliberately NOT
extracted — cursor watermarking (two genuinely different machines) and a
phase-tagged error skeleton (six connectors classify on six different
keys; a text skeleton would break source-chain downcasting). What
cleared it:

## `config::Document`

The validated config-document contract. Every rdlt connector accepts
configuration as YAML text, JSON text, or an already-parsed
`serde_json::Value` (the embedder path), and every entry must run the
same validation. The trait makes that a construction property: implement
`validate` (the one gate, in your crate, with your error type and your
message spellings) and inherit `from_yaml`/`from_json`/`from_value`.

The seam renders **no text**. The associated `Error` type only has to
absorb the two parser errors via its own `From` impls — which is what
lets every connector's frozen, connector-specific wording survive
adoption unchanged ("parsing postgres source config: …" and
"invalid REST source YAML: …" disagree in spelling, and one connector
deliberately leaves parse errors unprefixed; a message-rendering seam
could reproduce none of that).

## `config::schema_of` (feature `schema`)

The one-line body behind every connector's `config_schema()`: the JSON
Schema generated from the config structs themselves, so the declared
schema and the parser cannot drift. Connectors keep their own public
`config_schema()` functions delegating here.

## Dependencies

Pure serde (`serde`, `serde_yaml`, `serde_json`; `schemars` behind the
`schema` feature). No rdlt dependencies at all — an out-of-tree
connector consumes this crate exactly as an in-tree one does.
