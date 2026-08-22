# Security policy

## Supported versions

rdlt is pre-release and supports only the current repository version. Security
fixes may change on-disk formats, configuration rules, and connector behavior
in place; legacy formats and compatibility shims are not supported.

## Trust boundaries

- Pipeline and connector configuration documents may be attacker-influenced.
  Documents are size-capped (8 MiB) before they are read, and a raw-text
  token-start scanner refuses YAML anchors and aliases — plus the few
  spellings it cannot decide line-locally (quoted scalars spanning lines,
  quote/tag/block-scalar indicators where a plain scalar may continue,
  verbatim tags) — before any deserialization, at both the pipeline-spec
  parse and the connector SDK's `Document::from_yaml`. An accepted document
  is therefore a tree, and what the parser materializes is bounded by the
  text that spells it.
- Installing or selecting a connector binary is an operator trust decision. A
  spawned connector executes with the operator's OS identity, but receives a
  scrubbed environment and a dedicated process group. Its wire frames,
  identifiers, socket advertisement, and error text are still treated as
  untrusted inputs for host availability, path safety, and terminal safety.
  Bounded for availability: declared streams per source (default 1024,
  `config::Config::with_max_streams_per_source`), rows and JSON values per push
  (1 M rows; 16 M object fields/array elements), columns per table (4096),
  tables per stream (64 Ki), batch-assembly cells (columns × rows, default
  2²⁸, `config::Config::with_max_batch_cells`), wire frames (64 MiB), untyped
  JSON documents (config, spec, and state documents 8 MiB; cursors 4 MiB
  measured on their serialized form — the WAL line budget), declared
  identifier lengths and key counts (names 1 KiB at the wire, 8–4096 for
  destination identifier rules), and rows per decoded Arrow batch (1 M at
  every decode seat, with width counted at every nesting depth and sibling
  names unique at every level), cursor node counts (64 Ki at every seat
  that parses one), part-close events per destination call (64 Ki, and only
  for tables the session ensured), ensured tables per session (64 Ki), and
  the document's own resource knobs (each with a ceiling a document cannot
  exceed). In-flight read bytes are capped by one budget shared across a
  run's streams — reserved on a frame's encoded size before it is decoded,
  and charged for a checkpoint's cursor as well as a batch. An admitted (gate-legal) frame can still expand
  in memory when it becomes typed values — a maximally dense legal streams
  reply retains about 5× its wire bytes as parsed specs
  (the arithmetic is derived at the client's declaration-decode seat,
  from the shape that maximizes it, so it moves with the type rather
  than drifting here) — and that typed
  floor is the accepted residual: the ceilings bound what a frame may
  DECLARE, and declaration-layer amplification beyond it (collections
  materialized before any gate could run) has been removed from the wire
  shapes themselves. Every Arrow decode seat runs the shared IPC
  framing pre-pass (declared lengths must fit the bytes that carry them)
  plus a panic belt. Connector diagnostic text (ErrorFrame messages,
  Status text) is rendered escaped and bounded (4 KiB); a served source
  admits at most 1024 concurrent reads (refused past it). A read's permit
  is held for as long as the client keeps its response stream open, with no
  stall deadline on purpose: the engine parks a read exactly when its
  destination is slow (the byte budget is the backpressure), so a deadline
  there would fail honest slow pipelines to reap a rogue client that the
  standing model already assumes can exhaust the process in other ways.
- Structured (Arrow) input REFUSES typed any cast arrow would round, wrap,
  or null; it never discards.
- Connector configuration can contain credentials, and must never be echoed
  in protocol errors. The engine and runtime send it only over the private
  local Unix socket of a connector they spawned. The sdk also exports a TCP
  binding (`serve::{source,destination}::run_on_tcp`, paired with the
  client's `Endpoint::Address`) for deployments that host a connector
  elsewhere: that binding is plaintext at this layer by design, the listener
  and its reachability are the deployer's, and confidentiality of the
  configuration crossing it (mTLS or an equivalent wrap on both ends) is the
  deployment's to provide. rdlt itself never dials it.
- Each pipeline work directory is private to the rdlt process. The WAL uses an
  ownership marker before recursive cleanup and refuses symlinks at file-open
  boundaries (including a symlinked WAL directory itself). A process that can
  write that directory has the same OS-level authority as rdlt: manifest
  BLAKE3 trailers detect accidental/torn damage; they do not authenticate
  records against a directory writer. Manifest reads are bounded (5 MiB per
  line, 1 GiB total, 8 KiB rules sidecar), and WAL segment footers must
  declare extents inside their own file (per block and in sum, and within
  what the file has actually allocated — a sparse file lying about its own
  size refuses as damage, never committing its holes resident).
- Corrupt or undecodable WAL data degrades to source re-extraction when safe.
  A WAL naming a different pipeline is preserved and refused rather than
  cleared.

## Reporting a vulnerability

Please report suspected vulnerabilities privately through GitHub's private
vulnerability reporting on the `rapidbyte-io/rdlt` repository (Security →
"Report a vulnerability", or
<https://github.com/rapidbyte-io/rdlt/security/advisories/new>) rather than
opening a public issue. Include the affected revision, reachable input
boundary, impact, and a minimal reproducer when possible. Do not include live
credentials or production data.
