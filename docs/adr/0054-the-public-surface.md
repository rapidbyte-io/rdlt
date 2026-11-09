# ADR 0054: The public surface

Status: accepted, 2026-11-06.

## Context

A pipeline was forty lines of plumbing: connector factories or a chain of providers, a plan, a
configuration, an environment and a run. Every surface (Rust, a command line, Python) needs the
same model, checks, errors and events; and ADR 0037 separates who writes a pipeline from who
operates its host.

## Decision

- **One model in one crate.** `rdlt` is the facade. A `Document`, or the `PipelineBuilder` that
  builds one, is what a pipeline's author writes; a `Host` is what its operator allows, and
  nothing in a document widens it. One lowering takes a document onto the engine's plan and
  configuration, and the connectors' references come from its two sides, so a mistake fails with
  the same error from code or a file. The engine parses no configuration: the document's types
  are the facade's, each word mapped onto the engine's, and `Document::json_schema` gives their
  JSON Schema.
- **One vocabulary.** Host: `connectors`, `trust_binaries`, `allow_network`, `writable`,
  `readable`, `always_readable`, `allow_env`, `allow_files`, `secret_store`, `remote`, `source`,
  `destination`, `transform`. Author: `access: { read, write, network }`, secret references,
  `connector: <id>` or `{ id, version, digest, endpoint }`, `transform: [{ code: name }]`.
  `rdlt-host`'s builders use the same words; its `Registry` keeps `trusted_source`, as `source`
  is its `Provider` method. The engine's concurrent destination writers are writers, each holding
  a table writer per table it writes.
- **Placement follows the reference.** A registered id runs in process, a reference with an
  endpoint runs remotely, anything else is a local binary from the host's connector directories.
  A placement the host was not given is refused (`placement_refused`), never made another way.
  `Host::new()` runs local connectors in bubblewrap and `.trust_binaries()` opts out; where no
  sandbox exists, the refusal names both remedies.
- **The network is the operator's to allow.** Nothing a document says widens what its host
  allows: `access: { network: true }` on a sandboxed local connector is refused
  (`placement_refused`, naming the setting) unless the host calls `allow_network`, as its paths
  lie within the roots the host names. A trusted binary has the host's access already, and a
  remote connector its own network.
- **Code is named, never embedded.** A document refers to in-process connectors and code
  transforms by the names the host registered.
- **A mask key is a secret reference.** It is one `${env:NAME}`, `${file:/path}` or
  `${secret:name}`, which the host resolves as it resolves a configuration's. A key written out
  is refused (`mask_key_literal`), never written into a document, and shown nowhere: a
  `MaskStep`'s `Debug` shows a key only as a reference, and no refusal quotes it.
- **No refusal quotes a value.** Each place in a document refuses a value of the wrong kind, a
  scalar, a list or a mapping, by the kind it found (text, an integer, a fraction, a boolean, a
  list, a mapping) and the kind it expected, and a word by the choices it could have been; never
  by the value itself, since a value may be a secret. A refusal is raised at the value's place,
  so a reader of the document gives the path to it.
- **The document is hostile input.** YAML or JSON, read by serde-saphyr: at most
  `DOCUMENT_BYTES` (1 MiB), nesting at most `DOCUMENT_DEPTH` (32), at most `DOCUMENT_NODES`
  nodes. A pass over granit-parser's events refuses anchors, aliases, tags, repeated keys, a
  second document and depth and nodes past the limits, each at its place; the reader then refuses
  a merge key and holds the same limits. There are no includes: a value kept in a file is a
  `${file:…}` reference the operator scopes. An error says `file:line:col`, the key path and the
  nearest known key within two edits, and never quotes a value. A connector's `config` is
  free-form and checked by the connector after its handshake. `Document::json_schema` is the
  document's schema; `HostFile` is the operator's file, read the same way.
- **Streams are named as the catalog shows them.** A stream's name in a document is its
  namespace, a dot and its name, or its name alone, and resolves to the catalog's stream that
  shows as it; a name two of the catalog's streams show as is refused (`stream_ambiguous`). A
  name holding `*` or `?` selects each stream of the catalog it matches, a stream named outright
  wins over a pattern, and a pattern that matches no stream of the catalog is refused
  (`pattern_unmatched`).
  A reset names a stream the same way, against the catalog as it is then, or by its identity
  (`StreamSelector`), for a namespaced stream neither the plan nor the catalog holds any more;
  a shown name that splits more than one way with neither to say which is refused, never guessed.
- **`build` checks everything that needs no data, in two passes.** Before a connector is placed:
  the document's version and names, its connectors' references, its commit, resources and retry,
  and each stream entry's modes, keys, columns, hints and transforms, with mask keys resolved,
  SQL parsed and code transforms found among the host's. Each entry is lowered once, and every
  stream it selects shares what was lowered, so a pattern over many streams resolves its keys
  once; only its SQL is parsed again for each stream, into a transform of its own that plans
  once for each input schema it meets, so no stream's plans push out another's. Then both
  connectors are placed and handshaken, the streams selected from the source's catalog, and each
  checked against the catalog and the destination's capabilities (`Engine::check`, which opens no
  session). What depends on what earlier runs committed, such as a stream's key against its
  table's, a run's start checks. A reset opens a session and so fences a run in progress, as a
  write should. Nothing reads state back until the destination contract has a read that fences
  nothing.
- **Errors.** One `Error` covers building and running, with the engine's kinds; the host's and
  the facade's codes join the engine's, which an embedder reads and never makes; a connector's
  code stays apart. A refusal says what to write instead.
- **Events.** Each run keeps a ring of events, `EVENT_BUFFER` by default and at most
  `EVENT_BUFFER_MAX`, which each subscriber reads at its own place; one that falls behind gets
  `Lagged { dropped }` and reads on. `Event::seq` is gap-free within a run, and the engine's
  observer only copies an event into the ring. `Run::stop` stops a run from the task that holds
  it; `Run::control` hands another task or a signal handler the same.
- **Observability.** Metrics are the facade's, through the `metrics` crate: counters and
  histograms from a run's events, gauges sampled from its status once a second, labelled by
  `pipeline` and `stream` alone; the engine has no metrics dependency. The engine opens `tracing`
  spans for a run, an attempt, a partition's read, a commit and its phases, and each connector
  call but a write; none a batch. The host sends a W3C `traceparent` in each call's gRPC metadata,
  from a context source the facade's `otel` installs; `serve` reads it only when it is a
  lower-case version 00 context of 55 bytes and runs the call in a span naming the host's trace.
  `rdlt::telemetry::otlp(level)` exports both over OTLP, configured by `OTEL_*`: an `https` collector
  is verified against the operating system's roots or `OTEL_EXPORTER_OTLP_CERTIFICATE`, a failed
  export is logged through `tracing` and never stalls a run, and no error or log names an
  endpoint, which may hold credentials. No span, metric label or trace attribute holds a secret
  or a connector's configuration; connector text in them (a stream or partition name) is shown as
  the host shows it.
- **The command line.** `rdlt` (crate `rdlt-cli`) is the facade with every feature, and
  `rdlt certify` the certifier's command line. The host's policy comes from the operator's
  `host.yaml` (`--host`, by default `$XDG_CONFIG_HOME/rdlt/host.yaml`) in the host's words, each
  flag given standing in place of the file's setting; with neither, the host is sandboxed, finds
  connectors in `$XDG_DATA_HOME/rdlt/connectors` and allows no secret. Results go to standard
  output and notes to standard error; `--output json` makes every result and error JSON on
  standard output, `auto` is plain on a terminal and JSON elsewhere, in the forms `docs/cli.md`
  records (a failure is `{"error": …}`, an `ErrorReport` or the command line's own
  `{"kind", "message"}`); a closed pipe ends quietly;
  connector text is shown as the host shows it. One table in code holds the exit codes: 0
  success, 1 certify findings (failed or incomplete), 2 config, 3 schema, 4 source, 5
  destination, 6 transform, 7 fenced, 8 memory, 64 usage, 70 internal, 74 I/O, 128 plus a signal.
  The first SIGINT or SIGTERM stops a run after its next commit, a second stops it at once; the
  report says whether the stop was clean. A run's events and notes are written by tasks of their
  own, so a reader that stops reading holds up neither the run nor its signals, and the process
  ends without waiting on the runtime's blocking pool, so a hung read cannot hold its exit.
  `rdlt state reset` opens a session and so fences a run in progress, as a write should; no verb
  reads state back until the destination contract has a read that fences nothing. The binary
  links the certifier and the reference connectors it certifies with; it serves no connector and
  places none compiled in, so `cargo xtask shipped`, which holds connector binaries to no test
  surface, does not list it.
- **Features.** `local` (default), `remote`, `sql` and `otel`. `rdlt::arrow` re-exports Arrow, and
  `rdlt::Value` and `rdlt::json!` serde_json's, so an embedder never chases their versions. The
  facade runs on tokio. `just public-api` holds the public API to a reviewed snapshot, in the
  lint job.

## Consequences

- An embedder who needs a placement the facade does not offer uses `rdlt-host` directly.
- `build` discovers the source's catalog, and `Engine::check` discovers it again.
- One compute pool serves every pipeline of a host.
