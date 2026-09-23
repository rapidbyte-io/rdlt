# ADR 0002: Connector contract shape

Status: accepted, 2026-09-23.

## Context

The spec sketches the Rust SDK with streams registered as methods
(`Streams::new().add("tickets", Self::tickets)`). Stable Rust cannot store a borrowed-argument
`async fn` with a `Send` future behind a name without boxing tricks that leak into author code.

## Decision

- Each stream is a type implementing `ReadStream<S>`, with its own `Cursor` type and
  `CURSOR_VERSION`; `Streams::new().with(TicketsStream)` registers it.
- Author traits declare `-> impl Future<Output = _> + Send`; authors write `async fn`. The SDK
  boxes futures only at the engine-facing traits (`Source`, `Destination`).
- `ConnectorErrorKind` adds `Fenced` (a stale session's commit) and `Stopped` (the engine asked a
  read to stop) to the spec's kinds; the SDK turns `Stopped` into a clean end of the read.
- Destinations store state as opaque `StateRecord { key, value }` pairs; the engine owns their
  encoding (`StateEntry`), so destinations never interpret state.
- `discard_staged` is not reachable from the engine-facing session: the SDK calls it inside
  `open`, so no engine code path can skip it.
- The `testing` feature certifies connectors in-process. Destination clauses read published data
  through a `Probe` the connector author supplies.
- The workspace allows `clippy::unused_async_trait_impl`: implementing an async trait method
  with `async fn` is correct even when that implementation never awaits.

## Consequences

Stream registration is one type per stream rather than one method. Cursor types are checked at
compile time per stream, and cursor format changes are explicit.
