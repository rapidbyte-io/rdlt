# ADR 0015: The wire protocol

Status: accepted, 2026-09-26.

## Context

M4 (spec §23) is large. It covers:

- the wire protocol;
- serving it from a connector;
- hosting connectors in processes and over the network, with supervision;
- the certification suite and its CLI.

Each is a crate of its own in the spec's workspace (§4.2), with tests of its own. One plan would
hold the whole of M4's code and review it only at the end. M4a is the protocol alone:
`rdlt-wire`, and the conversions between the contract's types and its messages.

## Decision

- **M4 is split in four**, along the spec's crates:
  - **M4a** is the protocol: the messages, the Arrow codec, the limits and the conversions.
  - **M4b** serves it and adapts it for the engine. `serve` and the remote source and
    destination run over an in-process socket, with handshake, credit, heartbeat and deadlines,
    and the `P` clauses.
  - **M4c** is `rdlt-host`: process and remote placement, supervision, respawn, the placement
    matrix, P1–P4 and L3.
  - **M4d** is `rdlt-certify`: its library and CLI, and the `K` kill matrix.
- **The messages** (§12.2, §12.3) are in seven `.proto` files under
  `crates/rdlt-wire/proto/rdlt/connector/v1/`, one per concern. The service, handshake, errors,
  types, catalog, state, source and destination messages are each in one file.
  - Every enum's zero is `UNSPECIFIED` and never sent; a receiver refuses it.
  - A logical type travels as the nodes of its tree in pre-order: a struct's node counts its
    fields, whose subtrees follow it, and a list's node is followed by its item's. A type nested
    as deep as the nesting limit (64) then crosses the wire. As a recursive message, two levels of
    protobuf per level of nesting, it stopped at 48, beyond prost's recursion limit of 100. The
    receiver bounds the depth itself, and refuses a deeper type by name.
  - A limit left 0 means the protocol's default, as a peer from before that limit existed leaves
    it. The handshake's response carries the connector's limits, as its request carries the
    host's.
  - A value that may be absent as a whole, such as a primary key, is a message of its own, so
    absent and empty differ.
- **The code is generated and committed.** `cargo xtask codegen` compiles the `.proto` files with
  protox, a protobuf compiler in Rust, and generates prost code in which every `bytes` field is
  `Bytes`. It formats that code as `cargo fmt` does.
  - Building rdlt-wire needs neither `protoc` nor a build script.
  - `just lint` runs `cargo xtask codegen --check`, which fails when the committed code is stale.
  - `xtask lint`, coverage and mutation testing skip the `generated` directory.
  - The service stubs come with M4b, which uses them.
- **The conversions live in `rdlt-connector`, under a `wire` feature.** The dependency rule
  (§4.3) gives `rdlt-wire` no workspace dependencies, and `rdlt-connector` depends on it.
  - Encoding never fails.
  - Decoding fails with a typed `Invalid`: a required field is missing, an enum value is unknown,
    a number does not fit, or a value breaks its type's rules (duplicate fields or streams,
    empty paths, unordered segments).
  - An error of a kind this end does not know decodes as internal. A limit this end does not know
    keeps its numbers under the name `limit`.
  - M4b's `serve` feature will imply `wire`.
- **State records travel as their opaque bytes.** A destination stores state records verbatim and
  never reads them, and the contract types their values as bytes. So `StateRecord` is a key and
  bytes on the wire, not the typed `StateEntry` §12.3 lists.
- **Arrow batches travel in Arrow Flight's `FlightData` layout** (§12.3): an IPC message
  flatbuffer and its body buffers.
  - The `Encoder` sends a schema once per schema epoch, then for each batch the dictionaries it
    needs that differ from those sent, then the batch. A new schema epoch sends every dictionary
    again.
  - The `Decoder` validates a schema once and caches it with its dictionaries (§12.4). A frame's
    body length and every buffer's offset and length are checked against the body before Arrow
    reads it, and compressed bodies are refused.
  - Delta dictionaries are refused: no end negotiates them, and a peer sending one delta after
    another would grow a dictionary without bound, copying it whole each time.
  - No node may declare more values than the larger of the row limit and eight per byte of body.
    Every value but a null or a run needs at least a bit, so a tiny frame cannot declare a child
    of 2^40 values for whatever reads the batch next to iterate.
  - The flatbuffer verifier's depth follows the nesting limit, so a schema nested to the limit
    passes it and one nested deeper is refused by the nesting limit, by name.
  - Arrow's decode runs inside `catch_unwind`. On corrupt node lengths Arrow's readers panic; the
    decoder turns such a panic into a typed error (§12.8).
- **Limits** (§12.8) are `rdlt_wire::Limits`, with the spec's defaults, enforced on receive with a
  typed `Refusal { code, field, limit, actual }`.
  - The decoder enforces frame size, rows per batch, columns per schema and nesting depth.
  - JSON pushes, cursors, configuration documents and control strings have `admit_*` functions,
    which M4b's receive paths call.
- **Fuzzing** (§20.7) gains two targets:
  - `wire_messages` decodes every message from arbitrary bytes, and converts those that decode.
  - `ipc_frame` corrupts real encodings of a few batches, so the corruption reaches the framing
    checks and Arrow's readers.
  - libfuzzer aborts on any panic, even a contained one, so `ipc_frame` quiets the panic hook, and
    only a panic that escapes the decoder fails it.
  - The nightly fuzz job runs one target per matrix leg, twenty minutes each.
- **The gates now cover `rdlt-wire`.** Coverage and mutation testing include it, as §20.14 lists.

## Consequences

- A connector author never sees the protocol: M4b's `serve` and the host speak it.
- The `.proto` files are the contract with connectors in other languages. From 1.0, `buf breaking`
  guards them (§12.7).
- Arrow panics that the decoder contains are still printed by the default panic hook, as the
  engine's contained panics are.
