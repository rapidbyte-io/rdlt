# ADR 0015: The wire protocol

Status: accepted, 2026-09-26.

## Context

Connectors reach the engine over a protocol: a connector serves it (ADR 0016), the host speaks it
to connectors in processes of their own and over the network (ADR 0017, ADR 0018), and
certification checks connectors through it (ADR 0020). The protocol is a crate of its own,
`rdlt-wire`, with tests of its own, beside the conversions between the contract's types and its
messages.

## Decision

- **The messages** are in seven `.proto` files under
  `crates/rdlt-wire/proto/rdlt/connector/v1/`, one per concern. The service, handshake, errors,
  types, catalog, state, source and destination messages are each in one file.
  - Every enum's zero is `UNSPECIFIED` and never sent; a receiver refuses it.
  - A logical type travels as the nodes of its tree in pre-order: a struct's node counts its
    fields, whose subtrees follow it, and a list's node is followed by its item's. A type nested
    as deep as the nesting limit (64) then crosses the wire. As a recursive message, two levels of
    protobuf per level of nesting, it would stop at 48, beyond prost's recursion limit of 100. The
    receiver bounds the depth itself, and refuses a deeper type by name.
  - A limit left 0 means the protocol's default, but for the dictionary limit, which has no
    default: a peer sets it, at least 256 KiB, and one left 0 is refused (ADR 0039). A handshake
    that sends no limits keeps every default. The handshake's response carries the connector's
    limits, as its request carries the host's, and the protocol version the connector speaks
    (ADR 0042).
  - A value that may be absent as a whole, such as a primary key, is a message of its own, so
    absent and empty differ.
- **The code is generated and committed.** `cargo xtask codegen` compiles the `.proto` files with
  protox, a protobuf compiler in Rust, and generates prost code in which every `bytes` field is
  `Bytes`. It formats that code as `cargo fmt` does.
  - Building rdlt-wire needs neither `protoc` nor a build script.
  - `just lint` runs `cargo xtask codegen --check`, which fails when the committed code is stale.
  - `xtask lint`, coverage and mutation testing skip the `generated` directory.
  - The data plane's calls (`Write`, `Read` and `ReadPublished`) are left out of the generated
    service, as `rdlt_wire::plane` serves and calls them by hand; their messages are generated,
    and the scan's forms describe every call's, the data plane's among them.
- **The conversions live in `rdlt-connector`, under a `wire` feature.** The dependency rule gives
  `rdlt-wire` no workspace dependencies, and `rdlt-connector` depends on it.
  - Encoding never fails.
  - Decoding fails with a typed `Invalid`: a required field is missing, an enum value is unknown,
    a number does not fit, or a value breaks its type's rules (duplicate fields or streams,
    empty paths, unordered segments).
  - An error of a kind this end does not know decodes as internal. A limit name this end does
    not know decodes with no limit attached, its code and message kept.
  - The `serve` feature implies `wire`.
- **State records travel as their opaque bytes.** A destination stores state records verbatim and
  never reads them, and the contract types their values as bytes. So `StateRecord` is a key and
  bytes on the wire, not a typed entry.
- **Arrow batches travel in Arrow Flight's `FlightData` layout**: an IPC message flatbuffer and its
  body buffers.
  - A batch's message carries `padding` before `data_body`: zero bytes that put the body at a
    multiple of 64 bytes from the message's gRPC prefix, so a receiver holding the message from
    a 64-byte address reads the body where it arrived (ADR 0038). Credit counts each message as
    sent, padding included. A JSON push is not padded.
  - The `Encoder` sends a schema once per schema epoch, then for each batch the dictionaries it
    needs that differ from those sent, then the batch. A new schema epoch sends every dictionary
    again.
  - The `Decoder` validates a schema once and caches it with its dictionaries. A frame's shape is
    walked against its schema before Arrow reads it: its buffers ordered, disjoint, padded and
    within the body, each long enough for its node, and its values and the bytes its views name
    within their limits; compressed bodies are refused (ADR 0038).
  - Delta dictionaries are refused: no end negotiates them, and a peer sending one delta after
    another would grow a dictionary without bound, copying it whole each time. The
    dictionaries a decoder holds are bounded together, by what a read keeps (ADR 0039).
  - A frame's values have one limit, `batch_values` (ADR 0038).
  - The flatbuffer verifier's depth follows the nesting limit, so a schema nested to the limit
    passes it and one nested deeper is refused by the nesting limit, by name.
  - Arrow's decode runs inside `catch_unwind`, and `rdlt-wire` does not compile with
    `panic = "abort"`. On corrupt node lengths Arrow's readers panic; the decoder turns such a
    panic into a typed error (ADR 0038).
- **Limits** are `rdlt_wire::Limits`, with the protocol's defaults, enforced on receive with a typed
  `Refusal { field, limit, actual }`, its `field` an `rdlt_wire::Limit` and its code
  `limit_exceeded`.
  - The decoder enforces frame size, rows per batch, columns per schema, nesting depth, the values
    of a frame, the bytes its views name and the bytes of a schema message, each with a limit or a
    refusal field of its own, and a sender cuts a batch to its receiver's limits (ADR 0038).
  - JSON pushes, cursors, configuration documents and control strings have `admit_*` functions,
    which the receive paths call.
- **Fuzzing** reaches the protocol through, among others, two targets:
  - `wire_messages` decodes every message from arbitrary bytes, and converts those that decode.
  - `ipc_frame` corrupts real encodings of a few batches, so the corruption reaches the framing
    checks and Arrow's readers.
  - `ipc_frame` fails only on a panic that escapes the decoder: a contained panic skips the hook
    (ADR 0038).
  - The nightly fuzz job runs one target per matrix leg, twenty minutes each.
- **The gates cover `rdlt-wire`.** Coverage and mutation testing include it.

## Consequences

- A connector author never sees the protocol: `serve` and the host speak it.
- The `.proto` files are the contract with connectors in other languages. From 1.0, `buf breaking`
  guards them.
- The decoder's contained panics are not printed (ADR 0038).
