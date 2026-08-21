//! THE FIELD-NUMBER PIN: every field number the `.proto` declares, in
//! every message and enum, against a frozen table written out here.
//!
//! This is the numbering half of the freeze's net; `test_frames.rs` is
//! the ENCODING half. The two are not redundant and neither subsumes
//! the other. The golden frames encode five representative messages and
//! compare the bytes, which proves the whole prost/tonic path really
//! puts those numbers on the wire — end-to-end, but a SAMPLE. This test
//! proves nothing about encoding and everything about coverage: it
//! reads the contract file itself, so a renumber inside a message no
//! golden frame samples (`Ensure`, `ReadState`, `Publish`, `Replay`,
//! `StreamList`, `ReceiptReply`, …) fails HERE, by name.
//!
//! Why that gap was worth closing: a renumber is invisible to every
//! other net this repo has. The Rust structs still compile (field names
//! don't change), and every in-tree end-to-end test passes, because
//! BOTH sides regenerate from this same file — client and server agree
//! with each other while disagreeing with the contract. The only party
//! that breaks is a third-party connector codegen'd from an earlier
//! copy of the proto, which is precisely the failure the freeze exists
//! to prevent, and precisely the one no in-tree test can feel.
//!
//! TWO EDGES OF THIS NET, recorded so nobody mistakes it for total.
//! Both are honest residuals, not oversights:
//!
//! - **`reserved N;` statements are skipped.** They carry no `=`, so
//!   the scanner never sees them. Reusing a reserved number still
//!   FAILS — the resurrected field is a row the frozen table does not
//!   have — but it fails as a generic unexpected row, not as "this
//!   number was retired". If reserved ranges ever appear here and that
//!   distinction starts to matter, parsing them is a small extension.
//! - **The table pins NUMBERS, not TYPES.** Changing a field's type at
//!   the same number — `uint64` to `string`, say — is every bit as
//!   breaking as a renumber, and this test passes it. Only a golden
//!   frame catches that, and only where one samples the message. The
//!   two nets are complementary, not nested.
//!
//! A THIRD gap existed and is now REFUSED rather than enumerated, which
//! is why the count above is still two. The scanner splits on
//! whitespace and looks for a bare `=` token, so a field written
//! compactly — `uint32 foo=3;`, or `string message =2;` — yielded no
//! `=` token and fell out of BOTH the parsed set and this table, and
//! the pin then passed green on a field it had never seen. An
//! exhaustive net that silently drops its subject is worse than no net.
//! The scanner now PANICS on any statement holding an `=` character it
//! cannot tokenize (see [`parse`]), so the compact spelling is a loud
//! failure instead of an invisible hole and the proto's style stays
//! uniform. Teaching the scanner to parse it would close the hole too,
//! but at the cost of two legal spellings for the one thing this whole
//! file exists to keep unambiguous.
//!
//! What a change to this table MEANS, so the next editor knows: adding
//! a row is legal (additive evolution — a new field takes a fresh
//! number, and updating this table is the deliberate moment where that
//! is reviewed). CHANGING a row's number, or deleting a row, is a
//! compatibility break and the freeze forbids it — the answer is to
//! revise the change, never the table. A field that must go away is
//! removed from service by making it unused and spelling its number
//! `reserved` in the proto; its number never returns to circulation.

/// The frozen table: `(container, field, number)` for every field in
/// every message and every value in every enum — containers in
/// alphabetical order, fields in the order the `.proto` declares them.
/// Messages that declare no fields carry no rows here; they are pinned
/// by [`FROZEN_CONTAINERS`] instead.
///
/// `oneof` members are attributed to their ENCLOSING message — a
/// `oneof` shares its message's number space, so that is the space the
/// pin has to cover.
const FROZEN_FIELD_NUMBERS: &[(&str, &str, u32)] = &[
    ("CheckReply", "ok", 1),
    ("CheckReply", "error", 2),
    ("Classification", "CLASSIFICATION_UNSPECIFIED", 0),
    ("Classification", "TRANSIENT", 1),
    ("Classification", "RATE_LIMITED", 2),
    ("Classification", "FATAL", 3),
    ("Ensure", "table_schema_json", 1),
    ("Ensure", "write_mode_json", 2),
    ("ErrorFrame", "classification", 1),
    ("ErrorFrame", "message", 2),
    ("ErrorFrame", "retry_after_ms", 3),
    ("ExistingReceipt", "load_id", 1),
    ("ExistingReceipt", "commit_seq", 2),
    ("HandshakeOk", "connector_id", 1),
    ("HandshakeOk", "connector_version", 2),
    ("HandshakeOk", "spec_json", 3),
    ("HandshakeOk", "capabilities_json", 4),
    ("HandshakeOk", "state_format_versions_json", 6),
    ("HandshakeReply", "ok", 1),
    ("HandshakeReply", "error", 2),
    ("HandshakeRequest", "protocol_version", 1),
    ("HandshakeRequest", "expected_role", 2),
    ("HandshakeRequest", "config_json", 3),
    ("Open", "pipeline", 1),
    ("Open", "load_id", 2),
    ("PartClosedEvent", "table", 1),
    ("PartClosedEvent", "encoded_bytes", 2),
    ("PartClosedEvent", "reason", 3),
    ("Publish", "commit_meta_json", 1),
    ("Published", "receipt_json", 1),
    ("ReadFrame", "raw_json", 1),
    ("ReadFrame", "arrow_ipc", 2),
    ("ReadFrame", "checkpoint_cursor_json", 3),
    ("ReadFrame", "error", 4),
    ("ReadRequest", "stream_spec_json", 1),
    ("ReadRequest", "since_cursor_json", 2),
    ("ReadState", "pipeline", 1),
    ("ReceiptReply", "receipt_json", 1),
    ("Replay", "commit_meta_json", 1),
    ("Replay", "receipt_json", 2),
    ("SessionReply", "opened", 1),
    ("SessionReply", "ensured", 2),
    ("SessionReply", "written", 3),
    ("SessionReply", "receipt", 4),
    ("SessionReply", "replayed", 5),
    ("SessionReply", "published", 6),
    ("SessionReply", "state", 7),
    ("SessionReply", "closed", 8),
    ("SessionReply", "error", 9),
    ("SessionReply", "part_closed", 10),
    ("SessionRequest", "open", 1),
    ("SessionRequest", "ensure", 2),
    ("SessionRequest", "write", 3),
    ("SessionRequest", "existing_receipt", 4),
    ("SessionRequest", "replay", 5),
    ("SessionRequest", "publish", 6),
    ("SessionRequest", "read_state", 7),
    ("SessionRequest", "close", 8),
    ("SpecReply", "spec_json", 1),
    ("StateReply", "state_doc_json", 1),
    ("StreamList", "stream_specs_jsonl", 2),
    ("StreamsReply", "ok", 1),
    ("StreamsReply", "error", 2),
    ("Write", "table", 1),
    ("Write", "arrow_ipc", 2),
];

/// Every message and enum the proto declares, frozen as a set. A field
/// table alone cannot notice a whole message vanishing (its rows just
/// disappear together, and a set comparison would name the fields but
/// not the shape that carried them), nor a message that never had
/// fields — `Empty`, `Close`, `CheckRequest`, `SpecRequest`,
/// `StreamsRequest` are load-bearing precisely BECAUSE they are empty:
/// they are the request/reply shapes whose existence the RPC signatures
/// depend on.
const FROZEN_CONTAINERS: &[&str] = &[
    "CheckReply",
    "CheckRequest",
    "Classification",
    "Close",
    "Empty",
    "Ensure",
    "ErrorFrame",
    "ExistingReceipt",
    "HandshakeOk",
    "HandshakeReply",
    "HandshakeRequest",
    "Open",
    "PartClosedEvent",
    "Publish",
    "Published",
    "ReadFrame",
    "ReadRequest",
    "ReadState",
    "ReceiptReply",
    "Replay",
    "SessionReply",
    "SessionRequest",
    "SpecReply",
    "SpecRequest",
    "StateReply",
    "StreamList",
    "StreamsReply",
    "StreamsRequest",
    "Write",
];

/// The contract file itself, read at compile time — the pin's subject
/// is the `.proto`, not the generated Rust, because the proto is what a
/// third party codegens from.
const PROTO: &str = include_str!("../../proto/rdlt_connector_v1.proto");

#[test]
fn every_field_number_in_the_proto_is_frozen() {
    let (mut parsed, _) = parse();
    let mut expected: Vec<(String, String, u32)> = FROZEN_FIELD_NUMBERS
        .iter()
        .map(|(container, field, number)| ((*container).into(), (*field).into(), *number))
        .collect();
    parsed.sort();
    expected.sort();

    // Named difference both ways: a renumbered/renamed/deleted field
    // shows up as a missing expectation, an added or renumbered one as
    // an unexpected parse — either names the exact (message, field,
    // number) rather than dumping two lists to eyeball.
    let missing: Vec<_> = expected.iter().filter(|e| !parsed.contains(e)).collect();
    let unexpected: Vec<_> = parsed.iter().filter(|p| !expected.contains(p)).collect();
    assert!(
        missing.is_empty() && unexpected.is_empty(),
        "the proto's field numbers moved away from the frozen table.\n\
         in the table but NOT in the proto (renumbered, renamed or deleted — \
         a compatibility BREAK, revise the change not the table): {missing:?}\n\
         in the proto but NOT in the table (a new field is legal — add its row \
         deliberately; a CHANGED number is not): {unexpected:?}"
    );
}

/// A number used twice inside one message is a proto that would not
/// even mean what it says. Cheap to check while the parse is in hand,
/// and it also catches a typo in the table above (which would otherwise
/// have to be wrong in exactly the same way as the proto to hide).
#[test]
fn no_field_number_repeats_within_a_container() {
    let (parsed, _) = parse();
    for (container, field, number) in &parsed {
        let twin = parsed
            .iter()
            .find(|(c, f, n)| c == container && n == number && f != field);
        assert!(
            twin.is_none(),
            "{container} uses number {number} for both `{field}` and `{}`",
            twin.expect("checked Some").1
        );
    }
}

/// Messages and enums cannot silently appear or vanish either: a
/// removed message is a compatibility break the field table alone
/// reports only as scattered missing fields, and the empty ones carry
/// no fields to report at all.
#[test]
fn every_declared_message_and_enum_is_frozen() {
    let (_, mut containers) = parse();
    let mut expected: Vec<String> = FROZEN_CONTAINERS.iter().map(|c| (*c).into()).collect();
    containers.sort();
    containers.dedup();
    expected.sort();
    assert_eq!(
        containers, expected,
        "the set of messages/enums the proto declares moved: removing one is a \
         compatibility break; adding one is additive and wants a deliberate row here"
    );
}

/// Parse `(container, field, number)` triples and the container list
/// out of the proto source.
///
/// Deliberately a ~40-line scanner rather than a protobuf-parser
/// dependency: the pin's whole value is that it reads the SAME text a
/// third party reads, and a parser crate would be one more thing whose
/// version could change what this test sees. The grammar it needs is
/// tiny — statements end at `;`, blocks open at `{` and close at `}` —
/// and everything it cannot understand it ignores rather than guesses —
/// with ONE exception, which is the point: a statement that plainly
/// carries a field number (`=` is present) but does not tokenize as
/// `<name> = <number>` is REFUSED with a panic, never ignored. Ignoring
/// it would drop the field from this parse and, being invisible, from
/// the frozen table as well — the pin would then pass green over a
/// number nobody checked.
///
/// Attribution rules: fields land under the enclosing `message`/`enum`;
/// a `oneof` re-attributes to its parent message (shared number space);
/// a `service` body owns nothing (its `rpc` lines carry no `=`, so
/// nothing is collected there anyway); a statement at file scope
/// (`syntax = "proto3";`) has no owner and is skipped.
fn parse() -> (Vec<(String, String, u32)>, Vec<String>) {
    /// Who a statement's fields belong to: `None` for a service body or
    /// file scope, `Some(message_or_enum)` otherwise.
    type Owner = Option<String>;

    let mut fields = Vec::new();
    let mut containers = Vec::new();
    let mut scopes: Vec<Owner> = Vec::new();
    let mut buffer = String::new();

    for line in PROTO.lines() {
        // Line comments only — this proto has no block comments and no
        // string literals outside `syntax`, so a plain split is exact.
        let code = line.split("//").next().unwrap_or("");
        for ch in code.chars() {
            match ch {
                '{' => {
                    let header: Vec<&str> = buffer.split_whitespace().collect();
                    let owner: Owner = match header.as_slice() {
                        ["message", name] | ["enum", name] => {
                            containers.push((*name).to_string());
                            Some((*name).to_string())
                        }
                        // A oneof's members share the enclosing
                        // message's numbers, so they inherit its owner.
                        ["oneof", _] => scopes.last().cloned().flatten(),
                        _ => None,
                    };
                    scopes.push(owner);
                    buffer.clear();
                }
                '}' => {
                    scopes.pop();
                    buffer.clear();
                }
                ';' => {
                    if let Some(Some(owner)) = scopes.last() {
                        let tokens: Vec<&str> = buffer.split_whitespace().collect();
                        match tokens.iter().position(|t| *t == "=") {
                            Some(eq) => {
                                let name = tokens[eq - 1];
                                let number: u32 = tokens[eq + 1].parse().unwrap_or_else(|_| {
                                    panic!("`{}` in {owner} has no numeric tag", buffer.trim())
                                });
                                fields.push((owner.clone(), name.to_string(), number));
                            }
                            // REFUSE, never skip. A statement holding an
                            // `=` character that is not its own token is
                            // the compact spelling (`uint32 foo=3;`,
                            // `string message =2;`). Ignoring it — what
                            // this scanner used to do — dropped the field
                            // from the parsed set AND from the frozen
                            // table at once, so the numbering pin passed
                            // green on a field it never saw: the
                            // exhaustive net failing open, which is the
                            // one way it must not fail. `reserved N;` and
                            // `rpc` lines carry no `=` at all and are
                            // still ignored, as intended.
                            None => assert!(
                                !buffer.contains('='),
                                "`{}` in {owner} spells its field number without spaces \
                                 around the `=`. This scanner deliberately understands one \
                                 spelling — `<type> <name> = <number>;` — and refuses the \
                                 other rather than silently skipping the field (a skipped \
                                 field is absent from the frozen table too, and the pin \
                                 then passes on a number nobody checked). Re-space the \
                                 statement in the .proto.",
                                buffer.trim()
                            ),
                        }
                    }
                    buffer.clear();
                }
                _ => buffer.push(ch),
            }
        }
        buffer.push(' ');
    }

    assert!(
        scopes.is_empty(),
        "unbalanced braces in the proto — the scanner cannot have parsed it correctly"
    );
    (fields, containers)
}
