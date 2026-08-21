
//! The rogue suite for the source wire clauses P3/P5/P6/P7: each
//! designated rogue proves its clause CAN fail, with the evidence
//! pinned full-string. The rogues serve in-process over UDS — no
//! spawn, no built bin — so these ride the bare (ungated) suite
//! through the [`WireProbe::attach_socket`] seam.

use proto::Classification;

use super::support::{assert_fail, assert_pass, verdict};
use super::*;

/// The declaration's framing rule, driven from the HOSTILE side: a
/// connector that answers the `Streams` RPC with bytes that are not
/// the joined documents the field promises must be refused by the
/// client, not split, parsed or retained on faith. Three shapes,
/// one after another over real sockets: a line that is not JSON at
/// all, a trailing newline (an empty final document), and more
/// declarations than the wire admits.
#[tokio::test]
async fn a_rogue_violating_the_declaration_framing_is_refused_by_the_client() {
    // A line the client PARSES, so each arm's framing violation is
    // what refuses it — a spec missing a required field would
    // refuse at line 1 and prove nothing about the framing.
    let spec = |name: &str| {
        format!(
            "{{\"name\":\"{name}\",\"primary_key\":null,\"cursor_field\":null,\
             \"type_hints\":{{}}}}"
        )
    };
    for (name, raw, expected) in [
        (
            "not json",
            format!("{}\nnot-json", spec("a")).into_bytes(),
            "syntax error",
        ),
        (
            "trailing newline",
            format!("{}\n", spec("a")).into_bytes(),
            "undecodable",
        ),
        (
            "over count",
            (0..1025)
                .map(|i| spec(&format!("s{i}")).into_bytes())
                .collect::<Vec<_>>()
                .join(&b'\n'),
            "over the 1024",
        ),
    ] {
        let dir = tempfile::tempdir().expect("tempdir");
        let socket = dir.path().join("rogue.sock");
        let _serving = rogue::serve_source(
            &socket,
            RogueSource {
                handshake: HandshakeScript::truthful(),
                streams: vec![],
                streams_raw: Some(raw),
                read_declared: vec![],
                read_undeclared: vec![],
                read_hold_open: false,
            },
        );
        let script =
            crate::clause::p::generic_tests::write_connector_fake(dir.path(), "rogue", &socket);
        let requirement = Requirement::new("rogue").with_path(&script);
        use rdlt_connector::source::Source as _;
        use rdlt_runtime::provider::Provider as _;
        let provider = rdlt_runtime::local::Local::new();
        let managed = provider
            .source(&requirement, &serde_json::json!({}))
            .await
            .expect("the handshake is truthful — the declaration is what misbehaves");
        let refusal = managed
            .streams()
            .await
            .expect_err(&format!("the {name} declaration is refused"));
        let rendered = format!("{refusal}");
        assert!(
            rendered.contains(expected),
            "the {name} refusal names its own violation (wanted {expected:?}): {rendered}"
        );
        // The well-formed line was ACCEPTED: a shape mismatch here
        // would mean the client refused line 1 and never judged the
        // framing this arm exists to drive.
        assert!(
            !rendered.contains("document shape mismatch"),
            "the {name} arm must exercise framing, not a malformed first line: {rendered}"
        );
    }
}

use crate::report::Verdict;
use crate::rogue::{self, HandshakeScript, RogueSource};

#[test]
fn p5_retains_only_the_first_bounded_set_of_violation_strings() {
    let mut violations = Vec::new();
    let mut omitted = 0usize;
    for index in 0..(MAX_P5_VIOLATIONS + 7) {
        retain_p5_violation(&mut violations, &mut omitted, || index.to_string());
    }
    assert_eq!(violations.len(), MAX_P5_VIOLATIONS);
    assert_eq!(omitted, 7);
    assert_eq!(violations.first().map(String::as_str), Some("0"));
}

/// Serve `rogue` in-process and run the full source wire-clause
/// sequence against it, requiring identity `required_id`.
async fn certify_rogue(rogue: RogueSource, required_id: &str) -> Report {
    let dir = tempfile::tempdir().expect("tempdir");
    let socket = dir.path().join("rogue.sock");
    let _serving = rogue::serve_source(&socket, rogue);
    let mut probe = WireProbe::attach_socket(&socket, Role::Source, &serde_json::json!({}))
        .await
        .expect("the rogue's socket dials");
    let mut report = Report::default();
    source_wire(&mut report, &mut probe, required_id).await;
    report
}

/// A well-shaped induced refusal: FATAL, bare cause text.
fn shaped_refusal() -> Vec<proto::ReadFrame> {
    vec![rogue::error_read_frame(rogue::error_frame(
        Classification::Fatal,
        "no such stream",
    ))]
}

/// THE SKEW CASE (no other test anywhere exercises
/// `spec.version != connector_version`): a rogue whose spec
/// document and wire identity disagree on VERSION fails P3 with
/// both values named, and ONLY P3. Its populated state-format map
/// is tolerated — P7 passes with it.
#[tokio::test]
async fn a_version_skewed_handshake_fails_p3_alone() {
    let report = certify_rogue(
        RogueSource {
            handshake: HandshakeScript::Ok {
                connector_id: "rogue",
                connector_version: "0.0.0",
                spec_name: "rogue",
                spec_version: "9.9.9",
                state_format_versions: &[("cursor", 2)],
            },
            streams: vec![],
            streams_raw: None,
            read_declared: vec![],
            read_undeclared: shaped_refusal(),
            read_hold_open: false,
        },
        "rogue",
    )
    .await;
    assert_fail(
        &report,
        "P3",
        "the handshake identity is skewed: spec_json carries version `9.9.9` but the wire \
         reported connector_version `0.0.0`",
    );
    assert_pass(&report, "P7");
    assert_pass(&report, "P5");
    assert_pass(&report, "P6");
}

/// The name half of the skew: spec_json naming somebody else than
/// the wire's connector_id fails P3 by VALUES — no `io.rapidbyte.*`
/// spelling is assumed anywhere.
#[tokio::test]
async fn a_name_skewed_handshake_fails_p3() {
    let report = certify_rogue(
        RogueSource {
            handshake: HandshakeScript::Ok {
                connector_id: "rogue",
                connector_version: "0.0.0",
                spec_name: "somebody-else",
                spec_version: "0.0.0",
                state_format_versions: &[],
            },
            streams: vec![],
            streams_raw: None,
            read_declared: vec![],
            read_undeclared: shaped_refusal(),
            read_hold_open: false,
        },
        "rogue",
    )
    .await;
    assert_fail(
        &report,
        "P3",
        "the handshake identity is skewed: spec_json names `somebody-else` but the wire \
         reported connector_id `rogue`",
    );
}

/// The requirement arm: a self-consistent identity that is not the
/// one the target requires still fails P3.
#[tokio::test]
async fn a_wrong_connector_id_fails_p3_against_the_requirement() {
    let report = certify_rogue(
        RogueSource {
            handshake: HandshakeScript::truthful(),
            streams: vec![],
            streams_raw: None,
            read_declared: vec![],
            read_undeclared: shaped_refusal(),
            read_hold_open: false,
        },
        "somebody-else",
    )
    .await;
    assert_fail(
        &report,
        "P3",
        "the handshake identity is skewed: the wire reported connector_id `rogue` but the \
         target requires `somebody-else`",
    );
}

/// P5's designated rogue: ONE arrow frame carrying TWO record
/// batches fails P5 with the count and the census, and only P5.
#[tokio::test]
async fn a_two_batch_arrow_frame_fails_p5_alone() {
    let report = certify_rogue(
        RogueSource {
            handshake: HandshakeScript::truthful(),
            streams: vec![StreamSpec::new("rogue_stream")],
            streams_raw: None,
            read_declared: vec![rogue::arrow_read_frame(2)],
            read_undeclared: shaped_refusal(),
            read_hold_open: false,
        },
        "rogue",
    )
    .await;
    assert_fail(
        &report,
        "P5",
        "an arrow read frame carried 2 record batches — the one-batch rule requires \
         exactly one (stream `rogue_stream`); frame census: 1 arrow, 0 raw_json, \
         0 checkpoint, 0 error, 0 empty",
    );
    assert_pass(&report, "P3");
    assert_pass(&report, "P6");
    assert_pass(&report, "P7");
}

/// The certification bar's oversized-frame arm: a rogue serving a
/// read frame LARGER than [`MAX_FRAME_BYTES`] must surface the
/// dial-side decode cap as a TYPED refusal — not a hang, not a
/// clean end of stream. It reports at P5, the clause walking the
/// declared streams' frames when the cap fires: the read stream
/// dies with the transport status carrying tonic's own
/// length-limit message, and the exact rendering is pinned
/// full-string. Only
/// P5 fails — the handshake clauses and P6's induced refusal ride
/// their own RPCs, untouched by the reset read stream.
#[tokio::test]
async fn an_oversized_read_frame_fails_p5_with_the_decode_cap_refusal() {
    let report = certify_rogue(
        RogueSource {
            handshake: HandshakeScript::truthful(),
            streams: vec![StreamSpec::new("rogue_stream")],
            streams_raw: None,
            read_declared: vec![rogue::oversized_read_frame()],
            read_undeclared: shaped_refusal(),
            read_hold_open: false,
        },
        "rogue",
    )
    .await;
    assert_fail(
        &report,
        "P5",
        "the read stream failed mid-flight with a transport status: code: 'Operation was \
         attempted past the valid range', message: \"Error, decoded message length too \
         large: found 67108870 bytes, the limit is: 67108864 bytes\"",
    );
    assert_pass(&report, "P3");
    assert_pass(&report, "P6");
    assert_pass(&report, "P7");
}

/// P6's designated rogue: an error frame whose MESSAGE begins with
/// a client rendering fails P6 with the pinned diagnosis, and only
/// P6 — the frame carries cause text; classification travels as
/// the enum.
#[tokio::test]
async fn a_client_rendering_in_the_frame_message_fails_p6_alone() {
    let report = certify_rogue(
        RogueSource {
            handshake: HandshakeScript::truthful(),
            streams: vec![],
            streams_raw: None,
            read_declared: vec![],
            read_undeclared: vec![rogue::error_read_frame(rogue::error_frame(
                Classification::Fatal,
                "fatal source error: boom",
            ))],
            read_hold_open: false,
        },
        "rogue",
    )
    .await;
    assert_fail(
        &report,
        "P6",
        "classification rendered inside the message — the frame carries cause text; \
         classification travels as the enum (the message begins with `fatal source \
         error: `)",
    );
    assert_pass(&report, "P3");
    assert_pass(&report, "P5");
    assert_pass(&report, "P7");
}

/// The framing pre-pass at THIS seat: a rogue serving an Arrow frame whose declared
/// metadata length dwarfs the frame must fail P5 TYPED — the shared
/// pre-pass's refusal — rather than memsetting gigabytes or aborting
/// the certifier process mid-clause. (The pin returning at all is
/// the no-abort proof; an abort kills this test's process.)
#[tokio::test]
async fn an_overdeclared_arrow_frame_fails_p5_typed() {
    let mut crafted = vec![0xff, 0xff, 0xff, 0xff];
    crafted.extend_from_slice(&0x7fff_fff0_i32.to_le_bytes());
    crafted.extend_from_slice(&[0u8; 16]);
    let report = certify_rogue(
        RogueSource {
            handshake: HandshakeScript::truthful(),
            streams: vec![StreamSpec::new("rogue_stream")],
            streams_raw: None,
            read_declared: vec![rogue::raw_arrow_read_frame(crafted)],
            read_undeclared: shaped_refusal(),
            read_hold_open: false,
        },
        "rogue",
    )
    .await;
    assert_fail(
        &report,
        "P5",
        "an arrow read frame does not decode as one Arrow IPC stream (stream \
         `rogue_stream`): a declared metadata length of 2147483632 bytes exceeds the \
         24-byte frame; frame census: 1 arrow, 0 raw_json, 0 checkpoint, 0 error, 0 empty",
    );
    assert_pass(&report, "P3");
    assert_pass(&report, "P6");
    assert_pass(&report, "P7");
}

/// The seat's second defense-in-depth arm: the client lane's
/// 160-byte fuzz reproducer, served raw. (Today this input refuses
/// at the pre-pass — its declared framing is already over the
/// frame's end — which is still the pinned property: a crafted
/// frame fails P5 TYPED, never an abort or an escaped unwind. The
/// belt's own synthetic pin sits beside [`caught_decode`].)
#[tokio::test]
async fn a_decoder_panicking_frame_fails_p5_typed() {
    const REPRO: [u8; 160] = [
        0xff, 0xff, 0xff, 0xff, 0x78, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x0a, 0x00, 0x0c, 0x00, 0x06, 0x00, 0x05, 0x00, 0x08, 0x00, 0x0a, 0x00, 0x00, 0x00,
        0x00, 0x01, 0x04, 0x00, 0x0c, 0x00, 0x00, 0x00, 0x08, 0x00, 0x08, 0x00, 0x00, 0x00,
        0x04, 0x00, 0x08, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
        0x14, 0x00, 0x00, 0x00, 0x10, 0x00, 0x14, 0x00, 0x08, 0x00, 0x06, 0x00, 0x07, 0x00,
        0x0c, 0x00, 0x00, 0x00, 0x10, 0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x02,
        0x10, 0x00, 0x00, 0x00, 0x1c, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x69, 0x64, 0x00, 0x00, 0x08, 0x00, 0x0c, 0x00,
        0x08, 0x00, 0x07, 0x00, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x40, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x29, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff,
        0x88, 0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0c, 0x00,
        0x16, 0x00, 0x06, 0x00, 0x05, 0x00,
    ];
    let report = certify_rogue(
        RogueSource {
            handshake: HandshakeScript::truthful(),
            streams: vec![StreamSpec::new("rogue_stream")],
            streams_raw: None,
            read_declared: vec![rogue::raw_arrow_read_frame(REPRO.to_vec())],
            read_undeclared: shaped_refusal(),
            read_hold_open: false,
        },
        "rogue",
    )
    .await;
    match verdict(&report, "P5") {
        Verdict::Fail(why) => assert!(
            why.starts_with("an arrow read frame does not decode as one Arrow IPC stream"),
            "the typed refusal, never an escaped unwind: {why}"
        ),
        other => panic!("P5 must fail typed on a panicking frame: {other:?}"),
    }
}

/// P6's terminality arm: a
/// frame served AFTER the error frame fails P6 by name — the wire's
/// error frames are terminal, and a connector that keeps talking
/// after one is exactly the rogue the arm exists to catch.
#[tokio::test]
async fn an_error_frame_with_trailing_frames_fails_p6() {
    let report = certify_rogue(
        RogueSource {
            handshake: HandshakeScript::truthful(),
            streams: vec![],
            streams_raw: None,
            read_declared: vec![],
            read_undeclared: vec![
                rogue::error_read_frame(rogue::error_frame(
                    Classification::Fatal,
                    "no such stream",
                )),
                rogue::json_read_frame(),
            ],
            read_hold_open: false,
        },
        "rogue",
    )
    .await;
    assert_fail(
        &report,
        "P6",
        "the ErrorFrame was not terminal — 1 frame(s) followed it",
    );
    assert_pass(&report, "P3");
    assert_pass(&report, "P5");
    assert_pass(&report, "P7");
}

/// A refusal that never arrives is also a P6 failure: a clean end
/// of stream on a nonexistent stream hides the refusal entirely.
#[tokio::test]
async fn a_clean_end_on_the_bogus_stream_fails_p6() {
    let report = certify_rogue(
        RogueSource {
            handshake: HandshakeScript::truthful(),
            streams: vec![],
            streams_raw: None,
            read_declared: vec![],
            read_undeclared: vec![],
            read_hold_open: false,
        },
        "rogue",
    )
    .await;
    assert_fail(
        &report,
        "P6",
        "reading a nonexistent stream produced no terminal ErrorFrame — a refusal must \
         arrive as a typed error frame, never a clean end of stream",
    );
}

/// An unclassified refusal fails P6: CLASSIFICATION_UNSPECIFIED is
/// the proto's zero value, not a classification.
#[tokio::test]
async fn an_unspecified_classification_fails_p6() {
    let report = certify_rogue(
        RogueSource {
            handshake: HandshakeScript::truthful(),
            streams: vec![],
            streams_raw: None,
            read_declared: vec![],
            read_undeclared: vec![rogue::error_read_frame(proto::ErrorFrame {
                classification: Classification::Unspecified as i32,
                message: "boom".to_string(),
                retry_after_ms: None,
            })],
            read_hold_open: false,
        },
        "rogue",
    )
    .await;
    assert_fail(
        &report,
        "P6",
        "the error frame's classification is CLASSIFICATION_UNSPECIFIED — a refusal must \
         carry a real classification",
    );
}

/// A refused handshake cascades: EVERY wire clause fails with the
/// one cause — including P7, whose only failure mode this is (its
/// map shape is enforced by protobuf decoding itself).
#[tokio::test]
async fn a_refused_handshake_cascades_every_wire_clause() {
    let report = certify_rogue(
        RogueSource {
            handshake: HandshakeScript::Refuse {
                message: "the config document is not mine",
            },
            streams: vec![],
            streams_raw: None,
            read_declared: vec![],
            read_undeclared: vec![],
            read_hold_open: false,
        },
        "rogue",
    )
    .await;
    for clause in SOURCE_WIRE {
        assert_fail(
            &report,
            clause,
            "the handshake was refused (FATAL): the config document is not mine",
        );
    }
}

/// The silent-but-alive rogue: it binds, the transport is up, and
/// the handshake never answers — the shape the SIGKILL matrix
/// cannot produce (a dead socket errors out) and the one only a
/// deadline catches. Certification must yield the TYPED timeout
/// outcome on every wire clause, never a hang: the test itself is
/// bounded at 45s (the clause budget plus margin) so a broken
/// budget fails THIS test, and the paused clock auto-advances the
/// waits so neither bound costs wall time (the P10 hang pin's
/// idiom). No new clause id: silence is not a new connector
/// obligation — every clause already carries the budget, and the
/// cascade with the one timeout spelling IS the typed verdict.
#[tokio::test(start_paused = true)]
async fn a_silent_but_alive_connector_fails_every_wire_clause_typed_not_hung() {
    let outcome = tokio::time::timeout(
        std::time::Duration::from_secs(45),
        certify_rogue(
            RogueSource {
                handshake: HandshakeScript::Silence,
                streams: vec![],
                streams_raw: None,
                read_declared: vec![],
                read_undeclared: vec![],
                read_hold_open: false,
            },
            "rogue",
        ),
    )
    .await;
    let report = outcome.expect("the certifier must outlive the silence — the budget fired");
    for clause in SOURCE_WIRE {
        assert_fail(
            &report,
            clause,
            "clause timed out after 30s — a connector that stalls fails the clause",
        );
    }
}
