
//! The P8–P12 rogue suite: each designated rogue proves its clause
//! fails with the pinned evidence, driving the probe functions
//! directly with the exact strings the destination certifier folds
//! into the report's Fail entries. In-process over UDS — no spawn,
//! no built bin — so all ride the bare (ungated) suite.

use super::*;
use crate::report::Verdict;
use crate::rogue::{self, OrderBookScript, SessionDiscipline};

/// P8's designated rogue: a destination that ACCEPTS a second
/// concurrent session fails the clause with the pinned evidence.
#[tokio::test]
async fn a_destination_accepting_a_second_session_fails_p8() {
    let dir = tempfile::tempdir().expect("tempdir");
    let socket = dir.path().join("rogue.sock");
    let _serving = rogue::serve_destination(&socket, SessionDiscipline::AcceptEverySession);

    let why = probe_one_session_ceiling(&socket, "pinned")
        .await
        .expect_err("a second session was accepted — P8 must fail");
    assert_eq!(
        why,
        "a second concurrent session was ACCEPTED — the wire allows exactly one session per \
         connector process; the second OpenSession must be refused with FailedPrecondition"
    );
}

/// P9's designated rogue: a destination that never releases the
/// slot after abandonment fails the clause within its window —
/// paused tokio time auto-advances the poll sleeps, so the 10s
/// window elapses without wall-clock cost.
#[tokio::test(start_paused = true)]
async fn a_destination_that_never_reclaims_fails_p9() {
    let dir = tempfile::tempdir().expect("tempdir");
    let socket = dir.path().join("rogue.sock");
    let _serving = rogue::serve_destination(&socket, SessionDiscipline::NeverReclaim);

    let why = probe_abandonment_reclaim(&socket, "pinned")
        .await
        .expect_err("the slot was never reclaimed — P9 must fail");
    let pinned = "abandoned session was not reclaimed: a fresh session still refused 10s \
                  after the stream ended without Close: ";
    let suffix = why.strip_prefix(pinned).unwrap_or_else(|| {
        panic!("the evidence must carry the pinned prefix `{pinned}`, got: {why}")
    });
    // The cause must be `settle_open_wire`'s window-exhaustion
    // spelling specifically — proof the failure came from the
    // ceiling refusal outlasting the reclaim window, not from some
    // other open error folded into the same prefix.
    let exhaustion = "the one-session refusal never lifted: ";
    assert!(
        suffix.starts_with(exhaustion),
        "the evidence must carry the exhaustion suffix `{exhaustion}`, got: {why}"
    );
}

/// Serve the order-book rogue and hand back the socket (the
/// tempdir rides along so it outlives the probe).
fn order_book_rogue(script: OrderBookScript) -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let socket = dir.path().join("rogue.sock");
    let _serving = rogue::serve_order_book(&socket, script);
    (dir, socket)
}

/// The P10 control: a server that keeps the whole grammar passes
/// the probe — proof the driver's happy path completes in the bare
/// suite, without a spawned bin (the gated file cell is the
/// real-connector twin of this pin).
#[tokio::test]
async fn a_conformant_order_book_passes_p10() {
    let (_dir, socket) = order_book_rogue(OrderBookScript::Conformant);
    probe_order_book(&socket, "pinned")
        .await
        .expect("a conformant order book must pass P10");
}

/// P10's first designated rogue: a destination that answers
/// `written` to a write on a never-ensured table fails with the
/// pinned evidence — the out-of-order probe is driven FIRST, so
/// nothing else in the sequence can mask the missing refusal.
#[tokio::test]
async fn a_destination_accepting_an_unordered_write_fails_p10() {
    let (_dir, socket) = order_book_rogue(OrderBookScript::AcceptWriteBeforeEnsure);
    let why = probe_order_book(&socket, "pinned")
        .await
        .expect_err("the unordered write was accepted — P10 must fail");
    assert_eq!(
        why,
        "an out-of-order `write` was ACCEPTED — a write to a never-ensured table must \
         be refused with a typed error frame"
    );
}

/// P10's second designated rogue: a destination that reports an
/// existing receipt, accepts `replay`, then ALSO accepts `publish`
/// with a freshly minted receipt fails with both receipts named —
/// the replay-vs-publish exclusivity violated in the only
/// wire-observable way (a refusal and the prior receipt are the
/// two legal answers).
#[tokio::test]
async fn a_destination_minting_a_fresh_receipt_on_a_replayed_load_fails_p10() {
    let (_dir, socket) = order_book_rogue(OrderBookScript::PublishOnReplay);
    let why = probe_order_book(&socket, "pinned")
        .await
        .expect_err("the publish minted a fresh receipt — P10 must fail");
    assert_eq!(
        why,
        "a `publish` for a load whose receipt already exists minted a NEW receipt — \
         after `existing_receipt` reports a receipt, `publish` must be refused or answer \
         that same receipt (existing {\"load_id\":\"certify-p10-pinned\",\"commit_seq\":1}, \
         published {\"load_id\":\"certify-p10-pinned\",\"commit_seq\":2})"
    );
}

/// P10's pass-3 rogue: a destination that behaves perfectly
/// for every session that ASKS `existing_receipt` first — passes 1
/// and 2 both do — but mints a fresh receipt on a no-ask republish
/// of a committed load fails with both receipts named: the
/// backend's own durable publish guard is missing, which is
/// exactly the wire-reachable exactly-once violation.
#[tokio::test]
async fn a_fresh_mint_on_a_no_ask_republish_fails_p10() {
    let (_dir, socket) = order_book_rogue(OrderBookScript::FreshMintOnNoAskRepublish);
    let why = probe_order_book(&socket, "pinned")
        .await
        .expect_err("the no-ask republish minted a fresh receipt — P10 must fail");
    assert_eq!(
        why,
        "a no-ask re-`publish` of a committed load minted a fresh receipt \
         (durable {\"load_id\":\"certify-p10-pinned\",\"commit_seq\":1}, \
         published {\"load_id\":\"certify-p10-pinned\",\"commit_seq\":2}) — with no \
         `existing_receipt` in between, `publish` must be refused or answer the prior \
         receipt"
    );
}

/// P10's part-event boundary rogue: a destination that answers
/// `closed` and then emits a `part_closed` event fails with the
/// pinned evidence — part events are legal anywhere before
/// `close`'s answer and nowhere after it.
#[tokio::test]
async fn a_part_event_after_the_close_reply_fails_p10() {
    let (_dir, socket) = order_book_rogue(OrderBookScript::PartEventAfterClose);
    let why = probe_order_book(&socket, "pinned")
        .await
        .expect_err("a part event crossed the close boundary — P10 must fail");
    assert_eq!(
        why,
        "a `part_closed` reply arrived after `close` was answered — part events and \
         replies are legal only before the session's end"
    );
}

/// The P11/P12 control: the conformant order book passes BOTH
/// write-side clauses — the two-batch frame is refused with a
/// well-shaped error frame, and every induced refusal carries bare
/// cause text (the gated file cell is the real-connector twin).
#[tokio::test]
async fn a_conformant_order_book_passes_p11_and_p12() {
    let (_dir, socket) = order_book_rogue(OrderBookScript::Conformant);
    probe_one_batch_write(&socket, "pinned")
        .await
        .expect("a conformant order book must pass P11");
    probe_error_frame_text(&socket, "pinned")
        .await
        .expect("a conformant order book must pass P12");
}

/// P11's designated rogue: a destination that answers `written` to
/// a write frame whose arrow_ipc payload carries TWO record batches
/// fails with the pinned evidence — and violates P11 ALONE (the
/// order book and the refusal texts hold, so the other session
/// clauses pass against the same rogue).
#[tokio::test]
async fn a_destination_accepting_a_two_batch_write_fails_p11() {
    let (_dir, socket) = order_book_rogue(OrderBookScript::AcceptMultiBatchWrite);
    let why = probe_one_batch_write(&socket, "pinned")
        .await
        .expect_err("the two-batch write was accepted — P11 must fail");
    assert_eq!(
        why,
        "a two-batch `write` was ACCEPTED — a write frame's arrow_ipc payload must carry \
         exactly one record batch, and a multi-batch frame must be refused with a typed \
         error frame"
    );
    probe_order_book(&socket, "pinned")
        .await
        .expect("the rogue violates P11 alone — P10 must pass");
    probe_error_frame_text(&socket, "pinned")
        .await
        .expect("the rogue violates P11 alone — P12 must pass");
}

/// P12's designated rogue: a destination whose induced refusal
/// carries a client rendering in its message (`fatal destination
/// error: boom`) fails with the pinned evidence — and violates P12
/// ALONE (the refusal still ARRIVES as a typed error frame, so the
/// order book holds and the other session clauses pass).
#[tokio::test]
async fn a_client_rendering_in_a_session_refusal_fails_p12() {
    let (_dir, socket) = order_book_rogue(OrderBookScript::RenderedRefusal);
    let why = probe_error_frame_text(&socket, "pinned")
        .await
        .expect_err("the refusal message carries a client rendering — P12 must fail");
    assert_eq!(
        why,
        "the out-of-order `write` refusal: classification rendered inside the message — \
         the frame carries cause text; classification travels as the enum (the message \
         begins with `fatal destination error: `)"
    );
    probe_order_book(&socket, "pinned")
        .await
        .expect("the rogue violates P12 alone — P10 must pass");
    probe_one_batch_write(&socket, "pinned")
        .await
        .expect("the rogue violates P12 alone — P11 must pass");
}

/// P10's third designated rogue: a destination that never answers
/// `close` proves the clause-timeout arm — the certifier OUTLIVES
/// the hang and renders the one timeout spelling. The test itself
/// is bounded at 45s (the clause budget plus margin) so a broken
/// timeout fails THIS test, not the suite; the paused clock
/// auto-advances the waits, so neither bound costs wall time.
#[tokio::test(start_paused = true)]
async fn a_destination_hanging_on_close_fails_p10_by_timeout() {
    let (_dir, socket) = order_book_rogue(OrderBookScript::HangOnClose);

    let mut report = Report::default();
    let outcome = tokio::time::timeout(
        Duration::from_secs(45),
        report_p10(&mut report, &socket, "pinned"),
    )
    .await;
    assert!(
        outcome.is_ok(),
        "the certifier must outlive the hang — the clause timeout never fired"
    );
    let entry = report
        .entries
        .iter()
        .find(|entry| entry.clause == "P10")
        .expect("report_p10 always writes a P10 entry");
    match &entry.verdict {
        Verdict::Fail(why) => assert_eq!(
            why,
            "clause timed out after 30s — a connector that stalls fails the clause"
        ),
        other => panic!("a hang must Fail P10 by timeout, got {other:?}"),
    }
}
