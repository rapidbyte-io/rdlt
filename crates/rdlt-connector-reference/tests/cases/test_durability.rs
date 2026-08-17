//! The publish durability barrier, pinned at the one altitude an
//! offline suite can reach: the CODE PATH STRUCTURE. An fsync's effect
//! is invisible without pulling power, and a mocked fsync would pin
//! nothing — so this suite asserts the calls and their order in the
//! source itself: every part and the state document are persisted
//! (write → fsync → rename → directory fsync) BEFORE the receipt
//! append, and the append fsyncs the log it wrote. A receipt that
//! could become durable ahead of its parts would, after power loss,
//! answer `existing_receipt` for a commit whose rows are gone —
//! replay would drop the redelivered staging and the loss would be
//! silent.

/// One of the destination's source files — the subject of every pin
/// here: `store.rs` for the write primitives, `session.rs` for the
/// publish order over them.
fn destination_source(file: &str) -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src/destination")
        .join(file);
    std::fs::read_to_string(&path).unwrap_or_else(|_| panic!("src/destination/{file} reads"))
}

/// The body of one `fn name` in the source: from its definition to the
/// next `fn ` at the same altitude — a free function in `store.rs`, a
/// method in `session.rs` — or EOF. Coarse on purpose — the pins below
/// only need call PRESENCE and ORDER, and a helper that parsed Rust
/// would be a bigger liability than the thing it checks.
fn body_of<'a>(source: &'a str, name: &str) -> &'a str {
    let definition = format!("fn {name}(");
    let start = source
        .find(&definition)
        .unwrap_or_else(|| panic!("`{definition}` not found — was the function renamed?"));
    let body = &source[start + definition.len()..];
    ["\nfn ", "\npub(crate) fn ", "\n    fn ", "\n    async fn "]
        .iter()
        .filter_map(|next| body.find(next))
        .min()
        .map_or(body, |end| &body[..end])
}

/// Ordered occurrence: every needle must appear, each strictly after
/// the previous one.
fn assert_in_order(haystack: &str, needles: &[&str], subject: &str) {
    let mut from = 0;
    for needle in needles {
        match haystack[from..].find(needle) {
            Some(at) => from += at + needle.len(),
            None => panic!(
                "{subject}: `{needle}` not found after position {from} — the durability \
                 order this suite pins is: {needles:?}"
            ),
        }
    }
}

/// `publish` persists the parts, then the state document, then appends
/// the receipt — the barrier that makes a durable receipt PROOF of
/// durable data — and only after ALL of that does staging clear: a
/// clear anywhere earlier hands a retried-after-transient-failure
/// commit empty staging, and its zero-part publish would mint a receipt
/// for rows that are gone (pinned behaviorally by
/// `a_retried_publish_after_a_transient_failure_re_persists_the_rows`).
#[test]
fn publish_persists_parts_and_state_before_the_receipt_append() {
    let source = destination_source("session.rs");
    assert_in_order(
        body_of(&source, "publish"),
        &[
            "part::name(",
            "store::persist_part(",
            "store::persist(",
            "STATE_FILE",
            "store::append_receipt(",
            "self.staged.clear()",
        ],
        "publish",
    );
}

/// `persist_part` is the same atomic-durable shape as `persist` —
/// stream-encoded, but the barrier is identical: file fsync BEFORE the
/// rename, directory fsync after.
#[test]
fn persist_part_syncs_the_file_before_the_rename_and_the_directory_after() {
    let source = destination_source("store.rs");
    assert_in_order(
        body_of(&source, "persist_part"),
        &["sync_all", "rename", "sync_dir"],
        "persist_part",
    );
}

/// `persist` is the atomic-durable write: the temporary fsynced BEFORE
/// the rename (a rename must never land pointing at unwritten cache),
/// the directory fsynced after (the rename itself must survive power
/// loss).
#[test]
fn persist_syncs_the_file_before_the_rename_and_the_directory_after() {
    let source = destination_source("store.rs");
    assert_in_order(
        body_of(&source, "persist"),
        &["sync_all", "rename", "sync_dir"],
        "persist",
    );
}

/// `append_receipt` fsyncs the log after the write — an unsynced
/// receipt is a receipt that may vanish, which is safe, but an
/// unsynced PART under a synced receipt is silent loss; both halves
/// of the barrier are pinned.
#[test]
fn the_receipt_append_syncs_the_log() {
    let source = destination_source("store.rs");
    assert_in_order(
        body_of(&source, "append_receipt"),
        &["writeln!", "sync_all"],
        "append_receipt",
    );
}
