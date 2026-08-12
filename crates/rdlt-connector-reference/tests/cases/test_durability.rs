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

/// The destination's source text — the subject of every pin here.
fn destination_source() -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/destination.rs");
    std::fs::read_to_string(&path).expect("src/destination.rs reads")
}

/// The body of one `fn name` in the source: from its definition to the
/// next `fn ` (or EOF). Coarse on purpose — the pins below only need
/// call PRESENCE and ORDER, and a helper that parsed Rust would be a
/// bigger liability than the thing it checks.
fn body_of<'a>(source: &'a str, name: &str) -> &'a str {
    let definition = format!("fn {name}(");
    let start = source
        .find(&definition)
        .unwrap_or_else(|| panic!("`{definition}` not found — was the function renamed?"));
    let body = &source[start + definition.len()..];
    match body
        .find("\n    fn ")
        .into_iter()
        .chain(body.find("\n    async fn "))
        .min()
    {
        Some(end) => &body[..end],
        None => body,
    }
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

/// `publish` persists the parts, then the state document, and only
/// THEN appends the receipt — the barrier that makes a durable receipt
/// PROOF of durable data.
#[test]
fn publish_persists_parts_and_state_before_the_receipt_append() {
    let source = destination_source();
    assert_in_order(
        body_of(&source, "publish"),
        &[
            "self.persist(&part",
            "self.persist(STATE_FILE",
            "self.append_receipt(",
        ],
        "publish",
    );
}

/// `persist` is the atomic-durable write: the temporary fsynced BEFORE
/// the rename (a rename must never land pointing at unwritten cache),
/// the directory fsynced after (the rename itself must survive power
/// loss).
#[test]
fn persist_syncs_the_file_before_the_rename_and_the_directory_after() {
    let source = destination_source();
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
    let source = destination_source();
    assert_in_order(
        body_of(&source, "append_receipt"),
        &["writeln!", "sync_all"],
        "append_receipt",
    );
}
