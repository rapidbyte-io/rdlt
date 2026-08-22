//! The doors: config gate, part-name ceiling, state-document ceilings, FIFO refusal, write-mode vocabulary, and a read-only check probe.

use super::*;

/// A trailing slash over an existing FILE must refuse at the probe:
/// `stat("…/file/")` reports NotADirectory, and a walk that steps to
/// `.parent()` skips the file (the path API normalizes the trailing
/// slash away, so the parent is the directory ABOVE it), passing a
/// probe whose `connect` then fails transient — retry bait against a
/// misconfiguration no retry fixes.
#[tokio::test]
async fn check_refuses_a_trailing_slash_over_a_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    let occupied = dir.path().join("occupied");
    std::fs::write(&occupied, b"x").expect("seed the file");
    let trailing = format!("{}/", occupied.display());
    let shell = Shell::<Reference>::from_value(json!({"path": trailing})).expect("valid config");
    let refused = shell
        .check()
        .await
        .expect_err("a file behind a trailing slash must refuse, not pass while connect fails");
    let rendered = refused.to_string();
    assert!(
        rendered.starts_with("fatal destination error: ")
            && rendered.contains("is not a directory"),
        "the refusal is fatal and names the occupant: {rendered}"
    );
}

/// The config gate's refusal, full-string: the one-field document
/// refuses an empty path with its own frozen wording. The gate is the
/// `Document` trait, so it is tested through it — no shell in between.
#[test]
fn the_config_gate_refuses_an_empty_path_with_the_frozen_spelling() {
    let refused = Config::from_value(json!({"path": ""})).unwrap_err();
    assert_eq!(
        refused.to_string(),
        "invalid reference destination config: `path` is empty — one output directory is required"
    );
}

/// The part-name bound holds END TO END: a table whose built part name
/// is exactly 247 bytes publishes through the staging prefix — its
/// staged temporary is exactly the 255-byte NAME_MAX floor — so the
/// gate's bound is proven against the longest decorated form the store
/// actually writes, not just asserted at the gate.
#[tokio::test]
async fn a_247_byte_part_name_publishes_through_its_staged_form() {
    let dir = tempfile::tempdir().expect("tempdir");
    let shell = shell_over(dir.path());
    let probe = DirProbe(dir.path().to_path_buf());
    // The `(load, 1)` suffix is 22 bytes, so a 225-byte table builds a
    // 247-byte part name.
    let table = TableName::new("t".repeat(225));
    let mut session = shell
        .open(OpenContext::new(PipelineId::new("p"), LoadId::new("load")))
        .await
        .expect("open");
    session
        .ensure_table(&schema_for(&"t".repeat(225)), &WriteMode::Append)
        .await
        .expect("ensure");
    session.write(&table, batch_of(&[1])).await.expect("write");
    session
        .commit(commit_meta_for(
            &PipelineId::new("p"),
            &LoadId::new("load"),
            1,
        ))
        .await
        .expect("a 247-byte part name publishes, staging included");
    assert_eq!(probe.count(&table).await.expect("count"), 1);
}

/// The state document's write side enforces the same 8 MiB ceiling its
/// read side does — write-what-you-can-read symmetry: a state document
/// this store persisted but could never read back would wedge every
/// later open, so the publish carrying it refuses typed instead.
#[tokio::test]
async fn an_over_ceiling_state_document_refuses_at_the_publish() {
    let dir = tempfile::tempdir().expect("tempdir");
    let shell = shell_over(dir.path());
    let mut session = shell
        .open(OpenContext::new(PipelineId::new("p"), LoadId::new("l")))
        .await
        .expect("open");
    session
        .ensure_table(&schema_for("events"), &WriteMode::Append)
        .await
        .expect("ensure");
    session
        .write(&TableName::new("events"), batch_of(&[1]))
        .await
        .expect("write");
    let mut meta = commit_meta_for(&PipelineId::new("p"), &LoadId::new("l"), 1);
    meta.state.cursors.insert(
        rdlt_connector_sdk::spi::core::id::StreamName::new("s"),
        rdlt_connector_sdk::spi::core::cursor::Cursor::new(serde_json::Value::String(
            "x".repeat(8 * 1024 * 1024 + 1024),
        )),
    );
    let refused = session
        .commit(meta)
        .await
        .expect_err("a state document past the read ceiling must refuse at the write");
    let rendered = refused.to_string();
    assert!(
        rendered.starts_with("fatal destination error: ")
            && rendered.contains("8388608")
            && rendered.contains("read"),
        "typed, naming the ceiling and the read symmetry: {rendered}"
    );
}

/// The state document's read seat rides the same pre-read gate: an
/// oversized occupant refuses typed naming the ceiling, before the
/// whole file materializes.
#[tokio::test]
async fn an_oversized_state_document_refuses_before_reading() {
    let dir = tempfile::tempdir().expect("tempdir");
    let oversized = vec![b'x'; 8 * 1024 * 1024 + 1];
    std::fs::write(dir.path().join("_reference_state.json"), oversized)
        .expect("seed the oversized state");
    let shell = shell_over(dir.path());
    let mut session = shell
        .open(OpenContext::new(PipelineId::new("p"), LoadId::new("l")))
        .await
        .expect("open");
    let refused = session
        .read_state(&PipelineId::new("p"))
        .await
        .expect_err("an oversized state document refuses");
    let rendered = refused.to_string();
    assert!(
        rendered.contains("ceiling") && !rendered.contains("corrupt state document"),
        "refused by size BEFORE any read, not after parsing: {rendered}"
    );
}

/// A FIFO squatting the receipt log refuses instead of HANGING: the
/// pre-read gate requires a regular file, so the open that would block
/// forever never happens. (No pre-fix RED run exists for this pin —
/// the pre-fix behavior IS the hang, in both the test and any mutation
/// of it — which is exactly why the gate exists.)
///
/// THE HARNESS SHAPE IS LOAD-BEARING: if the regular-file check is
/// ever mutated away, the commit lands in a synchronous
/// `std::fs::read` on a reader-less FIFO — a wedge no default nextest
/// profile terminates. A `tokio::time::timeout` around the call cannot
/// save it (a blocking read inside the future never yields, so the
/// timer never fires), and a wedged TOKIO worker also blocks the
/// runtime's own shutdown, so even a spawned-task timeout would fail
/// the test and then hang the binary's exit. Hence a DETACHED
/// `std::thread` (its own tiny runtime) and a channel `recv_timeout`:
/// a regression FAILS loudly in two seconds, and the wedged thread
/// dies with the process instead of blocking anything.
#[tokio::test]
async fn a_fifo_at_the_receipt_log_refuses_instead_of_hanging() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fifo = dir.path().join("_reference_receipts.json");
    let made = std::process::Command::new("mkfifo")
        .arg(&fifo)
        .status()
        .expect("mkfifo runs");
    assert!(made.success(), "the fixture FIFO exists");
    let shell = shell_over(dir.path());
    let mut session = shell
        .open(OpenContext::new(PipelineId::new("p"), LoadId::new("l")))
        .await
        .expect("open");
    session
        .ensure_table(&schema_for("events"), &WriteMode::Append)
        .await
        .expect("ensure");
    session
        .write(&TableName::new("events"), batch_of(&[1]))
        .await
        .expect("write");

    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let outcome = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a current-thread runtime builds")
            .block_on(session.commit(commit_meta_for(&PipelineId::new("p"), &LoadId::new("l"), 1)));
        let _ = tx.send(outcome);
    });
    let refused = rx
        .recv_timeout(std::time::Duration::from_secs(2))
        .expect("the refusal must be immediate — a timeout here is the pre-gate hang")
        .expect_err("a FIFO occupant refuses typed");
    assert!(
        refused.to_string().contains("not a regular file"),
        "the refusal names the shape: {refused}"
    );
}

/// Replace is typed-unsupported, recorded never silent: accepting it
/// would append where the pipeline asked for the table's contents to
/// be replaced, quietly forever.
#[tokio::test]
async fn a_replace_write_mode_refuses_with_the_frozen_spelling() {
    let dir = tempfile::tempdir().expect("tempdir");
    let shell = shell_over(dir.path());
    let mut session = shell
        .open(OpenContext::new(
            PipelineId::new("ref-replace"),
            LoadId::new("ref-load-r"),
        ))
        .await
        .expect("open");
    let refused = session
        .ensure_table(&schema_for("events"), &WriteMode::Replace)
        .await
        .expect_err("replace must refuse");
    assert_eq!(
        refused.to_string(),
        "fatal destination error: reference destination: table `events`: write mode `replace` \
         is not supported — jsonl parts are append-only"
    );
}

/// Merge refuses the same way — typed, never silent. The engine's
/// validate gate refuses Merge against the declared `merge = false`
/// capability, but a host driving the backend directly never passes
/// that gate: accepting Merge here would append where the caller asked
/// for upsert-by-key, duplicating every redelivery quietly forever.
#[tokio::test]
async fn a_merge_write_mode_refuses_with_the_frozen_spelling() {
    let dir = tempfile::tempdir().expect("tempdir");
    let shell = shell_over(dir.path());
    let mut session = shell
        .open(OpenContext::new(
            PipelineId::new("ref-merge"),
            LoadId::new("ref-load-m"),
        ))
        .await
        .expect("open");
    let refused = session
        .ensure_table(
            &schema_for("events"),
            &WriteMode::Merge {
                key: vec!["id".into()],
            },
        )
        .await
        .expect_err("merge must refuse");
    assert_eq!(
        refused.to_string(),
        "fatal destination error: reference destination: table `events`: write mode `merge` \
         is not supported — jsonl parts are append-only"
    );
}

/// The reachability probe is READ-ONLY and honest: a clean directory
/// (or a not-yet-created path under one) passes without creating
/// anything; a path whose nearest existing ancestor is a FILE is the
/// misconfiguration connect would hit, refused fatal at check instead.
#[tokio::test]
async fn the_check_probe_is_read_only_and_refuses_a_file_in_the_way() {
    let dir = tempfile::tempdir().expect("tempdir");

    // Existing directory: reachable, and the probe creates nothing.
    let shell = shell_over(dir.path());
    shell.check().await.expect("an existing directory passes");

    // Absent target under an existing directory: still reachable —
    // connect would create it — and STILL nothing is created.
    let absent = dir.path().join("not").join("yet");
    let shell = Shell::<Reference>::from_value(json!({"path": absent})).expect("valid config");
    shell.check().await.expect("a creatable path passes");
    assert!(
        !dir.path().join("not").exists(),
        "the probe must not create anything"
    );

    // A FILE where a directory must be: fatal, typed, at check time.
    let file = dir.path().join("occupied");
    std::fs::write(&file, b"not a directory").expect("write");
    for path in [file.clone(), file.join("child")] {
        let shell = Shell::<Reference>::from_value(json!({"path": path})).expect("valid config");
        let error = shell.check().await.expect_err("a file in the way refuses");
        let text = error.to_string();
        assert!(
            text.contains("is not a directory"),
            "the refusal names the shape: {text}"
        );
        assert!(
            matches!(
                error,
                rdlt_connector_sdk::spi::error::DestinationError::Fatal(_)
            ),
            "a misconfigured path is fatal, not retryable: {error:?}"
        );
    }
}

/// A symlink planted at any name the store writes — the staged state
/// temporary, the receipt journal, the lease — is never followed: the
/// publish or open refuses typed and the file the link points at keeps
/// every byte. And an output directory another user could write is
/// refused at open, before any of those names is touched.
#[tokio::test]
async fn planted_symlinks_are_never_followed_and_a_shared_directory_is_refused() {
    use std::os::unix::fs::PermissionsExt as _;
    let dir = tempfile::tempdir().expect("tempdir");
    let out = dir.path().join("out");
    std::fs::create_dir(&out).expect("mkdir");
    std::fs::set_permissions(&out, std::fs::Permissions::from_mode(0o700)).expect("chmod");
    let victim = dir.path().join("victim");
    std::fs::write(&victim, b"precious").expect("victim");
    for planted in ["_staged-_reference_state.json", "_reference_receipts.json"] {
        std::os::unix::fs::symlink(&victim, out.join(planted)).expect("plant");
    }
    let shell = shell_over(&out);
    let table = TableName::new("events");
    let mut session = shell
        .open(OpenContext::new(PipelineId::new("p"), LoadId::new("load")))
        .await
        .expect("open");
    session
        .ensure_table(&schema_for("events"), &WriteMode::Append)
        .await
        .expect("ensure");
    session.write(&table, batch_of(&[1])).await.expect("write");
    let refused = session
        .commit(commit_meta_for(
            &PipelineId::new("p"),
            &LoadId::new("load"),
            1,
        ))
        .await
        .expect_err("a planted link at a store name refuses the publish");
    assert!(
        refused
            .to_string()
            .contains("a symlink — refusing to follow it")
            || refused.to_string().contains("not a regular file"),
        "{refused}"
    );
    assert_eq!(std::fs::read(&victim).expect("victim"), b"precious");

    // The lease, too.
    let leased = dir.path().join("leased");
    std::fs::create_dir(&leased).expect("mkdir");
    std::fs::set_permissions(&leased, std::fs::Permissions::from_mode(0o700)).expect("chmod");
    std::os::unix::fs::symlink(&victim, leased.join("_reference_lease.lock")).expect("plant");
    let refused = match shell_over(&leased)
        .open(OpenContext::new(PipelineId::new("p"), LoadId::new("load")))
        .await
    {
        Err(error) => error,
        Ok(_) => panic!("a planted lease link refuses the open"),
    };
    assert!(refused.to_string().contains("a symlink"), "{refused}");
    assert_eq!(std::fs::read(&victim).expect("victim"), b"precious");

    // A directory other users can write is not adopted.
    let shared = dir.path().join("shared");
    std::fs::create_dir(&shared).expect("mkdir");
    std::fs::set_permissions(&shared, std::fs::Permissions::from_mode(0o777)).expect("chmod");
    let refused = match shell_over(&shared)
        .open(OpenContext::new(PipelineId::new("p"), LoadId::new("load")))
        .await
    {
        Err(error) => error,
        Ok(_) => panic!("a shared directory is refused"),
    };
    assert!(refused.to_string().contains("0777"), "{refused}");
    assert!(
        std::fs::read_dir(&shared).expect("list").next().is_none(),
        "nothing was created under the refused directory"
    );
}
