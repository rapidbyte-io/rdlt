//! One pipeline per state slot, one session per process — the lease releases on drop.

use super::*;


/// ONE state slot means ONE pipeline per directory: a second pipeline
/// reading the slot must refuse typed — answering `None` would read as
/// "never committed", so the engine would re-extract from scratch,
/// append every already-loaded row a second time, and the next publish
/// would destroy the first pipeline's cursors.
#[tokio::test]
async fn another_pipelines_state_refuses_rather_than_reading_fresh() {
    let dir = tempfile::tempdir().expect("tempdir");
    let shell = shell_over(dir.path());
    let pipeline_a = PipelineId::new("orders");
    let load = LoadId::new("ref-load-o");
    let mut session = shell
        .open(OpenContext::new(pipeline_a.clone(), load.clone()))
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
    session
        .commit(commit_meta_for(&pipeline_a, &load, 1))
        .await
        .expect("commit");
    drop(session);

    let mut session = shell
        .open(OpenContext::new(
            PipelineId::new("customers"),
            LoadId::new("ref-load-c"),
        ))
        .await
        .expect("open under the second pipeline");
    let refused = session
        .read_state(&PipelineId::new("customers"))
        .await
        .expect_err("a foreign state slot must refuse, never read fresh");
    assert_eq!(
        refused.to_string(),
        format!(
            "fatal destination error: reference destination: {} carries the state of \
             pipeline `orders` — this session is pipeline `customers`, and one directory \
             holds ONE pipeline's state: reading it as fresh would append every \
             already-loaded row again, and the next publish would destroy `orders`' \
             cursors; give each pipeline its own output directory",
            dir.path().join("_reference_state.json").display()
        )
    );
}

/// The foreign-pipeline state refusal quotes the OCCUPANT's pipeline id
/// — content read off DISK, which nothing upstream ever gated — through
/// the bounded diagnostic render.
#[tokio::test]
async fn a_foreign_pipeline_state_refusal_renders_the_occupant_inert() {
    let dir = tempfile::tempdir().expect("tempdir");
    let shell = shell_over(dir.path());
    let occupant = PipelineId::new("evil\u{1b}]52;c;A\u{7}pipe");
    let load = LoadId::new("l");
    let mut session = shell
        .open(OpenContext::new(occupant.clone(), load.clone()))
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
    session
        .commit(commit_meta_for(&occupant, &load, 1))
        .await
        .expect("commit under the hostile pipeline id");
    drop(session);

    let mut session = shell
        .open(OpenContext::new(PipelineId::new("p"), LoadId::new("l2")))
        .await
        .expect("re-open");
    let refused = session
        .read_state(&PipelineId::new("p"))
        .await
        .expect_err("a foreign state slot refuses");
    let rendered = refused.to_string();
    assert!(
        !rendered.contains('\u{1b}') && !rendered.contains('\u{7}'),
        "no raw control byte survives the refusal: {rendered:?}"
    );
    assert!(
        rendered.contains("\\u{1b}"),
        "the occupant arrives spelled out: {rendered}"
    );
}

/// The session lease: two concurrent sessions of one pipeline would
/// each read the same persisted cursor and publish the same rows under
/// their own load ids — so the second open refuses typed with the
/// frozen spelling, and the lease releases with the session (drop or
/// process death), never blocking a successor.
#[tokio::test]
async fn a_second_concurrent_session_refuses_and_the_lease_releases_on_drop() {
    let dir = tempfile::tempdir().expect("tempdir");
    let shell = shell_over(dir.path());
    let held = shell
        .open(OpenContext::new(
            PipelineId::new("ref-lease"),
            LoadId::new("ref-load-a"),
        ))
        .await
        .expect("first open");
    let refused = match shell
        .open(OpenContext::new(
            PipelineId::new("ref-lease"),
            LoadId::new("ref-load-b"),
        ))
        .await
    {
        Ok(_) => panic!("a second concurrent session must refuse"),
        Err(refused) => refused,
    };
    assert_eq!(
        refused.to_string(),
        format!(
            "fatal destination error: reference destination: another session holds the lease \
             at {} — one session per output directory",
            dir.path().join("_reference_lease.lock").display()
        )
    );
    drop(held);
    shell
        .open(OpenContext::new(
            PipelineId::new("ref-lease"),
            LoadId::new("ref-load-c"),
        ))
        .await
        .expect("the lease released with the dropped session");
}
