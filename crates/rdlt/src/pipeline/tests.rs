use std::path::PathBuf;

use super::Placement;

#[test]
fn a_placement_says_where_its_connector_runs_and_an_endpoint_only_by_its_host_and_port() {
    assert_eq!(
        Placement::of(rdlt_host::Placement::InProcess),
        Placement::InProcess
    );
    let path = PathBuf::from("/opt/rdlt/bin/rdlt-connector-sqlite");
    let spawned = rdlt_host::Placement::Process { path: path.clone() };
    assert_eq!(Placement::of(spawned), Placement::Process { path });
    let listening = rdlt_host::Placement::Remote {
        endpoint: "grpcs://db.example:7443".to_owned(),
    };
    let shown = Placement::Remote {
        endpoint: "db.example:7443".to_owned(),
    };
    assert_eq!(Placement::of(listening), shown);
    let unusable = rdlt_host::Placement::Remote {
        endpoint: "grpcs://user:CANARY@db.example:7443".to_owned(),
    };
    let Placement::Remote { endpoint } = Placement::of(unusable) else {
        panic!("a remote placement");
    };
    assert!(!endpoint.contains("CANARY"), "{endpoint}");
}

#[test]
fn a_builder_sets_each_part_of_its_document() {
    use std::time::Duration;

    use crate::{
        Commit, Connector, Defaults, Document, Pipeline, ReadMode, Resources, Retry, Side, Stream,
        Until,
    };

    let side = || Side::new(Connector::new("a.b"));
    let builder = Pipeline::builder("p", side(), side())
        .stream(Stream::new("s"))
        .defaults(Defaults::new().read(ReadMode::Cdc))
        .commit(Commit::new().rows(5))
        .resources(Resources::new().writers(2))
        .retry(Retry::new().initial(Duration::from_secs(1)))
        .until(Until::Forever);
    let mut document = Document::new("p", side(), side());
    document.streams = vec![Stream::new("s")];
    document.defaults = Defaults::new().read(ReadMode::Cdc);
    document.commit = Some(Commit::new().rows(5));
    document.resources = Resources::new().writers(2);
    document.retry = Some(Retry::new().initial(Duration::from_secs(1)));
    document.until = Until::Forever;
    assert_eq!(builder.document(), &document);
    assert_eq!(
        Pipeline::from_document(document.clone()).document(),
        &document
    );
}

#[tokio::test(start_paused = true)]
async fn a_reset_refuses_a_name_that_splits_more_than_one_way_where_nothing_says_which() {
    use rdlt_connector_reference::{MemoryDestination, MemorySource};

    use crate::{Code, Connector, Host, ResetScope, Side, Stream, StreamName};

    let memory = || Connector::new("io.rapidbyte.memory");
    let source = Side::new(memory()).config(serde_json::json!({ "streams": { "users": [] } }));
    let destination = Side::new(memory()).config(serde_json::json!({ "store": "splits" }));
    let host = Host::new()
        .source::<MemorySource>()
        .destination::<MemoryDestination>();
    let pipeline = super::Pipeline::builder("splits", source, destination)
        .stream(Stream::new("users"))
        .build(&host)
        .await
        .expect("built");
    let code = |refused: &crate::Error| refused.code().map(Code::as_str).map(str::to_owned);
    let reset = pipeline.reset(["a.b.c"], ResetScope::Tables).await;
    let refused = reset.expect_err("a + b.c or a.b + c");
    assert_eq!(code(&refused).as_deref(), Some("stream_ambiguous"));
    assert_eq!(refused.stream(), Some("a.b.c"));
    assert!(refused.to_string().contains("identity"), "{refused}");
    let exact = StreamName::with_namespace("a", "b.c").expect("a stream's name");
    let reset = pipeline.reset([exact], ResetScope::Tables).await;
    let refused = reset.expect_err("never committed");
    assert_eq!(code(&refused).as_deref(), Some("stream_not_found"));
    let reset = pipeline.reset(["a.b"], ResetScope::Tables).await;
    let refused = reset.expect_err("never committed");
    assert_eq!(code(&refused).as_deref(), Some("stream_not_found"));
}

#[tokio::test(start_paused = true)]
async fn a_reset_reaches_a_stream_gone_from_plan_and_catalog_by_its_identity() {
    use rdlt_connector_reference::{MemoryDestination, MemorySource};

    use crate::{Code, Connector, Host, ResetScope, Side, Stream, StreamName};

    let memory = || Connector::new("io.rapidbyte.memory");
    let source = Side::new(memory()).config(serde_json::json!({ "streams": { "users": [] } }));
    let destination = Side::new(memory()).config(serde_json::json!({ "store": "identity" }));
    let host = Host::new()
        .source::<MemorySource>()
        .destination::<MemoryDestination>();
    let pipeline = super::Pipeline::builder("identity", source, destination)
        .stream(Stream::new("users"))
        .build(&host)
        .await
        .expect("built");
    let identity = StreamName::with_namespace("sales", "orders").expect("a stream's name");
    let mut catalog = None;
    let chosen = pipeline
        .selected(identity.clone().into(), &mut catalog)
        .await;
    assert_eq!(chosen.expect("its own name"), identity);
    let guessed = pipeline.selected("sales.orders".into(), &mut catalog).await;
    assert_eq!(
        guessed.expect("a name").namespace(),
        None,
        "a shown name says no namespace"
    );
    let reset = pipeline.reset([identity], ResetScope::Positions).await;
    let refused = reset.expect_err("never committed");
    assert_eq!(refused.code().map(Code::as_str), Some("stream_not_found"));
    assert_eq!(refused.stream(), Some("sales.orders"));
}
