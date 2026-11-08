//! Building pipelines, and asking them what they are.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use rdlt::{
    Code, Column, Connector, Document, ErrorKind, Hint, Host, MaskStep, MaskWith, Origin, Pipeline,
    Placement, ReadMode, ResetScope, Side, Stream, TransformStep, WriteMode,
};

use rdlt_connector::{
    self as connector, Catalog, ConnectContext, ConnectorError, ConnectorErrorKind,
    SourceConnector, StreamName, StreamSpec, Streams,
};

use crate::support::{Vault, memory_host, memory_pipeline};

#[tokio::test(start_paused = true)]
async fn a_built_pipeline_checks_discovers_and_refuses_to_reset_what_it_never_committed() {
    let rows = serde_json::json!([{ "id": 1 }, { "id": 2 }]);
    let pipeline = memory_pipeline("built", "facade-built", &rows)
        .build(&memory_host())
        .await
        .expect("built");
    assert_eq!(pipeline.source().placement, Placement::InProcess);
    assert_eq!(pipeline.destination().id, "io.rapidbyte.memory");
    let checked = pipeline.check().await.expect("checked");
    assert_eq!(
        (checked.pipeline.as_str(), checked.streams.as_slice()),
        ("built", &["users".to_owned()][..])
    );
    assert_eq!(checked.source, *pipeline.source());
    let catalog = pipeline.discover().await.expect("discovered");
    assert_eq!(catalog.iter().count(), 1);
    let reset = pipeline.reset(["users"], ResetScope::Positions).await;
    let refused = reset.expect_err("nothing recorded");
    assert_eq!(refused.code().map(Code::as_str), Some("stream_not_found"));
    let unnamed = pipeline.reset(["bad\u{7}"], ResetScope::Tables).await;
    assert_eq!(
        unnamed.expect_err("no name").code().map(Code::as_str),
        Some("stream_invalid")
    );
    assert_eq!(pipeline.document().pipeline, "built");
}

#[tokio::test(start_paused = true)]
async fn a_mistake_fails_with_the_same_error_from_a_builder_and_a_document() {
    let wrong = || {
        memory_pipeline("modes", "facade-modes", &serde_json::json!([])).stream(
            Stream::new("orders")
                .read(ReadMode::Incremental)
                .write(WriteMode::Replace),
        )
    };
    let built = wrong().build(&memory_host()).await.expect_err("refused");
    let written = serde_json::to_value(wrong().document()).unwrap();
    let document: Document = serde_json::from_value(written).unwrap();
    let read = Pipeline::from_document(document)
        .build(&memory_host())
        .await
        .expect_err("refused");
    assert_eq!((built.code(), built.kind()), (read.code(), read.kind()));
    assert!(built.code().is_some());
}

#[tokio::test(start_paused = true)]
async fn build_refuses_a_stream_the_source_does_not_offer_before_any_run() {
    let refused = memory_pipeline("absent", "facade-absent", &serde_json::json!([]))
        .stream(Stream::new("orders"))
        .build(&memory_host())
        .await
        .expect_err("refused");
    assert_eq!(refused.code().map(Code::as_str), Some("stream_not_found"));
    assert_eq!(refused.stream(), Some("orders"));
}

#[tokio::test(start_paused = true)]
async fn build_refuses_what_needs_no_connector_before_it_places_one() {
    let unplaced = Side::new(Connector::new("io.example.nowhere"));
    let mut versioned = Pipeline::builder("early", unplaced.clone(), unplaced.clone())
        .stream(Stream::new("users"))
        .document()
        .clone();
    versioned.version = 2;
    let refused = Pipeline::from_document(versioned)
        .build(&memory_host())
        .await
        .expect_err("refused");
    assert_eq!(refused.code().map(Code::as_str), Some("document_version"));
    let unplaceable = Pipeline::builder("unplaced", unplaced.clone(), unplaced)
        .stream(Stream::new("users"))
        .build(&Host::new())
        .await
        .expect_err("no placement");
    assert_eq!(
        unplaceable.origin(),
        Origin::Source,
        "the source is placed first"
    );
}

#[tokio::test(start_paused = true)]
async fn a_pattern_selects_what_the_source_offers_and_the_check_names_each_stream() {
    let source = Side::new(Connector::new("io.rapidbyte.memory")).config(serde_json::json!({
        "streams": { "users": [{ "id": 1 }], "orders": [{ "id": 2 }], "other": [] }
    }));
    let destination = Side::new(Connector::new("io.rapidbyte.memory"))
        .config(serde_json::json!({ "store": "facade-patterns" }));
    let pipeline = Pipeline::builder("patterns", source, destination)
        .stream(Stream::new("o*"))
        .stream(Stream::new("users"))
        .build(&memory_host())
        .await
        .expect("built");
    let checked = pipeline.check().await.expect("checked");
    assert_eq!(checked.streams, ["users", "orders", "other"]);
}

#[tokio::test(start_paused = true)]
async fn a_source_that_refuses_its_configuration_is_a_config_error_of_the_source() {
    let source = Side::new(Connector::new("io.rapidbyte.memory"))
        .config(serde_json::json!({ "streams": {}, "page_size": 0 }));
    let destination = Side::new(Connector::new("io.rapidbyte.memory"))
        .config(serde_json::json!({ "store": "facade-refused" }));
    let refused = Pipeline::builder("refused", source, destination)
        .stream(Stream::new("users"))
        .build(&memory_host())
        .await
        .expect_err("refused");
    assert_eq!(
        (refused.kind(), refused.origin()),
        (ErrorKind::Config, Origin::Source)
    );
    assert_eq!(refused.code().map(Code::as_str), Some("handshake_failed"));
}

#[tokio::test(start_paused = true)]
async fn build_refuses_every_mistake_of_the_document_before_it_places_a_connector() {
    // The host places nothing: a refusal it gives proves the document was checked first.
    let nowhere = || Side::new(Connector::new("io.example.nowhere"));
    let early = |stream: Stream| Pipeline::builder("early", nowhere(), nowhere()).stream(stream);
    let masked = |key: &str| {
        let mask = MaskStep::new(["email"], MaskWith::HmacSha256).key(key);
        early(Stream::new("users").transform(TransformStep::Mask(mask)))
    };
    let empty = Hint::Decimal {
        precision: 0,
        scale: 0,
    };
    let modes = Stream::new("users")
        .read(ReadMode::Incremental)
        .write(WriteMode::Replace);
    let mut versioned = early(Stream::new("users")).document().clone();
    versioned.version = 2;
    let mut mistakes = vec![
        (Pipeline::from_document(versioned), "document_version"),
        (
            Pipeline::builder("early", nowhere(), nowhere()),
            "plan_empty",
        ),
        (early(Stream::new("bad\u{7}")), "stream_invalid"),
        (
            early(Stream::new("users").column("p", Column::new().hint(empty))),
            "document_invalid",
        ),
        (early(modes), "plan_mode_invalid"),
        (
            early(Stream::new("users").transform(TransformStep::Code("nowhere".to_owned()))),
            "transform_unknown",
        ),
        (masked("the key itself"), "mask_key_literal"),
        (masked("${secret:mask_key}"), "secret_refused"),
    ];
    if cfg!(feature = "sql") {
        let query = TransformStep::Sql("SELECT id FROM input ORDER BY id".to_owned());
        mistakes.push((
            early(Stream::new("users").transform(query)),
            "transform_invalid",
        ));
    }
    for (builder, code) in mistakes {
        let refused = builder.build(&Host::new()).await.expect_err("refused");
        assert_eq!(refused.code().map(Code::as_str), Some(code), "{refused:?}");
    }
}

#[tokio::test(start_paused = true)]
async fn a_pattern_resolves_its_mask_key_once_for_every_stream_it_selects() {
    let resolved = Arc::new(AtomicUsize::new(0));
    let host = memory_host().secret_store(Vault::counting(Arc::clone(&resolved)));
    let row = serde_json::json!([{ "id": 1, "email": "a@example.com" }]);
    let streams = serde_json::json!({ "streams": { "users": row, "orders": row, "other": row } });
    let source = Side::new(Connector::new("io.rapidbyte.memory")).config(streams);
    let destination = Side::new(Connector::new("io.rapidbyte.memory"))
        .config(serde_json::json!({ "store": "facade-once" }));
    let mask = MaskStep::new(["email"], MaskWith::HmacSha256).key("${secret:mask_key}");
    let pipeline = Pipeline::builder("once", source, destination)
        .stream(Stream::new("*").transform(TransformStep::Mask(mask)))
        .build(&host)
        .await
        .expect("built");
    assert_eq!(pipeline.check().await.expect("checked").streams.len(), 3);
    assert_eq!(resolved.load(Ordering::SeqCst), 1, "one entry, one lookup");
}

#[tokio::test(start_paused = true)]
async fn a_mask_key_never_shows_in_the_pipeline_its_document_or_an_error() {
    let host = memory_host().secret_store(Vault::counting(Arc::default()));
    let masked = |key: &str| {
        let mask = MaskStep::new(["email"], MaskWith::HmacSha256).key(key);
        let rows = serde_json::json!([{ "id": 1, "email": "a@example.com" }]);
        let source = Side::new(Connector::new("io.rapidbyte.memory"))
            .config(serde_json::json!({ "streams": { "users": rows } }));
        let destination = Side::new(Connector::new("io.rapidbyte.memory"))
            .config(serde_json::json!({ "store": "facade-masked" }));
        Pipeline::builder("masked", source, destination)
            .stream(Stream::new("users").transform(TransformStep::Mask(mask)))
    };
    let builder = masked("${secret:mask_key}");
    assert!(!format!("{builder:?}").contains("CANARY"));
    let pipeline = builder.build(&host).await.expect("built");
    assert!(!format!("{pipeline:?}").contains("CANARY"));
    let written = serde_json::to_string(pipeline.document()).expect("it writes");
    assert!(written.contains("${secret:mask_key}"), "{written}");
    assert!(!written.contains("CANARY"), "{written}");
    let literal = masked("CANARY-literal");
    assert!(!format!("{literal:?}").contains("CANARY"));
    let refused = literal.build(&host).await.expect_err("a key written out");
    assert_eq!(refused.code().map(Code::as_str), Some("mask_key_literal"));
    let said = format!(
        "{refused} {refused:?} {}",
        serde_json::to_string(&refused.report()).expect("a report writes")
    );
    assert!(!said.contains("CANARY"), "{said}");
}

#[tokio::test(start_paused = true)]
async fn a_reset_names_a_stream_the_pipeline_no_longer_selects_as_the_catalog_shows_it() {
    let rows = serde_json::json!([{ "id": 1 }]);
    let streams = serde_json::json!({ "streams": { "users": rows, "legacy": rows } });
    let source = Side::new(Connector::new("io.rapidbyte.memory")).config(streams);
    let destination = Side::new(Connector::new("io.rapidbyte.memory"))
        .config(serde_json::json!({ "store": "facade-legacy" }));
    let pipeline = Pipeline::builder("kept", source, destination)
        .stream(Stream::new("users"))
        .build(&memory_host())
        .await
        .expect("built");
    let reset = pipeline
        .reset(["legacy", "gone"], ResetScope::Positions)
        .await;
    let refused = reset.expect_err("never committed");
    assert_eq!(refused.code().map(Code::as_str), Some("stream_not_found"));
    let shown = format!("{pipeline:?}");
    for part in ["\"kept\"", "io.rapidbyte.memory"] {
        assert!(shown.contains(part), "{shown}");
    }
}

#[tokio::test(start_paused = true)]
async fn a_destination_refusing_its_configuration_fails_the_build_as_the_destination_s() {
    let source = Side::new(Connector::new("io.rapidbyte.memory"))
        .config(serde_json::json!({ "streams": { "users": [] } }));
    let destination = Side::new(Connector::new("io.rapidbyte.memory"))
        .config(serde_json::json!({ "shelf": "facade-refused" }));
    let refused = Pipeline::builder("refused", source, destination)
        .stream(Stream::new("users"))
        .build(&memory_host())
        .await
        .expect_err("refused");
    let said = refused.to_string();
    assert_eq!(
        (refused.kind(), refused.origin()),
        (ErrorKind::Config, Origin::Destination),
        "{said}"
    );
}

/// What a [`Cataloged`] source's configuration lists to have it never answer.
const STALLS: &str = "stalls";

/// A source that reads nothing: its catalog holds the streams its configuration lists, each a
/// namespace and a name or a name alone, cannot be read where it lists none, and is never
/// answered where it lists [`STALLS`].
struct Cataloged(Vec<Vec<String>>);

impl SourceConnector for Cataloged {
    const ID: &'static str = "io.test.cataloged";
    const VERSION: &'static str = "1.0.0";
    type Config = Vec<Vec<String>>;

    async fn connect(streams: Vec<Vec<String>>, _: &ConnectContext) -> connector::Result<Self> {
        Ok(Self(streams))
    }

    async fn check(&self) -> connector::Result<()> {
        Ok(())
    }

    fn streams(&self) -> Streams<Self> {
        Streams::new()
    }

    async fn discover(&self) -> connector::Result<Catalog> {
        if self.0.is_empty() {
            let unreachable = "the catalog's server is unreachable";
            return Err(ConnectorError::new(
                ConnectorErrorKind::Transient,
                unreachable,
            ));
        }
        if self.0.iter().flatten().any(|part| part == STALLS) {
            std::future::pending::<()>().await;
        }
        let named = self.0.iter().map(|parts| match parts.as_slice() {
            [namespace, name] => StreamName::with_namespace(namespace, name),
            [name] => StreamName::new(name),
            _ => panic!("a stream is named by one part or two"),
        });
        let specs = named.map(|name| StreamSpec::new(name.expect("a stream's name")));
        Ok(Catalog::new(specs.collect()).expect("a catalog"))
    }
}

#[tokio::test(start_paused = true)]
async fn a_reset_resolves_a_name_against_the_source_s_catalog() {
    let listed = serde_json::json!([["users"], ["a.b", "c"], ["a", "b.c"]]);
    let source = Side::new(Connector::new(Cataloged::ID)).config(listed);
    let destination = Side::new(Connector::new("io.rapidbyte.memory"))
        .config(serde_json::json!({ "store": "facade-cataloged" }));
    let host = memory_host().source::<Cataloged>();
    let pipeline = Pipeline::builder("cataloged", source, destination)
        .stream(Stream::new("users"))
        .build(&host)
        .await
        .expect("built");
    let reset = pipeline.reset(["a.b.c"], ResetScope::Positions).await;
    let refused = reset.expect_err("two streams of the catalog show as a.b.c");
    assert_eq!(refused.code().map(Code::as_str), Some("stream_ambiguous"));
    assert!(
        refused.to_string().contains("the source's catalog"),
        "{refused}"
    );
}

#[tokio::test(start_paused = true)]
async fn a_source_that_never_answers_discovery_fails_the_build_after_the_connector_wait() {
    let source = Side::new(Connector::new(Cataloged::ID)).config(serde_json::json!([[STALLS]]));
    let destination = Side::new(Connector::new("io.rapidbyte.memory"))
        .config(serde_json::json!({ "store": "facade-stalled" }));
    let host = memory_host().source::<Cataloged>();
    let building = Pipeline::builder("stalled", source, destination)
        .stream(Stream::new("users"))
        .build(&host);
    let began = tokio::time::Instant::now();
    let built = tokio::time::timeout(Duration::from_hours(1), building).await;
    let refused = built
        .expect("bounded by the connector wait")
        .expect_err("refused");
    assert_eq!(began.elapsed(), Duration::from_mins(30));
    let said = refused.to_string();
    assert_eq!(
        refused.code().map(Code::as_str),
        Some("connector_failed"),
        "{said}"
    );
    assert_eq!(
        refused.connector_code(),
        Some("deadline_exceeded"),
        "{said}"
    );
    assert_eq!(refused.origin(), Origin::Source, "{said}");
    assert!(refused.is_retryable(), "{said}");
}

#[tokio::test(start_paused = true)]
async fn a_source_whose_catalog_cannot_be_read_fails_the_build_as_the_source_s() {
    let source = Side::new(Connector::new(Cataloged::ID)).config(serde_json::json!([]));
    let destination = Side::new(Connector::new("io.rapidbyte.memory"))
        .config(serde_json::json!({ "store": "facade-undiscoverable" }));
    let host = memory_host().source::<Cataloged>();
    let refused = Pipeline::builder("undiscoverable", source, destination)
        .stream(Stream::new("users"))
        .build(&host)
        .await
        .expect_err("refused");
    let said = refused.to_string();
    assert_eq!(
        refused.code().map(Code::as_str),
        Some("connector_failed"),
        "{said}"
    );
    assert_eq!(refused.origin(), Origin::Source, "{said}");
    assert!(refused.is_retryable(), "{said}");
    assert!(said.contains("discovering the catalog"), "{said}");
}
