//! A JSON number whose exponent no canonical text holds is refused as its push arrives, from a
//! JSON push as from an Arrow column of JSON, whatever the stream does with it.

use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use rdlt_engine::{ErrorKind, Nested, RunOutcome, SchemaSettings, StreamPlan, WriteMode};

use crate::support::batches::{BatchStream, batches};
use crate::support::{commit_every, engine, memory, pipeline, published_json, stream};

/// An exponent of nineteen significant digits: its place is beyond a 64-bit integer.
const TINY: &str = "1e-1234567890123456789";

/// The ways a stream may treat its rows: as they are, normalized, and kept as history.
fn plans() -> [(&'static str, StreamPlan, bool); 3] {
    [
        ("append", stream("events"), false),
        (
            "normalized",
            stream("events").schema(SchemaSettings::new().nested(Nested::normalize())),
            false,
        ),
        ("history", stream("events").write(WriteMode::History), true),
    ]
}

/// Loads `source` as `plan` says into the store `name`.
async fn load(name: &str, plan: StreamPlan, source: BatchStream) -> RunOutcome {
    let source = batches(name, vec![source]).await;
    engine(commit_every(1))
        .run(pipeline(name, [plan]), source, memory(name).await)
        .await
}

/// Asserts the run was refused, typed, and stored nothing.
fn refused(name: &str, outcome: &RunOutcome) {
    let error = outcome.error.as_ref().expect("the number is refused");
    assert_eq!(
        (error.kind(), error.code()),
        (ErrorKind::Source, Some("limit_exceeded")),
        "{name}: {error:?}"
    );
    assert!(
        published_json(name, "events").is_empty(),
        "{name} stored rows"
    );
}

#[tokio::test(start_paused = true)]
async fn a_pushed_number_no_canonical_text_holds_is_refused_as_it_arrives() {
    // In a column of JSON, beside a string, and in a column of floats, alone.
    let pushes = [
        format!("{{\"id\":1,\"x\":\"s\"}}\n{{\"id\":2,\"x\":{TINY}}}"),
        format!("{{\"id\":1,\"x\":1.5}}\n{{\"id\":2,\"x\":{TINY}}}"),
        "{\"id\":1,\"x\":[1,{\"y\":0e12345678901234567890}]}".to_owned(),
    ];
    for (index, push) in pushes.iter().enumerate() {
        for (kind, plan, keyed) in plans() {
            let name = format!("pushed_exponent_{kind}_{index}");
            let mut source = BatchStream::json("events", &[push.as_str()]);
            if keyed {
                source = source.primary_key(&["id"]);
            }
            let outcome = load(&name, plan, source).await;
            refused(&name, &outcome);
        }
    }
}

#[tokio::test(start_paused = true)]
async fn an_arrow_number_no_canonical_text_holds_is_refused_as_it_arrives() {
    let extension = [("ARROW:extension:name".to_owned(), "arrow.json".to_owned())];
    let json = Field::new("x", DataType::Utf8, true).with_metadata(extension.into());
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        json,
    ]));
    let values: ArrayRef = Arc::new(StringArray::from(vec!["\"s\"", TINY]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2])), values])
        .expect("columns match their schema");
    for (kind, plan, keyed) in plans() {
        let name = format!("arrow_exponent_{kind}");
        let mut source = BatchStream::new("events", vec![batch.clone()]);
        if keyed {
            source = source.primary_key(&["id"]);
        }
        let outcome = load(&name, plan, source).await;
        refused(&name, &outcome);
    }
}

#[tokio::test(start_paused = true)]
async fn text_that_reads_like_such_a_number_beside_a_zero_loads_as_text() {
    let push = "{\"id\":1,\"x\":0.0,\"note\":\"1e1234567890123456789\"}\n\
                {\"id\":2,\"x\":1e-0000000000000000000001}";
    let name = "exponent_lookalike";
    let outcome = load(name, stream("events"), BatchStream::json("events", &[push])).await;
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    let mut rows = published_json(name, "events");
    rows.sort_by_key(|row| row["id"].as_i64());
    assert_eq!(rows[0]["note"], "1e1234567890123456789");
    assert_eq!(rows[1]["x"], 0.1);
}
