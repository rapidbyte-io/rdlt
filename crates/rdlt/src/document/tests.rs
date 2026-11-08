use std::collections::BTreeSet;
use std::time::Duration;

use proptest::prelude::*;
use rdlt_connector::{Catalog, StreamName, StreamSpec};

use super::Document;
pub(crate) use super::fault::tests::{holds, said};
use crate::{
    Access, Column, Commit, Connector, Defaults, DeleteMode, Hint, Literal, MaskStep, MaskWith,
    Nested, OnTruncate, OnUnsupported, ReadMode, Resources, RetentionLoss, Retry, SchemaPolicy,
    SchemaRules, Side, Stream, TransformStep, Until, WriteMode,
};

/// The example pipeline of the facade's design, as JSON.
fn example_json() -> serde_json::Value {
    serde_json::json!({
        "version": 1,
        "pipeline": "orders",
        "source": { "connector": "io.rapidbyte.postgres", "config": { "dsn": "${secret:pg_dsn}" } },
        "destination": {
            "connector": "io.rapidbyte.sqlite",
            "config": { "path": "/data/out.db" },
            "access": { "write": ["/data"] }
        },
        "defaults": { "write": "append", "schema": { "policy": "evolve" }, "nested": "native" },
        "streams": [
            {
                "name": "public.orders",
                "read": "cdc",
                "write": { "merge": { "key": ["id"] } },
                "include": ["id", "email", "amount", "status"],
                "transform": [
                    { "mask": { "columns": ["email"], "with": "hmac_sha256", "key": "${secret:mask_key}" } },
                    { "sql": "SELECT *, amount * 100 AS cents FROM input WHERE status <> 'test'" }
                ]
            },
            { "name": "public.*" }
        ],
        "commit": { "every": "10s", "rows": 1_000_000 },
        "resources": { "memory": "512MiB", "writers": 8, "partitions": 16 },
        "retry": { "max_attempts": 8 }
    })
}

/// The same pipeline, built; later tests lower and run it.
pub(crate) fn example() -> Document {
    let source = Side::new(Connector::new("io.rapidbyte.postgres"))
        .config(serde_json::json!({ "dsn": "${secret:pg_dsn}" }));
    let destination = Side::new(Connector::new("io.rapidbyte.sqlite"))
        .config(serde_json::json!({ "path": "/data/out.db" }))
        .access(Access::new().write("/data"));
    let mask = MaskStep::new(["email"], MaskWith::HmacSha256).key("${secret:mask_key}");
    let orders = Stream::new("public.orders")
        .read(ReadMode::Cdc)
        .write(WriteMode::merge(["id"]))
        .include_only(["id", "email", "amount", "status"])
        .transform(TransformStep::Mask(mask))
        .transform(TransformStep::Sql(
            "SELECT *, amount * 100 AS cents FROM input WHERE status <> 'test'".to_owned(),
        ));
    let mut document = Document::new("orders", source, destination);
    document.defaults = Defaults::new()
        .write(WriteMode::Append)
        .schema(SchemaRules::new().policy(SchemaPolicy::Evolve))
        .nested(Nested::Native);
    document.streams = vec![orders, Stream::new("public.*")];
    document.commit = Some(Commit::new().every(Duration::from_secs(10)).rows(1_000_000));
    document.resources = Resources::new().memory(512 << 20).writers(8).partitions(16);
    document.retry = Some(Retry::new().max_attempts(8));
    document
}

/// `value` read as a `T`, and written back as `value`.
fn reads_and_writes<T>(value: &serde_json::Value, expected: &T)
where
    T: serde::Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
{
    let read: T = serde_json::from_value(value.clone()).expect("it reads");
    assert_eq!(&read, expected, "{value}");
    assert_eq!(&serde_json::to_value(&read).expect("it writes"), value);
}

#[test]
fn the_example_reads_as_the_builder_builds_it_and_writes_back_alike() {
    let read: Document = serde_json::from_value(example_json()).expect("the example reads");
    assert_eq!(read, example());
    let written = serde_json::to_value(&read).expect("a document writes");
    assert_eq!(serde_json::from_value::<Document>(written).unwrap(), read);
}

#[test]
fn a_write_mode_is_a_word_or_a_word_with_its_key() {
    for (json, mode) in [
        (
            serde_json::json!("merge"),
            WriteMode::Merge { key: Vec::new() },
        ),
        (
            serde_json::json!({ "history": { "key": ["id"] } }),
            WriteMode::history(["id"]),
        ),
        (
            serde_json::json!({ "merge": { "key": ["a", "b"] } }),
            WriteMode::merge(["a", "b"]),
        ),
        (
            serde_json::json!("history"),
            WriteMode::History { key: Vec::new() },
        ),
        (serde_json::json!("replace"), WriteMode::Replace),
        (serde_json::json!("append"), WriteMode::Append),
    ] {
        reads_and_writes(&json, &mode);
    }
    for refused in [
        serde_json::json!("upsert"),
        serde_json::json!({ "append": { "key": [] } }),
        serde_json::json!({ "merge": { "key": [] }, "history": { "key": [] } }),
        serde_json::json!({ "merge": { "keys": [] } }),
        serde_json::json!({}),
        serde_json::json!(1),
    ] {
        assert!(
            serde_json::from_value::<WriteMode>(refused.clone()).is_err(),
            "{refused}"
        );
    }
}

#[test]
fn nesting_is_a_word_or_a_normalization_with_its_depth() {
    for (json, nested) in [
        (serde_json::json!("native"), Nested::Native),
        (serde_json::json!("json"), Nested::Json),
        (
            serde_json::json!("normalize"),
            Nested::Normalize { max_depth: 8 },
        ),
        (
            serde_json::json!({ "normalize": { "max_depth": 3 } }),
            Nested::Normalize { max_depth: 3 },
        ),
    ] {
        reads_and_writes(&json, &nested);
    }
    let deep: Nested =
        serde_json::from_value(serde_json::json!({ "normalize": { "max_depth": 8 } }))
            .expect("an explicit depth of 8 reads");
    assert_eq!(deep, Nested::Normalize { max_depth: 8 });
    for refused in [
        serde_json::json!("flat"),
        serde_json::json!({ "native": { "max_depth": 3 } }),
        serde_json::json!({ "normalize": { "depth": 3 } }),
    ] {
        assert!(
            serde_json::from_value::<Nested>(refused.clone()).is_err(),
            "{refused}"
        );
    }
}

#[test]
fn a_run_reads_until_its_source_is_exhausted_forever_or_for_a_while() {
    for (json, until) in [
        (serde_json::json!("exhausted"), Until::Exhausted),
        (serde_json::json!("forever"), Until::Forever),
        (
            serde_json::json!("10m"),
            Until::For(Duration::from_secs(600)),
        ),
    ] {
        reads_and_writes(&json, &until);
    }
    for refused in [
        serde_json::json!("never"),
        serde_json::json!("10"),
        serde_json::json!(10),
    ] {
        assert!(
            serde_json::from_value::<Until>(refused.clone()).is_err(),
            "{refused}"
        );
    }
    assert!(Until::default().is_exhausted());
    assert!(!Until::Forever.is_exhausted());
}

#[test]
fn a_connector_is_an_id_or_an_id_with_requirements() {
    let pinned = Connector::new("io.rapidbyte.sqlite").endpoint("grpcs://db.example:7443");
    let json =
        serde_json::json!({ "id": "io.rapidbyte.sqlite", "endpoint": "grpcs://db.example:7443" });
    reads_and_writes(&json, &pinned);
    assert_eq!(pinned.id(), "io.rapidbyte.sqlite");
    let required = Connector::new("io.rapidbyte.sqlite")
        .version("^1.2")
        .digest("ab".repeat(32));
    let json = serde_json::json!({ "id": "io.rapidbyte.sqlite", "version": "^1.2", "digest": "ab".repeat(32) });
    reads_and_writes(&json, &required);
    reads_and_writes(
        &serde_json::json!("io.rapidbyte.sqlite"),
        &Connector::new("io.rapidbyte.sqlite"),
    );
    let unknown = serde_json::json!({ "id": "x.y", "url": "grpcs://a:1" });
    assert!(serde_json::from_value::<Connector>(unknown).is_err());
    assert!(serde_json::from_value::<Connector>(serde_json::json!(7)).is_err());
}

#[test]
fn every_transform_step_reads_as_it_writes() {
    let constants = TransformStep::constant([
        ("live", Literal::Boolean(true)),
        ("source", Literal::Text("pg".to_owned())),
        ("tier", Literal::Integer(-3)),
    ]);
    for (json, step) in [
        (
            serde_json::json!({ "rename": { "amount": "cents" } }),
            TransformStep::rename([("amount", "cents")]),
        ),
        (
            serde_json::json!({ "mask": { "columns": ["address.street"], "with": "redact", "text": "-" } }),
            TransformStep::Mask(MaskStep::new(["address.street"], MaskWith::Redact).text("-")),
        ),
        (
            serde_json::json!({ "mask": { "columns": ["email"], "with": "null" } }),
            TransformStep::Mask(MaskStep::new(["email"], MaskWith::Null)),
        ),
        (
            serde_json::json!({ "constant": { "source": "pg", "tier": -3, "live": true } }),
            constants,
        ),
        (
            serde_json::json!({ "sql": "SELECT id FROM input" }),
            TransformStep::Sql("SELECT id FROM input".to_owned()),
        ),
        (
            serde_json::json!({ "code": "pseudonymize" }),
            TransformStep::Code("pseudonymize".to_owned()),
        ),
    ] {
        reads_and_writes(&json, &step);
    }
}

#[test]
fn constants_keep_the_order_they_are_written_in() {
    let read = |text: &str| serde_json::from_str::<TransformStep>(text).expect("it reads");
    let constants = read(r#"{ "constant": { "tier": 2, "source": "pg" } }"#);
    let ordered = [
        ("tier", Literal::Integer(2)),
        ("source", Literal::Text("pg".to_owned())),
    ];
    assert_eq!(constants, TransformStep::constant(ordered));
}

#[test]
fn a_rename_reads_the_same_however_its_keys_were_ordered() {
    let read = |text: &str| serde_json::from_str::<TransformStep>(text).expect("it reads");
    let renames = read(r#"{ "rename": { "b": "c", "a": "b" } }"#);
    assert_eq!(renames, read(r#"{ "rename": { "a": "b", "b": "c" } }"#));
    assert_eq!(renames, TransformStep::rename([("b", "c"), ("a", "b")]));
    let value = serde_json::to_value(&renames).expect("it writes");
    let again = serde_json::from_value::<TransformStep>(value).expect("it reads");
    assert_eq!(again, renames);
    let written = serde_json::to_string(&renames).expect("it writes");
    assert_eq!(written, r#"{"rename":{"a":"b","b":"c"}}"#);
}

#[test]
fn renames_and_constants_are_mappings_and_say_so() {
    for (refused, expected) in [
        (
            serde_json::json!({ "rename": ["a", "b"] }),
            "a mapping of each column to its new name",
        ),
        (
            serde_json::json!({ "constant": "tier" }),
            "a mapping of each column to its value",
        ),
    ] {
        let error = serde_json::from_value::<TransformStep>(refused).expect_err("refused");
        assert!(error.to_string().contains(expected), "{error}");
    }
}

#[test]
fn a_constant_holds_text_an_integer_or_a_boolean_and_nothing_else() {
    let large = serde_json::json!({ "constant": { "n": u64::MAX } });
    for refused in [
        serde_json::json!({ "constant": { "price": 1.5 } }),
        serde_json::json!({ "constant": { "empty": null } }),
        serde_json::json!({ "constant": { "list": [1] } }),
        serde_json::json!({ "constant": { "object": { "a": 1 } } }),
        large,
    ] {
        assert!(
            serde_json::from_value::<TransformStep>(refused.clone()).is_err(),
            "{refused}"
        );
    }
}

#[test]
fn a_stream_and_its_columns_read_every_setting() {
    let json = serde_json::json!({
        "name": "events",
        "read": "incremental",
        "write": "replace",
        "deletes": "soft",
        "on_truncate": "ignore",
        "on_retention_loss": "reset",
        "schema": { "policy": "discard_value", "on_unsupported": "refuse" },
        "nested": "json",
        "columns": {
            "at": { "hint": "timestamp", "policy": "freeze" },
            "price": { "hint": { "decimal": { "precision": 10, "scale": 2 } }, "nested": "native" },
            "tags": { "on_unsupported": "variant_column", "policy": "discard_row" }
        },
        "exclude": ["secret"],
        "transform": [{ "code": "pseudonymize" }]
    });
    let stream = Stream::new("events")
        .read(ReadMode::Incremental)
        .write(WriteMode::Replace)
        .deletes(DeleteMode::Soft)
        .on_truncate(OnTruncate::Ignore)
        .on_retention_loss(RetentionLoss::Reset)
        .schema(
            SchemaRules::new()
                .policy(SchemaPolicy::DiscardValue)
                .on_unsupported(OnUnsupported::Refuse),
        )
        .nested(Nested::Json)
        .column(
            "at",
            Column::new()
                .hint(Hint::Timestamp)
                .policy(SchemaPolicy::Freeze),
        )
        .column(
            "price",
            Column::new()
                .hint(Hint::Decimal {
                    precision: 10,
                    scale: 2,
                })
                .nested(Nested::Native),
        )
        .column(
            "tags",
            Column::new()
                .on_unsupported(OnUnsupported::VariantColumn)
                .policy(SchemaPolicy::DiscardRow),
        )
        .exclude(["secret"])
        .transform(TransformStep::Code("pseudonymize".to_owned()));
    reads_and_writes(&json, &stream);
    let fields = serde_json::json!({ "name": "s", "include": ["a"], "transform": [] });
    let read: Stream = serde_json::from_value(fields).expect("an empty transform list reads");
    assert_eq!(read, Stream::new("s").include_only(["a"]));
    let unknown = serde_json::json!({ "name": "s", "fields": {} });
    assert!(serde_json::from_value::<Stream>(unknown).is_err());
}

#[test]
fn every_hint_and_policy_word_reads_as_it_writes() {
    let hints = [
        ("bool", Hint::Bool),
        ("int8", Hint::Int8),
        ("int16", Hint::Int16),
        ("int32", Hint::Int32),
        ("int64", Hint::Int64),
        ("float32", Hint::Float32),
        ("float64", Hint::Float64),
        ("utf8", Hint::Utf8),
        ("binary", Hint::Binary),
        ("date", Hint::Date),
        ("uuid", Hint::Uuid),
        ("json", Hint::Json),
        ("timestamp", Hint::Timestamp),
    ];
    for (word, hint) in hints {
        reads_and_writes(&serde_json::json!(word), &hint);
    }
    for (word, mode) in [
        ("full", ReadMode::Full),
        ("incremental", ReadMode::Incremental),
        ("cdc", ReadMode::Cdc),
    ] {
        reads_and_writes(&serde_json::json!(word), &mode);
    }
    for (word, deletes) in [
        ("hard", DeleteMode::Hard),
        ("soft", DeleteMode::Soft),
        ("ignore", DeleteMode::Ignore),
    ] {
        reads_and_writes(&serde_json::json!(word), &deletes);
    }
    reads_and_writes(&serde_json::json!("apply"), &OnTruncate::Apply);
    reads_and_writes(&serde_json::json!("fail"), &RetentionLoss::Fail);
}

#[test]
fn commit_resources_and_retry_read_sizes_and_durations() {
    let json = serde_json::json!({
        "version": 1,
        "pipeline": "p",
        "source": { "connector": "a.b" },
        "destination": { "connector": "c.d", "access": { "read": ["/in"], "network": true } },
        "streams": [{ "name": "s" }],
        "commit": { "every": "500ms", "rows": 10, "bytes": "64KiB" },
        "resources": { "memory": "1GiB" },
        "retry": { "max_attempts": 3, "initial": "1s", "max_delay": "1m" },
        "until": "forever"
    });
    let mut document = Document::new(
        "p",
        Side::new(Connector::new("a.b")),
        Side::new(Connector::new("c.d")).access(Access::new().read("/in").network()),
    );
    document.streams = vec![Stream::new("s")];
    document.commit = Some(
        Commit::new()
            .every(Duration::from_millis(500))
            .rows(10)
            .bytes(64 << 10),
    );
    document.resources = Resources::new().memory(1 << 30);
    document.retry = Some(
        Retry::new()
            .max_attempts(3)
            .initial(Duration::from_secs(1))
            .max_delay(Duration::from_secs(60)),
    );
    document.until = Until::Forever;
    reads_and_writes(&json, &document);
    let counted = serde_json::json!({ "memory": 1_048_576 });
    let read: Resources =
        serde_json::from_value(counted).expect("a size reads as a count of bytes");
    assert_eq!(read, Resources::new().memory(1 << 20));
    for refused in [
        serde_json::json!({ "memory": -1 }),
        serde_json::json!({ "memory": "1 GiB" }),
        serde_json::json!({ "memory": 1.5 }),
    ] {
        assert!(
            serde_json::from_value::<Resources>(refused.clone()).is_err(),
            "{refused}"
        );
    }
    for refused in [
        serde_json::json!({ "every": 10 }),
        serde_json::json!({ "every": "10" }),
    ] {
        assert!(
            serde_json::from_value::<Commit>(refused.clone()).is_err(),
            "{refused}"
        );
    }
}

#[test]
fn a_document_says_nothing_of_what_it_leaves_as_it_was() {
    let mut document = Document::new(
        "p",
        Side::new(Connector::new("a.b")),
        Side::new(Connector::new("c.d")),
    );
    document.streams = vec![Stream::new("s")];
    let written = serde_json::to_value(&document).expect("a document writes");
    assert_eq!(
        written,
        serde_json::json!({
            "version": 1,
            "pipeline": "p",
            "source": { "connector": "a.b" },
            "destination": { "connector": "c.d" },
            "streams": [{ "name": "s" }]
        })
    );
    assert!(Access::new().is_empty());
    assert!(!Access::new().network().is_empty());
    assert!(Defaults::new().is_empty());
    assert!(!Defaults::new().read(ReadMode::Cdc).is_empty());
    assert!(Resources::new().is_empty());
    assert!(!Resources::new().partitions(2).is_empty());
}

#[test]
fn the_document_s_schema_names_its_keys_as_a_document_writes_them() {
    let schema = serde_json::to_value(schemars::schema_for!(Document)).expect("a schema writes");
    let stream = &schema["$defs"]["Stream"]["properties"];
    assert!(stream.get("include").is_some(), "{stream}");
    assert!(stream.get("included").is_none(), "{stream}");
    assert_eq!(schema["additionalProperties"], serde_json::json!(false));
    let written = schema.to_string();
    for unit in ["(ns|us|ms|s|m|h)", "(B|KiB|MiB|GiB|TiB)"] {
        assert!(written.contains(unit), "{written}");
    }
}

/// A document holding `fields` over the fewest keys a document needs.
fn document_with(fields: &serde_json::Value) -> serde_json::Value {
    let side = serde_json::json!({ "connector": "a.b" });
    let mut document = serde_json::json!({
        "version": 1, "pipeline": "p", "source": side, "destination": side, "streams": []
    });
    for (key, value) in fields.as_object().expect("fields") {
        document[key] = value.clone();
    }
    document
}

/// A document of one stream holding `fields`.
fn stream_with(fields: &serde_json::Value) -> serde_json::Value {
    let mut stream = serde_json::json!({ "name": "s" });
    for (key, value) in fields.as_object().expect("fields") {
        stream[key] = value.clone();
    }
    document_with(&serde_json::json!({ "streams": [stream] }))
}

/// A document of one stream masking with `fields`.
fn mask_with(fields: &serde_json::Value) -> serde_json::Value {
    let mut mask = serde_json::json!({ "columns": ["e"], "with": "hmac_sha256" });
    for (key, value) in fields.as_object().expect("fields") {
        mask[key] = value.clone();
    }
    stream_with(&serde_json::json!({ "transform": [{ "mask": mask }] }))
}

/// Documents refused for a value of a kind its place does not take, each holding the canary
/// `31337424` or `CANARY`, or the fraction `0.9876543`.
pub(crate) fn refusals() -> Vec<serde_json::Value> {
    let mut refused = value_refusals();
    refused.extend(setting_refusals());
    refused
}

fn value_refusals() -> Vec<serde_json::Value> {
    vec![
        document_with(&serde_json::json!({ "version": "CANARY" })),
        document_with(&serde_json::json!({ "pipeline": 31_337_424 })),
        document_with(&serde_json::json!({ "source": { "connector": 31_337_424 } })),
        document_with(&serde_json::json!({ "source": { "connector": { "id": 31_337_424 } } })),
        document_with(
            &serde_json::json!({ "source": { "connector": "a.b", "access": { "read": [31_337_424] } } }),
        ),
        document_with(&serde_json::json!({ "resources": { "memory": 0.987_654_3 } })),
        document_with(&serde_json::json!({ "resources": { "memory": "CANARY" } })),
        document_with(&serde_json::json!({ "resources": { "writers": "CANARY" } })),
        document_with(&serde_json::json!({ "resources": { "partitions": true } })),
        document_with(&serde_json::json!({ "commit": { "every": 31_337_424 } })),
        document_with(&serde_json::json!({ "commit": { "rows": 0.987_654_3 } })),
        document_with(&serde_json::json!({ "retry": { "max_attempts": "CANARY" } })),
        document_with(&serde_json::json!({ "until": 31_337_424 })),
        document_with(&serde_json::json!({ "until": "CANARY" })),
        stream_with(&serde_json::json!({ "name": 31_337_424 })),
        stream_with(&serde_json::json!({ "include": [31_337_424] })),
        stream_with(&serde_json::json!({ "read": "CANARY" })),
        stream_with(&serde_json::json!({ "read": 31_337_424 })),
        stream_with(&serde_json::json!({ "write": "CANARY" })),
        stream_with(&serde_json::json!({ "write": { "CANARY": {} } })),
        stream_with(&serde_json::json!({ "write": { "merge": { "key": [31_337_424] } } })),
        stream_with(&serde_json::json!({ "nested": { "normalize": { "max_depth": "CANARY" } } })),
        stream_with(&serde_json::json!({ "columns": { "c": { "hint": "CANARY" } } })),
        stream_with(&serde_json::json!({ "columns": { "c": { "policy": 31_337_424 } } })),
        stream_with(
            &serde_json::json!({ "columns": { "c": { "hint": { "decimal": { "precision": "CANARY", "scale": 1 } } } } }),
        ),
        stream_with(&serde_json::json!({ "transform": [{ "sql": 31_337_424 }] })),
        stream_with(&serde_json::json!({ "transform": [{ "rename": { "a": 31_337_424 } }] })),
        stream_with(&serde_json::json!({ "transform": [{ "constant": { "a": 0.987_654_3 } }] })),
        mask_with(&serde_json::json!({ "key": 31_337_424 })),
        mask_with(&serde_json::json!({ "with": "CANARY" })),
    ]
}

/// Documents refused at the settings beside a stream's mode, a read, a retry and a connector.
fn setting_refusals() -> Vec<serde_json::Value> {
    vec![
        mask_with(&serde_json::json!({ "with": "redact", "text": 31_337_424 })),
        document_with(&serde_json::json!({ "defaults": { "read": "CANARY" } })),
        document_with(&serde_json::json!({ "defaults": { "write": "CANARY" } })),
        document_with(&serde_json::json!({ "defaults": { "nested": "CANARY" } })),
        document_with(&serde_json::json!({ "commit": { "bytes": "CANARY" } })),
        document_with(&serde_json::json!({ "retry": { "initial": 31_337_424 } })),
        document_with(&serde_json::json!({ "retry": { "max_delay": "CANARY" } })),
        document_with(
            &serde_json::json!({ "source": { "connector": { "id": "a.b", "version": 31_337_424 } } }),
        ),
        document_with(
            &serde_json::json!({ "source": { "connector": { "id": "a.b", "digest": 31_337_424 } } }),
        ),
        document_with(
            &serde_json::json!({ "source": { "connector": { "id": "a.b", "endpoint": 31_337_424 } } }),
        ),
        stream_with(&serde_json::json!({ "deletes": "CANARY" })),
        stream_with(&serde_json::json!({ "on_truncate": "CANARY" })),
        stream_with(&serde_json::json!({ "on_retention_loss": "CANARY" })),
        stream_with(&serde_json::json!({ "exclude": [31_337_424] })),
        stream_with(&serde_json::json!({ "columns": { "c": { "nested": "CANARY" } } })),
        stream_with(&serde_json::json!({ "columns": { "c": { "on_unsupported": "CANARY" } } })),
        stream_with(&serde_json::json!({ "transform": [{ "code": 31_337_424 }] })),
        stream_with(&serde_json::json!({ "write": { "CANARY": 5 } })),
        stream_with(&serde_json::json!({ "nested": { "CANARY": 5 } })),
        stream_with(&serde_json::json!({ "columns": { "c": { "hint": { "CANARY": 5 } } } })),
    ]
}

/// Documents each holding a canary where a mapping or a list belongs.
pub(crate) fn misplaced() -> Vec<serde_json::Value> {
    let side = serde_json::json!({ "connector": "a.b" });
    let document = |key: &str, value: serde_json::Value| {
        let mut document = serde_json::json!({
            "version": 1, "pipeline": "p", "source": side, "destination": side, "streams": []
        });
        document[key] = value;
        document
    };
    let stream = |key: &str, value: serde_json::Value| {
        let mut stream = serde_json::json!({ "name": "s" });
        stream[key] = value;
        document("streams", serde_json::json!([stream]))
    };
    let access = |access: serde_json::Value| {
        document(
            "source",
            serde_json::json!({ "connector": "a.b", "access": access }),
        )
    };
    vec![
        document("source", serde_json::json!(31_337_424)),
        document("destination", serde_json::json!("CANARY")),
        document("defaults", serde_json::json!("CANARY")),
        document("defaults", serde_json::json!({ "schema": "CANARY" })),
        document("streams", serde_json::json!("CANARY")),
        document("streams", serde_json::json!(["CANARY"])),
        document("commit", serde_json::json!("CANARY")),
        document("resources", serde_json::json!(31_337_424)),
        document("retry", serde_json::json!("CANARY")),
        access(serde_json::json!("CANARY")),
        access(serde_json::json!({ "network": "CANARY" })),
        access(serde_json::json!({ "network": 31_337_424 })),
        access(serde_json::json!({ "read": "CANARY" })),
        stream("schema", serde_json::json!("CANARY")),
        stream("columns", serde_json::json!("CANARY")),
        stream("columns", serde_json::json!({ "c": "CANARY" })),
        stream(
            "columns",
            serde_json::json!({ "c": { "hint": { "decimal": "CANARY" } } }),
        ),
        stream("include", serde_json::json!("CANARY")),
        stream("write", serde_json::json!({ "merge": "CANARY" })),
        stream("nested", serde_json::json!({ "normalize": "CANARY" })),
        stream("transform", serde_json::json!("CANARY")),
        stream("transform", serde_json::json!(["CANARY"])),
        stream("transform", serde_json::json!([31_337_424])),
        stream("transform", serde_json::json!([{ "mask": "CANARY" }])),
        stream("transform", serde_json::json!([{ "CANARY": 1 }])),
        stream("transform", serde_json::json!([{ "rename": "CANARY" }])),
        stream("transform", serde_json::json!([{ "constant": "CANARY" }])),
        stream(
            "transform",
            serde_json::json!([{ "mask": { "columns": "CANARY" } }]),
        ),
    ]
}

#[test]
fn no_refusal_quotes_the_value_it_refused() {
    for refused in refusals().into_iter().chain(misplaced()) {
        let error = serde_json::from_value::<Document>(refused.clone()).expect_err("refused");
        let said = error.to_string();
        for canary in ["CANARY", "31337424", "9876543"] {
            assert!(!said.contains(canary), "{said} for {refused}");
        }
    }
}

#[test]
fn a_mask_key_shows_only_as_the_reference_it_should_be() {
    let literal = MaskStep::new(["email"], MaskWith::HmacSha256).key("CANARY-key");
    let shown = format!("{literal:?}");
    assert!(!shown.contains("CANARY"), "{shown}");
    let referred = MaskStep::new(["email"], MaskWith::HmacSha256).key("${secret:mask_key}");
    assert!(format!("{referred:?}").contains("${secret:mask_key}"));
    let mut document = example();
    document.streams[0].transform[0] = TransformStep::Mask(literal);
    assert!(!format!("{document:?}").contains("CANARY"));
}

#[test]
fn a_container_given_a_scalar_is_refused_at_its_path_by_the_kind_it_expected() {
    let side = serde_json::json!({ "connector": "a.b" });
    let head = serde_json::json!({ "version": 1, "pipeline": "p", "destination": side });
    let mut listed = head.clone();
    listed["source"] = side;
    listed["streams"] = serde_json::json!("CANARY");
    let mut shared = head;
    shared["source"] = serde_json::json!({ "connector": "a.b", "access": { "network": "CANARY" } });
    shared["streams"] = serde_json::json!([]);
    for (written, path, expected) in [
        (listed, "streams", "expected a list"),
        (shared, "source.access.network", "expected true or false"),
    ] {
        let refused =
            serde_path_to_error::deserialize::<_, Document>(written).expect_err("refused");
        assert_eq!(refused.path().to_string(), path);
        let said = refused.inner().to_string();
        assert!(
            said.contains(expected) && !said.contains("CANARY"),
            "{said}"
        );
    }
}

#[test]
fn every_refusal_says_what_it_expected() {
    let side = serde_json::json!({ "connector": "a.b" });
    let document = |key: &str, value: serde_json::Value| {
        let mut document = serde_json::json!({
            "version": 1, "pipeline": "p", "source": side, "destination": side, "streams": []
        });
        document[key] = value;
        document
    };
    let stream = |key: &str, value: serde_json::Value| {
        let mut stream = serde_json::json!({ "name": "s" });
        stream[key] = value;
        document("streams", serde_json::json!([stream]))
    };
    for (refused, expected) in [
        (document("pipeline", serde_json::json!(5)), "expected text"),
        (
            document("version", serde_json::json!("1")),
            "expected a whole number",
        ),
        (
            document("source", serde_json::json!(5)),
            "expected a mapping",
        ),
        (
            document("source", serde_json::json!({ "connector": 5 })),
            "expected a connector's id, or { id, version, digest, endpoint }",
        ),
        (
            document("resources", serde_json::json!({ "memory": true })),
            "expected a size such as 512MiB",
        ),
        (
            stream("columns", serde_json::json!(5)),
            "expected a mapping of names",
        ),
        (
            stream("write", serde_json::json!(5)),
            "expected a word, or a mapping of one word to its settings",
        ),
        (
            stream("read", serde_json::json!("full_refresh")),
            "expected one of full, incremental, cdc",
        ),
        (
            stream("transform", serde_json::json!([5])),
            "expected a mapping of one kind of step to its settings",
        ),
        (
            stream(
                "transform",
                serde_json::json!([{ "constant": { "c": [1] } }]),
            ),
            "expected text, an integer or a boolean",
        ),
    ] {
        let said = serde_json::from_value::<Document>(refused)
            .expect_err("refused")
            .to_string();
        assert!(said.contains(expected), "{said}");
    }
}

#[test]
fn no_debug_shows_a_side_s_configuration() {
    let inline = serde_json::json!({ "password": "CANARY", "dsn": "pg://user:CANARY@db/x" });
    let side = Side::new(Connector::new("io.rapidbyte.postgres"))
        .config(inline)
        .access(Access::new().read("/in"));
    let document = Document::new("p", side.clone(), side.clone());
    let builder = crate::Pipeline::builder("p", side.clone(), side.clone());
    for shown in [
        format!("{side:?}"),
        format!("{side:#?}"),
        format!("{document:?}"),
        format!("{builder:?}"),
    ] {
        assert!(!shown.contains("CANARY"), "{shown}");
        for part in ["io.rapidbyte.postgres", "config", "/in"] {
            assert!(shown.contains(part), "{shown}");
        }
    }
}

#[test]
fn no_debug_shows_more_of_an_endpoint_than_where_it_listens() {
    let shown = |endpoint: &str| {
        let connector = Connector::new("io.rapidbyte.postgres").endpoint(endpoint);
        let side = Side::new(connector);
        let document = Document::new("p", side.clone(), side.clone());
        let builder = crate::Pipeline::builder("p", side.clone(), side.clone());
        [
            format!("{side:?}"),
            format!("{document:?}"),
            format!("{builder:?}"),
        ]
    };
    for written in [
        "grpcs://user:CANARY@db.example:7443",
        "CANARY is no endpoint",
    ] {
        for shown in shown(written) {
            assert!(!shown.contains("CANARY"), "{shown}");
            assert!(
                shown.contains("***") && shown.contains("io.rapidbyte.postgres"),
                "{shown}"
            );
        }
    }
    for shown in shown("grpcs://db.example:7443") {
        assert!(shown.contains("db.example:7443"), "{shown}");
        assert!(!shown.contains("grpcs"), "{shown}");
    }
}

#[test]
fn an_empty_include_is_written_and_an_absent_one_is_not() {
    let empty = Stream::new("s").include_only(Vec::<String>::new());
    let written = serde_json::to_value(&empty).expect("it writes");
    assert_eq!(written, serde_json::json!({ "name": "s", "include": [] }));
    assert_eq!(
        serde_json::from_value::<Stream>(written).expect("it reads"),
        empty
    );
    let absent = serde_json::to_value(Stream::new("s")).expect("it writes");
    assert_eq!(absent, serde_json::json!({ "name": "s" }));
    let null = serde_json::json!({ "name": "s", "include": null });
    assert_eq!(
        serde_json::from_value::<Stream>(null).expect("it reads"),
        Stream::new("s")
    );
    let schema = Document::json_schema();
    let columns = &schema["$defs"]["Stream"]["properties"]["include"];
    assert_eq!(
        columns["type"],
        serde_json::json!(["array", "null"]),
        "{columns}"
    );
}

#[test]
fn a_null_size_or_duration_says_nothing() {
    let resources = serde_json::json!({ "memory": null, "writers": 2 });
    let resources = serde_json::from_value::<Resources>(resources).expect("it reads");
    assert_eq!(resources, Resources::new().writers(2));
    let retry = serde_json::json!({ "initial": null, "max_delay": null, "max_attempts": 3 });
    let retry = serde_json::from_value::<Retry>(retry).expect("it reads");
    assert_eq!(retry, Retry::new().max_attempts(3));
    let commit = serde_json::json!({ "every": null, "bytes": null, "rows": 5 });
    let commit = serde_json::from_value::<Commit>(commit).expect("it reads");
    assert_eq!(commit, Commit::new().rows(5));
}

#[test]
fn a_side_shares_the_network_only_where_it_says_so() {
    for shared in [true, false] {
        let access = serde_json::json!({ "network": shared });
        let read = serde_json::from_value::<Access>(access).expect("it reads");
        assert_eq!(read.network, shared);
    }
}

#[test]
fn the_document_s_schema_defines_each_value_written_by_hand() {
    let schema = Document::json_schema();
    let defined = &schema["$defs"];
    for (name, description) in [
        ("WriteMode", "How a stream's rows reach its table."),
        ("Nested", "How nested values are stored."),
        (
            "Connector",
            "A connector's id, or its id with what its placement must honour.",
        ),
        ("Constant", "A constant: text, an integer or a boolean."),
        (
            "Size",
            "Bytes: a whole number, or digits and one of B, KiB, MiB, GiB, TiB.",
        ),
        (
            "Duration",
            "A duration: digits and one of ns, us, ms, s, m, h.",
        ),
        ("Until", "How long a run reads."),
    ] {
        assert_eq!(defined[name]["description"], description, "{name}");
    }
    assert_eq!(defined["Constant"]["minimum"], i64::MIN);
    assert_eq!(defined["Constant"]["maximum"], i64::MAX);
    let writes = &defined["WriteMode"]["oneOf"];
    let words = serde_json::json!(["append", "replace", "merge", "history"]);
    assert_eq!(writes[0]["enum"], words);
    assert_eq!(writes[1]["required"], serde_json::json!(["merge"]));
    assert_eq!(writes[2]["required"], serde_json::json!(["history"]));
}

#[test]
fn a_mask_key_written_out_is_never_written() {
    let literal = MaskStep::new(["email"], MaskWith::HmacSha256).key("CANARY-key");
    let stream = Stream::new("users").transform(TransformStep::Mask(literal));
    let refused = serde_json::to_value(&stream).expect_err("refused");
    assert!(!refused.to_string().contains("CANARY"), "{refused}");
    let referred = MaskStep::new(["email"], MaskWith::HmacSha256).key("${secret:mask_key}");
    let written = serde_json::to_value(TransformStep::Mask(referred)).expect("written");
    assert_eq!(written["mask"]["key"], "${secret:mask_key}");
}

#[test]
fn a_secret_reference_is_one_reference_and_nothing_else() {
    for (text, refers) in [
        ("${secret:mask_key}", true),
        ("${env:MASK_KEY}", true),
        ("${file:/run/secrets/mask}", true),
        ("the key itself", false),
        ("${vault:mask}", false),
        ("${secret:}", false),
        ("prefix ${secret:mask}", false),
        ("${secret:a}${secret:b}", false),
        ("$${secret:mask}", false),
        ("${secret:mask", false),
        ("${secret}", false),
    ] {
        assert_eq!(super::transform::refers_to_secret(text), refers, "{text}");
    }
}

#[test]
fn the_document_s_json_schema_is_what_tools_check_one_against() {
    let schema = Document::json_schema();
    assert_eq!(schema["title"], "Document");
    let required = schema["required"].as_array().expect("required keys");
    for key in ["version", "pipeline", "source", "destination", "streams"] {
        assert!(required.contains(&serde_json::json!(key)), "{key}");
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(rdlt_testkit::cases(256)))]

    /// A built document written as YAML reads back equal, and lowers to an equal plan.
    #[test]
    fn a_document_written_reads_back_to_the_same_plan(document in super::drawn::documents()) {
        let written = document.to_yaml().expect("a built document writes");
        let read = Document::parse(&written).expect("a written document reads");
        prop_assert_eq!(&read, &document);
        let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
        let lowered = |document: &Document| {
            let named = document.streams.iter().filter_map(|s| StreamName::new(&s.name).ok());
            let names: BTreeSet<StreamName> = named.collect();
            let offered = Catalog::new(names.into_iter().map(StreamSpec::new).collect()).unwrap();
            runtime.block_on(async {
                let prepared = crate::lower::prepare(document, &crate::Host::new()).await;
                prepared
                    .and_then(|prepared| prepared.bind(&offered))
                    .map(|lowered| (lowered.plan, lowered.config))
                    .map_err(|error| error.code())
            })
        };
        prop_assert_eq!(lowered(&read), lowered(&document));
    }

    /// Any text a document holds, however hostile to YAML's syntax, is written so it reads back:
    /// quoted where it would otherwise be a tag, an alias, a number, a boolean or a comment.
    #[test]
    fn any_text_reads_back_as_it_was_written(text in any::<String>(), key in any::<String>()) {
        let mut document = example();
        document.pipeline = text.clone();
        document.streams[1].name = text.clone();
        document.source.config = serde_json::json!({ key.clone(): text.clone(), "t": [text.clone()] });
        let written = document.to_yaml().expect("it writes");
        prop_assert_eq!(Document::parse(&written).expect("it reads"), document);
    }
}

#[tokio::test]
async fn a_document_reads_from_its_file_and_errors_name_the_file() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("orders.yaml");
    std::fs::write(&path, example().to_yaml().unwrap()).unwrap();
    assert_eq!(Document::from_path(&path).await.unwrap(), example());
    std::fs::write(&path, "version: 1\nwhat: 1\n").unwrap();
    let error = Document::from_path(&path).await.unwrap_err();
    assert!(
        error
            .to_string()
            .starts_with(&format!("{}:2:1:", path.display())),
        "{error}"
    );
    let missing = Document::from_path(directory.path().join("absent.yaml"))
        .await
        .unwrap_err();
    assert_eq!(
        missing.code().map(rdlt_engine::Code::as_str),
        Some("document_unreadable")
    );
    assert!(missing.to_string().contains("absent.yaml"), "{missing}");
    assert!(std::error::Error::source(&missing).is_some());
    let directory_is_no_file = Document::from_path(directory.path()).await.unwrap_err();
    assert_eq!(
        directory_is_no_file.code().map(rdlt_engine::Code::as_str),
        Some("document_unreadable")
    );
}

#[tokio::test]
async fn a_file_past_the_limit_is_refused_at_any_byte_and_one_at_it_reads() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("large.yaml");
    let text = example().to_yaml().unwrap();
    let limit = crate::limits::DOCUMENT_BYTES;
    let padded = |bytes: usize| format!("{text}# {}\n", "x".repeat(bytes - text.len() - 3));
    std::fs::write(&path, padded(limit)).unwrap();
    assert_eq!(Document::from_path(&path).await.unwrap(), example());
    std::fs::write(&path, padded(limit + 1)).unwrap();
    let error = Document::from_path(&path).await.unwrap_err();
    assert_eq!(
        error.code().map(rdlt_engine::Code::as_str),
        Some("document_too_large")
    );
    assert!(error.to_string().contains("large.yaml"), "{error}");
    // A byte after the limit that splits a character is still a file too large, not text unread.
    let mut bytes = padded(limit).into_bytes();
    bytes.extend_from_slice(&[0xe2, 0x82]);
    std::fs::write(&path, bytes).unwrap();
    let error = Document::from_path(&path).await.unwrap_err();
    assert_eq!(
        error.code().map(rdlt_engine::Code::as_str),
        Some("document_too_large")
    );
    std::fs::write(&path, [b'v', 0xff]).unwrap();
    let unreadable = Document::from_path(&path).await.unwrap_err();
    assert_eq!(
        unreadable.code().map(rdlt_engine::Code::as_str),
        Some("document_unreadable")
    );
}

#[tokio::test]
async fn a_path_that_is_no_regular_file_is_refused_without_waiting_on_it() {
    let directory = tempfile::tempdir().unwrap();
    let fifo = directory.path().join("a fifo");
    let made = std::process::Command::new("mkfifo").arg(&fifo).status();
    assert!(made.unwrap().success(), "mkfifo makes a pipe");
    for path in [fifo, directory.path().to_owned()] {
        let shown = path.display().to_string();
        let reading = tokio::time::timeout(Duration::from_secs(30), Document::from_path(path));
        let error = reading
            .await
            .unwrap_or_else(|_| panic!("reading {shown} must not wait on it"))
            .expect_err("no regular file reads");
        assert_eq!(
            error.code().map(rdlt_engine::Code::as_str),
            Some("document_unreadable"),
            "{error}"
        );
        assert!(error.to_string().contains("regular file"), "{error}");
    }
}

#[tokio::test]
async fn a_file_of_no_text_is_refused_holding_none_of_its_bytes() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("document.yaml");
    std::fs::write(&path, b"CANARY-value-9f3\xff").unwrap();
    let error = Document::from_path(&path).await.unwrap_err();
    assert_eq!(
        error.code().map(rdlt_engine::Code::as_str),
        Some("document_unreadable")
    );
    let said = said(&error);
    assert!(!holds(&said, "CANARY-value-9f3"), "{said}");
    assert!(error.to_string().contains("utf-8"), "{error}");
}

#[test]
fn a_document_read_from_text_names_its_origin_where_a_file_would_stand() {
    let written = example().to_yaml().unwrap();
    let read = crate::Pipeline::from_text(&written, "<python>").expect("read");
    assert_eq!(read.document(), &example());
    let error = Document::from_text("version: 1\nwhat: 1\n", "<python>").unwrap_err();
    assert!(error.to_string().starts_with("<python>:2:1:"), "{error}");
    let refused = crate::Pipeline::from_text("version: 1\nwhat: 1\n", "<python>").unwrap_err();
    assert!(
        refused.to_string().starts_with("<python>:2:1:"),
        "{refused}"
    );
}

#[tokio::test]
async fn a_pipeline_reads_from_its_file() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("orders.yaml");
    std::fs::write(&path, example().to_yaml().unwrap()).unwrap();
    let read = crate::Pipeline::from_path(&path).await.expect("it reads");
    assert_eq!(read.document(), &example());
    let refused = crate::Pipeline::from_path(directory.path().join("absent.yaml"))
        .await
        .unwrap_err();
    assert_eq!(
        refused.code().map(rdlt_engine::Code::as_str),
        Some("document_unreadable")
    );
}

/// An `include` naming no column stays apart from none at all: the plan refuses the first and
/// takes the second, so the reader and the writer must not merge them.
#[test]
fn an_include_of_no_columns_reads_and_writes_apart_from_no_include() {
    let head =
        "version: 1\npipeline: p\nsource: { connector: a.b }\ndestination: { connector: c.d }\n";
    let listed = Document::parse(&format!("{head}streams: [{{ name: s, include: [] }}]\n"))
        .expect("an empty include reads");
    assert_eq!(listed.streams[0].included, Some(Vec::new()));
    let written = listed.to_yaml().expect("it writes");
    assert_eq!(Document::parse(&written).expect("it reads back"), listed);
    let bare =
        Document::parse(&format!("{head}streams: [{{ name: s }}]\n")).expect("no include reads");
    assert_eq!(bare.streams[0].included, None);
    assert!(!bare.to_yaml().expect("it writes").contains("include"));
}

#[test]
fn a_version_other_than_1_is_refused_by_the_reader_and_the_schema_alike() {
    let text = |version: &str| {
        format!(
            "version: {version}\npipeline: p\nsource: {{ connector: a.b }}\n\
             destination: {{ connector: c.d }}\nstreams: []\n"
        )
    };
    let written = |version: u32| {
        serde_json::json!({
            "version": version, "pipeline": "p", "source": { "connector": "a.b" },
            "destination": { "connector": "c.d" }, "streams": []
        })
    };
    let validator = validator();
    for version in ["2", "300"] {
        let refused = Document::parse(&text(version)).expect_err("another version");
        assert_eq!(
            refused.code().map(rdlt_engine::Code::as_str),
            Some("document_version")
        );
        let said = refused.to_string();
        assert!(said.starts_with("1:"), "{said}");
        assert!(!said.contains(version), "{said}");
    }
    assert!(!validator.is_valid(&written(2)));
    assert!(Document::parse(&text("1")).is_ok());
    assert!(validator.is_valid(&written(1)));
}

#[test]
fn a_document_with_a_key_written_out_is_never_written() {
    let literal = MaskStep::new(["email"], MaskWith::HmacSha256).key("CANARY-key");
    let mut document = example();
    document.streams[0].transform[0] = TransformStep::Mask(literal);
    let refused = document
        .to_yaml()
        .expect_err("a key written out is refused");
    assert_eq!(
        refused.code().map(rdlt_engine::Code::as_str),
        Some("mask_key_literal")
    );
    let said = format!(
        "{refused} {refused:?} {}",
        serde_json::to_string(&refused.report()).unwrap()
    );
    assert!(!said.contains("CANARY"), "{said}");
}

#[test]
fn the_document_s_json_schema_is_as_recorded() {
    let schema = serde_json::to_string_pretty(&Document::json_schema()).unwrap();
    insta::assert_snapshot!("document_schema", schema);
}

/// A check of documents against the document's schema.
fn validator() -> jsonschema::Validator {
    jsonschema::validator_for(&Document::json_schema()).expect("the schema compiles")
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(rdlt_testkit::cases(128)))]

    /// What the reader takes, the schema takes.
    #[test]
    fn a_drawn_document_meets_the_schema(document in super::drawn::documents()) {
        let written = serde_json::to_value(&document).expect("a document writes");
        prop_assert!(validator().is_valid(&written), "{}", written);
    }
}

#[test]
fn the_schema_refuses_what_the_reader_refuses_of_a_key_or_a_number() {
    let validator = validator();
    let document = serde_json::json!({
        "version": 1, "pipeline": "p", "source": { "connector": "a.b" },
        "destination": { "connector": "c.d" }, "streams": []
    });
    let changed = |key: &str, value: serde_json::Value| {
        let mut changed = document.clone();
        changed[key] = value;
        changed
    };
    assert!(validator.is_valid(&document));
    let decimal = serde_json::json!([{
        "name": "s",
        "columns": { "c": { "hint": { "decimal": { "precision": 1, "scale": 1, "other": 1 } } } }
    }]);
    for refused in [
        changed("version", serde_json::json!(2)),
        changed("streams", decimal),
        changed(
            "retry",
            serde_json::json!({ "max_attempts": u64::from(u32::MAX) + 1 }),
        ),
    ] {
        assert!(!validator.is_valid(&refused), "{refused}");
    }
    let held = serde_json::json!({ "max_attempts": u32::MAX });
    assert!(validator.is_valid(&changed("retry", held)));
}

/// A document's indentation can outgrow what the reader takes: a compact document nested deep
/// around many short entries reads, and its YAML would not.
#[test]
fn a_document_whose_yaml_would_not_read_back_is_refused_at_writing() {
    let entries: serde_json::Map<String, serde_json::Value> = (0..18_000)
        .map(|index| (format!("k{index:05}"), serde_json::json!(1)))
        .collect();
    let mut config = serde_json::Value::Object(entries);
    for _ in 0..27 {
        config = serde_json::json!({ "a": config });
    }
    let source = Side::new(Connector::new("a.b")).config(config);
    let mut document = Document::new("p", source, Side::new(Connector::new("c.d")));
    document.streams = vec![Stream::new("s")];
    let compact = serde_json::to_string(&document).expect("a document writes");
    let read = Document::parse(&compact).expect("the compact document reads");
    let refused = read
        .to_yaml()
        .expect_err("its YAML would be past the limit");
    assert_eq!(
        refused.code().map(rdlt_engine::Code::as_str),
        Some("document_too_large")
    );
}
