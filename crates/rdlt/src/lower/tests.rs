use std::time::Duration;

use rdlt_connector::{
    BoxFuture, Catalog, ColumnPath, ConnectorId, DecimalType, LogicalType, Role, StreamName,
};
use rdlt_connector::{ReadMode as Read, Secret, StreamSpec, TimeUnit};
use rdlt_engine::{Code, ErrorKind, Mask, Op, Origin, SchemaSettings};
use rdlt_host::{ConnectorRef, Digest, SecretFault, SecretReference, SecretResolver};

use super::{Lowered, prepare, reference};
use crate::{
    Access, Column, Commit, Connector, Defaults, DeleteMode, Document, Error, Hint, Host, Literal,
    MaskStep, MaskWith, Nested, OnTruncate, OnUnsupported, ReadMode, Resources, RetentionLoss,
    SchemaPolicy, SchemaRules, Side, Stream, TransformStep, Until, WriteMode,
};

fn document(streams: Vec<Stream>) -> Document {
    let memory = || Side::new(Connector::new("io.rapidbyte.memory"));
    let mut document = Document::new("lowered", memory(), memory());
    document.streams = streams;
    document
}

/// A source's catalog of `names`, each a stream's name or its namespace and name.
fn catalog(names: &[&[&str]]) -> Catalog {
    let streams = names.iter().map(|parts| {
        let name = match parts {
            [name] => StreamName::new(name),
            [namespace, name] => StreamName::with_namespace(namespace, name),
            _ => panic!("a stream's name has one part or two"),
        };
        StreamSpec::new(name.unwrap())
    });
    Catalog::new(streams.collect()).unwrap()
}

/// `document` lowered on `host`, its streams selected from `catalog`.
async fn lower(document: &Document, host: &Host, catalog: &Catalog) -> Result<Lowered, Error> {
    prepare(document, host).await?.bind(catalog)
}

/// `document` lowered on a host of no secrets or code, from a catalog of its streams' names.
async fn lowered(document: &Document) -> Result<Lowered, Error> {
    let named = document
        .streams
        .iter()
        .filter_map(|stream| StreamName::new(&stream.name).ok());
    let offered = Catalog::new(named.map(StreamSpec::new).collect()).unwrap();
    lower(document, &Host::new(), &offered).await
}

#[test]
fn a_side_names_its_connector_s_requirements_and_access() {
    let digest = "0a".repeat(32);
    let connector = Connector::new("io.rapidbyte.sqlite")
        .version("^1.2")
        .digest(&digest)
        .endpoint("grpcs://db.example:7443");
    let access = Access::new().read("/in").write("/out").network();
    let side = Side::new(connector).access(access);
    let named = reference(&side, Role::Destination).expect("a reference");
    let expected = ConnectorRef::new(ConnectorId::parse("io.rapidbyte.sqlite").unwrap())
        .version(semver::VersionReq::parse("^1.2").unwrap())
        .digest(Digest([0x0a; 32]))
        .endpoint("grpcs://db.example:7443")
        .read("/in")
        .write("/out")
        .network();
    assert_eq!(named, expected);
    let bare = reference(
        &Side::new(Connector::new("io.rapidbyte.memory")),
        Role::Source,
    );
    let id = ConnectorId::parse("io.rapidbyte.memory").unwrap();
    assert_eq!(bare.expect("a reference"), ConnectorRef::new(id));
}

#[test]
fn a_digest_is_sixty_four_hex_digits() {
    let upper = "AB".repeat(32);
    let named = reference(
        &Side::new(Connector::new("a.b").digest(&upper)),
        Role::Source,
    );
    assert_eq!(
        named.expect("upper-case hex reads").digest,
        Some(Digest([0xab; 32]))
    );
    for wrong in [
        "0".repeat(63),
        "0".repeat(65),
        "0".repeat(66),
        format!("+f{}", "0".repeat(62)),
        format!("0g{}", "0".repeat(62)),
        format!("é{}", "0".repeat(62)),
    ] {
        let refused = reference(
            &Side::new(Connector::new("a.b").digest(wrong.clone())),
            Role::Source,
        );
        let refused = refused.expect_err("not a digest");
        assert_eq!(
            refused.code(),
            Some(Code::new("reference_invalid")),
            "{wrong}"
        );
    }
}

#[test]
fn a_reference_that_is_not_one_is_refused_as_a_mistake_of_its_author() {
    for side in [
        Side::new(Connector::new("not an id")),
        Side::new(Connector::new("a.b").version("one point two")),
    ] {
        let refused = reference(&side, Role::Destination).expect_err("refused");
        assert_eq!(refused.code(), Some(Code::new("reference_invalid")));
        assert_eq!(refused.kind(), ErrorKind::Config);
        assert!(refused.to_string().contains("destination"), "{refused}");
        assert!(std::error::Error::source(&refused).is_some());
    }
}

#[cfg(feature = "sql")]
#[tokio::test(start_paused = true)]
async fn the_example_lowers_to_its_plan_and_configuration() {
    let offered = catalog(&[&["public", "orders"], &["public", "users"], &["other", "x"]]);
    let example = crate::document::tests::example();
    let unkeyed = lower(&example, &Host::new(), &offered).await;
    let refused = unkeyed.err().and_then(|error| error.code());
    assert_eq!(
        refused.map(Code::as_str),
        Some("secret_refused"),
        "the host keeps no store"
    );
    let host = Host::new().secret_store(crate::host::tests::Vault);
    let lowered = lower(&example, &host, &offered).await.expect("lowered");
    let [orders, users] = lowered.plan.streams() else {
        panic!("two streams: {:?}", lowered.plan.streams());
    };
    assert_eq!(
        orders.name(),
        &StreamName::with_namespace("public", "orders").unwrap()
    );
    assert_eq!(
        (orders.read_mode(), orders.write_mode()),
        (Read::Cdc, rdlt_engine::WriteMode::Merge)
    );
    assert_eq!(orders.merge_key(), Some(&[ColumnPath::from("id")][..]));
    assert_eq!(
        orders.included().map(std::collections::BTreeSet::len),
        Some(4)
    );
    assert_eq!(orders.transforms().len(), 2);
    let masked = Op::mask(
        ["email"],
        Mask::hmac_sha256(Secret::new("CANARY-mask".to_owned())),
    );
    assert_eq!(orders.transforms()[0], masked.into());
    assert_eq!(
        users.name(),
        &StreamName::with_namespace("public", "users").unwrap()
    );
    assert_eq!(users.write_mode(), rdlt_engine::WriteMode::Append);
    let pipeline = SchemaSettings::new()
        .policy(rdlt_engine::SchemaPolicy::Evolve)
        .nested(rdlt_engine::Nested::Native);
    assert_eq!(lowered.plan.schema_settings(), &pipeline);
    assert_eq!(lowered.plan.until(), rdlt_engine::Until::Exhausted);
    let config = &lowered.config;
    assert_eq!(config.writers().map(std::num::NonZeroU16::get), Some(8));
    assert_eq!(config.memory().get(), 512 << 20);
    assert_eq!(config.partitions().get(), 16);
    let commit = config.commit().expect("a commit policy");
    assert_eq!(commit.every(), Some(Duration::from_secs(10)));
    assert_eq!(
        commit.rows().map(std::num::NonZeroU64::get),
        Some(1_000_000)
    );
    assert_eq!(config.retry().attempts().get(), 8);
}

#[tokio::test(start_paused = true)]
async fn a_pattern_selects_the_catalog_s_streams_and_a_named_stream_wins() {
    let document = document(vec![
        Stream::new("public.*"),
        Stream::new("public.orders").write(WriteMode::Replace),
        Stream::new("other.?"),
    ]);
    let offered = catalog(&[
        &["public", "orders"],
        &["public", "users"],
        &["other", "x"],
        &["other", "yz"],
    ]);
    let lowered = lower(&document, &Host::new(), &offered)
        .await
        .expect("lowered");
    let selected: Vec<(String, rdlt_engine::WriteMode)> = lowered
        .plan
        .streams()
        .iter()
        .map(|stream| (stream.name().to_string(), stream.write_mode()))
        .collect();
    let replace = rdlt_engine::WriteMode::Replace;
    let append = rdlt_engine::WriteMode::Append;
    let expected = [
        ("public.orders", replace),
        ("public.users", append),
        ("other.x", append),
    ];
    let expected: Vec<(String, rdlt_engine::WriteMode)> = expected
        .into_iter()
        .map(|(name, mode)| (name.to_owned(), mode))
        .collect();
    assert_eq!(selected, expected);
    let named = &lowered.plan.streams()[0];
    assert_eq!(
        named.name().namespace(),
        Some("public"),
        "a name is the catalog's stream"
    );
}

#[tokio::test(start_paused = true)]
async fn a_named_stream_the_catalog_lacks_is_planned_as_it_reads() {
    let document = document(vec![Stream::new("absent"), Stream::new("public.orders")]);
    let lowered = lower(&document, &Host::new(), &catalog(&[&["public", "x"]])).await;
    let lowered = lowered.expect("the engine's check refuses what the catalog lacks");
    let names: Vec<&StreamName> = lowered
        .plan
        .streams()
        .iter()
        .map(rdlt_engine::StreamPlan::name)
        .collect();
    let absent = StreamName::new("absent").unwrap();
    let orders = StreamName::new("public.orders").unwrap();
    assert_eq!(names, [&absent, &orders]);
}

#[tokio::test(start_paused = true)]
async fn policies_lower_to_the_pipeline_the_stream_and_the_column() {
    let mut document = document(vec![
        Stream::new("a")
            .schema(SchemaRules::new().policy(SchemaPolicy::Evolve))
            .column("x", Column::new().policy(SchemaPolicy::DiscardRow))
            .column("hinted", Column::new().hint(Hint::Int32)),
    ]);
    document.defaults = Defaults::new().schema(SchemaRules::new().policy(SchemaPolicy::Freeze));
    let lowered = lowered(&document).await.expect("lowered");
    let stream = &lowered.plan.streams()[0];
    let policy = |policy| SchemaSettings::new().policy(policy);
    assert_eq!(
        lowered.plan.schema_settings(),
        &policy(rdlt_engine::SchemaPolicy::Freeze)
    );
    assert_eq!(
        stream.schema_settings(),
        &policy(rdlt_engine::SchemaPolicy::Evolve)
    );
    let column = stream.column_settings(&"x".into());
    assert_eq!(column, Some(&policy(rdlt_engine::SchemaPolicy::DiscardRow)));
    assert_eq!(
        stream.column_settings(&"hinted".into()),
        None,
        "a hint alone sets nothing else"
    );
    assert_eq!(stream.hinted(&"hinted".into()), Some(&LogicalType::Int32));
}

#[tokio::test(start_paused = true)]
async fn a_change_stream_lowers_with_its_deletes_truncates_retention_and_columns() {
    let decimal = Hint::Decimal {
        precision: 10,
        scale: 2,
    };
    let changes = Stream::new("changes")
        .read(ReadMode::Cdc)
        .write(WriteMode::merge(["id"]))
        .deletes(DeleteMode::Soft)
        .on_truncate(OnTruncate::Ignore)
        .on_retention_loss(RetentionLoss::Reset)
        .nested(Nested::Normalize { max_depth: 3 })
        .schema(SchemaRules::new().on_unsupported(OnUnsupported::Refuse))
        .column("price", Column::new().hint(decimal))
        .column(
            "at",
            Column::new().hint(Hint::Timestamp).nested(Nested::Json),
        )
        .column("id", Column::new().policy(SchemaPolicy::Freeze))
        .column(
            "tags",
            Column::new().on_unsupported(OnUnsupported::VariantColumn),
        )
        .exclude(["secret"]);
    let lowered = lowered(&document(vec![changes])).await.expect("lowered");
    let changes = &lowered.plan.streams()[0];
    assert_eq!(changes.read_mode(), Read::Cdc);
    assert_eq!(changes.delete_mode(), rdlt_engine::DeleteMode::Soft);
    assert_eq!(changes.truncate_mode(), rdlt_engine::OnTruncate::Ignore);
    assert_eq!(changes.retention_loss(), rdlt_engine::RetentionLoss::Reset);
    let settings = SchemaSettings::new()
        .on_unsupported(rdlt_engine::OnUnsupported::Refuse)
        .nested(rdlt_engine::Nested::Normalize { max_depth: 3 });
    assert_eq!(changes.schema_settings(), &settings);
    let decimal = LogicalType::Decimal(DecimalType::new(10, 2).unwrap());
    assert_eq!(changes.hinted(&"price".into()), Some(&decimal));
    let timestamp = LogicalType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    assert_eq!(changes.hinted(&"at".into()), Some(&timestamp));
    let json = SchemaSettings::new().nested(rdlt_engine::Nested::Json);
    assert_eq!(changes.column_settings(&"at".into()), Some(&json));
    let frozen = SchemaSettings::new().policy(rdlt_engine::SchemaPolicy::Freeze);
    assert_eq!(changes.column_settings(&"id".into()), Some(&frozen));
    let variant = SchemaSettings::new().on_unsupported(rdlt_engine::OnUnsupported::VariantColumn);
    assert_eq!(changes.column_settings(&"tags".into()), Some(&variant));
    let excluded: Vec<&ColumnPath> = changes.excluded().collect();
    assert_eq!(excluded, [&ColumnPath::from("secret")]);
}

#[tokio::test(start_paused = true)]
async fn streams_take_the_defaults_and_the_pipeline_reads_as_long_as_it_says() {
    let mut document = document(vec![
        Stream::new("versions")
            .write(WriteMode::history(["id"]))
            .nested(Nested::Native),
        Stream::new("kept").read(ReadMode::Full).include_only(["a"]),
    ]);
    document.defaults = Defaults::new()
        .read(ReadMode::Incremental)
        .nested(Nested::Json);
    document.until = Until::For(Duration::from_secs(60));
    let lowered = lowered(&document).await.expect("lowered");
    let [versions, kept] = lowered.plan.streams() else {
        panic!("two streams");
    };
    assert_eq!(
        versions.read_mode(),
        Read::Incremental,
        "the default's read"
    );
    assert_eq!(versions.write_mode(), rdlt_engine::WriteMode::History);
    assert_eq!(versions.merge_key(), Some(&[ColumnPath::from("id")][..]));
    let native = SchemaSettings::new().nested(rdlt_engine::Nested::Native);
    assert_eq!(versions.schema_settings(), &native);
    assert_eq!(kept.read_mode(), Read::Full);
    assert_eq!(kept.write_mode(), rdlt_engine::WriteMode::Append);
    let included = kept.included().expect("an allow-list");
    assert!(included.contains(&ColumnPath::from("a")));
    let until = rdlt_engine::Until::For(Duration::from_secs(60));
    assert_eq!(lowered.plan.until(), until);
    let json = SchemaSettings::new().nested(rdlt_engine::Nested::Json);
    assert_eq!(lowered.plan.schema_settings(), &json);
}

#[tokio::test(start_paused = true)]
async fn transform_steps_lower_in_order_to_the_engine_s_ops_and_the_host_s_code() {
    let host = Host::new().transform("unchanged", Op::rename([("x", "y")]));
    let constants = TransformStep::constant([
        ("source", Literal::Text("pg".to_owned())),
        ("tier", Literal::Integer(2)),
    ]);
    let steps = Stream::new("people")
        .transform(TransformStep::rename([("a", "b")]))
        .transform(TransformStep::Mask(MaskStep::new(
            ["address.street"],
            MaskWith::Redact,
        )))
        .transform(TransformStep::Mask(
            MaskStep::new(["b"], MaskWith::Redact).text("-"),
        ))
        .transform(TransformStep::Mask(MaskStep::new(
            ["email"],
            MaskWith::Null,
        )))
        .transform(constants)
        .transform(TransformStep::Code("unchanged".to_owned()));
    let document = document(vec![steps]);
    let offered = catalog(&[&["people"]]);
    let lowered = lower(&document, &host, &offered).await.expect("lowered");
    let street = ColumnPath::new(["address", "street"]).unwrap();
    let code = host.code("unchanged").expect("registered");
    let expected: Vec<rdlt_engine::TransformStep> = vec![
        Op::rename([("a", "b")]).into(),
        Op::mask([street], Mask::redact("***")).into(),
        Op::mask(["b"], Mask::redact("-")).into(),
        Op::mask(["email"], Mask::Null).into(),
        Op::constant([
            ("source", Literal::Text("pg".to_owned())),
            ("tier", Literal::Integer(2)),
        ])
        .into(),
        code.into(),
    ];
    assert_eq!(lowered.plan.streams()[0].transforms(), expected);
}

/// Checks lowering `document` is refused as a configuration coded `code`, of `stream`.
async fn refused(document: &Document, code: &str, stream: Option<&str>) {
    let refused = lowered(document).await.expect_err("refused");
    assert_eq!(refused.code().map(Code::as_str), Some(code), "{refused:?}");
    assert_eq!(refused.kind(), ErrorKind::Config, "{refused:?}");
    assert_eq!(refused.stream(), stream, "{refused:?}");
}

#[tokio::test(start_paused = true)]
async fn lowering_refuses_a_document_s_mistakes_with_their_codes() {
    let mut versioned = document(vec![Stream::new("a")]);
    versioned.version = 2;
    refused(&versioned, "document_version", None).await;
    let mut unnamed = document(vec![Stream::new("a")]);
    unnamed.pipeline = "not a name".to_owned();
    refused(&unnamed, "reference_invalid", None).await;
    let mut unreferenced = document(vec![Stream::new("a")]);
    unreferenced.destination = Side::new(Connector::new("not an id"));
    refused(&unreferenced, "reference_invalid", None).await;
    let mut uncommitted = document(vec![Stream::new("a")]);
    uncommitted.commit = Some(Commit::new());
    refused(&uncommitted, "commit_policy_invalid", None).await;
    let mut unattempted = document(vec![Stream::new("a")]);
    unattempted.retry = Some(crate::Retry::new().max_attempts(0));
    refused(&unattempted, "config_invalid", None).await;
    let mut unbudgeted = document(vec![Stream::new("a")]);
    unbudgeted.resources = Resources::new().memory(0);
    refused(&unbudgeted, "config_invalid", None).await;
    refused(
        &document(vec![Stream::new("a\u{7}")]),
        "stream_invalid",
        None,
    )
    .await;
}

/// A document whose source is `side`.
#[cfg(feature = "local")]
fn sourced(side: Side) -> Document {
    let mut sourced = document(vec![Stream::new("a")]);
    sourced.source = side;
    sourced
}

#[cfg(feature = "local")]
#[tokio::test(start_paused = true)]
async fn a_document_shares_the_network_only_where_its_host_allows_it() {
    let networked = |connector: Connector| Side::new(connector).access(Access::new().network());
    let spawned = || networked(Connector::new("io.rapidbyte.sqlite"));
    let refusal = |document: Document| async move {
        let prepared = prepare(&document, &Host::new()).await;
        prepared.err().expect("the host has not allowed it")
    };
    let source = refusal(sourced(spawned())).await;
    let mut written = document(vec![Stream::new("a")]);
    written.destination = spawned();
    let destination = refusal(written).await;
    for (refused, origin) in [(source, Origin::Source), (destination, Origin::Destination)] {
        assert_eq!(refused.code().map(Code::as_str), Some("placement_refused"));
        assert_eq!(
            (refused.kind(), refused.origin()),
            (ErrorKind::Config, origin)
        );
        assert!(refused.to_string().contains("allow_network"), "{refused}");
    }
    for host in [Host::new().allow_network(), Host::new().trust_binaries()] {
        assert!(prepare(&sourced(spawned()), &host).await.is_ok());
    }
    let remote =
        networked(Connector::new("io.rapidbyte.sqlite").endpoint("grpcs://db.example:7443"));
    assert!(prepare(&sourced(remote), &Host::new()).await.is_ok());
    let compiled = networked(Connector::new("io.rapidbyte.memory"));
    let host = Host::new().source::<rdlt_connector_reference::MemorySource>();
    assert!(prepare(&sourced(compiled), &host).await.is_ok());
    let quiet = Side::new(Connector::new("io.rapidbyte.sqlite"));
    assert!(prepare(&sourced(quiet), &Host::new()).await.is_ok());
}

#[tokio::test(start_paused = true)]
async fn lowering_refuses_a_stream_s_mistakes_naming_the_stream() {
    let one = |stream: Stream| document(vec![stream]);
    let unknown = Stream::new("a").transform(TransformStep::Code("nowhere".into()));
    refused(&one(unknown), "transform_unknown", Some("a")).await;
    let masked = |mask: MaskStep| one(Stream::new("a").transform(TransformStep::Mask(mask)));
    let hmac = || MaskStep::new(["email"], MaskWith::HmacSha256);
    refused(&masked(hmac()), "mask_key_missing", Some("a")).await;
    refused(
        &masked(hmac().key("the key itself")),
        "mask_key_literal",
        Some("a"),
    )
    .await;
    for (misplaced, said) in [
        (
            hmac().key("k").text("t"),
            "a mask with hmac_sha256 takes no text",
        ),
        (
            MaskStep::new(["e"], MaskWith::Redact).key("k"),
            "a mask with redact takes no key",
        ),
        (
            MaskStep::new(["e"], MaskWith::Null).text("t"),
            "a mask with null takes no text",
        ),
        (
            MaskStep::new(["e"], MaskWith::Null).key("k"),
            "a mask with null takes no key",
        ),
    ] {
        let misplaced = masked(misplaced);
        refused(&misplaced, "document_unknown_key", Some("a")).await;
        let refusal = lowered(&misplaced).await.expect_err("refused");
        assert!(refusal.to_string().contains(said), "{refusal}");
    }
    let empty = Hint::Decimal {
        precision: 0,
        scale: 0,
    };
    let hinted = Stream::new("a").column("p", Column::new().hint(empty));
    refused(&one(hinted), "document_invalid", Some("a")).await;
    let modes = rdlt_engine::PipelinePlan::new(
        rdlt_connector::PipelineId::parse("lowered").unwrap(),
        [rdlt_engine::StreamPlan::new(StreamName::new("a").unwrap())
            .read(Read::Incremental)
            .write(rdlt_engine::WriteMode::Replace)],
    );
    let code = modes.unwrap_err().code().map(Code::as_str).expect("coded");
    let replaced = Stream::new("a")
        .read(ReadMode::Incremental)
        .write(WriteMode::Replace);
    refused(&one(replaced), code, Some("a")).await;
}

#[cfg(feature = "sql")]
#[tokio::test(start_paused = true)]
async fn a_sql_step_lowers_to_a_planned_query_or_is_refused_as_the_engine_codes_it() {
    let query = |text: &str| {
        document(vec![
            Stream::new("a").transform(TransformStep::Sql(text.to_owned())),
        ])
    };
    let planned = lowered(&query("SELECT id FROM input"))
        .await
        .expect("lowered");
    assert_eq!(planned.plan.streams()[0].transforms().len(), 1);
    let refused = lowered(&query("SELECT id FROM input ORDER BY id"))
        .await
        .expect_err("refused");
    assert_eq!(refused.code(), Some(rdlt_engine::codes::TRANSFORM_INVALID));
    assert_eq!(refused.stream(), Some("a"));
}

/// A store of one secret, which counts each lookup.
#[derive(Debug, Default)]
struct Counted(std::sync::Arc<std::sync::atomic::AtomicUsize>);

impl SecretResolver for Counted {
    fn resolve<'a>(
        &'a self,
        _: &'a SecretReference,
    ) -> BoxFuture<'a, Result<Secret<String>, SecretFault>> {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Box::pin(async { Ok(Secret::new("CANARY-mask".to_owned())) })
    }
}

#[tokio::test(start_paused = true)]
async fn a_pattern_lowers_its_transforms_once_and_every_stream_it_selects_shares_them() {
    let looked = std::sync::Arc::default();
    let host = Host::new()
        .secret_store(Counted(std::sync::Arc::clone(&looked)))
        .transform("kept", Op::rename([("x", "y")]));
    let mask = MaskStep::new(["email"], MaskWith::HmacSha256).key("${secret:mask_key}");
    let entry = Stream::new("public.*")
        .transform(TransformStep::Mask(mask))
        .transform(TransformStep::Code("kept".to_owned()));
    let offered = catalog(&[&["public", "a"], &["public", "b"], &["public", "c"]]);
    let lowered = lower(&document(vec![entry]), &host, &offered)
        .await
        .expect("lowered");
    assert_eq!(
        looked.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "one lookup"
    );
    let [a, b, c] = lowered.plan.streams() else {
        panic!("three streams");
    };
    assert_eq!(
        a.transforms(),
        b.transforms(),
        "a code transform is the same, shared"
    );
    assert_eq!(b.transforms(), c.transforms());
}

/// More streams of distinct schemas than a SQL transform keeps plans for: each selected stream
/// has an `Sql` of its own, so none plans its schema twice for another's having pushed it out.
#[cfg(feature = "sql")]
#[tokio::test(start_paused = true)]
async fn each_stream_a_pattern_selects_plans_its_sql_once_for_its_own_schema() {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};

    let query = TransformStep::Sql("SELECT id FROM input".to_owned());
    let entry = Stream::new("public.*").transform(query);
    let tables: Vec<String> = (0..9).map(|table| format!("t{table}")).collect();
    let names: Vec<[&str; 2]> = tables.iter().map(|table| ["public", table]).collect();
    let names: Vec<&[&str]> = names.iter().map(|name| &name[..]).collect();
    let lowered = lower(&document(vec![entry]), &Host::new(), &catalog(&names))
        .await
        .expect("lowered");
    let sql: Vec<Arc<dyn rdlt_engine::Transform>> = lowered
        .plan
        .streams()
        .iter()
        .map(|stream| match stream.transforms() {
            [rdlt_engine::TransformStep::Code(sql)] => Arc::clone(sql),
            _ => panic!("one sql transform"),
        })
        .collect();
    assert_eq!(sql.len(), tables.len());
    let schemas: Vec<Schema> = tables
        .iter()
        .map(|table| {
            let id = Field::new("id", DataType::Int64, false);
            Schema::new(vec![id, Field::new(table, DataType::Utf8, true)])
        })
        .collect();
    let planned = |(sql, schema): (&Arc<dyn rdlt_engine::Transform>, &Schema)| {
        sql.output(schema).expect("planned")
    };
    let first: Vec<_> = sql.iter().zip(&schemas).map(planned).collect();
    let again: Vec<_> = sql.iter().zip(&schemas).map(planned).collect();
    for (first, again) in first.iter().zip(&again) {
        assert!(
            Arc::ptr_eq(first, again),
            "a stream's plan is kept, not made again"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn a_rename_giving_a_column_a_name_another_gives_up_is_refused_however_written() {
    for pairs in [[("a", "b"), ("b", "c")], [("b", "c"), ("a", "b")]] {
        let renames = Stream::new("a").transform(TransformStep::rename(pairs));
        refused(&document(vec![renames]), "transform_invalid", Some("a")).await;
    }
}

/// A secret store that never answers.
#[derive(Debug)]
struct Stalled;

impl SecretResolver for Stalled {
    fn resolve<'a>(
        &'a self,
        _: &'a SecretReference,
    ) -> BoxFuture<'a, Result<Secret<String>, SecretFault>> {
        Box::pin(std::future::pending())
    }
}

#[tokio::test(start_paused = true)]
async fn a_mask_key_s_secret_that_never_resolves_fails_at_the_host_s_wait() {
    let wait = Duration::from_secs(90);
    let host = Host::new().secret_store(Stalled).build_wait(wait);
    let mask = MaskStep::new(["email"], MaskWith::HmacSha256).key("${secret:mask_key}");
    let stream = Stream::new("a").transform(TransformStep::Mask(mask));
    let asking = document(vec![stream]);
    let began = tokio::time::Instant::now();
    let prepared = tokio::time::timeout(Duration::from_hours(1), prepare(&asking, &host)).await;
    let refused = prepared.expect("bounded").err().expect("refused");
    assert_eq!(began.elapsed(), wait);
    assert_eq!(refused.code(), Some(rdlt_engine::codes::DEADLINE_EXCEEDED));
    assert!(refused.is_retryable());
    assert_eq!(refused.stream(), Some("a"));
}

#[tokio::test(start_paused = true)]
async fn a_malformed_endpoint_is_refused_before_either_connector_is_placed() {
    let unusable = || Side::new(Connector::new("io.rapidbyte.memory").endpoint("no endpoint"));
    let mut sourced = document(vec![Stream::new("a")]);
    sourced.source = unusable();
    let mut written = document(vec![Stream::new("a")]);
    written.destination = unusable();
    for (asking, origin) in [(sourced, Origin::Source), (written, Origin::Destination)] {
        let refused = prepare(&asking, &Host::new()).await.err().expect("refused");
        assert_eq!(refused.code().map(Code::as_str), Some("endpoint_invalid"));
        assert_eq!(
            (refused.kind(), refused.origin()),
            (ErrorKind::Config, origin)
        );
    }
}

#[tokio::test(start_paused = true)]
async fn an_empty_include_is_a_restriction_and_an_absent_one_is_none() {
    let read = serde_json::json!({ "name": "a", "include": [] });
    let read = serde_json::from_value::<Stream>(read).expect("it reads");
    let built = Stream::new("a").include_only(Vec::<String>::new());
    for empty in [read, built] {
        refused(&document(vec![empty]), "plan_include_invalid", Some("a")).await;
    }
    let absent = serde_json::from_value::<Stream>(serde_json::json!({ "name": "a" }));
    let absent = document(vec![absent.expect("it reads")]);
    let lowered = lowered(&absent).await.expect("no restriction");
    assert!(lowered.plan.streams()[0].included().is_none());
}

#[tokio::test(start_paused = true)]
async fn a_shown_name_is_held_to_its_parts_limits() {
    let (namespace, name) = ("n".repeat(200), "t".repeat(200));
    let shown = format!("{namespace}.{name}");
    let offered = catalog(&[&[namespace.as_str(), name.as_str()]]);
    let entry = document(vec![Stream::new(shown.clone())]);
    let lowered = lower(&entry, &Host::new(), &offered)
        .await
        .expect("lowered");
    let parted = (Some(namespace.as_str()), name.as_str());
    let stream = lowered.plan.streams()[0].name();
    assert_eq!((stream.namespace(), stream.name()), parted);
    let unlisted = super::unlisted(&shown).expect("one namespace and name");
    assert_eq!((unlisted.namespace(), unlisted.name()), parted);
    let twice = ["n".repeat(100), "m".repeat(100), "t".repeat(100)].join(".");
    let refused_twice = super::unlisted(&twice).expect_err("two namespaces it could end at");
    assert_eq!(
        refused_twice.code().map(Code::as_str),
        Some("stream_ambiguous")
    );
    for unnamed in ["n".repeat(300), format!("{}.t", "n".repeat(300))] {
        refused(
            &document(vec![Stream::new(unnamed)]),
            "stream_invalid",
            None,
        )
        .await;
    }
}

#[tokio::test(start_paused = true)]
async fn a_name_two_catalog_streams_show_as_is_ambiguous_and_a_pattern_matching_none_is_refused() {
    let alike = catalog(&[&["a.b", "c"], &["a", "b.c"]]);
    let named = lower(&document(vec![Stream::new("a.b.c")]), &Host::new(), &alike).await;
    let refused = named.expect_err("two streams show as a.b.c");
    assert_eq!(refused.code().map(Code::as_str), Some("stream_ambiguous"));
    assert_eq!(refused.stream(), Some("a.b.c"));
    let offered = catalog(&[&["public", "orders"]]);
    let typo = lower(
        &document(vec![Stream::new("pubic.*")]),
        &Host::new(),
        &offered,
    )
    .await;
    let refused = typo.expect_err("the pattern matches nothing");
    assert_eq!(refused.code().map(Code::as_str), Some("pattern_unmatched"));
    assert_eq!(refused.stream(), Some("pubic.*"));
    let named = document(vec![Stream::new("public.orders"), Stream::new("public.*")]);
    let lowered = lower(&named, &Host::new(), &offered).await;
    let lowered = lowered.expect("a pattern matching only a named stream selects nothing more");
    assert_eq!(lowered.plan.streams().len(), 1);
}

#[tokio::test(start_paused = true)]
async fn a_merge_without_a_key_matches_by_the_source_s_primary_key() {
    let merged = document(vec![
        Stream::new("a")
            .read(ReadMode::Cdc)
            .write(WriteMode::merge(Vec::<String>::new())),
    ]);
    let lowered = lowered(&merged).await.expect("lowered");
    let stream = &lowered.plan.streams()[0];
    assert_eq!(stream.write_mode(), rdlt_engine::WriteMode::Merge);
    assert_eq!(stream.merge_key(), None);
}

#[cfg(not(feature = "sql"))]
#[tokio::test(start_paused = true)]
async fn a_sql_step_needs_the_sql_feature() {
    let query = Stream::new("a").transform(TransformStep::Sql("SELECT id FROM input".to_owned()));
    let refused = lowered(&document(vec![query]))
        .await
        .expect_err("no SQL in this build");
    assert_eq!(
        refused.code().map(Code::as_str),
        Some("transform_unavailable")
    );
}

#[test]
fn every_mode_of_the_document_is_the_engine_s_mode() {
    use super::settings;

    for (mode, read) in [
        (ReadMode::Full, Read::Full),
        (ReadMode::Incremental, Read::Incremental),
        (ReadMode::Cdc, Read::Cdc),
    ] {
        assert_eq!(settings::read(mode), read);
    }
    for (mode, deletes) in [
        (DeleteMode::Hard, rdlt_engine::DeleteMode::Hard),
        (DeleteMode::Soft, rdlt_engine::DeleteMode::Soft),
        (DeleteMode::Ignore, rdlt_engine::DeleteMode::Ignore),
    ] {
        assert_eq!(settings::deletes(mode), deletes);
    }
    assert_eq!(
        settings::truncates(OnTruncate::Apply),
        rdlt_engine::OnTruncate::Apply
    );
    assert_eq!(
        settings::truncates(OnTruncate::Ignore),
        rdlt_engine::OnTruncate::Ignore
    );
    assert_eq!(
        settings::retention(RetentionLoss::Fail),
        rdlt_engine::RetentionLoss::Fail
    );
    assert_eq!(
        settings::retention(RetentionLoss::Reset),
        rdlt_engine::RetentionLoss::Reset
    );
    assert_eq!(
        settings::until(Until::Exhausted),
        rdlt_engine::Until::Exhausted
    );
    assert_eq!(settings::until(Until::Forever), rdlt_engine::Until::Forever);
}

#[test]
fn every_schema_setting_of_the_document_is_the_engine_s() {
    use super::settings;

    for (policy, engine) in [
        (SchemaPolicy::Evolve, rdlt_engine::SchemaPolicy::Evolve),
        (SchemaPolicy::Freeze, rdlt_engine::SchemaPolicy::Freeze),
        (
            SchemaPolicy::DiscardRow,
            rdlt_engine::SchemaPolicy::DiscardRow,
        ),
        (
            SchemaPolicy::DiscardValue,
            rdlt_engine::SchemaPolicy::DiscardValue,
        ),
    ] {
        let rules = SchemaRules::new().policy(policy);
        assert_eq!(
            settings::schema(Some(&rules), None),
            SchemaSettings::new().policy(engine)
        );
    }
    for (unsupported, engine) in [
        (
            OnUnsupported::VariantColumn,
            rdlt_engine::OnUnsupported::VariantColumn,
        ),
        (OnUnsupported::Refuse, rdlt_engine::OnUnsupported::Refuse),
    ] {
        let rules = SchemaRules::new().on_unsupported(unsupported);
        let expected = SchemaSettings::new().on_unsupported(engine);
        assert_eq!(settings::schema(Some(&rules), None), expected);
    }
    for (nested, engine) in [
        (Nested::Native, rdlt_engine::Nested::Native),
        (Nested::Json, rdlt_engine::Nested::Json),
        (
            Nested::Normalize { max_depth: 2 },
            rdlt_engine::Nested::Normalize { max_depth: 2 },
        ),
    ] {
        assert_eq!(
            settings::schema(None, Some(nested)),
            SchemaSettings::new().nested(engine)
        );
    }
}

#[test]
fn every_hint_of_the_document_is_the_engine_s_type() {
    for (hint, logical) in [
        (Hint::Bool, LogicalType::Bool),
        (Hint::Int8, LogicalType::Int8),
        (Hint::Int16, LogicalType::Int16),
        (Hint::Int32, LogicalType::Int32),
        (Hint::Int64, LogicalType::Int64),
        (Hint::Float32, LogicalType::Float32),
        (Hint::Float64, LogicalType::Float64),
        (Hint::Utf8, LogicalType::Utf8),
        (Hint::Binary, LogicalType::Binary),
        (Hint::Date, LogicalType::Date),
        (Hint::Uuid, LogicalType::Uuid),
        (Hint::Json, LogicalType::Json),
    ] {
        assert_eq!(super::settings::logical(hint).expect("a type"), logical);
    }
}

#[tokio::test(start_paused = true)]
async fn the_engine_s_configuration_takes_every_commit_resource_and_retry_setting() {
    let mut configured = document(vec![Stream::new("a")]);
    configured.commit = Some(Commit::new().bytes(64 << 10));
    configured.resources = Resources::new().memory(1 << 30).writers(2).partitions(4);
    configured.retry = Some(
        crate::Retry::new()
            .max_attempts(3)
            .initial(Duration::from_millis(10))
            .max_delay(Duration::from_secs(1)),
    );
    let set = lowered(&configured).await.expect("lowered");
    let config = &set.config;
    let bytes = config.commit().and_then(rdlt_engine::CommitPolicy::bytes);
    assert_eq!(bytes.map(std::num::NonZeroU64::get), Some(64 << 10));
    assert_eq!(config.memory().get(), 1 << 30);
    assert_eq!(config.writers().map(std::num::NonZeroU16::get), Some(2));
    assert_eq!(config.partitions().get(), 4);
    let retry = rdlt_engine::RetryPolicy::default()
        .max_attempts(3)
        .initial(Duration::from_millis(10))
        .max_delay(Duration::from_secs(1));
    assert_eq!(config.retry(), &retry);
    configured.retry = Some(crate::Retry::new().initial(Duration::from_millis(10)));
    let attempted = lowered(&configured).await.expect("lowered");
    let retry = rdlt_engine::RetryPolicy::default().initial(Duration::from_millis(10));
    assert_eq!(attempted.config.retry(), &retry, "the engine's attempts");
}

#[tokio::test(start_paused = true)]
async fn a_code_transform_the_host_lacks_is_refused_naming_those_it_has() {
    let host = Host::new()
        .transform("kept", Op::rename([("x", "y")]))
        .transform("also", Op::rename([("p", "q")]));
    let unknown = Stream::new("a").transform(TransformStep::Code("nowhere".to_owned()));
    let offered = catalog(&[&["a"]]);
    let refused = lower(&document(vec![unknown]), &host, &offered)
        .await
        .expect_err("refused");
    assert_eq!(refused.code().map(Code::as_str), Some("transform_unknown"));
    let said = refused.to_string();
    assert!(said.contains("also, kept"), "{said}");
}
