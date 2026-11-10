use rdlt_connector::{
    Catalog, ConnectorError, Cursor, Field, Fields, HostRefusal, LogicalType, Partition,
    PartitionId, PartitionPlan, StreamName, StreamSpec, TableSchema,
};

use super::{NAMED_BY_SECRET, catalog, plan};
use crate::secrets::Redactions;

const SECRET: &str = "hunter2-secret";

fn cursor() -> Cursor {
    Cursor::new(1, b"at").expect("a valid cursor")
}

fn redactions() -> Redactions {
    let redactions = Redactions::new();
    redactions.add(SECRET);
    redactions
}

/// A catalog of one stream, `stream` in `namespace`, with a field `column` holding a struct
/// with a list of `nested`, keyed and timed by `column`.
fn offered(namespace: &str, stream: &str, column: &str, nested: &str) -> Catalog {
    let item = Field::new(nested, LogicalType::Int64, true);
    let within = Fields::new(vec![Field::new(
        "items",
        LogicalType::List(Box::new(item)),
        true,
    )])
    .expect("distinct names");
    let schema = TableSchema::new(vec![
        Field::new(column, LogicalType::Int64, false),
        Field::new("record", LogicalType::Struct(within), true),
    ])
    .expect("a valid schema");
    let name = StreamName::with_namespace(namespace, stream).expect("a valid name");
    let spec = StreamSpec::new(name)
        .with_schema(schema)
        .with_primary_key([column])
        .with_change_time(column);
    Catalog::new(vec![spec]).expect("a valid catalog")
}

/// Asserts `error` is the host's refusal: a configuration error, coded as the host's and not the
/// connector's, saying nothing of the secret.
fn assert_refused(error: &ConnectorError) {
    let refusal = HostRefusal::of(error).map(|refusal| refusal.code());
    assert_eq!((refusal, error.code()), (Some(NAMED_BY_SECRET), None));
    assert_eq!(error.kind(), rdlt_connector::ConnectorErrorKind::Config);
    assert!(!error.to_string().contains(SECRET), "{error}");
}

#[test]
fn a_catalog_with_a_name_that_contains_a_secret_is_refused_and_says_nothing_of_it() {
    let named = format!("orders_{SECRET}");
    for refused in [
        offered(&named, "orders", "id", "n"),
        offered("public", &named, "id", "n"),
        offered("public", "orders", &named, "n"),
        offered("public", "orders", "id", &named),
    ] {
        let error = catalog(refused, &redactions()).expect_err("refused");
        assert_refused(&error);
    }
}

#[test]
fn a_catalog_and_a_plan_whose_names_contain_no_secret_pass_as_they_are() {
    let offered = offered("public", "orders", "id", "n");
    let passed = catalog(offered.clone(), &redactions()).expect("passed");
    assert_eq!(passed, offered);
    let partition = PartitionId::parse("p0").expect("a valid id");
    let planned =
        PartitionPlan::new(vec![Partition::new(partition.clone())]).start(partition, cursor());
    let passed = plan(planned.clone(), &redactions()).expect("passed");
    assert_eq!(passed, planned);
}

#[test]
fn a_plan_with_a_partition_id_that_contains_a_secret_is_refused() {
    let named = PartitionId::parse(format!("p-{SECRET}")).expect("a valid id");
    let other = PartitionId::parse("p0").expect("a valid id");
    let planned = PartitionPlan::new(vec![Partition::new(named.clone())]);
    let started = PartitionPlan::new(vec![Partition::new(other)]).start(named, cursor());
    for refused in [planned, started] {
        let error = plan(refused, &redactions()).expect_err("refused");
        assert_refused(&error);
    }
}
