//! The lowering differential (spec §20.4): batches of every logical type, in every encoding a
//! source may send, lowered by plans into a table each batch evolves, for any destination's
//! capabilities, against the reference lowering each value on its own.

mod decode;

use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use arrow_array::{RecordBatch, RecordBatchOptions};
use proptest::prelude::*;
use rdlt_connector::{
    Capabilities, ColumnPath, LoadId, SchemaChanges, SchemaVersion, SegmentId, StreamName,
    TablePath, TableRef, TableSchema, TypeKind,
};

use super::reference::{Canon, lower, storage};
use super::{LoweringPlan, Stamp};
use crate::drawn::{Drawn, Encoding, Scalar, Shape, array, field, neighbors, values};
use crate::error::ErrorKind;
use crate::naming::Naming;
use crate::plan::StreamPlan;
use crate::policy::{Nested, SchemaPolicy, SchemaSettings};
use crate::table::{Incoming, LineageColumns, MetaNames, Model, Resolver, Settings, TableView};

/// Every kind of value a destination may store natively.
const KINDS: [TypeKind; 19] = [
    TypeKind::Null,
    TypeKind::Bool,
    TypeKind::Int8,
    TypeKind::Int16,
    TypeKind::Int32,
    TypeKind::Int64,
    TypeKind::Float32,
    TypeKind::Float64,
    TypeKind::Decimal,
    TypeKind::Utf8,
    TypeKind::Binary,
    TypeKind::Date,
    TypeKind::Time,
    TypeKind::Timestamp,
    TypeKind::Duration,
    TypeKind::Uuid,
    TypeKind::Json,
    TypeKind::Struct,
    TypeKind::List,
];

/// A destination's capabilities: any set of native types, text always among them, any nested
/// support, and every schema change or only added columns.
fn capabilities() -> impl Strategy<Value = Capabilities> {
    (
        proptest::collection::vec(any::<bool>(), KINDS.len()),
        any::<[bool; 4]>(),
    )
        .prop_map(|(kinds, [structs, lists, json, widens])| {
            let mut capabilities = Capabilities::minimal();
            capabilities.types = KINDS
                .iter()
                .zip(kinds)
                .filter(|(_, stored)| *stored)
                .map(|(kind, _)| *kind)
                .collect();
            capabilities.types.insert(TypeKind::Utf8);
            capabilities.nested.structs = structs;
            capabilities.nested.lists = lists;
            capabilities.nested.json = json;
            if widens {
                capabilities.schema_changes = SchemaChanges::all();
            }
            capabilities
        })
}

fn policy() -> impl Strategy<Value = SchemaPolicy> {
    prop_oneof![
        3 => Just(SchemaPolicy::Evolve),
        1 => Just(SchemaPolicy::DiscardValue),
        1 => Just(SchemaPolicy::DiscardRow),
    ]
}

fn resolver(capabilities: Capabilities, policy: SchemaPolicy, nested: Nested) -> Resolver {
    let stream = StreamName::new("s").expect("a valid name");
    let naming = Naming::new(capabilities.identifiers.clone());
    Resolver {
        settings: Settings {
            pipeline: SchemaSettings::default(),
            stream: StreamPlan::new(stream.clone())
                .schema(SchemaSettings::new().policy(policy).nested(nested)),
            key: Vec::new(),
        },
        stream,
        meta: MetaNames::assign(&naming, false, LineageColumns::None).expect("metadata names"),
        naming,
        capabilities: Arc::new(capabilities),
        root: None,
    }
}

/// `drawn` as a batch, its incoming schema, and its columns' names and logical types.
fn batch((columns, rows): &Drawn) -> (RecordBatch, Incoming) {
    let arrays: Vec<_> = columns
        .iter()
        .enumerate()
        .map(|(column, (_, shape))| {
            let values: Vec<&Scalar> = rows.iter().map(|row| &row[column]).collect();
            array(shape, &values)
        })
        .collect();
    let fields: Vec<_> = columns
        .iter()
        .zip(&arrays)
        .map(|((name, shape), array)| field(name, shape, array, true))
        .collect();
    let schema = Arc::new(arrow_schema::Schema::new(fields));
    let options = RecordBatchOptions::new().with_row_count(Some(rows.len()));
    let batch = RecordBatch::try_new_with_options(Arc::clone(&schema), arrays, &options)
        .expect("the drawn batch is valid");
    let schema = TableSchema::from_arrow(&schema).expect("every drawn encoding has a logical type");
    for (field, (name, shape)) in schema.fields().iter().zip(columns) {
        assert_eq!(
            field.logical_type(),
            &shape.logical,
            "column {name}'s encoding"
        );
    }
    let paths = columns
        .iter()
        .map(|(name, _)| ColumnPath::from(name.as_str()))
        .collect();
    (batch, Incoming { schema, paths })
}

/// Each row of `prepared`, lowered into `view`, read back.
fn decoded(
    view: &TableView,
    prepared: &RecordBatch,
    sources: &[Option<rdlt_connector::LogicalType>],
) -> Vec<Vec<Canon>> {
    for (column, lowered) in view.lowered.iter().enumerate() {
        assert_eq!(
            prepared.column(column).data_type(),
            &lowered.to_arrow(),
            "column {column} is stored as its lowered type"
        );
    }
    (0..prepared.num_rows())
        .map(|row| {
            (0..view.model.columns.len())
                .map(|column| {
                    let logical = view.model.columns[column].logical_type();
                    decode::cell(
                        prepared.column(column).as_ref(),
                        row,
                        logical,
                        &view.lowered[column],
                        &decode::hint(logical, sources[column].as_ref()),
                    )
                })
                .collect()
        })
        .collect()
}

fn stamp() -> Stamp {
    Stamp {
        load_id: LoadId::from_parts(UNIX_EPOCH + Duration::from_secs(1_000), 1),
        loaded_at: UNIX_EPOCH + Duration::from_secs(1_000),
        segment: SegmentId(1),
        first_row: 0,
    }
}

/// Lowers each of `batches` into the table they evolve, checking each against the reference.
fn check(
    capabilities: &Capabilities,
    policy: SchemaPolicy,
    nested: Nested,
    batches: &[Drawn],
) -> Result<(), TestCaseError> {
    let resolver = resolver(capabilities.clone(), policy, nested);
    let table = TableRef {
        path: TablePath::new(["t"]).expect("a valid path"),
        name: "t".into(),
        version: SchemaVersion(1),
        generation: None,
        merge: None,
    };
    let mut model = Model::default();
    for drawn in batches {
        let (batch, incoming) = batch(drawn);
        let resolution = match resolver.resolve(&model, &incoming) {
            Ok(resolution) => resolution,
            // A destination that cannot change a column refuses the change; nothing lowers.
            Err(error) if error.kind() == ErrorKind::Schema => return Ok(()),
            Err(error) => panic!("resolving: {error:?}"),
        };
        let view = Arc::new(TableView::new(&table, resolution.model.clone(), &resolver));
        for (column, lowered) in view.model.columns.iter().zip(&view.lowered) {
            prop_assert_eq!(
                lowered,
                &storage(column.logical_type(), nested, capabilities)
            );
        }
        let plan = LoweringPlan::new(
            resolver.stream.clone(),
            Arc::clone(&view),
            incoming,
            resolution.routes,
        );
        let columns: Vec<_> = drawn
            .0
            .iter()
            .map(|(name, shape)| (name.clone(), shape.logical.clone()))
            .collect();
        let expected = lower(&view, policy, &columns, &drawn.1);
        let prepared = match plan.prepare(&batch, None, &stamp()) {
            Ok(prepared) => prepared,
            Err(error) if error.code() == Some("value_unrepresentable") => {
                prop_assert!(
                    expected.refused,
                    "refused a batch the table holds: {error:?}"
                );
                return Ok(());
            }
            Err(error) => panic!("lowering: {error:?}"),
        };
        prop_assert!(!expected.refused, "held a value its column cannot hold");
        prop_assert_eq!(
            decoded(&view, &prepared.batch, &expected.sources),
            expected.rows
        );
        prop_assert_eq!(
            (prepared.discarded_rows, prepared.discarded_values),
            (expected.discarded_rows, expected.discarded_values)
        );
        model = resolution.model;
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(crate::drawn::cases(1024)))]

    #[test]
    fn a_plan_lowers_each_value_as_the_reference_does(
        capabilities in capabilities(),
        policy in policy(),
        nested in prop_oneof![Just(Nested::Native), Just(Nested::Json)],
        batches in neighbors::batches(),
    ) {
        check(&capabilities, policy, nested, &batches)?;
    }
}

/// Every type and encoding the shape of a column or anything inside it takes.
fn seen(shape: &Shape, into: &mut std::collections::BTreeSet<(TypeKind, String)>) {
    let encoding = match shape.encoding {
        Encoding::FixedSize(_) => "FixedSize".to_owned(),
        other => format!("{other:?}"),
    };
    into.insert((shape.logical.kind(), encoding));
    for child in &shape.children {
        seen(child, into);
    }
}

#[test]
fn the_drawn_shapes_hold_every_type_in_every_encoding() {
    use proptest::strategy::ValueTree;
    use proptest::test_runner::TestRunner;
    let mut runner = TestRunner::deterministic();
    let strategy = values::shape(2);
    let mut drawn = std::collections::BTreeSet::new();
    for _ in 0..20_000 {
        let shape = strategy.new_tree(&mut runner).expect("a shape").current();
        seen(&shape, &mut drawn);
    }
    let mut expected = std::collections::BTreeSet::new();
    let every = |kind: TypeKind, encodings: &[&'static str]| -> Vec<(TypeKind, String)> {
        ["Plain", "Dictionary", "RunEnd"]
            .iter()
            .chain(encodings)
            .map(|encoding| (kind, (*encoding).to_owned()))
            .collect()
    };
    for kind in KINDS {
        let extra: &[&'static str] = match kind {
            TypeKind::Int16 | TypeKind::Int32 | TypeKind::Int64 => &["Unsigned"],
            TypeKind::Float32 => &["Half"],
            TypeKind::Decimal => &["Unsigned", "Decimal32", "Decimal64", "Decimal256"],
            TypeKind::Utf8 | TypeKind::Json => &["Large", "View"],
            TypeKind::Binary => &["Large", "View", "FixedSize"],
            TypeKind::Date => &["Date64"],
            _ => &[],
        };
        match kind {
            TypeKind::Struct => {
                expected.insert((kind, "Plain".to_owned()));
            }
            TypeKind::List => expected.extend(
                ["Plain", "Large", "View", "LargeView", "FixedSize", "Map"]
                    .map(|encoding| (kind, encoding.to_owned())),
            ),
            _ => expected.extend(every(kind, extra)),
        }
    }
    let missing: Vec<_> = expected.difference(&drawn).collect();
    assert!(missing.is_empty(), "never drawn: {missing:?}");
}
