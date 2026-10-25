//! The lowering differential: batches of every logical type, in every encoding a source may send,
//! lowered by plans into a table each batch evolves, for any destination's capabilities, against
//! the reference lowering each value on its own.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use arrow_array::Array;
use arrow_array::cast::AsArray;
use arrow_array::{RecordBatch, RecordBatchOptions};
use proptest::prelude::*;
use rdlt_connector::{
    Capabilities, ChangeOp, ColumnPath, LoadId, OP_COLUMN, ReadMode, SEQ_COLUMN, SchemaChanges,
    SchemaVersion, SegmentId, StreamName, TablePath, TableRef, TableSchema, TypeKind,
    UNCHANGED_COLUMN, UnchangedFlags,
};

use rdlt_testkit::canon::{Canon, storage};
use rdlt_testkit::decode;
use rdlt_testkit::drawn::{Drawn, Encoding, KINDS, Scalar, Shape, array, field, neighbors};

use super::reference::{Expected, lower, merged, unsuperseded, written};
use super::{ChangeRows, LoweringPlan, Prepared, Stamp};
use crate::error::{ErrorKind, codes};
use crate::naming::Naming;
use crate::plan::{StreamPlan, WriteMode};
use crate::policy::{Nested, SchemaPolicy, SchemaSettings};
use crate::table::resolve::Route;
use crate::table::{
    ChangeLayout, Incoming, LineageColumns, MetaNames, Model, Resolver, Settings, TableView,
};

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

/// The drawn columns' names, as `rdlt_testkit::drawn` names them.
const NAMES: [&str; 3] = ["a", "b", "c"];
/// The key column every keyed batch carries before its drawn columns.
const KEY: &str = "k";
/// Rows a case draws keys, ops and flags for: four batches of at most five rows.
const ROWS: usize = 24;

/// How the drawn batches' table keeps their rows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    /// Every row appended.
    Append,
    /// Merged by `k`: the last row of each key kept.
    Merge,
    /// A change stream merged by `k`, deletes removing rows.
    Changes,
}

/// What a keyed case pushes beside the drawn columns, a row at a time across its batches: each
/// row's key, op, and unchanged flags over the pushed batch's ordinals, the key's never set.
#[derive(Clone, Debug)]
struct Beside {
    keys: Vec<i64>,
    ops: Vec<ChangeOp>,
    flags: Vec<Option<u16>>,
}

fn mode() -> impl Strategy<Value = Mode> {
    prop_oneof![2 => Just(Mode::Append), 1 => Just(Mode::Merge), 1 => Just(Mode::Changes)]
}

fn beside() -> impl Strategy<Value = Beside> {
    let op = prop_oneof![
        Just(ChangeOp::Insert),
        Just(ChangeOp::Update),
        Just(ChangeOp::Delete)
    ];
    (
        proptest::collection::vec(0..3_i64, ROWS),
        proptest::collection::vec(op, ROWS),
        proptest::collection::vec(proptest::option::of(any::<u16>()), ROWS),
    )
        .prop_map(|(keys, ops, flags)| Beside {
            keys,
            ops,
            // A flag on the key refuses the batch, which the keyed tests hold.
            flags: flags
                .into_iter()
                .map(|flags| flags.map(|flags| flags & !0b10))
                .collect(),
        })
}

/// A policy for each drawn column, or the stream's.
fn overrides() -> impl Strategy<Value = [Option<SchemaPolicy>; 3]> {
    [
        proptest::option::of(policy()),
        proptest::option::of(policy()),
        proptest::option::of(policy()),
    ]
}

fn resolver(
    capabilities: Capabilities,
    (policy, overrides): (SchemaPolicy, &[Option<SchemaPolicy>; 3]),
    nested: Nested,
    mode: Mode,
) -> Resolver {
    let stream = StreamName::new("s").expect("a valid name");
    let naming = Naming::new(capabilities.identifiers.clone());
    let mut plan =
        StreamPlan::new(stream.clone()).schema(SchemaSettings::new().policy(policy).nested(nested));
    for (name, policy) in NAMES.iter().zip(overrides) {
        if let Some(policy) = policy {
            plan = plan.column(*name, SchemaSettings::new().policy(*policy));
        }
    }
    let (plan, meta) = match mode {
        Mode::Append => (
            plan,
            MetaNames::assign(&naming, false, LineageColumns::None),
        ),
        Mode::Merge => (
            plan.write(WriteMode::Merge).key([KEY]),
            MetaNames::assign(&naming, true, LineageColumns::None),
        ),
        Mode::Changes => (
            plan.write(WriteMode::Merge).key([KEY]).read(ReadMode::Cdc),
            MetaNames::assign_changes(
                &naming,
                true,
                LineageColumns::None,
                Some(ChangeLayout::Merge { soft: false }),
            ),
        ),
    };
    let key = plan
        .merge_key()
        .map(<[ColumnPath]>::to_vec)
        .unwrap_or_default();
    Resolver {
        settings: Settings {
            pipeline: SchemaSettings::default(),
            stream: plan,
            key,
            owner: None,
        },
        stream,
        meta,
        naming,
        capabilities: Arc::new(capabilities),
        root: None,
        columns: u64::MAX,
        unwidened: BTreeSet::new(),
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
    let incoming = Incoming::of(schema, paths, std::slice::from_ref(&batch));
    (batch, incoming)
}

/// `drawn` with the key column `k` first, its rows' keys taken from `keys`.
fn keyed((columns, rows): &Drawn, keys: &[i64]) -> Drawn {
    let key = Shape {
        logical: rdlt_connector::LogicalType::Int64,
        encoding: Encoding::Plain,
        children: Vec::new(),
    };
    let columns = std::iter::once((KEY.to_owned(), key)).chain(columns.iter().cloned());
    let rows = rows.iter().zip(keys).map(|(row, key)| {
        std::iter::once(Scalar::Int(*key))
            .chain(row.iter().cloned())
            .collect()
    });
    (columns.collect(), rows.collect())
}

/// `data`, a keyed batch, pushed as a change batch: its op, `k`, its sequence, the drawn
/// columns and its unchanged flags, each row's op and flags from `beside` from `first` on.
fn pushed(data: &RecordBatch, beside: &Beside, first: usize) -> RecordBatch {
    use arrow_array::{ArrayRef, BinaryArray, FixedSizeBinaryArray, Int8Array};
    let rows = data.num_rows();
    let ops = Int8Array::from_iter_values((first..first + rows).map(|row| beside.ops[row].code()));
    let seqs = (first..first + rows).map(|row| Some(u128::try_from(row).unwrap().to_be_bytes()));
    let seqs = FixedSizeBinaryArray::try_from_sparse_iter_with_size(seqs, 16).unwrap();
    let flags: BinaryArray = (first..first + rows)
        .map(|row| beside.flags[row].map(u16::to_le_bytes))
        .collect();
    let schema = data.schema();
    let field = |name: &str, array: &dyn Array| {
        Arc::new(arrow_schema::Field::new(
            name,
            array.data_type().clone(),
            true,
        ))
    };
    let mut fields = vec![
        field(OP_COLUMN, &ops),
        Arc::clone(&schema.fields()[0]),
        field(SEQ_COLUMN, &seqs),
    ];
    fields.extend(schema.fields().iter().skip(1).cloned());
    fields.push(field(UNCHANGED_COLUMN, &flags));
    let mut columns: Vec<ArrayRef> =
        vec![Arc::new(ops), Arc::clone(data.column(0)), Arc::new(seqs)];
    columns.extend(data.columns().iter().skip(1).cloned());
    columns.push(Arc::new(flags));
    let options = RecordBatchOptions::new().with_row_count(Some(rows));
    RecordBatch::try_new_with_options(
        Arc::new(arrow_schema::Schema::new(fields)),
        columns,
        &options,
    )
    .unwrap()
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
        epoch: rdlt_connector::Epoch(1),
        load_id: LoadId::from_parts(UNIX_EPOCH + Duration::from_secs(1_000), 1),
        loaded_at: UNIX_EPOCH + Duration::from_secs(1_000),
        received_at: UNIX_EPOCH + Duration::from_secs(1_000),
        segment: SegmentId(1),
        first_row: 0,
    }
}

/// Lowers each of `batches` into the table they evolve, checking each against the reference.
fn check(
    capabilities: &Capabilities,
    policies: (SchemaPolicy, &[Option<SchemaPolicy>; 3]),
    nested: Nested,
    (mode, beside): (Mode, &Beside),
    batches: &[Drawn],
) -> Result<(), TestCaseError> {
    let resolver = resolver(capabilities.clone(), policies, nested, mode);
    let native = nested == Nested::Native;
    let table = TableRef {
        path: TablePath::new(["t"]).expect("a valid path"),
        name: "t".into(),
        version: SchemaVersion(1),
        generation: None,
        merge: None,
    };
    let mut model = Model::default();
    let mut first = 0;
    for drawn in batches {
        let drawn = match mode {
            Mode::Append => drawn.clone(),
            Mode::Merge | Mode::Changes => keyed(drawn, &beside.keys[first..]),
        };
        let (batch, incoming) = batch(&drawn);
        let resolution = match resolver.resolve(&model, &incoming) {
            Ok(resolution) => resolution,
            // A destination that cannot change a column refuses the change; nothing lowers.
            Err(error) if error.kind() == ErrorKind::Schema => return Ok(()),
            Err(error) => panic!("resolving: {error:?}"),
        };
        let view =
            Arc::new(TableView::new(&table, resolution.model.clone(), &resolver).expect("a view"));
        for (column, lowered) in view.model.columns.iter().zip(&view.lowered) {
            prop_assert_eq!(
                lowered,
                &storage(column.logical_type(), native, capabilities)
            );
        }
        let plan = (&resolver, &view, incoming, resolution.routes);
        let case = (mode, beside, first);
        let Some((prepared, expected)) = prepared(plan, &drawn, &batch, case)? else {
            // The batch is refused, as the reference refuses it.
            return Ok(());
        };
        compare(
            &view,
            &prepared,
            &expected,
            case,
            (drawn.1.len(), drawn.0.len()),
        )?;
        model = resolution.model;
        first += drawn.1.len();
    }
    Ok(())
}

/// `batch`, of `drawn`, prepared by the plan lowering it into `view`, each of its columns
/// following its policy, and what the reference expects; `None` where the plan refuses the
/// batch, as the reference does.
///
/// A change stream's batch is pushed with its rows' ops and flags from `beside`, the first its
/// row `first`'s.
fn prepared(
    (resolver, view, incoming, routes): (&Resolver, &Arc<TableView>, Incoming, Vec<Route>),
    drawn: &Drawn,
    batch: &RecordBatch,
    (mode, beside, first): (Mode, &Beside, usize),
) -> Result<Option<(Prepared, Expected)>, TestCaseError> {
    let each: Vec<SchemaPolicy> = incoming
        .paths
        .iter()
        .map(|path| resolver.settings.column(path).policy)
        .collect();
    let plan = LoweringPlan::new(resolver.stream.clone(), Arc::clone(view), incoming, routes)
        .with_policies(each.clone());
    let columns: Vec<_> = drawn
        .0
        .iter()
        .map(|(name, shape)| (name.clone(), shape.logical.clone()))
        .collect();
    let expected = lower(view, &each, &columns, &drawn.1);
    let prepared = match mode {
        Mode::Changes => {
            let (data, changes) =
                ChangeRows::split(&pushed(batch, beside, first)).expect("a change batch");
            prop_assert_eq!(data.columns(), batch.columns());
            plan.prepare(data, None, &stamp(), Some(changes))
        }
        Mode::Append | Mode::Merge => plan.prepare(batch.clone(), None, &stamp(), None),
    };
    match prepared {
        Ok(prepared) => {
            prop_assert!(!expected.refused, "held a value its column cannot hold");
            Ok(Some((prepared, expected)))
        }
        Err(error) if error.code() == Some(codes::VALUE_UNREPRESENTABLE) => {
            prop_assert!(
                expected.refused,
                "refused a batch the table holds: {error:?}"
            );
            Ok(None)
        }
        Err(error) => panic!("lowering: {error:?}"),
    }
}

/// Checks `prepared` against `expected`, for a batch of `rows` rows and `columns` columns, the
/// key among them where it has one: what the policies discard, the rows the table's `mode`
/// keeps of those, their values and, for a change stream, their unchanged flags, the rows'
/// keys, ops and flags those of `beside` from `first` on.
fn compare(
    view: &TableView,
    prepared: &Prepared,
    expected: &Expected,
    (mode, beside, first): (Mode, &Beside, usize),
    (rows, columns): (usize, usize),
) -> Result<(), TestCaseError> {
    prop_assert_eq!(
        (prepared.discarded_rows, prepared.discarded_values),
        (expected.discarded_rows, expected.discarded_values)
    );
    let keys = &beside.keys[first..];
    let ops = &beside.ops[first..];
    let flags: Vec<Option<BTreeSet<usize>>> = (0..rows)
        .map(|row| written(beside.flags[first + row], columns - 1, &expected.written))
        .collect();
    let rows = match mode {
        Mode::Append => expected.kept.clone(),
        Mode::Merge => merged(&expected.kept, keys),
        Mode::Changes => unsuperseded(&expected.kept, keys, ops, &flags),
    };
    let at = |row: &usize| {
        let at = expected.kept.iter().position(|kept| kept == row);
        at.expect("a merge keeps only rows the policies keep")
    };
    let cells: Vec<Vec<Canon>> = rows
        .iter()
        .map(|row| expected.rows[at(row)].clone())
        .collect();
    prop_assert_eq!(decoded(view, &prepared.batch, &expected.sources), cells);
    if mode == Mode::Changes {
        let names = view.meta.changes.as_ref().expect("a change stream's table");
        let stored = prepared
            .batch
            .column_by_name(&names.unchanged)
            .expect("a change stream's table has its flags");
        let stored = stored.as_binary::<i32>();
        let found: Vec<Option<BTreeSet<usize>>> = (0..stored.len())
            .map(|row| {
                stored
                    .is_valid(row)
                    .then(|| UnchangedFlags::new(stored.value(row)).ordinals().collect())
            })
            .collect();
        let wanted: Vec<_> = rows.iter().map(|row| flags[*row].clone()).collect();
        prop_assert_eq!(found, wanted);
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(rdlt_testkit::cases(1024)))]

    #[test]
    fn a_plan_lowers_each_value_as_the_reference_does(
        capabilities in capabilities(),
        policy in policy(),
        overrides in overrides(),
        nested in prop_oneof![Just(Nested::Native), Just(Nested::Json)],
        mode in mode(),
        beside in beside(),
        batches in neighbors::batches(),
    ) {
        check(&capabilities, (policy, &overrides), nested, (mode, &beside), &batches)?;
    }
}
