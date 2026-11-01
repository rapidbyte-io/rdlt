//! What the lowering differential draws: a destination's capabilities, the stream's policies, how
//! its table keeps rows and what a keyed case pushes beside them, and the drawn batches as a
//! source sends them.

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow_array::{Array, RecordBatch, RecordBatchOptions};
use proptest::prelude::*;
use rdlt_connector::{
    Capabilities, ChangeOp, ColumnPath, OP_COLUMN, ReadMode, SEQ_COLUMN, SchemaChanges, StreamName,
    TableSchema, TypeKind, UNCHANGED_COLUMN,
};
use rdlt_testkit::drawn::{Drawn, Encoding, KINDS, Scalar, Shape, array, field};

use crate::naming::Naming;
use crate::plan::{StreamPlan, WriteMode};
use crate::policy::{Nested, SchemaPolicy, SchemaSettings};
use crate::table::{ChangeLayout, Incoming, LineageColumns, MetaNames, Resolver, Settings};

/// A destination's capabilities: any set of native types, text always among them, any nested
/// support, and every schema change or only added columns.
pub(super) fn capabilities() -> impl Strategy<Value = Capabilities> {
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

pub(super) fn policy() -> impl Strategy<Value = SchemaPolicy> {
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
pub(super) enum Mode {
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
pub(super) struct Beside {
    pub(super) keys: Vec<i64>,
    pub(super) ops: Vec<ChangeOp>,
    pub(super) flags: Vec<Option<u16>>,
}

pub(super) fn mode() -> impl Strategy<Value = Mode> {
    prop_oneof![2 => Just(Mode::Append), 1 => Just(Mode::Merge), 1 => Just(Mode::Changes)]
}

pub(super) fn beside() -> impl Strategy<Value = Beside> {
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
pub(super) fn overrides() -> impl Strategy<Value = [Option<SchemaPolicy>; 3]> {
    [
        proptest::option::of(policy()),
        proptest::option::of(policy()),
        proptest::option::of(policy()),
    ]
}

pub(super) fn resolver(
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
pub(super) fn batch((columns, rows): &Drawn) -> (RecordBatch, Incoming) {
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
    let incoming = Incoming::of(Arc::new(schema), paths, std::slice::from_ref(&batch));
    (batch, incoming)
}

/// `drawn` with the key column `k` first, its rows' keys taken from `keys`.
pub(super) fn keyed((columns, rows): &Drawn, keys: &[i64]) -> Drawn {
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
pub(super) fn pushed(data: &RecordBatch, beside: &Beside, first: usize) -> RecordBatch {
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
