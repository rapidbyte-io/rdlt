//! The lowering differential: batches of every logical type, in every encoding a source may send,
//! lowered by plans into a table each batch evolves, for any destination's capabilities, against
//! the reference lowering each value on its own.

mod inputs;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use arrow_array::cast::AsArray;
use arrow_array::{Array, RecordBatch};
use proptest::prelude::*;
use rdlt_connector::{Capabilities, SchemaVersion, SegmentId, TablePath, TableRef, UnchangedFlags};
use rdlt_testkit::canon::{Canon, storage};
use rdlt_testkit::decode;
use rdlt_testkit::drawn::{Drawn, neighbors};

use super::reference::{Expected, lower, merged, unsuperseded, written};
use super::{ChangeRows, LoweringPlan, Prepared, Stamp};
use crate::error::{ErrorKind, codes};
use crate::policy::{Nested, SchemaPolicy};
use crate::report::DiscardReason;
use crate::table::resolve::Route;
use crate::table::{Incoming, Model, Resolver, TableView};
use inputs::{
    Beside, Mode, batch, beside, capabilities, keyed, mode, overrides, policy, pushed, resolver,
};

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
    let discards = prepared.discards.reported();
    prop_assert!(
        discards
            .listed
            .iter()
            .all(|discard| *discard.table == *view.table.name)
    );
    let by_column: BTreeMap<(String, DiscardReason), (u64, u64)> = discards
        .listed
        .into_iter()
        .map(|discard| {
            (
                (discard.column, discard.reason),
                (discard.rows, discard.values),
            )
        })
        .collect();
    prop_assert_eq!(&by_column, &expected.discards);
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
