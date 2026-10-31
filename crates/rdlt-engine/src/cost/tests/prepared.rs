//! Preparing a batch in every mode, a merge's compaction, a history table's hashes, a change
//! stream's flags and a child table's lineage among them, allocates no more at its peak than the
//! write path reserved for lowering it.

use std::sync::Arc;
use std::time::UNIX_EPOCH;

use arrow_array::types::Int64Type;
use arrow_array::{ArrayRef, Int32Array, ListArray, RecordBatch, UInt32Array};
use rdlt_connector::{
    Capabilities, Epoch, RootKey, SchemaVersion, SegmentId, TablePath, TableRef, TableSchema,
    TypeKind,
};

use super::lowering::{SLACK, peak};
use crate::fixtures::lowering::{Case, LoweringCase, Storage};
use crate::normalize::{Lineage, Parent, Part, PartSchemas, Shape, normalize};
use crate::table::{Incoming, LineageColumns, LoweringPlan, MetaNames, Model, Stamp, TableView};

/// Rows of each case's batch: enough that what an array takes beside its values is little of it.
const ROWS: u32 = 2_048;

#[test]
fn preparing_a_batch_in_any_mode_allocates_no_more_than_its_charge() {
    let mut beyond = Vec::new();
    for case in LoweringCase::ALL {
        for storage in [Storage::Native, Storage::Text] {
            let built = Case::stored(case, ROWS, storage);
            let charge = built.charge();
            let (prepared, peak) = peak(|| built.prepare());
            let prepared = prepared.unwrap();
            assert_eq!(prepared.batch.num_rows(), built.kept().rows, "{case:?}");
            if peak > charge + SLACK {
                beyond.push(format!(
                    "{} stored {storage:?}: charged {charge}, allocated {peak}",
                    case.name()
                ));
            }
        }
    }
    assert!(beyond.is_empty(), "{}", beyond.join("\n"));
}

#[test]
fn preparing_a_merge_lacking_most_of_its_tables_columns_allocates_no_more_than_its_charge() {
    let mut beyond = Vec::new();
    for storage in [Storage::Native, Storage::Text] {
        let built = Case::sparse_merge(ROWS, 500, storage);
        let charge = built.charge();
        let (prepared, peak) = peak(|| built.prepare());
        assert_eq!(prepared.unwrap().batch.num_rows(), built.kept().rows);
        if peak > charge + SLACK {
            beyond.push(format!(
                "stored {storage:?}: charged {charge}, allocated {peak}"
            ));
        }
    }
    assert!(beyond.is_empty(), "{}", beyond.join("\n"));
}

/// The plan lowering `part`, a child table's, into a table of a destination that stores what
/// `capabilities` say.
fn child_plan(part: &Part, capabilities: &Capabilities) -> LoweringPlan {
    child_plan_of(part, capabilities, None)
}

/// [`child_plan`], of a merge stream whose table `root` names where it is given.
fn child_plan_of(part: &Part, capabilities: &Capabilities, root: Option<RootKey>) -> LoweringPlan {
    let mut resolver = crate::table::testing::resolver("events");
    resolver.capabilities = Arc::new(capabilities.clone());
    resolver.meta = MetaNames::assign(&resolver.naming, root.is_some(), LineageColumns::Child);
    resolver.root = root;
    let incoming = Incoming::declared(TableSchema::from_arrow(&part.batch.schema()).unwrap());
    let resolution = resolver.resolve(&Model::default(), &incoming).unwrap();
    let table = TableRef {
        path: TablePath::new(["events", "items"]).unwrap(),
        name: "events__items".into(),
        version: SchemaVersion(0),
        generation: None,
        merge: None,
    };
    let view = Arc::new(TableView::new(&table, resolution.model, &resolver).unwrap());
    LoweringPlan::new(resolver.stream.clone(), view, incoming, resolution.routes)
}

/// The lineage of `rows` child rows whose every part and position is at its widest, as are their
/// parents' and roots'.
fn widest(rows: usize) -> Lineage {
    let positions = || -> ArrayRef {
        let count = u32::try_from(rows).unwrap();
        Arc::new(UInt32Array::from_iter_values(
            (0..count).map(|row| u32::MAX - row),
        ))
    };
    Lineage {
        part: u32::MAX,
        row: positions(),
        keys: None,
        last: None,
        parent: Some(Parent {
            path: vec![Arc::from("items")],
            part: u32::MAX - 1,
            row: positions(),
            root_row: positions(),
            idx: Arc::new(Int32Array::from_iter_values(
                0..i32::try_from(rows).unwrap(),
            )),
        }),
    }
}

#[test]
fn preparing_a_child_tables_lineage_at_the_widest_stamp_allocates_no_more_than_its_charge() {
    let rows = i64::from(ROWS);
    let lists = (0..rows).map(|row| Some(vec![Some(row)]));
    let lists: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(lists));
    let batch = RecordBatch::try_from_iter([("items", lists)]).unwrap();
    let shape = Shape {
        max_depth: 8,
        whole: std::collections::BTreeSet::new(),
        key: Vec::new(),
    };
    let parts = normalize(&batch, &shape, &mut PartSchemas::default()).unwrap();
    let part = &parts[1];
    // Every number of each id at its widest: 37 bytes an id.
    let lineage = widest(part.batch.num_rows());
    // The widest epoch is the widest a load's `Int64` epoch column holds.
    let stamp = Stamp {
        epoch: Epoch(i64::MAX.unsigned_abs()),
        loaded_at: UNIX_EPOCH,
        received_at: UNIX_EPOCH,
        segment: SegmentId(u64::MAX),
        first_row: u64::MAX,
    };
    let mut beyond = Vec::new();
    for bytes in [true, false] {
        let mut capabilities = Capabilities::minimal();
        if !bytes {
            capabilities.types.remove(&TypeKind::Binary);
        }
        let plan = child_plan(part, &capabilities);
        let rendering = crate::cost::rendering(&capabilities);
        let stored = plan.stored();
        let mut measure = rendering.lowering(&part.batch, &stored, plan.row_bytes(), u64::MAX);
        let charge = measure.expanded(0..part.batch.num_rows());
        let (prepared, peak) =
            peak(|| plan.prepare(part.batch.clone(), Some(&lineage), &stamp, None));
        assert_eq!(prepared.unwrap().batch.num_rows(), part.batch.num_rows());
        if peak > charge + SLACK {
            beyond.push(format!(
                "bytes stored as bytes {bytes}: charged {charge}, allocated {peak}"
            ));
        }
    }
    assert!(beyond.is_empty(), "{}", beyond.join("\n"));
}

#[test]
fn preparing_a_merge_child_tables_rows_of_the_roots_found_last_allocates_no_more_than_its_charge() {
    let rows = i64::from(ROWS);
    let lists = (0..rows).map(|row| Some(vec![Some(row), Some(row)]));
    let lists: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(lists));
    let batch = RecordBatch::try_from_iter([("items", lists)]).unwrap();
    let shape = Shape {
        max_depth: 8,
        whole: std::collections::BTreeSet::new(),
        key: Vec::new(),
    };
    let parts = normalize(&batch, &shape, &mut PartSchemas::default()).unwrap();
    let part = &parts[1];
    let mut lineage = part.lineage.clone();
    // Every other root is the last of its key: half the items stay.
    lineage.keys = Some(Arc::new(arrow_array::BinaryArray::from_iter_values(
        (0..ROWS).map(u32::to_be_bytes),
    )));
    lineage.last = Some((0..ROWS).map(|root| root % 2 == 1).collect());
    let root = RootKey {
        table: "events".into(),
        id: "_rdlt_id".into(),
        seq: "_rdlt_seq".into(),
    };
    let mut beyond = Vec::new();
    for bytes in [true, false] {
        let mut capabilities = Capabilities::minimal();
        if !bytes {
            capabilities.types.remove(&TypeKind::Binary);
        }
        let plan = child_plan_of(part, &capabilities, Some(root.clone()));
        let rendering = crate::cost::rendering(&capabilities);
        let stored = plan.stored();
        let mut measure = rendering.lowering(&part.batch, &stored, plan.row_bytes(), u64::MAX);
        let charge = measure.expanded(0..part.batch.num_rows());
        let stamp = Stamp {
            epoch: Epoch(1),
            loaded_at: UNIX_EPOCH,
            received_at: UNIX_EPOCH,
            segment: SegmentId(1),
            first_row: 0,
        };
        let (prepared, peak) =
            peak(|| plan.prepare(part.batch.clone(), Some(&lineage), &stamp, None));
        assert_eq!(
            prepared.unwrap().batch.num_rows(),
            part.batch.num_rows() / 2
        );
        if peak > charge + SLACK {
            beyond.push(format!(
                "bytes stored as bytes {bytes}: charged {charge}, allocated {peak}"
            ));
        }
    }
    assert!(beyond.is_empty(), "{}", beyond.join("\n"));
}
