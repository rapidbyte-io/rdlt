//! Deterministic `_rdlt_id` as executable properties (SC-006, design doc §5.4).

use proptest::prelude::*;
use rdlt_core::identity::{FieldValue, RowIdBuilder, child_row_id};

type Row = Vec<(String, Option<Vec<u8>>)>;

fn any_row() -> impl Strategy<Value = Row> {
    proptest::collection::vec(
        (
            ".{0,20}",
            proptest::option::of(proptest::collection::vec(any::<u8>(), 0..40)),
        ),
        0..12,
    )
}

fn id_of(row: &Row, keyed: bool) -> rdlt_core::RowId {
    let mut builder = if keyed {
        RowIdBuilder::keyed()
    } else {
        RowIdBuilder::keyless()
    };
    for (name, value) in row {
        let fv = match value {
            None => FieldValue::Null,
            Some(bytes) => FieldValue::Bytes(bytes),
        };
        builder.field(name, fv);
    }
    builder.finish()
}

proptest! {
    /// Identical input rows always produce identical ids — across runs, machines,
    /// and (by construction: no ambient state) time.
    #[test]
    fn deterministic(row in any_row(), keyed in any::<bool>()) {
        prop_assert_eq!(id_of(&row, keyed), id_of(&row, keyed));
    }

    /// Different content produces different ids (collision would need a blake3 break;
    /// what this actually guards is input canonicalization — no two distinct field
    /// sequences may serialize to the same hash input).
    #[test]
    fn content_sensitive(a in any_row(), b in any_row()) {
        prop_assume!(a != b);
        prop_assert_ne!(id_of(&a, false), id_of(&b, false));
    }

    /// Child ids are sensitive to parent, position, and content — nested changes
    /// produce new child ids, which is what subtree merge relies on.
    #[test]
    fn child_ids_track_all_inputs(a in any_row(), b in any_row(), pos in 0u64..1000) {
        prop_assume!(a != b);
        let (pa, pb) = (id_of(&a, false), id_of(&b, false));
        prop_assert_ne!(child_row_id(&pa, pos, &pa), child_row_id(&pb, pos, &pa));
        prop_assert_ne!(child_row_id(&pa, pos, &pa), child_row_id(&pa, pos + 1, &pa));
        prop_assert_ne!(child_row_id(&pa, pos, &pa), child_row_id(&pa, pos, &pb));
    }
}
