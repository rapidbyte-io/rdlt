use std::sync::Arc;

use arrow_array::{ArrayRef, UInt32Array};
use rdlt_connector::{Epoch, SegmentId};
use serde_json::json;

use super::walked;
use crate::normalize::tests::{parts, shape};
use crate::normalize::{Part, Received};

/// Two orders, each with one item holding one tag: a root, an item and a tag table.
fn orders() -> Vec<Part> {
    let records = [
        json!({"id": 1, "items": [{"sku": "a", "tags": ["x"]}]}),
        json!({"id": 2, "items": [{"sku": "b", "tags": ["y"]}]}),
    ];
    let parts = parts(&records, &shape(8));
    assert_eq!(parts.len(), 3);
    parts
}

fn received() -> Received {
    Received {
        epoch: Epoch(1),
        segment: SegmentId(1),
        first_row: 0,
    }
}

fn positions(values: &[u32]) -> ArrayRef {
    Arc::new(UInt32Array::from(values.to_vec()))
}

#[test]
fn rows_whose_lineage_names_the_rows_they_come_from_are_walked() {
    let tables = walked(&[(orders(), received())], false).unwrap();
    let tags = &tables[&vec!["items".to_owned(), "tags".to_owned()]];
    assert_eq!(tags.len(), 2);
    assert!(tags[0].at.starts_with("#0/"), "{}", tags[0].at);
}

#[test]
fn a_child_whose_parent_is_in_a_table_its_own_does_not_extend_is_refused() {
    // A tag table at the item table's own path, and one beside it.
    for path in [vec!["items"], vec!["other", "tags"]] {
        let mut parts = orders();
        parts[2].path = path.iter().map(|segment| Arc::from(*segment)).collect();
        assert!(walked(&[(parts, received())], false).is_err(), "{path:?}");
    }
}

#[test]
fn a_child_whose_root_is_not_the_root_it_comes_from_is_refused() {
    // The tag of the first order naming the second as its root.
    let mut parts = orders();
    let parent = parts[2].lineage.parent.as_mut().unwrap();
    parent.root_row = positions(&[1, 1]);
    assert!(walked(&[(parts, received())], false).is_err());
    // A tag naming its own item as its root: a row it comes from, but no root.
    let mut parts = orders();
    let (items, _) = parts[1].lineage.columns(&received()).unwrap();
    parts[2].lineage.keys = Some(items);
    assert!(walked(&[(parts, received())], false).is_err());
}

#[test]
fn two_rows_sharing_an_id_are_refused() {
    let parts = orders();
    let again = (parts.clone(), received());
    assert!(walked(&[(parts, received()), again], false).is_err());
}

#[test]
fn a_piece_holding_a_value_of_no_json_form_here_is_a_disagreement() {
    let days: ArrayRef = Arc::new(arrow_array::Date32Array::from(vec![1]));
    let batch = arrow_array::RecordBatch::try_from_iter([("at", days)]).unwrap();
    let pieces = [(batch, super::Cut::Piece)];
    assert!(super::normalized_against_reference(&pieces, 8, None).is_err());
}
