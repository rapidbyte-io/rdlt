use std::collections::BTreeSet;

use proptest::prelude::*;

use super::{SegmentRange, SegmentSet};
use crate::id::SegmentId;

fn ids(values: &[u64]) -> SegmentSet {
    values.iter().copied().map(SegmentId).collect()
}

fn ranges(set: &SegmentSet) -> Vec<(u64, u64)> {
    set.ranges()
        .iter()
        .map(|range| (range.first.0, range.last.0))
        .collect()
}

type Case = (&'static [u64], &'static [(u64, u64)]);

#[test]
fn inserting_merges_adjacent_ids_into_ranges() {
    let cases: &[Case] = &[
        (&[], &[]),
        (&[5], &[(5, 5)]),
        (&[1, 2, 3], &[(1, 3)]),
        (&[3, 1, 2], &[(1, 3)]),
        (&[1, 3, 5, 4], &[(1, 1), (3, 5)]),
        (&[1, 3, 2], &[(1, 3)]),
        (&[10, 1, 5, 5], &[(1, 1), (5, 5), (10, 10)]),
        (
            &[u64::MAX, u64::MAX - 1, 0],
            &[(0, 0), (u64::MAX - 1, u64::MAX)],
        ),
    ];
    for (input, expected) in cases {
        assert_eq!(ranges(&ids(input)), *expected, "input {input:?}");
    }
}

#[test]
fn deserializing_refuses_overlapping_or_adjacent_ranges() {
    let ok = r#"[{"first":1,"last":2},{"first":4,"last":4}]"#;
    assert_eq!(
        serde_json::from_str::<SegmentSet>(ok).unwrap(),
        ids(&[1, 2, 4])
    );
    for bad in [
        r#"[{"first":1,"last":2},{"first":3,"last":4}]"#,
        r#"[{"first":4,"last":4},{"first":1,"last":2}]"#,
        r#"[{"first":2,"last":1}]"#,
    ] {
        assert!(serde_json::from_str::<SegmentSet>(bad).is_err(), "{bad}");
    }
}

fn segment_ids() -> impl Strategy<Value = Vec<u64>> {
    proptest::collection::vec(prop_oneof![0u64..64, (u64::MAX - 8)..=u64::MAX], 0..40)
}

proptest! {
    #[test]
    fn a_segment_set_behaves_like_a_set(values in segment_ids(), probe in 0u64..70) {
        let set = ids(&values);
        let oracle: BTreeSet<u64> = values.iter().copied().collect();
        prop_assert_eq!(set.iter().map(|id| id.0).collect::<Vec<_>>(), oracle.iter().copied().collect::<Vec<_>>());
        prop_assert_eq!(set.len(), u64::try_from(oracle.len()).unwrap());
        prop_assert_eq!(set.is_empty(), oracle.is_empty());
        prop_assert_eq!(set.contains(SegmentId(probe)), oracle.contains(&probe));
        prop_assert!(set.ranges().windows(2).all(|pair| pair[0].last.0 + 1 < pair[1].first.0));
    }

    #[test]
    fn segment_sets_round_trip_through_json(values in segment_ids()) {
        let set = ids(&values);
        let json = serde_json::to_string(&set).unwrap();
        prop_assert_eq!(serde_json::from_str::<SegmentSet>(&json).unwrap(), set);
    }
}

#[test]
fn a_range_is_inclusive() {
    let set: SegmentSet = serde_json::from_str(r#"[{"first":3,"last":5}]"#).unwrap();
    assert_eq!(
        set.ranges(),
        &[SegmentRange {
            first: SegmentId(3),
            last: SegmentId(5)
        }]
    );
    assert!(set.contains(SegmentId(5)) && !set.contains(SegmentId(6)));
}
