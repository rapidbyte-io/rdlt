//! Written cases at the boundaries the single passes turn on: a change at a truncate's own
//! sequence, a truncate at a row's or a version's, an upsert between two truncates, a tombstone
//! at the bound, and rows that share part of a composite key.

use rdlt_connector::ChangeOp::{self, Delete, Insert, Truncate};

use super::{Change, MODES, Mode, Shape, compared};

/// A change of `key` in region 0 at `seq`, deleted at `at` where it says so.
fn change(op: ChangeOp, key: i64, seq: u16, at: Option<i64>) -> Change {
    Change {
        op,
        key,
        region: 0,
        value: Some(format!("v{key}")),
        n: Some(key),
        at,
        flags: 0,
        seq,
        from: i64::from(seq),
    }
}

/// Commits each of `cases` to SQLite and the reference in each of `modes`.
fn alike(cases: &[(&str, Vec<Vec<Vec<Change>>>)], modes: &[Mode], regions: bool) {
    for mode in modes {
        for (name, commits) in cases {
            let outcome = compared(
                commits,
                Shape {
                    mode: *mode,
                    regions,
                },
            );
            assert!(outcome.is_ok(), "{mode:?}, {name}: {outcome:?}");
        }
    }
}

#[test]
fn a_change_at_a_truncate_s_own_sequence_lands_as_the_reference_lands_it() {
    let cases = vec![
        (
            "an insert after a truncate of its sequence",
            vec![
                vec![vec![
                    change(Insert, 1, 1, None),
                    change(Truncate, 0, 4, Some(9)),
                    change(Insert, 2, 4, None),
                ]],
                vec![vec![change(Insert, 3, 4, None)]],
            ],
        ),
        (
            "an insert before a truncate of its sequence",
            vec![
                vec![vec![
                    change(Insert, 1, 1, None),
                    change(Insert, 2, 4, None),
                    change(Truncate, 0, 4, Some(9)),
                ]],
                vec![vec![change(Insert, 3, 5, None)]],
            ],
        ),
        (
            "a published row at a later commit's truncate",
            vec![
                vec![vec![change(Insert, 1, 4, None), change(Insert, 2, 3, None)]],
                vec![vec![change(Truncate, 0, 4, Some(9))]],
                vec![vec![change(Insert, 2, 3, None), change(Insert, 1, 4, None)]],
            ],
        ),
        (
            "a delete at a truncate's sequence, then its key again",
            vec![
                vec![vec![change(Insert, 1, 2, None)]],
                vec![vec![
                    change(Delete, 1, 4, Some(8)),
                    change(Truncate, 0, 4, Some(9)),
                ]],
                vec![vec![change(Insert, 1, 4, None), change(Insert, 1, 3, None)]],
            ],
        ),
        (
            "a delete at an earlier truncate's sequence, sent again after it",
            vec![
                vec![vec![change(Insert, 1, 2, None), change(Insert, 2, 2, None)]],
                vec![vec![
                    change(Delete, 1, 5, Some(8)),
                    change(Truncate, 0, 5, Some(9)),
                ]],
                vec![vec![change(Insert, 1, 5, None), change(Insert, 2, 5, None)]],
                vec![vec![change(Insert, 1, 6, None)]],
            ],
        ),
    ];
    alike(&cases, &MODES[..2], false);
    alike(&cases, &[Mode::HistoryHard, Mode::HistorySoft], false);
}

#[test]
fn an_upsert_between_two_truncates_of_a_commit_is_marked_by_the_second() {
    let held = vec![vec![change(Insert, 1, 1, None)]];
    let around = |first: Option<i64>, upsert: Option<i64>, second: Option<i64>| {
        vec![vec![
            change(Truncate, 0, 3, first),
            change(Insert, 1, 4, upsert),
            change(Truncate, 0, 6, second),
        ]]
    };
    let mut other = around(Some(5), None, Some(7));
    other[0].push(change(Insert, 2, 6, None));
    let mut batches = around(Some(5), None, Some(7));
    let last = batches[0].split_off(2);
    let middle = batches[0].split_off(1);
    batches.extend([middle, last]);
    let mut unheld = around(Some(5), None, Some(7));
    unheld[0].insert(0, change(Insert, 1, 2, None));
    let cases = vec![
        ("alone", vec![held.clone(), around(Some(5), None, Some(7))]),
        (
            "beside a key at the second truncate",
            vec![held.clone(), other],
        ),
        ("in three batches", vec![held.clone(), batches]),
        (
            "the upsert says a time",
            vec![held.clone(), around(Some(5), Some(2), Some(7))],
        ),
        (
            "the times swapped",
            vec![held.clone(), around(Some(7), None, Some(5))],
        ),
        (
            "the times equal",
            vec![held.clone(), around(Some(5), None, Some(5))],
        ),
        ("a key never held", vec![unheld]),
    ];
    alike(&cases, &MODES[..2], false);
    alike(&cases, &[Mode::HistoryHard, Mode::HistorySoft], false);
    // A table that keeps no history takes a first truncate that says no time.
    let untimed = vec![("the first untimed", vec![held, around(None, None, Some(7))])];
    alike(&untimed, &[Mode::Soft], false);
}

#[test]
fn truncates_of_a_commit_without_an_upsert_between_them_mark_as_the_reference_marks() {
    let held = vec![vec![change(Insert, 1, 1, None)]];
    let commit = |changes: Vec<Change>| vec![held.clone(), vec![changes]];
    let cases = vec![
        (
            "no upsert between",
            commit(vec![
                change(Truncate, 0, 3, Some(5)),
                change(Truncate, 0, 6, Some(7)),
            ]),
        ),
        (
            "one truncate after the upsert",
            commit(vec![
                change(Insert, 1, 4, None),
                change(Truncate, 0, 6, Some(7)),
            ]),
        ),
        (
            "one truncate before the upsert",
            commit(vec![
                change(Truncate, 0, 3, Some(5)),
                change(Insert, 1, 4, None),
            ]),
        ),
    ];
    alike(&cases, &MODES[..2], false);
    alike(&cases, &[Mode::HistoryHard, Mode::HistorySoft], false);
}

#[test]
fn a_truncate_at_a_version_s_own_sequence_leaves_it() {
    let cases = vec![
        (
            "a truncate at its key's version",
            vec![
                vec![vec![change(Insert, 1, 4, None)]],
                vec![vec![change(Truncate, 0, 4, Some(9))]],
            ],
        ),
        (
            "two truncates, the first at the version",
            vec![
                vec![vec![change(Insert, 1, 4, None)]],
                vec![vec![
                    change(Truncate, 0, 4, Some(8)),
                    change(Truncate, 0, 6, Some(9)),
                ]],
            ],
        ),
        (
            "a truncate at an upsert of its own commit",
            vec![vec![vec![
                change(Insert, 1, 2, None),
                change(Truncate, 0, 5, Some(9)),
                change(Insert, 1, 5, None),
                change(Insert, 2, 5, None),
            ]]],
        ),
        (
            "a truncate at a delete's sequence",
            vec![
                vec![vec![change(Insert, 1, 2, None)]],
                vec![vec![
                    change(Delete, 1, 5, Some(7)),
                    change(Truncate, 0, 5, Some(9)),
                ]],
                vec![vec![change(Insert, 1, 5, None)]],
            ],
        ),
    ];
    alike(&cases, &MODES, false);
}

#[test]
fn a_tombstone_at_the_bound_s_own_sequence_still_buries_its_key() {
    let cases = vec![(
        "a delete and a truncate of one sequence, then the key at it and past it",
        vec![
            vec![vec![change(Insert, 1, 2, None), change(Insert, 2, 3, None)]],
            vec![vec![
                change(Delete, 1, 6, Some(7)),
                change(Truncate, 0, 6, Some(9)),
            ]],
            vec![vec![change(Insert, 1, 6, None), change(Insert, 2, 6, None)]],
            vec![vec![change(Insert, 1, 7, None)]],
        ],
    )];
    alike(&cases, &[Mode::Hard, Mode::HistoryHard], false);
}

#[test]
fn a_truncate_removes_no_row_at_its_own_sequence_beside_a_row_sharing_part_of_its_key() {
    let regional = |op, key, region, seq, at| Change {
        region,
        ..change(op, key, seq, at)
    };
    let cases = vec![
        (
            "two regions of one id, the later at the truncate",
            vec![
                vec![vec![
                    regional(Insert, 1, 0, 1, None),
                    regional(Insert, 1, 1, 4, None),
                ]],
                vec![vec![change(Truncate, 0, 4, Some(9))]],
            ],
        ),
        (
            "two ids of one region, either side of the truncate",
            vec![
                vec![vec![
                    regional(Insert, 1, 1, 3, None),
                    regional(Insert, 2, 1, 5, None),
                ]],
                vec![vec![
                    change(Truncate, 0, 5, Some(9)),
                    regional(Insert, 3, 1, 5, None),
                ]],
            ],
        ),
    ];
    alike(&cases, &MODES[..2], true);
    alike(&cases, &[Mode::HistoryHard, Mode::HistorySoft], true);
}
