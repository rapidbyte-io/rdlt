use std::collections::BTreeSet;

use super::{
    Counted, Counts, VALGRIND, Verdict, accepted, allocations, change, per_iteration, table,
    totals, unknown, valgrind_version, verdict,
};

const CALLGRIND: &str = "version: 1\ncreator: callgrind-3.25.1\npid: 7\n\
    cmd:  allocations passthrough/null_sink 3\npart: 1\n\npositions: line\nevents: Ir\n\
    summary: 153006793\n\nfl=(1) ???\nfn=(1) 0x0000000000001100\n0 3\n\ntotals: 153006793\n";

fn counts(instructions: u64, allocations: u64) -> Counts {
    Counts {
        instructions,
        allocations,
    }
}

fn counted(case: &str, base: Option<Counts>, head: Option<Counts>) -> Counted {
    Counted {
        case: case.to_owned(),
        base,
        head,
    }
}

#[test]
fn the_total_is_the_first_count_of_the_totals_line() {
    assert_eq!(totals(CALLGRIND).unwrap(), 153_006_793);
    assert_eq!(totals("totals: 12 4 5\n").unwrap(), 12);
}

#[test]
fn callgrind_output_without_a_total_is_an_error() {
    for text in ["summary: 12\n", "totals:\n", "totals: twelve\n"] {
        assert!(totals(text).is_err(), "{text:?}");
    }
}

#[test]
fn the_allocations_are_the_count_the_run_printed() {
    assert_eq!(allocations("allocations 3436\n").unwrap(), 3436);
    for output in ["", "allocations\n", "allocations many\n"] {
        assert!(allocations(output).is_err(), "{output:?}");
    }
}

#[test]
fn per_iteration_is_the_difference_over_the_iterations_between() {
    assert_eq!(per_iteration(144_139_217, 153_006_793).unwrap(), 4_433_788);
    assert_eq!(per_iteration(10, 10).unwrap(), 0);
    assert!(per_iteration(11, 10).is_err());
}

#[test]
fn change_is_a_percentage_of_the_base() {
    let cases = [
        (100, 102, 2.0),
        (100, 98, -2.0),
        (55_313_777, 56_610_340, 2.344),
        (0, 0, 0.0),
    ];
    for (base, head, expected) in cases {
        assert!(
            (change(base, head) - expected).abs() < 0.001,
            "{base} -> {head}"
        );
    }
    assert!(change(0, 1).is_infinite());
}

#[test]
fn each_count_comes_to_its_verdict() {
    let none = BTreeSet::new();
    let accepting: BTreeSet<String> = ["shred/nested".to_owned()].into();
    let case = |base, head| counted("shred/nested", base, head);
    let cases = [
        (
            case(Some(counts(100, 50)), Some(counts(102, 51))),
            &none,
            Verdict::Within,
        ),
        (
            case(Some(counts(100, 50)), Some(counts(90, 10))),
            &none,
            Verdict::Within,
        ),
        (
            case(Some(counts(100, 50)), Some(counts(103, 50))),
            &none,
            Verdict::Grew,
        ),
        (
            case(Some(counts(100, 50)), Some(counts(100, 52))),
            &none,
            Verdict::Grew,
        ),
        (
            case(Some(counts(100, 0)), Some(counts(100, 1))),
            &none,
            Verdict::Grew,
        ),
        (
            case(Some(counts(100, 40)), Some(counts(100, 41))),
            &none,
            Verdict::Grew,
        ),
        (
            case(Some(counts(100, 50)), Some(counts(103, 50))),
            &accepting,
            Verdict::Accepted,
        ),
        (
            case(Some(counts(100, 50)), Some(counts(101, 50))),
            &accepting,
            Verdict::Within,
        ),
        (case(None, Some(counts(100, 50))), &none, Verdict::New),
        (case(Some(counts(100, 50)), None), &none, Verdict::Gone),
    ];
    for (counted, accepted, expected) in cases {
        assert_eq!(verdict(&counted, 2.0, accepted), expected, "{counted:?}");
    }
}

#[test]
fn trailers_name_one_case_a_line() {
    let named = accepted("normalize/keyless/nested\n\n  wal/encode \n\n");
    assert_eq!(
        named,
        BTreeSet::from([
            "normalize/keyless/nested".to_owned(),
            "wal/encode".to_owned()
        ])
    );
    assert!(accepted("\n\n").is_empty());
}

#[test]
fn a_trailer_naming_no_case_is_found() {
    let cases = [
        counted(
            "passthrough/null_sink",
            Some(counts(1, 1)),
            Some(counts(1, 1)),
        ),
        counted("wal/scan", None, Some(counts(1, 1))),
    ];
    let named: BTreeSet<String> =
        ["passthrough/null_sink".to_owned(), "wal/scna".to_owned()].into();
    assert_eq!(unknown(&named, &cases), Some("wal/scna"));
    let named: BTreeSet<String> =
        ["passthrough/null_sink".to_owned(), "wal/scan".to_owned()].into();
    assert_eq!(unknown(&named, &cases), None);
}

#[test]
fn the_table_gives_each_case_its_counts_changes_and_verdict() {
    let cases = [
        counted(
            "ipc/roundtrip",
            Some(counts(200, 10)),
            Some(counts(200, 10)),
        ),
        counted(
            "passthrough/null_sink",
            Some(counts(100, 40)),
            Some(counts(166, 40)),
        ),
        counted("wal/scan", None, Some(counts(7, 1))),
    ];
    assert_eq!(
        table(&cases, 2.0, &BTreeSet::new()),
        "| Case | Instructions | Change | Allocations | Change | Verdict |\n\
         |---|---:|---:|---:|---:|---|\n\
         | `ipc/roundtrip` | 200 → 200 | +0.00% | 10 → 10 | +0.00% | within |\n\
         | `passthrough/null_sink` | 100 → 166 | +66.00% | 40 → 40 | +0.00% | more than 2% above the base |\n\
         | `wal/scan` | 7 | - | 1 | - | new |\n"
    );
}

#[test]
fn valgrind_versions_read_as_major_and_minor() {
    let versions = [
        ("valgrind-3.25.1\n", (3, 25)),
        ("valgrind-3.22.0", (3, 22)),
        ("valgrind-3.26.0.GIT", (3, 26)),
    ];
    for (printed, expected) in versions {
        assert_eq!(valgrind_version(printed).unwrap(), expected, "{printed:?}");
    }
    for printed in ["", "3.22.0", "valgrind-3", "valgrind-three.1"] {
        assert!(valgrind_version(printed).is_err(), "{printed:?}");
    }
    assert!((3, 21) < VALGRIND && (3, 22) >= VALGRIND);
}
