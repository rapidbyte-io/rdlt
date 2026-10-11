use std::fs;
use std::path::Path;

use super::check;
use crate::rules::Rule;

fn record(root: &Path, name: &str, text: &str) {
    let dir = root.join("docs/adr");
    fs::create_dir_all(&dir).unwrap();
    fs::write(dir.join(name), text).unwrap();
}

/// The lines `check` finds a change narrated on, in one record holding `text`.
fn flagged(text: &str) -> Vec<usize> {
    let root = tempfile::tempdir().unwrap();
    record(root.path(), "0001-a.md", text);
    check(root.path())
        .unwrap()
        .into_iter()
        .map(|(path, finding)| {
            assert_eq!(path, Path::new("docs/adr/0001-a.md"));
            assert_eq!(finding.rule, Rule::RecordHistory);
            finding.line
        })
        .collect()
}

#[test]
fn a_record_that_narrates_a_change_is_refused_where_it_does() {
    let cases: &[(&str, &[usize])] = &[
        ("Status: accepted, 2026-09-23; amended 2026-09-25.\n", &[1]),
        (
            "- A bullet.\n  - Amended 2026-10-03 (ADR 0046): ids are long.\n",
            &[2],
        ),
        ("A rule.\n(Superseded by ADR 0038: one limit.)\n", &[2]),
        ("This supersedes ADR 0015 where it says so.\n", &[1]),
        ("The rule, superseded\nby ADR 0039, stays.\n", &[1]),
        ("This ADR amends both.\n", &[1]),
        ("ADR 0045 is amended by this one.\n", &[1]),
        (
            "  - Buffers are checked.\n    (ADR 0038 replaces the check of each buffer.)\n",
            &[2],
        ),
        (
            "(ADR 0039 keeps a signal as its partition's state.)\n",
            &[1],
        ),
        ("It is reserved before it is built since ADR 0040.\n", &[1]),
        ("A third,\nSince ADR 0040 it is reserved.\n", &[2]),
        (
            "And what replay stages, which ADR 0045 reserves since.\n",
            &[1],
        ),
    ];
    for (text, lines) in cases {
        assert_eq!(flagged(text), lines.to_vec(), "{text:?}");
    }
}

#[test]
fn a_record_stating_its_decision_passes() {
    for text in [
        "A commit removes what it supersedes.\n",
        "A row of its key supersedes the one before it.\n",
        "A plan made for a view already superseded is used but not cached.\n",
        "The lattice differs from spec §8.2: `Int64 ∨ Float64` is `Json`.\n",
        "- A table replaced whole, by a rule\n  (ADR 0049). It holds.\n",
        "  (ADR 0045): receipts are forgotten before the horizon.\n",
        "A stream is refused\n(ADR 0029, `change_read_unreplayable`).\n",
        "\n\n(ADR 0021).\n",
    ] {
        assert_eq!(flagged(text), Vec::<usize>::new(), "{text:?}");
    }
}

#[test]
fn a_tree_without_records_has_none_to_check() {
    let root = tempfile::tempdir().unwrap();
    assert!(check(root.path()).unwrap().is_empty());
}

#[test]
fn only_markdown_files_are_records() {
    let root = tempfile::tempdir().unwrap();
    record(root.path(), "notes.txt", "Amended yesterday.\n");
    assert!(check(root.path()).unwrap().is_empty());
}
