//! How a table's list grows over many commits: the files stay few, and no rule keeps them few
//! by writing the table again and again.

use rdlt_connector::{Field, LogicalType, SchemaVersion, TablePath, TableRef, TableSchema};

use super::{ids, listed, runs};
use crate::files::FileFormat;
use crate::files::manifest::{self, Listed};
use crate::files::session::Location;
use crate::files::session::tests::{Sessions, location};
use crate::limits::COMPACT_BYTES;

/// Applies the runs to merge to `files`, of which a commit added the last `added`, as a merge
/// that reads and writes nothing; the rows it would write.
fn merge(location: &Location, files: &mut Vec<Listed>, added: usize) -> u64 {
    let mut written = 0;
    for run in runs(location, files, added) {
        let rows = files[run.clone()].iter().map(|file| file.rows).sum();
        let bytes = files[run.clone()].iter().map(|file| file.bytes).sum();
        let path = files[run.start].path.clone();
        files.splice(run, [Listed { path, rows, bytes }]);
        written += rows;
    }
    written
}

/// The most files a list of `rows` rows holds while each file holds more than twice the rows
/// of the file after it.
fn halving(rows: u64) -> usize {
    usize::try_from(rows.max(1).ilog2()).unwrap() + 1
}

/// The most rows merges write for a table of `rows` rows: each row once with its commit, and
/// then only into a file half as large again as the file it was in.
fn rewritten(rows: u64) -> u64 {
    // 1.5 to the power of 12 is more than 2 to the power of 7.
    rows * (2 + u64::from(rows.max(1).ilog2()) * 12 / 7)
}

/// The rows of each file a commit adds, by the commit's number.
type Shape = Box<dyn FnMut(u64) -> Vec<u64>>;

/// The rows of each file the commit numbered `commit` adds, for each shape of a steady source
/// and for sources of no steady shape.
fn shapes() -> Vec<(&'static str, Shape)> {
    let steady = |files: &'static [u64]| -> Shape { Box::new(move |_| files.to_vec()) };
    let mut state = 0x9e37_79b9_7f4a_7c15_u64;
    let mut random = move |below: u64| {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (state >> 33) % below
    };
    vec![
        ("less than a batch", steady(&[3])),
        ("one batch", steady(&[8])),
        ("a batch and its remainder", steady(&[8, 1])),
        ("a large batch and a small remainder", steady(&[1024, 5])),
        ("batches and a remainder", steady(&[8, 8, 8, 1])),
        ("a remainder first", steady(&[1, 8])),
        ("one row", steady(&[1])),
        (
            "a large commit among small ones",
            Box::new(|commit| {
                if commit % 10 == 0 {
                    vec![1000]
                } else {
                    vec![1]
                }
            }),
        ),
        (
            "no shape",
            Box::new(move |_| (0..=random(4)).map(|_| 1 + random(1024)).collect()),
        ),
    ]
}

#[test]
fn a_list_stays_short_and_is_not_rewritten_whole_whatever_shape_its_commits_have() {
    let (_root, location) = location(FileFormat::Jsonl);
    // A row of one byte never fills a file; a row of a mebibyte fills one every 64 rows.
    for row_bytes in [1, 1024 * 1024] {
        for (shape, mut commit) in shapes() {
            let (mut files, mut rows, mut written) = (Vec::new(), 0, 0);
            for number in 0..500 {
                let adds = commit(number);
                rows += adds.iter().sum::<u64>();
                let mut added = listed(FileFormat::Jsonl, &adds);
                for file in &mut added {
                    file.bytes = file.rows * row_bytes;
                }
                files.extend(added);
                written += merge(&location, &mut files, adds.len());
                // Every two files next to each other at least halve, or would overfill a file.
                for pair in files.windows(2) {
                    assert!(
                        pair[0].rows > 2 * pair[1].rows
                            || pair[0].bytes + pair[1].bytes > COMPACT_BYTES,
                        "{shape} {row_bytes}: {pair:?} after commit {number}"
                    );
                }
            }
            let full = usize::try_from(2 * rows * row_bytes / COMPACT_BYTES).unwrap();
            let most = (full + 1) * halving(rows);
            assert!(files.len() <= most, "{shape} {row_bytes}: {}", files.len());
            let bound = rewritten(rows);
            assert!(written <= bound, "{shape} {row_bytes}: {written} of {rows}");
            assert_eq!(files.iter().map(|file| file.rows).sum::<u64>(), rows);
        }
    }
}

/// The rows of a list's files, how many of them a commit added, and the run to merge.
type Case = (&'static [u64], usize, Option<(usize, usize)>);

#[test]
fn the_files_a_commit_adds_merge_whatever_rows_they_hold() {
    let (_root, location) = location(FileFormat::Jsonl);
    let cases: [Case; 8] = [
        // A batch and its remainder: the commit's files merge, and the list's rule goes on.
        (&[100, 8, 1], 2, Some((1, 3))),
        (&[100, 8, 1], 1, None),
        (&[18, 8, 1], 2, Some((0, 3))),
        (&[19, 8, 1], 2, Some((1, 3))),
        (&[1024, 5, 1024, 5], 2, Some((0, 4))),
        (&[5000, 5, 1024, 5], 2, Some((1, 4))),
        // One file added merges with none where the file before it is too large.
        (&[8, 1, 9], 1, Some((0, 3))),
        (&[30, 9], 1, None),
    ];
    for (rows, added, expected) in cases {
        let files = listed(FileFormat::Jsonl, rows);
        let runs: Vec<_> = runs(&location, &files, added)
            .into_iter()
            .map(|run| (run.start, run.end))
            .collect();
        let expected: Vec<_> = expected.into_iter().collect();
        assert_eq!(runs, expected, "{rows:?} {added}");
    }
}

#[test]
fn the_files_a_commit_adds_merge_into_as_many_files_as_they_fill() {
    let (_root, location) = location(FileFormat::Jsonl);
    let third = COMPACT_BYTES / 3;
    let sized = |bytes: &[u64]| {
        let mut files = listed(FileFormat::Jsonl, &vec![1; bytes.len()]);
        for (file, bytes) in files.iter_mut().zip(bytes) {
            file.bytes = *bytes;
        }
        files
    };
    // Five files of a third of a full file: three fill one, and the two before them the next.
    let files = sized(&[third; 5]);
    assert_eq!(runs(&location, &files, 5), [2..5, 0..2]);
    // The earliest of them merges on with a file listed before, by the list's rule.
    let files = sized(&[1, third, third, third, third]);
    assert_eq!(runs(&location, &files, 4), [2..5, 0..2]);
    // Files each larger than half a full file merge with none.
    let files = sized(&[COMPACT_BYTES / 2 + 1; 3]);
    assert_eq!(runs(&location, &files, 3), []);
    // A file of another format among them parts the runs.
    let mut files = sized(&[1; 5]);
    files[2].path = files[2].path.replace(".jsonl", ".arrow");
    assert_eq!(runs(&location, &files, 5), [3..5, 0..2]);
}

fn table() -> TableRef {
    TableRef {
        path: TablePath::new(["rows"]).unwrap(),
        name: "rows".into(),
        version: SchemaVersion(1),
        generation: None,
        merge: None,
    }
}

/// The files the latest manifest lists for the table.
fn published(sessions: &Sessions) -> Vec<Listed> {
    let latest = manifest::latest(&sessions.location.dir).unwrap().unwrap();
    latest.tables["rows"].files.clone()
}

#[test]
fn an_append_table_of_batches_and_remainders_lists_few_files_and_rewrites_few_rows() {
    for format in [FileFormat::Jsonl, FileFormat::Arrow] {
        let sessions = Sessions::new(format);
        let schema = TableSchema::new(vec![Field::new("id", LogicalType::Int64, false)]).unwrap();
        sessions.create(&table(), &schema);
        let (mut next, mut written, mut seen) = (0, 0, Vec::new());
        for commit in 0..40_u64 {
            // A full batch, then the remainder after it, as a writer of fixed batches stages.
            let full: Vec<i64> = (next..next + 4).collect();
            sessions.stage(&table(), 2 * commit + 1, ids(&full));
            sessions.stage(&table(), 2 * commit + 2, ids(&[next + 4]));
            next += 5;
            let segments = [2 * commit + 1, 2 * commit + 2];
            sessions
                .commit(&sessions.meta(1, commit + 1, &segments))
                .unwrap();
            let files = published(&sessions);
            // What a merge wrote is every listed file no commit before listed.
            written += files
                .iter()
                .filter(|file| file.path.contains("/compacted/") && !seen.contains(&file.path))
                .map(|file| file.rows)
                .sum::<u64>();
            seen = files.into_iter().map(|file| file.path).collect();
        }
        let rows = u64::try_from(next).unwrap();
        assert_eq!(sessions.ids("rows"), (0..next).collect::<Vec<_>>());
        assert!(seen.len() <= halving(rows), "{format:?}: {}", seen.len());
        assert!(written <= rewritten(rows), "{format:?}: {written}");
        // Nothing but what is listed stays on disk.
        let mut listed = seen.clone();
        listed.sort();
        assert_eq!(sessions.data_files(), listed, "{format:?}");
    }
}
