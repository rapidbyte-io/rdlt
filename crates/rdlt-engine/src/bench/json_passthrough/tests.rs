use std::num::NonZeroUsize;

use bytes::Bytes;

use super::{JsonCell, JsonPassthrough};
use crate::Cores;
use crate::bench::{Corpus, PUSH_BYTES};

#[test]
fn the_cells_are_each_corpus_from_one_partition_and_from_sixteen() {
    let names: Vec<String> = JsonCell::all().into_iter().map(JsonCell::name).collect();
    assert_eq!(
        names,
        ["nested/1p", "nested/16p", "wide_200/1p", "wide_200/16p"]
    );
}

#[test]
fn a_run_pushes_sixteen_pushes_of_the_default_coalescing_target() {
    let cell = JsonCell {
        corpus: Corpus::Wide(200),
        partitions: NonZeroUsize::MIN,
    };
    let pushes = cell.pushes();
    assert_eq!(pushes.len(), 16);
    assert!(pushes[..15].iter().all(|push| push.len() >= PUSH_BYTES));
    assert!(pushes[15].len() > PUSH_BYTES / 2);
}

#[test]
fn every_cell_loads_every_row_of_its_pushes_on_every_run() {
    let cores = Cores::new(NonZeroUsize::new(2).unwrap());
    for cell in JsonCell::all() {
        let pushes = cell.tested();
        assert_eq!(pushes.len(), 2 * cell.partitions.get(), "{}", cell.name());
        let lines: usize = pushes.iter().map(lines).sum();
        let bytes: usize = pushes.iter().map(Bytes::len).sum();
        let passthrough = JsonPassthrough::try_new(cores, cell, pushes).unwrap();
        assert_eq!(passthrough.rows(), u64::try_from(lines).unwrap());
        assert_eq!(passthrough.bytes(), u64::try_from(bytes).unwrap());
        for _ in 0..2 {
            let report = passthrough.run();
            assert_eq!(report.rows, passthrough.rows(), "{}", cell.name());
            assert_eq!(report.commits, 1, "{}", cell.name());
            let shredded = &report.streams["events"].shred;
            let least = u64::try_from(2 * cell.partitions.get()).unwrap();
            assert!(shredded.parsed >= least, "{}", cell.name());
        }
    }
}

fn lines(push: &Bytes) -> usize {
    memchr::memchr_iter(b'\n', push).count()
}
