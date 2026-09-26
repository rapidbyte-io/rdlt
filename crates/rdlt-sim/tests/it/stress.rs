use rdlt_sim::{seeds, stress};

#[test]
#[ignore = "runs on real threads and the real clock; run it with `just stress`"]
fn every_row_lands_exactly_once_on_many_threads() {
    for seed in seeds(20) {
        stress(seed);
    }
}
