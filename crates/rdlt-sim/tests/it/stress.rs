use rdlt_sim::{Weight, for_each_seed, seeds, stress};

#[test]
#[ignore = "runs on real threads and the real clock; run it with `just stress`"]
fn every_row_lands_exactly_once_on_many_threads() {
    for_each_seed("stress", seeds(20), Weight::Threaded, stress);
}
