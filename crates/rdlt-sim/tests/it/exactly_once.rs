use rdlt_sim::{
    Features, Recorded, Seed, SplitMix64, Weight, check_exactly_once, for_each_seed, seeds,
};

/// Seeds that each found a defect when first run, kept so they stay green.
const FOUND: [u64; 22] = [
    // Over the network: a served writer that panicked ended its write as though it were done.
    19,
    // Over the network: a host whose handshake a partition cut short held its connection, and
    // its connector's stop, forever.
    9_576,
    // A pushed float written to a JSON variant column with more digits than it needs.
    21_118,
    // A batch of a normalized merge stream whose every row a new array dropped: its key's type
    // was never resolved, so it met no refusal.
    163_723,
    // Over the network: a dropped connection's HTTP/2 task outlived it until its pings timed out
    // (rdlt-host's liveness tests guard it).
    157_941,
    // Over the network: a commit held until the network healed landed after the runs were judged
    // (the network's tests guard it).
    386_903,
    // Logged: a failed load's commit of a new full read, replayed in the next phase, landed rows
    // the model had not counted, until a phase waited for every log to be replayed.
    8_746,
    // Logged, shared: one pipeline's crash tore the other's live log, which then wrote on after
    // the tear, until a crash took only its own worker's logs.
    7_331,
    // Streaming: a run that followed the source ended before the rows a refusal needed arrived,
    // and succeeded, as the model now allows.
    370,
    // Streaming: reads cut batches wherever rows had arrived, so JSON pushes inferred types the
    // model never saw, until they served whole checkpoint groups.
    4_516,
    // Pressed: a partition that ended at its last cursor sealed it again unreserved, so a
    // commit recording many such cursors passed the log's share of the budget.
    820,
    // Over the network, pressed: a stopped connector listened again only once its drain ended,
    // which a host holding back its reads kept going past every placement.
    28960,
    // Pressed: partitions held what observing their JSON pushes might hold while they waited
    // for what building them took, until none could build.
    1_987,
    // A column of integers made for a column of JSON as its table was created was recorded
    // exact, though the integers split into it were read only as they were lowered.
    7_964,
    // A frozen normalized stream's declared array whose table no row had made yet was refused
    // as new to the table once a crashed attempt had created the stream's own table.
    77_581,
    // Logged in a small log: eight partitions' segments each spanned several chunks, which
    // stayed, holding committed frames beside open ones, until no batch fitted.
    53_249,
    // Reset: an incremental stream whose partitions never held a row, so the pipeline recorded
    // nothing of it, was refused as no stream it knew, which the world took for a failure.
    249_911, 728_591, 820_601, 918_105,
    // Reset: a followed stream that never held a row, whose read had sent its position all the
    // same, was reset, which a world that judged by the rows alone took for a failure.
    685_665,
    // Logs in an object store: a mark's and a head's creates given up landed after their log
    // was removed, and the head named a body deleted with it, which no replay could read.
    725_620,
];

#[test]
fn every_row_lands_exactly_once_through_faults_crashes_and_concurrent_runs() {
    let checked = for_each_seed("exactly_once", seeds(20), Weight::One, check_exactly_once);
    // The budget presses on a good share of the seeds: their pushes wait for lowering's room and
    // their cursors for a commit. The test job's twenty seeds are too few to hold the share; the
    // shards' and the nightly's thousands hold it.
    if checked.len() >= 100 {
        let pushes = checked.iter().filter(|run| run.memory_waits > 0).count();
        let cursors = checked.iter().filter(|run| run.cursor_waits > 0).count();
        assert!(
            pushes * 5 >= checked.len() && cursors * 10 >= checked.len(),
            "of {} seeds, {pushes} made pushes wait and {cursors} cursors",
            checked.len()
        );
    }
}

#[test]
fn seeds_that_once_found_a_defect_pass() {
    for_each_seed(
        "found",
        FOUND.map(Seed::new),
        Weight::One,
        check_exactly_once,
    );
}

/// A seed whose run acknowledged rows its destination then failed to commit, so only the
/// write-ahead log held them: with replay disabled, the source never serves them again and every
/// later run fails.
#[test]
fn a_seed_whose_rows_only_the_log_held_passes() {
    // What the seed's timing line records beside its outcome.
    let recorded = check_exactly_once(Seed::new(61)).recorded();
    assert!(recorded["digest"].is_u64(), "{recorded:?}");
    assert!(
        recorded["simulated_ms"].as_f64().is_some_and(|ms| ms > 0.0),
        "{recorded:?}"
    );
    let drawn = Features::draw(&mut SplitMix64::new(61));
    assert_eq!(recorded["features"], serde_json::to_value(drawn).unwrap());
}
