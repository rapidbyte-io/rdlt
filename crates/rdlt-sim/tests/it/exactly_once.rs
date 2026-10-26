use rdlt_sim::{
    Features, Reach, Recorded, Seed, SplitMix64, Weight, Workload, check_exactly_once,
    for_each_seed, seeds,
};

/// Seeds on which the simulation once found a defect, each held to the path it found it on.
///
/// A seed fails if its defect comes back, unless its comment says otherwise: such a seed keeps
/// only the path, and its defect may come back unseen.
const FOUND: [(u64, Reach); 17] = [
    // Over the network: a host whose handshake a partition cut short held its connection, and
    // its connector's stop, forever.
    (743, Reach::LinkCut),
    // A batch of a normalized merge stream whose every row a new array dropped: its key's type
    // was never resolved, so it met no refusal. The defect may not fail this seed.
    (22, Reach::Refused),
    // Streaming: a run that followed the source ended before the rows a refusal needed arrived,
    // and succeeded, as the model now allows.
    (2_656, Reach::Spared),
    // Streaming: reads cut batches wherever rows had arrived, so JSON pushes inferred types the
    // model never saw, until they served whole checkpoint groups.
    (4_516, Reach::Followed),
    // A column of integers made for a column of JSON as its table was created was recorded
    // exact, though the integers split into it were read only as they were lowered.
    (29_442, Reach::Widened),
    // A frozen normalized stream's declared array whose table no row had made yet was refused
    // as new to the table once a crashed attempt had created the stream's own table.
    (167_631, Reach::Crashed),
    // Reset: an incremental stream whose partitions never held a row, so the pipeline recorded
    // nothing of it, was refused as no stream it knew, which the world took for a failure.
    (249_911, Reach::ResetRefused),
    (728_591, Reach::ResetRefused),
    (820_601, Reach::ResetRefused),
    (918_105, Reach::ResetRefused),
    // A merge table's column widened to JSON over values written as floats and as plain text,
    // which the destination then held as JSON, and the check read as values of JSON, not of
    // the types they were written as.
    (11_754, Reach::Widened),
    (302, Reach::Widened),
    // A child table no row had reached yet was merged by its roots' commit, under no columns,
    // which the merge refused. The defect may not fail this seed.
    (86_557, Reach::EmptyChild),
    // A date written into timestamps, whose column then widens to JSON, holds a timestamp's text.
    (642_848, Reach::Widened),
    // A run held more than the heap's allowance beside what it charged. The defect may not fail
    // these seeds.
    (577_320, Reach::Crashed),
    (777_590, Reach::Replanned),
    // Over the network: a heartbeat drawn to wait 102 ms for its echoes lost a source whose reads'
    // transport frames, queued ahead of the echoes, took longer than that to cross the link, so a
    // run with nothing new took two attempts. The defect may not fail this seed.
    (583_907, Reach::Framing),
];

#[test]
fn every_row_lands_exactly_once_through_faults_crashes_and_concurrent_runs() {
    let checked = for_each_seed("exactly_once", seeds(20), Weight::One, check_exactly_once);
    // The budget presses on a good share of the seeds: their pushes, or the frames carrying them,
    // wait for lowering's room and their cursors for a commit. The test job's twenty seeds are too
    // few to hold the share; the shards' and the nightly's thousands hold it.
    if checked.len() >= 100 {
        let pushes = checked
            .iter()
            .filter(|run| {
                let waits = &run.waits;
                waits.intake.count + waits.decode.count + waits.work.count > 0
            })
            .count();
        let cursors = checked
            .iter()
            .filter(|run| run.waits.cursors.count > 0)
            .count();
        assert!(
            pushes * 5 >= checked.len() && cursors * 10 >= checked.len(),
            "of {} seeds, {pushes} made pushes wait and {cursors} cursors",
            checked.len()
        );
    }
}

#[test]
fn seeds_that_once_found_a_defect_pass() {
    let seeds = FOUND.map(|(seed, _)| Seed::new(seed));
    for_each_seed("found", seeds, Weight::One, |seed| {
        let checked = check_exactly_once(seed);
        crate::reaches(&FOUND, seed, checked.reached);
        checked
    });
}

/// The seed of [`a_seed_whose_rows_only_the_source_held_passes`].
const SERVED_AGAIN: u64 = 14;

/// A seed whose run read rows from a source that forgets and did not land them: the source was
/// told nothing of them, so it serves them again to the next run, which lands them once.
#[test]
fn a_seed_whose_rows_only_the_source_held_passes() {
    let checked = check_exactly_once(Seed::new(SERVED_AGAIN));
    assert!(checked.reached.has(Reach::ServedAgain));
    // What the seed's timing line records beside its outcome.
    let recorded = checked.recorded();
    assert!(recorded["digest"].is_u64(), "{recorded:?}");
    assert!(
        recorded["simulated_ms"].as_f64().is_some_and(|ms| ms > 0.0),
        "{recorded:?}"
    );
    let drawn = Features::draw(&mut SplitMix64::new(SERVED_AGAIN));
    assert!(drawn.forgetting, "{drawn:?}");
    assert_eq!(recorded["features"], serde_json::to_value(drawn).unwrap());
}

/// A seed's attempts are measured, so the check cannot switch itself off: its timing line records
/// what the worst held beyond its charge and, where the connectors are placed apart, what its reads
/// hold.
#[test]
fn attempts_are_measured_in_process_over_the_network_and_in_played_processes() {
    for (seed, network, process) in [(0, false, false), (508, true, false), (1, false, true)] {
        let features = Features::draw(&mut SplitMix64::new(seed));
        assert_eq!((features.network, features.process), (network, process));
        let recorded = check_exactly_once(Seed::new(seed)).recorded();
        let bytes = |name: &str| recorded[name].as_u64().expect("every term is recorded");
        assert!(bytes("held_bytes") > 0, "{recorded:?}");
        assert_eq!(
            bytes("queued_bytes") > 0,
            network || process,
            "{recorded:?}"
        );
    }
}

/// The first seeds whose source churns a stream runs follow re-plan changed sets and pass, so the
/// churn cannot switch itself off.
#[test]
fn seeds_whose_source_churns_replan_changed_sets_and_pass() {
    let churning = (0..)
        .filter(|seed| {
            let mut rng = SplitMix64::new(*seed);
            let features = Features::draw(&mut rng);
            let workload = Workload::generate(&mut rng, features);
            features.churn && workload.streams.iter().any(|stream| stream.follows)
        })
        .take(8)
        .map(Seed::new);
    let checked = for_each_seed("churning", churning, Weight::One, check_exactly_once);
    let replanned = checked
        .iter()
        .filter(|checked| checked.reached.has(Reach::Replanned))
        .count();
    assert!(
        replanned > 0,
        "none of {} churning seeds re-planned",
        checked.len()
    );
}
