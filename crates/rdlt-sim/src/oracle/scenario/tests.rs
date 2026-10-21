use std::time::Duration;

use super::{Scenario, pick};
use crate::rng::SplitMix64;

/// When each of a thousand disruptions drawn for runs of `length` lands.
fn landings(length: Option<Duration>) -> Vec<Duration> {
    let mut rng = SplitMix64::new(3);
    (0..1000)
        .filter_map(|_| match pick(&mut rng, length) {
            Scenario::Plain => None,
            Scenario::Crash(after) | Scenario::Stop(after) | Scenario::StopNow(after) => {
                Some(after)
            }
            Scenario::Concurrent(after) => Some(after * 2),
        })
        .collect()
}

#[test]
fn a_disruption_lands_within_the_length_of_the_last_run_that_succeeded() {
    for length in [Duration::from_millis(40), Duration::from_secs(30)] {
        let landed = landings(Some(length));
        assert!(
            landed.iter().all(|after| *after <= length),
            "{length:?}: {landed:?}"
        );
        let late = landed.iter().filter(|after| **after > length / 2).count();
        assert!(
            late * 4 > landed.len(),
            "{length:?}: few landed in the second half"
        );
    }
    assert!(
        landings(None)
            .iter()
            .all(|after| *after < Duration::from_secs(2))
    );
}
