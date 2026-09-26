//! What a merge stream's key columns may meet: a key widens where the destination can, and is
//! refused otherwise.

use rdlt_connector::{Capabilities, LogicalType};
use rdlt_engine::Nested;

use super::{Arrival, KEY_CHANGED, Outcome, Step, outcome, widens};
use crate::workload::{Relaxed, SimStream};

/// What a merge stream's key batches may meet: a key column widens where the destination can,
/// and is refused otherwise.
pub(super) fn key_outcome(
    stream: &SimStream,
    capabilities: &Capabilities,
    phase: usize,
) -> Outcome {
    let nested = stream.resolved(None, Relaxed::default()).nested;
    let arrivals: Vec<Vec<Arrival>> = (0..=phase)
        .map(|at| {
            let mut types = Vec::new();
            for partition in 0..stream.partitions.len() {
                let arrival = Arrival::Typed(stream.key_type(partition, at).clone());
                if !stream.read(partition, at).is_empty() && !types.contains(&arrival) {
                    types.push(arrival);
                }
            }
            types
        })
        .collect();
    outcome(
        Some(LogicalType::Int64),
        &arrivals,
        KEY_CHANGED,
        |current, arrival| key_step(current, arrival, nested, capabilities),
    )
}

/// What a key batch column arriving as `arrival` does to a key column of `current`: a key
/// column widens where the destination can, and is refused otherwise.
pub(super) fn key_step(
    current: Option<&LogicalType>,
    arrival: &Arrival,
    nested: Nested,
    capabilities: &Capabilities,
) -> Step {
    let (Some(current), Arrival::Typed(logical)) = (current, arrival) else {
        return Step::Unknown;
    };
    let joined = current.join(logical);
    let widened = joined != LogicalType::Json && widens(current, &joined, nested, capabilities);
    if joined == *current || widened {
        Step::To(joined)
    } else {
        Step::Refused
    }
}
