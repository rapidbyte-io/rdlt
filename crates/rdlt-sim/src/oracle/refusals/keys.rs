//! What a merge stream's key columns may meet: a key widens where the destination can and its
//! schema is not frozen, and is refused otherwise.

use rdlt_connector::{Capabilities, LogicalType};
use rdlt_engine::{Nested, SchemaPolicy};

use super::{Arrival, FROZEN, KEY_CHANGED, Outcome, Step, outcome, widens};
use crate::workload::{Relaxed, SimStream};

/// What a merge stream's key batches may meet, its settings as `relaxed` leaves them: a key
/// column widens where the destination can and the schema is not frozen, and is refused
/// otherwise.
pub(super) fn key_outcome(
    stream: &SimStream,
    relaxed: Relaxed,
    capabilities: &Capabilities,
    phase: usize,
) -> Outcome {
    let resolved = stream.resolved(None, relaxed);
    let frozen = resolved.policy == SchemaPolicy::Freeze;
    let code = if frozen { FROZEN } else { KEY_CHANGED };
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
        code,
        |current, arrival| key_step(current, arrival, frozen, resolved.nested, capabilities),
    )
}

/// What a key batch column arriving as `arrival` does to a key column of `current`: a key
/// column widens where the destination can and the schema is not `frozen`, and is refused
/// otherwise.
pub(super) fn key_step(
    current: Option<&LogicalType>,
    arrival: &Arrival,
    frozen: bool,
    nested: Nested,
    capabilities: &Capabilities,
) -> Step {
    let (Some(current), Arrival::Typed(logical)) = (current, arrival) else {
        return Step::Unknown;
    };
    let joined = current.join(logical);
    let widened =
        !frozen && joined != LogicalType::Json && widens(current, &joined, nested, capabilities);
    if joined == *current || widened {
        Step::To(joined)
    } else {
        Step::Refused
    }
}
