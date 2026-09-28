//! What a merge stream's key columns may meet: a key widens where the destination can and its
//! schema is not frozen, and is refused otherwise.

use rdlt_connector::{Capabilities, LogicalType, TypeKind};
use rdlt_engine::{Nested, SchemaPolicy};
use rdlt_testkit::canon::storage;

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
/// column widens where the destination can, the schema is not `frozen`, and the destination
/// stores both types by value, so equal keys still match; it is refused otherwise.
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
    let widened = !frozen
        && joined != LogicalType::Json
        && widens(current, &joined, nested, capabilities)
        && by_value(current, nested, capabilities)
        && by_value(&joined, nested, capabilities);
    if joined == *current || widened {
        Step::To(joined)
    } else {
        Step::Refused
    }
}

/// Whether the destination stores `logical` as itself, or as an integer of another width where it
/// is one: a type it stores rendered into another, as a decimal into text, renders equal values
/// differently once the type changes.
fn by_value(logical: &LogicalType, nested: Nested, capabilities: &Capabilities) -> bool {
    let stored = storage(logical, nested == Nested::Native, capabilities);
    let integer = |kind| {
        matches!(
            kind,
            TypeKind::Int8 | TypeKind::Int16 | TypeKind::Int32 | TypeKind::Int64
        )
    };
    stored.kind() == logical.kind() || (integer(stored.kind()) && integer(logical.kind()))
}
