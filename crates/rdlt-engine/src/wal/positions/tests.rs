use std::collections::BTreeMap;

use rdlt_connector::{
    Cursor, PartitionId, PartitionState, PipelineState, StateChange, StateEntry, StateKey,
    StreamName, StreamState,
};

use super::Positions;

fn stream(name: &str) -> StreamName {
    StreamName::new(name).expect("a valid stream")
}

fn partition(id: &str) -> PartitionId {
    PartitionId::parse(id).expect("a valid partition")
}

fn at(next: u64) -> PartitionState {
    PartitionState::Cursor(Cursor::encode(1, &next).expect("a cursor"))
}

fn put(name: &str, id: &str, state: PartitionState) -> StateChange {
    StateChange::Put(
        StateEntry::Partition {
            stream: stream(name),
            partition: partition(id),
            state,
        }
        .to_record(),
    )
}

#[test]
fn positions_follow_the_state_they_start_from_and_the_commits_after_it() {
    let mut streams = BTreeMap::new();
    streams.insert(
        stream("orders"),
        StreamState {
            partitions: [
                (partition("p0"), at(3)),
                (partition("p1"), PartitionState::Done),
            ]
            .into_iter()
            .collect(),
            ..StreamState::default()
        },
    );
    let state = PipelineState {
        streams,
        ..PipelineState::default()
    };
    let mut positions = Positions::of(&state);
    assert_eq!(
        positions.get(&stream("orders"), &partition("p0")),
        Some(&at(3))
    );
    assert_eq!(
        positions.get(&stream("orders"), &partition("p1")),
        Some(&PartitionState::Done)
    );
    assert_eq!(positions.get(&stream("users"), &partition("p0")), None);
    positions.apply(&[
        StateChange::Delete(StateKey::Partition(stream("orders"), partition("p1")).encode()),
        put("orders", "p0", at(9)),
        put("users", "p0", at(1)),
        // Records of anything else change no position.
        StateChange::Delete(StateKey::Phase(stream("orders")).encode()),
        StateChange::Put(
            StateEntry::Phase {
                stream: stream("orders"),
                phase: 2,
            }
            .to_record(),
        ),
    ]);
    assert_eq!(
        positions.get(&stream("orders"), &partition("p0")),
        Some(&at(9))
    );
    assert_eq!(positions.get(&stream("orders"), &partition("p1")), None);
    assert_eq!(
        positions.get(&stream("users"), &partition("p0")),
        Some(&at(1))
    );
}

#[test]
fn phases_follow_the_state_they_start_from_and_the_commits_after_it() {
    let phased = |phase| StreamState {
        phase,
        ..StreamState::default()
    };
    let streams = [(stream("orders"), phased(1)), (stream("users"), phased(0))];
    let state = PipelineState {
        streams: streams.into_iter().collect(),
        ..PipelineState::default()
    };
    let mut positions = Positions::of(&state);
    assert_eq!(positions.phase(&stream("orders")), 1);
    assert_eq!(positions.phase(&stream("users")), 0);
    let begun = |name, phase| {
        StateChange::Put(
            StateEntry::Phase {
                stream: stream(name),
                phase,
            }
            .to_record(),
        )
    };
    positions.apply(&[begun("orders", 2), begun("users", 1)]);
    assert_eq!(positions.phase(&stream("orders")), 2);
    assert_eq!(positions.phase(&stream("users")), 1);
    // A reset deletes a stream's phase: it reads from its first again.
    positions.apply(&[StateChange::Delete(
        StateKey::Phase(stream("orders")).encode(),
    )]);
    assert_eq!(positions.phase(&stream("orders")), 0);
    assert_eq!(positions.phase(&stream("users")), 1);
}
