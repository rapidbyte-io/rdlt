use std::collections::BTreeMap;

use std::time::{Duration, UNIX_EPOCH};

use proptest::prelude::*;
use rdlt_connector::{
    Cursor, GenerationId, LoadId, PartitionId, PartitionState, PipelineState, StateChange,
    StateEntry, StateKey, StreamName, StreamState,
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

fn load(seconds: u64) -> LoadId {
    LoadId::from_parts(UNIX_EPOCH + Duration::from_secs(seconds), 1)
}

fn put(name: &str, id: &str, state: PartitionState) -> StateChange {
    recorded(name, id, state, 1)
}

/// The change recording `id` of stream `name` at `state`, by load `seconds`.
fn recorded(name: &str, id: &str, state: PartitionState, seconds: u64) -> StateChange {
    StateChange::Put(
        StateEntry::Partition {
            stream: stream(name),
            partition: partition(id),
            state,
            load: load(seconds),
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

#[test]
fn done_partitions_are_listed_the_earliest_recorded_first_as_commits_move_them() {
    let mut state = PipelineState::default();
    for (id, seconds) in [("old", 1), ("cursor", 2)] {
        let position = if id == "old" {
            PartitionState::Done
        } else {
            at(1)
        };
        state
            .apply(&recorded("orders", id, position, seconds))
            .expect("applies");
    }
    let mut positions = Positions::of(&state);
    positions.apply(&[
        recorded("orders", "newest", PartitionState::Done, 9),
        recorded("events", "middle", PartitionState::Done, 5),
        recorded("orders", "cursor", PartitionState::Done, 7),
    ]);
    let done = |positions: &Positions| -> Vec<(LoadId, String)> {
        positions
            .done()
            .into_iter()
            .map(|(load, _, id)| (load, id.to_string()))
            .collect()
    };
    assert_eq!(
        done(&positions),
        [
            (load(1), "old".to_owned()),
            (load(5), "middle".to_owned()),
            (load(7), "cursor".to_owned()),
            (load(9), "newest".to_owned()),
        ]
    );
    let gone = StateKey::Partition(stream("events"), partition("middle")).encode();
    positions.apply(&[
        StateChange::Delete(gone),
        recorded("orders", "old", at(2), 10),
    ]);
    assert_eq!(
        done(&positions),
        [
            (load(7), "cursor".to_owned()),
            (load(9), "newest".to_owned())
        ]
    );
}

/// The streams a property's changes name.
const STREAMS: [&str; 2] = ["orders", "users"];
/// The partitions a property's changes name, in each stream.
const PARTITIONS: [&str; 3] = ["p0", "p1", "p2"];

/// One change a commit records.
#[derive(Clone, Debug)]
enum Change {
    /// A partition's position, a cursor or done, by the load of `.3` seconds.
    Position(usize, usize, Option<u64>, u64),
    /// A partition's position deleted.
    Unplaced(usize, usize),
    /// A stream's phase.
    Phase(usize, u16),
    /// A stream's phase deleted.
    Unphased(usize),
    /// A record of no position or phase: the load whose commit first reached the destination.
    Other(u64),
    /// A record of no position or phase beside a stream's: the generation it cycles to.
    Cycled(usize, u64),
    /// A stream's generation record deleted, as its cycle completes.
    Uncycled(usize),
}

fn change() -> impl Strategy<Value = Change> {
    let (streams, partitions) = (0..STREAMS.len(), 0..PARTITIONS.len());
    prop_oneof![
        4 => (streams.clone(), partitions.clone(), proptest::option::of(0_u64..9), 1_u64..4)
            .prop_map(|(name, id, next, seconds)| Change::Position(name, id, next, seconds)),
        2 => (streams.clone(), partitions).prop_map(|(name, id)| Change::Unplaced(name, id)),
        1 => (streams.clone(), any::<u16>()).prop_map(|(name, phase)| Change::Phase(name, phase)),
        1 => streams.clone().prop_map(Change::Unphased),
        1 => (1_u64..4).prop_map(Change::Other),
        1 => (streams.clone(), any::<u64>())
            .prop_map(|(name, generation)| Change::Cycled(name, generation)),
        1 => streams.prop_map(Change::Uncycled),
    ]
}

/// The state change `change` is.
fn written(change: &Change) -> StateChange {
    match *change {
        Change::Position(name, id, next, seconds) => {
            let state = next.map_or(PartitionState::Done, at);
            recorded(STREAMS[name], PARTITIONS[id], state, seconds)
        }
        Change::Unplaced(name, id) => StateChange::Delete(
            StateKey::Partition(stream(STREAMS[name]), partition(PARTITIONS[id])).encode(),
        ),
        Change::Phase(name, phase) => StateChange::Put(
            StateEntry::Phase {
                stream: stream(STREAMS[name]),
                phase,
            }
            .to_record(),
        ),
        Change::Unphased(name) => {
            StateChange::Delete(StateKey::Phase(stream(STREAMS[name])).encode())
        }
        Change::Other(seconds) => StateChange::Put(StateEntry::Origin(load(seconds)).to_record()),
        Change::Cycled(name, generation) => StateChange::Put(
            StateEntry::Generation {
                stream: stream(STREAMS[name]),
                generation: GenerationId(generation),
            }
            .to_record(),
        ),
        Change::Uncycled(name) => {
            StateChange::Delete(StateKey::Generation(stream(STREAMS[name])).encode())
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(rdlt_testkit::cases(64)))]

    /// After any commits' changes, each partition stands where the last change naming it put
    /// it, by that change's load, each stream at the last phase recorded or phase 0, done
    /// partitions are listed the earliest recorded first, and applying commits one at a time is
    /// applying them together.
    #[test]
    fn positions_are_what_the_commits_record_in_order(
        commits in proptest::collection::vec(proptest::collection::vec(change(), 0..6), 0..6),
    ) {
        let mut positions = Positions::of(&PipelineState::default());
        let mut placed: BTreeMap<(usize, usize), (PartitionState, LoadId)> = BTreeMap::new();
        let mut phases: BTreeMap<usize, u16> = BTreeMap::new();
        let mut all = Vec::new();
        for commit in &commits {
            let changes: Vec<StateChange> = commit.iter().map(written).collect();
            positions.apply(&changes);
            all.extend(changes);
            for change in commit {
                match *change {
                    Change::Position(name, id, next, seconds) => {
                        let state = next.map_or(PartitionState::Done, at);
                        placed.insert((name, id), (state, load(seconds)));
                    }
                    Change::Unplaced(name, id) => {
                        placed.remove(&(name, id));
                    }
                    Change::Phase(name, phase) => {
                        phases.insert(name, phase);
                    }
                    Change::Unphased(name) => {
                        phases.remove(&name);
                    }
                    Change::Other(_) | Change::Cycled(..) | Change::Uncycled(_) => {}
                }
            }
        }
        for (name, stream_name) in STREAMS.iter().enumerate() {
            for (id, partition_id) in PARTITIONS.iter().enumerate() {
                prop_assert_eq!(
                    positions.get(&stream(stream_name), &partition(partition_id)),
                    placed.get(&(name, id)).map(|(state, _)| state)
                );
            }
            prop_assert_eq!(
                positions.phase(&stream(stream_name)),
                phases.get(&name).copied().unwrap_or(0)
            );
        }
        let mut done: Vec<(LoadId, StreamName, PartitionId)> = placed
            .iter()
            .filter(|(_, (state, _))| *state == PartitionState::Done)
            .map(|((name, id), (_, load))| (*load, stream(STREAMS[*name]), partition(PARTITIONS[*id])))
            .collect();
        done.sort();
        let listed: Vec<(LoadId, StreamName, PartitionId)> = positions
            .done()
            .into_iter()
            .map(|(load, name, id)| (load, name.clone(), id.clone()))
            .collect();
        prop_assert_eq!(listed, done);
        let mut together = Positions::of(&PipelineState::default());
        together.apply(&all);
        prop_assert_eq!(together, positions);
    }
}
