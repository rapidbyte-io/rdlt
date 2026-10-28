use proptest::prelude::*;
use rdlt_connector::{
    Cursor, Epoch, GenerationId, PartitionId, PartitionState, PipelineState, StateChange,
    StateEntry, StateKey, StreamName,
};
use std::collections::BTreeMap;

use super::{DoneMarkers, Recorded};

fn stream(name: &str) -> StreamName {
    StreamName::new(name).expect("a valid stream")
}

fn partition(id: &str) -> PartitionId {
    PartitionId::parse(id).expect("a valid partition")
}

fn at(next: u64) -> PartitionState {
    PartitionState::Cursor(Cursor::encode(1, &next).expect("a cursor"))
}

/// The change recording `id` of stream `name` at `state`, by the session of epoch `number`.
fn recorded(name: &str, id: &str, state: PartitionState, number: u64) -> StateChange {
    StateChange::Put(
        StateEntry::Partition {
            stream: stream(name),
            partition: partition(id),
            state,
            epoch: Epoch(number),
        }
        .to_record(),
    )
}

/// Lands `delta` as a commit of another session's, whose states the markers decode.
fn decoded(markers: &mut DoneMarkers, delta: &[StateChange]) {
    let none = BTreeMap::new();
    markers.landed(
        delta,
        &Recorded {
            epoch: Epoch(0),
            states: &none,
        },
    );
}

/// Each marker, by epoch and partition id.
fn listed(markers: &DoneMarkers) -> Vec<(Epoch, String)> {
    markers
        .listed()
        .into_iter()
        .map(|(epoch, _, id)| (epoch, id.to_string()))
        .collect()
}

#[test]
fn done_markers_follow_the_state_they_start_from_and_the_commits_after_it() {
    let mut state = PipelineState::default();
    for (id, number) in [("old", 1), ("cursor", 2)] {
        let position = if id == "old" {
            PartitionState::Done
        } else {
            at(1)
        };
        state
            .apply(&recorded("orders", id, position, number))
            .expect("applies");
    }
    let mut markers = DoneMarkers::from(&state);
    decoded(
        &mut markers,
        &[
            recorded("orders", "newest", PartitionState::Done, 9),
            recorded("events", "middle", PartitionState::Done, 5),
            recorded("orders", "cursor", PartitionState::Done, 7),
        ],
    );
    assert_eq!(
        listed(&markers),
        [
            (Epoch(1), "old".to_owned()),
            (Epoch(5), "middle".to_owned()),
            (Epoch(7), "cursor".to_owned()),
            (Epoch(9), "newest".to_owned()),
        ]
    );
    let gone = StateKey::Partition(stream("events"), partition("middle")).encode();
    decoded(
        &mut markers,
        &[
            StateChange::Delete(gone),
            recorded("orders", "old", at(2), 10),
        ],
    );
    assert_eq!(
        listed(&markers),
        [
            (Epoch(7), "cursor".to_owned()),
            (Epoch(9), "newest".to_owned())
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
    /// A partition's position, a cursor or done, by the session of epoch `.3`.
    Position(usize, usize, Option<u64>, u64),
    /// A partition's position deleted.
    Unplaced(usize, usize),
    /// A record of no position: a stream's phase.
    Phase(usize, u16),
    /// A record of no position: the generation a stream cycles to.
    Cycled(usize, u64),
    /// A record of no position deleted: a stream's generation, as its cycle completes.
    Uncycled(usize),
}

fn change() -> impl Strategy<Value = Change> {
    let (streams, partitions) = (0..STREAMS.len(), 0..PARTITIONS.len());
    prop_oneof![
        4 => (streams.clone(), partitions.clone(), proptest::option::of(0_u64..9), 1_u64..4)
            .prop_map(|(name, id, next, number)| Change::Position(name, id, next, number)),
        2 => (streams.clone(), partitions).prop_map(|(name, id)| Change::Unplaced(name, id)),
        1 => (streams.clone(), any::<u16>()).prop_map(|(name, phase)| Change::Phase(name, phase)),
        1 => (streams.clone(), any::<u64>())
            .prop_map(|(name, generation)| Change::Cycled(name, generation)),
        1 => streams.prop_map(Change::Uncycled),
    ]
}

/// The state change `change` is.
fn written(change: &Change) -> StateChange {
    match *change {
        Change::Position(name, id, next, number) => {
            let state = next.map_or(PartitionState::Done, at);
            recorded(STREAMS[name], PARTITIONS[id], state, number)
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

    /// After any commits' changes, the partitions held done are those the last change naming
    /// each left done, by that change's epoch, listed the earliest recorded first, and landing
    /// commits one at a time is landing them together.
    #[test]
    fn done_markers_are_what_the_commits_record_in_order(
        commits in proptest::collection::vec(proptest::collection::vec(change(), 0..6), 0..6),
    ) {
        let mut markers = DoneMarkers::default();
        let mut placed: BTreeMap<(usize, usize), (PartitionState, Epoch)> = BTreeMap::new();
        let mut all = Vec::new();
        for commit in &commits {
            let changes: Vec<StateChange> = commit.iter().map(written).collect();
            decoded(&mut markers, &changes);
            all.extend(changes);
            for change in commit {
                match *change {
                    Change::Position(name, id, next, number) => {
                        let state = next.map_or(PartitionState::Done, at);
                        placed.insert((name, id), (state, Epoch(number)));
                    }
                    Change::Unplaced(name, id) => {
                        placed.remove(&(name, id));
                    }
                    Change::Phase(..) | Change::Cycled(..) | Change::Uncycled(_) => {}
                }
            }
        }
        let mut done: Vec<(Epoch, StreamName, PartitionId)> = placed
            .iter()
            .filter(|(_, (state, _))| *state == PartitionState::Done)
            .map(|((name, id), (_, epoch))| (*epoch, stream(STREAMS[*name]), partition(PARTITIONS[*id])))
            .collect();
        done.sort();
        let listed: Vec<(Epoch, StreamName, PartitionId)> = markers
            .listed()
            .into_iter()
            .map(|(epoch, name, id)| (epoch, name.clone(), id.clone()))
            .collect();
        prop_assert_eq!(listed, done);
        let mut together = DoneMarkers::default();
        decoded(&mut together, &all);
        prop_assert_eq!(together, markers);
    }

    /// A session's commit, landed from the states it was made from, leaves the markers its records
    /// decode to, whatever stood before and however often it names a partition.
    #[test]
    fn a_commit_landed_from_its_states_marks_what_its_records_say(
        before in proptest::collection::vec(change(), 0..6),
        commit in proptest::collection::vec(change(), 0..8),
    ) {
        let ours: Vec<Change> = commit
            .iter()
            .map(|change| match change {
                Change::Position(name, id, next, _) => Change::Position(*name, *id, *next, 1),
                other => other.clone(),
            })
            .collect();
        let mut states = BTreeMap::new();
        for change in &ours {
            if let Change::Position(name, id, next, _) = change {
                let state = next.map_or(PartitionState::Done, at);
                states.insert((stream(STREAMS[*name]), partition(PARTITIONS[*id])), state);
            }
        }
        let delta: Vec<StateChange> = ours.iter().map(written).collect();
        let mut from_records = DoneMarkers::default();
        decoded(&mut from_records, &before.iter().map(written).collect::<Vec<_>>());
        let mut from_states = from_records.clone();
        decoded(&mut from_records, &delta);
        from_states.landed(&delta, &Recorded { epoch: Epoch(1), states: &states });
        prop_assert_eq!(from_states, from_records);
    }
}

#[test]
fn landing_a_commit_copies_none_of_its_cursors() {
    let cursor = Cursor::new(1, &vec![b'c'; 1 << 20]).unwrap();
    let state = PartitionState::Cursor(cursor);
    let delta = vec![recorded("orders", "p0", state.clone(), 1)];
    let states = BTreeMap::from([((stream("orders"), partition("p0")), state)]);
    let made = Recorded {
        epoch: Epoch(1),
        states: &states,
    };
    let (peak, markers) = crate::cost::tests::peak_alone(|| {
        let mut markers = DoneMarkers::default();
        markers.landed(&delta, &made);
        markers
    });
    assert!(
        peak < 4 << 10,
        "landing the commit allocated {peak} bytes at its peak"
    );
    assert!(markers.listed().is_empty());
}
