use std::collections::BTreeMap;

use proptest::prelude::*;
use rdlt_wire::prost::Message as _;

use super::super::v1;
use super::{answer_bytes, commit_bytes, counted, record_bytes};
use crate::commit::{CommitKey, CommitMeta};
use crate::cursor::Cursor;
use crate::id::{Epoch, PartitionId, SegmentId, StreamName};
use crate::state::{PartitionState, StateChange, StateEntry, StateRecord, StreamState};

fn records() -> impl Strategy<Value = Vec<StateRecord>> {
    let record = (".{0,300}", proptest::collection::vec(any::<u8>(), 0..300));
    proptest::collection::vec(record, 0..20).prop_map(|records| {
        records
            .into_iter()
            .map(|(key, value)| StateRecord {
                key,
                value: value.into(),
            })
            .collect()
    })
}

fn changes() -> impl Strategy<Value = Vec<StateChange>> {
    let delete = ".{0,300}".prop_map(StateChange::Delete);
    let put = records().prop_map(|records| {
        records
            .into_iter()
            .map(StateChange::Put)
            .collect::<Vec<_>>()
    });
    (put, proptest::collection::vec(delete, 0..6)).prop_map(|(mut puts, deletes)| {
        puts.extend(deletes);
        puts
    })
}

proptest! {
    #[test]
    fn records_hold_in_an_open_s_answer_what_their_measures_sum_to(records in records()) {
        let answer = v1::OpenResponse {
            session: u64::MAX,
            epoch: u64::MAX,
            landed: Some(v1::Receipt {
                key: Some(v1::CommitKey { epoch: u64::MAX, seq: u64::MAX }),
                committed_at: Some(v1::Instant { seconds: i64::MAX, nanos: 999_999_999 }),
                rows: u64::MAX,
                bytes: u64::MAX,
            }),
            state: records.iter().map(v1::StateRecord::from).collect(),
        };
        let scanned = counted(rdlt_wire::scan::response("Open"), &answer.encode_to_vec());
        let measured: u64 = records.iter().map(record_bytes).sum::<u64>() + answer_bytes(&[]);
        prop_assert_eq!(answer_bytes(&records), measured);
        // An answer's handle, epoch and landed receipt hold nothing decoded beyond their fields.
        prop_assert_eq!(scanned, measured);
        // What a message holds decoded is never less than what it takes on the wire.
        prop_assert!(u64::try_from(answer.encoded_len()).unwrap() <= scanned);
    }

    #[test]
    fn a_commit_s_request_takes_what_its_measure_says(changes in changes(), seq in 1_u64..1 << 40) {
        let meta = CommitMeta {
            key: CommitKey { epoch: Epoch(seq), seq },
            after: Some(CommitKey { epoch: Epoch(seq), seq: seq - 1 }),
            segments: [SegmentId(seq)].into_iter().collect(),
            abandoned: crate::SegmentSet::new(),
            state_delta: changes,
            finish_generations: Vec::new(),
            child_tables: Vec::new(),
            drop_tables: Vec::new(),
        };
        let request = v1::CommitRequest { session: 1, meta: Some(v1::CommitMeta::from(&meta)) };
        let scanned = counted(rdlt_wire::scan::request("Commit"), &request.encode_to_vec());
        // Its session's handle holds what a number does decoded, whichever it is.
        prop_assert_eq!(commit_bytes(&meta), scanned);
        prop_assert!(u64::try_from(request.encoded_len()).unwrap() <= scanned);
    }

    #[test]
    fn a_stream_s_positions_take_no_more_in_a_plan_or_a_report_than_in_an_open_s_answer(
        cursors in proptest::collection::vec(proptest::collection::vec(any::<u8>(), 0..400), 1..20),
    ) {
        let stream = StreamName::new("events").unwrap();
        let positions: BTreeMap<PartitionId, PartitionState> = cursors
            .into_iter()
            .enumerate()
            .map(|(index, bytes)| {
                let cursor = Cursor::new(1, &bytes).unwrap();
                (PartitionId::parse(format!("p{index}")).unwrap(), PartitionState::Cursor(cursor))
            })
            .collect();
        let records: Vec<StateRecord> = positions
            .iter()
            .map(|(partition, state)| {
                let entry = StateEntry::Partition {
                    stream: stream.clone(),
                    partition: partition.clone(),
                    state: state.clone(),
                    epoch: Epoch(1),
                };
                entry.to_record()
            })
            .collect();
        // What the state limit holds: an open's answer carrying the stream's records.
        let stored = answer_bytes(&records);
        let state = StreamState { partitions: positions.clone(), ..StreamState::default() };
        let plan = v1::PlanRequest {
            stream: Some(v1::StreamName::from(&stream)),
            state: Some(v1::StreamState::from(&state)),
        };
        let report = v1::CommittedRequest {
            stream: Some(v1::StreamName::from(&stream)),
            cursors: positions
                .iter()
                .filter_map(|(partition, state)| match state {
                    PartitionState::Cursor(cursor) => Some(v1::CommittedCursor {
                        partition: partition.to_string(),
                        cursor: Some(v1::Cursor::from(cursor)),
                    }),
                    PartitionState::Done => None,
                })
                .collect(),
        };
        let plan = counted(rdlt_wire::scan::request("Plan"), &plan.encode_to_vec());
        let report = counted(rdlt_wire::scan::request("Committed"), &report.encode_to_vec());
        prop_assert!(plan <= stored, "{plan} > {stored}");
        prop_assert!(report <= stored, "{report} > {stored}");
    }
}
