use std::collections::BTreeMap;
use std::time::UNIX_EPOCH;

use proptest::prelude::*;
use rdlt_wire::prost::Message as _;

use super::super::v1;
use super::{answer_bytes, commit_bytes, counted, record_bytes};
use crate::commit::CommitMeta;
use crate::cursor::Cursor;
use crate::id::{CommitSeq, Epoch, LoadId, PartitionId, SegmentId, StreamName};
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

proptest! {
    #[test]
    fn records_hold_in_an_open_s_answer_what_their_measures_sum_to(records in records()) {
        let answer = v1::OpenResponse {
            session: u64::MAX,
            epoch: u64::MAX,
            state: records.iter().map(v1::StateRecord::from).collect(),
        };
        let scanned = counted(rdlt_wire::scan::response("Open"), &answer.encode_to_vec());
        let measured: u64 = records.iter().map(record_bytes).sum::<u64>() + answer_bytes(&[]);
        prop_assert_eq!(answer_bytes(&records), measured);
        // An answer's handle and epoch hold nothing decoded beyond their fields.
        prop_assert_eq!(scanned, measured);
        // What a message holds decoded is never less than what it takes on the wire.
        prop_assert!(u64::try_from(answer.encoded_len()).unwrap() <= scanned);
    }

    #[test]
    fn a_commit_s_request_takes_what_its_measure_says(records in records(), seq in 1_u64..1 << 40) {
        let meta = CommitMeta {
            load_id: LoadId::from_parts(UNIX_EPOCH, 3),
            commit_seq: (1..seq.min(64)).fold(CommitSeq::FIRST, |seq, _| seq.next()),
            epoch: Epoch(seq),
            segments: [SegmentId(seq)].into_iter().collect(),
            state_delta: records.into_iter().map(StateChange::Put).collect(),
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
    fn a_stream_s_positions_take_less_in_a_plan_or_a_report_than_stored(
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
        let stored: u64 = positions
            .iter()
            .map(|(partition, state)| {
                let entry = StateEntry::Partition {
                    stream: stream.clone(),
                    partition: partition.clone(),
                    state: state.clone(),
                    load: LoadId::from_parts(UNIX_EPOCH, 1),
                };
                record_bytes(&entry.to_record())
            })
            .sum();
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
