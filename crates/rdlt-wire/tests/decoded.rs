//! What a message decodes to is no more than its scan counts, measured on the heap, for the
//! shapes of message a hostile peer may send and the shapes a connector sends of its data; and
//! what the data plane holds decoding a batch.

#![forbid(unsafe_code)]

#[path = "decoded/plane.rs"]
mod plane;

use bytes::Bytes;
use proptest::prelude::*;
use rdlt_wire::prost::Message;
use rdlt_wire::scan::{Form, decoded, differential, request, response};
use rdlt_wire::v1;

#[global_allocator]
static HEAP: peak_alloc::PeakAlloc = peak_alloc::PeakAlloc;

/// Entries of a repeated field: one past a power of two, so a vector holds twice as many as it
/// needs beside the half it grew from.
const ENTRIES: usize = (1 << 14) + 1;

/// What `call`, run once, held on the heap at its peak.
fn peak_of(call: &mut dyn FnMut()) -> usize {
    HEAP.reset_peak_usage();
    let before = HEAP.current_usage();
    call();
    HEAP.peak_usage().saturating_sub(before)
}

/// What decoding `message` as `M` holds at its peak, against what the scan of it as `form`
/// counts.
fn measured<M: Message + Default>(form: &Form, message: &impl Message) -> (usize, usize) {
    let bytes = Bytes::from(message.encode_to_vec());
    let counted = decoded(form, &bytes, usize::MAX).expect("the message scans");
    HEAP.reset_peak_usage();
    let before = HEAP.current_usage();
    let held = M::decode(bytes.clone()).expect("the message decodes");
    let peak = HEAP.peak_usage().saturating_sub(before);
    drop(held);
    (peak, counted)
}

/// Checks that the scan counts no less than `message` holds decoded as `M`, and not much more.
fn holds<M: Message + Default>(form: &Form, message: &impl Message) {
    let (peak, counted) = measured::<M>(form, message);
    assert!(
        peak <= counted,
        "{}: holds {peak}, counted {counted}",
        form.name
    );
    assert!(
        counted <= 4 * peak + 4096,
        "{}: holds {peak}, counted {counted}",
        form.name
    );
}

fn answer(method: &str) -> &'static Form {
    response(method).expect("a method of the service")
}

fn asked(method: &str) -> &'static Form {
    request(method).expect("a method of the service")
}

fn name(name: &str) -> v1::StreamName {
    v1::StreamName {
        namespace: None,
        name: name.to_owned(),
    }
}

#[test]
fn a_catalog_of_empty_streams_is_counted_as_it_decodes() {
    let catalog = v1::Catalog {
        streams: vec![v1::StreamSpec::default(); ENTRIES],
    };
    holds::<v1::Catalog>(answer("Discover"), &catalog);
}

#[test]
fn a_catalog_of_typed_columns_is_counted_as_it_decodes() {
    let node = v1::TypeNode {
        name: "item".to_owned(),
        nullable: true,
        kind: Some(v1::type_node::Kind::Int64(v1::Unit {})),
    };
    let field = |index: usize| v1::Field {
        name: format!("column_{index}"),
        r#type: Some(v1::LogicalType {
            nodes: vec![node.clone(); 3],
        }),
        nullable: index.is_multiple_of(2),
    };
    let stream = |index: usize| v1::StreamSpec {
        name: Some(name(&format!("stream_{index}"))),
        schema: Some(v1::TableSchema {
            fields: (0..33).map(field).collect(),
        }),
        read_modes: vec![1, 2],
        ..v1::StreamSpec::default()
    };
    let catalog = v1::Catalog {
        streams: (0..129).map(stream).collect(),
    };
    holds::<v1::Catalog>(answer("Discover"), &catalog);
}

#[test]
fn a_plan_of_empty_starts_and_of_named_partitions_is_counted_as_it_decodes() {
    let empty = v1::PlanResponse {
        starts: vec![v1::PartitionState::default(); ENTRIES],
        ..v1::PlanResponse::default()
    };
    holds::<v1::PlanResponse>(answer("Plan"), &empty);
    let named = v1::PlanResponse {
        partitions: (0..ENTRIES).map(|index| format!("p{index}")).collect(),
        starts: (0..ENTRIES)
            .map(|index| v1::PartitionState {
                partition: format!("p{index}"),
                state: Some(v1::partition_state::State::Cursor(v1::Cursor {
                    version: 1,
                    bytes: Bytes::from(index.to_le_bytes().to_vec()),
                })),
            })
            .collect(),
        ..v1::PlanResponse::default()
    };
    holds::<v1::PlanResponse>(answer("Plan"), &named);
}

#[test]
fn an_opened_state_is_counted_as_it_decodes() {
    let empty = v1::OpenResponse {
        state: vec![v1::StateRecord::default(); ENTRIES],
        ..v1::OpenResponse::default()
    };
    holds::<v1::OpenResponse>(answer("Open"), &empty);
    let records = v1::OpenResponse {
        state: (0..ENTRIES)
            .map(|index| v1::StateRecord {
                key: format!("{{\"partition\":[\"s\",\"p{index}\"]}}"),
                value: Bytes::from(vec![b'x'; 40]),
            })
            .collect(),
        ..v1::OpenResponse::default()
    };
    holds::<v1::OpenResponse>(answer("Open"), &records);
}

#[test]
fn a_commit_of_empty_child_tables_or_changes_is_counted_as_it_decodes() {
    let children = v1::CommitRequest {
        session: 1,
        meta: Some(v1::CommitMeta {
            child_tables: vec![v1::ChildTable::default(); ENTRIES],
            ..v1::CommitMeta::default()
        }),
    };
    holds::<v1::CommitRequest>(asked("Commit"), &children);
    let changes = v1::CommitRequest {
        session: 1,
        meta: Some(v1::CommitMeta {
            abandoned: Vec::new(),
            state_delta: vec![v1::StateChange::default(); ENTRIES],
            ..v1::CommitMeta::default()
        }),
    };
    holds::<v1::CommitRequest>(asked("Commit"), &changes);
}

#[test]
fn a_report_of_committed_positions_is_counted_as_it_decodes() {
    let report = v1::CommittedRequest {
        stream: Some(name("s")),
        cursors: (0..ENTRIES)
            .map(|index| v1::CommittedCursor {
                partition: format!("p{index}"),
                cursor: None,
            })
            .collect(),
    };
    holds::<v1::CommittedRequest>(asked("Committed"), &report);
}

#[test]
fn entries_whose_vectors_hold_one_each_are_counted_as_they_decode() {
    // Streams of one column of one node, of one cursor field of one empty segment, and of one
    // read mode: each vector holds the room of its first growth for one entry.
    let node = v1::TypeNode {
        kind: Some(v1::type_node::Kind::Int64(v1::Unit {})),
        ..v1::TypeNode::default()
    };
    let column = v1::StreamSpec {
        schema: Some(v1::TableSchema {
            fields: vec![v1::Field {
                r#type: Some(v1::LogicalType { nodes: vec![node] }),
                ..v1::Field::default()
            }],
        }),
        ..v1::StreamSpec::default()
    };
    let segment = v1::StreamSpec {
        cursor_fields: vec![v1::ColumnPath {
            segments: vec![String::new()],
        }],
        ..v1::StreamSpec::default()
    };
    let mode = v1::StreamSpec {
        read_modes: vec![1],
        ..v1::StreamSpec::default()
    };
    for stream in [column, segment, mode] {
        let catalog = v1::Catalog {
            streams: vec![stream; 1 << 16 | 1],
        };
        holds::<v1::Catalog>(answer("Discover"), &catalog);
    }
}

/// An encoding of fields of numbers 1 to 15 of every wire type, nesting as deep as `depth`.
fn encoding(depth: u32) -> impl Strategy<Value = Vec<u8>> {
    let number = 1_u8..16;
    let leaf = prop_oneof![
        (number.clone(), any::<u64>()).prop_map(|(number, value)| {
            let mut field = vec![number << 3];
            let mut value = value % 300;
            while value >= 0x80 {
                field.push(u8::try_from(value & 0x7f).expect("seven bits") | 0x80);
                value >>= 7;
            }
            field.push(u8::try_from(value).expect("seven bits"));
            field
        }),
        number
            .clone()
            .prop_map(|number| [vec![number << 3 | 1], vec![0; 8]].concat()),
        number
            .clone()
            .prop_map(|number| [vec![number << 3 | 5], vec![0; 4]].concat()),
        (
            number.clone(),
            proptest::collection::vec(any::<u8>(), 0..12)
        )
            .prop_map(|(number, bytes)| delimited(number, &bytes)),
    ];
    let fields = proptest::collection::vec(leaf, 0..6).prop_map(|fields| fields.concat());
    fields.prop_recursive(depth, 64, 6, move |inner| {
        let number = 1_u8..16;
        prop_oneof![
            (number.clone(), inner.clone()).prop_map(|(number, nested)| delimited(number, &nested)),
            (number, inner.clone()).prop_map(|(number, nested)| {
                [vec![number << 3 | 3], nested, vec![number << 3 | 4]].concat()
            }),
            proptest::collection::vec(inner, 1..4).prop_map(|fields| fields.concat()),
        ]
    })
}

/// A length-delimited field `number` of `payload`.
fn delimited(number: u8, payload: &[u8]) -> Vec<u8> {
    let length = u8::try_from(payload.len().min(127)).expect("seven bits");
    [
        &[number << 3 | 2, length][..],
        &payload[..usize::from(length)],
    ]
    .concat()
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 2048, ..ProptestConfig::default() })]

    #[test]
    fn whatever_a_message_decodes_from_the_scan_takes_counting_no_less_than_it_holds(
        bytes in encoding(4),
        which in 0_usize..64,
    ) {
        let decoding = differential::decoding(which, &bytes, &peak_of).unwrap();
        if decoding.decoded {
            let counted = decoding.counted;
            prop_assert!(counted.is_ok(), "{}: {:?} for {:02x?}", decoding.form.name, counted, bytes);
            let counted = counted.unwrap();
            prop_assert!(decoding.held <= counted, "{}: held {}, counted {} for {:02x?}", decoding.form.name, decoding.held, counted, bytes);
        }
    }
}

#[test]
fn every_message_the_calls_carry_is_held_to_the_scan() {
    assert_eq!(differential::count(), 29);
}
