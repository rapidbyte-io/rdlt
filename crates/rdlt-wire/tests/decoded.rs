//! What a message decodes to is no more than its scan counts, measured on the heap, for the
//! shapes of message a hostile peer may send and the shapes a connector sends of its data.

#![forbid(unsafe_code)]

use bytes::Bytes;
use rdlt_wire::prost::Message;
use rdlt_wire::scan::{Form, decoded, request, response};
use rdlt_wire::v1;

#[global_allocator]
static HEAP: peak_alloc::PeakAlloc = peak_alloc::PeakAlloc;

/// Entries of a repeated field: one past a power of two, so a vector holds twice as many as it
/// needs beside the half it grew from.
const ENTRIES: usize = (1 << 14) + 1;

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
