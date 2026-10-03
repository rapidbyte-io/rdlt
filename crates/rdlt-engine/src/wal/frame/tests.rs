use std::sync::Arc;
use std::time::UNIX_EPOCH;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use proptest::prelude::*;
use rdlt_connector::{
    CommitMeta, CommitSeq, Cursor, Epoch, Field, LoadId, LogicalType, PartitionId, PartitionState,
    PipelineId, SchemaVersion, SegmentId, StateChange, StateRecord, StreamName, TablePath,
    TableRef, TableSchema,
};
use rdlt_testkit::drawn::{self, Drawn};
use rdlt_testkit::nested;

use super::{
    Batch, BatchHeader, BegunPhase, Committing, End, Fence, Frame, HEAD, Header, PREAMBLE, Seal,
    Table, VERSION,
};

fn load() -> LoadId {
    LoadId::from_parts(UNIX_EPOCH, 7)
}

fn table() -> TableRef {
    TableRef {
        path: TablePath::new(["orders"]).expect("a valid path"),
        name: "orders".into(),
        version: SchemaVersion(2),
        generation: None,
        merge: None,
    }
}

/// One frame of every kind but a batch's.
fn metadata() -> Vec<Frame> {
    let schema = TableSchema::new(vec![Field::new("id", LogicalType::Int64, false)])
        .expect("a valid schema");
    let meta = CommitMeta {
        load_id: load(),
        commit_seq: CommitSeq::FIRST.next(),
        epoch: Epoch(3),
        segments: [SegmentId(1), SegmentId(4)].into_iter().collect(),
        state_delta: vec![StateChange::Put(StateRecord {
            key: "k".to_owned(),
            value: bytes::Bytes::from_static(b"\x00\xffvalue"),
        })],
        finish_generations: Vec::new(),
        child_tables: Vec::new(),
        drop_tables: Vec::new(),
    };
    vec![
        Frame::Header(Header {
            pipeline: PipelineId::parse("orders").expect("a valid pipeline"),
            load: load(),
            chunk: 4,
            epoch: Epoch(3),
            opened: Some((load(), CommitSeq::FIRST)),
        }),
        Frame::Schema(Table {
            index: 0,
            table: table(),
            schema,
        }),
        Frame::Seal(Seal {
            segment: SegmentId(4),
            stream: StreamName::new("orders").expect("a valid stream"),
            partition: PartitionId::parse("p0").expect("a valid partition"),
            replayable: true,
            phase: 2,
            from: Some(PartitionState::Done),
            state: PartitionState::Cursor(Cursor::new(1, b"{}").expect("a cursor")),
            batches: 3,
            rows: 40,
        }),
        Frame::Begun(BegunPhase {
            stream: StreamName::new("orders").expect("a valid stream"),
            phase: 2,
            changes: vec![StateChange::Delete("stale".to_owned())],
        }),
        Frame::Commit(Box::new(Committing {
            meta,
            seals: 1,
            phases: 1,
        })),
        Frame::Closed,
        Frame::End(End {
            live: vec![1, 3],
            received: vec![CommitSeq::FIRST],
        }),
        Frame::Fence(Fence {
            pipeline: PipelineId::parse("orders").expect("a valid pipeline"),
            load: load(),
            chunk: 5,
        }),
    ]
}

/// The drawn batch as Arrow data.
fn arrow(drawn: &Drawn) -> RecordBatch {
    let (columns, rows) = drawn;
    let arrays: Vec<_> = columns
        .iter()
        .enumerate()
        .map(|(index, (_, shape))| {
            let values: Vec<_> = rows.iter().map(|row| &row[index]).collect();
            drawn::array(shape, &values)
        })
        .collect();
    let fields: Vec<_> = columns
        .iter()
        .zip(&arrays)
        .map(|((name, shape), array)| drawn::field(name, shape, array, true))
        .collect();
    let options = arrow_array::RecordBatchOptions::new().with_row_count(Some(rows.len()));
    RecordBatch::try_new_with_options(Arc::new(Schema::new(fields)), arrays, &options)
        .expect("a drawn batch is valid")
}

/// What a log's batches may hold at the default memory.
fn limits() -> rdlt_wire::Limits {
    super::limits(crate::config::EngineConfig::default().memory().get())
}

/// The frame `bytes` holds, alone.
fn decoded(bytes: &[u8]) -> Frame {
    super::decode(bytes, limits()).expect("the frame decodes")
}

#[test]
fn every_metadata_frame_decodes_as_it_was_written() {
    for frame in metadata() {
        let bytes = frame.encode().expect("the frame encodes");
        assert_eq!(decoded(&bytes), frame);
    }
}

#[test]
fn a_seal_that_does_not_name_its_phase_is_refused() {
    let frame = metadata()
        .into_iter()
        .find(|frame| matches!(frame, Frame::Seal(_)))
        .expect("a seal");
    let bytes = frame.encode().expect("the frame encodes");
    let mut payload: serde_json::Value =
        serde_json::from_slice(&bytes[9..]).expect("a seal's payload is JSON");
    payload
        .as_object_mut()
        .expect("an object")
        .remove("phase")
        .expect("the seal names its phase");
    let payload = serde_json::to_vec(&payload).expect("JSON encodes");
    let lacking = super::framed(bytes[0], &payload).expect("a frame");
    assert!(super::decode(&lacking, limits()).is_err());
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(rdlt_testkit::cases(128)))]

    #[test]
    fn a_batch_of_every_type_and_encoding_decodes_as_it_was_written(
        drawn in drawn::neighbors::batches(),
        segment in any::<u64>(),
    ) {
        for batch in &drawn {
            let frame = Frame::Batch(Batch { segment: SegmentId(segment), table: 3, ordinal: segment, batch: arrow(batch) });
            let bytes = frame.encode().expect("the frame encodes");
            prop_assert_eq!(decoded(&bytes), frame);
        }
    }
}

#[test]
fn a_chunk_s_frames_read_back_and_any_byte_changed_or_cut_is_refused() {
    let frames = metadata();
    let mut chunk = super::preamble().to_vec();
    let mut starts = Vec::new();
    for frame in &frames {
        starts.push(chunk.len());
        chunk.extend_from_slice(&frame.encode().expect("the frame encodes"));
    }
    assert_eq!(super::frames(&chunk, limits()).expect("it reads"), frames);
    // Its kind, length and checksum are checked as its payload is: no flipped bit passes.
    for at in PREAMBLE..chunk.len() {
        for bit in [0x01, 0x80] {
            let mut flipped = chunk.clone();
            flipped[at] ^= bit;
            assert!(super::frames(&flipped, limits()).is_err(), "byte {at}");
        }
    }
    // A chunk cut inside a frame is refused; one cut between frames is the frames before.
    for cut in PREAMBLE..chunk.len() {
        let read = super::frames(&chunk[..cut], limits());
        match starts.iter().position(|start| *start == cut) {
            Some(whole) => assert_eq!(read.expect("whole frames"), frames[..whole]),
            None => assert!(read.is_err(), "cut at {cut}"),
        }
    }
}

#[test]
fn a_frame_of_zeros_is_no_frame() {
    // An empty frame of kind 0 whose checksum is zero, as a region of zeros reads.
    assert!(super::decode(&[0; HEAD], limits()).is_err());
    let empty = super::framed(0, &[]).expect("a frame");
    assert_ne!(
        &empty[5..],
        &[0; 4],
        "an empty frame's checksum is not zero"
    );
}

#[test]
fn a_chunk_of_another_format_or_none_at_all_is_told_from_one_damaged() {
    let preamble = super::preamble();
    assert_eq!(&preamble[..8], b"rdltwal\0");
    assert_eq!(u16::from_le_bytes([preamble[8], preamble[9]]), VERSION);
    super::check_preamble(&preamble).expect("this format");
    for cut in 0..PREAMBLE {
        assert!(super::check_preamble(&preamble[..cut]).is_err());
    }
    for at in 0..PREAMBLE {
        let mut damaged = preamble;
        damaged[at] ^= 0x10;
        let refused = super::check_preamble(&damaged).expect_err("refused");
        assert!(refused.to_string().contains("damaged"), "{refused}");
    }
    // A preamble that holds together, of another version or of no log.
    let resealed = |mut preamble: [u8; PREAMBLE]| {
        let check = crc32c::crc32c(&preamble[..10]);
        preamble[10..].copy_from_slice(&check.to_le_bytes());
        preamble
    };
    let mut older = preamble;
    older[8..10].copy_from_slice(&(VERSION - 1).to_le_bytes());
    let refused = super::check_preamble(&resealed(older)).expect_err("refused");
    assert!(refused.to_string().contains("format 2"), "{refused}");
    let mut other = preamble;
    other[..8].copy_from_slice(b"notalog\0");
    let refused = super::check_preamble(&resealed(other)).expect_err("refused");
    assert!(refused.to_string().contains("no chunk"), "{refused}");
}

#[test]
fn a_frame_whose_checksum_matches_but_whose_payload_does_not_decode_is_an_error() {
    let frame = super::framed(2, b"not json").expect("a frame");
    assert!(super::decode(&frame, limits()).is_err());
    // An unknown kind is refused too, and a closing frame that holds anything.
    for kind in [99, 7, 6] {
        let frame = super::framed(kind, b"{}").expect("a frame");
        assert!(super::decode(&frame, limits()).is_err(), "kind {kind}");
    }
}

/// Arrow data the `wal_log` fuzz target garbled, as when it made a raw Arrow reader panic.
const GARBLED: &[u8] = include_bytes!("garbled_batch.ipc");

/// A batch frame of segment 1 holding `arrow`, a batch of `rows` rows in the wire's framing.
fn batch_frame(arrow: &[u8], rows: u64) -> bytes::Bytes {
    let header = serde_json::to_vec(&BatchHeader {
        segment: SegmentId(1),
        table: 0,
        ordinal: 0,
        rows,
    })
    .expect("a header");
    let mut payload = u32::try_from(header.len())
        .expect("small")
        .to_le_bytes()
        .to_vec();
    payload.extend_from_slice(&header);
    payload.extend_from_slice(arrow);
    super::framed(3, &payload).expect("a frame")
}

#[test]
fn a_batch_whose_arrow_data_does_not_hold_together_is_an_error_not_a_panic() {
    assert!(super::decode(&batch_frame(GARBLED, 1), limits()).is_err());
}

/// `batch` in a batch frame, and the frames the frame's bytes decode to.
fn round_trip(batch: RecordBatch) -> (Frame, Vec<Frame>) {
    let frame = Frame::Batch(Batch {
        segment: SegmentId(1),
        table: 0,
        ordinal: 1,
        batch,
    });
    let bytes = frame.encode().expect("the frame encodes");
    let read = decoded(&bytes);
    (frame, vec![read])
}

#[test]
fn a_batch_beyond_what_a_connector_may_send_reads_back_as_the_engine_logged_it() {
    use arrow_array::{BinaryArray, NullArray};
    // The engine logs what it lowered, which may hold more columns, rows and values, more bytes
    // and longer names than the wire lets a connector send in one frame: as much as a request
    // for lowering holds.
    let columns = 10_001;
    let wide = RecordBatch::try_new(
        Arc::new(Schema::new(
            (0..columns)
                .map(|column| {
                    arrow_schema::Field::new(
                        format!("c{column}"),
                        arrow_schema::DataType::Null,
                        true,
                    )
                })
                .collect::<Vec<_>>(),
        )),
        (0..columns)
            .map(|_| Arc::new(NullArray::new(1)) as arrow_array::ArrayRef)
            .collect(),
    )
    .expect("a wide batch");
    let long = RecordBatch::try_from_iter([(
        "n",
        Arc::new(NullArray::new(1_048_577)) as arrow_array::ArrayRef,
    )])
    .expect("a long batch");
    let large = RecordBatch::try_from_iter([(
        "blob",
        Arc::new(BinaryArray::from_iter_values([vec![7_u8; 48 << 20]])) as arrow_array::ArrayRef,
    )])
    .expect("a large batch");
    let named = RecordBatch::try_from_iter([
        ("n".repeat((64 << 10) + 1), Arc::new(NullArray::new(1)) as _),
        ("m".repeat(4 << 20), Arc::new(NullArray::new(1)) as _),
    ])
    .expect("a batch of long names");
    for batch in [wide, long, large, named] {
        let (frame, read) = round_trip(batch);
        assert_eq!(read, [frame]);
    }
}

#[test]
fn a_logged_batch_beyond_what_a_request_for_lowering_holds_is_refused() {
    use arrow_array::{BinaryArray, NullArray};
    // At 16 MiB of memory a request holds 4 MiB: as many rows and values, and bytes of a frame.
    let limits = super::limits(16 << 20);
    let request = 4 << 20;
    let nulls = |rows: usize| {
        let column = Arc::new(NullArray::new(rows)) as arrow_array::ArrayRef;
        RecordBatch::try_from_iter([("n", column)]).expect("a batch")
    };
    let frame = |batch: RecordBatch| {
        Frame::Batch(Batch {
            segment: SegmentId(1),
            table: 0,
            ordinal: 0,
            batch,
        })
        .encode()
        .expect("the frame encodes")
    };
    super::decode(&frame(nulls(request)), limits).expect("as many rows as a request holds");
    assert!(super::decode(&frame(nulls(request + 1)), limits).is_err());
    let blob = |bytes: usize| {
        let column = Arc::new(BinaryArray::from_iter_values([vec![7_u8; bytes]]));
        RecordBatch::try_from_iter([("b", column as arrow_array::ArrayRef)]).expect("a batch")
    };
    super::decode(&frame(blob(request - (1 << 10))), limits).expect("a frame within a request");
    assert!(super::decode(&frame(blob(request)), limits).is_err());
}

#[test]
fn a_logged_batch_whose_rows_are_forged_is_refused_not_decoded_as_said() {
    use arrow_array::NullArray;
    let rows = 0x0151_5151_usize;
    let column = Arc::new(NullArray::new(rows)) as arrow_array::ArrayRef;
    let batch = RecordBatch::try_from_iter([("n", column)]).expect("a batch");
    let logged = super::arrow::encode(&batch).expect("the batch encodes");
    let said = i64::try_from(rows).expect("fits").to_le_bytes();
    for forged in [-1_i64, i64::MAX, i64::MIN, 1 << 40] {
        // The batch's length, its column's and its column's nulls, as a tamperer rewrites them.
        let mut tampered = logged.clone();
        let mut at = 0;
        while let Some(found) = tampered[at..].windows(8).position(|window| window == said) {
            tampered[at + found..at + found + 8].copy_from_slice(&forged.to_le_bytes());
            at += found + 8;
        }
        let read = super::decode(
            &batch_frame(&tampered, u64::from_le_bytes(forged.to_le_bytes())),
            limits(),
        );
        assert!(read.is_err(), "{forged}");
    }
    // Its header's rows disagreeing with what its data holds.
    let read = super::decode(&batch_frame(&logged, 1), limits());
    assert!(read.is_err());
    let rows = u64::try_from(rows).expect("fits");
    super::decode(&batch_frame(&logged, rows), limits()).expect("as said");
}

#[test]
fn a_batch_whose_buffers_share_bytes_is_refused_whatever_its_size() {
    use arrow_array::Int64Array;
    let values = Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])) as arrow_array::ArrayRef;
    let batch = RecordBatch::try_from_iter([("n", values)]).expect("a batch");
    let logged = super::arrow::encode(&batch).expect("the batch encodes");
    assert_eq!(
        super::arrow::decode(&logged, limits()).expect("it decodes"),
        batch
    );
    // The values' buffer follows the validity's, 64 bytes into the body and 24 bytes long; here
    // it starts with it.
    let described: Vec<u8> = [64_i64, 24]
        .into_iter()
        .flat_map(i64::to_le_bytes)
        .collect();
    let at = logged
        .windows(described.len())
        .position(|window| window == described)
        .expect("the values' buffer is described");
    let mut shared = logged;
    shared[at..at + 8].copy_from_slice(&0_i64.to_le_bytes());
    assert!(super::arrow::decode(&shared, limits()).is_err());
}

#[test]
fn a_batch_of_empty_lists_of_runs_reads_back_as_the_engine_logged_it() {
    use arrow_array::types::Int32Type;
    use arrow_array::{Int32Array, ListArray, RunArray};
    use arrow_buffer::OffsetBuffer;
    // Arrow's writer describes a run ending at zero for the lists' unused items, which its own
    // reader refuses; the log must still replay.
    let runs = RunArray::<Int32Type>::try_new(&vec![2, 5].into(), &Int32Array::from(vec![7, 8]))
        .expect("runs");
    let item = Arc::new(arrow_schema::Field::new(
        "item",
        arrow_array::Array::data_type(&runs).clone(),
        true,
    ));
    let lists = ListArray::new(
        item,
        OffsetBuffer::from_lengths([0, 0, 0]),
        Arc::new(runs),
        None,
    );
    let batch = RecordBatch::try_from_iter([("l", Arc::new(lists) as arrow_array::ArrayRef)])
        .expect("a batch of empty lists");
    let (frame, read) = round_trip(batch);
    assert_eq!(read, [frame]);
}

#[test]
fn a_batch_that_cannot_be_encoded_fails_with_the_encoders_error_as_its_cause() {
    use arrow_array::types::Int8Type;
    use arrow_array::{ArrayRef, DictionaryArray, Int8Array, StringArray};
    // A dictionary whose values are a dictionary: no schema message describes it.
    let tags: ArrayRef = Arc::new(StringArray::from(vec!["a"]));
    let inner = DictionaryArray::<Int8Type>::try_new(Int8Array::from(vec![0]), tags).unwrap();
    let keys = Int8Array::from(vec![0]);
    let outer = DictionaryArray::<Int8Type>::try_new(keys, Arc::new(inner)).unwrap();
    let batch = RecordBatch::try_from_iter([("c", Arc::new(outer) as ArrayRef)]).unwrap();
    let report = super::arrow::encode(&batch).unwrap_err().report();
    assert_eq!(report.message, "encoding a write-ahead log batch");
    assert_eq!(report.causes.len(), 1, "{report:?}");
    assert!(report.causes[0].contains("dictionary"), "{report:?}");
}

#[test]
fn a_commit_frame_takes_little_more_than_the_state_it_records() {
    let cursor = Cursor::new(1, &vec![b'c'; 300_000]).expect("a cursor within the limit");
    let entry = rdlt_connector::StateEntry::Partition {
        stream: StreamName::new("orders").expect("a valid stream"),
        partition: PartitionId::parse("p0").expect("a valid partition"),
        state: PartitionState::Cursor(cursor),
        load: load(),
    };
    let meta = CommitMeta {
        load_id: load(),
        commit_seq: CommitSeq::FIRST,
        epoch: Epoch(1),
        segments: rdlt_connector::SegmentSet::new(),
        state_delta: vec![StateChange::Put(entry.to_record())],
        finish_generations: Vec::new(),
        child_tables: Vec::new(),
        drop_tables: Vec::new(),
    };
    let committing = Committing {
        meta,
        seals: 0,
        phases: 0,
    };
    let frame = Frame::Commit(Box::new(committing.clone()))
        .encode()
        .expect("the frame encodes");
    // The cursor is base64 in its state value, and the value base64 in the frame: under twice
    // the cursor, where a number a byte took five times it.
    assert!(frame.len() < 2 * 300_000, "{} bytes", frame.len());
    assert_eq!(
        super::commit(&committing.meta, 0, 0).expect("the frame encodes"),
        frame
    );
    assert_eq!(decoded(&frame), Frame::Commit(Box::new(committing)));
}

/// The deepest a table's types may nest.
fn limit() -> usize {
    usize::try_from(rdlt_connector::limits::MAX_NESTING_DEPTH).expect("a small limit")
}

/// A table of an id and a column nested `depth` levels deep, as `nesting` says.
fn nested_schema(depth: usize, nesting: nested::Nesting) -> TableSchema {
    TableSchema::new(vec![
        Field::new("id", LogicalType::Int64, false),
        Field::new("c", nested::logical(depth, nesting), true),
    ])
    .expect("a schema within the limit")
}

#[test]
fn a_schema_frame_nested_to_the_limit_reads_back() {
    for nesting in nested::NESTINGS {
        let frame = Frame::Schema(Table {
            index: 0,
            table: table(),
            schema: nested_schema(limit(), nesting),
        });
        let bytes = frame.encode().expect("the frame encodes");
        let read = super::decode(&bytes, limits()).map_err(|error| error.to_string());
        assert_eq!(read, Ok(frame), "{nesting:?}");
    }
}

#[test]
fn a_batch_frame_nested_to_the_limit_reads_back() {
    for nesting in nested::NESTINGS {
        let schema = Arc::new(nested_schema(limit(), nesting).to_arrow());
        let rows = [
            serde_json::json!({ "id": 1, "c": nested::value(limit(), nesting) }),
            serde_json::json!({ "id": 2, "c": null }),
        ];
        let mut decoder = arrow_json::ReaderBuilder::new(Arc::clone(&schema))
            .build_decoder()
            .expect("a decoder of the schema");
        decoder.serialize(&rows).expect("the rows decode");
        let batch = decoder.flush().expect("a batch").expect("rows");
        let (frame, read) = round_trip(batch);
        assert_eq!(read, [frame], "{nesting:?}");
    }
}

/// The JSON pointers of every object in `value`, `at` and below.
fn objects(value: &serde_json::Value, at: String, found: &mut Vec<String>) {
    match value {
        serde_json::Value::Object(fields) => {
            for (name, field) in fields {
                objects(field, format!("{at}/{name}"), found);
            }
            found.push(at);
        }
        serde_json::Value::Array(items) => {
            for (index, item) in items.iter().enumerate() {
                objects(item, format!("{at}/{index}"), found);
            }
        }
        _ => {}
    }
}

#[test]
fn a_frame_with_a_field_this_build_does_not_know_is_refused() {
    for frame in metadata() {
        let bytes = frame.encode().expect("the frame encodes");
        let Ok(payload) = serde_json::from_slice::<serde_json::Value>(&bytes[HEAD..]) else {
            continue;
        };
        let mut found = Vec::new();
        objects(&payload, String::new(), &mut found);
        for pointer in found {
            let mut grown = payload.clone();
            grown
                .pointer_mut(&pointer)
                .and_then(serde_json::Value::as_object_mut)
                .expect("an object")
                .insert("unknown".to_owned(), serde_json::json!(1));
            let grown = serde_json::to_vec(&grown).expect("JSON encodes");
            let framed = super::framed(bytes[0], &grown).expect("a frame");
            assert!(
                super::decode(&framed, limits()).is_err(),
                "{frame:?} with a field at {pointer:?}"
            );
        }
    }
}

#[test]
fn a_frame_lacking_any_member_it_writes_is_refused() {
    use rdlt_testkit::required::every_member_required;
    let mut frames = metadata();
    // Each member that may hold nothing, holding nothing.
    frames.push(Frame::Header(Header {
        pipeline: PipelineId::parse("orders").expect("a valid pipeline"),
        load: load(),
        chunk: 0,
        epoch: Epoch(1),
        opened: None,
    }));
    frames.push(Frame::Seal(Seal {
        segment: SegmentId(5),
        stream: StreamName::with_namespace("public", "orders").expect("a valid stream"),
        partition: PartitionId::parse("p1").expect("a valid partition"),
        replayable: false,
        phase: 0,
        from: None,
        state: PartitionState::Done,
        batches: 0,
        rows: 0,
    }));
    frames.push(Frame::End(End::default()));
    for frame in frames {
        let value = match &frame {
            Frame::Header(header) => serde_json::to_value(header),
            Frame::Seal(seal) => serde_json::to_value(seal),
            Frame::Begun(begun) => serde_json::to_value(begun),
            Frame::Schema(table) => serde_json::to_value(&table.table),
            Frame::End(end) => serde_json::to_value(end),
            Frame::Fence(fence) => serde_json::to_value(fence),
            _ => continue,
        }
        .expect("the frame serializes");
        match frame {
            Frame::Header(_) => every_member_required::<Header>(&value),
            Frame::End(_) => every_member_required::<End>(&value),
            Frame::Fence(_) => every_member_required::<Fence>(&value),
            Frame::Seal(_) => every_member_required::<Seal>(&value),
            Frame::Begun(_) => every_member_required::<BegunPhase>(&value),
            _ => every_member_required::<TableRef>(&value),
        }
    }
    let head = BatchHeader {
        segment: SegmentId(1),
        table: 0,
        ordinal: 3,
        rows: 7,
    };
    every_member_required::<BatchHeader>(&serde_json::to_value(head).expect("serializes"));
    let commit = serde_json::json!({ "meta": metadata().into_iter().find_map(|frame| match frame {
        Frame::Commit(commit) => Some(commit.meta),
        _ => None,
    }), "seals": 1, "phases": 0 });
    every_member_required::<Committing>(&commit);
}
