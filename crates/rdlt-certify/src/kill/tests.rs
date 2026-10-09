use std::future::Future;
use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};

use super::destination::{Fault, ROWS, every_row_once};
use super::killing::Schedule;
use super::rows::{Parted, parted, rendered};
use super::source::uncompared;
use super::{DRAWS, Loaded, Proof, drawn, proven, unproven};
use rdlt_connector::testing::render::{RenderError, Rendering};

/// The rows of `batches`, rendered with no limit.
fn rows(batches: &[RecordBatch]) -> Vec<String> {
    let mut rendering = Rendering::new(usize::MAX);
    let mut rendering = std::pin::pin!(rendered(batches, &mut rendering));
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    loop {
        if let std::task::Poll::Ready(rows) = rendering.as_mut().poll(&mut context) {
            return rows.expect("the rows render");
        }
    }
}

fn batch(columns: Vec<(&str, ArrayRef)>) -> RecordBatch {
    RecordBatch::try_from_iter(columns).expect("a valid batch")
}

/// Every id a load generates.
fn generated() -> Vec<i64> {
    (0..i64::try_from(ROWS).expect("few rows")).collect()
}

fn ids(values: &[i64]) -> RecordBatch {
    batch(vec![("id", Arc::new(Int64Array::from(values.to_vec())))])
}

/// Whether `schedules` draw every point of each range, and none outside it: each after a
/// commit that published rows, and early enough that a load committing a few times reaches it.
fn draw_every_point(schedules: &[Schedule]) -> bool {
    let within = schedules.iter().all(|schedule| {
        (2..=3).contains(&schedule.settled)
            && (3..=5).contains(&schedule.commit)
            && schedule
                .answer
                .is_some_and(|answer| (2..=4).contains(&answer))
    });
    let settled =
        (2..=3).all(|settled| schedules.iter().any(|schedule| schedule.settled == settled));
    let commit = (3..=5).all(|commit| schedules.iter().any(|schedule| schedule.commit == commit));
    let answer = (2..=4).all(|answer| {
        schedules
            .iter()
            .any(|schedule| schedule.answer == Some(answer))
    });
    within && settled && commit && answer
}

#[test]
fn seeds_draw_every_point_of_each_range_and_none_beyond() {
    let schedules: Vec<Schedule> = (0..4096)
        .map(|seed| Schedule::seeded(seed * 257, true))
        .collect();
    assert!(draw_every_point(&schedules));
    assert!(
        (0..4096).all(|seed| Schedule::seeded(seed, false).answer.is_none()),
        "a source's loads lose no answer"
    );
}

#[test]
fn a_seed_draws_each_point_from_bits_of_its_own() {
    let drawn = |settled, commit, answer| Schedule {
        settled,
        commit,
        answer,
    };
    assert_eq!(Schedule::seeded(1, true), drawn(3, 3, Some(2)));
    assert_eq!(Schedule::seeded(515, true), drawn(3, 5, Some(2)));
    assert_eq!(Schedule::seeded(0x0002_0100, true), drawn(2, 3, Some(4)));
}

#[test]
fn a_clock_that_ticks_in_microseconds_still_draws_every_point() {
    let schedules: Vec<Schedule> = (0..600_u64)
        .map(|tick| Schedule::seeded(drawn(None, tick * 1000), true))
        .collect();
    assert!(draw_every_point(&schedules));
}

#[test]
fn a_chosen_seed_is_drawn_as_chosen_and_the_clock_mixed() {
    assert_eq!(drawn(Some(515), 7), 515);
    assert_eq!(drawn(None, 0), 0xe220_a839_7b1d_cdaf);
    assert_eq!(drawn(None, 1), 0x910a_2dec_8902_5cc1);
}

#[test]
fn rows_render_by_column_name_without_metadata_whatever_their_batches() {
    let names: ArrayRef = Arc::new(StringArray::from(vec![Some("b"), None]));
    let first = batch(vec![
        ("name", names),
        ("id", Arc::new(Int64Array::from(vec![2, 1]))),
        ("_rdlt_load_id", Arc::new(StringArray::from(vec!["x", "y"]))),
    ]);
    let second = batch(vec![
        ("id", Arc::new(Int64Array::from(vec![2]))),
        ("name", Arc::new(StringArray::from(vec!["b"]))),
    ]);
    assert_eq!(
        rows(&[first, second]),
        ["id=1, name=null", "id=2, name=b", "id=2, name=b"]
    );
}

#[test]
fn an_instant_no_calendar_holds_renders_as_its_integer_instead_of_panicking() {
    // A second no calendar holds once its zone's offset is added.
    let edge = arrow_array::TimestampSecondArray::from(vec![8_210_266_876_799_i64])
        .with_timezone("+14:00");
    let edge = [batch(vec![("at", Arc::new(edge))])];
    let rows = rows(&edge);
    assert_eq!(rows.len(), 1);
    assert!(rows[0].contains("8210266876799"), "{rows:?}");
}

#[test]
fn tables_with_the_same_rows_do_not_part() {
    let rows = vec!["id=1".to_owned(), "id=2".to_owned()];
    assert_eq!(parted(&rows, &rows), None);
    assert_eq!(parted(&[], &[]), None);
}

#[test]
fn tables_part_at_their_first_missing_or_extra_row() {
    let clean = vec!["id=1".to_owned(), "id=2".to_owned(), "id=3".to_owned()];
    let missing = vec!["id=1".to_owned(), "id=3".to_owned()];
    let extra = vec![
        "id=1".to_owned(),
        "id=2".to_owned(),
        "id=2".to_owned(),
        "id=3".to_owned(),
    ];
    assert_eq!(parted(&clean, &missing), Some(Parted::Missing("id=2")));
    assert_eq!(parted(&clean, &extra), Some(Parted::Extra("id=2")));
    assert_eq!(parted(&clean, &clean[..2]), Some(Parted::Missing("id=3")));
    assert_eq!(parted(&clean[..2], &clean), Some(Parted::Extra("id=3")));
    assert_eq!(parted(&missing, &clean), Some(Parted::Extra("id=2")));
}

#[test]
fn every_generated_row_once_is_exactly_once() {
    let all = generated();
    every_row_once(&[ids(&all[..100]), ids(&all[100..])]).expect("each row once");
}

#[test]
fn a_repeated_missing_or_stray_row_is_not_exactly_once() {
    let mut repeated = generated();
    repeated.push(17);
    assert_eq!(every_row_once(&[ids(&repeated)]), Err(Fault::Repeated(17)));
    let missing: Vec<i64> = generated().into_iter().filter(|id| *id != 30).collect();
    assert_eq!(every_row_once(&[ids(&missing)]), Err(Fault::Missing(30)));
    let mut stray = generated();
    stray.push(-1);
    assert_eq!(every_row_once(&[ids(&stray)]), Err(Fault::Stray(-1)));
}

#[test]
fn ids_are_found_in_any_case_and_any_width_of_integer() {
    let narrow: Vec<i32> = (0..i32::try_from(ROWS).expect("few rows")).collect();
    let narrow = batch(vec![(
        "ID",
        Arc::new(arrow_array::Int32Array::from(narrow)),
    )]);
    every_row_once(&[narrow]).expect("ids of any width are integers");
}

#[test]
fn a_table_whose_ids_no_read_back_admits_is_not_exactly_once() {
    let unnamed = batch(vec![("key", Arc::new(Int64Array::from(vec![0])))]);
    let words = batch(vec![("id", Arc::new(StringArray::from(vec!["zero"])))]);
    // Text that spells integers is no column of integers either.
    let spelled = batch(vec![("id", Arc::new(StringArray::from(vec!["0"])))]);
    let nulls = batch(vec![(
        "id",
        Arc::new(Int64Array::from(vec![Some(0), None])),
    )]);
    let item = Arc::new(arrow_schema::Field::new_list_field(
        arrow_schema::DataType::Int64,
        true,
    ));
    let nested: ArrayRef = Arc::new(arrow_array::ListArray::new_null(item, 1));
    let nested = batch(vec![("id", nested)]);
    // More rows than any clause reads of a table, of ids that cost no bytes.
    let nothing: ArrayRef = Arc::new(arrow_array::NullArray::new(1 << 20));
    let flood = batch(vec![("id", nothing)]);
    for unread in [unnamed, words, spelled, nulls, nested, flood] {
        let kind = unread.schema();
        let fault = every_row_once(&[unread]);
        assert!(matches!(fault, Err(Fault::Unread(_))), "{kind}: {fault:?}");
    }
}

#[test]
fn each_fault_reads_apart_and_names_its_row() {
    let faults = [
        Fault::Unread("words".into()),
        Fault::Repeated(17),
        Fault::Missing(17),
        Fault::Stray(17),
    ];
    let read: std::collections::BTreeSet<String> = faults.iter().map(ToString::to_string).collect();
    assert_eq!(read.len(), faults.len(), "{read:?}");
    for fault in &faults[1..] {
        assert!(fault.to_string().contains("17"), "{fault}");
    }
    assert!(faults[0].to_string().contains("words"), "{}", faults[0]);
}

#[test]
fn a_load_proves_something_only_once_a_kill_landed_and_interrupted_it() {
    let unkilled = rdlt_host::Kills::new();
    // A kill that reached nothing: no connection was seen to end after it.
    let unreached = rdlt_host::Kills::new();
    unreached.kill();
    // A kill that ended a connection.
    let landed = rdlt_host::Kills::new();
    let (_peer, stream) = tokio::io::duplex(8);
    let mut cut = landed.sever(stream);
    landed.kill();
    let met = ready(tokio::io::AsyncWriteExt::write_all(&mut cut, b"x"));
    assert!(met.is_err() && landed.landed() == 1);
    let unproven_by = [
        (&unkilled, false),
        (&unkilled, true),
        (&unreached, false),
        // An attempt failed, as one whose answer the clause lost does: no kill landed.
        (&unreached, true),
        (&landed, false),
    ];
    let mut reasons = std::collections::BTreeSet::new();
    for (kills, interrupted) in unproven_by {
        let Some(Loaded::Unseen(reason)) = unproven(kills, interrupted, 9) else {
            panic!("{} kills, interrupted: {interrupted}", kills.count());
        };
        // Each says the seed that drew its kill points.
        assert!(reason.contains('9'), "{reason}");
        reasons.insert(reason);
    }
    assert_eq!(
        reasons.len(),
        3,
        "each cause has a reason of its own: {reasons:?}"
    );
    assert!(unproven(&landed, true, 9).is_none());
    // What landed only on connections this host cut proves less, and says so.
    assert_eq!((landed.landed(), landed.cut()), (1, 1));
    assert_eq!(Proof::of(&landed), Proof::Cut);
    assert_eq!(Proof::seen(1, 1), Proof::Cut);
    assert_eq!(Proof::seen(7, 7), Proof::Cut);
    // One connection that ended without being cut is a connector seen to stop.
    assert_eq!(Proof::seen(1, 0), Proof::Ended);
    assert_eq!(Proof::seen(7, 6), Proof::Ended);
    assert_ne!(Proof::Cut.note(), Proof::Ended.note());
}

/// Runs [`proven`] over loads that `interrupts` says each draw interrupts, returning its outcome and
/// the runs and seeds of the loads it ran.
fn drew(chosen: Option<u64>, interrupts: impl Fn(usize) -> bool) -> (Loaded, Vec<(u64, u64)>) {
    let loads = std::sync::Mutex::new(Vec::new());
    let outcome = ready(proven(chosen, 100, |run, seed| {
        let mut loads = loads.lock().expect("unpoisoned");
        loads.push((run, seed));
        let interrupted = interrupts(loads.len());
        async move {
            if interrupted {
                Loaded::Kept(Proof::Ended)
            } else {
                Loaded::Unseen(format!("kill seed {seed}"))
            }
        }
    }));
    (outcome, loads.into_inner().expect("unpoisoned"))
}

/// The output of `future`, which never waits.
fn ready<F: Future>(future: F) -> F::Output {
    let waker = std::task::Waker::noop();
    match std::pin::pin!(future).poll(&mut std::task::Context::from_waker(waker)) {
        std::task::Poll::Ready(output) => output,
        std::task::Poll::Pending => panic!("the loads never wait"),
    }
}

#[test]
fn a_clause_loads_again_with_new_kill_points_until_a_kill_interrupts_a_load() {
    // The third load is the first a kill interrupts: each is named and killed apart.
    let (outcome, loads) = drew(None, |load| load == 3);
    assert!(matches!(outcome, Loaded::Kept(Proof::Ended)));
    assert_eq!(loads.len(), 3);
    let runs: std::collections::BTreeSet<_> = loads.iter().map(|(run, _)| *run).collect();
    let seeds: std::collections::BTreeSet<_> = loads.iter().map(|(_, seed)| *seed).collect();
    assert_eq!((runs.len(), seeds.len()), (3, 3));
    assert_eq!(loads[0], (100, drawn(None, 100)));
    // A load no kill ever interrupts proves nothing after the last draw.
    let (outcome, loads) = drew(None, |_| false);
    assert!(matches!(outcome, Loaded::Unseen(_)));
    assert_eq!(loads.len(), usize::try_from(DRAWS).expect("few draws"));
    // A chosen seed loads once, as chosen, so its run can be repeated.
    let (outcome, loads) = drew(Some(7), |_| false);
    assert!(matches!(outcome, Loaded::Unseen(_)));
    assert_eq!(loads, [(100, 7)]);
}

#[test]
fn a_destination_is_loaded_in_the_first_mode_it_declares_that_publishes_each_row_once() {
    use rdlt_connector::WriteModes;
    use rdlt_engine::WriteMode;

    use super::destination::written;
    for bits in 0_u8..16 {
        let modes = WriteModes {
            append: bits & 1 != 0,
            merge: bits & 2 != 0,
            replace: bits & 4 != 0,
            history: bits & 8 != 0,
        };
        let expected = match bits {
            _ if modes.append => Some(WriteMode::Append),
            _ if modes.merge => Some(WriteMode::Merge),
            _ if modes.replace => Some(WriteMode::Replace),
            _ => None,
        };
        match (written(modes), expected) {
            (Ok(mode), Some(expected)) => assert_eq!(mode, expected, "{modes:?}"),
            // History alone is loaded by an engine, in a mode the clause cannot check.
            (Err(Loaded::Unobserved(_)), None) => assert!(modes.history, "{modes:?}"),
            // Only a destination no engine loads is left out.
            (Err(Loaded::Inapplicable(_)), None) => assert_eq!(bits, 0, "{modes:?}"),
            _ => panic!("{modes:?}"),
        }
    }
}

#[test]
fn every_stream_an_engine_reads_is_loaded_as_it_reads_it() {
    use rdlt_connector::{ReadMode, StreamName, StreamSpec};
    use rdlt_engine::WriteMode;

    use super::source::planned;
    let stream = |modes: &[ReadMode], keyed: bool| {
        let spec = StreamSpec::new(StreamName::new("events").expect("a valid name"));
        let spec = spec.with_read_modes(modes.iter().copied());
        if keyed {
            spec.with_primary_key(["id"])
        } else {
            spec
        }
    };
    let (full, incremental, changes) = (ReadMode::Full, ReadMode::Incremental, ReadMode::Cdc);
    let cases = [
        (
            stream(&[incremental, full, changes], true),
            incremental,
            WriteMode::Append,
        ),
        (
            stream(&[incremental], false),
            incremental,
            WriteMode::Append,
        ),
        (stream(&[full, changes], true), full, WriteMode::Replace),
        (stream(&[full], false), full, WriteMode::Replace),
        // A stream read only as changes is merged by its key, and appended when it has none.
        (stream(&[changes], true), changes, WriteMode::Merge),
        (stream(&[changes], false), changes, WriteMode::Append),
    ];
    for (spec, read, write) in cases {
        let planned = planned(&spec).expect("a stream that is read is loaded");
        assert_eq!((planned.read_mode(), planned.write_mode()), (read, write));
    }
    assert!(planned(&stream(&[], true)).is_none());
}

/// A source of one partition that checkpoints once, with a cursor of the bytes its
/// configuration says, and ends.
struct Cursors {
    bytes: usize,
}

impl rdlt_connector::Source for Cursors {
    fn check(&self) -> rdlt_connector::BoxFuture<'_, rdlt_connector::Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn discover(
        &self,
    ) -> rdlt_connector::BoxFuture<'_, rdlt_connector::Result<rdlt_connector::Catalog>> {
        Box::pin(async { Ok(rdlt_connector::Catalog::default()) })
    }

    fn plan<'a>(
        &'a self,
        _: &'a rdlt_connector::StreamName,
        _: &'a rdlt_connector::StreamState,
    ) -> rdlt_connector::BoxFuture<'a, rdlt_connector::Result<rdlt_connector::PartitionPlan>> {
        Box::pin(async { Err(rdlt_connector::ConnectorError::internal("nothing to plan")) })
    }

    fn read(
        &self,
        _: rdlt_connector::ReadRequest,
        mut sink: rdlt_connector::PartitionSink,
    ) -> rdlt_connector::BoxFuture<'_, rdlt_connector::Result<()>> {
        let bytes = self.bytes;
        Box::pin(async move {
            let cursor = rdlt_connector::Cursor::new(1, &vec![b'c'; bytes])?;
            let checkpoint = rdlt_connector::SourceEvent::Checkpoint {
                cursor,
                answers: None,
            };
            sink.send(checkpoint).await
        })
    }

    fn committed<'a>(
        &'a self,
        _: &'a rdlt_connector::StreamName,
        _: &'a [(rdlt_connector::PartitionId, rdlt_connector::Cursor)],
    ) -> rdlt_connector::BoxFuture<'a, rdlt_connector::Result<()>> {
        Box::pin(async { Ok(()) })
    }
}

/// Connects [`Cursors`] from `{ "bytes": n }`.
struct CursorsFactory(rdlt_connector::ConnectorSpec);

impl rdlt_connector::SourceFactory for CursorsFactory {
    fn spec(&self) -> &rdlt_connector::ConnectorSpec {
        &self.0
    }

    fn connect(
        &self,
        config: serde_json::Value,
        _: rdlt_connector::ConnectContext,
    ) -> rdlt_connector::BoxFuture<'_, rdlt_connector::Result<Box<dyn rdlt_connector::Source>>>
    {
        Box::pin(async move {
            let bytes = config["bytes"]
                .as_u64()
                .and_then(|bytes| usize::try_from(bytes).ok());
            let bytes = bytes.ok_or_else(|| rdlt_connector::ConnectorError::config("no bytes"))?;
            Ok(Box::new(Cursors { bytes }) as Box<dyn rdlt_connector::Source>)
        })
    }
}

/// Reads a partition of a source of cursors of `bytes`, placed as a kill clause places its
/// target, told `admitted`: how the read ended.
async fn read_through_a_kill_clause(
    admitted: &rdlt_wire::Limits,
    bytes: usize,
) -> rdlt_connector::Result<()> {
    let id = rdlt_connector::ConnectorId::parse("test.cursors").expect("an id");
    let spec = rdlt_connector::ConnectorSpec {
        id: id.clone(),
        version: "0.0.0".to_owned(),
        config_schema: serde_json::json!({}),
    };
    let served = rdlt_connector::serve::Served::new().with_source(Box::new(CursorsFactory(spec)));
    let target = crate::Target::served(served);
    let (provider, reference) = target.provider(&id, &rdlt_host::Kills::new(), admitted);
    let config = serde_json::json!({ "bytes": bytes });
    let placed = provider.source(&reference, &config).await.expect("placed");
    let (sink, mut feed) =
        rdlt_connector::partition_channel(std::num::NonZeroUsize::new(4).expect("not zero"));
    let request = rdlt_connector::ReadRequest::new(
        rdlt_connector::StreamName::new("events").expect("a name"),
        rdlt_connector::Partition::new(rdlt_connector::PartitionId::parse("p0").expect("an id")),
        None,
    );
    let drained = async { while feed.recv().await.is_some() {} };
    let (read, ()) = tokio::join!(placed.connector.read(request, sink), drained);
    read
}

#[tokio::test]
async fn a_kill_clause_tells_its_target_the_limits_its_engine_admits_exactly() {
    let admitted = super::limits().expect("the limits");
    // Those of an engine of the clause's budget: a cursor of sixteen kilobytes, where the wire
    // takes four mebibytes.
    let engine = rdlt_engine::EngineConfig::builder()
        .memory(34 << 20)
        .build()
        .expect("valid");
    assert_eq!(admitted, engine.limits());
    let limit = usize::try_from(admitted.cursor_bytes).expect("a size");
    assert_eq!(limit, 16_384);
    for bytes in [limit - 1, limit] {
        read_through_a_kill_clause(&admitted, bytes)
            .await
            .unwrap_or_else(|error| panic!("{bytes} bytes: {error}"));
    }
    let refused = read_through_a_kill_clause(&admitted, limit + 1)
        .await
        .expect_err("a cursor beyond what the engine admits");
    assert!(refused.to_string().contains("cursor bytes"), "{refused}");
}

#[test]
fn a_table_whose_rows_do_not_render_is_named_with_why_it_is_not_compared() {
    let reason = uncompared("orders", &RenderError::Beyond(10));
    assert_eq!(
        reason,
        "table `orders` cannot be compared: the rows render as more than the 10 bytes of text a \
         comparison holds"
    );
}
