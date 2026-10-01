//! Sources that send more than a clause holds, rows that cost no bytes, and instants no calendar
//! holds: certification bounds what it holds and renders, and never panics.

use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use arrow_array::{ArrayRef, BinaryArray, NullArray, RecordBatch, TimestampSecondArray};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::json;

use super::failed;
use crate::catalog::{Checkpointing, StreamSpec};
use crate::emitter::Emitter;
use crate::error::Result;
use crate::id::{PartitionId, StreamName};
use crate::source::{Partition, ReadStream, SourceConnector, Streams};
use crate::spec::ConnectContext;
use crate::state::StreamState;
use crate::testing::limits::{HELD_BYTES, HELD_ROWS};
use crate::testing::{Outcome, Verdict, certify_source};

/// The most pushes any one read of a flood had taken, by the flood's name.
static TAKEN: LazyLock<Mutex<BTreeMap<String, u64>>> = LazyLock::new(Mutex::default);

fn took(name: &str, pushes: u64) {
    let mut taken = TAKEN.lock().expect("unpoisoned");
    let most = taken.entry(name.to_owned()).or_default();
    *most = (*most).max(pushes);
}

fn taken(name: &str) -> u64 {
    let taken = TAKEN.lock().expect("unpoisoned");
    taken.get(name).copied().unwrap_or_default()
}

#[derive(Default, Deserialize, JsonSchema)]
#[serde(default)]
struct FloodConfig {
    /// The name its reads are counted under, apart from every other test's.
    name: String,
    /// What each push holds: `wide`, a row of a mebibyte; `nulls`, a million rows of
    /// nothing; `edge`, an instant no calendar holds once its zone's offset is added.
    pushes: String,
    /// How many pushes a read sends, unless it is stopped.
    count: u64,
    /// How many partitions it plans.
    partitions: u32,
    /// Whether a checkpoint follows each push.
    checkpoints: bool,
}

struct Flood(FloodConfig);

impl SourceConnector for Flood {
    const ID: &'static str = "io.test.flood";
    const VERSION: &'static str = "0.0.1";
    type Config = FloodConfig;

    async fn connect(config: FloodConfig, _context: &ConnectContext) -> Result<Self> {
        Ok(Self(config))
    }

    async fn check(&self) -> Result<()> {
        Ok(())
    }

    fn streams(&self) -> Streams<Self> {
        Streams::new().with(Pushes)
    }
}

struct Pushes;

impl ReadStream<Flood> for Pushes {
    type Cursor = u64;

    fn spec(&self) -> StreamSpec {
        StreamSpec::new(StreamName::new("flood").unwrap())
            .with_checkpointing(Checkpointing::Natural)
    }

    async fn partitions(&self, source: &Flood, _state: &StreamState) -> Result<Vec<Partition>> {
        let named = |index: u32| Partition::new(PartitionId::parse(format!("p{index}")).unwrap());
        Ok((0..source.0.partitions.max(1)).map(named).collect())
    }

    async fn read(
        &self,
        source: &Flood,
        _partition: &Partition,
        cursor: u64,
        out: &mut Emitter<u64>,
    ) -> Result<()> {
        let column: ArrayRef = match source.0.pushes.as_str() {
            "wide" => Arc::new(BinaryArray::from_iter_values([vec![7_u8; 1 << 20]])),
            "nulls" => Arc::new(NullArray::new(1 << 20)),
            _ => Arc::new(
                TimestampSecondArray::from(vec![8_210_266_876_799_i64]).with_timezone("+14:00"),
            ),
        };
        let batch = RecordBatch::try_from_iter([("value", column)]).unwrap();
        for push in cursor..source.0.count {
            out.batch(batch.clone()).await?;
            took(&source.0.name, push + 1 - cursor);
            if source.0.checkpoints {
                out.checkpoint(&(push + 1)).await?;
            }
        }
        Ok(())
    }
}

/// The events a read may run ahead of the clause that reads it: the channel between them.
const AHEAD: u64 = 66;

#[tokio::test]
async fn a_source_that_sends_more_than_a_clause_holds_is_stopped_and_left_unobserved() {
    // Three times what a clause holds, a mebibyte a push.
    let held = u64::try_from(HELD_BYTES >> 20).unwrap();
    for checkpoints in [false, true] {
        let name = format!("wide_{checkpoints}");
        let config = json!({
            "name": name, "pushes": "wide", "count": 3 * held, "checkpoints": checkpoints,
        });
        let report = certify_source::<Flood>(config).await;
        assert!(failed(&report).is_empty(), "{report}");
        assert_eq!(report.verdict(), Verdict::Incomplete, "{report}");
        for clause in ["S-RESUME", "S-PARTITION", "S-STOP"] {
            let outcome = report.outcome(clause);
            assert!(
                matches!(outcome, Some(Outcome::Unobserved(_))),
                "{clause}: {report}"
            );
        }
        // No read was taken past what a clause holds, and what was on its way.
        assert!(taken(&name) <= held + AHEAD, "{}", taken(&name));
    }
}

#[tokio::test]
async fn a_source_within_what_a_clause_holds_is_read_whole_however_many_partitions_share_it() {
    // A quarter of what a clause holds in each partition: one is within it, and four, with what
    // holds them, beyond.
    let quarter = u64::try_from(HELD_BYTES >> 22).unwrap();
    for (partitions, observed) in [(1, true), (4, false)] {
        let name = format!("shared_{partitions}");
        let config = json!({
            "name": name, "pushes": "wide", "count": quarter, "partitions": partitions,
        });
        let report = certify_source::<Flood>(config).await;
        assert!(failed(&report).is_empty(), "{report}");
        let stop = report.outcome("S-STOP");
        assert_eq!(
            stop == Some(&Outcome::Passed),
            observed,
            "{partitions}: {report}"
        );
    }
}

#[tokio::test]
async fn rows_that_cost_no_bytes_are_counted_and_cannot_outrun_a_clause() {
    // Ten times the rows a clause holds, a million a push, each push a few bytes.
    let held = u64::try_from(HELD_ROWS >> 20).unwrap();
    let began = Instant::now();
    let config = json!({
        "name": "nulls", "pushes": "nulls", "count": 10 * held, "checkpoints": true,
    });
    let report = certify_source::<Flood>(config).await;
    assert!(failed(&report).is_empty(), "{report}");
    for clause in ["S-RESUME", "S-PARTITION"] {
        let outcome = report.outcome(clause);
        assert!(
            matches!(outcome, Some(Outcome::Unobserved(_))),
            "{clause}: {report}"
        );
    }
    assert!(taken("nulls") <= held + AHEAD, "{}", taken("nulls"));
    // Rendering each row of them takes minutes.
    assert!(
        began.elapsed() < Duration::from_secs(30),
        "{:?}",
        began.elapsed()
    );
}

#[tokio::test]
async fn an_instant_no_calendar_holds_is_compared_instead_of_panicking() {
    let config = json!({ "name": "edge", "pushes": "edge", "count": 4, "checkpoints": true });
    let report = certify_source::<Flood>(config).await;
    assert!(failed(&report).is_empty(), "{report}");
    assert_eq!(
        report.outcome("S-PARTITION"),
        Some(&Outcome::Passed),
        "{report}"
    );
}
