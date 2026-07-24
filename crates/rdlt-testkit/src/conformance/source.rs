//! Source conformance (clauses S1–S6): the resume law, checkpoint coverage, and
//! cancellation behavior — verified black-box against any deterministic source.

use rdlt_connector::{Cursor, PushPayload, ReadRequest, Source, StreamSpec, records_channel};
use serde_json::Value;

use super::ConformanceFailure;

/// Byte budget for the harness's record channel — large enough that a well-behaved
/// source never blocks on backpressure while being certified.
const CHANNEL_BYTE_BUDGET: usize = 16 << 20;

/// What one full read produced: row groups separated by checkpoints.
#[derive(Debug, Default)]
struct Observed {
    /// (rows pushed since previous checkpoint, checkpoint that closed the group).
    groups: Vec<(Vec<Value>, Option<Cursor>)>,
}

impl Observed {
    fn all_rows(&self) -> Vec<Value> {
        self.groups
            .iter()
            .flat_map(|(rows, _)| rows.iter().cloned())
            .collect()
    }

    fn checkpoints(&self) -> Vec<Cursor> {
        self.groups.iter().filter_map(|(_, c)| c.clone()).collect()
    }

    /// Rows covered by (pushed before) checkpoint `cursor`.
    fn rows_covered_by(&self, cursor: &Cursor) -> Vec<Value> {
        let mut covered = Vec::new();
        for (rows, checkpoint) in &self.groups {
            covered.extend(rows.iter().cloned());
            if checkpoint.as_ref() == Some(cursor) {
                return covered;
            }
        }
        covered
    }
}

async fn read_all<S: Source>(
    source: &S,
    spec: &StreamSpec,
    since: Option<Cursor>,
) -> Result<Observed, String> {
    let (out, mut input) = records_channel(CHANNEL_BYTE_BUDGET);
    let request = ReadRequest::new(spec.clone(), since, out);
    let reader = source.read(request);
    tokio::pin!(reader);

    let mut observed = Observed::default();
    let mut current: Vec<Value> = Vec::new();
    let mut read_result: Option<Result<(), String>> = None;
    loop {
        tokio::select! {
            push = input.recv() => match push {
                Some(push) => match push.payload {
                    PushPayload::RawJson(bytes) => {
                        for doc in serde_json::Deserializer::from_slice(&bytes).into_iter::<Value>() {
                            match doc.map_err(|e| format!("source pushed invalid JSON: {e}"))? {
                                Value::Array(items) => current.extend(items),
                                value => current.push(value),
                            }
                        }
                    }
                    PushPayload::Arrow(batch) => {
                        // Arrow-pushing sources: track row counts as opaque rows.
                        current.extend((0..batch.num_rows()).map(|_| Value::Null));
                    }
                    PushPayload::Checkpoint(cursor) => {
                        observed.groups.push((std::mem::take(&mut current), Some(cursor)));
                    }
                },
                None => break,
            },
            result = &mut reader, if read_result.is_none() => {
                read_result = Some(result.map_err(|e| format!("source read failed: {e}")));
            }
        }
    }
    if !current.is_empty() {
        observed.groups.push((std::mem::take(&mut current), None));
    }
    if let Some(Err(e)) = read_result {
        return Err(e);
    }
    Ok(observed)
}

/// Run the full source conformance suite. The source must be deterministic (same
/// data on every uncursored read) and should declare at least one stream with
/// checkpoints — resume clauses cannot be certified without them.
pub async fn verify_source<S: Source>(source: &S) -> Vec<ConformanceFailure> {
    let mut failures = Vec::new();
    let fail = |clause, message: String| ConformanceFailure { clause, message };

    let streams = match source.streams().await {
        Ok(streams) => streams,
        Err(e) => {
            failures.push(fail("S1", format!("streams() failed: {e}")));
            return failures;
        }
    };
    if streams.is_empty() {
        failures.push(fail("S1", "source declares no streams".into()));
        return failures;
    }

    for spec in &streams {
        // Baseline full read.
        let full = match read_all(source, spec, None).await {
            Ok(observed) => observed,
            Err(e) => {
                failures.push(fail("S1", format!("stream `{}`: {e}", spec.name)));
                continue;
            }
        };

        let checkpoints = full.checkpoints();
        if checkpoints.is_empty() {
            failures.push(fail(
                "S2",
                format!(
                    "stream `{}` never checkpoints — resume (S1) cannot be certified \
                     and every restart re-reads everything",
                    spec.name
                ),
            ));
            continue;
        }

        // S1 + S2 as one law: for every checkpoint c,
        //   full_read == rows_covered_by(c) ++ read(since = c).
        for cursor in &checkpoints {
            let resumed = match read_all(source, spec, Some(cursor.clone())).await {
                Ok(observed) => observed,
                Err(e) => {
                    failures.push(fail(
                        "S1",
                        format!("stream `{}` resume from {cursor:?}: {e}", spec.name),
                    ));
                    continue;
                }
            };
            let mut expected = full.rows_covered_by(cursor);
            let suffix = resumed.all_rows();
            expected.extend(suffix.iter().cloned());
            if expected != full.all_rows() {
                failures.push(fail(
                    "S1",
                    format!(
                        "stream `{}`: read(since={cursor:?}) must emit exactly the rows \
                         after that checkpoint — prefix+resume ({} rows) != full read \
                         ({} rows, or content differs)",
                        spec.name,
                        expected.len(),
                        full.all_rows().len(),
                    ),
                ));
            }
        }

        // S4: a closed channel means cancellation — return promptly, without error.
        let (out, mut input) = records_channel(CHANNEL_BYTE_BUDGET);
        input.close();
        drop(input);
        let request = ReadRequest::new(spec.clone(), None, out);
        match tokio::time::timeout(std::time::Duration::from_secs(5), source.read(request)).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => failures.push(fail(
                "S4",
                format!(
                    "stream `{}`: a closed channel is cancellation, not an error — got {e}",
                    spec.name
                ),
            )),
            Err(_) => failures.push(fail(
                "S4",
                format!(
                    "stream `{}`: source hung >5s on a closed channel",
                    spec.name
                ),
            )),
        }
    }

    failures
}
