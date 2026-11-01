//! One chunk parsed: its records observed and, where its builders have room, built into columns
//! as they arrive, fast or with exact numbers.

use std::sync::Arc;

use rdlt_connector::Exclusion;
use rdlt_wire::limits::count;
use serde::de::DeserializeSeed;

use super::build::Record;
use super::meter::{Beyond, Columns, Meter};
use super::observe::Shape;
use super::records::Chunk;
use super::visit::{Context, RecordSeed, Row};
use super::{Parsed, ShredError, ShredLimits, any, exact, observing};
use crate::report::ShredCounts;

/// Parses `chunk`, observing its values and building them into columns as they arrive, within
/// what it was admitted for; a chunk whose builders would take more is observed instead.
///
/// A chunk where the fast parse may have rounded an integer, or misread a vast exponent, is parsed
/// again with its numbers exact, unless `limits` has every chunk parsed exactly from its start; so
/// is one it refuses, since it stops at the first value it refuses, may have read a vast exponent
/// before that value as zero, and checks a record is text only as far as it reads: the exact
/// parse refuses the record as it would wherever its chunk was cut.
pub(super) fn parse(
    chunk: Chunk,
    limits: ShredLimits,
    beyond: &Arc<Beyond>,
    excluded: &Exclusion,
) -> Result<Parsed, ShredError> {
    let (mut exact, mut counts) = (limits.exact, ShredCounts::default());
    let allowance = count(chunk.bytes()).saturating_mul(limits.admitted);
    // The most records it may hold: blank lines among lines count too.
    let most = chunk.count();
    loop {
        counts.parse(exact);
        let context = Context::new(Meter::new(allowance), Columns::new(limits.columns));
        let mut record = Record::empty(most);
        let appended = each(&chunk, exact, &context, |bytes| {
            let row = Row {
                record: &mut record,
                context: &context,
                excluded: any(excluded),
            };
            visit(bytes, RecordSeed(row), exact, &context)
        });
        let appended = match appended {
            Err(_) if !exact => {
                exact = true;
                continue;
            }
            appended => appended?,
        };
        if appended.tripped {
            drop(record);
            counts.tripped = counts.tripped.saturating_add(1);
            return observed(chunk, (exact, counts), limits, beyond, excluded);
        }
        if appended.imprecise && !exact {
            exact = true;
            continue;
        }
        return Ok(Parsed {
            rows: appended.rows,
            longest: appended.longest,
            shape: record.shape(),
            record: Some(record),
            spoiled: context.spoiled(),
            exact,
            json_floats: context.json_floats(),
            spent: context.meter.spent(),
            chunk,
            counts,
        });
    }
}

/// Observes `chunk`'s values, exactly where `exact` says or where the fast parse may have misread a
/// number or refuses a record, building nothing; `counts` are the passes over it before.
fn observed(
    chunk: Chunk,
    (mut exact, mut counts): (bool, ShredCounts),
    limits: ShredLimits,
    beyond: &Arc<Beyond>,
    excluded: &Exclusion,
) -> Result<Parsed, ShredError> {
    let allowance = count(chunk.bytes()).saturating_mul(limits.admitted);
    loop {
        counts.observe(exact);
        let meter = Meter::observing(allowance, beyond);
        let context = Context::new(meter, Columns::new(limits.columns));
        let mut shape = Shape::default();
        let appended = each(&chunk, exact, &context, |bytes| {
            let row = observing::Record {
                shape: &mut shape,
                context: &context,
                excluded: any(excluded),
            };
            visit(bytes, RecordSeed(row), exact, &context)
        });
        let appended = match appended {
            Err(_) if !exact => {
                context.meter.give_back();
                exact = true;
                continue;
            }
            appended => appended?,
        };
        if appended.imprecise && !exact {
            context.meter.give_back();
            exact = true;
            continue;
        }
        return Ok(Parsed {
            chunk,
            rows: appended.rows,
            longest: appended.longest,
            record: None,
            shape,
            spoiled: true,
            exact,
            json_floats: context.json_floats(),
            spent: context.meter.spent(),
            counts,
        });
    }
}

/// Parses the record `bytes` into `seed`, with exact numbers where `exact` says.
pub(super) fn visit<S>(
    bytes: &[u8],
    seed: S,
    exact: bool,
    context: &Context,
) -> Result<(), sonic_rs::Error>
where
    S: for<'de> DeserializeSeed<'de, Value = ()>,
{
    if exact {
        return exact::visit(bytes, seed, context);
    }
    let mut deserializer = sonic_rs::Deserializer::from_slice(bytes);
    seed.deserialize(&mut deserializer)
        .and_then(|()| deserializer.end())
}

/// How parsing a chunk went.
pub(super) struct Appended {
    /// Whether the builders would have taken more than the chunk was admitted for.
    tripped: bool,
    /// Whether the parse stopped at a number it may have misread, or, parsing exactly, whether it
    /// parsed so.
    pub(super) imprecise: bool,
    /// The records it read.
    rows: usize,
    /// Bytes: the longest record it read.
    longest: usize,
}

/// Parses each record of `chunk` with `record`, its numbers exact where `exact` says.
///
/// The fast parse reads an integer beyond 64 bits as the float nearest it, and refuses one beyond a
/// float's range, so it stops at the first record holding a whole float of 2⁶³ or more and a run
/// of digits as long as such an integer, or at the first refusal, leaving the chunk to be parsed
/// again exactly. A parse whose builders trip their meter stops there.
/// A record it refuses is numbered within the chunk.
pub(super) fn each(
    chunk: &Chunk,
    exact: bool,
    context: &Context,
    mut record: impl FnMut(&[u8]) -> Result<(), sonic_rs::Error>,
) -> Result<Appended, ShredError> {
    let (mut rows, mut longest) = (0, 0);
    for bytes in chunk.records() {
        let (bytes, index) = (bytes?, rows);
        rows += 1;
        longest = longest.max(bytes.len());
        let parsed = record(bytes);
        if !exact {
            context.confirm(bytes);
        }
        if let Err(error) = parsed {
            if let Some(fault) = context.fault() {
                return Err(fault);
            }
            // A tripped parse is observed next, which finds again whether it must be exact.
            if context.meter.tripped() {
                return Ok(Appended {
                    tripped: true,
                    imprecise: false,
                    rows,
                    longest,
                });
            }
            // The fast parse refuses an integer beyond a float's range as it refuses invalid JSON;
            // the exact parse tells them apart.
            if exact {
                return Err(invalid(index, &error));
            }
            context.reparse();
        }
        if context.imprecise() && !exact {
            break;
        }
    }
    Ok(Appended {
        tripped: false,
        imprecise: exact || context.imprecise(),
        rows,
        longest,
    })
}

/// The error for record `index` of a chunk, which sonic-rs refused with `error`: what broke and
/// where, without the excerpt of the record sonic-rs quotes after it.
fn invalid(index: usize, error: &sonic_rs::Error) -> ShredError {
    let message = error.to_string();
    let what = message.lines().next().unwrap_or_default();
    ShredError::Record {
        index,
        what: what.to_owned(),
    }
}
