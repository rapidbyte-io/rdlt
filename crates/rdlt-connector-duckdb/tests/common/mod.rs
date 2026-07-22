//! Shared structured-Arrow source for the feature-013 cells: keyed STRUCTURED
//! streams (no `_rdlt_id`) — the shape the 008/010 options apply to, same as
//! the postgres cells' driver. Not every test binary uses every helper.
#![allow(dead_code)]

use std::sync::Arc;

use arrow_array::{ArrayRef, BooleanArray, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use rdlt_connector::core::Cursor;
use rdlt_connector::{ConnectorSpec, ReadRequest, Source, SourceError, StreamSpec};

/// One column of test data.
#[derive(Clone)]
pub enum Col {
    I64(Vec<Option<i64>>),
    Str(Vec<Option<String>>),
    Bool(Vec<Option<bool>>),
}

pub fn i64s(values: &[Option<i64>]) -> Col {
    Col::I64(values.to_vec())
}
pub fn strs(values: &[Option<&str>]) -> Col {
    Col::Str(values.iter().map(|v| v.map(str::to_owned)).collect())
}
pub fn bools(values: &[Option<bool>]) -> Col {
    Col::Bool(values.to_vec())
}

pub fn batch(cols: &[(&str, Col)]) -> RecordBatch {
    let fields: Vec<Field> = cols
        .iter()
        .map(|(name, col)| {
            let ty = match col {
                Col::I64(_) => DataType::Int64,
                Col::Str(_) => DataType::Utf8,
                Col::Bool(_) => DataType::Boolean,
            };
            Field::new(*name, ty, true)
        })
        .collect();
    let arrays: Vec<ArrayRef> = cols
        .iter()
        .map(|(_, col)| -> ArrayRef {
            match col {
                Col::I64(v) => Arc::new(Int64Array::from(v.clone())),
                Col::Str(v) => Arc::new(StringArray::from(v.clone())),
                Col::Bool(v) => Arc::new(BooleanArray::from(v.clone())),
            }
        })
        .collect();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).expect("test batch")
}

/// Structured keyed source: each entry in `units` is pushed as one batch
/// followed by its own checkpoint — under the default commit policy each
/// becomes its own commit unit (the split-feed driver); a single unit is the
/// common case.
pub struct StructuredSource {
    pub stream: &'static str,
    pub key: &'static [&'static str],
    pub units: Vec<RecordBatch>,
    /// Cursor offset: this extraction's units sit at `base+1 ..= base+n`.
    /// A SECOND engine run redelivering fresh data must advance its base past
    /// the first run's cursors, exactly like a real incremental source.
    pub base: u64,
}

impl StructuredSource {
    /// The same feed positioned after `base` committed checkpoints.
    pub fn at(mut self, base: u64) -> Self {
        self.base = base;
        self
    }
}

#[async_trait]
impl Source for StructuredSource {
    fn spec(&self) -> ConnectorSpec {
        ConnectorSpec::new("structured-test", "0.0.0")
    }

    async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError> {
        Ok(vec![
            StreamSpec::new(self.stream)
                .structured()
                .with_primary_key(self.key.iter().copied()),
        ])
    }

    async fn read(&self, mut req: ReadRequest) -> Result<(), SourceError> {
        // Honor the resume cursor like a real source (clause E6): a crash-
        // recovered run re-reads AFTER the last committed checkpoint —
        // re-emitting committed units would (correctly) trip the single-unit
        // rules for scoped/retire tables.
        let resume: u64 = req
            .since
            .as_ref()
            .and_then(|c| c.as_value().as_u64())
            .unwrap_or(0);
        for (i, unit) in self.units.iter().enumerate() {
            let seq = self.base + i as u64 + 1;
            if seq <= resume {
                continue;
            }
            let _ = req.out.arrow(unit.clone()).await;
            let _ = req.out.checkpoint(Cursor::new(seq)).await;
        }
        Ok(())
    }
}

pub fn one_unit(
    stream: &'static str,
    key: &'static [&'static str],
    rows: RecordBatch,
) -> StructuredSource {
    StructuredSource {
        stream,
        key,
        units: vec![rows],
        base: 0,
    }
}
