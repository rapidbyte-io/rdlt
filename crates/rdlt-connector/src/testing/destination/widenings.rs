//! `D-SCHEMA`'s widenings: every pair of kinds a destination widens in place, at the edges of
//! the narrower type, read back as the values they were.

#[cfg(test)]
mod tests;

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_array::cast::AsArray;
use arrow_array::types::{Decimal128Type, Decimal256Type, Float64Type, Int64Type};
use arrow_array::{Array, ArrayRef, Decimal128Array, Float64Array, Int32Array, Int64Array};
use arrow_schema::DataType;

use super::{Bench, commit, meta};
use crate::OpenedSession;
use crate::capabilities::Capabilities;
use crate::commit::CommitMeta;
use crate::destination::TableChange;
use crate::id::{CommitSeq, SegmentId};
use crate::instants;
use crate::schema::TableSchema;
use crate::testing::reason::Listed;
use crate::testing::{Violation, bounded_call};
use crate::types::{DecimalType, Field, LogicalType, TimeUnit, TypeKind};

impl Bench<'_> {
    /// Widens a column of a table of its own for each pair of kinds the destination declares it
    /// stores and widens in place, holding the narrower type's edges, and reads every value back
    /// as it was; commits from `seq` on.
    ///
    /// The engine sends a kind the destination does not store as text, whose widening is the
    /// engine's own.
    pub(super) async fn widenings_keep_every_value(
        &self,
        opened: &mut OpenedSession,
        seq: CommitSeq,
    ) -> Result<CommitSeq, Violation> {
        let mut seq = seq;
        // Each widening's rows in a segment of their own, after those the clause wrote before.
        let segments = (100..).map(SegmentId);
        let widenings = checked(self.destination.capabilities());
        for (place, ((from, to), segment)) in widenings.into_iter().zip(segments).enumerate() {
            seq = seq.next();
            let table = self.other_table(&format!("widened_{place}"));
            let create = TableChange::Create {
                table: table.clone(),
                schema: TableSchema::new(vec![Field::new("v", from.clone(), true)])
                    .expect("the certification schema is valid"),
            };
            bounded_call("apply_schema", opened.session.apply_schema(&create)).await?;
            let values = edges(&from);
            let mut writer = bounded_call("writer", opened.session.writer(&table)).await?;
            let rows = RecordBatch::try_from_iter([("v", Arc::clone(&values))])
                .expect("the certification batch is valid");
            bounded_call("write", writer.write(segment, rows)).await?;
            bounded_call("flush", writer.flush()).await?;
            let widen = TableChange::Widen {
                table: table.clone(),
                column: "v".into(),
                from: from.clone(),
                to: to.clone(),
            };
            bounded_call("apply_schema", opened.session.apply_schema(&widen)).await?;
            let widened = CommitMeta {
                commit_seq: seq,
                ..meta(self.load_id(1), opened.epoch, &[segment.0], Vec::new())
            };
            commit(&mut opened.session, &widened).await?;
            let mut expected = denotations(values.as_ref(), &to);
            let mut published = Vec::new();
            for batch in self.read(&table).await?.batches() {
                let column = batch.stored("v").ok_or_else(|| {
                    Violation::from("a published batch of a widened column has no `v` column")
                })?;
                published.extend(denotations(column.as_ref(), &to));
            }
            expected.sort();
            published.sort();
            if published != expected {
                return Err(Violation::from(format_args!(
                    "a column widened from {from} to {to} read back {}, not {}",
                    Listed(&published),
                    Listed(&expected)
                )));
            }
        }
        Ok(seq)
    }
}

/// The pairs of types the clause widens for a destination of `capabilities`: one of each pair of
/// kinds it declares it stores and widens in place.
fn checked(capabilities: &Capabilities) -> Vec<(LogicalType, LogicalType)> {
    let stored = |(from, to): &&(TypeKind, TypeKind)| {
        capabilities.types.contains(from) && capabilities.types.contains(to)
    };
    let declared = capabilities.schema_changes.widenings.iter().filter(stored);
    declared.filter_map(|pair| typed(*pair)).collect()
}

/// A pair of types of the kinds `(from, to)` the lattice widens one to the other, the wider in
/// a zone and units its narrower type's values could move by; none for nested kinds, whose
/// fields widen as these do.
fn typed((from, to): (TypeKind, TypeKind)) -> Option<(LogicalType, LogicalType)> {
    use TypeKind as K;
    let decimal = |precision, scale| {
        LogicalType::Decimal(DecimalType::new(precision, scale).expect("a valid decimal"))
    };
    let integer = |kind: TypeKind| match kind {
        K::Int8 => Some(LogicalType::Int8),
        K::Int16 => Some(LogicalType::Int16),
        K::Int32 => Some(LogicalType::Int32),
        K::Int64 => Some(LogicalType::Int64),
        _ => None,
    };
    Some(match (from, to) {
        (K::Float32, K::Float64) => (LogicalType::Float32, LogicalType::Float64),
        (K::Decimal, K::Decimal) => (decimal(5, 2), decimal(12, 4)),
        (K::Date, K::Timestamp) => (
            LogicalType::Date,
            LogicalType::Timestamp(TimeUnit::Microsecond, Some("America/New_York".into())),
        ),
        (K::Timestamp, K::Timestamp) => (
            LogicalType::Timestamp(TimeUnit::Second, None),
            LogicalType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
        ),
        (K::Time, K::Time) => (
            LogicalType::Time(TimeUnit::Second),
            LogicalType::Time(TimeUnit::Nanosecond),
        ),
        (K::Duration, K::Duration) => (
            LogicalType::Duration(TimeUnit::Second),
            LogicalType::Duration(TimeUnit::Nanosecond),
        ),
        (from, K::Decimal) => (integer(from)?, decimal(21, 2)),
        (from, K::Float64) => (integer(from)?, LogicalType::Float64),
        (from, to) => (integer(from)?, integer(to)?),
    })
}

/// Values of `logical` at its edges and between, which the type it widens to holds: a
/// 32-bit float whose own shortest text names another 64-bit float among them.
fn edges(logical: &LogicalType) -> ArrayRef {
    let data_type = logical.to_arrow();
    let cast = |values: ArrayRef| {
        arrow_cast::cast(&values, &data_type).expect("certification values are castable")
    };
    match logical {
        LogicalType::Float32 => cast(Arc::new(Float64Array::from(vec![
            0.1,
            -1.5,
            f64::from(f32::MAX),
            f64::from(f32::MIN_POSITIVE),
            16_777_216.0,
        ]))),
        LogicalType::Decimal(decimal) => {
            let values = Decimal128Array::from(vec![0, 1, -1, 99_999, -99_999]);
            let (precision, scale) = (decimal.precision(), decimal.scale());
            let scale = i8::try_from(scale).unwrap_or(0);
            Arc::new(
                values
                    .with_precision_and_scale(precision, scale)
                    .expect("certification decimals are valid"),
            )
        }
        LogicalType::Date | LogicalType::Time(_) => {
            cast(Arc::new(Int32Array::from(vec![-1, 0, 1, 18_262, 86_399])))
        }
        LogicalType::Int8 => cast(Arc::new(Int64Array::from(vec![-128, -1, 0, 1, 127]))),
        LogicalType::Int16 => cast(Arc::new(Int64Array::from(vec![-32_768, -1, 0, 32_767]))),
        LogicalType::Int32 => cast(Arc::new(Int64Array::from(vec![
            i64::from(i32::MIN),
            -1,
            0,
            i64::from(i32::MAX),
        ]))),
        // Seconds a nanosecond still holds, and instants a day around the epoch.
        _ => cast(Arc::new(Int64Array::from(vec![
            -9_000_000_000,
            -86_401,
            -1,
            0,
            1_577_854_800,
        ]))),
    }
}

/// What each value of `column`, read back from a column of `to`, denotes: an instant, a time
/// of day or a duration in nanoseconds, and a number as the shortest decimal text of its value.
///
/// A column read back as the integers counting a temporal type's units is read as them.
fn denotations(column: &dyn Array, to: &LogicalType) -> Vec<String> {
    let temporal = matches!(
        to,
        LogicalType::Date
            | LogicalType::Time(_)
            | LogicalType::Timestamp(..)
            | LogicalType::Duration(_)
    );
    let counted;
    let column = if temporal && column.data_type().is_integer() {
        counted = arrow_cast::cast(&arrow_array::make_array(column.to_data()), &to.to_arrow());
        match &counted {
            Ok(counted) => counted.as_ref(),
            Err(_) => column,
        }
    } else {
        column
    };
    (0..column.len()).map(|row| denoted(column, row)).collect()
}

/// What the value at `row` of `column` denotes.
fn denoted(column: &dyn Array, row: usize) -> String {
    if column.is_null(row) {
        return "null".to_owned();
    }
    if let Some(value) = instants::stored(column, row) {
        let nanos = instants::nanos(column.data_type(), i128::from(value));
        return format!("{} ns", nanos.unwrap_or_default());
    }
    let one = column.slice(row, 1);
    let number = match column.data_type() {
        DataType::Decimal128(_, scale) => shifted(
            &one.as_primitive::<Decimal128Type>().value(0).to_string(),
            *scale,
        ),
        DataType::Decimal256(_, scale) => shifted(
            &one.as_primitive::<Decimal256Type>().value(0).to_string(),
            *scale,
        ),
        DataType::Float16 | DataType::Float32 | DataType::Float64 => {
            match arrow_cast::cast(&one, &DataType::Float64) {
                Ok(wide) => format!("{}", wide.as_primitive::<Float64Type>().value(0)),
                Err(error) => return format!("unreadable: {error}"),
            }
        }
        data_type if data_type.is_integer() => match arrow_cast::cast(&one, &DataType::Int64) {
            Ok(wide) => wide.as_primitive::<Int64Type>().value(0).to_string(),
            Err(error) => return format!("unreadable: {error}"),
        },
        other => return format!("a value of {other}"),
    };
    if number.contains('.') {
        number
            .trim_end_matches('0')
            .trim_end_matches('.')
            .to_owned()
    } else {
        number
    }
}

/// `digits`, an integer's text, divided by ten to the `scale`.
fn shifted(digits: &str, scale: i8) -> String {
    let (sign, digits) = digits
        .strip_prefix('-')
        .map_or(("", digits), |digits| ("-", digits));
    let scale = usize::try_from(scale).unwrap_or(0);
    let padded = format!("{digits:0>width$}", width = scale + 1);
    let (whole, fraction) = padded.split_at(padded.len() - scale);
    format!("{sign}{whole}.{fraction}")
}
