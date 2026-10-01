//! Rows rendered as text, so batches of any shape compare row by row.
//!
//! What a connector sends is rendered within a limit on the text, a piece at a time, and never
//! through a calendar: instants, dates, times and spans render as the integers they are.

#[cfg(test)]
mod tests;

use std::fmt::Write as _;
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use arrow_schema::{DataType, Field, Fields};

use super::limits::YIELD_ROWS;

/// Why rows could not be rendered.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum RenderError {
    /// The rows take more text than the limit the rendering was given.
    #[error("the rows render as more than the {0} bytes of text a comparison holds")]
    Beyond(usize),
    /// A column is of a type that does not render.
    #[error("column `{column}` of type {kind} does not render: {reason}")]
    Unrendered {
        /// The column's name.
        column: String,
        /// Its type.
        kind: String,
        /// Why it does not render.
        reason: String,
    },
}

/// Renders rows as text, within a limit on all it renders.
#[derive(Debug)]
pub struct Rendering {
    limit: usize,
    room: usize,
}

impl Rendering {
    /// A rendering of at most `limit` bytes of text, all its rows together.
    pub fn new(limit: usize) -> Self {
        Self { limit, room: limit }
    }

    /// Each row of `batch` as text: its columns `keep` keeps, in name order, each as its name
    /// and value, a null as `null`.
    ///
    /// It yields to the runtime between pieces, so what bounds the caller can end it.
    ///
    /// # Errors
    ///
    /// [`RenderError::Beyond`] once the rows take more than the limit, and
    /// [`RenderError::Unrendered`] for a column of a type that does not render.
    pub async fn rows(
        &mut self,
        batch: &RecordBatch,
        keep: impl Fn(&str) -> bool,
    ) -> Result<Vec<String>, RenderError> {
        let schema = batch.schema();
        let mut columns = Vec::new();
        for (field, column) in schema.fields().iter().zip(batch.columns()) {
            if keep(field.name()) {
                columns.push(plain(field, column)?);
            }
        }
        columns.sort_by(|left, right| left.0.cmp(&right.0));
        let mut rows = Vec::new();
        let mut next = 0;
        while next < batch.num_rows() {
            let until = next.saturating_add(YIELD_ROWS).min(batch.num_rows());
            self.piece(&columns, next..until, &mut rows)?;
            next = until;
            if next < batch.num_rows() {
                tokio::task::yield_now().await;
            }
        }
        Ok(rows)
    }

    /// Renders the rows `piece` names of `columns`, labelled, onto `rows`.
    fn piece(
        &mut self,
        columns: &[(String, ArrayRef)],
        piece: std::ops::Range<usize>,
        rows: &mut Vec<String>,
    ) -> Result<(), RenderError> {
        let options = FormatOptions::default().with_null("null");
        let mut formatters = Vec::with_capacity(columns.len());
        for (label, column) in columns {
            let formatter = ArrayFormatter::try_new(column.as_ref(), &options)
                .map_err(|error| unrendered(label, column.data_type(), &error))?;
            formatters.push((label.as_str(), formatter));
        }
        for row in piece {
            let mut text = Text {
                text: String::new(),
                room: &mut self.room,
            };
            for (index, (label, formatter)) in formatters.iter().enumerate() {
                let separator = if index == 0 { "" } else { ", " };
                let named = write!(text, "{separator}{label}=");
                let written = named.is_ok() && formatter.value(row).write(&mut text).is_ok();
                // Within its room, every type a formatter takes renders: what stopped it is
                // the room.
                if !written {
                    return Err(RenderError::Beyond(self.limit));
                }
            }
            rows.push(text.text);
        }
        Ok(())
    }

    /// Charges `text`, rendered elsewhere, against the limit.
    ///
    /// # Errors
    ///
    /// [`RenderError::Beyond`] once the text rendered takes more than the limit.
    pub fn charge(&mut self, text: &str) -> Result<(), RenderError> {
        if text.len() > self.room {
            self.room = 0;
            return Err(RenderError::Beyond(self.limit));
        }
        self.room -= text.len();
        Ok(())
    }
}

/// A row's text, written within what room the rendering has left.
struct Text<'a> {
    text: String,
    room: &'a mut usize,
}

impl std::fmt::Write for Text<'_> {
    fn write_str(&mut self, piece: &str) -> std::fmt::Result {
        if piece.len() > *self.room {
            *self.room = 0;
            return Err(std::fmt::Error);
        }
        *self.room -= piece.len();
        self.text.push_str(piece);
        Ok(())
    }
}

fn unrendered(column: &str, kind: &DataType, reason: &dyn std::fmt::Display) -> RenderError {
    RenderError::Unrendered {
        column: column.to_owned(),
        kind: kind.to_string(),
        reason: reason.to_string(),
    }
}

/// `column` under its label, with every instant, date, time and span in it, at any depth, as
/// the integer it is: Arrow renders those through a calendar, which holds none near its ends.
///
/// A column so changed is labelled with its own type, which says what its integers count.
fn plain(field: &Field, column: &ArrayRef) -> Result<(String, ArrayRef), RenderError> {
    let kind = column.data_type();
    if !calendar(kind) {
        return Ok((field.name().clone(), Arc::clone(column)));
    }
    // What holds them where no cast reaches, as a union does, does not render.
    let integers = integers(kind).filter(|integers| !calendar(integers));
    let Some(integers) = integers else {
        let reason = "it holds instants where they cannot be read as integers";
        return Err(unrendered(field.name(), kind, &reason));
    };
    let label = format!("{}<{kind}>", field.name());
    arrow_cast::cast(column, &integers)
        .map(|column| (label, column))
        .map_err(|error| unrendered(field.name(), kind, &error))
}

/// Whether `kind` holds, at any depth, a type Arrow renders through a calendar or a clock.
fn calendar(kind: &DataType) -> bool {
    match kind {
        DataType::Timestamp(..)
        | DataType::Date32
        | DataType::Date64
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Duration(_) => true,
        DataType::List(item)
        | DataType::LargeList(item)
        | DataType::ListView(item)
        | DataType::LargeListView(item)
        | DataType::FixedSizeList(item, _)
        | DataType::Map(item, _) => calendar(item.data_type()),
        DataType::Struct(fields) => fields.iter().any(|field| calendar(field.data_type())),
        DataType::Union(fields, _) => fields.iter().any(|(_, field)| calendar(field.data_type())),
        DataType::Dictionary(_, values) => calendar(values),
        DataType::RunEndEncoded(_, values) => calendar(values.data_type()),
        _ => false,
    }
}

/// `kind` with each temporal type in it replaced by the integer type that holds it; none when
/// it holds no temporal type.
fn integers(kind: &DataType) -> Option<DataType> {
    let field = |field: &Arc<Field>| -> Option<Arc<Field>> {
        let kind = integers(field.data_type())?;
        Some(Arc::new(field.as_ref().clone().with_data_type(kind)))
    };
    let fields = |fields: &Fields| -> Option<Fields> {
        let changed: Vec<Option<Arc<Field>>> = fields.iter().map(field).collect();
        changed.iter().any(Option::is_some).then(|| {
            let kept = fields.iter().zip(changed);
            kept.map(|(old, new)| new.unwrap_or_else(|| Arc::clone(old)))
                .collect()
        })
    };
    match kind {
        DataType::Timestamp(..)
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Duration(_) => Some(DataType::Int64),
        DataType::Date32 | DataType::Time32(_) => Some(DataType::Int32),
        DataType::List(item) => field(item).map(DataType::List),
        DataType::LargeList(item) => field(item).map(DataType::LargeList),
        DataType::ListView(item) => field(item).map(DataType::ListView),
        DataType::LargeListView(item) => field(item).map(DataType::LargeListView),
        DataType::FixedSizeList(item, size) => {
            field(item).map(|item| DataType::FixedSizeList(item, *size))
        }
        DataType::Map(entries, sorted) => {
            field(entries).map(|entries| DataType::Map(entries, *sorted))
        }
        DataType::Struct(of) => fields(of).map(DataType::Struct),
        DataType::Dictionary(keys, values) => {
            integers(values).map(|values| DataType::Dictionary(keys.clone(), Box::new(values)))
        }
        DataType::RunEndEncoded(ends, values) => {
            field(values).map(|values| DataType::RunEndEncoded(Arc::clone(ends), values))
        }
        _ => None,
    }
}
