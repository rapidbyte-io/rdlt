//! `D-ENCODING`: dictionary-encoded columns publish their values.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Int8Type, Int64Type};
use arrow_array::{ArrayRef, DictionaryArray, Int8Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::DataType;

use super::{Bench, commit, meta};
use crate::id::SegmentId;
use crate::testing::Violation;

/// A published row: its id and name.
type Row = (i64, Option<String>);

impl Bench<'_> {
    /// Stages a batch whose `name` column is dictionary-encoded, as the engine sends columns that
    /// hold one value per batch, commits it, and reads the values back.
    pub(super) async fn dictionaries_publish_their_values(&self) -> Result<(), Violation> {
        let mut opened = self.open(self.destination, 1).await?;
        let mut writer = self.writer(&mut opened.session).await?;
        writer
            .write(SegmentId(1), encoded())
            .await
            .map_err(|error| Violation::from(format!("write: {error}")))?;
        writer
            .flush()
            .await
            .map_err(|error| Violation::from(format!("flush: {error}")))?;
        commit(
            &mut opened.session,
            &meta(self.load_id(1), opened.epoch, &[1], Vec::new()),
        )
        .await?;
        let batches = self
            .probe
            .published(&self.table())
            .await
            .map_err(|error| Violation::from(format!("probe: {error}")))?;
        let mut rows = batches
            .iter()
            .map(rows)
            .collect::<Result<Vec<_>, _>>()?
            .concat();
        rows.sort();
        let expected = vec![
            (1, Some("ann".to_owned())),
            (2, None),
            (3, Some("ann".to_owned())),
        ];
        if rows == expected {
            Ok(())
        } else {
            Err(format!("published {rows:?}, expected {expected:?}").into())
        }
    }
}

/// Three rows whose names are a dictionary of one value, with a null among them.
fn encoded() -> RecordBatch {
    let ids: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
    let keys = Int8Array::from(vec![Some(0), None, Some(0)]);
    let values: ArrayRef = Arc::new(StringArray::from(vec!["ann"]));
    let names: ArrayRef = Arc::new(
        DictionaryArray::<Int8Type>::try_new(keys, values).expect("the keys index the values"),
    );
    RecordBatch::try_from_iter([("id", ids), ("name", names)])
        .expect("the certification batch is valid")
}

/// The rows of a published `batch`, whatever types the destination stores them as.
fn rows(batch: &RecordBatch) -> Result<Vec<Row>, Violation> {
    let column = |name: &str, to: &DataType| {
        let array = batch
            .column_by_name(name)
            .ok_or_else(|| Violation::from(format!("no published column {name}")))?;
        arrow_cast::cast(array, to)
            .map_err(|error| Violation::from(format!("reading {name}: {error}")))
    };
    let ids = column("id", &DataType::Int64)?;
    let names = column("name", &DataType::Utf8)?;
    let (ids, names) = (ids.as_primitive::<Int64Type>(), names.as_string::<i32>());
    Ok(ids
        .iter()
        .zip(names.iter())
        .filter_map(|(id, name)| Some((id?, name.map(str::to_owned))))
        .collect())
}
