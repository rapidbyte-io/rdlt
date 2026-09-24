//! `D-ENCODING`: dictionary-encoded columns publish their values.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Int8Type, Int64Type, TimestampMicrosecondType};
use arrow_array::{
    Array, ArrayRef, DictionaryArray, FixedSizeBinaryArray, Int8Array, Int64Array, RecordBatch,
    StringArray, TimestampMicrosecondArray,
};
use arrow_schema::{DataType, Schema};

use super::{Bench, commit, meta};
use crate::destination::TableChange;
use crate::id::SegmentId;
use crate::schema::TableSchema;
use crate::testing::{Violation, bounded};
use crate::types::{Field, LogicalType, TimeUnit, TypeKind};

/// The load start every row carries, in microseconds since the epoch.
const LOADED_AT: i64 = 1_700_000_000_000_000;

/// The load id every row carries.
const LOAD_ID: [u8; 16] = [7; 16];

/// A published row: its id, name, load start and load id.
type Row = (i64, Option<String>, Option<i64>, Option<Vec<u8>>);

impl Bench<'_> {
    /// Stages, commits and reads back three rows whose columns but `id` are dictionaries.
    ///
    /// Each is a dictionary of one value, as the engine sends the load id and load start: `name`
    /// always, `at` and `load` where the destination stores timestamps and uuids.
    pub(super) async fn dictionaries_publish_their_values(&self) -> Result<(), Violation> {
        let types = &self.destination.capabilities().types;
        let fields = fields(
            types.contains(&TypeKind::Timestamp),
            types.contains(&TypeKind::Uuid),
        );
        let mut opened = self.open(self.destination, 1).await?;
        let table = self.table();
        let schema = TableSchema::new(fields.clone()).expect("the certification schema is valid");
        let create = TableChange::Create {
            table: table.clone(),
            schema,
        };
        bounded("apply_schema", opened.session.apply_schema(&create))
            .await?
            .map_err(|error| Violation::from(format!("apply_schema: {error}")))?;
        let mut writer = bounded("writer", opened.session.writer(&table))
            .await?
            .map_err(|error| Violation::from(format!("writer: {error}")))?;
        writer
            .write(SegmentId(1), encoded(&fields))
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
            .published(&table)
            .await
            .map_err(|error| Violation::from(format!("probe: {error}")))?;
        let mut rows = batches
            .iter()
            .map(|batch| rows(batch, &fields))
            .collect::<Result<Vec<_>, _>>()?
            .concat();
        rows.sort();
        let expected = expected(&fields);
        if rows == expected {
            Ok(())
        } else {
            Err(format!("published {rows:?}, expected {expected:?}").into())
        }
    }
}

/// The table's fields: `id` and `name`, then `at` and `load` where the destination stores them.
fn fields(timestamps: bool, uuids: bool) -> Vec<Field> {
    let mut fields = vec![
        Field::new("id", LogicalType::Int64, false),
        Field::new("name", LogicalType::Utf8, true),
    ];
    if timestamps {
        let at = LogicalType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        fields.push(Field::new("at", at, true));
    }
    if uuids {
        fields.push(Field::new("load", LogicalType::Uuid, true));
    }
    fields
}

/// Three rows of `fields`: every column but `id` a dictionary of one value, null in the second.
fn encoded(fields: &[Field]) -> RecordBatch {
    let mut columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![1, 2, 3]))];
    let mut arrow_fields = vec![fields[0].to_arrow()];
    for field in &fields[1..] {
        let values: ArrayRef = match field.name() {
            "name" => Arc::new(StringArray::from(vec!["ann"])),
            "at" => Arc::new(TimestampMicrosecondArray::from(vec![LOADED_AT]).with_timezone("UTC")),
            _ => Arc::new(
                FixedSizeBinaryArray::try_from_iter([LOAD_ID].into_iter())
                    .expect("the load id is 16 bytes"),
            ),
        };
        let keys = Int8Array::from(vec![Some(0), None, Some(0)]);
        let dictionary =
            DictionaryArray::<Int8Type>::try_new(keys, values).expect("the keys index the values");
        let plain = field.to_arrow();
        let data_type = DataType::Dictionary(
            Box::new(DataType::Int8),
            Box::new(plain.data_type().clone()),
        );
        arrow_fields.push(plain.with_data_type(data_type));
        columns.push(Arc::new(dictionary));
    }
    RecordBatch::try_new(Arc::new(Schema::new(arrow_fields)), columns)
        .expect("the certification batch is valid")
}

/// The rows `encoded` holds, as `rows` reads them back.
fn expected(fields: &[Field]) -> Vec<Row> {
    let has = |name: &str| fields.iter().any(|field| field.name() == name);
    let row = |id: i64, set: bool| {
        (
            id,
            set.then(|| "ann".to_owned()),
            (set && has("at")).then_some(LOADED_AT),
            (set && has("load")).then(|| LOAD_ID.to_vec()),
        )
    };
    vec![row(1, true), row(2, false), row(3, true)]
}

/// The rows of a published `batch` of `fields`, whatever types the destination stores them as.
fn rows(batch: &RecordBatch, fields: &[Field]) -> Result<Vec<Row>, Violation> {
    let column = |name: &str| -> Result<Option<ArrayRef>, Violation> {
        let Some(field) = fields.iter().find(|field| field.name() == name) else {
            return Ok(None);
        };
        let array = batch
            .column_by_name(name)
            .ok_or_else(|| Violation::from(format!("no published column {name}")))?;
        arrow_cast::cast(array, &field.logical_type().to_arrow())
            .map(Some)
            .map_err(|error| Violation::from(format!("reading {name}: {error}")))
    };
    let ids = column("id")?.ok_or("no id field")?;
    let names = column("name")?.ok_or("no name field")?;
    let (at, load) = (column("at")?, column("load")?);
    let (ids, names) = (ids.as_primitive::<Int64Type>(), names.as_string::<i32>());
    let at = at
        .as_ref()
        .map(AsArray::as_primitive::<TimestampMicrosecondType>);
    let load = load.as_ref().map(AsArray::as_fixed_size_binary);
    Ok((0..batch.num_rows())
        .filter(|&row| ids.is_valid(row))
        .map(|row| {
            (
                ids.value(row),
                names.is_valid(row).then(|| names.value(row).to_owned()),
                at.filter(|at| at.is_valid(row)).map(|at| at.value(row)),
                load.filter(|load| load.is_valid(row))
                    .map(|load| load.value(row).to_vec()),
            )
        })
        .collect())
}
