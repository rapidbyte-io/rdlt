use std::fmt::Write as _;
use std::io::Write as _;
use std::sync::Arc;

use arrow_array::cast::AsArray as _;
use arrow_array::types::Int64Type;
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use super::Held;
use crate::limits::READ_BATCH_ROWS;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Utf8, true),
        Field::new("c", DataType::Float64, true),
    ]))
}

/// The batches `lines` read as, under [`schema`].
fn read(lines: &str) -> Result<Vec<RecordBatch>, arrow_schema::ArrowError> {
    let mut file = tempfile::tempfile().unwrap();
    file.write_all(lines.as_bytes()).unwrap();
    std::io::Seek::rewind(&mut file).unwrap();
    let mut rows = Held::new(file, &schema());
    let mut batches = Vec::new();
    while let Some(batch) = rows.next()? {
        batches.push(batch);
    }
    Ok(batches)
}

/// Each batch's column names and the ids of its rows.
fn shapes(batches: &[RecordBatch]) -> Vec<(Vec<String>, Vec<i64>)> {
    batches
        .iter()
        .map(|batch| {
            let schema = batch.schema();
            let names = schema.fields().iter().map(|field| field.name().clone());
            let ids = batch
                .column(0)
                .as_primitive::<Int64Type>()
                .values()
                .to_vec();
            (names.collect(), ids)
        })
        .collect()
}

fn names(columns: &[&str]) -> Vec<String> {
    columns.iter().map(|name| (*name).to_owned()).collect()
}

#[test]
fn lines_are_read_in_runs_of_the_columns_they_name_in_the_file_s_order() {
    let lines = concat!(
        "{\"id\":1}\n",
        "{\"id\":2}\n",
        "{\"id\":3}\n",
        "{\"id\":4,\"a\":40,\"b\":\"x\",\"c\":1.5}\n",
        "\n",
        "{\"id\":5,\"a\":50}\r\n",
        "{\"id\":6}\n",
        "{\"id\":7}\n",
        "{\"b\":\"y\",\"id\":8}\n",
        "{\"id\":9,\"a\":90}",
    );
    let batches = read(lines).unwrap();
    // A line joins the batch before it while the batch lacks no more cells than it holds:
    // the fourth would leave the first three lacking nine of sixteen.
    let expected = vec![
        (names(&["id"]), vec![1, 2, 3]),
        (names(&["id", "a", "b", "c"]), vec![4, 5, 6, 7, 8, 9]),
    ];
    assert_eq!(shapes(&batches), expected);
    // Each column holds its table's type and the values written, null where a row lacks it.
    let wide = &batches[1];
    assert_eq!(wide.schema().field(3), schema().field(3));
    let a = wide.column(1).as_primitive::<Int64Type>();
    assert_eq!((a.value(0), a.value(1), a.is_null(2)), (40, 50, true));
    let texts = wide.column(2).as_string::<i32>();
    assert_eq!(
        (texts.value(0), texts.is_null(3), texts.value(4)),
        ("x", true, "y")
    );
}

#[test]
fn a_null_or_a_name_the_table_lacks_is_no_column_of_a_line_s() {
    let lines = concat!(
        "{\"id\":1,\"a\":null,\"other\":{\"deep\":[1,2]}}\n",
        "{\"id\":2,\"id\":2}\n",
        "{\"id\":3,\"c\":\"NaN\"}\n",
    );
    let batches = read(lines).unwrap();
    assert_eq!(shapes(&batches), [(names(&["id", "c"]), vec![1, 2, 3])]);
    let floats = batches[0]
        .column(1)
        .as_primitive::<arrow_array::types::Float64Type>();
    assert!(floats.is_null(0) && floats.is_null(1) && floats.value(2).is_nan());
    assert_eq!(
        shapes(&read("{\"id\":1,\"a\":null}\n").unwrap())[0].0,
        names(&["id"])
    );
}

#[test]
fn a_run_is_cut_at_the_rows_a_batch_holds() {
    let rows = i64::try_from(READ_BATCH_ROWS).unwrap() * 2 + 1;
    let mut lines = String::new();
    for id in 0..rows {
        writeln!(lines, "{{\"id\":{id}}}").unwrap();
    }
    let batches = read(&lines).unwrap();
    let sizes: Vec<usize> = batches.iter().map(RecordBatch::num_rows).collect();
    assert_eq!(sizes, [READ_BATCH_ROWS, READ_BATCH_ROWS, 1]);
    let ids: Vec<i64> = shapes(&batches)
        .into_iter()
        .flat_map(|(_, ids)| ids)
        .collect();
    assert_eq!(ids, (0..rows).collect::<Vec<i64>>());
}

#[test]
fn a_line_that_is_no_one_record_or_lacks_a_column_every_row_holds_is_refused() {
    for line in [
        "[1]\n",
        "7\n",
        "{\"id\":1} {\"id\":2}\n",
        "{\"id\":1\n",
        "{\"a\":1}\n",
        "{\"id\":null}\n",
        "{\"id\":\"x\"}\n",
    ] {
        let lines = format!("{{\"id\":0}}\n{line}");
        assert!(read(&lines).is_err(), "{line:?}");
    }
    assert!(read("").unwrap().is_empty());
    assert!(read("\n \n").unwrap().is_empty());
}

#[test]
fn a_column_no_line_of_a_batch_names_costs_the_batch_nothing() {
    let wide: Vec<Field> = std::iter::once(Field::new("id", DataType::Int64, false))
        .chain((0..1_000).map(|column| Field::new(format!("c{column}"), DataType::Int64, true)))
        .collect();
    let wide: SchemaRef = Arc::new(Schema::new(wide));
    let mut file = tempfile::tempfile().unwrap();
    for id in 0..20_000 {
        if id == 10_000 {
            write!(file, "{{\"id\":{id}").unwrap();
            for column in 0..1_000 {
                write!(file, ",\"c{column}\":7").unwrap();
            }
            writeln!(file, "}}").unwrap();
            continue;
        }
        writeln!(file, "{{\"id\":{id}}}").unwrap();
    }
    std::io::Seek::rewind(&mut file).unwrap();
    let mut rows = Held::new(file, &wide);
    let (mut held, mut count, mut widest) = (0, 0, Vec::new());
    while let Some(batch) = rows.next().unwrap() {
        held += batch.get_array_memory_size();
        count += batch.num_rows();
        if batch.num_columns() != 1 {
            widest.push((batch.num_columns(), batch.num_rows()));
        }
    }
    assert_eq!(count, 20_000);
    // The wide row shares its batch with as few narrow rows as cost no more than it holds.
    assert_eq!(widest, [(1_001, 2)]);
    assert!(held < 1024 * 1024, "the batches hold {held} bytes");
}
