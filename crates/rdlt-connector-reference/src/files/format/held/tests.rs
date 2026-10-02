use std::fmt::Write as _;
use std::io::Write as _;
use std::sync::Arc;

use arrow_array::cast::AsArray as _;
use arrow_array::types::Int64Type;
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use super::Held;
use crate::limits::{READ_BATCH_ROWS, RUN_ABSENT_CELLS, RUN_ROWS};

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Utf8, true),
        Field::new("c", DataType::Float64, true),
    ]))
}

/// A file of `lines`, at its start.
fn file_of(lines: &str) -> std::fs::File {
    let mut file = tempfile::tempfile().unwrap();
    file.write_all(lines.as_bytes()).unwrap();
    std::io::Seek::rewind(&mut file).unwrap();
    file
}

/// The batches `lines` read as, under [`schema`].
fn read(lines: &str) -> Result<Vec<RecordBatch>, arrow_schema::ArrowError> {
    let mut rows = Held::new(file_of(lines), &schema());
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
    // Lines of a few columns each, fewer than a run is left to gather, are one batch of the
    // columns any of them names.
    let expected = vec![(
        names(&["id", "a", "b", "c"]),
        vec![1, 2, 3, 4, 5, 6, 7, 8, 9],
    )];
    assert_eq!(shapes(&batches), expected);
    // Each column holds its table's type and the values written, null where a row lacks it.
    let wide = &batches[0];
    assert_eq!(wide.schema().field(3), schema().field(3));
    let a = wide.column(1).as_primitive::<Int64Type>();
    assert_eq!((a.is_null(0), a.value(3), a.value(4)), (true, 40, 50));
    let texts = wide.column(2).as_string::<i32>();
    assert_eq!(
        (texts.value(3), texts.is_null(6), texts.value(7)),
        ("x", true, "y")
    );
}

/// `narrow` lines of an id alone, then one of every column, then `narrow` more.
fn around_a_wide_line(narrow: i64) -> String {
    let mut lines = String::new();
    for id in 0..=2 * narrow {
        if id == narrow {
            writeln!(lines, "{{\"id\":{id},\"a\":1,\"b\":\"x\",\"c\":1.5}}").unwrap();
        } else {
            writeln!(lines, "{{\"id\":{id}}}").unwrap();
        }
    }
    lines
}

#[test]
fn a_run_ends_where_a_line_would_leave_it_lacking_more_cells_than_it_holds_once_it_is_long() {
    let run = i64::try_from(RUN_ROWS).unwrap();
    // A run shorter than one is left to gather takes the wide line and what follows it.
    let batches = read(&around_a_wide_line(run / 4)).unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_columns(), 4);
    // A longer run of narrow lines ends before the wide line, which starts the next; that run
    // takes narrow lines until it is long and lacks more cells than it holds.
    let batches = read(&around_a_wide_line(2 * run)).unwrap();
    let sizes: Vec<(usize, usize)> = batches
        .iter()
        .map(|batch| (batch.num_columns(), batch.num_rows()))
        .collect();
    let (long, rest) = (RUN_ROWS * 2, RUN_ROWS + 1);
    assert_eq!(sizes, [(1, long), (4, RUN_ROWS), (1, rest)]);
    let ids: Vec<i64> = shapes(&batches)
        .into_iter()
        .flat_map(|(_, ids)| ids)
        .collect();
    assert_eq!(ids, (0..=4 * run).collect::<Vec<i64>>());
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
        "{\"id\":1}{\"id\":2}\n",
        "{\"id\":1}]\n",
        "{\"id\":1\n",
        "{\"id\":1,\"b\":\"x}\n",
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

/// The rows of the dense reader's batches of `lines`, under [`schema`].
fn dense(lines: &str) -> Result<usize, arrow_schema::ArrowError> {
    let mut rows = crate::files::format::json::Rows::new(file_of(lines), &schema())?;
    let mut read = 0;
    while let Some(batch) = rows.next()? {
        read += batch.num_rows();
    }
    Ok(read)
}

#[test]
fn a_line_of_one_record_is_taken_or_refused_as_the_reader_of_whole_schemas_does() {
    let deep = format!(
        "{{\"id\":1,\"deep\":{}1{}}}\n",
        "[".repeat(300),
        "]".repeat(300)
    );
    for lines in [
        "{\"id\":1,\"a\":1,\"a\":2}\n",
        "{\"id\":1,}\n",
        "{\"id\":1,\"c\":1e999}\n",
        "{\"id\":1,\"a\":1e999}\n",
        "\u{feff}{\"id\":1}\n",
        "{\"id\":1,\"b\":\"\\ud800\"}\n",
        "{\"id\":1,\"b\":\"a\\\"}{b\\\\\"}\n",
        "  {\"id\" : 1 , \"\\u0062\" : \"x\"}  \n",
        "{\"id\":1,\"other\":{\"id\":null,\"a\":[{\"b\":\"}\"}]}}\n",
        deep.as_str(),
    ] {
        let held = read(lines).map(|batches| batches.iter().map(RecordBatch::num_rows).sum());
        assert_eq!(held.ok(), dense(lines).ok(), "{lines:?}");
    }
    // A key written with an escape names its column.
    let batches = read("{\"id\":1,\"\\u0062\":\"x\"}\n").unwrap();
    assert_eq!(shapes(&batches), [(names(&["id", "b"]), vec![1])]);
}

#[test]
fn lines_that_alternate_between_few_and_many_columns_cost_what_the_whole_schema_costs() {
    let width = 6;
    let mut fields = vec![Field::new("id", DataType::Int64, false)];
    fields.extend((0..width).map(|c| Field::new(format!("c{c}"), DataType::Int64, true)));
    let wide: SchemaRef = Arc::new(Schema::new(fields));
    let mut lines = String::new();
    for id in 0..100_000 {
        write!(lines, "{{\"id\":{id}").unwrap();
        for column in (0..width).filter(|_| id % 2 == 1) {
            write!(lines, ",\"c{column}\":1").unwrap();
        }
        writeln!(lines, "}}").unwrap();
    }
    let mut held = Held::new(file_of(&lines), &wide);
    let (mut sparse, mut batches) = (0, 0);
    while let Some(batch) = held.next().unwrap() {
        sparse += batch.get_array_memory_size();
        batches += 1;
    }
    let mut whole = crate::files::format::json::Rows::new(file_of(&lines), &wide).unwrap();
    let (mut dense, mut dense_batches) = (0, 0);
    while let Some(batch) = whole.next().unwrap() {
        dense += batch.get_array_memory_size();
        dense_batches += 1;
    }
    assert!(
        batches <= dense_batches + 1,
        "{batches} against {dense_batches}"
    );
    assert!(sparse <= dense + dense / 4, "{sparse} against {dense}");
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
    // The wide row shares its batch with no more narrow rows than a run's absent cells allow:
    // some before it and some after, a thousand cells each.
    assert_eq!(widest.len(), 1, "{widest:?}");
    let (columns, rows) = widest[0];
    let most = RUN_ABSENT_CELLS / 1_000 + 2;
    assert!(columns == 1_001 && rows <= most, "{widest:?}");
    // Every column of every row would be 160 MB.
    assert!(held < 2 * 1024 * 1024, "the batches hold {held} bytes");
}
