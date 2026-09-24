use rdlt_connector::{Field, LogicalType, TableSchema};

use super::{read, update};

/// `current` with a nullable Int64 column `name` added.
fn with(current: Option<&TableSchema>, name: &str) -> TableSchema {
    let mut fields: Vec<Field> = current
        .map(|schema| schema.fields().iter().cloned().collect())
        .unwrap_or_default();
    fields.push(Field::new(name, LogicalType::Int64, true));
    TableSchema::new(fields).unwrap()
}

fn names(root: &std::path::Path) -> Vec<String> {
    read(root, "t")
        .unwrap()
        .unwrap()
        .fields()
        .iter()
        .map(|field| field.name().to_owned())
        .collect()
}

#[test]
fn a_change_made_while_another_is_worked_out_is_never_lost() {
    let root = tempfile::tempdir().unwrap();
    let mut interleaved = false;
    update(root.path(), "t", |current| {
        if !interleaved {
            interleaved = true;
            update(root.path(), "t", |current| Ok(Some(with(current, "a")))).unwrap();
        }
        Ok(Some(with(current, "b")))
    })
    .unwrap();
    assert_eq!(names(root.path()), ["a", "b"]);
}

#[test]
fn a_change_that_changes_nothing_writes_nothing() {
    let root = tempfile::tempdir().unwrap();
    assert_eq!(read(root.path(), "t").unwrap(), None);
    update(root.path(), "t", |current| Ok(Some(with(current, "a")))).unwrap();
    update(root.path(), "t", |_| Ok(None)).unwrap();
    assert_eq!(names(root.path()), ["a"]);
}
