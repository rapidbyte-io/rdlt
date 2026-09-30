use rdlt_connector::{Field, LogicalType, PipelineId, TableSchema};

use super::{claim, owner, read, release, update};

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

#[cfg(unix)]
#[test]
fn a_catalog_that_cannot_be_listed_is_an_error_not_a_missing_table() {
    use std::os::unix::fs::PermissionsExt as _;
    let root = tempfile::tempdir().unwrap();
    update(root.path(), "t", |current| Ok(Some(with(current, "a")))).unwrap();
    let catalog = root.path().join("_rdlt").join("tables").join("t");
    let mode = |mode| std::fs::set_permissions(&catalog, std::fs::Permissions::from_mode(mode));
    mode(0o000).unwrap();
    // Where permissions bind nothing, as for root, the fault cannot be made.
    let listable = std::fs::read_dir(&catalog).is_ok();
    let read = read(root.path(), "t");
    mode(0o755).unwrap();
    if !listable {
        read.expect_err("the catalog cannot be listed");
    }
}

fn pipeline(name: &str) -> PipelineId {
    PipelineId::parse(name).unwrap()
}

#[test]
fn a_released_catalog_is_gone_and_any_pipeline_may_create_the_table_again() {
    let root = tempfile::tempdir().unwrap();
    claim(root.path(), "t", &pipeline("a")).unwrap();
    update(root.path(), "t", |current| Ok(Some(with(current, "x")))).unwrap();
    release(root.path(), "t", &pipeline("a")).unwrap();
    assert_eq!(read(root.path(), "t").unwrap(), None);
    assert_eq!(owner(root.path(), "t").unwrap(), None);
    claim(root.path(), "t", &pipeline("b")).unwrap();
    assert_eq!(owner(root.path(), "t").unwrap().as_deref(), Some("b"));
    // Releasing what is not there, or what another pipeline now owns, changes nothing.
    release(root.path(), "u", &pipeline("a")).unwrap();
    release(root.path(), "t", &pipeline("a")).unwrap();
    assert_eq!(owner(root.path(), "t").unwrap().as_deref(), Some("b"));
}

#[test]
fn an_owner_that_cannot_be_read_is_an_error_not_a_missing_owner() {
    let root = tempfile::tempdir().unwrap();
    // A directory where the owner file belongs reads as neither an owner nor none.
    let owner_path = root
        .path()
        .join("_rdlt")
        .join("tables")
        .join("t")
        .join("owner");
    std::fs::create_dir_all(&owner_path).unwrap();
    assert!(owner(root.path(), "t").is_err());
    assert!(release(root.path(), "t", &pipeline("a")).is_err());
}

#[cfg(unix)]
#[test]
fn a_catalog_that_cannot_be_moved_away_is_not_released() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir().unwrap();
    claim(root.path(), "t", &pipeline("a")).unwrap();
    let tables = root.path().join("_rdlt").join("tables");
    std::fs::set_permissions(&tables, std::fs::Permissions::from_mode(0o555)).unwrap();
    let released = release(root.path(), "t", &pipeline("a"));
    std::fs::set_permissions(&tables, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert!(released.is_err(), "{released:?}");
    assert_eq!(owner(root.path(), "t").unwrap().as_deref(), Some("a"));
}
