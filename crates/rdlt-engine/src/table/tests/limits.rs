//! What a table's model may grow to: its schema's version, and what state may record of it.

use rdlt_connector::{LogicalType, SchemaVersion, TableState};

use super::{capabilities, created, plan, resolver, schema};
use crate::error::ErrorKind;
use crate::table::model::Model;

/// The state recording `model` at `version`.
fn recorded(model: &Model, version: u32) -> TableState {
    TableState {
        schema: Some((SchemaVersion(version), model.schema())),
        physical: Some("s".into()),
        names: model.names.clone(),
        sequences: None,
        history: false,
        exact: model.exact.clone(),
    }
}

#[test]
fn a_schema_change_past_the_last_version_is_refused() {
    let resolver = resolver(capabilities(), plan(), &[]);
    let model = created(&resolver, &[("id", LogicalType::Int64)]);
    let wider = schema(&[("id", LogicalType::Int64), ("extra", LogicalType::Utf8)]);
    let next = Model::from_state(Some(&recorded(&model, u32::MAX - 1))).unwrap();
    assert_eq!(
        resolver.resolve(&next, &wider).unwrap().model.version,
        u32::MAX
    );
    let last = Model::from_state(Some(&recorded(&model, u32::MAX))).unwrap();
    let error = resolver.resolve(&last, &wider).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Schema);
    assert_eq!(error.code(), Some("schema_version_exhausted"));
    // A batch that changes nothing still fits.
    let same = schema(&[("id", LogicalType::Int64)]);
    assert_eq!(
        resolver.resolve(&last, &same).unwrap().model.version,
        u32::MAX
    );
}

#[test]
fn a_recorded_schema_at_version_zero_is_refused() {
    let resolver = resolver(capabilities(), plan(), &[]);
    let model = created(&resolver, &[("id", LogicalType::Int64)]);
    let error = Model::from_state(Some(&recorded(&model, 0))).unwrap_err();
    assert_eq!(error.code(), Some("state_invalid"));
    assert_eq!(
        Model::from_state(Some(&recorded(&model, 1)))
            .unwrap()
            .version,
        1
    );
}

#[test]
fn a_change_past_the_last_an_attempt_counts_is_refused() {
    let plain = resolver(capabilities(), plan(), &[]);
    let mut model = created(&plain, &[("id", LogicalType::Int64)]);
    model.revision = u32::MAX;
    let wider = schema(&[("id", LogicalType::Int64), ("extra", LogicalType::Utf8)]);
    let error = plain.resolve(&model, &wider).unwrap_err();
    assert_eq!(error.code(), Some("schema_version_exhausted"));
    // A normalized stream's table is created by a batch of nulls alone.
    let mut normalized = resolver(capabilities(), plan(), &[]);
    normalized.meta.id = Some("_rdlt_id".into());
    let nulls = schema(&[("n", LogicalType::Null)]);
    let fresh = normalized.resolve(&Model::default(), &nulls).unwrap();
    assert_eq!((fresh.model.version, fresh.model.revision), (1, 1));
    let counted = Model {
        revision: u32::MAX,
        ..Model::default()
    };
    let error = normalized.resolve(&counted, &nulls).unwrap_err();
    assert_eq!(error.code(), Some("schema_version_exhausted"));
}
