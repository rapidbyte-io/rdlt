//! A source's catalog and a destination's capabilities on the wire.

use std::collections::BTreeSet;
use std::num::NonZeroU16;

use super::types::type_kind;
use super::{Invalid, narrow, required, v1};
use crate::capabilities::{
    Capabilities, CommitKind, DeleteModes, IdentifierCase, IdentifierChars, IdentifierRules,
    NestedSupport, SchemaChanges, WriteModes,
};
use crate::catalog::{Catalog, Checkpointing, Partitioning, ReadMode, StreamSpec};
use crate::id::StreamName;
use crate::schema::{ColumnPath, TableSchema};

impl From<ReadMode> for v1::ReadMode {
    fn from(mode: ReadMode) -> Self {
        match mode {
            ReadMode::Full => Self::Full,
            ReadMode::Incremental => Self::Incremental,
            ReadMode::Cdc => Self::Cdc,
        }
    }
}

/// The read mode `value` names.
pub(super) fn read_mode(value: i32) -> Result<ReadMode, Invalid> {
    match v1::ReadMode::try_from(value) {
        Ok(v1::ReadMode::Full) => Ok(ReadMode::Full),
        Ok(v1::ReadMode::Incremental) => Ok(ReadMode::Incremental),
        Ok(v1::ReadMode::Cdc) => Ok(ReadMode::Cdc),
        Ok(v1::ReadMode::Unspecified) | Err(_) => Err(Invalid::Unknown("read mode")),
    }
}

fn partitioning(value: i32) -> Result<Partitioning, Invalid> {
    match v1::Partitioning::try_from(value) {
        Ok(v1::Partitioning::Single) => Ok(Partitioning::Single),
        Ok(v1::Partitioning::Planned) => Ok(Partitioning::Planned),
        Ok(v1::Partitioning::Unspecified) | Err(_) => Err(Invalid::Unknown("partitioning")),
    }
}

fn checkpointing(value: i32) -> Result<Checkpointing, Invalid> {
    match v1::Checkpointing::try_from(value) {
        Ok(v1::Checkpointing::Natural) => Ok(Checkpointing::Natural),
        Ok(v1::Checkpointing::OnDemand) => Ok(Checkpointing::OnDemand),
        Ok(v1::Checkpointing::Unspecified) | Err(_) => Err(Invalid::Unknown("checkpointing")),
    }
}

impl From<&StreamSpec> for v1::StreamSpec {
    fn from(spec: &StreamSpec) -> Self {
        let paths = |paths: &[ColumnPath]| paths.iter().map(v1::ColumnPath::from).collect();
        Self {
            name: Some(v1::StreamName::from(spec.name())),
            schema: spec.schema().map(v1::TableSchema::from),
            primary_key: spec
                .primary_key()
                .map(|key| v1::ColumnPaths { paths: paths(key) }),
            cursor_fields: paths(spec.cursor_fields()),
            read_modes: spec
                .read_modes()
                .iter()
                .map(|mode| v1::ReadMode::from(*mode) as i32)
                .collect(),
            partitioning: match spec.partitioning() {
                Partitioning::Single => v1::Partitioning::Single,
                Partitioning::Planned => v1::Partitioning::Planned,
            } as i32,
            checkpointing: match spec.checkpointing() {
                Checkpointing::Natural => v1::Checkpointing::Natural,
                Checkpointing::OnDemand => v1::Checkpointing::OnDemand,
            } as i32,
            replayable: spec.is_replayable(),
            change_time: spec.change_time().map(v1::ColumnPath::from),
        }
    }
}

/// The column paths `paths` hold.
fn column_paths(paths: Vec<v1::ColumnPath>) -> Result<Vec<ColumnPath>, Invalid> {
    paths.into_iter().map(ColumnPath::try_from).collect()
}

impl TryFrom<v1::StreamSpec> for StreamSpec {
    type Error = Invalid;

    fn try_from(spec: v1::StreamSpec) -> Result<Self, Invalid> {
        let name = StreamName::try_from(required("stream name", spec.name)?)?;
        let modes = spec
            .read_modes
            .into_iter()
            .map(read_mode)
            .collect::<Result<Vec<_>, _>>()?;
        let mut decoded = Self::new(name)
            .with_read_modes(modes)
            .with_partitioning(partitioning(spec.partitioning)?)
            .with_checkpointing(checkpointing(spec.checkpointing)?)
            .with_replayable(spec.replayable);
        if let Some(schema) = spec.schema {
            decoded = decoded.with_schema(TableSchema::try_from(schema)?);
        }
        if let Some(key) = spec.primary_key {
            decoded = decoded.with_primary_key(column_paths(key.paths)?);
        }
        for field in column_paths(spec.cursor_fields)? {
            decoded = decoded.with_cursor_field(field);
        }
        if let Some(column) = spec.change_time {
            decoded = decoded.with_change_time(ColumnPath::try_from(column)?);
        }
        Ok(decoded)
    }
}

impl From<&Catalog> for v1::Catalog {
    fn from(catalog: &Catalog) -> Self {
        Self {
            streams: catalog.iter().map(v1::StreamSpec::from).collect(),
        }
    }
}

impl TryFrom<v1::Catalog> for Catalog {
    type Error = Invalid;

    fn try_from(catalog: v1::Catalog) -> Result<Self, Invalid> {
        let streams = catalog
            .streams
            .into_iter()
            .map(StreamSpec::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        Self::new(streams).map_err(|error| Invalid::rejected("catalog", error))
    }
}

impl From<&IdentifierRules> for v1::IdentifierRules {
    fn from(rules: &IdentifierRules) -> Self {
        Self {
            case: match rules.case {
                IdentifierCase::Preserve => v1::IdentifierCase::Preserve,
                IdentifierCase::Lower => v1::IdentifierCase::Lower,
                IdentifierCase::Upper => v1::IdentifierCase::Upper,
            } as i32,
            max_len: u32::from(rules.max_len.get()),
            chars: match rules.chars {
                IdentifierChars::Any => v1::IdentifierChars::Any,
                IdentifierChars::AsciiWord => v1::IdentifierChars::AsciiWord,
            } as i32,
            reserved: rules.reserved.iter().cloned().collect(),
            reserved_table_prefixes: rules.reserved_table_prefixes.iter().cloned().collect(),
        }
    }
}

impl TryFrom<v1::IdentifierRules> for IdentifierRules {
    type Error = Invalid;

    fn try_from(rules: v1::IdentifierRules) -> Result<Self, Invalid> {
        let case = match v1::IdentifierCase::try_from(rules.case) {
            Ok(v1::IdentifierCase::Preserve) => IdentifierCase::Preserve,
            Ok(v1::IdentifierCase::Lower) => IdentifierCase::Lower,
            Ok(v1::IdentifierCase::Upper) => IdentifierCase::Upper,
            Ok(v1::IdentifierCase::Unspecified) | Err(_) => {
                return Err(Invalid::Unknown("identifier case"));
            }
        };
        let chars = match v1::IdentifierChars::try_from(rules.chars) {
            Ok(v1::IdentifierChars::Any) => IdentifierChars::Any,
            Ok(v1::IdentifierChars::AsciiWord) => IdentifierChars::AsciiWord,
            Ok(v1::IdentifierChars::Unspecified) | Err(_) => {
                return Err(Invalid::Unknown("identifier characters"));
            }
        };
        let max_len: u16 = narrow("identifier length", rules.max_len)?;
        Ok(Self {
            case,
            max_len: NonZeroU16::new(max_len).ok_or(Invalid::OutOfRange("identifier length"))?,
            chars,
            reserved: rules.reserved.into_iter().collect(),
            reserved_table_prefixes: rules.reserved_table_prefixes.into_iter().collect(),
        })
    }
}

impl From<&Capabilities> for v1::Capabilities {
    fn from(capabilities: &Capabilities) -> Self {
        let modes = &capabilities.write_modes;
        let nested = &capabilities.nested;
        Self {
            commit: match capabilities.commit {
                CommitKind::Transactional => v1::CommitKind::Transactional,
                CommitKind::Manifest => v1::CommitKind::Manifest,
            } as i32,
            write_modes: Some(v1::WriteModes {
                append: modes.append,
                replace: modes.replace,
                merge: modes.merge,
                history: modes.history,
            }),
            delete_modes: Some(v1::DeleteModes {
                hard: capabilities.delete_modes.hard,
                soft: capabilities.delete_modes.soft,
            }),
            partial_updates: capabilities.partial_updates,
            nested: Some(v1::NestedSupport {
                structs: nested.structs,
                lists: nested.lists,
                json: nested.json,
            }),
            types: capabilities
                .types
                .iter()
                .map(|kind| v1::TypeKind::from(*kind) as i32)
                .collect(),
            schema_changes: Some(v1::SchemaChanges {
                add_column: capabilities.schema_changes.add_column,
                widenings: capabilities
                    .schema_changes
                    .widenings
                    .iter()
                    .map(|(from, to)| v1::Widening {
                        from: v1::TypeKind::from(*from) as i32,
                        to: v1::TypeKind::from(*to) as i32,
                    })
                    .collect(),
            }),
            identifiers: Some(v1::IdentifierRules::from(&capabilities.identifiers)),
            max_parallel_writers: u32::from(capabilities.max_parallel_writers.get()),
            preferred_batch_bytes: capabilities.preferred_batch_bytes,
        }
    }
}

impl TryFrom<v1::Capabilities> for Capabilities {
    type Error = Invalid;

    fn try_from(capabilities: v1::Capabilities) -> Result<Self, Invalid> {
        let commit = match v1::CommitKind::try_from(capabilities.commit) {
            Ok(v1::CommitKind::Transactional) => CommitKind::Transactional,
            Ok(v1::CommitKind::Manifest) => CommitKind::Manifest,
            Ok(v1::CommitKind::Unspecified) | Err(_) => {
                return Err(Invalid::Unknown("commit kind"));
            }
        };
        let modes = required("write modes", capabilities.write_modes)?;
        let deletes = required("delete modes", capabilities.delete_modes)?;
        let nested = required("nested support", capabilities.nested)?;
        let changes = required("schema changes", capabilities.schema_changes)?;
        let widenings = changes
            .widenings
            .into_iter()
            .map(|widening| Ok((type_kind(widening.from)?, type_kind(widening.to)?)))
            .collect::<Result<BTreeSet<_>, Invalid>>()?;
        let writers: u16 = narrow("parallel writers", capabilities.max_parallel_writers)?;
        Ok(Self {
            commit,
            write_modes: WriteModes {
                append: modes.append,
                replace: modes.replace,
                merge: modes.merge,
                history: modes.history,
            },
            delete_modes: DeleteModes {
                hard: deletes.hard,
                soft: deletes.soft,
            },
            partial_updates: capabilities.partial_updates,
            nested: NestedSupport {
                structs: nested.structs,
                lists: nested.lists,
                json: nested.json,
            },
            types: capabilities
                .types
                .into_iter()
                .map(type_kind)
                .collect::<Result<_, _>>()?,
            schema_changes: SchemaChanges {
                add_column: changes.add_column,
                widenings,
            },
            identifiers: IdentifierRules::try_from(required(
                "identifiers",
                capabilities.identifiers,
            )?)?,
            max_parallel_writers: NonZeroU16::new(writers)
                .ok_or(Invalid::OutOfRange("parallel writers"))?,
            preferred_batch_bytes: capabilities.preferred_batch_bytes,
        })
    }
}
