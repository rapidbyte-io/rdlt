//! A pipeline's streams: what each loads and how, and what every stream takes by default.

use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{DeleteMode, TransformStep, WriteMode, read};
use super::{Hint, Nested, OnTruncate, OnUnsupported, ReadMode, RetentionLoss, SchemaPolicy};

/// A stream the pipeline loads, or every stream of the source's catalog its name matches where
/// the name holds `*` or `?`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct Stream {
    /// The stream's name, as the source's catalog shows it, or a pattern of `*` and `?`.
    #[serde(deserialize_with = "read::text")]
    pub name: String,
    /// How it is read; the default's, else in full.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read: Option<ReadMode>,
    /// How its rows reach its table; the default's, else appended.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write: Option<WriteMode>,
    /// What its deletes do, for a change stream merged by key; hard by default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deletes: Option<DeleteMode>,
    /// What its truncates do, for a change stream merged by key; applied by default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_truncate: Option<OnTruncate>,
    /// What it does when its source no longer holds where a read would resume; fails by default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_retention_loss: Option<RetentionLoss>,
    /// What its schema does, where it differs from the default's.
    #[serde(
        default,
        deserialize_with = "read::optional_mapping",
        skip_serializing_if = "Option::is_none"
    )]
    pub schema: Option<SchemaRules>,
    /// How its nested values are stored, where it differs from the default's.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nested: Option<Nested>,
    /// Each top-level column's own settings, by name.
    #[serde(
        default,
        deserialize_with = "read::named_mappings",
        skip_serializing_if = "BTreeMap::is_empty"
    )]
    pub columns: BTreeMap<String, Column>,
    /// The only top-level columns it loads, beside its keys and change columns; none listed is
    /// a restriction too, which the plan refuses, and unlike this being absent.
    #[serde(
        rename = "include",
        default,
        deserialize_with = "read::optional_texts",
        skip_serializing_if = "Option::is_none"
    )]
    pub included: Option<Vec<String>>,
    /// Top-level columns it leaves out.
    #[serde(
        default,
        deserialize_with = "read::texts",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub exclude: Vec<String>,
    /// What reshapes its rows, in order, before they reach its table.
    #[serde(
        default,
        deserialize_with = "read::list",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub transform: Vec<TransformStep>,
}

impl Stream {
    /// The stream `name`, or every stream a pattern `name` matches, with the defaults.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            read: None,
            write: None,
            deletes: None,
            on_truncate: None,
            on_retention_loss: None,
            schema: None,
            nested: None,
            columns: BTreeMap::new(),
            included: None,
            exclude: Vec::new(),
            transform: Vec::new(),
        }
    }

    /// The stream, read as `mode` says.
    #[must_use]
    pub fn read(mut self, mode: ReadMode) -> Self {
        self.read = Some(mode);
        self
    }

    /// The stream, written as `mode` says.
    #[must_use]
    pub fn write(mut self, mode: WriteMode) -> Self {
        self.write = Some(mode);
        self
    }

    /// The stream, its deletes doing as `mode` says.
    #[must_use]
    pub fn deletes(mut self, mode: DeleteMode) -> Self {
        self.deletes = Some(mode);
        self
    }

    /// The stream, its truncates doing as `mode` says.
    #[must_use]
    pub fn on_truncate(mut self, mode: OnTruncate) -> Self {
        self.on_truncate = Some(mode);
        self
    }

    /// The stream, doing as `retention` says where its source dropped where a read would resume.
    #[must_use]
    pub fn on_retention_loss(mut self, retention: RetentionLoss) -> Self {
        self.on_retention_loss = Some(retention);
        self
    }

    /// The stream, its schema doing as `rules` say.
    #[must_use]
    pub fn schema(mut self, rules: SchemaRules) -> Self {
        self.schema = Some(rules);
        self
    }

    /// The stream, its nested values stored as `nested` says.
    #[must_use]
    pub fn nested(mut self, nested: Nested) -> Self {
        self.nested = Some(nested);
        self
    }

    /// The stream, its column `name` set as `column` says.
    #[must_use]
    pub fn column(mut self, name: impl Into<String>, column: Column) -> Self {
        self.columns.insert(name.into(), column);
        self
    }

    /// The stream, loading only `columns` of its top-level columns, beside those listed before,
    /// its keys and its change columns.
    #[must_use]
    pub fn include_only(mut self, columns: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.included
            .get_or_insert_default()
            .extend(columns.into_iter().map(Into::into));
        self
    }

    /// The stream, leaving out `columns` too.
    #[must_use]
    pub fn exclude(mut self, columns: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.exclude.extend(columns.into_iter().map(Into::into));
        self
    }

    /// The stream, reshaped by `step` after the steps before it.
    #[must_use]
    pub fn transform(mut self, step: TransformStep) -> Self {
        self.transform.push(step);
        self
    }
}

/// What every stream takes where it says nothing.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct Defaults {
    /// How streams are read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read: Option<ReadMode>,
    /// How streams' rows reach their tables.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write: Option<WriteMode>,
    /// What schemas do.
    #[serde(
        default,
        deserialize_with = "read::optional_mapping",
        skip_serializing_if = "Option::is_none"
    )]
    pub schema: Option<SchemaRules>,
    /// How nested values are stored.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nested: Option<Nested>,
}

impl Defaults {
    /// None: each stream takes the engine's.
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether nothing is set.
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }

    /// The defaults, reading as `mode` says.
    #[must_use]
    pub fn read(mut self, mode: ReadMode) -> Self {
        self.read = Some(mode);
        self
    }

    /// The defaults, writing as `mode` says.
    #[must_use]
    pub fn write(mut self, mode: WriteMode) -> Self {
        self.write = Some(mode);
        self
    }

    /// The defaults, schemas doing as `rules` say.
    #[must_use]
    pub fn schema(mut self, rules: SchemaRules) -> Self {
        self.schema = Some(rules);
        self
    }

    /// The defaults, nested values stored as `nested` says.
    #[must_use]
    pub fn nested(mut self, nested: Nested) -> Self {
        self.nested = Some(nested);
        self
    }
}

/// What a schema does with a value that would change its table, and with a change its
/// destination cannot apply.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct SchemaRules {
    /// What happens to a value that would change its table, or that its column cannot hold.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<SchemaPolicy>,
    /// What happens to a change the destination cannot apply in place.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_unsupported: Option<OnUnsupported>,
}

impl SchemaRules {
    /// None: each takes the level above's.
    pub fn new() -> Self {
        Self::default()
    }

    /// The rules, with `policy`.
    #[must_use]
    pub fn policy(mut self, policy: SchemaPolicy) -> Self {
        self.policy = Some(policy);
        self
    }

    /// The rules, doing as `unsupported` says with a change the destination cannot apply.
    #[must_use]
    pub fn on_unsupported(mut self, unsupported: OnUnsupported) -> Self {
        self.on_unsupported = Some(unsupported);
        self
    }
}

/// A column's own settings, over its stream's and the defaults.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct Column {
    /// What happens to a value that would change its table, or that it cannot hold.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<SchemaPolicy>,
    /// What happens to a change of it the destination cannot apply in place.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_unsupported: Option<OnUnsupported>,
    /// How its nested values are stored.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nested: Option<Nested>,
    /// Its fixed type: values that do not fit it are changes its policy handles.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hint: Option<Hint>,
}

impl Column {
    /// None: the column takes its stream's settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// The column, with `policy`.
    #[must_use]
    pub fn policy(mut self, policy: SchemaPolicy) -> Self {
        self.policy = Some(policy);
        self
    }

    /// The column, doing as `unsupported` says with a change the destination cannot apply.
    #[must_use]
    pub fn on_unsupported(mut self, unsupported: OnUnsupported) -> Self {
        self.on_unsupported = Some(unsupported);
        self
    }

    /// The column, its nested values stored as `nested` says.
    #[must_use]
    pub fn nested(mut self, nested: Nested) -> Self {
        self.nested = Some(nested);
        self
    }

    /// The column, of the fixed type `hint`.
    #[must_use]
    pub fn hint(mut self, hint: Hint) -> Self {
        self.hint = Some(hint);
        self
    }
}
