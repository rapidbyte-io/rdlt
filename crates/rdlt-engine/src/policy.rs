//! Schema policies: what the engine does with a batch that would change its table (spec §8.4,
//! §8.7), set per pipeline, stream, table and column.

#[cfg(test)]
mod tests;

/// What happens to values that would change their table's schema.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SchemaPolicy {
    /// Change the schema.
    #[default]
    Evolve,
    /// Fail with a schema error.
    Freeze,
    /// Drop every row carrying the change, and count them.
    DiscardRow,
    /// Load the row with the offending value nulled, and count the values.
    DiscardValue,
}

/// What happens to a change the destination cannot apply in place.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum OnUnsupported {
    /// Add a sibling column `<name>__<kind>` for the values of the new type; older values stay.
    #[default]
    VariantColumn,
    /// Fail with a schema error.
    Refuse,
}

/// How nested values are stored.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Nested {
    /// As the destination's native struct and list types, or as `Json` where it has none.
    #[default]
    Native,
    /// As one `Json` value per nested column.
    Json,
    /// Normalized (spec §8.7): arrays become child tables at any depth, objects flatten into one
    /// column per field, and containers nested deeper than `max_depth` are stored as `Json`.
    ///
    /// Only pipelines and streams normalize; a column set to [`Nested::Native`] or
    /// [`Nested::Json`] in a normalized stream is stored whole. The settings of
    /// a stream's column apply to the columns it flattens into and the child tables of its arrays.
    Normalize {
        /// How deep objects and arrays normalize; deeper ones are stored as `Json`.
        max_depth: u8,
    },
}

impl Nested {
    /// Normalized to the default depth of 8.
    pub const fn normalize() -> Self {
        Self::Normalize { max_depth: 8 }
    }
}

/// Schema settings at one level of a pipeline; unset settings inherit from the level above:
/// column, then table, then stream, then pipeline.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SchemaSettings {
    policy: Option<SchemaPolicy>,
    on_unsupported: Option<OnUnsupported>,
    nested: Option<Nested>,
}

impl SchemaSettings {
    /// Settings that all inherit.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the schema policy.
    #[must_use]
    pub fn policy(mut self, policy: SchemaPolicy) -> Self {
        self.policy = Some(policy);
        self
    }

    /// Sets what happens to changes the destination cannot apply.
    #[must_use]
    pub fn on_unsupported(mut self, on_unsupported: OnUnsupported) -> Self {
        self.on_unsupported = Some(on_unsupported);
        self
    }

    /// Sets how nested values are stored.
    #[must_use]
    pub fn nested(mut self, nested: Nested) -> Self {
        self.nested = Some(nested);
        self
    }

    /// How nested values are stored, if these settings say.
    pub(crate) fn nested_setting(self) -> Option<Nested> {
        self.nested
    }
}

/// The settings that apply to one column once inheritance is resolved.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Resolved {
    pub(crate) policy: SchemaPolicy,
    pub(crate) on_unsupported: OnUnsupported,
    pub(crate) nested: Nested,
}

/// Resolves a column's settings over the chain column → table → stream → pipeline: each setting
/// comes from the most specific level that sets it, and defaults otherwise.
///
/// This is the only place settings are inherited.
pub(crate) fn resolve(levels: [Option<&SchemaSettings>; 4]) -> Resolved {
    let set = || levels.iter().flatten();
    Resolved {
        policy: set().find_map(|level| level.policy).unwrap_or_default(),
        on_unsupported: set()
            .find_map(|level| level.on_unsupported)
            .unwrap_or_default(),
        nested: set().find_map(|level| level.nested).unwrap_or_default(),
    }
}
