//! A document's words for modes, policies and types as the engine's.

use rdlt_connector::{DecimalType, LogicalType, TimeUnit};
use rdlt_engine::SchemaSettings;

use crate::{
    Column, DeleteMode, Error, Hint, Nested, OnTruncate, OnUnsupported, ReadMode, RetentionLoss,
    SchemaPolicy, SchemaRules, Until, codes,
};

/// How a stream is read, as the source reads it.
pub(super) fn read(mode: ReadMode) -> rdlt_connector::ReadMode {
    match mode {
        ReadMode::Full => rdlt_connector::ReadMode::Full,
        ReadMode::Incremental => rdlt_connector::ReadMode::Incremental,
        ReadMode::Cdc => rdlt_connector::ReadMode::Cdc,
    }
}

/// What a change stream's deletes do, as the engine does it.
pub(super) fn deletes(mode: DeleteMode) -> rdlt_engine::DeleteMode {
    match mode {
        DeleteMode::Hard => rdlt_engine::DeleteMode::Hard,
        DeleteMode::Soft => rdlt_engine::DeleteMode::Soft,
        DeleteMode::Ignore => rdlt_engine::DeleteMode::Ignore,
    }
}

/// What a change stream's truncates do, as the engine does it.
pub(super) fn truncates(mode: OnTruncate) -> rdlt_engine::OnTruncate {
    match mode {
        OnTruncate::Apply => rdlt_engine::OnTruncate::Apply,
        OnTruncate::Ignore => rdlt_engine::OnTruncate::Ignore,
    }
}

/// What a stream does when its source dropped where a read would resume, as the engine does it.
pub(super) fn retention(loss: RetentionLoss) -> rdlt_engine::RetentionLoss {
    match loss {
        RetentionLoss::Fail => rdlt_engine::RetentionLoss::Fail,
        RetentionLoss::Reset => rdlt_engine::RetentionLoss::Reset,
    }
}

/// How long a run reads, as the engine reads it.
pub(super) fn until(until: Until) -> rdlt_engine::Until {
    match until {
        Until::Exhausted => rdlt_engine::Until::Exhausted,
        Until::Forever => rdlt_engine::Until::Forever,
        Until::For(duration) => rdlt_engine::Until::For(duration),
    }
}

/// The engine's schema settings of `rules` and `nested`.
pub(super) fn schema(rules: Option<&SchemaRules>, nested: Option<Nested>) -> SchemaSettings {
    let mut settings = SchemaSettings::new();
    if let Some(policy) = rules.and_then(|rules| rules.policy) {
        settings = settings.policy(self::policy(policy));
    }
    if let Some(unsupported) = rules.and_then(|rules| rules.on_unsupported) {
        settings = settings.on_unsupported(self::unsupported(unsupported));
    }
    if let Some(nested) = nested {
        settings = settings.nested(self::nested(nested));
    }
    settings
}

/// The engine's settings of `column`, where it sets any but its type.
pub(super) fn column(column: Column) -> Option<SchemaSettings> {
    let rules = SchemaRules {
        policy: column.policy,
        on_unsupported: column.on_unsupported,
    };
    let set = rules.policy.is_some() || rules.on_unsupported.is_some() || column.nested.is_some();
    set.then(|| schema(Some(&rules), column.nested))
}

fn policy(policy: SchemaPolicy) -> rdlt_engine::SchemaPolicy {
    match policy {
        SchemaPolicy::Evolve => rdlt_engine::SchemaPolicy::Evolve,
        SchemaPolicy::Freeze => rdlt_engine::SchemaPolicy::Freeze,
        SchemaPolicy::DiscardRow => rdlt_engine::SchemaPolicy::DiscardRow,
        SchemaPolicy::DiscardValue => rdlt_engine::SchemaPolicy::DiscardValue,
    }
}

fn unsupported(unsupported: OnUnsupported) -> rdlt_engine::OnUnsupported {
    match unsupported {
        OnUnsupported::VariantColumn => rdlt_engine::OnUnsupported::VariantColumn,
        OnUnsupported::Refuse => rdlt_engine::OnUnsupported::Refuse,
    }
}

fn nested(nested: Nested) -> rdlt_engine::Nested {
    match nested {
        Nested::Native => rdlt_engine::Nested::Native,
        Nested::Json => rdlt_engine::Nested::Json,
        Nested::Normalize { max_depth } => rdlt_engine::Nested::Normalize { max_depth },
    }
}

/// The logical type `hint` names.
pub(super) fn logical(hint: Hint) -> Result<LogicalType, Error> {
    Ok(match hint {
        Hint::Bool => LogicalType::Bool,
        Hint::Int8 => LogicalType::Int8,
        Hint::Int16 => LogicalType::Int16,
        Hint::Int32 => LogicalType::Int32,
        Hint::Int64 => LogicalType::Int64,
        Hint::Float32 => LogicalType::Float32,
        Hint::Float64 => LogicalType::Float64,
        Hint::Utf8 => LogicalType::Utf8,
        Hint::Binary => LogicalType::Binary,
        Hint::Date => LogicalType::Date,
        Hint::Uuid => LogicalType::Uuid,
        Hint::Json => LogicalType::Json,
        Hint::Timestamp => LogicalType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        Hint::Decimal { precision, scale } => {
            let decimal = DecimalType::new(precision, scale).map_err(|error| {
                let message = format!("a decimal hint's precision and scale are not one: {error}");
                Error::config_caused(codes::DOCUMENT_INVALID, message, error)
            })?;
            LogicalType::Decimal(decimal)
        }
    })
}
