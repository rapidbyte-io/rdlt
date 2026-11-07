//! A stream entry's transform steps, checked once, and each stream's engine transforms.

use rdlt_connector::ColumnPath;
use rdlt_engine::{Mask, Op, TransformStep as Step};

use crate::{Error, Host, MaskStep, MaskWith, TransformStep, codes};

/// A stream entry's transform step, checked once.
pub(super) enum Checked {
    /// The engine's step, which every stream the entry selects shares: an op, a mask with its
    /// key resolved, or the host's code.
    Shared(Step),
    /// A SQL transform's text, parsed once to check it; each stream parses it again, so each
    /// plans for the schemas it meets and none pushes another's plans out.
    Sql(String),
}

/// `steps`, in order, checked: mask keys resolved through `host`'s secrets, SQL parsed, and
/// code transforms found among the host's.
pub(super) async fn lowered(steps: &[TransformStep], host: &Host) -> Result<Vec<Checked>, Error> {
    let mut lowered = Vec::new();
    for step in steps {
        lowered.push(match step {
            TransformStep::Rename(pairs) => Checked::Shared(
                Op::rename(pairs.iter().map(|(from, to)| (from.as_str(), to.as_str()))).into(),
            ),
            TransformStep::Mask(mask) => {
                let with = masked(mask, host).await?;
                Checked::Shared(
                    Op::mask(mask.columns.iter().map(|column| path(column)), with).into(),
                )
            }
            TransformStep::Constant(columns) => Checked::Shared(
                Op::constant(
                    columns
                        .iter()
                        .map(|(column, value)| (column.as_str(), value.clone())),
                )
                .into(),
            ),
            TransformStep::Sql(text) => {
                sql(text)?;
                Checked::Sql(text.clone())
            }
            TransformStep::Code(name) => {
                Checked::Shared(host.code(name).ok_or_else(|| unknown(name, host))?.into())
            }
        });
    }
    Ok(lowered)
}

/// The engine's transforms of one stream from its entry's `steps`: the shared steps, and a SQL
/// transform of its own for each SQL step.
pub(super) fn bound(steps: &[Checked]) -> Result<Vec<Step>, Error> {
    steps
        .iter()
        .map(|step| match step {
            Checked::Shared(step) => Ok(step.clone()),
            Checked::Sql(text) => sql(text),
        })
        .collect()
}

/// The refusal of a code transform named `name`, which `host` does not register.
fn unknown(name: &str, host: &Host) -> Error {
    let registered: Vec<&str> = host.codes().collect();
    let registered = if registered.is_empty() {
        "none".to_owned()
    } else {
        registered.join(", ")
    };
    let message = format!(
        "a code transform names `{name}`, which the host does not register; it registers: \
         {registered}"
    );
    Error::config(codes::TRANSFORM_UNKNOWN, message)
}

/// The column `column` names: a top-level name, or a path through structs, a dot parting each
/// field.
fn path(column: &str) -> ColumnPath {
    ColumnPath::new(column.split('.')).expect("splitting text gives at least one part")
}

/// How `mask` masks each value, its key resolved through `host`'s secrets.
async fn masked(mask: &MaskStep, host: &Host) -> Result<Mask, Error> {
    let misplaced = match mask.with {
        MaskWith::HmacSha256 => mask.text.as_ref().map(|_| "text"),
        MaskWith::Redact => mask.key.as_ref().map(|_| "key"),
        MaskWith::Null => mask
            .key
            .as_ref()
            .map(|_| "key")
            .or(mask.text.as_ref().map(|_| "text")),
    };
    if let Some(field) = misplaced {
        let message = format!("a mask with {} takes no {field}", word(mask.with));
        return Err(Error::config(codes::DOCUMENT_UNKNOWN_KEY, message));
    }
    match mask.with {
        MaskWith::HmacSha256 => {
            let Some(key) = mask.key.as_deref() else {
                let message = "a mask with hmac_sha256 needs a key: a secret reference, such as \
                               ${secret:mask_key}";
                return Err(Error::config(codes::MASK_KEY_MISSING, message));
            };
            Ok(Mask::hmac_sha256(host.key(key).await?))
        }
        MaskWith::Redact => Ok(Mask::redact(mask.text.as_deref().unwrap_or("***"))),
        MaskWith::Null => Ok(Mask::Null),
    }
}

/// How a document writes `with`.
fn word(with: MaskWith) -> &'static str {
    match with {
        MaskWith::HmacSha256 => "hmac_sha256",
        MaskWith::Redact => "redact",
        MaskWith::Null => "null",
    }
}

/// The engine's transform for the SQL `text`, parsed, which plans once for each input schema it
/// meets.
#[cfg(feature = "sql")]
fn sql(text: &str) -> Result<Step, Error> {
    rdlt_sql::Sql::parse(text).map(Into::into).map_err(|error| {
        let message = format!("a sql transform is refused: {error}");
        Error::config_caused(error.code(), message, error)
    })
}

/// Refuses the SQL `text`: this build runs none.
#[cfg(not(feature = "sql"))]
fn sql(_text: &str) -> Result<Step, Error> {
    let message = "a sql transform needs the facade's `sql` feature, which this build lacks";
    Err(Error::config(codes::TRANSFORM_UNAVAILABLE, message))
}
