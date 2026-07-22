//! The format layer — what each file format IS to rdlt, shared by the
//! source and (after the dest absorption) the destination side.

pub(crate) mod jsonl;
pub(crate) mod parquet;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Format {
    Jsonl,
    Parquet,
}
