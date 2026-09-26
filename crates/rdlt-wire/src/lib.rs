//! The wire protocol between rdlt and connectors that run out of process: the messages of
//! package `rdlt.connector.v1`, the codec carrying Arrow batches in them, and the limits each
//! end enforces on what it receives.

pub mod codec;
pub mod error;
pub mod limits;

/// The messages of package `rdlt.connector.v1`, generated from the `.proto` files under
/// `proto/` by `cargo xtask codegen`.
#[expect(
    clippy::doc_markdown,
    clippy::struct_excessive_bools,
    reason = "generated code keeps the .proto files' comments and messages as written"
)]
#[path = "generated/rdlt.connector.v1.rs"]
pub mod v1;

pub use codec::{Decoder, Encoder, IpcFrame};
pub use error::{Frame, Problem, WireError};
pub use limits::{Limits, Refusal};
