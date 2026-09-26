//! The wire protocol between rdlt and connectors that run out of process: the messages of
//! package `rdlt.connector.v1`.

/// The messages of package `rdlt.connector.v1`, generated from the `.proto` files under
/// `proto/` by `cargo xtask codegen`.
#[expect(
    clippy::doc_markdown,
    clippy::struct_excessive_bools,
    reason = "generated code keeps the .proto files' comments and messages as written"
)]
#[path = "generated/rdlt.connector.v1.rs"]
pub mod v1;
