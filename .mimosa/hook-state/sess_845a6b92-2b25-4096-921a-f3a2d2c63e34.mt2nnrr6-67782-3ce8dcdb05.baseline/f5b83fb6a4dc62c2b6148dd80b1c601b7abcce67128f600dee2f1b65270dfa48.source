// Include shim: `build.rs` compiles `proto/rdlt_connector_v0.proto` into
// `$OUT_DIR/rdlt.connector.v0.rs` (the file name follows the proto
// `package`). This file's only job is pulling that generated source in —
// it is spliced (via `lib.rs`'s own `include!`) as the body of the
// `crate::proto` module, so `//!` inner-doc comments are invalid here: they
// would document whatever follows at the splice site, not this file.
include!(concat!(env!("OUT_DIR"), "/rdlt.connector.v0.rs"));
