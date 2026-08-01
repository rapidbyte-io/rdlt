//! Builder-misuse errors: every `build()` rejection arm produces a typed
//! config error naming the offender — none may slip through to run time.

use rdlt::prelude::*;
use rdlt_connector::Destination as _;
use rdlt_testkit::{MemoryDestination, MemorySource};

fn source() -> MemorySource {
    MemorySource::new(vec![])
}

fn merge_less() -> MemoryDestination {
    let caps = MemoryDestination::new().capabilities().with_merge(false);
    MemoryDestination::new().with_capabilities(caps)
}

/// A destination without merge capability rejects Merge at build — both as
/// the default write mode and per-stream, each naming what requested it.
#[test]
fn merge_against_a_merge_less_destination_is_rejected_at_build() {
    let dest = merge_less();
    let err = Pipeline::builder("p")
        .source(source())
        .destination(dest.clone())
        .write_mode(WriteMode::Merge {
            key: vec!["id".into()],
        })
        .build()
        .expect_err("default merge must be rejected");
    assert!(
        err.to_string().contains("default write mode"),
        "names the default request: {err}"
    );

    let err = Pipeline::builder("p")
        .source(source())
        .destination(dest)
        .write_mode_for(
            "orders",
            WriteMode::Merge {
                key: vec!["id".into()],
            },
        )
        .build()
        .expect_err("per-stream merge must be rejected");
    assert!(
        err.to_string().contains("orders"),
        "names the stream: {err}"
    );
}

/// Merge with an EMPTY key is rejected in both spellings.
#[test]
fn empty_merge_key_is_rejected_at_build() {
    let err = Pipeline::builder("p")
        .source(source())
        .destination(MemoryDestination::new())
        .write_mode(WriteMode::Merge { key: vec![] })
        .build()
        .expect_err("empty default key");
    assert!(err.to_string().contains("at least one key column"), "{err}");

    let err = Pipeline::builder("p")
        .source(source())
        .destination(MemoryDestination::new())
        .write_mode_for("orders", WriteMode::Merge { key: vec![] })
        .build()
        .expect_err("empty per-stream key");
    assert!(
        err.to_string().contains("`orders`"),
        "names the stream: {err}"
    );
}
