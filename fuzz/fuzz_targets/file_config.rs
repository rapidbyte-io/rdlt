//! Fuzz: user configuration parsing (feature 003 R22 target 3).
//!
//! Config YAML is user-typed input; parsing/validation must return typed errors,
//! never panic or hang.

#![no_main]

use libfuzzer_sys::fuzz_target;
use rdlt_connector_file::FileSource;

fuzz_target!(|data: &[u8]| {
    let Ok(text) = std::str::from_utf8(data) else {
        return;
    };
    let _ = FileSource::from_yaml(text);
});
