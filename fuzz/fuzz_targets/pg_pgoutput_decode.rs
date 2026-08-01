//! Feature 009 T002: the pgoutput parser is total — arbitrary bytes may
//! only produce typed parse errors, never a panic or runaway allocation.

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    rdlt_connector_postgres::testsupport::source::fuzz_pgoutput_decode(data);
});
