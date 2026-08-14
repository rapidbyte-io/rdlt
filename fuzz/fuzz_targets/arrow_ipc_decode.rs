//! Fuzz: Arrow IPC stream decode over arbitrary bytes (047 M7) — the
//! construction the client performs on every `arrow_ipc` payload a
//! connector serves (`rdlt-connector-client`'s `decode_one_batch`: a
//! `StreamReader` over the raw frame bytes, batches pulled to stream
//! end): typed errors only, never a panic. A whole nested parser
//! sitting opposite arbitrary third-party bytes.

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let Ok(reader) = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(data), None)
    else {
        return;
    };
    for batch in reader {
        if batch.is_err() {
            return;
        }
    }
});
