//! Shredding untrusted JSON gives one answer whichever way it is shredded: cut into small chunks
//! each built within the engine's allowance, cut into large chunks with room to build whatever
//! they hold, or cut small with every chunk parsed exactly from its start.
//!
//! The answer is the batches, concatenated and compared value for value, a column of JSON by its
//! text, which keeps each number as it was written; or one refusal, its code and its message,
//! which names the record, key or number refused where it names one. No way fails inside the
//! shredder. A push refused for a repeated key and for something else may be refused for either.

#![forbid(unsafe_code)]
#![no_main]

use arrow_array::RecordBatch;
use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use rdlt_engine::bench::{self, ADMITTED, Parse, Refused};
use rdlt_engine::codes;

/// Bytes: a chunk larger than any input the fuzzer makes, so each push is one chunk.
const LARGE_CHUNK: usize = 1 << 20;

/// Bytes a byte of text is admitted for where a chunk's builders have room for whatever it holds.
const ROOMY: u64 = 1 << 20;

/// One way of shredding the pushes.
#[derive(Debug)]
struct Way {
    chunk_bytes: usize,
    admitted: u64,
    parse: Parse,
}

/// What a way answers: the batches as one, or the refusal.
type Answer = Result<Option<RecordBatch>, Refused>;

fuzz_target!(|input: &[u8]| {
    // The first byte sizes the small chunks; the rest is the push.
    let Some((&size, json)) = input.split_first() else {
        return;
    };
    let small = usize::from(size) * 16 + 1;
    let push = Bytes::copy_from_slice(json);
    // Two copies meet across pushes and chunks, joining their shapes.
    let pushes = [push.clone(), push];
    let ways = [
        Way {
            chunk_bytes: small,
            admitted: ADMITTED,
            parse: Parse::Fast,
        },
        Way {
            chunk_bytes: LARGE_CHUNK,
            admitted: ROOMY,
            parse: Parse::Fast,
        },
        Way {
            chunk_bytes: small,
            admitted: ROOMY,
            parse: Parse::Exact,
        },
    ];
    let answers = ways.map(|way| {
        let answer = bench::shred_with(&pushes, way.chunk_bytes, way.admitted, way.parse);
        if let Err(refused) = &answer {
            let internal = codes::SHRED_INTERNAL.as_str();
            assert_ne!(refused.code, internal, "{way:?}: {}", refused.message);
        }
        (way, answer.map(|batches| joined(&batches)))
    });
    if refused_for_a_repeated_key_among_others(&answers) {
        return;
    }
    let [(engine, first), rest @ ..] = &answers;
    for (way, answer) in rest {
        assert_eq!(answer, first, "{way:?} against {engine:?}");
    }
});

/// Whether every way refuses and one refuses a key an object repeats, which may differ between
/// them: a key repeated in a value is found at the key where its column builds the value, at the
/// object's end where it renders it as JSON text, and only on building again where it skips it,
/// so a push refused for another reason too may be refused for either.
fn refused_for_a_repeated_key_among_others(answers: &[(Way, Answer)]) -> bool {
    let duplicate = codes::JSON_DUPLICATE_KEY.as_str();
    let repeated = |answer: &Answer| matches!(answer, Err(refused) if refused.code == duplicate);
    answers.iter().all(|(_, answer)| answer.is_err())
        && answers.iter().any(|(_, answer)| repeated(answer))
}

/// The batches of one way as one batch, or none where the pushes hold no record.
fn joined(batches: &[RecordBatch]) -> Option<RecordBatch> {
    let first = batches.first()?;
    let joined = arrow_select::concat::concat_batches(&first.schema(), batches);
    Some(joined.expect("one way's batches share one schema"))
}
