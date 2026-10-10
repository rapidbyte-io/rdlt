use std::time::Duration;

use super::{
    LAST_WORDS, LAST_WORDS_BYTES, OUTPUT_BYTES_BURST, OUTPUT_BYTES_PER_SECOND, OUTPUT_LINE_BYTES,
    OUTPUT_LINES_BURST, OUTPUT_LINES_PER_SECOND, SECRET_BYTES, SECRET_NAME_BYTES,
    SECRET_REFERENCES, TAIL_BYTES,
};

#[test]
fn the_limits_are_those_documented() {
    assert_eq!(TAIL_BYTES, 8_192);
    assert_eq!(LAST_WORDS_BYTES, 3_072);
    assert_eq!(OUTPUT_LINE_BYTES, 1_024);
    assert_eq!(OUTPUT_LINES_BURST, 256);
    assert_eq!(OUTPUT_LINES_PER_SECOND, 32);
    assert_eq!(OUTPUT_BYTES_BURST, 4_194_304);
    assert_eq!(OUTPUT_BYTES_PER_SECOND, 1_048_576);
    assert_eq!(LAST_WORDS, Duration::from_secs(1));
    assert_eq!(SECRET_REFERENCES, 1_024);
    assert_eq!(SECRET_BYTES, 65_536);
    assert_eq!(SECRET_NAME_BYTES, 4_096);
}
