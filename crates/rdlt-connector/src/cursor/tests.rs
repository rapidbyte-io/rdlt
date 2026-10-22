use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use bytes::Bytes;
use proptest::prelude::*;
use serde::{Deserialize, Serialize};

use super::Cursor;
use crate::error::ConnectorErrorKind;
use rdlt_wire::limits::CURSOR_BYTES;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Since {
    updated_after: Option<String>,
}

#[test]
fn typed_cursors_round_trip() {
    let since = Since {
        updated_after: Some("2026-09-23T00:00:00Z".to_owned()),
    };
    let cursor = Cursor::encode(3, &since).unwrap();
    assert_eq!(cursor.version(), 3);
    assert_eq!(
        cursor.bytes().as_ref(),
        br#"{"updated_after":"2026-09-23T00:00:00Z"}"#
    );
    assert_eq!(cursor.decode::<Since>(3).unwrap(), since);
}

#[test]
fn a_cursor_in_another_format_is_a_config_error() {
    let cursor = Cursor::encode(
        1,
        &Since {
            updated_after: None,
        },
    )
    .unwrap();
    let error = cursor.decode::<Since>(2).unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::Config);
    assert_eq!(error.code(), Some("cursor_version"));
}

#[test]
fn malformed_cursor_bytes_are_a_data_error() {
    let cursor = Cursor::new(1, b"not json").unwrap();
    assert_eq!(
        cursor.decode::<Since>(1).unwrap_err().kind(),
        ConnectorErrorKind::Data
    );
}

#[test]
fn cursors_over_the_limit_are_refused() {
    let limit = usize::try_from(CURSOR_BYTES).unwrap();
    assert!(Cursor::new(1, &vec![0; limit]).is_ok());
    let error = Cursor::new(1, &vec![0; limit + 1]).unwrap_err();
    assert_eq!(error.limit().unwrap().actual, CURSOR_BYTES + 1);
}

/// A cursor as JSON: its version, and its bytes in base64 as one string.
#[derive(Serialize)]
struct Spelled {
    version: u16,
    base64: String,
}

proptest! {
    #[test]
    fn a_cursor_is_written_as_its_version_and_its_bytes_in_base64_and_read_back(
        version in any::<u16>(),
        bytes in proptest::collection::vec(any::<u8>(), 0..2048),
    ) {
        let cursor = Cursor::new(version, &bytes).unwrap();
        let json = serde_json::to_vec(&cursor).unwrap();
        let spelled = Spelled { version, base64: STANDARD.encode(&bytes) };
        prop_assert_eq!(&json, &serde_json::to_vec(&spelled).unwrap());
        prop_assert_eq!(serde_json::from_slice::<Cursor>(&json).unwrap(), cursor.clone());
        // A reader hands the decoder text it is about to reuse, not text it can borrow.
        prop_assert_eq!(serde_json::from_reader::<_, Cursor>(json.as_slice()).unwrap(), cursor);
    }
}

#[test]
fn a_cursor_s_base64_escaped_in_json_reads_as_its_text() {
    let json = r#"{"version":1,"base64":"\u0059Q=="}"#;
    let cursor: Cursor = serde_json::from_str(json).unwrap();
    assert_eq!(cursor, Cursor::new(1, b"a").unwrap());
}

#[test]
fn a_cursor_missing_a_member_naming_another_or_of_bad_base64_is_refused() {
    for json in [
        r#"{"version":1}"#,
        r#"{"base64":"YQ=="}"#,
        r#"{"version":1,"base64":"YQ==","other":1}"#,
        r#"{"version":1,"base64":"Y"}"#,
        r#"{"version":1,"base64":"@@@"}"#,
        r#"{"version":1,"base64":7}"#,
    ] {
        assert!(serde_json::from_str::<Cursor>(json).is_err(), "{json}");
    }
}

#[test]
fn a_cursor_in_json_beyond_the_cursor_limit_is_refused() {
    let bytes = vec![0_u8; usize::try_from(CURSOR_BYTES).unwrap() + 1];
    let json = serde_json::to_vec(&Spelled {
        version: 1,
        base64: STANDARD.encode(&bytes),
    })
    .unwrap();
    assert!(serde_json::from_slice::<Cursor>(&json).is_err());
}

#[test]
fn a_cursor_owns_exactly_its_bytes() {
    // A few bytes of a large message, as a transport hands them over.
    let message = Bytes::from(vec![7_u8; 1 << 20]);
    let cursor = Cursor::new(1, &message.slice(100..104)).unwrap();
    assert_eq!(cursor.bytes().as_ref(), [7_u8; 4]);
    assert!(
        message.is_unique(),
        "the cursor keeps the message it arrived in alive"
    );
}

#[test]
fn a_cursor_whose_bytes_are_not_text_says_what_it_expects() {
    let error = serde_json::from_str::<Cursor>(r#"{"version":1,"base64":7}"#).unwrap_err();
    assert!(
        error.to_string().contains("a cursor's bytes in base64"),
        "{error}"
    );
}
