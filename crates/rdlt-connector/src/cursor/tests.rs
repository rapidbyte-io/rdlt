use bytes::Bytes;
use proptest::prelude::*;
use serde::{Deserialize, Serialize};

use super::Cursor;
use crate::error::ConnectorErrorKind;
use crate::limits::MAX_CURSOR_BYTES;

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
    let cursor = Cursor::new(1, Bytes::from_static(b"not json")).unwrap();
    assert_eq!(
        cursor.decode::<Since>(1).unwrap_err().kind(),
        ConnectorErrorKind::Data
    );
}

#[test]
fn cursors_over_the_limit_are_refused() {
    let limit = usize::try_from(MAX_CURSOR_BYTES).unwrap();
    assert!(Cursor::new(1, Bytes::from(vec![0; limit])).is_ok());
    let error = Cursor::new(1, Bytes::from(vec![0; limit + 1])).unwrap_err();
    assert_eq!(error.limit().unwrap().actual, MAX_CURSOR_BYTES + 1);
}

#[test]
fn deserializing_refuses_bad_base64() {
    assert!(serde_json::from_str::<Cursor>(r#"{"version":1,"base64":"@@@"}"#).is_err());
}

proptest! {
    #[test]
    fn cursors_round_trip_through_json(version in any::<u16>(), bytes in proptest::collection::vec(any::<u8>(), 0..512)) {
        let cursor = Cursor::new(version, Bytes::from(bytes)).unwrap();
        let json = serde_json::to_string(&cursor).unwrap();
        prop_assert_eq!(serde_json::from_str::<Cursor>(&json).unwrap(), cursor);
    }
}
