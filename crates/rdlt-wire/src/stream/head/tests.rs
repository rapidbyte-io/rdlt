use proptest::prelude::*;

use super::{Bound, Bounds, Call, HEAD_BYTES, Head, HeadError, Kind, Malformed, Rows};
use crate::limits::{Limit, Refusal};

/// An end that sends every kind of frame, each within `bytes`.
fn every(bytes: u32) -> Bounds {
    let bound = |limit| Bound { bytes, limit };
    Bounds {
        batch: Some(bound(Limit::FrameBytes)),
        json: Some(bound(Limit::JsonPushBytes)),
        message: bound(Limit::ControlMessageBytes),
    }
}

/// The bytes of a head of `len`, `code`, `flags`, `reserved` and `tag`, as given.
fn raw(len: u32, code: u8, flags: u8, reserved: u16, tag: u64) -> [u8; HEAD_BYTES] {
    let mut bytes = [0; HEAD_BYTES];
    bytes[..4].copy_from_slice(&len.to_le_bytes());
    bytes[4] = code;
    bytes[5] = flags;
    bytes[6..8].copy_from_slice(&reserved.to_le_bytes());
    bytes[8..].copy_from_slice(&tag.to_le_bytes());
    bytes
}

#[test]
fn a_head_s_bytes_are_its_length_kind_flags_and_tag_in_little_endian_order() {
    let head = Head::new(
        Kind::Batch {
            rows: Rows::Changes,
            dictionary: true,
            tag: 0x0102_0304_0506_0708,
        },
        0x0a0b_0c0d,
    );
    assert_eq!(
        head.encode(),
        [
            0x0d, 0x0c, 0x0b, 0x0a, 1, 3, 0, 0, 0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01
        ]
    );
    let opening = Head::new(
        Kind::Message {
            opens: Some(Call::ReadPublished),
        },
        7,
    );
    assert_eq!(opening.encode(), raw(7, 3, 3, 0, 0));
}

#[test]
fn a_malformed_head_is_refused_by_what_is_wrong_with_it() {
    let bounds = every(1024);
    let cases: [([u8; HEAD_BYTES], Malformed); 8] = [
        (raw(1, 0, 0, 0, 0), Malformed::Kind(0)),
        (raw(1, 4, 0, 0, 0), Malformed::Kind(4)),
        (
            raw(1, 1, 4, 0, 0),
            Malformed::Flags {
                kind: "batch",
                flags: 4,
            },
        ),
        (
            raw(1, 2, 1, 0, 0),
            Malformed::Flags {
                kind: "json",
                flags: 1,
            },
        ),
        (
            raw(1, 3, 4, 0, 0),
            Malformed::Flags {
                kind: "message",
                flags: 4,
            },
        ),
        (raw(1, 1, 0, 1, 0), Malformed::Reserved(1)),
        (
            raw(1, 2, 0, 0, 9),
            Malformed::Tag {
                kind: "json",
                tag: 9,
            },
        ),
        (
            raw(1, 3, 1, 0, 9),
            Malformed::Tag {
                kind: "message",
                tag: 9,
            },
        ),
    ];
    for (bytes, malformed) in cases {
        assert_eq!(
            Head::parse(bytes, &bounds),
            Err(HeadError::Malformed(malformed)),
            "{bytes:?}"
        );
    }
}

#[test]
fn a_kind_this_end_does_not_send_is_refused_before_its_length_is_read() {
    let bounds = Bounds {
        batch: None,
        json: None,
        ..every(1024)
    };
    for (code, kind) in [(1, "batch"), (2, "json")] {
        assert_eq!(
            Head::parse(raw(u32::MAX, code, 0, 0, 0), &bounds),
            Err(HeadError::Malformed(Malformed::Unexpected(kind)))
        );
    }
}

#[test]
fn a_body_beyond_its_kind_s_bound_is_refused_by_that_kind_s_limit() {
    let bounds = Bounds {
        batch: Some(Bound {
            bytes: 100,
            limit: Limit::FrameBytes,
        }),
        json: Some(Bound {
            bytes: 200,
            limit: Limit::JsonPushBytes,
        }),
        message: Bound {
            bytes: 300,
            limit: Limit::ControlMessageBytes,
        },
    };
    for (code, limit, bytes) in [
        (1, Limit::FrameBytes, 100),
        (2, Limit::JsonPushBytes, 200),
        (3, Limit::ControlMessageBytes, 300),
    ] {
        assert!(Head::parse(raw(bytes, code, 0, 0, 0), &bounds).is_ok());
        assert_eq!(
            Head::parse(raw(bytes + 1, code, 0, 0, 0), &bounds),
            Err(HeadError::Refused(Refusal {
                field: limit,
                limit: u64::from(bytes),
                actual: u64::from(bytes) + 1,
            }))
        );
    }
}

/// Any head a sender may send.
fn any_head() -> impl Strategy<Value = Head> {
    let opens = prop_oneof![
        Just(None),
        Just(Some(Call::Read)),
        Just(Some(Call::Write)),
        Just(Some(Call::ReadPublished)),
    ];
    let rows = prop_oneof![Just(Rows::Arrow), Just(Rows::Changes)];
    let kind = prop_oneof![
        (rows, any::<bool>(), any::<u64>()).prop_map(|(rows, dictionary, tag)| Kind::Batch {
            rows,
            dictionary,
            tag
        }),
        Just(Kind::Json),
        opens.prop_map(|opens| Kind::Message { opens }),
    ];
    (kind, any::<u32>()).prop_map(|(kind, len)| Head::new(kind, len))
}

proptest! {
    /// A head is parsed as it was encoded, by an end that sends its kind within its length.
    #[test]
    fn a_head_is_parsed_as_it_was_encoded(head in any_head()) {
        prop_assert_eq!(Head::parse(head.encode(), &every(u32::MAX)), Ok(head));
    }

    /// Whatever sixteen bytes arrive, a head parsed from them is encoded as them again, and its
    /// body is within its kind's bound.
    #[test]
    fn whatever_bytes_parse_as_a_head_are_its_encoding(
        bytes in any::<[u8; HEAD_BYTES]>(),
        bound in any::<u32>(),
    ) {
        if let Ok(head) = Head::parse(bytes, &every(bound)) {
            prop_assert_eq!(head.encode(), bytes);
            prop_assert!(head.len <= bound);
        }
    }
}
