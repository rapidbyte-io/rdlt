use serde::Deserialize;
use serde::de::value::{
    BoolDeserializer, BytesDeserializer, CharDeserializer, Error, F64Deserializer, I64Deserializer,
    I128Deserializer, StrDeserializer, U64Deserializer, U128Deserializer,
};

use super::{Count, Mapped, Text, flag, nearest};
use crate::limits::KEY_BYTES;

/// What a reader said refusing a value, which must not quote it.
fn said<T>(read: Result<T, Error>) -> String {
    let said = read.err().expect("refused").to_string();
    for canary in ["31337424", "9876543", "CANARY"] {
        assert!(!said.contains(canary), "{said}");
    }
    said
}

#[test]
fn text_refuses_every_other_scalar_by_its_kind() {
    said(Text::deserialize(BoolDeserializer::<Error>::new(true)));
    said(Text::deserialize(I64Deserializer::<Error>::new(31_337_424)));
    said(Text::deserialize(U64Deserializer::<Error>::new(31_337_424)));
    said(Text::deserialize(I128Deserializer::<Error>::new(
        31_337_424,
    )));
    said(Text::deserialize(U128Deserializer::<Error>::new(
        31_337_424,
    )));
    said(Text::deserialize(F64Deserializer::<Error>::new(
        0.987_654_3,
    )));
}

#[test]
fn a_count_takes_a_whole_number_within_its_type_and_refuses_the_rest_by_kind() {
    let Ok(Count(seven)) = Count::<u8>::deserialize(U64Deserializer::<Error>::new(7)) else {
        panic!("a count of 7 reads");
    };
    assert_eq!(seven, 7);
    let Ok(Count(seven)) = Count::<u8>::deserialize(I64Deserializer::<Error>::new(7)) else {
        panic!("a signed count of 7 reads");
    };
    assert_eq!(seven, 7);
    said(Count::<u8>::deserialize(U64Deserializer::<Error>::new(
        31_337_424,
    )));
    said(Count::<u8>::deserialize(I64Deserializer::<Error>::new(
        -31_337_424,
    )));
    said(Count::<u8>::deserialize(BoolDeserializer::<Error>::new(
        true,
    )));
    said(Count::<u8>::deserialize(I128Deserializer::<Error>::new(
        31_337_424,
    )));
    said(Count::<u8>::deserialize(U128Deserializer::<Error>::new(
        31_337_424,
    )));
    said(Count::<u8>::deserialize(F64Deserializer::<Error>::new(
        0.987_654_3,
    )));
    said(Count::<u8>::deserialize(StrDeserializer::<Error>::new(
        "CANARY",
    )));
}

#[test]
fn a_reader_refuses_a_character_and_bytes_by_kind() {
    let bytes = || BytesDeserializer::<Error>::new(b"CANARY");
    said(Text::deserialize(bytes()));
    said(Count::<u8>::deserialize(bytes()));
    said(Mapped::<serde_json::Value>::deserialize(bytes()));
    said(flag(bytes()));
    let character = || CharDeserializer::<Error>::new('Z');
    let Ok(Text(text)) = Text::deserialize(character()) else {
        panic!("a character reads as text");
    };
    assert_eq!(text, "Z");
    for refused in [
        said(Count::<u8>::deserialize(character())),
        said(Mapped::<serde_json::Value>::deserialize(character())),
        said(flag(character())),
    ] {
        assert!(!refused.contains('Z'), "{refused}");
    }
}

#[test]
fn nearest_finds_a_word_within_two_edits_and_leaves_far_or_long_ones() {
    let known = ["append", "replace", "merge"];
    assert_eq!(nearest("apend", known), Some("append"));
    assert_eq!(nearest("replce", known), Some("replace"));
    assert_eq!(nearest("mer", known), Some("merge"));
    assert_eq!(nearest("append", known), Some("append"));
    assert_eq!(nearest("xylophone", known), None);
    assert_eq!(nearest("app", known), None);
    assert_eq!(nearest("ap", known), None);
    let long = "a".repeat(KEY_BYTES);
    let longer = "a".repeat(KEY_BYTES + 1);
    let known: &'static str = Box::leak(long.clone().into_boxed_str());
    assert_eq!(nearest(&long, [known]), Some(known));
    assert_eq!(nearest(&longer, [known]), None);
}
