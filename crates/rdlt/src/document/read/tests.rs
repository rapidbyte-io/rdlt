use serde::Deserialize;
use serde::de::value::{
    BoolDeserializer, Error, F64Deserializer, I64Deserializer, I128Deserializer, StrDeserializer,
    U64Deserializer, U128Deserializer,
};

use super::{Count, Text};

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
