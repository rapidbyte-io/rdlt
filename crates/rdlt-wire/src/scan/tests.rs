use prost::Message as _;

use super::{MAX_DEPTH, Unscanned, decoded, request, response};
use crate::v1;

/// The service's methods.
const METHODS: [&str; 16] = [
    "Handshake",
    "Configure",
    "Check",
    "Discover",
    "Plan",
    "Read",
    "Committed",
    "Open",
    "ApplySchema",
    "Write",
    "Commit",
    "Close",
    "Heartbeat",
    "ReadPublished",
    "ReadAcknowledged",
    "Ping",
];

#[test]
fn each_method_s_request_and_answer_have_a_form() {
    for method in &METHODS[..15] {
        assert!(request(method).is_some(), "{method}");
        assert!(response(method).is_some(), "{method}");
    }
    assert!(request("Ping").is_none(), "no method of that name");
    assert_eq!(response("Discover").map(|form| form.name), Some("Catalog"));
    assert_eq!(
        request("Commit").map(|form| form.name),
        Some("CommitRequest")
    );
}

#[test]
fn an_empty_message_holds_its_own_size() {
    let form = response("Discover").unwrap();
    assert_eq!(decoded(form, &[], usize::MAX), Ok(size_of::<v1::Catalog>()));
}

#[test]
fn each_empty_entry_of_a_repeated_message_holds_four_of_its_size() {
    let form = response("Discover").unwrap();
    let one = decoded(form, &[0x0a, 0x00], usize::MAX).unwrap();
    let two = decoded(form, &[0x0a, 0x00, 0x0a, 0x00], usize::MAX).unwrap();
    assert_eq!(two - one, 4 * size_of::<v1::StreamSpec>());
}

#[test]
fn a_string_holds_its_length_or_the_least_a_vector_allocates() {
    let form = response("Plan").unwrap();
    let encoded = |partition: &str| {
        v1::PlanResponse {
            partitions: vec![partition.to_owned()],
            ..v1::PlanResponse::default()
        }
        .encode_to_vec()
    };
    let base = size_of::<v1::PlanResponse>() + 4 * size_of::<String>();
    assert_eq!(decoded(form, &encoded("p"), usize::MAX), Ok(base + 8));
    let long = "p".repeat(100);
    assert_eq!(decoded(form, &encoded(&long), usize::MAX), Ok(base + 100));
}

#[test]
fn packed_numbers_hold_four_of_their_size_for_each_byte() {
    let form = response("Discover").unwrap();
    let catalog = v1::Catalog {
        streams: vec![v1::StreamSpec {
            read_modes: vec![1, 2, 3],
            ..v1::StreamSpec::default()
        }],
    };
    let packed = decoded(form, &catalog.encode_to_vec(), usize::MAX).unwrap();
    let empty = decoded(form, &[0x0a, 0x00], usize::MAX).unwrap();
    assert_eq!(packed - empty, 3 * 4 * 4);
}

#[test]
fn fields_the_form_does_not_know_hold_their_bytes() {
    let form = response("Discover").unwrap();
    // Field 15 as a varint, as eight bytes, as four bytes, as a length of bytes and as a group
    // holding a varint.
    let unknown = [
        0x78, 0x01, 0x79, 0, 0, 0, 0, 0, 0, 0, 0, 0x7d, 0, 0, 0, 0, 0x7a, 0x02, 0x0a, 0x00, 0x7b,
        0x08, 0x01, 0x7c,
    ];
    let counted = decoded(form, &unknown, usize::MAX).unwrap();
    // Each field's key aside, as the scan takes the key first.
    assert_eq!(counted, size_of::<v1::Catalog>() + unknown.len() - 5);
    assert!(
        v1::Catalog::decode(unknown.as_slice())
            .unwrap()
            .streams
            .is_empty()
    );
}

#[test]
fn a_group_the_form_does_not_know_is_walked_as_the_decoder_skips_it() {
    let form = response("Discover").unwrap();
    // An empty group of field 1000 before an empty stream.
    let grouped = [0xc3, 0x3e, 0xc4, 0x3e, 0x0a, 0x00];
    assert!(decoded(form, &grouped, usize::MAX).is_ok());
    assert!(v1::Catalog::decode(grouped.as_slice()).is_ok());
    // A group ended by another field's end, or not ended, does not decode.
    for unended in [&[0xc3, 0x3e, 0xcc, 0x3e][..], &[0xc3, 0x3e]] {
        assert_eq!(
            decoded(form, unended, usize::MAX),
            Err(Unscanned::Malformed)
        );
        assert!(v1::Catalog::decode(unended).is_err());
    }
}

#[test]
fn an_encoding_that_does_not_decode_is_refused() {
    let form = response("Discover").unwrap();
    for malformed in [
        &[0x0a][..],
        &[0x0a, 0x05, 0x00],
        &[0x0b],
        &[0x0c],
        &[0x0e],
        &[0x80; 11],
        &[0x09, 0, 0],
        &[0x0d, 0],
        &[0x00, 0x00],
        &[0x80, 0x80, 0x80, 0x80, 0x10, 0x00],
    ] {
        assert_eq!(
            decoded(form, malformed, usize::MAX),
            Err(Unscanned::Malformed),
            "{malformed:?}"
        );
    }
}

#[test]
fn a_count_beyond_its_bound_stops_the_walk_there() {
    let form = response("Discover").unwrap();
    let bloated = [0x0a, 0x00].repeat(1_000);
    let bound = 10 * size_of::<v1::StreamSpec>();
    let counted = decoded(form, &bloated, bound).unwrap();
    assert!(counted > bound);
    assert!(
        counted <= bound + 4 * size_of::<v1::StreamSpec>(),
        "{counted}"
    );
    // A malformed tail past the bound is not reached.
    let mut tailed = bloated.clone();
    tailed.push(0x0a);
    assert!(decoded(form, &tailed, bound).is_ok());
}

#[test]
fn groups_nest_as_deep_as_the_decoder_takes_them_and_no_deeper() {
    let form = response("Discover").unwrap();
    let nested = |levels: usize| [vec![0x7b; levels], vec![0x7c; levels]].concat();
    assert!(decoded(form, &nested(MAX_DEPTH - 1), usize::MAX).is_ok());
    assert!(v1::Catalog::decode(nested(MAX_DEPTH - 1).as_slice()).is_ok());
    assert_eq!(
        decoded(form, &nested(MAX_DEPTH), usize::MAX),
        Err(Unscanned::Deep)
    );
    assert!(v1::Catalog::decode(nested(MAX_DEPTH).as_slice()).is_err());
}

#[test]
fn an_entry_of_one_byte_holds_the_least_a_vector_of_bytes_allocates() {
    use super::{Field, Form, Kind};
    // No message of the protocol repeats a one-byte number; a form that does counts so.
    static FLAGS: Form = Form {
        name: "Flags",
        size: 24,
        fields: &[Field {
            number: 1,
            kind: Kind::Scalar(1),
            repeated: true,
        }],
    };
    assert_eq!(decoded(&FLAGS, &[0x08, 0x01], usize::MAX), Ok(24 + 8));
    assert_eq!(
        decoded(&FLAGS, &[0x0a, 0x02, 0x01, 0x00], usize::MAX),
        Ok(24 + 2 * 8)
    );
}

#[test]
fn a_field_of_the_largest_number_a_key_holds_is_walked_and_one_beyond_refused() {
    let catalog = response("Discover").unwrap();
    // A varint field of the largest number, 2^29 - 1, which no form knows, and of one more.
    let field = |number: u64| {
        let mut bytes = Vec::new();
        prost::encoding::encode_varint(number << 3, &mut bytes);
        bytes.push(0x01);
        bytes
    };
    let largest = u64::from(u32::MAX >> 3);
    let taken = field(largest);
    assert!(v1::Catalog::decode(taken.as_slice()).is_ok());
    assert!(decoded(catalog, &taken, usize::MAX).is_ok());
    let beyond = field(largest + 1);
    assert!(v1::Catalog::decode(beyond.as_slice()).is_err());
    assert_eq!(
        decoded(catalog, &beyond, usize::MAX),
        Err(Unscanned::Malformed)
    );
}
