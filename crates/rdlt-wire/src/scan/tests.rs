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
fn each_empty_entry_of_a_repeated_message_holds_three_of_its_size() {
    let form = response("Discover").unwrap();
    let one = decoded(form, &[0x0a, 0x00], usize::MAX).unwrap();
    let two = decoded(form, &[0x0a, 0x00, 0x0a, 0x00], usize::MAX).unwrap();
    assert_eq!(two - one, 3 * size_of::<v1::StreamSpec>());
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
    let base = size_of::<v1::PlanResponse>() + 3 * size_of::<String>();
    assert_eq!(decoded(form, &encoded("p"), usize::MAX), Ok(base + 8));
    let long = "p".repeat(100);
    assert_eq!(decoded(form, &encoded(&long), usize::MAX), Ok(base + 100));
}

#[test]
fn packed_numbers_hold_three_of_their_size_for_each_byte() {
    let form = response("Discover").unwrap();
    let catalog = v1::Catalog {
        streams: vec![v1::StreamSpec {
            read_modes: vec![1, 2, 3],
            ..v1::StreamSpec::default()
        }],
    };
    let packed = decoded(form, &catalog.encode_to_vec(), usize::MAX).unwrap();
    let empty = decoded(form, &[0x0a, 0x00], usize::MAX).unwrap();
    assert_eq!(packed - empty, 3 * 4 * 3);
}

#[test]
fn fields_the_form_does_not_know_hold_nothing() {
    let form = response("Discover").unwrap();
    // Field 15 as a varint, as eight bytes, as four bytes and as a length of bytes.
    let unknown = [
        0x78, 0x01, 0x79, 0, 0, 0, 0, 0, 0, 0, 0, 0x7d, 0, 0, 0, 0, 0x7a, 0x02, 0x0a, 0x00,
    ];
    assert_eq!(
        decoded(form, &unknown, usize::MAX),
        Ok(size_of::<v1::Catalog>())
    );
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
        counted <= bound + 3 * size_of::<v1::StreamSpec>(),
        "{counted}"
    );
    // A malformed tail past the bound is not reached.
    let mut tailed = bloated.clone();
    tailed.push(0x0a);
    assert!(decoded(form, &tailed, bound).is_ok());
}

#[test]
fn an_encoding_nesting_deeper_than_any_form_is_refused() {
    use super::{Field, Form, Kind};
    static LOOP: Form = Form {
        name: "Loop",
        size: 8,
        fields: &[Field {
            number: 1,
            kind: Kind::Message(&LOOP),
            repeated: false,
        }],
    };
    let mut nested = Vec::new();
    for _ in 0..MAX_DEPTH {
        let mut outer = vec![0x0a, u8::try_from(nested.len()).unwrap()];
        outer.extend(&nested);
        nested = outer;
    }
    assert_eq!(decoded(&LOOP, &nested, usize::MAX), Err(Unscanned::Deep));
    assert!(decoded(&LOOP, &nested[2..], usize::MAX).is_ok());
}

#[test]
fn unpacked_numbers_hold_three_of_their_size_each() {
    let form = response("Discover").unwrap();
    // A stream whose read modes, field 5, come one varint each rather than packed.
    let unpacked = [0x0a, 0x04, 0x28, 0x01, 0x28, 0x02];
    let counted = decoded(form, &unpacked, usize::MAX).unwrap();
    let empty = decoded(form, &[0x0a, 0x00], usize::MAX).unwrap();
    assert_eq!(counted - empty, 2 * 3 * 4);
}
