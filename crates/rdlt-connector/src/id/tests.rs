use std::time::{Duration, UNIX_EPOCH};

use proptest::prelude::*;

use super::{
    CommitSeq, ConnectorId, Epoch, IdError, LoadId, PartitionId, PipelineId, StreamName, TablePath,
};

#[test]
fn text_ids_accept_values_at_their_length_limit() {
    let longest = "x".repeat(128);
    assert_eq!(
        PipelineId::parse(longest.clone()).unwrap().as_str(),
        longest
    );
}

#[test]
fn text_ids_accept_their_alphabets() {
    assert_eq!(
        PipelineId::parse("orders-prod_2.v1").unwrap().as_str(),
        "orders-prod_2.v1"
    );
    assert_eq!(
        ConnectorId::parse("io.rapidbyte.postgres")
            .unwrap()
            .to_string(),
        "io.rapidbyte.postgres"
    );
    assert_eq!(
        PartitionId::parse("ctid 0..1000 ✓").unwrap().as_str(),
        "ctid 0..1000 ✓"
    );
    assert_eq!(PartitionId::whole(), PartitionId::parse("whole").unwrap());
}

#[test]
fn text_ids_reject_what_their_alphabets_exclude() {
    let cases = [
        (
            PipelineId::parse("").unwrap_err(),
            IdError::Empty {
                kind: "pipeline id",
            },
        ),
        (
            PipelineId::parse("orders prod").unwrap_err(),
            IdError::InvalidChar {
                kind: "pipeline id",
                character: ' ',
            },
        ),
        (
            PipelineId::parse("x".repeat(129)).unwrap_err(),
            IdError::TooLong {
                kind: "pipeline id",
                max: 128,
                actual: 129,
            },
        ),
        (
            ConnectorId::parse("io.Rapidbyte").unwrap_err(),
            IdError::InvalidChar {
                kind: "connector id",
                character: 'R',
            },
        ),
        (
            PartitionId::parse("a\nb").unwrap_err(),
            IdError::InvalidChar {
                kind: "partition id",
                character: '\n',
            },
        ),
    ];
    for (actual, expected) in cases {
        assert_eq!(actual, expected);
    }
}

#[test]
fn ids_deserialize_only_when_valid() {
    let valid: PipelineId = serde_json::from_str("\"orders\"").unwrap();
    assert_eq!(valid.as_str(), "orders");
    assert!(serde_json::from_str::<PipelineId>("\"has space\"").is_err());
    assert!(serde_json::from_str::<TablePath>("[]").is_err());
}

#[test]
fn stream_names_deserialize_only_when_valid() {
    let valid = StreamName::with_namespace("public", "orders").unwrap();
    let json = serde_json::to_string(&valid).unwrap();
    assert_eq!(serde_json::from_str::<StreamName>(&json).unwrap(), valid);
    for invalid in [
        r#"{"namespace":null,"name":""}"#,
        r#"{"namespace":"","name":"orders"}"#,
        r#"{"namespace":null,"name":"\u001b[31m"}"#,
    ] {
        assert!(
            serde_json::from_str::<StreamName>(invalid).is_err(),
            "{invalid}"
        );
    }
}

#[test]
fn stream_names_display_with_their_namespace() {
    assert_eq!(StreamName::new("orders").unwrap().to_string(), "orders");
    let qualified = StreamName::with_namespace("public", "orders").unwrap();
    assert_eq!(qualified.to_string(), "public.orders");
    assert_eq!(qualified.namespace(), Some("public"));
    assert_eq!(qualified.name(), "orders");
    assert!(StreamName::with_namespace("", "orders").is_err());
}

#[test]
fn table_paths_need_at_least_one_valid_segment() {
    assert_eq!(
        TablePath::new(["orders", "items"]).unwrap().to_string(),
        "orders/items"
    );
    assert_eq!(
        TablePath::new(Vec::<&str>::new()).unwrap_err(),
        IdError::Empty { kind: "table path" }
    );
    assert!(TablePath::new(["orders", ""]).is_err());
}

#[test]
fn load_ids_are_ordered_by_time() {
    let earlier = LoadId::from_parts(UNIX_EPOCH + Duration::from_secs(100), u128::MAX);
    let later = LoadId::from_parts(UNIX_EPOCH + Duration::from_secs(101), 0);
    assert!(earlier < later);
    assert_eq!(
        earlier.as_bytes()[6] >> 4,
        7,
        "the version nibble marks UUIDv7"
    );
    assert_eq!(
        LoadId::from_parts(UNIX_EPOCH, 0).to_string(),
        "00000000-0000-7000-8000-000000000000"
    );
}

#[test]
fn load_ids_before_the_epoch_clamp_to_it() {
    let before = LoadId::from_parts(UNIX_EPOCH - Duration::from_secs(5), 1);
    assert_eq!(before, LoadId::from_parts(UNIX_EPOCH, 1));
}

#[test]
fn counters_advance_by_one() {
    assert_eq!(CommitSeq::FIRST.get(), 1);
    assert_eq!(CommitSeq::FIRST.next().get(), 2);
    assert_eq!(Epoch(u64::MAX).next(), Epoch(u64::MAX));
    assert_eq!(Epoch(4).next(), Epoch(5));
}

proptest! {
    #[test]
    fn parsed_ids_round_trip_through_json(text in "[A-Za-z0-9._-]{1,128}") {
        let id = PipelineId::parse(&text).unwrap();
        let json = serde_json::to_string(&id).unwrap();
        prop_assert_eq!(serde_json::from_str::<PipelineId>(&json).unwrap(), id);
    }

    #[test]
    fn table_paths_round_trip_through_json(segments in proptest::collection::vec("[^\\p{Cc}]{1,20}", 1..5)) {
        let path = TablePath::new(&segments).unwrap();
        let json = serde_json::to_string(&path).unwrap();
        prop_assert_eq!(serde_json::from_str::<TablePath>(&json).unwrap(), path);
    }
}

#[test]
fn load_ids_differ_when_their_random_bits_differ() {
    assert_ne!(
        LoadId::from_parts(UNIX_EPOCH, 1),
        LoadId::from_parts(UNIX_EPOCH, 2)
    );
    assert_ne!(
        LoadId::from_parts(UNIX_EPOCH, 1 << 64),
        LoadId::from_parts(UNIX_EPOCH, 2 << 64)
    );
}
