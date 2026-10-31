use super::{CONTROL_STRING_BYTES, FRAME_BYTES, Limit, Limits, MESSAGE_ROOM, Refusal, count};
use crate::v1;

#[test]
fn a_value_at_its_limit_is_admitted_and_one_beyond_refused_with_what_it_measured() {
    let limits = Limits::default();
    let at = usize::try_from(FRAME_BYTES).unwrap();
    assert_eq!(limits.admit_frame(at), Ok(()));
    assert_eq!(
        limits.admit_frame(at + 1),
        Err(Refusal {
            field: Limit::FrameBytes,
            limit: FRAME_BYTES,
            actual: FRAME_BYTES + 1,
        })
    );
    let long = "x".repeat(usize::try_from(CONTROL_STRING_BYTES).unwrap() + 1);
    let refusal = limits.admit_string(&long).unwrap_err();
    assert_eq!(
        (refusal.field, refusal.actual),
        (Limit::ControlStringBytes, CONTROL_STRING_BYTES + 1)
    );
}

#[test]
fn each_admission_measures_against_its_own_limit() {
    let limits = Limits {
        frame_bytes: 1,
        batch_rows: 2,
        schema_columns: 3,
        nesting_depth: 4,
        json_push_bytes: 5,
        cursor_bytes: 6,
        config_bytes: 7,
        control_string_bytes: 8,
        batch_values: 9,
        schema_bytes: 10,
        dictionary_bytes: 11,
        ..Limits::default()
    };
    let refused =
        |result: Result<(), Refusal>| result.map_err(|refusal| (refusal.field, refusal.limit));
    assert_eq!(
        refused(limits.admit_json(6)),
        Err((Limit::JsonPushBytes, 5))
    );
    assert_eq!(
        refused(limits.admit_cursor(7)),
        Err((Limit::CursorBytes, 6))
    );
    assert_eq!(
        refused(limits.admit_config(8)),
        Err((Limit::ConfigBytes, 7))
    );
    assert_eq!(
        refused(limits.admit_string("123456789")),
        Err((Limit::ControlStringBytes, 8))
    );
    assert_eq!(
        refused(limits.admit_schema(11)),
        Err((Limit::SchemaBytes, 10))
    );
    assert_eq!(refused(limits.admit_schema(10)), Ok(()));
    assert_eq!(refused(limits.admit_json(5)), Ok(()));
}

#[test]
fn limits_cross_the_wire_unchanged() {
    let limits = Limits {
        frame_bytes: 1,
        batch_rows: 2,
        schema_columns: 3,
        nesting_depth: 4,
        json_push_bytes: 5,
        cursor_bytes: 6,
        config_bytes: 7,
        control_string_bytes: 8,
        batch_values: 9,
        schema_bytes: 10,
        dictionary_bytes: 11,
        catalog_bytes: 12,
        state_bytes: 13,
        control_message_bytes: 14,
    };
    assert_eq!(Limits::from(v1::Limits::from(limits)), limits);
}

#[test]
fn the_default_limits_are_the_protocols() {
    assert_eq!(
        Limits::default(),
        Limits {
            frame_bytes: 67_108_864,
            batch_rows: 1_048_576,
            schema_columns: 10_000,
            nesting_depth: 64,
            json_push_bytes: 67_108_864,
            cursor_bytes: 4_194_304,
            config_bytes: 8_388_608,
            control_string_bytes: 65_536,
            batch_values: 67_108_864,
            schema_bytes: 4_194_304,
            // A frame's bytes and its message's room.
            dictionary_bytes: 67_109_120,
            catalog_bytes: 4_194_304,
            state_bytes: 16_777_216,
            control_message_bytes: 262_144,
        }
    );
}

#[test]
fn a_limit_the_peer_leaves_unset_is_the_protocols_default() {
    // But the dictionaries' limit, which a peer sets as it is: one of none is below the least.
    let unset = v1::Limits::default();
    assert_eq!(
        Limits::from(unset),
        Limits {
            dictionary_bytes: 0,
            ..Limits::default()
        }
    );
    let refused = Limits::from(unset).admit_peer().unwrap_err();
    assert_eq!((refused.field, refused.actual), (Limit::DictionaryBytes, 0));
    let partly = v1::Limits {
        batch_rows: 7,
        batch_values: 8,
        schema_bytes: 9,
        dictionary_bytes: 10,
        ..v1::Limits::default()
    };
    assert_eq!(
        Limits::from(partly),
        Limits {
            batch_rows: 7,
            batch_values: 8,
            schema_bytes: 9,
            dictionary_bytes: 10,
            ..Limits::default()
        }
    );
}

/// The words of the comment above the line of `text` that is `declaration`, past its attributes,
/// as one line.
fn documented(text: &str, declaration: &str) -> String {
    let lines: Vec<&str> = text.lines().collect();
    let at = lines
        .iter()
        .position(|line| line.trim() == declaration)
        .unwrap_or_else(|| panic!("`{declaration}` is declared"));
    let mut comment: Vec<&str> = lines[..at]
        .iter()
        .rev()
        .skip_while(|line| line.trim_start().starts_with("#["))
        .take_while(|line| line.trim_start().starts_with("//"))
        .copied()
        .collect();
    comment.reverse();
    comment
        .iter()
        .flat_map(|line| line.split_whitespace())
        .filter(|word| !word.chars().all(|c| c == '/'))
        .collect::<Vec<_>>()
        .join(" ")
}

#[test]
fn the_protocol_names_each_least_a_peer_may_set_and_the_limit_without_a_default() {
    use super::{MIN_BATCH_ROWS, MIN_BATCH_VALUES, MIN_DICTIONARY_BYTES, MIN_FRAME_BYTES};
    // The generated code is what a Rust peer reads: it must say what the protocol file says.
    let proto = include_str!("../../proto/rdlt/connector/v1/connector.proto");
    let generated = include_str!("../generated/rdlt.connector.v1.rs");
    let sources = [
        (proto, "message Limits {", "uint64 dictionary_bytes = 11;"),
        (
            generated,
            "pub struct Limits {",
            "pub dictionary_bytes: u64,",
        ),
    ];
    for (text, message, field) in sources {
        let limits = documented(text, message);
        for least in [
            format!("{MIN_FRAME_BYTES} bytes"),
            format!("{MIN_BATCH_ROWS} rows"),
            format!("{MIN_BATCH_VALUES} values"),
            format!("dictionary_bytes below {MIN_DICTIONARY_BYTES}"),
        ] {
            assert!(limits.contains(&least), "{least:?} is not in {limits:?}");
        }
        assert!(
            limits.contains("dictionary_bytes, which has no default"),
            "{limits:?}"
        );
        let dictionary = documented(text, field);
        assert!(
            dictionary.contains(&format!("at least {MIN_DICTIONARY_BYTES}")),
            "{dictionary:?}"
        );
    }
}

#[test]
fn the_credit_floor_the_calls_and_a_messages_overhead_are_the_protocols() {
    assert_eq!(super::CREDIT_FLOOR, super::MIN_FRAME_BYTES);
    assert_eq!(super::MAX_CALLS, 200);
    let limits = Limits {
        frame_bytes: 10,
        ..Limits::default()
    };
    assert_eq!(limits.decoding(super::Class::Data), 10 + 65_536);
}

/// The least limits a peer may set: each a sender cuts batches to at its protocol minimum.
fn least() -> Limits {
    use super::{MIN_BATCH_ROWS, MIN_BATCH_VALUES, MIN_DICTIONARY_BYTES, MIN_FRAME_BYTES};
    Limits {
        frame_bytes: MIN_FRAME_BYTES,
        batch_rows: MIN_BATCH_ROWS,
        batch_values: MIN_BATCH_VALUES,
        dictionary_bytes: MIN_DICTIONARY_BYTES,
        ..Limits::default()
    }
}

#[test]
fn a_peers_limit_below_its_protocol_minimum_is_refused() {
    use super::{
        LIMIT_BELOW_MINIMUM, MIN_BATCH_ROWS, MIN_BATCH_VALUES, MIN_DICTIONARY_BYTES,
        MIN_FRAME_BYTES, Shortfall,
    };
    type Field = fn(&mut Limits) -> &mut u64;
    let short: [(Field, Limit, u64); 4] = [
        (
            |limits| &mut limits.frame_bytes,
            Limit::FrameBytes,
            MIN_FRAME_BYTES,
        ),
        (
            |limits| &mut limits.batch_rows,
            Limit::BatchRows,
            MIN_BATCH_ROWS,
        ),
        (
            |limits| &mut limits.batch_values,
            Limit::BatchValues,
            MIN_BATCH_VALUES,
        ),
        (
            |limits| &mut limits.dictionary_bytes,
            Limit::DictionaryBytes,
            MIN_DICTIONARY_BYTES,
        ),
    ];
    for (field, name, minimum) in short {
        let mut limits = least();
        *field(&mut limits) = minimum - 1;
        let shortfall = Shortfall {
            code: LIMIT_BELOW_MINIMUM,
            field: name,
            minimum,
            actual: minimum - 1,
        };
        assert_eq!(limits.admit_peer(), Err(shortfall));
    }
}

#[test]
fn a_peers_limits_are_admitted_from_the_protocols_minimums_up() {
    use super::{MIN_BATCH_ROWS, MIN_BATCH_VALUES, MIN_DICTIONARY_BYTES, MIN_FRAME_BYTES};
    assert_eq!(
        (
            MIN_FRAME_BYTES,
            MIN_BATCH_ROWS,
            MIN_BATCH_VALUES,
            MIN_DICTIONARY_BYTES
        ),
        (4_194_304, 1_024, 1_048_576, 262_144)
    );
    assert_eq!(Limits::default().admit_peer(), Ok(()));
    let least = least();
    assert_eq!(least.admit_peer(), Ok(()));
    // Limits a sender does not cut batches to have no minimum: one too low only refuses.
    let others = Limits {
        schema_columns: 1,
        nesting_depth: 1,
        json_push_bytes: 1,
        cursor_bytes: 1,
        config_bytes: 1,
        control_string_bytes: 1,
        schema_bytes: 1,
        catalog_bytes: 1,
        state_bytes: 1,
        control_message_bytes: 1,
        ..least
    };
    assert_eq!(others.admit_peer(), Ok(()));
}

#[test]
fn the_lesser_of_two_ends_limits_is_the_lesser_of_each() {
    let low = Limits {
        frame_bytes: 1,
        batch_rows: 2,
        schema_columns: 3,
        nesting_depth: 4,
        json_push_bytes: 5,
        cursor_bytes: 6,
        config_bytes: 7,
        control_string_bytes: 8,
        batch_values: 9,
        schema_bytes: 10,
        dictionary_bytes: 11,
        catalog_bytes: 21,
        state_bytes: 22,
        control_message_bytes: 23,
    };
    let high = Limits {
        frame_bytes: 11,
        batch_rows: 12,
        schema_columns: 13,
        nesting_depth: 14,
        json_push_bytes: 15,
        cursor_bytes: 16,
        config_bytes: 17,
        control_string_bytes: 18,
        batch_values: 19,
        schema_bytes: 20,
        dictionary_bytes: 21,
        catalog_bytes: 31,
        state_bytes: 32,
        control_message_bytes: 33,
    };
    assert_eq!(low.lesser(&high), low);
    assert_eq!(high.lesser(&low), low);
    // Each limit on its own.
    let mixed = Limits {
        batch_rows: 12,
        json_push_bytes: 15,
        batch_values: 19,
        ..low
    };
    assert_eq!(mixed.lesser(&high), mixed);
    assert_eq!(high.lesser(&mixed), mixed);
}

#[test]
fn dictionaries_and_staged_frames_are_bounded_in_frames() {
    let limits = Limits {
        frame_bytes: 10,
        ..Limits::default()
    };
    // A frame's worth of dictionaries is its bytes and its message's room.
    assert_eq!(
        (limits.held_dictionary_bytes(), limits.staged_bytes()),
        (10 + MESSAGE_ROOM, 40)
    );
    assert_eq!(limits.admit_dictionaries(10 + MESSAGE_ROOM), Ok(()));
    assert_eq!(
        limits.admit_dictionaries(11 + MESSAGE_ROOM),
        Err(Refusal {
            field: Limit::DictionaryBytes,
            limit: 10 + MESSAGE_ROOM,
            actual: 11 + MESSAGE_ROOM,
        })
    );
    assert_eq!(limits.admit_staged(40), Ok(()));
    let refusal = limits.admit_staged(41).unwrap_err();
    assert_eq!((refusal.field, refusal.limit), (Limit::StagedBytes, 40));
    // A limit of their own below a frame's bytes bounds dictionaries, and staged frames not.
    let fewer = Limits {
        dictionary_bytes: 4,
        ..limits
    };
    assert_eq!(
        (fewer.held_dictionary_bytes(), fewer.staged_bytes()),
        (4, 40)
    );
    let refusal = fewer.admit_dictionaries(5).unwrap_err();
    assert_eq!((refusal.field, refusal.limit), (Limit::DictionaryBytes, 4));
    // A receiver that lifts both limits lifts these with them.
    let unlimited = Limits {
        frame_bytes: u64::MAX,
        dictionary_bytes: u64::MAX,
        ..Limits::default()
    };
    assert_eq!(unlimited.admit_staged(u64::MAX), Ok(()));
    assert_eq!(unlimited.admit_dictionaries(u64::MAX), Ok(()));
    assert_eq!(
        Limits::default().held_dictionary_bytes(),
        (64 << 20) + MESSAGE_ROOM
    );
    assert_eq!(Limits::default().staged_bytes(), 256 << 20);
}

#[test]
fn each_class_of_message_is_decoded_within_its_own_limit() {
    use super::{Class, HANDSHAKE_BYTES};
    let limits = Limits {
        frame_bytes: 1,
        cursor_bytes: 2,
        config_bytes: 3,
        schema_bytes: 4,
        catalog_bytes: 5,
        state_bytes: 6,
        control_message_bytes: 7,
        ..Limits::default()
    };
    let overhead = 65_536;
    let excluded = 262_144;
    let expected = [
        (Class::Handshake, usize::try_from(HANDSHAKE_BYTES).unwrap()),
        (Class::Control, 7),
        (Class::Catalog, 5),
        (Class::State, 6),
        (Class::Config, 3 + overhead),
        (Class::Schema, 4 + overhead),
        (Class::Cursor, 2 + overhead + excluded),
        (Class::Data, 1 + overhead),
    ];
    for (class, bytes) in expected {
        assert_eq!(limits.decoding(class), bytes, "{class:?}");
    }
    assert_eq!(HANDSHAKE_BYTES, 4_194_304);
}

#[test]
fn each_class_of_message_may_hold_so_many_times_its_bytes_once_decoded() {
    use super::{Class, DECODED_PER_BYTE};
    let limits = Limits::default();
    let times = |class| limits.decoded(class) / limits.decoding(class);
    assert_eq!(DECODED_PER_BYTE, 16);
    let expected = [
        (Class::Handshake, 4),
        (Class::Config, 4),
        (Class::Cursor, 4),
        (Class::State, 1),
        (Class::Control, 16),
        (Class::Catalog, 16),
        (Class::Schema, 16),
        (Class::Data, 2),
    ];
    for (class, expected) in expected {
        assert_eq!(times(class), expected, "{class:?}");
    }
    // State is bounded on what it holds decoded: its bound is its limit.
    assert_eq!(limits.decoded(Class::State), 16 << 20);
    assert_eq!(limits.decoded(Class::Catalog), 64 << 20);
}

#[test]
fn the_largest_message_is_the_largest_of_any_class() {
    use super::Class;
    let limits = Limits::default();
    assert_eq!(limits.largest(), limits.decoding(Class::Data));
    let state = Limits {
        state_bytes: 80 << 20,
        ..Limits::default()
    };
    assert_eq!(state.largest(), 80 << 20);
    let handshake = Limits {
        frame_bytes: 1,
        ..Limits::default()
    };
    assert_eq!(handshake.largest(), handshake.decoding(Class::State));
}

#[test]
fn each_call_s_answer_has_the_class_of_what_it_carries() {
    use super::Class;
    let expected = [
        ("Handshake", Class::Handshake),
        ("Configure", Class::Handshake),
        ("Discover", Class::Catalog),
        ("Plan", Class::State),
        ("Open", Class::State),
        ("Read", Class::Data),
        ("ReadPublished", Class::Data),
        ("Commit", Class::Control),
        ("Write", Class::Control),
        ("Check", Class::Control),
    ];
    for (method, expected) in expected {
        assert_eq!(Class::of_answer(method), expected, "{method}");
    }
}

#[test]
fn each_call_s_request_is_bounded_as_what_it_carries() {
    use super::Class;
    use crate::bounded::Bounds;
    let expected = [
        ("Handshake", Class::Handshake),
        ("Configure", Class::Config),
        ("Check", Class::Control),
        ("Discover", Class::Control),
        ("Plan", Class::State),
        ("Read", Class::Cursor),
        ("Committed", Class::State),
        ("Open", Class::Control),
        ("ApplySchema", Class::Schema),
        ("Write", Class::Data),
        ("Commit", Class::State),
        ("Close", Class::Control),
        ("Heartbeat", Class::Control),
        ("ReadPublished", Class::Control),
        ("ReadAcknowledged", Class::Control),
    ];
    for (method, class) in expected {
        assert_eq!(Class::of_request(method), class, "{method}");
    }
    // A schema change is bounded by the schema limit, beyond any other control message's.
    let limits = Limits::default();
    let request = crate::scan::request("ApplySchema");
    let bounds = Bounds::of(&limits, Class::of_request("ApplySchema"), request);
    let schema = usize::try_from(limits.schema_bytes).unwrap() + 65_536;
    assert_eq!((bounds.wire, bounds.decoded), (schema, schema * 16));
    assert!(bounds.wire > limits.decoding(Class::Control));
}

#[test]
fn a_count_is_the_same_number_as_a_u64() {
    for value in [0, 1, 7, 1 << 40, usize::MAX] {
        assert_eq!(u128::from(count(value)), u128::try_from(value).unwrap());
    }
}

#[test]
fn the_protocol_limits_have_their_specified_values() {
    use super::{
        BATCH_ROWS, BATCH_VALUES, CONFIG_BYTES, CURSOR_BYTES, JSON_PUSH_BYTES, NESTING_DEPTH,
        SCHEMA_COLUMNS,
    };

    let mib = 1 << 20;
    assert_eq!(FRAME_BYTES, 64 * mib);
    assert_eq!(BATCH_ROWS, mib);
    assert_eq!(BATCH_VALUES, 64 * mib);
    assert_eq!(SCHEMA_COLUMNS, 10_000);
    assert_eq!(NESTING_DEPTH, 64);
    assert_eq!(JSON_PUSH_BYTES, 64 * mib);
    assert_eq!(CURSOR_BYTES, 4 * mib);
    assert_eq!(CONFIG_BYTES, 8 * mib);
}

#[test]
fn every_limit_has_one_name_and_is_found_by_it() {
    let mut names = std::collections::BTreeSet::new();
    for limit in Limit::ALL {
        assert!(names.insert(limit.name()), "{limit:?} shares its name");
        assert_eq!(Limit::named(limit.name()), Some(limit));
        assert_eq!(limit.to_string(), limit.name());
    }
    // One limit, one name: a schema's width is "schema columns" wherever it is refused.
    assert_eq!(Limit::named("batch columns"), None);
    assert_eq!(Limit::named(""), None);
    assert_eq!(Limit::named("frame bytes "), None);
}

#[test]
fn excluded_columns_are_measured_as_a_read_s_start_carries_them() {
    use super::excluded_bytes;
    use prost::Message as _;
    for names in [
        vec![],
        vec!["a".to_owned()],
        vec!["secret".to_owned(), "x".repeat(200), "é".repeat(70_000)],
    ] {
        let start = v1::ReadStart {
            exclude: names.clone(),
            ..v1::ReadStart::default()
        };
        let measured = excluded_bytes(names.iter().map(String::as_str));
        assert_eq!(
            measured,
            count(start.encoded_len()),
            "{} names",
            names.len()
        );
    }
}

#[test]
fn a_read_s_start_at_its_cursor_and_exclusion_limits_fits_its_class() {
    use super::{CURSOR_BYTES, Class, EXCLUDED_BYTES, excluded_bytes};
    use prost::Message as _;
    let limits = Limits::default();
    let longest = |bytes: usize| "n".repeat(bytes);
    // One-byte names, three bytes each with their key and length, are the most entries the
    // exclusion's bytes hold, and decode to the most.
    let exclude = vec!["x".to_owned(); usize::try_from(EXCLUDED_BYTES / 3).unwrap()];
    assert!(excluded_bytes(exclude.iter().map(String::as_str)) <= EXCLUDED_BYTES);
    let start = v1::ReadStart {
        stream: Some(v1::StreamName {
            namespace: Some(longest(256)),
            name: longest(256),
        }),
        partition: longest(256),
        cursor: Some(v1::Cursor {
            version: u32::MAX,
            bytes: vec![7; usize::try_from(CURSOR_BYTES).unwrap()].into(),
        }),
        barrier: u64::MAX,
        unbounded: true,
        follow: true,
        exclude,
    };
    let control = v1::ReadControl {
        control: Some(v1::read_control::Control::Start(start)),
    };
    let encoded = control.encode_to_vec();
    assert!(
        encoded.len() <= limits.decoding(Class::Cursor),
        "{} bytes",
        encoded.len()
    );
    let form = crate::scan::request("Read").unwrap();
    let decoded = crate::scan::decoded(form, &encoded, usize::MAX).unwrap();
    assert!(
        decoded <= limits.decoded(Class::Cursor),
        "{decoded} bytes decoded"
    );
}
