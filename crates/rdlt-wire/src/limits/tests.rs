use super::{CONTROL_STRING_BYTES, FRAME_BYTES, LIMIT_EXCEEDED, Limits, Refusal};
use crate::v1;

#[test]
fn a_value_at_its_limit_is_admitted_and_one_beyond_refused_with_what_it_measured() {
    let limits = Limits::default();
    let at = usize::try_from(FRAME_BYTES).unwrap();
    assert_eq!(limits.admit_frame(at), Ok(()));
    assert_eq!(
        limits.admit_frame(at + 1),
        Err(Refusal {
            code: LIMIT_EXCEEDED,
            field: "frame bytes",
            limit: FRAME_BYTES,
            actual: FRAME_BYTES + 1,
        })
    );
    let long = "x".repeat(usize::try_from(CONTROL_STRING_BYTES).unwrap() + 1);
    let refusal = limits.admit_string(&long).unwrap_err();
    assert_eq!(
        (refusal.field, refusal.actual),
        ("control string bytes", CONTROL_STRING_BYTES + 1)
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
    assert_eq!(refused(limits.admit_json(6)), Err(("json push bytes", 5)));
    assert_eq!(refused(limits.admit_cursor(7)), Err(("cursor bytes", 6)));
    assert_eq!(refused(limits.admit_config(8)), Err(("config bytes", 7)));
    assert_eq!(
        refused(limits.admit_string("123456789")),
        Err(("control string bytes", 8))
    );
    assert_eq!(refused(limits.admit_schema(11)), Err(("schema bytes", 10)));
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
            dictionary_bytes: 67_108_864,
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
    assert_eq!((refused.field, refused.actual), ("dictionary bytes", 0));
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
    let short: [(Field, &str, u64); 4] = [
        (
            |limits| &mut limits.frame_bytes,
            "frame bytes",
            MIN_FRAME_BYTES,
        ),
        (
            |limits| &mut limits.batch_rows,
            "batch rows",
            MIN_BATCH_ROWS,
        ),
        (
            |limits| &mut limits.batch_values,
            "batch values",
            MIN_BATCH_VALUES,
        ),
        (
            |limits| &mut limits.dictionary_bytes,
            "dictionary bytes",
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
    assert_eq!(
        (limits.held_dictionary_bytes(), limits.staged_bytes()),
        (10, 40)
    );
    assert_eq!(limits.admit_dictionaries(10), Ok(()));
    assert_eq!(
        limits.admit_dictionaries(11),
        Err(Refusal {
            code: LIMIT_EXCEEDED,
            field: "dictionary bytes",
            limit: 10,
            actual: 11,
        })
    );
    assert_eq!(limits.admit_staged(40), Ok(()));
    let refusal = limits.admit_staged(41).unwrap_err();
    assert_eq!((refusal.field, refusal.limit), ("staged bytes", 40));
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
    assert_eq!((refusal.field, refusal.limit), ("dictionary bytes", 4));
    // A receiver that lifts both limits lifts these with them.
    let unlimited = Limits {
        frame_bytes: u64::MAX,
        dictionary_bytes: u64::MAX,
        ..Limits::default()
    };
    assert_eq!(unlimited.admit_staged(u64::MAX), Ok(()));
    assert_eq!(unlimited.admit_dictionaries(u64::MAX), Ok(()));
    assert_eq!(Limits::default().held_dictionary_bytes(), 64 << 20);
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
    let expected = [
        (Class::Handshake, usize::try_from(HANDSHAKE_BYTES).unwrap()),
        (Class::Control, 7),
        (Class::Catalog, 5),
        (Class::State, 6),
        (Class::Config, 3 + overhead),
        (Class::Schema, 4 + overhead),
        (Class::Cursor, 2 + overhead),
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
