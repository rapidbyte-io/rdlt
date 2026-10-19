//! Proofs that the limits a handshake admits meet every minimum and every rule between them,
//! for every limit set within each proof's bound.

use super::{
    Class, LIMIT_BELOW_MINIMUM, Limit, Limits, MESSAGE_ROOM, MIN_BATCH_ROWS, MIN_BATCH_VALUES,
    MIN_DICTIONARY_BYTES, MIN_FRAME_BYTES, Refusal, count,
};
use crate::v1;

/// Any limits a peer may send.
fn any_sent() -> v1::Limits {
    v1::Limits {
        frame_bytes: kani::any(),
        batch_rows: kani::any(),
        schema_columns: kani::any(),
        nesting_depth: kani::any(),
        json_push_bytes: kani::any(),
        cursor_bytes: kani::any(),
        config_bytes: kani::any(),
        control_string_bytes: kani::any(),
        batch_values: kani::any(),
        schema_bytes: kani::any(),
        dictionary_bytes: kani::any(),
        catalog_bytes: kani::any(),
        state_bytes: kani::any(),
        control_message_bytes: kani::any(),
    }
}

/// Any limits an end may hold, zeros included.
fn any_limits() -> Limits {
    Limits {
        frame_bytes: kani::any(),
        batch_rows: kani::any(),
        schema_columns: kani::any(),
        nesting_depth: kani::any(),
        json_push_bytes: kani::any(),
        cursor_bytes: kani::any(),
        config_bytes: kani::any(),
        control_string_bytes: kani::any(),
        batch_values: kani::any(),
        schema_bytes: kani::any(),
        dictionary_bytes: kani::any(),
        catalog_bytes: kani::any(),
        state_bytes: kani::any(),
        control_message_bytes: kani::any(),
    }
}

/// The limits a handshake reads from what a peer sent: as sent, or the defaults where it sent
/// none.
fn read(sent: Option<v1::Limits>) -> Limits {
    sent.map(Limits::from).unwrap_or_default()
}

/// A peer's limits are read as sent where set, as the protocol's defaults where left unset,
/// but the dictionary limit, taken as sent; and limits sent with every limit set cross back
/// unchanged.
///
/// Bound: none; every value of every limit.
#[kani::proof]
fn a_peer_s_limits_read_as_sent_or_as_the_protocol_s_defaults() {
    let sent = any_sent();
    let (read, defaults) = (Limits::from(sent), Limits::default());
    let or = |sent: u64, default: u64| if sent == 0 { default } else { sent };
    assert_eq!(read.frame_bytes, or(sent.frame_bytes, defaults.frame_bytes));
    assert_eq!(read.batch_rows, or(sent.batch_rows, defaults.batch_rows));
    assert_eq!(
        read.schema_columns,
        or(sent.schema_columns, defaults.schema_columns)
    );
    assert_eq!(
        read.nesting_depth,
        or(sent.nesting_depth, defaults.nesting_depth)
    );
    assert_eq!(
        read.json_push_bytes,
        or(sent.json_push_bytes, defaults.json_push_bytes)
    );
    assert_eq!(
        read.cursor_bytes,
        or(sent.cursor_bytes, defaults.cursor_bytes)
    );
    assert_eq!(
        read.config_bytes,
        or(sent.config_bytes, defaults.config_bytes)
    );
    assert_eq!(
        read.control_string_bytes,
        or(sent.control_string_bytes, defaults.control_string_bytes)
    );
    assert_eq!(
        read.batch_values,
        or(sent.batch_values, defaults.batch_values)
    );
    assert_eq!(
        read.schema_bytes,
        or(sent.schema_bytes, defaults.schema_bytes)
    );
    assert_eq!(read.dictionary_bytes, sent.dictionary_bytes);
    assert_eq!(
        read.catalog_bytes,
        or(sent.catalog_bytes, defaults.catalog_bytes)
    );
    assert_eq!(read.state_bytes, or(sent.state_bytes, defaults.state_bytes));
    assert_eq!(
        read.control_message_bytes,
        or(sent.control_message_bytes, defaults.control_message_bytes)
    );
    assert!(Limits::from(v1::Limits::from(read)) == read);
}

/// A limit set a handshake admits, from any limits a peer sends or none, meets every
/// protocol minimum, is refused naming the first it does not, and keeps every rule between its
/// limits: no limit is zero, dictionaries are held within a frame and its room but never below
/// their least, a write stages at least a frame, each class's messages are decoded within so
/// many times their wire bound, and the largest message is the largest of any class.
///
/// Bound: none; every value of every limit, sent or not.
#[kani::proof]
#[kani::unwind(20)]
fn every_limit_set_a_handshake_admits_meets_every_minimum_and_rule() {
    let sent = kani::any::<bool>().then(any_sent);
    let limits = read(sent);
    let floors = [
        (Limit::FrameBytes, MIN_FRAME_BYTES, limits.frame_bytes),
        (Limit::BatchRows, MIN_BATCH_ROWS, limits.batch_rows),
        (Limit::BatchValues, MIN_BATCH_VALUES, limits.batch_values),
        (
            Limit::DictionaryBytes,
            MIN_DICTIONARY_BYTES,
            limits.dictionary_bytes,
        ),
    ];
    match limits.admit_peer() {
        Ok(()) => {
            for (_, minimum, actual) in floors {
                assert!(actual >= minimum);
            }
            let fields = [
                limits.schema_columns,
                limits.nesting_depth,
                limits.json_push_bytes,
                limits.cursor_bytes,
                limits.config_bytes,
                limits.control_string_bytes,
                limits.schema_bytes,
                limits.catalog_bytes,
                limits.state_bytes,
                limits.control_message_bytes,
            ];
            for field in fields {
                assert!(field > 0);
            }
            let held = limits.held_dictionary_bytes();
            assert!(held >= MIN_DICTIONARY_BYTES);
            assert!(held <= limits.frame_bytes.saturating_add(MESSAGE_ROOM));
            assert!(held <= limits.dictionary_bytes);
            assert!(limits.staged_bytes() >= limits.frame_bytes);
            let frame = usize::try_from(limits.frame_bytes).unwrap_or(usize::MAX);
            assert!(limits.decoding(Class::Data) >= frame);
            let largest = limits.largest();
            let mut reached = false;
            for class in Class::ALL {
                assert!(class.decoded_per_byte() >= 1);
                assert_eq!(
                    limits.decoded(class),
                    limits
                        .decoding(class)
                        .saturating_mul(class.decoded_per_byte())
                );
                assert!(limits.decoded(class) >= limits.decoding(class));
                assert!(largest >= limits.decoding(class));
                reached |= largest == limits.decoding(class);
            }
            assert!(reached);
        }
        Err(refused) => {
            let (field, minimum, actual) = floors
                .into_iter()
                .find(|(_, minimum, actual)| actual < minimum)
                .expect("a refusal names a limit below its minimum");
            assert!(refused.code == LIMIT_BELOW_MINIMUM);
            assert!(refused.field == field);
            assert_eq!(refused.minimum, minimum);
            assert_eq!(refused.actual, actual);
        }
    }
    assert!(sent.is_some() || limits.admit_peer().is_ok());
}

/// What two ends agree, the lesser of each of their limits, meets every minimum both meet, is
/// within each end's limits, and is the same whichever end takes it.
///
/// Bound: none; every value of every limit at both ends.
#[kani::proof]
#[kani::unwind(6)]
fn what_two_ends_agree_meets_every_minimum_both_meet() {
    let (one, other) = (any_limits(), any_limits());
    let agreed = one.lesser(&other);
    assert!(agreed == other.lesser(&one));
    assert!(one.lesser(&one) == one);
    assert!(agreed.lesser(&one) == agreed);
    assert!(agreed.lesser(&other) == agreed);
    if one.admit_peer().is_ok() && other.admit_peer().is_ok() {
        assert!(agreed.admit_peer().is_ok());
    }
}

/// A value is admitted exactly when it is within its limit, refused naming the limit, its value
/// and the value otherwise, and each admission measures against its own limit.
///
/// Bound: none; every limit set, every value, every limit named.
#[kani::proof]
fn a_value_is_admitted_exactly_within_its_own_limit() {
    let at: usize = kani::any();
    kani::assume(at < Limit::ALL.len());
    let (field, limit, actual) = (Limit::ALL[at], kani::any::<u64>(), kani::any::<u64>());
    let admitted = Limits::admit(field, limit, actual);
    if actual <= limit {
        assert!(admitted == Ok(()));
    } else {
        assert!(
            admitted
                == Err(Refusal {
                    field,
                    limit,
                    actual
                })
        );
    }
    let limits = any_limits();
    let bytes: usize = kani::any();
    let measured = count(bytes);
    assert!(
        limits.admit_frame(bytes) == Limits::admit(Limit::FrameBytes, limits.frame_bytes, measured)
    );
    assert!(
        limits.admit_schema(bytes)
            == Limits::admit(Limit::SchemaBytes, limits.schema_bytes, measured)
    );
    assert!(
        limits.admit_json(bytes)
            == Limits::admit(Limit::JsonPushBytes, limits.json_push_bytes, measured)
    );
    assert!(
        limits.admit_cursor(bytes)
            == Limits::admit(Limit::CursorBytes, limits.cursor_bytes, measured)
    );
    assert!(
        limits.admit_config(bytes)
            == Limits::admit(Limit::ConfigBytes, limits.config_bytes, measured)
    );
    let total: u64 = kani::any();
    assert!(
        limits.admit_dictionaries(total)
            == Limits::admit(
                Limit::DictionaryBytes,
                limits.held_dictionary_bytes(),
                total
            )
    );
    assert!(
        limits.admit_staged(total)
            == Limits::admit(Limit::StagedBytes, limits.staged_bytes(), total)
    );
}
