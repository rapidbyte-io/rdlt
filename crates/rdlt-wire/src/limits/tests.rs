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
        }
    );
}

#[test]
fn a_limit_the_peer_leaves_unset_is_the_protocols_default() {
    let unset = v1::Limits::default();
    assert_eq!(Limits::from(unset), Limits::default());
    let partly = v1::Limits {
        batch_rows: 7,
        ..v1::Limits::default()
    };
    assert_eq!(
        Limits::from(partly),
        Limits {
            batch_rows: 7,
            ..Limits::default()
        }
    );
}

#[test]
fn the_credit_window_and_a_messages_overhead_are_the_protocols() {
    assert_eq!(super::CREDIT_WINDOW, 4_194_304);
    let limits = Limits {
        frame_bytes: 10,
        ..Limits::default()
    };
    assert_eq!(limits.message_bytes(), 10 + 65_536);
}
