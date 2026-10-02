use rdlt_wire::Limits;
use rdlt_wire::limits::MIN_FRAME_BYTES;

use super::{admitted, kept, least};
use crate::budget::Shares;

const MIB: u64 = 1 << 20;

#[test]
fn the_default_budget_admits_these_limits() {
    let limits = admitted(Shares::of(256 * MIB), 16);
    assert_eq!(
        limits,
        Limits {
            // What the wire carries, less than pushes may take beside what a read keeps.
            frame_bytes: 64 * MIB,
            // A third of the 108 MiB pushes may take.
            json_push_bytes: 36 * MIB,
            cursor_bytes: 4 * MIB,
            // A read keeps 4 MiB: half for its dictionaries, half for its schema, whose
            // message is a fifth of that.
            dictionary_bytes: 2 * MIB,
            schema_bytes: 2 * MIB / 5,
            ..Limits::default()
        }
    );
}

#[test]
fn each_limit_follows_its_share_and_never_passes_the_protocol_s_default() {
    for (capacity, readers) in [
        (16 * MIB, 16),
        (16 * MIB, 1),
        (64 * MIB, 4),
        (256 * MIB, 16),
        (256 * MIB, 1),
        (4_096 * MIB, 2),
        (u64::MAX, 1),
    ] {
        let shares = Shares::of(capacity);
        let limits = admitted(shares, readers);
        let kept = kept(shares, readers);
        let defaults = Limits::default();
        let what = format!("{capacity} bytes read by {readers}");
        assert_eq!(limits.lesser(&defaults), limits, "{what}");
        // A frame's batch, the dictionaries its keys name and its schema fit what pushes take.
        assert!(
            limits.frame_bytes.saturating_add(kept) <= shares.intake.max(kept),
            "{what}"
        );
        assert!(
            limits.json_push_bytes.saturating_mul(3) <= shares.intake,
            "{what}"
        );
        // A cursor fits its share, and two of them, each recorded twice over, the log's.
        assert!(limits.cursor_bytes <= shares.cursors, "{what}");
        assert!(
            limits.cursor_bytes.saturating_mul(4) <= shares.log,
            "{what}"
        );
        // A schema's message, the schema decoded from it and the dictionaries fit what a read
        // keeps.
        let schema = limits.schema_bytes.saturating_mul(5);
        assert!(
            schema.saturating_add(limits.dictionary_bytes) <= kept,
            "{what}"
        );
        // What the budget does not bound is the protocol's.
        assert_eq!(
            (
                limits.batch_rows,
                limits.batch_values,
                limits.schema_columns
            ),
            (
                defaults.batch_rows,
                defaults.batch_values,
                defaults.schema_columns
            ),
            "{what}"
        );
    }
}

#[test]
fn small_shares_lower_each_limit_below_the_protocol_s_default() {
    let limits = admitted(Shares::of(16 * MIB), 16);
    assert_eq!(
        (
            limits.frame_bytes,
            limits.json_push_bytes,
            limits.cursor_bytes,
            limits.dictionary_bytes,
            limits.schema_bytes
        ),
        (
            6 * MIB + 768 * 1024 - 256 * 1024,
            (6 * MIB + 768 * 1024) / 3,
            256 * 1024,
            128 * 1024,
            128 * 1024 / 5
        )
    );
}

#[test]
fn the_least_budget_admits_the_protocol_s_least_frame_and_a_byte_less_does_not() {
    for readers in [1, 2, 4, 16, 64, 1_000] {
        let least = least(readers);
        let admits = |capacity: u64| admitted(Shares::of(capacity), readers);
        assert!(
            admits(least).frame_bytes >= MIN_FRAME_BYTES,
            "{readers} readers"
        );
        assert_eq!(admits(least).admit_peer(), Ok(()), "{readers} readers");
        let below = admits(least - 1).admit_peer().unwrap_err();
        assert_eq!(below.field, "frame bytes", "{readers} readers");
    }
    // Fewer reads each keep more, which pushes then leave room for.
    assert_eq!(least(16), 10_324_437);
    assert_eq!(least(1), 24_403_217);
    assert!(least(1) > least(2) && least(2) > least(16) && least(16) > least(1_000));
}
