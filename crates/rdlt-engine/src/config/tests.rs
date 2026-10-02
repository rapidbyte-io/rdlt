use std::num::NonZeroU32;
use std::time::Duration;

use super::{BatchPolicy, CommitPolicy, EngineConfig, RetryPolicy};
use crate::error::ErrorKind;

#[test]
fn a_commit_policy_needs_a_nonzero_threshold() {
    for (every, rows, bytes) in [
        (None, None, None),
        (Some(Duration::ZERO), None, None),
        (None, Some(0), None),
        (None, None, Some(0)),
    ] {
        let error = CommitPolicy::new(every, rows, bytes).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Config);
        assert_eq!(error.code(), Some("commit_policy_invalid"));
    }
    let policy = CommitPolicy::new(Some(Duration::from_secs(3)), Some(10), Some(20)).unwrap();
    assert_eq!(policy.every(), Some(Duration::from_secs(3)));
    assert_eq!(policy.rows().map(std::num::NonZero::get), Some(10));
    assert_eq!(policy.bytes().map(std::num::NonZero::get), Some(20));
}

#[test]
fn a_commit_is_due_once_any_threshold_is_reached() {
    let policy = CommitPolicy::new(None, Some(10), Some(100)).unwrap();
    assert!(!policy.is_due(9, 99));
    assert!(policy.is_due(10, 0));
    assert!(policy.is_due(0, 100));
    let interval = CommitPolicy::new(Some(Duration::from_secs(1)), None, None).unwrap();
    assert!(!interval.is_due(u64::MAX, u64::MAX));
}

#[test]
fn the_default_commit_policy_is_a_minute_or_a_gibibyte() {
    let policy = CommitPolicy::default();
    assert_eq!(policy.every(), Some(Duration::from_secs(60)));
    assert_eq!(policy.rows(), None);
    assert_eq!(policy.bytes().map(std::num::NonZero::get), Some(1 << 30));
}

fn failures(count: u32) -> NonZeroU32 {
    NonZeroU32::new(count).unwrap()
}

#[test]
fn retry_delays_grow_exponentially_up_to_the_cap_with_full_jitter() {
    const SECOND: u64 = 1_000_000_000;
    let policy = RetryPolicy::default()
        .initial(Duration::from_secs(1))
        .max_delay(Duration::from_secs(10));
    let delay = |count, random| policy.delay(failures(count), random);
    // A draw of exactly the ceiling's nanoseconds lands on the ceiling; one more wraps to zero.
    assert_eq!(delay(1, SECOND), Duration::from_secs(1));
    assert_eq!(delay(1, SECOND + 1), Duration::ZERO);
    assert_eq!(delay(2, 2 * SECOND), Duration::from_secs(2));
    assert_eq!(delay(3, 4 * SECOND), Duration::from_secs(4));
    assert_eq!(delay(5, 10 * SECOND), Duration::from_secs(10));
    assert_eq!(delay(5, 16 * SECOND), Duration::from_nanos(6 * SECOND - 1));
    assert_eq!(delay(u32::MAX, 10 * SECOND), Duration::from_secs(10));
    assert_eq!(delay(2, 3 * SECOND), Duration::from_nanos(SECOND - 1));
}

#[test]
fn the_default_retry_policy_makes_five_attempts_and_resets_after_progress() {
    let policy = RetryPolicy::default();
    assert_eq!(policy.attempts().get(), 5);
    assert!(policy.resets_after_progress());
    assert!(!policy.reset_after_progress(false).resets_after_progress());
    assert_eq!(policy.max_attempts(0).attempts().get(), 1);
    assert_eq!(policy.max_attempts(7).attempts().get(), 7);
}

#[test]
fn the_builder_applies_defaults_and_settings() {
    let defaults = EngineConfig::builder().build().unwrap();
    assert_eq!(defaults, EngineConfig::default());
    assert_eq!(defaults.memory().get(), 256 << 20);
    assert_eq!(defaults.lanes(), None);
    assert_eq!(defaults.lane_window().get(), 4);
    assert_eq!(defaults.partitions().get(), 16);
    assert_eq!(defaults.partition_buffer().get(), 16);
    assert_eq!(defaults.barrier_wait(), Duration::from_secs(5));
    let policy = CommitPolicy::new(None, Some(5), None).unwrap();
    let retry = RetryPolicy::default().max_attempts(2);
    let batch = BatchPolicy::new(10, 20, Duration::from_millis(30), 40).unwrap();
    let config = EngineConfig::builder()
        .memory(64 << 20)
        .lanes(3)
        .lane_window(2)
        .partitions(5)
        .partition_buffer(6)
        .barrier_wait(Duration::from_millis(7))
        .batch(batch)
        .commit(policy)
        .retry(retry)
        .build()
        .unwrap();
    assert_eq!(config.memory().get(), 64 << 20);
    assert_eq!(config.lanes().map(std::num::NonZero::get), Some(3));
    assert_eq!(config.lane_window().get(), 2);
    assert_eq!(config.partitions().get(), 5);
    assert_eq!(config.partition_buffer().get(), 6);
    assert_eq!(config.barrier_wait(), Duration::from_millis(7));
    assert_eq!(config.batch(), &batch);
    assert_eq!(config.commit(), Some(&policy));
    assert_eq!(config.retry(), &retry);
}

#[test]
fn memory_below_what_the_protocol_s_least_frame_needs_is_refused_naming_the_least() {
    for partitions in [1, 4, 16] {
        let least = EngineConfig::least_memory(partitions);
        let build = |memory| {
            EngineConfig::builder()
                .memory(memory)
                .partitions(partitions)
                .build()
        };
        let config = build(least).unwrap();
        assert!(config.limits().admit_peer().is_ok());
        let refused = build(least - 1).unwrap_err();
        assert_eq!(
            (refused.kind(), refused.code()),
            (ErrorKind::Config, Some("memory_below_minimum"))
        );
        let said = refused.to_string();
        assert!(said.contains(&format!("is {least} bytes")), "{said}");
        assert!(said.contains(&format!("{partitions} partitions")), "{said}");
    }
    // About thirty-two mebibytes at the default sixteen partitions, and the default is far
    // above it.
    assert_eq!(EngineConfig::least_memory(16), 33_811_576);
    assert!(EngineConfig::builder().build().is_ok());
}

#[test]
fn the_limits_are_the_lesser_of_those_configured_and_those_the_memory_admits() {
    use rdlt_wire::Limits;
    let defaults = EngineConfig::default().limits();
    assert_eq!(
        defaults,
        Limits {
            frame_bytes: (32 << 20) - 7_489 * 33 - 1_024,
            json_push_bytes: (100 << 20) / 3,
            cursor_bytes: (4 << 20) / 34,
            dictionary_bytes: 2 << 20,
            schema_bytes: (2 << 20) / 5,
            schema_columns: 7_489,
            ..Limits::default()
        }
    );
    // A limit configured below what the memory admits stays, and one above is lowered.
    let configured = Limits {
        json_push_bytes: 1 << 20,
        dictionary_bytes: 32 << 20,
        batch_rows: 2_000,
        ..Limits::default()
    };
    let config = EngineConfig::builder().limits(configured).build().unwrap();
    assert_eq!(
        config.limits(),
        Limits {
            json_push_bytes: 1 << 20,
            batch_rows: 2_000,
            ..defaults
        }
    );
    // Fewer partitions let each read keep more.
    let two = EngineConfig::builder().partitions(2).build().unwrap();
    assert_eq!(two.limits().dictionary_bytes, 16 << 20);
}

#[test]
fn a_retry_policy_whose_first_delay_is_its_longest_is_valid() {
    let equal = RetryPolicy::default()
        .initial(Duration::from_secs(5))
        .max_delay(Duration::from_secs(5));
    assert!(EngineConfig::builder().retry(equal).build().is_ok());
}

#[test]
fn a_batch_policy_needs_every_threshold_above_zero() {
    let second = Duration::from_secs(1);
    for (bytes, rows, latency, chunk, name) in [
        (0, 1, second, 1, "target_bytes"),
        (1, 0, second, 1, "max_rows"),
        (1, 1, Duration::ZERO, 1, "max_latency"),
        (1, 1, second, 0, "chunk_bytes"),
    ] {
        let error = BatchPolicy::new(bytes, rows, latency, chunk).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Config);
        assert_eq!(error.code(), Some("batch_policy_invalid"));
        assert!(error.to_string().contains(name), "{error}");
    }
    let policy = BatchPolicy::new(1, 2, second, 3).unwrap();
    assert_eq!(
        (
            policy.target_bytes().get(),
            policy.max_rows().get(),
            policy.max_latency(),
            policy.chunk_bytes().get()
        ),
        (1, 2, second, 3)
    );
}

#[test]
fn the_default_batch_policy_is_eight_mebibytes_a_mebirow_or_a_second() {
    let policy = BatchPolicy::default();
    assert_eq!(policy.target_bytes().get(), 8 << 20);
    assert_eq!(policy.max_rows().get(), 1 << 20);
    assert_eq!(policy.max_latency(), Duration::from_secs(1));
    assert_eq!(policy.chunk_bytes().get(), 1 << 20);
    assert_eq!(EngineConfig::default().batch(), &policy);
}

#[test]
fn an_unset_commit_policy_commits_every_ten_seconds_where_the_run_streams() {
    let config = EngineConfig::default();
    assert_eq!(config.commit(), None);
    assert_eq!(config.commit_for(false), CommitPolicy::default());
    let streaming = config.commit_for(true);
    assert_eq!(streaming.every(), Some(Duration::from_secs(10)));
    assert_eq!(streaming.bytes(), CommitPolicy::default().bytes());
    let set = CommitPolicy::new(Some(Duration::from_secs(3)), None, None).unwrap();
    let config = EngineConfig::builder().commit(set).build().unwrap();
    assert_eq!(config.commit_for(true), set);
    assert_eq!(config.commit_for(false), set);
}

#[test]
fn a_following_run_plans_again_every_minute_unless_told_otherwise_and_never_every_instant() {
    assert_eq!(EngineConfig::default().replan(), Duration::from_secs(60));
    let every = Duration::from_millis(250);
    let config = EngineConfig::builder().replan(every).build().unwrap();
    assert_eq!(config.replan(), every);
    let refused = EngineConfig::builder().replan(Duration::ZERO).build();
    assert_eq!(
        refused.unwrap_err().code(),
        Some("config_invalid"),
        "a zero interval would plan without end"
    );
}
