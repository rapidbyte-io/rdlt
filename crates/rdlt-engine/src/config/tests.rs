use std::num::NonZeroU32;
use std::time::Duration;

use super::{CommitPolicy, EngineConfig, RetryPolicy};
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
    let config = EngineConfig::builder()
        .memory(1024)
        .lanes(3)
        .lane_window(2)
        .partitions(5)
        .partition_buffer(6)
        .barrier_wait(Duration::from_millis(7))
        .commit(policy)
        .retry(retry)
        .build()
        .unwrap();
    assert_eq!(config.memory().get(), 1024);
    assert_eq!(config.lanes().map(std::num::NonZero::get), Some(3));
    assert_eq!(config.lane_window().get(), 2);
    assert_eq!(config.partitions().get(), 5);
    assert_eq!(config.partition_buffer().get(), 6);
    assert_eq!(config.barrier_wait(), Duration::from_millis(7));
    assert_eq!(config.commit(), &policy);
    assert_eq!(config.retry(), &retry);
}

#[test]
fn a_retry_policy_whose_first_delay_is_its_longest_is_valid() {
    let equal = RetryPolicy::default()
        .initial(Duration::from_secs(5))
        .max_delay(Duration::from_secs(5));
    assert!(EngineConfig::builder().retry(equal).build().is_ok());
}
