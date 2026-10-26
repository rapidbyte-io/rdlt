use std::num::NonZeroUsize;
use std::time::Duration;

use proptest::prelude::*;

use super::Parts;

/// Bytes: a part, small, so a few draws fill one.
const PART: u64 = 100;

#[derive(Clone, Debug)]
enum Step {
    /// A read that is not followed takes a part where one is free.
    Take,
    /// One of them lets its part go.
    Release(usize),
    /// A followed read keeps this many bytes more, at most a part.
    Draw(u64),
    /// One of the draws is given back.
    GiveBack(usize),
}

fn steps() -> impl Strategy<Value = Vec<Step>> {
    let step = prop_oneof![
        Just(Step::Take),
        any::<usize>().prop_map(Step::Release),
        (1..=PART).prop_map(Step::Draw),
        any::<usize>().prop_map(Step::GiveBack),
    ];
    prop::collection::vec(step, 0..64)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(rdlt_testkit::cases(1_024)))]

    /// Followed reads hold as many parts as what they keep takes, never all of them, and are
    /// refused only where they need one more they may not take; a read that is not followed
    /// always comes to hold one; every part is back once all are let go.
    #[test]
    fn followed_reads_hold_the_parts_what_they_keep_takes_and_leave_one(
        count in 1..=8_usize,
        steps in steps(),
    ) {
        let parts = Parts::new(NonZeroUsize::new(count).unwrap(), PART);
        let (mut taken, mut drawn) = (Vec::new(), Vec::new());
        for step in steps {
            let held = count - parts.free.available_permits() - taken.len();
            match step {
                Step::Take => taken.extend(parts.try_take()),
                Step::Release(index) if !taken.is_empty() => {
                    drop(taken.swap_remove(index % taken.len()));
                }
                Step::Draw(bytes) => {
                    let kept = drawn.iter().sum::<u64>() + bytes;
                    let free = parts.free.available_permits();
                    match parts.draw(bytes) {
                        Ok(()) => drawn.push(bytes),
                        Err(full) => {
                            let needed = usize::try_from(kept.div_ceil(PART)).unwrap();
                            prop_assert!(needed > held, "refused a draw within its parts");
                            prop_assert!(needed > count - 1 || needed - held > free);
                            prop_assert_eq!(full.kept, kept);
                            prop_assert!(full.room < kept);
                        }
                    }
                }
                Step::GiveBack(index) if !drawn.is_empty() => {
                    parts.give_back(drawn.swap_remove(index % drawn.len()));
                }
                Step::Release(_) | Step::GiveBack(_) => {}
            }
            let kept = drawn.iter().sum::<u64>();
            let held = count - parts.free.available_permits() - taken.len();
            prop_assert_eq!(held, usize::try_from(kept.div_ceil(PART)).unwrap());
            prop_assert!(held < count, "followed reads hold every part");
        }
        taken.clear();
        prop_assert!(parts.try_take().is_some(), "no part for a read that is not followed");
        for bytes in drawn {
            parts.give_back(bytes);
        }
        prop_assert_eq!(parts.free.available_permits(), count);
    }
}

#[tokio::test(start_paused = true)]
#[expect(
    clippy::disallowed_methods,
    reason = "the test bounds its waits on tokio's paused clock directly"
)]
async fn a_read_that_is_not_followed_waits_for_a_part_until_one_frees() {
    let parts = Parts::new(NonZeroUsize::new(2).unwrap(), PART);
    let first = parts.try_take().unwrap();
    parts.draw(PART).unwrap();
    assert!(parts.try_take().is_none());
    let waiting = tokio::time::timeout(Duration::from_secs(3600), parts.take()).await;
    assert!(waiting.is_err(), "a part was taken where none was free");
    drop(first);
    let freed = tokio::time::timeout(Duration::from_secs(1), parts.take()).await;
    assert!(freed.is_ok(), "a freed part was not taken");
}
