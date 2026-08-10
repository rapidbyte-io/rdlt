//! The clause budget with the PROBE CLOCK STOPPED (round-5 fix).
//!
//! One 30s clause budget used to span whole suite phases INCLUDING up
//! to four `--probe-cmd` invocations that are each individually
//! budgeted 20s — arithmetic that could not hold: probe latency well
//! inside its own documented budget exhausted the suite budget and
//! failed every clause with the timeout spelling blaming the
//! CONNECTOR. The fix is structural: the probe the certifier passes is
//! wrapped to meter the wall time its counts spend (in-flight time
//! included), and the deadline extends by exactly that meter — the
//! clause budget then bounds SPI traffic alone, while each probe count
//! keeps its own [`PROBE_TIMEOUT`-style] bound and fails naming
//! ITSELF.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use rdlt_connector::core::TableName;
use rdlt_testkit::conformance::destination::{ProbeError, TableProbe};

/// The shared meter: completed probe time plus the start of any count
/// currently in flight.
#[derive(Default)]
struct ClockState {
    accumulated: Duration,
    in_flight: Option<tokio::time::Instant>,
}

/// The handle both sides share — the wrapped probe writes it, the
/// budget loop reads it.
#[derive(Clone, Default)]
pub(crate) struct ProbeClock(Arc<Mutex<ClockState>>);

impl ProbeClock {
    /// Everything the probe has spent so far, the in-flight count's
    /// elapsed included — what the deadline extends by.
    fn spent(&self) -> Duration {
        let state = self.0.lock().expect("probe clock lock");
        state.accumulated
            + state
                .in_flight
                .map(|start| start.elapsed())
                .unwrap_or_default()
    }
}

/// The certifier's own bound on ONE probe count — what keeps the
/// stop-clock from deleting the no-hang guarantee (round-6 fix): the
/// clause clock stops while a count runs, so without this bound a
/// never-returning probe would hang certification forever. Every probe
/// the certifier drives — library-supplied and first-party alike — is
/// bounded HERE, and a stalled count fails naming ITSELF, never the
/// connector.
const PROBE_BOUND: Duration = Duration::from_secs(30);

/// The probe wrapper that stops the clause clock while a count runs —
/// and bounds each count at [`PROBE_BOUND`], so the credited allowance
/// is itself finite.
pub(crate) struct StopClockProbe<'a> {
    inner: &'a dyn TableProbe,
    clock: ProbeClock,
    bound: Duration,
}

impl<'a> StopClockProbe<'a> {
    /// Wrap `inner`; the returned clock feeds [`timeout_excluding_probe`].
    pub(crate) fn new(inner: &'a dyn TableProbe) -> (Self, ProbeClock) {
        Self::with_bound(inner, PROBE_BOUND)
    }

    /// [`Self::new`] with an explicit per-count bound — the test seam.
    pub(crate) fn with_bound(inner: &'a dyn TableProbe, bound: Duration) -> (Self, ProbeClock) {
        let clock = ProbeClock::default();
        (
            Self {
                inner,
                clock: clock.clone(),
                bound,
            },
            clock,
        )
    }
}

#[async_trait]
impl TableProbe for StopClockProbe<'_> {
    async fn count(&self, table: &TableName) -> Result<u64, ProbeError> {
        self.clock.0.lock().expect("probe clock lock").in_flight =
            Some(tokio::time::Instant::now());
        let outcome = match tokio::time::timeout(self.bound, self.inner.count(table)).await {
            Ok(outcome) => outcome,
            Err(_elapsed) => Err(ProbeError {
                message: format!(
                    "the table probe did not answer within {}s — a stalling probe fails \
                     the clause it serves, never hangs the certifier",
                    self.bound.as_secs()
                ),
            }),
        };
        let mut state = self.clock.0.lock().expect("probe clock lock");
        if let Some(start) = state.in_flight.take() {
            state.accumulated += start.elapsed();
        }
        outcome
    }
}

/// `tokio::time::timeout` with the deadline extended by everything
/// `clock` meters: `budget` bounds only the time OUTSIDE probe counts.
/// The loop recomputes on every inner expiry because the allowance
/// grows while a count is in flight — an expiry that lands mid-count
/// is never final, and the recomputation is bounded by the number of
/// counts the future makes.
pub(crate) async fn timeout_excluding_probe<F>(
    budget: Duration,
    clock: &ProbeClock,
    future: F,
) -> Result<F::Output, tokio::time::error::Elapsed>
where
    F: std::future::Future,
{
    tokio::pin!(future);
    let started = tokio::time::Instant::now();
    loop {
        let allowed = budget + clock.spent();
        let Some(remaining) = allowed.checked_sub(started.elapsed()) else {
            // Force an Elapsed value the way the stdlib of this crate
            // does everywhere: a zero-duration timeout on a pending
            // future.
            return Err(
                tokio::time::timeout(Duration::ZERO, std::future::pending::<()>())
                    .await
                    .expect_err("a zero timeout on a pending future elapses"),
            );
        };
        match tokio::time::timeout(remaining, &mut future).await {
            Ok(output) => return Ok(output),
            Err(elapsed) => {
                if started.elapsed() >= budget + clock.spent() {
                    return Err(elapsed);
                }
                // The allowance grew while a count was in flight —
                // keep waiting on the same future.
            }
        }
    }
}

#[cfg(test)]
mod tests {
    //! The arithmetic pins, on paused time (no wall cost): probe time
    //! must not spend the clause budget; SPI time still must.

    use super::*;

    struct SleepyProbe(Duration);

    #[async_trait]
    impl TableProbe for SleepyProbe {
        async fn count(&self, _table: &TableName) -> Result<u64, ProbeError> {
            tokio::time::sleep(self.0).await;
            Ok(0)
        }
    }

    /// A slow-but-legal probe (each count far past the whole budget)
    /// must NOT fail the suite: the clock stops during counts, so only
    /// the (tiny) SPI time spends the budget.
    #[tokio::test(start_paused = true)]
    async fn probe_time_does_not_spend_the_clause_budget() {
        let sleepy = SleepyProbe(Duration::from_secs(9));
        let (probe, clock) = StopClockProbe::new(&sleepy);
        let outcome = timeout_excluding_probe(Duration::from_secs(2), &clock, async {
            for _ in 0..4 {
                // 4 counts x 9s = 36s of probe time under a 2s budget.
                probe.count(&TableName::new("t")).await.expect("counts");
                // A sliver of SPI time between counts.
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            "done"
        })
        .await;
        assert_eq!(outcome.expect("probe time is excluded"), "done");
    }

    /// The no-hang bound restored (round-6 fix): a NEVER-returning
    /// probe fails its count within [`PROBE_BOUND`]'s stand-in — the
    /// clause proceeds with a probe failure naming the probe, and the
    /// whole run stays bounded instead of hanging on a stopped clock.
    #[tokio::test(start_paused = true)]
    async fn a_never_returning_probe_fails_within_its_own_bound() {
        struct HungProbe;

        #[async_trait]
        impl TableProbe for HungProbe {
            async fn count(&self, _table: &TableName) -> Result<u64, ProbeError> {
                std::future::pending().await
            }
        }

        let hung = HungProbe;
        let (probe, clock) = StopClockProbe::with_bound(&hung, Duration::from_secs(3));
        let outcome = timeout_excluding_probe(Duration::from_secs(2), &clock, async {
            probe
                .count(&TableName::new("t"))
                .await
                .expect_err("the stalled count must fail, not hang")
        })
        .await;
        let error = outcome.expect("the suite completes — bounded probe, bounded budget");
        assert!(
            error.message.contains("did not answer within 3s"),
            "the failure names the probe's own bound: {}",
            error.message
        );
    }

    /// A genuinely hung SPI call still times out: nothing meters, so
    /// the budget is the plain deadline.
    #[tokio::test(start_paused = true)]
    async fn spi_time_still_spends_the_clause_budget() {
        let sleepy = SleepyProbe(Duration::from_secs(1));
        let (_probe, clock) = StopClockProbe::new(&sleepy);
        let outcome = timeout_excluding_probe(Duration::from_secs(2), &clock, async {
            tokio::time::sleep(Duration::from_secs(60)).await;
            "unreachable"
        })
        .await;
        assert!(outcome.is_err(), "an unmetered hang must elapse the budget");
    }
}
