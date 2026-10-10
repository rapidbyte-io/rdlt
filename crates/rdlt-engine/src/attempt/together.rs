//! Futures awaited together within one task, a bounded number at once, their outputs in order.

use std::future::{Future, poll_fn};
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::task::Poll;

/// The outputs of `futures`, in their order, awaited together with no more than `bound` of them
/// started and unfinished at once: each starts, in order, as soon as one before it finishes.
pub(super) async fn in_order<F: Future>(futures: Vec<F>, bound: NonZeroUsize) -> Vec<F::Output> {
    let mut outputs: Vec<Option<F::Output>> = std::iter::repeat_with(|| None)
        .take(futures.len())
        .collect();
    let mut waiting = futures.into_iter().enumerate();
    let mut running: Vec<(usize, Pin<Box<F>>)> = Vec::with_capacity(bound.get());
    poll_fn(|cx| {
        loop {
            while running.len() < bound.get() {
                let Some((index, future)) = waiting.next() else {
                    break;
                };
                running.push((index, Box::pin(future)));
            }
            let before = running.len();
            running.retain_mut(|(index, future)| match future.as_mut().poll(cx) {
                Poll::Ready(output) => {
                    outputs[*index] = Some(output);
                    false
                }
                Poll::Pending => true,
            });
            if running.is_empty() && waiting.len() == 0 {
                return Poll::Ready(());
            }
            if running.len() == before {
                return Poll::Pending;
            }
        }
    })
    .await;
    outputs.into_iter().flatten().collect()
}

#[cfg(test)]
mod tests;
