//! What a seed's thread holds on the heap over an attempt, against what its runs account for:
//! their reservations, a push a partition, and an allowance.

#[cfg(test)]
mod tests;

use alloc_tracker::{Session, ThreadSpan};

/// A stretch of the calling thread's work whose heap peak is measured: the most the thread held
/// beyond what it held as the stretch began.
pub(super) struct Window {
    session: Session,
    span: Option<ThreadSpan>,
}

impl Window {
    /// Starts measuring the calling thread's heap, from what it holds now.
    pub(super) fn open() -> Self {
        let session = Session::new().no_stdout().no_file();
        let span = session.operation("window").measure_thread().iterations(1);
        Self {
            session,
            span: Some(span),
        }
    }

    /// Bytes: the most the thread held beyond what it held as the window opened.
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "a peak is a whole, non-negative number of bytes far below 2^53"
    )]
    pub(super) fn close(mut self) -> u64 {
        drop(self.span.take());
        let report = self.session.to_report();
        report
            .operations()
            .find_map(|(_, operation)| operation.peak_outstanding_bytes())
            .map_or(0, |peak| peak as u64)
    }
}

/// Whether the binary's allocator counts this thread's allocations, without which every window
/// measures nothing.
pub(super) fn installed() -> bool {
    let window = Window::open();
    drop(std::hint::black_box(vec![0_u8; 64]));
    window.close() >= 64
}
