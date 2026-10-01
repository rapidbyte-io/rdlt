//! Cuts a batch by rows into the fewest frames its receiver's limits admit.

#[cfg(test)]
mod tests;

use arrow_array::RecordBatch;

use super::compact::compacted;
use super::measure::measured;
use super::{Encoder, IpcFrame};
use crate::error::{Frame, Problem, WireError};
use crate::limits::{Limits, Refusal};

impl Encoder {
    /// The frames of `batch`, which must be in the schema last encoded, cut by rows into the
    /// fewest batches that `limits`, its receiver's, admit: the dictionaries it needs that differ
    /// from those sent, then each batch in order.
    ///
    /// Each frame is measured as its receiver measures it, so none is refused there.
    ///
    /// # Errors
    ///
    /// A [`WireError::Refused`] naming the limit when one row, or a dictionary, is beyond it;
    /// [`WireError::Arrow`] when Arrow cannot encode the batch. No frame of the batch is to be
    /// sent then, and a batch after it needs its schema encoded again.
    pub fn batch_within(
        &mut self,
        batch: &RecordBatch,
        limits: &Limits,
    ) -> Result<Vec<IpcFrame>, WireError> {
        let cut = self.cut(batch, limits);
        if cut.is_err() {
            // The dictionaries queued for the frames were not sent, so the schema epoch is over.
            self.columns = None;
        }
        cut
    }

    fn cut(&mut self, batch: &RecordBatch, limits: &Limits) -> Result<Vec<IpcFrame>, WireError> {
        let rows = batch.num_rows();
        let mut frames = Vec::new();
        let (mut start, mut guess) = (0, rows);
        loop {
            let rest = batch.slice(start, rows - start);
            let taken = self.longest(&rest, guess, limits, &mut frames)?;
            start += taken;
            guess = taken;
            if start >= rows {
                return Ok(frames);
            }
        }
    }

    /// Queues the frame of the longest prefix of `rest` that `limits` admit, trying `guess` rows
    /// first, after the dictionaries it needs: how many rows it holds.
    fn longest(
        &mut self,
        rest: &RecordBatch,
        guess: usize,
        limits: &Limits,
        frames: &mut Vec<IpcFrame>,
    ) -> Result<usize, WireError> {
        let most = usize::try_from(limits.batch_rows).unwrap_or(usize::MAX);
        let most = rest.num_rows().min(most.max(1));
        // The most rows known to fit, with their frame, and the fewest known not to, with why.
        let (mut low, mut kept) = (0, None);
        let (mut high, mut refused) = (most + 1, None);
        let (mut rows, mut step) = (guess.min(most), 1_usize);
        loop {
            let piece = rest.slice(0, rows);
            // The whole of a batch goes as it is; a part of one carries only its rows.
            let piece = if rows == rest.num_rows() && frames.is_empty() {
                piece
            } else {
                compacted(&piece).map_err(|source| WireError::Arrow {
                    frame: Frame::Batch,
                    encoding: true,
                    source,
                })?
            };
            match self.trial(&piece, limits, frames)? {
                Ok(frame) => (low, kept) = (rows, Some(frame)),
                Err(refusal) => (high, refused) = (rows, Some(refusal)),
            }
            if low + 1 >= high {
                break;
            }
            // Past a prefix that fits, a row more, then twice as many, until one does not;
            // then halfway between.
            rows = if refused.is_some() {
                low + (high - low) / 2
            } else {
                low.saturating_add(step).min(most)
            };
            step = step.saturating_mul(2);
        }
        match (kept, refused) {
            (Some(frame), _) => {
                frames.push(frame);
                Ok(low)
            }
            (None, Some(refusal)) => Err(refusal.into()),
            (None, None) => Err(WireError::malformed(Frame::Batch, Problem::NoSchema)),
        }
    }

    /// Encodes `piece` and queues the dictionaries it needs: its frame, or the refusal of the
    /// limit it is beyond.
    fn trial(
        &mut self,
        piece: &RecordBatch,
        limits: &Limits,
        frames: &mut Vec<IpcFrame>,
    ) -> Result<Result<IpcFrame, Refusal>, WireError> {
        let (dictionaries, frame) = self.encoded(piece)?;
        for dictionary in dictionaries {
            measured(self.columns.as_ref(), limits, &dictionary)?;
            frames.push(dictionary);
        }
        match measured(self.columns.as_ref(), limits, &frame) {
            Ok(_) => Ok(Ok(frame)),
            Err(WireError::Refused(refusal)) => Ok(Err(refusal)),
            Err(error) => Err(error),
        }
    }
}
