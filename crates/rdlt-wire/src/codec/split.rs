//! Cuts a batch by rows into frames its receiver's limits admit, a frame at a time: nothing but
//! the batch itself is held beyond the frame being sent.

#[cfg(test)]
mod tests;

use arrow_array::RecordBatch;

use super::compact::compacted;
use super::count::counted;
use super::measure::measured;
use super::{Encoder, IpcFrame};
use crate::error::{Frame, WireError};
use crate::limits::{Limits, Refusal};

/// A batch on its way to a receiver, and how far it has got.
#[derive(Clone, Debug)]
pub struct Cut {
    batch: RecordBatch,
    limits: Limits,
    /// Rows already framed.
    sent: usize,
    /// Rows the last piece held, tried first for the next.
    guess: usize,
    /// Whether the batch is still to be tried whole.
    whole: bool,
    done: bool,
}

impl Cut {
    /// `batch`, to be sent within `limits`, its receiver's.
    pub fn new(batch: RecordBatch, limits: Limits) -> Self {
        let guess = batch.num_rows();
        Self {
            batch,
            limits,
            sent: 0,
            guess,
            whole: true,
            done: false,
        }
    }

    /// Whether every row has been framed, or the batch refused.
    pub fn is_done(&self) -> bool {
        self.done
    }
}

impl Encoder {
    /// The frames carrying the next rows of `cut`, whose batch must be in the schema last
    /// encoded: the dictionaries they need that differ from those sent, then one batch of as
    /// many rows as the receiver's limits admit; `None` once every row has been framed.
    ///
    /// The batch goes as it is where it fits. Otherwise each piece holds only what its rows
    /// name, and is the longest its receiver's rows, values and view bytes admit; where the
    /// frame's bytes bind, it is full to within a row of its average size. Each frame is
    /// measured as its receiver measures it, so none is refused there.
    ///
    /// # Errors
    ///
    /// A [`WireError::Refused`] naming the limit when one row, or a dictionary, is beyond it;
    /// [`WireError::Arrow`] when Arrow cannot encode the batch. Pieces before the row were
    /// framed; the cut is done, and a batch after it needs its schema encoded again.
    pub fn piece(&mut self, cut: &mut Cut) -> Result<Option<Vec<IpcFrame>>, WireError> {
        if cut.done {
            return Ok(None);
        }
        let framed = self.framed(cut);
        if framed.is_err() {
            // The dictionaries queued for the frame were not sent, so the schema epoch is over.
            self.columns = None;
            cut.done = true;
        }
        framed.map(Some)
    }

    fn framed(&mut self, cut: &mut Cut) -> Result<Vec<IpcFrame>, WireError> {
        let (rows, limits) = (cut.batch.num_rows(), cut.limits);
        let mut frames = Vec::new();
        let mut refused = None;
        if std::mem::take(&mut cut.whole) {
            // What an uncut batch's columns count is at least what its frame holds.
            match admitted(&cut.batch, &limits) {
                Ok(()) => match self.trial(&cut.batch, &limits, &mut frames)? {
                    Ok(frame) => {
                        cut.done = true;
                        frames.push(frame);
                        return Ok(frames);
                    }
                    Err(refusal) => {
                        cut.guess = scaled(rows, refusal.limit, refusal.actual);
                        refused = Some(refusal);
                    }
                },
                Err(refusal) => cut.guess = scaled(rows, refusal.limit, refusal.actual),
            }
        }
        let rest = cut.batch.slice(cut.sent, rows - cut.sent);
        if let Some(refusal) = refused.filter(|_| rest.num_rows() == 0) {
            return Err(refusal.into());
        }
        let (taken, frame) = self.longest(&rest, cut.guess, &limits, &mut frames)?;
        cut.sent += taken;
        cut.guess = taken;
        cut.done = cut.sent >= rows;
        frames.push(frame);
        Ok(frames)
    }

    /// The frame of the longest prefix of `rest` that `limits` admit, trying `guess` rows first,
    /// and how many rows it holds; the dictionaries it needs are queued.
    fn longest(
        &mut self,
        rest: &RecordBatch,
        guess: usize,
        limits: &Limits,
        frames: &mut Vec<IpcFrame>,
    ) -> Result<(usize, IpcFrame), WireError> {
        let most = usize::try_from(limits.batch_rows).unwrap_or(usize::MAX);
        let most = rest.num_rows().min(most.max(1));
        let guess = guess.clamp(1, most);
        let (shaped, mut piece) = shaped(rest, guess, most, limits)?;
        // The most rows whose frame fits, and the fewest whose shape or frame does not.
        let (mut fits, mut beyond) = (None, shaped + 1);
        let mut rows = guess.min(shaped);
        if rows < shaped {
            piece = compact(&rest.slice(0, rows))?;
        }
        loop {
            match self.trial(&piece, limits, frames)? {
                Ok(frame) => {
                    // Full when a row more of the piece's average size would not fit.
                    let bytes = frame.header.len().saturating_add(frame.body.len());
                    let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
                    let fuller = scaled(rows, limits.frame_bytes, bytes).min(beyond - 1);
                    if fuller <= rows {
                        return Ok((rows, frame));
                    }
                    (fits, rows) = (Some((rows, frame)), fuller);
                }
                Err(refusal) => {
                    beyond = rows;
                    let low = fits.as_ref().map_or(0, |(rows, _)| *rows);
                    if low + 1 >= beyond {
                        return fits.ok_or_else(|| refusal.into());
                    }
                    let fewer = scaled(rows, refusal.limit, refusal.actual);
                    rows = fewer.clamp(low + 1, beyond - 1);
                }
            }
            piece = compact(&rest.slice(0, rows))?;
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

/// The most rows of `rest`, up to `most`, that hold no more values and view bytes than `limits`
/// admit, counted without encoding them, with those rows holding only what they name; `guess`
/// rows are tried first.
fn shaped(
    rest: &RecordBatch,
    guess: usize,
    most: usize,
    limits: &Limits,
) -> Result<(usize, RecordBatch), WireError> {
    let tried = |rows: usize| -> Result<Result<RecordBatch, Refusal>, WireError> {
        let piece = compact(&rest.slice(0, rows))?;
        Ok(admitted(&piece, limits).map(|()| piece))
    };
    // The most rows known to fit, or why none does; the fewest known not to, and why.
    let mut fits = tried(guess)?.map(|piece| (guess, piece));
    let mut beyond = fits.as_ref().err().map(|refusal| (guess, *refusal));
    let (mut step, mut misses) = (1_usize, u32::from(fits.is_err()));
    loop {
        let low = fits.as_ref().map_or(0, |(rows, _)| *rows);
        let high = beyond.as_ref().map_or(most + 1, |(rows, _)| *rows);
        if low + 1 >= high {
            return fits.map_err(WireError::from);
        }
        // Past rows that fit, a row more, then twice as many; past rows that do not, as many
        // as the limit is of what they hold, then halfway.
        let rows = match &beyond {
            Some((rows, refusal)) if misses == 1 => {
                scaled(*rows, refusal.limit, refusal.actual).clamp(low + 1, high - 1)
            }
            Some(_) if misses > 1 => low + (high - low) / 2,
            _ => low.saturating_add(step).min(high - 1),
        };
        match tried(rows)? {
            Ok(piece) => (fits, misses, step) = (Ok((rows, piece)), 0, step.saturating_mul(2)),
            Err(refusal) => {
                if fits.is_err() {
                    fits = Err(refusal);
                }
                (beyond, misses, step) = (Some((rows, refusal)), misses + 1, 1);
            }
        }
    }
}

/// Admits what the columns of `batch` count, within the values and view bytes of `limits`.
fn admitted(batch: &RecordBatch, limits: &Limits) -> Result<(), Refusal> {
    let counted = counted(batch);
    Limits::admit("batch values", limits.batch_values, counted.values)?;
    Limits::admit("view bytes", limits.frame_bytes, counted.view_bytes)
}

/// `piece` holding only what its rows name.
fn compact(piece: &RecordBatch) -> Result<RecordBatch, WireError> {
    compacted(piece).map_err(|source| WireError::Arrow {
        frame: Frame::Batch,
        encoding: true,
        source,
    })
}

/// How many of `rows` rows a limit of `limit` admits, when they hold `actual` between them: at
/// least one.
fn scaled(rows: usize, limit: u64, actual: u64) -> usize {
    let rows = u128::try_from(rows).unwrap_or(u128::MAX);
    let scaled = rows.saturating_mul(u128::from(limit)) / u128::from(actual.max(1));
    usize::try_from(scaled).unwrap_or(usize::MAX).max(1)
}
