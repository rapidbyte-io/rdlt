//! Cuts a batch by rows into frames its receiver's limits admit, a frame at a time: the rows
//! are weighed in stretches, in order, and each piece is narrowed to what its rows name and
//! encoded once, so nothing but the batch and the frame being sent is held.

#[cfg(test)]
mod tests;

use arrow_array::RecordBatch;

use super::compact::Narrower;
use super::measure::measured;
use super::weigh::{Weigher, Weight};
use super::{Encoder, IpcFrame};
use crate::error::{Frame, WireError};
use crate::limits::{Limits, Refusal};

/// A batch on its way to a receiver, and how far it has got.
#[derive(Debug)]
pub struct Cut {
    batch: RecordBatch,
    limits: Limits,
    weigher: Weigher,
    narrower: Narrower,
    /// Rows already framed.
    sent: usize,
    done: bool,
    /// What the cut has cost, and what a test has it leave out of its weighing.
    #[cfg(test)]
    pub(super) probe: Probe,
}

/// What a cut has cost so far, for tests of its bounds.
#[cfg(test)]
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct Probe {
    /// Rows weighed, those of a stretch that did not fit too.
    pub(super) weighed: usize,
    /// Pieces handed to the narrower.
    pub(super) compactions: usize,
    /// Pieces encoded again with half the rows.
    pub(super) halvings: usize,
    /// Whether the weighing leaves out a frame's padding and header, to fall short.
    pub(super) unpadded: bool,
}

impl Cut {
    /// `batch`, to be sent within `limits`: the lesser of its receiver's and its sender's own.
    pub fn new(batch: RecordBatch, limits: Limits) -> Self {
        Self {
            weigher: Weigher::within(&batch, limits.batch_values),
            narrower: Narrower::default(),
            batch,
            limits,
            sent: 0,
            done: false,
            #[cfg(test)]
            probe: Probe::default(),
        }
    }

    /// Whether every row has been framed, or the batch refused.
    pub fn is_done(&self) -> bool {
        self.done
    }

    /// Bytes: what a frame takes beside what its rows weigh.
    fn overhead(&self) -> u64 {
        #[cfg(test)]
        if self.probe.unpadded {
            return 0;
        }
        self.weigher.overhead()
    }

    /// What `rows` rows from `start` add to the piece being weighed.
    fn weighed(&mut self, start: usize, rows: usize) -> Weight {
        #[cfg(test)]
        {
            self.probe.weighed += rows;
        }
        self.weigher.weigh_rows(start..start + rows)
    }

    /// How many of the rows left make the longest piece their weights say the limits admit,
    /// and what they weigh: at least one row, whatever it weighs in bytes.
    ///
    /// Stretches of rows are weighed, each twice as long as the last while they fit and half
    /// as long once one did not: a piece costs a few times the weighing of its own rows.
    fn longest(&mut self) -> Result<(usize, Weight), Refusal> {
        let limits = self.limits;
        Limits::admit("batch rows", limits.batch_rows, 1)?;
        let most = usize::try_from(limits.batch_rows).unwrap_or(usize::MAX);
        let most = most.min(self.batch.num_rows() - self.sent);
        let overhead = self.overhead();
        let fits = |weight: &Weight| {
            weight.values <= limits.batch_values
                && weight.view_bytes <= limits.frame_bytes
                && weight.frame_bytes().saturating_add(overhead) <= limits.frame_bytes
        };
        self.weigher.begin();
        // A row's values and view bytes are weighed as its receiver counts them.
        let mut weight = self.weighed(self.sent, 1);
        Limits::admit("batch values", limits.batch_values, weight.values)?;
        Limits::admit("view bytes", limits.frame_bytes, weight.view_bytes)?;
        // How many more rows are tried while every stretch fitted, and once one did not, how
        // many are known not to fit.
        let (mut taken, mut step, mut unfit) = (1, 1_usize, None);
        while taken < most {
            let rows = match unfit {
                None => step.min(most - taken),
                Some(unfit) if unfit > 1 => unfit / 2,
                Some(_) => break,
            };
            self.weigher.mark();
            let mut longer = weight;
            longer += self.weighed(self.sent + taken, rows);
            if fits(&longer) {
                (taken, weight) = (taken + rows, longer);
                step = step.saturating_mul(2);
                unfit = unfit.map(|unfit| unfit - rows);
            } else {
                self.weigher.rewind();
                unfit = Some(rows);
            }
        }
        Ok((taken, weight))
    }

    /// The next `rows` rows as they go: holding only what they name.
    fn piece(&mut self, rows: usize) -> Result<RecordBatch, WireError> {
        #[cfg(test)]
        {
            self.probe.compactions += 1;
        }
        let piece = self.batch.slice(self.sent, rows);
        self.narrower
            .batch(&piece)
            .map_err(|source| WireError::Arrow {
                frame: Frame::Batch,
                encoding: true,
                source,
            })
    }
}

impl Encoder {
    /// The frames carrying the next rows of `cut`, whose batch must be in the schema last
    /// encoded: the dictionaries they need that differ from those sent, then one batch of as
    /// many rows as the limits admit; `None` once every row has been framed.
    ///
    /// The rows are weighed as the receiver will count them, so a piece is the longest its
    /// rows, values and view bytes admit, and the longest whose weight in bytes fits a frame.
    /// A piece holds only what its rows name. Each frame is measured as its receiver measures
    /// it before it is handed over, so none is refused there.
    ///
    /// # Errors
    ///
    /// A [`WireError::Refused`] naming the limit when one row, or a dictionary, is beyond it;
    /// [`WireError::Arrow`] when Arrow cannot encode the batch. The pieces before the row were
    /// handed over; the cut is done, and a batch after it needs its schema encoded again.
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
        let mut frames = Vec::new();
        // A batch of no rows is one frame of none.
        let (mut rows, _) = match cut.batch.num_rows() {
            0 => (0, Weight::default()),
            _ => cut.longest()?,
        };
        loop {
            let piece = cut.piece(rows)?;
            match self.trial(&piece, &cut.limits, &mut frames)? {
                Ok(frame) => {
                    cut.sent += rows;
                    cut.done = cut.sent >= cut.batch.num_rows();
                    frames.push(frame);
                    return Ok(frames);
                }
                Err(refusal) if rows <= 1 => return Err(refusal.into()),
                // The rows weighed less than their frame takes: half as many.
                Err(_) => rows /= 2,
            }
            #[cfg(test)]
            {
                cut.probe.halvings += 1;
            }
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
