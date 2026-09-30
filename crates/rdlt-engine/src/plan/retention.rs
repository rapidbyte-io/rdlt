//! What a stream does when its source no longer holds where a read would resume.

/// What a stream does when its source's retention dropped where a partition's read would resume
/// (the owner's streaming item 4).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RetentionLoss {
    /// The run fails with the source's `retention_lost` error: data is never skipped silently.
    #[default]
    Fail,
    /// The partition reads again from its source's earliest, skipping what the source dropped;
    /// the report counts each reset.
    Reset,
}
