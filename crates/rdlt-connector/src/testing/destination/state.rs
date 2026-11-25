//! `D-STATE`: committed state records are returned by the next open of their pipeline, and by
//! no other pipeline's.

use bytes::Bytes;

use super::{Bench, commit, meta};
use crate::id::PipelineId;
use crate::state::{StateChange, StateRecord};
use crate::testing::Violation;

impl Bench<'_> {
    /// Commits a record through one connection, and another pipeline's record under the same key,
    /// and reads back through the other connection, so state kept in one connection's memory, or
    /// shared by pipelines, is caught: each pipeline's open returns its own record alone, and a
    /// delete in one leaves the other's.
    pub(super) async fn state_round_trips(&self) -> Result<(), Violation> {
        let record = |value: &'static [u8]| StateRecord {
            key: "certify".to_owned(),
            value: Bytes::from_static(value),
        };
        let (ours, theirs) = (record(b"{\"v\":1}"), record(b"{\"v\":2}"));
        let other = PipelineId::parse(format!("{}_other", self.name()))
            .map_err(|error| Violation::from(format!("pipeline id: {error}")))?;
        let mut opened = self.open(self.destination).await?;
        commit(&mut opened, meta(&[], vec![StateChange::Put(ours.clone())])).await?;
        let mut elsewhere = self.open_as(self.destination, other.clone()).await?;
        let put = vec![StateChange::Put(theirs.clone())];
        commit(&mut elsewhere, meta(&[], put)).await?;
        let mut reopened = self.open(self.peer).await?;
        if reopened.state != [ours.clone()] {
            return Err(format!(
                "the next open returned {:?}, not the pipeline's one committed record",
                reopened.state
            )
            .into());
        }
        let found = self.open_as(self.peer, other.clone()).await?.state;
        if found != [theirs.clone()] {
            return Err(format!(
                "another pipeline's open returned {found:?}, not its one committed record"
            )
            .into());
        }
        let delete = vec![StateChange::Delete(ours.key.clone())];
        commit(&mut reopened, meta(&[], delete)).await?;
        let kept = self.open(self.destination).await?.state;
        if kept.iter().any(|stored| stored.key == ours.key) {
            return Err("a deleted record was returned by the next open".into());
        }
        let found = self.open_as(self.destination, other).await?.state;
        if found != [theirs] {
            return Err(format!(
                "after another pipeline deleted its record, an open returned {found:?}"
            )
            .into());
        }
        Ok(())
    }
}
