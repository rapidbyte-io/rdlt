//! Killing a spawned connector before a chosen commit: a destination that counts commits and, at
//! the chosen one, kills the processes the victim's host spawned before passing it on.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use rdlt_connector::{
    BoxFuture, Capabilities, CommitMeta, Destination, DestinationSession, DestinationWriter,
    OpenContext, OpenedSession, Receipt, Result, TableChange, TableRef,
};
use rdlt_host::Kills;

/// `inner`, whose `commit`th commit, counted across its sessions, first kills what `kills` reaches.
pub(crate) fn killing(
    inner: Arc<dyn Destination>,
    kills: Kills,
    commit: u64,
) -> Arc<dyn Destination> {
    Arc::new(Killing {
        inner,
        point: Arc::new(Point {
            kills,
            commit,
            commits: AtomicU64::new(0),
        }),
    })
}

/// Where the kill falls, and the commits counted towards it.
struct Point {
    kills: Kills,
    commit: u64,
    commits: AtomicU64,
}

impl Point {
    /// Counts a commit, killing first where it is the chosen one.
    fn commit(&self) {
        if self.commits.fetch_add(1, Ordering::SeqCst) + 1 == self.commit {
            self.kills.kill();
        }
    }
}

struct Killing {
    inner: Arc<dyn Destination>,
    point: Arc<Point>,
}

impl Destination for Killing {
    fn capabilities(&self) -> &Capabilities {
        self.inner.capabilities()
    }

    fn check(&self) -> BoxFuture<'_, Result<()>> {
        self.inner.check()
    }

    fn open<'a>(&'a self, context: &'a OpenContext) -> BoxFuture<'a, Result<OpenedSession>> {
        Box::pin(async move {
            let opened = self.inner.open(context).await?;
            Ok(OpenedSession {
                session: Box::new(Session {
                    inner: opened.session,
                    point: Arc::clone(&self.point),
                }),
                ..opened
            })
        })
    }
}

struct Session {
    inner: Box<dyn DestinationSession>,
    point: Arc<Point>,
}

impl DestinationSession for Session {
    fn apply_schema<'a>(&'a mut self, change: &'a TableChange) -> BoxFuture<'a, Result<()>> {
        self.inner.apply_schema(change)
    }

    fn writer<'a>(
        &'a mut self,
        table: &'a TableRef,
    ) -> BoxFuture<'a, Result<Box<dyn DestinationWriter>>> {
        self.inner.writer(table)
    }

    fn commit<'a>(&'a mut self, meta: &'a CommitMeta) -> BoxFuture<'a, Result<Receipt>> {
        self.point.commit();
        self.inner.commit(meta)
    }

    fn close(self: Box<Self>) -> BoxFuture<'static, Result<()>> {
        self.inner.close()
    }
}
