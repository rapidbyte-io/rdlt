//! A connector's process played in this process, for the simulation: given what a spawned
//! connector is given, checked, drained, stopped and killed as one is, with nothing spawned, no
//! descriptor passed and no thread started.

use std::ffi::OsString;
use std::fmt;
use std::process::ExitStatus;
use std::sync::Arc;

use rdlt_connector::ConnectorId;
use tokio::io::AsyncRead;
use tokio_util::sync::CancellationToken;

use super::{Ended, Launch, Launched, Owner, Process, Told, Unspawned, command, draining};
use crate::kills::Kills;
use crate::network::Stream;
use crate::secrets::Redactions;

/// What a played connector's script is given as its process starts.
#[derive(Debug)]
pub struct Playing {
    /// The connector its reference names.
    pub id: ConnectorId,
    /// The arguments a spawned connector would be given.
    pub args: Vec<OsString>,
    /// Cancelled when the host asks the connector to stop, as a spawned one's input ending asks.
    pub stopped: CancellationToken,
    /// Cancelled when a kill kills it, where one may.
    pub killed: Option<CancellationToken>,
}

/// What a played connector's process gives the host, as a spawned one's gives it.
pub struct Played {
    /// Its process id, as the host is told it.
    pub id: u32,
    /// The host's end of its socket.
    pub socket: Box<dyn Stream>,
    /// Its standard output.
    pub stdout: Box<dyn AsyncRead + Send + Unpin>,
    /// Its standard error.
    pub stderr: Box<dyn AsyncRead + Send + Unpin>,
    /// How it exited, once it has.
    pub exit: Arc<Ended<ExitStatus>>,
}

impl fmt::Debug for Played {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Played")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

/// What plays each connector's process.
#[derive(Clone)]
pub(crate) struct Play(Arc<dyn Fn(Playing) -> std::io::Result<Played> + Send + Sync>);

impl Play {
    pub(crate) fn new(
        play: impl Fn(Playing) -> std::io::Result<Played> + Send + Sync + 'static,
    ) -> Self {
        Self(Arc::new(play))
    }
}

impl fmt::Debug for Play {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Play")
    }
}

impl Process {
    /// Plays `launch`'s connector with `play`, its output scrubbed of `redactions`, once the
    /// checks every start makes pass: the host's end of its socket, and the process.
    pub(super) fn played(
        launch: &Launch,
        play: &Play,
        redactions: Redactions,
    ) -> Result<Launched, Unspawned> {
        command::checked(launch)?;
        let killed = launch.kills.as_ref().map(Kills::next);
        let stopped = CancellationToken::new();
        let playing = Playing {
            id: launch.id.clone(),
            args: command::arguments(launch),
            stopped: stopped.clone(),
            killed: killed.clone(),
        };
        let Played {
            id,
            socket,
            stdout,
            stderr,
            exit,
        } = (play.0)(playing)?;
        let (tail, stderr_closed) =
            draining(Some(stdout), Some(stderr), id, &launch.id, &redactions);
        if let Some(Told(told)) = &launch.told {
            told(id);
        }
        let socket = match (&launch.kills, &killed) {
            (Some(kills), Some(killed)) => kills.watch(socket, killed.clone()),
            _ => socket,
        };
        let process = Self {
            owner: Owner::Played(stopped),
            killed,
            exit,
            stderr_closed,
            tail,
            redactions,
        };
        Ok((socket, process))
    }
}
