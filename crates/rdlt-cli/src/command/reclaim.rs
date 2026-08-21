//! The `reclaim` subcommand: sweep crashed connectors' serve
//! directories NOW, instead of waiting for the next spawn's automatic
//! sweep to do it. The rmdir-only rule is the sdk's own (its liveness
//! guarantee is the Guard-unlink pin), so a LIVE connector's directory
//! is never touched — the count this prints is exactly the dead debris
//! that existed.

use crate::exit;

pub(crate) fn reclaim() -> Result<(), exit::Error> {
    let reclaimed = rdlt_connector_sdk::serve::reclaim_dead_serve_dirs();
    match reclaimed {
        0 => render::stderr::line("no stale connector directories to reclaim"),
        1 => render::stderr::line("reclaimed 1 stale connector directory"),
        n => render::stderr::line(&format!("reclaimed {n} stale connector directories")),
    }
    Ok(())
}

use crate::render;
