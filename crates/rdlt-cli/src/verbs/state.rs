//! `rdlt state reset`.

use std::path::Path;

use rdlt::ResetScope;

use crate::Exit;
use crate::args::HostArgs;
use crate::output::{self, Failure, Mode};
use crate::ui::{self, Tone, Ui};

/// Resets `streams` of the pipeline `document` describes: their positions, or their tables too
/// where `tables` asks.
pub(crate) async fn reset(
    document: &Path,
    streams: &[String],
    tables: bool,
    host: &HostArgs,
    mode: Mode,
    ui: &Ui,
) -> Result<Exit, Failure> {
    ui.task("Placing connectors");
    let builder = crate::host::document(document).await?;
    let pipeline = builder.build(&crate::host::host(host).await?).await?;
    let scope = if tables {
        ResetScope::Tables
    } else {
        ResetScope::Positions
    };
    ui.task("Resetting");
    let reset = pipeline.reset(streams, scope).await?;
    let json = serde_json::json!({
        "streams": reset.streams.iter().map(ToString::to_string).collect::<Vec<_>>(),
        "dropped": reset.dropped.iter().map(ToString::to_string).collect::<Vec<_>>(),
        "epoch": reset.epoch.0,
    });
    let plain = || {
        vec![format!(
            "reset {} at epoch {}",
            streams.join(", "),
            reset.epoch.0
        )]
    };
    let rich = || {
        let streams: Vec<String> = reset.streams.iter().map(ToString::to_string).collect();
        let dropped: Vec<String> = reset.dropped.iter().map(ToString::to_string).collect();
        ui::reset(&streams, &dropped, reset.epoch.0, ui)
    };
    output::result(ui, mode, Some(Tone::Good), plain, rich, &json).await?;
    Ok(Exit::Success)
}
