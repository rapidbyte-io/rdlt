//! How a failure reads on a terminal: what failed, why, and a hint where one really helps.

#[cfg(test)]
mod tests;

use rdlt::{Code, ErrorKind, ErrorReport, Origin};

use super::{Tone, Ui, wrapped};
use crate::output::{Failure, shown};

/// The hints by the code the engine or the host gave a failure; a hint never recommends
/// weakening a default without saying what that costs.
const HINTS: [(&str, &str); 6] = [
    (
        "connector_not_found",
        "`rdlt connectors` lists the binaries the host finds; a connector is an executable \
         named rdlt-connector-<name> in a --connectors directory",
    ),
    (
        "cursor_missing",
        "keep reading it in full, or `rdlt state reset <document> <stream> --tables`, which \
         drops the stream's tables for the next run to load again from its beginning",
    ),
    (
        "grant_outside",
        "the sandbox can write only what you grant; pass --writable naming a root that holds \
         the path",
    ),
    (
        "sandbox_missing",
        "install bubblewrap (`bwrap`), or pass --trust-binaries, which runs connectors with \
         your own access",
    ),
    (
        "sandbox_unavailable",
        "user namespaces are unavailable here; --trust-binaries runs connectors with your own \
         access, in no sandbox",
    ),
    (
        "sandbox_unsupported",
        "this platform has no sandbox; --trust-binaries runs connectors with your own access",
    ),
];

/// The hints by a connector's own code: a table of its own, as a connector is untrusted and
/// must not be able to choose any of the host's words, least of all one that drops its sandbox.
const CONNECTOR_HINTS: [(&str, &str); 1] = [(
    "database_not_found",
    "the path may be wrong; a sandboxed connector can also reach only what the document's \
     `access` grants and --writable allows",
)];

/// The text a terminal is shown for `failure`.
pub(crate) fn failure(failure: &Failure, ui: &Ui) -> String {
    match &failure.report {
        Some(report) => reported(report, failure.file_not_found, ui),
        None => block(&format!("{} {}", mark(ui), shown(&failure.message))),
    }
}

/// The text a terminal is shown for certification's failure `message`.
pub(crate) fn certify_failure(message: &str, ui: &Ui) -> String {
    block(&format!("{} {}", mark(ui), shown(message)))
}

fn reported(report: &ErrorReport, file_not_found: bool, ui: &Ui) -> String {
    let who = match report.origin {
        Origin::Source => "source failed: ",
        Origin::Destination => "destination failed: ",
        Origin::Engine => "",
    };
    let mut text = block(&format!("{} {who}{}", mark(ui), shown(&report.message)));
    let brush = ui.looks().err();
    for cause in &report.causes {
        if !report.message.contains(cause.as_str()) {
            text.push_str(&block(&format!(
                "  {} {}",
                brush.paint(Tone::Dim, "caused by:"),
                shown(cause)
            )));
        }
    }
    if let Some(hint) = hint(report, file_not_found) {
        text.push_str(&block(&format!(
            "  {} {hint}",
            brush.paint(Tone::Dim, "hint:")
        )));
    }
    text
}

/// The hint for what `report` says failed, if one really helps; `file_not_found` says that what
/// could not be read is not there.
fn hint(report: &ErrorReport, file_not_found: bool) -> Option<&'static str> {
    if report.kind == ErrorKind::Fenced {
        return Some(
            "a newer run or session of this pipeline opened: run this one again once it ends",
        );
    }
    let code = report.code.map(Code::as_str);
    if code == Some("document_unreadable") {
        return file_not_found.then_some("`rdlt init` writes a starter document");
    }
    let found = |table: &[(&str, &'static str)], code: &str| {
        let hint = table.iter().find(|(known, _)| *known == code);
        hint.map(|(_, hint)| *hint)
    };
    code.and_then(|code| found(&HINTS, code))
        .or_else(|| found(&CONNECTOR_HINTS, report.connector_code.as_deref()?))
}

fn mark(ui: &Ui) -> String {
    ui.looks().err().paint(Tone::Bad, ui.looks().marks().fail)
}

/// `line`, wrapped to standard error's width, its later lines indented.
fn block(line: &str) -> String {
    let width = super::err_width().saturating_sub(1);
    let mut text = String::new();
    for wrapped in wrapped(line, width, "  ") {
        text.push_str(&wrapped);
        text.push('\n');
    }
    text
}
