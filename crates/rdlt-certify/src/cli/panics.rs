//! What this process prints when it panics: one bounded line a terminal shows and does not obey.

#[cfg(test)]
mod tests;

use std::io::Write as _;
use std::panic::PanicHookInfo;

use rdlt_certify::line;

/// Prints each panic of this process to standard error through [`said`], scrubbed of the
/// configuration's secrets, in place of the message as it was raised, which may carry what a
/// connector sent.
pub(super) fn contain() {
    report(|said| {
        let said = super::redactions().scrubbed(said.to_owned());
        writeln!(std::io::stderr(), "rdlt-certify: {said}").ok();
    });
}

/// Hands each panic of this process, as [`said`] says it, to `print`.
fn report(print: impl Fn(&str) + Send + Sync + 'static) {
    std::panic::set_hook(Box::new(move |panic| print(&said(panic))));
}

/// `panic` as one line: where it was raised and its message, cut and escaped as a clause's
/// reason is.
fn said(panic: &PanicHookInfo<'_>) -> String {
    let message = panic.payload_as_str().unwrap_or("with no message");
    match panic.location() {
        Some(at) => line(format_args!("panicked at {at}: {message}")),
        None => line(format_args!("panicked: {message}")),
    }
}
