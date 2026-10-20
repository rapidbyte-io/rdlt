//! A subscriber that looks for a world's secrets in every event and span its thread logs.

use std::fmt::Write as _;
use std::sync::Arc;

use parking_lot::Mutex;
use tracing::span::{Attributes, Id, Record};
use tracing::subscriber::Interest;
use tracing::{Event, Level, Metadata, Subscriber};

use super::Canary;

/// What a thread logged that held a secret.
#[derive(Clone)]
pub(crate) struct Logged {
    canary: Canary,
    leaks: Arc<Mutex<Vec<String>>>,
}

impl Logged {
    pub(super) fn new(canary: Canary) -> Self {
        Self {
            canary,
            leaks: Arc::default(),
        }
    }

    /// Each event or span logged so far that held a secret, as a broken invariant.
    pub(crate) fn leaks(&self) -> Vec<String> {
        self.leaks.lock().clone()
    }

    fn look(&self, what: &str, fields: &str) {
        if let Some(secret) = self.canary.found(fields) {
            let leak = format!("{what} logged the secret {secret}: {fields}");
            self.leaks.lock().push(leak);
        }
    }
}

/// Whether what `metadata` describes is looked at: everything of rdlt's own, and the rest from
/// `Level::INFO` up, as an operator's log keeps it.
fn watched(metadata: &Metadata<'_>) -> bool {
    metadata.target().starts_with("rdlt") || *metadata.level() <= Level::INFO
}

/// An event's or a span's fields, each written as `name=value`.
#[derive(Default)]
struct Fields(String);

impl tracing::field::Visit for Fields {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        write!(self.0, "{}={value:?} ", field.name()).ok();
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        write!(self.0, "{}={value} ", field.name()).ok();
    }

    /// An error's causes too, as an operator's log writes them beside it.
    fn record_error(
        &mut self,
        field: &tracing::field::Field,
        value: &(dyn std::error::Error + 'static),
    ) {
        write!(self.0, "{}={} ", field.name(), super::texts(value)).ok();
    }
}

impl Subscriber for Logged {
    fn register_callsite(&self, metadata: &'static Metadata<'static>) -> Interest {
        if watched(metadata) {
            Interest::always()
        } else {
            Interest::never()
        }
    }

    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        watched(metadata)
    }

    fn new_span(&self, attributes: &Attributes<'_>) -> Id {
        let mut fields = Fields::default();
        attributes.record(&mut fields);
        self.look("a span", &fields.0);
        Id::from_u64(1)
    }

    fn record(&self, _: &Id, values: &Record<'_>) {
        let mut fields = Fields::default();
        values.record(&mut fields);
        self.look("a span", &fields.0);
    }

    fn record_follows_from(&self, _: &Id, _: &Id) {}

    fn event(&self, event: &Event<'_>) {
        let mut fields = Fields::default();
        event.record(&mut fields);
        self.look("an event", &fields.0);
    }

    fn enter(&self, _: &Id) {}

    fn exit(&self, _: &Id) {}
}
