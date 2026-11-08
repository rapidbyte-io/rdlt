//! A `tracing` subscriber that keeps every span's name, fields and parent, for tests of what code
//! traces.

use std::cell::RefCell;
use std::fmt::Write as _;
use std::sync::Arc;

use parking_lot::Mutex;
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Metadata, Subscriber};
use tracing_core::span::Current;

/// One span opened: its name, its fields as `name=value` text, recorded ones included, its
/// metadata and the place of its parent.
#[derive(Debug)]
struct Opened {
    name: String,
    fields: String,
    meta: &'static Metadata<'static>,
    parent: Option<usize>,
}

thread_local! {
    /// The spans the calling thread has entered, innermost last, as places in the list of spans.
    static ENTERED: RefCell<Vec<usize>> = const { RefCell::new(Vec::new()) };
}

/// Every span opened while it is the default subscriber: its name, its fields and where it
/// descends from.
#[derive(Clone, Debug, Default)]
pub struct Spans(Arc<Mutex<Vec<Opened>>>);

impl Spans {
    /// The fields of each span named `name`, in the order they opened.
    pub fn named(&self, name: &str) -> Vec<String> {
        self.0
            .lock()
            .iter()
            .filter(|opened| opened.name == name)
            .map(|opened| opened.fields.clone())
            .collect()
    }

    /// Whether there is a span named `name`, and each one has a span named `ancestor` among its
    /// parent, its parent's parent and so on.
    pub fn all_descend_from(&self, name: &str, ancestor: &str) -> bool {
        let spans = self.0.lock();
        let mut found = false;
        for (place, opened) in spans.iter().enumerate() {
            if opened.name != name {
                continue;
            }
            found = true;
            let mut up = spans[place].parent;
            let mut descends = false;
            while let Some(parent) = up {
                descends |= spans[parent].name == ancestor;
                up = spans[parent].parent;
            }
            if !descends {
                return false;
            }
        }
        found
    }

    fn top() -> Option<usize> {
        ENTERED.with(|entered| entered.borrow().last().copied())
    }
}

struct Fields<'a>(&'a mut String);

impl Visit for Fields<'_> {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        write!(self.0, "{}={value:?} ", field.name()).ok();
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        write!(self.0, "{}={value} ", field.name()).ok();
    }
}

impl Subscriber for Spans {
    fn enabled(&self, _: &Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, attributes: &Attributes<'_>) -> Id {
        let mut fields = String::new();
        attributes.record(&mut Fields(&mut fields));
        let mut spans = self.0.lock();
        let parent = if attributes.is_root() {
            None
        } else if let Some(explicit) = attributes.parent() {
            Some(place(explicit))
        } else {
            Self::top()
        };
        spans.push(Opened {
            name: attributes.metadata().name().to_owned(),
            fields,
            meta: attributes.metadata(),
            parent,
        });
        Id::from_u64(u64::try_from(spans.len()).expect("a test opens fewer spans than u64 counts"))
    }

    fn record(&self, span: &Id, values: &Record<'_>) {
        if let Some(opened) = self.0.lock().get_mut(place(span)) {
            values.record(&mut Fields(&mut opened.fields));
        }
    }

    fn record_follows_from(&self, _: &Id, _: &Id) {}

    fn event(&self, _: &Event<'_>) {}

    fn enter(&self, span: &Id) {
        ENTERED.with(|entered| entered.borrow_mut().push(place(span)));
    }

    fn exit(&self, span: &Id) {
        let at = place(span);
        ENTERED.with(|entered| {
            let mut entered = entered.borrow_mut();
            if let Some(found) = entered.iter().rposition(|open| *open == at) {
                entered.remove(found);
            }
        });
    }

    fn current_span(&self) -> Current {
        match Self::top() {
            Some(top) => {
                let meta = self.0.lock()[top].meta;
                Current::new(
                    Id::from_u64(u64::try_from(top + 1).expect("a small place")),
                    meta,
                )
            }
            None => Current::none(),
        }
    }
}

/// The place in the list of spans of the span `id` names.
fn place(id: &Id) -> usize {
    usize::try_from(id.into_u64() - 1).expect("an id this subscriber made")
}
