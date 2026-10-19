use crate::cost::tests::HEAP;
use crate::shred::meter::{Columns, Meter};
use crate::shred::observe::Observed;
use crate::shred::observing::Look;
use crate::shred::visit::Context;

/// A record of `keys` keys, each holding a number of ten digits, as JSON text.
fn wide(keys: usize) -> String {
    let fields: Vec<String> = (0..keys)
        .map(|key| format!("\"k{key}\":{}", 1_000_000_000 + key))
        .collect();
    format!("{{{}}}", fields.join(","))
}

/// Whether the exact parse took `text`, observed as a record's value is.
fn walked(text: &str) -> bool {
    let context = Context::new(Meter::reserved(), Columns::new(u64::MAX));
    let mut node = Observed::Null;
    let look = Look {
        node: &mut node,
        context: &context,
        depth: 1,
        target: None,
    };
    super::visit(text.as_bytes(), look, &context).is_ok()
}

#[test]
fn a_thread_that_parsed_exactly_keeps_no_more_than_three_mebibytes() {
    let text = wide(400_000);
    let kept = std::thread::scope(|scope| {
        scope
            .spawn(|| {
                let before = HEAP.current_usage();
                assert!(walked(&text));
                assert!(walked(&text));
                HEAP.current_usage().saturating_sub(before)
            })
            .join()
            .unwrap()
    });
    assert!(
        kept <= 3 << 20,
        "a thread kept {kept} bytes once it had parsed"
    );
}
