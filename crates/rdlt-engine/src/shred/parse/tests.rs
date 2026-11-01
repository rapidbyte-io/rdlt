use bytes::Bytes;

use super::{each, visit};
use crate::shred::meter::{Columns, Meter};
use crate::shred::observe::Observed;
use crate::shred::observing::Look;
use crate::shred::records::chunks;
use crate::shred::visit::Context;

#[test]
fn the_fast_parse_stops_at_the_first_record_it_may_have_misread() {
    // 2^64 reads as a whole float of 2^63 or more, which only its digits tell exactly.
    let text = "{\"n\":18446744073709551616}\n{\"n\":1}\n{\"n\":2}\n";
    let chunk = chunks(&[Bytes::from(text)], 1 << 20).unwrap().remove(0);
    let context = Context::new(Meter::reserved(), Columns::new(u64::MAX));
    let appended = each(&chunk, false, &context, |bytes| {
        let mut node = Observed::Null;
        let look = Look {
            node: &mut node,
            context: &context,
            depth: 1,
            target: None,
        };
        visit(bytes, look, false, &context)
    })
    .unwrap();
    assert!(appended.imprecise, "the fast parse took 2^64 as exact");
    assert_eq!(
        appended.rows, 1,
        "the fast parse read on past what it misread"
    );
}
