//! A push decoded from a frame its read charged before decoding it enters within that charge,
//! and every other event lets its charge go before it is admitted.

use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use bytes::Bytes;

use super::{Noted, json};
use crate::cursor::Cursor;
use crate::error::{ConnectorError, ConnectorErrorKind};
use crate::sink::{
    Admission, Permit, Push, SourceEvent, admitted_partition_channel, partition_channel,
};
use crate::spec::BoxFuture;

/// Admits a charged push within its charge, refusing it where `refuse`, and every other event
/// at once; notes each in `order`, and whether its charge was still held as it was admitted.
struct Within {
    refuse: bool,
    order: Arc<Mutex<Vec<&'static str>>>,
}

impl Admission for Within {
    fn admit(&self, _: &SourceEvent) -> BoxFuture<'_, crate::Result<Option<Permit>>> {
        self.order.lock().unwrap().push("admitted");
        Box::pin(async { Ok(None) })
    }

    fn admit_charged(&self, _: &Push, charge: &mut Permit) -> crate::Result<()> {
        assert!(
            charge.downcast_ref::<Noted>().is_some(),
            "the push's own charge"
        );
        self.order.lock().unwrap().push("admitted within");
        if self.refuse {
            return Err(ConnectorError::internal("more than its frame was charged"));
        }
        Ok(())
    }

    fn charge(&self, bytes: u64) -> crate::Result<Permit> {
        Ok(Box::new(bytes))
    }
}

fn noted(what: &'static str, order: &Arc<Mutex<Vec<&'static str>>>) -> Noted {
    Noted {
        what,
        order: Arc::clone(order),
    }
}

#[tokio::test]
async fn a_charged_push_enters_holding_its_frame_s_charge_and_waits_for_no_admission() {
    let order = Arc::new(Mutex::new(Vec::new()));
    let within = Arc::new(Within {
        refuse: false,
        order: Arc::clone(&order),
    });
    let (mut sink, mut feed) = admitted_partition_channel(NonZeroUsize::MIN, within);
    let charge = Box::new(noted("charge", &order)) as Permit;
    sink.send_charged(json("[1]"), charge).await.unwrap();
    let (event, permit) = feed.recv_admitted().await.unwrap();
    assert_eq!(event, json("[1]"));
    assert_eq!(*order.lock().unwrap(), ["admitted within"]);
    let permit = permit.expect("the push's permit");
    assert!(
        permit.downcast_ref::<Noted>().is_some(),
        "its frame's charge"
    );
    drop(permit);
    assert_eq!(*order.lock().unwrap(), ["admitted within", "charge"]);
}

#[tokio::test]
async fn a_charged_push_refused_is_freed_before_its_charge_goes() {
    let order = Arc::new(Mutex::new(Vec::new()));
    let within = Arc::new(Within {
        refuse: true,
        order: Arc::clone(&order),
    });
    let (mut sink, mut feed) = admitted_partition_channel(NonZeroUsize::MIN, within);
    let push = SourceEvent::Push(Push::Json(Bytes::from_owner(noted("text", &order))));
    let charge = Box::new(noted("charge", &order)) as Permit;
    let refused = sink.send_charged(push, charge).await.unwrap_err();
    assert_eq!(refused.kind(), ConnectorErrorKind::Internal);
    assert_eq!(
        *order.lock().unwrap(),
        ["admitted within", "text", "charge"]
    );
    drop(sink);
    assert!(feed.recv().await.is_none(), "nothing entered");
}

#[tokio::test]
async fn any_other_charged_event_lets_its_charge_go_before_it_is_admitted() {
    let order = Arc::new(Mutex::new(Vec::new()));
    let within = Arc::new(Within {
        refuse: false,
        order: Arc::clone(&order),
    });
    let (mut sink, mut feed) = admitted_partition_channel(NonZeroUsize::new(4).unwrap(), within);
    let checkpoint = SourceEvent::Checkpoint {
        cursor: Cursor::new(1, b"c").unwrap(),
        answers: None,
    };
    for event in [checkpoint, SourceEvent::Replan] {
        order.lock().unwrap().clear();
        let charge = Box::new(noted("charge", &order)) as Permit;
        sink.send_charged(event.clone(), charge).await.unwrap();
        assert_eq!(*order.lock().unwrap(), ["charge", "admitted"], "{event:?}");
        assert_eq!(feed.recv().await, Some(event));
    }
}

#[tokio::test]
async fn a_channel_that_admits_nothing_lets_a_charge_go_and_carries_no_permit() {
    let order = Arc::new(Mutex::new(Vec::new()));
    let (mut sink, mut feed) = partition_channel(NonZeroUsize::MIN);
    let charge = Box::new(noted("charge", &order)) as Permit;
    sink.send_charged(json("[1]"), charge).await.unwrap();
    assert_eq!(*order.lock().unwrap(), ["charge"]);
    let (event, permit) = feed.recv_admitted().await.unwrap();
    assert_eq!(event, json("[1]"));
    assert!(permit.is_none());
}
