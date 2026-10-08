use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::Semaphore;

use super::{Bounded, Passed, bounds, fed, message, poll};
use crate::bounded::{Charge, Charging};
use crate::limits::Class;

/// Charges each message's bytes once one of its permits is given, recording each charge and
/// whether the charge before it was still held when it was asked for.
struct Gate {
    permits: Arc<Semaphore>,
    asked: Mutex<Vec<(Class, usize, bool)>>,
    /// How many of its charges are held now.
    holding: Arc<AtomicUsize>,
    refuse: bool,
}

/// One of a gate's charges, held until it is dropped.
struct Hold {
    _permit: tokio::sync::OwnedSemaphorePermit,
    holding: Arc<AtomicUsize>,
}

impl Drop for Hold {
    fn drop(&mut self) {
        self.holding.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Gate {
    /// A gate with no permits, refusing every charge where `refuse`.
    fn new(refuse: bool) -> Arc<Self> {
        Arc::new(Self {
            permits: Arc::new(Semaphore::new(0)),
            asked: Mutex::default(),
            holding: Arc::default(),
            refuse,
        })
    }
}

impl Gate {
    /// Whether any of its charges is held.
    fn holds(&self) -> bool {
        self.holding.load(Ordering::SeqCst) > 0
    }
}

impl Charge for Gate {
    fn charge(&self, class: Class, bytes: usize) -> Charging {
        let held_before = self.holds();
        self.asked.lock().unwrap().push((class, bytes, held_before));
        let (permits, refuse) = (Arc::clone(&self.permits), self.refuse);
        let holding = Arc::clone(&self.holding);
        Box::pin(async move {
            if refuse {
                return Err(tonic::Status::resource_exhausted("no room"));
            }
            let permit = permits.acquire_owned().await.unwrap();
            holding.fetch_add(1, Ordering::SeqCst);
            let hold = Hold {
                _permit: permit,
                holding,
            };
            Ok(Box::new(hold) as crate::bounded::Held)
        })
    }
}

#[test]
fn a_message_is_passed_on_once_what_its_scan_counts_is_charged_and_held_until_decoded() {
    let gate = Gate::new(false);
    let (first, second) = (message(&[0x0a, 0x00]), message(&[0x0a, 0x00, 0x0a, 0x00]));
    let (feed, body) = fed();
    feed.send(http_body::Frame::data(
        [first.clone(), second.clone()].concat().into(),
    ))
    .unwrap();
    let mut body = Bounded::new(body, bounds(1024, 1 << 20), None)
        .charged(Some(Arc::clone(&gate) as Arc<dyn Charge>));
    assert_eq!(
        poll(&mut body),
        Passed::Waits,
        "not passed on before it is charged"
    );
    gate.permits.add_permits(1);
    assert_eq!(poll(&mut body), Passed::Data(first));
    assert!(
        gate.holds(),
        "the charge is held while the message is decoded"
    );
    gate.permits.add_permits(1);
    assert_eq!(poll(&mut body), Passed::Data(second));
    let one = size_of::<crate::v1::Catalog>() + 4 * size_of::<crate::v1::StreamSpec>();
    let asked = gate.asked.lock().unwrap().clone();
    assert_eq!(
        asked,
        [
            (Class::Catalog, one, false),
            (
                Class::Catalog,
                one + 4 * size_of::<crate::v1::StreamSpec>(),
                false
            )
        ],
        "each at its scan's count, the one before released first"
    );
    body.release();
    assert!(!gate.holds());
    gate.permits.add_permits(1);
    drop(feed);
    assert_eq!(poll(&mut body), Passed::End);
    drop(body);
    assert_eq!(
        gate.permits.available_permits(),
        3,
        "every charge is given back"
    );
}

#[test]
fn an_answer_s_charge_is_given_back_as_it_ends() {
    let gate = Gate::new(false);
    gate.permits.add_permits(1);
    let (feed, body) = fed();
    feed.send(http_body::Frame::data(message(&[0x0a, 0x00]).into()))
        .unwrap();
    let mut body = Bounded::new(body, bounds(1024, 1 << 20), None)
        .charged(Some(Arc::clone(&gate) as Arc<dyn Charge>));
    assert!(matches!(poll(&mut body), Passed::Data(_)));
    assert_eq!(gate.permits.available_permits(), 0);
    drop(body);
    assert_eq!(gate.permits.available_permits(), 1);
}

#[test]
fn a_message_whose_charge_is_refused_fails_the_call() {
    let gate = Gate::new(true);
    let (feed, body) = fed();
    feed.send(http_body::Frame::data(message(&[0x0a, 0x00]).into()))
        .unwrap();
    let mut body =
        Bounded::new(body, bounds(1024, 1 << 20), None).charged(Some(gate as Arc<dyn Charge>));
    assert_eq!(
        poll(&mut body),
        Passed::Failed(tonic::Code::ResourceExhausted)
    );
}
