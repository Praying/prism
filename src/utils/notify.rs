use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Waker;

#[derive(Clone, Debug)]
pub struct Notify {
    shared: Arc<NotifyShared>,
}

impl Notify {
    pub fn new() -> Self {
        Notify {
            shared: Arc::new(NotifyShared {
                waker: Mutex::new(None),
                count: AtomicU16::new(1),
                expect: AtomicU16::new(std::u16::MAX),
            }),
        }
    }

    pub fn register(&self, waker: &Waker) {
        let mut w = self.shared.waker.lock().unwrap();
        if w.as_ref().map_or(true, |w2| !w2.will_wake(waker)) {
            *w = Some(waker.clone());
        }
    }

    pub fn notify(&self) {
        if let Some(waker) = self.shared.waker.lock().unwrap().as_ref() {
            waker.wake_by_ref();
        }
    }

    pub fn set_expect(&self, expect: u16) {
        self.shared.expect.store(expect, Ordering::SeqCst);
    }

    pub fn expect(&self) -> u16 {
        self.shared.expect.load(Ordering::SeqCst)
    }

    pub fn fetch_sub(&self, val: u16) -> u16 {
        self.shared.count.fetch_sub(val, Ordering::SeqCst)
    }

    pub fn fetch_add(&self, val: u16) -> u16 {
        self.shared.count.fetch_add(val, Ordering::SeqCst)
    }
}

impl Default for Notify {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
struct NotifyShared {
    waker: Mutex<Option<Waker>>,
    count: AtomicU16,
    expect: AtomicU16,
}
