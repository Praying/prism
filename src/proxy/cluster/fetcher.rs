use crate::com::AsError;
use crate::protocol::redis::resp::{nodes_reply_to_layout, Message};
use crate::proxy::cluster::Cluster;
use futures::channel::mpsc::{self, Receiver, Sender};
use futures::StreamExt;
use lazy_static::lazy_static;
use rand::{thread_rng, Rng};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub enum TriggerBy {
    Interval,
    Moved,
    Error,
    Connect(String),
}

pub(crate) type TriggerSender = Sender<TriggerBy>;
pub(crate) type TriggerReceiver = Receiver<TriggerBy>;

pub fn trigger_channel() -> (TriggerSender, TriggerReceiver) {
    mpsc::channel(128)
}

pub struct SingleFlightTrigger {
    ticker: Duration,
    latest: Mutex<Instant>,
    counter: Mutex<u32>,
    fetch: Mutex<TriggerSender>,
}

lazy_static! {
    static ref GAPS: HashSet<u32> = {
        let mut set = HashSet::new();
        for i in 4..=16 {
            set.insert(2u32.pow(i));
        }
        set
    };
}

impl SingleFlightTrigger {
    pub fn new(interval: u64, fetch: TriggerSender) -> Self {
        SingleFlightTrigger {
            ticker: Duration::from_secs(interval),
            latest: Mutex::new(Instant::now()),
            counter: Mutex::new(0),
            fetch: Mutex::new(fetch),
        }
    }

    pub fn try_trigger(&self) -> bool {
        if self.incr_counter() || self.latest.lock().unwrap().elapsed() > self.ticker {
            self.trigger();
            true
        } else {
            false
        }
    }

    pub fn ensure_trgger(&self) {
        self.trigger();
    }

    fn trigger(&self) {
        let mut fetch = self.fetch.lock().unwrap();
        if fetch.try_send(TriggerBy::Error).is_ok() {
            *self.latest.lock().unwrap() = Instant::now();
            *self.counter.lock().unwrap() = 0;
            tracing::info!("succeed trigger fetch process");
            return;
        }
        tracing::warn!("fail to trigger fetch process due fetch channel is full or closed.");
    }

    fn incr_counter(&self) -> bool {
        let mut counter = self.counter.lock().unwrap();
        *counter = counter.wrapping_add(1);
        if Self::check_gap(*counter & 0x00_00f_fff) {
            return true;
        }
        false
    }

    fn check_gap(left: u32) -> bool {
        GAPS.contains(&left)
    }
}


pub struct Fetch {
    cluster: Arc<Cluster>,
}

impl Fetch {
    pub fn new(cluster: Arc<Cluster>) -> Fetch {
        Fetch { cluster }
    }

    pub async fn run(&mut self, mut trigger: TriggerReceiver) {
        tracing::info!("fetcher started");
        while let Some(_trigger_by) = trigger.next().await {
            // The fetch is now done in the main cluster run loop.
            // This loop is now only for periodic updates.
            // let addr = match self.get_random_addr() {
            //     Some(addr) => addr,
            //     None => {
            //         error!("failed to get random addr");
            //         continue;
            //     }
            // };
            // let resp = match request_one(&addr).await {
            //     Ok(resp) => resp,
            //     Err(err) => {
            //         error!("failed to fetch cluster info due to: {}", err);
            //         continue;
            //     }
            // };
            // if let Err(err) = self.update_from_message(&resp) {
            //     error!("failed to update cluster info due to: {}", err);
            // }
        }
        tracing::info!("fetcher stopped");
    }

    pub(crate) fn update_from_message(&self, resp: &Message) -> Result<(), AsError> {
        let layout = match nodes_reply_to_layout(resp) {
            Ok(Some(layout)) => layout,
            Ok(None) => {
                tracing::warn!("slots not full covered, this may be not allow in aster");
                return Ok(());
            }
            Err(err) => {
                tracing::warn!("fail to parse cmd reply due to {}", err);
                return Err(AsError::ProxyFail(
                    "fail to parse cmd reply".to_string(),
                ));
            }
        };

        if self.cluster.try_update_all_slots(layout.into()) {
            tracing::info!("succeed to update cluster slots table");
        } else {
            tracing::debug!("unable to change cluster slots table, this may not be an error");
        }
        Ok(())
    }

    fn get_random_addr(&mut self) -> Option<String> {
        let servers = self.cluster.cc.lock().unwrap().servers.clone();
        if servers.is_empty() {
            return None;
        }
        let mut rng = thread_rng();
        let position = rng.gen_range(0..servers.len());
        servers.get(position).cloned()
    }
}
