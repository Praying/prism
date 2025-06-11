use tokio::sync::mpsc::{self, Receiver, Sender};
use rand::{thread_rng, Rng};
use lazy_static::lazy_static;
use tracing::{debug, info, trace};

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

// use crate::protocol::redis::{new_cluster_slots_cmd, slots_reply_to_replicas};
use crate::proxy::cluster::Cluster;

#[derive(Debug, Clone)]
pub enum TriggerBy {
    Interval,
    Moved,
    Error,
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
        let fetch = self.fetch.lock().unwrap();
        if fetch.try_send(TriggerBy::Error).is_ok() {
            *self.latest.lock().unwrap() = Instant::now();
            *self.counter.lock().unwrap() = 0;
            info!("succeed trigger fetch process");
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
        Fetch {
            cluster,
        }
    }

    pub async fn run(&mut self, mut trigger: TriggerReceiver) {
        info!("fetcher started");
        while let Some(trigger_by) = trigger.recv().await {
             match trigger_by {
                TriggerBy::Interval => {
                    trace!("fetcher trigger by interval");
                }
                TriggerBy::Error => {
                    debug!("fetcher trigger by proxy error");
                }
                TriggerBy::Moved => {
                    debug!("fetcher trigger by moved");
                }
            }
            self.fetch().await;
        }
        info!("fetcher stopped");
    }

    async fn fetch(&mut self) {
        // let addr = match self.get_random_addr() {
        //     Some(addr) => addr,
        //     None => return,
        // };

        // tracing::info!("start fetch from remote address {}", addr);
        // let cmd = new_cluster_slots_cmd();

        // if let Err(err) = self.cluster.dispatch_to(&addr, cmd.clone()).await {
        //     error!("fail to fetch CLUSTER SLOTS from {} due to {}", addr, err);
        //     return;
        // }

        // let layout = match slots_reply_to_replicas(&cmd) {
        //     Ok(Some(layout)) => layout,
        //     Ok(None) => {
        //         tracing::warn!("slots not full covered, this may be not allow in aster");
        //         return;
        //     }
        //     Err(err) => {
        //         tracing::warn!("fail to parse cmd reply from {} due to {}", addr, err);
        //         return;
        //     }
        // };

        // if self.cluster.try_update_all_slots(layout) {
        //     tracing::info!("succeed to update cluster slots table by {}", addr);
        // } else {
        //     tracing::debug!("unable to change cluster slots table, this may not be an error");
        // }
    }

    fn get_random_addr(&mut self) -> Option<String> {
        let mut rng = thread_rng();
        let cc = self.cluster.cc.lock().unwrap();
        if cc.servers.is_empty() {
            return None;
        }
        let position = rng.gen_range(0..cc.servers.len());
        cc.servers.get(position).cloned()
    }
}
