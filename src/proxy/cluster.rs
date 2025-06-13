pub mod back;
pub mod fetcher;
pub mod front;
pub mod redirect;
pub use redirect::{Redirect, Redirection};

use crate::com::{AsError, ClusterConfig};
use crate::protocol::redis::{
    cmd::{Cmd},
    resp::{self, ClusterLayout},
};
use crate::proxy::standalone::{fnv, Request};
use crate::utils::thread::spawn;
use bytes::BytesMut;
use fetcher::Fetch;
use futures::channel::mpsc;
use futures::{SinkExt, StreamExt, TryFutureExt};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::time::{sleep, Duration};
use tracing::{error, info, warn};

// Define a command channel for removing dead connections
enum ClusterCmd {
    Remove(String),
}

pub struct Cluster {
    pub cc: Arc<Mutex<ClusterConfig>>,
    pub slots: Arc<Mutex<Vec<String>>>,
    pub conns: Arc<Mutex<HashMap<String, mpsc::Sender<Cmd>>>>,
    trigger_tx: mpsc::Sender<fetcher::TriggerBy>,
    cmd_tx: mpsc::Sender<ClusterCmd>,
}

impl Cluster {
    pub fn new(
        cc: ClusterConfig, trigger_tx: mpsc::Sender<fetcher::TriggerBy>,
        cmd_tx: mpsc::Sender<ClusterCmd>,
    ) -> Self {
        let mut slots = Vec::with_capacity(16384);
        for _ in 0..16384 {
            slots.push(String::new());
        }

        Self {
            cc: Arc::new(Mutex::new(cc)),
            slots: Arc::new(Mutex::new(slots)),
            conns: Arc::new(Mutex::new(HashMap::new())),
            trigger_tx,
            cmd_tx,
        }
    }

    async fn request_one(&self, addr: &str) -> Result<resp::Message, AsError> {
        let mut stream = tokio::net::TcpStream::connect(addr).await?;

        let password = self.cc.lock().unwrap().redis_auth.clone();
        if let Some(password) = password {
            let auth_cmd = resp::Message::auth(&password);
            stream.write_all(&auth_cmd.data).await?;
            let mut buf = BytesMut::with_capacity(128);
            let _ = stream.read_buf(&mut buf).await?;
            let resp = resp::Message::parse(&mut buf)?.ok_or(AsError::BadMessage)?;
            if resp.is_error() {
                return Err(AsError::BadAuth);
            }
        }

        let cmd = resp::Message::new_cluster_nodes();
        stream.write_all(&cmd.data).await?;

        let mut buf = BytesMut::with_capacity(8192);
        let _ = stream.read_buf(&mut buf).await?;

        let resp = resp::Message::parse(&mut buf)?.ok_or(AsError::BadMessage)?;
        Ok(resp)
    }

    pub fn update_slot(&self, slot: usize, to: String) -> bool {
        let mut slots = self.slots.lock().unwrap();
        if slots[slot] != to {
            slots[slot] = to;
            return true;
        }
        false
    }

    pub fn trigger_fetch(&self, trigger_by: fetcher::TriggerBy) {
        if let Err(err) = self.trigger_tx.clone().try_send(trigger_by) {
            error!("fail to trigger fetch due to {}", err);
        }
    }

    pub async fn dispatch(&self, cmd: Cmd) -> Result<(), AsError> {
        let mut redirection = Redirection::new(cmd);
        let mut retries = 0;

        loop {
            if retries >= MAX_RETRIES {
                let err = AsError::RequestReachMaxCycle;
                return redirection.return_err(err).await;
            }
            retries += 1;

            let (addr, ask) = if let Some((addr, ask)) = redirection.take_redirect_addr() {
                (addr, ask)
            } else {
                let cmd_for_hash = redirection.get_cmd();
                let key = crate::proxy::standalone::Request::key_hash(
                    &cmd_for_hash, // use the clone
                    self.cc
                        .lock()
                        .unwrap()
                        .hash_tag
                        .as_ref()
                        .unwrap()
                        .as_bytes(),
                    crate::utils::crc::crc16,
                );
                let slot = key as usize & (16384 - 1);
                (self.slots.lock().unwrap()[slot].clone(), false)
            };

            let mut cmd_to_send = redirection.get_cmd();

            if ask {
                let mut asking_cmd = Cmd::new_asking();
                let (tx, rx) = tokio::sync::oneshot::channel();
                asking_cmd.set_reply_sender(tx);

                self.dispatch_to(&addr, asking_cmd).await?;
                let _ = rx.await;
            }

            let (tx, rx) = tokio::sync::oneshot::channel();
            cmd_to_send.set_reply_sender(tx);

            if let Err(err) = self.dispatch_to(&addr, cmd_to_send).await {
                return redirection.return_err(err).await;
            }

            match rx.await {
                Ok(resp) => {
                    if resp.is_error() {
                        let rstr = String::from_utf8_lossy(&resp.data);
                        if let Some(redirect) = Redirect::parse_from(&rstr) {
                            self.trigger_fetch(fetcher::TriggerBy::Moved);
                            redirection.set_redirect(redirect);
                            continue;
                        }
                    }
                    return redirection.return_ok(resp).await;
                }
                Err(_e) => {
                    return redirection.return_err(AsError::Canceled).await;
                }
            }
        }
    }

    pub async fn dispatch_to(&self, to: &str, cmd: Cmd) -> Result<(), AsError> {
        let sender = {
            let conns = self.conns.lock().unwrap();
            conns.get(to).cloned()
        };

        if let Some(mut tx) = sender {
            return tx.send(cmd).map_err(|_e| AsError::Canceled).await;
        }

        warn!("connection to {} not found, maybe need to fetch", to);
        self.trigger_fetch(fetcher::TriggerBy::Connect(to.to_string()));
        Err(AsError::TryAgain)
    }

    pub fn is_ready(&self) -> bool {
        !self.slots.lock().unwrap().iter().all(String::is_empty)
    }

    pub fn try_update_all_slots(&self, layout: ClusterLayout) -> bool {
        let mut new_masters = HashSet::new();
        let mut changed = false;
        let mut new_conns = Vec::new();

        for replica in &layout.replicas {
            let addr = String::from_utf8_lossy(&replica.master).to_string();
            new_masters.insert(addr.clone());

            let should_add = { !self.conns.lock().unwrap().contains_key(&addr) };

            if should_add {
                let (tx, rx) = mpsc::channel(1024);
                let cc = self.cc.lock().unwrap().clone();
                let addr_for_task = addr.clone();
                let mut cmd_tx_clone = self.cmd_tx.clone();
                let fut = async move {
                    if let Err(err) = back::run(Arc::new(cc), addr_for_task.clone(), rx).await {
                        warn!(
                            "fail to run back task for master {} by {}",
                            addr_for_task,
                            err.to_string()
                        );
                    }
                    // When back task exits, send a command to remove it from the pool.
                    if let Err(e) = cmd_tx_clone.send(ClusterCmd::Remove(addr_for_task)).await {
                        error!("failed to send remove command for dead connection: {}", e);
                    }
                };
                spawn(fut);
                new_conns.push((addr, tx));
                changed = true;
            }
        }

        let mut conns = self.conns.lock().unwrap();
        for (addr, tx) in new_conns {
            conns.insert(addr, tx);
        }

        conns.retain(|addr, _| new_masters.contains(addr));

        let mut slots = self.slots.lock().unwrap();
        for i in 0..layout.slots.len() {
            let replica_index = layout.slots[i] as usize;
            if replica_index >= layout.replicas.len() {
                error!("replica index out of bound");
                continue;
            }
            let replica = &layout.replicas[replica_index];
            let addr = String::from_utf8_lossy(&replica.master).to_string();
            if slots[i] != addr {
                slots[i] = addr;
                changed = true;
            }
        }
        changed
    }

    fn remove_conn(&self, addr: &str) {
        if self.conns.lock().unwrap().remove(addr).is_some() {
            info!("Removed dead connection for {}", addr);
        }
    }
}

const RETRY_DELAY: Duration = Duration::from_secs(1);
const MAX_RETRIES: usize = 5;

pub async fn run(cc: ClusterConfig) -> Result<(), AsError> {
    let listen_addr = cc.listen_addr.clone();

    let (trigger_tx, trigger_rx) = fetcher::trigger_channel();
    let (cmd_tx, mut cmd_rx) = mpsc::channel(16);
    let cluster = Arc::new(Cluster::new(cc, trigger_tx, cmd_tx));

    let mut fetcher = Fetch::new(cluster.clone());

    let mut retries = 0;
    while retries < MAX_RETRIES {
        let update_result = async {
            let addrs = cluster.cc.lock().unwrap().servers.clone();
            if addrs.is_empty() {
                return Err(AsError::BadConfig("missing node address".to_string()));
            }
            let resp = cluster.request_one(&addrs[0]).await?;
            fetcher.update_from_message(&resp)
        }
        .await;

        let is_ready = cluster.is_ready();

        if let Err(e) = &update_result {
            error!("failed to update cluster info: {}", e);
        }

        if update_result.is_ok() && is_ready {
            info!("Cluster is ready to serve.");
            break;
        }

        retries += 1;
        if retries >= MAX_RETRIES {
            return Err(AsError::BadConfig(
                "failed to initialize cluster after several retries".to_string(),
            ));
        }

        info!(
            "failed to fetch cluster info, will retry in {} seconds. (attempt {}/{})",
            RETRY_DELAY.as_secs(),
            retries,
            MAX_RETRIES
        );
        sleep(RETRY_DELAY).await;
    }

    // Main command processing loop
    let cluster_clone_for_cmd = cluster.clone();
    spawn(async move {
        while let Some(cmd) = cmd_rx.next().await {
            match cmd {
                ClusterCmd::Remove(addr) => {
                    cluster_clone_for_cmd.remove_conn(&addr);
                }
            }
        }
    });

    spawn(async move {
        fetcher.run(trigger_rx).await;
        info!("fetcher stopped");
    });

    let listen = TcpListener::bind(&listen_addr).await?;
    info!("start listening on {}", listen_addr);
    loop {
        let (stream, addr) = listen.accept().await?;
        info!("accept a new connection from {}", addr);

        let cluster_clone = cluster.clone();
        let fut = async move {
            if let Err(err) = front::run(stream, cluster_clone).await {
                error!("front codec failed with err {}", err);
            }
        };

        spawn(fut);
    }
}
