pub mod back;
pub mod fetcher;
pub mod front;
pub mod init;
pub mod redirect;

use crate::com::create_reuse_port_listener;
use crate::com::{gethostbyname, AsError};
use crate::com::{ClusterConfig, CODE_PORT_IN_USE};
use crate::protocol::redis::{Cmd, Message, BackCodec};
use crate::proxy::standalone::Request;
// use crate::protocol::redis::{Cmd, RedisHandleCodec, RedisNodeCodec, SLOTS_COUNT};
const SLOTS_COUNT: usize = 16384;
use crate::proxy::cluster::fetcher::SingleFlightTrigger;
use crate::utils::crc::crc16;

use crate::metrics::front_conn_incr;

pub type ReplicaLayout = (Vec<String>, Vec<Vec<String>>);

// use failure::Error;
use futures::{future::{self}, FutureExt, SinkExt, StreamExt};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::time::{interval, timeout, Duration};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::codec::Framed;

use tokio::net::TcpStream;
use tracing::info;

use std::cell::Cell;
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::process;
use std::sync::{Arc, Mutex, Weak};
use std::time::Instant;

const MAX_NODE_PIPELINE_SIZE: usize = 16 * 1024; // 16k

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Redirect {
    Move { slot: usize, to: String },
    Ask { slot: usize, to: String },
}

impl Redirect {
    pub(crate) fn is_ask(&self) -> bool {
        match self {
            Redirect::Ask { .. } => true,
            _ => false,
        }
    }
}

#[derive(Clone)]
pub struct Redirection {
    pub target: Redirect,
    pub cmd: Cmd,
}

impl Redirection {
    pub(crate) fn new(is_move: bool, slot: usize, to: String, cmd: Cmd) -> Redirection {
        if is_move {
            Redirection {
                target: Redirect::Move { slot, to },
                cmd,
            }
        } else {
            Redirection {
                target: Redirect::Ask { slot, to },
                cmd,
            }
        }
    }
}

pub use Redirect::{Ask, Move};

pub struct RedirectFuture {}

pub struct Cluster {
    pub cc: Mutex<ClusterConfig>,

    slots: Mutex<Slots>,
    conns: Mutex<Conns>,
    hash_tag: Vec<u8>,
    read_from_slave: bool,

    moved: Sender<Redirection>,
    fetch: Mutex<Option<Arc<SingleFlightTrigger>>>,
    latest: Mutex<Instant>,
}

pub type MovedRecv = Receiver<Redirection>;

impl Cluster {
    async fn setup_fetch_trigger(self: &Arc<Self>) {
        let interval_millis = self.cc.lock().unwrap().fetch_interval_ms();
        let mut interval = interval(Duration::from_millis(interval_millis));
        interval.tick().await; // consume the first tick

        let (tx, rx) = fetcher::trigger_channel();
        let trigger = Arc::new(SingleFlightTrigger::new(1, tx.clone()));
        let _ = self.fetch.lock().unwrap().replace(trigger);

        let mut fetcher = fetcher::Fetch::new(self.clone());
        tokio::spawn(async move {
            fetcher.run(rx).await;
        });

        let interval_tx = tx;
        tokio::spawn(async move {
            let mut interval_timer = interval;
            loop {
                interval_timer.tick().await;
                if let Err(err) = interval_tx.send(fetcher::TriggerBy::Interval).await {
                    tracing::error!("failed to send interval trigger, fetcher may be stopped: {}", err);
                    break;
                }
            }
        });
    }

    async fn handle_front_conn(self: &Arc<Self>, sock: TcpStream) {
        let cluster_name = self.cc.lock().unwrap().name.clone();
        sock.set_nodelay(true).unwrap_or_else(|err| {
            tracing::warn!(
                "cluster {} fail to set nodelay but skip, due to {:?}",
                cluster_name, err
            );
        });

        let client_str = match sock.peer_addr() {
            Ok(client) => client.to_string(),
            Err(err) => {
                tracing::error!(
                    "cluster {} fail to get client name err {:?}",
                    cluster_name, err
                );
                "unknown".to_string()
            }
        };

        front_conn_incr(&cluster_name);
        // let codec = RedisHandleCodec {};
        // let (output, input) = codec.framed(sock).split();
        // let front = front::Front::new(client_str, self.clone(), input, output);
        // tokio::spawn(front.run());
        unimplemented!();
    }
}

impl Cluster {
    async fn _run(self: Arc<Self>, moved_rx: MovedRecv) -> Result<(), AsError> {
        let mut redirect_handler = redirect::RedirectHandler::new(self.clone(), moved_rx);
        tokio::spawn(async move { redirect_handler.run().await });

        self.setup_fetch_trigger().await;
        
        let addr = self.cc.lock().unwrap().listen_addr.parse::<SocketAddr>()
            .expect("parse socket never fail");

        let listen = match create_reuse_port_listener(&addr) {
            Ok(v) => v,
            Err(e) => {
                tracing::error!("listen {} error: {}", addr, e);
                process::exit(CODE_PORT_IN_USE);
            }
        };
        let listener = tokio::net::TcpListener::from_std(listen)?;

        loop {
            match listener.accept().await {
                Ok((sock, _)) => {
                    let cluster = self.clone();
                    tokio::spawn(async move {
                        cluster.handle_front_conn(sock).await;
                    });
                }
                Err(e) => {
                    tracing::error!("fail to accept incoming sock due to {}", e);
                }
            }
        }
    }
}

pub async fn run(cc: ClusterConfig) -> Result<(), AsError> {
    let mut initializer = init::Initializer::new(cc);
    initializer.run().await
}

impl Cluster {
    fn get_addr(&self, slot: usize, is_read: bool) -> String {
        // trace!("get slot={} and is_read={}", slot, is_read);
        if self.read_from_slave && is_read {
            if let Some(replica) = self.slots.lock().unwrap().get_replica(slot) {
                if replica != "" {
                    return replica.to_string();
                }
            }
        }
        self.slots
            .lock()
            .unwrap()
            .get_master(slot)
            .map(|x| x.to_string())
            .expect("master addr never be empty")
    }

    pub fn trigger_fetch(&self, trigger_by: fetcher::TriggerBy) {
        if let Some(trigger) = self.fetch.lock().unwrap().clone() {
            let if_triggered = match trigger_by {
                fetcher::TriggerBy::Moved => {
                    trigger.try_trigger();
                    true
                }
                _ => trigger.try_trigger(),
            };

            if if_triggered {
                info!("succeed trigger fetch process by {:?}", trigger_by);
                return;
            }
        } else {
            tracing::warn!("fail to trigger fetch process due to trigger event uninitialed");
        }
    }

    pub async fn dispatch_to(&self, addr: &str, cmd: Cmd) -> Result<(), AsError> {
        if !cmd.can_cycle() {
            cmd.set_error(&AsError::ClusterFailDispatch);
            return Ok(());
        }

        for i in 0..2 {
            let sender = {
                let conns = self.conns.lock().unwrap();
                conns.get(addr).map(|x| x.sender())
            };

            if let Some(sender) = sender {
                if sender.send(Ok(cmd.clone())).await.is_ok() {
                    return Ok(());
                }
            }

            if i == 0 {
                tracing::warn!("fail to send to backend, maybe connect is broken {}, try to reconnect", addr);
                let sender = self.connect(addr).await?;
                let mut conns = self.conns.lock().unwrap();
                conns.insert(addr, sender);
            }
        }

        Err(AsError::BackendClosedError(addr.to_string()))
    }

    async fn inner_dispatch_all(&self, cmds: &mut VecDeque<Cmd>) -> Result<usize, AsError> {
        let mut count = 0usize;
        loop {
            if cmds.is_empty() {
                return Ok(count);
            }
            let cmd = cmds.pop_front().expect("cmds pop front never be empty");
            if !cmd.can_cycle() {
                cmd.set_error(&AsError::ProxyFail);
                continue;
            }
            let slot = {
                let hash_tag = self.hash_tag.as_ref();
                let signed = cmd.key_hash(hash_tag, crc16);
                if signed == std::u64::MAX {
                    cmd.set_error(&AsError::BadRequest);
                    continue;
                }

                (signed as usize) % SLOTS_COUNT
            };

            let addr = self.get_addr(slot, cmd.ctype().is_read());
            let mut conns = self.conns.lock().unwrap();

            if let Some(sender) = conns.get_mut(&addr).map(|x| x.sender()) {
                match sender.try_send(Ok(cmd)) {
                    Ok(_) => {
                        // trace!("success start command into backend");
                        count += 1;
                    }
                    Err(tokio::sync::mpsc::error::TrySendError::Full(Ok(cmd))) => {
                        cmd.add_cycle();
                        cmds.push_front(cmd);
                        return Ok(count);
                    }
                    Err(tokio::sync::mpsc::error::TrySendError::Closed(Ok(cmd))) => {
                        cmd.add_cycle();
                        cmds.push_front(cmd);
                        let sender = self.connect(&addr).await?;
                        conns.insert(addr.as_str(), sender);
                        return Ok(count);
                    }
                    _ => unreachable!(),
                }
            } else {
                cmds.push_front(cmd);
                let sender = self.connect(&addr).await?;
                conns.insert(addr.as_str(), sender);
                return Ok(count);
            }
        }
    }

    pub async fn dispatch_all(&self, cmds: &mut VecDeque<Cmd>) -> Result<usize, AsError> {
        let count = self.inner_dispatch_all(cmds).await?;
        if count != 0 {
            *self.latest.lock().unwrap() = Instant::now();
        }
        Ok(count)
    }

    #[allow(unused)]
    pub(crate) fn since_latest(&self) -> Duration {
        self.latest.lock().unwrap().elapsed()
    }

    pub(crate) fn try_update_all_slots(&self, layout: ReplicaLayout) -> bool {
        let (masters, replicas) = layout;
        let updated = self
            .slots
            .lock()
            .unwrap()
            .try_update_all(masters.clone(), replicas);
        if updated {
            let handle = &mut self.cc.lock().unwrap();
            handle.servers.clear();
            handle.servers.extend_from_slice(&masters);
        }
        updated
    }

    pub(crate) fn update_slot(&self, slot: usize, addr: String) -> bool {
        debug_assert!(slot <= SLOTS_COUNT);
        self.slots.lock().unwrap().update_slot(slot, addr)
    }

    pub(crate) async fn connect(&self, addr: &str) -> Result<Sender<Result<Cmd, ()>>, AsError> {
        let is_replica = !self.slots.lock().unwrap().is_master(addr);

        let (cluster_name, read_timeout, write_timeout, fetch) = {
            let cc = self.cc.lock().unwrap();
            let fetch = self.fetch.lock().unwrap();
            (
                cc.name.clone(),
                cc.read_timeout.clone(),
                cc.write_timeout.clone(),
                fetch.as_ref().map(|x| Arc::downgrade(x)).unwrap_or_default(),
            )
        };

        ConnBuilder::new()
            .moved(self.moved.clone())
            .cluster(cluster_name)
            .node(addr.to_string())
            .read_timeout(read_timeout)
            .write_timeout(write_timeout)
            .fetch(fetch)
            .replica(is_replica)
            .connect()
            .await
    }
}

pub(crate) struct Conns {
    inner: HashMap<String, Conn<Sender<Result<Cmd, ()>>>>,
}

impl Conns {
    fn get(&self, s: &str) -> Option<&Conn<Sender<Result<Cmd, ()>>>> {
        self.inner.get(s)
    }

    fn get_mut(&mut self, s: &str) -> Option<&mut Conn<Sender<Result<Cmd, ()>>>> {
        self.inner.get_mut(s)
    }

    fn insert(&mut self, s: &str, sender: Sender<Result<Cmd, ()>>) {
        let conn = Conn {
            addr: s.to_string(),
            sender,
        };
        self.inner.insert(s.to_string(), conn);
    }
}

impl Default for Conns {
    fn default() -> Conns {
        Conns {
            inner: HashMap::new(),
        }
    }
}

#[allow(unused)]
struct Conn<S> {
    addr: String,
    sender: S,
}

impl<S> Conn<S> {
    fn sender(&self) -> S where S: Clone {
        self.sender.clone()
    }
}

struct Slots {
    masters: Vec<String>,
    replicas: Vec<Replica>,

    all_masters: HashSet<String>,
    all_replicas: HashSet<String>,
}

impl Slots {
    fn try_update_all(&mut self, masters: Vec<String>, replicas: Vec<Vec<String>>) -> bool {
        let mut changed = false;
        for (i, master) in masters.iter().enumerate().take(SLOTS_COUNT) {
            if &self.masters[i] != master {
                changed = true;
                self.masters[i] = master.clone();
                self.all_masters.insert(master.clone());
            }
        }

        for (i, replica) in replicas.iter().enumerate().take(SLOTS_COUNT) {
            let len_not_eqal = self.replicas[i].addrs.len() != replica.len();
            if len_not_eqal || self.replicas[i].addrs.as_slice() != replica.as_slice() {
                self.replicas[i] = Replica {
                    addrs: replica.clone(),
                    current: Cell::new(0),
                };
                self.all_replicas.extend(replica.clone().into_iter());
                changed = true;
            }
        }

        changed
    }

    fn update_slot(&mut self, slot: usize, addr: String) -> bool {
        let old = self.masters[slot].clone();
        self.masters[slot] = addr.clone();
        self.all_masters.insert(addr.clone());
        old != addr
    }

    fn get_master(&self, slot: usize) -> Option<&str> {
        self.masters.get(slot).map(|x| x.as_str())
    }

    fn get_replica(&self, slot: usize) -> Option<&str> {
        self.replicas.get(slot).map(|x| x.get_replica())
    }

    pub fn get_all_masters(&self) -> Vec<String> {
        self.all_masters.iter().cloned().collect()
    }

    pub fn get_all_replicas(&self) -> Vec<String> {
        self.all_replicas.iter().cloned().collect()
    }

    pub(crate) fn is_master(&self, addr: &str) -> bool {
        self.all_masters.contains(addr)
    }
}

impl Default for Slots {
    fn default() -> Slots {
        Slots {
            masters: vec!["".to_string(); SLOTS_COUNT],
            replicas: vec![Replica::default(); SLOTS_COUNT],
            all_masters: HashSet::new(),
            all_replicas: HashSet::new(),
        }
    }
}

#[derive(Clone, Debug)]
struct Replica {
    addrs: Vec<String>,
    current: Cell<usize>,
}

impl Replica {
    fn get_replica(&self) -> &str {
        if self.addrs.is_empty() {
            return "";
        }
        let current = self.current.get();
        let next = (current + 1) % self.addrs.len();
        self.current.set(next);
        self.addrs[current].as_str()
    }
}

impl Default for Replica {
    fn default() -> Replica {
        Replica {
            addrs: vec![],
            current: Cell::new(0),
        }
    }
}

fn silence_send_req(cmd: Cmd, tx: &mut Sender<Result<Cmd, ()>>) {
    match tx.try_send(Ok(cmd)) {
        Ok(_) => {}
        Err(tokio::sync::mpsc::error::TrySendError::Closed(Ok(cmd))) => {
            let _ = cmd.get_reply().map(|e| {
                tracing::error!("fail to send cmd for broken channel {:?}", e);
            });
        }
        Err(tokio::sync::mpsc::error::TrySendError::Full(Ok(cmd))) => {
            let _ = cmd.get_reply().map(|e| {
                tracing::error!("fail to send cmd for full channel {:?}", e);
            });
        }
        _ => unreachable!(),
    }
}


pub(crate) struct ConnBuilder {
    cluster: Option<String>,
    node: Option<String>,
    read_timeout: Option<u64>,
    write_timeout: Option<u64>,
    moved: Option<Sender<Redirection>>,
    fetch: Option<Weak<SingleFlightTrigger>>,
    is_replica: bool,
}

impl ConnBuilder {
    pub(crate) fn new() -> ConnBuilder {
        ConnBuilder {
            cluster: None,
            node: None,
            read_timeout: None,
            write_timeout: None,
            moved: None,
            fetch: None,
            is_replica: false,
        }
    }

    pub(crate) fn fetch(mut self, fetch: Weak<SingleFlightTrigger>) -> Self {
        self.fetch = Some(fetch);
        self
    }

    pub(crate) fn cluster(mut self, cluster: String) -> Self {
        self.cluster = Some(cluster);
        self
    }

    pub(crate) fn moved(mut self, moved: Sender<Redirection>) -> Self {
        self.moved = Some(moved);
        self
    }

    pub(crate) fn read_timeout(mut self, rt: Option<u64>) -> Self {
        self.read_timeout = rt;
        self
    }



    pub(crate) fn write_timeout(mut self, wt: Option<u64>) -> Self {
        self.write_timeout = wt;
        self
    }

    pub(crate) fn node(mut self, node: String) -> Self {
        self.node = Some(node);
        self
    }

    pub(crate) fn replica(mut self, is_replica: bool) -> Self {
        self.is_replica = is_replica;
        self
    }

    // connect to one backend
    pub(crate) async fn connect(self) -> Result<Sender<Result<Cmd, ()>>, AsError> {
        let node_addr = self.node.expect("node must be some");
        let cluster = self.cluster.expect("cluster must be some");
        let moved = self.moved.expect("moved must be some");
        let rt = self.read_timeout;
        let wt = self.write_timeout;
        let is_replica = self.is_replica;

        let addr = gethostbyname(&node_addr).map_err(|e| AsError::BadHost(e.to_string()))?;

        let fut = TcpStream::connect(addr);
        let stream = match timeout(Duration::from_millis(100), fut).await {
            Ok(Ok(stream)) => stream,
            Ok(Err(err)) => {
                tracing::error!(
                    "fail to connect to {} by given name {} reason {}",
                    addr, node_addr, err
                );
                return Err(AsError::IoError(err));
            }
            Err(err) => {
                tracing::error!(
                    "fail to connect to {} by given name {} reason {}",
                    addr, node_addr, err
                );
                return Err(AsError::IoError(tokio::io::Error::new(
                    tokio::io::ErrorKind::TimedOut,
                    err,
                )));
            }
        };

        let codec = BackCodec::default();
        let (back_tx, back_rx) = Framed::new(stream, codec).split();

        let (tx, rx) = mpsc::channel(MAX_NODE_PIPELINE_SIZE);
        let input = ReceiverStream::new(rx);
        let output = back_tx.sink_map_err(AsError::from);
        let recv = back_rx.filter_map(|x| future::ready(x.ok()));
        
        if is_replica {
            let cmd = Cmd::from(Message::new_read_only());
            silence_send_req(cmd, &mut tx.clone());
        }

        let backend = back::run(
            cluster,
            node_addr.clone(),
            input,
            output,
            recv,
            moved,
            rt.unwrap_or(1000),
        );
        tokio::spawn(backend);

        let ping_cmd = Cmd::ping_request();
        if let Err(err) = tx.send(Ok(ping_cmd.clone())).await {
            tracing::error!("fail to send cmd to backend {}", err);
            tracing::warn!(
                "fail to init connection of replica due to {:?}",
                ping_cmd.get_reply()
            );
            return Err(AsError::ConnectFail(addr.to_string()));
        }

        if is_replica {
            let cmd = Cmd::from(Message::new_read_only());
            if let Err(err) = tx.send(Ok(cmd.clone())).await {
                let _ = cmd.get_reply().map(|e| tracing::error!("fail to send read only cmd {:?}", e));
                return Err(AsError::ConnectFail(addr.to_string()));
            }
        }
        Ok(tx)
    }
}
