pub mod back;
pub mod fnv;
pub mod front;
pub mod ketama;
pub mod ping;
pub mod reload;

use crate::com::AsError;
use crate::com::{CacheType, ClusterConfig};
use crate::metrics::front_conn_incr;
use crate::protocol::redis::resp::Message;
use crate::protocol::{mc, redis, IntoReply};
use anyhow::Result;
use bytes::BytesMut;
use futures::{future, prelude::*, StreamExt};
use ketama::HashRing;
use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::Waker;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{self, Sender};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::codec::{Decoder, Encoder};
use tracing::{error, info};

pub(crate) enum PingCmd {
    Reconnect(String),
}

pub trait Request: Clone + Send + Sync + 'static {
    type Reply: Clone + IntoReply<Self::Reply> + From<AsError> + Send;
    type ReplySender: Send;

    type FrontCodec: Decoder<Item = Self, Error = AsError>
        + Encoder<Self::Reply, Error = AsError>
        + Default
        + Send
        + 'static;
    type BackCodec: Decoder<Item = Self::Reply, Error = AsError>
        + Encoder<Self, Error = AsError>
        + Default
        + Send
        + 'static;

    fn ping_request() -> Self;

    fn reregister(&mut self, waker: &Waker);

    fn key_hash(&self, hash_tag: &[u8], hasher: fn(&[u8]) -> u64) -> u64;

    fn subs(&self) -> Option<Vec<Self>>;

    fn mark_total(&self, cluster: &str);

    fn mark_remote(&self, cluster: &str);

    fn is_done(&self) -> bool;
    fn is_error(&self) -> bool;

    fn add_cycle(&self);
    fn can_cycle(&self) -> bool;

    fn valid(&self) -> bool;

    fn set_reply(&mut self, t: Self::Reply);
    fn set_error(&mut self, t: AsError);
    fn set_reply_sender(&mut self, sender: Self::ReplySender);

    fn get_sendtime(&self) -> Option<Instant>;
}

pub struct Cluster<T: Request> {
    pub cc: Arc<ClusterConfig>,
    hash_tag: Vec<u8>,
    spots: HashMap<String, usize>,
    alias: HashMap<String, String>,

    _marker: PhantomData<T>,
    ring: HashRing,
    pub conns: Conns<T>,
    pings: HashMap<String, Arc<AtomicBool>>,
}

impl<T: Request + Send + Sync + Unpin + 'static> Cluster<T> {
    fn ping_fail_limit(&self) -> u8 {
        self.cc.ping_fail_limit.as_ref().cloned().unwrap_or(0)
    }

    fn ping_interval(&self) -> u64 {
        self.cc.ping_interval.as_ref().cloned().unwrap_or(300_000)
    }

    fn ping_succ_interval(&self) -> u64 {
        self.cc
            .ping_succ_interval
            .as_ref()
            .cloned()
            .unwrap_or(1_000)
    }
}

pub async fn connect_redis(
    cc: &ClusterConfig, node: &str,
) -> Result<tokio::sync::mpsc::Sender<redis::cmd::Cmd>, AsError> {
    let addr = node.parse::<SocketAddr>()?;
    let mut stream = TcpStream::connect(addr).await?;
    stream.set_nodelay(true)?;

    if let Some(password) = &cc.redis_auth {
        let auth_msg = redis::resp::Message::auth(password);
        stream.write_all(&auth_msg.data).await?;

        let mut buf = BytesMut::with_capacity(256);
        let n = stream.read_buf(&mut buf).await?;
        if n == 0 {
            return Err(AsError::BackendClosedError(
                "redis auth failed with empty reply".to_string(),
            ));
        }

        let msg = match redis::resp::Message::parse(&mut buf)? {
            Some(msg) => msg,
            None => {
                return Err(AsError::BackendClosedError(
                    "redis auth failed with incomplete reply".to_string(),
                ));
            }
        };
        if msg.is_error() {
            return Err(AsError::BadAuth);
        }
    }

    let (tx, rx) = tokio::sync::mpsc::channel(1024 * 8);
    let rx = ReceiverStream::new(rx);

    let codec = <redis::cmd::Cmd as Request>::BackCodec::default();
    let (sink, stream) = codec.framed(stream).split();

    let fut = back::run_redis(node.to_string(), rx, sink, stream);
    tokio::spawn(fut);

    Ok(tx)
}

pub async fn connect_mc(
    _cc: &ClusterConfig, node: &str,
) -> Result<tokio::sync::mpsc::Sender<mc::Cmd>, AsError> {
    let addr = node.parse::<SocketAddr>()?;
    let stream = TcpStream::connect(addr).await?;
    stream.set_nodelay(true)?;

    let (tx, rx) = tokio::sync::mpsc::channel(1024 * 8);
    let rx = ReceiverStream::new(rx);

    let codec = <mc::Cmd as Request>::BackCodec::default();
    let (sink, stream) = codec.framed(stream).split();

    let fut = back::run_mc(node.to_string(), rx, sink, stream);
    tokio::spawn(fut);

    Ok(tx)
}

pub struct Conns<T: Request> {
    _marker: PhantomData<T>,
    inner: HashMap<String, Conn<tokio::sync::mpsc::Sender<T>>>,
}

impl<T: Request> Conns<T> {
    fn addrs(&self) -> HashSet<String> {
        self.inner.keys().cloned().collect()
    }

    pub fn get_mut(&mut self, s: &str) -> Option<&mut Conn<Sender<T>>> {
        self.inner.get_mut(s)
    }

    fn remove(&mut self, addr: &str) -> Option<Conn<tokio::sync::mpsc::Sender<T>>> {
        self.inner.remove(addr)
    }

    fn insert(&mut self, s: &str, sender: tokio::sync::mpsc::Sender<T>) {
        let conn = Conn {
            addr: s.to_string(),
            sender,
        };
        self.inner.insert(s.to_string(), conn);
    }
}

impl<T: Request> Default for Conns<T> {
    fn default() -> Conns<T> {
        Conns {
            inner: HashMap::new(),
            _marker: Default::default(),
        }
    }
}

#[allow(unused)]
struct Conn<S> {
    addr: String,
    sender: S,
}

impl<S> Conn<S> {
    fn sender(&mut self) -> &mut S {
        &mut self.sender
    }
}

struct ServerLine {
    addr: String,
    weight: usize,
    alias: Option<String>,
}

impl ServerLine {
    fn parse_servers(servers: &[String]) -> Result<Vec<ServerLine>, AsError> {
        let mut sl = Vec::with_capacity(servers.len());
        for server in servers {
            let mut iter = server.split(' ');
            let first_part = iter.next().expect("first partation must exists");
            if first_part.chars().filter(|x| *x == ':').count() == 1 {
                let alias = iter.next().map(|x| x.to_string());
                sl.push(ServerLine {
                    addr: first_part.to_string(),
                    weight: 1,
                    alias,
                });
                continue;
            }

            let mut fp_sp = first_part.rsplitn(2, ':').filter(|x| !x.is_empty());
            let weight = {
                let weight_str = fp_sp.next().unwrap_or("1");
                weight_str.parse::<usize>()?
            };
            let addr = fp_sp.next().expect("addr never be absent").to_owned();
            drop(fp_sp);
            let alias = iter.next().map(|x| x.to_string());
            sl.push(ServerLine {
                addr,
                weight,
                alias,
            });
        }
        Ok(sl)
    }

    fn unwrap_spot(sls: &[ServerLine]) -> (Vec<String>, Vec<String>, Vec<usize>) {
        let mut nodes = Vec::new();
        let mut alias = Vec::new();
        let mut weights = Vec::new();
        for sl in sls {
            if sl.alias.is_some() {
                alias.push(
                    sl.alias
                        .as_ref()
                        .cloned()
                        .expect("node addr can't be empty"),
                );
            }
            nodes.push(sl.addr.clone());
            weights.push(sl.weight);
        }
        (nodes, alias, weights)
    }
}

pub(crate) async fn worker_redis_main(cc: Arc<ClusterConfig>, listener: Arc<TcpListener>) {
    info!("worker started");
    type T = crate::protocol::redis::cmd::Cmd;

    let hash_tag = cc
        .hash_tag
        .as_ref()
        .map(|x| x.as_bytes().to_vec())
        .unwrap_or_else(|| vec![]);

    let (ping_tx, mut ping_rx) = mpsc::channel(128);

    let mut cluster = Cluster::<T> {
        cc: cc.clone(),
        hash_tag,
        spots: HashMap::new(),
        alias: HashMap::new(),
        _marker: PhantomData,
        ring: HashRing::empty(),
        conns: Conns::default(),
        pings: HashMap::new(),
    };

    if let Err(e) = reinit_redis(&mut cluster, (*cc).clone(), ping_tx.clone()).await {
        error!("fail to init redis backend servers for {}", e);
        return;
    }

    let ping_fail_limit = cluster.ping_fail_limit();
    if ping_fail_limit > 0 {
        let ping_interval = cluster.ping_interval();
        let ping_succ_interval = cluster.ping_succ_interval();
        let alias_map = cluster.alias.clone();
        for (alias, node) in alias_map.into_iter() {
            let conn_tx = cluster.conns.get_mut(&node).unwrap().sender().clone();
            setup_ping_redis(
                &mut cluster.pings,
                ping_tx.clone(),
                conn_tx,
                alias,
                node,
                ping_interval,
                ping_succ_interval,
                ping_fail_limit,
            );
        }
    }

    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (socket, addr) = match accepted {
                    Ok(ret) => ret,
                    Err(e) => {
                        error!("fail to accept new connection due to {}", e);
                        continue;
                    }
                };

                info!("accept a new connection from {}", addr);
                if let Err(err) = handle_connection_redis(&mut cluster, socket).await {
                    error!(
                        "fail to handle connection for {} due to {:?}",
                        addr, err
                    );
                }
            }
            Some(cmd) = ping_rx.recv() => {
                match cmd {
                    PingCmd::Reconnect(addr) => {
                        info!("reconnecting to {}", addr);
                        match connect_redis(&cluster.cc, &addr).await {
                            Ok(sender) => cluster.conns.insert(&addr, sender),
                            Err(e) => error!("failed to reconnect to {}: {}", addr, e),
                        }
                    }
                }
            }
        }
    }
}

pub(crate) async fn worker_mc_main(_cc: Arc<ClusterConfig>, _listener: Arc<TcpListener>) {
    info!("memcache support is not fully implemented yet");
}

async fn handle_connection_redis(
    cluster: &mut Cluster<redis::cmd::Cmd>, socket: TcpStream,
) -> Result<(), AsError> {
    front_conn_incr();
    let codec = <redis::cmd::Cmd as Request>::FrontCodec::default();
    let (mut sink, mut stream) = codec.framed(socket).split();

    while let Some(Ok(mut req)) = stream.next().await {
        let req_start_time = Instant::now();
        req.mark_total(&cluster.cc.name);

        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        req.set_reply_sender(reply_tx);

        if let Some(subs) = req.subs() {
            proxy_route_redis_multi(cluster, req, subs).await;
        } else {
            proxy_route_redis(cluster, req).await;
        }

        match reply_rx.await {
            Ok(resp) => {
                if let Err(err) = sink.send(resp).await {
                    error!("front sink send failed {:?}", err);
                    return Err(err.into());
                }
                info!("request total spend time: {:?}", req_start_time.elapsed());
            }
            Err(err) => {
                error!("fail to receive from proxy_route thread {:?}", err);
                return Err(AsError::BackendFail("channel closed".to_string()));
            }
        }
    }
    Ok(())
}

async fn proxy_route_redis(cluster: &mut Cluster<redis::cmd::Cmd>, mut req: redis::cmd::Cmd) {
    let route_start_time = Instant::now();

    let hash_tag = &cluster.hash_tag;
    let key_hash = req.key_hash(hash_tag, fnv::fnv1a64);

    let backend_addr = {
        let ring = &cluster.ring;
        ring.get_node(key_hash).map(|s| s.to_string())
    };

    let backend_addr = match backend_addr {
        Some(addr) => addr,
        None => {
            req.set_error(AsError::BackendFail(format!(
                "backend not found for key {:?}",
                req.msg.get_key()
            )));
            return;
        }
    };

    let sender = cluster
        .conns
        .get_mut(&backend_addr)
        .map(|conn| conn.sender().clone());

    if let Some(sender) = sender {
        if let Err(err) = sender.send(req).await {
            let mut req = err.0;
            error!("fail to send request to backend due to send error");
            req.set_error(AsError::BackendFail("backend chan is closed".to_string()));
        }
    } else {
        req.set_error(AsError::BackendFail(
            "backend connection not found".to_string(),
        ));
    }
    info!("proxy route spend time: {:?}", route_start_time.elapsed());
}
#[inline(never)]
async fn proxy_route_redis_multi(
    cluster: &mut Cluster<redis::cmd::Cmd>, mut req: redis::cmd::Cmd, subs: Vec<redis::cmd::Cmd>,
) {
    let mut replies = Vec::new();
    for mut sub_req in subs {
        let (tx, mut rx) = tokio::sync::oneshot::channel();
        sub_req.set_reply_sender(tx);
        proxy_route_redis(cluster, sub_req).await;
        match rx.try_recv() {
            Ok(resp) => replies.push(resp),
            Err(_) => {
                let resp = Message::from(AsError::BackendFail("channel closed".to_string()));
                replies.push(resp);
            }
        }
    }
    let merged = Message::from_msgs(replies);
    req.set_reply(merged);
}

async fn reinit_redis(
    cluster: &mut Cluster<redis::cmd::Cmd>, cc: ClusterConfig, ping_tx: mpsc::Sender<PingCmd>,
) -> Result<(), AsError> {
    let sls = ServerLine::parse_servers(&cc.servers)?;
    let (nodes, alias, weights) = ServerLine::unwrap_spot(&sls);
    let alias_map: HashMap<_, _> = alias
        .clone()
        .into_iter()
        .zip(nodes.clone().into_iter())
        .collect();
    let alias_rev: HashMap<_, _> = alias_map
        .iter()
        .map(|(x, y)| (y.clone(), x.clone()))
        .collect();
    let spots_map: HashMap<_, _> = if alias.is_empty() {
        nodes
            .clone()
            .into_iter()
            .zip(weights.clone().into_iter())
            .collect()
    } else {
        alias
            .clone()
            .into_iter()
            .zip(weights.clone().into_iter())
            .collect()
    };
    let hash_ring = if alias.is_empty() {
        HashRing::new(nodes.clone(), weights)?
    } else {
        HashRing::new(alias.clone(), weights)?
    };
    let addrs: HashSet<_> = if !alias_map.is_empty() {
        alias_map.values().map(|x| x.to_string()).collect()
    } else {
        spots_map.keys().map(|x| x.to_string()).collect()
    };
    let old_addrs = cluster.conns.addrs();

    let new_addrs = addrs.difference(&old_addrs);
    let unused_addrs = old_addrs.difference(&addrs);
    for addr in new_addrs {
        let sender = connect_redis(&cc, &*addr).await?;
        cluster.conns.insert(&*addr, sender.clone());
        let ping_fail_limit = cluster.ping_fail_limit();
        if ping_fail_limit > 0 {
            let ping_interval = cluster.ping_interval();
            let ping_succ_interval = cluster.ping_succ_interval();
            let alias = alias_rev.get(addr).expect("alias must exists").to_string();

            setup_ping_redis(
                &mut cluster.pings,
                ping_tx.clone(),
                sender,
                alias,
                addr.to_string(),
                ping_interval,
                ping_succ_interval,
                ping_fail_limit,
            );
        }
    }

    for addr in unused_addrs {
        cluster.conns.remove(addr);
        let alias = alias_rev.get(addr).expect("alias must exists").to_string();
        if let Some(ping) = cluster.pings.get(&alias) {
            ping.store(false, Ordering::SeqCst);
        }
    }

    cluster.ring = hash_ring;
    cluster.spots = spots_map;
    cluster.alias = alias_map;

    Ok(())
}

fn setup_ping_redis(
    pings: &mut HashMap<String, Arc<AtomicBool>>, ping_tx: mpsc::Sender<PingCmd>,
    conn_tx: mpsc::Sender<crate::protocol::redis::cmd::Cmd>, alias: String, node: String,
    ping_interval: u64, ping_succ_interval: u64, ping_fail_limit: u8,
) {
    if pings.contains_key(&alias) {
        return;
    }
    let cancel = Arc::new(AtomicBool::new(true));
    pings.insert(alias.clone(), cancel.clone());
    type T = crate::protocol::redis::cmd::Cmd;
    let ping_task = ping::Ping::<T>::new(
        ping_tx,
        conn_tx,
        alias,
        node,
        cancel,
        ping_interval,
        ping_succ_interval,
        ping_fail_limit,
    );
    tokio::spawn(ping_task.run());
}
