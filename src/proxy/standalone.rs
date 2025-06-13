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
use std::sync::{Arc, Mutex};
use std::task::Waker;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc::Sender;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::codec::{Decoder, Encoder};
use tracing::{error, info};

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
    pub cc: Mutex<ClusterConfig>,
    hash_tag: Vec<u8>,
    spots: Mutex<HashMap<String, usize>>,
    alias: Mutex<HashMap<String, String>>,

    _marker: PhantomData<T>,
    ring: Mutex<HashRing>,
    conns: Mutex<Conns<T>>,
    pings: Mutex<HashMap<String, Arc<AtomicBool>>>,
}

impl<T: Request + Send + Sync + Unpin + 'static> Cluster<T> {
    fn ping_fail_limit(&self) -> u8 {
        self.cc
            .lock()
            .unwrap()
            .ping_fail_limit
            .as_ref()
            .cloned()
            .unwrap_or(0)
    }

    fn ping_interval(&self) -> u64 {
        self.cc
            .lock()
            .unwrap()
            .ping_interval
            .as_ref()
            .cloned()
            .unwrap_or(300_000)
    }

    fn ping_succ_interval(&self) -> u64 {
        self.cc
            .lock()
            .unwrap()
            .ping_succ_interval
            .as_ref()
            .cloned()
            .unwrap_or(1_000)
    }
}

pub async fn connect_redis(
    cc: &ClusterConfig,
    node: &str,
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
    _cc: &ClusterConfig,
    node: &str,
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

struct Conns<T: Request> {
    _marker: PhantomData<T>,
    inner: HashMap<String, Conn<tokio::sync::mpsc::Sender<T>>>,
}

impl<T: Request> Conns<T> {
    fn addrs(&self) -> HashSet<String> {
        self.inner.keys().cloned().collect()
    }

    fn get_mut(&mut self, s: &str) -> Option<&mut Conn<Sender<T>>> {
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
        // e.g.: 192.168.1.2:1074:10 redis-20
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

pub async fn run(cc: ClusterConfig) -> Result<()> {
    info!("standalone cluster {} running", cc.name);
    match cc.cache_type {
        CacheType::Redis => run_inner_redis(cc).await,
        CacheType::Memcache | CacheType::MemcacheBinary => run_inner_mc(cc).await,
        _ => unreachable!(),
    }
}

async fn run_inner_redis(cc: ClusterConfig) -> Result<()> {
    type T = crate::protocol::redis::cmd::Cmd;

    let hash_tag = cc
        .hash_tag
        .as_ref()
        .map(|x| x.as_bytes().to_vec())
        .unwrap_or_else(|| vec![]);

    let cluster = Arc::new(Cluster::<T> {
        cc: Mutex::new(cc.clone()),
        hash_tag,
        spots: Mutex::new(HashMap::new()),
        alias: Mutex::new(HashMap::new()),
        _marker: PhantomData,
        ring: Mutex::new(HashRing::empty()),
        conns: Mutex::new(Conns::default()),
        pings: Mutex::new(HashMap::new()),
    });

    reinit_redis(&cluster, cc.clone()).await?;

    let ping_fail_limit = cluster.ping_fail_limit();
    if ping_fail_limit > 0 {
        let ping_interval = cluster.ping_interval();
        let ping_succ_interval = cluster.ping_succ_interval();
        let alias_map = cluster.alias.lock().unwrap().clone();
        for (alias, node) in alias_map.into_iter() {
            setup_ping_redis(
                &cluster,
                &alias,
                &node,
                ping_interval,
                ping_succ_interval,
                ping_fail_limit,
            );
        }
    }

    let listen_addr = cc.listen_addr;
    let listener = tokio::net::TcpListener::bind(listen_addr).await?;
    info!("standalone cluster listening on {}", listen_addr);

    loop {
        let (socket, addr) = listener.accept().await?;
        info!("accept a new connection from {}", addr);
        let cluster_clone = cluster.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_connection_redis(cluster_clone, socket).await {
                error!(
                    "fail to handle connection for {} due to {:?}",
                    addr, err
                );
            }
        });
    }
}

async fn handle_connection_redis(
    cluster: Arc<Cluster<redis::cmd::Cmd>>,
    socket: TcpStream,
) -> Result<(), AsError> {
    front_conn_incr();
    let codec = <redis::cmd::Cmd as Request>::FrontCodec::default();
    let (mut sink, mut stream) = codec.framed(socket).split();

    while let Some(Ok(mut req)) = stream.next().await {
        let req_start_time = Instant::now();
        req.mark_total(&cluster.cc.lock().unwrap().name);
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        req.set_reply_sender(reply_tx);

        if let Some(subs) = req.subs() {
            let cluster_clone = cluster.clone();
            let fut = proxy_route_redis_multi(cluster_clone, req, subs);
            tokio::spawn(fut);
        } else {
            let cluster_clone = cluster.clone();
            let fut = proxy_route_redis(cluster_clone, req);
            tokio::spawn(fut);
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

async fn proxy_route_redis(
    cluster: Arc<Cluster<redis::cmd::Cmd>>,
    mut req: redis::cmd::Cmd,
) {
    let route_start_time = Instant::now();
    let hash_tag = &cluster.hash_tag;
    let key_hash = req.key_hash(hash_tag, fnv::fnv1a64);

    let backend_addr = {
        let ring = cluster.ring.lock().unwrap();
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
        .lock()
        .unwrap()
        .get_mut(&backend_addr)
        .map(|conn| conn.sender().clone());

    if let Some(sender) = sender {
        if let Err(err) = sender.send(req).await {
            let mut req = err.0;
            error!(
                "fail to send request to backend {} due to send error",
                backend_addr
            );
            req.set_error(AsError::BackendFail(
                "backend chan is closed".to_string(),
            ));
        }
    } else {
        req.set_error(AsError::BackendFail(format!(
            "backend connection not found for key {:?}",
            req.msg.get_key()
        )));
    }
    info!("proxy route spend time: {:?}", route_start_time.elapsed());
}
#[inline(never)]
async fn proxy_route_redis_multi(
    cluster: Arc<Cluster<redis::cmd::Cmd>>,
    mut req: redis::cmd::Cmd,
    subs: Vec<redis::cmd::Cmd>,
) {
    // handle mget/mset commands
    let mut futs = Vec::new();

    for mut sub_req in subs {
        let (tx, rx) = tokio::sync::oneshot::channel();
        sub_req.set_reply_sender(tx);
        let cluster_clone = cluster.clone();
        let fut = async move {
            proxy_route_redis(cluster_clone, sub_req).await;
            rx.await
        };
        futs.push(fut);
    }

    let mut replies = Vec::new();
    let results = future::join_all(futs).await;
    for result in results {
        match result {
            Ok(resp) => replies.push(resp),
            Err(e) => {
                error!("fail to join all due to {:?}", e);
                let resp = Message::from(AsError::BackendFail(e.to_string()));
                replies.push(resp);
            }
        }
    }
    let merged = Message::from_msgs(replies);
    req.set_reply(merged);
}

// a lot of function needs to be specialized for redis and mc
// I will do it step by step
async fn reinit_redis(
    cluster: &Arc<Cluster<redis::cmd::Cmd>>,
    cc: ClusterConfig,
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
        HashRing::new(nodes, weights)?
    } else {
        HashRing::new(alias, weights)?
    };
    let addrs: HashSet<_> = if !alias_map.is_empty() {
        alias_map.values().map(|x| x.to_string()).collect()
    } else {
        spots_map.keys().map(|x| x.to_string()).collect()
    };
    let old_addrs = cluster.conns.lock().unwrap().addrs();

    let new_addrs = addrs.difference(&old_addrs);
    let unused_addrs = old_addrs.difference(&addrs);
    for addr in new_addrs {
        let sender = connect_redis(&cc, &*addr).await?;
        cluster.conns.lock().unwrap().insert(&*addr, sender);
        let ping_fail_limit = cluster.ping_fail_limit();
        if ping_fail_limit > 0 {
            let ping_interval = cluster.ping_interval();
            let ping_succ_interval = cluster.ping_succ_interval();
            let alias = alias_rev
                .get(addr)
                .expect("alias must exists")
                .to_string();
            setup_ping_redis(
                &cluster,
                &alias,
                addr,
                ping_interval,
                ping_succ_interval,
                ping_fail_limit,
            );
        }
    }

    for addr in unused_addrs {
        cluster.conns.lock().unwrap().remove(addr);
        let alias = alias_rev
            .get(addr)
            .expect("alias must exists")
            .to_string();
        if let Some(ping) = cluster.pings.lock().unwrap().get(&alias) {
            ping.store(false, Ordering::SeqCst);
        }
    }

    let mut ring = cluster.ring.lock().unwrap();
    *ring = hash_ring;
    let mut spots = cluster.spots.lock().unwrap();
    *spots = spots_map;
    let mut alias = cluster.alias.lock().unwrap();
    *alias = alias_map;

    Ok(())
}

fn setup_ping_redis(
    cluster: &Arc<Cluster<redis::cmd::Cmd>>,
    alias: &str,
    node: &str,
    ping_interval: u64,
    ping_succ_interval: u64,
    ping_fail_limit: u8,
) {
    let ping = ping::Ping::new(
        Arc::downgrade(cluster),
        alias.to_string(),
        node.to_string(),
        Arc::new(AtomicBool::new(true)),
        ping_interval,
        ping_succ_interval,
        ping_fail_limit,
    );
    tokio::spawn(ping.run());
}

async fn run_inner_mc(_cc: ClusterConfig) -> Result<()> {
    unimplemented!()
}
