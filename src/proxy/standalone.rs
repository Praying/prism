pub mod back;
pub mod fnv;
pub mod front;
pub mod ketama;
pub mod ping;
pub mod reload;

use futures::StreamExt;
use tokio::net::TcpStream;
use tokio::sync::mpsc::Sender;
use tokio_util::codec::{Decoder, Encoder};

use std::task::Waker;
use tracing::{debug, info};
use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::protocol::{mc, redis};

use crate::metrics::front_conn_incr;

use crate::com::AsError;
use crate::com::{CacheType, ClusterConfig};
use crate::protocol::IntoReply;

use ketama::HashRing;

pub trait Request: Clone + Send + Sync {
    type Reply: Clone + IntoReply<Self::Reply> + From<AsError>;
    type ReplySender: Send;

    type FrontCodec: Decoder<Item = Self, Error = AsError>
        + Encoder<Self, Error = AsError>
        + Default
        + 'static;
    type BackCodec: Decoder<Item = Self::Reply, Error = AsError>
        + Encoder<Self, Error = AsError>
        + Default
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

    fn set_reply<R: IntoReply<Self::Reply>>(&self, t: R);
    fn set_error(&self, t: &AsError);
    fn set_reply_sender(&self, sender: Self::ReplySender);

    fn get_sendtime(&self) -> Option<Instant>;
}

pub struct Cluster<T> {
    pub cc: Mutex<ClusterConfig>,
    hash_tag: Vec<u8>,
    spots: Mutex<HashMap<String, usize>>,
    alias: Mutex<HashMap<String, String>>,

    _marker: PhantomData<T>,
    ring: Mutex<HashRing>,
    conns: Mutex<Conns<T>>,
    pings: Mutex<HashMap<String, Arc<AtomicBool>>>,
}

impl<T: Request + Send + Sync + 'static> Cluster<T> {

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

    fn setup_ping(
        self: &Arc<Self>,
        alias: &str,
        node: &str,
        ping_interval: u64,
        ping_succ_interval: u64,
        ping_fail_limit: u8,
    ) {
        const CANCEL: bool = false;
        let handle = Arc::new(AtomicBool::new(CANCEL));
        {
            let mut pings = self.pings.lock().unwrap();
            pings.insert(node.to_string(), handle.clone());
        }

        let ping = ping::Ping::new(
            Arc::downgrade(self),
            alias.to_string(),
            node.to_string(),
            handle,
            ping_interval,
            ping_succ_interval,
            ping_fail_limit,
        );
        tokio::spawn(ping.run());
    }

    pub(crate) async fn reinit(self: &Arc<Self>, cc: ClusterConfig) -> Result<(), AsError> {
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
        let old_addrs = self.conns.lock().unwrap().addrs();

        let new_addrs = addrs.difference(&old_addrs);
        let unused_addrs = old_addrs.difference(&addrs);
        for addr in new_addrs {
            self.reconnect(&*addr).await;
            let ping_fail_limit = self.ping_fail_limit();
            if ping_fail_limit > 0 {
                let ping_interval = self.ping_interval();
                let ping_succ_interval = self.ping_succ_interval();
                let alias = alias_rev
                    .get(addr)
                    .expect("alias must be exists")
                    .to_string();
                self.setup_ping(
                    &alias,
                    addr,
                    ping_interval,
                    ping_succ_interval,
                    ping_fail_limit,
                );
            }
        }

        for addr in unused_addrs {
            self.conns.lock().unwrap().remove(&addr);
            let mut pings = self.pings.lock().unwrap();
            if let Some(handle) = pings.remove(addr) {
                handle.store(true, Ordering::SeqCst);
            }
        }

        *self.cc.lock().unwrap() = cc;
        *self.ring.lock().unwrap() = hash_ring;
        *self.alias.lock().unwrap() = alias_map;
        *self.spots.lock().unwrap() = spots_map;
        Ok(())
    }

    fn has_alias(&self) -> bool {
        !self.alias.lock().unwrap().is_empty()
    }

    fn get_node(&self, name: String) -> String {
        if !self.has_alias() {
            return name;
        }

        self.alias
            .lock()
            .unwrap()
            .get(&name)
            .expect("alias name must exists")
            .to_string()
    }

    pub(crate) async fn add_node(&self, name: String) -> Result<(), AsError> {
        let weight = self.spots.lock().unwrap().get(&name).cloned();
        if let Some(weight) = weight {
            let addr = self.get_node(name.clone());
            let (cluster_name, read_timeout, write_timeout) = {
                let cc = self.cc.lock().unwrap();
                (cc.name.clone(), cc.read_timeout, cc.write_timeout)
            };
            let conn = connect(&cluster_name, &addr, read_timeout, write_timeout).await?;
            self.conns.lock().unwrap().insert(&addr, conn);
            self.ring.lock().unwrap().add_node(name, weight);
        }
        Ok(())
    }

    pub(crate) fn remove_node(&self, name: String) {
        self.ring.lock().unwrap().del_node(&name);
        let node = self.get_node(name);
        if self.conns.lock().unwrap().remove(&node).is_some() {
            info!("dropping backend connection of {} due active delete", node);
        }
    }

    pub(crate) async fn reconnect(&self, addr: &str) {
        self.conns.lock().unwrap().remove(addr);
        debug!("trying to reconnect to {}", addr);
        let (cluster_name, read_timeout, write_timeout) = {
            let cc = self.cc.lock().unwrap();
            (cc.name.clone(), cc.read_timeout, cc.write_timeout)
        };
        match connect(&cluster_name, &addr, read_timeout, write_timeout).await {
            Ok(sender) => self.conns.lock().unwrap().insert(&addr, sender),
            Err(err) => {
                tracing::error!("fail to reconnect to {} due to {:?}", addr, err);
            }
        }
    }

}

pub async fn connect<T>(
    cluster: &str,
    node: &str,
    rt: Option<u64>,
    _wt: Option<u64>,
) -> Result<tokio::sync::mpsc::Sender<T>, AsError>
where
    T: Request + 'static,
{
    let addr = node.parse::<SocketAddr>()?;
    let stream = TcpStream::connect(addr).await?;
    stream.set_nodelay(true)?;

    let (tx, rx) = tokio::sync::mpsc::channel(1024 * 8);
    
    let codec = T::BackCodec::default();
    let (sink, stream) = codec.framed(stream).split();
    
    // TODO: make back::Back async
    // let backend = back::Back::new(cluster.to_string(), node.to_string(), rx, sink, stream, rt.unwrap_or(1000));
    // tokio::spawn(backend);

    Ok(tx)
}

struct Conns<T> {
    _marker: PhantomData<T>,
    inner: HashMap<String, Conn<tokio::sync::mpsc::Sender<T>>>,
}

impl<T> Conns<T> {
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

impl<T> Default for Conns<T> {
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

use std::net::SocketAddr;
use anyhow::Result;

pub async fn run(cc: ClusterConfig) -> Result<()> {
    info!("standalone cluster {} running", cc.name);
    match cc.cache_type {
        CacheType::Redis => run_inner::<redis::Cmd>(cc).await,
        CacheType::Memcache | CacheType::MemcacheBinary => run_inner::<mc::Cmd>(cc).await,
        _ => unreachable!(),
    }
}

async fn run_inner<T: Request + 'static>(cc: ClusterConfig) -> Result<()> {
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

    cluster.reinit(cc.clone()).await?;


    let ping_fail_limit = cluster.ping_fail_limit();
    if ping_fail_limit > 0 {
        let ping_interval = cluster.ping_interval();
        let ping_succ_interval = cluster.ping_succ_interval();
        let alias_map = cluster.alias.lock().unwrap().clone();
        for (alias, node) in alias_map.into_iter() {
            cluster.setup_ping(
                &alias,
                &node,
                ping_interval,
                ping_succ_interval,
                ping_fail_limit,
            );
        }
    }
    // let reloader = reload::Reloader::new(cluster.clone());
    // tokio::spawn(reloader);


    let addr = cc
        .listen_addr
        .parse::<SocketAddr>()?;

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    info!("standalone cluster {} listening on {}", cc.name, addr);

    loop {
        let (sock, client_addr) = listener.accept().await?;
        let cluster_clone = cluster.clone();
        tokio::spawn(async move {
            info!("accept new connection from {}", client_addr);
            if let Err(e) = handle_connection(cluster_clone, sock).await {
                tracing::error!("handle connection from {} failed: {}", client_addr, e);
            }
        });
    }
}

async fn handle_connection<T: Request + 'static>(cluster: Arc<Cluster<T>>, sock: TcpStream) -> Result<()> {
    if let Err(err) = sock.set_nodelay(true) {
        tracing::warn!(
            "cluster {} fail to set nodelay but skip, due to {:?}",
            cluster.cc.lock().unwrap().name,
            err
        );
    }

    let codec = T::FrontCodec::default();
    let (output, input) = codec.framed(sock).split();

    front_conn_incr(&cluster.cc.lock().unwrap().name);
    // TODO: make front::Front async
    // let fut = front::Front::new(cluster, input, output);
    // fut.await?;
    Ok(())
}
