use net2::TcpBuilder;
use std::net::SocketAddr;
use tokio::net::TcpStream;

use std::collections::{BTreeMap, BTreeSet};
use tracing::error;
use std::env;
use std::fs;
use std::net::{AddrParseError, ToSocketAddrs};
use std::num;
use std::path::Path;

pub mod meta;

pub const ENV_ASTER_DEFAULT_THREADS: &str = "ASTER_DEFAULT_THREAD";
const DEFAULT_FETCH_INTERVAL_MS: u64 = 30 * 60 * 1000;

pub const CODE_PORT_IN_USE: i32 = 1;

#[derive(Debug, thiserror::Error)]
pub enum AsError {
    #[error("config is bad for fields {0}")]
    BadConfig(String),
    #[error("fail to parse int in config")]
    StrParseIntError(#[from] num::ParseIntError),

    #[error("bad host {0}")]
    BadHost(String),

    #[error("connect to {0} fail")]
    ConnectFail(String),

    #[error("invalid message")]
    BadMessage,

    #[error("message is ok but request bad or not allowed")]
    BadRequest,

    #[error("request not supported")]
    RequestNotSupport,

    #[error("inline request don't support multi keys")]
    RequestInlineWithMultiKeys,

    #[error("message reply is bad")]
    BadReply,

    #[error("command timeout")]
    CmdTimeout,

    #[error("proxy fail")]
    ProxyFail,

    #[error("connection closed of {0}")]
    ConnClosed(String),

    #[error("fail due retry send, reached limit")]
    RequestReachMaxCycle,

    #[error("fail to parse integer {0}")]
    ParseIntError(#[from] btoi::ParseIntegerError),

    #[error("CLUSTER SLOTS must be replied with array")]
    WrongClusterSlotsReplyType,

    #[error("CLUSTER SLOTS must contains slot info")]
    WrongClusterSlotsReplySlot,

    #[error("cluster fail to proxy command")]
    ClusterFailDispatch,

    #[error("unexpected io error {0}")]
    IoError(#[from] tokio::io::Error), // io_error

    #[error("remote connection has been active closed: {0}")]
    BackendClosedError(String),

    #[error("fail to redirect command")]
    RedirectFailError,

    #[error("fail to init cluster {0} due to all seed nodes is die")]
    ClusterAllSeedsDie(String),

    #[error("fail to load config toml error {0}")]
    ConfigError(#[from] toml::de::Error), // de error

    #[error("fail to load system info")]
    SystemError,
    
    #[error("fail to parse socket address")]
    AddrParseError(#[from] AddrParseError),

    #[error("there is nothing happening")]
    None,
}

impl PartialEq for AsError {
    fn eq(&self, other: &AsError) -> bool {
        match (self, other) {
            (Self::None, Self::None) => true,
            (Self::BadMessage, Self::BadMessage) => true,
            (Self::BadRequest, Self::BadRequest) => true,
            (Self::RequestNotSupport, Self::RequestNotSupport) => true,
            (Self::RequestInlineWithMultiKeys, Self::RequestInlineWithMultiKeys) => true,
            (Self::BadReply, Self::BadReply) => true,
            (Self::ProxyFail, Self::ProxyFail) => true,
            (Self::RequestReachMaxCycle, Self::RequestReachMaxCycle) => true,
            (Self::ParseIntError(inner), Self::ParseIntError(other_inner)) => inner == other_inner,
            (Self::WrongClusterSlotsReplyType, Self::WrongClusterSlotsReplyType) => true,
            (Self::WrongClusterSlotsReplySlot, Self::WrongClusterSlotsReplySlot) => true,
            (Self::ClusterFailDispatch, Self::ClusterFailDispatch) => true,
            (Self::RedirectFailError, Self::RedirectFailError) => true,
            (Self::BackendClosedError(inner), Self::BackendClosedError(other_inner)) => {
                inner == other_inner
            }
            (Self::StrParseIntError(inner), Self::StrParseIntError(other_inner)) => {
                inner == other_inner
            }
            (Self::ClusterAllSeedsDie(inner), Self::ClusterAllSeedsDie(other_inner)) => {
                inner == other_inner
            }

            (Self::IoError(inner), Self::IoError(other_inner)) => {
                inner.kind() == other_inner.kind()
            }
            (Self::ConfigError(_), Self::ConfigError(_)) => true,
            (Self::SystemError, Self::SystemError) => true,
            (Self::ConnClosed(addr1), Self::ConnClosed(addr2)) => addr1 == addr2,
            (Self::AddrParseError(inner), Self::AddrParseError(other_inner)) => inner == other_inner,
            (Self::BadHost(addr1), Self::BadHost(addr2)) => addr1 == addr2,
            (Self::ConnectFail(addr1), Self::ConnectFail(addr2)) => addr1 == addr2,
            _ => false,
        }
    }
}


#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    #[serde(default)]
    pub clusters: Vec<ClusterConfig>,
}

impl Config {
    pub fn cluster(&self, name: &str) -> Option<ClusterConfig> {
        for cluster in &self.clusters {
            if cluster.name == name {
                return Some(cluster.clone());
            }
        }
        None
    }

    fn servers_map(&self) -> BTreeMap<String, BTreeSet<String>> {
        self.clusters
            .iter()
            .map(|x| {
                (
                    x.name.clone(),
                    x.servers.iter().map(|x| x.to_string()).collect(),
                )
            })
            .collect()
    }

    pub fn reload_equals(&self, other: &Config) -> bool {
        let equals_map = self.servers_map();
        let others_map = other.servers_map();
        equals_map == others_map
    }

    pub fn valid(&self) -> Result<(), AsError> {
        Ok(())
    }

    pub fn load<P: AsRef<Path>>(p: P) -> Result<Config, AsError> {
        let path = p.as_ref();
        let data = fs::read_to_string(path)?;
        let mut cfg: Config = toml::from_str(&data)?;
        let thread = Config::load_thread_from_env();
        for cluster in &mut cfg.clusters[..] {
            if cluster.thread.is_none() {
                cluster.thread = Some(thread);
            }
        }
        Ok(cfg)
    }

    fn load_thread_from_env() -> usize {
        let thread_str = env::var(ENV_ASTER_DEFAULT_THREADS).unwrap_or_else(|_| "4".to_string());
        thread_str.parse::<usize>().unwrap_or_else(|_| 4)
    }
}

#[derive(Deserialize, Debug, Clone, Copy)]
pub enum CacheType {
    #[serde(rename = "redis")]
    Redis,
    #[serde(rename = "memcache")]
    Memcache,
    #[serde(rename = "memcache_binary")]
    MemcacheBinary,
    #[serde(rename = "redis_cluster")]
    RedisCluster,
}

impl Default for CacheType {
    fn default() -> CacheType {
        CacheType::RedisCluster
    }
}

#[derive(Clone, Debug, Deserialize, Default)]
pub struct ClusterConfig {
    pub name: String,
    pub listen_addr: String,
    pub hash_tag: Option<String>,

    pub thread: Option<usize>,
    pub cache_type: CacheType,

    pub read_timeout: Option<u64>,
    pub write_timeout: Option<u64>,

    #[serde(default)]
    pub servers: Vec<String>,

    // cluster special
    pub fetch_interval: Option<u64>,
    pub read_from_slave: Option<bool>,

    // proxy special
    pub ping_fail_limit: Option<u8>,
    pub ping_interval: Option<u64>,
    pub ping_succ_interval: Option<u64>,

    // dead codes

    // command not support now
    pub dial_timeout: Option<u64>,
    // dead option: not support other proto
    pub listen_proto: Option<String>,

    // dead option: always 1
    pub node_connections: Option<usize>,
}

impl ClusterConfig {
    pub(crate) fn hash_tag_bytes(&self) -> Vec<u8> {
        self.hash_tag
            .as_ref()
            .map(|x| x.as_bytes().to_vec())
            .unwrap_or_else(|| vec![])
    }

    pub(crate) fn fetch_interval_ms(&self) -> u64 {
        self.fetch_interval.unwrap_or(DEFAULT_FETCH_INTERVAL_MS)
    }
}

#[cfg(windows)]
pub(crate) fn create_reuse_port_listener(
    addr: &SocketAddr,
) -> Result<std::net::TcpListener, std::io::Error> {
    let builder = TcpBuilder::new_v4()?;
    let std_listener = builder
        .reuse_address(true)
        .expect("os not support SO_REUSEADDR")
        .bind(addr)?
        .listen(std::i32::MAX)?;
    Ok(std_listener)
}

#[cfg(not(windows))]
pub(crate) fn create_reuse_port_listener(
    addr: &SocketAddr,
) -> Result<std::net::TcpListener, std::io::Error> {
    use net2::unix::UnixTcpBuilderExt;

    let builder = TcpBuilder::new_v4()?;
    let std_listener = builder
        .reuse_address(true)
        .expect("os not support SO_REUSEADDR")
        .reuse_port(true)
        .expect("os not support SO_REUSEPORT")
        .bind(addr)?
        .listen(std::i32::MAX)?;
    Ok(std_listener)
}

#[cfg(not(unix))]
#[inline]
pub fn set_read_write_timeout(
    sock: TcpStream,
    _rt: Option<u64>,
    _wt: Option<u64>,
) -> Result<TcpStream, AsError> {
    Ok(sock)
}

#[cfg(unix)]
#[inline]
pub fn set_read_write_timeout(
    sock: TcpStream,
    rt: Option<u64>,
    wt: Option<u64>,
) -> Result<TcpStream, AsError> {
    use std::os::unix::io::AsRawFd;
    use std::os::unix::io::FromRawFd;
    use std::time::Duration;

    let nrt = rt.map(Duration::from_millis);
    let nwt = wt.map(Duration::from_millis);
    let fd = sock.as_raw_fd();

    let nsock = unsafe { std::net::TcpStream::from_raw_fd(fd) };
    std::mem::forget(sock);

    nsock.set_read_timeout(nrt)?;
    nsock.set_write_timeout(nwt)?;
    let stream = TcpStream::from_std(nsock)?;

    return Ok(stream);
}

pub(crate) fn gethostbyname(name: &str) -> Result<SocketAddr, AsError> {
    let mut iter = name.to_socket_addrs().map_err(|err| {
        error!("fail to resolve addr to {} by {}", name, err);
        AsError::BadConfig("servers".to_string())
    })?;
    let addr = iter
        .next()
        .ok_or_else(|| AsError::BadConfig(format!("servers:{}", name)))?;

    Ok(addr)
}
