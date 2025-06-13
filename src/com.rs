pub mod meta;

use std::collections::HashSet;
use std::net::SocketAddr;
use std::string::FromUtf8Error;
use std::sync::atomic::{AtomicBool, Ordering};
use std::num::ParseIntError;

use serde_derive::{Deserialize, Serialize};
use thiserror::Error;

pub static METRICS_ENABLED: AtomicBool = AtomicBool::new(false);

pub fn get_metrics_enabled() -> bool {
    METRICS_ENABLED.load(Ordering::Relaxed)
}

pub fn set_metrics_enabled(enabled: bool) {
    METRICS_ENABLED.store(enabled, Ordering::Relaxed);
}

#[derive(Debug, Error)]
pub enum AsError {
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("invalid node address format: {0}")]
    InvalidNodeAddress(String),
    #[error("unsupported command: {0}")]
    UnsupportedCommand(String),
    #[error("command is not allowed to proxy: {0}")]
    NotAllowed(String),
    #[error("backend failed: {0}")]
    BackendFail(String),
    #[error("proxy failed: {0}")]
    ProxyFail(String),
    #[error("config file failed: {0}")]
    ConfigFileFail(String),
    #[error("bad config: {0}")]
    BadConfig(String),
    #[error("bad message")]
    BadMessage,
    #[error("bad auth")]
    BadAuth,
    #[error("backend closed")]
    BackendClosedError(String),
    #[error("cluster command fail to forward")]
    ClusterFail,
    #[error("cluster command fail with asking")]
    ClusterAsking,
    #[error("cluster command fail with moved")]
    ClusterMoved,
    #[error("bad redirect")]
    BadRedirect,
    #[error("request reach max cycle")]
    RequestReachMaxCycle,
    #[error("try again")]
    TryAgain,
    #[error("eof")]
    Eof,
    #[error("io error")]
    IoError(#[from] std::io::Error),
    #[error("addr parse error")]
    AddrParseError(#[from] std::net::AddrParseError),
    #[error("parse int error")]
    ParseIntError(#[from] ParseIntError),
    #[error("parse utf8 error")]
    ParseUtf8Error(#[from] FromUtf8Error),
    #[error("btoi error")]
    Btoi(#[from] btoi::ParseIntegerError),
    #[error("other error: {0}")]
    Other(String),
    #[error("canceled")]
    Canceled,
}

impl Clone for AsError {
    fn clone(&self) -> Self {
        match self {
            AsError::InvalidArgument(s) => AsError::InvalidArgument(s.clone()),
            AsError::InvalidNodeAddress(s) => AsError::InvalidNodeAddress(s.clone()),
            AsError::UnsupportedCommand(s) => AsError::UnsupportedCommand(s.clone()),
            AsError::NotAllowed(s) => AsError::NotAllowed(s.clone()),
            AsError::BackendFail(s) => AsError::BackendFail(s.clone()),
            AsError::ProxyFail(s) => AsError::ProxyFail(s.clone()),
            AsError::ConfigFileFail(s) => AsError::ConfigFileFail(s.clone()),
            AsError::BadConfig(s) => AsError::BadConfig(s.clone()),
            AsError::BadMessage => AsError::BadMessage,
            AsError::BadAuth => AsError::BadAuth,
            AsError::BackendClosedError(s) => AsError::BackendClosedError(s.clone()),
            AsError::ClusterFail => AsError::ClusterFail,
            AsError::ClusterAsking => AsError::ClusterAsking,
            AsError::ClusterMoved => AsError::ClusterMoved,
            AsError::BadRedirect => AsError::BadRedirect,
            AsError::RequestReachMaxCycle => AsError::RequestReachMaxCycle,
            AsError::TryAgain => AsError::TryAgain,
            AsError::Eof => AsError::Eof,
            AsError::IoError(e) => AsError::Other(e.to_string()),
            AsError::AddrParseError(e) => AsError::Other(e.to_string()),
            AsError::ParseIntError(e) => AsError::Other(e.to_string()),
            AsError::ParseUtf8Error(e) => AsError::Other(e.to_string()),
            AsError::Btoi(e) => AsError::Other(e.to_string()),
            AsError::Other(s) => AsError::Other(s.clone()),
            AsError::Canceled => AsError::Canceled,
        }
    }
}

pub type AtomicGuard<T> = std::sync::Arc<tokio::sync::Mutex<T>>;

pub fn new_atomic_guard<T>(t: T) -> AtomicGuard<T> {
    std::sync::Arc::new(tokio::sync::Mutex::new(t))
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub clusters: Vec<ClusterConfig>,
}

impl Config {
    pub fn valid(&self) -> Result<(), AsError> {
        Ok(())
    }

    pub fn reload_equals(&self, other: &Self) -> bool {
        self == other
    }

    pub fn load(path: &str) -> Result<Self, AsError> {
        let mut f =
            std::fs::File::open(path).map_err(|e| AsError::ConfigFileFail(e.to_string()))?;
        let mut data = String::new();
        std::io::Read::read_to_string(&mut f, &mut data)
            .map_err(|e| AsError::ConfigFileFail(e.to_string()))?;
        let cfg: Config =
            toml::from_str(&data).map_err(|e| AsError::ConfigFileFail(e.to_string()))?;
        Ok(cfg)
    }

    pub fn get_cluster(&self, name: &str) -> Option<&ClusterConfig> {
        self.clusters.iter().find(|c| c.name == name)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub enum CacheType {
    #[serde(rename = "redis")]
    Redis,
    #[serde(rename = "memcache")]
    Memcache,
    #[serde(rename = "memcache_binary")]
    MemcacheBinary,
    #[serde(rename = "redis_cluster")]
    RedisCluster,
    #[serde(rename = "redis_cluster_proxy")]
    RedisClusterProxy,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct RedisConfig {
    pub dial_timeout_ms: u64,
    pub read_timeout_ms: u64,
    pub write_timeout_ms: u64,
    pub pool_size: usize,
}

fn default_tcp_keepalive_secs() -> u64 {
    0
}

fn default_redis_ping_interval_secs() -> u64 {
    0
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct ClusterConfig {
    pub name: String,
    pub listen_addr: SocketAddr,
    pub listen_proto: String,
    pub hash_tag: Option<String>,
    pub cache_type: CacheType,
    pub servers: Vec<String>,
    #[serde(default = "default_tcp_keepalive_secs")]
    pub tcp_keepalive_secs: u64,
    #[serde(default = "default_redis_ping_interval_secs")]
    pub redis_ping_interval_secs: u64,
    pub thread: usize,
    pub redis_auth: Option<String>,
    #[serde(flatten)]
    pub redis: Option<RedisConfig>,
    pub read_only_servers: Option<Vec<String>>,
    pub server_retry_interval: Option<u64>,
    pub server_failure_limit: Option<usize>,
    pub server_connections: Option<usize>,
    pub ping_interval: Option<u64>,
    pub ping_fail_limit: Option<u8>,
    pub ping_succ_interval: Option<u64>,
    pub auto_eject_hosts: Option<bool>,
    pub connect_timeout: Option<u64>,
    pub read_timeout: Option<u64>,
    pub write_timeout: Option<u64>,
    pub dial_timeout: Option<u64>,
    pub key_hash_type: Option<String>,
    pub preconnect: Option<bool>,
    pub slowlog_log_slower_than: Option<i64>,
    pub slowlog_max_len: Option<usize>,
    pub command_rename: Option<std::collections::HashMap<String, String>>,
    pub read_pref: Option<String>,
    pub require_master_auth: Option<bool>,
    pub read_from_slave_enabled: Option<bool>,
    pub slaves_for_read: Option<HashSet<String>>,
    pub read_slaves: Option<Vec<String>>,
}
