use lazy_static::lazy_static;
use network_interface::{NetworkInterface, NetworkInterfaceConfig};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::com::ClusterConfig;

// This constant is likely defined elsewhere, maybe under a feature flag.
// For now, let's define a placeholder.
const METRICS_ENABLED: &bool = &true;

lazy_static! {
    static ref CLUSTER_META: Mutex<HashMap<String, Arc<ClusterMeta>>> = Mutex::new(HashMap::new());
}

pub fn meta_init(meta: ClusterMeta) {
    let mut guard = CLUSTER_META.lock().unwrap();
    guard.insert(meta.cc.name.clone(), Arc::new(meta));
}

pub fn get_cluster_meta(cluster_name: &str) -> Option<Arc<ClusterMeta>> {
    let guard = CLUSTER_META.lock().unwrap();
    guard.get(cluster_name).cloned()
}

#[derive(Debug, Clone)]
pub struct ClusterMeta {
    pub cc: ClusterConfig,
    pub ip: String,
    pub metrics_enabled: bool,
}

pub fn load_meta(cc: ClusterConfig, ip: Option<String>) -> ClusterMeta {
    let local_ip = ip.unwrap_or_else(get_local_ip);
    ClusterMeta {
        cc,
        ip: local_ip,
        metrics_enabled: *METRICS_ENABLED,
    }
}

fn get_local_ip() -> String {
    let mut local_ip = "127.0.0.1".to_string();
    if let Ok(ifaces) = NetworkInterface::show() {
        for iface in ifaces {
            for addr in iface.addr {
                if !addr.ip().is_loopback() {
                    if addr.ip().is_ipv4() {
                        local_ip = addr.ip().to_string();
                        return local_ip;
                    }
                }
            }
        }
    }
    local_ip
}
