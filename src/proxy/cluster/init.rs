use std::time::Duration;
use tokio::time::timeout;

use crate::com::{AsError, ClusterConfig};
use crate::protocol::redis::{Cmd};
// use crate::protocol::redis::{new_cluster_slots_cmd, slots_reply_to_replicas, Cmd};
use crate::proxy::standalone::connect;

pub struct Initializer {
    cc: ClusterConfig,
}

impl Initializer {
    pub fn new(cc: ClusterConfig) -> Initializer {
        Initializer { cc }
    }

    pub async fn run(&mut self) -> Result<(), AsError> {
        for addr in self.cc.servers.iter() {
            tracing::info!("start to connect to backend {}", &addr);
            let connect_fut = connect::<Cmd>(
                &self.cc.name,
                addr,
                self.cc.read_timeout,
                self.cc.write_timeout,
            );

            match timeout(Duration::from_secs(1), connect_fut).await {
                Ok(Ok(mut _sender)) => {
                    unimplemented!();
                }
                Ok(Err(err)) => {
                    tracing::warn!("fail to connect to backend {} due to {}", &addr, &err);
                }
                Err(_) => {
                    tracing::warn!("fail to connect to backend {} due to timeout", &addr);
                }
            }
        }
        Err(AsError::ClusterAllSeedsDie(self.cc.name.clone()))
    }
}
