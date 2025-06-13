#![allow(
    clippy::missing_safety_doc,
    clippy::uninit_vec,
    clippy::upper_case_acronyms
)]

pub mod com;
pub mod metrics;
pub mod protocol;
pub mod proxy;
pub mod utils;

use crate::com::{AsError, CacheType, Config};
use clap::{self, App, Arg};
use futures::future::BoxFuture;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::runtime;
use tokio::signal::unix::{signal, SignalKind};
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

pub const PRISM_VERSION: &str = env!("CARGO_PKG_VERSION");
pub fn run() -> Result<(), ()> {
    let rt = runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");

    if let Err(err) = rt.block_on(async {
        let matches = App::new("prism-proxy")
            .version(PRISM_VERSION)
            .author("Praying. <codegpt@gmail.com>")
            .about("Prism is a light, fast and powerful cache proxy written in rust.")
            .arg(
                Arg::with_name("config")
                    .value_name("FILE")
                    .help("Sets a custom config file")
                    .takes_value(true)
                    .required(true),
            ).arg(
            Arg::with_name("ip")
                .short("i")
                .long("ip")
                .help("expose given ip for CLUSTER SLOTS/NODES command(may be used by jedis cluster connection).")
                .takes_value(true),
        )
            .arg(
                Arg::with_name("metrics")
                    .short("m")
                    .long("metrics")
                    .help("port to expose prometheus (if compile without 'metrics' feature, this flag will be ignore).")
                    .takes_value(true),
            )
            .arg(
                Arg::with_name("version")
                    .short("V")
                    .long("version")
                    .help("show the version of prism."),
            )
            .arg(
                Arg::with_name("reload")
                    .short("r")
                    .long("reload")
                    .help("enable reload feature for standalone proxy mode."),
            )
            .get_matches();

        let config_path = matches.value_of("config").expect("config file is needed");
        let cfg = Config::load(config_path).expect("load config file failed");

        let mut handles = Vec::new();

        for cluster_config in cfg.clusters.into_iter() {
            let name = cluster_config.name.clone();
            info!("start cluster {} with config {:?}", name, &cluster_config);
            if cluster_config.servers.is_empty() {
                warn!("cluster {} with empty servers", name);
                continue;
            }

            let workers = cluster_config.thread;
            let cc = Arc::new(cluster_config);

            let listener = if cc.cache_type != CacheType::RedisCluster {
                let listener = TcpListener::bind(&cc.listen_addr)
                    .await
                    .expect("bind failed");
                Some(Arc::new(listener))
            } else {
                None
            };

            for i in 0..workers {
                info!("spawning worker {} for cluster {}", i, name);

                let cc_clone = cc.clone();
                let name_clone = name.clone();
                let listener_clone = listener.clone();

                let handle: JoinHandle<()> = tokio::spawn(async move {
                    if let Some(listener) = listener_clone {
                        let fut: BoxFuture<'static, ()> = match &cc_clone.cache_type {
                            CacheType::Redis => {
                                Box::pin(proxy::standalone::worker_redis_main(cc_clone, listener))
                            }
                            CacheType::Memcache => {
                                Box::pin(proxy::standalone::worker_mc_main(cc_clone, listener))
                            }
                            CacheType::MemcacheBinary => {
                                Box::pin(async { unimplemented!() })
                            }
                            CacheType::RedisClusterProxy => {
                                Box::pin(async { unimplemented!() })
                            }
                            CacheType::RedisCluster => {
                                // This path should not be taken due to the listener check above.
                                return;
                            }
                        };
                        fut.await;
                    } else {
                        // This is the redis_cluster path
                        let fut = proxy::cluster::run((*cc_clone).clone());
                        if let Err(e) = fut.await {
                            tracing::error!(
                                "cluster {} exited with error: {}",
                                name_clone,
                                e
                            );
                        }
                    }
                });
                handles.push(handle);
            }
        }

        let mut terminate = signal(SignalKind::terminate()).unwrap();
        let mut interrupt = signal(SignalKind::interrupt()).unwrap();

        tokio::select! {
            _ = terminate.recv() => {
                info!("receive terminate signal");
            },
            _ = interrupt.recv() => {
                info!("receive interrupt signal");
            },
        }

        for handle in &handles {
            handle.abort();
        }

        for handle in handles {
            if let Err(e) = handle.await {
                if !e.is_cancelled() {
                    error!("worker exited with error: {}", e);
                }
            }
        }

        info!("all clusters are closed");
        Ok::<(), AsError>(())
    }) {
        error!("fail to start servers due to {:?}", err);
        return Err(());
    }

    Ok(())
}
