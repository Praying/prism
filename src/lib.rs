// #![deny(warnings)]
extern crate lazy_static;
#[macro_use]
extern crate serde_derive;

extern crate clap;

#[macro_use]
extern crate prometheus;

pub mod metrics;

use clap::{App, Arg};

pub const PRISM_VERSION: &str = env!("CARGO_PKG_VERSION");

pub mod com;
pub mod protocol;
pub mod proxy;
pub(crate) mod utils;

use anyhow::Result;
use tokio::signal;
use tokio::task::JoinHandle;

use com::meta::{load_meta, meta_init};
use com::ClusterConfig;
use tracing::{debug, info};

pub async fn run() -> Result<()> {
    tracing_subscriber::fmt::init();

    let matches = App::new("prism")
        .version(PRISM_VERSION)
        .author("Praying. <codegpt@gmail.com>")
        .about("Prism is a light, fast and powerful cache proxy written in rust.")
        .arg(
            Arg::with_name("config")
                .value_name("FILE")
                .help("Sets a custom config file")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("ip")
                .short('i')
                .long("ip")
                .help("expose given ip for CLUSTER SLOTS/NODES command(may be used by jedis cluster connection).")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("metrics")
                .short('m')
                .long("metrics")
                .help("port to expose prometheus (if compile without 'metrics' feature, this flag will be ignore).")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("version")
                .short('V')
                .long("version")
                .help("show the version of prism."),
        )
        .arg(
            Arg::with_name("reload")
                .short('r')
                .long("reload")
                .help("enable reload feature for standalone proxy mode."),
        )
        .get_matches();
    let config = matches.value_of("config").unwrap_or("default.toml");
    let watch_file = config.to_string();
    let ip = matches.value_of("ip").map(|x| x.to_string());
    let enable_reload = matches.is_present("reload");
    info!("[prism-{}] loading config from {}", PRISM_VERSION, config);
    let cfg = com::Config::load(&config)?;
    debug!("use config : {:?}", cfg);
    assert!(
        !cfg.clusters.is_empty(),
        "clusters is absent of config file"
    );
    // TODO: make reload async
    // crate::proxy::standalone::reload::init(&watch_file, cfg.clone(), enable_reload)?;

    let mut handles = Vec::new();

    for cluster in cfg.clusters.into_iter() {
        if cluster.servers.is_empty() {
            tracing::warn!(
                "fail to running cluster {} in addr {} due filed `servers` is empty",
                cluster.name, cluster.listen_addr
            );
            continue;
        }

        if cluster.name.is_empty() {
            tracing::warn!(
                "fail to running cluster {} in addr {} due filed `name` is empty",
                cluster.name, cluster.listen_addr
            );
            continue;
        }

        info!(
            "starting prism cluster {} in addr {}",
            cluster.name, cluster.listen_addr
        );

        let handle = spawn_cluster(cluster, ip.clone());
        handles.push(handle);
    }

    let port_str = matches.value_of("metrics").unwrap_or("2110");
    let port = port_str.parse::<usize>().unwrap_or(2110);
    let metrics_handle = tokio::spawn(async move {
        // TODO: make metrics::init async
        // metrics::init(port).await
    });
    handles.push(metrics_handle);

    info!("all services started, press ctrl-c to shutdown");
    signal::ctrl_c().await?;
    info!("received ctrl-c, shutting down");

    for handle in handles {
        handle.abort();
    }

    Ok(())
}

fn spawn_cluster(cc: ClusterConfig, ip: Option<String>) -> JoinHandle<()> {
    tokio::spawn(async move {
        // The original logic spawns multiple threads (workers).
        // In the new model, we can spawn multiple async tasks onto the single runtime.
        // For now, we'll just run one task per cluster for simplicity.
        // We can revisit this to spawn multiple tasks if needed for performance.
        let meta = load_meta(cc.clone(), ip);
        info!("setup meta info with {:?}", meta);
        meta_init(meta);

        // The tokio::task::Builder is not available in stable tokio 1.x.
        // We'll just run the cluster logic directly in the spawned task.
        let name = cc.name.clone();
        match &cc.cache_type {
            com::CacheType::RedisCluster => {
                if let Err(e) = proxy::cluster::run(cc).await {
                    tracing::error!("cluster {} exited with error: {}", name, e);
                }
            }
            _ => {
                if let Err(e) = proxy::standalone::run(cc).await {
                    tracing::error!("cluster {} exited with error: {}", name, e);
                }
            }
        }
    })
}
