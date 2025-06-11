# Prism

```
  ____  _     _     _     __  __
 |  _ \| |__ (_) __| | __|  \/  |
 | |_) | '_ \| |/ _` |/ _` |\/| |
 |  __/| | | | | (_| | (_| |  | |
 |_|   |_| |_|_|\__,_|\__,_|  |_|

```

A lightweight, high-performance, and powerful cache proxy for Redis and Memcache, written in Rust.

[![GitHub Actions Status](https://github.com/Praying/prism/actions/workflows/rust.yml/badge.svg)](https://github.com/Praying/prism/actions)
[![Crates.io](https://img.shields.io/crates/v/prism-proxy.svg)](https://crates.io/crates/prism-proxy)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](/LICENSE)

Prism is a versatile cache proxy that supports multiple protocols and proxying models, designed to be a drop-in replacement for services like [Twemproxy](https://github.com/twitter/twemproxy) with added features and superior performance.

## 🌟 Features

*   **High Performance:** Built with Rust, offering exceptional speed and low resource consumption.
*   **Multiple Protocols:** Supports Memcache, Redis (standalone), and Redis Cluster protocols.
*   **Flexible Proxy Models:**
    *   **Proxy Mode:** Standard proxying, compatible with Twemproxy.
    *   **Cluster Mode:** Allows non-cluster Redis clients to seamlessly connect to a Redis Cluster. (Inspired by [Corvus](https://github.com/eleme/corvus))
*   **Connection Pooling:** Efficiently manages connections to backend servers.
*   **Health Checking:** Actively pings backend nodes and can automatically eject failed nodes.
*   **Read from Slave:** Supports read balancing from slave nodes in a Redis setup.
*   **Configuration Hot-Reload:** Configuration can be reloaded without service interruption.

## 🚀 Getting Started

### Prerequisites

You need to have the Rust toolchain installed. If you don't have it, you can install it via [rustup](https://rustup.rs/).

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

### Build & Run

1.  Clone the repository:
    ```bash
    git clone https://github.com/path-to-your-repo/prism.git
    cd prism
    ```

2.  Build the project in release mode:
    ```bash
    cargo build --release
    ```

3.  Run Prism with a configuration file:
    ```bash
    RUST_LOG=info RUST_BACKTRACE=1 ./target/release/prism default.toml
    ```
    *   `RUST_LOG=info`: Sets the logging level.
    *   `RUST_BACKTRACE=1`: Shows a backtrace on panic.

## ⚙️ Configuration

Prism uses a TOML file for configuration. Here is an example demonstrating the main options.

```toml
# default.toml

# You can define multiple clusters. Each will listen on a different port.
[[clusters]]
# A unique name for the cluster.
name = "test-redis-cluster"

# The address and port for the proxy to listen on.
listen_addr = "0.0.0.0:9001"

# The type of backend cache.
# Supported values: "memcache", "redis", "redis_cluster"
cache_type = "redis_cluster"

# Backend server list. The format depends on `cache_type`.
#
# For `cache_type = "redis_cluster"`, list seed nodes:
servers = ["127.0.0.1:7000", "127.0.0.1:7001"]
#
# For `cache_type = "redis"` or `"memcache"`, use the format "${addr}:${weight} ${alias}":
# servers = [
#     "127.0.0.1:7001:10 redis-1",
#     "127.0.0.1:7002:10 redis-2",
# ]

# Number of worker threads. It's recommended to set this to the number of CPU cores.
thread = 4

# Socket read timeout in milliseconds.
read_timeout = 2000

# Socket write timeout in milliseconds.
write_timeout = 2000


############################
# Cluster Mode Configuration
############################
# (Only for `cache_type = "redis_cluster"`)

# How often to fetch cluster topology information, in seconds. Default is 600.
fetch = 600

# Enable to allow read commands to be sent to slave nodes for load balancing.
read_from_slave = true


##########################
# Proxy Mode Configuration
##########################
# (Only for `cache_type = "redis"` or `"memcache"`)

# The node will be ejected after this many consecutive ping failures.
# Set to 0 to disable health checking and ejection.
ping_fail_limit = 3

# The interval for pinging backend nodes, in milliseconds.
ping_interval = 10000
```

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a pull request or open an issue.

## 📜 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgements

Prism is a fork of and inspired by [aster](https://github.com/wayslog/aster), originally created by [wayslog](https://github.com/wayslog). We are grateful for their foundational work.
