use tokio::time::{self, Interval};

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use crate::protocol::IntoReply;
use crate::proxy::standalone::{Cluster, Request};
use tracing::{debug, info};

#[derive(Debug)]
enum State<T> {
    Justice(bool),
    OnFail,
    OnSuccess,
    Sending(T),
    Waitting(T),
}

pub struct Ping<T: Request> {
    cluster: Weak<Cluster<T>>,

    name: String,
    addr: String,

    fail_interval: Interval,
    succ_interval: Interval,

    count: u8,
    limit: u8,

    state: State<T>,
    cancel: Arc<AtomicBool>,
}

impl<T: Request + Send + 'static + From<crate::protocol::redis::cmd::Cmd>> Ping<T>
where
    T::Reply: IntoReply<crate::protocol::redis::resp::Message>,
{
    pub fn new(
        cluster: Weak<Cluster<T>>,
        name: String,
        addr: String,
        cancel: Arc<AtomicBool>,
        interval_millis: u64,
        succ_interval_millis: u64,
        limit: u8,
    ) -> Ping<T> {
        let fail_interval = time::interval_at(
            tokio::time::Instant::now() + Duration::from_secs(1),
            Duration::from_millis(interval_millis),
        );
        let succ_interval = time::interval_at(
            tokio::time::Instant::now() + Duration::from_secs(1),
            Duration::from_millis(succ_interval_millis),
        );

        Ping {
            cluster,
            name,
            addr,
            fail_interval,
            succ_interval,
            limit,
            count: 0,
            state: State::OnSuccess,
            cancel,
        }
    }

    pub async fn run(mut self) {
        loop {
            if self.cancel.load(Ordering::SeqCst) {
                info!(
                    "ping to {}({}) was canceld by handle",
                    self.name, self.addr
                );
                return;
            }

            let state = std::mem::replace(&mut self.state, State::Justice(false));
            match state {
                State::Justice(is_last_succ) => {
                    if is_last_succ {
                        if self.count > self.limit {
                            // removed but success next time
                            let cluster = self.cluster.upgrade();
                            if let Some(_cluster) = cluster {
                                let _name = self.name.clone();
                                tokio::spawn(async move {
                                    // TODO: add node by reload
                                });
                            } else {
                                return;
                            }
                        }
                        self.count = 0;
                        self.state = State::OnSuccess;
                    } else {
                        self.count = self.count.wrapping_add(1);
                        debug!("ping state fail count={} limit={}", self.count, self.limit);

                        #[allow(clippy::comparison_chain)]
                        if self.count == self.limit {
                            if let Some(_cluster) = self.cluster.upgrade() {
                                info!("remove node={} addr={} by ping error", self.name, self.addr);
                                // TODO: remove node by reload
                            } else {
                                return;
                            }
                            self.state = State::OnFail;
                        } else if self.count > self.limit {
                            self.state = State::OnFail;
                        } else {
                            self.state = State::OnSuccess;
                        }

                        let cluster = self.cluster.upgrade();
                        if let Some(_cluster) = cluster {
                            let _addr = self.addr.clone();
                            tokio::spawn(async move {
                                // TODO: reconnect by reload
                            });
                        } else {
                            return;
                        }
                    }
                }
                State::OnSuccess => {
                    self.succ_interval.tick().await;
                    let cmd = T::ping_request();
                    self.state = State::Sending(cmd);
                }
                State::OnFail => {
                    self.fail_interval.tick().await;
                    let cmd = T::ping_request();
                    self.state = State::Sending(cmd);
                }
                State::Sending(cmd) => {
                    let rc_cmd = cmd.clone();
                    if !rc_cmd.can_cycle() {
                        self.state = State::Justice(false);
                        continue;
                    }
                    let sender = if let Some(cluster) = self.cluster.upgrade() {
                        let mut conns = cluster.conns.lock().unwrap();
                        if let Some(conn) = conns.get_mut(&self.addr) {
                            Some(conn.sender().clone())
                        } else {
                            info!("connection to {} not found for ping", self.addr);
                            None
                        }
                    } else {
                        info!("cluster has drooped and exit ping");
                        return;
                    };

                    if let Some(sender) = sender {
                        match sender.send(rc_cmd).await {
                            Ok(_) => {
                                self.state = State::Waitting(cmd);
                            }
                            Err(err) => {
                                info!("fail to dispatch_to {} due to {:?}", self.addr, err);
                                self.state = State::Justice(false);
                            }
                        }
                    } else {
                        self.state = State::Justice(false);
                    }
                }

                State::Waitting(ref cmd) => {
                    // This is a busy loop, which is not ideal.
                    // A better way would be to use a waker.
                    // The `Request` trait has `reregister`, which is now removed from the call sites.
                    // For now, a small sleep is added to prevent pegging the CPU.
                    if !cmd.is_done() {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                        self.state = state; // put it back
                        continue;
                    }
                    self.state = State::Justice(!cmd.is_error());
                }
            }
        }
    }
}

