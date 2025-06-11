use crate::com::AsError;
use crate::protocol::redis::Cmd;
use crate::proxy::cluster::fetcher::TriggerBy;
use crate::proxy::cluster::Cluster;
use crate::proxy::standalone::Request;

use futures::{Sink, Stream, SinkExt, StreamExt};
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::time::{self, Duration};
use tracing::error;

const MAX_BATCH_SIZE: usize = 8 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum State {
    Running,
    Closed,
}

pub struct Front<I, O>
where
    I: Stream<Item = Result<Cmd, AsError>> + Unpin,
    O: Sink<Cmd, Error = AsError> + Unpin,
{
    cluster: Arc<Cluster>,

    client: String,

    input: I,
    output: O,

    sendq: VecDeque<Cmd>,
    waitq: VecDeque<Cmd>,

    state: State,
}

impl<I, O> Front<I, O>
where
    I: Stream<Item = Result<Cmd, AsError>> + Unpin,
    O: Sink<Cmd, Error = AsError> + Unpin,
{
    pub fn new(client: String, cluster: Arc<Cluster>, input: I, output: O) -> Front<I, O> {
        Front {
            cluster,
            client,
            input,
            output,
            sendq: VecDeque::with_capacity(MAX_BATCH_SIZE),
            waitq: VecDeque::with_capacity(MAX_BATCH_SIZE),
            state: State::Running,
        }
    }

    async fn run(mut self) {
        let mut tick = time::interval(Duration::from_secs(1));
        loop {
            tokio::select! {
                _ = tick.tick() => {
                    if self.state == State::Closed {
                        return
                    }
                }

                ret = self.input.next() => {
                    let cmd = match ret {
                        Some(Ok(cmd)) => cmd,
                        Some(Err(err)) => {
                            error!("fail to read from client {} due to {}", self.client, err);
                            self.state = State::Closed;
                            continue;
                        }
                        None => {
                            self.state = State::Closed;
                            continue;
                        }
                    };

                    cmd.mark_total(&self.cluster.cc.lock().unwrap().name);

                    if cmd.valid() && !cmd.is_done() {
                        // for done command, never send to backend
                        if let Some(subs) = cmd.subs() {
                            self.sendq.extend(subs.into_iter());
                        } else {
                            self.sendq.push_back(cmd.clone());
                        }
                    }
                    self.waitq.push_back(cmd);
                }
            }

            self.try_send().await;
            self.try_reply().await;
        }
    }

    async fn try_reply(&mut self) -> Result<(), AsError> {
        let mut count = 0usize;
        loop {
            if self.waitq.is_empty() {
                break;
            }

            let cmd = self.waitq.pop_front().expect("command never be error");

            if !cmd.is_done() {
                self.waitq.push_front(cmd);
                break;
            }

            if cmd.is_error() {
                self.cluster.trigger_fetch(TriggerBy::Error);
            }

            if let Err(err) = self.output.send(cmd).await {
                error!("fail to reply to client {} {}", self.client, err);
                self.output.close().await?;
                return Err(err);
            }
            count += 1;
        }
        if count > 0 {
            self.output.flush().await?;
        }
        Ok(())
    }

    async fn try_send(&mut self) -> Result<usize, AsError> {
        self.cluster.dispatch_all(&mut self.sendq).await
    }
}

impl<I, O> Drop for Front<I, O>
where
    I: Stream<Item = Result<Cmd, AsError>> + Unpin,
    O: Sink<Cmd, Error = AsError> + Unpin,
{
    fn drop(&mut self) {
        crate::metrics::front_conn_decr(&self.cluster.cc.lock().unwrap().name);
    }
}
