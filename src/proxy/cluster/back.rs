use crate::com::AsError;
use crate::protocol::redis::{Cmd, Message};
use crate::proxy::cluster::Redirection;
use crate::proxy::standalone::Request;
use futures::{Sink, SinkExt, Stream, StreamExt};
use std::collections::VecDeque;
use std::time::Duration;
use tokio::sync::mpsc::Sender;
use tokio::time;
use tracing::{debug, info};

const MAX_PIPELINE: usize = 8 * 1024;

pub async fn run<I, O, R>(
    cluster: String,
    addr: String,
    mut input: I,
    mut output: O,
    mut recv: R,
    moved: Sender<Redirection>,
    rt: u64,
) -> Result<(), AsError>
where
    I: Stream<Item = Result<Cmd, ()>> + Unpin,
    O: Sink<Cmd, Error = AsError> + Unpin,
    R: Stream<Item = Message> + Unpin,
{
    let inner_err = AsError::ConnClosed(addr.clone());
    let mut cmdq: VecDeque<Cmd> = VecDeque::with_capacity(MAX_PIPELINE);
    let timeout = Duration::from_millis(rt);

    loop {
        tokio::select! {
            // biased select to prioritize reading from the backend
            biased;

            // 1. read response from backend
            Some(msg) = recv.next() => {

                let cmd = match cmdq.pop_front() {
                    Some(cmd) => cmd,
                    None => {
                        tracing::error!("unexpected response from backend {}, no matching request", addr);
                        continue;
                    }
                };

                if let Some(redirect) = msg.check_redirect() {
                    cmd.add_cycle();
                    if redirect.is_ask() {
                        cmd.set_ask();
                        cmd.unset_moved();
                    } else {
                        cmd.set_moved();
                        cmd.unset_ask();
                    }

                    let redirection = Redirection {
                        target: redirect,
                        cmd,
                    };

                    if let Err(se) = moved.send(redirection).await {
                        let red: Redirection = se.0;
                        tracing::error!("fail to redirect cmd {:?}", red.target);
                        red.cmd.set_error(&AsError::RedirectFailError);
                        return Err(AsError::RedirectFailError);
                    }
                } else {
                    cmd.set_reply(msg);
                }
            }

            // 2. read request from frontend
            Some(result) = input.next() => {
                let cmd = match result {
                    Ok(cmd) => cmd,
                    Err(_) => {
                        // input stream is closed
                        break;
                    }
                };

                if let Err(err) = time::timeout(timeout, output.send(cmd.clone())).await {
                    tracing::error!("fail to send cmd to backend to {} due to timeout", addr);
                    cmd.set_error(&AsError::CmdTimeout);
                    return Err(AsError::CmdTimeout);
                } else {
                    // cmd.cluster_mark_remote(&cluster);
                    cmdq.push_back(cmd);
                }
            }
        }
    }

    // cleanup
    for cmd in cmdq.drain(..) {
        cmd.set_error(&inner_err);
    }

    Ok(())
}

pub async fn blackhole<S>(addr: String, mut input: S)
where
    S: Stream<Item = Cmd> + Unpin,
{
    let inner_err = AsError::ConnClosed(addr.clone());
    while let Some(cmd) = input.next().await {
        debug!(
            "backend close cmd in blackhole addr:{} and cmd:{:?}",
            addr, cmd
        );
        cmd.set_error(&inner_err);
    }
    info!("backend blackhole exists of {}", addr);
}
