use crate::com::AsError;
use crate::proxy::standalone::Request;
use tracing::info;
use futures::{Sink, SinkExt, Stream, StreamExt};
use std::collections::VecDeque;
use std::time::Duration;
use tokio::time;

const MAX_PIPELINE: usize = 512;

pub async fn run<T, I, O, R>(
    cluster: String,
    addr: String,
    mut input: I,
    mut output: O,
    mut recv: R,
    rt: u64,
) -> Result<(), AsError>
where
    T: Request,
    I: Stream<Item = T> + Unpin,
    O: Sink<T, Error = AsError> + Unpin,
    R: Stream<Item = T::Reply> + Unpin,
{
    let timeout = Duration::from_millis(rt);
    let mut cmdq: VecDeque<T> = VecDeque::with_capacity(MAX_PIPELINE);

    loop {
        tokio::select! {
            biased;

            // 1. read response from backend
            Some(msg) = recv.next() => {
                 if let Some(cmd) = cmdq.pop_front() {
                    cmd.set_reply(msg);
                 } else {
                    tracing::error!("unexpected response from backend {}, no matching request", addr);
                 }
            }

            // 2. read request from frontend
            Some(cmd) = input.next() => {
                 if let Err(err) = time::timeout(timeout, output.send(cmd.clone())).await {
                    tracing::error!("fail to send cmd to backend to {} due to timeout", addr);
                    cmd.set_error(&AsError::CmdTimeout);
                    return Err(AsError::CmdTimeout);
                } else {
                    cmd.mark_remote(&cluster);
                    cmdq.push_back(cmd);
                }
            }
        }
    }
}

pub async fn blackhole<T, S>(addr: String, mut input: S)
where
    T: Request,
    S: Stream<Item = T> + Unpin,
{
    while let Some(cmd) = input.next().await {
        info!("backend bloackhole clear the connection for {}", addr);
        cmd.set_error(&AsError::BackendClosedError(addr.clone()));
    }
    info!("backend blackhole exists of {}", addr);
}
