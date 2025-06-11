use crate::com::AsError;
use crate::metrics::front_conn_decr;
use crate::proxy::standalone::{Cluster, Request};
use futures::{Sink, SinkExt, Stream, StreamExt};
use std::collections::VecDeque;
use std::sync::Arc;
use tracing::{error, warn};

const MAX_BATCH_SIZE: usize = 2048;

pub async fn run<T, I, O>(
    client: String,
    cluster: Arc<Cluster<T>>,
    mut input: I,
    mut output: O,
) -> Result<(), AsError>
where
    T: Request + 'static,
    I: Stream<Item = Result<T, AsError>> + Unpin,
    O: Sink<T, Error = AsError> + Unpin,
{
    let mut sendq = VecDeque::with_capacity(MAX_BATCH_SIZE);
    let mut waitq = VecDeque::with_capacity(MAX_BATCH_SIZE);

    loop {
        tokio::select! {
            // 1. read from client
            result = input.next(), if sendq.is_empty() => {
                let cmd = match result {
                    Some(Ok(cmd)) => cmd,
                    Some(Err(err)) => {
                        error!("fail to read from client {} due to {}", client, err);
                        break;
                    }
                    None => {
                        // client closed
                        break;
                    }
                };

                cmd.mark_total(&cluster.cc.lock().unwrap().name);
                if cmd.valid() && !cmd.is_done() {
                    if let Some(subs) = cmd.subs() {
                        sendq.extend(subs.into_iter());
                    } else {
                        sendq.push_back(cmd.clone());
                    }
                }
                waitq.push_back(cmd);
            },

            // 2. dispatch to backend
            result = dispatch_all(&cluster, &mut sendq), if !sendq.is_empty() => {
                if let Err(err) = result {
                    error!("fail to dispatch to backend due to {}", err);
                    // In this case, we can't do much more than closing the connection.
                    // The commands in waitq will be replied with an error.
                    break;
                }
            },

            // 3. reply to client
            result = reply_all(&mut output, &mut waitq), if !waitq.is_empty() && waitq.iter().all(|cmd: &T| cmd.is_done()) => {
                 if let Err(err) = result {
                    error!("fail to reply to client {} {}", client, err);
                    break;
                 }
            },
        }

        if waitq.len() > MAX_BATCH_SIZE {
            warn!(
                "client {} waitq size is larger than {}, maybe something is wrong",
                client, MAX_BATCH_SIZE
            );
        }
    }

    front_conn_decr(&cluster.cc.lock().unwrap().name);
    Ok(())
}

async fn reply_all<T, O>(output: &mut O, waitq: &mut VecDeque<T>) -> Result<(), AsError>
where
    T: Request,
    O: Sink<T, Error = AsError> + Unpin,
{
    while let Some(cmd) = waitq.pop_front() {
        if let Err(err) = output.send(cmd).await {
            return Err(err);
        }
    }
    output.flush().await
}

async fn dispatch_all<T: Request + 'static>(
    cluster: &Arc<Cluster<T>>,
    sendq: &mut VecDeque<T>,
) -> Result<(), AsError> {
    while let Some(cmd) = sendq.pop_front() {
        let key = cmd.key_hash(&cluster.hash_tag, crate::proxy::standalone::fnv::fnv1a64);
        let addr = cluster.ring.lock().unwrap().get_node(key).map(|s| s.to_string());
        if let Some(addr) = addr {
            let mut conns = cluster.conns.lock().unwrap();
            if let Some(conn) = conns.get_mut(&addr) {
                if let Err(err) = conn.sender().send(cmd).await {
                    error!("fail to send cmd to {} due to {}", addr, err);
                    // If send fails, we should probably put it back to the sendq and retry.
                    // But for simplicity, we just drop it.
                }
            } else {
                // TODO: handle node not found
            }
        } else {
            // TODO: handle node not found
        }
    }
    Ok(())
}
