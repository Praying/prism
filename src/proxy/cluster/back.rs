use crate::com::{AsError, ClusterConfig};
use crate::protocol::redis::cmd::Cmd;
use crate::protocol::redis::resp::Message;
use crate::proxy::standalone::Request;
use futures::channel::mpsc;
use futures::{SinkExt, StreamExt};
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpSocket;
use tokio::sync::oneshot;
use tokio::time::{timeout, Duration};
use tokio_util::codec::{Decoder, Encoder, Framed};
use tracing::{error, info};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const BACKEND_TIMEOUT: Duration = Duration::from_secs(10);

pub async fn run(
    cc: Arc<ClusterConfig>,
    addr: String,
    mut rx: mpsc::Receiver<Cmd>,
) -> Result<(), AsError> {
    info!("[back:{}] task started, connecting", addr);

    let sock_addr: SocketAddr = addr.parse()?;
    let socket = if sock_addr.is_ipv4() {
        TcpSocket::new_v4()?
    } else {
        TcpSocket::new_v6()?
    };

    if cc.tcp_keepalive_secs > 0 {
        // Enable TCP keepalive with OS-level defaults.
        socket.set_keepalive(true)?;
    }

    let connect_fut = socket.connect(sock_addr);

    let mut stream = match timeout(CONNECT_TIMEOUT, connect_fut).await {
        Ok(Ok(stream)) => stream,
        Ok(Err(err)) => {
            error!("[back:{}] fail to connect, error is {}", addr, err);
            return Err(AsError::from(err));
        }
        Err(_) => {
            error!("[back:{}] connect timeout", addr);
            return Err(AsError::BackendFail(format!("connect timeout to {}", addr)));
        }
    };
    info!("[back:{}] connected", addr);

    if let Some(password) = cc.redis_auth.as_deref() {
        info!("[back:{}] authenticating", addr);
        let mut cmd =
            crate::protocol::redis::cmd::Cmd::new(crate::protocol::redis::resp::Message::auth(password));
        let (tx, _rx) = tokio::sync::oneshot::channel();
        cmd.set_reply_sender(tx);
        let mut codec =
            <crate::protocol::redis::cmd::Cmd as crate::proxy::standalone::Request>::BackCodec::default();
        let mut buf = bytes::BytesMut::new();
        codec.encode(cmd, &mut buf)?;
        use tokio::io::AsyncWriteExt;
        stream.write_all(&buf).await?;

        let mut resp_buf = bytes::BytesMut::with_capacity(128);
        use tokio::io::AsyncReadExt;
        stream.read_buf(&mut resp_buf).await?;
        let resp = codec.decode(&mut resp_buf)?.ok_or(AsError::BadMessage)?;
        if resp.is_error() {
            error!("[back:{}] auth failed", addr);
            return Err(AsError::BadAuth);
        }
        info!("[back:{}] authenticated", addr);
    }

    let (mut sink, mut stream) = Framed::new(
        stream,
        <crate::protocol::redis::cmd::Cmd as crate::proxy::standalone::Request>::BackCodec::default(),
    )
    .split();

    let mut senders: VecDeque<oneshot::Sender<Message>> = VecDeque::new();

    let mut ping_interval = if cc.redis_ping_interval_secs > 0 {
        Some(tokio::time::interval(Duration::from_secs(
            cc.redis_ping_interval_secs,
        )))
    } else {
        None
    };

    loop {
        tokio::select! {
            // This is the cleanest way to handle an optional interval.
            // The branch is only evaluated if the interval exists.
            _ = async {
                if let Some(interval) = &mut ping_interval {
                    interval.tick().await;
                } else {
                    // This future will be pending forever if interval is None,
                    // effectively disabling this branch.
                    std::future::pending::<()>().await;
                }
            }, if ping_interval.is_some() => {
                info!("[back:{}] sending keepalive ping", addr);
                let mut cmd = Cmd::new(Message::ping());
                let (tx, _rx) = oneshot::channel();
                cmd.set_reply_sender(tx);
                senders.push_back(cmd.take_reply_sender().unwrap());
                if let Err(err) = sink.send(cmd).await {
                    error!("[back:{}] fail to send keepalive ping, error is {}", addr, err);
                    break;
                }
            },
            Some(mut cmd) = rx.next() => {
                if let Some(tx) = cmd.take_reply_sender() {
                    senders.push_back(tx);
                    match timeout(BACKEND_TIMEOUT, sink.send(cmd)).await {
                        Ok(Ok(_)) => {}
                        Ok(Err(err)) => {
                            error!("[back:{}] fail to send cmd to backend, error is {}", addr, err);
                            if let Some(tx) = senders.pop_front() {
                                let resp = Message::from(AsError::BackendFail(err.to_string()));
                                let _ = tx.send(resp);
                            }
                            break;
                        }
                        Err(_) => {
                            error!("[back:{}] timeout sending command to backend", addr);
                            if let Some(tx) = senders.pop_front() {
                               let resp = Message::from(AsError::BackendFail(format!("timeout sending to {}", addr)));
                               let _ = tx.send(resp);
                            }
                            break;
                        }
                    }
                } else {
                    error!("[back:{}] cmd missing reply sender", addr);
                }
            },
            res = timeout(BACKEND_TIMEOUT, stream.next()) => {
                match res {
                    Ok(Some(Ok(resp))) => {
                        if let Some(tx) = senders.pop_front() {
                            let _ = tx.send(resp);
                        } else {
                            error!("[BUG][back:{}] no sender found for response", addr);
                        }
                    },
                    Ok(Some(Err(e))) => {
                        error!("[back:{}] backend stream error: {}", addr, e);
                        break;
                    },
                    Ok(None) => {
                        info!("[back:{}] backend stream closed", addr);
                        break;
                    },
                    Err(_) => {
                        error!("[back:{}] timeout waiting for response from backend", addr);
                        if let Some(tx) = senders.pop_front() {
                           let resp = Message::from(AsError::BackendFail(format!("timeout waiting for response from {}", addr)));
                           let _ = tx.send(resp);
                        }
                        break;
                    }
                }
            }
        }
    }
    info!("[back:{}] task finished", addr);
    Ok(())
}
