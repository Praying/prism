use crate::com::AsError;
use crate::proxy::cluster::Cluster;
use crate::proxy::standalone::Request;

use futures::{SinkExt, StreamExt};
use log::{error, info};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use std::sync::Arc;
use tokio::sync::oneshot;

pub async fn run(stream: TcpStream, cluster: Arc<Cluster>) -> Result<(), AsError> {
    let peer_addr = stream.peer_addr().map(|addr| addr.to_string()).unwrap_or_else(|_| "unknown".to_string());
    info!("[front:{}] new connection", peer_addr);

    let codec = <crate::protocol::redis::cmd::Cmd as Request>::FrontCodec::default();
    let (mut sink, mut stream) = Framed::new(stream, codec).split();

    loop {
        //info!("[front:{}] waiting for command", peer_addr);
        match stream.next().await {
            Some(Ok(mut cmd)) => {
                //info!("[front:{}] received command, dispatching", peer_addr);
                let (tx, rx) = oneshot::channel();
                cmd.set_reply_sender(tx);
                if let Err(err) = cluster.dispatch(cmd).await {
                    if matches!(err, AsError::Canceled) {
                        //info!("[front:{}] dispatch command failed because client connection may be closed", peer_addr);
                        break Ok(());
                    }
                    error!("[front:{}] fail to dispatch cmd to cluster, error is {}", peer_addr, err);
                    break Err(err);
                }
                //info!("[front:{}] dispatch command returned, now waiting for reply", peer_addr);

                match rx.await {
                    Ok(reply) => {
                        //info!("[front:{}] received reply from back task", peer_addr);
                        if let Err(err) = sink.send(reply).await {
                            error!("[front:{}] fail to send reply to client, error is {}", peer_addr, err);
                            break Err(err);
                        }
                        //info!("[front:{}] sent reply to client successfully", peer_addr);
                    }
                    Err(err) => {
                       info!(
                           "[front:{}] fail to recv from oneshot, maybe connection closed, error is {}",
                           peer_addr, err
                       );
                       break Ok(());
                   }
                }
            }
            Some(Err(err)) => {
                if matches!(err, AsError::Canceled) {
                    info!("[front:{}] connection closed", peer_addr);
                    break Ok(());
                }
                error!("[front:{}] codec error {}", peer_addr, err);
                break Err(err);
            }
            None => {
                info!("[front:{}] connection closed by peer", peer_addr);
                break Ok(());
            }
        }
    }
}
