use crate::com::AsError;
use crate::proxy::standalone::Request;
use futures::{Sink, SinkExt, Stream, StreamExt};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use tracing::{error, info};

pub async fn run_redis<
    Downstream: Stream<Item = crate::protocol::redis::cmd::Cmd> + Unpin,
    UpstreamSink: Sink<crate::protocol::redis::cmd::Cmd, Error = AsError> + Unpin,
    UpstreamStream: Stream<Item = Result<crate::protocol::redis::resp::Message, AsError>> + Unpin,
>(
    address: String, mut downstream: Downstream, mut upstream_sink: UpstreamSink,
    mut upstream_stream: UpstreamStream,
) -> Result<(), AsError> {
    let pending_cmds = Arc::new(Mutex::new(VecDeque::new()));

    loop {
        tokio::select! {
            item = downstream.next() => {
                match item {
                    Some(req) => {
                        pending_cmds.lock().unwrap().push_back(req.clone());
                        if let Err(err) = upstream_sink.send(req).await {
                            error!("fail to send request to {} due to {}, will close the connection", address, err);
                            // Fail all pending commands
                            for mut cmd in pending_cmds.lock().unwrap().drain(..) {
                                cmd.set_error(err.clone());
                            }
                            return Err(err);
                        }
                    }
                    None => {
                        info!("downstream of {} is closed", address);
                        return Ok(());
                    }
                }
            },
            item = upstream_stream.next() => {
                match item {
                    Some(Ok(rep)) => {
                        if let Some(mut cmd) = pending_cmds.lock().unwrap().pop_front() {
                            cmd.set_reply(rep);
                        } else {
                            error!("got an unexpected reply from {}", address);
                        }
                    }
                    Some(Err(err)) => {
                        error!("fail to read reply from {} due to {}", address, err);
                        // Fail all pending commands
                        for mut cmd in pending_cmds.lock().unwrap().drain(..) {
                            cmd.set_error(err.clone());
                        }
                        return Err(err);
                    }
                    None => {
                        info!("upstream of {} is closed", address);
                        // Fail all pending commands
                        let err = AsError::BackendClosedError(address.clone());
                        for mut cmd in pending_cmds.lock().unwrap().drain(..) {
                            cmd.set_error(err.clone());
                        }
                        return Ok(());
                    }
                }
            }
        }
    }
}

pub async fn run_mc<
    Downstream: Stream<Item = crate::protocol::mc::Cmd> + Unpin,
    UpstreamSink: Sink<crate::protocol::mc::Cmd, Error = AsError> + Unpin,
    UpstreamStream: Stream<Item = Result<crate::protocol::mc::Message, AsError>> + Unpin,
>(
    address: String, mut downstream: Downstream, mut upstream_sink: UpstreamSink,
    mut upstream_stream: UpstreamStream,
) -> Result<(), AsError> {
    let pending_cmds = Arc::new(Mutex::new(VecDeque::new()));

    loop {
        tokio::select! {
            item = downstream.next() => {
                match item {
                    Some(req) => {
                        pending_cmds.lock().unwrap().push_back(req.clone());
                        if let Err(err) = upstream_sink.send(req).await {
                            error!("fail to send request to {} due to {}, will close the connection", address, err);
                            // Fail all pending commands
                            for mut cmd in pending_cmds.lock().unwrap().drain(..) {
                                cmd.set_error(err.clone());
                            }
                            return Err(err);
                        }
                    }
                    None => {
                        info!("downstream of {} is closed", address);
                        return Ok(());
                    }
                }
            },
            item = upstream_stream.next() => {
                match item {
                    Some(Ok(rep)) => {
                        if let Some(mut cmd) = pending_cmds.lock().unwrap().pop_front() {
                            cmd.set_reply(rep);
                        } else {
                            error!("got an unexpected reply from {}", address);
                        }
                    }
                    Some(Err(err)) => {
                        error!("fail to read reply from {} due to {}", address, err);
                        for mut cmd in pending_cmds.lock().unwrap().drain(..) {
                            cmd.set_error(err.clone());
                        }
                        return Err(err);
                    }
                    None => {
                        info!("upstream of {} is closed", address);
                        let err = AsError::BackendClosedError(address.clone());
                        for mut cmd in pending_cmds.lock().unwrap().drain(..) {
                            cmd.set_error(err.clone());
                        }
                        return Ok(());
                    }
                }
            }
        }
    }
}

use bytes::BytesMut;
use tokio_util::codec::{Decoder, Encoder};

#[derive(Default)]
pub struct Codec {}

impl Encoder<crate::protocol::redis::cmd::Cmd> for Codec {
    type Error = AsError;
    fn encode(
        &mut self, item: crate::protocol::redis::cmd::Cmd, dst: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        dst.extend_from_slice(&item.msg.data);
        Ok(())
    }
}

impl Decoder for Codec {
    type Item = crate::protocol::redis::resp::Message;
    type Error = AsError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        crate::protocol::redis::resp::Message::parse(src)
    }
}
