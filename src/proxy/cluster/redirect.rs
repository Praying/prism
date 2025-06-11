use crate::proxy::standalone::Request;
use tokio::sync::mpsc::Receiver;
use tracing::info;

use crate::com::AsError;
use crate::proxy::cluster::fetcher;
use crate::proxy::cluster::{Cluster, Redirect, Redirection};

use std::sync::Arc;

pub struct RedirectHandler {
    cluster: Arc<Cluster>,
    moved_rx: Receiver<Redirection>,
}

impl RedirectHandler {
    pub fn new(cluster: Arc<Cluster>, moved_rx: Receiver<Redirection>) -> RedirectHandler {
        RedirectHandler {
            cluster,
            moved_rx,
        }
    }

    pub async fn run(&mut self) {
        while let Some(redirection) = self.moved_rx.recv().await {
            self.handle(redirection).await;
        }
        info!("succeed to exits redirection handler");
    }

    async fn handle(&self, redirection: Redirection) {
        let Redirection { target, cmd } = redirection;

        if !cmd.can_cycle() {
            cmd.set_error(&AsError::RequestReachMaxCycle);
            return;
        }

        let (slot, to, is_move) = match target {
            Redirect::Move { slot, to } => (slot, to, true),
            Redirect::Ask { slot, to } => (slot, to, false),
        };
        if is_move {
            info!(
                "cluster {} slot {} was moved to {}",
                self.cluster.cc.lock().unwrap().name,
                slot,
                to
            );
            if self.cluster.update_slot(slot, to.clone()) {
                self.cluster.trigger_fetch(fetcher::TriggerBy::Moved);
            }
        }

        let rc_cmd = cmd.clone();
        if let Err(err) = self.cluster.dispatch_to(&to, cmd).await {
             tracing::error!("fail to dispath moved cmd to backend {} due to {}", to, err);
        } else {
            rc_cmd.add_cycle();
            std::mem::drop(rc_cmd);
        }
    }
}
