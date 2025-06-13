use crate::com::AsError;
use crate::protocol::redis::cmd::Cmd;
use crate::protocol::redis::resp::Message;
use tokio::sync::oneshot;

use std::str;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Redirect {
    Moved { slot: u64, addr: String },
    Asking { addr: String },
}

impl Redirect {
    pub fn parse_from(resp: &str) -> Option<Self> {
        let parts: Vec<&str> = resp.split_whitespace().collect();
        if parts.is_empty() {
            return None;
        }

        match parts[0] {
            "MOVED" => {
                if parts.len() == 3 {
                    let slot = parts[1].parse().ok()?;
                    let addr = parts[2].to_string();
                    Some(Redirect::Moved { slot, addr })
                } else {
                    None
                }
            }
            "ASKING" => {
                if parts.len() == 2 {
                    let addr = parts[1].to_string();
                    Some(Redirect::Asking { addr })
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}

pub struct Redirection {
    cmd: Cmd,
    reply_sender: Option<oneshot::Sender<Message>>,
    redirect: Option<Redirect>,
}

impl Redirection {
    pub fn new(mut cmd: Cmd) -> Self {
        let reply_sender = cmd.take_reply_sender();
        Self {
            cmd,
            reply_sender: reply_sender,
            redirect: None,
        }
    }

    pub fn get_cmd(&self) -> Cmd {
        // Since we need to retry, we clone the command but clear the sender.
        // The real sender is stored in the Redirection struct.
        let mut new_cmd = self.cmd.clone();
        new_cmd.take_reply_sender();
        new_cmd
    }

    pub fn set_redirect(&mut self, redirect: Redirect) {
        self.redirect = Some(redirect);
    }

    pub fn take_redirect_addr(&mut self) -> Option<(String, bool)> {
        if let Some(redirect) = self.redirect.take() {
            match redirect {
                Redirect::Moved { addr, .. } => Some((addr, false)),
                Redirect::Asking { addr } => Some((addr, true)),
            }
        } else {
            None
        }
    }

    pub async fn return_ok(mut self, resp: Message) -> Result<(), AsError> {
        if let Some(sender) = self.reply_sender.take() {
            sender.send(resp).map_err(|_| AsError::Canceled)
        } else {
            Ok(())
        }
    }

    pub async fn return_err(mut self, err: AsError) -> Result<(), AsError> {
        if let Some(sender) = self.reply_sender.take() {
            sender
                .send(Message::from(err))
                .map_err(|_| AsError::Canceled)
        } else {
            Ok(())
        }
    }
}
