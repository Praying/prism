pub mod cmd;
pub mod resp;

use std::sync::{Arc, Mutex};
use std::task::Waker;
use std::time::Instant;

use bytes::BytesMut;
use tokio::sync::oneshot;
use tokio_util::codec::{Decoder, Encoder};

use crate::com::AsError;
use crate::protocol::{CmdFlags, CmdType, IntoReply};
use crate::proxy::standalone::Request;
use crate::utils::trim_hash_tag;

pub use self::resp::Message;

const MAX_CYCLE: u8 = 1;

#[derive(Clone, Debug)]
pub struct Cmd {
    inner: Arc<Mutex<InnerCmd>>,
}

impl Cmd {
    pub fn ctype(&self) -> CmdType {
        self.inner.lock().unwrap().ctype
    }

    pub fn set_ask(&self) {
        self.inner.lock().unwrap().flags.insert(CmdFlags::ASK);
    }

    pub fn unset_moved(&self) {
        self.inner.lock().unwrap().flags.remove(CmdFlags::MOVED);
    }

    pub fn set_moved(&self) {
        self.inner.lock().unwrap().flags.insert(CmdFlags::MOVED);
    }

    pub fn unset_ask(&self) {
        self.inner.lock().unwrap().flags.remove(CmdFlags::ASK);
    }

    pub fn get_reply(&self) -> Option<Message> {
        self.inner.lock().unwrap().reply.clone()
    }
}

impl Request for Cmd {
    type Reply = Message;
    type ReplySender = oneshot::Sender<Message>;
    type FrontCodec = FrontCodec;
    type BackCodec = BackCodec;

    fn ping_request() -> Self {
        let inner = InnerCmd {
            ctype: CmdType::Ctrl,
            flags: CmdFlags::empty(),
            cycle: 0,
            req: Message::ping(),
            reply: None,
            subs: None,
            reply_tx: None,
        };
        Cmd {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    fn reregister(&mut self, _waker: &Waker) {
        // TODO
    }

    fn key_hash(&self, hash_tag: &[u8], hasher: fn(&[u8]) -> u64) -> u64 {
        let inner = self.inner.lock().unwrap();
        let key = inner.req.get_key();
        hasher(trim_hash_tag(key, hash_tag))
    }

    fn subs(&self) -> Option<Vec<Self>> {
        self.inner.lock().unwrap().subs.clone()
    }

    fn is_done(&self) -> bool {
        if let Some(subs) = self.subs() {
            subs.iter().all(|x| x.is_done())
        } else {
            self.inner.lock().unwrap().is_done()
        }
    }

    fn add_cycle(&self) {
        self.inner.lock().unwrap().add_cycle()
    }

    fn can_cycle(&self) -> bool {
        self.inner.lock().unwrap().can_cycle()
    }

    fn is_error(&self) -> bool {
        self.inner.lock().unwrap().is_error()
    }

    fn valid(&self) -> bool {
        // TODO: check this
        true
    }

    fn set_reply<R: IntoReply<Message>>(&self, t: R) {
        let reply = t.into_reply();
        self.inner.lock().unwrap().set_reply(reply);
    }

    fn set_error(&self, err: &AsError) {
        let reply: Message = Message::from(err);
        self.inner.lock().unwrap().set_error(reply);
    }

    fn set_reply_sender(&self, sender: Self::ReplySender) {
        self.inner.lock().unwrap().reply_tx = Some(sender);
    }

    fn mark_total(&self, _cluster: &str) {
        // TODO: metrics
    }

    fn mark_remote(&self, _cluster: &str) {
        // TODO: metrics
    }

    fn get_sendtime(&self) -> Option<Instant> {
        // TODO: metrics
        None
    }
}


impl From<Message> for Cmd {
    fn from(req: Message) -> Self {
        let ctype = cmd::get_cmd_type(&req);
        let subs = if ctype.is_mget() || ctype.is_mset() || ctype.is_del() {
            let mut subs = Vec::with_capacity(req.len() - 1);
            for sub_req in req.mk_subs() {
                subs.push(Cmd::from(sub_req));
            }
            Some(subs)
        } else {
            None
        };

        let inner = InnerCmd {
            ctype,
            flags: CmdFlags::empty(),
            cycle: 0,
            req,
            reply: None,
            subs,
            reply_tx: None,
        };
        Cmd {
            inner: Arc::new(Mutex::new(inner)),
        }
    }
}

#[derive(Debug)]
struct InnerCmd {
    ctype: CmdType,
    flags: CmdFlags,
    cycle: u8,

    req: Message,
    reply: Option<Message>,

    subs: Option<Vec<Cmd>>,
    reply_tx: Option<oneshot::Sender<Message>>,
}

impl InnerCmd {
    fn is_done(&self) -> bool {
        self.flags.contains(CmdFlags::DONE)
    }

    fn is_error(&self) -> bool {
        self.flags.contains(CmdFlags::ERROR)
    }

    pub fn can_cycle(&self) -> bool {
        self.cycle < MAX_CYCLE
    }

    pub fn add_cycle(&mut self) {
        self.cycle += 1;
    }

    pub fn set_reply(&mut self, reply: Message) {
        self.reply = Some(reply);
        self.set_done();

        if let Some(tx) = self.reply_tx.take() {
            if let Some(reply) = self.reply.as_ref() {
                let _ = tx.send(reply.clone());
            }
        }
    }

    pub fn set_error(&mut self, reply: Message) {
        self.set_reply(reply);
        self.flags.insert(CmdFlags::ERROR);
    }

    fn set_done(&mut self) {
        self.flags.insert(CmdFlags::DONE);
    }
}

#[derive(Default)]
pub struct FrontCodec {}

impl Decoder for FrontCodec {
    type Item = Cmd;
    type Error = AsError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match Message::parse(src) {
            Ok(Some(msg)) => Ok(Some(Cmd::from(msg))),
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

impl Encoder<Cmd> for FrontCodec {
    type Error = AsError;

    fn encode(&mut self, item: Cmd, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let inner = item.inner.lock().unwrap();
        if let Some(subs) = inner.subs.as_ref() {
            let mut replies = Vec::with_capacity(subs.len());
            for sub in subs.iter() {
                let sub_inner = sub.inner.lock().unwrap();
                if let Some(reply) = sub_inner.reply.as_ref() {
                    replies.push(reply.clone());
                } else {
                    // should not happen
                    return Err(AsError::BadReply);
                }
            }
            let reply = Message::from_msgs(replies);
            reply.save(dst);
        } else if let Some(reply) = inner.reply.as_ref() {
            reply.save(dst);
        }
        Ok(())
    }
}

#[derive(Default)]
pub struct BackCodec {}

impl Decoder for BackCodec {
    type Item = Message;
    type Error = AsError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        Message::parse(src)
    }
}

impl Encoder<Cmd> for BackCodec {
    type Error = AsError;

    fn encode(&mut self, item: Cmd, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let inner = item.inner.lock().unwrap();
        inner.req.save(dst);
        Ok(())
    }
}
