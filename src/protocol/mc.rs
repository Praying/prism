use bytes::BytesMut;
use std::task::Waker;

use tokio_util::codec::{Decoder, Encoder};

use crate::metrics::*;

use crate::com::AsError;
use crate::protocol::{CmdFlags, CmdType, IntoReply};
use crate::proxy::standalone::Request;
use crate::utils::trim_hash_tag;

use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

pub mod msg;
pub use self::msg::Message;
use std::time::Instant;

const MAX_CYCLE: u8 = 1;

#[derive(Clone)]
pub struct Cmd {
    cmd: Arc<Mutex<Command>>,
}

impl Request for Cmd {
    type Reply = Message;
    type ReplySender = oneshot::Sender<Message>;
    type FrontCodec = FrontCodec;
    type BackCodec = BackCodec;

    fn ping_request() -> Self {
        let cmd = Command {
            ctype: CmdType::Read,
            flags: CmdFlags::empty(),
            cycle: 0,

            req: Message::version_request(),
            reply: None,
            subs: None,

            total_tracker: None,

            remote_tracker: None,
            reply_tx: None,
        };
        Cmd {
            cmd: Arc::new(Mutex::new(cmd)),
        }
    }

    fn reregister(&mut self, _waker: &Waker) {
        // self.notify.set_task(task);
        // TODO: update Notify to store Waker.
    }

    fn key_hash(&self, hash_tag: &[u8], hasher: fn(&[u8]) -> u64) -> u64 {
        let cmd = self.cmd.lock().unwrap();
        let key = cmd.req.get_key();
        hasher(trim_hash_tag(key, hash_tag))
    }

    fn subs(&self) -> Option<Vec<Self>> {
        self.cmd.lock().unwrap().subs.clone()
    }

    fn is_done(&self) -> bool {
        if let Some(subs) = self.subs() {
            subs.iter().all(|x| x.is_done())
        } else {
            self.cmd.lock().unwrap().is_done()
        }
    }

    fn add_cycle(&self) {
        self.cmd.lock().unwrap().add_cycle()
    }
    fn can_cycle(&self) -> bool {
        self.cmd.lock().unwrap().can_cycle()
    }

    fn is_error(&self) -> bool {
        self.cmd.lock().unwrap().is_error()
    }

    fn valid(&self) -> bool {
        true
    }

    fn set_reply<R: IntoReply<Message>>(&self, t: R) {
        let reply = t.into_reply();
        self.cmd.lock().unwrap().set_reply(reply);
    }

    fn set_error(&self, t: &AsError) {
        let reply: Message = t.into_reply();
        self.cmd.lock().unwrap().set_error(reply);
    }

    fn set_reply_sender(&self, sender: Self::ReplySender) {
        self.cmd.lock().unwrap().reply_tx = Some(sender);
    }

    fn mark_total(&self, cluster: &str) {
        let timer = total_tracker(cluster);
        self.cmd.lock().unwrap().total_tracker.replace(timer);
    }

    fn mark_remote(&self, cluster: &str) {
        let timer = remote_tracker(cluster);
        self.cmd.lock().unwrap().remote_tracker.replace(timer);
    }

    fn get_sendtime(&self) -> Option<Instant> {
        let mut c = self.cmd.lock().unwrap();
        match c.remote_tracker.take() {
            Some(t) => {
                let s = t.start.clone();
                c.remote_tracker = Some(t);
                Some(s)
            }
            None => None,
        }
    }
}

impl Cmd {
    fn from_msg(msg: Message) -> Cmd {
        let flags = CmdFlags::empty();
        let ctype = CmdType::Read;
        let sub_msgs = msg.mk_subs();

        let subs: Vec<_> = sub_msgs
            .into_iter()
            .map(|sub_msg| {
                let command = Command {
                    ctype,
                    flags,
                    cycle: 0,
                    req: sub_msg,
                    reply: None,
                    subs: None,

                    total_tracker: None,

                    remote_tracker: None,
                    reply_tx: None,
                };
                Cmd {
                    cmd: Arc::new(Mutex::new(command)),
                }
            })
            .collect();
        let subs = if subs.is_empty() { None } else { Some(subs) };
        let command = Command {
            ctype: CmdType::Read,
            flags: CmdFlags::empty(),
            cycle: 0,
            req: msg,
            reply: None,
            subs,

            total_tracker: None,

            remote_tracker: None,
            reply_tx: None,
        };
        Cmd {
            cmd: Arc::new(Mutex::new(command)),
        }
    }
}

impl From<Message> for Cmd {
    fn from(msg: Message) -> Cmd {
        Cmd::from_msg(msg)
    }
}

#[allow(unused)]
pub struct Command {
    ctype: CmdType,
    flags: CmdFlags,
    cycle: u8,

    req: Message,
    reply: Option<Message>,

    subs: Option<Vec<Cmd>>,

    total_tracker: Option<Tracker>,

    remote_tracker: Option<Tracker>,
    reply_tx: Option<oneshot::Sender<Message>>,
}

impl Command {
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

        let _ = self.remote_tracker.take();
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
        match Message::parse(src).map(|x| x.map(Into::into)) {
            Ok(val) => Ok(val),
            Err(AsError::BadMessage) => {
                let cmd: Cmd = Message::raw_inline_reply().into();
                cmd.set_error(&AsError::BadMessage);
                Ok(Some(cmd))
            }
            Err(err) => Err(err),
        }
    }
}

impl Encoder<Cmd> for FrontCodec {
    type Error = AsError;

    fn encode(&mut self, item: Cmd, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let mut cmd = item.cmd.lock().unwrap();
        if let Some(subs) = cmd.subs.as_ref().cloned() {
            for sub in subs {
                self.encode(sub, dst)?;
            }
            cmd.req.try_save_ends(dst);
        } else {
            let reply = cmd.reply.take().expect("reply must exits");
            cmd.req.save_reply(reply, dst)?;
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
        item.cmd.lock().unwrap().req.save_req(dst)
    }
}

#[test]
fn test_mc_parse_wrong_case() {
    test_mc_parse_error_in_path("../fuzz/corpus/fuzz_mc_parser/");
    test_mc_parse_error_in_path("../fuzz/artifacts/fuzz_mc_parser/");
}

#[cfg(test)]
fn test_mc_parse_error_in_path(prefix: &str) {
    use std::fs::{self, File};
    use std::io::prelude::*;
    use std::io::BufReader;

    if let Ok(dir) = fs::read_dir(prefix) {
        for entry in dir {
            let entry = entry.unwrap();
            let path = entry.path();
            println!("parsing abs_path: {:?}", path);
            let fd = File::open(path).unwrap();
            let mut buffer = BufReader::new(fd);
            let mut data = Vec::new();
            buffer.read_to_end(&mut data).unwrap();
            println!("data is {:?}", &data[..]);
            let mut src = BytesMut::from(&data[..]);
            let mut codec = FrontCodec::default();

            loop {
                let result = codec.decode(&mut src);
                match result {
                    Ok(Some(_)) => {}
                    Ok(None) => break,
                    Err(_err) => break,
                }
            }
        }
    }
}
