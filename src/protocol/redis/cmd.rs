use crate::com::AsError;
use crate::protocol::redis::resp::{Message, RESP_STRING};
use crate::protocol::CmdType;
use crate::proxy::standalone::{back, front, Request};
use bytes::Bytes;
use hashbrown::HashMap;
use lazy_static::lazy_static;
use std::fmt;
use std::task::Waker;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::oneshot;
use tracing::error;

lazy_static! {
    pub static ref CMD_TYPE: HashMap<&'static [u8], CmdType> = {
        let mut hmap = HashMap::new();
        hmap.insert(&b"DEL"[..], CmdType::Del);
        hmap.insert(&b"UNLINK"[..], CmdType::Del);
        hmap.insert(&b"DUMP"[..], CmdType::Read);
        hmap.insert(&b"EXISTS"[..], CmdType::Exists);
        hmap.insert(&b"EXPIRE"[..], CmdType::Write);
        hmap.insert(&b"EXPIREAT"[..], CmdType::Write);
        hmap.insert(&b"KEYS"[..], CmdType::NotSupport);
        hmap.insert(&b"MIGRATE"[..], CmdType::NotSupport);
        hmap.insert(&b"MOVE"[..], CmdType::NotSupport);
        hmap.insert(&b"OBJECT"[..], CmdType::NotSupport);
        hmap.insert(&b"PERSIST"[..], CmdType::Write);
        hmap.insert(&b"PEXPIRE"[..], CmdType::Write);
        hmap.insert(&b"PEXPIREAT"[..], CmdType::Write);
        hmap.insert(&b"PTTL"[..], CmdType::Read);
        hmap.insert(&b"RANDOMKEY"[..], CmdType::NotSupport);
        hmap.insert(&b"RENAME"[..], CmdType::NotSupport);
        hmap.insert(&b"RENAMENX"[..], CmdType::NotSupport);
        hmap.insert(&b"RESTORE"[..], CmdType::Write);
        hmap.insert(&b"SCAN"[..], CmdType::NotSupport);
        hmap.insert(&b"SORT"[..], CmdType::Write);
        hmap.insert(&b"TTL"[..], CmdType::Read);
        hmap.insert(&b"TYPE"[..], CmdType::Read);
        hmap.insert(&b"WAIT"[..], CmdType::NotSupport);
        hmap.insert(&b"APPEND"[..], CmdType::Write);
        hmap.insert(&b"BITCOUNT"[..], CmdType::Read);
        hmap.insert(&b"BITOP"[..], CmdType::NotSupport);
        hmap.insert(&b"BITPOS"[..], CmdType::Read);
        hmap.insert(&b"DECR"[..], CmdType::Write);
        hmap.insert(&b"DECRBY"[..], CmdType::Write);
        hmap.insert(&b"GET"[..], CmdType::Read);
        hmap.insert(&b"GETBIT"[..], CmdType::Read);
        hmap.insert(&b"GETRANGE"[..], CmdType::Read);
        hmap.insert(&b"GETSET"[..], CmdType::Write);
        hmap.insert(&b"INCR"[..], CmdType::Write);
        hmap.insert(&b"INCRBY"[..], CmdType::Write);
        hmap.insert(&b"INCRBYFLOAT"[..], CmdType::Write);
        hmap.insert(&b"MGET"[..], CmdType::MGet);
        hmap.insert(&b"MSET"[..], CmdType::MSet);
        hmap.insert(&b"MSETNX"[..], CmdType::NotSupport);
        hmap.insert(&b"PSETEX"[..], CmdType::Write);
        hmap.insert(&b"SET"[..], CmdType::Write);
        hmap.insert(&b"SETBIT"[..], CmdType::Write);
        hmap.insert(&b"SETEX"[..], CmdType::Write);
        hmap.insert(&b"SETNX"[..], CmdType::Write);
        hmap.insert(&b"SETRANGE"[..], CmdType::Write);
        hmap.insert(&b"BITFIELD"[..], CmdType::Write);
        hmap.insert(&b"STRLEN"[..], CmdType::Read);
        hmap.insert(&b"SUBSTR"[..], CmdType::Read);
        hmap.insert(&b"HDEL"[..], CmdType::Write);
        hmap.insert(&b"HEXISTS"[..], CmdType::Read);
        hmap.insert(&b"HGET"[..], CmdType::Read);
        hmap.insert(&b"HGETALL"[..], CmdType::Read);
        hmap.insert(&b"HINCRBY"[..], CmdType::Write);
        hmap.insert(&b"HINCRBYFLOAT"[..], CmdType::Write);
        hmap.insert(&b"HKEYS"[..], CmdType::Read);
        hmap.insert(&b"HLEN"[..], CmdType::Read);
        hmap.insert(&b"HMGET"[..], CmdType::Read);
        hmap.insert(&b"HMSET"[..], CmdType::Write);
        hmap.insert(&b"HSET"[..], CmdType::Write);
        hmap.insert(&b"HSETNX"[..], CmdType::Write);
        hmap.insert(&b"HSTRLEN"[..], CmdType::Read);
        hmap.insert(&b"HVALS"[..], CmdType::Read);
        hmap.insert(&b"HSCAN"[..], CmdType::Read);
        hmap.insert(&b"BLPOP"[..], CmdType::NotSupport);
        hmap.insert(&b"BRPOP"[..], CmdType::NotSupport);
        hmap.insert(&b"BRPOPLPUSH"[..], CmdType::NotSupport);
        hmap.insert(&b"LINDEX"[..], CmdType::Read);
        hmap.insert(&b"LINSERT"[..], CmdType::Write);
        hmap.insert(&b"LLEN"[..], CmdType::Read);
        hmap.insert(&b"LPOP"[..], CmdType::Write);
        hmap.insert(&b"LPUSH"[..], CmdType::Write);
        hmap.insert(&b"LPUSHX"[..], CmdType::Write);
        hmap.insert(&b"LRANGE"[..], CmdType::Read);
        hmap.insert(&b"LREM"[..], CmdType::Write);
        hmap.insert(&b"LSET"[..], CmdType::Write);
        hmap.insert(&b"LTRIM"[..], CmdType::Write);
        hmap.insert(&b"RPOP"[..], CmdType::Write);
        hmap.insert(&b"RPOPLPUSH"[..], CmdType::Write);
        hmap.insert(&b"RPUSH"[..], CmdType::Write);
        hmap.insert(&b"RPUSHX"[..], CmdType::Write);
        hmap.insert(&b"SADD"[..], CmdType::Write);
        hmap.insert(&b"SCARD"[..], CmdType::Read);
        hmap.insert(&b"SDIFF"[..], CmdType::Read);
        hmap.insert(&b"SDIFFSTORE"[..], CmdType::Write);
        hmap.insert(&b"SINTER"[..], CmdType::Read);
        hmap.insert(&b"SINTERSTORE"[..], CmdType::Write);
        hmap.insert(&b"SISMEMBER"[..], CmdType::Read);
        hmap.insert(&b"SMEMBERS"[..], CmdType::Read);
        hmap.insert(&b"SMOVE"[..], CmdType::Write);
        hmap.insert(&b"SPOP"[..], CmdType::Write);
        hmap.insert(&b"SRANDMEMBER"[..], CmdType::Read);
        hmap.insert(&b"SREM"[..], CmdType::Write);
        hmap.insert(&b"SUNION"[..], CmdType::Read);
        hmap.insert(&b"SUNIONSTORE"[..], CmdType::Write);
        hmap.insert(&b"SSCAN"[..], CmdType::Read);
        hmap.insert(&b"ZADD"[..], CmdType::Write);
        hmap.insert(&b"ZCARD"[..], CmdType::Read);
        hmap.insert(&b"ZCOUNT"[..], CmdType::Read);
        hmap.insert(&b"ZINCRBY"[..], CmdType::Write);
        hmap.insert(&b"ZINTERSTORE"[..], CmdType::Write);
        hmap.insert(&b"ZLEXCOUNT"[..], CmdType::Read);
        hmap.insert(&b"ZRANGE"[..], CmdType::Read);
        hmap.insert(&b"ZRANGEBYLEX"[..], CmdType::Read);
        hmap.insert(&b"ZRANGEBYSCORE"[..], CmdType::Read);
        hmap.insert(&b"ZRANK"[..], CmdType::Read);
        hmap.insert(&b"ZREM"[..], CmdType::Write);
        hmap.insert(&b"ZREMRANGEBYLEX"[..], CmdType::Write);
        hmap.insert(&b"ZREMRANGEBYRANK"[..], CmdType::Write);
        hmap.insert(&b"ZREMRANGEBYSCORE"[..], CmdType::Write);
        hmap.insert(&b"ZREVRANGE"[..], CmdType::Read);
        hmap.insert(&b"ZREVRANGEBYLEX"[..], CmdType::Read);
        hmap.insert(&b"ZREVRANGEBYSCORE"[..], CmdType::Read);
        hmap.insert(&b"ZREVRANK"[..], CmdType::Read);
        hmap.insert(&b"ZSCORE"[..], CmdType::Read);
        hmap.insert(&b"ZUNIONSTORE"[..], CmdType::Write);
        hmap.insert(&b"ZSCAN"[..], CmdType::Read);
        hmap.insert(&b"PFADD"[..], CmdType::Write);
        hmap.insert(&b"PFCOUNT"[..], CmdType::Read);
        hmap.insert(&b"PFMERGE"[..], CmdType::Write);
        hmap.insert(&b"GEOADD"[..], CmdType::Write);
        hmap.insert(&b"GEODIST"[..], CmdType::Read);
        hmap.insert(&b"GEOHASH"[..], CmdType::Read);
        hmap.insert(&b"GEOPOS"[..], CmdType::Write);
        hmap.insert(&b"GEORADIUS"[..], CmdType::Write);
        hmap.insert(&b"GEORADIUSBYMEMBER"[..], CmdType::Write);
        hmap.insert(&b"EVAL"[..], CmdType::Eval);
        hmap.insert(&b"EVALSHA"[..], CmdType::NotSupport);
        hmap.insert(&b"AUTH"[..], CmdType::Ctrl);
        hmap.insert(&b"ECHO"[..], CmdType::Ctrl);
        hmap.insert(&b"PING"[..], CmdType::Ctrl);
        hmap.insert(&b"INFO"[..], CmdType::Ctrl);
        hmap.insert(&b"PROXY"[..], CmdType::NotSupport);
        hmap.insert(&b"SLOWLOG"[..], CmdType::NotSupport);
        hmap.insert(&b"QUIT"[..], CmdType::Ctrl);
        hmap.insert(&b"SELECT"[..], CmdType::NotSupport);
        hmap.insert(&b"TIME"[..], CmdType::NotSupport);
        hmap.insert(&b"CONFIG"[..], CmdType::NotSupport);
        hmap.insert(&b"CLUSTER"[..], CmdType::Ctrl);
        hmap.insert(&b"READONLY"[..], CmdType::Ctrl);
        hmap
    };
}

impl CmdType {
    pub fn is_read(self) -> bool {
        CmdType::Read == self || self.is_mget() || self.is_exists()
    }
    pub fn is_write(self) -> bool {
        CmdType::Write == self
    }
    pub fn is_mget(self) -> bool {
        CmdType::MGet == self
    }
    pub fn is_mset(self) -> bool {
        CmdType::MSet == self
    }
    pub fn is_exists(self) -> bool {
        CmdType::Exists == self
    }
    pub fn is_eval(self) -> bool {
        CmdType::Eval == self
    }
    pub fn is_del(self) -> bool {
        CmdType::Del == self
    }
    pub fn is_not_support(self) -> bool {
        CmdType::NotSupport == self
    }
    pub fn is_ctrl(self) -> bool {
        CmdType::Ctrl == self
    }
}

pub fn get_cmd_type(msg: &Message) -> CmdType {
    if let Some(data) = msg.nth(0) {
        if let Some(ctype) = CMD_TYPE.get(data) {
            return *ctype;
        }
    }
    CmdType::NotSupport
}

pub struct Cmd {
    pub msg: Message,
    cmd_type: CmdType,
    start: Instant,
    cycle: Arc<AtomicUsize>,
    reply_sender: Option<oneshot::Sender<Message>>,
}

impl fmt::Debug for Cmd {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Cmd")
            .field("msg", &self.msg)
            .field("cmd_type", &self.cmd_type)
            .field("start", &self.start)
            .field("cycle", &self.cycle)
            .field("reply_sender", &"Option<oneshot::Sender<Message>>")
            .finish()
    }
}

impl Clone for Cmd {
    fn clone(&self) -> Self {
        Self {
            msg: self.msg.clone(),
            cmd_type: self.cmd_type,
            start: self.start,
            cycle: self.cycle.clone(),
            reply_sender: None,
        }
    }
}

impl From<AsError> for Cmd {
    fn from(err: AsError) -> Self {
        let msg = Message::from(err);
        Cmd::new(msg)
    }
}

impl Default for Cmd {
    fn default() -> Self {
        Self {
            msg: Message::plain(Bytes::new(), RESP_STRING),
            cmd_type: CmdType::NotSupport,
            start: Instant::now(),
            cycle: Arc::new(AtomicUsize::new(0)),
            reply_sender: None,
        }
    }
}

impl Request for Cmd {
    type Reply = Message;
    type ReplySender = oneshot::Sender<Message>;

    type FrontCodec = front::Codec;
    type BackCodec = back::Codec;

    fn ping_request() -> Self {
        let msg = Message::new_ping_request();
        Self::new(msg)
    }

    fn reregister(&mut self, _waker: &Waker) {
        // no-op, since we use oneshot channel now
    }

    fn key_hash(&self, hash_tag: &[u8], hasher: fn(&[u8]) -> u64) -> u64 {
        let key = self.msg.nth(1).unwrap_or(&[]);
        let mut start = 0;
        let mut end = key.len();
        if !hash_tag.is_empty() {
            if let Some(s) = memchr::memchr(hash_tag[0], &key) {
                if let Some(e) = memchr::memchr(hash_tag[1], &key[s + 1..]) {
                    start = s + 1;
                    end = start + e;
                }
            }
        }
        hasher(&key[start..end])
    }

    fn subs(&self) -> Option<Vec<Self>> {
        if self.cmd_type.is_mget() || self.cmd_type.is_mset() || self.cmd_type.is_del() {
            let subs = self.msg.mk_subs();
            if subs.is_empty() {
                return None;
            }
            return Some(subs.into_iter().map(Self::new).collect());
        }
        None
    }

    fn mark_total(&self, cluster: &str) {
        crate::metrics::cluster_cmd_total(cluster, self.cmd_name());
    }

    fn mark_remote(&self, cluster: &str) {
        crate::metrics::cluster_cmd_remote(cluster, self.cmd_name());
    }

    fn is_done(&self) -> bool {
        // This is tricky with oneshot. For now, we assume it's never "done"
        // in the old sense. The caller will await the oneshot receiver.
        false
    }

    fn is_error(&self) -> bool {
        // Same as is_done, this state is now external to the Cmd itself.
        false
    }

    fn add_cycle(&self) {
        self.cycle.fetch_add(1, Ordering::Relaxed);
    }

    fn can_cycle(&self) -> bool {
        self.cycle.load(Ordering::Relaxed) < 1
    }

    fn valid(&self) -> bool {
        !self.cmd_type.is_not_support()
    }

    fn set_reply(&mut self, rep: Self::Reply) {
        if let Some(sender) = self.reply_sender.take() {
            if let Err(err) = sender.send(rep) {
                error!("send reply failed {:?}", err);
            }
        }
    }

    fn set_error(&mut self, err: AsError) {
        self.set_reply(Message::from(err))
    }

    fn set_reply_sender(&mut self, sender: Self::ReplySender) {
        self.reply_sender = Some(sender);
    }

    fn get_sendtime(&self) -> Option<Instant> {
        Some(self.start)
    }
}

impl Cmd {
    pub fn new(msg: Message) -> Self {
        let cmd_type = get_cmd_type(&msg);
        Self {
            msg,
            cmd_type,
            start: Instant::now(),
            cycle: Arc::new(AtomicUsize::new(0)),
            reply_sender: None,
        }
    }

    fn cmd_name(&self) -> &str {
        if let Some(data) = self.msg.nth(0) {
            std::str::from_utf8(data).unwrap_or("unknown")
        } else {
            "unknown"
        }
    }

    pub fn new_cluster_slots_cmd() -> Self {
        let msg = Message::new_cluster_slots();
        Self::new(msg)
    }

    pub fn take_reply_sender(&mut self) -> Option<oneshot::Sender<Message>> {
        self.reply_sender.take()
    }

    pub fn new_asking() -> Self {
        let msg = Message::new_asking();
        Self::new(msg)
    }
}

impl From<Message> for Cmd {
    fn from(msg: Message) -> Self {
        Self::new(msg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::redis::resp::Message;
    use crate::utils::crc;
    use bytes::Bytes;

    #[test]
    fn test_key_hash_crc16() {
        let cmd = Cmd::new(Message::new_bulks(vec![
            Bytes::from("set"),
            Bytes::from("k1"),
            Bytes::from("v1"),
        ]));
        let hash_tag = &b""[..];
        let hash = cmd.key_hash(hash_tag, crc::crc16);
        assert_eq!(hash % 16384, 12706);
    }
}
