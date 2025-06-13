use crate::com::*;
use crate::utils::simdfind;
use crate::utils::Range;

use bytes::{BufMut, Bytes, BytesMut};
use std::str;

pub const RESP_STRING: u8 = b'+';
pub const RESP_INT: u8 = b':';
pub const RESP_ERROR: u8 = b'-';
pub const RESP_BULK: u8 = b'$';
pub const RESP_ARRAY: u8 = b'*';

pub const BYTE_CR: u8 = b'\r';
pub const BYTE_LF: u8 = b'\n';

pub const BYTES_CMD_CLUSTER_SLOTS: &[u8] = b"*2\r\n$7\r\nCLUSTER\r\n$5\r\nSLOTS\r\n";
pub const BYTES_CMD_CLUSTER_NODES: &[u8] = b"*2\r\n$7\r\nCLUSTER\r\n$5\r\nNODES\r\n";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotRange {
    pub from: usize,
    pub to: usize,
    pub master: String,
    pub slaves: Vec<String>,
}

// contains Range means body cursor range [begin..end] for non-array type
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RespType {
    String(Range),
    Error(Range),
    Integer(Range),
    // contains head range and bulk body range
    Bulk(Range, Range),
    // contains head range and sub vecs
    Array(Range, Vec<RespType>),

    Inline(Vec<Range>),
}

impl RespType {
    pub fn array(&self) -> Option<&Vec<RespType>> {
        match self {
            RespType::Array(_, rv) => Some(rv),
            _ => None,
        }
    }

    pub fn to_range(&self) -> Result<Range, AsError> {
        match self {
            RespType::String(rg) => Ok(*rg),
            RespType::Error(rg) => Ok(*rg),
            RespType::Integer(rg) => Ok(*rg),
            RespType::Bulk(_head, body) => Ok(Range::new(body.begin(), body.end() - 2)),
            RespType::Array(head, subs) => {
                if let Some(last) = subs.last() {
                    let last_range = last.to_range()?;
                    Ok(Range::new(head.begin(), last_range.end()))
                } else {
                    Ok(*head)
                }
            }
            RespType::Inline(fields) => {
                let first = fields.first().cloned().unwrap_or_default();
                let last = fields.last().cloned().unwrap_or_default();
                Ok(Range::new(first.begin(), last.end()))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageMut {
    pub rtype: RespType,
    pub data: BytesMut,
}

impl MessageMut {
    fn try_parse_inline(src: &[u8]) -> Result<Option<MsgPack>, AsError> {
        let mut cursor = 0;
        let mut fields = Vec::new();
        for data in src.split(|x| *x == b' ') {
            fields.push(Range::new(cursor, cursor + data.len()));
            cursor = cursor + data.len() + 1;
        }
        Ok(Some(MsgPack {
            rtype: RespType::Inline(fields),
            size: src.len(),
        }))
    }

    fn parse_inner(cursor: usize, src: &[u8]) -> Result<Option<MsgPack>, AsError> {
        let pos = if let Some(p) = simdfind::find_lf_simd(&src[cursor..]) {
            p
        } else {
            return Ok(None);
        };

        if pos == 0 {
            return Err(AsError::BadMessage);
        }

        // detect pos -1 is CR
        if src[cursor + pos - 1] != BYTE_CR {
            // should detect inline
            return Self::try_parse_inline(&src[cursor..cursor + pos + 1]);
        }

        match src[cursor] {
            RESP_STRING => {
                return Ok(Some(MsgPack {
                    rtype: RespType::String(Range::new(cursor, cursor + pos + 1)),
                    size: pos + 1,
                }));
            }
            RESP_INT => {
                return Ok(Some(MsgPack {
                    rtype: RespType::Integer(Range::new(cursor, cursor + pos + 1)),
                    size: pos + 1,
                }));
            }
            RESP_ERROR => {
                return Ok(Some(MsgPack {
                    rtype: RespType::Error(Range::new(cursor, cursor + pos + 1)),
                    size: pos + 1,
                }));
            }
            RESP_BULK => {
                let csize = match btoi::btoi::<isize>(&src[cursor + 1..cursor + pos - 1]) {
                    Ok(csize) => csize,
                    Err(_err) => return Err(AsError::BadMessage),
                };

                if csize == -1 {
                    return Ok(Some(MsgPack {
                        rtype: RespType::Bulk(
                            Range::new(cursor, cursor + 5),
                            Range::new(cursor, cursor + 5),
                        ),
                        size: 5,
                    }));
                } else if csize < 0 {
                    return Err(AsError::BadMessage);
                }

                let total_size = (pos + 1) + (csize as usize) + 2;

                if src.len() >= cursor + total_size {
                    return Ok(Some(MsgPack {
                        rtype: RespType::Bulk(
                            Range::new(cursor, cursor + pos + 1),
                            Range::new(cursor + pos + 1, cursor + total_size),
                        ),
                        size: total_size,
                    }));
                }
            }
            RESP_ARRAY => {
                let csize = match btoi::btoi::<isize>(&src[cursor + 1..cursor + pos - 1]) {
                    Ok(csize) => csize,
                    Err(_err) => return Err(AsError::BadMessage),
                };
                if csize == -1 {
                    return Ok(Some(MsgPack {
                        rtype: RespType::Array(Range::new(cursor, cursor + 5), vec![]),
                        size: 5,
                    }));
                } else if csize < 0 {
                    return Err(AsError::BadMessage);
                }
                let mut mycursor = cursor + pos + 1;
                let mut items = Vec::new();
                for _ in 0..csize {
                    if let Some(MsgPack { rtype, size }) = Self::parse_inner(mycursor, &src[..])? {
                        mycursor += size;
                        items.push(rtype);
                    } else {
                        return Ok(None);
                    }
                }
                return Ok(Some(MsgPack {
                    rtype: RespType::Array(Range::new(cursor, cursor + pos + 1), items),
                    size: mycursor - cursor,
                }));
            }
            _ => {
                if cursor != 0 {
                    return Err(AsError::BadMessage);
                }
                // inline command
                return Self::try_parse_inline(&src[cursor..cursor + pos + 1]);
            }
        }

        Ok(None)
    }

    pub fn parse(src: &mut BytesMut) -> Result<Option<MessageMut>, AsError> {
        let rslt = match Self::parse_inner(0, &src[..]) {
            Ok(r) => r,
            Err(err) => {
                // TODO: should change it as wrong bad command error
                if let Some(pos) = simdfind::find_lf_simd(&src[..]) {
                    let _ = src.split_to(pos + 1);
                }
                return Err(err);
            }
        };

        if let Some(MsgPack { size, rtype }) = rslt {
            let data = src.split_to(size);
            return Ok(Some(MessageMut { data, rtype }));
        }
        Ok(None)
    }
}

impl MessageMut {
    pub fn nth_mut(&mut self, index: usize) -> Option<&mut [u8]> {
        if let Some(range) = self.get_nth_data_range(index) {
            Some(&mut self.data.as_mut()[range.begin()..range.end()])
        } else {
            None
        }
    }

    pub fn nth(&self, index: usize) -> Option<&[u8]> {
        if let Some(range) = self.get_nth_data_range(index) {
            Some(&self.data.as_ref()[range.begin()..range.end()])
        } else {
            None
        }
    }

    fn get_nth_data_range(&self, index: usize) -> Option<Range> {
        if let RespType::Array(_, items) = &self.rtype {
            if let Some(item) = items.get(index) {
                match item {
                    RespType::String(Range { begin, end }) => {
                        return Some(Range {
                            begin: begin + 1,
                            end: end - 2,
                        });
                    }
                    RespType::Error(Range { begin, end }) => {
                        return Some(Range {
                            begin: begin + 1,
                            end: end - 2,
                        });
                    }
                    RespType::Integer(Range { begin, end }) => {
                        return Some(Range {
                            begin: begin + 1,
                            end: end - 2,
                        });
                    }
                    RespType::Bulk(_, Range { begin, end }) => {
                        return Some(Range {
                            begin: *begin,
                            end: end - 2,
                        });
                    }
                    _ => return None,
                }
            }
        }
        if let RespType::Inline(fields) = &self.rtype {
            if let Some(rng) = fields.get(index) {
                let mut end = rng.end();
                let len = rng.len();
                if len == 0 {
                    return Some(*rng);
                }
                if len > 0 && self.data[len - 1] == BYTE_LF {
                    end -= 1;
                    if len > 1 && self.data[len - 2] == BYTE_CR {
                        end -= 1;
                    }
                }
                return Some(Range::new(rng.begin(), end));
            }
        }
        None
    }
}

struct MsgPack {
    rtype: RespType,
    size: usize,
}

impl From<MessageMut> for Message {
    fn from(MessageMut { rtype, data }: MessageMut) -> Message {
        Message {
            data: data.freeze(),
            rtype,
        }
    }
}

pub struct MessageIter<'a> {
    msg: &'a Message,
    index: usize,
}

impl<'a> Iterator for MessageIter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        let current = self.index;
        self.index += 1;
        self.msg.nth(current)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Message {
    pub rtype: RespType,
    pub data: Bytes,
}

impl From<&AsError> for Message {
    fn from(err: &AsError) -> Self {
        Message::plain(err.to_string(), RESP_ERROR)
    }
}

impl From<AsError> for Message {
    fn from(err: AsError) -> Self {
        Message::plain(err.to_string(), RESP_ERROR)
    }
}


impl Message {
    pub fn auth(password: &str) -> Message {
        let mut buf = BytesMut::new();
        buf.put_slice(b"*2\r\n");
        buf.put_slice(b"$4\r\nAUTH\r\n");
        buf.put_slice(b"$");
        buf.put_slice(password.len().to_string().as_bytes());
        buf.put_slice(b"\r\n");
        buf.put_slice(password.as_bytes());
        buf.put_slice(b"\r\n");

        let mut rdata = buf;
        let msg = MessageMut::parse(&mut rdata).unwrap().unwrap();
        msg.into()
    }

    pub fn ping() -> Message {
        Message::new_ping_request()
    }

    pub fn parse(src: &mut BytesMut) -> Result<Option<Message>, AsError> {
        MessageMut::parse(src).map(|x| x.map(Into::into))
    }

    pub fn from_msgs(msgs: Vec<Message>) -> Message {
        let mut rdata = BytesMut::new();
        rdata.put_u8(RESP_ARRAY);
        rdata.put_slice(msgs.len().to_string().as_bytes());
        rdata.put_slice(b"\r\n");
        for msg in msgs {
            msg.save(&mut rdata);
        }

        // This is a hack, we don't parse the rtype for combined messages.
        // The caller should handle it.
        Message {
            data: rdata.freeze(),
            rtype: RespType::Array(Range::new(0, 0), vec![]),
        }
    }
pub fn is_error(&self) -> bool {
        matches!(&self.rtype, RespType::Error(_))
    }

    pub fn get_key(&self) -> &[u8] {
        self.nth(1).unwrap_or(b"")
    }

    pub fn len(&self) -> usize {
        match &self.rtype {
            RespType::Array(_, items) => items.len(),
            RespType::Inline(fields) => fields.len(),
            _ => 1,
        }
    }
    
    pub fn mk_subs(&self) -> Vec<Message> {
        let cmd = match self.nth(0) {
            Some(c) => c,
            None => return vec![],
        };
    
        if self.len() <= 1 {
            return vec![];
        }
    
        let mut subs = Vec::with_capacity(self.len() - 1);
        for i in 1..self.len() {
            let key = match self.nth(i) {
                Some(k) => k,
                None => continue,
            };
    
            let mut rdata = BytesMut::new();
            // *2\r\n
            rdata.put_slice(b"*2\r\n");
    
            // $cmd_len\r\ncmd\r\n
            rdata.put_u8(RESP_BULK);
            rdata.put_slice(cmd.len().to_string().as_bytes());
            rdata.put_slice(b"\r\n");
            rdata.put_slice(cmd);
            rdata.put_slice(b"\r\n");
    
            // $key_len\r\nkey\r\n
            rdata.put_u8(RESP_BULK);
            rdata.put_slice(key.len().to_string().as_bytes());
            rdata.put_slice(b"\r\n");
            rdata.put_slice(key);
            rdata.put_slice(b"\r\n");
            
            if let Ok(Some(mut_msg)) = MessageMut::parse(&mut rdata) {
                 subs.push(mut_msg.into());
            }
        }
        subs
    }

    pub fn new_cluster_slots() -> Message {
        Message {
            data: Bytes::from(BYTES_CMD_CLUSTER_SLOTS),
            rtype: RespType::Array(
                Range::new(0, 4),
                vec![
                    RespType::Bulk(Range::new(4, 8), Range::new(8, 17)),
                    RespType::Bulk(Range::new(17, 21), Range::new(21, 28)),
                ],
            ),
        }
    }

    pub fn new_cluster_nodes() -> Message {
        Message {
            data: Bytes::from(BYTES_CMD_CLUSTER_NODES),
            rtype: RespType::Array(
                Range::new(0, 4),
                vec![
                    RespType::Bulk(Range::new(4, 8), Range::new(8, 17)),
                    RespType::Bulk(Range::new(17, 21), Range::new(21, 28)),
                ],
            ),
        }
    }

    pub fn new_read_only() -> Message {
        Message {
            data: Bytes::from("*1\r\n$8\r\nREADONLY\r\n"),
            rtype: RespType::Array(
                Range::new(0, 4),
                vec![RespType::Bulk(Range::new(4, 8), Range::new(8, 18))],
            ),
        }
    }

    pub fn new_ping_request() -> Message {
        Message {
            data: Bytes::from("*1\r\n$4\r\nPING\r\n"),
            rtype: RespType::Array(
                Range::new(0, 4),
                vec![RespType::Bulk(Range::new(4, 8), Range::new(8, 14))],
            ),
        }
    }

    pub fn new_asking() -> Message {
        Message {
            data: Bytes::from("*1\r\n$6\r\nASKING\r\n"),
            rtype: RespType::Array(
                Range::new(0, 4),
                vec![RespType::Bulk(Range::new(4, 8), Range::new(8, 16))],
            ),
        }
    }

    pub fn new_bulks(bulks: Vec<Bytes>) -> Message {
        let mut rdata = BytesMut::new();
        let mut items = Vec::with_capacity(bulks.len());

        // Array Header
        rdata.put_u8(RESP_ARRAY);
        rdata.put_slice(bulks.len().to_string().as_bytes());
        rdata.put_slice(b"\r\n");
        let head_range = Range::new(0, rdata.len());

        // Bulks
        for bulk in bulks {
            let head_start = rdata.len();
            rdata.put_u8(RESP_BULK);
            rdata.put_slice(bulk.len().to_string().as_bytes());
            rdata.put_slice(b"\r\n");
            let body_start = rdata.len();
            rdata.put_slice(&bulk);
            rdata.put_slice(b"\r\n");
            let body_end = rdata.len();

            let item_head_range = Range::new(head_start, body_start);
            let item_body_range = Range::new(body_start, body_end);
            items.push(RespType::Bulk(item_head_range, item_body_range));
        }

        Message {
            data: rdata.freeze(),
            rtype: RespType::Array(head_range, items),
        }
    }

    pub fn inline_raw(data: Bytes) -> Message {
        let rngs = vec![Range::new(0, data.len())];
        Message {
            rtype: RespType::Inline(rngs),
            data,
        }
    }

    pub fn plain<I: Into<Bytes>>(data: I, resp_type: u8) -> Message {
        let bytes = data.into();
        let mut rdata = BytesMut::new();
        let total_len = 1 /* resp_type */ + bytes.len() + 2 /*\r\n*/;
        rdata.reserve(total_len);
        rdata.put_u8(resp_type);
        rdata.put(bytes.as_ref());
        rdata.put_u8(BYTE_CR);
        rdata.put_u8(BYTE_LF);

        let rtype = if resp_type == RESP_STRING {
            RespType::String(Range::new(0, total_len))
        } else if resp_type == RESP_INT {
            RespType::Integer(Range::new(0, total_len))
        } else if resp_type == RESP_ERROR {
            RespType::Error(Range::new(0, total_len))
        } else {
            unreachable!("fail to create uon plain message");
        };

        Message {
            data: rdata.into(),
            rtype,
        }
    }

    pub fn bulk_from_slice(data: &[u8]) -> Message {
        let mut rdata = BytesMut::new();
        rdata.put_u8(RESP_BULK);
        rdata.put_slice(data.len().to_string().as_bytes());
        rdata.put_slice(b"\r\n");
        rdata.put_slice(data);
        rdata.put_slice(b"\r\n");
        let mut msg_data = rdata;
        let msg = MessageMut::parse(&mut msg_data).unwrap().unwrap();
        msg.into()
    }

    pub fn save(&self, buf: &mut BytesMut) -> usize {
        self.save_by_rtype(&self.rtype, buf)
    }

    pub fn save_by_rtype(&self, rtype: &RespType, buf: &mut BytesMut) -> usize {
        match rtype {
            RespType::Array(head, subs) => {
                let head_slice = &self.data[head.as_range()];
                buf.put_slice(head_slice);
                for sub in subs {
                    self.save_by_rtype(sub, buf);
                }
                head_slice.len()
            }
            RespType::Inline(fields) => {
                let mut size = 0;
                for (i, field) in fields.iter().enumerate() {
                    let field_slice = &self.data[field.as_range()];
                    buf.put_slice(field_slice);
                    size += field_slice.len();
                    if i != fields.len() - 1 {
                        buf.put_u8(b' ');
                        size += 1;
                    }
                }
                size
            }
            _ => {
                let range = self.get_range(Some(rtype)).unwrap();
                let slice = &self.data[range.as_range()];
                buf.put_slice(slice);
                slice.len()
            }
        }
    }

    pub fn iter(&self) -> MessageIter {
        MessageIter {
            msg: self,
            index: 0,
        }
    }

    pub fn is_cluster_slots(&self) -> bool {
        self.data.as_ref() == BYTES_CMD_CLUSTER_SLOTS
    }

    pub fn nth(&self, index: usize) -> Option<&[u8]> {
        self.get_nth_data_range(index)
            .map(|x| &self.data[x.as_range()])
    }

    fn get_nth_data_range(&self, index: usize) -> Option<Range> {
        if let RespType::Array(_, items) = &self.rtype {
            if let Some(item) = items.get(index) {
                return item.to_range().ok();
            }
        }
        if let RespType::Inline(fields) = &self.rtype {
            if let Some(rng) = fields.get(index) {
                return Some(*rng);
            }
        }
        None
    }

    pub(super) fn get_range(&self, rtype: Option<&RespType>) -> Option<Range> {
        let rtype = if let Some(rtype) = rtype {
            rtype
        } else {
            &self.rtype
        };
        match rtype {
            RespType::String(rg) => Some(*rg),
            RespType::Error(rg) => Some(*rg),
            RespType::Integer(rg) => Some(*rg),
            RespType::Bulk(head, body) => Some(Range::new(head.begin(), body.end())),
            RespType::Array(head, subs) => {
                if let Some(last) = subs.last() {
                    if let Some(last_range) = self.get_range(Some(last)) {
                        Some(Range::new(head.begin(), last_range.end()))
                    } else {
                        Some(*head)
                    }
                } else {
                    Some(*head)
                }
            }
            RespType::Inline(fields) => {
                let first = fields.first().cloned().unwrap_or_default();
                let last = fields.last().cloned().unwrap_or_default();
                Some(Range::new(first.begin(), last.end()))
            }
        }
    }

    pub fn check_redirect(&self) -> Option<crate::proxy::cluster::Redirect> {
        match self.rtype {
            RespType::Error(_) => parse_redirect(self.nth(0)),
            _ => None,
        }
    }
}

fn parse_redirect(data: Option<&[u8]>) -> Option<crate::proxy::cluster::Redirect> {
    let data = data?;
    let mut iter = data.split(|x| *x == b' ');
    let first = iter.next()?;
    if first.eq_ignore_ascii_case(b"MOVED") {
        let slot = iter.next()?;
        let to_str = iter.next()?;
        Some(crate::proxy::cluster::Redirect::Moved {
            slot: btoi::btoi(slot).ok()?,
            addr: String::from_utf8(to_str.to_vec()).ok()?,
        })
    } else if first.eq_ignore_ascii_case(b"ASK") {
        let to_str = iter.next()?;
        Some(crate::proxy::cluster::Redirect::Asking {
            addr: String::from_utf8(to_str.to_vec()).ok()?,
        })
    } else {
        None
    }
}

pub fn parse_slot_ranges(msg: &Message) -> Result<Vec<SlotRange>, AsError> {
    let mut ranges = Vec::new();
    let resp_arr = msg.rtype.array().ok_or(AsError::BadMessage)?.clone();

    for item in resp_arr.into_iter() {
        let fields = item.array().ok_or(AsError::BadMessage)?.clone();
        if fields.len() < 3 {
            return Err(AsError::BadMessage);
        }

        let mut slot_range = SlotRange {
            from: 0,
            to: 0,
            master: String::new(),
            slaves: Vec::new(),
        };

        let from_rg = fields[0].to_range()?;
        let to_rg = fields[1].to_range()?;
        let master_array = fields[2].array().ok_or(AsError::BadMessage)?.clone();

        if master_array.len() < 2 {
            return Err(AsError::BadMessage);
        }
        let master_ip_rg = master_array[0].to_range()?;
        let master_port_rg = master_array[1].to_range()?;

        slot_range.from = btoi::btoi(&msg.data[from_rg.as_range()])?;
        slot_range.to = btoi::btoi(&msg.data[to_rg.as_range()])?;
        let master_ip = String::from_utf8(msg.data[master_ip_rg.as_range()].to_vec())?;
        let master_port = btoi::btoi::<u16>(&msg.data[master_port_rg.as_range()])?;
        slot_range.master = format!("{}:{}", master_ip, master_port);

        for slave_fields in fields.iter().skip(3) {
            let slave_array = slave_fields.array().ok_or(AsError::BadMessage)?.clone();
            if slave_array.len() < 2 {
                continue;
            }
            let slave_ip_rg = slave_array[0].to_range()?;
            let slave_port_rg = slave_array[1].to_range()?;
            let slave_ip = String::from_utf8(msg.data[slave_ip_rg.as_range()].to_vec())?;
            let slave_port = btoi::btoi::<u16>(&msg.data[slave_port_rg.as_range()])?;
            slot_range.slaves.push(format!("{}:{}", slave_ip, slave_port));
        }
        ranges.push(slot_range);
    }
    Ok(ranges)
}

#[derive(Debug, Clone)]
pub struct Replica {
    pub slot: Range,
    pub master: Bytes,
    pub slaves: Vec<Bytes>,
}

#[derive(Debug, Clone)]
pub struct ClusterLayout {
    pub replicas: Vec<Replica>,
    pub slots: [u16; 16384],
}

#[derive(Debug, Clone)]
pub struct Layout {
    pub slots: Vec<SlotRange>,
}

pub fn slots_reply_to_replicas(msg: &Message) -> Result<Option<ClusterLayout>, AsError> {
    if msg.is_error() {
        let err_msg = String::from_utf8_lossy(&msg.data).to_string();
        return Err(AsError::ProxyFail(err_msg));
    }

    let ranges = parse_slot_ranges(msg)?;
    let mut slots = [0u16; 16384];
    let mut replicas = Vec::with_capacity(ranges.len());
    let mut cursor = 0;

    for (i, range) in ranges.iter().enumerate() {
        let replica = Replica {
            slot: Range::new(range.from, range.to),
            master: Bytes::from(range.master.clone()),
            slaves: range
                .slaves
                .iter()
                .map(|x| Bytes::from(x.clone()))
                .collect(),
        };
        replicas.push(replica);

        for j in range.from..=range.to {
            slots[j] = i as u16;
        }

        if cursor != range.from {
            return Ok(None);
        }
        cursor = range.to + 1;
    }

    if cursor != 16384 {
        return Ok(None);
    }

    let layout = ClusterLayout { replicas, slots };

    Ok(Some(layout))
}

use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeInfo {
    pub addr: String,
    pub is_master: bool,
    pub slots: Vec<Range>,
}

pub fn nodes_reply_to_layout(msg: &Message) -> Result<Option<Layout>, AsError> {
    let mut nodes = HashMap::new();
    let mut master_slaves: HashMap<String, Vec<String>> = HashMap::new();

    if let RespType::Bulk(_, body_range) = &msg.rtype {
        let data = &msg.data[body_range.begin()..body_range.end()];
        let data_str = str::from_utf8(data).map_err(|_| AsError::BadMessage)?;

        for line in data_str.lines() {
            let parts: Vec<&str> = line.split(' ').collect();
            if parts.len() < 8 {
                continue;
            }

            let id = parts[0].to_string();
            let addr_part = parts[1];
            let flags = parts[2];
            let master_id = parts[3];

            let addr = addr_part.split('@').next().unwrap_or("").to_string();
            let is_master = flags.contains("master");

            if is_master {
                let mut slots = Vec::new();
                for part in &parts[8..] {
                    if part.contains('-') {
                        let range_parts: Vec<&str> = part.split('-').collect();
                        if range_parts.len() == 2 {
                            if let (Ok(start), Ok(end)) = (
                                range_parts[0].parse::<usize>(),
                                range_parts[1].parse::<usize>(),
                            ) {
                                slots.push(Range::new(start, end + 1));
                            }
                        }
                    } else if let Ok(slot) = part.parse::<usize>() {
                        slots.push(Range::new(slot, slot + 1));
                    }
                }
                nodes.insert(
                    id.clone(),
                    NodeInfo {
                        addr: addr.clone(),
                        is_master: true,
                        slots,
                    },
                );
                master_slaves.entry(id).or_insert_with(Vec::new);
            } else if flags.contains("slave") && master_id != "-" {
                // Slaves are processed later, we just need their address for now.
                 nodes.insert(
                    id,
                    NodeInfo {
                        addr: addr.clone(),
                        is_master: false,
                        slots: vec![],
                    },
                );
                 if master_slaves.get_mut(master_id).is_some() {
                    master_slaves.get_mut(master_id).unwrap().push(addr);
                 } else {
                    master_slaves.insert(master_id.to_string(), vec![addr]);
                 }
            }
        }

        let mut slot_ranges = Vec::new();
        for (master_id, master_info) in nodes.iter().filter(|(_, info)| info.is_master) {
            for range in &master_info.slots {
                let slaves = master_slaves.get(master_id).cloned().unwrap_or_default();
                slot_ranges.push(SlotRange {
                    from: range.begin(),
                    to: range.end() - 1,
                    master: master_info.addr.clone(),
                    slaves,
                });
            }
        }
        
        slot_ranges.sort_by(|a, b| a.from.cmp(&b.from));

        if slot_ranges.is_empty() {
            return Ok(None);
        }

        return Ok(Some(Layout { slots: slot_ranges }));
    }

    Ok(None)
}


impl From<Layout> for ClusterLayout {
    fn from(layout: Layout) -> Self {
        let mut slots = [0u16; 16384];
        let mut replicas = Vec::with_capacity(layout.slots.len());
        let mut cursor = 0;

        for (i, range) in layout.slots.iter().enumerate() {
            let replica = Replica {
                slot: crate::utils::Range::new(range.from, range.to),
                master: Bytes::from(range.master.clone()),
                slaves: range
                    .slaves
                    .iter()
                    .map(|x| Bytes::from(x.clone()))
                    .collect(),
            };
            replicas.push(replica);

            for j in range.from..=range.to {
                slots[j] = i as u16;
            }

            if cursor != range.from {
                // TODO: log this
            }
            cursor = range.to + 1;
        }

        ClusterLayout { replicas, slots }
    }
}
