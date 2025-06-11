use crate::com::*;
use crate::proxy::cluster::Redirect;
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
    pub fn array(self) -> Option<Vec<RespType>> {
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
            RespType::Bulk(head, body) => Ok(Range::new(head.begin(), body.end())),
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
                let len = rng.range();
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
        let len = data.len();
        let len_str = len.to_string();
        let total_len = 1 /* $ */ + len_str.len() + 2 /* \r\n */ + len + 2 /* \r\n */;
        rdata.reserve(total_len);
        rdata.put_u8(RESP_BULK);
        rdata.put_slice(len_str.as_bytes());
        rdata.put_slice(b"\r\n");
        rdata.put_slice(data);
        rdata.put_slice(b"\r\n");

        let head_range = Range::new(0, 1 + len_str.len() + 2);
        let body_range = Range::new(head_range.end(), head_range.end() + len + 2);

        Message {
            data: rdata.freeze(),
            rtype: RespType::Bulk(head_range, body_range),
        }
    }

    pub fn save(&self, buf: &mut BytesMut) -> usize {
        self.save_by_rtype(&self.rtype, buf)
    }

    pub fn save_by_rtype(&self, rtype: &RespType, buf: &mut BytesMut) -> usize {
        match rtype {
            RespType::String(rg) => {
                buf.extend_from_slice(&self.data.as_ref()[rg.begin()..rg.end()]);
                rg.range()
            }
            RespType::Error(rg) => {
                buf.extend_from_slice(&self.data.as_ref()[rg.begin()..rg.end()]);
                rg.range()
            }
            RespType::Integer(rg) => {
                buf.extend_from_slice(&self.data.as_ref()[rg.begin()..rg.end()]);
                rg.range()
            }
            RespType::Bulk(head, body) => {
                let data = &self.data.as_ref()[head.begin()..body.end()];
                buf.extend_from_slice(data);
                (body.end - head.begin) as usize
            }
            RespType::Array(head, subs) => {
                buf.extend_from_slice(&self.data.as_ref()[head.begin()..head.end()]);
                let mut size = head.range();
                for sub in subs {
                    size += self.save_by_rtype(sub, buf);
                }
                size
            }
            RespType::Inline(fields) => {
                let first_begin = fields.first().map(|x| x.begin()).unwrap_or(0);
                let last_end = fields.last().map(|x| x.end()).unwrap_or(0);
                if first_begin != last_end {
                    buf.extend_from_slice(&self.data.as_ref()[first_begin..last_end]);
                }
                last_end - first_begin
            }
        }
    }

    pub fn raw_data(&self) -> &[u8] {
        self.data.as_ref()
    }

    pub fn data(&self) -> Option<&[u8]> {
        let range = self.get_range(Some(&self.rtype));
        range.map(|rg| &self.data.as_ref()[rg.begin()..rg.end()])
    }

    pub fn nth(&self, index: usize) -> Option<&[u8]> {
        if let Some(range) = self.get_nth_data_range(index) {
            return Some(&self.data.as_ref()[range.begin()..range.end()]);
        }

        if index == 0 {
            // only zero shot path to data
            return self.data();
        }
        None
    }

    fn get_nth_data_range(&self, index: usize) -> Option<Range> {
        if let RespType::Array(_, items) = &self.rtype {
            return self.get_range(items.get(index));
        }
        if let RespType::Inline(fields) = &self.rtype {
            if let Some(rng) = fields.get(index) {
                let mut end = rng.end();
                let len = rng.range();
                if rng.begin() == rng.end() {
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

    pub(super) fn get_data_of_range(&self, rg: Range) -> &[u8] {
        &self.data.as_ref()[rg.begin()..rg.end()]
    }

    pub(super) fn get_range(&self, rtype: Option<&RespType>) -> Option<Range> {
        if let Some(item) = rtype {
            match item {
                RespType::String(Range { begin, end }) => {
                    return Some(Range {
                        begin: begin + 1,
                        end: end - 2,
                    })
                }
                RespType::Error(Range { begin, end }) => {
                    return Some(Range {
                        begin: begin + 1,
                        end: end - 2,
                    })
                }
                RespType::Integer(Range { begin, end }) => {
                    return Some(Range {
                        begin: begin + 1,
                        end: end - 2,
                    })
                }
                RespType::Bulk(_, Range { begin, end }) => {
                    return Some(Range {
                        begin: *begin,
                        end: end - 2,
                    })
                }
                _ => return None,
            }
        }
        None
    }

    pub fn iter(&self) -> MessageIter {
        MessageIter { msg: self, index: 0 }
    }

    pub fn check_redirect(&self) -> Option<Redirect> {
        match self.rtype {
            RespType::Error(_) => {
                let data = self.data().unwrap_or(b"");
                parse_redirect(data)
            }
            _ => None,
        }
    }
}

fn parse_redirect(data: &[u8]) -> Option<Redirect> {
    let mut iter = data.split(|x| *x == b' ');
    let first = iter.next()?;
    let is_move = if first == b"MOVED" {
        true
    } else if first == b"ASK" {
        false
    } else {
        return None;
    };

    let slot_bytes = iter.next()?;
    let slot = btoi::btoi(slot_bytes).ok()?;

    let addr_bytes = iter.next()?;
    let to = str::from_utf8(addr_bytes).ok()?.to_string();

    if is_move {
        Some(Redirect::Move { slot, to })
    } else {
        Some(Redirect::Ask { slot, to })
    }
}

pub fn parse_slot_ranges(msg: &Message) -> Result<Vec<SlotRange>, AsError> {
    let subs = msg.rtype.clone().array().ok_or(AsError::BadMessage)?;
    let mut slots = Vec::new();
    for sub in subs {
        let mut sub_subs = sub.array().ok_or(AsError::BadMessage)?;

        let from_range = sub_subs.remove(0).to_range()?;
        let from = btoi::btoi::<usize>(msg.get_data_of_range(from_range))
            .ok()
            .ok_or(AsError::BadMessage)?;

        let to_range = sub_subs.remove(0).to_range()?;
        let to = btoi::btoi::<usize>(msg.get_data_of_range(to_range))
            .ok()
            .ok_or(AsError::BadMessage)?;

        let mut master_node = sub_subs.remove(0).array().ok_or(AsError::BadMessage)?;

        let master_host_range = master_node.remove(0).to_range()?;
        let master_host = msg.get_data_of_range(master_host_range).to_vec();

        let master_port_range = master_node.remove(0).to_range()?;
        let master_port = btoi::btoi::<u16>(msg.get_data_of_range(master_port_range))
            .ok()
            .ok_or(AsError::BadMessage)?;

        let mut slaves = Vec::new();
        for slave_node in sub_subs {
            let mut slave_node_subs = slave_node.array().ok_or(AsError::BadMessage)?;

            let slave_host_range = slave_node_subs.remove(0).to_range()?;
            let slave_host = msg.get_data_of_range(slave_host_range).to_vec();

            let slave_port_range = slave_node_subs.remove(0).to_range()?;
            let slave_port = btoi::btoi::<u16>(msg.get_data_of_range(slave_port_range))
                .ok()
                .ok_or(AsError::BadMessage)?;

            slaves.push(format!(
                "{}:{}",
                String::from_utf8_lossy(&slave_host),
                slave_port
            ));
        }
        slots.push(SlotRange {
            from,
            to,
            master: format!(
                "{}:{}",
                String::from_utf8_lossy(&master_host),
                master_port
            ),
            slaves,
        });
    }
    Ok(slots)
}
