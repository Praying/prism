use crate::com::AsError;
use crate::protocol::redis::cmd::Cmd;
use crate::protocol::redis::resp::{self, Message};
use bytes::BytesMut;
use tokio_util::codec::{Decoder, Encoder};

#[derive(Default)]
pub struct Codec {}

impl Encoder<Message> for Codec {
    type Error = AsError;

    fn encode(&mut self, item: Message, dst: &mut BytesMut) -> Result<(), Self::Error> {
        item.save(dst);
        Ok(())
    }
}

impl Decoder for Codec {
    type Item = Cmd;
    type Error = AsError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match resp::Message::parse(src)? {
            Some(msg) => Ok(Some(Cmd::new(msg))),
            None => Ok(None),
        }
    }
}
