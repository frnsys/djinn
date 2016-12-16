use std::{io, marker};
use tokio_core::io::{Io, Codec, Framed, EasyBuf};
use tokio_proto::pipeline::{ServerProto, ClientProto};
use rmp_serialize::{Encoder, Decoder};
use rustc_serialize::{Encodable, Decodable};
use super::message::Message;

pub fn decode<R: Decodable>(buf: &mut EasyBuf) -> io::Result<Option<R>> {
    let len = buf.len();
    let bytes = buf.drain_to(len);
    let mut decoder = Decoder::new(bytes.as_slice());
    match Decodable::decode(&mut decoder) {
        Ok(v) => Ok(Some(v)),
        Err(e) => Err(io::Error::new(io::ErrorKind::Other, format!("{}", e))),
    }
}

pub fn encode<R: Encodable>(msg: R, buf: &mut Vec<u8>) -> io::Result<()> {
    match msg.encode(&mut Encoder::new(buf)) {
        Ok(_) => Ok(()),
        Err(e) => Err(io::Error::new(io::ErrorKind::Other, format!("{}", e))),
    }
}

pub struct MsgPackProtocol<Req, Res>
    where Req: Message,
          Res: Message
{
    req: marker::PhantomData<Req>,
    res: marker::PhantomData<Res>,
}

impl<Req, Res> MsgPackProtocol<Req, Res>
    where Req: Message,
          Res: Message
{
    pub fn new() -> MsgPackProtocol<Req, Res> {
        MsgPackProtocol {
            req: marker::PhantomData,
            res: marker::PhantomData,
        }
    }
}

impl<T: Io + 'static, Req: 'static, Res: 'static> ServerProto<T> for MsgPackProtocol<Req, Res>
    where Req: Message,
          Res: Message
{
    type Request = Req;
    type Response = Res;
    type Error = io::Error;
    type Transport = Framed<T, MsgPackCodec<Req, Res>>;
    type BindTransport = io::Result<Framed<T, MsgPackCodec<Req, Res>>>;

    fn bind_transport(&self, io: T) -> io::Result<Framed<T, MsgPackCodec<Req, Res>>> {
        Ok(io.framed(MsgPackCodec::new()))
    }
}

impl<T: Io + 'static, Req: 'static, Res: 'static> ClientProto<T> for MsgPackProtocol<Res, Req>
    where Req: Message,
          Res: Message
{
    type Request = Req;
    type Response = Res;
    type Error = io::Error;
    type Transport = Framed<T, MsgPackCodec<Res, Req>>;
    type BindTransport = io::Result<Framed<T, MsgPackCodec<Res, Req>>>;

    fn bind_transport(&self, io: T) -> io::Result<Framed<T, MsgPackCodec<Res, Req>>> {
        Ok(io.framed(MsgPackCodec::new()))
    }
}

pub struct MsgPackCodec<Req, Res>
    where Req: Message,
          Res: Message
{
    req: marker::PhantomData<Req>,
    res: marker::PhantomData<Res>,
}

impl<Req, Res> MsgPackCodec<Req, Res>
    where Req: Message,
          Res: Message
{
    pub fn new() -> MsgPackCodec<Req, Res> {
        MsgPackCodec {
            req: marker::PhantomData,
            res: marker::PhantomData,
        }
    }
}

impl<Req, Res> Codec for MsgPackCodec<Req, Res>
    where Req: Message,
          Res: Message
{
    type In = Req;
    type Out = Res;

    fn decode(&mut self, buf: &mut EasyBuf) -> io::Result<Option<Req>> {
        let len = buf.len();
        if len == 0 {
            // done with request
            Ok(None)
        } else {
            decode(buf)
        }
    }

    fn encode(&mut self, msg: Res, buf: &mut Vec<u8>) -> io::Result<()> {
        encode(msg, buf)
    }
}
