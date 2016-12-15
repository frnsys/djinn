use rustc_serialize;
use std::{io, marker};
use tokio_core::io::{Io, Codec, Framed, EasyBuf};
use tokio_proto::pipeline::{ServerProto, ClientProto};
use super::msgpack;

pub trait SerDeser
    : rustc_serialize::Decodable + rustc_serialize::Encodable + ::std::fmt::Debug + Send + Sync
    {
}
impl<T> SerDeser for T where T : rustc_serialize::Decodable + rustc_serialize::Encodable + ::std::fmt::Debug + Send + Sync {}

pub struct MsgPackProtocol<Req, Res>
    where Req: SerDeser,
          Res: SerDeser
{
    req: marker::PhantomData<Req>,
    res: marker::PhantomData<Res>,
}

impl<Req, Res> MsgPackProtocol<Req, Res>
    where Req: SerDeser,
          Res: SerDeser
{
    pub fn new() -> MsgPackProtocol<Req, Res> {
        MsgPackProtocol {
            req: marker::PhantomData,
            res: marker::PhantomData,
        }
    }
}

impl<T: Io + 'static, Req: 'static, Res: 'static> ServerProto<T> for MsgPackProtocol<Req, Res>
    where Req: SerDeser,
          Res: SerDeser
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
    where Req: SerDeser,
          Res: SerDeser
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
    where Req: SerDeser,
          Res: SerDeser
{
    req: marker::PhantomData<Req>,
    res: marker::PhantomData<Res>,
}

impl<Req, Res> MsgPackCodec<Req, Res>
    where Req: SerDeser,
          Res: SerDeser
{
    pub fn new() -> MsgPackCodec<Req, Res> {
        MsgPackCodec {
            req: marker::PhantomData,
            res: marker::PhantomData,
        }
    }
}

impl<Req, Res> Codec for MsgPackCodec<Req, Res>
    where Req: SerDeser,
          Res: SerDeser
{
    type In = Req;
    type Out = Res;

    fn decode(&mut self, buf: &mut EasyBuf) -> io::Result<Option<Req>> {
        let len = buf.len();
        if len == 0 {
            // done with request
            Ok(None)
        } else {
            msgpack::decode(buf)
        }
    }

    fn encode(&mut self, msg: Res, buf: &mut Vec<u8>) -> io::Result<()> {
        msgpack::encode(msg, buf)
    }
}
