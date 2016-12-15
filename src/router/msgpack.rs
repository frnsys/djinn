use std::io;
use tokio_core::io::EasyBuf;
use rmp_serialize::{Encoder, Decoder};
use rustc_serialize::{Encodable, Decodable};

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
