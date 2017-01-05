use std::io;
use rmp_serialize::decode::Error;
use rmp_serialize::{Encoder, Decoder};
use rustc_serialize::{Encodable, Decodable};

pub fn decode<R: Decodable>(inp: Vec<u8>) -> Result<R, Error> {
    let mut decoder = Decoder::new(&inp[..]);
    Decodable::decode(&mut decoder)
}

pub fn encode<R: Encodable>(data: R) -> Result<Vec<u8>, io::Error> {
    let mut buf = Vec::<u8>::new();
    match data.encode(&mut Encoder::new(&mut buf)) {
        Ok(_) => Ok(buf),
        Err(e) => Err(io::Error::new(io::ErrorKind::Other, format!("{}", e))),
    }
}
