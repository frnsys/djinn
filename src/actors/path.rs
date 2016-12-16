use std::net::SocketAddr;
use rustc_serialize::{Decodable, Encodable, Decoder, Encoder};

#[derive(Debug, Copy, Clone, PartialEq)]
pub struct RemoteAddr(pub SocketAddr);

impl Decodable for RemoteAddr {
    fn decode<D: Decoder>(d: &mut D) -> Result<RemoteAddr, D::Error> {
        let s = try!(d.read_str());
        let addr = s.parse().expect("Unable to parse socket address");
        Ok(RemoteAddr(addr))
    }
}

impl Encodable for RemoteAddr {
    fn encode<S: Encoder>(&self, s: &mut S) -> Result<(), S::Error> {
        let addr = self.0.to_string();
        let addr = addr.as_str();
        s.emit_str(addr)
    }
}

#[derive(RustcDecodable, RustcEncodable, Debug, Copy, Clone, PartialEq)]
pub enum ActorPath {
    Local { id: usize },
    Remote { addr: RemoteAddr, id: usize },
}
