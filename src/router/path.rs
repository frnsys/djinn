use std::net::SocketAddr;
use rustc_serialize::{json, Decodable, Encodable, Decoder, Encoder};

#[derive(Debug, Copy, Clone)]
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
        // TODO seems weird
        let addr = self.0.to_string();
        let addr = addr.as_str();
        s.emit_str(addr)
    }
}

// TODO eventually use this to locate actors locally or across a network
#[derive(RustcDecodable, RustcEncodable, Debug)]
pub enum ActorPath {
    Local { id: usize },
    Remote { addr: RemoteAddr, id: usize },
}
