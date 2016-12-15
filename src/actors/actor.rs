use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use rustc_serialize::{json, Decodable, Encodable, Decoder, Encoder};

pub type Inbox<M> = Arc<RwLock<Vec<M>>>;

pub trait Message : Encodable + Decodable + Send + Sync {}

// Auto implement Message for types that satisfy these traits
impl<T> Message for T where T : Encodable + Decodable + Send + Sync {}

pub trait Actor : Send + Sync {
    type M : Message;
    type R : Message;
    fn inbox(&self) -> &Inbox<Self::M>;
    fn handle_msg(&self, Self::M) -> Self::R;

    // TODO it should be the router that places the messages in actor mailboxes
    fn recv_msg(&self, payload: String) -> Self::R {
        let decoded: Self::M = json::decode(&payload).unwrap();
        self.handle_msg(decoded)
    }
    fn send_msg(&self, message: Self::M, actor: &Actor<M=Self::M, R=Self::R>) -> Self::R {
        // TODO check if actor is local, if so, directly call their handle msg func
        // let resp = actor.handle_msg(message);

        let encoded = json::encode(&message).unwrap();
        // ignoring actual message transport

        actor.recv_msg(encoded)
    }
}

pub type ActorRef<A: Actor> = Arc<RwLock<Box<A>>>;
pub type ActorVec<A: Actor> = Vec<ActorRef<A>>;
pub type ActorVecRef<A: Actor> = Arc<RwLock<ActorVec<A>>>;

pub struct RemoteAddr(SocketAddr);

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
#[derive(RustcDecodable, RustcEncodable)]
pub enum ActorPath {
    Local { id: usize },
    Remote { addr: RemoteAddr, id: usize }
}
