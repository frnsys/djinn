use std::sync::{Arc, RwLock};
use rustc_serialize::{json, Decodable, Encodable};
use router::SerDeser;

pub type Inbox<M> = Arc<RwLock<Vec<M>>>;

pub trait Actor: Send + Sync {
    type M: SerDeser;
    type R: SerDeser;
    fn id(&self) -> usize;
    fn inbox(&self) -> &Inbox<Self::M>;
    fn handle_msg(&self, Self::M) -> Self::R;

    // TODO it should be the router that places the messages in actor mailboxes
    fn recv_msg(&self, payload: String) -> Self::R {
        let decoded: Self::M = json::decode(&payload).unwrap();
        self.handle_msg(decoded)
    }
    fn send_msg(&self, message: Self::M, actor: &Actor<M = Self::M, R = Self::R>) -> Self::R {
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
