use std::sync::{Arc, RwLock};
use super::message::Message;

pub type Inbox<M> = Arc<RwLock<Vec<M>>>;

pub trait Actor: Send + Sync {
    type M: Message;
    fn id(&self) -> usize;
    fn inbox(&self) -> &Inbox<Self::M>;
    fn handle_msg(&self, Self::M) -> Self::M;
}

pub type ActorRef<A: Actor> = Arc<RwLock<Box<A>>>;
pub type ActorVec<A: Actor> = Vec<ActorRef<A>>;
pub type ActorVecRef<A: Actor> = Arc<RwLock<ActorVec<A>>>;
