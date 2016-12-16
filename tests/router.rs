#![feature(box_syntax)]

extern crate djinn;
extern crate rustc_serialize;
use std::sync::{Arc, RwLock};
use djinn::actors::{Actor, Inbox};
use djinn::actors::router::{Router, RoutingMessage};
use djinn::actors::path::ActorPath;

#[derive(RustcDecodable, RustcEncodable, Debug, PartialEq)]
pub enum ExampleMessage {
    Foo { x: u8, y: u8 },
    Bar { z: u8 },
}

#[derive(Debug)]
struct ExampleActor {
    id: usize,
    inbox: Inbox<ExampleMessage>,
}
impl ExampleActor {
    pub fn new(id: usize) -> ExampleActor {
        ExampleActor {
            id: id,
            inbox: Arc::new(RwLock::new(Vec::new())),
        }
    }
}

impl Actor for ExampleActor {
    type M = ExampleMessage;
    fn handle_msg(&self, message: Self::M) -> Self::M {
        match message {
            ExampleMessage::Foo { x, y } => {
                ExampleMessage::Foo {
                    x: x * 2,
                    y: y + 1,
                }
            }
            ExampleMessage::Bar { z } => ExampleMessage::Bar { z: z + 10 },
        }
    }

    fn inbox(&self) -> &Inbox<Self::M> {
        &self.inbox
    }

    fn id(&self) -> usize {
        self.id
    }
}


#[test]
fn router() {
    let addr = "127.0.0.1:8080".to_string();
    let actors = Arc::new(RwLock::new(Vec::new()));
    let actor = box ExampleActor::new(0);
    {
        let mut actors_ = actors.write().unwrap();
        actors_.push(Arc::new(RwLock::new(actor)));
    }
    let router = Router::new(addr, actors);
    router.serve();
    let sender = ActorPath::Local { id: 1 };
    let recipient = ActorPath::Local { id: 0 };
    let resp = router.send_msg(ExampleMessage::Bar { z: 10 }, sender, recipient);
    assert_eq!(resp, RoutingMessage::Ok);
}

#[test]
fn router_nonexistent_actor() {
    let addr = "127.0.0.1:8080".to_string();
    let actors = Arc::new(RwLock::new(Vec::new()));
    let actor = box ExampleActor::new(0);
    {
        let mut actors_ = actors.write().unwrap();
        actors_.push(Arc::new(RwLock::new(actor)));
    }
    let router = Router::new(addr, actors);
    router.serve();
    let sender = ActorPath::Local { id: 1 };
    let recipient = ActorPath::Local { id: 2 }; // no actor with this id
    let resp = router.send_msg(ExampleMessage::Bar { z: 10 }, sender, recipient);
    assert_eq!(resp, RoutingMessage::Err("No actor with id".to_string()));
}
