#![feature(box_syntax)]

extern crate djinn;
extern crate futures;
extern crate futures_cpupool;
extern crate rustc_serialize;
use std::sync::{Arc, RwLock};
use futures::{BoxFuture, Future, collect, finished};
use futures_cpupool::CpuPool;
use djinn::actors::{Actor, Inbox, dispatcher};

#[derive(RustcDecodable, RustcEncodable, PartialEq, Debug)]
pub enum ExampleReturn {
    Foo(u8),
    Bar(u8),
}

#[derive(RustcDecodable, RustcEncodable)]
pub enum ExampleMessage {
    Foo { x: u8, y: u8 },
    Bar { z: u8 },
}

struct ExampleActor {
    id: usize,
    inbox: Inbox<ExampleMessage>
}
impl ExampleActor {
    pub fn new(id: usize) -> ExampleActor {
        ExampleActor {
            id: id,
            inbox: Arc::new(RwLock::new(Vec::new()))
        }
    }

    // TODO see if we can avoid using boxes here
    // <https://github.com/alexcrichton/futures-rs/blob/master/TUTORIAL.md#returning-futures>
    pub fn decide(&self) -> BoxFuture<String, String> {
        finished(format!("actor-{}", self.id)).boxed()
    }
}

impl Actor for ExampleActor {
    type M = ExampleMessage;
    type R = ExampleReturn;
    fn handle_msg(&self, message: Self::M) -> Self::R {
        match message {
            ExampleMessage::Foo { x, y } => ExampleReturn::Foo(x+y),
            ExampleMessage::Bar { z } => ExampleReturn::Bar(10+z),
        }
    }

    fn inbox(&self) -> &Inbox<Self::M> {
        &self.inbox
    }
}

fn main() {
    let n_workers = 10;
    let pool = CpuPool::new(n_workers);
    let actors = Arc::new(RwLock::new(Vec::new()));
    {
        let mut actors_ = actors.write().unwrap();
        for i in 0..10 {
            let actor = box ExampleActor::new(i);
            actors_.push(Arc::new(RwLock::new(actor)));
        }
    }

    dispatcher(actors.clone(), 10);

    // call decide methods on agents
    // this is basically the manager (aside from manager-leader communication)
    // TODO break this out into a Manager proper
    let mut futs = Vec::new();
    for actor in actors.read().unwrap().iter() {
        let actor = actor.write().unwrap();
        futs.push(pool.spawn(actor.decide()));
    }
    let f = collect(futs);
    let f = f.then(|x| {
        println!("{:?}", x);
        x
    });
    let _ = f.wait();
}