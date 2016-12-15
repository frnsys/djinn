use std::thread;
use std::cmp::min;
use threadpool::ThreadPool;
use std::collections::HashMap;
use super::actor::{Actor, ActorRef, ActorVecRef};
use router::{Router, RoutingMessage};

// Max amount of messages to process per actor
// during an activation
const THROUGHPUT: usize = 100;

/// Manages a pool of actors,
/// dispatching them to threads when they have queued messages.
pub fn dispatcher<A>(actors: ActorVecRef<A>, n_threads: usize)
    where A: Actor + 'static
{
    thread::spawn(move || {
        // TODO should this just be another futures cpupool?
        let pool = ThreadPool::new(n_threads);
        let router = Router::<A::M>::new();

        // lookup actors by id
        let mut lookup = HashMap::<usize, ActorRef<A>>::new();
        {
            let actors = actors.clone();
            let actors = actors.read().unwrap();
            for actor in actors.iter() {
                let actor_r = actor.read().unwrap();
                lookup.insert(actor_r.id(), actor.clone());
            }
        }

        loop {
            // give the actors the messages from the router
            let mut messages = router.inbox.write().unwrap();
            for msg in messages.drain(..) {
                match msg {
                    RoutingMessage::Message { sender, recipient, message } => {
                        match lookup.get(&recipient) {
                            Some(actor) => {
                                let actor_r = actor.read().unwrap();
                                let mut inbox = actor_r.inbox().write().unwrap();
                                inbox.push(message);
                            }
                            _ => println!("foo"),
                        }
                    }
                    _ => println!("foo"),
                }
            }
            let actors = actors.clone();
            let actors = actors.read().unwrap();
            for actor in actors.iter() {
                let actor_r = actor.read().unwrap();
                let n_queued = {
                    let inbox = actor_r.inbox().read().unwrap();
                    inbox.len()
                };
                if n_queued > 0 {
                    let n_messages = min(n_queued, THROUGHPUT);
                    let mut inbox = actor_r.inbox().write().unwrap();
                    let chunk: Vec<A::M> = inbox.drain(0..n_messages).collect();
                    let actor = actor.clone();
                    pool.execute(move || {
                        for msg in chunk {
                            let actor = actor.write().unwrap();
                            let _ = actor.handle_msg(msg);
                        }
                    });
                }
            }
        }
    });
}
