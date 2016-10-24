extern crate threadpool;
use std::thread;
use std::cmp::min;
use threadpool::ThreadPool;
use super::actor::{Actor, ActorVecRef};

// Max amount of messages to process per actor
// during an activation
const THROUGHPUT: usize = 100;

/// Manages a pool of actors,
/// dispatching them to threads when they have queued messages.
pub fn dispatcher<A>(actors : ActorVecRef<A>, n_threads : usize) where A : Actor + 'static {
    thread::spawn(move || {
        // TODO should this just be another futures cpupool?
        let pool = ThreadPool::new(n_threads);
        loop {
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
                    let chunk : Vec<A::M> = inbox.drain(0..n_messages).collect();
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
