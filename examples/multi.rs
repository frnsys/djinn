//! # Multiple agent types
//! Simple example of multiple types of agents.
//! You can use an enum, where each variant is a different type of agent.
//! Here we're just using fields on enum variants for state, but you could also define separate
//! state structs that you wrap enum variants around.

extern crate djinn;
extern crate redis;
extern crate redis_cluster;
extern crate rustc_serialize;

use redis::Client;
use djinn::{Agent, Manager, Simulation, Population, Updates, Redis, run};

#[derive(RustcDecodable, RustcEncodable, Debug, PartialEq, Clone)]
struct Cat {
    purrs: usize,
}

#[derive(RustcDecodable, RustcEncodable, Debug, PartialEq, Clone)]
struct Person {
    health: isize,
}

#[derive(RustcDecodable, RustcEncodable, Debug, PartialEq, Clone)]
enum State {
    Person(Person),
    Cat(Cat),
}

#[derive(RustcDecodable, RustcEncodable, Debug, PartialEq, Clone)]
struct World {}

#[derive(RustcDecodable, RustcEncodable, Debug, PartialEq, Clone)]
enum Update {
    ChangeHealth(isize),
    Purr,
}

#[derive(Clone)]
struct MultiSim;

impl Simulation for MultiSim {
    type State = State;
    type Update = Update;
    type World = World;

    fn decide<R: Redis>(&self,
                        agent: &Agent<Self::State>,
                        world: &Self::World,
                        population: &Population<Self, R>,
                        updates: &mut Updates<Self>)
                        -> () {
        match agent.state {
            State::Person(..) => {
                updates.queue(agent.id, Update::ChangeHealth(-1));
            }
            State::Cat(..) => updates.queue(agent.id, Update::Purr),
        }
    }

    fn update(&self, mut state: &mut Self::State, updates: Vec<Self::Update>) -> bool {
        let updated = updates.len() > 0;
        for update in updates {
            match *state {
                State::Cat(ref mut cat) => {
                    match update {
                        Update::Purr => {
                            cat.purrs += 1;
                        }
                        _ => (),
                    }
                }
                State::Person(ref mut person) => {
                    match update {
                        Update::ChangeHealth(change) => {
                            person.health += change;
                        }
                        _ => (),
                    }
                }
            }
        }
        updated
    }
}

fn main() {
    let sim = MultiSim {};
    let world = World {};

    // Setup the manager
    let addr = "redis://127.0.0.1/";
    let pop_client = Client::open(addr).unwrap();
    let mut manager = Manager::new(addr, pop_client, sim.clone());

    // Spawn the population
    manager.spawns(vec![State::Person(Person { health: 100 }), State::Cat(Cat { purrs: 0 })]);

    manager = run(sim, world, manager, 4, 10);
}
