//! Simple example of multiple types of agents.
//! You can use an enum, where each variant is a different type of agent.
//! Here we're just using fields on enum variants for state, but you could also define separate
//! state structs that you wrap enum variants around.

extern crate djinn;
extern crate redis;
extern crate redis_cluster;
extern crate rustc_serialize;

use std::thread;
use redis::Client;
use djinn::{Agent, Manager, Simulation, Population, Worker, Uuid, Redis};

#[derive(RustcDecodable, RustcEncodable, Debug, PartialEq, Clone)]
pub enum MyState {
    Person { name: String, health: isize },
    Cat { name: String, purrs: usize },
}

#[derive(RustcDecodable, RustcEncodable, Debug, PartialEq, Clone)]
pub struct MyWorld {
    weather: String,
}

#[derive(RustcDecodable, RustcEncodable, Debug, PartialEq, Clone)]
pub enum MyUpdate {
    ChangeName(String),
    ChangeHealth(isize),
    Purr,
}

#[derive(Clone)]
pub struct MySimulation;

impl Simulation for MySimulation {
    type State = MyState;
    type Update = MyUpdate;
    type World = MyWorld;

    fn setup<R: Redis>(&self, agent: Agent<Self::State>, population: &Population<Self, R>) -> () {}

    fn decide<R: Redis>(&self,
                        agent: Agent<Self::State>,
                        world: Self::World,
                        population: &Population<Self, R>)
                        -> Vec<(Uuid, Self::Update)> {
        let mut updates = Vec::new();
        match agent.state {
            MyState::Person { name, health } => {
                updates.push((agent.id, MyUpdate::ChangeHealth(-1)));
            }
            MyState::Cat { .. } => updates.push((agent.id, MyUpdate::Purr)),
        }
        updates
    }

    fn update(&self, state: Self::State, updates: Vec<Self::Update>) -> Self::State {
        let mut state = state.clone();
        for update in updates {
            match state {
                MyState::Cat { name, purrs } => {
                    state = self.update_cat(name, purrs, update);
                }
                MyState::Person { name, health } => {
                    state = self.update_person(name, health, update);
                }
            }
        }
        state
    }
}

impl MySimulation {
    fn update_cat(&self, name: String, purrs: usize, update: MyUpdate) -> MyState {
        match update {
            MyUpdate::Purr => {
                MyState::Cat {
                    name: name,
                    purrs: purrs + 1,
                }
            }
            _ => {
                MyState::Cat {
                    name: name,
                    purrs: purrs,
                }
            }
        }
    }

    fn update_person(&self, name: String, health: isize, update: MyUpdate) -> MyState {
        match update {
            MyUpdate::ChangeHealth(change) => {
                MyState::Person {
                    name: name,
                    health: health + change,
                }
            }
            _ => {
                MyState::Person {
                    name: name,
                    health: health,
                }
            }
        }
    }
}

fn main() {
    let person = MyState::Person {
        name: "hello".to_string(),
        health: 100,
    };
    let cat = MyState::Cat {
        name: "goodbye".to_string(),
        purrs: 0,
    };

    let sim = MySimulation {};
    let world = MyWorld { weather: "sunny".to_string() };

    // Setup the manager
    let addr = "redis://127.0.0.1/";
    let pop_client = Client::open(addr).unwrap();
    let mut manager = Manager::new(addr, pop_client, sim, world);

    // Spawn the population
    manager.population.spawn(person.clone());
    let id = manager.population.spawn(cat.clone());
    assert_eq!(manager.population.count(), 2);

    // Create a worker on a separate thread
    let worker_t = thread::spawn(move || {
        let sim = MySimulation {};
        let pop_client = Client::open(addr).unwrap();
        let worker = Worker::new(addr, pop_client, sim);
        worker.start();
    });

    let n_steps = 10;

    // Run the manager on a separate thread
    let manager_t = thread::spawn(move || {
        manager.run(n_steps);
        manager
    });

    manager = manager_t.join().unwrap();
    worker_t.join().unwrap();

    // Check that things are working
    let agent = match manager.population.get_agent(id) {
        Some(a) => a,
        None => panic!("Couldn't find the agent"),
    };
    println!("{:?}", agent);
}
