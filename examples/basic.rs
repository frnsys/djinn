extern crate djinn;
extern crate rustc_serialize;

use std::thread;
use djinn::{Agent, Manager, Simulation, Population, Worker, Uuid};

#[derive(RustcDecodable, RustcEncodable, Debug, PartialEq, Clone)]
pub struct MyState {
    name: String,
    health: usize,
}

#[derive(RustcDecodable, RustcEncodable, Debug, PartialEq, Clone)]
pub struct MyWorld {
    weather: String,
}

#[derive(RustcDecodable, RustcEncodable, Debug, PartialEq, Clone)]
pub enum MyUpdate {
    ChangeName(String),
    ChangeHealth(usize),
}

#[derive(Clone)]
pub struct MySimulation;

impl Simulation for MySimulation {
    type State = MyState;
    type Update = MyUpdate;
    type World = MyWorld;

    fn setup(&self, agent: Agent<Self::State>, population: &Population<Self>) -> () {
        population.index(agent.state.name.as_ref(), agent.id.clone());
    }

    fn decide(&self,
              agent: Agent<Self::State>,
              world: Self::World,
              population: &Population<Self>)
              -> Vec<(Uuid, Self::Update)> {
        let mut updates = Vec::new();
        match agent.state.name.as_ref() {
            "hello" => {
                let agents = population.lookup("goodbye");
                for a in agents {
                    updates.push((a.id, MyUpdate::ChangeHealth(12)));
                }
            }
            "goodbye" => (),
            _ => println!("my name is unknown"),
        }
        updates
    }

    fn update(&self, state: Self::State, updates: Vec<Self::Update>) -> Self::State {
        let mut state = state.clone();
        for update in updates {
            state = match update {
                MyUpdate::ChangeName(name) => {
                    MyState {
                        name: name,
                        health: state.health,
                    }
                }
                MyUpdate::ChangeHealth(health) => {
                    MyState {
                        name: state.name,
                        health: state.health + health,
                    }
                }
            }
        }
        state
    }
}

fn main() {
    let health = 10;
    let state = MyState {
        name: "hello".to_string(),
        health: 0,
    };
    let state2 = MyState {
        name: "goodbye".to_string(),
        health: health,
    };
    let addr = "redis://127.0.0.1/";
    let world = MyWorld { weather: "sunny".to_string() };
    let sim = MySimulation {};
    let mut manager = Manager::<MySimulation>::new(addr, sim, world);

    manager.population.spawn(state.clone());
    let id = manager.population.spawn(state2.clone());
    assert_eq!(manager.population.count(), 2);

    let worker_t = thread::spawn(move || {
        let sim = MySimulation {};
        let worker = Worker::new(addr, sim);
        worker.start();
    });

    let n_steps = 10;
    let manager_t = thread::spawn(move || {
        manager.start(n_steps);
        manager
    });

    manager = manager_t.join().unwrap();
    worker_t.join().unwrap();

    let agent = match manager.population.get(id) {
        Some(a) => a,
        None => panic!("Couldn't find the agent"),
    };

    println!("{:?}", agent);
    assert_eq!(agent.state.health, health + (12 * n_steps));
}
