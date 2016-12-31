extern crate djinn;
extern crate redis;
extern crate rustc_serialize;

use std::thread;
use redis::{Commands, Client};
use djinn::{Agent, Manager, Simulation, Population, Worker, Uuid, ws_server};

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

    // Create a websocket server to pass messages to frontend clients
    let ws_t = ws_server("127.0.0.1:3012", addr);

    // Give the frontend some time to connect
    thread::sleep_ms(2000);

    // Create a client to listen to our reports
    let log_t = thread::spawn(move || {
        let client = Client::open(addr).unwrap();
        let mut pubsub = client.get_pubsub().unwrap();
        pubsub.subscribe("weather").unwrap();
        for _ in 0..n_steps {
            let msg = pubsub.get_message().unwrap();
            let payload: String = msg.get_payload().unwrap();
            println!("This step's weather is {}", payload);
        }
    });

    // Register a really simple reporter
    manager.register_reporter(1, |pop, conn| {
        let world = pop.world();
        let _: () = conn.publish("weather", world.weather.clone()).unwrap();
        pop.ws_emit(world.weather.clone());
    });

    let manager_t = thread::spawn(move || {
        manager.start(n_steps);
        manager
    });

    manager = manager_t.join().unwrap();
    worker_t.join().unwrap();
    log_t.join().unwrap();

    let agent = match manager.population.get(id) {
        Some(a) => a,
        None => panic!("Couldn't find the agent"),
    };

    println!("{:?}", agent);
    assert_eq!(agent.state.health, health + (12 * n_steps));

    ws_t.join().unwrap();
}
