//! # Basic simulation
//! A really basic simulation to demonstrate
//! some features of Djinn:
//!
//! - how to write a basic simulation
//! - how to create a websocket server and publish to it
//! - how to publish and listen to events
//! - how to register a simulation reporter
//! - how to run a simulation

extern crate djinn;
extern crate redis;
extern crate rustc_serialize;

use std::thread;
use redis::{Client, Commands};
use djinn::{Agent, Manager, Simulation, Population, Updates, Redis, WebSocketServer, run};

const HEALTH_START: usize = 10;
const HEALTH_CHANGE: usize = 10;

#[derive(RustcDecodable, RustcEncodable, Debug, PartialEq, Clone)]
pub struct State {
    health: usize,
}

#[derive(RustcDecodable, RustcEncodable, Debug, PartialEq, Clone)]
pub struct World {
    weather: String,
}

#[derive(RustcDecodable, RustcEncodable, Debug, PartialEq, Clone)]
pub enum Update {
    ChangeHealth(usize),
}

#[derive(Clone)]
pub struct BasicSim;

impl Simulation for BasicSim {
    type State = State;
    type Update = Update;
    type World = World;

    fn decide<R: Redis>(&self,
                        agent: &Agent<Self::State>,
                        world: &Self::World,
                        population: &Population<Self, R>,
                        updates: &mut Updates<Self>)
                        -> () {
        updates.queue(agent.id, Update::ChangeHealth(HEALTH_CHANGE));
    }

    fn update(&self, mut state: &mut Self::State, updates: Vec<Self::Update>) -> bool {
        let old_health = state.health;
        for update in updates {
            match update {
                Update::ChangeHealth(health) => {
                    state.health += health;
                }
            }
        }
        state.health != old_health
    }
}

fn main() {
    let sim = BasicSim {};
    let world = World { weather: "sunny".to_string() };

    // Setup the manager
    let addr = "redis://127.0.0.1/";
    let client = Client::open(addr).unwrap();
    let mut manager = Manager::new(addr, client, sim.clone());

    // Spawn the population
    manager.spawn(State { health: 0 });
    let id = manager.spawn(State { health: HEALTH_START });

    // Create a websocket server to pass messages to frontend clients
    let mut ws = WebSocketServer::new("127.0.0.1:3012", addr);
    ws.start();

    // Give the frontend some time to connect
    thread::sleep_ms(2000);

    let n_steps = 10;

    // Create a client to listen to events
    let log_t = thread::spawn(move || {
        let client = Client::open(addr).unwrap();
        let mut pubsub = client.get_pubsub().unwrap();
        pubsub.subscribe("weather").unwrap();
        for step in 0..n_steps {
            let msg = pubsub.get_message().unwrap();
            let payload: String = msg.get_payload().unwrap();
            println!("[{:02}] This step's weather is {}", step, payload);
        }
    });

    // Register a really simple reporter
    manager.register_reporter(1, |step, pop, conn| {
        let world = pop.world();
        let _: () = conn.publish("weather", world.weather.clone()).unwrap();
        let _: () = conn.publish("ws", world.weather.clone()).unwrap();
    });

    manager = run(sim, world, manager, 4, n_steps);
    log_t.join().unwrap();

    // Check that things are working
    let agent = match manager.population.get_agent(id) {
        Some(a) => a,
        None => panic!("Couldn't find the agent"),
    };
    println!("{:?}", agent);
    assert_eq!(agent.state.health, HEALTH_START + (HEALTH_CHANGE * n_steps));

    // Shutdown the websocket server
    ws.shutdown();
}
