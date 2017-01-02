extern crate djinn;
extern crate redis;
extern crate redis_cluster;
extern crate rustc_serialize;

use std::thread;
use redis::{Commands, Client};
use redis_cluster::Cluster;
use djinn::{Agent, Manager, Simulation, Population, Worker, Uuid, WebSocketServer};

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

    fn setup<C: Commands>(&self,
                          agent: Agent<Self::State>,
                          population: &Population<Self, C>)
                          -> () {
        population.index(agent.state.name.as_ref(), agent.id.clone());
    }

    fn decide<C: Commands>(&self,
                           agent: Agent<Self::State>,
                           world: Self::World,
                           population: &Population<Self, C>)
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

    let sim = MySimulation {};
    let world = MyWorld { weather: "sunny".to_string() };

    // Setup the manager
    let addr = "redis://127.0.0.1/";

    let startup_nodes =
        vec!["redis://127.0.0.1:7000", "redis://127.0.0.1:7001", "redis://127.0.0.1:7002"];
    let pop_client = Cluster::new(startup_nodes.clone());
    // let pop_client = Client::open(addr).unwrap();
    let mut manager = Manager::new(addr, pop_client, sim, world);

    // Spawn the population
    manager.population.spawn(state.clone());
    let id = manager.population.spawn(state2.clone());
    assert_eq!(manager.population.count(), 2);

    // Create a worker on a separate thread
    let worker_t = thread::spawn(move || {
        let sim = MySimulation {};
        // let pop_client = Client::open(addr).unwrap();
        let pop_client = Cluster::new(startup_nodes.clone());
        let worker = Worker::new(addr, pop_client, sim);
        worker.start();
    });

    // Create a websocket server to pass messages to frontend clients
    let mut ws = WebSocketServer::new("127.0.0.1:3012", addr);
    ws.start();

    // Give the frontend some time to connect
    thread::sleep_ms(2000);

    let n_steps = 10;

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
        let _: () = conn.publish("ws", world.weather.clone()).unwrap();
    });

    // Run the manager on a separate thread
    let manager_t = thread::spawn(move || {
        manager.run(n_steps);
        manager
    });

    manager = manager_t.join().unwrap();
    worker_t.join().unwrap();
    log_t.join().unwrap();

    // Check that things are working
    let agent = match manager.population.get_agent(id) {
        Some(a) => a,
        None => panic!("Couldn't find the agent"),
    };
    println!("{:?}", agent);
    assert_eq!(agent.state.health, health + (12 * n_steps));

    // Shutdown the websocket server
    ws.shutdown();
}
