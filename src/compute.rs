use std::io;
use std::ops::Deref;
use std::collections::HashMap;
use rmp_serialize::decode::Error;
use rmp_serialize::{Encoder, Decoder};
use rustc_serialize::{Encodable, Decodable};
use sim::{Agent, State, Simulation};
use redis::{Commands, Connection, Client, PipelineCommands, pipe};
use uuid::Uuid;

fn decode<R: Decodable>(inp: Vec<u8>) -> Result<R, Error> {
    let mut decoder = Decoder::new(&inp[..]);
    Decodable::decode(&mut decoder)
}

fn encode<R: Encodable>(data: R) -> Result<Vec<u8>, io::Error> {
    let mut buf = Vec::<u8>::new();
    match data.encode(&mut Encoder::new(&mut buf)) {
        Ok(_) => Ok(buf),
        Err(e) => Err(io::Error::new(io::ErrorKind::Other, format!("{}", e))),
    }
}

fn get_agent<S: State>(id: Uuid, conn: &Connection) -> Option<Agent<S>> {
    let data = conn.get(id.to_string()).unwrap();
    Some(Agent {
        id: id,
        state: decode(data).unwrap(),
    })
}

fn set_agent<S: State>(id: Uuid, state: S, conn: &Connection) {
    // let data = json::encode(&state).unwrap();
    let data = encode(&state).unwrap();
    let _: () = conn.set(id.to_string(), data).unwrap();
}

/// An interface to the agent population.
pub struct Population<S: Simulation> {
    conn: Connection,
    simulation: S,
}

impl<S: Simulation> Population<S> {
    pub fn new(addr: &str, simulation: S) -> Population<S> {
        let client = Client::open(addr).unwrap();
        Population {
            conn: client.get_connection().unwrap(),
            simulation: simulation,
        }
    }

    pub fn count(&self) -> usize {
        self.conn.scard::<&str, usize>("population").unwrap()
    }

    pub fn world(&self) -> S::World {
        let data = self.conn.get("world").unwrap();
        decode(data).unwrap()
    }

    /// Send a String to websocket clients.
    pub fn ws_emit(&self, message: String) -> () {
        let _: () = self.conn.publish("ws", message).unwrap();
    }

    /// Create a new agent with the specified state, returning the new agent's id.
    pub fn spawn(&self, state: S::State) -> Uuid {
        let id = Uuid::new_v4();
        set_agent(id, state.clone(), &self.conn);
        let _: () = self.conn.sadd("population", id.to_string()).unwrap();
        self.simulation.setup(Agent {
                                  id: id,
                                  state: state.clone(),
                              },
                              &self);
        id
    }

    /// Get an agent by id.
    pub fn get(&self, id: Uuid) -> Option<Agent<S::State>> {
        get_agent(id, &self.conn)
    }

    /// Deletes an agent by id.
    pub fn kill(&self, id: Uuid) {
        let _: () = self.conn.del(id.to_string()).unwrap();
        let _: () = self.conn.srem("population", id.to_string()).unwrap();
    }

    /// Lookup agents at a particular index.
    pub fn lookup(&self, index: &str) -> Vec<Agent<S::State>> {
        let ids: Vec<String> = self.conn.smembers(format!("idx:{}", index)).unwrap();
        match ids.len() {
            0 => Vec::new(),
            1 => {
                let id = ids[0].as_ref();
                let state_data: Vec<u8> = self.conn.get(id).unwrap();
                let agent = Agent {
                    id: Uuid::parse_str(id).unwrap(),
                    state: decode(state_data).unwrap(),
                };
                vec![agent]
            }
            _ => {
                let states_data: Vec<Vec<u8>> = self.conn.get(ids.clone()).unwrap();
                ids.iter()
                    .zip(states_data)
                    .map(|(id, state_data)| {
                        Agent {
                            id: Uuid::parse_str(id).unwrap(),
                            state: decode(state_data).unwrap(),
                        }
                    })
                    .collect()
            }
        }
    }

    /// Add an agent (id) to an index.
    pub fn index(&self, index: &str, id: Uuid) {
        let _: () = self.conn.sadd(format!("idx:{}", index), id.to_string()).unwrap();
    }

    /// Reset indices.
    pub fn reset_indices(&self) {
        let keys: Vec<String> = self.conn.keys("idx:*").unwrap();
        if keys.len() > 0 {
            let _: () = self.conn.del(keys).unwrap();
        }
    }
}

pub struct Manager<S: Simulation> {
    conn: Connection,
    reporters: HashMap<usize, Box<Fn(&Population<S>, &Connection) -> () + Send>>,
    pub population: Population<S>,
}

impl<S: Simulation> Manager<S> {
    pub fn new(addr: &str, simulation: S, world: S::World) -> Manager<S> {
        let client = Client::open(addr).unwrap();
        let conn = client.get_connection().unwrap();

        let data = encode(&world).unwrap();
        let _: () = conn.set("world", data).unwrap();

        // reset sets
        let _: () = conn.del("workers").unwrap();
        let _: () = conn.del("finished").unwrap();
        let _: () = conn.del("population").unwrap();
        let _: () = conn.del("to_decide").unwrap();
        let _: () = conn.del("to_update").unwrap();

        let population = Population::new(addr, simulation);
        population.reset_indices();

        // start with "idle" phase
        let _: () = conn.set("current_phase", "idle").unwrap();

        Manager {
            conn: conn,
            population: population,
            reporters: HashMap::new(),
        }
    }

    pub fn start(&self, n_steps: usize) -> () {
        let mut steps = 0;

        if self.n_workers() == 0 {
            println!("Waiting for at least one worker...");
            while self.n_workers() == 0 {
            }
            println!("Ok");
        }

        // copy population to the "to_decide" set
        let _: () = self.conn.sunionstore("to_decide", "population").unwrap();

        while steps < n_steps {
            // run any register reporters, if appropriate
            for (interval, reporter) in self.reporters.iter() {
                if steps % interval == 0 {
                    reporter(&self.population, &self.conn);
                }
            }

            let _: () = self.conn.publish("command", "decide").unwrap();
            let _: () = self.conn.set("current_phase", "decide").unwrap();
            self.wait_until_finished();
            let _: () = self.conn.publish("command", "update").unwrap();
            let _: () = self.conn.set("current_phase", "update").unwrap();
            self.wait_until_finished();
            steps += 1;
        }

        let _: () = self.conn.publish("command", "terminate").unwrap();
    }

    /// Register a reporter function to be called every `n_steps`.
    /// It receives a `Population` which can be used to query agents,
    /// compute aggregate statistics, etc, and a Redis connection
    /// that can be used, for example, to send reports via pubsub.
    pub fn register_reporter<F>(&mut self, n_steps: usize, func: F) -> ()
        where F: Fn(&Population<S>, &Connection) -> () + Send + 'static
    {
        self.reporters.insert(n_steps, Box::new(func));
    }

    fn wait_until_finished(&self) {
        while self.conn.scard::<&str, usize>("finished").unwrap() != self.n_workers() {
        }
    }

    pub fn n_workers(&self) -> usize {
        self.conn.scard::<&str, usize>("workers").unwrap()
    }
}

pub struct Worker<S: Simulation> {
    id: Uuid,
    addr: String,
    population: Population<S>,
    simulation: S,
}

impl<S: Simulation> Worker<S> {
    pub fn new(addr: &str, simulation: S) -> Worker<S> {
        Worker {
            id: Uuid::new_v4(),
            addr: addr.to_owned(),
            population: Population::new(addr, simulation.clone()),
            simulation: simulation,
        }
    }

    pub fn start(&self) {
        let client = Client::open(self.addr.deref()).unwrap();
        let conn = client.get_connection().unwrap();

        // register with the manager
        let _: () = conn.sadd("workers", self.id.to_string()).unwrap();

        // subscribe to the command channel
        let mut pubsub = client.get_pubsub().unwrap();
        pubsub.subscribe("command").unwrap();

        // check what the current phase is
        let phase: String = client.get("current_phase").unwrap();
        self.process_cmd(phase.as_ref(), &conn);

        loop {
            let msg = pubsub.get_message().unwrap();
            let payload: String = msg.get_payload().unwrap();
            self.process_cmd(payload.as_ref(), &conn);
            if payload == "terminate" {
                break;
            }
        }
    }

    fn process_cmd(&self, cmd: &str, conn: &Connection) {
        match cmd {
            "terminate" => {
                let _: () = conn.srem("workers", self.id.to_string()).unwrap();
            }
            "decide" => {
                self.decide(&self.simulation, &conn);
                let _: () = conn.sadd("finished", self.id.to_string()).unwrap();
            }
            "update" => {
                self.update(&self.simulation, &conn);
                let _: () = conn.sadd("finished", self.id.to_string()).unwrap();
            }
            "idle" => (),
            s => println!("Unrecognized command: {}", s),
        }
    }

    fn decide(&self, simulation: &S, conn: &Connection) {
        let world: S::World = {
            let world_data = conn.get("world").unwrap();
            decode(world_data).unwrap()
        };

        while let Ok(id) = conn.spop::<&str, String>("to_decide") {
            let id = Uuid::parse_str(&id).unwrap();
            match get_agent::<S::State>(id, &conn) {
                Some(agent) => {
                    let updates = simulation.decide(agent, world.clone(), &self.population);
                    let mut rpipe = pipe();
                    for (id, update) in updates {
                        let data = encode(&update).unwrap();
                        rpipe.lpush(format!("updates:{}", id), data).ignore();
                    }
                    rpipe.sadd("to_update", id.to_string()).ignore();
                    let _: () = rpipe.query(conn).unwrap();
                }
                None => (),
            }
        }
    }

    fn update(&self, simulation: &S, conn: &Connection) {
        while let Ok(id) = conn.spop::<&str, String>("to_update") {
            let updates: Vec<S::Update> = {
                let key = format!("updates:{}", id);
                let updates_data: Vec<Vec<u8>> = conn.lrange(&key, 0, -1)
                    .unwrap();
                let _: () = conn.del(&key).unwrap();
                if updates_data.len() == 0 {
                    Vec::new()
                } else {
                    updates_data.iter().map(|data| decode(data.clone()).unwrap()).collect()
                }
            };
            let id = Uuid::parse_str(&id).unwrap();
            match get_agent::<S::State>(id, &conn) {
                Some(agent) => {
                    let new_state = simulation.update(agent.state.clone(), updates);
                    set_agent(id, new_state, &conn);
                    let _: () = conn.sadd("to_decide", id.to_string()).unwrap();
                }
                None => (),
            }
        }
    }
}
