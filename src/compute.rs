use std::io;
use std::collections::HashMap;
use rmp_serialize::decode::Error;
use rmp_serialize::{Encoder, Decoder};
use rustc_serialize::{Encodable, Decodable};
use sim::{Agent, Simulation};
use redis::{Commands, Client, Connection, PipelineCommands, pipe};
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

/// An interface to the Redis-backed agent population.
pub struct Population<S: Simulation, C: Commands> {
    conn: C,
    simulation: S,
}

impl<S: Simulation, C: Commands> Population<S, C> {
    pub fn new(simulation: S, conn: C) -> Population<S, C> {
        Population {
            conn: conn,
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

    pub fn set_world(&self, world: S::World) {
        let data = encode(&world).unwrap();
        let _: () = self.conn.set("world", data).unwrap();
    }

    /// Create a new agent with the specified state, returning the new agent's id.
    pub fn spawn(&self, state: S::State) -> Uuid {
        let id = Uuid::new_v4();
        self.set_agent(id, state.clone());
        let _: () = self.conn.sadd("population", id.to_string()).unwrap();
        self.simulation.setup(Agent {
                                  id: id,
                                  state: state.clone(),
                              },
                              &self);
        id
    }

    /// Get an agent by id.
    pub fn get_agent(&self, id: Uuid) -> Option<Agent<S::State>> {
        let data = self.conn.get(id.to_string()).unwrap();
        Some(Agent {
            id: id,
            state: decode(data).unwrap(),
        })
    }

    /// Set an agent state by id.
    pub fn set_agent(&self, id: Uuid, state: S::State) {
        let data = encode(&state).unwrap();
        let _: () = self.conn.set(id.to_string(), data).unwrap();
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

    /// Reset the population.
    pub fn reset(&self) {
        // reset sets
        let _: () = self.conn.del("population").unwrap();
        self.reset_indices();
    }
}

pub struct Manager<S: Simulation, C: Commands> {
    conn: Connection,
    reporters: HashMap<usize, Box<Fn(&Population<S, C>, &Connection) -> () + Send>>,
    pub population: Population<S, C>,
}

impl<S: Simulation, C: Commands> Manager<S, C> {
    pub fn new(addr: &str, conn: C, simulation: S, world: S::World) -> Manager<S, C> {
        let population = Population::new(simulation, conn);
        population.reset();
        population.set_world(world);

        // conn for commanding workers
        let client = Client::open(addr).unwrap();
        let conn = client.get_connection().unwrap();

        let m = Manager {
            population: population,
            reporters: HashMap::new(),
            conn: conn,
        };
        m.reset();
        m
    }

    /// Reset the manager. This unregisters all workers and queues.
    pub fn reset(&self) {
        // start with "idle" phase
        let _: () = self.conn.set("current_phase", "idle").unwrap();

        // reset sets
        let _: () = self.conn.del("workers").unwrap();
        let _: () = self.conn.del("finished").unwrap();
        let _: () = self.conn.del("to_decide").unwrap();
        let _: () = self.conn.del("to_update").unwrap();
    }

    /// Run the simulation for `n_steps`.
    pub fn run(&self, n_steps: usize) -> () {
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
        where F: Fn(&Population<S, C>, &Connection) -> () + Send + 'static
    {
        self.reporters.insert(n_steps, Box::new(func));
    }

    fn wait_until_finished(&self) {
        while self.conn.scard::<&str, usize>("finished").unwrap() != self.n_workers() {
        }
    }

    /// Get the number of workers.
    pub fn n_workers(&self) -> usize {
        self.conn.scard::<&str, usize>("workers").unwrap()
    }
}

pub struct Worker<S: Simulation, C: Commands> {
    id: Uuid,
    manager: Client,
    population: Population<S, C>,
    simulation: S,
}

impl<S: Simulation, C: Commands> Worker<S, C> {
    pub fn new(addr: &str, conn: C, simulation: S) -> Worker<S, C> {
        Worker {
            id: Uuid::new_v4(),
            manager: Client::open(addr).unwrap(),
            population: Population::new(simulation.clone(), conn),
            simulation: simulation,
        }
    }

    pub fn start(&self) {
        // register with the manager
        let _: () = self.manager.sadd("workers", self.id.to_string()).unwrap();

        // subscribe to the command channel
        let mut pubsub = self.manager.get_pubsub().unwrap();
        pubsub.subscribe("command").unwrap();

        // check what the current phase is
        let phase: String = self.manager.get("current_phase").unwrap();
        self.process_cmd(phase.as_ref(), &self.manager);

        loop {
            let msg = pubsub.get_message().unwrap();
            let payload: String = msg.get_payload().unwrap();
            self.process_cmd(payload.as_ref(), &self.manager);
            if payload == "terminate" {
                break;
            }
        }
    }

    fn process_cmd(&self, cmd: &str, conn: &Client) {
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

    fn decide(&self, simulation: &S, conn: &Client) {
        let world = self.population.world();
        while let Ok(id) = conn.spop::<&str, String>("to_decide") {
            let id = Uuid::parse_str(&id).unwrap();
            match self.population.get_agent(id) {
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

    fn update(&self, simulation: &S, conn: &Client) {
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
            match self.population.get_agent(id) {
                Some(agent) => {
                    let new_state = simulation.update(agent.state.clone(), updates);
                    self.population.set_agent(id, new_state);
                    let _: () = conn.sadd("to_decide", id.to_string()).unwrap();
                }
                None => (),
            }
        }
    }
}
