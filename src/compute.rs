use std::io;
use std::thread;
use std::collections::HashMap;
use rmp_serialize::decode::Error;
use rmp_serialize::{Encoder, Decoder};
use rustc_serialize::{Encodable, Decodable};
use sim::{Agent, Simulation, State};
use redis::{Commands, Client};
use uuid::Uuid;

pub trait Redis: Commands + Send + Clone {}
impl<T> Redis for T where T: Commands + Send + Clone {}

// use hash tags to ensure these keys hash to the same slot
const POPULATION_KEY: &'static str = "{pop}population";
const TO_DECIDE_KEY: &'static str = "{pop}to_decide";
const TO_UPDATE_KEY: &'static str = "{pop}to_update";
const POP_UPDATES_KEY: &'static str = "{pop}updates";

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

#[derive(RustcDecodable, RustcEncodable, Debug, PartialEq, Clone)]
pub enum PopulationUpdate<S: State> {
    Spawn(Uuid, S),
    Kill(Uuid),
}

/// An interface to the Redis-backed agent population.
pub struct Population<S: Simulation, C: Redis> {
    conn: C,
    simulation: S,
}

impl<S: Simulation, C: Redis> Population<S, C> {
    pub fn new(simulation: S, conn: C) -> Population<S, C> {
        Population {
            conn: conn,
            simulation: simulation,
        }
    }

    pub fn count(&self) -> usize {
        self.conn.scard::<&str, usize>(POPULATION_KEY).unwrap()
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
    /// This does not actually spawn the agent, it just queues it.
    /// Run the `update` method to execute it (and other queued updates).
    pub fn spawn(&self, state: S::State) -> Uuid {
        let id = Uuid::new_v4();
        let update = PopulationUpdate::Spawn(id, state);
        let data = encode(&update).unwrap();
        let _: () = self.conn.sadd(POP_UPDATES_KEY, data).unwrap();
        id
    }

    fn _spawn(&self, id: Uuid, state: S::State) {
        self.set_agent(id, state.clone());
        let _: () = self.conn.sadd(POPULATION_KEY, id.to_string()).unwrap(); // TODO should add to to_decide
        self.simulation.setup(Agent {
                                  id: id,
                                  state: state.clone(),
                              },
                              &self);
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
    /// This does not actually execute the kill, it just queues it.
    /// Run the `update` method to execute it (and other queued updates).
    pub fn kill(&self, id: Uuid) {
        let update: PopulationUpdate<S::State> = PopulationUpdate::Kill(id);
        let data = encode(&update).unwrap();
        let _: () = self.conn.sadd(POP_UPDATES_KEY, data).unwrap();
    }

    fn _kill(&self, id: Uuid) {
        let _: () = self.conn.del(id.to_string()).unwrap();
        let _: () = self.conn.srem(POPULATION_KEY, id.to_string()).unwrap();
        let _: () = self.conn.srem(TO_DECIDE_KEY, id.to_string()).unwrap();
        let _: () = self.conn.srem(TO_UPDATE_KEY, id.to_string()).unwrap();
    }

    /// Process queued updates.
    pub fn update(&self) {
        while let Ok(data) = self.conn.spop::<&str, Vec<u8>>(POP_UPDATES_KEY) {
            // this doesn't know when to stop popping;
            // it will pop empty vecs when it's done
            if data.len() > 0 {
                let update: PopulationUpdate<S::State> = decode(data).unwrap();
                match update {
                    PopulationUpdate::Kill(id) => self._kill(id),
                    PopulationUpdate::Spawn(id, state) => self._spawn(id, state),
                }
            } else {
                break;
            }
        }
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
                ids.iter()
                    .map(|id| {
                        let state_data = self.conn.get(id).unwrap();
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
        let _: () = self.conn.del(POPULATION_KEY).unwrap();
        let _: () = self.conn.del(TO_DECIDE_KEY).unwrap();
        let _: () = self.conn.del(TO_UPDATE_KEY).unwrap();
        let _: () = self.conn.del(POP_UPDATES_KEY).unwrap();
        self.reset_indices();
    }
}

pub struct Manager<S: Simulation, C: Redis> {
    addr: String,
    conn: Client,
    reporters: HashMap<usize, Box<Fn(&Population<S, C>, &Client) -> () + Send>>,
    pub population: Population<S, C>,
}

impl<S: Simulation, C: Redis> Manager<S, C> {
    pub fn new(addr: &str, conn: C, simulation: S, world: S::World) -> Manager<S, C> {
        let population = Population::new(simulation, conn);
        population.reset();
        population.set_world(world);

        // conn for commanding workers
        let client = Client::open(addr).unwrap();

        let m = Manager {
            addr: addr.to_owned(),
            population: population,
            reporters: HashMap::new(),
            conn: client,
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
        let _: () = self.population.conn.sunionstore(TO_DECIDE_KEY, POPULATION_KEY).unwrap();

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
            self.population.update();
            steps += 1;
        }

        let _: () = self.conn.publish("command", "terminate").unwrap();
    }

    /// Register a reporter function to be called every `n_steps`.
    /// It receives a `Population` which can be used to query agents,
    /// compute aggregate statistics, etc, and a Redis connection
    /// that can be used, for example, to send reports via pubsub.
    pub fn register_reporter<F>(&mut self, n_steps: usize, func: F) -> ()
        where F: Fn(&Population<S, C>, &Client) -> () + Send + 'static
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

pub struct Worker<S: Simulation, C: Redis> {
    id: Uuid,
    manager: Client,
    population: Population<S, C>,
    simulation: S,
}

impl<S: Simulation, C: Redis> Worker<S, C> {
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
        self.process_cmd(phase.as_ref());

        loop {
            let msg = pubsub.get_message().unwrap();
            let payload: String = msg.get_payload().unwrap();
            self.process_cmd(payload.as_ref());
            if payload == "terminate" {
                break;
            }
        }
    }

    fn process_cmd(&self, cmd: &str) {
        match cmd {
            "terminate" => {
                let _: () = self.manager.srem("workers", self.id.to_string()).unwrap();
            }
            "decide" => {
                self.decide();
                let _: () = self.manager.sadd("finished", self.id.to_string()).unwrap();
            }
            "update" => {
                self.update();
                let _: () = self.manager.sadd("finished", self.id.to_string()).unwrap();
            }
            "idle" => (),
            s => println!("Unrecognized command: {}", s),
        }
    }

    fn decide(&self) {
        let world = self.population.world();
        while let Ok(id) = self.population.conn.spop::<&str, String>(TO_DECIDE_KEY) {
            let id = Uuid::parse_str(&id).unwrap();
            match self.population.get_agent(id) {
                Some(agent) => {
                    let updates = self.simulation.decide(agent, world.clone(), &self.population);
                    // let mut rpipe = pipe();
                    // for (id, update) in updates {
                    //     let data = encode(&update).unwrap();
                    //     rpipe.lpush(format!("updates:{}", id), data).ignore();
                    // }
                    // rpipe.sadd(TO_UPDATE_KEY, id.to_string()).ignore();
                    // let _: () = rpipe.query(&self.population.conn).unwrap();

                    for (id, update) in updates {
                        let data = encode(&update).unwrap();
                        let _: () =
                            self.population.conn.lpush(format!("updates:{}", id), data).unwrap();
                    }
                    let _: () = self.population.conn.sadd(TO_UPDATE_KEY, id.to_string()).unwrap();
                }
                None => (),
            }
        }
    }

    fn update(&self) {
        while let Ok(id) = self.population.conn.spop::<&str, String>(TO_UPDATE_KEY) {
            let updates: Vec<S::Update> = {
                let key = format!("updates:{}", id);
                let updates_data: Vec<Vec<u8>> = self.population
                    .conn
                    .lrange(&key, 0, -1)
                    .unwrap();
                let _: () = self.population.conn.del(&key).unwrap();
                if updates_data.len() == 0 {
                    Vec::new()
                } else {
                    updates_data.iter().map(|data| decode(data.clone()).unwrap()).collect()
                }
            };
            let id = Uuid::parse_str(&id).unwrap();
            match self.population.get_agent(id) {
                Some(agent) => {
                    let new_state = self.simulation.update(agent.state.clone(), updates);
                    self.population.set_agent(id, new_state);
                    let _: () = self.population.conn.sadd(TO_DECIDE_KEY, id.to_string()).unwrap();
                }
                None => (),
            }
        }
    }
}


/// Convenience function for running a simulation/manager with a n workers.
/// This blocks until the simulation is finished running.
pub fn run<S: Simulation + 'static, R: Redis + 'static>(sim: S,
                                                        manager: Manager<S, R>,
                                                        n_workers: usize,
                                                        n_steps: usize)
                                                        -> Manager<S, R> {

    let addr = manager.addr.clone();
    let pop_client = manager.population.conn.clone();

    // run the manager on a separate thread
    let manager_t = thread::spawn(move || {
        manager.run(n_steps);
        manager
    });

    run_workers(addr.as_ref(), pop_client, sim.clone(), n_workers);
    manager_t.join().unwrap()
}

/// Convenience function to run a node of n workers.
/// This blocks until the workers are done.
pub fn run_workers<S: Simulation + 'static, R: Redis + 'static>(addr: &str,
                                                                pop_client: R,
                                                                sim: S,
                                                                n_workers: usize) {
    let addr = addr.to_owned();
    let worker_ts: Vec<thread::JoinHandle<()>> = (0..n_workers)
        .map(|_| {
            // create a worker on a separate thread
            let addr = addr.clone();
            let sim = sim.clone();
            let pop_client = pop_client.clone();
            thread::spawn(move || {
                let worker = Worker::new(addr.as_ref(), pop_client, sim);
                worker.start();
            })
        })
        .collect();

    // block til done running
    for t in worker_ts {
        t.join().unwrap();
    }
}
