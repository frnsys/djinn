use std::io;
use std::thread;
use std::collections::{HashMap, HashSet};
use rmp_serialize::decode::Error;
use rmp_serialize::{Encoder, Decoder};
use rustc_serialize::{Encodable, Decodable};
use sim::{Agent, Simulation, State};
use redis::{Cmd, Commands, Client};
use uuid::Uuid;

use time::PreciseTime;

pub trait Redis: Commands + Send + Sync + Clone {}
impl<T> Redis for T where T: Commands + Send + Sync + Clone {}

// use hash tags to ensure these keys hash to the same slot
const POPULATION_KEY: &'static str = "{pop}population";
const TO_DECIDE_KEY: &'static str = "{pop}to_decide";
const TO_UPDATE_KEY: &'static str = "{pop}to_update";
const POP_UPDATES_KEY: &'static str = "{pop}updates";

// How many agents workers should fetch and process at once.
// This larger this is, the less network communication with Redis (i.e. reduces overhead).
// But if you make this too large, the advantage of multiple workers decreases.
// TODO this should probably be configurable by the end user
const THROUGHPUT: usize = 500;

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

pub struct Updates<S: Simulation> {
    updates: HashMap<String, Vec<Vec<u8>>>,
    pop_updates: Vec<PopulationUpdate<S::State>>,
    to_update: HashSet<String>,
}

impl<S: Simulation> Updates<S> {
    pub fn new() -> Updates<S> {
        Updates {
            updates: HashMap::new(),
            pop_updates: Vec::new(),
            to_update: HashSet::new(),
        }
    }

    pub fn queue(&mut self, id: &String, update: S::Update) {
        let key = format!("updates:{}", id);
        let data = encode(&update).unwrap();
        let mut updates = self.updates.entry(key).or_insert(Vec::new());
        updates.push(data);
        self.to_update.insert(id.clone());
    }

    pub fn queue_world(&mut self, update: S::Update) {
        let data = encode(&update).unwrap();
        let mut updates = self.updates.entry("updates:world".to_string()).or_insert(Vec::new());
        updates.push(data);
    }

    /// Create a new agent with the specified state, returning the new agent's id.
    /// This does not actually spawn the agent, it just queues it.
    /// Run the `update` method to execute it (and other queued updates).
    pub fn spawn(&mut self, state: S::State) -> String {
        let id = Uuid::new_v4().to_string();
        let update = PopulationUpdate::Spawn(id.clone(), state);
        self.pop_updates.push(update);
        id
    }

    /// Deletes an agent by id.
    /// This does not actually execute the kill, it just queues it.
    /// Run the `update` method to execute it (and other queued updates).
    pub fn kill(&mut self, agent: Agent<S::State>) {
        let update: PopulationUpdate<S::State> = PopulationUpdate::Kill(agent.id, agent.state);
        self.pop_updates.push(update);
    }

    /// Push these local updates to Redis.
    fn push<R: Redis>(&mut self, pop: &Population<S, R>) {
        for (key, updates) in self.updates.drain() {
            let _: () = pop.conn
                .lpush(key, updates)
                .unwrap();
        }
        if self.pop_updates.len() > 0 {
            let pop_updates: Vec<Vec<u8>> =
                self.pop_updates.iter().map(|u| encode(u).unwrap()).collect();
            let _: () = pop.conn.sadd(POP_UPDATES_KEY, pop_updates).unwrap();
        }
        if self.to_update.len() > 0 {
            let updates: Vec<String> = self.to_update.drain().collect();
            let _: () = pop.conn
                .sadd(TO_UPDATE_KEY, updates)
                .unwrap();
        }
    }
}

#[derive(RustcDecodable, RustcEncodable, Debug, PartialEq, Clone)]
pub enum PopulationUpdate<S: State> {
    Spawn(String, S),
    Kill(String, S),
}

/// An interface to the Redis-backed agent population.
pub struct Population<S: Simulation, C: Redis> {
    pub conn: C,
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


    /// Get an agent by id.
    pub fn get_agent(&self, id: String) -> Option<Agent<S::State>> {
        let data = self.conn.get(id.clone()).unwrap();
        Some(Agent {
            id: id,
            state: decode(data).unwrap(),
        })
    }

    /// Get agents by ids.
    /// If you need to fetch multiple agents, you should use this as it makes only one network
    /// request.
    // TODO this will probably not work with a redis cluster b/c each id hashes to a different
    // slot...
    pub fn get_agents(&self, ids: Vec<String>) -> Vec<Agent<S::State>> {
        if ids.len() == 1 {
            let id = ids[0].clone();
            let agent = self.get_agent(id).unwrap();
            vec![agent]
        } else if ids.len() > 0 {
            let datas = self.conn.get::<Vec<String>, Vec<Vec<u8>>>(ids.clone()).unwrap();
            ids.iter()
                .zip(datas.iter())
                .map(|(id, data)| {
                    Agent {
                        id: id.to_string(),
                        state: decode(data.clone()).unwrap(),
                    }
                })
                .collect()
        } else {
            Vec::new()
        }
    }

    /// Set an agent state by id.
    pub fn set_agent(&self, id: String, state: S::State) {
        let data = encode(&state).unwrap();
        let _: () = self.conn.set(id, data).unwrap();
    }

    /// Set multple agent states by ids.
    /// If you need to update multiple agents, you should use this as it makes only one network
    /// request.
    pub fn set_agents(&self, mut updates: Vec<(String, S::State)>) {
        if updates.len() == 1 {
            let (id, state) = updates.pop().unwrap();
            self.set_agent(id, state);
        } else if updates.len() > 0 {
            let encoded: Vec<(String, Vec<u8>)> = updates.drain(..)
                .map(|(id, state)| (id, encode(&state).unwrap()))
                .collect();
            let _: () = self.conn.set_multiple(encoded.as_slice()).unwrap();
        }
    }

    fn spawns(&self, mut to_spawn: Vec<(String, S::State)>) {
        if to_spawn.len() > 0 {
            let start = PreciseTime::now();
            let ids: Vec<String> = to_spawn.iter().map(|&(ref id, _)| id.clone()).collect();
            let end = PreciseTime::now();
            println!("poppppp: \textracting ids took: {}", start.to(end));

            // TODO pipeline this?
            let start = PreciseTime::now();
            let _: () = self.conn.sadd(POPULATION_KEY, ids.clone()).unwrap();
            let end = PreciseTime::now();
            println!("poppppp: \tadding ids took: {}", start.to(end));

            let start = PreciseTime::now();
            let agents = to_spawn.iter()
                .map(|&(ref id, ref state)| {
                    Agent {
                        id: id.clone(),
                        state: state.clone(),
                    }
                })
                .collect();
            let end = PreciseTime::now();
            println!("poppppp: \textracting agents took: {}", start.to(end));

            let start = PreciseTime::now();
            self.set_agents(to_spawn);
            let end = PreciseTime::now();
            println!("poppppp: \tsetting states took: {}", start.to(end));

            let start = PreciseTime::now();
            self.simulation.on_spawns(agents, &self);
            let end = PreciseTime::now();
            println!("poppppp: \ton spawning took: {}", start.to(end));
        }
    }


    fn kills(&self, mut to_kill: Vec<(String, S::State)>) {
        if to_kill.len() > 0 {
            let ids: Vec<String> = to_kill.iter().map(|&(ref id, _)| id.clone()).collect();

            // TODO pipeline this? or use set operations?
            let _: () = self.conn.del(ids.clone()).unwrap();
            let _: () = self.conn.srem(POPULATION_KEY, ids.clone()).unwrap();

            let agents = to_kill.drain(..)
                .map(|(id, state)| {
                    Agent {
                        id: id,
                        state: state,
                    }
                })
                .collect();
            self.simulation.on_deaths(agents, &self);
        }
    }

    // TODO these should be handled by workers too
    /// Process queued updates.
    pub fn update(&self) {
        let mut to_kill = Vec::new();
        let mut to_spawn = Vec::new();
        let n_updates: usize = self.conn.scard::<&str, usize>(POP_UPDATES_KEY).unwrap();
        println!("queued updates: {:?}", n_updates);
        let updates = self.conn.smembers::<&str, Vec<Vec<u8>>>(POP_UPDATES_KEY).unwrap();

        let start = PreciseTime::now();
        for data in updates {
            let update: PopulationUpdate<S::State> = decode(data).unwrap();
            match update {
                PopulationUpdate::Kill(id, state) => {
                    to_kill.push((id, state));
                }
                PopulationUpdate::Spawn(id, state) => {
                    to_spawn.push((id, state));
                }
            }
        }
        let end = PreciseTime::now();
        println!("poppppp: sorting/decoding updates took: {}", start.to(end));
        let start = PreciseTime::now();
        self.kills(to_kill);
        self.spawns(to_spawn);
        let end = PreciseTime::now();
        println!("poppppp: running updates took: {}", start.to(end));
    }

    /// Lookup agents at a particular index.
    pub fn lookup(&self, index: &str) -> Vec<Agent<S::State>> {
        let mut ids: Vec<String> = self.conn.smembers(format!("idx:{}", index)).unwrap();
        match ids.len() {
            0 => Vec::new(),
            1 => {
                let id = ids.pop().unwrap();
                let state_data: Vec<u8> = self.conn.get(&id).unwrap();
                let agent = Agent {
                    id: id,
                    state: decode(state_data).unwrap(),
                };
                vec![agent]
            }
            _ => {
                ids.iter()
                    .map(|id| {
                        let state_data = self.conn.get(id).unwrap();
                        Agent {
                            id: id.to_string(),
                            state: decode(state_data).unwrap(),
                        }
                    })
                    .collect()
            }
        }
    }

    pub fn count_index(&self, index: &str) -> usize {
        self.conn.scard(format!("idx:{}", index)).unwrap()
    }

    /// Add an agent (id) to an index.
    pub fn index(&self, index: &str, id: String) {
        let _: () = self.conn.sadd(format!("idx:{}", index), id).unwrap();
    }

    /// Add agents (ids) to an index.
    pub fn indexes(&self, index: &str, ids: Vec<String>) {
        if ids.len() > 0 {
            let _: () = self.conn.sadd(format!("idx:{}", index), ids).unwrap();
        }
    }

    /// Remove an agent (id) from an index.
    pub fn unindex(&self, index: &str, id: String) {
        let _: () = self.conn.srem(format!("idx:{}", index), id).unwrap();
    }

    /// Remove an agent (id) from an index.
    pub fn unindexes(&self, index: &str, ids: Vec<String>) {
        if ids.len() > 0 {
            let _: () = self.conn.srem(format!("idx:{}", index), ids).unwrap();
        }
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
    reporters: HashMap<usize, Box<Fn(usize, &Population<S, C>, &Client) -> () + Send>>,
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
    pub fn run(&self, simulation: S, n_steps: usize) -> () {
        let mut steps = 0;

        if self.n_workers() == 0 {
            println!("Waiting for at least one worker...");
            while self.n_workers() == 0 {
            }
            println!("Ok");
        }

        while steps < n_steps {
            let start = PreciseTime::now();

            // run any register reporters, if appropriate
            for (interval, reporter) in self.reporters.iter() {
                if steps % interval == 0 {
                    reporter(steps, &self.population, &self.conn);
                }
            }

            // copy population to the "to_decide" set
            let _: () = self.population.conn.sunionstore(TO_DECIDE_KEY, POPULATION_KEY).unwrap();

            println!("starting step");
            let s = PreciseTime::now();
            let _: () = self.conn.publish("command", "decide").unwrap();
            let _: () = self.conn.set("current_phase", "decide").unwrap();
            self.wait_until_finished();
            let _: () = self.conn.del("finished").unwrap();
            let e = PreciseTime::now();
            println!("STEP: decide took: {}", s.to(e));

            // TODO move this to a worker?
            {
                let world = self.population.world();
                let mut queued_updates = Updates::new();
                simulation.world_decide(world, &self.population, &mut queued_updates);
                queued_updates.push(&self.population);
            }

            let s = PreciseTime::now();
            let _: () = self.conn.publish("command", "update").unwrap();
            let _: () = self.conn.set("current_phase", "update").unwrap();
            self.wait_until_finished();
            let _: () = self.conn.del("finished").unwrap();
            let e = PreciseTime::now();
            println!("STEP: update took: {}", s.to(e));

            let s = PreciseTime::now();
            self.population.update();
            let e = PreciseTime::now();
            println!("STEP: pop update took: {}", s.to(e));

            let _: () = self.conn.publish("command", "sync").unwrap();
            let _: () = self.conn.set("current_phase", "sync").unwrap();
            self.wait_until_finished();

            steps += 1;

            let end = PreciseTime::now();
            println!("step took: {}", start.to(end));
        }

        println!("done. terminating workers");
        let _: () = self.conn.publish("command", "terminate").unwrap();
    }

    /// Register a reporter function to be called every `n_steps`.
    /// It receives a `Population` which can be used to query agents,
    /// compute aggregate statistics, etc, and a Redis connection
    /// that can be used, for example, to send reports via pubsub.
    pub fn register_reporter<F>(&mut self, n_steps: usize, func: F) -> ()
        where F: Fn(usize, &Population<S, C>, &Client) -> () + Send + 'static
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

    pub fn spawn(&self, states: Vec<S::State>) -> Vec<String> {
        let mut updates = Updates::new();
        let ids = states.iter().map(|s| updates.spawn(s.clone())).collect();
        updates.push(&self.population);
        ids
    }
}

pub struct Worker<S: Simulation, C: Redis> {
    id: Uuid,
    manager: Client,
    population: Population<S, C>,
    local: Vec<Agent<S::State>>,
    simulation: S,
}

impl<S: Simulation, C: Redis> Worker<S, C> {
    pub fn new(addr: &str, conn: C, simulation: S) -> Worker<S, C> {
        Worker {
            id: Uuid::new_v4(),
            manager: Client::open(addr).unwrap(),
            population: Population::new(simulation.clone(), conn),
            simulation: simulation,
            local: Vec::new(),
        }
    }

    pub fn start(&mut self) {
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

    /// Fetch queued new agents assigned to this worker
    /// and kills those queued to die.
    fn sync_population(&mut self) {
        let key = format!("spawn:{}", self.id);
        let mut datas: Vec<Vec<u8>> = self.population
            .conn
            .lrange(&key, 0, -1)
            .unwrap();
        if datas.len() > 0 {
            let _: () = self.population.conn.del(&key).unwrap();
            let agents = datas.drain(..).map(|data| decode(data.clone()).unwrap());
            self.local.extend(agents);
        }

        let key = format!("kill:{}", self.id);
        let ids: Vec<String> = self.population
            .conn
            .lrange(&key, 0, -1)
            .unwrap();
        if ids.len() > 0 {
            let _: () = self.population.conn.del(&key).unwrap();
            // TODO maybe self.local should be a hashmap of id->States instead
            // that way we don't have to iterate over the whole damn thing to kill some agents
            let local = self.local.drain(..).filter(|a| !ids.contains(&a.id)).collect();
            self.local = local;
        }
    }

    fn process_cmd(&mut self, cmd: &str) {
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
            "sync" => {
                self.sync_population();
                let _: () = self.manager.sadd("finished", self.id.to_string()).unwrap();
            }
            "idle" => (),
            s => println!("Unrecognized command: {}", s),
        }
    }

    fn decide(&self) {
        let world = self.population.world();
        let mut cmd = Cmd::new();
        let mut to_update: Vec<String> = Vec::new();
        let mut queued_updates = Updates::new();
        cmd.arg("SPOP").arg(TO_DECIDE_KEY).arg(THROUGHPUT);
        loop {
            let start = PreciseTime::now();
            let ids = cmd.query(&self.population.conn).unwrap();
            let agents = self.population.get_agents(ids);
            let n_agents = agents.len();
            for agent in agents {
                self.simulation.decide(agent, world.clone(), &self.population, &mut queued_updates);
            }

            if n_agents > 0 {
                let end = PreciseTime::now();
                let t = start.to(end);
                println!("\t decide loop took: {}", t);
                println!("\t\tn agents: {}", n_agents);
                println!("\t\tper agent: {:.3}",
                         t.num_milliseconds() as f64 / n_agents as f64);
            }

            if n_agents < THROUGHPUT {
                break;
            }
        }

        // push out updates
        let start = PreciseTime::now();
        queued_updates.push(&self.population);
        let end = PreciseTime::now();
        println!("\t\tdecide update push took: {}", start.to(end));
    }

    fn update(&self) {
        let mut cmd = Cmd::new();
        let mut to_change: Vec<(String, S::State)> = Vec::new();
        cmd.arg("SPOP").arg(TO_UPDATE_KEY).arg(THROUGHPUT);
        let mut update_time = 0.0;
        let mut fetch_time = 0.0;
        loop {
            let start = PreciseTime::now();
            let ids = cmd.query(&self.population.conn).unwrap();
            let agents = self.population.get_agents(ids);
            let end = PreciseTime::now();
            let t = start.to(end);
            fetch_time += (t.num_milliseconds() as f64) / 1000.0;
            let n_agents = agents.len();
            // TODO here we're still making a request per agent to update
            // is there a way to bulk-fetch updates for multiple agents?
            for agent in agents {
                let updates: Vec<S::Update> = {
                    let key = format!("updates:{}", agent.id);
                    // TODO see previous note
                    let start = PreciseTime::now();
                    let updates_data: Vec<Vec<u8>> = self.population
                        .conn
                        .lrange(&key, 0, -1)
                        .unwrap();
                    let end = PreciseTime::now();
                    let t = start.to(end);
                    update_time += (t.num_milliseconds() as f64) / 1000.0;
                    if updates_data.len() == 0 {
                        continue;
                    } else {
                        let _: () = self.population.conn.del(&key).unwrap();
                        updates_data.iter().map(|data| decode(data.clone()).unwrap()).collect()
                    }
                };

                let new_state = self.simulation.update(agent.state.clone(), updates);
                if new_state != agent.state {
                    to_change.push((agent.id, new_state));
                }
            }
            if n_agents < THROUGHPUT {
                break;
            }
        }
        println!("\tspend time fetching agent datas: {:.3}", fetch_time);
        println!("\tspend time fetching updates: {:.3}", update_time);
        if to_change.len() > 0 {
            println!("changing: {}", to_change.len());
            self.population.set_agents(to_change);
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
    let sim_m = sim.clone();

    // run the manager on a separate thread
    let manager_t = thread::spawn(move || {
        manager.run(sim_m, n_steps);
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
                let mut worker = Worker::new(addr.as_ref(), pop_client, sim);
                worker.start();
            })
        })
        .collect();

    // block til done running
    for t in worker_ts {
        t.join().unwrap();
    }
}
