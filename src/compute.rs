use std::thread;
use std::collections::{HashMap, HashSet};
use hash::WHasher;
use ser::{decode, encode};
use sim::{Agent, Simulation, State};
use redis::{Commands, Client};
use uuid::Uuid;

use std::time::Duration;
use time::PreciseTime;

pub trait Redis: Commands + Send + Sync + Clone {}
impl<T> Redis for T where T: Commands + Send + Sync + Clone {}

const POPULATION_KEY: &'static str = "population";
const POP_UPDATES_KEY: &'static str = "updates:population";

pub struct Updates<'a, S: Simulation> {
    updates: HashMap<usize, Vec<(String, S::Update)>>,
    world_updates: Vec<S::Update>,
    pop_updates: Vec<PopulationUpdate<S::State>>,
    hasher: &'a WHasher,
}

impl<'a, S: Simulation> Updates<'a, S> {
    pub fn new(hasher: &WHasher) -> Updates<S> {
        Updates {
            updates: HashMap::new(),
            world_updates: Vec::new(),
            pop_updates: Vec::new(),
            hasher: hasher,
        }
    }

    pub fn queue(&mut self, id: &String, update: S::Update) {
        let worker_id = self.hasher.hash(id);
        self.updates.entry(worker_id).or_insert(Vec::new()).push((id.clone(), update));
    }

    pub fn queue_world(&mut self, update: S::Update) {
        self.world_updates.push(update);
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
    pub fn kill(&mut self, agent: &Agent<S::State>) {
        let update: PopulationUpdate<S::State> = PopulationUpdate::Kill(agent.id.clone(),
                                                                        agent.state.clone());
        self.pop_updates.push(update);
    }

    /// Push these local updates to Redis.
    fn push<R: Redis>(&mut self, pop: &Population<S, R>) {
        for (worker_id, mut updates) in self.updates.drain() {
            let key = format!("update:{}", worker_id);
            let encoded: Vec<Vec<u8>> = updates.drain(..).map(|u| encode(&u).unwrap()).collect();
            let _: () = pop.conn
                .lpush(key, encoded)
                .unwrap();
        }
        if self.pop_updates.len() > 0 {
            let pop_updates: Vec<Vec<u8>> =
                self.pop_updates.drain(..).map(|u| encode(u).unwrap()).collect();
            let _: () = pop.conn.sadd(POP_UPDATES_KEY, pop_updates).unwrap();
        }
        if self.world_updates.len() > 0 {
            let world_updates: Vec<Vec<u8>> =
                self.world_updates.drain(..).map(|u| encode(u).unwrap()).collect();
            let _: () = pop.conn.sadd("updates:world", world_updates).unwrap(); // TODO what is on the receiving end of this?
        }
    }
}

#[derive(RustcDecodable, RustcEncodable, Debug, PartialEq, Clone)]
pub enum PopulationUpdate<S: State> {
    Spawn(String, S),
    Kill(String, S),
}

/// An interface to the Redis-backed agent population.
#[derive(Clone)]
pub struct Population<S: Simulation, C: Redis> {
    pub conn: C,
    simulation: S,
    hasher: Option<WHasher>,
}

impl<S: Simulation, C: Redis> Population<S, C> {
    pub fn new(simulation: S, conn: C) -> Population<S, C> {
        Population {
            conn: conn,
            simulation: simulation,
            hasher: None,
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

    fn spawns(&self, to_spawn: Vec<(String, S::State)>) {
        if to_spawn.len() > 0 {
            let ids: Vec<String> = to_spawn.iter().map(|&(ref id, _)| id.clone()).collect();
            let _: () = self.conn.sadd(POPULATION_KEY, ids.clone()).unwrap();

            // map the workers we need to send new agents to
            let hasher = self.hasher.as_ref().unwrap();
            let mut targets: HashMap<usize, Vec<Vec<u8>>> = HashMap::new();
            let agents = to_spawn.iter()
                .map(|&(ref id, ref state)| {
                    let a = Agent {
                        id: id.clone(),
                        state: state.clone(),
                    };
                    targets.entry(hasher.hash(id))
                        .or_insert(Vec::new())
                        .push(encode(&a).unwrap());
                    a
                })
                .collect();
            self.set_agents(to_spawn);

            for (worker_id, agents) in targets.drain() {
                let key = format!("spawn:{}", worker_id);
                let _: () = self.conn.lpush(key, agents).unwrap();
            }
            self.simulation.on_spawns(agents, &self);
        }
    }

    fn kills(&self, mut to_kill: Vec<(String, S::State)>) {
        if to_kill.len() > 0 {
            let ids: Vec<String> = to_kill.iter().map(|&(ref id, _)| id.clone()).collect();

            // TODO pipeline this? or use set operations?
            let _: () = self.conn.del(ids.clone()).unwrap();
            let _: () = self.conn.srem(POPULATION_KEY, ids.clone()).unwrap();

            let hasher = self.hasher.as_ref().unwrap();
            let mut targets: HashMap<usize, Vec<String>> = HashMap::new();
            let agents = to_kill.drain(..)
                .map(|(id, state)| {
                    let a = Agent {
                        id: id.clone(),
                        state: state,
                    };
                    targets.entry(hasher.hash(&id))
                        .or_insert(Vec::new())
                        .push(id);
                    a
                })
                .collect();
            for (worker_id, ids) in targets.drain() {
                let key = format!("kill:{}", worker_id);
                let _: () = self.conn.lpush(key, ids).unwrap();
            }

            self.simulation.on_deaths(agents, &self);
        }
    }

    /// Process queued updates.
    pub fn update(&self) {
        let mut to_kill = Vec::new();
        let mut to_spawn = Vec::new();

        let updates = self.conn.smembers::<&str, Vec<Vec<u8>>>(POP_UPDATES_KEY).unwrap();
        let _: () = self.conn.del(POP_UPDATES_KEY).unwrap();

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

        self.kills(to_kill);
        self.spawns(to_spawn);
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
        // reset sets
        let _: () = self.conn.del("workers").unwrap();
        let _: () = self.conn.del("finished").unwrap();
    }

    /// Run the simulation for `n_steps`.
    pub fn run(&self, simulation: S, states: Vec<S::State>, n_steps: usize) -> () {
        let mut steps = 0;
        let mut n_workers = 0;
        while n_workers == 0 {
            println!("Waiting for at least one worker...");
            let wait = Duration::from_millis(1000);
            thread::sleep(wait);
            n_workers = self.n_workers();
        }
        println!("Ok, found {} workers.", n_workers);

        // queue ids for workers to claim
        let ids: Vec<usize> = (0..n_workers).collect();
        let _: () = self.conn.del("worker_ids").unwrap();
        let _: () = self.conn.lpush("worker_ids", ids).unwrap();
        let hasher = WHasher::new(n_workers);
        let mut population = self.population.clone();
        population.hasher = Some(hasher.clone());

        // spawn initial population
        {
            let mut updates = Updates::new(&hasher);
            let _: Vec<String> = states.iter().map(|s| updates.spawn(s.clone())).collect();
            updates.push(&population);
        }

        // tell workers we're starting
        let _: () = self.conn.publish("command", "start").unwrap();

        while steps < n_steps {
            let start = PreciseTime::now();

            // pre-step
            let s = PreciseTime::now();
            population.update();
            let e = PreciseTime::now();
            println!("MANAGER: pop update took: {}", s.to(e));

            let s = PreciseTime::now();
            population.update();
            let _: () = self.conn.publish("command", "sync").unwrap();
            self.wait_until_finished();
            let _: () = self.conn.del("finished").unwrap();
            let e = PreciseTime::now();
            println!("MANAGER: sync took: {}", s.to(e));

            // run any registered reporters, if appropriate
            println!("starting step");
            for (interval, reporter) in self.reporters.iter() {
                if steps % interval == 0 {
                    reporter(steps, &population, &self.conn);
                }
            }

            let s = PreciseTime::now();
            let _: () = self.conn.publish("command", "decide").unwrap();
            self.wait_until_finished();
            let _: () = self.conn.del("finished").unwrap();
            let e = PreciseTime::now();
            println!("MANAGER: decide took: {}", s.to(e));

            // TODO move this to a worker?
            {
                let world = population.world();
                let mut queued_updates = Updates::new(&hasher);
                simulation.world_decide(&world, &population, &mut queued_updates);
                queued_updates.push(&population);
            }

            let s = PreciseTime::now();
            let _: () = self.conn.publish("command", "update").unwrap();
            self.wait_until_finished();
            let _: () = self.conn.del("finished").unwrap();
            let e = PreciseTime::now();
            println!("MANAGER: update took: {}", s.to(e));
            steps += 1;

            let end = PreciseTime::now();
            println!("MANAGER: STEP TOOK: {}", start.to(end));
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
}

pub struct Worker<S: Simulation, C: Redis> {
    id: usize,
    uid: Uuid,
    manager: Client,
    population: Population<S, C>,
    local: Vec<Agent<S::State>>,
    local_ids: HashSet<String>,
    updates: HashMap<String, Vec<S::Update>>,
    simulation: S,
    hasher: WHasher,
}

impl<S: Simulation, C: Redis> Worker<S, C> {
    pub fn new(addr: &str, conn: C, simulation: S) -> Worker<S, C> {
        Worker {
            id: 0,
            uid: Uuid::new_v4(),
            manager: Client::open(addr).unwrap(),
            population: Population::new(simulation.clone(), conn),
            simulation: simulation,
            local: Vec::new(),
            local_ids: HashSet::new(),
            updates: HashMap::new(),
            hasher: WHasher::new(0),
        }
    }

    pub fn start(&mut self) {
        // register with the manager
        let _: () = self.manager.sadd("workers", self.uid.to_string()).unwrap();

        // subscribe to the command channel
        let mut pubsub = self.manager.get_pubsub().unwrap();
        pubsub.subscribe("command").unwrap();

        // each iteration of this loop is one simulation run
        'outer: loop {
            // reset
            self.local.clear();
            self.local_ids.clear();
            self.updates.clear();

            // wait til we get the go-ahead from the manager
            let mut started = false;
            while !started {
                let msg = pubsub.get_message().unwrap();
                let payload: String = msg.get_payload().unwrap();
                started = payload == "start";
            }

            // get an id
            self.id = self.manager.lpop("worker_ids").unwrap();
            let n_workers = self.manager.scard::<&str, usize>("workers").unwrap();
            self.hasher = WHasher::new(n_workers);
            self.population.hasher = Some(self.hasher.clone());

            'inner: loop {
                let msg = pubsub.get_message().unwrap();
                let payload: String = msg.get_payload().unwrap();
                self.process_cmd(payload.as_ref());
                if payload == "terminate" {
                    break 'outer; // TODO eventually we will want to just break this inner loop, i.e. end one run of the simulation but keep the worker up for more
                }
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
            let agents: Vec<Agent<S::State>> = datas.drain(..)
                .map(|data| {
                    let a: Agent<S::State> = decode(data).unwrap();
                    self.local_ids.insert(a.id.clone());
                    a
                })
                .collect();
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
            // let local = self.local
            //     .drain(..)
            //     .filter(|a| {
            //         let keep = !ids.contains(&a.id);
            //         if !keep {
            //             self.local_ids.remove(&a.id);
            //         }
            //         keep
            //     })
            //     .collect();
            // self.local = local;
            // TODO remove from self.local_ids
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

    fn decide(&mut self) {
        let world = self.population.world();
        let mut queued_updates = Updates::new(&self.hasher);

        let s = PreciseTime::now();
        for agent in self.local.iter() {
            self.simulation.decide(agent, // TODO this should prob just be a ref
                                   &world,
                                   &self.population,
                                   &mut queued_updates);
        }
        let e = PreciseTime::now();
        println!("\tWORKER: decide loop took: {}", s.to(e));

        // push out updates
        // first grab local updates
        let s = PreciseTime::now();
        match queued_updates.updates.remove(&self.id) {
            Some(mut updates) => {
                for (id, update) in updates.drain(..) {
                    self.updates.entry(id).or_insert(Vec::new()).push(update);
                }
            }
            None => (),
        };
        queued_updates.push(&self.population);
        let e = PreciseTime::now();
        println!("\tWORKER: decide update push took: {}", s.to(e));
    }

    fn update(&mut self) {
        let mut to_change: Vec<(String, S::State)> = Vec::new();

        // get updates queued by other workers
        let key = format!("updates:{}", self.id);
        let mut remote_updates: Vec<Vec<u8>> = self.population
            .conn
            .lrange(&key, 0, -1)
            .unwrap();
        let _: () = self.population.conn.del(&key).unwrap();

        for data in remote_updates.drain(..) {
            let (id, update) = decode(data).unwrap();
            self.updates.entry(id).or_insert(Vec::new()).push(update);
        }

        for agent in self.local.iter() {
            let updates = match self.updates.remove(&agent.id) {
                Some(updates) => updates,
                None => continue,
            };
            let new_state = self.simulation.update(agent.state.clone(), updates);
            if new_state != agent.state {
                to_change.push((agent.id.clone(), new_state));
            };
        }
        if to_change.len() > 0 {
            self.population.set_agents(to_change);
        }
    }
}

/// Convenience function for running a simulation/manager with a n workers.
/// This blocks until the simulation is finished running.
pub fn run<S: Simulation + 'static, R: Redis + 'static>(sim: S,
                                                        manager: Manager<S, R>,
                                                        states: Vec<S::State>,
                                                        n_workers: usize,
                                                        n_steps: usize)
                                                        -> Manager<S, R> {

    let addr = manager.addr.clone();
    let pop_client = manager.population.conn.clone();
    let sim_m = sim.clone();

    // run the manager on a separate thread
    let manager_t = thread::spawn(move || {
        manager.run(sim_m, states, n_steps);
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
