use uuid::Uuid;
use std::{thread, time};
use std::collections::{HashMap, HashSet};
use redis::{Commands, Client, Connection, PubSub};
use hash::{WHasher, hash};
use ser::{decode, encode};
use sim::{Agent, Simulation, State};
use time::PreciseTime;

pub trait Redis: Commands + Send + Sync + Clone {}
impl<T> Redis for T where T: Commands + Send + Sync + Clone {}

const POPULATION_KEY: &'static str = "population";
const POP_UPDATES_KEY: &'static str = "updates:population";
const WORLD_UPDATES_KEY: &'static str = "updates:world";

pub struct Updates<'a, S: Simulation> {
    updates: HashMap<usize, Vec<(u64, S::Update)>>,
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

    pub fn queue(&mut self, id: u64, update: S::Update) {
        let worker_id = self.hasher.hash(id);
        self.updates.entry(worker_id).or_insert(Vec::new()).push((id, update));
    }

    pub fn queue_world(&mut self, update: S::Update) {
        self.world_updates.push(update);
    }

    /// Create a new agent with the specified state, returning the new agent's id.
    /// This does not actually spawn the agent, it just queues it.
    /// Run the `update` method to execute it (and other queued updates).
    pub fn spawn(&mut self, state: S::State) -> u64 {
        let id = hash(&Uuid::new_v4().to_string());
        let update = PopulationUpdate::Spawn(id, state);
        self.pop_updates.push(update);
        id
    }

    /// Deletes an agent by id.
    /// This does not actually execute the kill, it just queues it.
    /// Run the `update` method to execute it (and other queued updates).
    pub fn kill(&mut self, agent: &Agent<S::State>) {
        let update: PopulationUpdate<S::State> = PopulationUpdate::Kill(agent.id,
                                                                        agent.state.clone());
        self.pop_updates.push(update);
    }

    /// Push these local updates to Redis.
    fn push<R: Redis>(&mut self, pop: &Population<S, R>) {
        for (worker_id, mut updates) in self.updates.drain() {
            let key = format!("updates:{}", worker_id);
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
            let _: () = pop.conn.sadd(WORLD_UPDATES_KEY, world_updates).unwrap();
        }
    }
}

#[derive(RustcDecodable, RustcEncodable, Debug, PartialEq, Clone)]
pub enum PopulationUpdate<S: State> {
    Spawn(u64, S),
    Kill(u64, S),
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
    pub fn get_agent(&self, id: u64) -> Option<Agent<S::State>> {
        let data = self.conn.get(id).unwrap();
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
    pub fn get_agents(&self, ids: Vec<u64>) -> Vec<Agent<S::State>> {
        if ids.len() == 1 {
            let id = ids[0];
            let agent = self.get_agent(id).unwrap();
            vec![agent]
        } else if ids.len() > 0 {
            let datas = self.conn.get::<Vec<u64>, Vec<Vec<u8>>>(ids.clone()).unwrap();
            ids.iter()
                .zip(datas.iter())
                .map(|(id, data)| {
                    Agent {
                        id: *id,
                        state: decode(data.clone()).unwrap(),
                    }
                })
                .collect()
        } else {
            Vec::new()
        }
    }

    /// Set an agent state by id.
    pub fn set_agent(&self, id: u64, state: S::State) {
        let data = encode(&state).unwrap();
        let _: () = self.conn.set(id, data).unwrap();
    }

    /// Set multple agent states by ids.
    /// If you need to update multiple agents, you should use this as it makes only one network
    /// request.
    pub fn set_agents(&self, mut updates: Vec<(u64, S::State)>) {
        if updates.len() == 1 {
            let (id, state) = updates.pop().unwrap();
            self.set_agent(id, state);
        } else if updates.len() > 0 {
            let encoded: Vec<(u64, Vec<u8>)> = updates.drain(..)
                .map(|(id, state)| (id, encode(&state).unwrap()))
                .collect();
            let _: () = self.conn.set_multiple(encoded.as_slice()).unwrap();
        }
    }

    fn spawns(&self, to_spawn: Vec<(u64, S::State)>) {
        if to_spawn.len() > 0 {
            let ids: Vec<u64> = to_spawn.iter().map(|&(id, _)| id).collect();
            let _: () = self.conn.sadd(POPULATION_KEY, ids.clone()).unwrap();

            // map the workers we need to send new agents to
            let hasher = self.hasher.as_ref().unwrap();
            let mut targets: HashMap<usize, Vec<Vec<u8>>> = HashMap::new();
            let agents = to_spawn.iter()
                .map(|&(id, ref state)| {
                    let a = Agent {
                        id: id,
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

    fn kills(&self, mut to_kill: Vec<(u64, S::State)>) {
        if to_kill.len() > 0 {
            let ids: Vec<u64> = to_kill.iter().map(|&(id, _)| id).collect();

            let _: () = self.conn.del(ids.clone()).unwrap();
            let _: () = self.conn.srem(POPULATION_KEY, ids.clone()).unwrap();

            let hasher = self.hasher.as_ref().unwrap();
            let mut targets: HashMap<usize, Vec<u64>> = HashMap::new();
            let agents = to_kill.drain(..)
                .map(|(id, state)| {
                    let a = Agent {
                        id: id,
                        state: state,
                    };
                    targets.entry(hasher.hash(id))
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
        let mut ids: Vec<u64> = self.conn.smembers(format!("idx:{}", index)).unwrap();
        match ids.len() {
            0 => Vec::new(),
            1 => {
                let id = ids.pop().unwrap();
                let state_data: Vec<u8> = self.conn.get(id).unwrap();
                let agent = Agent {
                    id: id,
                    state: decode(state_data).unwrap(),
                };
                vec![agent]
            }
            _ => {
                ids.drain(..)
                    .map(|id| {
                        let state_data = self.conn.get(id).unwrap();
                        Agent {
                            id: id,
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
    pub fn index(&self, index: &str, id: u64) {
        let _: () = self.conn.sadd(format!("idx:{}", index), id).unwrap();
    }

    /// Add agents (ids) to an index.
    pub fn indexes(&self, index: &str, ids: Vec<u64>) {
        if ids.len() > 0 {
            let _: () = self.conn.sadd(format!("idx:{}", index), ids).unwrap();
        }
    }

    /// Remove an agent (id) from an index.
    pub fn unindex(&self, index: &str, id: u64) {
        let _: () = self.conn.srem(format!("idx:{}", index), id).unwrap();
    }

    /// Remove an agent (id) from an index.
    pub fn unindexes(&self, index: &str, ids: Vec<u64>) {
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
    conn: Connection,
    reporters: HashMap<usize, Box<Fn(usize, &Population<S, C>, &Connection) -> () + Send>>,
    pub population: Population<S, C>,
    initial_pop: Vec<Vec<u8>>,
}

impl<S: Simulation, C: Redis> Manager<S, C> {
    pub fn new(addr: &str, conn: C, simulation: S) -> Manager<S, C> {
        let population = Population::new(simulation, conn);
        population.reset();

        // conn for commanding workers
        let client = Client::open(addr).unwrap();

        let m = Manager {
            addr: addr.to_owned(),
            population: population,
            reporters: HashMap::new(),
            conn: client.get_connection().unwrap(),
            initial_pop: Vec::new(),
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
    pub fn run(&self, simulation: S, world: S::World, n_steps: usize) -> () {
        let mut steps = 0;
        let mut n_workers = 0;
        while n_workers == 0 {
            println!("Waiting for at least one worker...");
            let wait = time::Duration::from_millis(1000);
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
        population.set_world(world);

        // push initial population
        let _: () = self.population
            .conn
            .sadd(POP_UPDATES_KEY, self.initial_pop.clone())
            .unwrap();

        // tell workers we're starting
        let _: () = self.conn.publish("command", "start").unwrap();

        while steps < n_steps {
            let start = PreciseTime::now();

            population.update();
            let _: () = self.conn.publish("command", "sync").unwrap();
            self.wait_until_finished();
            let _: () = self.conn.del("finished").unwrap();

            // run any registered reporters, if appropriate
            for (interval, reporter) in self.reporters.iter() {
                if steps % interval == 0 {
                    reporter(steps, &population, &self.conn);
                }
            }

            let _: () = self.conn.publish("command", "decide").unwrap();
            self.wait_until_finished();
            let _: () = self.conn.del("finished").unwrap();

            // TODO move this to a worker?
            let world = population.world();
            {
                let mut queued_updates = Updates::new(&hasher);
                simulation.world_decide(&world, &population, &mut queued_updates);
                queued_updates.push(&population);
            }

            let _: () = self.conn.publish("command", "update").unwrap();
            self.wait_until_finished();
            let _: () = self.conn.del("finished").unwrap();

            // update world
            // TODO move this to a worker?
            {
                let mut datas =
                    self.conn.smembers::<&str, Vec<Vec<u8>>>(WORLD_UPDATES_KEY).unwrap();
                let _: () = self.conn.del(WORLD_UPDATES_KEY).unwrap();

                let updates: Vec<S::Update> =
                    datas.drain(..).map(|data| decode(data).unwrap()).collect();
                let world = simulation.world_update(world, updates);
                population.set_world(world);
            }

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
        where F: Fn(usize, &Population<S, C>, &Connection) -> () + Send + 'static
    {
        self.reporters.insert(n_steps, Box::new(func));
    }

    fn wait_until_finished(&self) {
        while self.conn.scard::<&str, usize>("finished").unwrap() != self.n_workers() {
        }
    }

    pub fn spawn(&mut self, state: S::State) -> u64 {
        let id = hash(&Uuid::new_v4().to_string());
        let update = PopulationUpdate::Spawn(id, state);
        let data = encode(update).unwrap();
        self.initial_pop.push(data);
        id

    }

    /// Get the number of workers.
    pub fn n_workers(&self) -> usize {
        self.conn.scard::<&str, usize>("workers").unwrap()
    }
}

pub struct Worker<S: Simulation, C: Redis> {
    id: usize,
    uid: Uuid,
    manager: Connection,
    population: Population<S, C>,
    local: HashMap<u64, Agent<S::State>>,
    local_ids: HashSet<u64>,
    updates: HashMap<u64, Vec<S::Update>>,
    pubsub: PubSub,
    simulation: S,
    hasher: WHasher,
}

impl<S: Simulation, C: Redis> Worker<S, C> {
    pub fn new(addr: &str, conn: C, simulation: S) -> Worker<S, C> {
        let client = Client::open(addr).unwrap();
        Worker {
            id: 0,
            uid: Uuid::new_v4(),
            manager: client.get_connection().unwrap(),
            population: Population::new(simulation.clone(), conn),
            simulation: simulation,
            local: HashMap::new(),
            local_ids: HashSet::new(),
            updates: HashMap::new(),
            hasher: WHasher::new(0),
            pubsub: client.get_pubsub().unwrap(),
        }
    }

    pub fn start(&mut self) {
        // register with the manager
        let _: () = self.manager.sadd("workers", self.uid.to_string()).unwrap();

        // subscribe to the command channel
        self.pubsub.subscribe("command").unwrap();

        // each iteration of this loop is one simulation run
        'outer: loop {
            // reset
            self.local.clear();
            self.local_ids.clear();
            self.updates.clear();

            // wait til we get the go-ahead from the manager
            let mut started = false;
            while !started {
                let msg = self.pubsub.get_message().unwrap();
                let payload: String = msg.get_payload().unwrap();
                started = payload == "start";
            }

            // get an id
            self.id = self.manager.lpop("worker_ids").unwrap();
            let n_workers = self.manager.scard::<&str, usize>("workers").unwrap();
            self.hasher = WHasher::new(n_workers);
            self.population.hasher = Some(self.hasher.clone());

            'inner: loop {
                let msg = self.pubsub.get_message().unwrap();
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
            let _: Vec<()> = datas.drain(..)
                .map(|data| {
                    let a: Agent<S::State> = decode(data).unwrap();
                    self.local_ids.insert(a.id);
                    self.local.insert(a.id, a);
                })
                .collect();
        }

        let key = format!("kill:{}", self.id);
        let mut ids: Vec<u64> = self.population
            .conn
            .lrange(&key, 0, -1)
            .unwrap();
        if ids.len() > 0 {
            let _: () = self.population.conn.del(&key).unwrap();
            let _: Vec<()> = ids.drain(..)
                .map(|id| {
                    self.local.remove(&id);
                    self.local_ids.remove(&id);
                })
                .collect();
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
            s => println!("Unrecognized command: {}", s),
        }
    }

    fn decide(&mut self) {
        let world = self.population.world();
        let mut queued_updates = Updates::new(&self.hasher);
        for (id, agent) in self.local.iter() {
            self.simulation.decide(agent, &world, &self.population, &mut queued_updates);
        }

        // push out updates
        // first grab local updates
        match queued_updates.updates.remove(&self.id) {
            Some(mut updates) => {
                for (id, update) in updates.drain(..) {
                    self.updates.entry(id).or_insert(Vec::new()).push(update);
                }
            }
            None => (),
        };
        queued_updates.push(&self.population);
    }

    fn update(&mut self) {
        let mut to_change: Vec<(u64, S::State)> = Vec::new();

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

        for (id, agent) in self.local.iter() {
            let updates = match self.updates.remove(&agent.id) {
                Some(updates) => updates,
                None => continue,
            };
            let new_state = self.simulation.update(agent.state.clone(), updates);
            if new_state != agent.state {
                to_change.push((agent.id, new_state.clone()));
            };
        }
        if to_change.len() > 0 {
            for &(id, ref state) in to_change.iter() {
                self.local.get_mut(&id).unwrap().state = state.clone();
            }
            self.population.set_agents(to_change);
        }
    }
}

/// Convenience function for running a simulation/manager with a n workers.
/// This blocks until the simulation is finished running.
pub fn run<S: Simulation + 'static, R: Redis + 'static>(sim: S,
                                                        world: S::World,
                                                        manager: Manager<S, R>,
                                                        n_workers: usize,
                                                        n_steps: usize)
                                                        -> Manager<S, R> {

    let addr = manager.addr.clone();
    let pop_client = manager.population.conn.clone();
    let sim_m = sim.clone();

    // run the manager on a separate thread
    let manager_t = thread::spawn(move || {
        manager.run(sim_m, world, n_steps);
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
