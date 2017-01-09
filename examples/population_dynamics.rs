//! Population Dynamics model
//! Refer to: <https://www.openabm.org/book/33102/6-population-dynamics>
//!
//! Demonstrates:
//! - toroidal space
//! - agents that are born and die
//!
//! General model:
//! - space
//!     - a toroidal grid
//!     - each cell has a resource which provides R energy to an agent
//!     - the resource is replenished with probability p_r
//! - agents
//!     - have a metabolism where they require m energy per step
//!     - if they don't have enough energy, they die
//!     - if their energy surpasses b_t, they split into two agents (birth)
//!     - they move randomly through the space


extern crate rand;
extern crate djinn;
extern crate redis;
extern crate redis_cluster;
extern crate rustc_serialize;

use rand::Rng;
use std::cmp;
use redis::Client;
use std::collections::{HashMap, HashSet};
use djinn::{Agent, Manager, Simulation, Population, Redis, Updates, run};

#[derive(RustcDecodable, RustcEncodable, Debug, Eq, PartialEq, Hash, Clone, Copy)]
pub struct Pos {
    x: usize,
    y: usize,
}

#[derive(RustcDecodable, RustcEncodable, Debug, PartialEq, Clone)]
pub struct Cell {
    pos: Pos,
    resources: usize,
    occupants: HashSet<u64>,
}

#[derive(RustcDecodable, RustcEncodable, Debug, PartialEq, Clone)]
pub struct State {
    resources: usize,
    pos: Pos,
}

#[derive(RustcDecodable, RustcEncodable, Debug, PartialEq, Clone)]
pub struct World {
    cells: HashMap<Pos, Cell>,
}

impl World {
    pub fn new(width: usize, height: usize, resource_per_cell: usize) -> World {
        let mut cells = HashMap::new();
        for x in 0..width {
            for y in 0..height {
                let pos = Pos { x: x, y: y };
                cells.insert(pos,
                             Cell {
                                 pos: pos,
                                 resources: resource_per_cell,
                                 occupants: HashSet::new(),
                             });
            }
        }
        World { cells: cells }
    }
}

#[derive(RustcDecodable, RustcEncodable, Debug, PartialEq, Clone)]
pub enum Update {
    GiveResource(usize),
    Replenish(Vec<Pos>),
    Drain(Vec<Pos>),
    NewOccupant(u64, Pos),
    MoveOccupant(u64, Pos, Pos),
    MoveTo(Pos),
}

#[derive(Clone)]
pub struct Sim {
    p_replenishment: f64,
    birth_threshold: usize,
    resource_per_cell: usize,
    resource_to_live: usize,
    start_resources: usize,
    width: usize,
    height: usize,
}

impl Simulation for Sim {
    type State = State;
    type Update = Update;
    type World = World;

    fn on_spawns<R: Redis>(&self,
                           agents: Vec<Agent<Self::State>>,
                           population: &Population<Self, R>)
                           -> () {
        let people = agents.iter().map(|agent| agent.id).collect();
        let _: () = population.indexes("people", people);
    }

    fn on_deaths<R: Redis>(&self,
                           agents: Vec<Agent<Self::State>>,
                           population: &Population<Self, R>)
                           -> () {
        let people = agents.iter().map(|agent| agent.id).collect();
        let _: () = population.unindexes("people", people);
    }

    fn decide<R: Redis>(&self,
                        agent: &Agent<Self::State>,
                        world: &Self::World,
                        pop: &Population<Self, R>,
                        updates: &mut Updates<Self>)
                        -> () {
        let s = agent.state.clone();
        if s.resources <= 0 {
            // died
            updates.kill(agent);
        } else {
            if s.resources >= self.birth_threshold {
                // birthed
                let id = updates.spawn(State {
                    resources: self.start_resources,
                    pos: s.pos,
                });
                updates.queue_world(Update::NewOccupant(id, s.pos));
            }

            let c = world.cells.get(&s.pos).unwrap();
            // move
            if c.resources <= 0 {
                // random adjacent cell
                let mut rng = rand::weak_rng();

                let x: isize = if rng.gen::<f64>() < 0.5 {
                    cmp::min(s.pos.x + 1, self.width - 1) as isize
                } else {
                    cmp::max((s.pos.x as isize) - 1, 0)
                };

                let y: isize = if rng.gen::<f64>() < 0.5 {
                    cmp::min(s.pos.y + 1, self.height - 1) as isize
                } else {
                    cmp::max((s.pos.y as isize) - 1, 0)
                };

                let pos = Pos {
                    x: (x as usize),
                    y: (y as usize),
                };
                updates.queue(agent.id, Update::MoveTo(pos));
                updates.queue_world(Update::MoveOccupant(agent.id, s.pos, pos));
            }
        }
    }

    fn update(&self, mut state: &mut Self::State, updates: Vec<Self::Update>) -> bool {
        let old_resources = state.resources;
        let mut changed = false;
        for update in updates {
            match update {
                // assuming each agent makes only one move per step
                Update::MoveTo(pos) => {
                    state.pos = pos;
                    changed = true;
                }
                Update::GiveResource(amt) => {
                    state.resources += amt;
                    changed = true;
                }
                _ => (),
            }
        }
        // metabolize
        if state.resources <= self.resource_to_live {
            state.resources = 0;
        } else {
            state.resources -= self.resource_to_live;
        }
        changed || state.resources != old_resources
    }

    fn world_decide<R: Redis>(&self,
                              world: &Self::World,
                              population: &Population<Self, R>,
                              updates: &mut Updates<Self>)
                              -> () {
        let mut to_drain = Vec::new();
        let mut to_replenish = Vec::new();
        let mut rng = rand::weak_rng();
        let _: Vec<()> = world.cells
            .iter()
            .map(|(pos, c)| {
                if c.resources > 0 {
                    let ids = rand::sample(&mut rng, c.occupants.clone(), 1);
                    if ids.len() > 0 {
                        updates.queue(ids[0], Update::GiveResource(c.resources));
                        to_drain.push(*pos);
                    }
                } else if rng.gen::<f64>() <= self.p_replenishment {
                    to_replenish.push(*pos);
                }
            })
            .collect();
        updates.queue_world(Update::Replenish(to_replenish));
        updates.queue_world(Update::Drain(to_drain));

    }

    fn world_update(&self, mut world: Self::World, updates: Vec<Self::Update>) -> Self::World {
        for update in updates {
            match update {
                // assuming each agent makes only one move per step
                Update::Replenish(to_replenish) => {
                    for pos in to_replenish {
                        let c = world.cells.get_mut(&pos).unwrap();
                        c.resources = self.resource_per_cell;
                    }
                }
                Update::Drain(to_drain) => {
                    for pos in to_drain {
                        let c = world.cells.get_mut(&pos).unwrap();
                        c.resources = 0;
                    }
                }
                Update::NewOccupant(id, pos) => {
                    world.cells.get_mut(&pos).unwrap().occupants.insert(id);
                }
                Update::MoveOccupant(id, pos_a, pos_b) => {
                    world.cells.get_mut(&pos_a).unwrap().occupants.remove(&id);
                    world.cells.get_mut(&pos_b).unwrap().occupants.insert(id);
                }
                _ => (),
            }
        }
        world
    }
}


fn main() {
    let sim = Sim {
        // FOR TESTING
        // p_replenishment: 1.,
        // birth_threshold: 0,
        // resource_per_cell: 1,
        // resource_to_live: 0,
        // start_resources: 1,
        p_replenishment: 0.8,
        birth_threshold: 10,
        resource_per_cell: 6,
        resource_to_live: 6,
        start_resources: 10,
        height: 20,
        width: 20,
    };

    let addr = "redis://127.0.0.1/";
    let mut world = World::new(sim.width, sim.height, sim.resource_per_cell);
    let pop_client = Client::open(addr).unwrap();
    let positions: Vec<Pos> = world.cells.keys().cloned().collect();
    let mut manager = Manager::new(addr, pop_client, sim.clone());

    println!("setting up");
    let start_pop_size = 10000;
    let mut rng = rand::thread_rng();
    for _ in 0..start_pop_size {
        let poss = rand::sample(&mut rng, positions.clone(), 1);
        let pos = poss[0];
        let id = manager.spawn(State {
            resources: sim.start_resources,
            pos: pos,
        });
        world.cells.get_mut(&pos).unwrap().occupants.insert(id);
    }

    // Register a really simple reporter
    manager.register_reporter(1, |step, pop, conn| {
        let popsize: usize = pop.count_index("people");
        println!("[{:02}] population: {}", step, popsize);
    });

    println!("running");
    run(sim, world, manager, 4, 10);
}
