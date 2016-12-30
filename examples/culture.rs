extern crate djinn;
extern crate time;
extern crate rand;
extern crate rustc_serialize;

use std::thread;
use time::PreciseTime;
use rand::{thread_rng, Rng, sample};
use djinn::{Agent, Manager, Simulation, Population, Worker, Uuid};

#[derive(RustcDecodable, RustcEncodable, Debug, PartialEq, Clone)]
pub struct State {
    altruism: f64,
    frugality: f64,
}

#[derive(RustcDecodable, RustcEncodable, Debug, PartialEq, Clone)]
pub struct World {}

#[derive(RustcDecodable, RustcEncodable, Debug, PartialEq, Clone)]
pub enum Update {
    Imitate(State),
}

#[derive(Clone)]
pub struct CultureSimulation;

impl Simulation for CultureSimulation {
    type State = State;
    type Update = Update;
    type World = World;

    fn setup(&self, agent: Agent<Self::State>, population: &Population<Self>) -> () {}

    fn decide(&self,
              agent: Agent<Self::State>,
              world: Self::World,
              population: &Population<Self>)
              -> Vec<(Uuid, Self::Update)> {
        let mut updates = Vec::new();

        let friends = population.lookup(agent.id.to_string().as_ref());

        for friend in friends {
            updates.push((agent.id.clone(), Update::Imitate(friend.state.clone())));
        }
        updates
    }
    fn update(&self, state: State, updates: Vec<Update>) -> Self::State {
        let mut state = state.clone();
        for update in updates {
            state = match update {
                Update::Imitate(s) => {
                    let diff_altruism = s.altruism - state.altruism;
                    let diff_frugality = s.frugality - state.frugality;
                    State {
                        altruism: s.altruism + diff_altruism * 0.01,
                        frugality: s.frugality + diff_frugality * 0.01,
                    }
                }
            }
        }
        state
    }
}

fn main() {
    let start = PreciseTime::now();

    let state = State {
        altruism: 0.5,
        frugality: 0.5,
    };
    let state2 = State {
        altruism: 1.,
        frugality: 1.,
    };

    let world = World {};
    let addr = "redis://127.0.0.1/";
    let sim = CultureSimulation {};
    let mut manager = Manager::<CultureSimulation>::new(addr, sim, world);

    let mut ids = Vec::new();
    let mut rng = thread_rng();
    {
        for _ in 0..200 {
            let roll: f64 = rng.gen();
            let s = if roll <= 0.5 {
                state.clone()
            } else {
                state2.clone()
            };
            let id = manager.population.spawn(s);
            ids.push(id);
        }
    }

    {
        // assign friends
        let n_friends = 10;
        for id in ids.clone() {
            let friends = sample(&mut rng, ids.clone(), n_friends);
            for fid in friends {
                manager.population.index(id.to_string().as_ref(), fid);
            }
        }
    }

    let end = PreciseTime::now();
    println!("setup took: {}", start.to(end));


    let start = PreciseTime::now();
    let worker_t = thread::spawn(move || {
        let sim = CultureSimulation {};
        let worker = Worker::new(addr, sim);
        worker.start();
    });

    let n_steps = 10;
    let manager_t = thread::spawn(move || {
        manager.start(n_steps);
        manager
    });

    manager = manager_t.join().unwrap();
    worker_t.join().unwrap();

    let end = PreciseTime::now();
    println!("run took: {}", start.to(end));

    let id = ids[0];
    let agent = match manager.population.get(id) {
        Some(a) => a,
        None => panic!("Couldn't find the agent"),
    };
    println!("{:?}", agent);
}
