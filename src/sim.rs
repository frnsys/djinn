use std::fmt::Debug;
use compute::{Population, Redis, Updates};
use rustc_serialize::{Decodable, Encodable};

pub trait State: Decodable + Encodable + Debug + Send + Sync + Clone + PartialEq {}
impl<T> State for T where T: Decodable + Encodable + Debug + Send + Sync + Clone + PartialEq {}

pub trait Update
    : Decodable + Encodable + Debug + Send + Sync + Clone + PartialEq {
}
impl<T> Update for T where T: Decodable + Encodable + Debug + Send + Sync + Clone + PartialEq {}

/// Agents are just structures containing a unique id and a state.
#[derive(RustcDecodable, RustcEncodable, Debug, PartialEq, Clone)]
pub struct Agent<S: State> {
    pub id: u64,
    pub state: S,
}

/// This trait's implementation defines the main logic of a simulation.
/// A single simulation step consists of two synchronized phases:
/// 1. `decide`: this is a _read-only_ phase where agents decide on what _updates_ to apply. The
///    updates themselves are _not_ applied in this phase.
/// 2. `update`: this is a phase where agents consider queued updates and compute a new state
///    accordingly.
pub trait Simulation: Sized + Send + Sync + Clone {
    type State: State;
    type World: State;
    type Update: Update;

    /// Called whenever a new agent is spawned.
    /// You can use this to, for example, build an index of agents by state values.
    fn on_spawns<R: Redis>(&self,
                           agents: Vec<Agent<Self::State>>,
                           population: &Population<Self, R>)
                           -> () {
    }

    /// Called whenever an agent is killed.
    /// You can use this to, for example, remove an agent from an index.
    fn on_deaths<R: Redis>(&self,
                           agents: Vec<Agent<Self::State>>,
                           population: &Population<Self, R>)
                           -> () {
    }

    /// Computes updates for the specified agents and/or other agents.
    fn decide<R: Redis>(&self,
                        agent: &Agent<Self::State>,
                        world: &Self::World,
                        population: &Population<Self, R>,
                        updates: &mut Updates<Self>)
                        -> ();

    /// Compute a final updated state given a starting state and updates.
    /// If there is some update you want to do every step, things will run faster if you implement it here.
    fn update(&self, state: &mut Self::State, updates: Vec<Self::Update>) -> bool;

    /// Compute updates for the world.
    fn world_decide<R: Redis>(&self,
                              world: &Self::World,
                              population: &Population<Self, R>,
                              updates: &mut Updates<Self>)
                              -> () {
    }

    /// Compute a final state for the world given updates.
    fn world_update(&self, world: Self::World, updates: Vec<Self::Update>) -> Self::World {
        world
    }
}
