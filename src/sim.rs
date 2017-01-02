use uuid::Uuid;
use std::fmt::Debug;
use redis::Commands;
use compute::Population;
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
    pub id: Uuid,
    pub state: S,
}

/// This trait's implementation defines the main logic of a simulation.
/// A single simulation step consists of two synchronized phases:
/// 1. `decide`: this is a _read-only_ phase where agents decide on what _updates_ to apply. The
///    updates themselves are _not_ applied in this phase.
/// 2. `update`: this is a phase where agents consider queued updates and compute a new state
///    accordingly.
pub trait Simulation: Sized + Clone {
    type State: State;
    type World: State;
    type Update: Update;

    /// Called whenever a new agent is spawned.
    /// You can use this to, for example, build an index of agents by state values.
    fn setup<C: Commands>(&self,
                          agent: Agent<Self::State>,
                          population: &Population<Self, C>)
                          -> ();

    /// Computes updates for the specified agents and/or other agents.
    fn decide<C: Commands>(&self,
                           agent: Agent<Self::State>,
                           world: Self::World,
                           population: &Population<Self, C>)
                           -> Vec<(Uuid, Self::Update)>;

    /// Compute a final updated state given a starting state and updates.
    fn update(&self, state: Self::State, updates: Vec<Self::Update>) -> Self::State;
}
