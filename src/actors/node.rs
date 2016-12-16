use super::router::Router;
use super::actor::{Actor, ActorVecRef};
use super::dispatch::dispatcher;

pub fn start_node<A>(addr: String, population: ActorVecRef<A>, n_threads: usize)
    where A: Actor + 'static
{
    // TODO register with leader node
    let router = Router::<A>::new(addr, population.clone());
    dispatcher(population, n_threads);
}
