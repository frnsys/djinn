use super::router::Router;
use super::actor::{Actor, ActorVecRef};
use super::dispatch::dispatcher;

pub fn start_node<A>(population: ActorVecRef<A>, n_threads: usize)
    where A: Actor + 'static
{
    let router = Router::<A::M>::new();
    dispatcher(population, n_threads);
}
