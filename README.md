# djinn

[![documentation](https://docs.rs/djinn/badge.svg)](https://docs.rs/djinn/)

[![Join the chat at https://gitter.im/MaxwellRebo/djinn](https://badges.gitter.im/MaxwellRebo/djinn.svg)](https://gitter.im/MaxwellRebo/djinn?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

A framework for distributed agent-based modeling.

## Examples

There are a few usage examples available in the `examples/` folder:

- `basic.rs`: a very simple simulation demonstrating many core features:
    - how to write a basic simulation
    - how to create a websocket server and publish to it
        - refer to `examples/ws.html` for an example of a frontend listening to the websocket server
    - how to publish and listen to events from within the simulation
    - how to register a reporter, which executes every `n` steps
    - how to run a simulation across multiple threads
- `multi.rs`: demonstrates how to handle multiple agent types
- `population_dynamics.rs`: a simple (discrete) spatial simulation
    - depending on the parameters, this can also test simulation speed under large populations
- `opinion_dynamics.rs`: a more substantial simulation with multiple agent types
- `qlearning.rs`: demonstrates how to implement a Q-learning (reinforcement learning) agent

To run an example, e.g. the `basic.rs` example:

    cargo run --example basic

## Dependencies

- `redis >= 3.2`

## A note on performance

Right now most of the overhead is from network communication with Redis. The `redis-rs` library does not have async IO yet (Rust is still working on it, see <https://github.com/mitsuhiko/redis-rs/pull/93>), but it probably will eventually, which should speed things up.

To squeeze the most speed out of a simulation, don't forget to compile it with the `--release` flag!

## Getting started

### Core concepts

#### Components

There are four components to a Djinn simulation:

- `State`: a struct or enum that represents an agent's state.
- `World`: a struct or enum that represents the world's state.
    - the world is a globally shared and mutable state
    - you can think of the world as a singleton agent
- `Update`: a struct or enum that represents updates for agents.
    - these essentially operate as messages that tell agents how to mutate their state
- `Simulation`: the `Simulation` trait is where the logic of the simulation is implemented

#### Decide/Update architecture

When designing a Djinn simulation it's important to understand how they are run.

Djinn simulations are distributed, which introduces the problem of race conditions. For example, an agent `A` may query another agent `B`'s state, but that information may be stale if `B` mutates its state in the meanwhile.

To avoid this problem, Djinn simulation steps operate in two phases:

1. a `decide` phase, where all agents compute changes to make to themselves or to other agents.
    - that is, this is a _read-only_ phase. State changes made here _do not_ persist and are invisible to other agents.
    - these updates are communicated by queueing `Update`s for this or other agents
2. an `update` phase, where agents read their queued `Update`s and accordingly update their state.

#### Population

The agent population of a simulation is spread out across multiple threads or machines.

Each worker thread manages its own population of agents and periodically synchronizes those agent states to a Redis instance.

When an agent needs to query the state of another agent, it first looks locally, and if necessary queries Redis.

Djinn passes around a `Population` struct which provides an interface to this distributed population. Refer to [the docs](https://docs.rs/djinn/) for more details.

### A very simple simulation

#### Design & implementation

Let's create a really simple simulation that doesn't do a whole lot, but will illustrate these core concepts. This is based off of the `examples/basic.rs` example, so refer to that if you get stuck.

First let's bring in some dependencies:

```rust
extern crate djinn;
extern crate rustc_serialize;

use djinn::{Agent, Simulation, Population, Updates, Redis};
```

Now we'll define the `State`, `World`, and `Update` for the simulation:

```rust
#[derive(RustcDecodable, RustcEncodable, Debug, PartialEq, Clone)]
pub struct State {
    health: usize,
}

#[derive(RustcDecodable, RustcEncodable, Debug, PartialEq, Clone)]
pub struct World {
    weather: String,
}

#[derive(RustcDecodable, RustcEncodable, Debug, PartialEq, Clone)]
pub enum Update {
    ChangeHealth(usize),
}
```

Each of these need to be serializable, so they can be transmitted over the network, and require some other traits, so we use the `derive` macro to handle that for us.

Our agents here will have some `health` value, the world keeps track of the `weather` shared by all agents, and our agents can receive updates on how to modify their `health`.

Then we need to implement the `Simulation` itself.

```rust
#[derive(Clone)]
pub struct BasicSim;

impl Simulation for BasicSim {
    // associate our types
    type State = State;
    type Update = Update;
    type World = World;

    // ... rest of the implementation goes here,
    // see below

}
```

The first step is to associate our `State`, `World`, and `Update` types.

The two methods we have to implement are `decide` and `update`, which correspond to the `decide` and `update` phases mentioned above.

The `decide` method accepts the following arguments:

- `agent`: the agent that is deciding
- `world`: the current world state
- `population`: the interface to the simulation population
- `updates`: a container where `Update`s are queued

We'll do an extremely simple implementation, where agents' health just constantly increase by 10:

```rust
// ...

    fn decide<R: Redis>(&self,
                        agent: &Agent<Self::State>,
                        world: &Self::World,
                        population: &Population<Self, R>,
                        updates: &mut Updates<Self>)
                        -> () {
        updates.queue(agent.id, Update::ChangeHealth(10));
    }

// ...
```

The `updates` struct's most important method is `queue`, as used above. It takes an agent id and an `Update` to send to that agent.

Then we implement the `update` method. This takes a `State` and a list of `Update`s. It returns a `bool` of whether or not any updates were made. This is just so the worker knows whether or not it needs to synchronize the agent's state to Redis, or if it can just ignore it.

Here we just take the updates and apply the specified change in health:

```rust
// ...

    fn update(&self, mut state: &mut Self::State, updates: Vec<Self::Update>) -> bool {
        let old_health = state.health;
        for update in updates {
            match update {
                Update::ChangeHealth(health) => {
                    state.health += health;
                }
            }
        }
        state.health != old_health
    }

// ...
```

Then we just return whether or not the agent's state has changed.

That's about it for our simulation's design and implementation. Now we can try running it.

#### Running the simulation

Because simulations are Redis-backed, we need to create a Redis client to use.

First we need to bring in the `redis` dependency:

```rust
extern crate redis;
use redis::{Client, Commands};
```

Then in our `main` function we can create our simulation and world:

```rust
fn main() {
    let sim = BasicSim {};
    let world = World { weather: "sunny".to_string() };

    // ... more to follow
}
```

We'll also create a Redis client:

```rust
fn main() {
    // ...
    let addr = "redis://127.0.0.1/";
    let client = Client::open(addr).unwrap();
    // ...
}
```

Djinn simulations are run by a `Manager`, which coordinates a bunch of `Worker`s. Most of the time you will not need to deal with `Worker`s directly, just the `Manager`.

Let's create a `Manager`. It takes an Redis address, a Redis client, and the simulation.

```rust
use djinn::Manager;

fn main() {
    // ...
    let mut manager = Manager::new(addr, client, sim.clone());
    // ...
}
```

The `Manager` provides an interface for spawning an initial population. We can spawn agents one-by-one by passing a `State` into `manager.spawn`:

```rust
fn main() {
    // ...
    manager.spawn(State { health: 0 });
    // ...
}
```

...or by passing multiple `State`s into `manager.spawns`:

```rust
fn main() {
    // ...
    manager.spawns(vec![
        State { health: 0 },
        State { health: 0 },
        State { health: 0 }
    ]);
    // ...
}
```

Each of these methods returns the id or ids of the agent(s) spawned so you can look them up later.

Now we want to run the simulation. Djinn provides a `run` function that easily runs a simulation locally across multiple threads:

```rust
use djinn::run;

fn main() {
    // ...
    n_steps = 10;
    n_workers = 4;
    manager = run(sim, world, manager, n_workers, n_steps);
}
```

And there you have a basic simulation with Djinn.

### Advanced features

For more complex examples, refer to the `examples/` folder. These demonstrate other features like:

- websocket servers (`basic.rs`)
- publishing events from within the simulation to listeners outside of it (`basic.rs`)
- reporters which are run every `n` steps (`basic.rs`)
- handling multiple agent types (`multi.rs`)
- implementing behavior extensions, e.g. Q-learning agents (`qlearning.rs`)
- (discrete) spatial simulation (`population_dynamics.rs`)
