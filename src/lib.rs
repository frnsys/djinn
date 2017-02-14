//! Djinn is a framework for writing distributed agent-based simulations (ABMs), providing:
//!
//! - a structure for defining agents and their behaviors/decision-making
//! along with some extensions for common behaviors.
//! - a distributed architecture mediated by Redis which allows computationally-intense
//! agents to be processed in parallel across multiple processes and/or multiple machines.
//! - utilities for logging and emitting events during the simulation, e.g. to a websocket
//! frontend.
//!
//! For examples refer to the `examples/` folder.

extern crate ws;
extern crate fnv;
extern crate uuid;
extern crate rand;
extern crate redis;
extern crate yaml_rust;
extern crate rmp_serialize;
extern crate rustc_serialize;

mod sim;
mod ser;
mod hash;
mod event;
mod compute;
pub mod ext;

pub use event::WebSocketServer;
pub use sim::{Agent, Update, State, Simulation};
pub use compute::{Population, Manager, Updates, Worker, Redis, run, run_workers};
