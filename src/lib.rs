extern crate ws;
extern crate uuid;
extern crate redis;
extern crate futures;
extern crate futures_cpupool;
extern crate rustc_serialize;
extern crate rmp_serialize;

mod sim;
mod event;
mod compute;

pub use uuid::Uuid;
pub use event::WebSocketServer;
pub use sim::{Agent, Update, State, Simulation};
pub use compute::{Population, Manager, Worker};
