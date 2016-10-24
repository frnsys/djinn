/// Control messages a manager can send to agents.
#[derive(RustcDecodable, RustcEncodable)]
pub enum Control {
    Update,
    Terminate,
}

/// Status messages an agent can send to a manager.
#[derive(RustcDecodable, RustcEncodable)]
pub enum Status {
    Updated,
    Terminated,
}
