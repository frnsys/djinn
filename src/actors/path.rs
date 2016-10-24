use std::net::SocketAddr;

// TODO eventually use this to locate actors locally or across a network
enum ActorPath {
    Local { id: usize },
    Remote { addr: SocketAddr, id: usize }
}
