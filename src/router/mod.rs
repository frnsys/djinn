mod path;
mod msgpack;
mod protocol;

use std::net;
use std::io::Error;
use std::sync::{Arc, RwLock};
use std::collections::HashMap;
use tokio_core::io::Io;
use tokio_core::reactor::Core;
use tokio_proto::{TcpClient, pipeline};
use tokio_core::net::{TcpStream, TcpListener};
use tokio_service::Service;
use futures::Future;
use futures::sink::Sink;
use futures::stream::Stream;
use super::actors::Inbox;
pub use self::path::ActorPath;
pub use self::protocol::{MsgPackProtocol, MsgPackCodec, SerDeser};

#[derive(RustcDecodable, RustcEncodable, Debug)]
pub enum RoutingMessage<T: SerDeser> {
    Ok,
    Message {
        sender: ActorPath,
        recipient: usize,
        message: T,
    },
}

pub struct Router<T: SerDeser + 'static> {
    core: Core,
    pub inbox: Inbox<RoutingMessage<T>>,
    clients: HashMap<net::SocketAddr,
                     pipeline::ClientService<TcpStream,
                                             MsgPackProtocol<RoutingMessage<T>,
                                                             RoutingMessage<T>>>>,
}

impl<T> Router<T>
    where T: SerDeser + 'static
{
    pub fn new() -> Router<T> {
        Router {
            core: Core::new().unwrap(),
            clients: HashMap::new(),
            inbox: Arc::new(RwLock::new(Vec::new())),
        }
    }

    // TODO these should start on separate threads too
    pub fn start_server(&self, addr: String) {
        let mut core = Core::new().unwrap();
        let handle = core.handle();
        let addr = addr.parse().unwrap();
        // TODO this should register with a leader node?
        let tcp_socket = TcpListener::bind(&addr, &handle).unwrap();
        println!("Listening on: {}", addr);

        let done = tcp_socket.incoming()
            .for_each(move |(socket, addr)| {
                println!("Received connection from: {}", addr);

                let inbox = self.inbox.clone();
                let (sink, stream) =
                    socket.framed(MsgPackCodec::<RoutingMessage<T>, RoutingMessage<T>>::new())
                        .split();
                let conn = stream.forward(sink.with(move |req| {
                        let req: RoutingMessage<T> = req;
                        println!("{:?}", req);
                        let mut inbox = inbox.write().unwrap();
                        inbox.push(req);
                        let res: Result<RoutingMessage<T>, Error> = Ok(RoutingMessage::Ok);
                        res
                    }))
                    .then(|_| Ok(()));
                handle.spawn(conn);
                Ok(())
            });
        let _ = core.run(done);
    }

    pub fn start_client(&mut self, addr: String) {
        // TODO connection pool?
        let handle = self.core.handle();
        let addr = addr.parse().unwrap();
        println!("connecting to {}", addr);
        let proto: MsgPackProtocol<RoutingMessage<T>, RoutingMessage<T>> = MsgPackProtocol::new();
        let client = TcpClient::new(proto).connect(&addr, &handle);
        let client = self.core.run(client).unwrap();
        self.clients.insert(addr, client);
    }

    pub fn send_msg(&mut self,
                    message: T,
                    sender: ActorPath,
                    recipient: ActorPath)
                    -> Result<RoutingMessage<T>, String> {
        match recipient {
            ActorPath::Local { id } => {
                let msg = RoutingMessage::Message {
                    sender: sender,
                    recipient: id,
                    message: message,
                };
                let mut inbox = self.inbox.write().unwrap();
                inbox.push(msg);
                Ok(RoutingMessage::Ok)
            }
            ActorPath::Remote { addr, id } => {
                let msg = RoutingMessage::Message {
                    sender: sender,
                    recipient: id,
                    message: message,
                };
                let addr = addr.0;
                match self.clients.get(&addr) {
                    Some(client) => {
                        let req = client.call(msg);
                        let res = self.core.run(req).unwrap();
                        Ok(res)
                    }
                    None => Err(format!("No client at {}", addr)),
                }
            }
        }
    }
}
