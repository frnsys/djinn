mod path;
mod msgpack;
mod protocol;

use std::sync::{Arc, RwLock};
use std::{io, marker, net};
use tokio_core::reactor::Core;
use tokio_core::net::TcpStream;
use tokio_service::Service;
use tokio_proto::{TcpServer, TcpClient, pipeline};
use futures::{future, Future, BoxFuture};
use self::protocol::{MsgPackProtocol, SerDeser};
use self::path::ActorPath;
use super::actors::Inbox;
use std::collections::HashMap;

#[derive(RustcDecodable, RustcEncodable, Debug)]
enum RoutingMessage<T: SerDeser> {
    Ok,
    Message {
        sender: ActorPath,
        recipient: ActorPath,
        message: T,
    },
}

struct RouterService<T>
    where T: SerDeser
{
    message: marker::PhantomData<T>,
    inbox: Inbox<RoutingMessage<T>>,
}

impl<T> RouterService<T>
    where T: SerDeser
{
    pub fn new() -> RouterService<T> {
        RouterService {
            message: marker::PhantomData,
            inbox: Arc::new(RwLock::new(Vec::new())),
        }
    }
}

impl<T> Service for RouterService<T>
    where T: SerDeser + 'static
{
    type Request = RoutingMessage<T>;
    type Response = RoutingMessage<T>;
    type Error = io::Error;
    type Future = BoxFuture<RoutingMessage<T>, io::Error>;

    fn call(&self, req: RoutingMessage<T>) -> Self::Future {
        // place the incoming message in the router's inbox
        // the dispatcher handles final delivery
        let mut inbox = self.inbox.write().unwrap();
        inbox.push(req);
        future::finished(RoutingMessage::Ok).boxed()
    }
}

struct Router<T: SerDeser + 'static> {
    core: Core,
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
        }
    }

    // TODO these should start on separate threads too
    pub fn start_server(&self, addr: String) {
        let addr = addr.parse().unwrap();
        println!("listening on {}", addr);
        // TODO how do I get access to the service inbox??
        TcpServer::new(MsgPackProtocol::new(), addr).serve(|| Ok(RouterService::<T>::new()));
        // TODO this should register with a leader node?
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
            // TODO temporary, should just put in own inbox
            ActorPath::Local { id } => Err(format!("temp")),
            ActorPath::Remote { addr, id } => {
                let addr = addr.0;
                match self.clients.get(&addr) {
                    Some(client) => {
                        let msg = RoutingMessage::Message {
                            sender: sender,
                            recipient: recipient,
                            message: message,
                        };
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
