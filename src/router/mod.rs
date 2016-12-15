mod path;
mod msgpack;
mod protocol;

use std::sync::{Arc, RwLock};
use std::net;
use std::io::Error;
use tokio_core::reactor::Core;
use tokio_core::net::TcpStream;
use tokio_service::Service;
use tokio_core::net::TcpListener;
use tokio_proto::{TcpClient, pipeline};
use futures::Future;
use tokio_core::io::Io;
use futures::stream::Stream;
use futures::sink::Sink;
use self::protocol::{MsgPackProtocol, MsgPackCodec, SerDeser};
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

struct Router<T: SerDeser + 'static> {
    core: Core,
    inbox: Inbox<RoutingMessage<T>>,
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
        let msg = RoutingMessage::Message {
            sender: sender,
            recipient: recipient.clone(),
            message: message,
        };
        match recipient {
            ActorPath::Local { id } => {
                let mut inbox = self.inbox.write().unwrap();
                inbox.push(msg);
                Ok(RoutingMessage::Ok)
            }
            ActorPath::Remote { addr, id } => {
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
