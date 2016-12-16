use std::{net, thread};
use std::io::Error;
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use tokio_core::io::Io;
use tokio_core::net::TcpListener;
use tokio_core::reactor::Core;
use tokio_proto::TcpClient;
use tokio_service::Service;
use futures::Future;
use futures::sink::Sink;
use futures::stream::Stream;
use super::actor::{Actor, ActorRef, ActorVecRef};
use super::path::{ActorPath, RemoteAddr};
use super::message::Message;
use super::protocol::{MsgPackProtocol, MsgPackCodec};

#[derive(RustcDecodable, RustcEncodable, Debug, PartialEq)]
pub enum RoutingMessage<T: Message> {
    Ok,
    Err(String),
    Tell {
        sender: ActorPath,
        recipient: usize,
        message: T,
    },
    Ask {
        sender: ActorPath,
        recipient: usize,
        message: T,
    },
    Response {
        sender: ActorPath,
        recipient: usize,
        message: T,
    },
}

pub struct Router<A: Actor + 'static> {
    addr: String,
    actors: Arc<Mutex<HashMap<usize, ActorRef<A>>>>,
}

impl<A> Router<A>
    where A: Actor + 'static
{
    pub fn new(addr: String, actors: ActorVecRef<A>) -> Router<A> {
        let mut lookup = HashMap::<usize, ActorRef<A>>::new();
        {
            let actors = actors.clone();
            let actors = actors.read().unwrap();
            for actor in actors.iter() {
                let actor_r = actor.read().unwrap();
                lookup.insert(actor_r.id(), actor.clone());
            }
        }
        Router {
            addr: addr,
            actors: Arc::new(Mutex::new(lookup)),
        }
    }

    pub fn serve(&self) {
        let addr = self.addr.clone();
        let actors = self.actors.clone();
        thread::spawn(move || {
            let mut core = Core::new().unwrap();
            let handle = core.handle();
            let addr = addr.parse().unwrap();
            let tcp_socket = TcpListener::bind(&addr, &handle).unwrap();
            println!("Listening on: {}", addr);

            let done = tcp_socket.incoming()
                .for_each(move |(socket, addr)| {
                    println!("Received connection from: {}", addr);

                    let actors = actors.clone();
                    let (sink, stream) =
                            socket.framed(MsgPackCodec::<RoutingMessage<A::M>,
                                                       RoutingMessage<A::M>>::new())
                                .split();
                    let conn = stream.forward(sink.with(move |req| {
                            let req: RoutingMessage<A::M> = req;
                            println!("{:?}", req);
                            let res: Result<RoutingMessage<A::M>, Error> = match req {
                                RoutingMessage::Tell { sender, recipient, message } => {
                                    match actors.lock().unwrap().get(&recipient) {
                                        Some(actor) => {
                                            let actor_r = actor.read().unwrap();
                                            let mut inbox = actor_r.inbox().write().unwrap();
                                            inbox.push(message);
                                            Ok(RoutingMessage::Ok)
                                        }
                                        None => {
                                            Ok(RoutingMessage::Err(format!("No actor with id {}",
                                                                           recipient)))
                                        }
                                    }
                                }
                                RoutingMessage::Ask { sender, recipient, message } => {
                                    match actors.lock().unwrap().get(&recipient) {
                                        Some(actor) => {
                                            let actor_r = actor.read().unwrap();
                                            let resp = actor_r.handle_msg(message);
                                            let id = match sender {
                                                ActorPath::Local { id } => id,
                                                ActorPath::Remote { addr, id } => id,
                                            };
                                            Ok(RoutingMessage::Response {
                                                recipient: id,
                                                sender: ActorPath::Remote {
                                                    addr: RemoteAddr(addr),
                                                    id: recipient,
                                                },
                                                message: resp,
                                            })
                                        }
                                        None => Ok(RoutingMessage::Ok),
                                    }
                                }
                                RoutingMessage::Ok => Ok(RoutingMessage::Ok),
                                RoutingMessage::Err(_) => Ok(RoutingMessage::Ok),
                                _ => Ok(RoutingMessage::Ok),
                            };
                            res
                        }))
                        .then(|_| Ok(()));
                    handle.spawn(conn);
                    Ok(())
                });
            let _ = core.run(done);
        });
    }

    fn send_msg(&self, addr: RemoteAddr, message: RoutingMessage<A::M>) -> RoutingMessage<A::M> {
        let mut core = Core::new().unwrap();
        let handle = core.handle();
        let addr = addr.0;
        println!("connecting to {}", addr);
        let proto: MsgPackProtocol<RoutingMessage<A::M>, RoutingMessage<A::M>> =
            MsgPackProtocol::new();
        let client = TcpClient::new(proto).connect(&addr, &handle);
        let res = core.run(client.and_then(|client| client.call(message)));
        match res {
            Ok(resp) => resp,
            Err(_) => RoutingMessage::Err("Error sending message".to_string()),
        }
    }

    pub fn tell(&self,
                message: A::M,
                sender: ActorPath,
                recipient: ActorPath)
                -> RoutingMessage<A::M> {
        match recipient {
            ActorPath::Local { id } => {
                match self.actors.lock().unwrap().get(&id) {
                    Some(actor) => {
                        let actor_r = actor.read().unwrap();
                        let mut inbox = actor_r.inbox().write().unwrap();
                        inbox.push(message);
                        RoutingMessage::Ok
                    }
                    None => RoutingMessage::Err("No actor with id".to_string()),
                }
            }
            ActorPath::Remote { addr, id } => {
                // TODO this should not just be tell, should have the option of ask
                let msg = RoutingMessage::Tell {
                    sender: sender,
                    recipient: id,
                    message: message,
                };
                self.send_msg(addr, msg)
            }
        }
    }

    pub fn ask(&self,
               message: A::M,
               sender: ActorPath,
               recipient: ActorPath)
               -> RoutingMessage<A::M> {
        match recipient {
            ActorPath::Local { id } => {
                match self.actors.lock().unwrap().get(&id) {
                    Some(actor) => {
                        let actor_r = actor.read().unwrap();
                        let resp = actor_r.handle_msg(message);
                        let sender_id = match sender {
                            ActorPath::Local { id } => id,
                            ActorPath::Remote { addr, id } => id,
                        };
                        RoutingMessage::Response {
                            recipient: sender_id,
                            sender: ActorPath::Local { id: id },
                            message: resp,
                        }
                    }
                    None => RoutingMessage::Err("No actor with id".to_string()),
                }
            }
            ActorPath::Remote { addr, id } => {
                // TODO this should not just be tell, should have the option of ask
                let msg = RoutingMessage::Ask {
                    sender: sender,
                    recipient: id,
                    message: message,
                };
                self.send_msg(addr, msg)
            }
        }
    }
}
