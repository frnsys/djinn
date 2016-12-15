mod msgpack;
mod protocol;

use std::{io, marker, net};
use tokio_core::reactor::Core;
use tokio_core::net::TcpStream;
use tokio_service::Service;
use tokio_proto::{TcpServer, TcpClient, pipeline};
use futures::{future, Future, BoxFuture};
use self::protocol::{MsgPackProtocol, SerDeser};
use std::collections::HashMap;

struct RouterService<T>
    where T: SerDeser
{
    message: marker::PhantomData<T>,
}

impl<T> Service for RouterService<T>
    where T: SerDeser + 'static
{
    type Request = Option<T>;
    type Response = Option<T>;
    type Error = io::Error;
    type Future = BoxFuture<Option<T>, io::Error>;

    fn call(&self, req: Option<T>) -> Self::Future {
        // TODO
        let res = None;
        future::finished(res).boxed()
    }
}

struct Router<T: SerDeser + 'static> {
    core: Core,
    clients: HashMap<net::SocketAddr,
                     pipeline::ClientService<TcpStream, MsgPackProtocol<Option<T>, Option<T>>>>,
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
        TcpServer::new(MsgPackProtocol::new(), addr)
            .serve(|| Ok(RouterService::<T> { message: marker::PhantomData }));
    }

    pub fn start_client(&mut self, addr: String) {
        // returns a client connected to addr
        // to make a request:
        //  let req = client.call(Message::Foo);
        //  let res = core.run(req).unwrap();
        let handle = self.core.handle();
        let addr = addr.parse().unwrap();
        println!("connecting to {}", addr);
        let proto: MsgPackProtocol<Option<T>, Option<T>> = MsgPackProtocol::new();
        let client = TcpClient::new(proto).connect(&addr, &handle);
        let client = self.core.run(client).unwrap();
        self.clients.insert(addr, client);
    }

    pub fn send_msg(&mut self, message: T, addr: String) -> Result<Option<T>, String> {
        let addr = addr.parse().unwrap();
        match self.clients.get(&addr) {
            Some(client) => {
                let req = client.call(Some(message));
                let res = self.core.run(req).unwrap();
                Ok(res)
            }
            None => Err(format!("No client at {}", addr)),
        }
    }
}
