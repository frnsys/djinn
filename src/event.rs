use std::thread;
use ws::WebSocket;
use redis::{Client, Commands};

/// A WebSocket server which receives and publishes events.
pub struct WebSocketServer {
    addr: String,
    redis_addr: String,
    t: Option<thread::JoinHandle<()>>,
}

impl WebSocketServer {
    pub fn new(addr: &str, redis_addr: &str) -> WebSocketServer {
        WebSocketServer {
            t: None,
            addr: addr.to_owned(),
            redis_addr: redis_addr.to_owned(),
        }
    }

    /// Runs a WebSocket server that subscribes to a Redis pubsub channel.
    /// Workers can publish messages to the channel and this server will forward them to
    /// connected WebSocket clients.
    pub fn start(&mut self) {
        let addr = self.addr.clone();
        let redis_addr = self.redis_addr.clone();
        self.t = Some(thread::spawn(move || {
            // websocket to broadcast to all clients
            let ws = WebSocket::new(|_| {
                    move |msg| {
                        println!("got: {}", msg);
                        Ok(())
                    }
                })
                .unwrap();
            let broadcaster = ws.broadcaster();

            // redis pubsub that workers can throw messages to
            let ps = thread::spawn(move || {
                let client = Client::open(&redis_addr[..]).unwrap();
                let mut pubsub = client.get_pubsub().unwrap();
                pubsub.subscribe("ws").unwrap();
                loop {
                    let msg = pubsub.get_message().unwrap();
                    let payload: String = msg.get_payload().unwrap();
                    if payload == "TERMINATE" {
                        broadcaster.shutdown().unwrap();
                        break;
                    } else {
                        broadcaster.send(payload).unwrap();
                    }
                }
            });

            ws.listen(&addr[..]).unwrap();
            ps.join().unwrap();
        }));
    }

    /// Shutdown the websocket server.
    pub fn shutdown(self) {
        match self.t {
            Some(t) => {
                let client = Client::open(self.redis_addr.as_ref()).unwrap();
                let conn = client.get_connection().unwrap();
                let _: () = conn.publish("ws", "TERMINATE").unwrap();
                t.join().unwrap();
            }
            None => (),
        }
    }
}
