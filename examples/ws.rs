extern crate ws;
extern crate redis;

use std::thread;
use ws::WebSocket;
use redis::Client;

fn main() {
    let mut ws = WebSocket::new(|_| {
            move |msg| {
                println!("got: {}", msg);
                Ok(())
            }
        })
        .unwrap();
    let broadcaster = ws.broadcaster();

    let ps = thread::spawn(move || {
        let client = Client::open("redis://127.0.0.1/").unwrap();
        let mut pubsub = client.get_pubsub().unwrap();
        pubsub.subscribe("ws").unwrap();
        loop {
            let msg = pubsub.get_message().unwrap();
            let payload: String = msg.get_payload().unwrap();
            broadcaster.send(payload).unwrap();
        }
    });

    ws.listen("127.0.0.1:3012").unwrap();
    ps.join().unwrap();
}
