use std::thread;
use ws::WebSocket;
use redis::Client;

/// Runs a WebSocket server that subscribes to a Redis pubsub channel.
/// Workers can send publish messages to the channel and this server will forward them to
/// connected WebSocket clients.
pub fn ws_server(addr: &'static str, redis_addr: &'static str) -> thread::JoinHandle<()> {
    thread::spawn(move || {
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
            let client = Client::open(redis_addr).unwrap();
            let mut pubsub = client.get_pubsub().unwrap();
            pubsub.subscribe("ws").unwrap();
            loop {
                let msg = pubsub.get_message().unwrap();
                let payload: String = msg.get_payload().unwrap();
                broadcaster.send(payload).unwrap();
            }
        });

        ws.listen(addr).unwrap();
        ps.join().unwrap();
    })
}
