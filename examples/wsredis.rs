extern crate redis;

use redis::{Client, Commands};

fn main() {
    let client = Client::open("redis://127.0.0.1/").unwrap();
    let conn = client.get_connection().unwrap();
    let _: () = conn.publish("ws", "woah").unwrap();
}
