extern crate ws;
extern crate redis;

use ws::connect;

fn main() {
    connect("ws://127.0.0.1:3012", |_| {
            move |msg| {
                println!("got: {}", msg);
                Ok(())
            }
        })
        .unwrap();
}
