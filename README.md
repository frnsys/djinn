# djinn

[![documentation](https://docs.rs/djinn/badge.svg)](https://docs.rs/djinn/)

[![Join the chat at https://gitter.im/MaxwellRebo/djinn](https://badges.gitter.im/MaxwellRebo/djinn.svg)](https://gitter.im/MaxwellRebo/djinn?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Urban simulation and market dynamics toolkit with connectors to popular machine learning tools

This is still heavily a work-in-progress, but there are some examples you can run:

    cargo run --example basic
    cargo run --example multiple

The `basic` example also has a demo of a frontend client using websockets. Open `examples/ws.html` in a browser, then run the `basic` example to see it in action.

## Dependencies

- `redis >= 3.2`

## A note on performance

Right now most of the overhead is from network communication with Redis. The `redis-rs` library does not have async IO yet (Rust is still working on it, see <https://github.com/mitsuhiko/redis-rs/pull/93>), but it probably will eventually, which should speed things up.

To squeeze the most speed out of a simulation, don't forget to compile it with the `--release` flag!
