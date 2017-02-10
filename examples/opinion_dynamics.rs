extern crate rand;
extern crate djinn;
extern crate redis;
extern crate rustc_serialize;

mod opdyn;

use redis::Client;
use djinn::{Manager, Agent, run};
use djinn::yaml::load_from_yaml;
use opdyn::{OpinionDynamicsSim, State, World, Person, Media, Opinion};


fn main() {
    let conf = load_from_yaml("opdyn.yaml");
    let sim = OpinionDynamicsSim {
        opinion_shift_proportion: conf["opinion_shift_proportion"].as_f64().unwrap(),
    };
    let world = World {};

    // Setup the manager
    let addr = "redis://127.0.0.1/";
    let pop_client = Client::open(addr).unwrap();
    let mut manager = Manager::new(addr, pop_client, sim.clone());

    let mut medias = vec![Media {
                              opinions: vec![Opinion {
                                                 polarity: -1,
                                                 priority: 5,
                                             },
                                             Opinion {
                                                 polarity: -5,
                                                 priority: 1,
                                             }],
                          },
                          Media {
                              opinions: vec![Opinion {
                                                 polarity: 1,
                                                 priority: 8,
                                             },
                                             Opinion {
                                                 polarity: 2,
                                                 priority: 4,
                                             }],
                          }];
    let media_ids = manager.spawns(medias.drain(..).map(|m| State::Media(m)).collect());

    let mut people = vec![Person::new(vec![Opinion {
                                               polarity: 100,
                                               priority: 1,
                                           },
                                           Opinion {
                                               polarity: 0,
                                               priority: 0,
                                           }]),
                          Person::new(vec![Opinion {
                                               polarity: -50,
                                               priority: 1,
                                           },
                                           Opinion {
                                               polarity: 100,
                                               priority: 0,
                                           }])];
    let people_ids = manager.spawns(people.drain(..).map(|m| State::Person(m)).collect());

    manager.register_reporter(1, |step, pop, _| {
        // Mean polarity of first issue
        let people: Vec<Agent<State>> = pop.lookup("people");
        let polarity = people.iter().fold(0, |acc, a| {
            match a.state {
                State::Person(ref p) => acc + p.opinions[0].polarity,
                _ => acc,
            }
        });
        println!("[{:02}] mean polarity: {}",
                 step,
                 (polarity as f64) / (people.len() as f64));
    });

    println!("running");
    run(sim, world, manager, 4, 10);
}
