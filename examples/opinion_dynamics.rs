extern crate rand;
extern crate djinn;
extern crate redis;
extern crate rustc_serialize;

use rand::Rng;
use redis::Client;
use djinn::{Agent, Manager, Simulation, Population, Updates, Redis, run};
use rand::distributions::{Weighted, WeightedChoice, IndependentSample};

#[derive(RustcDecodable, RustcEncodable, Debug, PartialEq, Clone)]
struct Opinion {
    polarity: f64,
    priority: f64,
}

#[derive(RustcDecodable, RustcEncodable, Debug, PartialEq, Clone)]
struct Person {
    opinions: Vec<Opinion>,
    medias: Vec<Edge>,
    friends: Vec<Edge>,
}

impl Person {
    pub fn new(opinions: Vec<Opinion>) -> Person {
        Person {
            opinions: opinions,
            medias: Vec::new(),
            friends: Vec::new(),
        }
    }
}

#[derive(RustcDecodable, RustcEncodable, Debug, PartialEq, Clone)]
struct Edge {
    to: u64,
    weight: u32,
}

#[derive(RustcDecodable, RustcEncodable, Debug, PartialEq, Clone)]
enum EdgeType {
    Media,
    Friend,
}

#[derive(RustcDecodable, RustcEncodable, Debug, PartialEq, Clone)]
struct Media {
    opinions: Vec<Opinion>,
}

#[derive(RustcDecodable, RustcEncodable, Debug, PartialEq, Clone)]
enum State {
    Person(Person),
    Media(Media),
}

#[derive(RustcDecodable, RustcEncodable, Debug, PartialEq, Clone)]
enum PersonUpdate {
    OpinionShift {
        idx: usize,
        polarity: f64,
        priority: f64,
    },
    TrustShift {
        id: u64,
        shift: f64,
        edgeType: EdgeType,
    },
    Meet { id: u64, trust: f64 },
    Discover { id: u64, trust: f64 },
}

#[derive(RustcDecodable, RustcEncodable, Debug, PartialEq, Clone)]
enum MediaUpdate {
    OpinionShift {
        idx: usize,
        polarity: f64,
        priority: f64,
    },
}

#[derive(RustcDecodable, RustcEncodable, Debug, PartialEq, Clone)]
enum Update {
    Person(PersonUpdate),
    Media(MediaUpdate),
}

#[derive(RustcDecodable, RustcEncodable, Debug, PartialEq, Clone)]
struct World {
    weather: String,
}

#[derive(Clone)]
struct OpinionDynamicsSim;

impl OpinionDynamicsSim {
    fn decide_person<R: Redis>(&self,
                               id: u64,
                               person: &Person,
                               pop: &Population<Self, R>,
                               updates: &mut Updates<Self>)
                               -> () {
        let mut rng = rand::weak_rng();

        // talk to a person or consume media?
        if rng.gen::<f64>() < 0.5 {
            // choose a media.
            // the less media this person is familiar with,
            // the more likely they will encounter a random one.
            // otherwise, they choose one with probability based on how much they trust it.
            let p_rand_media = (person.medias.len() as f64) / 2.; // TODO denom should be a config val
            let media = if rng.gen::<f64>() < p_rand_media {
                let m = pop.random("media");

                // create an edge
                // TODO check if already has edge to this one
                updates.queue(id,
                              Update::Person(PersonUpdate::Discover {
                                  id: id,
                                  trust: 0., // TODO bootstrap trust in some way
                              }));
                m
            } else {
                let mut items: Vec<Weighted<u64>> = person.medias
                    .iter()
                    .map(|e| {
                        Weighted {
                            item: e.to,
                            weight: e.weight,
                        }
                    })
                    .collect();
                let wc = WeightedChoice::new(&mut items);
                let id = wc.ind_sample(&mut rng);
                pop.get_agent(id).unwrap()
            };
        } else {
            // choose a person to talk to.
            let p_rand_person = (person.friends.len() as f64) / 2.; // TODO denom should be a config val
            let person = if rng.gen::<f64>() < p_rand_person {
                let p = pop.random("person"); // TODO prob shouldnt be themselves
                // create an edge
                // TODO check if already has edge to this one
                updates.queue(id,
                              Update::Person(PersonUpdate::Meet {
                                  id: id,
                                  trust: 0., // TODO bootstrap trust in some way
                              }));
                p
            } else {
                let mut items: Vec<Weighted<u64>> = person.friends
                    .iter()
                    .map(|e| {
                        Weighted {
                            item: e.to,
                            weight: e.weight,
                        }
                    })
                    .collect();
                let wc = WeightedChoice::new(&mut items);
                let id = wc.ind_sample(&mut rng);
                pop.get_agent(id).unwrap()
            };
        }
    }
}

impl Simulation for OpinionDynamicsSim {
    type State = State;
    type Update = Update;
    type World = World;

    fn decide<R: Redis>(&self,
                        agent: &Agent<Self::State>,
                        world: &Self::World,
                        pop: &Population<Self, R>,
                        updates: &mut Updates<Self>)
                        -> () {
        match agent.state {
            State::Person(ref p) => {
                self.decide_person(agent.id, p, pop, updates);
            }
            State::Media(ref m) => {
                // TODO
            }
        }
    }

    fn update(&self, mut state: &mut Self::State, updates: Vec<Self::Update>) -> bool {
        // TODO
        false
    }

    fn on_spawns<R: Redis>(&self,
                           agents: Vec<Agent<Self::State>>,
                           population: &Population<Self, R>)
                           -> () {
        // index newly created media/people
        // so we can sample them later
        let mut media = Vec::new();
        let mut people = Vec::new();
        let _: Vec<()> = agents.iter()
            .map(|a| {
                match a.state {
                    State::Person(_) => people.push(a.id),
                    State::Media(_) => media.push(a.id),
                }
            })
            .collect();
        let _: () = population.indexes("media", media);
        let _: () = population.indexes("people", people);
    }
}

// TODO
// person step:
//  - sample a person to talk to, based on trust. or encounter random.
//  - read a story from media, based on opinion alignment and importance
//  - talk to person
//  - change opinion based on media
//  - change opinion based on talking to person
//

fn main() {
    // TODO
    // - bootstrap social network
    // - people more likely talk to likeminded friends
    // - randomly meet new people

    let sim = OpinionDynamicsSim {};

    // TODO
    let world = World { weather: "sunny".to_string() };

    // Setup the manager
    let addr = "redis://127.0.0.1/";
    let pop_client = Client::open(addr).unwrap();
    let mut manager = Manager::new(addr, pop_client, sim.clone());

    let mut medias = vec![Media {
                              opinions: vec![Opinion {
                                                 polarity: -1.,
                                                 priority: 0.5,
                                             },
                                             Opinion {
                                                 polarity: -0.5,
                                                 priority: 1.,
                                             }],
                          },
                          Media {
                              opinions: vec![Opinion {
                                                 polarity: 1.,
                                                 priority: 0.8,
                                             },
                                             Opinion {
                                                 polarity: -0.2,
                                                 priority: 0.4,
                                             }],
                          }];
    let media_ids = manager.spawns(medias.drain(..).map(|m| State::Media(m)).collect());

    let mut people = vec![Person::new(vec![Opinion {
                                               polarity: 1.,
                                               priority: 1.,
                                           },
                                           Opinion {
                                               polarity: 0.,
                                               priority: 0.,
                                           }]),
                          Person::new(vec![Opinion {
                                               polarity: -1.,
                                               priority: 1.,
                                           },
                                           Opinion {
                                               polarity: 1.,
                                               priority: 0.,
                                           }])];
    let people_ids = manager.spawns(people.drain(..).map(|m| State::Person(m)).collect());

    // run(sim, world, manager, 4, 10);
}
