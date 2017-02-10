extern crate rand;
extern crate djinn;
extern crate redis;
extern crate rustc_serialize;

use rand::Rng;
use redis::Client;
use djinn::{Agent, Manager, Simulation, Population, Updates, Redis, run};
use rand::distributions::{Weighted, WeightedChoice, IndependentSample};
use std::collections::HashMap;

// when two people share the exact same polarity on an opinion,
// this is how much their trust improves by
const MAX_POS_TRUST_SHIFT: f64 = 1.;

// if two people have a polarity within +/- this value,
// their trust improves
const MAX_POS_TRUST_SHIFT_RANGE: f64 = 4.;

// when an opinion shifts, it shifts by this proportion of
// the opinion difference
const OPINION_SHIFT_PROPORTION: f64 = 0.1;

#[derive(RustcDecodable, RustcEncodable, Debug, PartialEq, Clone)]
struct Opinion {
    polarity: i32,
    priority: u32,
}

#[derive(RustcDecodable, RustcEncodable, Debug, PartialEq, Clone)]
struct Person {
    opinions: Vec<Opinion>,
    medias: HashMap<u64, u32>,
    friends: HashMap<u64, u32>,
}

impl Person {
    pub fn new(opinions: Vec<Opinion>) -> Person {
        Person {
            opinions: opinions,
            medias: HashMap::new(),
            friends: HashMap::new(),
        }
    }

    /// Select a random opinion weighted by personal priority.
    fn rand_opinion_idx(&self, mut rng: &mut Rng, opinions: &Vec<Opinion>) -> usize {
        let mut items: Vec<Weighted<usize>> = opinions.iter()
            .enumerate()
            .map(|(i, o)| {
                Weighted {
                    item: i,
                    weight: self.opinions[i].priority,
                }
            })
            .collect();
        let wc = WeightedChoice::new(&mut items);
        wc.ind_sample(&mut rng).clone()
    }

    /// Select a random edge by weight.
    fn rand_edge(&self, mut rng: &mut Rng, edges: &HashMap<u64, u32>) -> u64 {
        let mut items: Vec<Weighted<u64>> = edges.iter()
            .map(|(id, weight)| {
                Weighted {
                    item: *id,
                    weight: *weight,
                }
            })
            .collect();
        let wc = WeightedChoice::new(&mut items);
        wc.ind_sample(&mut rng)
    }

    /// Trust is computed from alignment of opinion polarities,
    /// weighted by the importance/priority of those opinions
    /// from the perspective of the subject.
    fn trust_from_opinions(&self, opinions: &Vec<Opinion>) -> u32 {
        self.opinions
            .iter()
            .zip(opinions.iter())
            .fold(0, |acc, (o1, o2)| acc + self.trust_from_opinion(o1, o2))
    }

    fn trust_from_opinion(&self, o1: &Opinion, o2: &Opinion) -> u32 {
        let dist = (o1.polarity - o2.polarity).abs();
        let x = ((2 - dist) as f64) / 1.;
        ((x - 1.) * (o1.priority as f64)).round() as u32
    }

    /// TODO atm only considering a shift _towards_ the other person's opinion,
    /// but totally possible that encountering a divergent opinion
    /// causes one to double-down on their own opinion.
    /// TODO atm this is only a one-way exchange, totally possible to influence the
    /// other person's opinion too.
    fn be_influenced(&self, op_idx: usize, op1: &Opinion, op2: &Opinion) -> PersonUpdate {
        let diff = op2.polarity - op1.polarity;
        let shift = ((diff as f64) * OPINION_SHIFT_PROPORTION).round() as i32;
        PersonUpdate::OpinionShift {
            idx: op_idx,
            polarity: shift,
        }
    }

    /// Based on hearing this person's opinion, adjust trust.
    /// The lower distance between their opinions, the more they trust each other.
    fn trust_shift(&self, op1: &Opinion, op2: &Opinion) -> i32 {
        let dist = (op1.polarity - op2.polarity).abs();
        let trust_shift = -((dist as f64) / MAX_POS_TRUST_SHIFT_RANGE).powi(2) +
                          MAX_POS_TRUST_SHIFT;
        trust_shift.round() as i32
    }
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
    OpinionShift { idx: usize, polarity: i32 },
    TrustShift {
        id: u64,
        shift: i32,
        edge_type: EdgeType,
    },
}

#[derive(RustcDecodable, RustcEncodable, Debug, PartialEq, Clone)]
enum MediaUpdate {
    Click { idx: usize, polarity: i32 },
}

#[derive(RustcDecodable, RustcEncodable, Debug, PartialEq, Clone)]
enum Update {
    Person(PersonUpdate),
    Media(MediaUpdate),
}

#[derive(RustcDecodable, RustcEncodable, Debug, PartialEq, Clone)]
struct World {}

#[derive(Clone)]
struct OpinionDynamicsSim;

impl OpinionDynamicsSim {
    fn decide_media<R: Redis>(&self,
                              id: u64,
                              media: &Media,
                              pop: &Population<Self, R>,
                              updates: &mut Updates<Self>)
                              -> () {
        // media's not really doing anything atm
    }

    fn decide_person<R: Redis>(&self,
                               id: u64,
                               person: &Person,
                               pop: &Population<Self, R>,
                               updates: &mut Updates<Self>)
                               -> () {
        let mut rng = rand::weak_rng();

        // talk to a person or consume media?
        let other = if rng.gen::<f64>() < 0.5 {
            // choose a media.
            // the less media this person is familiar with,
            // the more likely they will encounter a random one.
            // otherwise, they choose one with probability based on how much they trust it.
            let p_rand_media = 1. - ((person.medias.len() as f64) / 2.); // TODO denom should be a config val
            if rng.gen::<f64>() < p_rand_media {
                pop.random("media")
            } else {
                let id = person.rand_edge(&mut rng, &person.medias);
                pop.get_agent(id).unwrap()
            }
        } else {
            // choose a person to talk to.
            let p_rand_person = 1. - ((person.friends.len() as f64) / 2.); // TODO denom should be a config val
            if rng.gen::<f64>() < p_rand_person {
                pop.random("people") // TODO prob shouldnt be themselves
            } else {
                let id = person.rand_edge(&mut rng, &person.friends);
                pop.get_agent(id).unwrap()
            }
        };
        match other.state {
            State::Person(ref p) => {
                let op_idx = person.rand_opinion_idx(&mut rng, &p.opinions);
                let ref op1 = person.opinions[op_idx];
                let ref op2 = p.opinions[op_idx];

                // naively bootstrap trust for new person as 0
                let trust = match person.friends.get(&other.id) {
                    Some(t) => *t,
                    None => 0,
                };
                let p_opinion_shift = ((trust as f64) + 0.01) / 100.;
                if rng.gen::<f64>() < p_opinion_shift {
                    updates.queue(id, Update::Person(person.be_influenced(op_idx, op1, op2)));
                }

                updates.queue(id,
                              Update::Person(PersonUpdate::TrustShift {
                                  id: other.id,
                                  shift: person.trust_shift(op1, op2),
                                  edge_type: EdgeType::Friend,
                              }))
            }
            State::Media(ref m) => {
                let op_idx = person.rand_opinion_idx(&mut rng, &m.opinions);
                let ref op1 = person.opinions[op_idx];
                let ref op2 = m.opinions[op_idx];

                // "click" on the story
                updates.queue(other.id,
                              Update::Media(MediaUpdate::Click {
                                  idx: op_idx,
                                  polarity: op1.polarity,
                              }));

                // naively bootstrap trust for new media as 0
                let trust = match person.medias.get(&other.id) {
                    Some(t) => *t,
                    None => 0,
                };

                let p_opinion_shift = ((trust as f64) + 0.01) / 100.;
                if rng.gen::<f64>() < p_opinion_shift {
                    updates.queue(id, Update::Person(person.be_influenced(op_idx, op1, op2)));
                }

                updates.queue(id,
                              Update::Person(PersonUpdate::TrustShift {
                                  id: other.id,
                                  shift: person.trust_shift(op1, op2),
                                  edge_type: EdgeType::Media,
                              }))
            }
        }
    }

    fn update_person(&self, mut person: &mut Person, updates: Vec<Update>) -> bool {
        let mut updated = false;
        for update in updates {
            match update {
                Update::Person(u) => {
                    match u {
                        PersonUpdate::OpinionShift { idx, polarity } => {
                            let ref mut op = person.opinions[idx];
                            op.polarity += polarity;
                            updated = true;
                        }
                        PersonUpdate::TrustShift { id, shift, edge_type } => {
                            match edge_type {
                                EdgeType::Friend => {
                                    let trust = *person.friends.entry(id).or_insert(0) as i32;
                                    person.friends.insert(id, (trust + shift) as u32);
                                }
                                EdgeType::Media => {
                                    let trust = *person.medias.entry(id).or_insert(0) as i32;
                                    person.medias.insert(id, (trust + shift) as u32);
                                }
                            }
                            updated = true;
                        }
                    }
                }
                _ => (),
            }
        }
        updated
    }

    fn update_media(&self, mut media: &mut Media, updates: Vec<Update>) -> bool {
        let mut updated = false;
        for update in updates {
            match update {
                Update::Media(u) => {
                    match u {
                        MediaUpdate::Click { idx, polarity } => {
                            let ref mut op = media.opinions[idx];
                            let diff = polarity - op.polarity;
                            op.polarity += ((polarity as f64) * OPINION_SHIFT_PROPORTION)
                                .round() as i32;
                            op.priority += 1;
                            updated = true;
                        }
                    }
                }
                _ => (),
            }
        }
        updated
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
                self.decide_media(agent.id, m, pop, updates);
            }
        }
    }

    fn update(&self, mut state: &mut Self::State, updates: Vec<Self::Update>) -> bool {
        match *state {
            State::Person(ref mut p) => self.update_person(p, updates),
            State::Media(ref mut m) => self.update_media(m, updates),
        }
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

fn main() {
    let sim = OpinionDynamicsSim {};
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
                                               polarity: 1,
                                               priority: 1,
                                           },
                                           Opinion {
                                               polarity: 0,
                                               priority: 0,
                                           }]),
                          Person::new(vec![Opinion {
                                               polarity: -1,
                                               priority: 1,
                                           },
                                           Opinion {
                                               polarity: 1,
                                               priority: 0,
                                           }])];
    let people_ids = manager.spawns(people.drain(..).map(|m| State::Person(m)).collect());

    println!("running");
    run(sim, world, manager, 4, 10);
}
