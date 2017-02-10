use rand::Rng;
use std::collections::HashMap;
use rand::distributions::{Weighted, WeightedChoice, IndependentSample};
use super::sim::PersonUpdate;

// when two people share the exact same polarity on an opinion,
// this is how much their trust improves by
const MAX_POS_TRUST_SHIFT: f64 = 1.;

// if two people have a polarity within +/- this value,
// their trust improves
const MAX_POS_TRUST_SHIFT_RANGE: f64 = 4.;

#[derive(RustcDecodable, RustcEncodable, Debug, PartialEq, Clone)]
pub struct Opinion {
    pub polarity: i32,
    pub priority: u32,
}

#[derive(RustcDecodable, RustcEncodable, Debug, PartialEq, Clone)]
pub struct Person {
    pub opinions: Vec<Opinion>,
    pub medias: HashMap<u64, u32>,
    pub friends: HashMap<u64, u32>,
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
    pub fn rand_opinion_idx(&self, mut rng: &mut Rng, opinions: &Vec<Opinion>) -> usize {
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
    pub fn rand_edge(&self, mut rng: &mut Rng, edges: &HashMap<u64, u32>) -> u64 {
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
    pub fn be_influenced(&self,
                         op_idx: usize,
                         op1: &Opinion,
                         op2: &Opinion,
                         op_shift_proportion: f64)
                         -> PersonUpdate {
        let diff = op2.polarity - op1.polarity;
        let shift = ((diff as f64) * op_shift_proportion).round() as i32;
        PersonUpdate::OpinionShift {
            idx: op_idx,
            polarity: shift,
        }
    }

    /// Based on hearing this person's opinion, adjust trust.
    /// The lower distance between their opinions, the more they trust each other.
    pub fn trust_shift(&self, op1: &Opinion, op2: &Opinion) -> i32 {
        let dist = (op1.polarity - op2.polarity).abs();
        let trust_shift = -((dist as f64) / MAX_POS_TRUST_SHIFT_RANGE).powi(2) +
                          MAX_POS_TRUST_SHIFT;
        trust_shift.round() as i32
    }
}

#[derive(RustcDecodable, RustcEncodable, Debug, PartialEq, Clone)]
pub struct Media {
    pub opinions: Vec<Opinion>,
}
