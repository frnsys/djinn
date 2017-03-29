extern crate rand;
extern crate djinn;

use djinn::ext::bnet::{Var, Dataset, BNet};
use std::collections::HashMap;
use rand::distributions::{IndependentSample, Range};


#[derive(Debug)]
enum Occupation {
    Unemployed,
    Student,
    Programmer,
}

#[derive(Debug, Eq, PartialEq, Hash, Clone)]
enum Vars {
    Age,
    Income,
    Occupation,
}

struct Datum {
    age: u8,
    income: usize,
    occupation: Occupation,
}

fn main() {
    // generate a dataset
    let n_samples = 100000;
    let mut rng = rand::thread_rng();
    let ages = Range::new(1, 100);
    let low_income = Range::new(0, 20000);
    let mid_income = Range::new(10000, 30000);
    let hii_income = Range::new(60000, 100000);
    let pop: Vec<Datum> = (0..n_samples)
        .map(|_| {
            let age = ages.ind_sample(&mut rng);
            let occupation = if age <= 14 {
                Occupation::Unemployed
            } else if age <= 24 {
                let os = vec![Occupation::Student, Occupation::Programmer];
                rand::sample(&mut rng, os, 1).pop().unwrap()
            } else if age <= 60 {
                Occupation::Programmer
            } else {
                Occupation::Unemployed
            };
            let income = match occupation {
                Occupation::Unemployed => low_income.ind_sample(&mut rng),
                Occupation::Student => mid_income.ind_sample(&mut rng),
                Occupation::Programmer => hii_income.ind_sample(&mut rng),
            };
            // println!("{:?}", age);
            // println!("{:?}", occupation);
            // println!("{:?}", income);
            Datum {
                age: age,
                income: income,
                occupation: occupation,
            }
        })
        .collect();

    // user needs to specify how to transform their existing data
    // into rows (i.e. each entry becomes a Vec<Var>)
    let dataset = Dataset::<Vars>::new(pop,
                                       |d| {
                                           vec![Var::Int(d.age as i64),
                                                Var::Int(d.income as i64),
                                                Var::Int(d.occupation as i64)]
                                       },
                                       vec![Vars::Age, Vars::Income, Vars::Occupation]);

    let mut graph = BNet::new(dataset);
    graph.add_edge(Vars::Age, Vars::Occupation);
    graph.add_edge(Vars::Occupation, Vars::Income);
    graph.register_grouper(Vars::Occupation, |ref var| {
        match **var {
            Var::Int(i) => i as usize,
            _ => 0,
        }
    });
    graph.register_grouper(Vars::Age, |ref var| {
        match **var {
            Var::Int(i) => {
                if i <= 14 {
                    0
                } else if i <= 24 {
                    1
                } else if i <= 60 {
                    2
                } else {
                    3
                }
            }
            _ => 4,
        }
    });
    graph.register_sampler(Vars::Age, |i| {
        let mut rng = rand::thread_rng();
        let ages = if i <= 14 {
            Range::new(1, 14)
        } else if i <= 24 {
            Range::new(15, 24)
        } else if i <= 60 {
            Range::new(25, 60)
        } else {
            Range::new(61, 100)
        };
        Var::Int(ages.ind_sample(&mut rng))
    });

    let mut given = HashMap::new();
    // given.insert(Vars::Age, Var::Int(50)); // should be 100% chance of occupation 2
    given.insert(Vars::Age, Var::Int(20)); // should be 50/50 occupations 1 and 2
    let prior = graph.p_n(&Vars::Occupation);
    let posterior = graph.probs_given(&Vars::Occupation, given);
    println!("{:?}", prior);
    println!("{:?}", posterior);
}
