extern crate rand;
use std::fmt::Debug;
use std::hash::Hash;
use std::collections::HashMap;
use rand::distributions::{IndependentSample, Range, Weighted, WeightedChoice};

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

struct Dataset<T: Eq + PartialEq + Hash + Debug> {
    // map var to col index
    cols: HashMap<T, usize>,
    rows: Vec<Vec<Var>>,
}

impl<T: Eq + PartialEq + Hash + Debug> Dataset<T> {
    pub fn new<F, D>(mut data: Vec<D>, to_row: F, mut cols: Vec<T>) -> Dataset<T>
        where F: Fn(D) -> Vec<Var>
    {
        let n_cols = cols.len();
        Dataset::<T> {
            cols: cols.drain(..).zip((0..n_cols)).collect(),
            rows: data.drain(..).map(to_row).collect(),
        }
    }
}

// random variable
// either categorical or continuous
// TODO extend this, a bit wasteful e.g. if you only need uints
#[derive(Clone, Debug, PartialEq)]
enum Var {
    Int(i64),
    Float(f64),
}

struct BNet<T: Eq + PartialEq + Hash + Debug + Clone> {
    graph: HashMap<T, Vec<T>>,
    dataset: Dataset<T>,
    groupers: HashMap<T, Box<Fn(&Var) -> usize>>,
    samplers: HashMap<T, Box<Fn(usize) -> Var>>,
}

impl<T: Eq + PartialEq + Hash + Debug + Clone> BNet<T> {
    pub fn new(dataset: Dataset<T>) -> BNet<T> {
        BNet {
            graph: HashMap::new(),
            groupers: HashMap::new(),
            samplers: HashMap::new(),
            dataset: dataset,
        }
    }

    pub fn register_grouper<F>(&mut self, n: T, func: F) -> ()
        where F: Fn(&Var) -> usize + 'static
    {
        self.groupers.insert(n, Box::new(func));
    }

    pub fn register_sampler<F>(&mut self, n: T, func: F) -> ()
        where F: Fn(usize) -> Var + 'static
    {
        self.samplers.insert(n, Box::new(func));
    }

    pub fn add_edge(&mut self, from: T, to: T) {
        self.graph.entry(from).or_insert_with(Vec::new).push(to);
    }

    // Get parents for a node.
    fn parents(&self, n: &T) -> Vec<&T> {
        self.graph
            .iter()
            .filter_map(|(k, v)| { if v.contains(n) { Some(k) } else { None } })
            .collect()
    }

    // Turn groups into a distribution, i.e. p(n) for a node.
    fn p_n(&self, n: &T) -> HashMap<usize, f64> {
        let groups = self.groups(n);
        let len = self.dataset.rows.len() as f64;
        groups.iter().map(|(k, v)| (*k, (v.len() as f64) / len)).collect()
    }

    // Group dataset rows by a variable using a grouper function.
    fn groups(&self, n: &T) -> HashMap<usize, Vec<Vec<Var>>> {
        self.group_by(n, &self.dataset.rows)
    }

    fn group_by(&self, n: &T, rows: &Vec<Vec<Var>>) -> HashMap<usize, Vec<Vec<Var>>> {
        let grouper = self.groupers.get(&n).unwrap();
        let mut groups = HashMap::<usize, Vec<Vec<Var>>>::new();
        let idx = *self.dataset.cols.get(&n).unwrap();
        let _: Vec<()> = rows.iter()
            .map(|r| {
                let group = grouper(&r[idx]);
                groups.entry(group).or_insert(vec![]).push(r.to_vec());
            })
            .collect();
        groups
    }

    // Probability distribution of a node given other variable values, assuming given variables are independent.
    // That is, it computes: prod(p(g|x_i) for x_i in given)/Z
    fn probs_given(&self, n: &T, given: HashMap<T, Var>) -> HashMap<usize, f64> {
        let mut probs = HashMap::new();
        let prior = self.p_n(n);
        let col = *self.dataset.cols.get(n).unwrap();
        let _: Vec<()> = self.groups(n)
            .iter()
            .map(|(k, group)| {
                let mut likelihood = 1.;
                let group_size = group.len() as f64;
                let prior_prob = prior.get(k).unwrap();

                let _: Vec<()> = given.iter()
                    .map(|(given_k, given_v)| {
                        // get the rows where the var `given_k` equals the val `given_v`
                        let rows_for_given = match self.groupers.get(given_k) {
                            // use a grouper, if one is specified
                            Some(grouper) => {
                                let group_id = grouper(given_v);
                                let subgroups = self.group_by(given_k, group);
                                match subgroups.get(&group_id) {
                                    Some(rows) => rows.len() as f64,
                                    None => 0.0,
                                }
                            }

                            // otherwise just get filter rows for exact matches
                            None => {
                                let col_ = *self.dataset.cols.get(given_k).unwrap();
                                group.iter().filter(|r| r[col_] == *given_v).count() as f64
                            }
                        };
                        likelihood *= rows_for_given / group_size;
                    })
                    .collect();

                probs.insert(*k, prior_prob * likelihood);
            })
            .collect();

        // normalize
        let total: f64 = probs.values().sum();
        for (_, prob) in probs.iter_mut() {
            *prob /= total;
        }

        probs
    }

    // b/c of rust's strictness with floats,
    // we have to convert probs to integers...
    // we lose fidelity as a result.
    fn probs_to_weights(&self, probs: HashMap<usize, f64>) -> Vec<Weighted<usize>> {
        probs.iter()
            .map(|(k, p)| {
                Weighted {
                    item: *k,
                    weight: (p * 1000.) as u32,
                }
            })
            .collect()
    }

    pub fn sample_node(&self, n: &T, mut sampled: HashMap<T, Var>) -> HashMap<T, Var> {
        let parents = self.parents(n);
        let dist = if parents.is_empty() {
            // if no parents, use p(n)
            self.p_n(n)
        } else {
            // if parents, use prod(p(n|x_i) for x_i in parents)
            // first, sample all parents
            for p in parents.iter() {
                if !sampled.contains_key(p) {
                    sampled = self.sample_node(p, sampled);
                }
            }

            // there must be a better way of doing this,
            // but the compiler is being touchy
            let mut s = sampled.clone();
            let given: HashMap<T, Var> = s.drain()
                .filter(|&(ref k, _)| parents.contains(&&k))
                .collect();
            self.probs_given(n, given)
        };
        let mut rng = rand::thread_rng();
        let mut choices = self.probs_to_weights(dist);
        let wc = WeightedChoice::new(&mut choices);
        let choice = wc.ind_sample(&mut rng);

        let val = match self.samplers.get(n) {
            Some(sampler) => sampler(choice),
            None => Var::Int(choice as i64),
        };
        sampled.insert(n.clone(), val);
        sampled
    }
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
