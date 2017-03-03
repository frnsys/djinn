extern crate rand;
use std::hash::Hash;
use std::collections::HashMap;
use rand::distributions::{IndependentSample, Range};

#[derive(Debug)]
enum Occupation {
    Unemployed,
    Student,
    Programmer,
}

#[derive(Eq, PartialEq, Hash)]
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


struct Dataset<T: Eq + PartialEq + Hash> {
    // map var to col index
    cols: HashMap<T, usize>,
    rows: Vec<Vec<Var>>,
}

impl<T: Eq + PartialEq + Hash> Dataset<T> {
    pub fn new<F, D>(mut data: Vec<D>, to_row: F, mut cols: Vec<T>) -> Dataset<T>
        where F: Fn(D) -> Vec<Var>
    {
        let n_cols = cols.len();
        Dataset::<T> {
            cols: cols.drain(..).zip((0..n_cols)).collect(),
            rows: data.drain(..).map(to_row).collect(),
        }
    }

    // Group rows in the dataset by a column.
    // Pass in a binning function, which returns the group a given row belongs to.
    fn group_by<F>(&self, var: T, bins: F) -> HashMap<usize, Vec<Vec<Var>>>
        where F: Fn(&Var) -> usize
    {
        let mut groups = HashMap::<usize, Vec<Vec<Var>>>::new();
        let idx = *self.cols.get(&var).unwrap();
        self.rows.iter().map(|r| {
            let group = bins(&r[idx]);
            groups.entry(group).or_insert(vec![]).push(r.to_vec());
        });
        groups // TODO
    }
}

// random variable
// either categorical or continuous
// TODO extend this, a bit wasteful e.g. if you only need uints
#[derive(Clone)]
enum Var {
    Int(i64),
    Float(f64),
}

struct BNet<T: Eq + PartialEq + Hash> {
    graph: HashMap<T, T>,
}

impl<T: Eq + PartialEq + Hash> BNet<T> {
    pub fn new() -> BNet<T> {
        BNet { graph: HashMap::new() }
    }

    // Add an edge from one variable A to another B,
    // indicating that A influences B.
    pub fn add_edge(&mut self, from: T, to: T) {
        self.graph.insert(from, to);
    }
}

// need to:
// [X] specify graph
// [X] group/bin data by variable

fn main() {
    // a row of is a column_name->variable hashmap
    // let row = HashMap::<str, Var>::new();

    // generate a dataset
    let mut rng = rand::thread_rng();
    let ages = Range::new(1, 100);
    let low_income = Range::new(0, 20000);
    let mid_income = Range::new(10000, 30000);
    let hii_income = Range::new(60000, 100000);
    let pop: Vec<Datum> = (0..10000)
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
                income: 0,
                occupation: Occupation::Unemployed,
            }
        })
        .collect();

    // need to convert the data into "rows"
    let dataset = Dataset::<Vars>::new(pop,
                                       |d| {
                                           vec![Var::Int(d.age as i64),
                                                Var::Int(d.income as i64),
                                                Var::Int(d.occupation as i64)]
                                       },
                                       vec![Vars::Age, Vars::Income, Vars::Occupation]);
    println!("{:?}", dataset.rows.len());

    let mut graph = BNet::new();
    graph.add_edge(Vars::Age, Vars::Occupation);
    graph.add_edge(Vars::Occupation, Vars::Income);
}
