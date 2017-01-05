use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;

pub fn hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

#[derive(Clone)]
pub struct WHasher {
    n_workers: usize,
}

impl WHasher {
    pub fn new(n_workers: usize) -> WHasher {
        WHasher { n_workers: n_workers }
    }

    /// Hashes an id to a value in the range of `n_workers`.
    pub fn hash(&self, id: u64) -> usize {
        let w = self.n_workers as f64;
        let p = (id as f64) / (u64::max_value() as f64);
        for j in 0..self.n_workers {
            if (j as f64) / w <= p && ((j + 1) as f64) / w > p {
                return j;
            }
        }
        self.n_workers - 1
    }
}
