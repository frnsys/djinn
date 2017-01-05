use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;

fn hash<T: Hash>(t: &T) -> u64 {
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
    pub fn hash(&self, id: &String) -> usize {
        let w = self.n_workers as f64;
        let h = hash(id);
        let p = (h as f64) / (u64::max_value() as f64);
        for j in 0..self.n_workers {
            if (j as f64) / w <= p && ((j + 1) as f64) / w > p {
                return j;
            }
        }
        self.n_workers - 1
    }
}
