//! Conveniently read from a YAML file, e.g. for loading configs.

use std::io::Read;
use std::fs::File;
use std::path::Path;
use yaml_rust::{YamlLoader, Yaml};

/// Easily load a YAML file by filename.
pub fn load_from_yaml(fname: &str) -> Yaml {
    let path = Path::new(fname);
    let mut file = File::open(&path).unwrap();
    let mut s = String::new();
    file.read_to_string(&mut s).unwrap();

    // just return the first yaml doc
    let mut docs = YamlLoader::load_from_str(&s).unwrap();
    docs.remove(0)
}
