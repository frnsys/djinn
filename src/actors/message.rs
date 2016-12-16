use std::fmt::Debug;
use rustc_serialize::{Decodable, Encodable};

pub trait Message: Decodable + Encodable + Debug + Send + Sync {}
impl<T> Message for T where T: Decodable + Encodable + Debug + Send + Sync {}
