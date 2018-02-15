#[cfg(test)]
#[macro_use]
extern crate compacts;
#[cfg(not(test))]
extern crate compacts;

extern crate linked_hash_map;
extern crate parking_lot;
extern crate rocksdb;

pub mod cache;

mod store;
mod index;
#[cfg(test)]
mod tests;

pub use compacts::bits;
pub use store::{Seek, Store};
pub use index::{Index, SharedIndex};

pub type Bytes = Vec<u8>;
