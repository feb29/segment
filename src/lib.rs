extern crate compacts;
extern crate rocksdb;
extern crate parking_lot;

mod repr;

use std::collections::BTreeMap;
use std::ops;
use parking_lot::Mutex;
use self::repr::Repr;

/// Table<T>
#[derive(Debug)]
pub struct Table<T> {
    table: Mutex<BTreeMap<T, Repr<T>>>,
}

impl<T> Table<T>
where
    T: Ord,
{
    pub fn new() -> Self {
        Table {
            table: Mutex::new(BTreeMap::new()),
        }
    }
}

// static T: &bool = &true;
// static F: &bool = &false;

// impl<T> ops::Index<T> for Table<T>
// where
//     T: Ord,
// {
//     type Output = Repr<T>;
//     fn index(&self, t: T) -> &Self::Output {
//         let tab = self.table.lock();
//         &tab[&t]
//     }
// }
